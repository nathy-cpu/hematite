//! SQL parser.
//!
//! The parser consumes lexer tokens and builds the AST used by planning.
//!
//! ```text
//! tokens -> statement kind
//!              |
//!              +--> CREATE / DROP
//!              +--> INSERT / UPDATE / DELETE
//!              +--> SELECT
//!                        |
//!                        +--> projection
//!                        +--> source table
//!                        +--> WHERE expression tree
//!                        +--> ORDER BY / LIMIT
//! ```
//!
//! Hematite keeps the grammar strict and explicit so later stages stay small and do not need to
//! repair ambiguous SQL.

use crate::error::{HematiteError, Result};
use crate::parser::ast::*;
use crate::parser::lexer::{Lexer, Token};
use crate::parser::types::{LiteralValue, SqlTypeName};

pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
    parameter_count: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            position: 0,
            parameter_count: 0,
        }
    }

    pub fn parse(&mut self) -> Result<Statement> {
        if self.tokens.is_empty() {
            return Err(HematiteError::ParseError("Empty input".to_string()));
        }

        let token = self.peek_token()?;

        match token {
            Token::Begin => self.parse_begin(),
            Token::Commit => self.parse_commit(),
            Token::Rollback => self.parse_rollback(),
            Token::Savepoint => self.parse_savepoint(),
            Token::Release => self.parse_release_savepoint(),
            Token::Explain => self.parse_explain(),
            Token::Describe => self.parse_describe(),
            Token::Show => self.parse_show(),
            Token::Select | Token::With => self.parse_select(),
            Token::Update => self.parse_update(),
            Token::Insert => self.parse_insert(),
            Token::Delete => self.parse_delete(),
            Token::Create => self.parse_create(),
            Token::Alter => self.parse_alter(),
            Token::Drop => self.parse_drop(),
            _ => Err(HematiteError::ParseError(format!(
                "Expected BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, EXPLAIN, DESCRIBE, SHOW, SELECT, UPDATE, INSERT, DELETE, CREATE, ALTER, or DROP, found: {:?}",
                token
            ))),
        }
    }

    fn parse_begin(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Begin)?;
        self.consume_token(&Token::Semicolon)?;
        Ok(Statement::Begin)
    }

    fn parse_commit(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Commit)?;
        self.consume_token(&Token::Semicolon)?;
        Ok(Statement::Commit)
    }

    fn parse_rollback(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Rollback)?;
        if matches!(self.peek_token(), Ok(Token::To)) {
            self.consume_token(&Token::To)?;
            if matches!(self.peek_token(), Ok(Token::Savepoint)) {
                self.consume_token(&Token::Savepoint)?;
            }
            let name = self.parse_identifier()?;
            self.consume_token(&Token::Semicolon)?;
            return Ok(Statement::RollbackToSavepoint(name));
        }
        self.consume_token(&Token::Semicolon)?;
        Ok(Statement::Rollback)
    }

    fn parse_savepoint(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Savepoint)?;
        let name = self.parse_identifier()?;
        self.consume_token(&Token::Semicolon)?;
        Ok(Statement::Savepoint(name))
    }

    fn parse_release_savepoint(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Release)?;
        if matches!(self.peek_token(), Ok(Token::Savepoint)) {
            self.consume_token(&Token::Savepoint)?;
        }
        let name = self.parse_identifier()?;
        self.consume_token(&Token::Semicolon)?;
        Ok(Statement::ReleaseSavepoint(name))
    }

    fn parse_select(&mut self) -> Result<Statement> {
        let (query, into_table) = self.parse_query_statement(true, true)?;
        match into_table {
            Some(table) => Ok(Statement::SelectInto(SelectIntoStatement { table, query })),
            None => Ok(Statement::Select(query)),
        }
    }

    fn parse_explain(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Explain)?;
        Ok(Statement::Explain(ExplainStatement {
            statement: Box::new(self.parse()?),
        }))
    }

    fn parse_describe(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Describe)?;
        let table = self.parse_identifier()?;
        self.consume_token(&Token::Semicolon)?;
        Ok(Statement::Describe(DescribeStatement { table }))
    }

    fn parse_show(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Show)?;
        match self.peek_token()? {
            Token::Tables => {
                self.consume_token(&Token::Tables)?;
                self.consume_token(&Token::Semicolon)?;
                Ok(Statement::ShowTables)
            }
            Token::Views => {
                self.consume_token(&Token::Views)?;
                self.consume_token(&Token::Semicolon)?;
                Ok(Statement::ShowViews)
            }
            Token::Indexes => {
                self.consume_token(&Token::Indexes)?;
                let table = if matches!(self.peek_token(), Ok(Token::From)) {
                    self.consume_token(&Token::From)?;
                    Some(self.parse_identifier()?)
                } else {
                    None
                };
                self.consume_token(&Token::Semicolon)?;
                Ok(Statement::ShowIndexes(table))
            }
            Token::Triggers => {
                self.consume_token(&Token::Triggers)?;
                let table = if matches!(self.peek_token(), Ok(Token::From)) {
                    self.consume_token(&Token::From)?;
                    Some(self.parse_identifier()?)
                } else {
                    None
                };
                self.consume_token(&Token::Semicolon)?;
                Ok(Statement::ShowTriggers(table))
            }
            Token::Create => {
                self.consume_token(&Token::Create)?;
                match self.peek_token()? {
                    Token::Table => {
                        self.consume_token(&Token::Table)?;
                        let table = self.parse_identifier()?;
                        self.consume_token(&Token::Semicolon)?;
                        Ok(Statement::ShowCreateTable(table))
                    }
                    Token::View => {
                        self.consume_token(&Token::View)?;
                        let view = self.parse_identifier()?;
                        self.consume_token(&Token::Semicolon)?;
                        Ok(Statement::ShowCreateView(view))
                    }
                    token => Err(HematiteError::ParseError(format!(
                        "Expected TABLE or VIEW after SHOW CREATE, found: {:?}",
                        token
                    ))),
                }
            }
            token => Err(HematiteError::ParseError(format!(
                "Expected TABLES, VIEWS, INDEXES, TRIGGERS, or CREATE after SHOW, found: {:?}",
                token
            ))),
        }
    }

    fn parse_query_statement(
        &mut self,
        expect_semicolon: bool,
        allow_into: bool,
    ) -> Result<(SelectStatement, Option<String>)> {
        let with_clause = if matches!(self.peek_token(), Ok(Token::With)) {
            self.parse_with_clause()?
        } else {
            Vec::new()
        };

        let (mut statement, into_table) =
            self.parse_select_statement(expect_semicolon, allow_into)?;
        statement.with_clause = with_clause;
        Ok((statement, into_table))
    }

    fn parse_select_statement(
        &mut self,
        expect_semicolon: bool,
        allow_into: bool,
    ) -> Result<(SelectStatement, Option<String>)> {
        self.consume_token(&Token::Select)?;
        let distinct = if matches!(self.peek_token(), Ok(Token::Distinct)) {
            self.consume_token(&Token::Distinct)?;
            true
        } else {
            false
        };

        let (columns, column_aliases) = self.parse_select_columns()?;

        let into_table = if allow_into && matches!(self.peek_token(), Ok(Token::Into)) {
            self.consume_token(&Token::Into)?;
            Some(self.parse_identifier()?)
        } else {
            None
        };

        self.consume_token(&Token::From)?;

        let from = self.parse_from_clause()?;

        let where_clause = if self.peek_token()? == Token::Where {
            Some(self.parse_where_clause()?)
        } else {
            None
        };

        let group_by = if matches!(self.peek_token(), Ok(Token::Group)) {
            self.parse_group_by_clause()?
        } else {
            Vec::new()
        };

        let having_clause = if matches!(self.peek_token(), Ok(Token::Having)) {
            Some(self.parse_having_clause()?)
        } else {
            None
        };

        let order_by = if matches!(self.peek_token(), Ok(Token::Order)) {
            self.parse_order_by_clause()?
        } else {
            Vec::new()
        };

        let (limit, limit_offset) = if matches!(self.peek_token(), Ok(Token::Limit)) {
            self.parse_limit_clause()?
        } else {
            (None, None)
        };

        let offset = if limit_offset.is_some() {
            limit_offset
        } else if matches!(self.peek_token(), Ok(Token::Offset)) {
            Some(self.parse_offset_clause()?)
        } else {
            None
        };

        let set_operation = if matches!(
            self.peek_token(),
            Ok(Token::Union | Token::Intersect | Token::Except)
        ) {
            let operator = match self.peek_token()? {
                Token::Union => {
                    self.consume_token(&Token::Union)?;
                    if matches!(self.peek_token(), Ok(Token::All)) {
                        self.consume_token(&Token::All)?;
                        SetOperator::UnionAll
                    } else {
                        SetOperator::Union
                    }
                }
                Token::Intersect => {
                    self.consume_token(&Token::Intersect)?;
                    SetOperator::Intersect
                }
                Token::Except => {
                    self.consume_token(&Token::Except)?;
                    SetOperator::Except
                }
                token => {
                    return Err(HematiteError::ParseError(format!(
                        "Expected set operation, found: {:?}",
                        token
                    )))
                }
            };

            Some(SetOperation {
                operator,
                right: Box::new(self.parse_select_statement(false, false)?.0),
            })
        } else {
            None
        };

        if expect_semicolon {
            self.consume_token(&Token::Semicolon)?;
        }

        Ok((
            SelectStatement {
                with_clause: Vec::new(),
                distinct,
                columns,
                column_aliases,
                from,
                where_clause,
                group_by,
                having_clause,
                order_by,
                limit,
                offset,
                set_operation,
            },
            into_table,
        ))
    }

    fn parse_with_clause(&mut self) -> Result<Vec<CommonTableExpression>> {
        self.consume_token(&Token::With)?;
        let recursive = if matches!(self.peek_token(), Ok(Token::Recursive)) {
            self.consume_token(&Token::Recursive)?;
            true
        } else {
            false
        };
        let mut ctes = Vec::new();

        loop {
            let name = self.parse_identifier()?;
            self.consume_token(&Token::As)?;
            self.consume_token(&Token::LeftParen)?;
            let query = self.parse_query_statement(false, false)?.0;
            self.consume_token(&Token::RightParen)?;
            ctes.push(CommonTableExpression {
                name,
                recursive,
                query: Box::new(query),
            });

            if matches!(self.peek_token(), Ok(Token::Comma)) {
                self.consume_token(&Token::Comma)?;
                continue;
            }

            break;
        }

        Ok(ctes)
    }

    fn parse_select_columns(&mut self) -> Result<(Vec<SelectItem>, Vec<Option<String>>)> {
        let mut columns = Vec::new();
        let mut aliases = Vec::new();

        let token = self.peek_token()?;

        if token == Token::Asterisk {
            self.consume_token(&Token::Asterisk)?;
            columns.push(SelectItem::Wildcard);
            aliases.push(None);
        } else {
            loop {
                columns.push(self.parse_select_item()?);
                aliases.push(self.parse_optional_alias()?);

                if self.peek_token()? == Token::Comma {
                    self.consume_token(&Token::Comma)?;
                    continue;
                } else {
                    break;
                }
            }
        }

        Ok((columns, aliases))
    }

    fn parse_select_item(&mut self) -> Result<SelectItem> {
        let token = self.peek_token()?;
        match token {
            Token::Count | Token::Sum | Token::Avg | Token::Min | Token::Max => {
                let expr = self.parse_aggregate_expression()?;
                if matches!(self.peek_token(), Ok(Token::Over)) {
                    self.parse_window_select_item(expr)
                } else {
                    self.aggregate_expression_to_select_item(expr)
                }
            }
            Token::Identifier(ref name)
                if self.next_token_is(&Token::LeftParen) && is_window_only_function_name(name) =>
            {
                self.parse_window_only_select_item()
            }
            Token::Identifier(_)
            | Token::StringLiteral(_)
            | Token::NumberLiteral(_)
            | Token::BooleanLiteral(_)
            | Token::Null
            | Token::NullLiteral
            | Token::Placeholder
            | Token::LeftParen
            | Token::Case
            | Token::Cast
            | Token::Interval
            | Token::Date
            | Token::Time
            | Token::DateTime
            | Token::Timestamp
            | Token::Left
            | Token::Right
            | Token::Minus => {
                let expr = self.parse_expression()?;
                Ok(match expr {
                    Expression::Column(name) => SelectItem::Column(name),
                    expr => SelectItem::Expression(expr),
                })
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected select item or aggregate, found: {:?}",
                token
            ))),
        }
    }

    fn parse_aggregate_expression(&mut self) -> Result<Expression> {
        let function = match self.peek_token()? {
            Token::Count => {
                self.consume_token(&Token::Count)?;
                AggregateFunction::Count
            }
            Token::Sum => {
                self.consume_token(&Token::Sum)?;
                AggregateFunction::Sum
            }
            Token::Avg => {
                self.consume_token(&Token::Avg)?;
                AggregateFunction::Avg
            }
            Token::Min => {
                self.consume_token(&Token::Min)?;
                AggregateFunction::Min
            }
            Token::Max => {
                self.consume_token(&Token::Max)?;
                AggregateFunction::Max
            }
            token => {
                return Err(HematiteError::ParseError(format!(
                    "Expected aggregate function, found: {:?}",
                    token
                )))
            }
        };

        self.consume_token(&Token::LeftParen)?;
        if function == AggregateFunction::Count && matches!(self.peek_token(), Ok(Token::Asterisk))
        {
            self.consume_token(&Token::Asterisk)?;
            self.consume_token(&Token::RightParen)?;
            return Ok(Expression::AggregateCall {
                function,
                target: AggregateTarget::All,
            });
        }

        let column = self.parse_identifier_reference()?;
        self.consume_token(&Token::RightParen)?;

        Ok(Expression::AggregateCall {
            function,
            target: AggregateTarget::Column(column),
        })
    }

    fn aggregate_expression_to_select_item(&self, expression: Expression) -> Result<SelectItem> {
        match expression {
            Expression::AggregateCall {
                function: AggregateFunction::Count,
                target: AggregateTarget::All,
            } => Ok(SelectItem::CountAll),
            Expression::AggregateCall {
                function,
                target: AggregateTarget::Column(column),
            } => Ok(SelectItem::Aggregate { function, column }),
            _ => Err(HematiteError::InternalError(
                "aggregate expression parser returned a non-aggregate expression".to_string(),
            )),
        }
    }

    fn parse_window_only_select_item(&mut self) -> Result<SelectItem> {
        let function_name = self.parse_identifier()?;
        self.consume_token(&Token::LeftParen)?;
        self.consume_token(&Token::RightParen)?;
        let function = match function_name.to_ascii_uppercase().as_str() {
            "ROW_NUMBER" => WindowFunction::RowNumber,
            "RANK" => WindowFunction::Rank,
            "DENSE_RANK" => WindowFunction::DenseRank,
            _ => {
                return Err(HematiteError::ParseError(format!(
                    "Unsupported window function '{}'",
                    function_name
                )))
            }
        };
        self.parse_window_item(function)
    }

    fn parse_window_select_item(&mut self, expression: Expression) -> Result<SelectItem> {
        let Expression::AggregateCall { function, target } = expression else {
            return Err(HematiteError::ParseError(
                "OVER(...) currently requires a ranking or aggregate function".to_string(),
            ));
        };
        self.parse_window_item(WindowFunction::Aggregate { function, target })
    }

    fn parse_window_item(&mut self, function: WindowFunction) -> Result<SelectItem> {
        self.consume_token(&Token::Over)?;
        self.consume_token(&Token::LeftParen)?;
        let partition_by = if matches!(self.peek_token(), Ok(Token::Partition)) {
            self.consume_token(&Token::Partition)?;
            self.consume_token(&Token::By)?;
            let mut exprs = Vec::new();
            loop {
                exprs.push(self.parse_expression()?);
                if matches!(self.peek_token(), Ok(Token::Comma)) {
                    self.consume_token(&Token::Comma)?;
                    continue;
                }
                break;
            }
            exprs
        } else {
            Vec::new()
        };
        let order_by = if matches!(self.peek_token(), Ok(Token::Order)) {
            self.parse_order_by_clause()?
        } else {
            Vec::new()
        };
        self.consume_token(&Token::RightParen)?;
        Ok(SelectItem::Window {
            function,
            window: WindowSpec {
                partition_by,
                order_by,
            },
        })
    }

    fn parse_from_clause(&mut self) -> Result<TableReference> {
        let from = self.parse_table_reference()?;
        self.parse_join_chain(from)
    }

    fn parse_table_reference(&mut self) -> Result<TableReference> {
        match self.peek_token()? {
            Token::Identifier(_) => {
                let table_name = self.parse_identifier()?;
                let alias = self.parse_optional_alias()?;
                Ok(TableReference::Table(table_name, alias))
            }
            Token::LeftParen => {
                self.consume_token(&Token::LeftParen)?;
                let subquery = self.parse_query_statement(false, false)?.0;
                self.consume_token(&Token::RightParen)?;
                let alias = self.parse_required_alias("derived table")?;
                Ok(TableReference::Derived {
                    subquery: Box::new(subquery),
                    alias,
                })
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected table name, found: {:?}",
                self.peek_token()?
            ))),
        }
    }

    fn parse_join_chain(&mut self, mut from: TableReference) -> Result<TableReference> {
        loop {
            match self.peek_token()? {
                Token::Comma => {
                    self.consume_token(&Token::Comma)?;
                    let right = self.parse_table_reference()?;
                    from = TableReference::CrossJoin(Box::new(from), Box::new(right));
                }
                Token::Join => {
                    self.consume_token(&Token::Join)?;
                    let right = self.parse_table_reference()?;
                    self.consume_token(&Token::On)?;
                    let on = self.parse_or_condition()?;
                    from = TableReference::InnerJoin {
                        left: Box::new(from),
                        right: Box::new(right),
                        on,
                    };
                }
                Token::Inner => {
                    self.consume_token(&Token::Inner)?;
                    self.consume_token(&Token::Join)?;
                    let right = self.parse_table_reference()?;
                    self.consume_token(&Token::On)?;
                    let on = self.parse_or_condition()?;
                    from = TableReference::InnerJoin {
                        left: Box::new(from),
                        right: Box::new(right),
                        on,
                    };
                }
                Token::Left => {
                    self.consume_token(&Token::Left)?;
                    if matches!(self.peek_token(), Ok(Token::Outer)) {
                        self.consume_token(&Token::Outer)?;
                    }
                    self.consume_token(&Token::Join)?;
                    let right = self.parse_table_reference()?;
                    self.consume_token(&Token::On)?;
                    let on = self.parse_or_condition()?;
                    from = TableReference::LeftJoin {
                        left: Box::new(from),
                        right: Box::new(right),
                        on,
                    };
                }
                Token::Right => {
                    self.consume_token(&Token::Right)?;
                    if matches!(self.peek_token(), Ok(Token::Outer)) {
                        self.consume_token(&Token::Outer)?;
                    }
                    self.consume_token(&Token::Join)?;
                    let right = self.parse_table_reference()?;
                    self.consume_token(&Token::On)?;
                    let on = self.parse_or_condition()?;
                    from = TableReference::RightJoin {
                        left: Box::new(from),
                        right: Box::new(right),
                        on,
                    };
                }
                Token::Full => {
                    self.consume_token(&Token::Full)?;
                    if matches!(self.peek_token(), Ok(Token::Outer)) {
                        self.consume_token(&Token::Outer)?;
                    }
                    self.consume_token(&Token::Join)?;
                    let right = self.parse_table_reference()?;
                    self.consume_token(&Token::On)?;
                    let on = self.parse_or_condition()?;
                    from = TableReference::FullOuterJoin {
                        left: Box::new(from),
                        right: Box::new(right),
                        on,
                    };
                }
                _ => break,
            }
        }

        Ok(from)
    }

    fn parse_where_clause(&mut self) -> Result<WhereClause> {
        self.consume_token(&Token::Where)?;

        let conditions = self.parse_conditions()?;

        Ok(WhereClause { conditions })
    }

    fn parse_order_by_clause(&mut self) -> Result<Vec<OrderByItem>> {
        self.consume_token(&Token::Order)?;
        self.consume_token(&Token::By)?;

        let mut items = Vec::new();
        loop {
            let column = self.parse_identifier_reference()?;
            let direction = match self.peek_token() {
                Ok(Token::Asc) => {
                    self.consume_token(&Token::Asc)?;
                    SortDirection::Asc
                }
                Ok(Token::Desc) => {
                    self.consume_token(&Token::Desc)?;
                    SortDirection::Desc
                }
                _ => SortDirection::Asc,
            };

            items.push(OrderByItem { column, direction });

            if matches!(self.peek_token(), Ok(Token::Comma)) {
                self.consume_token(&Token::Comma)?;
                continue;
            }

            break;
        }

        Ok(items)
    }

    fn parse_group_by_clause(&mut self) -> Result<Vec<Expression>> {
        self.consume_token(&Token::Group)?;
        self.consume_token(&Token::By)?;

        let mut items = Vec::new();
        loop {
            items.push(self.parse_expression()?);
            if matches!(self.peek_token(), Ok(Token::Comma)) {
                self.consume_token(&Token::Comma)?;
                continue;
            }
            break;
        }
        Ok(items)
    }

    fn parse_having_clause(&mut self) -> Result<WhereClause> {
        self.consume_token(&Token::Having)?;
        let conditions = self.parse_conditions()?;
        Ok(WhereClause { conditions })
    }

    fn parse_limit_clause(&mut self) -> Result<(Option<usize>, Option<usize>)> {
        self.consume_token(&Token::Limit)?;
        let first = self.parse_non_negative_integer_clause("LIMIT")?;
        if matches!(self.peek_token(), Ok(Token::Comma)) {
            self.consume_token(&Token::Comma)?;
            let second = self.parse_non_negative_integer_clause("LIMIT")?;
            Ok((Some(second), Some(first)))
        } else {
            Ok((Some(first), None))
        }
    }

    fn parse_offset_clause(&mut self) -> Result<usize> {
        self.consume_token(&Token::Offset)?;
        self.parse_non_negative_integer_clause("OFFSET")
    }

    fn parse_non_negative_integer_clause(&mut self, clause_name: &str) -> Result<usize> {
        match self.peek_token()? {
            Token::NumberLiteral(value) if value.fract() == 0.0 && value >= 0.0 => {
                self.consume_token(&Token::NumberLiteral(value))?;
                Ok(value as usize)
            }
            token => Err(HematiteError::ParseError(format!(
                "Expected non-negative integer after {}, found: {:?}",
                clause_name, token
            ))),
        }
    }

    fn parse_conditions(&mut self) -> Result<Vec<Condition>> {
        Ok(vec![self.parse_or_condition()?])
    }

    fn parse_or_condition(&mut self) -> Result<Condition> {
        let mut condition = self.parse_and_condition()?;

        while matches!(self.peek_token(), Ok(Token::Or)) {
            self.consume_token(&Token::Or)?;
            let right = self.parse_and_condition()?;
            condition = Condition::Logical {
                left: Box::new(condition),
                operator: LogicalOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(condition)
    }

    fn parse_and_condition(&mut self) -> Result<Condition> {
        let mut condition = self.parse_primary_condition()?;

        while matches!(self.peek_token(), Ok(Token::And)) {
            self.consume_token(&Token::And)?;
            let right = self.parse_primary_condition()?;
            condition = Condition::Logical {
                left: Box::new(condition),
                operator: LogicalOperator::And,
                right: Box::new(right),
            };
        }

        Ok(condition)
    }

    fn parse_primary_condition(&mut self) -> Result<Condition> {
        if self.peek_token()? == Token::Not {
            self.consume_token(&Token::Not)?;
            if self.peek_token()? == Token::Exists {
                self.consume_token(&Token::Exists)?;
                return self.parse_exists_condition(true);
            }
            return Ok(Condition::Not(Box::new(self.parse_primary_condition()?)));
        }

        if self.peek_token()? == Token::Exists {
            self.consume_token(&Token::Exists)?;
            return self.parse_exists_condition(false);
        }

        if self.peek_token()? == Token::LeftParen {
            self.consume_token(&Token::LeftParen)?;
            let condition = self.parse_or_condition()?;
            self.consume_token(&Token::RightParen)?;
            Ok(condition)
        } else {
            self.parse_condition()
        }
    }

    fn parse_condition(&mut self) -> Result<Condition> {
        let left = self.parse_value_expression()?;

        if matches!(self.peek_token(), Ok(Token::Not)) {
            self.consume_token(&Token::Not)?;
            if matches!(self.peek_token(), Ok(Token::In)) {
                self.consume_token(&Token::In)?;
                return self.parse_in_list_condition(left, true);
            }
            if matches!(self.peek_token(), Ok(Token::Like)) {
                self.consume_token(&Token::Like)?;
                return self.parse_like_condition(left, true);
            }
            if matches!(self.peek_token(), Ok(Token::Between)) {
                self.consume_token(&Token::Between)?;
                return self.parse_between_condition(left, true);
            }
            return Err(HematiteError::ParseError(
                "Expected IN, LIKE, or BETWEEN after NOT in predicate".to_string(),
            ));
        }

        if matches!(self.peek_token(), Ok(Token::In)) {
            self.consume_token(&Token::In)?;
            return self.parse_in_list_condition(left, false);
        }

        if matches!(self.peek_token(), Ok(Token::Between)) {
            self.consume_token(&Token::Between)?;
            return self.parse_between_condition(left, false);
        }

        if matches!(self.peek_token(), Ok(Token::Like)) {
            self.consume_token(&Token::Like)?;
            return self.parse_like_condition(left, false);
        }

        if matches!(self.peek_token(), Ok(Token::Is)) {
            self.consume_token(&Token::Is)?;
            let is_not = if matches!(self.peek_token(), Ok(Token::Not)) {
                self.consume_token(&Token::Not)?;
                true
            } else {
                false
            };
            self.consume_token(&Token::Null)?;
            return Ok(Condition::NullCheck { expr: left, is_not });
        }

        let operator = self.parse_comparison_operator()?;

        let right = self.parse_value_expression()?;

        Ok(Condition::Comparison {
            left,
            operator,
            right,
        })
    }

    fn parse_in_list_condition(&mut self, expr: Expression, is_not: bool) -> Result<Condition> {
        self.consume_token(&Token::LeftParen)?;
        if matches!(self.peek_token(), Ok(Token::Select | Token::With)) {
            let subquery = self.parse_query_statement(false, false)?.0;
            self.consume_token(&Token::RightParen)?;
            return Ok(Condition::InSubquery {
                expr,
                subquery: Box::new(subquery),
                is_not,
            });
        }

        let mut values = Vec::new();

        loop {
            values.push(self.parse_expression()?);
            if matches!(self.peek_token(), Ok(Token::Comma)) {
                self.consume_token(&Token::Comma)?;
                continue;
            }
            break;
        }

        if values.is_empty() {
            return Err(HematiteError::ParseError(
                "IN list must contain at least one expression".to_string(),
            ));
        }

        self.consume_token(&Token::RightParen)?;
        Ok(Condition::InList {
            expr,
            values,
            is_not,
        })
    }

    fn parse_exists_condition(&mut self, is_not: bool) -> Result<Condition> {
        self.consume_token(&Token::LeftParen)?;
        let subquery = self.parse_query_statement(false, false)?.0;
        self.consume_token(&Token::RightParen)?;
        Ok(Condition::Exists {
            subquery: Box::new(subquery),
            is_not,
        })
    }

    fn parse_between_condition(&mut self, expr: Expression, is_not: bool) -> Result<Condition> {
        let lower = self.parse_value_expression()?;
        self.consume_token(&Token::And)?;
        let upper = self.parse_value_expression()?;
        Ok(Condition::Between {
            expr,
            lower,
            upper,
            is_not,
        })
    }

    fn parse_like_condition(&mut self, expr: Expression, is_not: bool) -> Result<Condition> {
        let pattern = self.parse_value_expression()?;
        Ok(Condition::Like {
            expr,
            pattern,
            is_not,
        })
    }

    fn parse_expression(&mut self) -> Result<Expression> {
        self.parse_or_expression()
    }

    fn parse_or_expression(&mut self) -> Result<Expression> {
        let mut expr = self.parse_and_expression()?;

        while matches!(self.peek_token(), Ok(Token::Or)) {
            self.consume_token(&Token::Or)?;
            let right = self.parse_and_expression()?;
            expr = Expression::Logical {
                left: Box::new(expr),
                operator: LogicalOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(expr)
    }

    fn parse_and_expression(&mut self) -> Result<Expression> {
        let mut expr = self.parse_not_expression()?;

        while matches!(self.peek_token(), Ok(Token::And)) {
            self.consume_token(&Token::And)?;
            let right = self.parse_not_expression()?;
            expr = Expression::Logical {
                left: Box::new(expr),
                operator: LogicalOperator::And,
                right: Box::new(right),
            };
        }

        Ok(expr)
    }

    fn parse_not_expression(&mut self) -> Result<Expression> {
        if matches!(self.peek_token(), Ok(Token::Not)) {
            self.consume_token(&Token::Not)?;
            return Ok(Expression::UnaryNot(Box::new(self.parse_not_expression()?)));
        }

        self.parse_predicate_expression()
    }

    fn parse_predicate_expression(&mut self) -> Result<Expression> {
        if matches!(self.peek_token(), Ok(Token::Exists)) {
            self.consume_token(&Token::Exists)?;
            return self.parse_exists_expression(false);
        }

        let left = self.parse_value_expression()?;

        if matches!(self.peek_token(), Ok(Token::Not)) {
            self.consume_token(&Token::Not)?;
            if matches!(self.peek_token(), Ok(Token::In)) {
                self.consume_token(&Token::In)?;
                return self.parse_in_list_expression(left, true);
            }
            if matches!(self.peek_token(), Ok(Token::Like)) {
                self.consume_token(&Token::Like)?;
                return self.parse_like_expression(left, true);
            }
            if matches!(self.peek_token(), Ok(Token::Between)) {
                self.consume_token(&Token::Between)?;
                return self.parse_between_expression(left, true);
            }
            return Err(HematiteError::ParseError(
                "Expected IN, LIKE, or BETWEEN after NOT in expression".to_string(),
            ));
        }

        if matches!(self.peek_token(), Ok(Token::In)) {
            self.consume_token(&Token::In)?;
            return self.parse_in_list_expression(left, false);
        }

        if matches!(self.peek_token(), Ok(Token::Between)) {
            self.consume_token(&Token::Between)?;
            return self.parse_between_expression(left, false);
        }

        if matches!(self.peek_token(), Ok(Token::Like)) {
            self.consume_token(&Token::Like)?;
            return self.parse_like_expression(left, false);
        }

        if matches!(self.peek_token(), Ok(Token::Is)) {
            self.consume_token(&Token::Is)?;
            let is_not = if matches!(self.peek_token(), Ok(Token::Not)) {
                self.consume_token(&Token::Not)?;
                true
            } else {
                false
            };
            self.consume_token(&Token::Null)?;
            return Ok(Expression::NullCheck {
                expr: Box::new(left),
                is_not,
            });
        }

        if self.peek_token_starts_comparison() {
            let operator = self.parse_comparison_operator()?;
            let right = self.parse_value_expression()?;
            return Ok(Expression::Comparison {
                left: Box::new(left),
                operator,
                right: Box::new(right),
            });
        }

        Ok(left)
    }

    fn parse_value_expression(&mut self) -> Result<Expression> {
        self.parse_additive_expression()
    }

    fn parse_in_list_expression(&mut self, expr: Expression, is_not: bool) -> Result<Expression> {
        self.consume_token(&Token::LeftParen)?;
        if matches!(self.peek_token(), Ok(Token::Select | Token::With)) {
            let subquery = self.parse_query_statement(false, false)?.0;
            self.consume_token(&Token::RightParen)?;
            return Ok(Expression::InSubquery {
                expr: Box::new(expr),
                subquery: Box::new(subquery),
                is_not,
            });
        }

        let mut values = Vec::new();

        loop {
            values.push(self.parse_expression()?);
            if matches!(self.peek_token(), Ok(Token::Comma)) {
                self.consume_token(&Token::Comma)?;
                continue;
            }
            break;
        }

        if values.is_empty() {
            return Err(HematiteError::ParseError(
                "IN list must contain at least one expression".to_string(),
            ));
        }

        self.consume_token(&Token::RightParen)?;
        Ok(Expression::InList {
            expr: Box::new(expr),
            values,
            is_not,
        })
    }

    fn parse_exists_expression(&mut self, is_not: bool) -> Result<Expression> {
        self.consume_token(&Token::LeftParen)?;
        let subquery = self.parse_query_statement(false, false)?.0;
        self.consume_token(&Token::RightParen)?;
        Ok(Expression::Exists {
            subquery: Box::new(subquery),
            is_not,
        })
    }

    fn parse_between_expression(&mut self, expr: Expression, is_not: bool) -> Result<Expression> {
        let lower = self.parse_value_expression()?;
        self.consume_token(&Token::And)?;
        let upper = self.parse_value_expression()?;
        Ok(Expression::Between {
            expr: Box::new(expr),
            lower: Box::new(lower),
            upper: Box::new(upper),
            is_not,
        })
    }

    fn parse_like_expression(&mut self, expr: Expression, is_not: bool) -> Result<Expression> {
        let pattern = self.parse_value_expression()?;
        Ok(Expression::Like {
            expr: Box::new(expr),
            pattern: Box::new(pattern),
            is_not,
        })
    }

    fn parse_additive_expression(&mut self) -> Result<Expression> {
        let mut expr = self.parse_multiplicative_expression()?;

        loop {
            let operator = match self.peek_token() {
                Ok(Token::Plus) => ArithmeticOperator::Add,
                Ok(Token::Minus) => ArithmeticOperator::Subtract,
                _ => break,
            };

            match operator {
                ArithmeticOperator::Add => self.consume_token(&Token::Plus)?,
                ArithmeticOperator::Subtract => self.consume_token(&Token::Minus)?,
                ArithmeticOperator::Multiply
                | ArithmeticOperator::Divide
                | ArithmeticOperator::Modulo => unreachable!(),
            }

            let right = self.parse_multiplicative_expression()?;
            expr = Expression::Binary {
                left: Box::new(expr),
                operator,
                right: Box::new(right),
            };
        }

        Ok(expr)
    }

    fn parse_multiplicative_expression(&mut self) -> Result<Expression> {
        let mut expr = self.parse_unary_expression()?;

        loop {
            let operator = match self.peek_token() {
                Ok(Token::Asterisk) => ArithmeticOperator::Multiply,
                Ok(Token::Slash) => ArithmeticOperator::Divide,
                Ok(Token::Percent) => ArithmeticOperator::Modulo,
                _ => break,
            };

            match operator {
                ArithmeticOperator::Multiply => self.consume_token(&Token::Asterisk)?,
                ArithmeticOperator::Divide => self.consume_token(&Token::Slash)?,
                ArithmeticOperator::Modulo => self.consume_token(&Token::Percent)?,
                ArithmeticOperator::Add | ArithmeticOperator::Subtract => unreachable!(),
            }

            let right = self.parse_unary_expression()?;
            expr = Expression::Binary {
                left: Box::new(expr),
                operator,
                right: Box::new(right),
            };
        }

        Ok(expr)
    }

    fn parse_unary_expression(&mut self) -> Result<Expression> {
        if matches!(self.peek_token(), Ok(Token::Minus)) {
            self.consume_token(&Token::Minus)?;
            return Ok(Expression::UnaryMinus(Box::new(
                self.parse_unary_expression()?,
            )));
        }

        self.parse_primary_expression()
    }

    fn parse_primary_expression(&mut self) -> Result<Expression> {
        let token = self.peek_token()?;
        match token {
            Token::Cast => self.parse_cast_expression(),
            Token::Case => self.parse_case_expression(),
            Token::Interval => self.parse_interval_literal(),
            Token::Date | Token::Time | Token::DateTime | Token::Timestamp
                if self.next_token_is(&Token::LeftParen) =>
            {
                self.parse_scalar_function_expression()
            }
            Token::Left | Token::Right if self.next_token_is(&Token::LeftParen) => {
                self.parse_scalar_function_expression()
            }
            Token::Count | Token::Sum | Token::Avg | Token::Min | Token::Max => {
                self.parse_aggregate_expression()
            }
            Token::Identifier(_) if self.next_token_is(&Token::LeftParen) => {
                self.parse_scalar_function_expression()
            }
            Token::Identifier(_) => Ok(Expression::Column(self.parse_identifier_reference()?)),
            Token::StringLiteral(value) => {
                self.consume_token(&Token::StringLiteral(value.clone()))?;
                Ok(Expression::Literal(LiteralValue::Text(value)))
            }
            Token::NumberLiteral(value) => {
                self.consume_token(&Token::NumberLiteral(value.clone()))?;
                if value.fract() == 0.0 {
                    Ok(Expression::Literal(LiteralValue::Integer(value as i32)))
                } else {
                    Ok(Expression::Literal(LiteralValue::Float(value)))
                }
            }
            Token::BooleanLiteral(value) => {
                self.consume_token(&Token::BooleanLiteral(value.clone()))?;
                Ok(Expression::Literal(LiteralValue::Boolean(value)))
            }
            Token::NullLiteral | Token::Null => {
                // `NULL` appears both as a constraint keyword and as an expression literal.
                if token == Token::NullLiteral {
                    self.consume_token(&Token::NullLiteral)?;
                } else {
                    self.consume_token(&Token::Null)?;
                }
                Ok(Expression::Literal(LiteralValue::Null))
            }
            Token::Placeholder => {
                self.consume_token(&Token::Placeholder)?;
                let index = self.parameter_count;
                self.parameter_count += 1;
                Ok(Expression::Parameter(index))
            }
            Token::LeftParen => {
                self.consume_token(&Token::LeftParen)?;
                if matches!(self.peek_token(), Ok(Token::Select | Token::With)) {
                    let subquery = self.parse_query_statement(false, false)?.0;
                    self.consume_token(&Token::RightParen)?;
                    return Ok(Expression::ScalarSubquery(Box::new(subquery)));
                }
                let expr = self.parse_expression()?;
                self.consume_token(&Token::RightParen)?;
                Ok(expr)
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected expression, found: {:?}",
                token
            ))),
        }
    }

    fn parse_interval_literal(&mut self) -> Result<Expression> {
        self.consume_token(&Token::Interval)?;
        let value = match self.peek_token()? {
            Token::StringLiteral(value) => {
                self.consume_token(&Token::StringLiteral(value.clone()))?;
                value
            }
            token => {
                return Err(HematiteError::ParseError(format!(
                    "Expected INTERVAL string literal, found: {:?}",
                    token
                )))
            }
        };
        let leading = self.parse_identifier()?.to_ascii_uppercase();
        self.consume_token(&Token::To)?;
        let trailing = self.parse_identifier()?.to_ascii_uppercase();

        let qualifier = match (leading.as_str(), trailing.as_str()) {
            ("YEAR", "MONTH") => IntervalQualifier::YearToMonth,
            ("DAY", "SECOND") => IntervalQualifier::DayToSecond,
            _ => {
                return Err(HematiteError::ParseError(format!(
                    "Unsupported INTERVAL qualifier '{} TO {}'",
                    leading, trailing
                )))
            }
        };

        Ok(Expression::IntervalLiteral { value, qualifier })
    }

    fn parse_case_expression(&mut self) -> Result<Expression> {
        self.consume_token(&Token::Case)?;
        let mut branches = Vec::new();

        while matches!(self.peek_token(), Ok(Token::When)) {
            self.consume_token(&Token::When)?;
            let condition = self.parse_expression()?;
            self.consume_token(&Token::Then)?;
            let result = self.parse_expression()?;
            branches.push(CaseWhenClause { condition, result });
        }

        if branches.is_empty() {
            return Err(HematiteError::ParseError(
                "CASE expression requires at least one WHEN ... THEN branch".to_string(),
            ));
        }

        let else_expr = if matches!(self.peek_token(), Ok(Token::Else)) {
            self.consume_token(&Token::Else)?;
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        self.consume_token(&Token::End)?;
        Ok(Expression::Case {
            branches,
            else_expr,
        })
    }

    fn parse_cast_expression(&mut self) -> Result<Expression> {
        self.consume_token(&Token::Cast)?;
        self.consume_token(&Token::LeftParen)?;
        let expr = self.parse_expression()?;
        self.consume_token(&Token::As)?;
        let target_type = self.parse_data_type()?;
        self.consume_token(&Token::RightParen)?;
        Ok(Expression::Cast {
            expr: Box::new(expr),
            target_type,
        })
    }

    fn parse_scalar_function_expression(&mut self) -> Result<Expression> {
        let function_name = self.parse_scalar_function_name()?;
        let function = ScalarFunction::from_identifier(&function_name).ok_or_else(|| {
            HematiteError::ParseError(format!("Unsupported scalar function '{}'", function_name))
        })?;
        self.consume_token(&Token::LeftParen)?;

        let mut args = Vec::new();
        if !matches!(self.peek_token(), Ok(Token::RightParen)) {
            loop {
                args.push(self.parse_expression()?);
                if matches!(self.peek_token(), Ok(Token::Comma)) {
                    self.consume_token(&Token::Comma)?;
                    continue;
                }
                break;
            }
        }

        self.consume_token(&Token::RightParen)?;
        Ok(Expression::ScalarFunctionCall { function, args })
    }

    fn parse_scalar_function_name(&mut self) -> Result<String> {
        match self.peek_token()? {
            Token::Identifier(_) => self.parse_identifier(),
            Token::Date => {
                self.consume_token(&Token::Date)?;
                Ok("DATE".to_string())
            }
            Token::Time => {
                self.consume_token(&Token::Time)?;
                Ok("TIME".to_string())
            }
            Token::Timestamp => {
                self.consume_token(&Token::Timestamp)?;
                Ok("TIMESTAMP".to_string())
            }
            Token::DateTime => {
                self.consume_token(&Token::DateTime)?;
                Ok("DATETIME".to_string())
            }
            Token::Left => {
                self.consume_token(&Token::Left)?;
                Ok("LEFT".to_string())
            }
            Token::Right => {
                self.consume_token(&Token::Right)?;
                Ok("RIGHT".to_string())
            }
            token => Err(HematiteError::ParseError(format!(
                "Expected scalar function name, found: {:?}",
                token
            ))),
        }
    }

    fn parse_comparison_operator(&mut self) -> Result<ComparisonOperator> {
        let token = self.peek_token()?;
        let operator = match token {
            Token::Equal => ComparisonOperator::Equal,
            Token::NotEqual => ComparisonOperator::NotEqual,
            Token::LessThan => ComparisonOperator::LessThan,
            Token::LessThanOrEqual => ComparisonOperator::LessThanOrEqual,
            Token::GreaterThan => ComparisonOperator::GreaterThan,
            Token::GreaterThanOrEqual => ComparisonOperator::GreaterThanOrEqual,
            _ => {
                return Err(HematiteError::ParseError(format!(
                    "Expected comparison operator, found: {:?}",
                    token
                )))
            }
        };

        self.consume_token(&token)?;
        Ok(operator)
    }

    fn parse_insert(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Insert)?;
        self.consume_token(&Token::Into)?;

        let table = self.parse_identifier()?;

        let (columns, source) = match self.peek_token()? {
            Token::LeftParen => {
                self.consume_token(&Token::LeftParen)?;
                let columns = self.parse_column_list()?;
                self.consume_token(&Token::RightParen)?;
                let source = match self.peek_token()? {
                    Token::Values => {
                        self.consume_token(&Token::Values)?;
                        InsertSource::Values(self.parse_value_lists()?)
                    }
                    Token::Select | Token::With => {
                        InsertSource::Select(Box::new(self.parse_query_statement(false, false)?.0))
                    }
                    token => {
                        return Err(HematiteError::ParseError(format!(
                            "Expected VALUES or SELECT after INSERT column list, found: {:?}",
                            token
                        )))
                    }
                };
                (columns, source)
            }
            Token::Set => {
                self.consume_token(&Token::Set)?;
                let assignments = self.parse_update_assignments()?;
                (
                    assignments
                        .iter()
                        .map(|assignment| assignment.column.clone())
                        .collect(),
                    InsertSource::Values(vec![assignments
                        .into_iter()
                        .map(|assignment| assignment.value)
                        .collect()]),
                )
            }
            token => {
                return Err(HematiteError::ParseError(format!(
                    "Expected column list or SET after INSERT INTO table, found: {:?}",
                    token
                )))
            }
        };

        let on_duplicate = if matches!(self.peek_token(), Ok(Token::On)) {
            self.consume_token(&Token::On)?;
            self.consume_token(&Token::Duplicate)?;
            self.consume_token(&Token::Key)?;
            self.consume_token(&Token::Update)?;
            Some(self.parse_update_assignments()?)
        } else {
            None
        };

        self.consume_token(&Token::Semicolon)?;

        Ok(Statement::Insert(InsertStatement {
            table,
            columns,
            source,
            on_duplicate,
        }))
    }

    fn parse_update(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Update)?;
        let table = self.parse_identifier()?;
        let alias = self.parse_optional_alias()?;
        let mut from = TableReference::Table(table.clone(), alias.clone());
        from = self.parse_join_chain(from)?;
        let has_explicit_source =
            !matches!(&from, TableReference::Table(name, None) if name == &table);
        self.consume_token(&Token::Set)?;
        let assignments = self.parse_update_assignments()?;

        let where_clause = if matches!(self.peek_token(), Ok(Token::Where)) {
            Some(self.parse_where_clause()?)
        } else {
            None
        };

        self.consume_token(&Token::Semicolon)?;

        let target_binding =
            has_explicit_source.then(|| alias.clone().unwrap_or_else(|| table.clone()));
        let source = has_explicit_source.then_some(from);

        Ok(Statement::Update(UpdateStatement {
            table,
            target_binding,
            source,
            assignments,
            where_clause,
        }))
    }

    fn parse_delete(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Delete)?;
        let (table, target_binding, source) = match self.peek_token()? {
            Token::From => {
                self.consume_token(&Token::From)?;
                let table = self.parse_identifier()?;
                (table, None, None)
            }
            Token::Identifier(_) => {
                let target_binding = self.parse_identifier()?;
                self.consume_token(&Token::From)?;
                let source = self.parse_from_clause()?;
                let table = self.resolve_delete_target_table(&target_binding, &source)?;
                (table, Some(target_binding), Some(source))
            }
            token => {
                return Err(HematiteError::ParseError(format!(
                    "Expected FROM or target table alias after DELETE, found: {:?}",
                    token
                )))
            }
        };

        let where_clause = if matches!(self.peek_token(), Ok(Token::Where)) {
            Some(self.parse_where_clause()?)
        } else {
            None
        };

        self.consume_token(&Token::Semicolon)?;

        Ok(Statement::Delete(DeleteStatement {
            table,
            target_binding,
            source,
            where_clause,
        }))
    }

    fn resolve_delete_target_table(
        &self,
        target_binding: &str,
        source: &TableReference,
    ) -> Result<String> {
        let mut matches = Vec::new();
        for binding in SelectStatement::collect_table_bindings(source) {
            let binding_name = binding.alias.as_deref().unwrap_or(&binding.table_name);
            if binding_name.eq_ignore_ascii_case(target_binding)
                || binding.table_name.eq_ignore_ascii_case(target_binding)
            {
                matches.push(binding.table_name);
            }
        }

        match matches.len() {
            1 => Ok(matches.remove(0)),
            0 => Err(HematiteError::ParseError(format!(
                "DELETE target '{}' does not match any table in the FROM clause",
                target_binding
            ))),
            _ => Err(HematiteError::ParseError(format!(
                "DELETE target '{}' is ambiguous in the FROM clause",
                target_binding
            ))),
        }
    }

    fn parse_create(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Create)?;
        let unique = if matches!(self.peek_token(), Ok(Token::Unique)) {
            self.consume_token(&Token::Unique)?;
            true
        } else {
            false
        };
        match self.peek_token()? {
            Token::Table => {
                if unique {
                    return Err(HematiteError::ParseError(
                        "CREATE UNIQUE TABLE is not supported".to_string(),
                    ));
                }
                self.consume_token(&Token::Table)?;
                let if_not_exists = self.parse_if_not_exists_clause()?;

                let table = self.parse_identifier()?;

                self.consume_token(&Token::LeftParen)?;

                let (columns, constraints) = self.parse_table_definition_items()?;

                self.consume_token(&Token::RightParen)?;
                self.consume_ignored_create_table_options()?;
                self.consume_token(&Token::Semicolon)?;

                Ok(Statement::Create(CreateStatement {
                    table,
                    columns,
                    constraints,
                    if_not_exists,
                }))
            }
            Token::View => {
                if unique {
                    return Err(HematiteError::ParseError(
                        "CREATE UNIQUE VIEW is not supported".to_string(),
                    ));
                }
                self.consume_token(&Token::View)?;
                let if_not_exists = self.parse_if_not_exists_clause()?;
                let view = self.parse_identifier()?;
                self.consume_token(&Token::As)?;
                let query = self.parse_query_statement(true, false)?.0;
                Ok(Statement::CreateView(CreateViewStatement {
                    view,
                    if_not_exists,
                    query,
                }))
            }
            Token::Trigger => {
                if unique {
                    return Err(HematiteError::ParseError(
                        "CREATE UNIQUE TRIGGER is not supported".to_string(),
                    ));
                }
                self.consume_token(&Token::Trigger)?;
                let trigger = self.parse_identifier()?;
                self.consume_token(&Token::After)?;
                let event = match self.peek_token()? {
                    Token::Insert => {
                        self.consume_token(&Token::Insert)?;
                        TriggerEvent::Insert
                    }
                    Token::Update => {
                        self.consume_token(&Token::Update)?;
                        TriggerEvent::Update
                    }
                    Token::Delete => {
                        self.consume_token(&Token::Delete)?;
                        TriggerEvent::Delete
                    }
                    token => {
                        return Err(HematiteError::ParseError(format!(
                            "Expected INSERT, UPDATE, or DELETE after AFTER, found: {:?}",
                            token
                        )))
                    }
                };
                self.consume_token(&Token::On)?;
                let table = self.parse_identifier()?;
                self.consume_token(&Token::As)?;
                let body = Box::new(self.parse()?);
                Ok(Statement::CreateTrigger(CreateTriggerStatement {
                    trigger,
                    table,
                    event,
                    body,
                }))
            }
            Token::Index | Token::Key => {
                if matches!(self.peek_token(), Ok(Token::Index)) {
                    self.consume_token(&Token::Index)?;
                } else {
                    self.consume_token(&Token::Key)?;
                }
                let if_not_exists = self.parse_if_not_exists_clause()?;
                let index_name = self.parse_identifier()?;
                self.consume_optional_index_type_clause()?;
                self.consume_token(&Token::On)?;
                let table = self.parse_identifier()?;
                self.consume_token(&Token::LeftParen)?;
                let columns = self.parse_column_list()?;
                self.consume_token(&Token::RightParen)?;
                self.consume_optional_index_type_clause()?;
                self.consume_token(&Token::Semicolon)?;

                Ok(Statement::CreateIndex(CreateIndexStatement {
                    index_name,
                    table,
                    columns,
                    unique,
                    if_not_exists,
                }))
            }
            token => Err(HematiteError::ParseError(format!(
                "Expected TABLE, VIEW, TRIGGER, INDEX, or KEY after CREATE, found: {:?}",
                token
            ))),
        }
    }

    fn peek_token_starts_comparison(&self) -> bool {
        matches!(
            self.peek_token(),
            Ok(Token::Equal
                | Token::NotEqual
                | Token::LessThan
                | Token::LessThanOrEqual
                | Token::GreaterThan
                | Token::GreaterThanOrEqual)
        )
    }

    fn parse_drop(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Drop)?;
        match self.peek_token()? {
            Token::Table => {
                self.consume_token(&Token::Table)?;
                let if_exists = self.parse_if_exists_clause()?;
                let table = self.parse_identifier()?;
                self.consume_token(&Token::Semicolon)?;
                Ok(Statement::Drop(DropStatement { table, if_exists }))
            }
            Token::View => {
                self.consume_token(&Token::View)?;
                let if_exists = self.parse_if_exists_clause()?;
                let view = self.parse_identifier()?;
                self.consume_token(&Token::Semicolon)?;
                Ok(Statement::DropView(DropViewStatement { view, if_exists }))
            }
            Token::Trigger => {
                self.consume_token(&Token::Trigger)?;
                let if_exists = self.parse_if_exists_clause()?;
                let trigger = self.parse_identifier()?;
                self.consume_token(&Token::Semicolon)?;
                Ok(Statement::DropTrigger(DropTriggerStatement {
                    trigger,
                    if_exists,
                }))
            }
            Token::Index => {
                self.consume_token(&Token::Index)?;
                let if_exists = self.parse_if_exists_clause()?;
                let index_name = self.parse_identifier()?;
                self.consume_token(&Token::On)?;
                let table = self.parse_identifier()?;
                self.consume_token(&Token::Semicolon)?;
                Ok(Statement::DropIndex(DropIndexStatement {
                    index_name,
                    table,
                    if_exists,
                }))
            }
            token => Err(HematiteError::ParseError(format!(
                "Expected TABLE, VIEW, TRIGGER, or INDEX after DROP, found: {:?}",
                token
            ))),
        }
    }

    fn parse_alter(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Alter)?;
        self.consume_token(&Token::Table)?;
        let table = self.parse_identifier()?;

        match self.peek_token()? {
            Token::Rename => {
                self.consume_token(&Token::Rename)?;
                let operation = if matches!(self.peek_token(), Ok(Token::Column)) {
                    self.consume_token(&Token::Column)?;
                    let old_name = self.parse_identifier()?;
                    self.consume_token(&Token::To)?;
                    let new_name = self.parse_identifier()?;
                    AlterOperation::RenameColumn { old_name, new_name }
                } else {
                    self.consume_token(&Token::To)?;
                    let new_name = self.parse_identifier()?;
                    AlterOperation::RenameTo(new_name)
                };
                self.consume_token(&Token::Semicolon)?;
                Ok(Statement::Alter(AlterStatement { table, operation }))
            }
            Token::Add => {
                self.consume_token(&Token::Add)?;
                let operation = match self.peek_token()? {
                    Token::Column => {
                        self.consume_token(&Token::Column)?;
                        AlterOperation::AddColumn(self.parse_column_definition()?)
                    }
                    Token::Constraint | Token::Check | Token::Unique | Token::Foreign => {
                        AlterOperation::AddConstraint(self.parse_table_constraint()?)
                    }
                    Token::Identifier(_) => {
                        AlterOperation::AddColumn(self.parse_column_definition()?)
                    }
                    token => {
                        return Err(HematiteError::ParseError(format!(
                            "Expected COLUMN or constraint after ADD, found: {:?}",
                            token
                        )))
                    }
                };
                self.consume_token(&Token::Semicolon)?;
                Ok(Statement::Alter(AlterStatement { table, operation }))
            }
            Token::Drop => {
                self.consume_token(&Token::Drop)?;
                let operation = match self.peek_token()? {
                    Token::Column => {
                        self.consume_token(&Token::Column)?;
                        AlterOperation::DropColumn(self.parse_identifier()?)
                    }
                    Token::Constraint => {
                        self.consume_token(&Token::Constraint)?;
                        AlterOperation::DropConstraint(self.parse_identifier()?)
                    }
                    Token::Identifier(_) => AlterOperation::DropColumn(self.parse_identifier()?),
                    token => {
                        return Err(HematiteError::ParseError(format!(
                            "Expected COLUMN or CONSTRAINT after DROP, found: {:?}",
                            token
                        )))
                    }
                };
                self.consume_token(&Token::Semicolon)?;
                Ok(Statement::Alter(AlterStatement { table, operation }))
            }
            Token::Alter => {
                self.consume_token(&Token::Alter)?;
                if matches!(self.peek_token(), Ok(Token::Column)) {
                    self.consume_token(&Token::Column)?;
                }
                let column_name = self.parse_identifier()?;
                let operation = match self.peek_token()? {
                    Token::Set => {
                        self.consume_token(&Token::Set)?;
                        match self.peek_token()? {
                            Token::Default => {
                                self.consume_token(&Token::Default)?;
                                let default_value = self.parse_default_value()?;
                                AlterOperation::AlterColumnSetDefault {
                                    column_name,
                                    default_value,
                                }
                            }
                            Token::Not => {
                                self.consume_token(&Token::Not)?;
                                self.consume_token(&Token::Null)?;
                                AlterOperation::AlterColumnSetNotNull { column_name }
                            }
                            token => {
                                return Err(HematiteError::ParseError(format!(
                                    "Expected DEFAULT or NOT NULL after SET, found: {:?}",
                                    token
                                )))
                            }
                        }
                    }
                    Token::Drop => {
                        self.consume_token(&Token::Drop)?;
                        match self.peek_token()? {
                            Token::Default => {
                                self.consume_token(&Token::Default)?;
                                AlterOperation::AlterColumnDropDefault { column_name }
                            }
                            Token::Not => {
                                self.consume_token(&Token::Not)?;
                                self.consume_token(&Token::Null)?;
                                AlterOperation::AlterColumnDropNotNull { column_name }
                            }
                            token => {
                                return Err(HematiteError::ParseError(format!(
                                    "Expected DEFAULT or NOT NULL after DROP, found: {:?}",
                                    token
                                )))
                            }
                        }
                    }
                    token => {
                        return Err(HematiteError::ParseError(format!(
                            "Expected SET or DROP after ALTER COLUMN, found: {:?}",
                            token
                        )))
                    }
                };
                self.consume_token(&Token::Semicolon)?;
                Ok(Statement::Alter(AlterStatement { table, operation }))
            }
            token => Err(HematiteError::ParseError(format!(
                "Expected supported ALTER TABLE operation, found: {:?}",
                token
            ))),
        }
    }

    fn parse_identifier(&mut self) -> Result<String> {
        let token = self.peek_token()?;
        match token {
            Token::Identifier(name) => {
                self.consume_token(&Token::Identifier(name.clone()))?;
                Ok(name)
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected identifier, found: {:?}",
                token
            ))),
        }
    }

    fn parse_identifier_reference(&mut self) -> Result<String> {
        let first = self.parse_identifier()?;
        if matches!(self.peek_token(), Ok(Token::Dot)) {
            self.consume_token(&Token::Dot)?;
            let second = self.parse_identifier()?;
            Ok(format!("{}.{}", first, second))
        } else {
            Ok(first)
        }
    }

    fn parse_optional_alias(&mut self) -> Result<Option<String>> {
        match self.peek_token() {
            Ok(Token::As) => {
                self.consume_token(&Token::As)?;
                Ok(Some(self.parse_identifier()?))
            }
            Ok(Token::Identifier(_)) => Ok(Some(self.parse_identifier()?)),
            _ => Ok(None),
        }
    }

    fn parse_if_not_exists_clause(&mut self) -> Result<bool> {
        if matches!(self.peek_token(), Ok(Token::If)) {
            self.consume_token(&Token::If)?;
            self.consume_token(&Token::Not)?;
            self.consume_token(&Token::Exists)?;
            return Ok(true);
        }
        Ok(false)
    }

    fn parse_if_exists_clause(&mut self) -> Result<bool> {
        if matches!(self.peek_token(), Ok(Token::If)) {
            self.consume_token(&Token::If)?;
            self.consume_token(&Token::Exists)?;
            return Ok(true);
        }
        Ok(false)
    }

    fn parse_required_alias(&mut self, subject: &str) -> Result<String> {
        self.parse_optional_alias()?
            .ok_or_else(|| HematiteError::ParseError(format!("{} must have an alias", subject)))
    }

    fn peek_identifier_keyword(&self, keyword: &str) -> bool {
        matches!(
            self.peek_token(),
            Ok(Token::Identifier(name)) if name.eq_ignore_ascii_case(keyword)
        )
    }

    fn consume_identifier_keyword(&mut self, keyword: &str) -> Result<()> {
        match self.peek_token()? {
            Token::Identifier(name) if name.eq_ignore_ascii_case(keyword) => {
                self.consume_token(&Token::Identifier(name.clone()))
            }
            token => Err(HematiteError::ParseError(format!(
                "Expected {}, found: {:?}",
                keyword, token
            ))),
        }
    }

    fn consume_optional_equals(&mut self) -> Result<()> {
        if matches!(self.peek_token(), Ok(Token::Equal)) {
            self.consume_token(&Token::Equal)?;
        }
        Ok(())
    }

    fn consume_optional_index_type_clause(&mut self) -> Result<()> {
        if !self.peek_identifier_keyword("USING") {
            return Ok(());
        }
        self.consume_identifier_keyword("USING")?;
        match self.peek_token()? {
            Token::Identifier(name)
                if name.eq_ignore_ascii_case("BTREE") || name.eq_ignore_ascii_case("HASH") =>
            {
                self.consume_token(&Token::Identifier(name.clone()))?;
                Ok(())
            }
            token => Err(HematiteError::ParseError(format!(
                "Expected BTREE or HASH after USING, found: {:?}",
                token
            ))),
        }
    }

    fn consume_ignored_create_table_options(&mut self) -> Result<()> {
        loop {
            if self.peek_identifier_keyword("ENGINE") {
                self.consume_identifier_keyword("ENGINE")?;
                self.consume_optional_equals()?;
                self.parse_identifier()?;
                continue;
            }

            if matches!(self.peek_token(), Ok(Token::AutoIncrement)) {
                self.consume_token(&Token::AutoIncrement)?;
                self.consume_optional_equals()?;
                self.consume_positive_integer_literal("AUTO_INCREMENT")?;
                continue;
            }

            if matches!(self.peek_token(), Ok(Token::Default)) {
                self.consume_token(&Token::Default)?;
                if self.peek_identifier_keyword("CHARSET") {
                    self.consume_identifier_keyword("CHARSET")?;
                    self.consume_optional_equals()?;
                    self.parse_identifier()?;
                    continue;
                }
                if self.peek_identifier_keyword("CHARACTER") {
                    self.consume_identifier_keyword("CHARACTER")?;
                    self.consume_identifier_keyword("SET")?;
                    self.consume_optional_equals()?;
                    self.parse_identifier()?;
                    continue;
                }
                return Err(HematiteError::ParseError(
                    "Unsupported DEFAULT table option".to_string(),
                ));
            }

            if self.peek_identifier_keyword("CHARACTER") {
                self.consume_identifier_keyword("CHARACTER")?;
                self.consume_identifier_keyword("SET")?;
                self.consume_optional_equals()?;
                self.parse_identifier()?;
                continue;
            }

            if self.peek_identifier_keyword("CHARSET") {
                self.consume_identifier_keyword("CHARSET")?;
                self.consume_optional_equals()?;
                self.parse_identifier()?;
                continue;
            }

            if self.peek_identifier_keyword("COLLATE") {
                self.consume_identifier_keyword("COLLATE")?;
                self.consume_optional_equals()?;
                self.parse_identifier()?;
                continue;
            }

            break;
        }

        Ok(())
    }

    fn parse_column_list(&mut self) -> Result<Vec<String>> {
        let mut columns = Vec::new();

        loop {
            let token = self.peek_token()?;
            match token {
                Token::Identifier(name) => {
                    self.consume_token(&Token::Identifier(name.clone()))?;
                    columns.push(name);
                }
                _ => {
                    return Err(HematiteError::ParseError(format!(
                        "Expected column name, found: {:?}",
                        token
                    )))
                }
            }

            if self.peek_token()? == Token::Comma {
                self.consume_token(&Token::Comma)?;
                continue;
            } else {
                break;
            }
        }

        Ok(columns)
    }

    fn parse_update_assignments(&mut self) -> Result<Vec<UpdateAssignment>> {
        let mut assignments = Vec::new();

        loop {
            let column = self.parse_identifier()?;
            self.consume_token(&Token::Equal)?;
            let value = self.parse_expression()?;
            assignments.push(UpdateAssignment { column, value });

            if matches!(self.peek_token(), Ok(Token::Comma)) {
                self.consume_token(&Token::Comma)?;
                continue;
            }

            break;
        }

        Ok(assignments)
    }

    fn parse_value_lists(&mut self) -> Result<Vec<Vec<Expression>>> {
        let mut value_lists = Vec::new();

        loop {
            self.consume_token(&Token::LeftParen)?;
            let mut values = Vec::new();

            loop {
                values.push(self.parse_expression()?);

                if self.peek_token()? == Token::Comma {
                    self.consume_token(&Token::Comma)?;
                    continue;
                } else {
                    break;
                }
            }

            self.consume_token(&Token::RightParen)?;
            value_lists.push(values);

            if self.peek_token()? == Token::Comma {
                self.consume_token(&Token::Comma)?;
                continue;
            } else {
                break;
            }
        }

        Ok(value_lists)
    }

    fn parse_table_definition_items(
        &mut self,
    ) -> Result<(Vec<ColumnDefinition>, Vec<TableConstraint>)> {
        let mut columns = Vec::new();
        let mut constraints = Vec::new();

        loop {
            match self.peek_token()? {
                Token::Constraint | Token::Check | Token::Foreign => {
                    constraints.push(self.parse_table_constraint()?);
                }
                Token::Identifier(_) => columns.push(self.parse_column_definition()?),
                token => {
                    return Err(HematiteError::ParseError(format!(
                        "Expected column definition or table constraint, found: {:?}",
                        token
                    )))
                }
            }

            if self.peek_token()? == Token::Comma {
                self.consume_token(&Token::Comma)?;
                continue;
            } else {
                break;
            }
        }

        Ok((columns, constraints))
    }

    fn parse_column_definition(&mut self) -> Result<ColumnDefinition> {
        let name = self.parse_identifier()?;

        let data_type = self.parse_data_type()?;

        let mut nullable = true;
        let mut primary_key = false;
        let mut auto_increment = false;
        let mut unique = false;
        let mut default_value = None;
        let mut check_constraint = None;
        let mut references = None;

        while let Ok(token) = self.peek_token() {
            match token {
                Token::Not => {
                    self.consume_token(&Token::Not)?;
                    if self.peek_token()? == Token::Null {
                        self.consume_token(&Token::Null)?;
                        nullable = false;
                    }
                }
                Token::Primary => {
                    self.consume_token(&Token::Primary)?;
                    self.consume_token(&Token::Key)?;
                    primary_key = true;
                    nullable = false;
                }
                Token::Unique => {
                    self.consume_token(&Token::Unique)?;
                    unique = true;
                }
                Token::AutoIncrement => {
                    self.consume_token(&Token::AutoIncrement)?;
                    auto_increment = true;
                }
                Token::Default => {
                    self.consume_token(&Token::Default)?;
                    default_value = Some(self.parse_default_value()?);
                }
                Token::Constraint | Token::Check => {
                    let constraint_name = self.parse_optional_constraint_name()?;
                    check_constraint =
                        Some(self.parse_check_constraint_definition(constraint_name)?);
                }
                Token::References => {
                    references = Some(self.parse_column_foreign_key(None, &name)?);
                }
                _ => break,
            }
        }

        Ok(ColumnDefinition {
            name,
            data_type,
            nullable,
            primary_key,
            auto_increment,
            unique,
            default_value,
            check_constraint,
            references,
        })
    }

    fn parse_table_constraint(&mut self) -> Result<TableConstraint> {
        let constraint_name = self.parse_optional_constraint_name()?;
        match self.peek_token()? {
            Token::Check => Ok(TableConstraint::Check(
                self.parse_check_constraint_definition(constraint_name)?,
            )),
            Token::Unique => Ok(TableConstraint::Unique(
                self.parse_unique_constraint_definition(constraint_name)?,
            )),
            Token::Foreign => Ok(TableConstraint::ForeignKey(
                self.parse_table_foreign_key(constraint_name)?,
            )),
            token => Err(HematiteError::ParseError(format!(
                "Expected CHECK, UNIQUE, or FOREIGN KEY constraint, found: {:?}",
                token
            ))),
        }
    }

    fn parse_optional_constraint_name(&mut self) -> Result<Option<String>> {
        if matches!(self.peek_token(), Ok(Token::Constraint)) {
            self.consume_token(&Token::Constraint)?;
            return Ok(Some(self.parse_identifier()?));
        }
        Ok(None)
    }

    fn parse_parenthesized_condition(&mut self) -> Result<Condition> {
        self.consume_token(&Token::LeftParen)?;
        let condition = self.parse_or_condition()?;
        self.consume_token(&Token::RightParen)?;
        Ok(condition)
    }

    fn parse_check_constraint_definition(
        &mut self,
        name: Option<String>,
    ) -> Result<CheckConstraintDefinition> {
        self.consume_token(&Token::Check)?;
        let condition = self.parse_parenthesized_condition()?;
        Ok(CheckConstraintDefinition {
            name,
            expression_sql: condition.to_sql(),
        })
    }

    fn parse_unique_constraint_definition(
        &mut self,
        name: Option<String>,
    ) -> Result<crate::parser::ast::UniqueConstraintDefinition> {
        self.consume_token(&Token::Unique)?;
        let columns = self.parse_column_reference_list()?;
        Ok(crate::parser::ast::UniqueConstraintDefinition { name, columns })
    }

    fn parse_table_foreign_key(&mut self, name: Option<String>) -> Result<ForeignKeyDefinition> {
        self.consume_token(&Token::Foreign)?;
        self.consume_token(&Token::Key)?;
        let columns = self.parse_column_reference_list()?;
        self.parse_foreign_key_reference(name, columns)
    }

    fn parse_column_foreign_key(
        &mut self,
        name: Option<String>,
        column: &str,
    ) -> Result<ForeignKeyDefinition> {
        self.parse_foreign_key_reference(name, vec![column.to_string()])
    }

    fn parse_column_reference_list(&mut self) -> Result<Vec<String>> {
        self.consume_token(&Token::LeftParen)?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_identifier()?);
            if matches!(self.peek_token(), Ok(Token::Comma)) {
                self.consume_token(&Token::Comma)?;
                continue;
            }
            break;
        }
        self.consume_token(&Token::RightParen)?;
        Ok(columns)
    }

    fn parse_foreign_key_reference(
        &mut self,
        name: Option<String>,
        columns: Vec<String>,
    ) -> Result<ForeignKeyDefinition> {
        self.consume_token(&Token::References)?;
        let referenced_table = self.parse_identifier()?;
        let referenced_columns = self.parse_column_reference_list()?;
        let mut on_delete = crate::parser::ast::ForeignKeyAction::Restrict;
        let mut on_update = crate::parser::ast::ForeignKeyAction::Restrict;
        while matches!(self.peek_token(), Ok(Token::On)) {
            self.consume_token(&Token::On)?;
            let target_is_delete = match self.peek_token()? {
                Token::Delete => {
                    self.consume_token(&Token::Delete)?;
                    true
                }
                Token::Update => {
                    self.consume_token(&Token::Update)?;
                    false
                }
                token => {
                    return Err(HematiteError::ParseError(format!(
                        "Expected DELETE or UPDATE after ON, found: {:?}",
                        token
                    )))
                }
            };
            let action = self.parse_foreign_key_action()?;
            if target_is_delete {
                on_delete = action;
            } else {
                on_update = action;
            }
        }
        Ok(ForeignKeyDefinition {
            name,
            columns,
            referenced_table,
            referenced_columns,
            on_delete,
            on_update,
        })
    }

    fn parse_foreign_key_action(&mut self) -> Result<crate::parser::ast::ForeignKeyAction> {
        match self.peek_token()? {
            Token::Restrict => {
                self.consume_token(&Token::Restrict)?;
                Ok(crate::parser::ast::ForeignKeyAction::Restrict)
            }
            Token::Cascade => {
                self.consume_token(&Token::Cascade)?;
                Ok(crate::parser::ast::ForeignKeyAction::Cascade)
            }
            Token::Set => {
                self.consume_token(&Token::Set)?;
                self.consume_token(&Token::Null)?;
                Ok(crate::parser::ast::ForeignKeyAction::SetNull)
            }
            token => Err(HematiteError::ParseError(format!(
                "Expected foreign key action, found: {:?}",
                token
            ))),
        }
    }

    fn parse_data_type(&mut self) -> Result<SqlTypeName> {
        let token = self.peek_token()?;
        let data_type = match token {
            Token::TinyInt => SqlTypeName::TinyInt,
            Token::SmallInt => SqlTypeName::SmallInt,
            Token::Integer | Token::Int => SqlTypeName::Integer,
            Token::BigInt => SqlTypeName::BigInt,
            Token::Text => SqlTypeName::Text,
            Token::Boolean | Token::Bool => SqlTypeName::Boolean,
            Token::Float => SqlTypeName::Float,
            Token::Real => SqlTypeName::Real,
            Token::Double => SqlTypeName::Double,
            Token::Decimal => {
                self.consume_token(&token)?;
                self.consume_optional_double_precision()?;
                self.consume_optional_unsigned()?;
                let (precision, scale) = self.parse_optional_numeric_precision()?;
                return Ok(SqlTypeName::Decimal { precision, scale });
            }
            Token::Numeric => {
                self.consume_token(&token)?;
                self.consume_optional_double_precision()?;
                self.consume_optional_unsigned()?;
                let (precision, scale) = self.parse_optional_numeric_precision()?;
                return Ok(SqlTypeName::Numeric { precision, scale });
            }
            Token::Blob => SqlTypeName::Blob,
            Token::Date => SqlTypeName::Date,
            Token::Time => {
                self.consume_token(&token)?;
                if matches!(self.peek_token(), Ok(Token::With)) {
                    self.consume_token(&Token::With)?;
                    self.consume_token(&Token::Time)?;
                    self.consume_token(&Token::Zone)?;
                    return Ok(SqlTypeName::TimeWithTimeZone);
                }
                return Ok(SqlTypeName::Time);
            }
            Token::DateTime => SqlTypeName::DateTime,
            Token::Timestamp => SqlTypeName::Timestamp,
            Token::Varchar | Token::Char | Token::BinaryType | Token::VarBinary => {
                self.consume_token(&token)?;
                let length = self.parse_type_length()?;
                return Ok(match token {
                    Token::Varchar => SqlTypeName::VarChar(length),
                    Token::Char => SqlTypeName::Char(length),
                    Token::BinaryType => SqlTypeName::Binary(length),
                    Token::VarBinary => SqlTypeName::VarBinary(length),
                    _ => unreachable!(),
                });
            }
            Token::Enum => {
                self.consume_token(&token)?;
                return Ok(SqlTypeName::Enum(self.parse_enum_variants()?));
            }
            _ => {
                return Err(HematiteError::ParseError(format!(
                    "Expected data type, found: {:?}",
                    token
                )))
            }
        };

        self.consume_token(&token)?;
        self.consume_optional_double_precision()?;
        self.consume_optional_unsigned()?;
        Ok(data_type)
    }

    fn parse_enum_variants(&mut self) -> Result<Vec<String>> {
        self.consume_token(&Token::LeftParen)?;
        let mut variants = Vec::new();
        loop {
            match self.peek_token()? {
                Token::StringLiteral(value) => {
                    self.consume_token(&Token::StringLiteral(value.clone()))?;
                    variants.push(value);
                }
                token => {
                    return Err(HematiteError::ParseError(format!(
                        "Expected ENUM string literal, found: {:?}",
                        token
                    )))
                }
            }

            match self.peek_token()? {
                Token::Comma => {
                    self.consume_token(&Token::Comma)?;
                }
                Token::RightParen => {
                    self.consume_token(&Token::RightParen)?;
                    break;
                }
                token => {
                    return Err(HematiteError::ParseError(format!(
                        "Expected ',' or ')' in ENUM type, found: {:?}",
                        token
                    )))
                }
            }
        }

        if variants.is_empty() {
            return Err(HematiteError::ParseError(
                "ENUM type requires at least one variant".to_string(),
            ));
        }

        Ok(variants)
    }

    fn parse_type_length(&mut self) -> Result<u32> {
        self.consume_token(&Token::LeftParen)?;
        let length = match self.peek_token()? {
            Token::NumberLiteral(length) if length.fract() == 0.0 && length > 0.0 => {
                self.consume_token(&Token::NumberLiteral(length))?;
                length as u32
            }
            token => {
                return Err(HematiteError::ParseError(format!(
                    "Expected positive integer length, found: {:?}",
                    token
                )))
            }
        };
        self.consume_token(&Token::RightParen)?;
        Ok(length)
    }

    fn parse_optional_numeric_precision(&mut self) -> Result<(Option<u32>, Option<u32>)> {
        if !matches!(self.peek_token(), Ok(Token::LeftParen)) {
            return Ok((None, None));
        }

        self.consume_token(&Token::LeftParen)?;
        let precision = self.consume_positive_integer_literal("precision")?;
        let mut scale = None;
        if matches!(self.peek_token(), Ok(Token::Comma)) {
            self.consume_token(&Token::Comma)?;
            scale = Some(self.consume_positive_integer_literal("scale")?);
        }
        self.consume_token(&Token::RightParen)?;
        Ok((Some(precision), scale))
    }

    fn consume_positive_integer_literal(&mut self, label: &str) -> Result<u32> {
        match self.peek_token()? {
            Token::NumberLiteral(value) if value.fract() == 0.0 && value >= 0.0 => {
                self.consume_token(&Token::NumberLiteral(value))?;
                Ok(value as u32)
            }
            token => Err(HematiteError::ParseError(format!(
                "Expected non-negative integer {} value, found: {:?}",
                label, token
            ))),
        }
    }

    fn consume_optional_double_precision(&mut self) -> Result<()> {
        if matches!(self.peek_token(), Ok(Token::Precision)) {
            self.consume_token(&Token::Precision)?;
        }
        Ok(())
    }

    fn consume_optional_unsigned(&mut self) -> Result<()> {
        if matches!(self.peek_token(), Ok(Token::Unsigned)) {
            self.consume_token(&Token::Unsigned)?;
        }
        Ok(())
    }

    fn parse_default_value(&mut self) -> Result<LiteralValue> {
        let token = self.peek_token()?;
        match token {
            Token::StringLiteral(value) => {
                self.consume_token(&Token::StringLiteral(value.clone()))?;
                Ok(LiteralValue::Text(value))
            }
            Token::NumberLiteral(value) => {
                self.consume_token(&Token::NumberLiteral(value.clone()))?;
                if value.fract() == 0.0 {
                    Ok(LiteralValue::Integer(value as i32))
                } else {
                    Ok(LiteralValue::Float(value))
                }
            }
            Token::BooleanLiteral(value) => {
                self.consume_token(&Token::BooleanLiteral(value.clone()))?;
                Ok(LiteralValue::Boolean(value))
            }
            Token::NullLiteral | Token::Null => {
                if token == Token::NullLiteral {
                    self.consume_token(&Token::NullLiteral)?;
                } else {
                    self.consume_token(&Token::Null)?;
                }
                Ok(LiteralValue::Null)
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected DEFAULT literal (NULL, number, string, boolean), found: {:?}",
                token
            ))),
        }
    }

    fn peek_token(&self) -> Result<Token> {
        if self.position < self.tokens.len() {
            Ok(self.tokens[self.position].clone())
        } else {
            Err(HematiteError::ParseError(
                "Unexpected end of input".to_string(),
            ))
        }
    }

    fn next_token_is(&self, expected: &Token) -> bool {
        self.tokens
            .get(self.position + 1)
            .is_some_and(|token| token == expected)
    }

    fn consume_token(&mut self, expected: &Token) -> Result<()> {
        let token = self.peek_token()?;
        if token == *expected {
            self.position += 1;
            Ok(())
        } else {
            Err(HematiteError::ParseError(format!(
                "Expected {:?}, found: {:?}",
                expected, token
            )))
        }
    }
}

pub fn parse_condition_fragment(sql: &str) -> Result<Condition> {
    let mut lexer = Lexer::new(sql.to_string());
    lexer.tokenize()?;
    let mut parser = Parser::new(lexer.get_tokens().to_vec());
    let condition = parser.parse_or_condition()?;
    if parser.position != parser.tokens.len() {
        return Err(HematiteError::ParseError(
            "Unexpected trailing tokens in CHECK constraint".to_string(),
        ));
    }
    Ok(condition)
}

fn is_window_only_function_name(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "ROW_NUMBER" | "RANK" | "DENSE_RANK"
    )
}
