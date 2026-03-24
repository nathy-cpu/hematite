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
use crate::parser::lexer::Token;

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
            Token::Select => self.parse_select(),
            Token::Update => self.parse_update(),
            Token::Insert => self.parse_insert(),
            Token::Delete => self.parse_delete(),
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            _ => Err(HematiteError::ParseError(format!(
                "Expected BEGIN, COMMIT, ROLLBACK, SELECT, UPDATE, INSERT, DELETE, CREATE, or DROP, found: {:?}",
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
        self.consume_token(&Token::Semicolon)?;
        Ok(Statement::Rollback)
    }

    fn parse_select(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Select)?;
        let distinct = if matches!(self.peek_token(), Ok(Token::Distinct)) {
            self.consume_token(&Token::Distinct)?;
            true
        } else {
            false
        };

        let (columns, column_aliases) = self.parse_select_columns()?;

        self.consume_token(&Token::From)?;

        let from = self.parse_table_reference()?;

        let where_clause = if self.peek_token()? == Token::Where {
            Some(self.parse_where_clause()?)
        } else {
            None
        };

        let order_by = if matches!(self.peek_token(), Ok(Token::Order)) {
            self.parse_order_by_clause()?
        } else {
            Vec::new()
        };

        let limit = if matches!(self.peek_token(), Ok(Token::Limit)) {
            Some(self.parse_limit_clause()?)
        } else {
            None
        };

        let offset = if matches!(self.peek_token(), Ok(Token::Offset)) {
            Some(self.parse_offset_clause()?)
        } else {
            None
        };

        self.consume_token(&Token::Semicolon)?;

        Ok(Statement::Select(SelectStatement {
            distinct,
            columns,
            column_aliases,
            from,
            where_clause,
            order_by,
            limit,
            offset,
        }))
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
                let token = self.peek_token()?;
                match token {
                    Token::Count => {
                        self.consume_token(&Token::Count)?;
                        self.consume_token(&Token::LeftParen)?;
                        self.consume_token(&Token::Asterisk)?;
                        self.consume_token(&Token::RightParen)?;
                        columns.push(SelectItem::CountAll);
                        aliases.push(self.parse_optional_alias()?);
                    }
                    Token::Sum | Token::Avg | Token::Min | Token::Max => {
                        columns.push(self.parse_aggregate_select_item()?);
                        aliases.push(self.parse_optional_alias()?);
                    }
                    Token::Identifier(_)
                    | Token::StringLiteral(_)
                    | Token::NumberLiteral(_)
                    | Token::BooleanLiteral(_)
                    | Token::Null
                    | Token::NullLiteral
                    | Token::Placeholder
                    | Token::LeftParen
                    | Token::Minus => {
                        let expr = self.parse_expression()?;
                        columns.push(match expr {
                            Expression::Column(name) => SelectItem::Column(name),
                            expr => SelectItem::Expression(expr),
                        });
                        aliases.push(self.parse_optional_alias()?);
                    }
                    _ => {
                        return Err(HematiteError::ParseError(format!(
                            "Expected select item or aggregate, found: {:?}",
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
        }

        Ok((columns, aliases))
    }

    fn parse_aggregate_select_item(&mut self) -> Result<SelectItem> {
        let function = match self.peek_token()? {
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
        let column = self.parse_identifier_reference()?;
        self.consume_token(&Token::RightParen)?;

        Ok(SelectItem::Aggregate { function, column })
    }

    fn parse_table_reference(&mut self) -> Result<TableReference> {
        match self.peek_token()? {
            Token::Identifier(_) => {
                let table_name = self.parse_identifier()?;
                let alias = self.parse_optional_alias()?;
                Ok(TableReference::Table(table_name, alias))
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected table name, found: {:?}",
                self.peek_token()?
            ))),
        }
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

    fn parse_limit_clause(&mut self) -> Result<usize> {
        self.consume_token(&Token::Limit)?;
        self.parse_non_negative_integer_clause("LIMIT")
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
            return Ok(Condition::Not(Box::new(self.parse_primary_condition()?)));
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
        let left = self.parse_expression()?;

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
            return Err(HematiteError::ParseError(
                "Expected IN or LIKE after NOT in predicate".to_string(),
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

        let right = self.parse_expression()?;

        Ok(Condition::Comparison {
            left,
            operator,
            right,
        })
    }

    fn parse_in_list_condition(&mut self, expr: Expression, is_not: bool) -> Result<Condition> {
        self.consume_token(&Token::LeftParen)?;
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

    fn parse_between_condition(&mut self, expr: Expression, is_not: bool) -> Result<Condition> {
        let lower = self.parse_expression()?;
        self.consume_token(&Token::And)?;
        let upper = self.parse_expression()?;
        Ok(Condition::Between {
            expr,
            lower,
            upper,
            is_not,
        })
    }

    fn parse_like_condition(&mut self, expr: Expression, is_not: bool) -> Result<Condition> {
        let pattern = self.parse_expression()?;
        Ok(Condition::Like {
            expr,
            pattern,
            is_not,
        })
    }

    fn parse_expression(&mut self) -> Result<Expression> {
        self.parse_additive_expression()
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
                ArithmeticOperator::Multiply | ArithmeticOperator::Divide => unreachable!(),
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
                _ => break,
            };

            match operator {
                ArithmeticOperator::Multiply => self.consume_token(&Token::Asterisk)?,
                ArithmeticOperator::Divide => self.consume_token(&Token::Slash)?,
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
            Token::Identifier(_) => Ok(Expression::Column(self.parse_identifier_reference()?)),
            Token::StringLiteral(value) => {
                self.consume_token(&Token::StringLiteral(value.clone()))?;
                Ok(Expression::Literal(crate::catalog::types::Value::Text(
                    value,
                )))
            }
            Token::NumberLiteral(value) => {
                self.consume_token(&Token::NumberLiteral(value.clone()))?;
                if value.fract() == 0.0 {
                    Ok(Expression::Literal(crate::catalog::types::Value::Integer(
                        value as i32,
                    )))
                } else {
                    Ok(Expression::Literal(crate::catalog::types::Value::Float(
                        value,
                    )))
                }
            }
            Token::BooleanLiteral(value) => {
                self.consume_token(&Token::BooleanLiteral(value.clone()))?;
                Ok(Expression::Literal(crate::catalog::types::Value::Boolean(
                    value,
                )))
            }
            Token::NullLiteral | Token::Null => {
                // `NULL` appears both as a constraint keyword and as an expression literal.
                if token == Token::NullLiteral {
                    self.consume_token(&Token::NullLiteral)?;
                } else {
                    self.consume_token(&Token::Null)?;
                }
                Ok(Expression::Literal(crate::catalog::types::Value::Null))
            }
            Token::Placeholder => {
                self.consume_token(&Token::Placeholder)?;
                let index = self.parameter_count;
                self.parameter_count += 1;
                Ok(Expression::Parameter(index))
            }
            Token::LeftParen => {
                self.consume_token(&Token::LeftParen)?;
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

        self.consume_token(&Token::LeftParen)?;

        let columns = self.parse_column_list()?;

        self.consume_token(&Token::RightParen)?;
        self.consume_token(&Token::Values)?;

        let values = self.parse_value_lists()?;

        self.consume_token(&Token::Semicolon)?;

        Ok(Statement::Insert(InsertStatement {
            table,
            columns,
            values,
        }))
    }

    fn parse_update(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Update)?;
        let table = self.parse_identifier()?;
        self.consume_token(&Token::Set)?;
        let assignments = self.parse_update_assignments()?;

        let where_clause = if matches!(self.peek_token(), Ok(Token::Where)) {
            Some(self.parse_where_clause()?)
        } else {
            None
        };

        self.consume_token(&Token::Semicolon)?;

        Ok(Statement::Update(UpdateStatement {
            table,
            assignments,
            where_clause,
        }))
    }

    fn parse_delete(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Delete)?;
        self.consume_token(&Token::From)?;

        let table = self.parse_identifier()?;

        let where_clause = if matches!(self.peek_token(), Ok(Token::Where)) {
            Some(self.parse_where_clause()?)
        } else {
            None
        };

        self.consume_token(&Token::Semicolon)?;

        Ok(Statement::Delete(DeleteStatement {
            table,
            where_clause,
        }))
    }

    fn parse_create(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Create)?;
        match self.peek_token()? {
            Token::Table => {
                self.consume_token(&Token::Table)?;

                let table = self.parse_identifier()?;

                self.consume_token(&Token::LeftParen)?;

                let columns = self.parse_column_definitions()?;

                self.consume_token(&Token::RightParen)?;
                self.consume_token(&Token::Semicolon)?;

                Ok(Statement::Create(CreateStatement { table, columns }))
            }
            Token::Index => {
                self.consume_token(&Token::Index)?;
                let index_name = self.parse_identifier()?;
                self.consume_token(&Token::On)?;
                let table = self.parse_identifier()?;
                self.consume_token(&Token::LeftParen)?;
                let columns = self.parse_column_list()?;
                self.consume_token(&Token::RightParen)?;
                self.consume_token(&Token::Semicolon)?;

                Ok(Statement::CreateIndex(CreateIndexStatement {
                    index_name,
                    table,
                    columns,
                }))
            }
            token => Err(HematiteError::ParseError(format!(
                "Expected TABLE or INDEX after CREATE, found: {:?}",
                token
            ))),
        }
    }

    fn parse_drop(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Drop)?;
        match self.peek_token()? {
            Token::Table => {
                self.consume_token(&Token::Table)?;
                let table = self.parse_identifier()?;
                self.consume_token(&Token::Semicolon)?;
                Ok(Statement::Drop(DropStatement { table }))
            }
            Token::Index => {
                self.consume_token(&Token::Index)?;
                let index_name = self.parse_identifier()?;
                self.consume_token(&Token::On)?;
                let table = self.parse_identifier()?;
                self.consume_token(&Token::Semicolon)?;
                Ok(Statement::DropIndex(DropIndexStatement {
                    index_name,
                    table,
                }))
            }
            token => Err(HematiteError::ParseError(format!(
                "Expected TABLE or INDEX after DROP, found: {:?}",
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

    fn parse_column_definitions(&mut self) -> Result<Vec<ColumnDefinition>> {
        let mut columns = Vec::new();

        loop {
            columns.push(self.parse_column_definition()?);

            if self.peek_token()? == Token::Comma {
                self.consume_token(&Token::Comma)?;
                continue;
            } else {
                break;
            }
        }

        Ok(columns)
    }

    fn parse_column_definition(&mut self) -> Result<ColumnDefinition> {
        let name = self.parse_identifier()?;

        let data_type = self.parse_data_type()?;

        let mut nullable = true;
        let mut primary_key = false;
        let mut default_value = None;

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
                Token::Default => {
                    self.consume_token(&Token::Default)?;
                    default_value = Some(self.parse_default_value()?);
                }
                _ => break,
            }
        }

        Ok(ColumnDefinition {
            name,
            data_type,
            nullable,
            primary_key,
            default_value,
        })
    }

    fn parse_data_type(&mut self) -> Result<crate::catalog::DataType> {
        let token = self.peek_token()?;
        let data_type = match token {
            Token::Integer => crate::catalog::DataType::Integer,
            Token::Text => crate::catalog::DataType::Text,
            Token::Boolean => crate::catalog::DataType::Boolean,
            Token::Float => crate::catalog::DataType::Float,
            _ => {
                return Err(HematiteError::ParseError(format!(
                    "Expected data type, found: {:?}",
                    token
                )))
            }
        };

        self.consume_token(&token)?;
        Ok(data_type)
    }

    fn parse_default_value(&mut self) -> Result<crate::catalog::types::Value> {
        let token = self.peek_token()?;
        match token {
            Token::StringLiteral(value) => {
                self.consume_token(&Token::StringLiteral(value.clone()))?;
                Ok(crate::catalog::types::Value::Text(value))
            }
            Token::NumberLiteral(value) => {
                self.consume_token(&Token::NumberLiteral(value.clone()))?;
                if value.fract() == 0.0 {
                    Ok(crate::catalog::types::Value::Integer(value as i32))
                } else {
                    Ok(crate::catalog::types::Value::Float(value))
                }
            }
            Token::BooleanLiteral(value) => {
                self.consume_token(&Token::BooleanLiteral(value.clone()))?;
                Ok(crate::catalog::types::Value::Boolean(value))
            }
            Token::NullLiteral | Token::Null => {
                if token == Token::NullLiteral {
                    self.consume_token(&Token::NullLiteral)?;
                } else {
                    self.consume_token(&Token::Null)?;
                }
                Ok(crate::catalog::types::Value::Null)
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
