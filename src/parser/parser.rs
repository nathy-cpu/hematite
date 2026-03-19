//! SQL query parser for converting tokens to AST

use crate::error::{HematiteError, Result};
use crate::parser::ast::*;
use crate::parser::lexer::Token;

pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            position: 0,
        }
    }

    pub fn parse(&mut self) -> Result<Statement> {
        if self.tokens.is_empty() {
            return Err(HematiteError::ParseError("Empty input".to_string()));
        }

        let token = self.peek_token()?;

        match token {
            Token::Select => self.parse_select(),
            Token::Update => self.parse_update(),
            Token::Insert => self.parse_insert(),
            Token::Delete => self.parse_delete(),
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            _ => Err(HematiteError::ParseError(format!(
                "Expected SELECT, UPDATE, INSERT, DELETE, CREATE, or DROP, found: {:?}",
                token
            ))),
        }
    }

    fn parse_select(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Select)?;

        let columns = self.parse_select_columns()?;

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

        // Expect semicolon
        self.consume_token(&Token::Semicolon)?;

        Ok(Statement::Select(SelectStatement {
            columns,
            from,
            where_clause,
            order_by,
            limit,
        }))
    }

    fn parse_select_columns(&mut self) -> Result<Vec<SelectItem>> {
        let mut columns = Vec::new();

        let token = self.peek_token()?;

        if token == Token::Asterisk {
            self.consume_token(&Token::Asterisk)?;
            columns.push(SelectItem::Wildcard);
        } else {
            // Parse column list
            loop {
                let token = self.peek_token()?;
                match token {
                    Token::Count => {
                        self.consume_token(&Token::Count)?;
                        self.consume_token(&Token::LeftParen)?;
                        self.consume_token(&Token::Asterisk)?;
                        self.consume_token(&Token::RightParen)?;
                        columns.push(SelectItem::CountAll);
                    }
                    Token::Identifier(name) => {
                        self.consume_token(&Token::Identifier(name.clone()))?;
                        columns.push(SelectItem::Column(name));
                    }
                    _ => {
                        return Err(HematiteError::ParseError(format!(
                            "Expected column name or COUNT(*), found: {:?}",
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

        Ok(columns)
    }

    fn parse_table_reference(&mut self) -> Result<TableReference> {
        let token = self.peek_token()?;
        match token {
            Token::Identifier(name) => {
                self.consume_token(&Token::Identifier(name.clone()))?;
                Ok(TableReference::Table(name))
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected table name, found: {:?}",
                token
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
            let column = self.parse_identifier()?;
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
        match self.peek_token()? {
            Token::NumberLiteral(value) if value.fract() == 0.0 && value >= 0.0 => {
                self.consume_token(&Token::NumberLiteral(value))?;
                Ok(value as usize)
            }
            token => Err(HematiteError::ParseError(format!(
                "Expected non-negative integer after LIMIT, found: {:?}",
                token
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

    fn parse_expression(&mut self) -> Result<Expression> {
        let token = self.peek_token()?;
        match token {
            Token::Identifier(name) => {
                self.consume_token(&Token::Identifier(name.clone()))?;
                Ok(Expression::Column(name))
            }
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
                // `NULL` is used both as a constraint keyword and as a literal in expressions.
                // We accept either token as a NULL literal here.
                if token == Token::NullLiteral {
                    self.consume_token(&Token::NullLiteral)?;
                } else {
                    self.consume_token(&Token::Null)?;
                }
                Ok(Expression::Literal(crate::catalog::types::Value::Null))
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
        self.consume_token(&Token::Table)?;

        let table = self.parse_identifier()?;

        self.consume_token(&Token::LeftParen)?;

        let columns = self.parse_column_definitions()?;

        self.consume_token(&Token::RightParen)?;
        self.consume_token(&Token::Semicolon)?;

        Ok(Statement::Create(CreateStatement { table, columns }))
    }

    fn parse_drop(&mut self) -> Result<Statement> {
        self.consume_token(&Token::Drop)?;
        self.consume_token(&Token::Table)?;
        let table = self.parse_identifier()?;
        self.consume_token(&Token::Semicolon)?;
        Ok(Statement::Drop(DropStatement { table }))
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

        // Parse constraints
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
