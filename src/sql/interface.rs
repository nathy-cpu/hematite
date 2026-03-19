//! High-level SQL interface

use crate::catalog::Value;
use crate::error::{HematiteError, Result};
use crate::parser::lexer::Token;
use crate::parser::{Lexer, Parser};
use crate::sql::connection::{Connection, PreparedStatement, Transaction};
use crate::sql::result::{ResultSet, Row, StatementResult};

/// High-level interface for SQL operations
pub struct Hematite {
    pub connection: Connection,
}

impl Hematite {
    /// Create a new database instance with an in-memory database
    pub fn new_in_memory() -> Result<Self> {
        // This project doesn't yet support a true in-memory backend; use a unique temp file to
        // avoid test contention when running in parallel.
        let connection = Connection::new(&unique_test_db_path("_test_in_memory"))?;
        Ok(Self { connection })
    }

    /// Create a new database instance with a file-based database
    pub fn new(database_path: &str) -> Result<Self> {
        let connection = Connection::new(database_path)?;
        Ok(Self { connection })
    }

    /// Execute a SQL statement and return the result
    pub fn execute(&mut self, sql: &str) -> Result<StatementResult> {
        let query_result = self.connection.execute(sql)?;

        // Convert QueryResult to StatementResult
        if query_result.columns.is_empty() {
            // This was a DML statement (INSERT, UPDATE, DELETE, CREATE, etc.)
            Ok(StatementResult::new(
                query_result.affected_rows,
                "Statement executed successfully".to_string(),
            ))
        } else {
            // This was a SELECT query - return as ResultSet
            Err(HematiteError::ParseError(
                "Use query() method for SELECT statements".to_string(),
            ))
        }
    }

    /// Execute a SQL query and return the result set
    pub fn query(&mut self, sql: &str) -> Result<ResultSet> {
        let query_result = self.connection.execute(sql)?;

        // Convert QueryResult to ResultSet
        Ok(ResultSet::new(query_result.columns, query_result.rows))
    }

    /// Execute a SQL statement and return the first row of the result
    pub fn query_row<F, R>(&mut self, sql: &str, f: F) -> Result<Option<R>>
    where
        F: FnOnce(&Row) -> Result<R>,
    {
        let result_set = self.query(sql)?;

        if let Some(row) = result_set.get_row(0) {
            Ok(Some(f(row)?))
        } else {
            Ok(None)
        }
    }

    /// Execute a SQL statement and return the first column of the first row
    pub fn query_one<T>(&mut self, sql: &str) -> Result<Option<T>>
    where
        T: FromValue,
    {
        self.query_row(sql, |row| {
            if let Some(value) = row.get(0) {
                T::from_value(value)
            } else {
                Err(HematiteError::ParseError("No value found".to_string()))
            }
        })
    }

    /// Prepare a SQL statement for repeated execution
    pub fn prepare(&mut self, sql: &str) -> Result<PreparedStatement> {
        self.connection.prepare(sql)
    }

    /// Execute a prepared statement
    pub fn execute_prepared(&mut self, stmt: &mut PreparedStatement) -> Result<StatementResult> {
        let query_result = stmt.execute(&mut self.connection)?;

        // Convert QueryResult to StatementResult
        if query_result.columns.is_empty() {
            // This was a DML statement (INSERT, UPDATE, DELETE, CREATE, etc.)
            Ok(StatementResult::new(
                query_result.affected_rows,
                "Statement executed successfully".to_string(),
            ))
        } else {
            // This was a SELECT query - return as ResultSet
            Err(HematiteError::ParseError(
                "Use query_prepared() method for SELECT statements".to_string(),
            ))
        }
    }

    /// Begin a new transaction
    pub fn transaction(&'_ mut self) -> Result<Transaction<'_>> {
        self.connection.begin_transaction()
    }

    /// Execute multiple SQL statements in sequence
    pub fn execute_batch(&mut self, sql: &str) -> Result<()> {
        let mut lexer = Lexer::new(sql.to_string());
        lexer.tokenize()?;

        let mut current_tokens = Vec::new();
        for token in lexer.get_tokens().iter().cloned() {
            current_tokens.push(token.clone());

            if token == Token::Semicolon {
                let mut parser = Parser::new(current_tokens);
                let statement = parser.parse()?;
                self.connection.execute_statement(statement)?;
                current_tokens = Vec::new();
            }
        }

        if !current_tokens.is_empty() {
            current_tokens.push(Token::Semicolon);
            let mut parser = Parser::new(current_tokens);
            let statement = parser.parse()?;
            self.connection.execute_statement(statement)?;
        }

        Ok(())
    }
}

fn unique_test_db_path(prefix: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{}_{}.db", prefix, nanos)
}

/// Trait for converting database values to Rust types
pub trait FromValue: Sized {
    fn from_value(value: &Value) -> Result<Self>;
}

impl FromValue for i32 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Integer(i) => Ok(*i),
            _ => Err(HematiteError::ParseError(format!(
                "Expected INTEGER, found {:?}",
                value
            ))),
        }
    }
}

impl FromValue for String {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Text(s) => Ok(s.clone()),
            _ => Err(HematiteError::ParseError(format!(
                "Expected TEXT, found {:?}",
                value
            ))),
        }
    }
}

impl FromValue for bool {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Boolean(b) => Ok(*b),
            _ => Err(HematiteError::ParseError(format!(
                "Expected BOOLEAN, found {:?}",
                value
            ))),
        }
    }
}

impl FromValue for f64 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Float(f) => Ok(*f),
            Value::Integer(i) => Ok(*i as f64), // Allow integer to float conversion
            _ => Err(HematiteError::ParseError(format!(
                "Expected FLOAT, found {:?}",
                value
            ))),
        }
    }
}

impl FromValue for Value {
    fn from_value(value: &Value) -> Result<Self> {
        Ok(value.clone())
    }
}

/// Builder pattern for creating database connections
pub struct HematiteBuilder {
    database_path: String,
}

impl HematiteBuilder {
    pub fn new() -> Self {
        Self {
            database_path: "_test.db".to_string(),
        }
    }

    pub fn database_path(mut self, path: &str) -> Self {
        self.database_path = path.to_string();
        self
    }

    pub fn build(self) -> Result<Hematite> {
        Hematite::new(&self.database_path)
    }
}

impl Default for HematiteBuilder {
    fn default() -> Self {
        Self::new()
    }
}
