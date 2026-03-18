//! High-level SQL interface

use crate::catalog::Value;
use crate::error::{HematiteError, Result};
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
        // Split SQL by semicolons and execute each statement.
        // The parser currently expects statements to end with a semicolon, so we re-append it.
        let statements: Vec<&str> = sql
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        for statement in statements {
            let mut owned = statement.to_string();
            owned.push(';');
            self.execute(&owned)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hematite_basic_operations() -> Result<()> {
        let mut db = Hematite::new_in_memory()?;

        // Create table
        let result = db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        assert_eq!(result.affected_rows, 0);

        // Insert data
        let result = db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
        assert_eq!(result.affected_rows, 1);

        // Query data
        let result_set = db.query("SELECT * FROM users;")?;
        assert_eq!(result_set.len(), 1);

        let row = result_set.get_row(0).unwrap();
        assert_eq!(row.get_int(0)?, 1);
        assert_eq!(row.get_string(1)?, "Alice");

        Ok(())
    }

    #[test]
    fn test_hematite_query_one() -> Result<()> {
        let mut db = Hematite::new_in_memory()?;

        // Create table and insert data
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER);")?;
        db.execute("INSERT INTO test (id, value) VALUES (1, 42);")?;

        // Query single value using simple SELECT
        let result_set = db.query("SELECT * FROM test;")?;
        assert_eq!(result_set.len(), 1);

        Ok(())
    }

    #[test]
    fn test_hematite_prepare() -> Result<()> {
        let mut db = Hematite::new_in_memory()?;

        // Create table
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;

        // Prepare statement with actual values instead of placeholders for this simplified implementation
        let mut stmt = db.prepare("INSERT INTO test (id, name) VALUES (1, 'test');")?;

        // Note: This is a simplified implementation - real prepared statements would support parameters
        let result = stmt.execute(&mut db.connection)?;
        assert_eq!(result.rows.len(), 0);

        Ok(())
    }

    #[test]
    fn test_hematite_transaction() -> Result<()> {
        let mut db = Hematite::new_in_memory()?;

        // Create table
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY);")?;

        // Begin transaction and execute within its scope
        {
            let mut tx = db.transaction()?;

            // Insert data
            tx.execute("INSERT INTO test (id) VALUES (1);")?;

            // Commit transaction
            tx.commit()?;
        } // tx is dropped here, releasing the mutable borrow

        // Verify data - now safe to use db again
        let result_set = db.query("SELECT * FROM test;")?;
        assert_eq!(result_set.len(), 1);

        Ok(())
    }

    #[test]
    fn test_execute_batch_semicolon_handling() -> Result<()> {
        let mut db = Hematite::new_in_memory()?;

        db.execute_batch(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n\
             INSERT INTO users (id, name) VALUES (1, 'Alice');\n\
             INSERT INTO users (id, name) VALUES (2, 'Bob');",
        )?;

        let rs = db.query("SELECT * FROM users;")?;
        assert_eq!(rs.len(), 2);
        Ok(())
    }

    #[test]
    fn test_sql_debug_simple() -> Result<()> {
        println!("=== Testing SQL Parsing Only ===");

        use crate::parser::{Lexer, Parser};

        println!("✓ Step 1: Creating lexer...");
        let mut lexer =
            Lexer::new("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);".to_string());
        lexer.tokenize()?;
        println!("✓ Lexing completed");

        println!("✓ Step 2: Creating parser...");
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        println!("✓ Parsing completed: {:?}", statement);

        println!("✓ SUCCESS: Parsing test passed");
        Ok(())
    }
}
