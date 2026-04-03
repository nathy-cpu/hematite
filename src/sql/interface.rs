//! High-level SQL interface

use crate::error::{HematiteError, Result};
use crate::query::{
    DateTimeValue, DateValue, DecimalValue, JournalMode, TimeValue, TimeWithTimeZoneValue, Value,
};
use crate::sql::connection::{Connection, PreparedStatement, Transaction};
use crate::sql::result::{ExecutedStatement, ResultSet, Row, StatementResult};
use crate::sql::script::ScriptIter;

/// High-level interface for SQL operations
pub struct Hematite {
    pub connection: Connection,
}

impl Hematite {
    /// Create a new database instance with an in-memory database
    pub fn new_in_memory() -> Result<Self> {
        let connection = Connection::new_in_memory()?;
        Ok(Self { connection })
    }

    /// Create a new database instance with a file-based database
    pub fn new(database_path: &str) -> Result<Self> {
        let connection = Connection::new(database_path)?;
        Ok(Self { connection })
    }

    /// Execute a SQL statement and return the result
    pub fn execute(&mut self, sql: &str) -> Result<StatementResult> {
        match self.connection.execute_result(sql)? {
            ExecutedStatement::Statement(result) => Ok(result),
            ExecutedStatement::Query(_) => Err(HematiteError::ParseError(
                "Use query() method for SELECT statements".to_string(),
            )),
        }
    }

    /// Execute a SQL query and return the result set
    pub fn query(&mut self, sql: &str) -> Result<ResultSet> {
        match self.connection.execute_result(sql)? {
            ExecutedStatement::Query(result_set) => Ok(result_set),
            ExecutedStatement::Statement(_) => Err(HematiteError::ParseError(
                "Use execute() method for non-SELECT statements".to_string(),
            )),
        }
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
        match ExecutedStatement::from_query_result(stmt.execute(&mut self.connection)?) {
            ExecutedStatement::Statement(result) => Ok(result),
            ExecutedStatement::Query(_) => Err(HematiteError::ParseError(
                "Use query_prepared() method for SELECT statements".to_string(),
            )),
        }
    }

    pub fn execute_result(&mut self, sql: &str) -> Result<ExecutedStatement> {
        self.connection.execute_result(sql)
    }

    pub fn iter_script<'a>(&'a mut self, sql: &str) -> Result<ScriptIter<'a>> {
        self.connection.iter_script(sql)
    }

    /// Begin a new transaction
    pub fn transaction(&'_ mut self) -> Result<Transaction<'_>> {
        self.connection.begin_transaction()
    }

    pub fn journal_mode(&self) -> Result<JournalMode> {
        self.connection.journal_mode()
    }

    pub fn set_journal_mode(&mut self, journal_mode: JournalMode) -> Result<()> {
        self.connection.set_journal_mode(journal_mode)
    }

    pub fn checkpoint_wal(&mut self) -> Result<()> {
        self.connection.checkpoint_wal()
    }

    /// Execute multiple SQL statements in sequence
    pub fn execute_batch(&mut self, sql: &str) -> Result<()> {
        self.connection.execute_batch(sql)
    }
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
                "Expected INT, found {:?}",
                value
            ))),
        }
    }
}

impl FromValue for String {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Text(s) => Ok(s.clone()),
            Value::Enum(s) => Ok(s.clone()),
            Value::Decimal(s) => Ok(s.to_string()),
            Value::Date(s) => Ok(s.to_string()),
            Value::Time(s) => Ok(s.to_string()),
            Value::DateTime(s) => Ok(s.to_string()),
            Value::TimeWithTimeZone(s) => Ok(s.to_string()),
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
            Value::Float32(f) => Ok(*f as f64),
            Value::Float(f) => Ok(*f),
            Value::Integer(i) => Ok(*i as f64), // Allow integer to float conversion
            Value::UInteger(i) => Ok(*i as f64),
            Value::BigInt(i) => Ok(*i as f64),
            Value::UBigInt(i) => Ok(*i as f64),
            Value::Int128(i) => Ok(*i as f64),
            Value::UInt128(i) => Ok(*i as f64),
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

impl FromValue for i64 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::BigInt(i) => Ok(*i),
            Value::Integer(i) => Ok(*i as i64),
            Value::UInteger(i) => Ok(*i as i64),
            _ => Err(HematiteError::ParseError(format!(
                "Expected INT64, found {:?}",
                value
            ))),
        }
    }
}

impl FromValue for i128 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Int128(i) => Ok(*i),
            Value::BigInt(i) => Ok(*i as i128),
            Value::Integer(i) => Ok(*i as i128),
            Value::UInteger(i) => Ok(*i as i128),
            Value::UBigInt(i) => Ok(*i as i128),
            _ => Err(HematiteError::ParseError(format!(
                "Expected INT128, found {:?}",
                value
            ))),
        }
    }
}

impl FromValue for u32 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::UInteger(i) => Ok(*i),
            Value::Integer(i) if *i >= 0 => Ok(*i as u32),
            _ => Err(HematiteError::ParseError(format!(
                "Expected UINT, found {:?}",
                value
            ))),
        }
    }
}

impl FromValue for u64 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::UBigInt(i) => Ok(*i),
            Value::UInteger(i) => Ok(*i as u64),
            Value::Integer(i) if *i >= 0 => Ok(*i as u64),
            _ => Err(HematiteError::ParseError(format!(
                "Expected UINT64, found {:?}",
                value
            ))),
        }
    }
}

impl FromValue for u128 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::UInt128(i) => Ok(*i),
            Value::UBigInt(i) => Ok(*i as u128),
            Value::UInteger(i) => Ok(*i as u128),
            Value::Integer(i) if *i >= 0 => Ok(*i as u128),
            _ => Err(HematiteError::ParseError(format!(
                "Expected UINT128, found {:?}",
                value
            ))),
        }
    }
}

impl FromValue for DecimalValue {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Decimal(value) => Ok(value.clone()),
            _ => Err(HematiteError::ParseError(format!(
                "Expected DECIMAL, found {:?}",
                value
            ))),
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Blob(value) => Ok(value.clone()),
            _ => Err(HematiteError::ParseError(format!(
                "Expected BLOB, found {:?}",
                value
            ))),
        }
    }
}

impl FromValue for DateValue {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Date(value) => Ok(*value),
            _ => Err(HematiteError::ParseError(format!(
                "Expected DATE, found {:?}",
                value
            ))),
        }
    }
}

impl FromValue for DateTimeValue {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::DateTime(value) => Ok(*value),
            _ => Err(HematiteError::ParseError(format!(
                "Expected DATETIME, found {:?}",
                value
            ))),
        }
    }
}

impl FromValue for TimeValue {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Time(value) => Ok(*value),
            _ => Err(HematiteError::ParseError(format!(
                "Expected TIME, found {:?}",
                value
            ))),
        }
    }
}

impl FromValue for TimeWithTimeZoneValue {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::TimeWithTimeZone(value) => Ok(*value),
            _ => Err(HematiteError::ParseError(format!(
                "Expected TIME WITH TIME ZONE, found {:?}",
                value
            ))),
        }
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
