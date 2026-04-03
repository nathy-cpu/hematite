//! SQL result set and row interface

use crate::error::{HematiteError, Result};
use crate::query::{
    DateTimeValue, DateValue, DecimalValue, IntervalDaySecondValue, IntervalYearMonthValue,
    QueryResult, TimeValue, TimeWithTimeZoneValue, TimestampValue, Value,
};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    column_index: HashMap<String, usize>,
}

impl ResultSet {
    pub fn new(columns: Vec<String>, rows: Vec<Vec<Value>>) -> Self {
        let mut column_index = HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            column_index.insert(col.clone(), i);
        }

        let rows = rows.into_iter().map(Row::new).collect();

        Self {
            columns,
            rows,
            column_index,
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn get_row(&self, index: usize) -> Option<&Row> {
        self.rows.get(index)
    }

    pub fn iter(&'_ self) -> std::slice::Iter<'_, Row> {
        self.rows.iter()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.column_index.get(column_name).copied()
    }
}

impl IntoIterator for ResultSet {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    pub fn get_by_name(
        &self,
        column_name: &str,
        column_index: &HashMap<String, usize>,
    ) -> Option<&Value> {
        if let Some(&idx) = column_index.get(column_name) {
            self.get(idx)
        } else {
            None
        }
    }

    pub fn get_int(&self, index: usize) -> Result<i32> {
        match self.get(index) {
            Some(Value::Integer(i)) => Ok(*i),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected INT, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_string(&self, index: usize) -> Result<String> {
        match self.get(index) {
            Some(Value::Text(s)) => Ok(s.clone()),
            Some(Value::Enum(s)) => Ok(s.clone()),
            Some(Value::Decimal(value)) => Ok(value.to_string()),
            Some(Value::Date(value)) => Ok(value.to_string()),
            Some(Value::Time(value)) => Ok(value.to_string()),
            Some(Value::DateTime(value)) => Ok(value.to_string()),
            Some(Value::Timestamp(value)) => Ok(value.to_string()),
            Some(Value::TimeWithTimeZone(value)) => Ok(value.to_string()),
            Some(Value::IntervalYearMonth(value)) => Ok(value.to_string()),
            Some(Value::IntervalDaySecond(value)) => Ok(value.to_string()),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected TEXT, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_bool(&self, index: usize) -> Result<bool> {
        match self.get(index) {
            Some(Value::Boolean(b)) => Ok(*b),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected BOOLEAN, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_float(&self, index: usize) -> Result<f64> {
        match self.get(index) {
            Some(Value::Float32(f)) => Ok(*f as f64),
            Some(Value::Float(f)) => Ok(*f),
            Some(Value::Float128(f)) => Ok(*f),
            Some(Value::Integer(i)) => Ok(*i as f64), // Allow integer to float conversion
            Some(Value::UInteger(i)) => Ok(*i as f64),
            Some(Value::BigInt(i)) => Ok(*i as f64),
            Some(Value::UBigInt(i)) => Ok(*i as f64),
            Some(Value::Int128(i)) => Ok(*i as f64),
            Some(Value::UInt128(i)) => Ok(*i as f64),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected FLOAT, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn is_null(&self, index: usize) -> bool {
        matches!(self.get(index), Some(Value::Null))
    }

    pub fn get_bigint(&self, index: usize) -> Result<i64> {
        match self.get(index) {
            Some(Value::BigInt(i)) => Ok(*i),
            Some(Value::Integer(i)) => Ok(*i as i64),
            Some(Value::UInteger(i)) => Ok(*i as i64),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected INT64, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_int128(&self, index: usize) -> Result<i128> {
        match self.get(index) {
            Some(Value::Int128(i)) => Ok(*i),
            Some(Value::BigInt(i)) => Ok(*i as i128),
            Some(Value::Integer(i)) => Ok(*i as i128),
            Some(Value::UInteger(i)) => Ok(*i as i128),
            Some(Value::UBigInt(i)) => Ok(*i as i128),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected INT128, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_uint(&self, index: usize) -> Result<u32> {
        match self.get(index) {
            Some(Value::UInteger(i)) => Ok(*i),
            Some(Value::Integer(i)) if *i >= 0 => Ok(*i as u32),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected UINT, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_uint64(&self, index: usize) -> Result<u64> {
        match self.get(index) {
            Some(Value::UBigInt(i)) => Ok(*i),
            Some(Value::UInteger(i)) => Ok(*i as u64),
            Some(Value::Integer(i)) if *i >= 0 => Ok(*i as u64),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected UINT64, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_uint128(&self, index: usize) -> Result<u128> {
        match self.get(index) {
            Some(Value::UInt128(i)) => Ok(*i),
            Some(Value::UBigInt(i)) => Ok(*i as u128),
            Some(Value::UInteger(i)) => Ok(*i as u128),
            Some(Value::Integer(i)) if *i >= 0 => Ok(*i as u128),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected UINT128, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_decimal(&self, index: usize) -> Result<DecimalValue> {
        match self.get(index) {
            Some(Value::Decimal(value)) => Ok(value.clone()),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected DECIMAL, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_blob(&self, index: usize) -> Result<Vec<u8>> {
        match self.get(index) {
            Some(Value::Blob(value)) => Ok(value.clone()),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected BLOB, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_date(&self, index: usize) -> Result<DateValue> {
        match self.get(index) {
            Some(Value::Date(value)) => Ok(*value),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected DATE, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_time(&self, index: usize) -> Result<TimeValue> {
        match self.get(index) {
            Some(Value::Time(value)) => Ok(*value),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected TIME, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_datetime(&self, index: usize) -> Result<DateTimeValue> {
        match self.get(index) {
            Some(Value::DateTime(value)) => Ok(*value),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected DATETIME, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_timestamp(&self, index: usize) -> Result<TimestampValue> {
        match self.get(index) {
            Some(Value::Timestamp(value)) => Ok(*value),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected TIMESTAMP, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_time_with_time_zone(&self, index: usize) -> Result<TimeWithTimeZoneValue> {
        match self.get(index) {
            Some(Value::TimeWithTimeZone(value)) => Ok(*value),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected TIME WITH TIME ZONE, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_interval_year_month(&self, index: usize) -> Result<IntervalYearMonthValue> {
        match self.get(index) {
            Some(Value::IntervalYearMonth(value)) => Ok(*value),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected INTERVAL YEAR TO MONTH, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_interval_day_second(&self, index: usize) -> Result<IntervalDaySecondValue> {
        match self.get(index) {
            Some(Value::IntervalDaySecond(value)) => Ok(*value),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected INTERVAL DAY TO SECOND, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }
}

impl IntoIterator for Row {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

#[derive(Debug, Clone)]
pub struct StatementResult {
    pub affected_rows: usize,
    pub last_insert_id: Option<i32>,
    pub message: String,
}

impl StatementResult {
    pub fn new(affected_rows: usize, message: String) -> Self {
        Self {
            affected_rows,
            last_insert_id: None,
            message,
        }
    }

    pub fn with_insert_id(affected_rows: usize, last_insert_id: i32, message: String) -> Self {
        Self {
            affected_rows,
            last_insert_id: Some(last_insert_id),
            message,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ExecutedStatement {
    Query(ResultSet),
    Statement(StatementResult),
}

impl ExecutedStatement {
    pub(crate) fn from_query_result(query_result: QueryResult) -> Self {
        if query_result.columns.is_empty() {
            Self::Statement(StatementResult::new(
                query_result.affected_rows,
                "Statement executed successfully".to_string(),
            ))
        } else {
            Self::Query(ResultSet::new(query_result.columns, query_result.rows))
        }
    }

    pub fn as_query(&self) -> Option<&ResultSet> {
        match self {
            Self::Query(result_set) => Some(result_set),
            Self::Statement(_) => None,
        }
    }

    pub fn as_statement(&self) -> Option<&StatementResult> {
        match self {
            Self::Query(_) => None,
            Self::Statement(result) => Some(result),
        }
    }
}
