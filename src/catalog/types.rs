//! Data types and values for the database

use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Integer,
    BigInt,
    Text,
    Boolean,
    Float,
    Decimal,
    Blob,
    Date,
    DateTime,
}

impl DataType {
    pub fn size(&self) -> usize {
        match self {
            DataType::Integer => 4,
            DataType::BigInt => 8,
            DataType::Text => 255, // Maximum length
            DataType::Boolean => 1,
            DataType::Float => 8,
            DataType::Decimal => 32,
            DataType::Blob => 255,
            DataType::Date => 10,
            DataType::DateTime => 19,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            DataType::Integer => "INTEGER",
            DataType::BigInt => "BIGINT",
            DataType::Text => "TEXT",
            DataType::Boolean => "BOOLEAN",
            DataType::Float => "FLOAT",
            DataType::Decimal => "DECIMAL",
            DataType::Blob => "BLOB",
            DataType::Date => "DATE",
            DataType::DateTime => "DATETIME",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalMode {
    Rollback,
    Wal,
}

#[derive(Debug, Clone)]
pub enum Value {
    Integer(i32),
    BigInt(i64),
    Text(String),
    Boolean(bool),
    Float(f64),
    Decimal(String),
    Blob(Vec<u8>),
    Date(String),
    DateTime(String),
    Null,
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Integer(_) => DataType::Integer,
            Value::BigInt(_) => DataType::BigInt,
            Value::Text(_) => DataType::Text,
            Value::Boolean(_) => DataType::Boolean,
            Value::Float(_) => DataType::Float,
            Value::Decimal(_) => DataType::Decimal,
            Value::Blob(_) => DataType::Blob,
            Value::Date(_) => DataType::Date,
            Value::DateTime(_) => DataType::DateTime,
            Value::Null => DataType::Text, // NULL can be any type
        }
    }

    pub fn is_compatible_with(&self, data_type: DataType) -> bool {
        match (self, data_type) {
            (Value::Integer(_), DataType::Integer) => true,
            (Value::BigInt(_), DataType::BigInt) => true,
            (Value::Text(_), DataType::Text) => true,
            (Value::Boolean(_), DataType::Boolean) => true,
            (Value::Float(_), DataType::Float) => true,
            (Value::Decimal(_), DataType::Decimal) => true,
            (Value::Blob(_), DataType::Blob) => true,
            (Value::Date(_), DataType::Date) => true,
            (Value::DateTime(_), DataType::DateTime) => true,
            (Value::Null, _) => true, // NULL is compatible with any type
            _ => false,
        }
    }

    pub fn as_integer(&self) -> Option<i32> {
        match self {
            Value::Integer(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) | Value::Decimal(s) | Value::Date(s) | Value::DateTime(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_bigint(&self) -> Option<i64> {
        match self {
            Value::BigInt(i) => Some(*i),
            Value::Integer(i) => Some(*i as i64),
            _ => None,
        }
    }

    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            Value::Blob(bytes) => Some(bytes),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::BigInt(a), Value::BigInt(b)) => a == b,
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::Decimal(a), Value::Decimal(b)) => a == b,
            (Value::Blob(a), Value::Blob(b)) => a == b,
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::DateTime(a), Value::DateTime(b)) => a == b,
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::BigInt(a), Value::BigInt(b)) => a.partial_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Decimal(a), Value::Decimal(b)) => a.partial_cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.partial_cmp(b),
            (Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
            (Value::DateTime(a), Value::DateTime(b)) => a.partial_cmp(b),
            (Value::Null, _) => Some(Ordering::Less), // NULL is always less than any value
            (_, Value::Null) => Some(Ordering::Greater),
            _ => None,
        }
    }
}
