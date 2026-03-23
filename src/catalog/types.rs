//! Data types and values for the database

use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Integer,
    Text,
    Boolean,
    Float,
}

impl DataType {
    pub fn size(&self) -> usize {
        match self {
            DataType::Integer => 4,
            DataType::Text => 255, // Maximum length
            DataType::Boolean => 1,
            DataType::Float => 8,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            DataType::Integer => "INTEGER",
            DataType::Text => "TEXT",
            DataType::Boolean => "BOOLEAN",
            DataType::Float => "FLOAT",
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
    Text(String),
    Boolean(bool),
    Float(f64),
    Null,
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Integer(_) => DataType::Integer,
            Value::Text(_) => DataType::Text,
            Value::Boolean(_) => DataType::Boolean,
            Value::Float(_) => DataType::Float,
            Value::Null => DataType::Text, // NULL can be any type
        }
    }

    pub fn is_compatible_with(&self, data_type: DataType) -> bool {
        match (self, data_type) {
            (Value::Integer(_), DataType::Integer) => true,
            (Value::Text(_), DataType::Text) => true,
            (Value::Boolean(_), DataType::Boolean) => true,
            (Value::Float(_), DataType::Float) => true,
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
            Value::Text(s) => Some(s),
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

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Null, _) => Some(Ordering::Less), // NULL is always less than any value
            (_, Value::Null) => Some(Ordering::Greater),
            _ => None,
        }
    }
}
