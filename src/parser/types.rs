//! Parser-owned SQL literal and type names.

use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlTypeName {
    Integer,
    Text,
    Boolean,
    Float,
}

#[derive(Debug, Clone)]
pub enum LiteralValue {
    Integer(i32),
    Text(String),
    Boolean(bool),
    Float(f64),
    Null,
}

impl LiteralValue {
    pub fn data_type(&self) -> SqlTypeName {
        match self {
            LiteralValue::Integer(_) => SqlTypeName::Integer,
            LiteralValue::Text(_) => SqlTypeName::Text,
            LiteralValue::Boolean(_) => SqlTypeName::Boolean,
            LiteralValue::Float(_) => SqlTypeName::Float,
            LiteralValue::Null => SqlTypeName::Text,
        }
    }

    pub fn is_compatible_with(&self, data_type: SqlTypeName) -> bool {
        match (self, data_type) {
            (LiteralValue::Integer(_), SqlTypeName::Integer) => true,
            (LiteralValue::Text(_), SqlTypeName::Text) => true,
            (LiteralValue::Boolean(_), SqlTypeName::Boolean) => true,
            (LiteralValue::Float(_), SqlTypeName::Float) => true,
            (LiteralValue::Null, _) => true,
            _ => false,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, LiteralValue::Null)
    }
}

impl PartialEq for LiteralValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (LiteralValue::Integer(a), LiteralValue::Integer(b)) => a == b,
            (LiteralValue::Text(a), LiteralValue::Text(b)) => a == b,
            (LiteralValue::Boolean(a), LiteralValue::Boolean(b)) => a == b,
            (LiteralValue::Float(a), LiteralValue::Float(b)) => a == b,
            (LiteralValue::Null, LiteralValue::Null) => true,
            _ => false,
        }
    }
}

impl Eq for LiteralValue {}

impl PartialOrd for LiteralValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (LiteralValue::Integer(a), LiteralValue::Integer(b)) => a.partial_cmp(b),
            (LiteralValue::Text(a), LiteralValue::Text(b)) => a.partial_cmp(b),
            (LiteralValue::Boolean(a), LiteralValue::Boolean(b)) => a.partial_cmp(b),
            (LiteralValue::Float(a), LiteralValue::Float(b)) => a.partial_cmp(b),
            (LiteralValue::Null, _) => Some(Ordering::Less),
            (_, LiteralValue::Null) => Some(Ordering::Greater),
            _ => None,
        }
    }
}
