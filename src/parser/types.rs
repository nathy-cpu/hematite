//! Parser-owned SQL literal and type names.

use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SqlTypeName {
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Text,
    Char(u32),
    VarChar(u32),
    Binary(u32),
    VarBinary(u32),
    Enum(Vec<String>),
    Boolean,
    Float,
    Real,
    Double,
    Decimal {
        precision: Option<u32>,
        scale: Option<u32>,
    },
    Numeric {
        precision: Option<u32>,
        scale: Option<u32>,
    },
    Blob,
    Date,
    Time,
    DateTime,
    Timestamp,
    TimeWithTimeZone,
}

impl SqlTypeName {
    pub fn to_sql(&self) -> String {
        match self {
            SqlTypeName::TinyInt => "TINYINT".to_string(),
            SqlTypeName::SmallInt => "SMALLINT".to_string(),
            SqlTypeName::Integer => "INTEGER".to_string(),
            SqlTypeName::BigInt => "BIGINT".to_string(),
            SqlTypeName::Text => "TEXT".to_string(),
            SqlTypeName::Char(length) => format!("CHAR({length})"),
            SqlTypeName::VarChar(length) => format!("VARCHAR({length})"),
            SqlTypeName::Binary(length) => format!("BINARY({length})"),
            SqlTypeName::VarBinary(length) => format!("VARBINARY({length})"),
            SqlTypeName::Enum(values) => format!(
                "ENUM({})",
                values
                    .iter()
                    .map(|value| format!("'{}'", value.replace('\'', "''")))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            SqlTypeName::Boolean => "BOOLEAN".to_string(),
            SqlTypeName::Float => "FLOAT".to_string(),
            SqlTypeName::Real => "REAL".to_string(),
            SqlTypeName::Double => "DOUBLE".to_string(),
            SqlTypeName::Decimal { precision, scale } => {
                format_numeric_type("DECIMAL", *precision, *scale)
            }
            SqlTypeName::Numeric { precision, scale } => {
                format_numeric_type("NUMERIC", *precision, *scale)
            }
            SqlTypeName::Blob => "BLOB".to_string(),
            SqlTypeName::Date => "DATE".to_string(),
            SqlTypeName::Time => "TIME".to_string(),
            SqlTypeName::DateTime => "DATETIME".to_string(),
            SqlTypeName::Timestamp => "TIMESTAMP".to_string(),
            SqlTypeName::TimeWithTimeZone => "TIME WITH TIME ZONE".to_string(),
        }
    }
}

fn format_numeric_type(name: &str, precision: Option<u32>, scale: Option<u32>) -> String {
    match (precision, scale) {
        (Some(precision), Some(scale)) => format!("{name}({precision}, {scale})"),
        (Some(precision), None) => format!("{name}({precision})"),
        (None, _) => name.to_string(),
    }
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
            (LiteralValue::Integer(_), SqlTypeName::TinyInt) => true,
            (LiteralValue::Integer(_), SqlTypeName::SmallInt) => true,
            (LiteralValue::Integer(_), SqlTypeName::Integer) => true,
            (LiteralValue::Integer(_), SqlTypeName::BigInt) => true,
            (LiteralValue::Integer(_), SqlTypeName::Decimal { .. }) => true,
            (LiteralValue::Integer(_), SqlTypeName::Numeric { .. }) => true,
            (LiteralValue::Float(_), SqlTypeName::Float) => true,
            (LiteralValue::Float(_), SqlTypeName::Real) => true,
            (LiteralValue::Float(_), SqlTypeName::Double) => true,
            (LiteralValue::Float(_), SqlTypeName::Decimal { .. }) => true,
            (LiteralValue::Float(_), SqlTypeName::Numeric { .. }) => true,
            (LiteralValue::Text(_), SqlTypeName::Text) => true,
            (LiteralValue::Text(_), SqlTypeName::Char(_)) => true,
            (LiteralValue::Text(_), SqlTypeName::VarChar(_)) => true,
            (LiteralValue::Text(_), SqlTypeName::Binary(_)) => true,
            (LiteralValue::Text(_), SqlTypeName::VarBinary(_)) => true,
            (LiteralValue::Text(_), SqlTypeName::Enum(_)) => true,
            (LiteralValue::Text(_), SqlTypeName::Blob) => true,
            (LiteralValue::Text(_), SqlTypeName::Date) => true,
            (LiteralValue::Text(_), SqlTypeName::Time) => true,
            (LiteralValue::Text(_), SqlTypeName::DateTime) => true,
            (LiteralValue::Text(_), SqlTypeName::Timestamp) => true,
            (LiteralValue::Text(_), SqlTypeName::TimeWithTimeZone) => true,
            (LiteralValue::Text(_), SqlTypeName::Decimal { .. }) => true,
            (LiteralValue::Text(_), SqlTypeName::Numeric { .. }) => true,
            (LiteralValue::Boolean(_), SqlTypeName::Boolean) => true,
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
