//! Parser-owned SQL literal and type names.

use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SqlTypeName {
    Int8,
    Int16,
    Int,
    Int64,
    Int128,
    UInt8,
    UInt16,
    UInt,
    UInt64,
    UInt128,
    Text,
    Char(u32),
    VarChar(u32),
    Binary(u32),
    VarBinary(u32),
    Enum(Vec<String>),
    Boolean,
    Float32,
    Float,
    Decimal {
        precision: Option<u32>,
        scale: Option<u32>,
    },
    Blob,
    Date,
    Time,
    DateTime,
    TimeWithTimeZone,
    IntervalYearMonth,
    IntervalDaySecond,
}

impl SqlTypeName {
    pub fn to_sql(&self) -> String {
        match self {
            SqlTypeName::Int8 => "INT8".to_string(),
            SqlTypeName::Int16 => "INT16".to_string(),
            SqlTypeName::Int => "INT".to_string(),
            SqlTypeName::Int64 => "INT64".to_string(),
            SqlTypeName::Int128 => "INT128".to_string(),
            SqlTypeName::UInt8 => "UINT8".to_string(),
            SqlTypeName::UInt16 => "UINT16".to_string(),
            SqlTypeName::UInt => "UINT".to_string(),
            SqlTypeName::UInt64 => "UINT64".to_string(),
            SqlTypeName::UInt128 => "UINT128".to_string(),
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
            SqlTypeName::Float32 => "FLOAT32".to_string(),
            SqlTypeName::Float => "FLOAT".to_string(),
            SqlTypeName::Decimal { precision, scale } => {
                format_numeric_type("DECIMAL", *precision, *scale)
            }
            SqlTypeName::Blob => "BLOB".to_string(),
            SqlTypeName::Date => "DATE".to_string(),
            SqlTypeName::Time => "TIME".to_string(),
            SqlTypeName::DateTime => "DATETIME".to_string(),
            SqlTypeName::TimeWithTimeZone => "TIME WITH TIME ZONE".to_string(),
            SqlTypeName::IntervalYearMonth => "INTERVAL YEAR TO MONTH".to_string(),
            SqlTypeName::IntervalDaySecond => "INTERVAL DAY TO SECOND".to_string(),
        }
    }

    pub fn supports_text_metadata(&self) -> bool {
        matches!(
            self,
            SqlTypeName::Text | SqlTypeName::Char(_) | SqlTypeName::VarChar(_)
        )
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
    Integer(i128),
    Text(String),
    Blob(Vec<u8>),
    Boolean(bool),
    Float(String),
    Null,
}

impl LiteralValue {
    pub fn data_type(&self) -> SqlTypeName {
        match self {
            LiteralValue::Integer(_) => SqlTypeName::Int,
            LiteralValue::Text(_) => SqlTypeName::Text,
            LiteralValue::Blob(_) => SqlTypeName::Blob,
            LiteralValue::Boolean(_) => SqlTypeName::Boolean,
            LiteralValue::Float(_) => SqlTypeName::Float,
            LiteralValue::Null => SqlTypeName::Text,
        }
    }

    pub fn is_compatible_with(&self, data_type: SqlTypeName) -> bool {
        match (self, data_type) {
            (LiteralValue::Integer(_), SqlTypeName::Int8) => true,
            (LiteralValue::Integer(_), SqlTypeName::Int16) => true,
            (LiteralValue::Integer(_), SqlTypeName::Int) => true,
            (LiteralValue::Integer(_), SqlTypeName::Int64) => true,
            (LiteralValue::Integer(_), SqlTypeName::Int128) => true,
            (LiteralValue::Integer(value), SqlTypeName::UInt8) => *value >= 0,
            (LiteralValue::Integer(value), SqlTypeName::UInt16) => *value >= 0,
            (LiteralValue::Integer(value), SqlTypeName::UInt) => *value >= 0,
            (LiteralValue::Integer(value), SqlTypeName::UInt64) => *value >= 0,
            (LiteralValue::Integer(value), SqlTypeName::UInt128) => *value >= 0,
            (LiteralValue::Integer(_), SqlTypeName::Decimal { .. }) => true,
            (LiteralValue::Float(_), SqlTypeName::Float32) => true,
            (LiteralValue::Float(_), SqlTypeName::Float) => true,
            (LiteralValue::Float(_), SqlTypeName::Decimal { .. }) => true,
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
            (LiteralValue::Text(_), SqlTypeName::TimeWithTimeZone) => true,
            (LiteralValue::Text(_), SqlTypeName::Decimal { .. }) => true,
            (LiteralValue::Blob(_), SqlTypeName::Binary(_)) => true,
            (LiteralValue::Blob(_), SqlTypeName::VarBinary(_)) => true,
            (LiteralValue::Blob(_), SqlTypeName::Blob) => true,
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
            (LiteralValue::Blob(a), LiteralValue::Blob(b)) => a == b,
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
            (LiteralValue::Blob(a), LiteralValue::Blob(b)) => a.partial_cmp(b),
            (LiteralValue::Boolean(a), LiteralValue::Boolean(b)) => a.partial_cmp(b),
            (LiteralValue::Float(a), LiteralValue::Float(b)) => {
                compare_normalized_float_literals(a, b)
            }
            (LiteralValue::Null, _) => Some(Ordering::Less),
            (_, LiteralValue::Null) => Some(Ordering::Greater),
            _ => None,
        }
    }
}

pub fn normalize_float_literal(input: &str) -> String {
    let trimmed = input.trim();
    let (negative, digits) = match trimmed.as_bytes().first() {
        Some(b'+') => (false, &trimmed[1..]),
        Some(b'-') => (true, &trimmed[1..]),
        _ => (false, trimmed),
    };

    let mut parts = digits.split('.');
    let integer = parts.next().unwrap_or_default().trim_start_matches('0');
    let integer = if integer.is_empty() { "0" } else { integer };
    let mut fraction = parts.next().unwrap_or_default().to_string();
    while fraction.ends_with('0') {
        fraction.pop();
    }

    let combined = if fraction.is_empty() {
        integer.to_string()
    } else {
        format!("{integer}.{fraction}")
    };

    if combined == "0" {
        "0".to_string()
    } else if negative {
        format!("-{combined}")
    } else {
        combined
    }
}

fn compare_normalized_float_literals(left: &str, right: &str) -> Option<Ordering> {
    let (left_negative, left_integer, left_fraction) = split_float_literal(left)?;
    let (right_negative, right_integer, right_fraction) = split_float_literal(right)?;

    if left_negative != right_negative {
        return Some(if left_negative {
            Ordering::Less
        } else {
            Ordering::Greater
        });
    }

    let ordering = left_integer
        .len()
        .cmp(&right_integer.len())
        .then_with(|| left_integer.cmp(right_integer))
        .then_with(|| {
            let len = left_fraction.len().max(right_fraction.len());
            let left_padded = format!("{left_fraction:0<len$}");
            let right_padded = format!("{right_fraction:0<len$}");
            left_padded.cmp(&right_padded)
        });

    Some(if left_negative {
        ordering.reverse()
    } else {
        ordering
    })
}

fn split_float_literal(input: &str) -> Option<(bool, &str, &str)> {
    let (negative, digits) = match input.as_bytes().first() {
        Some(b'-') => (true, &input[1..]),
        _ => (false, input),
    };
    let mut parts = digits.split('.');
    let integer = parts.next()?;
    let fraction = parts.next().unwrap_or_default();
    Some((negative, integer, fraction))
}
