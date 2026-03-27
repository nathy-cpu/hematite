//! Boundary adapters from parser-owned syntax nodes into catalog/query runtime values.
//!
//! Parser stays independent from catalog runtime types on purpose. All value/type translation
//! that carries semantic meaning downward should pass through this file so the layer boundary stays
//! obvious and centralized.

use crate::catalog::{DataType, Value};
use crate::parser::{LiteralValue, SqlTypeName};

pub(crate) fn lower_literal_value(value: &LiteralValue) -> Value {
    match value {
        LiteralValue::Integer(value) => Value::Integer(*value),
        LiteralValue::Text(value) => Value::Text(value.clone()),
        LiteralValue::Boolean(value) => Value::Boolean(*value),
        LiteralValue::Float(value) => Value::Float(*value),
        LiteralValue::Null => Value::Null,
    }
}

pub(crate) fn raise_literal_value(value: &Value) -> LiteralValue {
    match value {
        Value::Integer(value) => LiteralValue::Integer(*value),
        Value::BigInt(value) => LiteralValue::Text(value.to_string()),
        Value::Text(value) => LiteralValue::Text(value.clone()),
        Value::Boolean(value) => LiteralValue::Boolean(*value),
        Value::Float(value) => LiteralValue::Float(*value),
        Value::Decimal(value) => LiteralValue::Text(value.to_string()),
        Value::Blob(value) => LiteralValue::Text(String::from_utf8_lossy(value).into_owned()),
        Value::Date(value) => LiteralValue::Text(value.to_string()),
        Value::DateTime(value) => LiteralValue::Text(value.to_string()),
        Value::Null => LiteralValue::Null,
    }
}

pub(crate) fn lower_type_name(data_type: SqlTypeName) -> DataType {
    match data_type {
        SqlTypeName::TinyInt => DataType::TinyInt,
        SqlTypeName::SmallInt => DataType::SmallInt,
        SqlTypeName::Integer => DataType::Integer,
        SqlTypeName::BigInt => DataType::BigInt,
        SqlTypeName::Text => DataType::Text,
        SqlTypeName::Char(length) => DataType::Char(length),
        SqlTypeName::VarChar(length) => DataType::VarChar(length),
        SqlTypeName::Boolean => DataType::Boolean,
        SqlTypeName::Float => DataType::Float,
        SqlTypeName::Real => DataType::Real,
        SqlTypeName::Double => DataType::Double,
        SqlTypeName::Decimal { precision, scale } => DataType::Decimal { precision, scale },
        SqlTypeName::Numeric { precision, scale } => DataType::Numeric { precision, scale },
        SqlTypeName::Blob => DataType::Blob,
        SqlTypeName::Date => DataType::Date,
        SqlTypeName::DateTime => DataType::DateTime,
    }
}
