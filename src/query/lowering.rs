//! Boundary adapters from parser-owned syntax nodes into catalog/query runtime values.
//!
//! Parser stays independent from catalog runtime types on purpose. All value/type translation
//! that carries semantic meaning downward should pass through this file so the layer boundary stays
//! obvious and centralized.

use crate::catalog::{DataType, Value};
use crate::parser::{LiteralValue, SqlTypeName};

pub(crate) fn lower_literal_value(value: &LiteralValue) -> Value {
    match value {
        LiteralValue::Integer(value) => {
            if let Ok(value) = i32::try_from(*value) {
                Value::Integer(value)
            } else if let Ok(value) = i64::try_from(*value) {
                Value::BigInt(value)
            } else {
                Value::UBigInt(u64::try_from(*value).expect("validated 64-bit literal"))
            }
        }
        LiteralValue::Text(value) => Value::Text(value.clone()),
        LiteralValue::Blob(value) => Value::Blob(value.clone()),
        LiteralValue::Boolean(value) => Value::Boolean(*value),
        LiteralValue::Float(value) => Value::Float(
            value
                .parse::<f64>()
                .expect("parser normalized a valid FLOAT literal"),
        ),
        LiteralValue::Null => Value::Null,
    }
}

pub(crate) fn raise_literal_value(value: &Value) -> LiteralValue {
    match value {
        Value::Integer(value) => LiteralValue::Integer((*value).into()),
        Value::UInteger(value) => LiteralValue::Integer((*value).into()),
        Value::BigInt(value) => LiteralValue::Text(value.to_string()),
        Value::UBigInt(value) => LiteralValue::Text(value.to_string()),
        Value::Text(value) => LiteralValue::Text(value.clone()),
        Value::Blob(value) => LiteralValue::Blob(value.clone()),
        Value::Boolean(value) => LiteralValue::Boolean(*value),
        Value::Float32(value) => LiteralValue::Float(value.to_string()),
        Value::Float(value) => LiteralValue::Float(value.to_string()),
        Value::Decimal(value) => LiteralValue::Text(value.to_string()),
        Value::Null => LiteralValue::Null,
    }
}

pub(crate) fn lower_type_name(data_type: SqlTypeName) -> DataType {
    match data_type {
        SqlTypeName::Int8 => DataType::Int8,
        SqlTypeName::Int16 => DataType::Int16,
        SqlTypeName::Int => DataType::Int,
        SqlTypeName::Int64 => DataType::Int64,
        SqlTypeName::UInt8 => DataType::UInt8,
        SqlTypeName::UInt16 => DataType::UInt16,
        SqlTypeName::UInt => DataType::UInt,
        SqlTypeName::UInt64 => DataType::UInt64,
        SqlTypeName::Text => DataType::Text,
        SqlTypeName::VarChar(length) => DataType::VarChar(length),
        SqlTypeName::Boolean => DataType::Boolean,
        SqlTypeName::Float32 => DataType::Float32,
        SqlTypeName::Float => DataType::Float,
        SqlTypeName::Decimal { precision, scale } => DataType::Decimal { precision, scale },
        SqlTypeName::Blob => DataType::Blob,
    }
}
