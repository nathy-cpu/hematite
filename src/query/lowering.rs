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
                Value::Int128(*value)
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
        Value::Int128(value) => LiteralValue::Text(value.to_string()),
        Value::UInt128(value) => LiteralValue::Text(value.to_string()),
        Value::Text(value) => LiteralValue::Text(value.clone()),
        Value::Blob(value) => LiteralValue::Blob(value.clone()),
        Value::Enum(value) => LiteralValue::Text(value.clone()),
        Value::Boolean(value) => LiteralValue::Boolean(*value),
        Value::Float32(value) => LiteralValue::Float(value.to_string()),
        Value::Float(value) => LiteralValue::Float(value.to_string()),
        Value::Decimal(value) => LiteralValue::Text(value.to_string()),
        Value::Date(value) => LiteralValue::Text(value.to_string()),
        Value::Time(value) => LiteralValue::Text(value.to_string()),
        Value::DateTime(value) => LiteralValue::Text(value.to_string()),
        Value::TimeWithTimeZone(value) => LiteralValue::Text(value.to_string()),
        Value::IntervalYearMonth(value) => LiteralValue::Text(value.to_string()),
        Value::IntervalDaySecond(value) => LiteralValue::Text(value.to_string()),
        Value::Null => LiteralValue::Null,
    }
}

pub(crate) fn lower_type_name(data_type: SqlTypeName) -> DataType {
    match data_type {
        SqlTypeName::Int8 => DataType::Int8,
        SqlTypeName::Int16 => DataType::Int16,
        SqlTypeName::Int => DataType::Int,
        SqlTypeName::Int64 => DataType::Int64,
        SqlTypeName::Int128 => DataType::Int128,
        SqlTypeName::UInt8 => DataType::UInt8,
        SqlTypeName::UInt16 => DataType::UInt16,
        SqlTypeName::UInt => DataType::UInt,
        SqlTypeName::UInt64 => DataType::UInt64,
        SqlTypeName::UInt128 => DataType::UInt128,
        SqlTypeName::Text => DataType::Text,
        SqlTypeName::Char(length) => DataType::Char(length),
        SqlTypeName::VarChar(length) => DataType::VarChar(length),
        SqlTypeName::Binary(length) => DataType::Binary(length),
        SqlTypeName::VarBinary(length) => DataType::VarBinary(length),
        SqlTypeName::Enum(values) => DataType::Enum(values),
        SqlTypeName::Boolean => DataType::Boolean,
        SqlTypeName::Float32 => DataType::Float32,
        SqlTypeName::Float => DataType::Float,
        SqlTypeName::Decimal { precision, scale } => DataType::Decimal { precision, scale },
        SqlTypeName::Blob => DataType::Blob,
        SqlTypeName::Date => DataType::Date,
        SqlTypeName::Time => DataType::Time,
        SqlTypeName::DateTime => DataType::DateTime,
        SqlTypeName::TimeWithTimeZone => DataType::TimeWithTimeZone,
        SqlTypeName::IntervalYearMonth => DataType::IntervalYearMonth,
        SqlTypeName::IntervalDaySecond => DataType::IntervalDaySecond,
    }
}
