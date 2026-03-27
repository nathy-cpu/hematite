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
        Value::Text(value) => LiteralValue::Text(value.clone()),
        Value::Boolean(value) => LiteralValue::Boolean(*value),
        Value::Float(value) => LiteralValue::Float(*value),
        Value::Null => LiteralValue::Null,
    }
}

pub(crate) fn lower_type_name(data_type: SqlTypeName) -> DataType {
    match data_type {
        SqlTypeName::Integer => DataType::Integer,
        SqlTypeName::Text => DataType::Text,
        SqlTypeName::Boolean => DataType::Boolean,
        SqlTypeName::Float => DataType::Float,
    }
}
