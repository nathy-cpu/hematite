//! Column definitions for database tables.

use super::types::{DataType, DateTimeValue, DateValue, DecimalValue, Value};
use super::ColumnId;
use crate::error::HematiteError;

#[derive(Debug, Clone)]
pub struct Column {
    pub id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub auto_increment: bool,
    pub default_value: Option<Value>,
}

impl Column {
    pub fn new(id: ColumnId, name: String, data_type: DataType) -> Self {
        Self {
            id,
            name,
            data_type,
            nullable: true,
            primary_key: false,
            auto_increment: false,
            default_value: None,
        }
    }

    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn primary_key(mut self, primary_key: bool) -> Self {
        self.primary_key = primary_key;
        if primary_key {
            self.nullable = false;
        }
        self
    }

    pub fn auto_increment(mut self, auto_increment: bool) -> Self {
        self.auto_increment = auto_increment;
        if auto_increment {
            self.nullable = false;
        }
        self
    }

    pub fn default_value(mut self, value: Value) -> Self {
        self.default_value = Some(value);
        self
    }

    pub fn validate_value(&self, value: &Value) -> bool {
        if value.is_null() {
            return self.nullable;
        }

        if !value.is_compatible_with(self.data_type) {
            return false;
        }

        match (&self.data_type, value) {
            (DataType::TinyInt, Value::Integer(value)) => i8::try_from(*value).is_ok(),
            (DataType::SmallInt, Value::Integer(value)) => i16::try_from(*value).is_ok(),
            (DataType::Char(length), Value::Text(text))
            | (DataType::VarChar(length), Value::Text(text)) => text.chars().count() <= *length as usize,
            (DataType::Decimal { precision, scale }, Value::Decimal(value))
            | (DataType::Numeric { precision, scale }, Value::Decimal(value)) => {
                value.fits_precision_scale(*precision, *scale)
            }
            _ => true,
        }
    }

    pub fn get_default_or_null(&self) -> Value {
        match &self.default_value {
            Some(value) => value.clone(),
            None => {
                if self.nullable {
                    Value::Null
                } else {
                    match &self.data_type {
                        DataType::TinyInt | DataType::SmallInt | DataType::Integer => {
                            Value::Integer(0)
                        }
                        DataType::BigInt => Value::BigInt(0),
                        DataType::Text | DataType::Char(_) | DataType::VarChar(_) => {
                            Value::Text(String::new())
                        }
                        DataType::Boolean => Value::Boolean(false),
                        DataType::Float | DataType::Real | DataType::Double => Value::Float(0.0),
                        DataType::Decimal { .. } | DataType::Numeric { .. } => {
                            Value::Decimal(DecimalValue::zero())
                        }
                        DataType::Blob => Value::Blob(Vec::new()),
                        DataType::Date => Value::Date(DateValue::epoch()),
                        DataType::DateTime => Value::DateTime(DateTimeValue::epoch()),
                    }
                }
            }
        }
    }

    pub fn size(&self) -> usize {
        self.data_type.size()
    }

    pub fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), HematiteError> {
        buffer.extend_from_slice(&self.id.as_u32().to_le_bytes());

        let name_bytes = self.name.as_bytes();
        buffer.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        buffer.extend_from_slice(name_bytes);

        write_data_type(buffer, &self.data_type);

        let mut flags = 0;
        if self.nullable {
            flags |= 0x01;
        }
        if self.primary_key {
            flags |= 0x02;
        }
        if self.auto_increment {
            flags |= 0x04;
        }
        buffer.push(flags);

        write_optional_value(buffer, self.default_value.as_ref());
        Ok(())
    }

    pub fn deserialize(buffer: &[u8], offset: &mut usize) -> Result<Self, HematiteError> {
        if *offset + 4 > buffer.len() {
            return Err(HematiteError::CorruptedData(
                "Invalid column data".to_string(),
            ));
        }

        let id = ColumnId::new(u32::from_le_bytes(
            buffer[*offset..*offset + 4]
                .try_into()
                .map_err(|_| HematiteError::CorruptedData("Invalid column id".to_string()))?,
        ));
        *offset += 4;

        let name_len = read_u32(buffer, offset, "column name length")? as usize;
        if *offset + name_len > buffer.len() {
            return Err(HematiteError::CorruptedData(
                "Invalid column name".to_string(),
            ));
        }
        let name = String::from_utf8(buffer[*offset..*offset + name_len].to_vec()).map_err(|_| {
            HematiteError::CorruptedData("Invalid UTF-8 in column name".to_string())
        })?;
        *offset += name_len;

        let data_type = read_data_type(buffer, offset)?;

        if *offset >= buffer.len() {
            return Err(HematiteError::CorruptedData(
                "Invalid column flags".to_string(),
            ));
        }
        let flags = buffer[*offset];
        *offset += 1;
        let nullable = (flags & 0x01) != 0;
        let primary_key = (flags & 0x02) != 0;
        let auto_increment = (flags & 0x04) != 0;

        let default_value = read_optional_value(buffer, offset)?;

        Ok(Self {
            id,
            name,
            data_type,
            nullable,
            primary_key,
            auto_increment,
            default_value,
        })
    }
}

fn write_data_type(buffer: &mut Vec<u8>, data_type: &DataType) {
    match data_type {
        DataType::TinyInt => buffer.push(0),
        DataType::SmallInt => buffer.push(1),
        DataType::Integer => buffer.push(2),
        DataType::BigInt => buffer.push(3),
        DataType::Text => buffer.push(4),
        DataType::Char(length) => {
            buffer.push(5);
            buffer.extend_from_slice(&length.to_le_bytes());
        }
        DataType::VarChar(length) => {
            buffer.push(6);
            buffer.extend_from_slice(&length.to_le_bytes());
        }
        DataType::Boolean => buffer.push(7),
        DataType::Float => buffer.push(8),
        DataType::Real => buffer.push(9),
        DataType::Double => buffer.push(10),
        DataType::Decimal { precision, scale } => {
            buffer.push(11);
            write_optional_u32(buffer, *precision);
            write_optional_u32(buffer, *scale);
        }
        DataType::Numeric { precision, scale } => {
            buffer.push(12);
            write_optional_u32(buffer, *precision);
            write_optional_u32(buffer, *scale);
        }
        DataType::Blob => buffer.push(13),
        DataType::Date => buffer.push(14),
        DataType::DateTime => buffer.push(15),
    }
}

fn read_data_type(buffer: &[u8], offset: &mut usize) -> Result<DataType, HematiteError> {
    if *offset >= buffer.len() {
        return Err(HematiteError::CorruptedData(
            "Invalid column data type".to_string(),
        ));
    }

    let tag = buffer[*offset];
    *offset += 1;

    Ok(match tag {
        0 => DataType::TinyInt,
        1 => DataType::SmallInt,
        2 => DataType::Integer,
        3 => DataType::BigInt,
        4 => DataType::Text,
        5 => DataType::Char(read_u32(buffer, offset, "CHAR length")?),
        6 => DataType::VarChar(read_u32(buffer, offset, "VARCHAR length")?),
        7 => DataType::Boolean,
        8 => DataType::Float,
        9 => DataType::Real,
        10 => DataType::Double,
        11 => DataType::Decimal {
            precision: read_optional_u32(buffer, offset, "DECIMAL precision")?,
            scale: read_optional_u32(buffer, offset, "DECIMAL scale")?,
        },
        12 => DataType::Numeric {
            precision: read_optional_u32(buffer, offset, "NUMERIC precision")?,
            scale: read_optional_u32(buffer, offset, "NUMERIC scale")?,
        },
        13 => DataType::Blob,
        14 => DataType::Date,
        15 => DataType::DateTime,
        _ => {
            return Err(HematiteError::CorruptedData(
                "Invalid data type".to_string(),
            ))
        }
    })
}

fn write_optional_u32(buffer: &mut Vec<u8>, value: Option<u32>) {
    buffer.extend_from_slice(&value.unwrap_or(u32::MAX).to_le_bytes());
}

fn read_optional_u32(
    buffer: &[u8],
    offset: &mut usize,
    label: &str,
) -> Result<Option<u32>, HematiteError> {
    let value = read_u32(buffer, offset, label)?;
    if value == u32::MAX {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

fn read_u32(buffer: &[u8], offset: &mut usize, label: &str) -> Result<u32, HematiteError> {
    if *offset + 4 > buffer.len() {
        return Err(HematiteError::CorruptedData(format!("Invalid {label}")));
    }
    let value = u32::from_le_bytes(
        buffer[*offset..*offset + 4]
            .try_into()
            .map_err(|_| HematiteError::CorruptedData(format!("Invalid {label}")))?,
    );
    *offset += 4;
    Ok(value)
}

fn write_optional_value(buffer: &mut Vec<u8>, value: Option<&Value>) {
    match value {
        None => buffer.push(255),
        Some(Value::Integer(value)) => {
            buffer.push(0);
            buffer.extend_from_slice(&value.to_le_bytes());
        }
        Some(Value::Text(value)) => {
            buffer.push(1);
            buffer.extend_from_slice(&(value.len() as u32).to_le_bytes());
            buffer.extend_from_slice(value.as_bytes());
        }
        Some(Value::Boolean(value)) => {
            buffer.push(2);
            buffer.push(u8::from(*value));
        }
        Some(Value::Float(value)) => {
            buffer.push(3);
            buffer.extend_from_slice(&value.to_le_bytes());
        }
        Some(Value::BigInt(value)) => {
            buffer.push(4);
            buffer.extend_from_slice(&value.to_le_bytes());
        }
        Some(Value::Decimal(value)) => {
            buffer.push(5);
            let text = value.to_string();
            buffer.extend_from_slice(&(text.len() as u32).to_le_bytes());
            buffer.extend_from_slice(text.as_bytes());
        }
        Some(Value::Blob(value)) => {
            buffer.push(6);
            buffer.extend_from_slice(&(value.len() as u32).to_le_bytes());
            buffer.extend_from_slice(value);
        }
        Some(Value::Date(value)) => {
            buffer.push(7);
            buffer.extend_from_slice(&value.days_since_epoch().to_le_bytes());
        }
        Some(Value::DateTime(value)) => {
            buffer.push(8);
            buffer.extend_from_slice(&value.seconds_since_epoch().to_le_bytes());
        }
        Some(Value::Null) => buffer.push(9),
    }
}

fn read_optional_value(buffer: &[u8], offset: &mut usize) -> Result<Option<Value>, HematiteError> {
    if *offset >= buffer.len() {
        return Err(HematiteError::CorruptedData(
            "Invalid default value".to_string(),
        ));
    }

    let tag = buffer[*offset];
    *offset += 1;
    Ok(match tag {
        0 => {
            let value = i32::from_le_bytes(
                read_fixed(buffer, offset, 4, "default integer")?
                    .try_into()
                    .unwrap(),
            );
            Some(Value::Integer(value))
        }
        1 => {
            let len = read_u32(buffer, offset, "default text length")? as usize;
            let bytes = read_fixed(buffer, offset, len, "default text")?;
            let text = String::from_utf8(bytes.to_vec()).map_err(|_| {
                HematiteError::CorruptedData("Invalid UTF-8 in default text".to_string())
            })?;
            Some(Value::Text(text))
        }
        2 => {
            let value = read_fixed(buffer, offset, 1, "default boolean")?[0] != 0;
            Some(Value::Boolean(value))
        }
        3 => {
            let value = f64::from_le_bytes(
                read_fixed(buffer, offset, 8, "default float")?
                    .try_into()
                    .unwrap(),
            );
            Some(Value::Float(value))
        }
        4 => {
            let value = i64::from_le_bytes(
                read_fixed(buffer, offset, 8, "default bigint")?
                    .try_into()
                    .unwrap(),
            );
            Some(Value::BigInt(value))
        }
        5 => {
            let len = read_u32(buffer, offset, "default decimal length")? as usize;
            let bytes = read_fixed(buffer, offset, len, "default decimal")?;
            let text = String::from_utf8(bytes.to_vec()).map_err(|_| {
                HematiteError::CorruptedData("Invalid UTF-8 in default decimal".to_string())
            })?;
            Some(Value::Decimal(DecimalValue::parse(&text).map_err(|_| {
                HematiteError::CorruptedData("Invalid default decimal".to_string())
            })?))
        }
        6 => {
            let len = read_u32(buffer, offset, "default blob length")? as usize;
            Some(Value::Blob(read_fixed(buffer, offset, len, "default blob")?.to_vec()))
        }
        7 => {
            let days = i32::from_le_bytes(
                read_fixed(buffer, offset, 4, "default date")?
                    .try_into()
                    .unwrap(),
            );
            Some(Value::Date(DateValue::from_days_since_epoch(days)))
        }
        8 => {
            let seconds = i64::from_le_bytes(
                read_fixed(buffer, offset, 8, "default datetime")?
                    .try_into()
                    .unwrap(),
            );
            Some(Value::DateTime(DateTimeValue::from_seconds_since_epoch(
                seconds,
            )))
        }
        9 => Some(Value::Null),
        255 => None,
        _ => {
            return Err(HematiteError::CorruptedData(
                "Invalid default value type".to_string(),
            ))
        }
    })
}

fn read_fixed<'a>(
    buffer: &'a [u8],
    offset: &mut usize,
    len: usize,
    label: &str,
) -> Result<&'a [u8], HematiteError> {
    if *offset + len > buffer.len() {
        return Err(HematiteError::CorruptedData(format!("Invalid {label}")));
    }
    let slice = &buffer[*offset..*offset + len];
    *offset += len;
    Ok(slice)
}
