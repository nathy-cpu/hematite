//! Column definitions for database tables

use super::types::{DataType, Value};
use super::ColumnId;

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
        // Check for NULL if not nullable
        if value.is_null() && !self.nullable {
            return false;
        }

        // Check type compatibility
        value.is_compatible_with(self.data_type)
    }

    pub fn get_default_or_null(&self) -> Value {
        match &self.default_value {
            Some(value) => value.clone(),
            None => {
                if self.nullable {
                    Value::Null
                } else {
                    // Provide default values for non-nullable columns
                    match self.data_type {
                        DataType::Integer => Value::Integer(0),
                        DataType::Text => Value::Text(String::new()),
                        DataType::Boolean => Value::Boolean(false),
                        DataType::Float => Value::Float(0.0),
                    }
                }
            }
        }
    }

    pub fn size(&self) -> usize {
        self.data_type.size()
    }

    pub fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), crate::error::HematiteError> {
        // Column ID (4 bytes)
        buffer.extend_from_slice(&self.id.as_u32().to_le_bytes());

        // Name length (4 bytes) + name
        let name_bytes = self.name.as_bytes();
        buffer.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        buffer.extend_from_slice(name_bytes);

        // Data type (1 byte)
        buffer.push(match self.data_type {
            DataType::Integer => 0,
            DataType::Text => 1,
            DataType::Boolean => 2,
            DataType::Float => 3,
        });

        // Flags (1 byte): bit 0 = nullable, bit 1 = primary_key, bit 2 = auto_increment
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

        // Default value (1 byte type + value)
        match &self.default_value {
            Some(value) => match value {
                Value::Integer(i) => {
                    buffer.push(0);
                    buffer.extend_from_slice(&i.to_le_bytes());
                }
                Value::Text(s) => {
                    buffer.push(1);
                    let bytes = s.as_bytes();
                    buffer.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    buffer.extend_from_slice(bytes);
                }
                Value::Boolean(b) => {
                    buffer.push(2);
                    buffer.push(*b as u8);
                }
                Value::Float(f) => {
                    buffer.push(3);
                    buffer.extend_from_slice(&f.to_le_bytes());
                }
                Value::Null => {
                    buffer.push(4);
                }
            },
            None => {
                buffer.push(255); // No default value
            }
        }

        Ok(())
    }

    pub fn deserialize(
        buffer: &[u8],
        offset: &mut usize,
    ) -> Result<Self, crate::error::HematiteError> {
        if *offset + 4 > buffer.len() {
            return Err(crate::error::HematiteError::CorruptedData(
                "Invalid column data".to_string(),
            ));
        }

        // Column ID
        let id = ColumnId::new(u32::from_le_bytes([
            buffer[*offset],
            buffer[*offset + 1],
            buffer[*offset + 2],
            buffer[*offset + 3],
        ]));
        *offset += 4;

        // Name
        if *offset + 4 > buffer.len() {
            return Err(crate::error::HematiteError::CorruptedData(
                "Invalid column name length".to_string(),
            ));
        }
        let name_len = u32::from_le_bytes([
            buffer[*offset],
            buffer[*offset + 1],
            buffer[*offset + 2],
            buffer[*offset + 3],
        ]) as usize;
        *offset += 4;

        if *offset + name_len > buffer.len() {
            return Err(crate::error::HematiteError::CorruptedData(
                "Invalid column name".to_string(),
            ));
        }
        let name =
            String::from_utf8(buffer[*offset..*offset + name_len].to_vec()).map_err(|_| {
                crate::error::HematiteError::CorruptedData(
                    "Invalid UTF-8 in column name".to_string(),
                )
            })?;
        *offset += name_len;

        // Data type
        if *offset >= buffer.len() {
            return Err(crate::error::HematiteError::CorruptedData(
                "Invalid column data type".to_string(),
            ));
        }
        let data_type = match buffer[*offset] {
            0 => DataType::Integer,
            1 => DataType::Text,
            2 => DataType::Boolean,
            3 => DataType::Float,
            _ => {
                return Err(crate::error::HematiteError::CorruptedData(
                    "Invalid data type".to_string(),
                ))
            }
        };
        *offset += 1;

        // Flags
        if *offset >= buffer.len() {
            return Err(crate::error::HematiteError::CorruptedData(
                "Invalid column flags".to_string(),
            ));
        }
        let flags = buffer[*offset];
        *offset += 1;
        let nullable = (flags & 0x01) != 0;
        let primary_key = (flags & 0x02) != 0;
        let auto_increment = (flags & 0x04) != 0;

        // Default value
        if *offset >= buffer.len() {
            return Err(crate::error::HematiteError::CorruptedData(
                "Invalid default value".to_string(),
            ));
        }
        let default_value = match buffer[*offset] {
            0 => {
                *offset += 1;
                if *offset + 4 > buffer.len() {
                    return Err(crate::error::HematiteError::CorruptedData(
                        "Invalid default integer".to_string(),
                    ));
                }
                let val = i32::from_le_bytes([
                    buffer[*offset],
                    buffer[*offset + 1],
                    buffer[*offset + 2],
                    buffer[*offset + 3],
                ]);
                *offset += 4;
                Some(Value::Integer(val))
            }
            1 => {
                *offset += 1;
                if *offset + 4 > buffer.len() {
                    return Err(crate::error::HematiteError::CorruptedData(
                        "Invalid default text length".to_string(),
                    ));
                }
                let text_len = u32::from_le_bytes([
                    buffer[*offset],
                    buffer[*offset + 1],
                    buffer[*offset + 2],
                    buffer[*offset + 3],
                ]) as usize;
                *offset += 4;

                if *offset + text_len > buffer.len() {
                    return Err(crate::error::HematiteError::CorruptedData(
                        "Invalid default text".to_string(),
                    ));
                }
                let text = String::from_utf8(buffer[*offset..*offset + text_len].to_vec())
                    .map_err(|_| {
                        crate::error::HematiteError::CorruptedData(
                            "Invalid UTF-8 in default text".to_string(),
                        )
                    })?;
                *offset += text_len;
                Some(Value::Text(text))
            }
            2 => {
                *offset += 1;
                if *offset >= buffer.len() {
                    return Err(crate::error::HematiteError::CorruptedData(
                        "Invalid default boolean".to_string(),
                    ));
                }
                let val = buffer[*offset] != 0;
                *offset += 1;
                Some(Value::Boolean(val))
            }
            3 => {
                *offset += 1;
                if *offset + 8 > buffer.len() {
                    return Err(crate::error::HematiteError::CorruptedData(
                        "Invalid default float".to_string(),
                    ));
                }
                let val = f64::from_le_bytes([
                    buffer[*offset],
                    buffer[*offset + 1],
                    buffer[*offset + 2],
                    buffer[*offset + 3],
                    buffer[*offset + 4],
                    buffer[*offset + 5],
                    buffer[*offset + 6],
                    buffer[*offset + 7],
                ]);
                *offset += 8;
                Some(Value::Float(val))
            }
            4 => {
                *offset += 1;
                Some(Value::Null)
            }
            255 => {
                *offset += 1;
                None
            }
            _ => {
                return Err(crate::error::HematiteError::CorruptedData(
                    "Invalid default value type".to_string(),
                ))
            }
        };

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
