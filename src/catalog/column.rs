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
            default_value: None,
        }
    }

    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn primary_key(mut self, primary_key: bool) -> Self {
        self.primary_key = primary_key;
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

        // Flags (1 byte): bit 0 = nullable, bit 1 = primary_key
        let mut flags = 0;
        if self.nullable {
            flags |= 0x01;
        }
        if self.primary_key {
            flags |= 0x02;
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
            default_value,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::types::{DataType, Value};

    #[test]
    fn test_column_creation() {
        let column = Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer);

        assert_eq!(column.id.as_u32(), 1);
        assert_eq!(column.name, "id");
        assert_eq!(column.data_type, DataType::Integer);
        assert!(column.nullable);
        assert!(!column.primary_key);
        assert!(column.default_value.is_none());
    }

    #[test]
    fn test_column_builder() {
        let column = Column::new(ColumnId::new(1), "name".to_string(), DataType::Text)
            .nullable(false)
            .primary_key(true)
            .default_value(Value::Text("default".to_string()));

        assert!(!column.nullable);
        assert!(column.primary_key);
        assert_eq!(
            column.default_value,
            Some(Value::Text("default".to_string()))
        );
    }

    #[test]
    fn test_column_validation() {
        // Test valid values
        let int_column = Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer);
        assert!(int_column.validate_value(&Value::Integer(42)));
        assert!(int_column.validate_value(&Value::Null)); // NULL is allowed by default

        let non_null_int_column =
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).nullable(false);
        assert!(non_null_int_column.validate_value(&Value::Integer(42)));
        assert!(!non_null_int_column.validate_value(&Value::Null)); // NULL not allowed

        // Test type compatibility
        assert!(!int_column.validate_value(&Value::Text("not an integer".to_string())));
        assert!(!int_column.validate_value(&Value::Boolean(true)));
        assert!(!int_column.validate_value(&Value::Float(3.14)));

        let text_column = Column::new(ColumnId::new(2), "name".to_string(), DataType::Text);
        assert!(text_column.validate_value(&Value::Text("hello".to_string())));
        assert!(!text_column.validate_value(&Value::Integer(42)));
    }

    #[test]
    fn test_column_default_values() {
        // Column with explicit default
        let column_with_default =
            Column::new(ColumnId::new(1), "status".to_string(), DataType::Text)
                .default_value(Value::Text("active".to_string()));
        assert_eq!(
            column_with_default.get_default_or_null(),
            Value::Text("active".to_string())
        );

        // Nullable column without default
        let nullable_column =
            Column::new(ColumnId::new(2), "description".to_string(), DataType::Text).nullable(true);
        assert_eq!(nullable_column.get_default_or_null(), Value::Null);

        // Non-nullable column without default (should get type default)
        let non_null_int_column =
            Column::new(ColumnId::new(3), "count".to_string(), DataType::Integer).nullable(false);
        assert_eq!(non_null_int_column.get_default_or_null(), Value::Integer(0));

        let non_null_text_column =
            Column::new(ColumnId::new(4), "name".to_string(), DataType::Text).nullable(false);
        assert_eq!(
            non_null_text_column.get_default_or_null(),
            Value::Text(String::new())
        );

        let non_null_bool_column =
            Column::new(ColumnId::new(5), "active".to_string(), DataType::Boolean).nullable(false);
        assert_eq!(
            non_null_bool_column.get_default_or_null(),
            Value::Boolean(false)
        );

        let non_null_float_column =
            Column::new(ColumnId::new(6), "price".to_string(), DataType::Float).nullable(false);
        assert_eq!(
            non_null_float_column.get_default_or_null(),
            Value::Float(0.0)
        );
    }

    #[test]
    fn test_column_size() {
        let int_column = Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer);
        assert_eq!(int_column.size(), 4);

        let text_column = Column::new(ColumnId::new(2), "name".to_string(), DataType::Text);
        assert_eq!(text_column.size(), 255);

        let bool_column = Column::new(ColumnId::new(3), "active".to_string(), DataType::Boolean);
        assert_eq!(bool_column.size(), 1);

        let float_column = Column::new(ColumnId::new(4), "price".to_string(), DataType::Float);
        assert_eq!(float_column.size(), 8);
    }

    #[test]
    fn test_column_serialization_roundtrip() -> Result<(), crate::error::HematiteError> {
        let original = Column::new(
            ColumnId::new(42),
            "test_column".to_string(),
            DataType::Integer,
        )
        .nullable(false)
        .primary_key(true)
        .default_value(Value::Integer(123));

        let mut buffer = Vec::new();
        original.serialize(&mut buffer)?;

        let mut offset = 0;
        let deserialized = Column::deserialize(&buffer, &mut offset)?;

        assert_eq!(original.id, deserialized.id);
        assert_eq!(original.name, deserialized.name);
        assert_eq!(original.data_type, deserialized.data_type);
        assert_eq!(original.nullable, deserialized.nullable);
        assert_eq!(original.primary_key, deserialized.primary_key);
        assert_eq!(original.default_value, deserialized.default_value);

        Ok(())
    }

    #[test]
    fn test_column_serialization_no_default() -> Result<(), crate::error::HematiteError> {
        let original = Column::new(ColumnId::new(1), "simple".to_string(), DataType::Boolean);

        let mut buffer = Vec::new();
        original.serialize(&mut buffer)?;

        let mut offset = 0;
        let deserialized = Column::deserialize(&buffer, &mut offset)?;

        assert_eq!(original.default_value, deserialized.default_value);
        assert!(deserialized.default_value.is_none());

        Ok(())
    }

    #[test]
    fn test_column_serialization_text_default() -> Result<(), crate::error::HematiteError> {
        let original = Column::new(ColumnId::new(1), "message".to_string(), DataType::Text)
            .default_value(Value::Text("hello world".to_string()));

        let mut buffer = Vec::new();
        original.serialize(&mut buffer)?;

        let mut offset = 0;
        let deserialized = Column::deserialize(&buffer, &mut offset)?;

        assert_eq!(
            deserialized.default_value,
            Some(Value::Text("hello world".to_string()))
        );

        Ok(())
    }

    #[test]
    fn test_column_serialization_null_default() -> Result<(), crate::error::HematiteError> {
        let original = Column::new(ColumnId::new(1), "optional".to_string(), DataType::Integer)
            .default_value(Value::Null);

        let mut buffer = Vec::new();
        original.serialize(&mut buffer)?;

        let mut offset = 0;
        let deserialized = Column::deserialize(&buffer, &mut offset)?;

        assert_eq!(deserialized.default_value, Some(Value::Null));

        Ok(())
    }

    #[test]
    fn test_column_deserialization_errors() {
        let buffer = vec![]; // Empty buffer
        let mut offset = 0;
        assert!(Column::deserialize(&buffer, &mut offset).is_err());

        let buffer = vec![1, 2, 3]; // Too short for column ID
        let mut offset = 0;
        assert!(Column::deserialize(&buffer, &mut offset).is_err());
    }

    #[test]
    fn test_column_clone() {
        let original = Column::new(ColumnId::new(1), "test".to_string(), DataType::Text)
            .nullable(false)
            .primary_key(true)
            .default_value(Value::Text("default".to_string()));

        let cloned = original.clone();
        assert_eq!(original.id, cloned.id);
        assert_eq!(original.name, cloned.name);
        assert_eq!(original.data_type, cloned.data_type);
        assert_eq!(original.nullable, cloned.nullable);
        assert_eq!(original.primary_key, cloned.primary_key);
        assert_eq!(original.default_value, cloned.default_value);
    }

    #[test]
    fn test_column_debug() {
        let column = Column::new(ColumnId::new(1), "test".to_string(), DataType::Integer);
        let debug_str = format!("{:?}", column);
        assert!(debug_str.contains("Column"));
        assert!(debug_str.contains("test"));
    }
}
