//! Row serialization and deserialization utilities

use crate::catalog::Value;
use crate::error::{HematiteError, Result};
use crate::storage::StoredRow;

pub struct RowSerializer;

impl RowSerializer {
    pub fn serialize(row: &[Value]) -> Result<Vec<u8>> {
        Self::serialize_stored_row(&StoredRow {
            row_id: 0,
            values: row.to_vec(),
        })
    }

    pub fn serialize_stored_row(row: &StoredRow) -> Result<Vec<u8>> {
        let mut data = Vec::new();

        // Write row length (placeholder, will be updated)
        data.extend_from_slice(&[0u8; 4]);
        data.extend_from_slice(&row.row_id.to_le_bytes());

        // Serialize each value
        for value in &row.values {
            match value {
                Value::Integer(i) => {
                    data.push(1); // Type marker for Integer
                    data.extend_from_slice(&i.to_le_bytes());
                }
                Value::Text(s) => {
                    data.push(2); // Type marker for Text
                    let bytes = s.as_bytes();
                    data.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    data.extend_from_slice(bytes);
                }
                Value::Boolean(b) => {
                    data.push(3); // Type marker for Boolean
                    data.push(*b as u8);
                }
                Value::Float(f) => {
                    data.push(4); // Type marker for Float
                    data.extend_from_slice(&f.to_le_bytes());
                }
                Value::Null => {
                    data.push(5); // Type marker for Null
                }
            }
        }

        // Update row length
        let row_length = (data.len() - 4) as u32;
        data[0..4].copy_from_slice(&row_length.to_le_bytes());

        Ok(data)
    }

    pub fn deserialize(data: &[u8]) -> Result<Vec<Value>> {
        Ok(Self::deserialize_stored_row(data)?.values)
    }

    pub fn deserialize_stored_row(data: &[u8]) -> Result<StoredRow> {
        if data.len() < 8 {
            return Err(HematiteError::CorruptedData("Truncated row id".to_string()));
        }

        let mut row_id_bytes = [0u8; 8];
        row_id_bytes.copy_from_slice(&data[0..8]);
        let row_id = u64::from_le_bytes(row_id_bytes);

        let mut values = Vec::new();
        let mut offset = 8;

        while offset < data.len() {
            if offset >= data.len() {
                break;
            }
            let type_marker = data[offset];
            offset += 1;

            let value = match type_marker {
                1 => {
                    // Integer
                    if offset + 4 > data.len() {
                        return Err(HematiteError::CorruptedData(
                            "Truncated INTEGER value".to_string(),
                        ));
                    }
                    let bytes = [
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ];
                    offset += 4;
                    Value::Integer(i32::from_le_bytes(bytes))
                }
                2 => {
                    // Text
                    if offset + 4 > data.len() {
                        return Err(HematiteError::CorruptedData(
                            "Truncated TEXT length".to_string(),
                        ));
                    }
                    let len_bytes = [
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ];
                    offset += 4;
                    let len = u32::from_le_bytes(len_bytes) as usize;
                    if offset + len > data.len() {
                        return Err(HematiteError::CorruptedData(
                            "Truncated TEXT payload".to_string(),
                        ));
                    }
                    let text = String::from_utf8(data[offset..offset + len].to_vec())
                        .map_err(|_| HematiteError::StorageError("Invalid UTF-8".to_string()))?;
                    offset += len;
                    Value::Text(text)
                }
                3 => {
                    // Boolean
                    if offset + 1 > data.len() {
                        return Err(HematiteError::CorruptedData(
                            "Truncated BOOLEAN value".to_string(),
                        ));
                    }
                    let b = data[offset] != 0;
                    offset += 1;
                    Value::Boolean(b)
                }
                4 => {
                    // Float
                    if offset + 8 > data.len() {
                        return Err(HematiteError::CorruptedData(
                            "Truncated FLOAT value".to_string(),
                        ));
                    }
                    let bytes = [
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                        data[offset + 4],
                        data[offset + 5],
                        data[offset + 6],
                        data[offset + 7],
                    ];
                    offset += 8;
                    Value::Float(f64::from_le_bytes(bytes))
                }
                5 => {
                    // Null
                    Value::Null
                }
                _ => {
                    return Err(HematiteError::StorageError(
                        "Invalid value type".to_string(),
                    ))
                }
            };

            values.push(value);
        }

        Ok(StoredRow { row_id, values })
    }

    pub fn read_row_length(data: &[u8]) -> Result<usize> {
        if data.len() < 4 {
            return Err(HematiteError::StorageError("Invalid row data".to_string()));
        }
        Ok(u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize)
    }
}
