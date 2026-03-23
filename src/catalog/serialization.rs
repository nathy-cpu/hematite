//! Relational row encoding.

use crate::catalog::Value;
use crate::error::{HematiteError, Result};

use super::record::StoredRow;

pub struct RowCodec;

impl RowCodec {
    pub fn encode_values(values: &[Value]) -> Result<Vec<u8>> {
        Self::encode_stored_row(&StoredRow {
            row_id: 0,
            values: values.to_vec(),
        })
    }

    pub fn encode_stored_row(row: &StoredRow) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&(0u32).to_le_bytes());
        buffer.extend_from_slice(&row.row_id.to_le_bytes());
        buffer.extend_from_slice(&(row.values.len() as u32).to_le_bytes());

        for value in &row.values {
            match value {
                Value::Integer(i) => {
                    buffer.push(1);
                    buffer.extend_from_slice(&i.to_le_bytes());
                }
                Value::Text(s) => {
                    buffer.push(2);
                    let bytes = s.as_bytes();
                    buffer.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    buffer.extend_from_slice(bytes);
                }
                Value::Boolean(b) => {
                    buffer.push(3);
                    buffer.push(u8::from(*b));
                }
                Value::Float(f) => {
                    buffer.push(4);
                    buffer.extend_from_slice(&f.to_le_bytes());
                }
                Value::Null => buffer.push(5),
            }
        }

        let payload_len = buffer.len() - 4;
        buffer[0..4].copy_from_slice(&(payload_len as u32).to_le_bytes());
        Ok(buffer)
    }

    pub fn decode_values(data: &[u8]) -> Result<Vec<Value>> {
        let encoded = if data.len() >= 4 {
            let payload_len = Self::read_payload_length(&data[0..4])?;
            if payload_len + 4 == data.len() {
                data.to_vec()
            } else {
                let mut encoded = Vec::with_capacity(data.len() + 4);
                encoded.extend_from_slice(&(data.len() as u32).to_le_bytes());
                encoded.extend_from_slice(data);
                encoded
            }
        } else {
            let mut encoded = Vec::with_capacity(data.len() + 4);
            encoded.extend_from_slice(&(data.len() as u32).to_le_bytes());
            encoded.extend_from_slice(data);
            encoded
        };

        Ok(Self::decode_stored_row(&encoded)?.values)
    }

    pub fn decode_stored_row(data: &[u8]) -> Result<StoredRow> {
        if data.len() < 12 {
            return Err(HematiteError::CorruptedData(
                "Stored row header is truncated".to_string(),
            ));
        }

        let mut offset = 0usize;
        let payload_len = Self::read_payload_length(&data[0..4])?;
        offset += 4;

        if payload_len + 4 > data.len() {
            return Err(HematiteError::CorruptedData(
                "Stored row length exceeds available bytes".to_string(),
            ));
        }

        let row_id = u64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);
        offset += 8;

        let value_count = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        let payload_end = payload_len + 4;
        let mut values = Vec::with_capacity(value_count);

        for _ in 0..value_count {
            if offset >= payload_end {
                return Err(HematiteError::CorruptedData(
                    "Stored row ended before all values were decoded".to_string(),
                ));
            }

            let tag = data[offset];
            offset += 1;

            let value = match tag {
                1 => {
                    if offset + 4 > payload_end {
                        return Err(HematiteError::CorruptedData(
                            "Integer value is truncated".to_string(),
                        ));
                    }
                    let value = i32::from_le_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ]);
                    offset += 4;
                    Value::Integer(value)
                }
                2 => {
                    if offset + 4 > payload_end {
                        return Err(HematiteError::CorruptedData(
                            "Text length is truncated".to_string(),
                        ));
                    }
                    let len = u32::from_le_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ]) as usize;
                    offset += 4;
                    if offset + len > payload_end {
                        return Err(HematiteError::CorruptedData(
                            "Text value is truncated".to_string(),
                        ));
                    }
                    let value =
                        String::from_utf8(data[offset..offset + len].to_vec()).map_err(|_| {
                            HematiteError::CorruptedData("Invalid UTF-8 in text value".to_string())
                        })?;
                    offset += len;
                    Value::Text(value)
                }
                3 => {
                    if offset >= payload_end {
                        return Err(HematiteError::CorruptedData(
                            "Boolean value is truncated".to_string(),
                        ));
                    }
                    let value = data[offset] != 0;
                    offset += 1;
                    Value::Boolean(value)
                }
                4 => {
                    if offset + 8 > payload_end {
                        return Err(HematiteError::CorruptedData(
                            "Float value is truncated".to_string(),
                        ));
                    }
                    let value = f64::from_le_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                        data[offset + 4],
                        data[offset + 5],
                        data[offset + 6],
                        data[offset + 7],
                    ]);
                    offset += 8;
                    Value::Float(value)
                }
                5 => Value::Null,
                _ => {
                    return Err(HematiteError::CorruptedData(format!(
                        "Unknown value tag {} in stored row",
                        tag
                    )))
                }
            };

            values.push(value);
        }

        Ok(StoredRow { row_id, values })
    }

    pub fn read_payload_length(prefix: &[u8]) -> Result<usize> {
        if prefix.len() != 4 {
            return Err(HematiteError::CorruptedData(
                "Row length prefix must be 4 bytes".to_string(),
            ));
        }

        Ok(u32::from_le_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]) as usize)
    }
}

pub struct IndexKeyCodec;

impl IndexKeyCodec {
    pub fn encode_key(values: &[Value]) -> Result<Vec<u8>> {
        RowCodec::encode_values(values)
    }

    pub fn encode_secondary_key(values: &[Value], row_id: u64) -> Result<Vec<u8>> {
        let mut key = Self::encode_key(values)?;
        key.extend_from_slice(&row_id.to_be_bytes());
        Ok(key)
    }

    pub fn decode_row_id(value: &[u8]) -> Result<u64> {
        if value.len() != 8 {
            return Err(HematiteError::CorruptedData(
                "Index rowid payload must be exactly 8 bytes".to_string(),
            ));
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(value);
        Ok(u64::from_be_bytes(bytes))
    }

    pub fn split_secondary_key(key: &[u8]) -> Result<(Vec<u8>, u64)> {
        if key.len() < 8 {
            return Err(HematiteError::CorruptedData(
                "Index entry is missing rowid bytes".to_string(),
            ));
        }
        let mut row_id_bytes = [0u8; 8];
        row_id_bytes.copy_from_slice(&key[key.len() - 8..]);
        let row_id = u64::from_be_bytes(row_id_bytes);
        Ok((key[..key.len() - 8].to_vec(), row_id))
    }
}

pub struct RowSerializer;

impl RowSerializer {
    pub fn serialize(values: &[Value]) -> Result<Vec<u8>> {
        RowCodec::encode_values(values)
    }

    pub fn serialize_stored_row(row: &StoredRow) -> Result<Vec<u8>> {
        RowCodec::encode_stored_row(row)
    }

    pub fn deserialize(data: &[u8]) -> Result<Vec<Value>> {
        RowCodec::decode_values(data)
    }

    pub fn deserialize_stored_row(data: &[u8]) -> Result<StoredRow> {
        RowCodec::decode_stored_row(data)
    }

    pub fn read_row_length(prefix: &[u8]) -> Result<usize> {
        RowCodec::read_payload_length(prefix)
    }
}
