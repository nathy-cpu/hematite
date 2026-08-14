//! Relational row and index-key encoding.
//!
//! The formats themselves live in [`crate::catalog::record_v4`] and
//! [`crate::catalog::index_key_v4`]; this module is the naming layer the rest of the
//! catalog calls through.

use crate::catalog::{index_key_v4, record_v4, Value};
use crate::error::{HematiteError, Result};

use super::record::StoredRow;

pub struct RowCodec;

impl RowCodec {
    pub fn encode_values(values: &[Value]) -> Result<Vec<u8>> {
        Ok(record_v4::encode(values))
    }

    pub fn encode_stored_row(row: &StoredRow) -> Result<Vec<u8>> {
        Ok(record_v4::encode(&row.values))
    }

    pub fn decode_values(data: &[u8]) -> Result<Vec<Value>> {
        record_v4::decode(data)
    }

    /// Encodes a `u64` row ID into a lexicographically sortable variable-length byte array.
    pub fn encode_row_id_key(row_id: u64) -> Vec<u8> {
        let bytes_needed = match row_id {
            0..=0xFF => 1,
            0x100..=0xFFFF => 2,
            0x10000..=0xFFFFFF => 3,
            0x1000000..=0xFFFFFFFF => 4,
            0x100000000..=0xFFFFFFFFFF => 5,
            0x10000000000..=0xFFFFFFFFFFFF => 6,
            0x1000000000000..=0xFFFFFFFFFFFFFF => 7,
            _ => 8,
        };

        let mut key = Vec::with_capacity(1 + bytes_needed);
        key.push(bytes_needed as u8);
        let be_bytes = row_id.to_be_bytes();
        key.extend_from_slice(&be_bytes[8 - bytes_needed..]);
        key
    }

    /// Decodes a `u64` row ID from the prefix-length variable-length byte array.
    pub fn decode_row_id_key(key: &[u8]) -> Result<u64> {
        if key.is_empty() {
            return Err(HematiteError::CorruptedData("Empty rowid key".to_string()));
        }
        let bytes_needed = key[0] as usize;
        if !(1..=8).contains(&bytes_needed) {
            return Err(HematiteError::CorruptedData(format!(
                "Invalid rowid key prefix length: {}",
                bytes_needed
            )));
        }
        if key.len() != 1 + bytes_needed {
            return Err(HematiteError::CorruptedData(format!(
                "Rowid key length mismatch: prefix specifies {} bytes but key length is {}",
                bytes_needed,
                key.len()
            )));
        }
        let mut be_bytes = [0u8; 8];
        be_bytes[8 - bytes_needed..].copy_from_slice(&key[1..]);
        Ok(u64::from_be_bytes(be_bytes))
    }

    pub fn decode_stored_row(row_id: u64, data: &[u8]) -> Result<StoredRow> {
        Ok(StoredRow {
            row_id,
            values: record_v4::decode(data)?,
        })
    }
}

pub struct IndexKeyCodec;

impl IndexKeyCodec {
    pub fn encode_key(values: &[Value]) -> Result<Vec<u8>> {
        Ok(index_key_v4::encode_key(values))
    }

    pub fn encode_secondary_key(values: &[Value], row_id: u64) -> Result<Vec<u8>> {
        Ok(index_key_v4::encode_secondary_key(values, row_id))
    }

    /// The payload stored against a primary-key index entry.
    ///
    /// A varint, so a small row id costs one byte where it used to cost eight.
    pub fn encode_row_id_value(row_id: u64) -> Vec<u8> {
        index_key_v4::encode_row_id_value(row_id)
    }

    pub fn decode_row_id(value: &[u8]) -> Result<u64> {
        index_key_v4::decode_row_id_value(value)
    }

    /// Splits a secondary-index key into its value prefix and trailing row id.
    ///
    /// Borrows the prefix — secondary entries carry no payload at all now, so this
    /// is the only way to recover the row id, and it runs on every entry of a scan.
    pub fn split_secondary_key(key: &[u8]) -> Result<(&[u8], u64)> {
        index_key_v4::split_secondary_key(key)
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

    pub fn deserialize_stored_row(row_id: u64, data: &[u8]) -> Result<StoredRow> {
        RowCodec::decode_stored_row(row_id, data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_id_key_codec_roundtrip() -> Result<()> {
        let test_cases = vec![
            0,
            1,
            127,
            128,
            255,
            256,
            65535,
            65536,
            16777215,
            16777216,
            4294967295,
            4294967296,
            u64::MAX,
        ];

        for row_id in test_cases {
            let encoded = RowCodec::encode_row_id_key(row_id);
            assert_eq!(encoded[0] as usize + 1, encoded.len());
            let decoded = RowCodec::decode_row_id_key(&encoded)?;
            assert_eq!(row_id, decoded);
        }
        Ok(())
    }

    #[test]
    fn test_row_id_key_codec_sorting() {
        let test_cases = vec![
            0,
            1,
            50,
            127,
            128,
            255,
            256,
            500,
            65535,
            65536,
            16777215,
            16777216,
            4294967295,
            4294967296,
            u64::MAX - 1,
            u64::MAX,
        ];

        for i in 0..test_cases.len() - 1 {
            let a = test_cases[i];
            let b = test_cases[i + 1];
            assert!(a < b);

            let encoded_a = RowCodec::encode_row_id_key(a);
            let encoded_b = RowCodec::encode_row_id_key(b);
            assert!(encoded_a < encoded_b, "lexicographical sort order violation: key({a}) >= key({b})\n  key({a}) = {:?}\n  key({b}) = {:?}", encoded_a, encoded_b);
        }
    }

    #[test]
    fn rows_roundtrip_through_the_facade() {
        let row = StoredRow {
            row_id: 42,
            values: vec![
                Value::Integer(7),
                Value::Text("hello".to_string()),
                Value::Null,
            ],
        };
        let encoded = RowCodec::encode_stored_row(&row).expect("encode");
        let decoded = RowCodec::decode_stored_row(row.row_id, &encoded).expect("decode");
        assert_eq!(decoded.row_id, row.row_id);
        assert_eq!(decoded.values, row.values);
    }

    /// The old row format carried a four-byte little-endian length prefix, and
    /// `decode_values` re-framed its input to add one when it was missing. The new
    /// format has no such prefix — the cell already knows how long its payload is —
    /// so decoding is a straight parse of exactly the bytes handed over.
    #[test]
    fn decoding_does_not_reframe_its_input() {
        let values = vec![Value::Integer(1), Value::Text("two".to_string())];
        let encoded = RowCodec::encode_values(&values).expect("encode");
        assert_eq!(RowCodec::decode_values(&encoded).expect("decode"), values);

        // Trailing bytes are not silently accepted as part of the record.
        let mut padded = encoded.clone();
        padded.push(0xFF);
        assert!(RowCodec::decode_values(&padded).is_err());
    }
}
