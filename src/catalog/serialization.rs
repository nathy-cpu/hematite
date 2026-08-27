//! Relational row and index-key encoding.
//!
//! The formats themselves live in [`crate::catalog::record_v4`] and
//! [`crate::catalog::index_key_v4`]; this module is the naming layer the rest of the
//! catalog calls through.

use crate::catalog::{index_key_v4, record_v4, Value};
use crate::error::{HematiteError, Result};

use super::record::StoredRow;

pub struct RowCodec;

/// Bytes needed to hold `value`, at least one.
fn significant_byte_width(value: u64) -> usize {
    match value {
        0..=0xFF => 1,
        0x100..=0xFFFF => 2,
        0x10000..=0xFFFFFF => 3,
        0x1000000..=0xFFFF_FFFF => 4,
        0x1_0000_0000..=0xFF_FFFF_FFFF => 5,
        0x100_0000_0000..=0xFFFF_FFFF_FFFF => 6,
        0x1_0000_0000_0000..=0xFF_FFFF_FFFF_FFFF => 7,
        _ => 8,
    }
}

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

    /// The row id a table keyed by a single integer column stores a row under.
    ///
    /// The key *is* the row id. Not a mapping of it — an `AUTO_INCREMENT` primary
    /// key is already filled from the row-id counter, so the two have always been
    /// the same number for the commonest case, and keeping them the same is what
    /// lets `RowIdLookup` and `PrimaryKeyLookup` reach the same row.
    ///
    /// A negative key is stored where its value belongs: [`Self::encode_row_id_key`]
    /// sorts row ids by their signed value, so a table keyed this way is physically
    /// ordered by its primary key across the whole range.
    ///
    /// `None` for anything that is not an integer, or a `u64` past what `i64`
    /// holds — [`Table::rowid_alias_column`] refuses those tables, and this is the
    /// second half of the same rule.
    ///
    /// [`Table::rowid_alias_column`]: crate::catalog::Table::rowid_alias_column
    pub fn rowid_for_primary_key(value: &Value) -> Option<u64> {
        let signed: i64 = match value {
            Value::Integer(v) => i64::from(*v),
            Value::BigInt(v) => *v,
            Value::UInteger(v) => i64::from(*v),
            Value::UBigInt(v) => i64::try_from(*v).ok()?,
            _ => return None,
        };
        Some(signed as u64)
    }

    /// The primary-key integer a row id was derived from.
    pub fn primary_key_for_rowid(row_id: u64) -> i64 {
        row_id as i64
    }

    /// Encodes a row id into bytes that sort in row-id order.
    ///
    /// Row ids are held as `u64` but their domain is `i64`: a table whose primary
    /// key is a single integer column stores each row under that key, and SQL
    /// integers are signed. So the ordering this has to preserve is signed, while
    /// byte-string comparison is not.
    ///
    /// A leading tag carries both the sign and the width, which is what lets the
    /// two coexist. Tags `0x81..=0x88` mark a non-negative id of 1..8 bytes and
    /// `0x78..=0x7F` a negative one of 8..1 bytes, so every negative tag sorts
    /// below every non-negative one, a wider magnitude sorts below a narrower one
    /// among negatives, and the body settles ties within a width in both cases.
    ///
    /// Compactness survives it: a small id is still two bytes, and a small
    /// negative id is now two bytes as well, where reinterpreting it as a large
    /// `u64` made it nine.
    ///
    /// The tag range does not overlap the 1..=8 length prefix this format used
    /// before, so a file written by an older build is rejected on the first key
    /// rather than silently misread.
    pub fn encode_row_id_key(row_id: u64) -> Vec<u8> {
        let signed = row_id as i64;
        // The magnitude that has to fit: the id itself when non-negative, and its
        // complement when not -- `!v` is `-v - 1`, which is what makes -1 the
        // narrowest negative rather than the widest.
        let magnitude = if signed < 0 { !signed as u64 } else { row_id };
        let width = significant_byte_width(magnitude);
        let tag = if signed < 0 {
            0x80 - width as u8
        } else {
            0x80 + width as u8
        };

        let mut key = Vec::with_capacity(1 + width);
        key.push(tag);
        key.extend_from_slice(&row_id.to_be_bytes()[8 - width..]);
        key
    }

    /// Decodes a row id written by [`Self::encode_row_id_key`].
    pub fn decode_row_id_key(key: &[u8]) -> Result<u64> {
        let Some(&tag) = key.first() else {
            return Err(HematiteError::CorruptedData("Empty rowid key".to_string()));
        };
        let (width, negative) = match tag {
            0x81..=0x88 => ((tag - 0x80) as usize, false),
            0x78..=0x7F => ((0x80 - tag) as usize, true),
            other => {
                return Err(HematiteError::CorruptedData(format!(
                    "Invalid rowid key tag: {other:#04x}"
                )))
            }
        };
        if key.len() != 1 + width {
            return Err(HematiteError::CorruptedData(format!(
                "Rowid key length mismatch: tag specifies {} bytes but key length is {}",
                width,
                key.len()
            )));
        }
        // Sign extension: a negative id was written as the low bytes of its
        // two's-complement form, so the bytes above it are all ones.
        let mut be_bytes = if negative { [0xFFu8; 8] } else { [0u8; 8] };
        be_bytes[8 - width..].copy_from_slice(&key[1..]);
        Ok(u64::from_be_bytes(be_bytes))
    }

    /// Decodes only the fields at `indices`, in that order. See
    /// [`record_v4::decode_fields`].
    pub fn decode_row_fields(data: &[u8], indices: &[usize]) -> Result<Vec<Value>> {
        record_v4::decode_fields(data, indices)
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
            // The leading byte is a tag now, not a plain length: it carries the
            // width in its distance from 0x80 and the sign in its direction.
            let width = encoded.len() - 1;
            let expected_tag = if (row_id as i64) < 0 {
                0x80 - width as u8
            } else {
                0x80 + width as u8
            };
            assert_eq!(encoded[0], expected_tag, "tag for {row_id}");
            let decoded = RowCodec::decode_row_id_key(&encoded)?;
            assert_eq!(row_id, decoded);
        }
        Ok(())
    }

    /// Row-id keys sort by their *signed* value, which is a deliberate change.
    ///
    /// They used to sort unsigned, and for ids handed out by a counter the two
    /// agree — a counter never reaches the top half of the range. They stopped
    /// agreeing when a table's primary key became its row id, because a SQL
    /// integer key can be negative. Under the old encoding those keys landed
    /// above every positive one; `u64::MAX` read as a row id is `-1`, and it now
    /// sorts where `-1` belongs.
    #[test]
    fn row_id_keys_sort_by_signed_value() {
        let ascending: Vec<u64> = vec![
            i64::MIN as u64,
            (-4_294_967_296i64) as u64,
            (-2i64) as u64,
            u64::MAX, // -1
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
            16777216,
            4294967296,
            i64::MAX as u64,
        ];

        for pair in ascending.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            assert!(
                (a as i64) < (b as i64),
                "fixture is not in ascending signed order"
            );
            let encoded_a = RowCodec::encode_row_id_key(a);
            let encoded_b = RowCodec::encode_row_id_key(b);
            assert!(
                encoded_a < encoded_b,
                "sort order violation: key({}) >= key({})",
                a as i64,
                b as i64
            );
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

    /// The key and the row id are the same number, in both directions and for
    /// every `i64`. An `AUTO_INCREMENT` key is filled from the row-id counter, so
    /// anything else would make those two disagree.
    #[test]
    fn a_primary_key_and_its_rowid_are_the_same_number() {
        for key in [i64::MIN, -1_000_000, -1, 0, 1, 1_000_000, i64::MAX] {
            let row_id = RowCodec::rowid_for_primary_key(&Value::BigInt(key)).expect("integer key");
            assert_eq!(RowCodec::primary_key_for_rowid(row_id), key);
        }
        assert_eq!(
            RowCodec::rowid_for_primary_key(&Value::Integer(42)),
            Some(42)
        );
    }

    /// Encoded keys sort in key order across the whole `i64` range, negatives
    /// included. This is what lets a table keyed by its primary key be physically
    /// sorted by that key — and what a range scan or a "MIN is the first leaf"
    /// optimisation would rely on if either ever lands.
    #[test]
    fn encoded_rowid_keys_sort_in_key_order() {
        let mut keys: Vec<i64> = vec![
            i64::MIN,
            -1_000_000_000_000,
            -65_536,
            -257,
            -256,
            -2,
            -1,
            0,
            1,
            255,
            256,
            70_000,
            i64::MAX,
        ];
        let mut encoded: Vec<(i64, Vec<u8>)> = keys
            .iter()
            .map(|&key| (key, RowCodec::encode_row_id_key(key as u64)))
            .collect();
        encoded.sort_by(|left, right| left.1.cmp(&right.1));
        keys.sort_unstable();
        assert_eq!(
            encoded.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
            keys,
            "encoded row-id keys did not sort in key order"
        );
    }

    /// Every `i64` round-trips, and small ids stay two bytes in both directions —
    /// which is the point of the tag rather than a plain sign flip, since flipping
    /// would make every small key eight bytes of body.
    #[test]
    fn rowid_keys_round_trip_and_stay_compact() {
        for key in [
            i64::MIN,
            -70_000,
            -256,
            -1,
            0,
            1,
            255,
            70_000,
            i64::MAX,
        ] {
            let encoded = RowCodec::encode_row_id_key(key as u64);
            assert_eq!(
                RowCodec::decode_row_id_key(&encoded).expect("decode") as i64,
                key,
                "{key} did not round-trip"
            );
        }
        assert_eq!(RowCodec::encode_row_id_key(1).len(), 2);
        assert_eq!(RowCodec::encode_row_id_key(-3i64 as u64).len(), 2);
    }

    /// The tag range does not overlap the plain 1..=8 length prefix this format
    /// used before, so an older file is rejected rather than misread.
    #[test]
    fn a_pre_tag_rowid_key_is_rejected() {
        for width in 1u8..=8 {
            let mut old_key = vec![width];
            old_key.extend(std::iter::repeat_n(0u8, width as usize));
            assert!(
                RowCodec::decode_row_id_key(&old_key).is_err(),
                "a {width}-byte pre-tag key was accepted"
            );
        }
    }

    #[test]
    fn only_integers_within_i64_get_a_rowid() {
        assert!(RowCodec::rowid_for_primary_key(&Value::Integer(7)).is_some());
        assert!(RowCodec::rowid_for_primary_key(&Value::UInteger(7)).is_some());
        assert!(RowCodec::rowid_for_primary_key(&Value::UBigInt(7)).is_some());
        assert!(RowCodec::rowid_for_primary_key(&Value::UBigInt(u64::MAX)).is_none());
        assert!(RowCodec::rowid_for_primary_key(&Value::Text("7".into())).is_none());
        assert!(RowCodec::rowid_for_primary_key(&Value::Null).is_none());
    }

}
