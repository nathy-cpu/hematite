//! Key/value codec boundary for B-tree payloads.
//!
//! The core B-tree stores opaque byte keys and byte values. Higher layers (catalog/table/index)
//! are responsible for typed encoding/decoding through this trait.

use crate::error::Result;

pub trait KeyValueCodec {
    type Key;
    type Value;

    fn encode_key(key: &Self::Key) -> Result<Vec<u8>>;
    fn decode_key(bytes: &[u8]) -> Result<Self::Key>;
    fn encode_value(value: &Self::Value) -> Result<Vec<u8>>;
    fn decode_value(bytes: &[u8]) -> Result<Self::Value>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RawBytesCodec;

impl KeyValueCodec for RawBytesCodec {
    type Key = Vec<u8>;
    type Value = Vec<u8>;

    fn encode_key(key: &Self::Key) -> Result<Vec<u8>> {
        Ok(key.clone())
    }

    fn decode_key(bytes: &[u8]) -> Result<Self::Key> {
        Ok(bytes.to_vec())
    }

    fn encode_value(value: &Self::Value) -> Result<Vec<u8>> {
        Ok(value.clone())
    }

    fn decode_value(bytes: &[u8]) -> Result<Self::Value> {
        Ok(bytes.to_vec())
    }
}
