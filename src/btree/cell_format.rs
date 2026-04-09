#![allow(dead_code)]

use crate::error::{HematiteError, Result};
use crate::storage::format::{
    choose_local_payload_size, decode_varint, encode_varint, table_max_leaf_payload,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableLeafCell {
    pub(crate) rowid: u64,
    pub(crate) payload_size: usize,
    pub(crate) local_payload: Vec<u8>,
    pub(crate) overflow_page_id: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableInteriorCell {
    pub(crate) left_child_page_id: u32,
    pub(crate) rowid: u64,
}

impl TableLeafCell {
    pub(crate) fn from_payload(
        rowid: u64,
        payload: &[u8],
        overflow_page_id: Option<u32>,
        usable_size: usize,
    ) -> Result<Self> {
        let local_payload_size = table_leaf_local_payload_size(usable_size, payload.len());
        if payload.len() > local_payload_size && overflow_page_id.is_none() {
            return Err(HematiteError::StorageError(
                "table leaf payload requires an overflow page id".to_string(),
            ));
        }

        Ok(Self {
            rowid,
            payload_size: payload.len(),
            local_payload: payload[..local_payload_size].to_vec(),
            overflow_page_id,
        })
    }

    pub(crate) fn encode(&self, usable_size: usize) -> Result<Vec<u8>> {
        validate_table_leaf_cell(self, usable_size)?;

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&encode_varint(self.payload_size as u64));
        bytes.extend_from_slice(&encode_varint(self.rowid));
        bytes.extend_from_slice(&self.local_payload);
        if let Some(overflow_page_id) = self.overflow_page_id {
            bytes.extend_from_slice(&overflow_page_id.to_be_bytes());
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8], usable_size: usize) -> Result<(Self, usize)> {
        let (payload_size, payload_size_len) = decode_varint(bytes)?;
        let (rowid, rowid_len) = decode_varint(&bytes[payload_size_len..])?;
        let payload_size = payload_size as usize;
        let local_payload_size = table_leaf_local_payload_size(usable_size, payload_size);
        let payload_start = payload_size_len + rowid_len;
        let payload_end = payload_start + local_payload_size;
        if payload_end > bytes.len() {
            return Err(HematiteError::StorageError(
                "table leaf cell payload is truncated".to_string(),
            ));
        }

        let overflow_page_id = if payload_size > local_payload_size {
            if payload_end + 4 > bytes.len() {
                return Err(HematiteError::StorageError(
                    "table leaf cell overflow pointer is truncated".to_string(),
                ));
            }
            let overflow_page_id = read_u32_be(bytes, payload_end);
            if overflow_page_id == 0 {
                return Err(HematiteError::StorageError(
                    "table leaf cell overflow pointer must be non-zero".to_string(),
                ));
            }
            Some(overflow_page_id)
        } else {
            None
        };

        let used = payload_end + overflow_page_id.map(|_| 4).unwrap_or(0);
        Ok((
            Self {
                rowid,
                payload_size,
                local_payload: bytes[payload_start..payload_end].to_vec(),
                overflow_page_id,
            },
            used,
        ))
    }
}

impl TableInteriorCell {
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        if self.left_child_page_id <= 1 {
            return Err(HematiteError::StorageError(
                "table interior cell child pointer cannot reference reserved pages".to_string(),
            ));
        }

        let mut bytes = Vec::with_capacity(4 + 9);
        bytes.extend_from_slice(&self.left_child_page_id.to_be_bytes());
        bytes.extend_from_slice(&encode_varint(self.rowid));
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<(Self, usize)> {
        if bytes.len() < 4 {
            return Err(HematiteError::StorageError(
                "table interior cell is truncated".to_string(),
            ));
        }

        let left_child_page_id = read_u32_be(bytes, 0);
        if left_child_page_id <= 1 {
            return Err(HematiteError::StorageError(
                "table interior cell child pointer cannot reference reserved pages".to_string(),
            ));
        }

        let (rowid, rowid_len) = decode_varint(&bytes[4..])?;
        Ok((
            Self {
                left_child_page_id,
                rowid,
            },
            4 + rowid_len,
        ))
    }
}

pub(crate) fn table_leaf_local_payload_size(usable_size: usize, payload_size: usize) -> usize {
    let max_leaf = table_max_leaf_payload(usable_size);
    if payload_size <= max_leaf {
        payload_size
    } else {
        choose_local_payload_size(usable_size, payload_size)
    }
}

fn validate_table_leaf_cell(cell: &TableLeafCell, usable_size: usize) -> Result<()> {
    let expected_local_payload_size = table_leaf_local_payload_size(usable_size, cell.payload_size);
    if cell.local_payload.len() != expected_local_payload_size {
        return Err(HematiteError::StorageError(format!(
            "table leaf cell local payload length {} does not match expected {}",
            cell.local_payload.len(),
            expected_local_payload_size
        )));
    }

    let overflow_required = cell.payload_size > expected_local_payload_size;
    match (overflow_required, cell.overflow_page_id) {
        (true, Some(0)) => {
            return Err(HematiteError::StorageError(
                "table leaf cell overflow pointer must be non-zero".to_string(),
            ));
        }
        (true, None) => {
            return Err(HematiteError::StorageError(
                "table leaf payload requires an overflow page id".to_string(),
            ));
        }
        (false, Some(_)) => {
            return Err(HematiteError::StorageError(
                "table leaf cell has an unnecessary overflow pointer".to_string(),
            ));
        }
        _ => {}
    }

    Ok(())
}

fn read_u32_be(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

#[cfg(test)]
mod tests {
    use super::{table_leaf_local_payload_size, TableInteriorCell, TableLeafCell};

    #[test]
    fn table_leaf_cell_roundtrips_without_overflow() {
        let payload = b"hello world".to_vec();
        let cell = TableLeafCell::from_payload(7, &payload, None, 4096).unwrap();
        let encoded = cell.encode(4096).unwrap();
        let (decoded, used) = TableLeafCell::decode(&encoded, 4096).unwrap();

        assert_eq!(used, encoded.len());
        assert_eq!(decoded.rowid, 7);
        assert_eq!(decoded.payload_size, payload.len());
        assert_eq!(decoded.local_payload, payload);
        assert_eq!(decoded.overflow_page_id, None);
    }

    #[test]
    fn table_leaf_cell_roundtrips_with_overflow_pointer() {
        let payload = vec![0xAA; 10_000];
        let expected_local = table_leaf_local_payload_size(4096, payload.len());
        let cell = TableLeafCell::from_payload(99, &payload, Some(44), 4096).unwrap();
        let encoded = cell.encode(4096).unwrap();
        let (decoded, used) = TableLeafCell::decode(&encoded, 4096).unwrap();

        assert_eq!(used, encoded.len());
        assert_eq!(decoded.rowid, 99);
        assert_eq!(decoded.payload_size, payload.len());
        assert_eq!(decoded.local_payload.len(), expected_local);
        assert_eq!(decoded.overflow_page_id, Some(44));
        assert_eq!(decoded.local_payload, payload[..expected_local]);
    }

    #[test]
    fn table_leaf_cell_rejects_missing_overflow_pointer() {
        let payload = vec![0x11; 10_000];
        let error = TableLeafCell::from_payload(3, &payload, None, 4096).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("table leaf payload requires an overflow page id"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn table_interior_cell_roundtrip() {
        let cell = TableInteriorCell {
            left_child_page_id: 12,
            rowid: 55,
        };
        let encoded = cell.encode().unwrap();
        let (decoded, used) = TableInteriorCell::decode(&encoded).unwrap();

        assert_eq!(used, encoded.len());
        assert_eq!(decoded, cell);
    }
}
