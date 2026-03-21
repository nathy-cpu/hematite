//! Rowid table cell formats for table B-tree migration.
//!
//! This module defines stable byte-level encodings for rowid-keyed table cells.

use crate::error::{HematiteError, Result};
use crate::storage::serialization::RowSerializer;
use crate::storage::PageId;
use crate::storage::StoredRow;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowidLeafCell {
    pub rowid: u64,
    pub payload: Vec<u8>,
}

pub const ROWID_LEAF_FIXED_HEADER_SIZE: usize = 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowidLeafCellLayout {
    pub rowid: u64,
    pub total_payload_len: u32,
    pub local_payload: Vec<u8>,
    pub overflow_first_page: PageId,
}

impl RowidLeafCellLayout {
    pub fn local_payload_len_for(total_payload_len: usize, max_local_payload: usize) -> usize {
        total_payload_len.min(max_local_payload)
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        if self.local_payload.len() > self.total_payload_len as usize {
            return Err(HematiteError::StorageError(
                "Local payload cannot exceed total payload length".to_string(),
            ));
        }

        let local_len = self.local_payload.len() as u32;
        let mut out = Vec::with_capacity(ROWID_LEAF_FIXED_HEADER_SIZE + self.local_payload.len());
        out.extend_from_slice(&self.rowid.to_le_bytes());
        out.extend_from_slice(&self.total_payload_len.to_le_bytes());
        out.extend_from_slice(&local_len.to_le_bytes());
        out.extend_from_slice(&self.overflow_first_page.as_u32().to_le_bytes());
        out.extend_from_slice(&self.local_payload);
        Ok(out)
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < ROWID_LEAF_FIXED_HEADER_SIZE {
            return Err(HematiteError::CorruptedData(
                "Rowid fixed leaf cell header is truncated".to_string(),
            ));
        }

        let rowid = u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let total_payload_len = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
        let local_len = u32::from_le_bytes([data[12], data[13], data[14], data[15]]) as usize;
        let overflow_first_page =
            PageId::new(u32::from_le_bytes([data[16], data[17], data[18], data[19]]));

        if ROWID_LEAF_FIXED_HEADER_SIZE + local_len != data.len() {
            return Err(HematiteError::CorruptedData(
                "Rowid fixed leaf local payload length mismatch".to_string(),
            ));
        }
        if local_len > total_payload_len as usize {
            return Err(HematiteError::CorruptedData(
                "Rowid fixed leaf local payload exceeds total payload length".to_string(),
            ));
        }

        Ok(Self {
            rowid,
            total_payload_len,
            local_payload: data[ROWID_LEAF_FIXED_HEADER_SIZE..].to_vec(),
            overflow_first_page,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedRowidRecord {
    pub cell: RowidLeafCellLayout,
    pub overflow_payload: Vec<u8>,
}

pub fn encode_stored_row_record(
    row: &StoredRow,
    max_local_payload: usize,
) -> Result<EncodedRowidRecord> {
    let mut payload = RowSerializer::serialize_stored_row(&StoredRow {
        row_id: 0,
        values: row.values.clone(),
    })?;
    if payload.len() < 4 {
        return Err(HematiteError::CorruptedData(
            "Stored row payload is truncated".to_string(),
        ));
    }
    payload.drain(0..4);
    let local_len = RowidLeafCellLayout::local_payload_len_for(payload.len(), max_local_payload);
    let local_payload = payload[0..local_len].to_vec();
    let overflow_payload = payload[local_len..].to_vec();

    Ok(EncodedRowidRecord {
        cell: RowidLeafCellLayout {
            rowid: row.row_id,
            total_payload_len: payload.len() as u32,
            local_payload,
            overflow_first_page: PageId::invalid(),
        },
        overflow_payload,
    })
}

pub fn decode_stored_row_record(rowid: u64, payload: &[u8]) -> Result<StoredRow> {
    let mut row = RowSerializer::deserialize_stored_row(payload)?;
    row.row_id = rowid;
    Ok(row)
}

impl RowidLeafCell {
    pub const HEADER_SIZE: usize = 12; // rowid(u64) + payload_len(u32)

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(Self::HEADER_SIZE + self.payload.len());
        out.extend_from_slice(&self.rowid.to_le_bytes());
        out.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < Self::HEADER_SIZE {
            return Err(HematiteError::CorruptedData(
                "Rowid leaf cell header is truncated".to_string(),
            ));
        }

        let rowid = u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let payload_len = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;
        if Self::HEADER_SIZE + payload_len != data.len() {
            return Err(HematiteError::CorruptedData(
                "Rowid leaf cell payload length mismatch".to_string(),
            ));
        }

        Ok(Self {
            rowid,
            payload: data[Self::HEADER_SIZE..].to_vec(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowidInternalCell {
    pub separator_rowid: u64,
    pub child_page_id: PageId,
}

impl RowidInternalCell {
    pub const SIZE: usize = 12; // separator_rowid(u64) + child_page_id(u32)

    pub fn encode(&self) -> [u8; Self::SIZE] {
        let mut out = [0u8; Self::SIZE];
        out[0..8].copy_from_slice(&self.separator_rowid.to_le_bytes());
        out[8..12].copy_from_slice(&self.child_page_id.as_u32().to_le_bytes());
        out
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() != Self::SIZE {
            return Err(HematiteError::CorruptedData(
                "Rowid internal cell size mismatch".to_string(),
            ));
        }

        let separator_rowid = u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let child_page_id = PageId::new(u32::from_le_bytes([data[8], data[9], data[10], data[11]]));

        Ok(Self {
            separator_rowid,
            child_page_id,
        })
    }
}
