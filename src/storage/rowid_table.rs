//! Rowid table cell formats for table B-tree migration.
//!
//! This module defines stable byte-level encodings for rowid-keyed table cells.

use crate::error::{HematiteError, Result};
use crate::storage::PageId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowidLeafCell {
    pub rowid: u64,
    pub payload: Vec<u8>,
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
