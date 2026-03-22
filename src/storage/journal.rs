//! Rollback journal records for pager-managed transactions.
//!
//! Contract:
//! - The journal stores original page images and pager checksum state.
//! - Recovery is process-crash only: `ACTIVE` journals are rolled back on open.
//! - `COMMITTED` journals are finalized by deleting the journal after reopen.

use crate::error::{HematiteError, Result};
use crate::storage::{PageId, PAGE_SIZE};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalState {
    Active = 1,
    Committed = 2,
}

impl JournalState {
    fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Active),
            2 => Ok(Self::Committed),
            _ => Err(HematiteError::StorageError(format!(
                "Unsupported journal state {}",
                value
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct JournalRecord {
    pub page_id: PageId,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct RollbackJournal {
    pub state: JournalState,
    pub original_file_len: u64,
    pub original_free_pages: Vec<PageId>,
    pub original_checksums: Vec<(PageId, u32)>,
    pub page_records: Vec<JournalRecord>,
}

impl RollbackJournal {
    const MAGIC: [u8; 4] = *b"HTRJ";
    const VERSION: u32 = 1;

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&Self::MAGIC);
        bytes.extend_from_slice(&Self::VERSION.to_le_bytes());
        bytes.push(self.state as u8);
        bytes.extend_from_slice(&self.original_file_len.to_le_bytes());

        bytes.extend_from_slice(&(self.original_free_pages.len() as u32).to_le_bytes());
        for page_id in &self.original_free_pages {
            bytes.extend_from_slice(&page_id.to_le_bytes());
        }

        bytes.extend_from_slice(&(self.original_checksums.len() as u32).to_le_bytes());
        for (page_id, checksum) in &self.original_checksums {
            bytes.extend_from_slice(&page_id.to_le_bytes());
            bytes.extend_from_slice(&checksum.to_le_bytes());
        }

        bytes.extend_from_slice(&(self.page_records.len() as u32).to_le_bytes());
        for record in &self.page_records {
            if record.data.len() != PAGE_SIZE {
                return Err(HematiteError::StorageError(format!(
                    "Journal page {} has invalid image size {}",
                    record.page_id,
                    record.data.len()
                )));
            }
            bytes.extend_from_slice(&record.page_id.to_le_bytes());
            bytes.extend_from_slice(&record.data);
        }

        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut offset = 0usize;

        if bytes.len() < 17 {
            return Err(HematiteError::StorageError(
                "Rollback journal is truncated".to_string(),
            ));
        }

        if bytes[offset..offset + 4] != Self::MAGIC {
            return Err(HematiteError::StorageError(
                "Invalid rollback journal magic".to_string(),
            ));
        }
        offset += 4;

        let version = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;
        if version != Self::VERSION {
            return Err(HematiteError::StorageError(format!(
                "Unsupported rollback journal version {}",
                version
            )));
        }

        let state = JournalState::from_u8(bytes[offset])?;
        offset += 1;

        let original_file_len = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        let free_page_count = read_u32(bytes, &mut offset)? as usize;
        let mut original_free_pages = Vec::with_capacity(free_page_count);
        for _ in 0..free_page_count {
            original_free_pages.push(read_u32(bytes, &mut offset)?);
        }

        let checksum_count = read_u32(bytes, &mut offset)? as usize;
        let mut original_checksums = Vec::with_capacity(checksum_count);
        for _ in 0..checksum_count {
            let page_id = read_u32(bytes, &mut offset)?;
            let checksum = read_u32(bytes, &mut offset)?;
            original_checksums.push((page_id, checksum));
        }

        let page_count = read_u32(bytes, &mut offset)? as usize;
        let mut page_records = Vec::with_capacity(page_count);
        for _ in 0..page_count {
            let page_id = read_u32(bytes, &mut offset)?;
            if offset + PAGE_SIZE > bytes.len() {
                return Err(HematiteError::StorageError(
                    "Rollback journal page image is truncated".to_string(),
                ));
            }
            let data = bytes[offset..offset + PAGE_SIZE].to_vec();
            offset += PAGE_SIZE;
            page_records.push(JournalRecord { page_id, data });
        }

        if offset != bytes.len() {
            return Err(HematiteError::StorageError(
                "Rollback journal has trailing bytes".to_string(),
            ));
        }

        Ok(Self {
            state,
            original_file_len,
            original_free_pages,
            original_checksums,
            page_records,
        })
    }
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    if *offset + 4 > bytes.len() {
        return Err(HematiteError::StorageError(
            "Rollback journal is truncated".to_string(),
        ));
    }
    let value = u32::from_le_bytes([
        bytes[*offset],
        bytes[*offset + 1],
        bytes[*offset + 2],
        bytes[*offset + 3],
    ]);
    *offset += 4;
    Ok(value)
}
