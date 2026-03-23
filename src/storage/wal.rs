//! Write-ahead log records for pager-managed WAL mode.
//!
//! Contract:
//! - The WAL stores committed page images plus the logical pager metadata state that becomes
//!   visible with that commit.
//! - Records are append-only and self-delimiting so a truncated tail can be ignored on open.
//! - Reader snapshots are derived from the last committed record visible to that reader.

use crate::error::{HematiteError, Result};
use crate::storage::{PageId, PAGE_SIZE};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFrame {
    pub page_id: PageId,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub sequence: u64,
    pub file_len: u64,
    pub free_pages: Vec<PageId>,
    pub checksums: Vec<(PageId, u32)>,
    pub frames: Vec<WalFrame>,
}

impl WalRecord {
    const MAGIC: [u8; 4] = *b"HTWL";
    const VERSION: u32 = 1;

    pub fn encode_file(records: &[Self]) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&Self::MAGIC);
        bytes.extend_from_slice(&Self::VERSION.to_le_bytes());
        for record in records {
            let payload = record.encode_payload()?;
            bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
            bytes.extend_from_slice(&checksum_bytes(&payload).to_le_bytes());
            bytes.extend_from_slice(&payload);
        }
        Ok(bytes)
    }

    pub fn decode_file(bytes: &[u8]) -> Result<Vec<Self>> {
        if bytes.is_empty() {
            return Ok(Vec::new());
        }

        if bytes.len() < 8 {
            return Err(HematiteError::StorageError(
                "WAL file is truncated".to_string(),
            ));
        }

        if bytes[..4] != Self::MAGIC {
            return Err(HematiteError::StorageError(
                "Invalid WAL file magic".to_string(),
            ));
        }

        let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if version != Self::VERSION {
            return Err(HematiteError::StorageError(format!(
                "Unsupported WAL file version {}",
                version
            )));
        }

        let mut offset = 8usize;
        let mut records = Vec::new();
        while offset < bytes.len() {
            if offset + 12 > bytes.len() {
                break;
            }

            let payload_len = u64::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
                bytes[offset + 4],
                bytes[offset + 5],
                bytes[offset + 6],
                bytes[offset + 7],
            ]) as usize;
            offset += 8;

            let expected_checksum = u32::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]);
            offset += 4;

            if offset + payload_len > bytes.len() {
                break;
            }

            let payload = &bytes[offset..offset + payload_len];
            let actual_checksum = checksum_bytes(payload);
            if actual_checksum != expected_checksum {
                return Err(HematiteError::StorageError(
                    "WAL record checksum mismatch".to_string(),
                ));
            }
            records.push(Self::decode_payload(payload)?);
            offset += payload_len;
        }

        Ok(records)
    }

    fn encode_payload(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.sequence.to_le_bytes());
        bytes.extend_from_slice(&self.file_len.to_le_bytes());

        bytes.extend_from_slice(&(self.free_pages.len() as u32).to_le_bytes());
        for page_id in &self.free_pages {
            bytes.extend_from_slice(&page_id.to_le_bytes());
        }

        bytes.extend_from_slice(&(self.checksums.len() as u32).to_le_bytes());
        for (page_id, checksum) in &self.checksums {
            bytes.extend_from_slice(&page_id.to_le_bytes());
            bytes.extend_from_slice(&checksum.to_le_bytes());
        }

        bytes.extend_from_slice(&(self.frames.len() as u32).to_le_bytes());
        for frame in &self.frames {
            if frame.data.len() != PAGE_SIZE {
                return Err(HematiteError::StorageError(format!(
                    "WAL frame {} has invalid image size {}",
                    frame.page_id,
                    frame.data.len()
                )));
            }
            bytes.extend_from_slice(&frame.page_id.to_le_bytes());
            bytes.extend_from_slice(&frame.data);
        }

        Ok(bytes)
    }

    fn decode_payload(bytes: &[u8]) -> Result<Self> {
        let mut offset = 0usize;
        let sequence = read_u64(bytes, &mut offset, "WAL record is truncated")?;
        let file_len = read_u64(bytes, &mut offset, "WAL record is truncated")?;

        let free_count =
            read_u32(bytes, &mut offset, "WAL free-page metadata is truncated")? as usize;
        let mut free_pages = Vec::with_capacity(free_count);
        for _ in 0..free_count {
            free_pages.push(read_u32(
                bytes,
                &mut offset,
                "WAL free-page metadata is truncated",
            )?);
        }

        let checksum_count =
            read_u32(bytes, &mut offset, "WAL checksum metadata is truncated")? as usize;
        let mut checksums = Vec::with_capacity(checksum_count);
        for _ in 0..checksum_count {
            let page_id = read_u32(bytes, &mut offset, "WAL checksum metadata is truncated")?;
            let checksum = read_u32(bytes, &mut offset, "WAL checksum metadata is truncated")?;
            checksums.push((page_id, checksum));
        }

        let frame_count = read_u32(bytes, &mut offset, "WAL frame metadata is truncated")? as usize;
        let mut frames = Vec::with_capacity(frame_count);
        for _ in 0..frame_count {
            let page_id = read_u32(bytes, &mut offset, "WAL frame metadata is truncated")?;
            if offset + PAGE_SIZE > bytes.len() {
                return Err(HematiteError::StorageError(
                    "WAL frame image is truncated".to_string(),
                ));
            }
            let data = bytes[offset..offset + PAGE_SIZE].to_vec();
            offset += PAGE_SIZE;
            frames.push(WalFrame { page_id, data });
        }

        if offset != bytes.len() {
            return Err(HematiteError::StorageError(
                "WAL record has trailing bytes".to_string(),
            ));
        }

        Ok(Self {
            sequence,
            file_len,
            free_pages,
            checksums,
            frames,
        })
    }
}

fn checksum_bytes(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 0x811C9DC5;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

fn read_u32(bytes: &[u8], offset: &mut usize, message: &str) -> Result<u32> {
    if *offset + 4 > bytes.len() {
        return Err(HematiteError::StorageError(message.to_string()));
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

fn read_u64(bytes: &[u8], offset: &mut usize, message: &str) -> Result<u64> {
    if *offset + 8 > bytes.len() {
        return Err(HematiteError::StorageError(message.to_string()));
    }
    let value = u64::from_le_bytes([
        bytes[*offset],
        bytes[*offset + 1],
        bytes[*offset + 2],
        bytes[*offset + 3],
        bytes[*offset + 4],
        bytes[*offset + 5],
        bytes[*offset + 6],
        bytes[*offset + 7],
    ]);
    *offset += 8;
    Ok(value)
}
