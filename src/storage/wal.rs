//! Write-ahead log representation for pager-managed WAL mode.
//!
//! WAL mode separates "what is committed" from "what is checkpointed into the main file". Each
//! committed record describes a complete pager-visible state transition.
//!
//! Record shape:
//!
//! ```text
//! WalRecord
//!   sequence number
//!   visible file length
//!   visible freelist snapshot
//!   visible checksum table entries
//!   page frames[]
//!       page id
//!       page bytes
//! ```
//!
//! Reconstruction algorithm:
//! - decode all complete records in order;
//! - ignore a truncated tail record;
//! - keep the last frame for each page id;
//! - expose the latest committed sequence as a `VisibleWalState`.
//!
//! Readers do not see "the current WAL file". They see the last committed sequence captured when
//! their read scope begins.

use crate::error::{HematiteError, Result};
use crate::storage::{PageId, PAGE_SIZE};
use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFrame {
    pub page_id: PageId,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibleWalState {
    pub visible_sequence: u64,
    pub file_len: u64,
    pub free_pages: Vec<PageId>,
    pub page_checksums: HashMap<PageId, u32>,
    pub page_overrides: HashMap<PageId, Vec<u8>>,
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
    const FILE_HEADER_LEN: usize = 8;

    #[cfg(test)]
    pub fn encode_file(records: &[Self]) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&Self::file_header());
        for record in records {
            bytes.extend_from_slice(&record.encode_entry()?);
        }
        Ok(bytes)
    }

    pub fn decode_file(bytes: &[u8]) -> Result<Vec<Self>> {
        if bytes.is_empty() {
            return Ok(Vec::new());
        }

        if bytes.len() < Self::FILE_HEADER_LEN {
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

        let mut offset = Self::FILE_HEADER_LEN;
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

        Self::validate_records(&records)?;
        Ok(records)
    }

    pub fn append_to_path<P: AsRef<Path>>(path: P, record: &Self) -> Result<()> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        let metadata = file.metadata()?;
        if metadata.len() == 0 {
            file.write_all(&Self::file_header())?;
        } else if metadata.len() < Self::FILE_HEADER_LEN as u64 {
            return Err(HematiteError::StorageError(
                "Existing WAL file has a truncated header".to_string(),
            ));
        }
        file.write_all(&record.encode_entry()?)?;
        file.sync_all()?;
        Ok(())
    }

    pub fn load_visible_state_from_path<P: AsRef<Path>>(
        path: P,
    ) -> Result<Option<VisibleWalState>> {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };

        let records = Self::decode_file(&bytes)?;
        Ok(Self::visible_state_from_records(&records))
    }

    pub fn visible_state_from_records(records: &[Self]) -> Option<VisibleWalState> {
        let last_record = records.last()?;
        let mut page_overrides = HashMap::new();
        for record in records {
            for frame in &record.frames {
                page_overrides.insert(frame.page_id, frame.data.clone());
            }
        }

        Some(VisibleWalState {
            visible_sequence: last_record.sequence,
            file_len: last_record.file_len,
            free_pages: last_record.free_pages.clone(),
            page_checksums: last_record.checksums.iter().copied().collect(),
            page_overrides,
        })
    }

    fn validate_records(records: &[Self]) -> Result<()> {
        let mut previous_sequence = 0u64;
        for record in records {
            if record.sequence <= previous_sequence {
                return Err(HematiteError::StorageError(
                    "WAL sequences must increase strictly".to_string(),
                ));
            }
            previous_sequence = record.sequence;

            let mut seen_frames = HashSet::new();
            for frame in &record.frames {
                if !seen_frames.insert(frame.page_id) {
                    return Err(HematiteError::StorageError(format!(
                        "WAL record {} contains duplicate frame for page {}",
                        record.sequence, frame.page_id
                    )));
                }
            }
        }
        Ok(())
    }

    fn file_header() -> [u8; Self::FILE_HEADER_LEN] {
        let mut header = [0u8; Self::FILE_HEADER_LEN];
        header[..4].copy_from_slice(&Self::MAGIC);
        header[4..].copy_from_slice(&Self::VERSION.to_le_bytes());
        header
    }

    fn encode_entry(&self) -> Result<Vec<u8>> {
        let payload = self.encode_payload()?;
        let mut bytes = Vec::with_capacity(12 + payload.len());
        bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&checksum_bytes(&payload).to_le_bytes());
        bytes.extend_from_slice(&payload);
        Ok(bytes)
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
