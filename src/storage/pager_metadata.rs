use crate::error::{HematiteError, Result};
use crate::storage::pager::JournalMode;
use crate::storage::PageId;
use std::collections::HashMap;

const METADATA_MAGIC: [u8; 4] = *b"HPM1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PersistedPagerState {
    pub(crate) journal_mode: JournalMode,
    pub(crate) free_pages: Vec<PageId>,
    pub(crate) checksums: HashMap<PageId, u32>,
}

impl PersistedPagerState {
    pub(crate) fn encode(&self, version: u32) -> Vec<u8> {
        let mut entries = self
            .checksums
            .iter()
            .map(|(page_id, checksum)| (*page_id, *checksum))
            .collect::<Vec<_>>();
        entries.sort_by_key(|(page_id, _)| *page_id);

        let mut free_pages = self.free_pages.clone();
        free_pages.sort_unstable();

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&METADATA_MAGIC);
        bytes.extend_from_slice(&version.to_le_bytes());
        bytes.push(match self.journal_mode {
            JournalMode::Rollback => 0,
            JournalMode::Wal => 1,
        });
        bytes.extend_from_slice(&(free_pages.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&(entries.len() as u32).to_le_bytes());

        let mut prev = 0u32;
        for page_id in free_pages {
            let delta = page_id.saturating_sub(prev);
            encode_varint_u32(delta, &mut bytes);
            prev = page_id;
        }

        let mut prev_checksum_page = 0u32;
        for (page_id, checksum) in entries {
            let delta = page_id.saturating_sub(prev_checksum_page);
            encode_varint_u32(delta, &mut bytes);
            encode_varint_u32(checksum, &mut bytes);
            prev_checksum_page = page_id;
        }

        bytes
    }

    pub(crate) fn decode_bytes(contents: &[u8], expected_version: u32) -> Result<Self> {
        if contents.starts_with(&METADATA_MAGIC) {
            return Self::decode_binary(contents, expected_version);
        }

        Err(HematiteError::StorageError(
            "Legacy pager metadata format is unsupported".to_string(),
        ))
    }

    fn decode_binary(contents: &[u8], expected_version: u32) -> Result<Self> {
        if contents.len() < 4 + 4 + 1 + 4 + 4 {
            return Err(HematiteError::StorageError(
                "Pager metadata header is truncated".to_string(),
            ));
        }

        let mut offset = 4;
        let version = u32::from_le_bytes(contents[offset..offset + 4].try_into().unwrap());
        offset += 4;
        if version != expected_version {
            return Err(HematiteError::StorageError(format!(
                "Unsupported pager checksum metadata version: expected {}, got {}",
                expected_version, version
            )));
        }

        let journal_mode = match contents[offset] {
            0 => JournalMode::Rollback,
            1 => JournalMode::Wal,
            other => {
                return Err(HematiteError::StorageError(format!(
                    "Unsupported pager journal mode {}",
                    other
                )))
            }
        };
        offset += 1;

        let free_count =
            u32::from_le_bytes(contents[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let checksum_count =
            u32::from_le_bytes(contents[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut free_pages = Vec::with_capacity(free_count);
        let mut prev = 0u32;
        for _ in 0..free_count {
            let delta = decode_varint_u32(contents, &mut offset)?;
            let page_id = prev.saturating_add(delta);
            free_pages.push(page_id);
            prev = page_id;
        }

        let mut checksums = HashMap::with_capacity(checksum_count);
        let mut prev_checksum_page = 0u32;
        for _ in 0..checksum_count {
            let delta = decode_varint_u32(contents, &mut offset)?;
            let page_id = prev_checksum_page.saturating_add(delta);
            let checksum = decode_varint_u32(contents, &mut offset)?;
            if checksums.insert(page_id, checksum).is_some() {
                return Err(HematiteError::StorageError(format!(
                    "Duplicate pager checksum entry for page {page_id}",
                )));
            }
            prev_checksum_page = page_id;
        }

        Ok(Self {
            journal_mode,
            free_pages,
            checksums,
        })
    }
}

fn encode_varint_u32(mut value: u32, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn decode_varint_u32(input: &[u8], offset: &mut usize) -> Result<u32> {
    let mut shift = 0;
    let mut result = 0u32;
    while *offset < input.len() {
        let byte = input[*offset];
        *offset += 1;
        result |= u32::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 32 {
            break;
        }
    }
    Err(HematiteError::StorageError(
        "Pager metadata contains truncated varint".to_string(),
    ))
}
