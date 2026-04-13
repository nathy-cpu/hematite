#![allow(dead_code)]

use crate::error::{HematiteError, Result};
use crate::storage::{PageId, PAGE_SIZE};
use std::collections::BTreeSet;

const V3_JOURNAL_MAGIC: &[u8; 4] = b"HTJ3";
const V3_JOURNAL_VERSION: u32 = 1;
const V3_JOURNAL_HEADER_SIZE: usize = 36;
const V3_JOURNAL_RECORD_PREFIX_SIZE: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V3JournalState {
    Active = 1,
    Committed = 2,
}

impl V3JournalState {
    fn encode(self) -> u8 {
        self as u8
    }

    fn decode(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Active),
            2 => Ok(Self::Committed),
            _ => Err(HematiteError::StorageError(format!(
                "Unsupported v3 rollback journal state {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V3JournalHeader {
    pub(crate) state: V3JournalState,
    pub(crate) page_size: u16,
    pub(crate) original_database_page_count: u32,
    pub(crate) sector_size_hint: u32,
    pub(crate) checksum_seed: u32,
    pub(crate) free_page_count: u32,
    pub(crate) checksum_count: u32,
    pub(crate) record_count: u32,
}

impl Default for V3JournalHeader {
    fn default() -> Self {
        Self {
            state: V3JournalState::Active,
            page_size: PAGE_SIZE as u16,
            original_database_page_count: 0,
            sector_size_hint: PAGE_SIZE as u32,
            checksum_seed: 0x4A4F5552,
            free_page_count: 0,
            checksum_count: 0,
            record_count: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V3JournalRecord {
    pub(crate) page_number: PageId,
    pub(crate) page_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V3RollbackJournal {
    pub(crate) header: V3JournalHeader,
    pub(crate) original_free_pages: Vec<PageId>,
    pub(crate) original_checksums: Vec<(PageId, u32)>,
    pub(crate) records: Vec<V3JournalRecord>,
}

impl V3JournalHeader {
    pub(crate) fn encode(&self) -> [u8; V3_JOURNAL_HEADER_SIZE] {
        let mut bytes = [0u8; V3_JOURNAL_HEADER_SIZE];
        bytes[..4].copy_from_slice(V3_JOURNAL_MAGIC);
        bytes[4..8].copy_from_slice(&V3_JOURNAL_VERSION.to_be_bytes());
        bytes[8] = self.state.encode();
        bytes[9..11].copy_from_slice(&self.page_size.to_be_bytes());
        bytes[11] = 0;
        bytes[12..16].copy_from_slice(&self.original_database_page_count.to_be_bytes());
        bytes[16..20].copy_from_slice(&self.sector_size_hint.to_be_bytes());
        bytes[20..24].copy_from_slice(&self.checksum_seed.to_be_bytes());
        bytes[24..28].copy_from_slice(&self.free_page_count.to_be_bytes());
        bytes[28..32].copy_from_slice(&self.checksum_count.to_be_bytes());
        bytes[32..36].copy_from_slice(&self.record_count.to_be_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < V3_JOURNAL_HEADER_SIZE {
            return Err(HematiteError::StorageError(
                "v3 rollback journal header is truncated".to_string(),
            ));
        }
        if &bytes[..4] != V3_JOURNAL_MAGIC {
            return Err(HematiteError::StorageError(
                "v3 rollback journal header magic mismatch".to_string(),
            ));
        }

        let version = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if version != V3_JOURNAL_VERSION {
            return Err(HematiteError::StorageError(format!(
                "Unsupported v3 rollback journal version {version}"
            )));
        }

        let page_size = u16::from_be_bytes([bytes[9], bytes[10]]);
        if page_size as usize != PAGE_SIZE {
            return Err(HematiteError::StorageError(format!(
                "Unsupported v3 rollback journal page size {page_size}"
            )));
        }

        Ok(Self {
            state: V3JournalState::decode(bytes[8])?,
            page_size,
            original_database_page_count: read_u32_be(bytes, 12),
            sector_size_hint: read_u32_be(bytes, 16),
            checksum_seed: read_u32_be(bytes, 20),
            free_page_count: read_u32_be(bytes, 24),
            checksum_count: read_u32_be(bytes, 28),
            record_count: read_u32_be(bytes, 32),
        })
    }
}

impl V3JournalRecord {
    pub(crate) fn encode(&self, checksum_seed: u32) -> Result<Vec<u8>> {
        if self.page_bytes.len() != PAGE_SIZE {
            return Err(HematiteError::StorageError(format!(
                "v3 rollback journal record for page {} has invalid image size {}",
                self.page_number,
                self.page_bytes.len()
            )));
        }

        let checksum = record_checksum(self.page_number, checksum_seed, &self.page_bytes);
        let mut bytes = Vec::with_capacity(V3_JOURNAL_RECORD_PREFIX_SIZE + PAGE_SIZE);
        bytes.extend_from_slice(&self.page_number.to_be_bytes());
        bytes.extend_from_slice(&checksum.to_be_bytes());
        bytes.extend_from_slice(&self.page_bytes);
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8], checksum_seed: u32) -> Result<(Self, usize)> {
        if bytes.len() < V3_JOURNAL_RECORD_PREFIX_SIZE + PAGE_SIZE {
            return Err(HematiteError::StorageError(
                "v3 rollback journal record is truncated".to_string(),
            ));
        }

        let page_number = read_u32_be(bytes, 0);
        let checksum = read_u32_be(bytes, 4);
        let page_bytes = bytes[8..8 + PAGE_SIZE].to_vec();
        let expected = record_checksum(page_number, checksum_seed, &page_bytes);
        if checksum != expected {
            return Err(HematiteError::StorageError(
                "v3 rollback journal record checksum mismatch".to_string(),
            ));
        }

        Ok((
            Self {
                page_number,
                page_bytes,
            },
            V3_JOURNAL_RECORD_PREFIX_SIZE + PAGE_SIZE,
        ))
    }
}

impl V3RollbackJournal {
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        validate_record_set(&self.records)?;

        let header = V3JournalHeader {
            free_page_count: self.original_free_pages.len() as u32,
            checksum_count: self.original_checksums.len() as u32,
            record_count: self.records.len() as u32,
            ..self.header.clone()
        };
        let mut bytes = Vec::with_capacity(
            V3_JOURNAL_HEADER_SIZE
                + self.original_free_pages.len() * std::mem::size_of::<PageId>()
                + self.original_checksums.len()
                    * (std::mem::size_of::<PageId>() + std::mem::size_of::<u32>())
                + self.records.len() * (V3_JOURNAL_RECORD_PREFIX_SIZE + PAGE_SIZE),
        );
        bytes.extend_from_slice(&header.encode());

        for page_id in &self.original_free_pages {
            bytes.extend_from_slice(&page_id.to_be_bytes());
        }

        for (page_id, checksum) in &self.original_checksums {
            bytes.extend_from_slice(&page_id.to_be_bytes());
            bytes.extend_from_slice(&checksum.to_be_bytes());
        }

        for record in &self.records {
            bytes.extend_from_slice(&record.encode(header.checksum_seed)?);
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let header = V3JournalHeader::decode(bytes)?;
        let mut offset = V3_JOURNAL_HEADER_SIZE;

        let mut original_free_pages = Vec::with_capacity(header.free_page_count as usize);
        for _ in 0..header.free_page_count {
            if offset + 4 > bytes.len() {
                return Err(HematiteError::StorageError(
                    "v3 rollback journal freelist metadata is truncated".to_string(),
                ));
            }
            original_free_pages.push(read_u32_be(bytes, offset));
            offset += 4;
        }

        let mut original_checksums = Vec::with_capacity(header.checksum_count as usize);
        for _ in 0..header.checksum_count {
            if offset + 8 > bytes.len() {
                return Err(HematiteError::StorageError(
                    "v3 rollback journal checksum metadata is truncated".to_string(),
                ));
            }
            original_checksums.push((read_u32_be(bytes, offset), read_u32_be(bytes, offset + 4)));
            offset += 8;
        }

        let mut records = Vec::with_capacity(header.record_count as usize);
        for _ in 0..header.record_count {
            let (record, used) = V3JournalRecord::decode(&bytes[offset..], header.checksum_seed)?;
            records.push(record);
            offset += used;
        }

        if offset != bytes.len() {
            return Err(HematiteError::StorageError(
                "v3 rollback journal has trailing bytes".to_string(),
            ));
        }

        validate_record_set(&records)?;
        Ok(Self {
            header,
            original_free_pages,
            original_checksums,
            records,
        })
    }
}

fn validate_record_set(records: &[V3JournalRecord]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for record in records {
        if !seen.insert(record.page_number) {
            return Err(HematiteError::StorageError(format!(
                "v3 rollback journal contains duplicate page {}",
                record.page_number
            )));
        }
    }
    Ok(())
}

fn record_checksum(page_number: PageId, checksum_seed: u32, page_bytes: &[u8]) -> u32 {
    let mut bytes = Vec::with_capacity(8 + page_bytes.len());
    bytes.extend_from_slice(&page_number.to_be_bytes());
    bytes.extend_from_slice(&checksum_seed.to_be_bytes());
    bytes.extend_from_slice(page_bytes);
    checksum_bytes(&bytes)
}

fn checksum_bytes(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 0x811C9DC5;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

fn read_u32_be(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

pub(crate) type V3JournalStateAlias = V3JournalState;

#[cfg(test)]
mod tests {
    use super::{
        V3JournalHeader, V3JournalRecord, V3JournalState, V3RollbackJournal, V3_JOURNAL_HEADER_SIZE,
    };
    use crate::storage::PAGE_SIZE;

    #[test]
    fn v3_rollback_journal_roundtrip() {
        let journal = V3RollbackJournal {
            header: V3JournalHeader {
                state: V3JournalState::Committed,
                original_database_page_count: 12,
                checksum_seed: 0xDEADBEEF,
                ..V3JournalHeader::default()
            },
            original_free_pages: vec![5, 9],
            original_checksums: vec![(2, 100), (7, 200)],
            records: vec![
                V3JournalRecord {
                    page_number: 2,
                    page_bytes: vec![0x11; PAGE_SIZE],
                },
                V3JournalRecord {
                    page_number: 9,
                    page_bytes: vec![0xAB; PAGE_SIZE],
                },
            ],
        };

        let encoded = journal.encode().expect("encode journal");
        let decoded = V3RollbackJournal::decode(&encoded).expect("decode journal");

        assert_eq!(decoded.header.state, V3JournalState::Committed);
        assert_eq!(decoded.header.original_database_page_count, 12);
        assert_eq!(decoded.header.checksum_seed, 0xDEADBEEF);
        assert_eq!(decoded.header.free_page_count, 2);
        assert_eq!(decoded.header.checksum_count, 2);
        assert_eq!(decoded.header.record_count, 2);
        assert_eq!(decoded.original_free_pages, journal.original_free_pages);
        assert_eq!(decoded.original_checksums, journal.original_checksums);
        assert_eq!(decoded.records, journal.records);
    }

    #[test]
    fn v3_rollback_journal_rejects_checksum_corruption() {
        let journal = V3RollbackJournal {
            header: V3JournalHeader::default(),
            original_free_pages: vec![],
            original_checksums: vec![],
            records: vec![V3JournalRecord {
                page_number: 3,
                page_bytes: vec![0x55; PAGE_SIZE],
            }],
        };

        let mut encoded = journal.encode().expect("encode journal");
        let checksum_index = V3_JOURNAL_HEADER_SIZE + 4;
        encoded[checksum_index] ^= 0x01;

        let error = V3RollbackJournal::decode(&encoded).expect_err("corruption should fail");
        assert!(
            error
                .to_string()
                .contains("v3 rollback journal record checksum mismatch"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn v3_rollback_journal_rejects_duplicate_pages() {
        let journal = V3RollbackJournal {
            header: V3JournalHeader::default(),
            original_free_pages: vec![],
            original_checksums: vec![],
            records: vec![
                V3JournalRecord {
                    page_number: 4,
                    page_bytes: vec![0x22; PAGE_SIZE],
                },
                V3JournalRecord {
                    page_number: 4,
                    page_bytes: vec![0x33; PAGE_SIZE],
                },
            ],
        };

        let error = journal.encode().expect_err("duplicate pages should fail");
        assert!(
            error
                .to_string()
                .contains("v3 rollback journal contains duplicate page 4"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn v3_rollback_journal_rejects_truncated_metadata_sections() {
        let journal = V3RollbackJournal {
            header: V3JournalHeader::default(),
            original_free_pages: vec![2],
            original_checksums: vec![(3, 10)],
            records: vec![],
        };

        let mut encoded = journal.encode().expect("encode journal");
        encoded.truncate(encoded.len() - 2);

        let error = V3RollbackJournal::decode(&encoded).expect_err("truncation should fail");
        assert!(
            error.to_string().contains("truncated"),
            "unexpected error: {error}"
        );
    }
}
