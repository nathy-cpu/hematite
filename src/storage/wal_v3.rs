#![allow(dead_code)]

use crate::error::{HematiteError, Result};
use crate::storage::PAGE_SIZE;
use std::collections::BTreeMap;

const V3_WAL_MAGIC: &[u8; 4] = b"HTW3";
const V3_WAL_VERSION: u32 = 1;
const V3_WAL_HEADER_SIZE: usize = 24;
const V3_WAL_FRAME_PREFIX_SIZE: usize = 28;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V3WalHeader {
    pub(crate) page_size: u16,
    pub(crate) checkpoint_sequence: u32,
    pub(crate) salt_1: u32,
    pub(crate) salt_2: u32,
}

impl Default for V3WalHeader {
    fn default() -> Self {
        Self {
            page_size: PAGE_SIZE as u16,
            checkpoint_sequence: 0,
            salt_1: 0x48454D41,
            salt_2: 0x54495445,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V3WalFrame {
    pub(crate) page_number: u32,
    pub(crate) database_page_count: u32,
    pub(crate) commit_sequence: u64,
    pub(crate) page_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V3WalFile {
    pub(crate) header: V3WalHeader,
    pub(crate) frames: Vec<V3WalFrame>,
}

impl V3WalHeader {
    pub(crate) fn encode(&self) -> [u8; V3_WAL_HEADER_SIZE] {
        let mut bytes = [0u8; V3_WAL_HEADER_SIZE];
        bytes[..4].copy_from_slice(V3_WAL_MAGIC);
        bytes[4..8].copy_from_slice(&V3_WAL_VERSION.to_be_bytes());
        bytes[8..10].copy_from_slice(&self.page_size.to_be_bytes());
        bytes[10..12].fill(0);
        bytes[12..16].copy_from_slice(&self.checkpoint_sequence.to_be_bytes());
        bytes[16..20].copy_from_slice(&self.salt_1.to_be_bytes());
        bytes[20..24].copy_from_slice(&self.salt_2.to_be_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < V3_WAL_HEADER_SIZE {
            return Err(HematiteError::StorageError(
                "v3 WAL header is truncated".to_string(),
            ));
        }
        if &bytes[..4] != V3_WAL_MAGIC {
            return Err(HematiteError::StorageError(
                "v3 WAL header magic mismatch".to_string(),
            ));
        }
        let version = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if version != V3_WAL_VERSION {
            return Err(HematiteError::StorageError(format!(
                "Unsupported v3 WAL version {version}"
            )));
        }

        let page_size = u16::from_be_bytes([bytes[8], bytes[9]]);
        if page_size as usize != PAGE_SIZE {
            return Err(HematiteError::StorageError(format!(
                "Unsupported v3 WAL page size {page_size}"
            )));
        }

        Ok(Self {
            page_size,
            checkpoint_sequence: u32::from_be_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
            salt_1: u32::from_be_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]),
            salt_2: u32::from_be_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]),
        })
    }
}

impl V3WalFrame {
    pub(crate) fn encode(&self, header: &V3WalHeader) -> Result<Vec<u8>> {
        if self.page_number == 0 {
            return Err(HematiteError::StorageError(
                "v3 WAL frame page number must be 1-based".to_string(),
            ));
        }
        if self.page_bytes.len() != PAGE_SIZE {
            return Err(HematiteError::StorageError(format!(
                "v3 WAL frame for page {} has invalid image size {}",
                self.page_number,
                self.page_bytes.len()
            )));
        }

        let mut bytes = Vec::with_capacity(V3_WAL_FRAME_PREFIX_SIZE + PAGE_SIZE);
        bytes.extend_from_slice(&self.page_number.to_be_bytes());
        bytes.extend_from_slice(&self.database_page_count.to_be_bytes());
        bytes.extend_from_slice(&self.commit_sequence.to_be_bytes());
        bytes.extend_from_slice(&header.salt_1.to_be_bytes());
        bytes.extend_from_slice(&header.salt_2.to_be_bytes());
        let checksum = frame_checksum(
            self.page_number,
            self.database_page_count,
            self.commit_sequence,
            header.salt_1,
            header.salt_2,
            &self.page_bytes,
        );
        bytes.extend_from_slice(&checksum.to_be_bytes());
        bytes.extend_from_slice(&self.page_bytes);
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8], header: &V3WalHeader) -> Result<(Self, usize)> {
        if bytes.len() < V3_WAL_FRAME_PREFIX_SIZE + PAGE_SIZE {
            return Err(HematiteError::StorageError(
                "v3 WAL frame is truncated".to_string(),
            ));
        }

        let page_number = read_u32_be(bytes, 0);
        let database_page_count = read_u32_be(bytes, 4);
        let commit_sequence = read_u64_be(bytes, 8);
        let salt_1 = read_u32_be(bytes, 16);
        let salt_2 = read_u32_be(bytes, 20);
        let checksum = read_u32_be(bytes, 24);
        let page_bytes = bytes[28..28 + PAGE_SIZE].to_vec();

        if salt_1 != header.salt_1 || salt_2 != header.salt_2 {
            return Err(HematiteError::StorageError(
                "v3 WAL frame salt mismatch".to_string(),
            ));
        }

        let expected_checksum = frame_checksum(
            page_number,
            database_page_count,
            commit_sequence,
            salt_1,
            salt_2,
            &page_bytes,
        );
        if checksum != expected_checksum {
            return Err(HematiteError::StorageError(
                "v3 WAL frame checksum mismatch".to_string(),
            ));
        }

        Ok((
            Self {
                page_number,
                database_page_count,
                commit_sequence,
                page_bytes,
            },
            V3_WAL_FRAME_PREFIX_SIZE + PAGE_SIZE,
        ))
    }
}

impl V3WalFile {
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(
            V3_WAL_HEADER_SIZE + self.frames.len() * (V3_WAL_FRAME_PREFIX_SIZE + PAGE_SIZE),
        );
        bytes.extend_from_slice(&self.header.encode());
        for frame in &self.frames {
            bytes.extend_from_slice(&frame.encode(&self.header)?);
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let header = V3WalHeader::decode(bytes)?;
        let mut offset = V3_WAL_HEADER_SIZE;
        let mut frames = Vec::new();
        while offset < bytes.len() {
            let (frame, used) = V3WalFrame::decode(&bytes[offset..], &header)?;
            frames.push(frame);
            offset += used;
        }
        validate_frame_order(&frames)?;
        Ok(Self { header, frames })
    }

    pub(crate) fn latest_commit_sequence(&self) -> Option<u64> {
        self.frames.last().map(|frame| frame.commit_sequence)
    }

    pub(crate) fn visible_pages_at(
        &self,
        commit_sequence: u64,
    ) -> Result<(u32, BTreeMap<u32, Vec<u8>>)> {
        let mut max_db_page_count = 0u32;
        let mut pages = BTreeMap::new();
        for frame in self
            .frames
            .iter()
            .filter(|frame| frame.commit_sequence <= commit_sequence)
        {
            max_db_page_count = max_db_page_count.max(frame.database_page_count);
            pages.insert(frame.page_number, frame.page_bytes.clone());
        }
        Ok((max_db_page_count, pages))
    }
}

fn validate_frame_order(frames: &[V3WalFrame]) -> Result<()> {
    let mut previous_commit = 0u64;
    for frame in frames {
        if frame.page_number == 0 {
            return Err(HematiteError::StorageError(
                "v3 WAL frame page number must be 1-based".to_string(),
            ));
        }
        if frame.commit_sequence < previous_commit {
            return Err(HematiteError::StorageError(
                "v3 WAL frames are not ordered by commit sequence".to_string(),
            ));
        }
        previous_commit = frame.commit_sequence;
    }
    Ok(())
}

fn frame_checksum(
    page_number: u32,
    database_page_count: u32,
    commit_sequence: u64,
    salt_1: u32,
    salt_2: u32,
    page_bytes: &[u8],
) -> u32 {
    let mut bytes = Vec::with_capacity(4 + 4 + 8 + 4 + 4 + page_bytes.len());
    bytes.extend_from_slice(&page_number.to_be_bytes());
    bytes.extend_from_slice(&database_page_count.to_be_bytes());
    bytes.extend_from_slice(&commit_sequence.to_be_bytes());
    bytes.extend_from_slice(&salt_1.to_be_bytes());
    bytes.extend_from_slice(&salt_2.to_be_bytes());
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

fn read_u64_be(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

#[cfg(test)]
mod tests {
    use super::{V3WalFile, V3WalFrame, V3WalHeader};

    #[test]
    fn v3_wal_roundtrip() {
        let wal = V3WalFile {
            header: V3WalHeader::default(),
            frames: vec![
                V3WalFrame {
                    page_number: 1,
                    database_page_count: 3,
                    commit_sequence: 1,
                    page_bytes: vec![7u8; crate::storage::PAGE_SIZE],
                },
                V3WalFrame {
                    page_number: 2,
                    database_page_count: 3,
                    commit_sequence: 1,
                    page_bytes: vec![9u8; crate::storage::PAGE_SIZE],
                },
                V3WalFrame {
                    page_number: 2,
                    database_page_count: 4,
                    commit_sequence: 2,
                    page_bytes: vec![11u8; crate::storage::PAGE_SIZE],
                },
            ],
        };

        let encoded = wal.encode().unwrap();
        let decoded = V3WalFile::decode(&encoded).unwrap();
        assert_eq!(decoded, wal);
    }

    #[test]
    fn v3_wal_reconstructs_visible_pages_for_commit_boundary() {
        let wal = V3WalFile {
            header: V3WalHeader::default(),
            frames: vec![
                V3WalFrame {
                    page_number: 1,
                    database_page_count: 3,
                    commit_sequence: 1,
                    page_bytes: vec![1u8; crate::storage::PAGE_SIZE],
                },
                V3WalFrame {
                    page_number: 2,
                    database_page_count: 3,
                    commit_sequence: 1,
                    page_bytes: vec![2u8; crate::storage::PAGE_SIZE],
                },
                V3WalFrame {
                    page_number: 2,
                    database_page_count: 4,
                    commit_sequence: 2,
                    page_bytes: vec![3u8; crate::storage::PAGE_SIZE],
                },
            ],
        };

        let (page_count_1, visible_1) = wal.visible_pages_at(1).unwrap();
        assert_eq!(page_count_1, 3);
        assert_eq!(visible_1.get(&2).unwrap()[0], 2);

        let (page_count_2, visible_2) = wal.visible_pages_at(2).unwrap();
        assert_eq!(page_count_2, 4);
        assert_eq!(visible_2.get(&2).unwrap()[0], 3);
    }

    #[test]
    fn v3_wal_rejects_checksum_corruption() {
        let wal = V3WalFile {
            header: V3WalHeader::default(),
            frames: vec![V3WalFrame {
                page_number: 1,
                database_page_count: 3,
                commit_sequence: 1,
                page_bytes: vec![7u8; crate::storage::PAGE_SIZE],
            }],
        };

        let mut encoded = wal.encode().unwrap();
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF;

        let err = V3WalFile::decode(&encoded).unwrap_err();
        assert!(err.to_string().contains("checksum mismatch"));
    }
}
