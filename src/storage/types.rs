//! Core storage types and constants.
//!
//! M0 storage contract notes:
//! - Single database file with 64-byte file header followed by fixed-size pages.
//! - Reserved logical pages:
//!   - page 0: database header
//!   - page 1: storage metadata
//! - The long-term target is a forest of B-trees: one catalog tree, one table tree per table,
//!   and one index tree per index.
//! - Row storage target: rowid-keyed table B-tree leaves with fixed cell layout and overflow
//!   pages for large payloads.
//! - Index storage target: indexed-key to rowid mappings.
//! - All durability-sensitive pages are expected to be checksummed.

use crate::error::Result;

pub const PAGE_SIZE: usize = 4096; // 4KB pages
pub const MAX_ROWS_PER_PAGE: usize = 100; // Approximate, depends on row size
pub const TABLE_PAGE_HEADER_SIZE: usize = 64;

/// Reserved page IDs for the single-file database layout.
///
/// Kept in `storage` to avoid higher-layer dependencies.
pub const DB_HEADER_PAGE_ID: PageId = PageId::new(0);
pub const STORAGE_METADATA_PAGE_ID: PageId = PageId::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId(u32);

impl PageId {
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }

    pub const fn invalid() -> Self {
        Self(u32::MAX)
    }
}

#[derive(Debug, Clone)]
pub struct Page {
    pub id: PageId,
    pub data: Vec<u8>,
}

impl Page {
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            data: vec![0u8; PAGE_SIZE],
        }
    }

    pub fn from_bytes(id: PageId, data: Vec<u8>) -> Result<Self> {
        if data.len() != PAGE_SIZE {
            return Err(crate::error::HematiteError::InvalidPage(id.as_u32()));
        }
        Ok(Self { id, data })
    }
}

// Table storage structures
#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub name: String,
    pub root_page_id: PageId,
    pub row_count: u64,
    pub next_row_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageStats {
    pub table_count: usize,
    pub total_rows: u64,
    pub free_page_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageIntegrityReport {
    pub table_count: usize,
    pub live_page_count: usize,
    pub free_page_count: usize,
    pub total_rows: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredRow {
    pub row_id: u64,
    pub values: Vec<crate::catalog::Value>,
}

#[derive(Debug, Clone)]
pub struct TablePageHeader {
    pub page_type: PageType,
    pub row_count: u32,
    pub next_page_id: PageId,
    pub prev_page_id: PageId,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageType {
    TableData,
    TableIndex,
    Free,
}
