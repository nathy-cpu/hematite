//! Core storage types and constants

use crate::error::Result;

pub const PAGE_SIZE: usize = 4096; // 4KB pages
pub const MAX_ROWS_PER_PAGE: usize = 100; // Approximate, depends on row size

/// Table storage constants
pub const TABLE_METADATA_PAGE_ID: PageId = PageId::new(0);

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
