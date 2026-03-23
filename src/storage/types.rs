//! Core storage types and constants.
//!
//! This file defines the vocabulary shared by the storage layer:
//! - `PageId`: logical page address;
//! - `Page`: a full fixed-size page image;
//! - reserved page ids and page size;
//! - pager integrity reporting types used by higher layers.
//!
//! The key distinction is that a `Page` is just bytes plus an id. Any meaning attached to those
//! bytes belongs to higher layers such as the B-tree or catalog code.

use crate::error::Result;

pub const PAGE_SIZE: usize = 4096;

pub const DB_HEADER_PAGE_ID: u32 = 0;
pub const STORAGE_METADATA_PAGE_ID: u32 = 1;
pub const INVALID_PAGE_ID: u32 = u32::MAX;

pub type PageId = u32;

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
            return Err(crate::error::HematiteError::InvalidPage(id));
        }
        Ok(Self { id, data })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PagerIntegrityReport {
    pub allocated_page_count: usize,
    pub free_page_count: usize,
    pub fragmented_free_page_count: usize,
    pub trailing_free_page_count: usize,
    pub checksummed_page_count: usize,
    pub verified_checksum_pages: usize,
}
