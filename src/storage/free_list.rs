//! Free-page bookkeeping for the storage layer.
//!
//! The freelist is small but important because page reuse, file growth, and compaction all depend
//! on it being correct.
//!
//! Data structure:
//!
//! ```text
//! Vec<PageId>
//!   tail = next page returned by reuse
//! ```
//!
//! Behavior:
//! - `push_free_page` is idempotent, so a page should never appear twice;
//! - `pop_free_page` returns pages in LIFO order, which tends to reuse recently-freed pages first;
//! - `compact_trailing_pages` removes free ids that form a suffix at the file high-water mark,
//!   allowing the file to shrink without moving live pages.
//!
//! Persistence format is versioned so the pager can reject freelist metadata that belongs to an
//! incompatible storage format.

use crate::error::Result;
use crate::storage::PageId;

#[derive(Debug, Clone, Default)]
pub struct FreeList {
    pages: Vec<PageId>,
}

impl FreeList {
    pub const METADATA_VERSION: u32 = 1;

    pub fn new() -> Self {
        Self { pages: Vec::new() }
    }

    pub fn pop_free_page(&mut self) -> Option<PageId> {
        self.pages.pop()
    }

    pub fn push_free_page(&mut self, page_id: PageId) {
        if !self.pages.contains(&page_id) {
            self.pages.push(page_id);
        }
    }

    pub fn as_slice(&self) -> &[PageId] {
        &self.pages
    }

    pub fn replace(&mut self, free_pages: Vec<PageId>) {
        self.pages = free_pages;
    }

    pub fn from_page_ids(pages: Vec<PageId>) -> Self {
        Self { pages }
    }

    pub fn into_page_ids(self) -> Vec<PageId> {
        self.pages
    }

    pub fn compact_trailing_pages(&mut self, next_page_id: &mut u32, minimum_next_page_id: u32) {
        while *next_page_id > minimum_next_page_id {
            let candidate = *next_page_id - 1;
            if let Some(position) = self.pages.iter().position(|page_id| *page_id == candidate) {
                self.pages.swap_remove(position);
                *next_page_id -= 1;
            } else {
                break;
            }
        }
    }

    pub fn serialize_metadata_lines(&self) -> Vec<String> {
        let mut lines = vec![
            format!("freelist_version={}", Self::METADATA_VERSION),
            format!("freelist_count={}", self.pages.len()),
        ];

        let mut pages = self.pages.clone();
        pages.sort_by_key(|page_id| *page_id);
        for page_id in pages {
            lines.push(format!("freelist|{}", page_id));
        }

        lines
    }

    pub fn deserialize_metadata_lines(
        version: u32,
        expected_count: usize,
        records: &[String],
    ) -> Result<Self> {
        if version != Self::METADATA_VERSION {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Unsupported freelist metadata version: expected {}, got {}",
                Self::METADATA_VERSION,
                version
            )));
        }

        let mut pages = Vec::with_capacity(records.len());
        for record in records {
            let payload = record.strip_prefix("freelist|").ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Invalid freelist metadata record prefix".to_string(),
                )
            })?;

            let page_id = payload.parse::<u32>().map_err(|_| {
                crate::error::HematiteError::StorageError(
                    "Invalid freelist page id metadata".to_string(),
                )
            })?;

            if pages.contains(&page_id) {
                return Err(crate::error::HematiteError::StorageError(format!(
                    "Duplicate freelist page id {} in metadata",
                    page_id
                )));
            }

            pages.push(page_id);
        }

        if pages.len() != expected_count {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Freelist metadata count mismatch: expected {}, got {}",
                expected_count,
                pages.len()
            )));
        }

        Ok(Self { pages })
    }
}
