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

#[cfg(test)]
use crate::error::Result;
use crate::storage::PageId;
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct FreeList {
    pages: Vec<PageId>,
    positions: HashMap<PageId, usize>,
}

impl FreeList {
    #[cfg(test)]
    pub const METADATA_VERSION: u32 = 1;

    pub fn new() -> Self {
        Self {
            pages: Vec::new(),
            positions: HashMap::new(),
        }
    }

    pub fn pop_free_page(&mut self) -> Option<PageId> {
        let page_id = self.pages.pop()?;
        self.positions.remove(&page_id);
        Some(page_id)
    }

    pub fn push_free_page(&mut self, page_id: PageId) {
        if self.positions.contains_key(&page_id) {
            return;
        }
        let position = self.pages.len();
        self.pages.push(page_id);
        self.positions.insert(page_id, position);
    }

    pub fn contains(&self, page_id: PageId) -> bool {
        self.positions.contains_key(&page_id)
    }

    pub fn remove_page(&mut self, page_id: PageId) -> bool {
        let Some(position) = self.positions.remove(&page_id) else {
            return false;
        };

        let Some(last_page_id) = self.pages.pop() else {
            return false;
        };
        if position < self.pages.len() {
            self.pages[position] = last_page_id;
            self.positions.insert(last_page_id, position);
        }
        true
    }

    pub fn as_slice(&self) -> &[PageId] {
        &self.pages
    }

    pub fn replace(&mut self, free_pages: Vec<PageId>) {
        self.pages.clear();
        self.positions.clear();
        for page_id in free_pages {
            if self.positions.contains_key(&page_id) {
                continue;
            }
            let position = self.pages.len();
            self.pages.push(page_id);
            self.positions.insert(page_id, position);
        }
    }

    pub fn compact_trailing_pages(&mut self, next_page_id: &mut u32, minimum_next_page_id: u32) {
        while *next_page_id > minimum_next_page_id {
            let candidate = *next_page_id - 1;
            if self.remove_page(candidate) {
                *next_page_id -= 1;
            } else {
                break;
            }
        }
    }

    #[cfg(test)]
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
        let mut positions = HashMap::with_capacity(records.len());
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

            if positions.contains_key(&page_id) {
                return Err(crate::error::HematiteError::StorageError(format!(
                    "Duplicate freelist page id {} in metadata",
                    page_id
                )));
            }

            positions.insert(page_id, pages.len());
            pages.push(page_id);
        }

        if pages.len() != expected_count {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Freelist metadata count mismatch: expected {}, got {}",
                expected_count,
                pages.len()
            )));
        }

        Ok(Self { pages, positions })
    }
}
