//! Free page list structure used by the storage layer.
//!
//! M1.3 contract:
//! - Encapsulates free-page bookkeeping behind a dedicated type instead of raw vectors.
//! - Ensures idempotent deallocation tracking.
//! - Supports trailing high-water compaction with `next_page_id`.

use crate::storage::PageId;

#[derive(Debug, Clone, Default)]
pub struct FreeList {
    pages: Vec<PageId>,
}

impl FreeList {
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

    pub fn compact_trailing_pages(&mut self, next_page_id: &mut u32, minimum_next_page_id: u32) {
        while *next_page_id > minimum_next_page_id {
            let candidate = PageId::new(*next_page_id - 1);
            if let Some(position) = self.pages.iter().position(|page_id| *page_id == candidate) {
                self.pages.swap_remove(position);
                *next_page_id -= 1;
            } else {
                break;
            }
        }
    }
}
