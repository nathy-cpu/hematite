//! In-memory page cache with LRU eviction.
//!
//! The buffer pool keeps recently-used pages in memory so the pager can avoid rereading them from
//! the backing file. It is intentionally simple:
//!
//! ```text
//! HashMap<PageId, Page>   = page storage
//! VecDeque<PageId>        = recency order
//! front                   = most recently used
//! back                    = eviction candidate
//! ```
//!
//! The pool does not know about dirty/clean semantics. The pager owns that policy and uses the
//! pool purely as a cache.

use crate::storage::{Page, PageId};
use std::collections::{HashMap, VecDeque};

#[derive(Debug)]
pub struct BufferPool {
    capacity: usize,
    pages: HashMap<PageId, Page>,
    lru_order: VecDeque<PageId>,
}

impl BufferPool {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity,
            pages: HashMap::new(),
            lru_order: VecDeque::new(),
        }
    }

    pub fn get(&mut self, page_id: PageId) -> Option<&Page> {
        if self.pages.contains_key(&page_id) {
            // Move to front (most recently used)
            self.update_lru(page_id);
            return self.pages.get(&page_id);
        } else {
            return None;
        }
    }

    pub fn put(&mut self, page: Page) {
        let page_id = page.id;

        // If page already exists, update it
        if self.pages.contains_key(&page_id) {
            self.pages.insert(page_id, page);
            self.update_lru(page_id);
            return;
        }

        // If at capacity, evict least recently used page
        if self.pages.len() >= self.capacity && self.capacity > 0 {
            if let Some(lru_id) = self.lru_order.pop_back() {
                self.pages.remove(&lru_id);
            }
        }

        // Only add page if capacity allows (and capacity > 0)
        if self.capacity > 0 && (self.pages.len() < self.capacity) {
            // Add new page to front
            self.pages.insert(page_id, page);
            self.lru_order.push_front(page_id);
        }
    }

    fn update_lru(&mut self, page_id: PageId) {
        // Remove from current position
        self.lru_order.retain(|&id| id != page_id);
        // Add to front (most recently used)
        self.lru_order.push_front(page_id);
    }

    pub fn clear(&mut self) {
        self.pages.clear();
        self.lru_order.clear();
    }

    pub fn remove(&mut self, page_id: PageId) {
        self.pages.remove(&page_id);
        // Remove from LRU order
        self.lru_order.retain(|&id| id != page_id);
    }

    pub fn len(&self) -> usize {
        self.pages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }
}
