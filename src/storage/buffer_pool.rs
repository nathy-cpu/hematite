//! Buffer pool for in-memory page caching with LRU eviction

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_basic_operations() {
        let mut pool = BufferPool::new(3);
        let page_id = PageId::new(1);
        let page = Page::new(page_id);

        // Test empty pool
        assert!(pool.get(page_id).is_none());
        assert_eq!(pool.len(), 0);

        // Test put and get
        pool.put(page.clone());
        assert!(pool.get(page_id).is_some());
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn test_buffer_pool_lru_eviction() {
        let mut pool = BufferPool::new(2);

        // Fill pool to capacity
        let page1 = Page::new(PageId::new(1));
        let page2 = Page::new(PageId::new(2));
        pool.put(page1.clone());
        pool.put(page2.clone());

        assert_eq!(pool.len(), 2);

        // Add third page (should evict first)
        let page3 = Page::new(PageId::new(3));
        pool.put(page3.clone());

        assert_eq!(pool.len(), 2);
        assert!(pool.get(PageId::new(1)).is_none()); // Evicted
        assert!(pool.get(PageId::new(2)).is_some()); // Still present
        assert!(pool.get(PageId::new(3)).is_some()); // New page
    }

    #[test]
    fn test_buffer_pool_lru_update() {
        let mut pool = BufferPool::new(3);

        let page1 = Page::new(PageId::new(1));
        let page2 = Page::new(PageId::new(2));
        let page3 = Page::new(PageId::new(3));

        // Add pages
        pool.put(page1);
        pool.put(page2);
        pool.put(page3);

        // Access page1 (should make it most recently used)
        pool.get(PageId::new(1));

        // Add page4 (should evict page2, not page1)
        let page4 = Page::new(PageId::new(4));
        pool.put(page4);

        assert!(pool.get(PageId::new(1)).is_some()); // Still present (accessed)
        assert!(pool.get(PageId::new(2)).is_none()); // Evicted (least recently used)
        assert!(pool.get(PageId::new(3)).is_some()); // Still present
        assert!(pool.get(PageId::new(4)).is_some()); // New page
    }

    #[test]
    fn test_buffer_pool_update_existing() {
        let mut pool = BufferPool::new(2);

        let page_id = PageId::new(1);
        let page1 = Page::new(page_id);
        let mut page2 = Page::new(page_id);
        page2.data[0] = 42; // Modified page

        // Add first page
        pool.put(page1);
        assert_eq!(pool.get(page_id).unwrap().data[0], 0);

        // Update with modified page
        pool.put(page2);
        assert_eq!(pool.get(page_id).unwrap().data[0], 42);
        assert_eq!(pool.len(), 1); // Still only one page
    }

    #[test]
    fn test_buffer_pool_remove() {
        let mut pool = BufferPool::new(3);

        let page1 = Page::new(PageId::new(1));
        let page2 = Page::new(PageId::new(2));

        pool.put(page1);
        pool.put(page2);

        assert_eq!(pool.len(), 2);

        // Remove page1
        pool.remove(PageId::new(1));
        assert_eq!(pool.len(), 1);
        assert!(pool.get(PageId::new(1)).is_none());
        assert!(pool.get(PageId::new(2)).is_some());

        // Remove non-existent page
        pool.remove(PageId::new(999));
        assert_eq!(pool.len(), 1); // No change
    }

    #[test]
    fn test_buffer_pool_capacity_zero() {
        let mut pool = BufferPool::new(0);

        let page = Page::new(PageId::new(1));
        pool.put(page);

        // Pool should remain empty since capacity is 0
        assert_eq!(pool.len(), 0);
        assert!(pool.get(PageId::new(1)).is_none());
    }
}
