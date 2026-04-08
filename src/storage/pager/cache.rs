use crate::storage::{Page, PageId};
use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone, Default)]
pub(crate) struct CachedPageMeta {
    pub(crate) pin_count: usize,
    pub(crate) dirty: bool,
    pub(crate) writeable: bool,
    pub(crate) journaled: bool,
    pub(crate) need_sync: bool,
    pub(crate) dont_write: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct PageCache {
    capacity: usize,
    pages: HashMap<PageId, Page>,
    meta: HashMap<PageId, CachedPageMeta>,
    lru_order: VecDeque<PageId>,
}

impl PageCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            pages: HashMap::new(),
            meta: HashMap::new(),
            lru_order: VecDeque::new(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(crate) fn get(&mut self, page_id: PageId) -> Option<&Page> {
        if self.pages.contains_key(&page_id) {
            self.touch(page_id);
            self.pages.get(&page_id)
        } else {
            None
        }
    }

    pub(crate) fn put(&mut self, page: Page) {
        let page_id = page.id;
        self.evict_if_needed(Some(page_id));
        self.pages.insert(page_id, page);
        self.meta.entry(page_id).or_default();
        self.touch(page_id);
    }

    pub(crate) fn remove(&mut self, page_id: PageId) {
        self.pages.remove(&page_id);
        self.meta.remove(&page_id);
        self.lru_order.retain(|&id| id != page_id);
    }

    pub(crate) fn reset(&mut self) {
        self.pages.clear();
        self.meta.clear();
        self.lru_order.clear();
    }

    pub(crate) fn mark_dirty(&mut self, page_id: PageId) {
        let meta = self.meta.entry(page_id).or_default();
        meta.dirty = true;
        meta.writeable = true;
    }

    pub(crate) fn clear_dirty(&mut self, page_id: PageId) {
        if let Some(meta) = self.meta.get_mut(&page_id) {
            meta.dirty = false;
            meta.writeable = false;
            meta.journaled = false;
            meta.need_sync = false;
            meta.dont_write = false;
        }
    }

    pub(crate) fn is_dirty(&self, page_id: PageId) -> bool {
        self.meta.get(&page_id).map(|meta| meta.dirty).unwrap_or(false)
    }

    pub(crate) fn dirty_page_ids(&self) -> Vec<PageId> {
        self.meta
            .iter()
            .filter_map(|(page_id, meta)| meta.dirty.then_some(*page_id))
            .collect()
    }

    pub(crate) fn dirty_count(&self) -> usize {
        self.meta.values().filter(|meta| meta.dirty).count()
    }

    pub(crate) fn pin(&mut self, page_id: PageId) {
        self.meta.entry(page_id).or_default().pin_count += 1;
    }

    #[allow(dead_code)]
    pub(crate) fn unpin(&mut self, page_id: PageId) {
        if let Some(meta) = self.meta.get_mut(&page_id) {
            meta.pin_count = meta.pin_count.saturating_sub(1);
        }
    }

    #[allow(dead_code)]
    pub(crate) fn mark_journaled(&mut self, page_id: PageId) {
        self.meta.entry(page_id).or_default().journaled = true;
    }

    #[allow(dead_code)]
    pub(crate) fn mark_need_sync(&mut self, page_id: PageId) {
        self.meta.entry(page_id).or_default().need_sync = true;
    }

    #[allow(dead_code)]
    pub(crate) fn set_dont_write(&mut self, page_id: PageId, dont_write: bool) {
        self.meta.entry(page_id).or_default().dont_write = dont_write;
    }

    fn touch(&mut self, page_id: PageId) {
        self.lru_order.retain(|&id| id != page_id);
        self.lru_order.push_front(page_id);
    }

    fn evict_if_needed(&mut self, incoming_page_id: Option<PageId>) {
        if self.capacity == 0 {
            self.reset();
            return;
        }

        if let Some(page_id) = incoming_page_id {
            if self.pages.contains_key(&page_id) {
                return;
            }
        }

        while self.pages.len() >= self.capacity {
            let candidate = self.lru_order.iter().rev().copied().find(|page_id| {
                self.meta
                    .get(page_id)
                    .map(|meta| !meta.dirty && meta.pin_count == 0)
                    .unwrap_or(true)
            });

            let Some(page_id) = candidate else {
                // If every cached page is pinned or dirty, prefer temporarily exceeding the target
                // over silently evicting write-critical state.
                break;
            };

            self.remove(page_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PageCache;
    use crate::storage::Page;

    #[test]
    fn dirty_pages_are_not_evicted_under_capacity_pressure() {
        let mut cache = PageCache::new(1);

        let mut first = Page::new(1);
        first.data[0] = 10;
        cache.put(first);
        cache.mark_dirty(1);

        let mut second = Page::new(2);
        second.data[0] = 20;
        cache.put(second);

        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_some());
        assert!(cache.is_dirty(1));
    }

    #[test]
    fn pinned_pages_are_not_evicted() {
        let mut cache = PageCache::new(1);
        cache.put(Page::new(1));
        cache.pin(1);
        cache.put(Page::new(2));

        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_some());
    }
}
