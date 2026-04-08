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
    pub(crate) dirty_sequence: Option<u64>,
}

#[derive(Debug, Clone)]
struct CachedPageEntry {
    page: Page,
    meta: CachedPageMeta,
}

#[derive(Debug, Clone)]
pub(crate) struct PageCache {
    capacity: usize,
    entries: HashMap<PageId, CachedPageEntry>,
    lru_order: VecDeque<PageId>,
    next_dirty_sequence: u64,
}

impl PageCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: HashMap::new(),
            lru_order: VecDeque::new(),
            next_dirty_sequence: 0,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(crate) fn get(&mut self, page_id: PageId) -> Option<&Page> {
        if self.entries.contains_key(&page_id) {
            self.touch(page_id);
            self.entries.get(&page_id).map(|entry| &entry.page)
        } else {
            None
        }
    }

    pub(crate) fn peek(&self, page_id: PageId) -> Option<&Page> {
        self.entries.get(&page_id).map(|entry| &entry.page)
    }

    pub(crate) fn meta(&self, page_id: PageId) -> Option<&CachedPageMeta> {
        self.entries.get(&page_id).map(|entry| &entry.meta)
    }

    pub(crate) fn put(&mut self, page: Page) {
        let page_id = page.id;
        self.evict_if_needed(Some(page_id));
        let meta = self
            .entries
            .remove(&page_id)
            .map(|entry| entry.meta)
            .unwrap_or_default();
        self.entries.insert(page_id, CachedPageEntry { page, meta });
        self.touch(page_id);
    }

    pub(crate) fn remove(&mut self, page_id: PageId) {
        self.entries.remove(&page_id);
        self.lru_order.retain(|&id| id != page_id);
    }

    pub(crate) fn reset(&mut self) {
        self.entries.clear();
        self.lru_order.clear();
        self.next_dirty_sequence = 0;
    }

    pub(crate) fn mark_dirty(&mut self, page_id: PageId) {
        let meta = &mut self.entries.entry(page_id).or_insert_with(|| CachedPageEntry {
            page: Page::new(page_id),
            meta: CachedPageMeta::default(),
        }).meta;
        if !meta.dirty {
            meta.dirty_sequence = Some(self.next_dirty_sequence);
            self.next_dirty_sequence = self.next_dirty_sequence.saturating_add(1);
        }
        meta.dirty = true;
        meta.writeable = true;
    }

    pub(crate) fn clear_dirty(&mut self, page_id: PageId) {
        if let Some(meta) = self.entries.get_mut(&page_id).map(|entry| &mut entry.meta) {
            meta.dirty = false;
            meta.writeable = false;
            meta.journaled = false;
            meta.need_sync = false;
            meta.dont_write = false;
            meta.dirty_sequence = None;
        }
    }

    pub(crate) fn is_dirty(&self, page_id: PageId) -> bool {
        self.meta(page_id).map(|meta| meta.dirty).unwrap_or(false)
    }

    pub(crate) fn dirty_page_ids(&self) -> Vec<PageId> {
        let mut dirty = self
            .entries
            .iter()
            .filter_map(|(page_id, entry)| {
                entry
                    .meta
                    .dirty
                    .then_some((*page_id, entry.meta.dirty_sequence.unwrap_or(u64::MAX)))
            })
            .collect::<Vec<_>>();
        dirty.sort_by_key(|(_, sequence)| *sequence);
        dirty.into_iter().map(|(page_id, _)| page_id).collect()
    }

    pub(crate) fn dirty_count(&self) -> usize {
        self.entries.values().filter(|entry| entry.meta.dirty).count()
    }

    pub(crate) fn pin(&mut self, page_id: PageId) {
        self.entries
            .entry(page_id)
            .or_insert_with(|| CachedPageEntry {
                page: Page::new(page_id),
                meta: CachedPageMeta::default(),
            })
            .meta
            .pin_count += 1;
    }

    #[allow(dead_code)]
    pub(crate) fn unpin(&mut self, page_id: PageId) {
        if let Some(meta) = self.entries.get_mut(&page_id).map(|entry| &mut entry.meta) {
            meta.pin_count = meta.pin_count.saturating_sub(1);
        }
    }

    #[allow(dead_code)]
    pub(crate) fn mark_journaled(&mut self, page_id: PageId) {
        self.entries
            .entry(page_id)
            .or_insert_with(|| CachedPageEntry {
                page: Page::new(page_id),
                meta: CachedPageMeta::default(),
            })
            .meta
            .journaled = true;
    }

    #[allow(dead_code)]
    pub(crate) fn mark_need_sync(&mut self, page_id: PageId) {
        self.entries
            .entry(page_id)
            .or_insert_with(|| CachedPageEntry {
                page: Page::new(page_id),
                meta: CachedPageMeta::default(),
            })
            .meta
            .need_sync = true;
    }

    #[allow(dead_code)]
    pub(crate) fn set_dont_write(&mut self, page_id: PageId, dont_write: bool) {
        self.entries
            .entry(page_id)
            .or_insert_with(|| CachedPageEntry {
                page: Page::new(page_id),
                meta: CachedPageMeta::default(),
            })
            .meta
            .dont_write = dont_write;
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
            if self.entries.contains_key(&page_id) {
                return;
            }
        }

        while self.entries.len() >= self.capacity {
            let candidate = self.lru_order.iter().rev().copied().find(|page_id| {
                self.entries
                    .get(page_id)
                    .map(|entry| !entry.meta.dirty && entry.meta.pin_count == 0)
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

    #[test]
    fn dirty_pages_keep_first_dirty_order() {
        let mut cache = PageCache::new(4);
        cache.put(Page::new(1));
        cache.put(Page::new(2));
        cache.put(Page::new(3));

        cache.mark_dirty(2);
        cache.mark_dirty(1);
        cache.mark_dirty(2);
        cache.mark_dirty(3);

        assert_eq!(cache.dirty_page_ids(), vec![2, 1, 3]);
    }

    #[test]
    fn peek_does_not_update_lru_order() {
        let mut cache = PageCache::new(2);
        cache.put(Page::new(1));
        cache.put(Page::new(2));

        assert!(cache.peek(1).is_some());
        cache.put(Page::new(3));

        assert!(cache.get(1).is_none());
        assert!(cache.get(2).is_some());
        assert!(cache.get(3).is_some());
    }

    #[test]
    fn cache_metadata_flags_track_page_state() {
        let mut cache = PageCache::new(2);
        cache.put(Page::new(1));

        cache.pin(1);
        cache.mark_dirty(1);
        cache.mark_journaled(1);
        cache.mark_need_sync(1);
        cache.set_dont_write(1, true);

        let meta = cache.meta(1).expect("page metadata should exist");
        assert_eq!(meta.pin_count, 1);
        assert!(meta.dirty);
        assert!(meta.writeable);
        assert!(meta.journaled);
        assert!(meta.need_sync);
        assert!(meta.dont_write);
        assert!(meta.dirty_sequence.is_some());

        cache.unpin(1);
        cache.clear_dirty(1);

        let meta = cache.meta(1).expect("page metadata should still exist");
        assert_eq!(meta.pin_count, 0);
        assert!(!meta.dirty);
        assert!(!meta.writeable);
        assert!(!meta.journaled);
        assert!(!meta.need_sync);
        assert!(!meta.dont_write);
        assert!(meta.dirty_sequence.is_none());
    }
}
