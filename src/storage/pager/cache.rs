use crate::storage::{Page, PageId};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub(crate) struct CachedPageMeta {
    pub(crate) manual_pin_count: usize,
    pub(crate) dirty: bool,
    pub(crate) writeable: bool,
    pub(crate) journaled: bool,
    pub(crate) need_sync: bool,
    pub(crate) dont_write: bool,
    pub(crate) dirty_sequence: Option<u64>,
    pub(crate) view_token: u64,
}

#[derive(Debug, Clone)]
struct CachedPageEntry {
    page: Arc<Page>,
    meta: CachedPageMeta,
    lru_prev: Option<PageId>,
    lru_next: Option<PageId>,
    dirty_prev: Option<PageId>,
    dirty_next: Option<PageId>,
}

#[derive(Debug, Clone)]
pub(crate) struct PageCache {
    capacity: usize,
    entries: HashMap<PageId, CachedPageEntry>,
    lru_head: Option<PageId>,
    lru_tail: Option<PageId>,
    dirty_head: Option<PageId>,
    dirty_tail: Option<PageId>,
    dirty_len: usize,
    next_dirty_sequence: u64,
}

impl PageCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: HashMap::new(),
            lru_head: None,
            lru_tail: None,
            dirty_head: None,
            dirty_tail: None,
            dirty_len: 0,
            next_dirty_sequence: 0,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    #[allow(dead_code)]
    pub(crate) fn get(&mut self, page_id: PageId) -> Option<Arc<Page>> {
        self.get_for_view(page_id, 0)
    }

    pub(crate) fn peek_for_view(&self, page_id: PageId, view_token: u64) -> Option<Arc<Page>> {
        self.entries.get(&page_id).and_then(|entry| {
            if entry.meta.dirty || entry.meta.writeable || entry.meta.view_token == view_token {
                Some(entry.page.clone())
            } else {
                None
            }
        })
    }

    pub(crate) fn get_for_view(&mut self, page_id: PageId, view_token: u64) -> Option<Arc<Page>> {
        let page = self.peek_for_view(page_id, view_token);
        if page.is_some() {
            self.touch(page_id);
        }
        page
    }

    pub(crate) fn put(&mut self, page: Page) {
        self.put_with_view(page, 0);
    }

    pub(crate) fn put_with_view(&mut self, page: Page, view_token: u64) {
        self.put_shared_with_view(Arc::new(page), view_token);
    }

    #[allow(dead_code)]
    pub(crate) fn put_shared(&mut self, page: Arc<Page>) {
        self.put_shared_with_view(page, 0);
    }

    pub(crate) fn put_shared_with_view(&mut self, page: Arc<Page>, view_token: u64) {
        let page_id = page.id;
        if !self.entries.contains_key(&page_id) {
            self.evict_if_needed(Some(page_id));
            self.entries.insert(
                page_id,
                CachedPageEntry {
                    page,
                    meta: CachedPageMeta {
                        view_token,
                        ..CachedPageMeta::default()
                    },
                    lru_prev: None,
                    lru_next: None,
                    dirty_prev: None,
                    dirty_next: None,
                },
            );
            self.attach_lru_front(page_id);
            return;
        }
        if let Some(entry) = self.entries.get_mut(&page_id) {
            entry.page = page;
            entry.meta.view_token = view_token;
        }
        self.touch(page_id);
    }

    pub(crate) fn peek(&self, page_id: PageId) -> Option<&Page> {
        self.entries.get(&page_id).map(|entry| entry.page.as_ref())
    }

    #[allow(dead_code)]
    pub(crate) fn peek_shared(&self, page_id: PageId) -> Option<Arc<Page>> {
        self.entries.get(&page_id).map(|entry| entry.page.clone())
    }

    pub(crate) fn meta(&self, page_id: PageId) -> Option<&CachedPageMeta> {
        self.entries.get(&page_id).map(|entry| &entry.meta)
    }

    #[cfg(test)]
    pub(crate) fn pin_count(&self, page_id: PageId) -> usize {
        self.entries
            .get(&page_id)
            .map(Self::entry_pin_count)
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn remove(&mut self, page_id: PageId) {
        if !self.entries.contains_key(&page_id) {
            return;
        }
        self.detach_lru(page_id);
        self.detach_dirty(page_id);
        self.entries.remove(&page_id);
    }

    pub(crate) fn reset(&mut self) {
        self.entries.clear();
        self.lru_head = None;
        self.lru_tail = None;
        self.dirty_head = None;
        self.dirty_tail = None;
        self.dirty_len = 0;
        self.next_dirty_sequence = 0;
    }

    pub(crate) fn mark_dirty(&mut self, page_id: PageId) {
        self.ensure_entry(page_id);
        let was_dirty = self
            .entries
            .get(&page_id)
            .map(|entry| entry.meta.dirty)
            .unwrap_or(false);
        let meta = &mut self
            .entries
            .get_mut(&page_id)
            .expect("entry should exist")
            .meta;
        if !meta.dirty {
            meta.dirty_sequence = Some(self.next_dirty_sequence);
            self.next_dirty_sequence = self.next_dirty_sequence.saturating_add(1);
        }
        meta.dirty = true;
        meta.writeable = true;
        if !was_dirty {
            self.attach_dirty_tail(page_id);
        }
    }

    pub(crate) fn clear_dirty(&mut self, page_id: PageId) {
        let was_dirty = self
            .entries
            .get(&page_id)
            .map(|entry| entry.meta.dirty)
            .unwrap_or(false);
        if was_dirty {
            self.detach_dirty(page_id);
        }
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
        let mut dirty = Vec::with_capacity(self.dirty_len);
        let mut current = self.dirty_head;
        while let Some(page_id) = current {
            dirty.push(page_id);
            current = self
                .entries
                .get(&page_id)
                .and_then(|entry| entry.dirty_next);
        }
        dirty
    }

    pub(crate) fn dirty_count(&self) -> usize {
        self.dirty_len
    }

    #[allow(dead_code)]
    pub(crate) fn pin(&mut self, page_id: PageId) {
        self.ensure_entry(page_id);
        self.entries
            .get_mut(&page_id)
            .expect("entry should exist")
            .meta
            .manual_pin_count += 1;
    }

    #[allow(dead_code)]
    pub(crate) fn unpin(&mut self, page_id: PageId) {
        if let Some(meta) = self.entries.get_mut(&page_id).map(|entry| &mut entry.meta) {
            meta.manual_pin_count = meta.manual_pin_count.saturating_sub(1);
        }
    }

    #[allow(dead_code)]
    pub(crate) fn mark_journaled(&mut self, page_id: PageId) {
        self.ensure_entry(page_id);
        self.entries
            .get_mut(&page_id)
            .expect("entry should exist")
            .meta
            .journaled = true;
    }

    #[allow(dead_code)]
    pub(crate) fn mark_need_sync(&mut self, page_id: PageId) {
        self.ensure_entry(page_id);
        self.entries
            .get_mut(&page_id)
            .expect("entry should exist")
            .meta
            .need_sync = true;
    }

    pub(crate) fn clear_need_sync(&mut self, page_id: PageId) {
        if let Some(entry) = self.entries.get_mut(&page_id) {
            entry.meta.need_sync = false;
        }
    }

    pub(crate) fn set_view_token(&mut self, page_id: PageId, view_token: u64) {
        if let Some(entry) = self.entries.get_mut(&page_id) {
            entry.meta.view_token = view_token;
        }
    }

    #[allow(dead_code)]
    pub(crate) fn set_dont_write(&mut self, page_id: PageId, dont_write: bool) {
        self.ensure_entry(page_id);
        self.entries
            .get_mut(&page_id)
            .expect("entry should exist")
            .meta
            .dont_write = dont_write;
    }

    fn touch(&mut self, page_id: PageId) {
        if self.lru_head == Some(page_id) || !self.entries.contains_key(&page_id) {
            return;
        }
        self.detach_lru(page_id);
        self.attach_lru_front(page_id);
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
            let mut candidate = self.lru_tail;
            while let Some(page_id) = candidate {
                let entry = self.entries.get(&page_id).expect("lru entry should exist");
                if !entry.meta.dirty && Self::entry_pin_count(entry) == 0 {
                    break;
                }
                candidate = entry.lru_prev;
            }

            let Some(page_id) = candidate else {
                // If every cached page is pinned or dirty, prefer temporarily exceeding the target
                // over silently evicting write-critical state.
                break;
            };

            self.remove(page_id);
        }
    }

    /// Returns pages that are dirty but have already been journaled, are not pinned,
    /// and do not require a journal sync — making them safe to spill (write through)
    /// to the database file to reclaim cache space.
    pub(crate) fn spillable_candidates(&self) -> Vec<PageId> {
        let mut candidates = Vec::new();
        let mut current = self.lru_tail;
        while let Some(page_id) = current {
            let entry = self.entries.get(&page_id).expect("lru entry should exist");
            if entry.meta.dirty
                && entry.meta.journaled
                && !entry.meta.need_sync
                && !entry.meta.dont_write
                && Self::entry_pin_count(entry) == 0
            {
                candidates.push(page_id);
            }
            current = entry.lru_prev;
        }
        candidates
    }

    /// Returns true if the cache is over capacity and has no clean pages to evict.
    pub(crate) fn needs_spill(&self) -> bool {
        if self.entries.len() < self.capacity {
            return false;
        }
        let mut current = self.lru_tail;
        while let Some(page_id) = current {
            let entry = self.entries.get(&page_id).expect("lru entry should exist");
            if !entry.meta.dirty && Self::entry_pin_count(entry) == 0 {
                return false;
            }
            current = entry.lru_prev;
        }
        true
    }

    fn entry_pin_count(entry: &CachedPageEntry) -> usize {
        let shared_pin_count = Arc::strong_count(&entry.page).saturating_sub(1);
        entry.meta.manual_pin_count + shared_pin_count
    }

    fn ensure_entry(&mut self, page_id: PageId) {
        if self.entries.contains_key(&page_id) {
            return;
        }
        self.evict_if_needed(Some(page_id));
        self.entries.insert(
            page_id,
            CachedPageEntry {
                page: Arc::new(Page::new(page_id)),
                meta: CachedPageMeta::default(),
                lru_prev: None,
                lru_next: None,
                dirty_prev: None,
                dirty_next: None,
            },
        );
        self.attach_lru_front(page_id);
    }

    /// Return an owned `Page` ready for in-place mutation by callers.
    /// This implements a simple copy-on-write helper at the cache level:
    /// - ensures a cache entry exists for `page_id`;
    /// - marks the cached entry as writeable;
    /// - returns a cloned `Page` (caller can mutate and then call `put`/write back).
    ///
    /// Note: this does not attempt in-place mutation of the cached Arc; it hands the
    /// caller an owned copy. Higher-level pager APIs can use this to avoid calling
    /// `entry.page.as_ref().clone()` at call sites and centralize the copy behavior.
    pub(crate) fn take_page_for_write(&mut self, page_id: PageId) -> Page {
        self.ensure_entry(page_id);
        let entry = self.entries.get_mut(&page_id).expect("entry should exist");
        entry.meta.writeable = true;
        // Clone the page bytes into an owned Page that the caller can mutate.
        (*entry.page).clone()
    }

    fn attach_lru_front(&mut self, page_id: PageId) {
        let old_head = self.lru_head;
        if let Some(entry) = self.entries.get_mut(&page_id) {
            entry.lru_prev = None;
            entry.lru_next = old_head;
        }
        if let Some(old_head_id) = old_head {
            if let Some(entry) = self.entries.get_mut(&old_head_id) {
                entry.lru_prev = Some(page_id);
            }
        } else {
            self.lru_tail = Some(page_id);
        }
        self.lru_head = Some(page_id);
    }

    fn detach_lru(&mut self, page_id: PageId) {
        let (prev, next) = match self.entries.get(&page_id) {
            Some(entry) => (entry.lru_prev, entry.lru_next),
            None => return,
        };
        match prev {
            Some(prev_id) => {
                if let Some(entry) = self.entries.get_mut(&prev_id) {
                    entry.lru_next = next;
                }
            }
            None => self.lru_head = next,
        }
        match next {
            Some(next_id) => {
                if let Some(entry) = self.entries.get_mut(&next_id) {
                    entry.lru_prev = prev;
                }
            }
            None => self.lru_tail = prev,
        }
        if let Some(entry) = self.entries.get_mut(&page_id) {
            entry.lru_prev = None;
            entry.lru_next = None;
        }
    }

    fn attach_dirty_tail(&mut self, page_id: PageId) {
        let old_tail = self.dirty_tail;
        if let Some(entry) = self.entries.get_mut(&page_id) {
            entry.dirty_prev = old_tail;
            entry.dirty_next = None;
        }
        match old_tail {
            Some(old_tail_id) => {
                if let Some(entry) = self.entries.get_mut(&old_tail_id) {
                    entry.dirty_next = Some(page_id);
                }
            }
            None => self.dirty_head = Some(page_id),
        }
        self.dirty_tail = Some(page_id);
        self.dirty_len = self.dirty_len.saturating_add(1);
    }

    fn detach_dirty(&mut self, page_id: PageId) {
        let (prev, next, was_dirty) = match self.entries.get(&page_id) {
            Some(entry) => (entry.dirty_prev, entry.dirty_next, entry.meta.dirty),
            None => return,
        };
        if !was_dirty {
            return;
        }
        match prev {
            Some(prev_id) => {
                if let Some(entry) = self.entries.get_mut(&prev_id) {
                    entry.dirty_next = next;
                }
            }
            None => self.dirty_head = next,
        }
        match next {
            Some(next_id) => {
                if let Some(entry) = self.entries.get_mut(&next_id) {
                    entry.dirty_prev = prev;
                }
            }
            None => self.dirty_tail = prev,
        }
        if let Some(entry) = self.entries.get_mut(&page_id) {
            entry.dirty_prev = None;
            entry.dirty_next = None;
        }
        self.dirty_len = self.dirty_len.saturating_sub(1);
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
        let held = cache.get(1).expect("page should be cached");
        cache.put(Page::new(2));

        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_some());
        drop(held);
    }

    #[test]
    fn shared_handles_count_as_live_pins_until_dropped() {
        let mut cache = PageCache::new(2);
        cache.put(Page::new(1));

        let held = cache.get(1).expect("page should be cached");
        assert_eq!(cache.pin_count(1), 1);

        cache.put(Page::new(2));
        assert!(cache.peek(1).is_some());
        assert!(cache.peek(2).is_some());

        drop(held);
        assert_eq!(cache.pin_count(1), 0);

        cache.put(Page::new(3));
        assert!(cache.peek(1).is_none());
        assert!(cache.peek(2).is_some());
        assert!(cache.peek(3).is_some());
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
    fn cache_hits_do_not_reorder_dirty_writeback_order() {
        let mut cache = PageCache::new(4);
        cache.put(Page::new(1));
        cache.put(Page::new(2));
        cache.put(Page::new(3));

        cache.mark_dirty(1);
        cache.mark_dirty(2);
        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_some());
        assert!(cache.get(1).is_some());

        assert_eq!(cache.dirty_page_ids(), vec![1, 2]);
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
        assert_eq!(meta.manual_pin_count, 1);
        assert!(meta.dirty);
        assert!(meta.writeable);
        assert!(meta.journaled);
        assert!(meta.need_sync);
        assert!(meta.dont_write);
        assert!(meta.dirty_sequence.is_some());
        assert_eq!(cache.pin_count(1), 1);

        cache.unpin(1);
        cache.clear_dirty(1);

        let meta = cache.meta(1).expect("page metadata should still exist");
        assert_eq!(meta.manual_pin_count, 0);
        assert!(!meta.dirty);
        assert!(!meta.writeable);
        assert!(!meta.journaled);
        assert!(!meta.need_sync);
        assert!(!meta.dont_write);
        assert!(meta.dirty_sequence.is_none());
        assert_eq!(cache.pin_count(1), 0);
    }

    #[test]
    fn view_tokens_filter_stale_read_hits_without_resetting_entries() {
        let mut cache = PageCache::new(2);
        let mut page = Page::new(1);
        page.data[0] = 7;
        cache.put_with_view(page, 11);

        assert!(cache.get_for_view(1, 11).is_some());
        assert!(cache.get_for_view(1, 12).is_none());

        cache.set_view_token(1, 12);
        let page = cache.get_for_view(1, 12).expect("page should be visible");
        assert_eq!(page.data[0], 7);
    }
}
