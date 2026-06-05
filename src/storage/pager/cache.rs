use crate::storage::{Page, PageId};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub(crate) struct CachedPageMeta {
    pub(crate) dirty: bool,
    pub(crate) writeable: bool,
    pub(crate) journaled: bool,
    pub(crate) need_sync: bool,
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
    /// Points to the first dirty-list entry whose `need_sync` is false.
    /// Used by `spillable_candidates()` to skip entries that still need
    /// a journal sync, turning spill-candidate lookup from O(n) to O(k)
    /// where k is the number of synced dirty pages.
    synced_head: Option<PageId>,
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
            synced_head: None,
        }
    }
    #[cfg(test)]
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

    #[allow(dead_code)]
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
    #[cfg(test)]
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
        self.synced_head = None;
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
            // If the page enters the dirty list with need_sync already false
            // (e.g. re-dirtied after a spill, or dirtied when the journal has
            // already been synced), register it with synced_head so
            // spillable_candidates() can find it.
            let needs_sync = self
                .entries
                .get(&page_id)
                .map(|e| e.meta.need_sync)
                .unwrap_or(true);
            if !needs_sync {
                self.maybe_update_synced_head(page_id);
            }
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
        let was_synced = self
            .entries
            .get(&page_id)
            .map(|e| !e.meta.need_sync)
            .unwrap_or(false);
        self.entries
            .get_mut(&page_id)
            .expect("entry should exist")
            .meta
            .need_sync = true;
        // If the page was the synced_head and is now marked as needing sync,
        // advance synced_head to the next dirty entry that doesn't need sync.
        if was_synced && self.synced_head == Some(page_id) {
            self.advance_synced_head_from(page_id);
        }
    }

    pub(crate) fn clear_need_sync(&mut self, page_id: PageId) {
        let was_needing_sync = self
            .entries
            .get(&page_id)
            .map(|e| e.meta.need_sync && e.meta.dirty)
            .unwrap_or(false);
        if let Some(entry) = self.entries.get_mut(&page_id) {
            entry.meta.need_sync = false;
        }
        // If this page just became synced and is on the dirty list,
        // it may be the new synced_head if it appears earlier than the current one.
        if was_needing_sync {
            self.maybe_update_synced_head(page_id);
        }
    }

    pub(crate) fn set_view_token(&mut self, page_id: PageId, view_token: u64) {
        if let Some(entry) = self.entries.get_mut(&page_id) {
            entry.meta.view_token = view_token;
        }
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
    ///
    /// Uses the `synced_head` pointer to start iteration from the first dirty entry
    /// known to not need a journal sync, skipping all need-sync entries (O(k) instead
    /// of O(n) where k = synced dirty pages).
    pub(crate) fn spillable_candidates(&self) -> Vec<PageId> {
        let mut candidates = Vec::new();
        let mut current = self.synced_head;
        while let Some(page_id) = current {
            let entry = self
                .entries
                .get(&page_id)
                .expect("dirty entry should exist");
            // synced_head guarantees !need_sync, but we still verify.
            if !entry.meta.need_sync && entry.meta.journaled && Self::entry_pin_count(entry) == 0 {
                candidates.push(page_id);
            }
            current = entry.dirty_next;
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
        Arc::strong_count(&entry.page).saturating_sub(1)
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
        // If the detached page is the synced_head, advance it.
        if self.synced_head == Some(page_id) {
            self.synced_head = next.and_then(|nid| {
                self.entries
                    .get(&nid)
                    .filter(|e| !e.meta.need_sync)
                    .map(|_| nid)
            });
            // If the immediate next page needs sync, find the real next synced page.
            if self.synced_head.is_none() && next.is_some() {
                self.advance_synced_head_from_next(next);
            }
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

    /// Walk forward from `page_id` through the dirty list to find the next entry
    /// with `need_sync = false`, and set it as `synced_head`. If none is found,
    /// `synced_head` is set to `None`.
    fn advance_synced_head_from(&mut self, page_id: PageId) {
        let next = self.entries.get(&page_id).and_then(|e| e.dirty_next);
        self.advance_synced_head_from_next(next);
    }

    /// Walk forward from `start` through the dirty list to find the first entry
    /// with `need_sync = false`.
    fn advance_synced_head_from_next(&mut self, start: Option<PageId>) {
        let mut cursor = start;
        while let Some(pid) = cursor {
            if let Some(entry) = self.entries.get(&pid) {
                if !entry.meta.need_sync {
                    self.synced_head = Some(pid);
                    return;
                }
                cursor = entry.dirty_next;
            } else {
                break;
            }
        }
        self.synced_head = None;
    }

    /// If `page_id` is on the dirty list and its `need_sync` is now false,
    /// check whether it should become the new `synced_head` (i.e., it appears
    /// earlier in the dirty list than the current synced_head, or there is no
    /// current synced_head).
    fn maybe_update_synced_head(&mut self, page_id: PageId) {
        let is_dirty = self
            .entries
            .get(&page_id)
            .map(|e| e.meta.dirty && !e.meta.need_sync)
            .unwrap_or(false);
        if !is_dirty {
            return;
        }

        let Some(current_head) = self.synced_head else {
            // No synced_head yet — this page is the first.
            self.synced_head = Some(page_id);
            return;
        };

        // Check if page_id appears before current_head in the dirty list
        // by comparing dirty_sequence numbers (lower = earlier in list).
        let page_seq = self
            .entries
            .get(&page_id)
            .and_then(|e| e.meta.dirty_sequence);
        let head_seq = self
            .entries
            .get(&current_head)
            .and_then(|e| e.meta.dirty_sequence);

        match (page_seq, head_seq) {
            (Some(ps), Some(hs)) if ps < hs => {
                self.synced_head = Some(page_id);
            }
            _ => {}
        }
    }
}
