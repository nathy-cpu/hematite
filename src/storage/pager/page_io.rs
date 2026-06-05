use super::{JournalMode, Pager, PagerLockMode, PagerState, STORAGE_METADATA_PAGE_ID};
use crate::error::Result;
use crate::storage::Page;
use crate::storage::PageId;
use std::sync::Arc;

impl Pager {
    pub fn read_page(&self, page_id: PageId) -> Result<Page> {
        Ok((*self.read_page_shared(page_id)?).clone())
    }

    pub(crate) fn read_page_shared(&self, page_id: PageId) -> Result<Arc<Page>> {
        self.check_error_state()?;
        let view_token = self.cache_view_token();
        if let Some(page) = self.cache_read()?.peek_for_view(page_id, view_token) {
            return Ok(page);
        }

        if let Some(transaction_next_page_id) = self.rollback_visible_next_page_id() {
            if page_id >= transaction_next_page_id || self.rollback_page_is_free(page_id) {
                return Err(crate::error::HematiteError::StorageError(format!(
                    "Page {} is not allocated in the active rollback transaction",
                    page_id
                )));
            }
            if self.rollback_page_is_uninitialized(page_id) {
                let page = Arc::new(Page::new(page_id));
                self.cache_write()?
                    .put_shared_with_view(page.clone(), view_token);
                return Ok(page);
            }
        }

        if self.journal_mode == JournalMode::Wal {
            if let Some(transaction) = self.active_wal_transaction() {
                if transaction.wal_free_page_set.contains(&page_id)
                    && page_id < transaction.wal_next_page_id
                {
                    return Err(crate::error::HematiteError::StorageError(format!(
                        "Page {} is deallocated in the active WAL transaction",
                        page_id
                    )));
                }

                let base_visible_state = self.latest_wal_state.as_ref();
                let base_visible_next_page_id = base_visible_state
                    .map(|state| state.visible_next_page_id())
                    .unwrap_or_else(|| self.file_manager.next_page_id());
                let base_page_is_free = base_visible_state
                    .map(|state| state.is_page_free(page_id))
                    .unwrap_or_else(|| self.file_manager.is_free_page(page_id));

                if (page_id >= base_visible_next_page_id || base_page_is_free)
                    && page_id < transaction.wal_next_page_id
                {
                    let page = Arc::new(Page::new(page_id));
                    self.cache_write()?
                        .put_shared_with_view(page.clone(), view_token);
                    return Ok(page);
                }
            }
        }

        if let Some(state) = self.current_wal_visible_state() {
            if !state.contains_page(page_id) {
                return Err(crate::error::HematiteError::StorageError(format!(
                    "Page {} is not allocated in the current WAL-visible state",
                    page_id
                )));
            }
            if let Some(data) = state.page_bytes(page_id) {
                let page = Arc::new(Page::from_bytes(page_id, data.to_vec())?);
                if let Some(expected_checksum) = state.checksum_for_page(page_id) {
                    let actual_checksum = Self::calculate_page_checksum(&page);
                    if actual_checksum != expected_checksum {
                        return Err(crate::error::HematiteError::CorruptedData(format!(
                            "WAL page checksum mismatch for page {}: expected {}, got {}",
                            page_id, expected_checksum, actual_checksum
                        )));
                    }
                }
                self.cache_write()?
                    .put_shared_with_view(page.clone(), view_token);
                return Ok(page);
            }
            let visible_next_page_id = state.visible_next_page_id();
            if page_id >= self.file_manager.next_page_id() && page_id < visible_next_page_id {
                let page = Arc::new(Page::new(page_id));
                self.cache_write()?
                    .put_shared_with_view(page.clone(), view_token);
                return Ok(page);
            }
        }

        let page = Arc::new(self.file_manager.read_page(page_id)?);
        let expected_checksum = self
            .current_wal_visible_state()
            .and_then(|state| state.checksum_for_page(page_id))
            .or_else(|| self.page_checksums.get(&page_id).copied());
        if let Some(expected_checksum) = expected_checksum {
            let actual_checksum = Self::calculate_page_checksum(&page);
            if actual_checksum != expected_checksum {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Page checksum mismatch for page {}: expected {}, got {}",
                    page_id, expected_checksum, actual_checksum
                )));
            }
        }
        self.cache_write()?
            .put_shared_with_view(page.clone(), view_token);
        Ok(page)
    }

    pub fn write_page(&mut self, page: Page) -> Result<()> {
        self.check_error_state()?;
        let page_id = page.id;
        self.snapshot_original_page(page_id)?;
        if let Some(transaction) = self.active_rollback_transaction_mut() {
            transaction.rollback_uninitialized_pages.remove(&page_id);
        }
        if page_id != STORAGE_METADATA_PAGE_ID {
            self.page_checksums
                .insert(page_id, Self::calculate_page_checksum(&page));
        }
        let rollback_transaction_active = self.active_rollback_transaction().is_some();
        let journal_needs_sync = self.journal_needs_sync;
        {
            let cache = self.cache_mut()?;
            cache.put(page);
            cache.mark_dirty(page_id);
            if rollback_transaction_active {
                // New pages do not have a pre-transaction image, so they are not journaled by
                // snapshot_original_page. Mark them as spill-eligible once the journal sync
                // barrier has been satisfied.
                cache.mark_journaled(page_id);
                if journal_needs_sync {
                    cache.mark_need_sync(page_id);
                }
            }
        }
        // Advance state from WriterLocked → WriterCacheMod on first cache
        // modification.  Skip if we have already advanced to WriterDbMod
        // (e.g. after a spill) — going backward would be an illegal transition.
        if self.transaction.is_some() && self.state != PagerState::WriterDbMod {
            self.transition_state(PagerState::WriterCacheMod)?;
        }

        // Spill already-journaled dirty pages to disk when the cache is over
        // capacity and no clean pages remain for eviction.
        if self.cache_read()?.needs_spill() {
            self.spill_pages()?;
        }

        Ok(())
    }

    /// Write already-journaled dirty pages through to the database file to
    /// reclaim cache space.  Only pages whose original images have already been
    /// captured in the rollback journal are eligible — so crash-recovery
    /// invariants are preserved.
    fn spill_pages(&mut self) -> Result<()> {
        if self.active_rollback_transaction().is_some() {
            self.sync_rollback_journal()?;
            self.apply_rollback_space_overlay_if_needed()?;
        }
        let candidates = self.cache_read()?.spillable_candidates();
        if candidates.is_empty() {
            return Ok(());
        }
        // Transition to WriterDbMod before writing pages to the database file.
        // The journal has been synced, so it is now safe to modify the db file.
        // WriterCacheMod → WriterDbMod and WriterDbMod → WriterDbMod are both
        // valid transitions, so this is safe to call unconditionally.
        if self.transaction.is_some() {
            self.transition_state(PagerState::WriterDbMod)?;
        }
        for page_id in candidates {
            // Skip metadata page — it must be written last during flush.
            if page_id == STORAGE_METADATA_PAGE_ID {
                continue;
            }
            let page = self.cache_read()?.peek_shared(page_id);
            if let Some(page) = page {
                if let Err(e) = self.file_manager.write_page(page.as_ref()) {
                    self.enter_error_state();
                    return Err(e);
                }
            }
            self.cache_mut()?.clear_dirty(page_id);
            // After clearing dirty, the page becomes a clean cache entry that
            // regular LRU eviction can reclaim.
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.check_error_state()?;
        if self.journal_mode == JournalMode::Wal && self.transaction.is_some() {
            return Err(crate::error::HematiteError::StorageError(
                "Cannot flush pager pages directly during an active WAL transaction".to_string(),
            ));
        }

        self.stage_persisted_state_page()?;
        if self.active_rollback_transaction().is_some() {
            self.sync_rollback_journal()?;
            self.apply_rollback_space_overlay_if_needed()?;
        }

        let dirty_ids = self.cache_read()?.dirty_page_ids();
        let mut metadata_page_dirty = false;

        for page_id in dirty_ids.iter().copied() {
            if page_id == STORAGE_METADATA_PAGE_ID {
                metadata_page_dirty = true;
                continue;
            }

            let page = self.cache_read()?.peek_shared(page_id);
            if let Some(page) = page {
                if let Err(e) = self.file_manager.write_page(page.as_ref()) {
                    self.enter_error_state();
                    return Err(e);
                }
            }
            self.cache_mut()?.clear_dirty(page_id);
        }

        // Metadata is written last so it cannot describe page state that has not reached disk.
        if metadata_page_dirty {
            let page = self.cache_read()?.peek_shared(STORAGE_METADATA_PAGE_ID);
            if let Some(page) = page {
                if let Err(e) = self.file_manager.write_page(page.as_ref()) {
                    self.enter_error_state();
                    return Err(e);
                }
            }
            self.cache_mut()?.clear_dirty(STORAGE_METADATA_PAGE_ID);
        }
        if let Err(e) = self.file_manager.flush() {
            self.enter_error_state();
            return Err(e);
        }
        if self.transaction.is_some() {
            self.transition_state(PagerState::WriterDbMod)?;
        } else if !matches!(self.lock_mode, PagerLockMode::Shared { .. }) {
            self.transition_state(PagerState::Open)?;
        }
        Ok(())
    }
}
