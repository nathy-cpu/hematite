use super::{JournalMode, Pager, PagerLockMode, PagerState, STORAGE_METADATA_PAGE_ID};
use crate::error::Result;
use crate::storage::Page;
use crate::storage::PageId;
use std::sync::Arc;

impl Pager {
    pub fn read_page(&self, page_id: PageId) -> Result<Page> {
        let shared = self.read_page_shared(page_id)?;
        let mut page = Page::new_for_overwrite(shared.id);
        page.data.copy_from_slice(&shared.data);
        Ok(page)
    }

    pub(crate) fn read_page_shared(&self, page_id: PageId) -> Result<Arc<Page>> {
        self.check_error_state()?;
        let view_token = self.cache_view_token();
        if let Some(page) = self.cache_read()?.peek_for_view(page_id, view_token) {
            crate::storage::stats::PagerStats::bump(&self.stats.pool_hits);
            return Ok(page);
        }
        // Everything below this point resolves the page some other way and installs
        // it, so the request has missed the pool however it ends up being served.
        crate::storage::stats::PagerStats::bump(&self.stats.pool_misses);

        if let Some(transaction_next_page_id) = self.rollback_visible_next_page_id() {
            if page_id >= transaction_next_page_id || self.rollback_page_is_free(page_id) {
                return Err(crate::error::HematiteError::StorageError(format!(
                    "Page {} is not allocated in the active rollback transaction",
                    page_id
                )));
            }
            if self.rollback_page_is_uninitialized(page_id) {
                // A page beyond the end of the file reads as zeros.
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
                    // A page beyond the end of the file reads as zeros.
                    let page = Arc::new(Page::new(page_id));
                    self.cache_write()?
                        .put_shared_with_view(page.clone(), view_token);
                    return Ok(page);
                }
            }
        }
        // WAL visible state: extract all needed info immutably first,
        // then perform the mutable page data read separately to avoid
        // overlapping borrows.
        let wal_info = self.current_wal_visible_state().map(|state| {
            (
                state.contains_page(page_id),
                state.has_page_override(page_id),
                state.visible_next_page_id(),
            )
        });

        if let Some((contains, has_override, visible_next_page_id)) = wal_info {
            if !contains {
                return Err(crate::error::HematiteError::StorageError(format!(
                    "Page {} is not allocated in the current WAL-visible state",
                    page_id
                )));
            }
            if has_override {
                if let Some(data) = self.read_wal_page_data_readonly(page_id)? {
                    let page = Arc::new(Page::from_bytes(page_id, data)?);
                    {
                        // A WAL frame carries the page image complete with its
                        // trailer, so the page checks itself.
                        use crate::storage::file_manager::{FileManager, PageTrailerState};
                        if FileManager::page_trailer_state(&page) == PageTrailerState::Mismatch {
                            return Err(crate::error::HematiteError::CorruptedData(format!(
                                "WAL page {page_id} does not match the checksum in its trailer"
                            )));
                        }
                    }
                    self.cache_write()?
                        .put_shared_with_view(page.clone(), view_token);
                    return Ok(page);
                }
            }
            if page_id >= self.file_manager.next_page_id() && page_id < visible_next_page_id {
                // A page beyond the end of the file reads as zeros.
                let page = Arc::new(Page::new(page_id));
                self.cache_write()?
                    .put_shared_with_view(page.clone(), view_token);
                return Ok(page);
            }
        }

        // `read_page_with_buf` goes through `read_exact_at`, which fills all 4096
        // bytes or fails, so this does not need zeroing first.
        let buf = crate::storage::types::take_page_buffer_for_overwrite();
        // `read_page_with_buf` verifies the page against the checksum in its own
        // trailer, which is the only integrity check this read needs.
        let page = Arc::new(self.file_manager.read_page_with_buf(page_id, buf)?);
        self.cache_write()?
            .put_shared_with_view(page.clone(), view_token);
        Ok(page)
    }

    pub fn write_page(&mut self, page: Page) -> Result<()> {
        self.check_error_state()?;
        let page_id = page.id;
        self.snapshot_original_page(page_id)?;
        if let Some(transaction) = self.active_rollback_transaction_mut() {
            // Guarded, because `Arc::make_mut` on a set a savepoint is sharing copies
            // it. Pages sit in this set only between being allocated and first
            // written, so the membership test almost always says no and the copy
            // almost never happens.
            if transaction.rollback_uninitialized_pages.contains(&page_id) {
                Arc::make_mut(&mut transaction.rollback_uninitialized_pages).remove(&page_id);
            }
        }
        let rollback_transaction_active = self.active_rollback_transaction().is_some();
        let journal_needs_sync = self.journal_needs_sync;
        {
            let cache = self.cache_mut()?;
            cache.put(page);
            cache.mark_dirty(page_id);
            if rollback_transaction_active {
                // New pages do not have a pre-transaction image, so snapshot_original_page
                // does not journal them; marking every written page journaled here makes
                // those spill-eligible too, once the journal sync barrier has been satisfied.
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
                crate::storage::stats::PagerStats::bump(&self.stats.spills);
            }
            self.cache_mut()?.clear_dirty(page_id);
            // After clearing dirty, the page becomes a clean cache entry that
            // regular LRU eviction can reclaim.
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.flush_inner(false)
    }

    /// Flushes as part of committing a rollback transaction.
    ///
    /// The difference from a plain `flush` is the commit intent: the set of pages
    /// this commit is about to write, with the contents each must end up holding. It
    /// has to be recorded after the persisted-state page is staged — so the intent
    /// covers that page's final bytes too — and before the journal is synced, so it
    /// is durable before any database page is overwritten.
    ///
    /// A plain `flush` must never write one. Flushing mid-transaction, or spilling,
    /// writes database pages while the transaction is still open; declaring an intent
    /// there would tell recovery a commit had landed when it had not.
    pub(super) fn flush_for_commit(&mut self) -> Result<()> {
        self.flush_inner(true)
    }

    fn flush_inner(&mut self, declare_commit_intent: bool) -> Result<()> {
        self.check_error_state()?;
        if self.journal_mode == JournalMode::Wal && self.transaction.is_some() {
            return Err(crate::error::HematiteError::StorageError(
                "Cannot flush pager pages directly during an active WAL transaction".to_string(),
            ));
        }

        // Staged first. Staging writes the free-page state into the metadata page,
        // which journals that page's pre-image and dirties it; both have to happen
        // before the commit intent is declared, so the intent covers the page's final
        // bytes, and before the journal sync, so its pre-image is durable before the
        // page is overwritten.
        self.stage_persisted_state_page()?;
        if self.active_rollback_transaction().is_some() {
            if declare_commit_intent {
                self.declare_rollback_commit_intent()?;
            }
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
