use super::{JournalMode, Pager, PagerLockMode, PagerState, STORAGE_METADATA_PAGE_ID};
use crate::error::Result;
use crate::storage::Page;
use crate::storage::PageId;

impl Pager {
    pub fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        self.check_error_state()?;
        if let Some(page) = self.cache.get(page_id) {
            return Ok(page.clone());
        }

        if self.journal_mode == JournalMode::Wal {
            if let Some(transaction) = &self.transaction {
                if page_id >= self.file_manager.next_page_id()
                    && page_id < transaction.wal_next_page_id
                {
                    let page = Page::new(page_id);
                    self.cache.put(page.clone());
                    return Ok(page);
                }
            }
        }

        if let Some(state) = self
            .wal_read_snapshot
            .as_ref()
            .or(self.latest_wal_state.as_ref())
        {
            if let Some(data) = state.page_overrides.get(&page_id) {
                let page = Page::from_bytes(page_id, data.clone())?;
                if let Some(expected_checksum) = state.page_checksums.get(&page_id) {
                    let actual_checksum = Self::calculate_page_checksum(&page);
                    if actual_checksum != *expected_checksum {
                        return Err(crate::error::HematiteError::CorruptedData(format!(
                            "WAL page checksum mismatch for page {}: expected {}, got {}",
                            page_id, expected_checksum, actual_checksum
                        )));
                    }
                }
                self.cache.put(page.clone());
                return Ok(page);
            }
            let visible_page_regions =
                state.file_len.saturating_sub(64) / crate::storage::PAGE_SIZE as u64;
            let visible_next_page_id = (visible_page_regions as u32).max(2);
            if page_id >= self.file_manager.next_page_id() && page_id < visible_next_page_id {
                let page = Page::new(page_id);
                self.cache.put(page.clone());
                return Ok(page);
            }
        }

        let page = self.file_manager.read_page(page_id)?;
        let expected_checksum = self
            .wal_read_snapshot
            .as_ref()
            .or(self.latest_wal_state.as_ref())
            .and_then(|state| state.page_checksums.get(&page_id))
            .or_else(|| self.page_checksums.get(&page_id));
        if let Some(expected_checksum) = expected_checksum {
            let actual_checksum = Self::calculate_page_checksum(&page);
            if actual_checksum != *expected_checksum {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Page checksum mismatch for page {}: expected {}, got {}",
                    page_id, expected_checksum, actual_checksum
                )));
            }
        }
        self.cache.put(page.clone());
        Ok(page)
    }

    pub fn write_page(&mut self, page: Page) -> Result<()> {
        self.check_error_state()?;
        let page_id = page.id;
        self.snapshot_original_page(page_id)?;
        if page_id != STORAGE_METADATA_PAGE_ID {
            self.page_checksums
                .insert(page_id, Self::calculate_page_checksum(&page));
        }
        self.cache.put(page);
        self.cache.mark_dirty(page_id);
        if self.transaction.is_some() {
            self.transition_state(PagerState::WriterCacheMod)?;
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

        let dirty_ids = self.cache.dirty_page_ids();
        let mut metadata_page_dirty = false;

        for page_id in dirty_ids.iter().copied() {
            if page_id == STORAGE_METADATA_PAGE_ID {
                metadata_page_dirty = true;
                continue;
            }

            if let Some(page) = self.cache.get(page_id) {
                if let Err(e) = self.file_manager.write_page(page) {
                    self.enter_error_state();
                    return Err(e);
                }
            }
            self.cache.clear_dirty(page_id);
        }

        // Metadata is written last so it cannot describe page state that has not reached disk.
        if metadata_page_dirty {
            if let Some(page) = self.cache.get(STORAGE_METADATA_PAGE_ID) {
                if let Err(e) = self.file_manager.write_page(page) {
                    self.enter_error_state();
                    return Err(e);
                }
            }
            self.cache.clear_dirty(STORAGE_METADATA_PAGE_ID);
        }
        if let Err(e) = self.file_manager.flush() {
            self.enter_error_state();
            return Err(e);
        }
        if let Err(e) = self.persist_checksums() {
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
