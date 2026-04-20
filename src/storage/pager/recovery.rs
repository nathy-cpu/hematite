use super::{JournalMode, Pager, WalFileStamp};
use crate::error::Result;
use crate::storage::journal::{
    append_journal_record, initialize_journal_file, mark_journal_committed,
    write_journal_record_count, JournalRecord, JournalState, RollbackJournal,
};
use crate::storage::pager_metadata::PersistedPagerState;
use crate::storage::wal::{
    append_committed_frames_to_path, load_visible_state_from_path_with_base, VisibleWalState,
    WalFrame,
};
use crate::storage::{
    file_len_for_next_page_id, metadata_page, Page, PageId, PAGE_SIZE, STORAGE_METADATA_PAGE_ID,
};
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

impl Pager {
    fn legacy_checksum_store_path(db_path: &Path) -> PathBuf {
        let mut file_name = db_path
            .file_name()
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("hematite.db"));
        file_name.push(".pager_checksums");
        match db_path.parent() {
            Some(parent) => parent.join(file_name),
            None => PathBuf::from(file_name),
        }
    }

    pub(super) fn journal_path(db_path: &Path) -> PathBuf {
        let mut file_name = db_path
            .file_name()
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("hematite.db"));
        file_name.push(".journal");
        match db_path.parent() {
            Some(parent) => parent.join(file_name),
            None => PathBuf::from(file_name),
        }
    }

    pub(super) fn wal_path(db_path: &Path) -> PathBuf {
        let mut file_name = db_path
            .file_name()
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("hematite.db"));
        file_name.push(".wal");
        match db_path.parent() {
            Some(parent) => parent.join(file_name),
            None => PathBuf::from(file_name),
        }
    }

    fn read_persisted_state_page(&mut self) -> Result<Option<Page>> {
        self.file_manager.sync_with_disk()?;
        if STORAGE_METADATA_PAGE_ID >= self.file_manager.next_page_id() {
            return Ok(None);
        }
        Ok(Some(self.file_manager.read_page(STORAGE_METADATA_PAGE_ID)?))
    }

    fn apply_persisted_state_page(&mut self, page: Option<&Page>) -> Result<()> {
        if let Some(page) = page {
            if let Some(bytes) = metadata_page::read_pager_metadata(&page.data)? {
                return self.apply_persisted_state(&bytes);
            }
        }

        if let Some(db_path) = &self.database_identity {
            let sidecar_path = Self::legacy_checksum_store_path(db_path);
            match fs::metadata(&sidecar_path) {
                Ok(_) => {
                    return Err(crate::error::HematiteError::StorageError(format!(
                        "Legacy pager checksum sidecar '{}' is unsupported",
                        sidecar_path.display()
                    )));
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            }
        }

        self.journal_mode = JournalMode::Rollback;
        self.file_manager.set_free_pages(Vec::new());
        self.page_checksums.clear();
        Ok(())
    }

    pub(super) fn load_persisted_state(&mut self) -> Result<()> {
        let page = self.read_persisted_state_page()?;
        self.apply_persisted_state_page(page.as_ref())
    }

    fn apply_persisted_state(&mut self, contents: &[u8]) -> Result<()> {
        let persisted =
            PersistedPagerState::decode_bytes(contents, Self::CHECKSUM_METADATA_VERSION)?;
        self.journal_mode = persisted.journal_mode;
        self.file_manager.set_free_pages(persisted.free_pages);
        self.page_checksums = persisted.checksums;
        Ok(())
    }

    pub(super) fn refresh_persisted_view(&mut self) -> Result<()> {
        if self.transaction.is_some() || self.cache_mut()?.dirty_count() != 0 {
            return Ok(());
        }

        let metadata_page = self.read_persisted_state_page()?;
        self.apply_persisted_state_page(metadata_page.as_ref())?;
        if self.journal_mode != JournalMode::Wal {
            self.cache_mut()?.reset();
            self.latest_wal_state = None;
            self.latest_wal_file_stamp = None;
            return Ok(());
        }
        self.load_latest_wal_state_if_changed(
            metadata_page.as_ref().map(|page| page.data.as_slice()),
        )
    }

    pub(super) fn snapshot_wal_visible_state(&mut self) -> Result<Arc<VisibleWalState>> {
        if let Some(state) = &self.latest_wal_state {
            return Ok(state.clone());
        }

        Ok(Arc::new(VisibleWalState::from_database_state(
            self.logical_file_len()?,
            self.logical_free_pages().to_vec(),
            self.page_checksums.clone(),
        )))
    }

    pub(super) fn load_latest_wal_state(&mut self) -> Result<()> {
        self.load_latest_wal_state_with_base_metadata(None)
    }

    fn load_latest_wal_state_if_changed(
        &mut self,
        baseline_metadata_page: Option<&[u8]>,
    ) -> Result<()> {
        if self.journal_mode != JournalMode::Wal {
            self.latest_wal_state = None;
            self.latest_wal_file_stamp = None;
            return Ok(());
        }

        let stamp = self.current_wal_file_stamp()?;
        if self.latest_wal_file_stamp == stamp {
            return Ok(());
        }

        self.load_latest_wal_state_with_base_metadata(baseline_metadata_page)
    }

    fn load_latest_wal_state_with_base_metadata(
        &mut self,
        baseline_metadata_page: Option<&[u8]>,
    ) -> Result<()> {
        #[cfg(test)]
        {
            self.wal_visible_state_reload_count =
                self.wal_visible_state_reload_count.saturating_add(1);
        }
        if self.journal_mode != JournalMode::Wal {
            self.latest_wal_state = None;
            self.latest_wal_file_stamp = None;
            return Ok(());
        }

        let Some(path) = &self.wal_path else {
            self.latest_wal_state = None;
            self.latest_wal_file_stamp = None;
            return Ok(());
        };

        let wal_file_stamp = self.current_wal_file_stamp()?;
        let owned_metadata_page;
        let metadata_page = if let Some(page) = baseline_metadata_page {
            page
        } else {
            owned_metadata_page = self.file_manager.read_page(STORAGE_METADATA_PAGE_ID)?;
            owned_metadata_page.data.as_slice()
        };
        self.latest_wal_state = load_visible_state_from_path_with_base(
            path,
            self.file_manager.file_len()?,
            self.file_manager.free_pages().to_vec(),
            self.page_checksums.clone(),
            metadata_page,
        )?
        .map(Arc::new);
        self.latest_wal_file_stamp = wal_file_stamp;
        Ok(())
    }

    fn current_wal_file_stamp(&self) -> Result<Option<WalFileStamp>> {
        let Some(path) = &self.wal_path else {
            return Ok(None);
        };

        match fs::metadata(path) {
            Ok(metadata) => Ok(Some(WalFileStamp {
                len: metadata.len(),
                modified: metadata.modified().ok(),
            })),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn refresh_wal_file_stamp_best_effort(&mut self) {
        self.latest_wal_file_stamp = self.current_wal_file_stamp().ok().flatten();
    }

    fn encoded_persisted_state_page(&mut self) -> Result<Page> {
        let free_pages = self.logical_free_pages().to_vec();

        let existing_page = self
            .cache_mut()?
            .peek(STORAGE_METADATA_PAGE_ID)
            .cloned()
            .unwrap_or_else(|| {
                self.file_manager
                    .read_page(STORAGE_METADATA_PAGE_ID)
                    .unwrap_or_else(|_| Page::new(STORAGE_METADATA_PAGE_ID))
            });

        let full_state = PersistedPagerState {
            journal_mode: self.journal_mode,
            free_pages: free_pages.clone(),
            checksums: self.page_checksums.clone(),
        };

        let encoded = full_state.encode(Self::CHECKSUM_METADATA_VERSION);
        let metadata_page_bytes =
            match metadata_page::write_pager_metadata(&existing_page.data, &encoded) {
                Ok(bytes) => bytes,
                Err(crate::error::HematiteError::StorageError(ref msg))
                    if msg.contains("exceeds page size") =>
                {
                    // Fallback: exclude checksums if they cause an overflow.
                    // This ensures large databases remain functional while small
                    // databases retain persistent integrity checking.
                    let fallback_state = PersistedPagerState {
                        journal_mode: self.journal_mode,
                        free_pages,
                        checksums: std::collections::HashMap::new(),
                    };
                    let fallback_encoded = fallback_state.encode(Self::CHECKSUM_METADATA_VERSION);
                    metadata_page::write_pager_metadata(&existing_page.data, &fallback_encoded)?
                }
                Err(err) => return Err(err),
            };

        Page::from_bytes(STORAGE_METADATA_PAGE_ID, metadata_page_bytes)
    }

    pub(super) fn stage_persisted_state_page(&mut self) -> Result<()> {
        let page = self.encoded_persisted_state_page()?;
        self.snapshot_original_page(STORAGE_METADATA_PAGE_ID)?;
        {
            let cache = self.cache_mut()?;
            cache.put(page);
            cache.mark_dirty(STORAGE_METADATA_PAGE_ID);
        }
        if self.active_rollback_transaction().is_some() {
            self.cache_mut()?.mark_need_sync(STORAGE_METADATA_PAGE_ID);
        }
        Ok(())
    }

    pub(super) fn persist_checksums(&mut self) -> Result<()> {
        let page = self.encoded_persisted_state_page()?;
        self.file_manager.write_page(&page)?;
        self.file_manager.flush()?;
        let cache = self.cache_mut()?;
        cache.put(page);
        cache.clear_dirty(STORAGE_METADATA_PAGE_ID);
        Ok(())
    }

    pub(super) fn snapshot_original_page(&mut self, page_id: PageId) -> Result<()> {
        if self.journal_mode == JournalMode::Wal {
            return Ok(());
        }

        let Some(transaction) = self.active_rollback_transaction() else {
            return Ok(());
        };

        if transaction.journaled_pages.contains(&page_id) {
            return Ok(());
        }

        let page_start = page_id as u64 * PAGE_SIZE as u64;
        let page_end = page_start.saturating_add(PAGE_SIZE as u64);
        if page_end > transaction.original_file_len {
            return Ok(());
        }

        let page = if let Some(page) = self.cache_mut()?.peek(page_id) {
            page.clone()
        } else {
            self.file_manager.read_page(page_id)?
        };

        if let Some(transaction) = self.active_rollback_transaction_mut() {
            for savepoint in &mut transaction.savepoints {
                let live_at_savepoint = page_id < savepoint.rollback_next_page_id
                    && !savepoint.rollback_free_page_set.contains(&page_id);
                if live_at_savepoint && savepoint.captured_page_ids.insert(page_id) {
                    savepoint.page_records.push(JournalRecord {
                        page_id,
                        data: page.data.to_vec(),
                    });
                }
            }
        }

        let record = {
            let Some(transaction) = self.active_rollback_transaction_mut() else {
                return Ok(());
            };
            let record = JournalRecord {
                page_id,
                data: page.data,
            };
            transaction.page_records.push(record.clone());
            transaction.journaled_pages.insert(page_id);
            record
        };
        {
            let cache = self.cache_mut()?;
            cache.mark_journaled(page_id);
            cache.mark_need_sync(page_id);
        }
        self.journal_needs_sync = true;
        self.append_rollback_journal_record(&record)?;
        Ok(())
    }

    /// Write the journal header and metadata section once at transaction begin.
    /// Keeps the file handle open for incremental record appending.
    pub(super) fn initialize_rollback_journal(&mut self) -> Result<()> {
        let Some(transaction) = self.active_rollback_transaction() else {
            return Ok(());
        };
        let Some(path) = &self.journal_path else {
            return Ok(());
        };

        let original_free_pages = transaction.original_free_pages.clone();
        let original_checksums: Vec<(PageId, u32)> = transaction
            .original_checksums
            .iter()
            .map(|(page_id, checksum)| (*page_id, *checksum))
            .collect();
        let (file, header_len) = initialize_journal_file(
            path,
            transaction.original_file_len,
            &original_free_pages,
            &original_checksums,
        )?;

        self.journal_file = Some(file);
        self.journal_record_count = 0;
        self.journal_header_len = header_len;
        self.journal_needs_sync = true;

        Ok(())
    }

    /// Append a single page record to the already-open journal file.
    pub(super) fn append_rollback_journal_record(&mut self, record: &JournalRecord) -> Result<()> {
        let Some(file) = &mut self.journal_file else {
            return Ok(());
        };

        append_journal_record(file, record)?;
        self.journal_record_count += 1;

        // Keep record count current so crash leaves decodable journal file.
        write_journal_record_count(file, self.journal_record_count)?;

        Ok(())
    }

    pub(super) fn sync_rollback_journal(&mut self) -> Result<()> {
        if !self.journal_needs_sync {
            return Ok(());
        }
        let Some(file) = &mut self.journal_file else {
            self.journal_needs_sync = false;
            return Ok(());
        };
        file.sync_all()?;
        // BUG-09 fix: only clear need_sync on pages that are already journaled.
        // Pages dirtied after this journal sync started have not had their
        // original images captured yet; clearing need_sync on them would allow
        // them to spill to the main file without journal coverage.
        let dirty_ids = self.cache_mut()?.dirty_page_ids();
        for page_id in dirty_ids {
            let is_journaled = self
                .cache_read()?
                .meta(page_id)
                .map(|m| m.journaled)
                .unwrap_or(false);
            if is_journaled {
                self.cache_mut()?.clear_need_sync(page_id);
            }
        }
        self.journal_needs_sync = false;
        Ok(())
    }

    /// Atomically mark the journal as committed by updating just the state byte
    /// and record count in the header — no full rewrite.
    pub(super) fn mark_rollback_journal_committed(&mut self) -> Result<()> {
        let Some(file) = &mut self.journal_file else {
            return Ok(());
        };

        mark_journal_committed(file, self.journal_record_count)
    }

    /// After a savepoint restore, truncate the journal to match the reduced
    /// page-record set and update the header's record count.
    pub(super) fn sync_rollback_journal_from_transaction(&mut self) -> Result<()> {
        if self.journal_mode != JournalMode::Rollback {
            return Ok(());
        }

        if self.active_rollback_transaction().is_some() {
            let new_record_count = self
                .active_rollback_transaction()
                .map(|t| t.page_records.len() as u32)
                .unwrap_or(0);

            if let Some(file) = &mut self.journal_file {
                let record_size = (8 + PAGE_SIZE) as u64;
                let new_file_len = self.journal_header_len + new_record_count as u64 * record_size;
                file.set_len(new_file_len)?;

                write_journal_record_count(file, new_record_count)?;
                file.sync_all()?;

                self.journal_record_count = new_record_count;
                self.journal_needs_sync = false;
            }
            Ok(())
        } else {
            self.remove_journal_file()
        }
    }

    /// Close the journal file handle and remove the file from disk.
    pub(super) fn remove_journal_file(&mut self) -> Result<()> {
        // Close the handle first so the file can be removed cleanly.
        self.journal_file = None;
        self.journal_record_count = 0;
        self.journal_header_len = 0;
        self.journal_needs_sync = false;

        let Some(path) = &self.journal_path else {
            return Ok(());
        };
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    pub(super) fn remove_wal_file(&self) -> Result<()> {
        let Some(path) = &self.wal_path else {
            return Ok(());
        };
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    pub(super) fn recover_if_needed(&mut self) -> Result<()> {
        let Some(path) = &self.journal_path else {
            return Ok(());
        };
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err.into()),
        };

        let journal = RollbackJournal::decode(&bytes)?;
        match journal.state {
            JournalState::Active => {
                if self.has_live_writer()? {
                    return Ok(());
                }
                self.restore_from_journal(&journal)?;
                self.remove_journal_file()?;
            }
            JournalState::Committed => {
                self.remove_journal_file()?;
            }
        }
        Ok(())
    }

    pub(super) fn rollback_from_active_transaction(&mut self) -> Result<()> {
        if self.journal_path.is_some() {
            let Some(path) = &self.journal_path else {
                unreachable!();
            };
            let journal = RollbackJournal::decode(&fs::read(path)?)?;
            return self.restore_from_journal(&journal);
        }

        let transaction = self.active_rollback_transaction().cloned().ok_or_else(|| {
            crate::error::HematiteError::StorageError("Pager transaction is not active".to_string())
        })?;
        let journal = RollbackJournal {
            state: JournalState::Active,
            original_file_len: transaction.original_file_len,
            original_free_pages: transaction.original_free_pages,
            original_checksums: transaction
                .original_checksums
                .into_iter()
                .collect::<Vec<_>>(),
            page_records: transaction.page_records,
        };
        self.restore_from_journal(&journal)
    }

    pub(super) fn restore_from_journal(&mut self, journal: &RollbackJournal) -> Result<()> {
        self.cache_mut()?.reset();
        self.file_manager
            .restore_file_len(journal.original_file_len)?;
        self.file_manager
            .set_free_pages(journal.original_free_pages.clone());

        for record in &journal.page_records {
            let page = Page::from_bytes(record.page_id, record.data.clone())?;
            self.file_manager.write_page(&page)?;
        }
        self.file_manager.flush()?;

        self.page_checksums = journal.original_checksums.iter().copied().collect();
        self.persist_checksums()
    }

    pub(super) fn rollback_wal_transaction(&mut self) -> Result<()> {
        let transaction = self.active_wal_transaction().cloned().ok_or_else(|| {
            crate::error::HematiteError::StorageError("Pager transaction is not active".to_string())
        })?;
        self.cache_mut()?.reset();
        self.page_checksums = transaction.original_checksums;
        Ok(())
    }

    pub(super) fn commit_wal_transaction(&mut self) -> Result<()> {
        let (wal_next_page_id, wal_free_pages) = {
            let transaction = self.active_wal_transaction().ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Pager transaction is not active".to_string(),
                )
            })?;
            (
                transaction.wal_next_page_id,
                transaction.wal_free_pages.clone(),
            )
        };
        let next_sequence = self
            .latest_wal_state
            .as_ref()
            .map(|state| state.visible_sequence + 1)
            .unwrap_or(1);

        let mut page_ids = self.cache_read()?.dirty_page_ids();
        page_ids.sort_unstable();

        let mut frames = Vec::with_capacity(page_ids.len());
        for page_id in page_ids {
            let page = self.cache_read()?.peek(page_id).cloned().ok_or_else(|| {
                crate::error::HematiteError::StorageError(format!(
                    "Dirty page {} missing from page cache",
                    page_id
                ))
            })?;
            frames.push(WalFrame::new(page_id, page.data));
        }
        let metadata_page = self
            .cache_read()?
            .peek(STORAGE_METADATA_PAGE_ID)
            .cloned()
            .unwrap_or(self.file_manager.read_page(STORAGE_METADATA_PAGE_ID)?);
        if let Some(path) = &self.wal_path {
            append_committed_frames_to_path(
                path,
                next_sequence,
                wal_next_page_id,
                &wal_free_pages,
                &self.page_checksums,
                &metadata_page.data,
                &frames,
            )?;
        }
        let mut updated_visible_state = if let Some(state) = &self.latest_wal_state {
            state.as_ref().clone()
        } else {
            VisibleWalState::from_database_state(
                self.file_manager.file_len()?,
                self.file_manager.free_pages().to_vec(),
                self.page_checksums.clone(),
            )
        };
        updated_visible_state.apply_committed_delta(
            next_sequence,
            file_len_for_next_page_id(wal_next_page_id),
            wal_free_pages.clone(),
            self.page_checksums.clone(),
            &frames,
        )?;
        self.latest_wal_state = Some(Arc::new(updated_visible_state));
        self.refresh_wal_file_stamp_best_effort();
        let committed_view_token = self.cache_view_token();
        let dirty_ids = self.cache_mut()?.dirty_page_ids();
        for page_id in dirty_ids {
            let cache = self.cache_mut()?;
            cache.clear_dirty(page_id);
            cache.set_view_token(page_id, committed_view_token);
        }
        Ok(())
    }
}
