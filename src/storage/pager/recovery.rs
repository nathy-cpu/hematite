use super::{JournalMode, Pager};
use crate::error::Result;
use crate::storage::journal::{JournalRecord, JournalState, RollbackJournal};
use crate::storage::pager_metadata::PersistedPagerState;
use crate::storage::wal::{
    append_committed_frames_to_path, load_visible_state_from_path_with_base, VisibleWalState,
    WalFrame,
};
use crate::storage::{metadata_page, Page, PageId, STORAGE_METADATA_PAGE_ID, PAGE_SIZE};
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

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

    pub(super) fn load_persisted_state(&mut self) -> Result<()> {
        let page = self.file_manager.read_page(STORAGE_METADATA_PAGE_ID)?;
        if let Some(bytes) = metadata_page::read_pager_metadata(&page.data)? {
            let contents = String::from_utf8(bytes).map_err(|_| {
                crate::error::HematiteError::StorageError(
                    "Invalid pager metadata encoding".to_string(),
                )
            })?;
            return self.apply_persisted_state(&contents);
        }

        if let Some(db_path) = &self.database_identity {
            let sidecar_path = Self::legacy_checksum_store_path(db_path);
            let contents = match fs::read_to_string(&sidecar_path) {
                Ok(contents) => Some(contents),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
                Err(err) => return Err(err.into()),
            };
            if let Some(contents) = contents {
                self.apply_persisted_state(&contents)?;
                self.persist_checksums()?;
                match fs::remove_file(&sidecar_path) {
                    Ok(()) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => return Err(err.into()),
                }
                return Ok(());
            }
        }

        self.journal_mode = JournalMode::Rollback;
        self.file_manager.set_free_pages(Vec::new());
        self.page_checksums.clear();
        Ok(())
    }

    fn apply_persisted_state(&mut self, contents: &str) -> Result<()> {
        let persisted = PersistedPagerState::decode(contents, Self::CHECKSUM_METADATA_VERSION)?;
        self.journal_mode = persisted.journal_mode;
        self.file_manager.set_free_pages(persisted.free_pages);
        self.page_checksums = persisted.checksums;
        Ok(())
    }

    pub(super) fn refresh_persisted_view(&mut self) -> Result<()> {
        if self.transaction.is_some() || self.cache.dirty_count() != 0 {
            return Ok(());
        }

        self.cache.reset();
        self.load_persisted_state()?;
        self.load_latest_wal_state()
    }

    pub(super) fn snapshot_wal_visible_state(&mut self) -> Result<VisibleWalState> {
        if let Some(state) = &self.latest_wal_state {
            return Ok(state.clone());
        }

        Ok(VisibleWalState::from_database_state(
            self.file_manager.file_len()?,
            self.file_manager.free_pages().to_vec(),
            self.page_checksums.clone(),
        ))
    }

    pub(super) fn load_latest_wal_state(&mut self) -> Result<()> {
        if self.journal_mode != JournalMode::Wal {
            self.latest_wal_state = None;
            return Ok(());
        }

        let Some(path) = &self.wal_path else {
            self.latest_wal_state = None;
            return Ok(());
        };

        let metadata_page = self.file_manager.read_page(STORAGE_METADATA_PAGE_ID)?;
        self.latest_wal_state = load_visible_state_from_path_with_base(
            path,
            self.file_manager.file_len()?,
            self.file_manager.free_pages().to_vec(),
            self.page_checksums.clone(),
            &metadata_page.data,
        )?;
        if self.transaction.is_none() && self.wal_read_snapshot.is_none() {
            self.cache.reset();
        }
        Ok(())
    }

    pub(super) fn persist_checksums(&mut self) -> Result<()> {
        let contents = PersistedPagerState {
            journal_mode: self.journal_mode,
            free_pages: self.file_manager.free_pages().to_vec(),
            checksums: self.page_checksums.clone(),
        }
        .encode(Self::CHECKSUM_METADATA_VERSION);
        let existing_page = self
            .cache
            .peek(STORAGE_METADATA_PAGE_ID)
            .cloned()
            .unwrap_or(self.file_manager.read_page(STORAGE_METADATA_PAGE_ID)?);
        let encoded = metadata_page::write_pager_metadata(&existing_page.data, contents.as_bytes())?;
        let page = Page::from_bytes(STORAGE_METADATA_PAGE_ID, encoded)?;
        self.file_manager.write_page(&page)?;
        self.file_manager.flush()?;
        self.cache.put(page);
        self.cache.clear_dirty(STORAGE_METADATA_PAGE_ID);
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

        let page_end = page_id as u64 * PAGE_SIZE as u64;
        if page_end > transaction.original_file_len {
            return Ok(());
        }

        let page = if let Some(page) = self.cache.peek(page_id) {
            page.clone()
        } else {
            self.file_manager.read_page(page_id)?
        };

        if let Some(transaction) = self.active_rollback_transaction_mut() {
            for savepoint in &mut transaction.savepoints {
                let live_at_savepoint = page_end <= savepoint.file_manager.file_len()
                    && !savepoint.file_manager.free_pages().contains(&page_id);
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
        self.cache.mark_journaled(page_id);
        self.append_rollback_journal_record(&record)?;
        Ok(())
    }

    pub(super) fn persist_journal(&self, state: JournalState) -> Result<()> {
        let Some(transaction) = self.active_rollback_transaction() else {
            return Ok(());
        };
        let Some(path) = &self.journal_path else {
            return Ok(());
        };

        let journal = RollbackJournal {
            state,
            original_file_len: transaction.original_file_len,
            original_free_pages: transaction.original_free_pages.clone(),
            original_checksums: transaction
                .original_checksums
                .iter()
                .map(|(page_id, checksum)| (*page_id, *checksum))
                .collect(),
            page_records: transaction.page_records.clone(),
        };
        let bytes = journal.encode()?;
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(())
    }

    pub(super) fn initialize_rollback_journal(&self) -> Result<()> {
        self.persist_journal(JournalState::Active)
    }

    pub(super) fn append_rollback_journal_record(&self, record: &JournalRecord) -> Result<()> {
        let _ = record;
        self.persist_journal(JournalState::Active)
    }

    pub(super) fn mark_rollback_journal_committed(&self) -> Result<()> {
        self.persist_journal(JournalState::Committed)
    }

    pub(super) fn sync_rollback_journal_from_transaction(&self) -> Result<()> {
        if self.journal_mode != JournalMode::Rollback {
            return Ok(());
        }

        if self.active_rollback_transaction().is_some() {
            self.persist_journal(JournalState::Active)
        } else {
            self.remove_journal_file()
        }
    }

    pub(super) fn remove_journal_file(&self) -> Result<()> {
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
        self.cache.reset();
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
        self.cache.reset();
        self.page_checksums = transaction.original_checksums;
        self.load_latest_wal_state()
    }

    pub(super) fn commit_wal_transaction(&mut self) -> Result<()> {
        let (wal_next_page_id, wal_free_pages) = {
            let transaction = self.active_wal_transaction().ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Pager transaction is not active".to_string(),
                )
            })?;
            (transaction.wal_next_page_id, transaction.wal_free_pages.clone())
        };
        let next_sequence = self
            .latest_wal_state
            .as_ref()
            .map(|state| state.visible_sequence + 1)
            .unwrap_or(1);

        let mut page_ids = self.cache.dirty_page_ids();
        page_ids.sort_unstable();

        let mut frames = Vec::with_capacity(page_ids.len());
        for page_id in page_ids {
            let page = self.cache.peek(page_id).cloned().ok_or_else(|| {
                crate::error::HematiteError::StorageError(format!(
                    "Dirty page {} missing from page cache",
                    page_id
                ))
            })?;
            frames.push(WalFrame {
                page_id,
                data: page.data,
            });
        }
        let metadata_page = self
            .cache
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
        self.load_latest_wal_state()?;
        for page_id in self.cache.dirty_page_ids() {
            self.cache.clear_dirty(page_id);
        }
        self.cache.reset();
        Ok(())
    }
}
