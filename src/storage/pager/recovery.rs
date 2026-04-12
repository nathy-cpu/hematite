use super::{JournalMode, Pager};
use crate::error::Result;
use crate::storage::journal::{JournalRecord, JournalState, RollbackJournal};
use crate::storage::journal_v3::{V3JournalHeader, V3JournalRecord, V3JournalState};
use crate::storage::pager_metadata::PersistedPagerState;
use crate::storage::wal::{
    append_committed_frames_to_path, load_visible_state_from_path_with_base, VisibleWalState,
    WalFrame,
};
use crate::storage::{
    metadata_page, next_page_id_for_file_len, Page, PageId, STORAGE_METADATA_PAGE_ID, PAGE_SIZE,
};
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
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
            return self.apply_persisted_state(&bytes);
        }

        if let Some(db_path) = &self.database_identity {
            let sidecar_path = Self::legacy_checksum_store_path(db_path);
            let contents = match fs::read_to_string(&sidecar_path) {
                Ok(contents) => Some(contents),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
                Err(err) => return Err(err.into()),
            };
            if let Some(contents) = contents {
                self.apply_persisted_state(contents.as_bytes())?;
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

    fn apply_persisted_state(&mut self, contents: &[u8]) -> Result<()> {
        let persisted =
            PersistedPagerState::decode_bytes(contents, Self::CHECKSUM_METADATA_VERSION)?;
        self.journal_mode = persisted.journal_mode;
        self.file_manager.set_free_pages(persisted.free_pages);
        self.page_checksums = persisted.checksums;
        Ok(())
    }

    pub(super) fn refresh_persisted_view(&mut self) -> Result<()> {
        if self.transaction.is_some() || self.cache.dirty_count() != 0 {
            return Ok(());
        }

        self.load_persisted_state()?;
        if self.journal_mode != JournalMode::Wal {
            self.cache.reset();
        }
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
        Ok(())
    }

    fn encoded_persisted_state_page(&mut self) -> Result<Page> {
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
        let encoded = metadata_page::write_pager_metadata(&existing_page.data, &contents)?;
        Page::from_bytes(STORAGE_METADATA_PAGE_ID, encoded)
    }

    pub(super) fn stage_persisted_state_page(&mut self) -> Result<()> {
        let page = self.encoded_persisted_state_page()?;
        self.snapshot_original_page(STORAGE_METADATA_PAGE_ID)?;
        self.cache.put(page);
        self.cache.mark_dirty(STORAGE_METADATA_PAGE_ID);
        if self.active_rollback_transaction().is_some() {
            self.cache.mark_need_sync(STORAGE_METADATA_PAGE_ID);
        }
        Ok(())
    }

    pub(super) fn persist_checksums(&mut self) -> Result<()> {
        let page = self.encoded_persisted_state_page()?;
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
        self.cache.mark_need_sync(page_id);
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
        let original_file_len = transaction.original_file_len;

        let header = V3JournalHeader {
            state: V3JournalState::Active,
            original_database_page_count: next_page_id_for_file_len(original_file_len),
            free_page_count: original_free_pages.len() as u32,
            checksum_count: original_checksums.len() as u32,
            record_count: 0,
            ..V3JournalHeader::default()
        };

        let mut bytes = Vec::with_capacity(
            36 + original_free_pages.len() * 4 + original_checksums.len() * 8,
        );
        bytes.extend_from_slice(&header.encode());
        for page_id in &original_free_pages {
            bytes.extend_from_slice(&page_id.to_be_bytes());
        }
        for (page_id, checksum) in &original_checksums {
            bytes.extend_from_slice(&page_id.to_be_bytes());
            bytes.extend_from_slice(&checksum.to_be_bytes());
        }

        let header_len = bytes.len() as u64;

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(path)?;
        file.write_all(&bytes)?;

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

        let v3_record = V3JournalRecord {
            page_number: record.page_id,
            page_bytes: record.data.clone(),
        };
        let checksum_seed = V3JournalHeader::default().checksum_seed;
        let encoded = v3_record.encode(checksum_seed)?;
        file.write_all(&encoded)?;
        self.journal_record_count += 1;

        // Update record_count in the header (offset 32) so the journal is
        // always self-consistent — a crash at any point leaves a decodable file.
        file.seek(SeekFrom::Start(32))?;
        file.write_all(&self.journal_record_count.to_be_bytes())?;

        // Seek back to end for the next append.
        file.seek(SeekFrom::End(0))?;

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
        for page_id in self.cache.dirty_page_ids() {
            self.cache.clear_need_sync(page_id);
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

        // State byte is at offset 8 in the V3 header.
        file.seek(SeekFrom::Start(8))?;
        file.write_all(&[V3JournalState::Committed as u8])?;

        // Record count is at offset 32 in the V3 header.
        file.seek(SeekFrom::Start(32))?;
        file.write_all(&self.journal_record_count.to_be_bytes())?;

        file.sync_all()?;
        Ok(())
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
                let new_file_len =
                    self.journal_header_len + new_record_count as u64 * record_size;
                file.set_len(new_file_len)?;

                // Update record_count at offset 32.
                file.seek(SeekFrom::Start(32))?;
                file.write_all(&new_record_count.to_be_bytes())?;
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
        let committed_view_token = self.cache_view_token();
        for page_id in self.cache.dirty_page_ids() {
            self.cache.clear_dirty(page_id);
            self.cache.set_view_token(page_id, committed_view_token);
        }
        Ok(())
    }
}
