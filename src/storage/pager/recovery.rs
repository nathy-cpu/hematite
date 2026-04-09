use super::{JournalMode, Pager};
use crate::error::Result;
use crate::storage::journal::{JournalRecord, JournalState, RollbackJournal};
use crate::storage::pager::locking::checksum_persist_lock;
use crate::storage::wal::{VisibleWalState, WalFrame, WalRecord};
use crate::storage::{file_len_for_next_page_id, Page, PageId, PAGE_SIZE};
use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

impl Pager {
    pub(super) fn checksum_store_path(db_path: &Path) -> PathBuf {
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
        let Some(path) = &self.checksum_store_path else {
            return Ok(());
        };

        let contents = match fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err.into()),
        };

        let mut lines = contents.lines();
        let version = lines
            .next()
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Missing pager checksum metadata version".to_string(),
                )
            })?
            .strip_prefix("version=")
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Pager checksum metadata is missing version prefix".to_string(),
                )
            })?
            .parse::<u32>()
            .map_err(|_| {
                crate::error::HematiteError::StorageError(
                    "Invalid pager checksum metadata version".to_string(),
                )
            })?;

        if version != Self::CHECKSUM_METADATA_VERSION {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Unsupported pager checksum metadata version: expected {}, got {}",
                Self::CHECKSUM_METADATA_VERSION,
                version
            )));
        }

        let mut next_line = lines.next().ok_or_else(|| {
            crate::error::HematiteError::StorageError(
                "Missing pager freelist metadata count".to_string(),
            )
        })?;

        if let Some(mode) = next_line.strip_prefix("journal_mode=") {
            self.journal_mode = JournalMode::parse(mode)?;
            next_line = lines.next().ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Missing pager freelist metadata count".to_string(),
                )
            })?;
        } else {
            self.journal_mode = JournalMode::Rollback;
        }

        let expected_free_count = next_line
            .strip_prefix("free_count=")
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Pager freelist metadata is missing count prefix".to_string(),
                )
            })?
            .parse::<usize>()
            .map_err(|_| {
                crate::error::HematiteError::StorageError(
                    "Invalid pager freelist metadata count".to_string(),
                )
            })?;

        let mut free_pages = Vec::with_capacity(expected_free_count);
        for _ in 0..expected_free_count {
            let line = lines.next().ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Pager freelist metadata ended early".to_string(),
                )
            })?;
            let page_id = line
                .strip_prefix("free|")
                .ok_or_else(|| {
                    crate::error::HematiteError::StorageError(
                        "Invalid pager freelist metadata record".to_string(),
                    )
                })?
                .parse::<u32>()
                .map_err(|_| {
                    crate::error::HematiteError::StorageError(
                        "Invalid pager freelist page id".to_string(),
                    )
                })?;
            free_pages.push(page_id);
        }

        let expected_count = lines
            .next()
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Missing pager checksum metadata count".to_string(),
                )
            })?
            .strip_prefix("checksum_count=")
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Pager checksum metadata is missing count prefix".to_string(),
                )
            })?
            .parse::<usize>()
            .map_err(|_| {
                crate::error::HematiteError::StorageError(
                    "Invalid pager checksum metadata count".to_string(),
                )
            })?;

        let mut checksums = HashMap::new();
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let payload = line.strip_prefix("checksum|").ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Invalid pager checksum metadata record".to_string(),
                )
            })?;
            let parts = payload.split('|').collect::<Vec<_>>();
            if parts.len() != 2 {
                return Err(crate::error::HematiteError::StorageError(
                    "Invalid pager checksum metadata record".to_string(),
                ));
            }
            let page_id = parts[0].parse::<u32>().map_err(|_| {
                crate::error::HematiteError::StorageError(
                    "Invalid pager checksum page id".to_string(),
                )
            })?;
            let checksum = parts[1].parse::<u32>().map_err(|_| {
                crate::error::HematiteError::StorageError(
                    "Invalid pager checksum value".to_string(),
                )
            })?;
            if checksums.insert(page_id, checksum).is_some() {
                return Err(crate::error::HematiteError::StorageError(format!(
                    "Duplicate pager checksum entry for page {}",
                    page_id
                )));
            }
        }

        if checksums.len() != expected_count {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Pager checksum metadata count mismatch: expected {}, got {}",
                expected_count,
                checksums.len()
            )));
        }

        self.file_manager.set_free_pages(free_pages);
        self.page_checksums = checksums;
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

        self.latest_wal_state = WalRecord::load_visible_state_from_path(path)?;
        Ok(())
    }

    pub(super) fn persist_checksums(&self) -> Result<()> {
        let Some(path) = &self.checksum_store_path else {
            return Ok(());
        };
        let _persist_guard = checksum_persist_lock().lock().map_err(|_| {
            crate::error::HematiteError::InternalError(
                "Pager checksum persistence mutex is poisoned".to_string(),
            )
        })?;

        let mut entries = self
            .page_checksums
            .iter()
            .map(|(page_id, checksum)| (*page_id, *checksum))
            .collect::<Vec<_>>();
        entries.sort_by_key(|(page_id, _)| *page_id);

        let mut lines = vec![
            format!("version={}", Self::CHECKSUM_METADATA_VERSION),
            format!("journal_mode={}", self.journal_mode.as_str()),
            format!("free_count={}", self.file_manager.free_pages().len()),
        ];
        for page_id in self.file_manager.free_pages() {
            lines.push(format!("free|{}", page_id));
        }
        lines.push(format!("checksum_count={}", entries.len()));
        for (page_id, checksum) in entries {
            lines.push(format!("checksum|{}|{}", page_id, checksum));
        }

        let contents = lines.join("\n");
        let mut temp_path = path.clone();
        let mut temp_name = temp_path
            .file_name()
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("hematite.checksum"));
        temp_name.push(".tmp");
        temp_path.set_file_name(temp_name);

        fs::write(&temp_path, contents)?;
        fs::rename(&temp_path, path)?;
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
        let Some(transaction) = self.active_rollback_transaction() else {
            return Ok(());
        };
        let Some(path) = &self.journal_path else {
            return Ok(());
        };

        let journal = RollbackJournal {
            state: JournalState::Active,
            original_file_len: transaction.original_file_len,
            original_free_pages: transaction.original_free_pages.clone(),
            original_checksums: transaction
                .original_checksums
                .iter()
                .map(|(page_id, checksum)| (*page_id, *checksum))
                .collect(),
            page_records: Vec::new(),
        };
        let bytes = journal.encode_header(0);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(())
    }

    pub(super) fn append_rollback_journal_record(&self, record: &JournalRecord) -> Result<()> {
        let Some(transaction) = self.active_rollback_transaction() else {
            return Ok(());
        };
        let Some(path) = &self.journal_path else {
            return Ok(());
        };

        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        file.seek(SeekFrom::End(0))?;
        file.write_all(&record.encode()?)?;

        let page_count_offset = RollbackJournal::page_count_offset(
            transaction.original_free_pages.len(),
            transaction.original_checksums.len(),
        );
        let page_count = transaction.page_records.len() as u32;
        file.seek(SeekFrom::Start(page_count_offset))?;
        file.write_all(&page_count.to_le_bytes())?;
        file.sync_all()?;
        Ok(())
    }

    pub(super) fn mark_rollback_journal_committed(&self) -> Result<()> {
        let Some(path) = &self.journal_path else {
            return Ok(());
        };

        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        file.seek(SeekFrom::Start(RollbackJournal::state_offset()))?;
        file.write_all(&[JournalState::Committed as u8])?;
        file.sync_all()?;
        Ok(())
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
        let transaction = self.active_wal_transaction().ok_or_else(|| {
            crate::error::HematiteError::StorageError("Pager transaction is not active".to_string())
        })?;
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

        let mut checksums = self
            .page_checksums
            .iter()
            .map(|(page_id, checksum)| (*page_id, *checksum))
            .collect::<Vec<_>>();
        checksums.sort_by_key(|(page_id, _)| *page_id);

        let record = WalRecord {
            sequence: next_sequence,
            file_len: file_len_for_next_page_id(transaction.wal_next_page_id),
            free_pages: transaction.wal_free_pages.clone(),
            checksums,
            frames,
        };

        self.append_wal_record(record)?;
        for page_id in self.cache.dirty_page_ids() {
            self.cache.clear_dirty(page_id);
        }
        self.persist_checksums()
    }

    pub(super) fn append_wal_record(&mut self, record: WalRecord) -> Result<()> {
        let next_state = self.snapshot_wal_visible_state()?.apply_record(&record)?;

        if let Some(path) = &self.wal_path {
            WalRecord::append_to_path(path, &record)?;
        }

        self.latest_wal_state = Some(next_state);
        Ok(())
    }
}
