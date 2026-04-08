use super::{JournalMode, Pager, PagerLockMode};
use crate::error::Result;
use crate::storage::Page;

impl Pager {
    pub fn set_journal_mode(&mut self, journal_mode: JournalMode) -> Result<()> {
        if self.transaction.is_some() {
            return Err(crate::error::HematiteError::StorageError(
                "Cannot change pager journal mode during an active transaction".to_string(),
            ));
        }
        if self.journal_mode == journal_mode {
            return Ok(());
        }
        if self.journal_mode == JournalMode::Wal && journal_mode == JournalMode::Rollback {
            if !self.can_checkpoint_wal()? {
                return Err(crate::error::HematiteError::StorageError(
                    "Cannot switch from WAL while readers are active".to_string(),
                ));
            }
            self.checkpoint_wal_unlocked()?;
        }
        if journal_mode == JournalMode::Rollback {
            self.remove_wal_file()?;
            self.latest_wal_state = None;
            self.wal_read_snapshot = None;
        } else {
            self.remove_journal_file()?;
        }
        self.journal_mode = journal_mode;
        if journal_mode == JournalMode::Wal {
            self.load_latest_wal_state()?;
        }
        self.persist_checksums()
    }

    pub fn checkpoint_wal(&mut self) -> Result<()> {
        self.check_error_state()?;
        if self.journal_mode != JournalMode::Wal {
            return Ok(());
        }
        if self.transaction.is_some() {
            return Err(crate::error::HematiteError::StorageError(
                "Cannot checkpoint WAL during an active transaction".to_string(),
            ));
        }
        if !self.can_checkpoint_wal()? {
            return Err(crate::error::HematiteError::StorageError(
                "Cannot checkpoint WAL while readers are active".to_string(),
            ));
        }
        self.checkpoint_wal_unlocked()
    }

    pub(super) fn can_checkpoint_wal(&self) -> Result<bool> {
        if self.database_identity.is_none() {
            return Ok(true);
        }

        let path = self.database_identity_path()?;
        let registry = self.lock_registry_map()?;
        let Some(entry) = registry.get(path) else {
            return Ok(true);
        };

        if entry.writer && self.lock_mode != PagerLockMode::Write {
            return Ok(false);
        }
        if entry.readers == 0 {
            return Ok(true);
        }
        let latest_sequence = self
            .latest_wal_state
            .as_ref()
            .map(|state| state.visible_sequence)
            .unwrap_or(0);
        Ok(entry
            .wal_reader_sequences
            .keys()
            .all(|sequence| *sequence == latest_sequence))
    }

    pub(super) fn checkpoint_wal_unlocked(&mut self) -> Result<()> {
        let Some(state) = self.latest_wal_state.clone() else {
            self.remove_wal_file()?;
            return Ok(());
        };

        self.file_manager.restore_file_len(state.file_len)?;
        self.file_manager.set_free_pages(state.free_pages.clone());
        self.file_manager.compact_free_pages()?;
        for (page_id, data) in &state.page_overrides {
            let page = Page::from_bytes(*page_id, data.clone())?;
            self.file_manager.write_page(&page)?;
        }
        self.file_manager.flush()?;
        self.page_checksums = state.page_checksums;
        self.latest_wal_state = None;
        self.wal_read_snapshot = None;
        self.remove_wal_file()?;
        self.persist_checksums()
    }
}
