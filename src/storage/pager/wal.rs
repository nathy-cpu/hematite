use super::{JournalMode, Pager, PagerLockMode};
use crate::error::Result;
use crate::storage::Page;

impl Pager {
    fn checkpoint_wal_guarded(&mut self) -> Result<()> {
        if let Err(err) = self.checkpoint_wal_unlocked() {
            self.enter_error_state();
            return Err(err);
        }
        Ok(())
    }

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
            self.checkpoint_wal_guarded()?;
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
        self.checkpoint_wal_guarded()
    }

    pub(super) fn can_checkpoint_wal(&self) -> Result<bool> {
        if self.database_identity.is_none() {
            return Ok(true);
        }

        if self.lock_mode != PagerLockMode::Write && self.wal_writer_active()? {
            return Ok(false);
        }

        let active_sequences = self.active_wal_reader_sequences()?;
        if active_sequences.is_empty() {
            return Ok(true);
        }

        let latest_sequence = self
            .latest_wal_state
            .as_ref()
            .map(|state| state.visible_sequence)
            .unwrap_or(0);
        Ok(active_sequences
            .into_iter()
            .all(|sequence| sequence == latest_sequence))
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
        self.cache_mut()?.reset();
        self.remove_wal_file()?;
        self.persist_checksums()
    }
}
