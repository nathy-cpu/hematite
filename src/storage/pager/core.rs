use super::{JournalMode, Pager, PagerState, PagerTransaction};
use crate::error::Result;
use std::collections::HashSet;

impl Pager {
    pub fn begin_transaction(&mut self) -> Result<()> {
        self.check_error_state()?;
        if self.transaction.is_some() {
            return Err(crate::error::HematiteError::StorageError(
                "Pager transaction is already active".to_string(),
            ));
        }

        self.acquire_write_lock()?;
        if let Err(err) = self.refresh_persisted_view() {
            let _ = self.release_write_lock();
            return Err(err);
        }

        let transaction = PagerTransaction {
            original_file_len: self.file_manager.file_len()?,
            original_free_pages: self.file_manager.free_pages().to_vec(),
            original_checksums: self.page_checksums.clone(),
            wal_next_page_id: self.file_manager.next_page_id(),
            wal_free_pages: self.file_manager.free_pages().to_vec(),
            journaled_pages: HashSet::new(),
            page_records: Vec::new(),
        };
        self.transaction = Some(transaction);
        self.state = PagerState::WriterLocked;
        if self.journal_mode == JournalMode::Rollback {
            self.begin_rollback_transaction()?;
        }
        Ok(())
    }

    pub fn commit_transaction(&mut self) -> Result<()> {
        self.check_error_state()?;
        if self.transaction.is_none() {
            return Err(crate::error::HematiteError::StorageError(
                "Pager transaction is not active".to_string(),
            ));
        }

        if self.journal_mode == JournalMode::Wal {
            self.commit_wal_transaction()?;
            if self.can_checkpoint_wal()? {
                self.checkpoint_wal_unlocked()?;
            }
            self.state = PagerState::WriterFinished;
        } else {
            self.commit_rollback_transaction()?;
        }
        self.remove_journal_file()?;
        self.transaction = None;
        self.release_write_lock()?;
        self.state = PagerState::Open;
        Ok(())
    }

    pub fn rollback_transaction(&mut self) -> Result<()> {
        if self.transaction.is_none() {
            return Err(crate::error::HematiteError::StorageError(
                "Pager transaction is not active".to_string(),
            ));
        }

        if self.journal_mode == JournalMode::Wal {
            self.rollback_wal_transaction()?;
        } else {
            self.rollback_rollback_transaction()?;
        }
        self.transaction = None;
        self.release_write_lock()?;
        self.state = PagerState::Open;
        Ok(())
    }
}
