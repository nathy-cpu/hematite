use super::{
    JournalMode, Pager, PagerState, PagerTransaction, RollbackSavepoint, RollbackTransaction,
    WalTransaction,
};
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

        self.enter_writer_scope()?;
        if let Err(err) = self.refresh_persisted_view() {
            let _ = self.exit_writer_scope_to_open();
            return Err(err);
        }

        self.transaction = Some(match self.journal_mode {
            JournalMode::Rollback => {
                let mut rollback = RollbackTransaction {
                    original_file_len: self.file_manager.file_len()?,
                    original_free_pages: self.file_manager.free_pages().to_vec(),
                    original_checksums: self.page_checksums.clone(),
                    journaled_pages: HashSet::new(),
                    page_records: Vec::new(),
                    savepoints: Vec::new(),
                    next_savepoint_id: 1,
                };
                let baseline = RollbackSavepoint {
                    id: 0,
                    file_manager: self.file_manager.snapshot()?,
                    page_checksums: self.page_checksums.clone(),
                    dirty_pages: Vec::new(),
                    transaction_page_record_count: 0,
                    page_records: Vec::new(),
                    captured_page_ids: HashSet::new(),
                };
                rollback.savepoints.push(baseline);
                PagerTransaction::Rollback(rollback)
            }
            JournalMode::Wal => {
                let visible_state = self.snapshot_wal_visible_state()?;
                self.page_checksums = visible_state.page_checksums.clone();
                PagerTransaction::Wal(WalTransaction {
                    wal_next_page_id: visible_state.visible_next_page_id(),
                    wal_free_pages: visible_state.free_pages.clone(),
                    original_checksums: visible_state.page_checksums,
                })
            }
        });
        self.transition_state(PagerState::WriterLocked)?;
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
            self.transition_state(PagerState::WriterFinished)?;
        } else {
            self.commit_rollback_transaction()?;
        }
        self.remove_journal_file()?;
        self.transaction = None;
        self.exit_writer_scope_to_open()?;
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
        self.exit_writer_scope_to_open()?;
        Ok(())
    }
}
