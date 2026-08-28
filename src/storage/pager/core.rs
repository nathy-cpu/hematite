use super::{
    JournalMode, Pager, PagerSavepoint, PagerState, PagerTransaction, RollbackTransaction,
    WalTransaction,
};
use crate::error::Result;
use crate::storage::page_hash::PageSet;
use std::sync::Arc;

/// Recycle a finished transaction into the scratch slots, freeing any data that is
/// only needed for rollback (dirty-page images, page-record copies, savepoint
/// snapshots) while keeping the bare allocations (Vec capacity, HashMap bucket
/// arrays) alive for reuse on the next `begin_transaction`.
fn stash_transaction(
    rollback_scratch: &mut Option<RollbackTransaction>,
    wal_scratch: &mut Option<WalTransaction>,
    txn: Option<PagerTransaction>,
) {
    match txn {
        Some(PagerTransaction::Rollback(mut r)) => {
            // savepoints carry dirty-page images (4 KB each) and checksum snapshots;
            // page_records carry original page data for the rollback journal.
            // Both must be dropped here — they are only valid for rollback.
            r.savepoints.clear();
            r.page_records.clear();
            // Clear stale scalar caches; they will be refilled by begin_transaction.
            r.original_free_pages.clear();
            Arc::make_mut(&mut r.rollback_uninitialized_pages).clear();
            r.journaled_pages.clear();
            *rollback_scratch = Some(r);
        }
        Some(PagerTransaction::Wal(mut w)) => {
            w.wal_free_pages.clear();
            w.wal_free_page_set.clear();
            *wal_scratch = Some(w);
        }
        None => {}
    }
}

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
                let mut rollback_free_list = crate::storage::free_list::FreeList::new();
                rollback_free_list.replace(self.file_manager.free_pages().to_vec());
                // Reuse a pre-allocated RollbackTransaction from the scratch slot when available,
                // so inner Vec/HashSet/HashMap allocations survive across transaction boundaries.
                let mut rollback =
                    self.rollback_scratch
                        .take()
                        .unwrap_or_else(|| RollbackTransaction {
                            original_file_len: 0,
                            original_free_pages: Vec::new(),
                            rollback_next_page_id: 0,
                            rollback_free_list: crate::storage::free_list::FreeList::new(),
                            rollback_uninitialized_pages: Arc::default(),
                            journaled_pages: PageSet::default(),
                            page_records: Vec::new(),
                            savepoints: Vec::new(),
                            next_savepoint_id: 1,
                        });
                rollback.original_file_len = self.file_manager.file_len()?;
                rollback.original_free_pages.clear();
                rollback
                    .original_free_pages
                    .extend_from_slice(self.file_manager.free_pages());
                rollback.rollback_next_page_id = self.file_manager.next_page_id();
                rollback.rollback_free_list = rollback_free_list;
                Arc::make_mut(&mut rollback.rollback_uninitialized_pages).clear();
                rollback.journaled_pages.clear();
                rollback.page_records.clear();
                rollback.savepoints.clear();
                rollback.next_savepoint_id = 1;
                let baseline = PagerSavepoint {
                    id: 0,
                    file_manager: self.file_manager.snapshot()?,
                    next_page_id: rollback.rollback_next_page_id,
                    free_pages: rollback.rollback_free_list.as_slice().to_vec(),
                    free_page_set: rollback
                        .rollback_free_list
                        .as_slice()
                        .iter()
                        .copied()
                        .collect(),
                    uninitialized_pages: Arc::clone(&rollback.rollback_uninitialized_pages),
                    page_images: Vec::new(),
                    captured_page_ids: PageSet::default(),
                };
                rollback.savepoints.push(baseline);
                PagerTransaction::Rollback(rollback)
            }
            JournalMode::Wal => {
                let visible_state = self.snapshot_wal_visible_state()?;
                // Reuse a pre-allocated WalTransaction when available.
                let mut wal = self.wal_scratch.take().unwrap_or_else(|| WalTransaction {
                    wal_next_page_id: 0,
                    wal_free_pages: Vec::new(),
                    wal_free_page_set: PageSet::default(),
                    savepoints: Vec::new(),
                    next_savepoint_id: 1,
                });
                wal.savepoints.clear();
                wal.next_savepoint_id = 1;
                wal.wal_next_page_id = visible_state.visible_next_page_id();
                wal.wal_free_pages.clone_from(&visible_state.free_pages);
                wal.wal_free_page_set
                    .clone_from(&visible_state.free_page_set);
                PagerTransaction::Wal(wal)
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
        // Rollback mode only. In WAL mode there is no rollback journal of ours to
        // retire, and retiring one unconditionally would destroy a valid journal
        // left behind by a mode switch — which is crash-recovery evidence.
        if self.journal_mode == JournalMode::Rollback {
            #[cfg(test)]
            let keep = self.keep_journal_after_commit;
            #[cfg(not(test))]
            let keep = false;
            if !keep {
                self.retire_journal_file()?;
            }
        }
        let old_txn = self.transaction.take();
        self.exit_writer_scope_to_open()?;
        stash_transaction(&mut self.rollback_scratch, &mut self.wal_scratch, old_txn);
        // Anything derived from the file is now potentially out of date.
        self.bump_durable_state_token();
        // After the transaction is closed out, so the checkpoint sees no active
        // writer and the commit is already durable whatever happens next.
        self.autocheckpoint_wal_if_needed();
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
        let old_txn = self.transaction.take();
        self.exit_writer_scope_to_open()?;
        stash_transaction(&mut self.rollback_scratch, &mut self.wal_scratch, old_txn);
        // A rollback moves the file back to where it was, which is just as much a
        // change to anything derived from it as a commit is.
        self.bump_durable_state_token();
        Ok(())
    }
}
