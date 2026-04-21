use super::{PageCache, Pager, PagerState, PagerTransaction, RollbackSavepoint};
use crate::error::Result;
use crate::storage::{file_manager::FileManagerSnapshot, Page, PageId};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub(crate) enum PagerSnapshot {
    Full {
        file_manager: FileManagerSnapshot,
        cache: PageCache,
        page_checksums: HashMap<PageId, u32>,
        transaction: Option<PagerTransaction>,
        state: PagerState,
    },
    RollbackSavepoint {
        id: u64,
    },
}

impl Pager {
    fn clone_dirty_pages(&self) -> Result<Vec<Page>> {
        let mut dirty_pages = Vec::new();
        for page_id in self.cache_read()?.dirty_page_ids() {
            let page = self.cache_read()?.peek(page_id).cloned().ok_or_else(|| {
                crate::error::HematiteError::StorageError(format!(
                    "Dirty page {} missing from page cache",
                    page_id
                ))
            })?;
            dirty_pages.push(page);
        }
        Ok(dirty_pages)
    }

    pub(crate) fn create_rollback_savepoint(&mut self) -> Result<u64> {
        let file_manager = self.file_manager.snapshot()?;
        let page_checksums = self.page_checksums.clone();
        let dirty_pages = self.clone_dirty_pages()?;
        let dirty_page_ids = dirty_pages
            .iter()
            .map(|page| page.id)
            .collect::<std::collections::HashSet<_>>();

        let savepoint = {
            let transaction = self.active_rollback_transaction().ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Rollback savepoints require an active rollback transaction".to_string(),
                )
            })?;

            RollbackSavepoint {
                id: transaction.next_savepoint_id,
                file_manager,
                rollback_next_page_id: transaction.rollback_next_page_id,
                rollback_free_pages: transaction.rollback_free_list.as_slice().to_vec(),
                rollback_free_page_set: transaction
                    .rollback_free_list
                    .as_slice()
                    .iter()
                    .copied()
                    .collect(),
                rollback_uninitialized_pages: transaction.rollback_uninitialized_pages.clone(),
                page_checksums,
                dirty_pages,
                page_records: Vec::new(),
                captured_page_ids: dirty_page_ids,
            }
        };
        let transaction = self.active_rollback_transaction_mut().ok_or_else(|| {
            crate::error::HematiteError::StorageError(
                "Rollback savepoint state disappeared during snapshot creation".to_string(),
            )
        })?;
        let id = savepoint.id.max(1);
        transaction.next_savepoint_id = id.saturating_add(1);
        transaction
            .savepoints
            .push(RollbackSavepoint { id, ..savepoint });
        Ok(id)
    }

    pub(crate) fn restore_rollback_savepoint(&mut self, id: u64) -> Result<()> {
        let Some(transaction) = self.active_rollback_transaction() else {
            return Ok(());
        };

        let position = transaction
            .savepoints
            .iter()
            .position(|savepoint| savepoint.id == id)
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(format!(
                    "Rollback savepoint {} is not active",
                    id
                ))
            })?;
        let savepoint = transaction.savepoints[position].clone();

        self.file_manager
            .restore_snapshot(savepoint.file_manager.clone())?;
        self.cache_mut()?.reset();
        self.page_checksums = savepoint.page_checksums.clone();
        for page in savepoint.dirty_pages.iter().cloned() {
            let page_id = page.id;
            let cache = self.cache_mut()?;
            cache.put(page);
            cache.mark_dirty(page_id);
        }
        for record in &savepoint.page_records {
            let page = Page::from_bytes(record.page_id, record.data.clone())?;
            let cache = self.cache_mut()?;
            cache.put(page);
            cache.mark_dirty(record.page_id);
        }

        let transaction = self.active_rollback_transaction_mut().ok_or_else(|| {
            crate::error::HematiteError::StorageError(
                "Rollback transaction disappeared during savepoint restore".to_string(),
            )
        })?;
        transaction.rollback_next_page_id = savepoint.rollback_next_page_id;
        transaction
            .rollback_free_list
            .replace(savepoint.rollback_free_pages.clone());
        transaction.rollback_uninitialized_pages = savepoint.rollback_uninitialized_pages.clone();
        transaction.savepoints.truncate(position + 1);
        self.sync_rollback_journal_from_transaction()?;
        self.state = if self.cache_mut()?.dirty_page_ids().is_empty() {
            PagerState::WriterLocked
        } else {
            PagerState::WriterCacheMod
        };
        Ok(())
    }

    pub(crate) fn snapshot(&mut self) -> Result<PagerSnapshot> {
        if self.journal_mode == super::JournalMode::Rollback
            && self.active_rollback_transaction().is_some()
        {
            return Ok(PagerSnapshot::RollbackSavepoint {
                id: self.create_rollback_savepoint()?,
            });
        }

        Ok(PagerSnapshot::Full {
            file_manager: self.file_manager.snapshot()?,
            cache: self.cache_mut()?.clone(),
            page_checksums: self.page_checksums.clone(),
            transaction: self.transaction.clone(),
            state: self.state,
        })
    }

    pub(crate) fn restore_snapshot(&mut self, snapshot: PagerSnapshot) -> Result<()> {
        match snapshot {
            PagerSnapshot::Full {
                file_manager,
                cache,
                page_checksums,
                transaction,
                state,
            } => {
                self.file_manager.restore_snapshot(file_manager)?;
                *self.cache_mut()? = cache;
                self.page_checksums = page_checksums;
                self.transaction = transaction;
                self.sync_rollback_journal_from_transaction()?;
                self.state = state;
                Ok(())
            }
            PagerSnapshot::RollbackSavepoint { id } => self.restore_rollback_savepoint(id),
        }
    }
}
