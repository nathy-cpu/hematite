use super::{PageCache, Pager, PagerTransaction, RollbackSavepoint};
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
    },
    RollbackSavepoint {
        id: u64,
    },
}

impl PagerSnapshot {
    pub(crate) fn into_transaction_baseline(mut self) -> Self {
        if let Self::Full {
            ref mut cache,
            ref mut transaction,
            ..
        } = self
        {
            for page_id in cache.dirty_page_ids() {
                cache.clear_dirty(page_id);
            }
            *transaction = None;
        }
        self
    }
}

impl Pager {
    fn clone_dirty_pages(&self) -> Result<Vec<Page>> {
        let mut dirty_pages = Vec::new();
        for page_id in self.cache.dirty_page_ids() {
            let page = self.cache.peek(page_id).cloned().ok_or_else(|| {
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
                page_checksums,
                dirty_pages,
                transaction_page_record_count: transaction.page_records.len(),
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
        self.cache.reset();
        self.page_checksums = savepoint.page_checksums.clone();
        for page in savepoint.dirty_pages.iter().cloned() {
            let page_id = page.id;
            self.cache.put(page);
            self.cache.mark_dirty(page_id);
        }
        for record in &savepoint.page_records {
            let page = Page::from_bytes(record.page_id, record.data.clone())?;
            self.cache.put(page);
            self.cache.mark_dirty(record.page_id);
        }

        let transaction = self.active_rollback_transaction_mut().ok_or_else(|| {
            crate::error::HematiteError::StorageError(
                "Rollback transaction disappeared during savepoint restore".to_string(),
            )
        })?;
        transaction.savepoints.truncate(position + 1);
        transaction
            .page_records
            .truncate(savepoint.transaction_page_record_count);
        transaction.journaled_pages = transaction
            .page_records
            .iter()
            .map(|record| record.page_id)
            .collect();
        self.sync_rollback_journal_from_transaction()?;
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
            cache: self.cache.clone(),
            page_checksums: self.page_checksums.clone(),
            transaction: self.transaction.clone(),
        })
    }

    pub(crate) fn restore_snapshot(&mut self, snapshot: PagerSnapshot) -> Result<()> {
        match snapshot {
            PagerSnapshot::Full {
                file_manager,
                cache,
                page_checksums,
                transaction,
            } => {
                self.file_manager.restore_snapshot(file_manager)?;
                self.cache = cache;
                self.page_checksums = page_checksums;
                self.transaction = transaction;
                self.sync_rollback_journal_from_transaction()?;
                Ok(())
            }
            PagerSnapshot::RollbackSavepoint { id } => self.restore_rollback_savepoint(id),
        }
    }
}
