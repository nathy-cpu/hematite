use super::{PageCache, Pager, PagerTransaction};
use crate::error::Result;
use crate::storage::{file_manager::FileManagerSnapshot, PageId};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub(crate) struct PagerSnapshot {
    file_manager: FileManagerSnapshot,
    cache: PageCache,
    page_checksums: HashMap<PageId, u32>,
    transaction: Option<PagerTransaction>,
}

impl PagerSnapshot {
    pub(crate) fn into_transaction_baseline(mut self) -> Self {
        for page_id in self.cache.dirty_page_ids() {
            self.cache.clear_dirty(page_id);
        }
        self.transaction = None;
        self
    }
}

impl Pager {
    pub(crate) fn snapshot(&self) -> Result<PagerSnapshot> {
        Ok(PagerSnapshot {
            file_manager: self.file_manager.snapshot()?,
            cache: self.cache.clone(),
            page_checksums: self.page_checksums.clone(),
            transaction: self.transaction.clone(),
        })
    }

    pub(crate) fn restore_snapshot(&mut self, snapshot: PagerSnapshot) -> Result<()> {
        self.file_manager.restore_snapshot(snapshot.file_manager)?;
        self.cache = snapshot.cache;
        self.page_checksums = snapshot.page_checksums;
        self.transaction = snapshot.transaction;
        self.sync_rollback_journal_from_transaction()?;
        Ok(())
    }
}
