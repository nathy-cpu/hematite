use super::{Pager, PagerState, PagerTransaction, RollbackSavepoint};
use crate::error::Result;
use crate::storage::{file_manager::FileManagerSnapshot, Page, PageId};
use std::collections::HashMap;
use std::sync::Arc;

/// The contents of a [`PagerSnapshot::Full`].
///
/// Split out and boxed because the enum is only ever as small as its largest
/// variant: inline, every snapshot cost a 424-byte move, including the
/// `RollbackSavepoint` case that carries nothing but an id — and one snapshot is
/// taken per row by `run_atomically`. Boxed, the enum is two words.
#[derive(Debug, Clone)]
pub(crate) struct FullSnapshot {
    pub(crate) file_manager: FileManagerSnapshot,
    /// Images of the pages that were dirty when the snapshot was taken.
    ///
    /// This used to be a clone of the entire [`PageCache`], which is both far
    /// more than restoring needs and impossible to keep once the cache becomes
    /// a fixed frame arena rather than a `HashMap` — an arena cannot be cloned.
    ///
    /// Only dirty pages differ from what durable storage holds, so only they
    /// need saving. A clean cached page is byte-identical to the file (or to the
    /// WAL frame the current snapshot resolves to) and can simply be re-read, so
    /// copying it would buy nothing.
    ///
    /// These are shared rather than copied: taking a snapshot costs one refcount
    /// bump per *dirty* page, where cloning the cache cost one per *cached* page
    /// plus a deep copy of the recycled-buffer pool. Holding the `Arc` also pins
    /// the page for the snapshot's lifetime, which is what stops a later write
    /// from mutating the image out from under it.
    pub(crate) dirty_pages: Vec<Arc<Page>>,
    pub(crate) page_checksums: HashMap<PageId, u32>,
    pub(crate) transaction: Option<PagerTransaction>,
    pub(crate) state: PagerState,
}

#[derive(Debug, Clone)]
pub(crate) enum PagerSnapshot {
    Full(Box<FullSnapshot>),
    RollbackSavepoint { id: u64 },
}

impl PagerSnapshot {
    pub(crate) fn into_transaction_baseline(mut self) -> Self {
        if let Self::Full(full) = &mut self {
            // A transaction baseline is "nothing is dirty", so restoring it drops
            // every dirty page rather than reinstating any.
            full.dirty_pages.clear();
            full.transaction = None;
            full.state = PagerState::Open;
        }
        self
    }
}

impl Pager {
    /// Shares the current dirty pages without copying them.
    fn share_dirty_pages(&self) -> Result<Vec<Arc<Page>>> {
        let mut dirty_pages = Vec::new();
        for page_id in self.cache_read()?.dirty_page_ids() {
            let page = self.cache_read()?.peek_shared(page_id).ok_or_else(|| {
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
        let dirty_pages = self.share_dirty_pages()?;
        let dirty_page_ids = dirty_pages
            .iter()
            .map(|page| page.id)
            .collect::<crate::storage::page_hash::PageSet>();

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
            cache.put_shared_with_view(page, 0);
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

        Ok(PagerSnapshot::Full(Box::new(FullSnapshot {
            file_manager: self.file_manager.snapshot()?,
            dirty_pages: self.share_dirty_pages()?,
            page_checksums: self.page_checksums.clone(),
            transaction: self.transaction.clone(),
            state: self.state,
        })))
    }

    /// How many rollback savepoints are currently held.
    ///
    /// Exposed so a test can assert they do not accumulate: `run_atomically` takes one
    /// per row, and each pins every page dirty at the time.
    #[cfg(test)]
    pub(crate) fn active_rollback_savepoint_count(&self) -> usize {
        self.active_rollback_transaction()
            .map(|transaction| transaction.savepoints.len())
            .unwrap_or(0)
    }

    /// Discards a savepoint, keeping the work done since it was taken.
    ///
    /// This is `RELEASE SAVEPOINT`. Without it `run_atomically` dropped only the
    /// savepoint's *id* on success and left the savepoint itself in
    /// `transaction.savepoints` until the transaction ended, so a 500-row batch
    /// accumulated 500 of them. Each holds an `Arc` to every page that was dirty when
    /// it was taken, which grows memory linearly in rows and — because those handles
    /// are shared — stops the page cache from ever reclaiming a buffer:
    /// `Arc::try_unwrap` on a displaced page always failed, so every page write
    /// allocated a fresh zeroed 4 KiB buffer. That was 40 690 `calloc`s and 7.9 % of a
    /// batched insert.
    ///
    /// Releasing truncates from this savepoint upward, matching SQL semantics: a
    /// release invalidates the savepoints taken after it too. `run_atomically` nests
    /// strictly, so in practice this pops exactly one.
    pub(crate) fn release_rollback_savepoint(&mut self, id: u64) -> Result<()> {
        let Some(transaction) = self.active_rollback_transaction_mut() else {
            return Ok(());
        };
        if let Some(position) = transaction
            .savepoints
            .iter()
            .position(|savepoint| savepoint.id == id)
        {
            transaction.savepoints.truncate(position);
        }
        Ok(())
    }

    /// Discards a snapshot without rolling back to it.
    pub(crate) fn release_snapshot(&mut self, snapshot: PagerSnapshot) -> Result<()> {
        match snapshot {
            // A full snapshot owns its page handles outright, so dropping it releases
            // them; there is no registry entry to remove.
            PagerSnapshot::Full(_) => Ok(()),
            PagerSnapshot::RollbackSavepoint { id } => self.release_rollback_savepoint(id),
        }
    }

    pub(crate) fn restore_snapshot(&mut self, snapshot: PagerSnapshot) -> Result<()> {
        match snapshot {
            PagerSnapshot::Full(full) => {
                let FullSnapshot {
                    file_manager,
                    dirty_pages,
                    page_checksums,
                    transaction,
                    state,
                } = *full;
                self.file_manager.restore_snapshot(file_manager)?;

                // Every page that is dirty now is one of two things: it was dirty at
                // the mark, in which case `dirty_pages` holds the image to put back,
                // or it was written after the mark, in which case durable storage
                // already holds the version we are restoring to. Dropping all of them
                // and reinstating the saved images handles both, and leaves clean
                // pages — which never diverged — resident.
                let currently_dirty = self.cache_read()?.dirty_page_ids();
                for page_id in currently_dirty {
                    self.cache_mut()?.remove(page_id);
                }
                for page in dirty_pages {
                    let page_id = page.id;
                    let cache = self.cache_mut()?;
                    cache.put_shared_with_view(page, 0);
                    cache.mark_dirty(page_id);
                }

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
