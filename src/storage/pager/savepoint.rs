use super::{Pager, PagerSavepoint, PagerState, PagerTransaction};
use crate::error::Result;
use crate::storage::{file_manager::FileManagerSnapshot, Page};
use std::sync::Arc;

/// The contents of a [`PagerSnapshot::Full`].
///
/// Split out and boxed because the enum is only ever as small as its largest
/// variant: inline, every snapshot cost a 424-byte move, including the
/// `PagerSavepoint` case that carries nothing but an id — and one snapshot is
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
    pub(crate) transaction: Option<PagerTransaction>,
    pub(crate) state: PagerState,
}

#[derive(Debug, Clone)]
pub(crate) enum PagerSnapshot {
    Full(Box<FullSnapshot>),
    PagerSavepoint { id: u64 },
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

    /// Takes a savepoint in whichever journal mode is active.
    ///
    /// O(1) in the number of dirty pages: nothing is copied here, and the undo log is
    /// filled lazily by `snapshot_original_page` as pages are modified.
    pub(crate) fn create_savepoint(&mut self) -> Result<u64> {
        let file_manager = self.file_manager.snapshot()?;

        let savepoint = match &self.transaction {
            Some(super::PagerTransaction::Rollback(transaction)) => PagerSavepoint {
                id: transaction.next_savepoint_id,
                file_manager,
                next_page_id: transaction.rollback_next_page_id,
                free_pages: transaction.rollback_free_list.as_slice().to_vec(),
                free_page_set: transaction
                    .rollback_free_list
                    .as_slice()
                    .iter()
                    .copied()
                    .collect(),
                uninitialized_pages: Arc::clone(&transaction.rollback_uninitialized_pages),
                page_images: Vec::new(),
                captured_page_ids: crate::storage::page_hash::PageSet::default(),
                created_page_ids: crate::storage::page_hash::PageSet::default(),
            },
            Some(super::PagerTransaction::Wal(transaction)) => PagerSavepoint {
                id: transaction.next_savepoint_id,
                file_manager,
                next_page_id: transaction.wal_next_page_id,
                free_pages: transaction.wal_free_pages.clone(),
                free_page_set: transaction.wal_free_page_set.clone(),
                uninitialized_pages: Arc::default(),
                page_images: Vec::new(),
                captured_page_ids: crate::storage::page_hash::PageSet::default(),
                created_page_ids: crate::storage::page_hash::PageSet::default(),
            },
            None => {
                return Err(crate::error::HematiteError::StorageError(
                    "Savepoints require an active transaction".to_string(),
                ))
            }
        };

        let id = savepoint.id.max(1);
        match &mut self.transaction {
            Some(super::PagerTransaction::Rollback(transaction)) => {
                transaction.next_savepoint_id = id.saturating_add(1);
                transaction
                    .savepoints
                    .push(PagerSavepoint { id, ..savepoint });
            }
            Some(super::PagerTransaction::Wal(transaction)) => {
                transaction.next_savepoint_id = id.saturating_add(1);
                transaction
                    .savepoints
                    .push(PagerSavepoint { id, ..savepoint });
            }
            None => {
                return Err(crate::error::HematiteError::StorageError(
                    "Transaction disappeared during savepoint creation".to_string(),
                ))
            }
        }
        Ok(id)
    }

    pub(crate) fn restore_savepoint(&mut self, id: u64) -> Result<()> {
        let Some(savepoints) = self.active_savepoints() else {
            return Ok(());
        };

        let position = savepoints
            .iter()
            .position(|savepoint| savepoint.id == id)
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(format!("Savepoint {} is not active", id))
            })?;
        let savepoint = savepoints[position].clone();

        self.file_manager
            .restore_snapshot(savepoint.file_manager.clone())?;

        // Undo exactly what changed, rather than resetting the cache and rebuilding
        // it. The reset was why the savepoint had to hold an image of every dirty
        // page: it threw away pages that were dirty at the mark and never written
        // again, and those exist nowhere on disk. Leaving them alone is both correct
        // and free -- they still hold the contents the savepoint recorded.
        for page_id in savepoint.created_page_ids.iter().copied() {
            self.cache_mut()?.remove(page_id);
        }
        for image in &savepoint.page_images {
            let page_id = image.id;
            let cache = self.cache_mut()?;
            cache.put_shared_with_view(Arc::clone(image), 0);
            cache.mark_dirty(page_id);
        }

        // Restore the allocation state each mode tracks separately, then drop the
        // savepoints taken after this one.
        match &mut self.transaction {
            Some(super::PagerTransaction::Rollback(transaction)) => {
                transaction.rollback_next_page_id = savepoint.next_page_id;
                transaction
                    .rollback_free_list
                    .replace(savepoint.free_pages.clone());
                transaction.rollback_uninitialized_pages =
                    Arc::clone(&savepoint.uninitialized_pages);
                transaction.savepoints.truncate(position + 1);
            }
            Some(super::PagerTransaction::Wal(transaction)) => {
                transaction.wal_next_page_id = savepoint.next_page_id;
                transaction.wal_free_pages.clone_from(&savepoint.free_pages);
                transaction
                    .wal_free_page_set
                    .clone_from(&savepoint.free_page_set);
                transaction.savepoints.truncate(position + 1);
            }
            None => {
                return Err(crate::error::HematiteError::StorageError(
                    "Transaction disappeared during savepoint restore".to_string(),
                ))
            }
        }
        self.sync_rollback_journal_from_transaction()?;
        self.state = if self.cache_mut()?.dirty_page_ids().is_empty() {
            PagerState::WriterLocked
        } else {
            PagerState::WriterCacheMod
        };
        Ok(())
    }

    pub(crate) fn snapshot(&mut self) -> Result<PagerSnapshot> {
        // Both modes take the cheap path now. WAL used to fall through to `Full`,
        // which copies a handle to every dirty page — per row, since `run_atomically`
        // snapshots per row. That made WAL inserts 21 % more expensive than rollback
        // for no reason other than the undo log being rollback-only.
        if self.transaction.is_some() {
            return Ok(PagerSnapshot::PagerSavepoint {
                id: self.create_savepoint()?,
            });
        }

        Ok(PagerSnapshot::Full(Box::new(FullSnapshot {
            file_manager: self.file_manager.snapshot()?,
            dirty_pages: self.share_dirty_pages()?,
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
    pub(crate) fn release_savepoint(&mut self, id: u64) -> Result<()> {
        let Some(savepoints) = self.active_savepoints_mut() else {
            return Ok(());
        };
        if let Some(position) = savepoints.iter().position(|savepoint| savepoint.id == id) {
            savepoints.truncate(position);
        }
        Ok(())
    }

    /// Discards a snapshot without rolling back to it.
    pub(crate) fn release_snapshot(&mut self, snapshot: PagerSnapshot) -> Result<()> {
        match snapshot {
            // A full snapshot owns its page handles outright, so dropping it releases
            // them; there is no registry entry to remove.
            PagerSnapshot::Full(_) => Ok(()),
            PagerSnapshot::PagerSavepoint { id } => self.release_savepoint(id),
        }
    }

    pub(crate) fn restore_snapshot(&mut self, snapshot: PagerSnapshot) -> Result<()> {
        match snapshot {
            PagerSnapshot::Full(full) => {
                let FullSnapshot {
                    file_manager,
                    dirty_pages,
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

                self.transaction = transaction;
                self.sync_rollback_journal_from_transaction()?;
                self.state = state;
                Ok(())
            }
            PagerSnapshot::PagerSavepoint { id } => self.restore_savepoint(id),
        }
    }
}
