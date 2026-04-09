//! Pager implementation.
//!
//! The pager is the only stateful component in the storage layer. It presents the rest of the
//! system with a logical database made of fixed-size pages and hides the machinery required to
//! make those pages durable, reusable, and transactionally visible.
//!
//! Main file layout:
//!
//! ```text
//! byte offset 0
//! +-----------------+-------------+
//! | logical page 0  | db header   |
//! +-----------------+-------------+
//! +-----------------+-------------+
//! | logical page 1  | metadata    |
//! +-----------------+-------------+
//! | logical page 2+ | payload     |
//! +-----------------+-------------+
//! ```
//!
//! Core state inside the pager:
//!
//! ```text
//!                  caller
//!                    |
//!                    v
//!      +----------------------------------+
//!      | read/write/allocate/deallocate    |
//!      +----------------------------------+
//!                    |
//!      +-------------+--------------+
//!      |                            |
//!      v                            v
//! page cache                  transaction state
//! dirty page ids              original pages / WAL frames
//! checksum cache              free-page deltas / file-len deltas
//!      |                            |
//!      +-------------+--------------+
//!                    |
//!                    v
//!              file manager
//! ```
//!
//! Commit algorithms:
//!
//! Rollback mode:
//! - capture the original page image before first write;
//! - persist the rollback journal;
//! - flush dirty main-file pages;
//! - finalize by deleting the journal.
//!
//! WAL mode:
//! - keep page mutations local to the transaction;
//! - append a committed record containing page frames plus pager-visible metadata;
//! - reconstruct reader-visible state by overlaying WAL frames on the main file;
//! - checkpoint later by copying the visible state back into the main file.
//!
//! Reader visibility in WAL mode:
//!
//! ```text
//! main-file page bytes
//!        +
//! last committed WAL sequence visible to this reader
//!        +
//! WAL frame overrides + checksum overrides + freelist snapshot
//!        =
//! effective database image
//! ```
//!
//! Important invariants:
//! - page allocation and freelist state must stay consistent with both the durable file and any
//!   in-flight transaction state;
//! - checksum metadata is part of the durable storage model, not optional verification data;
//! - checkpoints cannot discard page images that are still needed by an active reader snapshot;
//! - higher layers never see partial page writes or raw filesystem ordering concerns.

#[path = "pager/cache.rs"]
mod cache;
#[path = "pager/core.rs"]
mod core;
#[path = "pager/integrity.rs"]
mod integrity;
#[path = "pager/journal.rs"]
mod journal;
#[path = "pager/locking.rs"]
mod locking;
#[path = "pager/page_io.rs"]
mod page_io;
#[path = "pager/reader.rs"]
mod reader;
#[path = "pager/recovery.rs"]
mod recovery;
#[path = "pager/savepoint.rs"]
mod savepoint;
#[path = "pager/space.rs"]
mod space;
#[path = "pager/state.rs"]
mod state;
#[cfg(test)]
#[path = "pager/test_support.rs"]
mod test_support;
#[path = "pager/wal.rs"]
mod wal;

use self::cache::PageCache;
use self::locking::WalReaderRegistration;
use self::state::PagerLockMode;
use crate::error::Result;
use crate::storage::journal::JournalRecord;
use crate::storage::wal::VisibleWalState;
use crate::storage::{
    file_manager::FileManager, file_manager::FileManagerSnapshot, Page, PageId,
    STORAGE_METADATA_PAGE_ID,
};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;

pub(crate) use self::savepoint::PagerSnapshot;
pub use self::state::{JournalMode, PagerState};

#[derive(Debug, Clone)]
pub(crate) struct RollbackTransaction {
    original_file_len: u64,
    original_free_pages: Vec<PageId>,
    original_checksums: HashMap<PageId, u32>,
    journaled_pages: HashSet<PageId>,
    page_records: Vec<JournalRecord>,
    savepoints: Vec<RollbackSavepoint>,
    next_savepoint_id: u64,
}

#[derive(Debug, Clone)]
struct RollbackSavepoint {
    id: u64,
    file_manager: FileManagerSnapshot,
    page_checksums: HashMap<PageId, u32>,
    dirty_pages: Vec<Page>,
    transaction_page_record_count: usize,
    page_records: Vec<JournalRecord>,
    captured_page_ids: HashSet<PageId>,
}

#[derive(Debug, Clone)]
pub(crate) struct WalTransaction {
    wal_next_page_id: PageId,
    wal_free_pages: Vec<PageId>,
    original_checksums: HashMap<PageId, u32>,
}

#[derive(Debug, Clone)]
pub(crate) enum PagerTransaction {
    Rollback(RollbackTransaction),
    Wal(WalTransaction),
}

fn compact_transaction_free_pages(transaction: &mut WalTransaction) {
    transaction.wal_free_pages.sort_unstable();
    transaction.wal_free_pages.dedup();
    while let Some(&last_page_id) = transaction.wal_free_pages.last() {
        if last_page_id + 1 != transaction.wal_next_page_id {
            break;
        }
        transaction.wal_free_pages.pop();
        transaction.wal_next_page_id = transaction.wal_next_page_id.saturating_sub(1);
    }
}

#[derive(Debug)]
pub struct Pager {
    file_manager: FileManager,
    cache: PageCache,
    page_checksums: HashMap<PageId, u32>,
    journal_mode: JournalMode,
    checksum_store_path: Option<PathBuf>,
    journal_path: Option<PathBuf>,
    wal_path: Option<PathBuf>,
    database_identity: Option<PathBuf>,
    lock_mode: PagerLockMode,
    rollback_lock_file: Option<File>,
    wal_write_lock_file: Option<File>,
    wal_reader_registration: Option<WalReaderRegistration>,
    wal_read_snapshot: Option<VisibleWalState>,
    latest_wal_state: Option<VisibleWalState>,
    transaction: Option<PagerTransaction>,
    state: PagerState,
}

impl Pager {
    pub const CHECKSUM_METADATA_VERSION: u32 = 1;

    pub fn new<P: AsRef<Path>>(path: P, cache_capacity: usize) -> Result<Self> {
        let checksum_store_path = Some(Self::checksum_store_path(path.as_ref()));
        let journal_path = Some(Self::journal_path(path.as_ref()));
        let wal_path = Some(Self::wal_path(path.as_ref()));
        let file_manager = FileManager::new(&path)?;
        let database_identity = fs::canonicalize(path.as_ref())
            .ok()
            .or_else(|| Some(path.as_ref().to_path_buf()));
        let mut pager = Self {
            file_manager,
            cache: PageCache::new(cache_capacity),
            page_checksums: HashMap::new(),
            journal_mode: JournalMode::Rollback,
            checksum_store_path,
            journal_path,
            wal_path,
            database_identity,
            lock_mode: PagerLockMode::None,
            rollback_lock_file: None,
            wal_write_lock_file: None,
            wal_reader_registration: None,
            wal_read_snapshot: None,
            latest_wal_state: None,
            transaction: None,
            state: PagerState::Open,
        };
        pager.recover_if_needed()?;
        pager.load_persisted_state()?;
        pager.load_latest_wal_state()?;
        Ok(pager)
    }

    pub fn new_in_memory(cache_capacity: usize) -> Result<Self> {
        let file_manager = FileManager::new_in_memory()?;
        Ok(Self {
            file_manager,
            cache: PageCache::new(cache_capacity),
            page_checksums: HashMap::new(),
            journal_mode: JournalMode::Rollback,
            checksum_store_path: None,
            journal_path: None,
            wal_path: None,
            database_identity: None,
            lock_mode: PagerLockMode::None,
            rollback_lock_file: None,
            wal_write_lock_file: None,
            wal_reader_registration: None,
            wal_read_snapshot: None,
            latest_wal_state: None,
            transaction: None,
            state: PagerState::Open,
        })
    }

    fn check_error_state(&self) -> Result<()> {
        if self.state == PagerState::Error {
            return Err(crate::error::HematiteError::StorageError(
                "Pager is in an error state and requires rollback or restart".to_string(),
            ));
        }
        Ok(())
    }

    pub fn transaction_active(&self) -> bool {
        self.transaction.is_some()
    }

    pub(crate) fn has_pending_changes(&self) -> bool {
        self.transaction.is_some() || self.cache.dirty_count() != 0
    }

    pub fn journal_mode(&self) -> JournalMode {
        self.journal_mode
    }

    fn active_rollback_transaction(&self) -> Option<&RollbackTransaction> {
        match &self.transaction {
            Some(PagerTransaction::Rollback(transaction)) => Some(transaction),
            _ => None,
        }
    }

    fn active_rollback_transaction_mut(&mut self) -> Option<&mut RollbackTransaction> {
        match &mut self.transaction {
            Some(PagerTransaction::Rollback(transaction)) => Some(transaction),
            _ => None,
        }
    }

    fn active_wal_transaction(&self) -> Option<&WalTransaction> {
        match &self.transaction {
            Some(PagerTransaction::Wal(transaction)) => Some(transaction),
            _ => None,
        }
    }

    fn active_wal_transaction_mut(&mut self) -> Option<&mut WalTransaction> {
        match &mut self.transaction {
            Some(PagerTransaction::Wal(transaction)) => Some(transaction),
            _ => None,
        }
    }

    fn current_wal_visible_state(&self) -> Option<&VisibleWalState> {
        self.wal_read_snapshot
            .as_ref()
            .or(self.latest_wal_state.as_ref())
    }
}

impl Drop for Pager {
    fn drop(&mut self) {
        match self.lock_mode {
            PagerLockMode::Write => {
                let _ = self.release_write_lock();
            }
            PagerLockMode::Shared { .. } => {
                let _ = self.release_shared_lock();
            }
            PagerLockMode::None => {}
        }
    }
}
