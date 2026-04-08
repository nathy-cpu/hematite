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
//! +-------------------------------+
//! | 64-byte file header region    |
//! +-------------------------------+
//! | logical page 0  | db header   |
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
//! buffer pool                 transaction state
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
#[path = "pager/journal.rs"]
mod journal;
#[path = "pager/locking.rs"]
mod locking;
#[path = "pager/recovery.rs"]
mod recovery;
#[path = "pager/reader.rs"]
mod reader;
#[path = "pager/page_io.rs"]
mod page_io;
#[path = "pager/space.rs"]
mod space;
#[path = "pager/savepoint.rs"]
mod savepoint;
#[path = "pager/state.rs"]
mod state;
#[path = "pager/wal.rs"]
mod wal;

use crate::error::Result;
use crate::storage::journal::JournalRecord;
use crate::storage::wal::VisibleWalState;
use crate::storage::{
    file_manager::FileManager,
    Page, PageId, PagerIntegrityReport, DB_HEADER_PAGE_ID, STORAGE_METADATA_PAGE_ID,
};
use self::cache::PageCache;
use self::state::PagerLockMode;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::path::PathBuf;

pub use self::state::{JournalMode, PagerState};
pub(crate) use self::savepoint::PagerSnapshot;

#[derive(Debug, Clone)]
struct PagerTransaction {
    original_file_len: u64,
    original_free_pages: Vec<PageId>,
    original_checksums: HashMap<PageId, u32>,
    wal_next_page_id: PageId,
    wal_free_pages: Vec<PageId>,
    journaled_pages: HashSet<PageId>,
    page_records: Vec<JournalRecord>,
}

fn compact_transaction_free_pages(transaction: &mut PagerTransaction) {
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

    pub fn validate_integrity(&mut self) -> Result<PagerIntegrityReport> {
        let (max_page_id_exclusive, logical_free_pages, logical_checksums, wal_overrides) =
            if let Some(state) = &self.latest_wal_state {
                let page_regions =
                    state.file_len.saturating_sub(64) / crate::storage::PAGE_SIZE as u64;
                (
                    (page_regions as u32).max(2),
                    state.free_pages.clone(),
                    state.page_checksums.clone(),
                    state.page_overrides.clone(),
                )
            } else {
                (
                    self.file_manager.next_page_id(),
                    self.file_manager.free_pages().to_vec(),
                    self.page_checksums.clone(),
                    HashMap::new(),
                )
            };

        let mut free_pages = HashSet::new();

        for &page_id in &logical_free_pages {
            if page_id == DB_HEADER_PAGE_ID || page_id == STORAGE_METADATA_PAGE_ID {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Reserved page {} cannot be marked free",
                    page_id
                )));
            }

            if page_id >= max_page_id_exclusive {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Free page {} exceeds allocated page range (next_page_id={})",
                    page_id, max_page_id_exclusive
                )));
            }

            if !free_pages.insert(page_id) {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Duplicate free page {} detected",
                    page_id
                )));
            }
        }

        if logical_checksums.contains_key(&STORAGE_METADATA_PAGE_ID) {
            return Err(crate::error::HematiteError::CorruptedData(format!(
                "Storage metadata page {} must not have pager checksum metadata",
                STORAGE_METADATA_PAGE_ID
            )));
        }

        let checksummed_pages = logical_checksums.into_iter().collect::<Vec<_>>();
        let checksummed_page_count = checksummed_pages.len();

        let mut verified_checksum_pages = 0usize;
        for (page_id, expected_checksum) in checksummed_pages {
            if page_id >= max_page_id_exclusive {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Checksum entry for page {} exceeds allocated page range (next_page_id={})",
                    page_id, max_page_id_exclusive
                )));
            }

            if free_pages.contains(&page_id) {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Page {} has checksum metadata but is marked free",
                    page_id
                )));
            }

            let page = if self.cache.is_dirty(page_id) {
                self.cache.get(page_id).cloned().ok_or_else(|| {
                    crate::error::HematiteError::StorageError(format!(
                        "Dirty page {} missing from buffer pool",
                        page_id
                    ))
                })?
            } else if let Some(data) = wal_overrides.get(&page_id) {
                Page::from_bytes(page_id, data.clone())?
            } else {
                self.file_manager.read_page(page_id)?
            };

            let actual_checksum = Self::calculate_page_checksum(&page);
            if actual_checksum != expected_checksum {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Page checksum mismatch for page {}: expected {}, got {}",
                    page_id, expected_checksum, actual_checksum
                )));
            }

            verified_checksum_pages += 1;
        }

        Ok(PagerIntegrityReport {
            allocated_page_count: self.file_manager.allocated_page_count(),
            free_page_count: free_pages.len(),
            fragmented_free_page_count: self.file_manager.fragmented_free_page_count(),
            trailing_free_page_count: self.file_manager.trailing_free_page_count(),
            checksummed_page_count,
            verified_checksum_pages,
        })
    }

    fn calculate_page_checksum(page: &Page) -> u32 {
        let mut hash: u32 = 0x811C9DC5;
        for byte in &page.data {
            hash ^= u32::from(*byte);
            hash = hash.wrapping_mul(0x01000193);
        }
        hash
    }

    #[cfg(test)]
    pub(crate) fn dirty_page_count(&self) -> usize {
        self.cache.dirty_count()
    }

    #[cfg(test)]
    pub(crate) fn wal_snapshot_sequence(&self) -> Option<u64> {
        self.wal_read_snapshot
            .as_ref()
            .map(|snapshot| snapshot.visible_sequence)
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

#[cfg(test)]
impl Pager {
    pub fn inject_io_failure(&mut self) {
        self.file_manager.inject_write_failure();
    }

    pub fn state(&self) -> PagerState {
        self.state
    }
}
