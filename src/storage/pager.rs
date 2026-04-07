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

use crate::error::Result;
use crate::storage::journal::{JournalRecord, JournalState, RollbackJournal};
use crate::storage::wal::{VisibleWalState, WalFrame, WalRecord};
use crate::storage::{
    buffer_pool::BufferPool,
    file_manager::{FileManager, FileManagerSnapshot},
    Page, PageId, PagerIntegrityReport, DB_HEADER_PAGE_ID, STORAGE_METADATA_PAGE_ID,
};
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, OnceLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalMode {
    Rollback,
    Wal,
}

impl JournalMode {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "rollback" => Ok(Self::Rollback),
            "wal" => Ok(Self::Wal),
            _ => Err(crate::error::HematiteError::StorageError(format!(
                "Unsupported pager journal mode '{}'",
                value
            ))),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Rollback => "rollback",
            Self::Wal => "wal",
        }
    }
}

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

#[derive(Debug, Clone)]
pub(crate) struct PagerSnapshot {
    file_manager: FileManagerSnapshot,
    buffer_pool: BufferPool,
    dirty_pages: HashSet<PageId>,
    page_checksums: HashMap<PageId, u32>,
    transaction: Option<PagerTransaction>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PagerState {
    Open,
    Reader,
    WriterLocked,
    WriterCacheMod,
    WriterDbMod,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PagerLockMode {
    None,
    Shared { depth: usize },
    Write,
}

#[derive(Debug, Clone, Default)]
struct LockRegistryEntry {
    readers: usize,
    writer: bool,
    wal_reader_sequences: HashMap<u64, usize>,
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

fn lock_registry() -> &'static Mutex<HashMap<PathBuf, LockRegistryEntry>> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, LockRegistryEntry>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Debug)]
pub struct Pager {
    file_manager: FileManager,
    buffer_pool: BufferPool,
    dirty_pages: HashSet<PageId>,
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
    buffer_pool_capacity: usize,
}

impl Pager {
    fn lock_registry_map(
        &self,
    ) -> Result<MutexGuard<'static, HashMap<PathBuf, LockRegistryEntry>>> {
        lock_registry().lock().map_err(|_| {
            crate::error::HematiteError::InternalError(
                "Pager lock registry mutex is poisoned".to_string(),
            )
        })
    }

    fn database_identity_path(&self) -> Result<&PathBuf> {
        self.database_identity.as_ref().ok_or_else(|| {
            crate::error::HematiteError::InternalError(
                "Pager database identity is not available".to_string(),
            )
        })
    }

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
            buffer_pool: BufferPool::new(cache_capacity),
            dirty_pages: HashSet::new(),
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
            buffer_pool_capacity: cache_capacity,
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
            buffer_pool: BufferPool::new(cache_capacity),
            dirty_pages: HashSet::new(),
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
            buffer_pool_capacity: cache_capacity,
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

    pub fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        self.check_error_state()?;
        if let Some(page) = self.buffer_pool.get(page_id) {
            return Ok(page.clone());
        }

        if self.journal_mode == JournalMode::Wal {
            if let Some(transaction) = &self.transaction {
                if page_id >= self.file_manager.next_page_id()
                    && page_id < transaction.wal_next_page_id
                {
                    let page = Page::new(page_id);
                    self.buffer_pool.put(page.clone());
                    return Ok(page);
                }
            }
        }

        if let Some(state) = self
            .wal_read_snapshot
            .as_ref()
            .or(self.latest_wal_state.as_ref())
        {
            if let Some(data) = state.page_overrides.get(&page_id) {
                let page = Page::from_bytes(page_id, data.clone())?;
                if let Some(expected_checksum) = state.page_checksums.get(&page_id) {
                    let actual_checksum = Self::calculate_page_checksum(&page);
                    if actual_checksum != *expected_checksum {
                        return Err(crate::error::HematiteError::CorruptedData(format!(
                            "WAL page checksum mismatch for page {}: expected {}, got {}",
                            page_id, expected_checksum, actual_checksum
                        )));
                    }
                }
                self.buffer_pool.put(page.clone());
                return Ok(page);
            }
        }

        let page = self.file_manager.read_page(page_id)?;
        let expected_checksum = self
            .wal_read_snapshot
            .as_ref()
            .or(self.latest_wal_state.as_ref())
            .and_then(|state| state.page_checksums.get(&page_id))
            .or_else(|| self.page_checksums.get(&page_id));
        if let Some(expected_checksum) = expected_checksum {
            let actual_checksum = Self::calculate_page_checksum(&page);
            if actual_checksum != *expected_checksum {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Page checksum mismatch for page {}: expected {}, got {}",
                    page_id, expected_checksum, actual_checksum
                )));
            }
        }
        self.buffer_pool.put(page.clone());
        Ok(page)
    }

    pub fn write_page(&mut self, page: Page) -> Result<()> {
        self.check_error_state()?;
        let page_id = page.id;
        self.snapshot_original_page(page_id)?;
        if page_id != STORAGE_METADATA_PAGE_ID {
            self.page_checksums
                .insert(page_id, Self::calculate_page_checksum(&page));
        }
        self.buffer_pool.put(page);
        self.dirty_pages.insert(page_id);
        Ok(())
    }

    pub fn allocate_page(&mut self) -> Result<PageId> {
        self.check_error_state()?;
        if self.journal_mode == JournalMode::Wal {
            if let Some(transaction) = &mut self.transaction {
                if let Some(page_id) = transaction.wal_free_pages.pop() {
                    return Ok(page_id);
                }
                let page_id = transaction.wal_next_page_id;
                transaction.wal_next_page_id += 1;
                return Ok(page_id);
            }
        }
        self.file_manager.allocate_page()
    }

    pub fn deallocate_page(&mut self, page_id: PageId) -> Result<()> {
        self.check_error_state()?;
        self.snapshot_original_page(page_id)?;
        self.buffer_pool.remove(page_id);
        self.dirty_pages.remove(&page_id);
        self.page_checksums.remove(&page_id);
        if self.journal_mode == JournalMode::Wal {
            if let Some(transaction) = &mut self.transaction {
                if !transaction.wal_free_pages.contains(&page_id) {
                    transaction.wal_free_pages.push(page_id);
                }
                compact_transaction_free_pages(transaction);
                return Ok(());
            }
            self.file_manager.deallocate_page_deferred(page_id);
            Ok(())
        } else {
            self.file_manager.deallocate_page(page_id)
        }
    }

    pub fn flush(&mut self) -> Result<()> {
        self.check_error_state()?;
        if self.journal_mode == JournalMode::Wal && self.transaction.is_some() {
            return Err(crate::error::HematiteError::StorageError(
                "Cannot flush pager pages directly during an active WAL transaction".to_string(),
            ));
        }

        let dirty_ids = self.dirty_pages.iter().copied().collect::<Vec<_>>();
        let mut metadata_page_dirty = false;

        for page_id in dirty_ids.iter().copied() {
            if page_id == STORAGE_METADATA_PAGE_ID {
                metadata_page_dirty = true;
                continue;
            }

            if let Some(page) = self.buffer_pool.get(page_id) {
                if let Err(e) = self.file_manager.write_page(page) {
                    self.state = PagerState::Error;
                    return Err(e);
                }
            }
            self.dirty_pages.remove(&page_id);
        }

        // Metadata is written last so it cannot describe page state that has not reached disk.
        if metadata_page_dirty {
            if let Some(page) = self.buffer_pool.get(STORAGE_METADATA_PAGE_ID) {
                if let Err(e) = self.file_manager.write_page(page) {
                    self.state = PagerState::Error;
                    return Err(e);
                }
            }
            self.dirty_pages.remove(&STORAGE_METADATA_PAGE_ID);
        }
        if let Err(e) = self.file_manager.flush() {
            self.state = PagerState::Error;
            return Err(e);
        }
        if let Err(e) = self.persist_checksums() {
            self.state = PagerState::Error;
            return Err(e);
        }
        Ok(())
    }

    pub fn begin_transaction(&mut self) -> Result<()> {
        self.check_error_state()?;
        if self.transaction.is_some() {
            return Err(crate::error::HematiteError::StorageError(
                "Pager transaction is already active".to_string(),
            ));
        }

        self.acquire_write_lock()?;

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
        if self.journal_mode == JournalMode::Rollback {
            self.persist_journal(JournalState::Active)?;
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
        } else {
            self.flush()?;
            self.persist_journal(JournalState::Committed)?;
        }
        self.remove_journal_file()?;
        self.transaction = None;
        self.release_write_lock()?;
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
            self.rollback_from_active_transaction()?;
            self.remove_journal_file()?;
        }
        self.transaction = None;
        self.release_write_lock()?;
        self.state = PagerState::Open;
        Ok(())
    }

    pub fn transaction_active(&self) -> bool {
        self.transaction.is_some()
    }

    pub(crate) fn snapshot(&self) -> Result<PagerSnapshot> {
        Ok(PagerSnapshot {
            file_manager: self.file_manager.snapshot()?,
            buffer_pool: self.buffer_pool.clone(),
            dirty_pages: self.dirty_pages.clone(),
            page_checksums: self.page_checksums.clone(),
            transaction: self.transaction.clone(),
        })
    }

    pub(crate) fn restore_snapshot(&mut self, snapshot: PagerSnapshot) -> Result<()> {
        self.file_manager.restore_snapshot(snapshot.file_manager)?;
        self.buffer_pool = snapshot.buffer_pool;
        self.dirty_pages = snapshot.dirty_pages;
        self.page_checksums = snapshot.page_checksums;
        self.transaction = snapshot.transaction;
        Ok(())
    }

    pub fn begin_read(&mut self) -> Result<()> {
        self.check_error_state()?;
        let previous_lock_mode = self.lock_mode;
        self.acquire_shared_lock()?;
        if let Err(err) = self.refresh_persisted_view() {
            let _ = self.release_shared_lock();
            return Err(err);
        }
        if self.journal_mode == JournalMode::Wal {
            if matches!(previous_lock_mode, PagerLockMode::Write) {
                return Ok(());
            }
            if matches!(previous_lock_mode, PagerLockMode::Shared { .. }) {
                return Ok(());
            }
            let snapshot = self.snapshot_wal_visible_state()?;
            self.register_wal_reader_sequence(snapshot.visible_sequence)?;
            self.wal_read_snapshot = Some(snapshot);
        }
        Ok(())
    }

    pub fn end_read(&mut self) -> Result<()> {
        if matches!(self.lock_mode, PagerLockMode::Shared { depth: 1 }) {
            if let Some(snapshot) = &self.wal_read_snapshot {
                self.unregister_wal_reader_sequence(snapshot.visible_sequence)?;
            }
        }
        self.wal_read_snapshot = None;
        self.release_shared_lock()
    }

    pub fn free_pages(&self) -> &[PageId] {
        self.file_manager.free_pages()
    }

    pub fn set_free_pages(&mut self, free_pages: Vec<PageId>) {
        self.file_manager.set_free_pages(free_pages);
    }

    pub fn checksum_entries(&self) -> Vec<(PageId, u32)> {
        self.page_checksums
            .iter()
            .map(|(page_id, checksum)| (*page_id, *checksum))
            .collect()
    }

    pub fn journal_mode(&self) -> JournalMode {
        self.journal_mode
    }

    pub fn set_journal_mode(&mut self, journal_mode: JournalMode) -> Result<()> {
        if self.transaction.is_some() {
            return Err(crate::error::HematiteError::StorageError(
                "Cannot change pager journal mode during an active transaction".to_string(),
            ));
        }
        if self.journal_mode == journal_mode {
            return Ok(());
        }
        if self.journal_mode == JournalMode::Wal && journal_mode == JournalMode::Rollback {
            if !self.can_checkpoint_wal()? {
                return Err(crate::error::HematiteError::StorageError(
                    "Cannot switch from WAL while readers are active".to_string(),
                ));
            }
            self.checkpoint_wal_unlocked()?;
        }
        if journal_mode == JournalMode::Rollback {
            self.remove_wal_file()?;
            self.latest_wal_state = None;
            self.wal_read_snapshot = None;
        } else {
            self.remove_journal_file()?;
        }
        self.journal_mode = journal_mode;
        if journal_mode == JournalMode::Wal {
            self.load_latest_wal_state()?;
        }
        self.persist_checksums()
    }

    pub fn checkpoint_wal(&mut self) -> Result<()> {
        self.check_error_state()?;
        if self.journal_mode != JournalMode::Wal {
            return Ok(());
        }
        if self.transaction.is_some() {
            return Err(crate::error::HematiteError::StorageError(
                "Cannot checkpoint WAL during an active transaction".to_string(),
            ));
        }
        if !self.can_checkpoint_wal()? {
            return Err(crate::error::HematiteError::StorageError(
                "Cannot checkpoint WAL while readers are active".to_string(),
            ));
        }
        self.checkpoint_wal_unlocked()
    }

    pub fn replace_checksums(&mut self, checksums: HashMap<PageId, u32>) {
        self.page_checksums = checksums;
    }

    pub fn file_len(&self) -> Result<u64> {
        self.file_manager.file_len()
    }

    pub fn allocated_page_count(&self) -> usize {
        self.file_manager.allocated_page_count()
    }

    pub fn fragmented_free_page_count(&self) -> usize {
        self.file_manager.fragmented_free_page_count()
    }

    pub fn trailing_free_page_count(&self) -> usize {
        self.file_manager.trailing_free_page_count()
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

            let page = if self.dirty_pages.contains(&page_id) {
                self.buffer_pool.get(page_id).cloned().ok_or_else(|| {
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
        self.dirty_pages.len()
    }

    #[cfg(test)]
    pub(crate) fn wal_snapshot_sequence(&self) -> Option<u64> {
        self.wal_read_snapshot
            .as_ref()
            .map(|snapshot| snapshot.visible_sequence)
    }

    fn checksum_store_path(db_path: &Path) -> PathBuf {
        let mut file_name = db_path
            .file_name()
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("hematite.db"));
        file_name.push(".pager_checksums");
        match db_path.parent() {
            Some(parent) => parent.join(file_name),
            None => PathBuf::from(file_name),
        }
    }

    fn acquire_shared_lock(&mut self) -> Result<()> {
        if self.database_identity.is_none() {
            return Ok(());
        }

        match self.lock_mode {
            PagerLockMode::Write if self.journal_mode == JournalMode::Wal => return Ok(()),
            PagerLockMode::Write => return Ok(()),
            PagerLockMode::Shared { depth } => {
                self.lock_mode = PagerLockMode::Shared { depth: depth + 1 };
                return Ok(());
            }
            PagerLockMode::None => {}
        }

        let path = self.database_identity_path()?.clone();
        let mut registry = self.lock_registry_map()?;
        let entry = registry.entry(path).or_default();
        if entry.writer && self.journal_mode == JournalMode::Rollback {
            return Err(crate::error::HematiteError::StorageError(
                "database is locked for writing".to_string(),
            ));
        }
        entry.readers += 1;
        self.lock_mode = PagerLockMode::Shared { depth: 1 };
        Ok(())
    }

    fn release_shared_lock(&mut self) -> Result<()> {
        let Some(path) = self.database_identity.as_ref() else {
            return Ok(());
        };

        match self.lock_mode {
            PagerLockMode::Write | PagerLockMode::None => return Ok(()),
            PagerLockMode::Shared { depth } if depth > 1 => {
                self.lock_mode = PagerLockMode::Shared { depth: depth - 1 };
                return Ok(());
            }
            PagerLockMode::Shared { .. } => {}
        }

        let mut registry = self.lock_registry_map()?;
        if let Some(entry) = registry.get_mut(path) {
            entry.readers = entry.readers.saturating_sub(1);
            if entry.readers == 0 && !entry.writer {
                registry.remove(path);
            }
        }
        self.lock_mode = PagerLockMode::None;
        Ok(())
    }

    fn register_wal_reader_sequence(&self, sequence: u64) -> Result<()> {
        let Some(path) = self.database_identity.as_ref() else {
            return Ok(());
        };
        let mut registry = self.lock_registry_map()?;
        let entry = registry.entry(path.clone()).or_default();
        *entry.wal_reader_sequences.entry(sequence).or_insert(0) += 1;
        Ok(())
    }

    fn unregister_wal_reader_sequence(&self, sequence: u64) -> Result<()> {
        let Some(path) = self.database_identity.as_ref() else {
            return Ok(());
        };
        let mut registry = self.lock_registry_map()?;
        if let Some(entry) = registry.get_mut(path) {
            if let Some(count) = entry.wal_reader_sequences.get_mut(&sequence) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    entry.wal_reader_sequences.remove(&sequence);
                }
            }
            if entry.readers == 0 && !entry.writer && entry.wal_reader_sequences.is_empty() {
                registry.remove(path);
            }
        }
        Ok(())
    }

    fn acquire_write_lock(&mut self) -> Result<()> {
        if self.database_identity.is_none() {
            self.lock_mode = PagerLockMode::Write;
            return Ok(());
        }
        if self.lock_mode == PagerLockMode::Write {
            return Ok(());
        }
        if matches!(self.lock_mode, PagerLockMode::Shared { .. }) {
            return Err(crate::error::HematiteError::StorageError(
                "cannot upgrade a shared database lock to a write lock".to_string(),
            ));
        }

        let path = self.database_identity_path()?.clone();
        let mut registry = self.lock_registry_map()?;
        let entry = registry.entry(path).or_default();
        if entry.writer || (self.journal_mode == JournalMode::Rollback && entry.readers > 0) {
            return Err(crate::error::HematiteError::StorageError(
                "database is locked".to_string(),
            ));
        }
        entry.writer = true;
        self.lock_mode = PagerLockMode::Write;
        Ok(())
    }

    fn release_write_lock(&mut self) -> Result<()> {
        let Some(path) = self.database_identity.as_ref() else {
            self.lock_mode = PagerLockMode::None;
            return Ok(());
        };
        if self.lock_mode != PagerLockMode::Write {
            return Ok(());
        }

        let mut registry = self.lock_registry_map()?;
        if let Some(entry) = registry.get_mut(path) {
            entry.writer = false;
            if entry.readers == 0 {
                registry.remove(path);
            }
        }
        self.lock_mode = PagerLockMode::None;
        Ok(())
    }

    fn journal_path(db_path: &Path) -> PathBuf {
        let mut file_name = db_path
            .file_name()
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("hematite.db"));
        file_name.push(".journal");
        match db_path.parent() {
            Some(parent) => parent.join(file_name),
            None => PathBuf::from(file_name),
        }
    }

    fn wal_path(db_path: &Path) -> PathBuf {
        let mut file_name = db_path
            .file_name()
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("hematite.db"));
        file_name.push(".wal");
        match db_path.parent() {
            Some(parent) => parent.join(file_name),
            None => PathBuf::from(file_name),
        }
    }

    fn load_persisted_state(&mut self) -> Result<()> {
        let Some(path) = &self.checksum_store_path else {
            return Ok(());
        };

        let contents = match fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err.into()),
        };

        let mut lines = contents.lines();
        let version = lines
            .next()
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Missing pager checksum metadata version".to_string(),
                )
            })?
            .strip_prefix("version=")
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Pager checksum metadata is missing version prefix".to_string(),
                )
            })?
            .parse::<u32>()
            .map_err(|_| {
                crate::error::HematiteError::StorageError(
                    "Invalid pager checksum metadata version".to_string(),
                )
            })?;

        if version != Self::CHECKSUM_METADATA_VERSION {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Unsupported pager checksum metadata version: expected {}, got {}",
                Self::CHECKSUM_METADATA_VERSION,
                version
            )));
        }

        let mut next_line = lines.next().ok_or_else(|| {
            crate::error::HematiteError::StorageError(
                "Missing pager freelist metadata count".to_string(),
            )
        })?;

        if let Some(mode) = next_line.strip_prefix("journal_mode=") {
            self.journal_mode = JournalMode::parse(mode)?;
            next_line = lines.next().ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Missing pager freelist metadata count".to_string(),
                )
            })?;
        } else {
            self.journal_mode = JournalMode::Rollback;
        }

        let expected_free_count = next_line
            .strip_prefix("free_count=")
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Pager freelist metadata is missing count prefix".to_string(),
                )
            })?
            .parse::<usize>()
            .map_err(|_| {
                crate::error::HematiteError::StorageError(
                    "Invalid pager freelist metadata count".to_string(),
                )
            })?;

        let mut free_pages = Vec::with_capacity(expected_free_count);
        for _ in 0..expected_free_count {
            let line = lines.next().ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Pager freelist metadata ended early".to_string(),
                )
            })?;
            let page_id = line
                .strip_prefix("free|")
                .ok_or_else(|| {
                    crate::error::HematiteError::StorageError(
                        "Invalid pager freelist metadata record".to_string(),
                    )
                })?
                .parse::<u32>()
                .map(|page_id| page_id)
                .map_err(|_| {
                    crate::error::HematiteError::StorageError(
                        "Invalid pager freelist page id".to_string(),
                    )
                })?;
            free_pages.push(page_id);
        }

        let expected_count = lines
            .next()
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Missing pager checksum metadata count".to_string(),
                )
            })?
            .strip_prefix("checksum_count=")
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Pager checksum metadata is missing count prefix".to_string(),
                )
            })?
            .parse::<usize>()
            .map_err(|_| {
                crate::error::HematiteError::StorageError(
                    "Invalid pager checksum metadata count".to_string(),
                )
            })?;

        let mut checksums = HashMap::new();
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let payload = line.strip_prefix("checksum|").ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Invalid pager checksum metadata record".to_string(),
                )
            })?;
            let parts = payload.split('|').collect::<Vec<_>>();
            if parts.len() != 2 {
                return Err(crate::error::HematiteError::StorageError(
                    "Invalid pager checksum metadata record".to_string(),
                ));
            }
            let page_id = parts[0].parse::<u32>().map_err(|_| {
                crate::error::HematiteError::StorageError(
                    "Invalid pager checksum page id".to_string(),
                )
            })?;
            let checksum = parts[1].parse::<u32>().map_err(|_| {
                crate::error::HematiteError::StorageError(
                    "Invalid pager checksum value".to_string(),
                )
            })?;
            if checksums.insert(page_id, checksum).is_some() {
                return Err(crate::error::HematiteError::StorageError(format!(
                    "Duplicate pager checksum entry for page {}",
                    page_id
                )));
            }
        }

        if checksums.len() != expected_count {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Pager checksum metadata count mismatch: expected {}, got {}",
                expected_count,
                checksums.len()
            )));
        }

        self.file_manager.set_free_pages(free_pages);
        self.page_checksums = checksums;
        Ok(())
    }

    fn refresh_persisted_view(&mut self) -> Result<()> {
        if self.transaction.is_some() || !self.dirty_pages.is_empty() {
            return Ok(());
        }

        self.buffer_pool = BufferPool::new(self.buffer_pool_capacity);
        self.load_persisted_state()?;
        self.load_latest_wal_state()
    }

    fn snapshot_wal_visible_state(&mut self) -> Result<VisibleWalState> {
        if let Some(state) = &self.latest_wal_state {
            return Ok(state.clone());
        }

        Ok(VisibleWalState {
            visible_sequence: 0,
            file_len: self.file_manager.file_len()?,
            free_pages: self.file_manager.free_pages().to_vec(),
            page_checksums: self.page_checksums.clone(),
            page_overrides: HashMap::new(),
        })
    }

    fn load_latest_wal_state(&mut self) -> Result<()> {
        if self.journal_mode != JournalMode::Wal {
            self.latest_wal_state = None;
            return Ok(());
        }

        let Some(path) = &self.wal_path else {
            self.latest_wal_state = None;
            return Ok(());
        };

        self.latest_wal_state = WalRecord::load_visible_state_from_path(path)?;
        Ok(())
    }

    fn persist_checksums(&self) -> Result<()> {
        let Some(path) = &self.checksum_store_path else {
            return Ok(());
        };

        let mut entries = self
            .page_checksums
            .iter()
            .map(|(page_id, checksum)| (*page_id, *checksum))
            .collect::<Vec<_>>();
        entries.sort_by_key(|(page_id, _)| *page_id);

        let mut lines = vec![
            format!("version={}", Self::CHECKSUM_METADATA_VERSION),
            format!("journal_mode={}", self.journal_mode.as_str()),
            format!("free_count={}", self.file_manager.free_pages().len()),
        ];
        for page_id in self.file_manager.free_pages() {
            lines.push(format!("free|{}", page_id));
        }
        lines.push(format!("checksum_count={}", entries.len()));
        for (page_id, checksum) in entries {
            lines.push(format!("checksum|{}|{}", page_id, checksum));
        }

        fs::write(path, lines.join("\n"))?;
        Ok(())
    }

    fn snapshot_original_page(&mut self, page_id: PageId) -> Result<()> {
        if self.journal_mode == JournalMode::Wal {
            return Ok(());
        }

        let Some(transaction) = &mut self.transaction else {
            return Ok(());
        };

        if transaction.journaled_pages.contains(&page_id) {
            return Ok(());
        }

        let page_end = 64 + ((page_id as u64 + 1) * crate::storage::PAGE_SIZE as u64);
        if page_end > transaction.original_file_len {
            return Ok(());
        }

        let page = self.file_manager.read_page(page_id)?;
        transaction.page_records.push(JournalRecord {
            page_id,
            data: page.data,
        });
        transaction.journaled_pages.insert(page_id);
        self.persist_journal(JournalState::Active)
    }

    fn persist_journal(&self, state: JournalState) -> Result<()> {
        let Some(transaction) = &self.transaction else {
            return Ok(());
        };
        let Some(path) = &self.journal_path else {
            return Ok(());
        };

        let journal = RollbackJournal {
            state,
            original_file_len: transaction.original_file_len,
            original_free_pages: transaction.original_free_pages.clone(),
            original_checksums: transaction
                .original_checksums
                .iter()
                .map(|(page_id, checksum)| (*page_id, *checksum))
                .collect(),
            page_records: transaction.page_records.clone(),
        };
        let bytes = journal.encode()?;
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(())
    }

    fn remove_journal_file(&self) -> Result<()> {
        let Some(path) = &self.journal_path else {
            return Ok(());
        };
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    fn remove_wal_file(&self) -> Result<()> {
        let Some(path) = &self.wal_path else {
            return Ok(());
        };
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    fn recover_if_needed(&mut self) -> Result<()> {
        let Some(path) = &self.journal_path else {
            return Ok(());
        };
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err.into()),
        };

        let journal = RollbackJournal::decode(&bytes)?;
        match journal.state {
            JournalState::Active => {
                self.restore_from_journal(&journal)?;
                self.remove_journal_file()?;
            }
            JournalState::Committed => {
                self.remove_journal_file()?;
            }
        }
        Ok(())
    }

    fn rollback_from_active_transaction(&mut self) -> Result<()> {
        let transaction = self.transaction.clone().ok_or_else(|| {
            crate::error::HematiteError::StorageError("Pager transaction is not active".to_string())
        })?;
        let journal = RollbackJournal {
            state: JournalState::Active,
            original_file_len: transaction.original_file_len,
            original_free_pages: transaction.original_free_pages,
            original_checksums: transaction
                .original_checksums
                .into_iter()
                .collect::<Vec<_>>(),
            page_records: transaction.page_records,
        };
        self.restore_from_journal(&journal)
    }

    fn restore_from_journal(&mut self, journal: &RollbackJournal) -> Result<()> {
        self.buffer_pool = BufferPool::new(self.buffer_pool_capacity);
        self.dirty_pages.clear();
        self.file_manager
            .restore_file_len(journal.original_file_len)?;
        self.file_manager
            .set_free_pages(journal.original_free_pages.clone());

        for record in &journal.page_records {
            let page = Page::from_bytes(record.page_id, record.data.clone())?;
            self.file_manager.write_page(&page)?;
        }
        self.file_manager.flush()?;

        self.page_checksums = journal.original_checksums.iter().copied().collect();
        self.persist_checksums()
    }

    fn rollback_wal_transaction(&mut self) -> Result<()> {
        let transaction = self.transaction.clone().ok_or_else(|| {
            crate::error::HematiteError::StorageError("Pager transaction is not active".to_string())
        })?;
        self.buffer_pool = BufferPool::new(self.buffer_pool_capacity);
        self.dirty_pages.clear();
        self.page_checksums = transaction.original_checksums;
        self.load_latest_wal_state()
    }

    fn commit_wal_transaction(&mut self) -> Result<()> {
        let transaction = self.transaction.as_ref().ok_or_else(|| {
            crate::error::HematiteError::StorageError("Pager transaction is not active".to_string())
        })?;
        let next_sequence = self
            .latest_wal_state
            .as_ref()
            .map(|state| state.visible_sequence + 1)
            .unwrap_or(1);

        let mut page_ids = self.dirty_pages.iter().copied().collect::<Vec<_>>();
        page_ids.sort_unstable();

        let mut frames = Vec::with_capacity(page_ids.len());
        for page_id in page_ids {
            let page = self.buffer_pool.get(page_id).cloned().ok_or_else(|| {
                crate::error::HematiteError::StorageError(format!(
                    "Dirty page {} missing from buffer pool",
                    page_id
                ))
            })?;
            frames.push(WalFrame {
                page_id,
                data: page.data,
            });
        }

        let mut checksums = self
            .page_checksums
            .iter()
            .map(|(page_id, checksum)| (*page_id, *checksum))
            .collect::<Vec<_>>();
        checksums.sort_by_key(|(page_id, _)| *page_id);

        let record = WalRecord {
            sequence: next_sequence,
            file_len: 64 + transaction.wal_next_page_id as u64 * crate::storage::PAGE_SIZE as u64,
            free_pages: transaction.wal_free_pages.clone(),
            checksums,
            frames,
        };

        self.append_wal_record(record)?;
        self.dirty_pages.clear();
        self.persist_checksums()
    }

    fn append_wal_record(&mut self, record: WalRecord) -> Result<()> {
        if let Some(path) = &self.wal_path {
            WalRecord::append_to_path(path, &record)?;
        } else {
            self.latest_wal_state = Some(VisibleWalState {
                visible_sequence: record.sequence,
                file_len: record.file_len,
                free_pages: record.free_pages.clone(),
                page_checksums: record.checksums.iter().copied().collect(),
                page_overrides: record
                    .frames
                    .iter()
                    .map(|frame| (frame.page_id, frame.data.clone()))
                    .collect(),
            });
            return Ok(());
        }

        self.load_latest_wal_state()
    }

    fn can_checkpoint_wal(&self) -> Result<bool> {
        if self.database_identity.is_none() {
            return Ok(true);
        }

        let path = self.database_identity_path()?;
        let registry = self.lock_registry_map()?;
        let Some(entry) = registry.get(path) else {
            return Ok(true);
        };

        if entry.writer && self.lock_mode != PagerLockMode::Write {
            return Ok(false);
        }
        if entry.readers == 0 {
            return Ok(true);
        }
        let latest_sequence = self
            .latest_wal_state
            .as_ref()
            .map(|state| state.visible_sequence)
            .unwrap_or(0);
        Ok(entry
            .wal_reader_sequences
            .keys()
            .all(|sequence| *sequence == latest_sequence))
    }

    fn checkpoint_wal_unlocked(&mut self) -> Result<()> {
        let Some(state) = self.latest_wal_state.clone() else {
            self.remove_wal_file()?;
            return Ok(());
        };

        self.file_manager.restore_file_len(state.file_len)?;
        self.file_manager.set_free_pages(state.free_pages.clone());
        self.file_manager.compact_free_pages()?;
        for (page_id, data) in &state.page_overrides {
            let page = Page::from_bytes(*page_id, data.clone())?;
            self.file_manager.write_page(&page)?;
        }
        self.file_manager.flush()?;
        self.page_checksums = state.page_checksums;
        self.latest_wal_state = None;
        self.wal_read_snapshot = None;
        self.remove_wal_file()?;
        self.persist_checksums()
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
