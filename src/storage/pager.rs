//! # Stateful Page Cache and Transaction Coordinator (Pager)
//!
//! The Pager is the central stateful component of the storage subsystem. It translates the concept of
//! physical file offsets into logical page abstractions, managing an in-memory page cache (buffer pool)
//! and orchestrating ACID transaction boundaries.
//!
//! ---
//!
//! ## 1. Core Database Pager Concepts
//!
//! ### Buffer Pool Management
//! Reading and writing bytes directly to disk for every query is extremely slow. A database engine maintains
//! an in-memory **Buffer Pool** (Page Cache). When a higher-level module (like the B-Tree cursor) requests
//! Page $N$, the pager first checks if it is in the buffer pool. If not, it reads the page from disk into a
//! cache slot (a frame).
//!
//! ### Cache Eviction (LRU)
//! The buffer pool has a fixed capacity. When a new page must be fetched but the cache is full, the pager
//! selects an existing page for eviction using a **Least Recently Used (LRU)** eviction policy.
//! * **Clean Pages**: If the page has not been modified, it is discarded immediately.
//! * **Dirty Pages**: If the page has been modified, it cannot be discarded until it is safely written to disk.
//! * **Pinned Pages**: Pages that are currently held by active cursors are "pinned" and are protected from eviction.
//!
//! ### Transaction Boundaries and Journaling
//! The Pager guarantees durability and atomicity by coordinating how and when dirty pages are written back to disk:
//! * **Rollback Journaling**: In-place updates. Before writing a dirty page to the database file, its original
//!   durable page content is saved in a rollback journal file. If the transaction aborts or the system crashes,
//!   the rollback journal is replayed to undo the changes.
//! * **Write-Ahead Logging (WAL)**: Out-of-place updates. Modifications are appended to a separate log file
//!   (the WAL). The main file remains untouched. Readers see a consistent snapshot by overlaying the WAL overrides
//!   on top of reads from the main database file.
//!
//! ---
//!
//! ## 2. Hematite File & Page Layout Invariants
//!
//! Hematite divides physical file layout into specific logical regions:
//!
//! | Page ID | Page Constant Name | Purpose | Layout Details |
//! |---|---|---|---|
//! | `0` | `DB_HEADER_PAGE_ID` | Database Header | The first 100 bytes are reserved; a 20-byte database header (magic `HMTD`, version, schema root page, next table ID, checksum) is written at the start. |
//! | `1` | `STORAGE_METADATA_PAGE_ID`| Pager Metadata | Dedicated metadata page starting with container magic `HMD1`. Stores pager state (journal mode, free pages, checksums) and catalog metadata. |
//! | `2+`| `FIRST_ALLOCATABLE_PAGE_ID`| User payload pages | Allocated for clustered B-Tree table leaf/interior pages, index B-Tree pages, or overflow page chains. |
//!
//! ---
//!
//! ## 3. Pager State Transitions
//!
//! ### Page Buffer State Transitions
//!
//! ```text
//!              +--------------------------------------+
//!              |                Clean                 |
//!              |         (Matches main file)          |
//!              +--------+--------------------+--------+
//!                       |                    ^
//!                  Page Write             Checkpoint
//!                       |                    |
//!                       v                    |
//!              +--------+--------------------+--------+
//!              |                Dirty                 |
//!              |          (In-memory only)            |
//!              +--------+-----------------------------+
//!                       |
//!                 Commit / Evict
//!                       |
//!                       v
//!              +--------+-----------------------------+
//!              |              Journaled               |
//!              |  (Written to WAL / Rollback Journal) |
//!              +--------+-----------------------------+
//!                       |
//!                 Flush to Main DB
//!                       v
//!              +--------------------------------------+
//!              |               Flushed                |
//!              |       (Durable in main file)         |
//!              +--------------------------------------+
//! ```
//!
//! ### Pager Transaction Lifecycle State Machine
//!
//! ```text
//!                      +----------------------+
//!                      |      1. Idle         |
//!                      +----------+-----------+
//!                                 |
//!                         Begin Transaction
//!                                 |
//!                                 v
//!                      +----------+-----------+
//!                      |      2. Read         |
//!                      +----------+-----------+
//!                                 |
//!                             Page Write
//!                                 |
//!                                 v
//!                      +----------+-----------+
//!                      |      3. Write        |
//!                      +----+-----------+-----+
//!                           |           |
//!                      Commit       Abort / Rollback
//!                           |           |
//!                           v           v
//!             +-------------+--+     +--+-------------+
//!             |  4. Committing |     |  5. Rollback   |
//!             |  (Flush WAL /  |     |  (Replay undo  |
//!             |   Journal)     |     |   or discard)  |
//!             +-------------+--+     +--+-------------+
//!                           |           |
//!                           +-----+-----+
//!                                 |
//!                                 v
//!                           Back to Idle
//! ```
//!
//! ---
//!
//! ## 4. WAL Reader Visibility Overlay
//!
//! When reading page $N$ in WAL mode, the Pager overlays active log frames over the main file to reconstruct
//! the transaction's visible state:
//!
//! ```text
//!                         Read Page N
//!                              |
//!                              v
//!                  +-----------+-----------+
//!                  |   Check Active WAL?   |
//!                  +-----+-----------+-----+
//!                        |           |
//!                    Yes |           | No
//!                        v           v
//!             +----------+---+   +---+----------+
//!             | Read frame   |   | Read page N  |
//!             | from WAL     |   | from main DB |
//!             | file (.wal)  |   | file (.db)   |
//!             +----------+---+   +---+----------+
//!                        |           |
//!                        +-----+-----+
//!                              |
//!                              v
//!                      Construct Page N
//! ```

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
#[cfg(test)]
#[path = "pager/tests.rs"]
mod tests;
#[path = "pager/wal.rs"]
mod wal;

use self::cache::PageCache;
use self::locking::{DbCoordination, HeldLocks};
use self::state::PagerLockMode;
use crate::error::Result;
use crate::storage::free_list::FreeList;
use crate::storage::journal::JournalRecord;
use crate::storage::page_hash::PageSet;
use crate::storage::stats::{PagerStats, StatsSnapshot};
use crate::storage::wal::VisibleWalState;
use crate::storage::{
    file_len_for_next_page_id, file_manager::FileManager, file_manager::FileManagerSnapshot, Page,
    PageId, DB_HEADER_PAGE_ID, STORAGE_METADATA_PAGE_ID,
};
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::SystemTime;

pub(crate) use self::savepoint::PagerSnapshot;
pub use self::state::{JournalMode, PagerState};

#[derive(Debug, Clone)]
pub(crate) struct RollbackTransaction {
    original_file_len: u64,
    original_free_pages: Vec<PageId>,
    rollback_next_page_id: PageId,
    rollback_free_list: FreeList,
    /// Pages this transaction allocated but has not yet written.
    ///
    /// Shared rather than copied: a savepoint is taken per row, each one recorded a
    /// full copy of this set, and page allocation is two orders of magnitude rarer
    /// than row insertion. Under `Arc`, taking a savepoint is a refcount bump and
    /// the copy happens on the next allocation instead — see `Arc::make_mut` at the
    /// three mutation sites.
    rollback_uninitialized_pages: Arc<PageSet>,
    journaled_pages: PageSet,
    page_records: Vec<JournalRecord>,
    savepoints: Vec<PagerSavepoint>,
    next_savepoint_id: u64,
}

/// A savepoint's undo state. Shared by both journal modes: what it takes to return to
/// a mark is the same question either way, and only how a *commit* becomes durable
/// differs between them.
#[derive(Debug, Clone)]
struct PagerSavepoint {
    id: u64,
    file_manager: FileManagerSnapshot,
    next_page_id: PageId,
    free_pages: Vec<PageId>,
    free_page_set: PageSet,
    /// Rollback mode only; empty for WAL, which has no uninitialised-page concept.
    uninitialized_pages: Arc<PageSet>,
    /// Pre-images of the pages modified since the savepoint was taken — the undo log.
    ///
    /// Shared handles, not copies. The cache already holds pages behind an `Arc`
    /// and replaces rather than mutates them when one is written, so the version a
    /// savepoint needs is the handle that was current when it captured. Keeping
    /// that handle also pins it: the write path's in-place fast path unwraps the
    /// `Arc` only when it is unique, so a page a savepoint is holding is copied on
    /// the next write instead of overwritten. That is one copy at most per page
    /// per savepoint, and only if the page is written again — where copying at
    /// capture time was 5.9 pages per deleted row, 120 MB of memcpy over 5 000
    /// deletes.
    ///
    /// Captured lazily by `snapshot_original_page`.
    /// The savepoint used to *also* hold shared images of every page already dirty
    /// when it was taken, because the capture below was skipped for any page the
    /// transaction had already journalled, leaving a hole. Separating the journal's
    /// per-transaction dedup from this per-savepoint one closed the hole and made
    /// taking a savepoint O(1) rather than O(dirty pages) — it ran per row.
    page_images: Vec<Arc<Page>>,
    captured_page_ids: PageSet,
    /// Pages the transaction created after this savepoint, which therefore have no
    /// pre-image. Rolling back drops them instead of restoring them.
    created_page_ids: PageSet,
}

#[derive(Debug, Clone)]
pub(crate) struct WalTransaction {
    wal_next_page_id: PageId,
    wal_free_pages: Vec<PageId>,
    wal_free_page_set: PageSet,
    savepoints: Vec<PagerSavepoint>,
    next_savepoint_id: u64,
}

#[derive(Debug, Clone)]
pub(crate) enum PagerTransaction {
    Rollback(RollbackTransaction),
    Wal(WalTransaction),
}

#[derive(Debug, Clone)]
struct WalReadSnapshot {
    visible_sequence: u64,
    state: Arc<VisibleWalState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WalFileStamp {
    len: u64,
    modified: Option<SystemTime>,
}

fn compact_transaction_free_pages(transaction: &mut WalTransaction) {
    transaction.wal_free_pages.sort_unstable();
    transaction.wal_free_pages.dedup();
    transaction.wal_free_page_set.clear();
    transaction
        .wal_free_page_set
        .extend(transaction.wal_free_pages.iter().copied());
    while let Some(&last_page_id) = transaction.wal_free_pages.last() {
        if last_page_id + 1 != transaction.wal_next_page_id {
            break;
        }
        transaction.wal_free_pages.pop();
        transaction.wal_free_page_set.remove(&last_page_id);
        transaction.wal_next_page_id = transaction.wal_next_page_id.saturating_sub(1);
    }
}

#[derive(Debug)]
pub struct Pager {
    file_manager: FileManager,
    /// Shared with `file_manager`, so page and fsync counts from both the backend
    /// and the journal/WAL land in one place.
    stats: Arc<PagerStats>,
    cache: RwLock<PageCache>,
    journal_mode: JournalMode,
    journal_path: Option<PathBuf>,
    wal_path: Option<PathBuf>,
    database_identity: Option<PathBuf>,
    lock_mode: PagerLockMode,
    /// Shared coordination with any other handle on this database, resolved lazily
    /// on first lock and `None` for an in-memory pager, which has nothing to share.
    coordination: Option<Arc<DbCoordination>>,
    held_locks: HeldLocks,
    /// The commit sequence this pager's read transaction registered, if any.
    wal_reader_sequence: Option<u64>,
    wal_read_snapshot: Option<WalReadSnapshot>,
    latest_wal_state: Option<Arc<VisibleWalState>>,
    latest_wal_file_stamp: Option<WalFileStamp>,
    /// How many frames the WAL file holds, when known.
    ///
    /// Autocheckpointing needs this on every commit to decide whether the WAL has
    /// grown past its threshold, and used to get it by stat'ing the WAL by path --
    /// one `statx` per commit for a number the pager itself determines, since it is
    /// the only writer. `None` means "ask the filesystem once, then remember".
    ///
    /// A stale value can only make the threshold fire a commit early or late, which
    /// for a heuristic is not a correctness question.
    wal_frame_count: Option<u64>,
    transaction: Option<PagerTransaction>,
    /// Pre-allocated rollback transaction state recycled between transactions.
    rollback_scratch: Option<RollbackTransaction>,
    /// Pre-allocated WAL transaction state recycled between transactions.
    wal_scratch: Option<WalTransaction>,
    state: PagerState,
    /// Open handle to the rollback journal file for incremental appending.
    journal_file: Option<crate::storage::journal::JournalFile>,
    /// Number of page records appended to the current journal file.
    journal_record_count: u32,
    /// Byte offset in the journal file where page records begin (after header + metadata).
    journal_header_len: u64,
    /// Whether the current rollback journal contents must be synced before dirty pages can spill
    /// or flush to the main database file.
    journal_needs_sync: bool,
    /// See [`Pager::durable_state_token`].
    durable_state_token: u64,
    /// Reused buffer for reading the persisted state page back to compare it.
    ///
    /// That read happens at the start of every read scope, twice per statement, and
    /// allocating a fresh page-sized buffer for it made a 4 KiB `malloc` and a
    /// 4 KiB zeroing part of the cost of every query.
    metadata_scratch: Option<Box<[u8]>>,
    #[cfg(test)]
    wal_visible_state_reload_count: usize,
    /// Leaves the journal on disk after a commit, standing in for a crash in the
    /// window between the database fsync and the journal being removed.
    #[cfg(test)]
    keep_journal_after_commit: bool,
}

impl Pager {
    pub const CHECKSUM_METADATA_VERSION: u32 = 1;

    fn cache_view_token(&self) -> u64 {
        if self.journal_mode != JournalMode::Wal {
            return 0;
        }
        self.current_wal_visible_state()
            .map(|state| state.visible_sequence.saturating_add(1))
            .unwrap_or(0)
    }

    pub fn new<P: AsRef<Path>>(path: P, cache_capacity: usize) -> Result<Self> {
        let journal_path = Some(Self::journal_path(path.as_ref()));
        let wal_path = Some(Self::wal_path(path.as_ref()));
        let file_manager = FileManager::new(&path)?;
        let database_identity = fs::canonicalize(path.as_ref())
            .ok()
            .or_else(|| Some(path.as_ref().to_path_buf()));
        let stats = file_manager.stats();
        let mut pager = Self {
            file_manager,
            stats,
            cache: RwLock::new(PageCache::new(cache_capacity)),
            journal_mode: JournalMode::Rollback,
            journal_path,
            wal_path,
            database_identity,
            lock_mode: PagerLockMode::None,
            coordination: None,
            held_locks: HeldLocks::default(),
            wal_reader_sequence: None,
            wal_read_snapshot: None,
            latest_wal_state: None,
            latest_wal_file_stamp: None,
            wal_frame_count: None,
            transaction: None,
            rollback_scratch: None,
            wal_scratch: None,
            state: PagerState::Open,
            journal_file: None,
            durable_state_token: 0,
            metadata_scratch: None,
            journal_record_count: 0,
            journal_header_len: 0,
            journal_needs_sync: false,
            #[cfg(test)]
            wal_visible_state_reload_count: 0,
            #[cfg(test)]
            keep_journal_after_commit: false,
        };
        pager.recover_if_needed()?;
        pager.load_persisted_state()?;
        pager.load_latest_wal_state()?;
        Ok(pager)
    }

    pub fn new_in_memory(cache_capacity: usize) -> Result<Self> {
        let file_manager = FileManager::new_in_memory()?;
        let stats = file_manager.stats();
        Ok(Self {
            file_manager,
            stats,
            cache: RwLock::new(PageCache::new(cache_capacity)),
            journal_mode: JournalMode::Rollback,
            journal_path: None,
            wal_path: None,
            database_identity: None,
            lock_mode: PagerLockMode::None,
            coordination: None,
            held_locks: HeldLocks::default(),
            wal_reader_sequence: None,
            wal_read_snapshot: None,
            latest_wal_state: None,
            latest_wal_file_stamp: None,
            wal_frame_count: None,
            transaction: None,
            rollback_scratch: None,
            wal_scratch: None,
            state: PagerState::Open,
            journal_file: None,
            durable_state_token: 0,
            metadata_scratch: None,
            journal_record_count: 0,
            journal_header_len: 0,
            journal_needs_sync: false,
            #[cfg(test)]
            wal_visible_state_reload_count: 0,
            #[cfg(test)]
            keep_journal_after_commit: false,
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

    pub(crate) fn has_pending_changes(&self) -> Result<bool> {
        Ok(self.transaction.is_some() || self.cache_read()?.dirty_count() != 0)
    }

    /// A point-in-time reading of the storage counters.
    ///
    /// Subtract two readings to get the work a single operation performed. This is
    /// how commit behaviour is asserted — fsyncs per commit, pages written per
    /// transaction — since wall-clock timings on a loaded machine drift far more
    /// than the effects being measured.
    pub fn stats(&self) -> StatsSnapshot {
        self.stats.snapshot()
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

    /// The active transaction's savepoints, whichever journal mode it is in.
    ///
    /// Returning to a mark is the same problem in both modes — undo the pages changed
    /// since it — so the savepoint machinery is shared. Only how a *commit* becomes
    /// durable differs.
    fn active_savepoints_mut(&mut self) -> Option<&mut Vec<PagerSavepoint>> {
        match &mut self.transaction {
            Some(PagerTransaction::Rollback(transaction)) => Some(&mut transaction.savepoints),
            Some(PagerTransaction::Wal(transaction)) => Some(&mut transaction.savepoints),
            None => None,
        }
    }

    fn active_savepoints(&self) -> Option<&Vec<PagerSavepoint>> {
        match &self.transaction {
            Some(PagerTransaction::Rollback(transaction)) => Some(&transaction.savepoints),
            Some(PagerTransaction::Wal(transaction)) => Some(&transaction.savepoints),
            None => None,
        }
    }

    fn current_wal_visible_state(&self) -> Option<&VisibleWalState> {
        self.wal_read_snapshot
            .as_ref()
            .map(|snapshot| snapshot.state.as_ref())
            .or_else(|| self.latest_wal_state.as_ref().map(Arc::as_ref))
    }

    /// Read WAL-overridden page data, getting mutable access to the WAL state's
    /// LRU cache. This uses Arc::make_mut to clone-on-write the VisibleWalState
    /// only if other readers are holding references.
    fn read_wal_page_data(&mut self, page_id: PageId) -> crate::error::Result<Option<Vec<u8>>> {
        // Try the read snapshot first (reader path)
        if let Some(snapshot) = &mut self.wal_read_snapshot {
            let state = Arc::make_mut(&mut snapshot.state);
            return state.read_page_data(page_id);
        }
        // Fall back to the latest WAL state (writer path)
        if let Some(state_arc) = &mut self.latest_wal_state {
            let state = Arc::make_mut(state_arc);
            return state.read_page_data(page_id);
        }
        Ok(None)
    }

    /// Immutable read of WAL page data. Checks the LRU cache without
    /// promotion and reads from the WAL file without caching.
    /// Used by `read_page_shared` which takes `&self`.
    fn read_wal_page_data_readonly(
        &self,
        page_id: PageId,
    ) -> crate::error::Result<Option<Vec<u8>>> {
        if let Some(snapshot) = &self.wal_read_snapshot {
            return snapshot.state.read_page_data_readonly(page_id);
        }
        if let Some(state) = &self.latest_wal_state {
            return state.read_page_data_readonly(page_id);
        }
        Ok(None)
    }

    fn rollback_visible_next_page_id(&self) -> Option<PageId> {
        self.active_rollback_transaction()
            .map(|transaction| transaction.rollback_next_page_id)
    }

    fn rollback_page_is_free(&self, page_id: PageId) -> bool {
        self.active_rollback_transaction()
            .map(|transaction| transaction.rollback_free_list.contains(page_id))
            .unwrap_or(false)
    }

    fn rollback_page_is_uninitialized(&self, page_id: PageId) -> bool {
        self.active_rollback_transaction()
            .map(|transaction| transaction.rollback_uninitialized_pages.contains(&page_id))
            .unwrap_or(false)
    }

    pub(super) fn apply_rollback_space_overlay_if_needed(&mut self) -> Result<()> {
        let Some(transaction) = self.active_rollback_transaction() else {
            return Ok(());
        };
        let next_page_id = transaction.rollback_next_page_id;
        let free_pages = transaction.rollback_free_list.as_slice().to_vec();
        self.file_manager
            .restore_file_len(file_len_for_next_page_id(next_page_id))?;
        self.file_manager.set_next_page_id(next_page_id);
        self.file_manager.set_free_pages(free_pages);
        Ok(())
    }

    pub(crate) fn logical_free_pages(&self) -> &[PageId] {
        if let Some(transaction) = self.active_rollback_transaction() {
            transaction.rollback_free_list.as_slice()
        } else if let Some(transaction) = self.active_wal_transaction() {
            &transaction.wal_free_pages
        } else {
            self.file_manager.free_pages()
        }
    }

    pub fn next_page_id(&self) -> PageId {
        if let Some(transaction) = self.active_rollback_transaction() {
            transaction.rollback_next_page_id
        } else if let Some(transaction) = self.active_wal_transaction() {
            transaction.wal_next_page_id
        } else {
            self.file_manager.next_page_id()
        }
    }

    pub(super) fn logical_file_len(&self) -> Result<u64> {
        if let Some(next_page_id) = self.rollback_visible_next_page_id() {
            return Ok(file_len_for_next_page_id(next_page_id));
        }
        self.file_manager.file_len()
    }

    pub(super) fn can_deallocate_page(page_id: PageId) -> Result<()> {
        if page_id == DB_HEADER_PAGE_ID || page_id == STORAGE_METADATA_PAGE_ID {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Cannot deallocate reserved page {}",
                page_id
            )));
        }
        Ok(())
    }

    fn cache_read(&self) -> Result<RwLockReadGuard<'_, PageCache>> {
        self.cache.read().map_err(|_| {
            crate::error::HematiteError::InternalError(
                "Pager page cache lock is poisoned".to_string(),
            )
        })
    }

    fn cache_write(&self) -> Result<RwLockWriteGuard<'_, PageCache>> {
        self.cache.write().map_err(|_| {
            crate::error::HematiteError::InternalError(
                "Pager page cache lock is poisoned".to_string(),
            )
        })
    }

    fn cache_mut(&mut self) -> Result<&mut PageCache> {
        self.cache.get_mut().map_err(|_| {
            crate::error::HematiteError::InternalError(
                "Pager page cache lock is poisoned".to_string(),
            )
        })
    }

    /// Prepare a page for in-place mutation.
    /// Captures original page image (for rollback journals) and returns an owned `Page`
    /// that callers may mutate and then write back using `write_page`.
    pub(crate) fn take_page_for_write(&mut self, page_id: PageId) -> Result<Page> {
        self.check_error_state()?;

        // Capture original page image so rollback journaling has baseline if needed.
        // This mirrors the behavior in `write_page` which snapshots original before
        // mutation. Doing it here avoids races where caller mutates without baseline.
        self.snapshot_original_page(page_id)?;

        // Ask cache for an owned Page ready for mutation (copy-on-write).
        let page = self.cache_mut()?.take_page_for_write(page_id);

        Ok(page)
    }

    /// Zero-copy variant of `take_page_for_write` for callers that guarantee
    /// this page will be committed via exactly one `write_page` call with no
    /// other pager access to the same `page_id` in between. See
    /// `PageCache::take_page_for_write_owned` for the exact contract.
    /// A token that changes whenever this pager's view of durable state does.
    ///
    /// Layers above cache things derived from the file — the catalog's schema and
    /// its per-table row counts, both of which it otherwise re-reads and re-parses
    /// from stored bytes on every statement. Two readings of this that agree mean
    /// nothing has been committed, by this process or another, in between; the
    /// derived state is still current and does not need rebuilding.
    ///
    /// Deliberately coarse. It moves on any commit, not only on ones that changed
    /// the thing a caller cares about, so a stale token is impossible and the worst
    /// outcome is a rebuild that was not strictly required.
    pub fn durable_state_token(&self) -> u64 {
        self.durable_state_token
    }

    pub(crate) fn bump_durable_state_token(&mut self) {
        self.durable_state_token = self.durable_state_token.wrapping_add(1);
    }

    /// Records that the B-tree moved one cell between two leaf pages.
    ///
    /// The rotation itself is above the pager, but this is the counter set the
    /// storage tests read, and the path is one a test can easily fail to reach.
    pub(crate) fn note_leaf_rotation(&self) {
        crate::storage::stats::PagerStats::bump(&self.stats.leaf_rotations);
    }

    pub(crate) fn take_page_for_write_owned(&mut self, page_id: PageId) -> Result<Page> {
        self.check_error_state()?;
        self.snapshot_original_page(page_id)?;
        let page = self.cache_mut()?.take_page_for_write_owned(page_id);
        Ok(page)
    }

    /// Build a fresh, zero-filled `Page` for `page_id` sourced from the
    /// cache's recycled buffer pool instead of a raw heap allocation. Intended
    /// for callers that are about to populate a brand-new page (e.g. a
    /// b-tree split's new sibling) and will write it back via `write_page`,
    /// which performs its own snapshot/checksum bookkeeping.
    pub(crate) fn new_page_buffer(&mut self, page_id: PageId) -> Result<Page> {
        // Callers fill part of the page and rely on the rest reading as zeros --
        // `write_reserved_blob` copies `bytes.len()` and leaves the remainder.
        Ok(Page::new(page_id))
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
