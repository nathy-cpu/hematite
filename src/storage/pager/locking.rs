//! Coordination between database handles in this process.
//!
//! This used to be cross-process coordination built from four `flock` sidecars: a
//! `.rollback.lock`, a `.wal.write.lock`, a `.wal.readers/` directory and one lock
//! file per live read transaction. It cost roughly eight syscalls per commit —
//! four `flock`s, an `unlink`, and the `openat`/`close` pairs to reach them — and
//! bought a guarantee the engine does not offer: Hematite is single-writer /
//! multi-reader **within one process**, and two processes opening the same database
//! was never supported. Paying for cross-process exclusion while not supporting
//! cross-process use is the worst of both.
//!
//! What replaces it is the same state held in memory, shared between handles on the
//! same database through a process-wide registry keyed by path. Waiting uses a
//! `Condvar`, so a blocked writer wakes the moment the lock frees rather than
//! sleeping through a backoff schedule; the five-second ceiling before giving up is
//! unchanged, and so is the error it produces.
//!
//! The locks themselves are unchanged in meaning:
//!
//! - **Rollback mode** puts readers and the writer on the same lock, because
//!   rollback journaling writes pages in place — a reader would see half a
//!   transaction. Reader acquisition is reentrant, counted by depth.
//! - **WAL mode** gives the writer its own lock and lets readers past entirely, so
//!   a reader never waits on a writer. Readers instead register the commit sequence
//!   they are reading at, so a checkpoint knows which frames are still needed.

use super::{JournalMode, Pager, PagerLockMode, PagerState};
use crate::error::{HematiteError, Result};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

/// How long an acquisition waits before reporting the database as locked.
///
/// Unchanged from the `flock` implementation, which polled with an exponential
/// backoff for the same five seconds.
const LOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// Which in-process locks a pager is currently holding.
///
/// Tracked explicitly rather than inferred, because releasing has to undo exactly
/// what was taken: the journal mode can change between acquire and release, and the
/// two modes do not use the same lock.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct HeldLocks {
    rollback_read: bool,
    rollback_write: bool,
    wal_write: bool,
}

#[derive(Debug, Default)]
struct CoordinationState {
    /// Live rollback readers across all handles on this database.
    rollback_readers: usize,
    rollback_writer: bool,
    wal_writer: bool,
    /// Commit sequences that live WAL read transactions still depend on, with a
    /// refcount each. A checkpoint must not discard frames any of these need.
    wal_readers: BTreeMap<u64, usize>,
}

/// The shared coordination state for one database file.
#[derive(Debug, Default)]
pub(crate) struct DbCoordination {
    state: Mutex<CoordinationState>,
    /// Signalled whenever a lock is released, waking anyone waiting to take it.
    released: Condvar,
}

impl DbCoordination {
    fn lock(&self) -> MutexGuard<'_, CoordinationState> {
        // A panic while holding this mutex leaves the counters describing locks whose
        // owner is gone. Recovering the inner value keeps the database usable rather
        // than poisoning every later operation; the alternative is that one panicking
        // test or caller bricks the handle.
        self.state.lock().unwrap_or_else(|err| err.into_inner())
    }

    /// Waits until `ready` holds, then runs `take`.
    ///
    /// Returns the "database is locked" error if the condition has not come true
    /// within [`LOCK_TIMEOUT`].
    fn acquire(
        &self,
        busy_message: &str,
        ready: impl Fn(&CoordinationState) -> bool,
        take: impl FnOnce(&mut CoordinationState),
    ) -> Result<()> {
        let mut state = self.lock();
        let deadline = Instant::now() + LOCK_TIMEOUT;
        while !ready(&state) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(HematiteError::StorageError(busy_message.to_string()));
            }
            let (next, timeout) = self
                .released
                .wait_timeout(state, remaining)
                .unwrap_or_else(|err| err.into_inner());
            state = next;
            if timeout.timed_out() && !ready(&state) {
                return Err(HematiteError::StorageError(busy_message.to_string()));
            }
        }
        take(&mut state);
        Ok(())
    }

    fn release(&self, give_back: impl FnOnce(&mut CoordinationState)) {
        give_back(&mut self.lock());
        self.released.notify_all();
    }

    fn acquire_rollback_read(&self) -> Result<()> {
        self.acquire(
            "database is locked for writing",
            |state| !state.rollback_writer,
            |state| state.rollback_readers += 1,
        )
    }

    fn release_rollback_read(&self) {
        self.release(|state| state.rollback_readers = state.rollback_readers.saturating_sub(1));
    }

    fn acquire_rollback_write(&self) -> Result<()> {
        self.acquire(
            "database is locked",
            |state| !state.rollback_writer && state.rollback_readers == 0,
            |state| state.rollback_writer = true,
        )
    }

    fn release_rollback_write(&self) {
        self.release(|state| state.rollback_writer = false);
    }

    fn acquire_wal_write(&self) -> Result<()> {
        self.acquire(
            "database is locked",
            |state| !state.wal_writer,
            |state| state.wal_writer = true,
        )
    }

    fn release_wal_write(&self) {
        self.release(|state| state.wal_writer = false);
    }

    fn rollback_writer_active(&self) -> bool {
        self.lock().rollback_writer
    }

    fn wal_writer_active(&self) -> bool {
        self.lock().wal_writer
    }

    fn register_wal_reader(&self, sequence: u64) {
        *self.lock().wal_readers.entry(sequence).or_insert(0) += 1;
    }

    fn unregister_wal_reader(&self, sequence: u64) {
        let mut state = self.lock();
        if let Some(count) = state.wal_readers.get_mut(&sequence) {
            *count -= 1;
            if *count == 0 {
                state.wal_readers.remove(&sequence);
            }
        }
    }

    fn active_wal_reader_sequences(&self) -> Vec<u64> {
        self.lock().wal_readers.keys().copied().collect()
    }
}

/// The coordination state for `database_identity`, shared by every handle on it.
///
/// Entries are never evicted, so a process that opens many distinct databases keeps
/// one small struct per path. That is deliberate: a handle can be dropped and
/// reopened while another still holds a lock, and dropping the entry in between
/// would lose the lock state that makes the two agree.
fn coordination_for(database_identity: &Path) -> Arc<DbCoordination> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Arc<DbCoordination>>>> = OnceLock::new();
    let registry = REGISTRY.get_or_init(Mutex::default);
    let mut handles = registry.lock().unwrap_or_else(|err| err.into_inner());
    handles
        .entry(database_identity.to_path_buf())
        .or_default()
        .clone()
}

impl Pager {
    /// The coordination state for this pager, if it is backed by a file.
    ///
    /// An in-memory pager has no identity to share, and nothing to coordinate with,
    /// so every lock operation on it succeeds without touching any state — which is
    /// what the sidecar implementation did too, by returning early whenever the
    /// database identity was absent.
    pub(super) fn coordination(&mut self) -> Option<Arc<DbCoordination>> {
        if let Some(coordination) = &self.coordination {
            return Some(coordination.clone());
        }
        let identity = self.database_identity.as_ref()?;
        let coordination = coordination_for(identity);
        self.coordination = Some(coordination.clone());
        Some(coordination)
    }

    pub(super) fn enter_reader_scope(&mut self) -> Result<PagerLockMode> {
        let previous_lock_mode = self.lock_mode;
        self.acquire_shared_lock()?;
        Ok(previous_lock_mode)
    }

    pub(super) fn leave_reader_scope(&mut self) -> Result<PagerLockMode> {
        self.release_shared_lock()?;
        Ok(self.lock_mode)
    }

    pub(super) fn enter_writer_scope(&mut self) -> Result<()> {
        self.acquire_write_lock()
    }

    pub(super) fn leave_writer_scope(&mut self) -> Result<PagerLockMode> {
        self.release_write_lock()?;
        Ok(self.lock_mode)
    }

    pub(super) fn exit_writer_scope_to_open(&mut self) -> Result<()> {
        let resulting_lock_mode = self.leave_writer_scope()?;
        debug_assert!(matches!(resulting_lock_mode, PagerLockMode::None));
        self.transition_state(PagerState::Open)
    }

    /// Like [`coordination`](Self::coordination) but takes `&self`, so it cannot
    /// cache what it resolves.
    ///
    /// The probes below run before this pager has taken any lock — `has_live_writer`
    /// is consulted during recovery at open — so they cannot rely on the cached
    /// field being populated. Resolving through the registry each time is a hashmap
    /// lookup under a mutex, on paths that run once per open or once per checkpoint.
    fn coordination_ref(&self) -> Option<Arc<DbCoordination>> {
        if let Some(coordination) = &self.coordination {
            return Some(coordination.clone());
        }
        self.database_identity.as_deref().map(coordination_for)
    }

    pub(super) fn has_live_writer(&self) -> Result<bool> {
        Ok(self
            .coordination_ref()
            .is_some_and(|coordination| coordination.rollback_writer_active()))
    }

    pub(super) fn wal_writer_active(&self) -> Result<bool> {
        Ok(self
            .coordination_ref()
            .is_some_and(|coordination| coordination.wal_writer_active()))
    }

    pub(super) fn active_wal_reader_sequences(&self) -> Result<Vec<u64>> {
        Ok(self
            .coordination_ref()
            .map(|coordination| coordination.active_wal_reader_sequences())
            .unwrap_or_default())
    }

    pub(super) fn acquire_shared_lock(&mut self) -> Result<()> {
        match self.lock_mode {
            PagerLockMode::Write => return Ok(()),
            PagerLockMode::Shared { depth } => {
                self.lock_mode = PagerLockMode::Shared { depth: depth + 1 };
                return Ok(());
            }
            PagerLockMode::None => {}
        }

        // Only rollback mode excludes readers and writers from each other. In WAL
        // mode a reader reads a snapshot, so it takes nothing and waits for nobody.
        if self.journal_mode == JournalMode::Rollback {
            if let Some(coordination) = self.coordination() {
                coordination.acquire_rollback_read()?;
                self.held_locks.rollback_read = true;
            }
        }

        self.lock_mode = PagerLockMode::Shared { depth: 1 };
        Ok(())
    }

    pub(super) fn release_shared_lock(&mut self) -> Result<()> {
        match self.lock_mode {
            PagerLockMode::Write | PagerLockMode::None => return Ok(()),
            PagerLockMode::Shared { depth } if depth > 1 => {
                self.lock_mode = PagerLockMode::Shared { depth: depth - 1 };
                return Ok(());
            }
            PagerLockMode::Shared { .. } => {}
        }

        if let Some(sequence) = self.wal_reader_sequence.take() {
            if let Some(coordination) = &self.coordination {
                coordination.unregister_wal_reader(sequence);
            }
            self.wal_read_snapshot = None;
        }

        if self.held_locks.rollback_read {
            if let Some(coordination) = &self.coordination {
                coordination.release_rollback_read();
            }
            self.held_locks.rollback_read = false;
        }

        self.lock_mode = PagerLockMode::None;
        Ok(())
    }

    pub(super) fn register_wal_reader_sequence(&mut self, sequence: u64) -> Result<()> {
        let Some(coordination) = self.coordination() else {
            return Ok(());
        };
        // Re-registering replaces whatever this pager held, matching the sidecar
        // version where a new lock file superseded the old one.
        if let Some(previous) = self.wal_reader_sequence.replace(sequence) {
            coordination.unregister_wal_reader(previous);
        }
        coordination.register_wal_reader(sequence);
        Ok(())
    }

    pub(super) fn unregister_wal_reader_sequence(&mut self, sequence: u64) -> Result<()> {
        let Some(held) = self.wal_reader_sequence else {
            return Ok(());
        };
        if held != sequence {
            return Ok(());
        }

        if let Some(coordination) = &self.coordination {
            coordination.unregister_wal_reader(sequence);
        }
        self.wal_reader_sequence = None;
        self.wal_read_snapshot = None;
        Ok(())
    }

    pub(super) fn acquire_write_lock(&mut self) -> Result<()> {
        if self.lock_mode == PagerLockMode::Write {
            return Ok(());
        }
        if matches!(self.lock_mode, PagerLockMode::Shared { .. }) {
            return Err(HematiteError::StorageError(
                "cannot upgrade a shared database lock to a write lock".to_string(),
            ));
        }

        let rollback_mode = self.journal_mode == JournalMode::Rollback;
        if let Some(coordination) = self.coordination() {
            if rollback_mode {
                coordination.acquire_rollback_write()?;
                self.held_locks.rollback_write = true;
            } else {
                coordination.acquire_wal_write()?;
                self.held_locks.wal_write = true;
            }
        }

        self.lock_mode = PagerLockMode::Write;
        Ok(())
    }

    pub(super) fn release_write_lock(&mut self) -> Result<()> {
        if self.lock_mode != PagerLockMode::Write {
            return Ok(());
        }

        if let Some(coordination) = &self.coordination {
            if self.held_locks.rollback_write {
                coordination.release_rollback_write();
            }
            if self.held_locks.wal_write {
                coordination.release_wal_write();
            }
        }
        self.held_locks.rollback_write = false;
        self.held_locks.wal_write = false;

        self.lock_mode = PagerLockMode::None;
        Ok(())
    }
}
