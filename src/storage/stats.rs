//! Counters for the work the storage layer actually does.
//!
//! # Why these exist
//!
//! Wall-clock benchmarks cannot police this rewrite. On the machine it is being
//! developed on, the *same unchanged code* measured 76 ms and 97 ms for the same
//! benchmark twenty minutes apart, and 1.55 ms against 3.97 ms for another. A change
//! worth 10 % is invisible under that much drift, and worse, drift that happens to
//! line up with which build ran first reads exactly like a real regression — it did,
//! once, and cost an hour to disprove.
//!
//! Counters do not drift. "This commit issues two fsyncs where it used to issue
//! four" and "this transaction writes each dirty page exactly once" are facts about
//! work performed, they hold on any machine under any load, and they can be asserted
//! in an ordinary `#[test]` that runs in CI without a benchmark harness.
//!
//! That is the point: the expensive operations in a storage engine are syscalls and
//! page copies, and those are all countable. Time is the derived quantity, and the
//! noisy one.
//!
//! # Cost
//!
//! Every counter is a `Relaxed` atomic add next to an operation that is already
//! doing a syscall or a 4 KiB copy, so the counters are free at the resolution
//! anything here cares about. `Relaxed` is right because no counter guards data —
//! they are read for reporting, never to make a decision.

use std::sync::atomic::{AtomicU64, Ordering};

/// Live counters, shared by every component that does storage work.
///
/// Held behind an `Arc` so the file backend, the journal and the WAL can all
/// increment the same set without threading a `&mut` through every call.
#[derive(Debug, Default)]
pub struct PagerStats {
    /// `fsync` calls, across the database file, the journal and the WAL. The single
    /// most important number here: it is what a commit's latency is made of.
    pub fsyncs: AtomicU64,
    /// Pages read from the file backend, i.e. buffer-pool misses that reached I/O.
    pub page_reads: AtomicU64,
    /// Pages written to the database file.
    pub page_writes: AtomicU64,
    /// Page requests served from the buffer pool without touching the backend.
    pub pool_hits: AtomicU64,
    /// Page requests that had to fault a page in.
    pub pool_misses: AtomicU64,
    /// Pages dropped from the pool to make room.
    pub evictions: AtomicU64,
    /// Dirty pages written out mid-transaction because the pool filled up.
    pub spills: AtomicU64,
    /// Bytes appended to the rollback journal.
    pub journal_bytes: AtomicU64,
    /// Frames appended to the WAL.
    pub wal_frames: AtomicU64,
    /// Completed WAL checkpoints.
    pub checkpoints: AtomicU64,
    /// 4 KiB page images copied to preserve a pre-image, for a savepoint's undo log
    /// or for the rollback journal.
    ///
    /// One per savepoint that needs the page, plus one for the journal, is the
    /// floor. `snapshot_original_page` used to make two more on top of that — a
    /// clone of the cached page it was only reading, and a clone of the journal
    /// record so the in-memory list and the journal writer could each have one.
    /// A savepoint is taken per row, so those two came to 79 MB of memcpy over a
    /// 20 000-row insert.
    pub undo_page_copies: AtomicU64,
    /// Times the database file's length was asked of the operating system.
    ///
    /// The pager already knows how long its file is -- it is the only writer, and
    /// every path that changes the length goes through it. Asking the OS is a
    /// `statx` per call, and this counter exists because that call used to sit
    /// inside `deallocate_page`, so a transaction that freed pages paid one syscall
    /// per page to re-learn a number it had just computed.
    pub file_stats: AtomicU64,
}

impl PagerStats {
    #[inline]
    pub(crate) fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn add(counter: &AtomicU64, amount: u64) {
        counter.fetch_add(amount, Ordering::Relaxed);
    }

    /// A consistent-enough point-in-time copy.
    ///
    /// Not an atomic snapshot across all fields — taking one would need a lock on
    /// the hot path to serve a reporting call, which is a bad trade. Callers read
    /// these between operations, where it makes no difference.
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            fsyncs: self.fsyncs.load(Ordering::Relaxed),
            page_reads: self.page_reads.load(Ordering::Relaxed),
            page_writes: self.page_writes.load(Ordering::Relaxed),
            pool_hits: self.pool_hits.load(Ordering::Relaxed),
            pool_misses: self.pool_misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            spills: self.spills.load(Ordering::Relaxed),
            journal_bytes: self.journal_bytes.load(Ordering::Relaxed),
            wal_frames: self.wal_frames.load(Ordering::Relaxed),
            checkpoints: self.checkpoints.load(Ordering::Relaxed),
            undo_page_copies: self.undo_page_copies.load(Ordering::Relaxed),
            file_stats: self.file_stats.load(Ordering::Relaxed),
        }
    }
}

/// A plain copy of [`PagerStats`], so tests can subtract two readings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StatsSnapshot {
    pub fsyncs: u64,
    pub page_reads: u64,
    pub page_writes: u64,
    pub pool_hits: u64,
    pub pool_misses: u64,
    pub evictions: u64,
    pub spills: u64,
    pub journal_bytes: u64,
    pub wal_frames: u64,
    pub checkpoints: u64,
    pub undo_page_copies: u64,
    pub file_stats: u64,
}

impl StatsSnapshot {
    /// Work done between two readings.
    ///
    /// Saturating, so a counter that somehow went backwards reports zero rather
    /// than a huge number that would read as a catastrophic regression.
    pub fn since(&self, earlier: &StatsSnapshot) -> StatsSnapshot {
        StatsSnapshot {
            fsyncs: self.fsyncs.saturating_sub(earlier.fsyncs),
            page_reads: self.page_reads.saturating_sub(earlier.page_reads),
            page_writes: self.page_writes.saturating_sub(earlier.page_writes),
            pool_hits: self.pool_hits.saturating_sub(earlier.pool_hits),
            pool_misses: self.pool_misses.saturating_sub(earlier.pool_misses),
            evictions: self.evictions.saturating_sub(earlier.evictions),
            spills: self.spills.saturating_sub(earlier.spills),
            journal_bytes: self.journal_bytes.saturating_sub(earlier.journal_bytes),
            wal_frames: self.wal_frames.saturating_sub(earlier.wal_frames),
            checkpoints: self.checkpoints.saturating_sub(earlier.checkpoints),
            undo_page_copies: self.undo_page_copies.saturating_sub(earlier.undo_page_copies),
            file_stats: self.file_stats.saturating_sub(earlier.file_stats),
        }
    }
}
