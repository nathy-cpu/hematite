//! Counters for the work the storage layer actually does.
//!
//! # Why these exist
//!
//! Wall-clock benchmarks cannot police the storage layer's performance. On the
//! development machine the *same unchanged code* measured 76 ms and 97 ms for the
//! same benchmark twenty minutes apart, and 1.55 ms against 3.97 ms for another. A
//! change worth 10 % is invisible under that much drift, and worse, drift that
//! happens to line up with which build ran first reads exactly like a real
//! regression, and costs hours to disprove.
//!
//! Counters do not drift. "This commit issues exactly two fsyncs" and "this
//! transaction writes each dirty page exactly once" are facts about work performed,
//! they hold on any machine under any load, and they can be asserted in an ordinary
//! `#[test]` that runs in CI without a benchmark harness.
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
    /// Pages dropped from the pool to make room. Never a dirty or pinned page: when
    /// only those remain, the pool grows past its capacity instead.
    pub evictions: AtomicU64,
    /// Dirty pages written to the database file mid-transaction because the pool
    /// filled up (`spill_pages`). Rollback mode only, and only pages whose original
    /// image is already synced to the journal.
    pub spills: AtomicU64,
    /// Bytes appended to the rollback journal.
    pub journal_bytes: AtomicU64,
    /// Frames appended to the WAL.
    pub wal_frames: AtomicU64,
    /// Completed WAL checkpoints: frames copied back to the database file and the
    /// WAL deleted. Removing a WAL that holds no committed state is not counted.
    pub checkpoints: AtomicU64,
    /// Page trailers verified on read, each an xxHash64 over 4 KiB.
    ///
    /// Verifying every page read would put an xxHash64 over 4 KiB on the read at
    /// the start of every read scope -- twice per statement, on a page that has
    /// almost always not changed, and 11 % of a point-lookup workload. That read
    /// compares against the cached copy first, and only a page that differs is
    /// verified.
    pub trailer_verifications: AtomicU64,
    /// Cells moved between two leaf pages to rebalance after a delete.
    ///
    /// A B-tree counter rather than a pager one, kept here because this is the set
    /// every layer of the storage stack reports into and a test can reach. It
    /// exists because the rotation is easy to write a test that never reaches: the
    /// whole suite ran it zero times while a 5 000-row `DELETE` ran it 1 672.
    pub leaf_rotations: AtomicU64,
    /// 4 KiB page images copied to preserve a pre-image, for a savepoint's undo log
    /// or for the rollback journal.
    ///
    /// One per savepoint that needs the page, plus one for the journal, is the
    /// floor, and `snapshot_original_page` is written to hit it. A savepoint is
    /// taken per row, so a spare copy on that path is worth tens of megabytes of
    /// memcpy over a bulk insert — which is why it is counted.
    pub undo_page_copies: AtomicU64,
    /// Times the database file's length was asked of the operating system.
    ///
    /// The pager already knows how long its file is -- it is the only writer, and
    /// every path that changes the length goes through it. Asking the OS is a
    /// `statx`, so this is counted to keep it off the hot paths: a stat inside
    /// `deallocate_page`, say, costs a syscall per freed page to re-learn a number
    /// the pager had just computed.
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
            trailer_verifications: self.trailer_verifications.load(Ordering::Relaxed),
            leaf_rotations: self.leaf_rotations.load(Ordering::Relaxed),
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
    pub trailer_verifications: u64,
    pub leaf_rotations: u64,
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
            trailer_verifications: self
                .trailer_verifications
                .saturating_sub(earlier.trailer_verifications),
            leaf_rotations: self.leaf_rotations.saturating_sub(earlier.leaf_rotations),
            undo_page_copies: self
                .undo_page_copies
                .saturating_sub(earlier.undo_page_copies),
            file_stats: self.file_stats.saturating_sub(earlier.file_stats),
        }
    }
}
