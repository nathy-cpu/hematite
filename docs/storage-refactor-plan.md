# Storage/Pager Rewrite Plan

This document records two things:

1. the original intended plan for rewriting Hematite's storage layer around a more SQLite-like pager architecture
2. a progress review of what has actually been completed so far

The goal of the rewrite is to improve correctness, concurrency behavior, and long-term performance without forcing upper layers such as the B-tree, catalog, and SQL interface to change their public contract.

## Original Plan

### Rewrite Goal

Rewrite the storage layer around a SQLite-style pager architecture while keeping the current upper-layer contract intact. The public `Pager` API and the types the B-tree, catalog, and SQL layers call today should remain stable. The rewrite should happen behind that boundary.

### What We Wanted To Reuse

The original plan deliberately avoided a giant replace-all. The intent was to keep and reuse the parts of Hematite that were already shaped well:

- `Page`, `PageId`, `PAGE_SIZE`, and reserved page identifiers from `src/storage/types.rs`
- the disk and in-memory backend concept in `src/storage/file_manager.rs`, narrowed to raw page IO, truncate, and sync concerns
- `src/storage/overflow.rs` conceptually, with only adapter changes needed for a new pager/cache design
- `ByteTreeStore`, catalog, and SQL surfaces unchanged at their public boundary

### Target Internal Shape

The target design was a SQLite-like split:

- `Pager`: state machine, locking, journaling/WAL, recovery, savepoints, and visibility rules
- `Page cache`: pinned page objects with dirty, journaled, sync-needed, and writeable flags
- `B-tree`: page-format interpretation only

### Planned Order Of Work

The original sequence was intentionally conservative:

1. define and freeze the current storage contract
2. build a new page-cache and pager state-machine core
3. rewrite rollback mode first
4. port savepoint and snapshot compatibility
5. rewrite WAL last on top of the new pager core

### Public Contract To Preserve

The rewrite was always supposed to preserve source compatibility for callers of:

- `Pager::{read_page, write_page, allocate_page, deallocate_page, flush}`
- `Pager::{begin_read, end_read, begin_transaction, commit_transaction, rollback_transaction}`
- `Pager::{journal_mode, set_journal_mode, checkpoint_wal}`
- `Pager::{snapshot, restore_snapshot, validate_integrity}`
- `ByteTreeStore` and all upper layers above it

The compatibility defaults were:

- keep `Page` as an owned full-page image at the public boundary
- keep page size and reserved page ids stable unless a later explicit file-format migration changes them
- keep both rollback and WAL modes available publicly
- permit internal file-format changes later, but only once rollback and recovery were genuinely rebuilt

### Planned Implementation Phases

#### Phase 0. Freeze The Contract

- treat `src/storage/mod.rs` and the current `Pager` methods as the stable compatibility boundary
- write down a characterization checklist from the current test suite:
  - lock semantics
  - rollback visibility
  - WAL reader snapshots
  - checkpoint behavior
  - snapshot and restore behavior used by catalog transactions and savepoints
  - integrity reporting
- stop adding new logic to the old monolithic `pager.rs` except bug fixes

#### Phase 1. Split Storage Internals Into A New Pager Core

Introduce internal modules under `src/storage/pager/` for:

- core transaction flow
- pager state
- page cache
- locking
- journal handling
- recovery
- savepoints
- WAL

Add an explicit pager state machine modeled after SQLite:

- `Open`
- `Reader`
- `WriterLocked`
- `WriterCacheMod`
- `WriterDbMod`
- `WriterFinished`
- `Error`

Illegal transitions should become explicit storage errors, and partial failures should poison the pager into `Error` until rollback or reopen.

#### Phase 2. Replace The Old Buffer Pool With A Real Page Cache

Replace the old `BufferPool<HashMap<PageId, Page>>` model with a more pager-owned cache that tracks:

- page id
- raw page bytes
- pin count
- dirty state
- writeable state
- journaled state
- sync-needed state

The cache should support:

- fetch shared page
- fetch writeable page
- pin and unpin
- dirty-page iteration in flush order
- eventual spill policy

#### Phase 3. Rewrite Rollback Mode First

Build a new rollback-journal implementation with SQLite-like sequencing:

- transaction begin only acquires locks and records original size metadata
- first write is when journaling begins
- original page image is journaled before the page becomes writeable
- dirty pages reach the main database only after journal durability is guaranteed
- commit is two-phase:
  - durable journal plus database writes
  - journal finalization and removal

This phase was explicitly meant to replace the older "snapshot broad pager state at transaction begin" shape with page-granular rollback behavior.

#### Phase 4. Introduce Pager-Level Savepoints And Snapshot Compatibility

Keep `PagerSnapshot` and `restore_snapshot()` as compatibility APIs, but reimplement them as thin compatibility layers over:

- visible database size
- freelist state
- checksum state
- savepoint markers
- cache invalidation points

This phase was meant to reduce upper-layer dependence on cloning wide pager state.

#### Phase 5. Replace The Locking Model

Replace the old in-process lock registry as the correctness backbone with a more explicit pager lock layer.

The minimum intent was:

- rollback mode: readers and writers exclude each other
- WAL mode: readers may coexist with a writer snapshot, but only one writer is active
- checkpointing must respect active readers

#### Phase 6. Rewrite WAL On Top Of The New Core

WAL was intentionally planned last. The goal was to stop relying on ad hoc visible-state refreshes and rewrite WAL around:

- durable sequence management
- proper writer begin and commit integration with pager states
- reader snapshot acquisition owned by the WAL layer
- checkpointing integrated with page cache ownership

#### Phase 7. Cleanup And Optional File-Format Migration

Once rollback, recovery, and WAL all ran on the new core, old sidecar formats and compatibility shims could be retired and file-format changes considered safely.

### Original Testing Intent

The rewrite plan called for more than "old tests still pass". It aimed to expand coverage with:

- state-machine transition tests
- first-dirty-page and journaling-order tests
- two-phase commit failure tests
- hot-journal recovery tests
- savepoint rollback tests
- pager error-state tests
- cache pin and dirty-order tests
- concurrency and mode-matrix tests
- fault-injection tests inspired by SQLite's emphasis on failure-path correctness

## Progress Review

This section describes the current state of the rewrite as of the latest refactor commits.

### What Has Been Completed

#### 1. `pager.rs` has been substantially decomposed

The old monolithic pager implementation has already been split into focused internal modules:

- `src/storage/pager/cache.rs`
- `src/storage/pager/core.rs`
- `src/storage/pager/integrity.rs`
- `src/storage/pager/journal.rs`
- `src/storage/pager/locking.rs`
- `src/storage/pager/page_io.rs`
- `src/storage/pager/reader.rs`
- `src/storage/pager/recovery.rs`
- `src/storage/pager/savepoint.rs`
- `src/storage/pager/space.rs`
- `src/storage/pager/state.rs`
- `src/storage/pager/test_support.rs`
- `src/storage/pager/wal.rs`

This is meaningful progress. The facade in `src/storage/pager.rs` is now much thinner and closer to the shape the rewrite wanted.

#### 2. The pager state machine is now real, not just descriptive

The pager already had state names, but recent work made those states enforceable:

- centralized transition validation now exists in `src/storage/pager/state.rs`
- reader, flush, journal commit, and transaction flows now use the transition helper
- invalid lock/state combinations are rejected instead of silently drifting
- pager error entry is now more explicit

This is directly relevant to reliability and aligns with the original plan.

#### 3. Lock coordination is cleaner and more intentional

Recent lock coordination work now includes:

- intent-level scope helpers:
  - `enter_reader_scope`
  - `leave_reader_scope`
  - `enter_writer_scope`
  - `leave_writer_scope`
- a shared writer teardown helper:
  - `exit_writer_scope_to_open`
- better in-memory lock bookkeeping, so in-memory pagers respect the same reader-state expectations as file-backed ones

This is not a full locking rewrite yet, but it is real progress toward one.

#### 4. Rollback, WAL, recovery, snapshot, integrity, and page IO are separated internally

The following responsibilities are no longer mixed together in one file:

- rollback-journal flow
- WAL lifecycle and checkpoint operations
- savepoint and snapshot compatibility
- recovery and persisted-state reload
- page reads, writes, and flushes
- allocation and free-page lifecycle
- integrity and checksum validation

This modularization makes the next actual behavior rewrites much less risky.

#### 5. Test coverage has improved along the path

The rewrite effort has already added meaningful regressions around:

- pager reader and writer state progression
- failure-driven error-state behavior
- reader-scope upgrade rejection
- read scopes inside writer transactions
- multithreaded multi-connection rollback and WAL behavior
- WAL snapshot visibility and stale-writer refresh behavior

This is especially valuable because it gives us confidence to change internals without guessing.

### What Is Only Partially Done

#### 1. The page cache is only partially SQLite-like

The cache has moved under pager ownership, which is good, but it is not yet a fully SQLite-like page cache with explicit pinned page headers, journaling flags, and spill policy. It is better factored, but not yet fully redesigned.

#### 2. Locking is cleaner, but still not fundamentally rewritten

The current lock layer is still based on the existing in-process registry model. The state and scope APIs are cleaner now, but the underlying coordination model is not yet the stronger pager-owned locking design the original plan called for.

#### 3. Snapshot/savepoint compatibility is still compatibility-shaped

The snapshot logic has been extracted, which is useful, but it has not yet been reimplemented as a thin savepoint-based compatibility layer over a rebuilt rollback core.

#### 4. Rollback mode still largely reflects the older behavior model

Rollback handling is now isolated in its own module, but it has not yet been rebuilt around the intended page-journal-first sequencing. This is the biggest gap between "well-factored current implementation" and "new pager design."

#### 5. WAL has been extracted, but not rewritten

The WAL path is better separated than before, but it still rides on the current pager behavior rather than a freshly rebuilt core. This is expected, because WAL was intentionally planned after rollback.

### What Has Not Really Started Yet

These goals from the original plan are still substantially ahead of us:

- a true rollback-journal rewrite that replaces broad snapshot-shaped behavior with page-granular journaling semantics
- a pager-owned savepoint/subjournal model
- a fully redesigned page cache with richer page-header flags and pin semantics
- a replacement for the current in-process lock registry as the correctness backbone
- a ground-up WAL rewrite on top of the rebuilt pager core
- the deeper SQLite-style failure-injection and hot-journal recovery matrix described in the original plan

### What This Means In Practice

The rewrite is no longer just a plan. We have completed the structural preparation work that makes the harder parts possible:

- the pager is decomposed
- state transitions are explicit
- lock coordination is cleaner
- test coverage is stronger

But the project is still in the "prepare the ground" stage, not yet the "replace the rollback engine" stage.

That distinction matters:

- We have reduced the risk of the real rewrite.
- We have not yet achieved the main behavioral goal of the real rewrite.

### Recommended Next Step

The next highest-value step is to begin the rollback-core rewrite itself.

That should start with a small, well-bounded slice:

- document the exact current rollback begin/commit/rollback behavior we must preserve at the public boundary
- identify where broad transaction snapshots are still doing work that should belong to page-granular rollback handling
- introduce the first rollback-core helper that records original page images in a more explicit journal-shaped way without yet changing the upper-layer API

In short:

- the scaffolding work is in good shape
- the lock/state groundwork is now credible
- the next milestone should be the first true rollback-behavior rewrite, not more facade extraction
