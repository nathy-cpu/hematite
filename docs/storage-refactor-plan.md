# Storage Format Overhaul Plan

This document replaces the earlier storage rewrite plan.

The previous refactor successfully improved pager structure, state management, fault handling, and
concurrency behavior, but it did not perform the kind of overhaul needed to unlock large storage
performance gains. In particular, it preserved Hematite's custom on-disk layout, custom B-tree
page shape, custom rollback journal, custom WAL record format, and sidecar metadata files.

That was too conservative for the actual goal.

If the goal is to get materially closer to SQLite's compactness, write efficiency, and read-path
behavior, then the next storage rewrite must explicitly include a file-format redesign, not just a
pager-behavior cleanup.

For the short operational view, see [storage-refactor-board.md](./storage-refactor-board.md).

## Reset In One Sentence

The next storage campaign should keep the upper library API stable where practical, but it should
stop preserving the current storage file format and instead rebuild the lower storage stack around
SQLite-like page, cache, rollback-journal, and WAL principles.

## Why The Previous Plan Was Not Enough

The previous storage rewrite concentrated on:

- pager decomposition
- explicit state transitions
- cleaner lock handling
- rollback and WAL behavior cleanup
- savepoint and fault-path correctness

That work was valuable, but it mostly changed behavior around the existing storage model.

It did not replace the deeper format-level choices that still dominate storage cost:

- a custom database file layout with a 64-byte prelude and reserved page 0/page 1 scheme
- custom B-tree node pages that serialize contiguous key/value sections instead of a SQLite-style
  slotted page with cell pointers, freeblocks, and fragments
- custom overflow page layout
- a WAL format that records whole visible-state transitions instead of compact frame-oriented append
- a rollback journal that still reflects Hematite-specific metadata shape
- sidecar checksum and freelist persistence outside the main database and journal/WAL files

Those choices are now the main structural gap between Hematite and SQLite on both performance and
disk efficiency.

## New Rewrite Goal

Rewrite the storage layer so that Hematite's on-disk representation and lower-level pager behavior
move substantially closer to SQLite's storage design:

- page-oriented main database file with SQLite-like header discipline
- slotted B-tree pages with pointer arrays, cell-content region, freeblocks, and defragmentation
- compact overflow handling
- pager-owned page cache with real pinned page objects and dirty-page discipline
- rollback journal with journal-first overwrite safety
- WAL based on page frames rather than whole visible-state snapshots
- no hot-path dependence on sidecar checksum or freelist files

This is now an explicit format migration effort, not a cleanup afterthought.

## Compatibility Boundary

The compatibility boundary changes from the previous plan.

### What We Still Want To Preserve

- the public high-level library surface
- `Hematite`, `Connection`, `Catalog`, and query-layer APIs where possible
- the broad pager call shape used by upper layers, unless a narrow internal-only adapter is cleaner
- test-observable SQL behavior

### What We No Longer Intend To Preserve

- the current main database file layout
- the current B-tree page format
- the current overflow page format
- the current rollback journal format
- the current WAL file format
- the current `.pager_checksums` sidecar model
- compatibility with databases written by the current format

That is an intentional choice. The old format has become part of the problem.

## New Target Shape

The new storage target is still layered, but it is more ambitious than the previous rewrite.

### Layering

- `Pager`
  owns locks, page cache, rollback journal, WAL, savepoints, recovery, and write ordering
- `Page Cache`
  owns pinned page objects, dirty ordering, spill policy, sync-needed tracking, and writeability
- `B-tree`
  owns page-content interpretation, cursor navigation, balancing, free-space accounting, and
  overflow behavior

### Main Design Principles To Copy From SQLite

- separate page identity and durability from page-content interpretation
- make pages slotted rather than fully rebuilt key/value blobs
- minimize full-page rewrites during ordinary mutation
- make first-dirty-page the true cost boundary for journaling
- make dirty, writeable, journaled, and need-sync page states explicit
- keep savepoints pager-owned
- store durable metadata in the main database and journal/WAL protocols, not in ad hoc sidecars

## New Scope

This overhaul now includes three things the previous plan explicitly postponed:

1. a new main-file format
2. a new B-tree page format
3. a new rollback/WAL format

Without those, we should not expect the kind of large performance jump we were looking for.

## Proposed Execution Order

The new order is incremental, but it is no longer format-conservative.

### Phase 0. Freeze The Upper Boundary And Acknowledge The Format Reset

- treat the SQL, catalog, and public Rust APIs as the desired compatibility boundary
- explicitly declare current on-disk files incompatible with the new storage generation
- stop treating the current page format as something to preserve
- keep the current pager refactor as scaffolding, not as the final target

### Phase 1. Define The New File Format Before Rewriting More Behavior

Write down the new format contract first:

- main file header placement and contents
- reserved pages and whether page 1 carries the database header
- B-tree page header layout
- cell pointer array layout
- freeblock and fragment accounting
- local payload versus overflow payload rules
- overflow page layout
- rollback journal record format
- WAL frame format

Nothing else should proceed until this format contract is written clearly.

### Phase 2. Rewrite B-tree Page Layout Around Slotted Pages

Replace the current contiguous serialized node shape with SQLite-like slotted pages:

- page header
- cell pointer array
- cell-content region growing from the end
- freeblocks
- fragment accounting
- defragmentation path

This should be treated as the real turning point of the rewrite.

Expected payoff:

- fewer whole-page memmoves on mutation
- better page density
- cheaper insert and delete paths
- a foundation for cursor-local search that touches less data

### Phase 3. Rewrite Overflow Storage To Match The New Cell Model

Rebuild overflow handling so it works naturally with the new page format:

- cell-local payload split rules
- explicit local versus overflow payload accounting
- overflow pages carrying continuation pointers and payload chunks
- cursor-friendly overflow traversal and caching

This phase should remove the current "custom overflow page with custom semantics" design.

### Phase 4. Replace The Cache With Real Pinned Page Objects

Turn the current cache into something materially closer to SQLite's page cache:

- one cache entry per logical page
- pin count used in production, not only in tests
- dirty list ordering
- writeable and need-sync state
- better spill candidate selection
- no repeated owned full-page cloning on hot internal paths when a pinned reference can be used

The public boundary may still expose owned `Page` values if necessary, but the internal pager
should stop being built around them.

### Phase 5. Rewrite Rollback Journaling Against The New Page Model

Build rollback mode around the new page and cache design:

- first write triggers journaling
- original page image is journaled exactly once
- journal durability is guaranteed before database overwrite
- dirty pages are written through a pager-controlled ordering discipline
- savepoints use pager-owned touched-page tracking rather than broad cloning

This phase should also remove any remaining dependence on sidecar checksum persistence.

### Phase 6. Move Structural Metadata Into The Main Storage Protocol

Eliminate the `.pager_checksums` sidecar and any other metadata paths that sit outside the main
database plus journal/WAL durability model.

The desired end state is:

- free-page state lives in the main database structure
- page integrity information is either embedded in page/journal/WAL structures or omitted where it
  is not worth the write amplification
- recovery never needs an extra sidecar parse to know what the durable state is

### Phase 7. Rewrite WAL As Frame-Oriented Append Storage

Replace the current whole-visible-state WAL format with a frame-oriented design:

- append one frame per dirty page
- commit records mark durable visibility boundaries
- reader snapshots refer to a sequence or end mark, not to a reconstructed whole-state blob
- checkpoint copies back visible frames to the main database using pager-owned rules

This is likely the single largest remaining storage difference from SQLite after the page format
itself.

### Phase 8. Rebuild B-tree Cursor Behavior Against The New Format

Once the page format exists, optimize navigation the way SQLite does:

- binary search over cell pointers
- near-position reuse
- cheaper next and previous movement
- cheaper rightmost append path
- less eager payload decoding

This phase is still lower storage and B-tree work, not SQL planning work.

### Phase 9. Add A Real Migration Story Or Explicitly Drop Old Files

Once the new format works, choose one of two honest paths:

- provide a one-time offline migrator from the old format to the new format
- declare old-format files unsupported and require fresh database creation

We should not accidentally drift into a half-compatible state.

Current decision:

- ship explicit old-format retirement first
- reject retired on-disk generations at open time with a clear error
- defer any offline migrator to a later, explicit project if demand justifies it

## What We Can Reuse From The Current Refactor

The current storage rewrite was not wasted. It gives us useful scaffolding:

- the modular pager split under `src/storage/pager/`
- explicit pager states
- clearer lock lifecycle helpers
- better fault tests
- better rollback and WAL behavioral tests
- a cleaner place to land new code without rebuilding the whole crate structure first

But we should be clear about the limit of that reuse:

- keep the scaffolding
- replace the format
- replace the hot paths that were built around the old format

## New Testing Plan

The test plan now needs to cover format-level behavior, not only pager-state behavior.

### Format Tests

- main-file header roundtrip
- B-tree page encode and decode roundtrip
- freeblock and fragment accounting validation
- defragmentation preserves logical contents
- overflow split and reassembly correctness
- rollback journal encode and decode roundtrip
- WAL frame encode and decode roundtrip

### Behavior Tests

- append-heavy insert workload without pathological page rewrite behavior
- point lookup and range-scan correctness on slotted pages
- delete and rebalance behavior on fragmented pages
- overflow payload read and delete correctness
- rollback recovery from partially written journal states
- WAL recovery and checkpoint correctness with the new frame format

### Reliability Tests

- corruption corpus tests for page headers, cell pointers, and overflow links
- journal truncation and torn-tail handling
- WAL truncation and torn-tail handling
- savepoint rollback after multiple page mutations
- repeated reopen and integrity validation after random workloads

### Performance Validation

Every major phase should be checked against:

- point reads
- append-heavy inserts
- mixed read/write
- overflow-heavy payload reads
- explicit transaction batches

The expectation is no longer "small gains are acceptable". The purpose of this overhaul is to
unlock a materially different storage profile.

## Acceptance Criteria

This campaign should not be considered successful unless all of the following become true:

- Hematite uses a new on-disk storage format materially closer to SQLite's storage model
- the old sidecar-driven storage metadata path is gone from the hot path
- rollback and WAL both operate on the new page model
- B-tree pages are slotted and support local free-space reuse
- benchmark results show large, obvious gains rather than marginal noise-level improvements

## Current Status

The previous storage rewrite should now be understood as completed groundwork, not as the final
performance overhaul.

What is done:

- pager decomposition
- state-machine enforcement
- lock-layer cleanup
- rollback/WAL/savepoint/recovery modularization
- meaningful fault and concurrency coverage

What is not done:

- a new file format
- a SQLite-like B-tree page format
- frame-oriented WAL
- removal of sidecar checksum persistence
- a cache built around pinned production page objects

That is the real gap we need to close next.
