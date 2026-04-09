# Storage Overhaul Milestone Board

This board replaces the earlier "storage rewrite" milestone view.

The earlier pager refactor is now treated as groundwork only. It improved structure and
correctness, but it did not deliver the intended file-format overhaul or the level of performance
change we were aiming for.

Use this board alongside [storage-refactor-plan.md](./storage-refactor-plan.md):

- the plan explains the revised architectural target
- this board shows the overhaul milestones, their purpose, and the next concrete move

## Status Legend

- `Done`: implemented and validated on the current codebase
- `In Progress`: meaningful groundwork is in place, but the milestone is not complete
- `Not Started`: still ahead of us
- `Blocked`: intentionally paused behind another milestone

## Groundwork Already Completed

These milestones are still valuable, but they should no longer be mistaken for the final storage
overhaul:

| Legacy Milestone | What It Gave Us | Current Interpretation |
|---|---|---|
| `M0` to `M3` | pager API stability, decomposition, state enforcement, lock cleanup | useful scaffolding |
| `M4` to `M7` | savepoint/recovery/integrity/page-IO separation | useful scaffolding |
| `M8` to `M14` | stronger tests, cache metadata groundwork, rollback/WAL correctness work, fault coverage | useful scaffolding |
| `M15` | cleanup and consolidation | completed cleanup of the old plan, but not the format overhaul we actually need |

## New Overhaul Board

| Milestone | Goal | Status | Evidence | Next Step |
|---|---|---|---|---|
| `F0` Overhaul Reset | Explicitly reset the storage effort around a format rewrite instead of behavior-only cleanup | `Done` | The storage plan and board now state that the previous refactor was insufficient because it preserved the old format | Start treating file-format redesign as first-class work |
| `F1` New Format Specification | Define the new main-file, B-tree page, overflow, rollback journal, and WAL formats before more code churn | `Done` | `docs/storage-format-spec.md`, `src/storage/format.rs`, `src/storage/journal_v3.rs`, and `src/storage/wal_v3.rs` now define and test the v3 layout primitives | Keep the runtime cutover aligned with the spec |
| `F2` Main File Layout Rewrite | Replace the current file prelude and custom reserved-page assumptions with the new database-file layout | `Done` | The live `FileManager`, WAL visibility math, rollback/WAL recovery, and catalog open-create flow now use a page-addressed main file with reserved pages `0` and `1`, and the old 64-byte prelude assumptions are removed from the active path | Carry the new layout assumptions into the remaining durable metadata cleanup work |
| `F3` Slotted B-tree Pages | Replace contiguous serialized node pages with SQLite-like slotted pages, cell pointers, freeblocks, and fragments | `Done` | `src/btree/node.rs` now serializes and reads live tree pages through the slotted-page model and the library suite passes on that path | Build cursor/read-path optimizations on top of the new page format |
| `F4` Overflow Rewrite | Rebuild overflow storage around the new cell model and local-payload split rules | `Done` | `src/storage/overflow.rs` now uses the v3 overflow page format, and large-value tree/storage tests pass on reopen, delete, and corruption cases | Remove any remaining assumptions that expect the old overflow bytes |
| `F5` Real Page Cache | Turn the cache into a production pinned-page cache rather than an owned-page map with metadata | `Done` | The live cache now stores shared page images internally, eviction respects active shared page handles as real pins, and WAL snapshot/visibility changes explicitly invalidate cached pages so the stronger residency model stays correct under concurrency | Build the remaining read-side work on top of the now-stable pinned cache |
| `F6` Rollback Journal Rewrite | Rebuild rollback journaling against the new page format and page-state model | `Done` | The live rollback path now persists and recovers through the `v3` rollback journal codec with the new on-disk header/record layout, and rollback crash/snapshot tests pass on that runtime path | Use the now-live `v3` rollback path as the baseline while removing remaining sidecar metadata in `F7` |
| `F7` Sidecar Metadata Removal | Remove `.pager_checksums` and other sidecar-driven durable metadata from the hot path | `Done` | Pager checksum/freelist metadata now lives in reserved page `1` through a shared metadata-page codec, legacy sidecars are migrated on open, test temp cleanup no longer depends on `.pager_checksums`, and WAL commits no longer rewrite main-file metadata on every append | Carry the main-file metadata split forward into the WAL frame rewrite so frame/checkpoint metadata stays single-sourced |
| `F8` WAL Frame Rewrite | Replace the current visible-state WAL with a frame-oriented WAL closer to SQLite's approach | `Done` | The live WAL runtime now appends `v3` page frames with explicit commit sequences and metadata-page commit markers, reopen/checkpoint visibility is reconstructed from committed frame groups instead of full visible-state records, and WAL fault/threaded tests pass on the frame path | Build the remaining cursor/read-path and migration decisions on top of the now-live frame log |
| `F9` Cursor And Read-Path Rewrite | Rebuild B-tree navigation around the new slotted-page format | `Done` | Hot B-tree point-lookups and cursor descent use the pager's shared-page path, tree open/validation/page-id/stats walkers now inspect slotted pages through shared-page lazy nodes, overflow-chain validation/collection no longer clones pages on read-only integrity passes, and the full test suite passes on the completed read-path cutover | Move to the format migration decision and then re-benchmark the new storage shape |
| `F10` Format Migration Decision | Choose and implement either offline migration or explicit old-format retirement | `Done` | Hematite now explicitly retires older on-disk generations: catalog open rejects non-current header generations with a migration/retirement error, the docs/spec/plan all record that no offline migrator is shipped yet, and regression tests cover retired-version and unknown-magic database files | Move to the performance validation campaign on the now-settled storage generation |
| `F11` Performance Validation Campaign | Re-benchmark only after the new format and lower storage model are real | `Not Started` | Current benchmarks still measure the old-format storage shape despite pager cleanup | Re-run point-read, append-write, mixed, and overflow-heavy workloads |

## What Is Actually Finished Right Now

Finished for the new overhaul:

- `F0` Overhaul Reset
- `F1` New Format Specification
- `F2` Main File Layout Rewrite
- `F3` Slotted B-tree Pages
- `F4` Overflow Rewrite
- `F5` Real Page Cache
- `F6` Rollback Journal Rewrite
- `F7` Sidecar Metadata Removal
- `F8` WAL Frame Rewrite
- `F9` Cursor And Read-Path Rewrite
- `F10` Format Migration Decision

Finished as groundwork from the prior campaign:

- old pager decomposition and state management
- old lock/refactor cleanup
- old rollback/WAL/savepoint/fault scaffolding

Not finished:

- `F11` Performance Validation Campaign

## Important Interpretation

The previous board made it too easy to think "storage rewrite complete" when what we had really
completed was:

- a safer pager structure
- a more testable pager structure
- a still-custom storage format

That is the core correction this new board is making.

## Recommended Execution Order From Here

1. Finish `F11` with same-spec benchmarks against the now-live storage generation.
2. Use flamegraphs from the new format/runtime before choosing any further optimization work.
3. Keep higher-layer query/planner work out of scope until the new storage baseline is measured honestly.
4. If benchmarks still disappoint, open a fresh post-`F11` board from measured hotspots rather than assumptions.

## Immediate Next Actions

- rerun canonical point-read, append-write, mixed, and overflow-heavy benchmarks on the current runtime
- capture fresh `perf` profiles and compare them against the pre-overhaul baseline
- record whether the new storage generation delivered meaningful wins before scheduling another refactor wave
