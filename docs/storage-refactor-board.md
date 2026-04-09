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
| `F1` New Format Specification | Define the new main-file, B-tree page, overflow, rollback journal, and WAL formats before more code churn | `Not Started` | No committed format spec yet for the new storage generation | Write the format contract in detail |
| `F2` Main File Layout Rewrite | Replace the current file prelude and custom reserved-page assumptions with the new database-file layout | `Not Started` | Current storage still uses the old 64-byte prelude plus page 0/page 1 scheme | Land the new main-file layout and header rules |
| `F3` Slotted B-tree Pages | Replace contiguous serialized node pages with SQLite-like slotted pages, cell pointers, freeblocks, and fragments | `Not Started` | Current `BTreeNode` format is still the old custom contiguous layout | Implement new page encode/decode and page-local mutation paths |
| `F4` Overflow Rewrite | Rebuild overflow storage around the new cell model and local-payload split rules | `Not Started` | Current overflow pages still follow the old custom format | Define payload split rules and new overflow pages |
| `F5` Real Page Cache | Turn the cache into a production pinned-page cache rather than an owned-page map with metadata | `Not Started` | Current cache metadata exists, but the hot path still revolves around owned `Page` values | Introduce pinned internal page objects and proper pin/unpin use |
| `F6` Rollback Journal Rewrite | Rebuild rollback journaling against the new page format and page-state model | `Not Started` | Current rollback journal still reflects the old storage format and metadata model | Journal the new page images and new structural metadata correctly |
| `F7` Sidecar Metadata Removal | Remove `.pager_checksums` and other sidecar-driven durable metadata from the hot path | `Not Started` | Current storage still persists checksum/freelist state through sidecars | Move durable metadata responsibility into the main file and journal/WAL protocols |
| `F8` WAL Frame Rewrite | Replace the current visible-state WAL with a frame-oriented WAL closer to SQLite's approach | `Not Started` | Current WAL still stores full visible-state transitions | Design and implement frame append, commit boundary, and checkpoint flow |
| `F9` Cursor And Read-Path Rewrite | Rebuild B-tree navigation around the new slotted-page format | `Not Started` | Current cursor/search behavior still reflects the old page model | Add pointer-array binary search, near-position reuse, and cheaper traversal |
| `F10` Format Migration Decision | Choose and implement either offline migration or explicit old-format retirement | `Not Started` | No final migration story exists yet | Decide whether to ship a migrator or require fresh databases |
| `F11` Performance Validation Campaign | Re-benchmark only after the new format and lower storage model are real | `Not Started` | Current benchmarks still measure the old-format storage shape despite pager cleanup | Re-run point-read, append-write, mixed, and overflow-heavy workloads |

## What Is Actually Finished Right Now

Finished for the new overhaul:

- `F0` Overhaul Reset

Finished as groundwork from the prior campaign:

- old pager decomposition and state management
- old lock/refactor cleanup
- old rollback/WAL/savepoint/fault scaffolding

Not finished:

- everything format-defining or format-dependent

## Important Interpretation

The previous board made it too easy to think "storage rewrite complete" when what we had really
completed was:

- a safer pager structure
- a more testable pager structure
- a still-custom storage format

That is the core correction this new board is making.

## Recommended Execution Order From Here

1. Write `F1` first, so implementation work is guided by a real format contract.
2. Do not spend effort optimizing the old page format any further unless required for a bug fix.
3. Treat `F3`, `F4`, `F6`, and `F8` as the actual performance-defining milestones.
4. Revisit higher-layer optimization only after the new storage shape exists and is benchmarked.

## Immediate Next Actions

- write the new storage format specification
- decide how closely Hematite should follow SQLite's page and WAL layouts versus only copying the principles
- map the current `BTreeNode`, overflow, journal, and WAL formats to their replacement designs
- add format-level tests before the first real implementation slice lands
