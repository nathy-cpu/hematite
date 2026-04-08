# Storage Rewrite Milestone Board

This board is the short operational view of the storage/pager rewrite.

Use it alongside [storage-refactor-plan.md](./storage-refactor-plan.md):

- the plan explains the architecture and intent
- this board shows status, evidence, and the next concrete move

## Status Legend

- `Done`: implemented and validated on the current codebase
- `In Progress`: meaningful groundwork is in place, but the milestone is not behaviorally complete
- `Not Started`: still ahead of us
- `Blocked`: intentionally paused behind another milestone

## Milestone Board

| Milestone | Goal | Status | Evidence | Next Step |
|---|---|---|---|---|
| `M0` Contract Freeze | Keep the upper pager API stable while refactoring internals | `Done` | `Pager` public API is still intact and upper layers still compile against it | Keep treating API changes as out of scope unless explicitly planned |
| `M1` Pager Decomposition | Break `pager.rs` into focused internal modules | `Done` | `src/storage/pager/` now contains `core`, `locking`, `reader`, `journal`, `wal`, `recovery`, `savepoint`, `page_io`, `space`, `integrity`, `state`, `cache`, `test_support` | Maintain module boundaries as behavior rewrites continue |
| `M2` State Machine Enforcement | Make pager states real and validated | `Done` | `transition_state()` and compatibility checks exist in [state.rs](./../src/storage/pager/state.rs) | Expand state-focused tests as rollback behavior is rewritten |
| `M3` Lock Coordination Cleanup | Move from ad hoc acquire/release usage toward intent-level scope handling | `Done` | Reader/writer scope helpers and shared writer teardown are in place; new pager fault regressions cover upgrade rejection and reader-in-writer behavior | Use these helpers as the only path for future lock lifecycle changes |
| `M4` Savepoint/Snapshot Separation | Isolate compatibility logic for snapshots and savepoints | `Done` | Snapshot logic lives in `savepoint.rs` and is no longer mixed into the pager facade | Revisit once rollback internals are rewritten |
| `M5` Recovery/WAL/Rollback Separation | Separate rollback, WAL, and recovery internals into distinct modules | `Done` | `journal.rs`, `wal.rs`, and `recovery.rs` now own those concerns structurally | Start changing behavior inside those modules rather than re-extracting more code |
| `M6` Integrity And Checksum Separation | Isolate integrity verification and checksum helpers | `Done` | `integrity.rs` owns validation and checksum calculation | Add more corruption-path tests when rollback journaling is rewritten |
| `M7` Page IO And Space Separation | Isolate page IO, flush, allocation, and free-page helpers | `Done` | `page_io.rs` and `space.rs` now own those pager responsibilities | Keep them stable while rewriting rollback behavior behind them |
| `M8` Test Harness Strengthening | Improve regression coverage for pager states, WAL, rollback, and concurrency | `In Progress` | Pager fault tests and threaded connection tests are stronger than before | Add more failure-path and journaling-order tests |
| `M9` Page Cache Redesign | Replace the current cache shape with a more SQLite-like pager-owned page cache | `In Progress` | Cache is pager-owned and modularized, but it is not yet a full pinned-page-header design | Decide the smallest first cache-behavior change worth landing after rollback work begins |
| `M10` Rollback Core Rewrite | Replace snapshot-shaped rollback behavior with explicit page-journal behavior | `Not Started` | Rollback logic is extracted, but behavior is still mostly the old model | Start by documenting current rollback begin/commit/rollback sequencing in tests |
| `M11` Savepoint/Subjournal Rewrite | Rebuild savepoint internals on top of the new rollback core | `Not Started` | Current snapshot compatibility is still compatibility-shaped | Wait until the rollback core exists |
| `M12` Locking Model Rewrite | Replace the current in-process registry as the true correctness backbone | `Not Started` | Current lock handling is cleaner, but still fundamentally registry-based | Defer until rollback behavior is clearer |
| `M13` WAL Rewrite | Rebuild WAL on top of the new pager core instead of layering on current behavior | `Not Started` | WAL is separated structurally, not behaviorally rewritten | Keep WAL changes behind rollback progress |
| `M14` Fault Injection Matrix | Add SQLite-style failure-path testing for journal, commit, checkpoint, and recovery edges | `Not Started` | We have pager fault tests, but not the broader failure matrix yet | Introduce one rollback journal failure test at a time once rollback rewrite begins |
| `M15` Cleanup And Optional Format Migration | Remove obsolete compatibility machinery after the new core is complete | `Blocked` | Too early; depends on rollback and WAL rewrites landing first | Revisit only after `M10` through `M14` materially advance |

## What Is Actually Finished Right Now

These are the milestones we can honestly treat as completed:

- `M0` Contract Freeze
- `M1` Pager Decomposition
- `M2` State Machine Enforcement
- `M3` Lock Coordination Cleanup
- `M4` Savepoint/Snapshot Separation
- `M5` Recovery/WAL/Rollback Separation
- `M6` Integrity And Checksum Separation
- `M7` Page IO And Space Separation

## Validation Checkpoint

This checkpoint was taken after the lock/state coordination refactors and before starting the next major rewrite steps.

### Result

`M0` through `M7` remain valid and do not need to be downgraded.

That means we can move forward to the next milestones without reopening the completed scaffold work first.

### Evidence Used

Structural evidence:

- the pager facade still exists in `src/storage/pager.rs`
- the internal module split is present under `src/storage/pager/`
- state validation is present in `src/storage/pager/state.rs`
- lock coordination helpers are present in `src/storage/pager/locking.rs`
- rollback, WAL, recovery, savepoint, page IO, space, and integrity concerns are split into dedicated modules

Behavioral evidence:

- `cargo test pager_fault -- --nocapture --test-threads=1`
- `cargo test storage::tests::pager_tests:: -- --nocapture`
- `cargo test sql::tests::connection_tests::test_threaded_rollback_multi_connection_reads_and_writes -- --nocapture`

### Important Interpretation

This checkpoint confirms that the scaffolding milestones are complete enough to build on.

It does **not** mean that rollback mode or WAL have been behaviorally rewritten yet.

More specifically:

- `M0` to `M3` are both structurally and behaviorally convincing
- `M4` to `M7` are structurally complete and well validated
- the remaining real behavior risk still begins at rollback-core work and beyond

## What Is Partially Finished

These areas have meaningful groundwork, but not the final behavior we want:

- `M8` Test Harness Strengthening
- `M9` Page Cache Redesign

## What Still Carries The Real Rewrite Risk

These are the milestones that will actually determine whether the rewrite succeeds:

- `M10` Rollback Core Rewrite
- `M11` Savepoint/Subjournal Rewrite
- `M12` Locking Model Rewrite
- `M13` WAL Rewrite
- `M14` Fault Injection Matrix

## Recommended Execution Order From Here

1. Finish `M8` enough to characterize current rollback behavior with tighter tests.
2. Start `M10` with the smallest page-journal-shaped rollback slice possible.
3. Only revisit `M9` in a bigger way if rollback work shows the cache shape is now the limiting factor.
4. Keep `M13` blocked behind real rollback-core progress.
5. Treat `M12` and `M14` as parallel reliability tracks once `M10` has genuine momentum.

## Immediate Next Actions

- Add rollback characterization tests for:
  - first dirty page journaling behavior
  - rollback restoration after multiple page writes
  - commit ordering around journal persistence and page flush
- Identify the first current rollback path that still depends on broad transaction snapshots.
- Introduce the first rollback-core helper that records original page images explicitly and keeps the current public pager behavior unchanged.
