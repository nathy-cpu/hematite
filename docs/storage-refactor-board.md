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
| `M8` Test Harness Strengthening | Improve regression coverage for pager states, WAL, rollback, and concurrency | `Done` | Pager fault tests, threaded rollback/WAL tests, rollback-journal characterization tests, committed-journal reopen tests, and truncated-journal rejection tests are in place | Extend only when a new behavior rewrite needs more coverage |
| `M9` Page Cache Redesign | Replace the current cache shape with a more SQLite-like pager-owned page cache | `Done` | The cache now stores page-local metadata inside cache entries, supports non-recency `peek`, and tracks deterministic dirty ordering for pager internals | Revisit only if rollback or WAL work exposes a real cache limitation |
| `M10` Rollback Core Rewrite | Replace snapshot-shaped rollback behavior with explicit page-journal behavior | `Done` | Rollback and WAL transaction state are split; rollback journals are initialized at begin, page images are appended incrementally, commit marks the on-disk journal committed, rollback restores from the journal, and snapshot restore rewrites the active journal to stay compatible with savepoints | Keep future savepoint and WAL work building on this journal-first rollback core |
| `M11` Savepoint/Subjournal Rewrite | Rebuild savepoint internals on top of the new rollback core | `Done` | Rollback transactions now own internal pager savepoints; rollback-mode snapshots create savepoint handles instead of cloning the full pager; writes feed per-savepoint page capture; savepoint restore rebuilds cache/journal state from pager-owned savepoint data | Keep future work focused on locking and WAL rather than re-expanding snapshot cloning |
| `M12` Locking Model Rewrite | Replace the current in-process registry as the true correctness backbone | `Done` | File-backed rollback and WAL lock sidecars now coordinate readers, writers, and WAL reader snapshots through OS locks; external lock-file tests and threaded rollback/WAL tests pass in the full suite | Move on to rebuilding WAL semantics on top of the new lock backbone |
| `M13` WAL Rewrite | Rebuild WAL on top of the new pager core instead of layering on current behavior | `Done` | WAL commits now advance pager-owned visible state directly, new writers seed from the latest WAL-visible file/free-page/checksum state, and WAL-visible reads now respect freed/truncated pages instead of falling back to stale main-file bytes; storage + threaded WAL tests pass in the full suite | Move on to the failure-path matrix that hardens the new WAL core |
| `M14` Fault Injection Matrix | Add SQLite-style failure-path testing for journal, commit, checkpoint, and recovery edges | `Done` | Pager fault coverage now includes injected checkpoint copyback failure with reopen validation, truncated WAL-tail reopen behavior, rollback flush failure, and rollback recovery-after-error paths; full storage + SQL suites remain green | Keep extending only when a new storage behavior adds a new failure surface |
| `M15` Cleanup And Optional Format Migration | Remove obsolete compatibility machinery after the new core is complete | `Done` | The remaining rewrite-era cleanup has been applied: stale rewrite status has been removed from the docs, pager/WAL helper paths were consolidated, and storage terminology now reflects the current page-cache design; optional file-format migration is intentionally deferred rather than mixed into cleanup | Treat future file-format changes as a separate explicit project, not part of the completed rewrite |

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
- `M8` Test Harness Strengthening
- `M9` Page Cache Redesign
- `M10` Rollback Core Rewrite
- `M11` Savepoint/Subjournal Rewrite
- `M12` Locking Model Rewrite
- `M13` WAL Rewrite
- `M14` Fault Injection Matrix
- `M15` Cleanup And Optional Format Migration

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

More specifically:

- `M0` to `M3` are both structurally and behaviorally convincing
- `M4` to `M7` are structurally complete and well validated
- the remaining real behavior risk is now mostly future format/perf work rather than missing pager failure coverage

## What Still Carries The Real Rewrite Risk

There are no remaining rewrite-critical milestones in the storage rewrite board.

## Recommended Execution Order From Here

1. Revisit cache or pager internals only if new correctness or performance work exposes a concrete need.
2. Treat any file-format migration as a new project with its own compatibility and rollout plan.

## Immediate Next Actions

- Keep the checkpoint-failure and truncated-WAL-tail regressions green as future storage work lands.
- Use performance work, not architectural uncertainty, to decide the next storage changes.
- If a file-format migration becomes desirable later, plan it explicitly instead of hiding it inside cleanup.
