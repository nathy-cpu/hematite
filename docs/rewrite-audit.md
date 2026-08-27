# Audit: the rewrite plan's problem catalogue against the code

Every problem the original exploration catalogued, checked against the code as it
stands rather than against notes. Written because notes proved unreliable twice in one
session: a savepoint branch was reported fixed when only its sibling had been, and
`auto_vacuum` was about to be attacked as expensive when it costs nothing measurable.

Status is one of **fixed** (with the commit), **live** (still present, with what it
costs where that was measured), **declined** (present, measured or reasoned as not
worth it), or **deliberate** (present by design, with the reason).

## Fixed

| problem | evidence |
|---|---|
| Rollback savepoints deep-copied every dirty page, per row — quadratic across a transaction | `a511bf9`; `delete_heavy/rollback` −30 %, scaling with rows per transaction |
| `PagerSnapshot::Full` cloned the whole `PageCache` including the buffer pool | `ad092cf` |
| `PagerSnapshot` moved 424 bytes per snapshot, including the tiny variant | `ad092cf`, boxed |
| Four in-place B-tree paths cloned the 4 KiB page they had just edited | `208f4c2`; appends −4 to −7 % |
| `can_merge_with` cloned and decoded both nodes, and was called twice per merge | `208f4c2` |
| `try_update_leaf_in_place` scanned every key instead of binary searching | `482598a` |
| `is_child_underflow` fully decoded a child to read a byte count | `482598a`; part of `delete_heavy` −12 % |
| `tree_store()` cloned an `Arc` at ~40 call sites | `482598a` |
| Tree space stats were quadratic in overflow pages | `482598a` |
| `read_rows_with_ids` decoded the table then cloned every row again | `1bfe279`; scans −12 % |
| Row-id collection decoded whole records to read the key | `1bfe279` |
| FK child scan materialised the child table once per parent row | `1bfe279` |
| `validate_table_indexes` reopened the index tree once per row | `1bfe279` |
| `LIMIT` decoded the whole table | `cba254a`; **25×** |
| 4 fsyncs per rollback commit | `2c579fa`, `59e572c`; now 2, the floor |
| WAL never checkpointed — unbounded growth | `8acb95b`; 2057 frames → bounded |
| WAL salts were constants, so the stale-frame guard could never fire | `8acb95b` |
| Overflow chains were walked twice per read | `6f5dd4d` |
| Spilled values held 489 bytes in the leaf, ~8 large rows per page | `6f5dd4d`; large-row scans −33 % |
| Four checksum implementations, one of them `DefaultHasher` | `5898fc2`; **12.5×** |
| Fixed-width record encoding | `320c420`; −11.5 % bytes/row |
| Text index keys ordered by length-low-byte | `320c420` |
| `i32` sorted below `i64` in mixed columns | `320c420` |
| Decimal keys ordered by scale before magnitude | `320c420` |
| `-0.0` and `0.0` produced different keys | `320c420` |
| Index-key integers cost a fixed 8 bytes | `08241eb` |
| Page cache keyed by SipHash | `b1bef12`; ~3 % on a cache hit |
| Page writes used `seek` + `write_all` | `320c420` |
| Sync ordering was untestable | `f2a53ba`, `3305589` |
| Projected column names were re-resolved per row — a UTF-8 char search plus a SipHash lookup, per column | `8046860`; 18 % of a scan's instructions |
| Scans copied every key out of the page and allocated a buffer for every value, spilled or not | `b4b3a85`; 4.07 → 2.07 allocations per row |
| `write_page` hashed a whole 4 KiB page on every call, so a bulk insert rehashed one leaf per row | `d51fa93`; −5.7 % of the insert path |
| `from_shared_page` validated every cell range on every page it loaded, including pages a cursor merely navigated past | `27a2b21`; −21.4 % insert, −6.0 % scan. Recorded here as *deliberate* until the write path was profiled: it was 4–6 % of a scan but ~28–30 % of a batched insert. Cells are now checked where they are read, and the whole-page structural checks are `BTreeNode::verify`, run by `validate_tree` and `open_tree`. The narrowing: duplicate or overlapping cell pointers in a non-root page are no longer caught on the read path |

## Declined — present, not worth it

| problem | measured or reasoned |
|---|---|
| `auto_vacuum` runs on every commit | Disabling it made `delete_heavy` **no faster** (54.1 ms against 50.6–52.6). It exits early when little is free. I was about to spend a session on this. |
| `read_reserved_blob` copies 4 KiB per call | ~300 ns against a commit costing tens of µs — under 0.1 %, below the noise floor everything here is measured at |
| `save_table_metadata` reads and writes 4 KiB every commit; `load_table_metadata` reads 4 KiB every `BEGIN` | Same order. Removing the copies needs partial page writes, which is a larger change than the payoff |
| `compact_transaction_free_pages` sorts and dedups on every WAL deallocation; `mark_page_active` scans linearly | Cold path. A naive `swap_remove` would silently under-compact trailing pages, so it needs restructuring rather than an edit |
| Cursors re-descend from a parent at every leaf boundary; leaves have no sibling pointers | A re-descent costs about two cache hits per ~50-row leaf — under 1 % of a scan. Arithmetic rather than a benchmark, but the margin is wide enough. Not worth a format change on scan grounds |

## Live — still present, unmeasured or unaddressed

Ordered by how much is known about them.

| problem | note |
|---|---|
| `SelectAccessPath::FullTableScan` materialises; no projection pushdown | The two allocations a scanned row still costs are its `Vec<Value>` and the projected `Vec<Value>`, both inherent to representing a row as `Vec<Value>`. Removing them means `RecordRef` reaching the executor, which is blocked on splitting `ExecutionContext` so the scan and the `WHERE` predicate can borrow disjoint fields. Note that the *resolution* half of this — turning column names into indices — turned out to be the larger cost and is now fixed |
| `StoredValueLayout` still costs 11 bytes per row | Part of the remaining bytes/row gap: 87.7 against a ~25 target |
| B-tree cells still use `u16` lengths and no varints | Same |
| `defragment_page` allocates a `Vec` per cell, up to 99 per call | |
| `take_page_for_write` unconditionally copies 4 KiB | The owned-write path; `take_page_for_write_owned` avoids it and now has four callers |
| `read_page` copies 4 KiB even on a cache hit | Used by compaction |
| `dirty_page_ids` allocates a `Vec` and does a hash lookup per link | Called from seven places |
| `sync_rollback_journal` takes three cache locks per dirty page | |
| WAL reads bypass the WAL page cache on the main path (`read_page_data_readonly`), reopening the file per access | Part of WAL v4 |
| `VisibleWalState` is deep-cloned by `Arc::make_mut` whenever a reader shares it | Same |
| WAL frames are written with two `write_all` calls each; each commit opens the WAL twice and validates its header twice | Same |
| "View tokens" invalidate every clean cached page after a WAL commit | Same |
| `refresh_persisted_view` does a 4 KiB read and a full decode on every `begin_read` | The fstat is gone, and so is the cache reset it was triggering. The read remains: it is how a second process's commit is noticed |
| `begin_transaction` clones the checksum map and free list twice | Per transaction; small at this scale, grows with the database |
| `initialize_journal_file` writes the whole free list and checksum map at every `BEGIN` | Same |
| `validate_integrity` reads every checksummed page individually | |
| The buffer pool is a `HashMap` with an intrusive LRU; no frame arena, no `Arc<Pager>` | Measured at 106 ns per hit and not a bottleneck in any current workload |

## What the audit changed

Two entries moved because measurement contradicted the plan, and one because it
contradicted an earlier claim of mine:

- `auto_vacuum` went from "attack next" to declined.
- The eager cell validation turned out to be a deliberate integrity cost whose real
  fix is elsewhere, not a performance bug.
- The rollback savepoint copy was found live after being reported fixed, and is now
  the largest single win of the session.

Profiling the scan afterwards then found the largest read cost in the catalogue's
blind spot. This is a storage-layer plan, but 18 % of a `SELECT id, a` scan was the
executor resolving column names to indices, once per column per row, recomputing an
answer identical for every row. Fixing that and the two transient allocations per row
took the scan from 3 790 to 2 671 instructions per row, **−29.5 %**, confirmed at
−27 % wall clock. Storage is now the minority of a scan's cost — see
`docs/bench-baseline.md`.

## The syscall and write-path sweep

A later pass measured the axis this catalogue never had: syscalls, via `strace -c` on
`examples/storage_profile.rs`, which was written for it. Four of the entries above
were wrong about *why* they were expensive, and the largest costs were not in the
catalogue at all.

Fixed in that pass:

| what | measured |
|---|---|
| Every pager snapshot asked the OS for the file's length; `run_atomically` takes one per row | `statx` 12 031 → 413 per 200 transactions |
| The rollback journal was the last writer driving a shared cursor | `lseek` 1 859 → 0; the record count costs one `pwrite` instead of three syscalls |
| The page cache was emptied at the start of every rollback transaction — the metadata-page fingerprint it compared is shared with the catalog, so our own commits invalidated it | `pool_misses` 1 400 → 0, `pread64` 1 844 → 415 |
| `snapshot_original_page` made four 4 KiB copies where it owed one | `undo_page_copies` per 5 000 updates 49 455 → 51 |
| `run_atomically` deep cloned the catalog's table-metadata map three times per row, plus a fourth it dropped unread | insert allocations −39 % overall |
| Deleting decoded whole leaves to test borrow and merge eligibility, to rotate one cell, and to descend | delete allocations 561 → 68 per row; instructions −74 % |

Two entries above were fixed by earlier phases and have been struck from *Live*:
`write_journal_record_count`'s three syscalls, and the checksum metadata that used to
be silently dropped past ~2 000 pages — per-page checksums live in page trailers now.

Still live, and now measured rather than suspected:

| what | cost |
|---|---|
| The journal is created and unlinked per transaction | `openat` + `unlink` + `ftruncate` per commit — about a third of syscall *time* in rollback mode. Needs the journal invalidated by a zeroed magic rather than by absence, which is a recovery-semantics change |
| The WAL file stamp and read path still stat and seek | 1 235 `statx` + 613 `lseek` per 200 WAL transactions |
| `Table::clone` per point lookup | 18.7 % of that workload's allocations, and in `src/query/` rather than here |

### The lesson repeated

The leaf-borrow rotation, rewritten in that pass, had **no test coverage at all** — a
probe compiled into it fired zero times across 745 tests and 1 672 times in one
5 000-row `DELETE`. That is the fourth vacuous or absent test this rewrite has found,
after the recycled-buffer pair, the page-trailer reservation and the corrupt-cell
accessor. The remedy is the same each time: assert that the work happened, using a
counter, not only that the result looks right.
