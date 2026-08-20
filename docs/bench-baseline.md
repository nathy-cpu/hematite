# Storage rewrite — baseline measurements

Numbers for the storage stack **as it was before the format v4 rewrite began**. Every
later phase is measured against this file.

> **Absolute times in this file are not comparable across sessions.** The same
> unchanged code measured 1.55 ms for `full_scan` when this baseline was captured and
> 3.97 ms a few hours later — a 2.6× swing from machine state alone. Only compare
> numbers taken back to back in one session, with the two versions built and run
> alternately. Byte counts are stable and can be compared freely.
>
> Results per phase are appended at the end of this file.

Reproduce with:

```
cargo bench --bench storage_micro
HEMATITE_BENCH_ROWS=5000 cargo bench --bench storage_workloads
```

Captured on Linux 6.12, rustc 1.97.1, `--release` with `lto = "thin"`,
`codegen-units = 1`. Absolute times are machine-specific; the ratios and the byte
counts are the parts that matter.

Workload fixture size was 5 000 rows (the default is 20 000). The disk-space figures
are essentially size-independent; the timings scale with it.

---

## Disk space — the headline metric

Bytes in the main database file, divided by row count, after a checkpoint.

| fixture | bytes on disk | **bytes/row** |
|---|---|---|
| `(id INT PK, a INT, b INT, c INT, d INT)` — five small integers | 495 616 | **99.1** |
| `(id INT PK, name TEXT, email TEXT, active BOOLEAN)` | 618 496 | **123.7** |

For context, the five-integer row carries about 20 bytes of information. The record
encoding spends 5 bytes on every small integer (1 tag + 4 fixed-width payload), the
`StoredValueLayout` wrapper adds 11 bytes per row, the B-tree cell header adds 4 more
plus a 2-byte pointer, and the rest is page slack and index pages.

Journal mode makes no difference to the final file size, as expected.

### Space reclamation after deletion

| | before | after deleting ~4/7 of rows |
|---|---|---|
| main file bytes | 495 616 | 491 520 |

**0.8 % of the file is returned** after deleting more than half the rows. Free pages
are tracked, but the only truncation path is the trailing-page compactor, and freed
interior pages are almost never at the tail.

---

## Micro-benchmarks — pager primitives

| benchmark | time | note |
|---|---|---|
| `page_read_hit` | 110 ns | a cache hit. Takes the cache lock up to 8 times per call. |
| `page_read_miss` | 4.61 µs | fault from the file, checksum verify, insert. |
| `page_write_dirty` | 3.93 µs | copy-on-write into a pooled buffer, checksum, cache insert. |
| `page_alloc_free` | 3.02 µs | allocate then immediately free one page. |
| `single_page_txn` / 64 pages | 50.0 µs | one dirty page, begin → commit. |
| `single_page_txn` / 1 024 pages | 47.3 µs | |
| `single_page_txn` / 8 192 pages | 46.3 µs | |
| `bulk_txn_1000_pages` | 13.8 ms | 13.8 µs per dirty page. |

`single_page_txn` doing the same work at every database size looks like good news and
is not. `BEGIN` serialises the whole free list and checksum map into the journal, and
`COMMIT` re-sorts and re-serialises both, so this *should* grow — it does not, because
both structures have to fit in a single 4 096-byte metadata page, and past roughly
2 000 pages the checksum map is **silently discarded in its entirety**. The flat line
is the integrity feature switching itself off.

### Checksum

| algorithm | time per 4 KiB page | throughput |
|---|---|---|
| FNV-1a, byte at a time (original) | 3.75 µs | 1.02 GiB/s |
| XXH64 (`storage::checksum`) | 300 ns | 12.7 GiB/s |

**12.5× faster.** A 4 KiB page write in rollback mode pays this at least twice — once
for the page checksum, once for the journal record — so it was a direct tax on every
write. This is the only change already landed at the time of writing (phase S1).

---

## Workloads

All figures for 5 000 rows unless stated. `batchN` = rows per `INSERT` statement, and
each statement is its own implicit transaction.

### Append

500 sequential inserts.

| | rollback | WAL |
|---|---|---|
| `batch1` | 63.3 ms (127 µs/row) | 49.4 ms (99 µs/row) |
| `batch100` | 16.0 ms (32 µs/row) | 10.4 ms (21 µs/row) |

### Read paths — where the materialized scan shows up

| benchmark | rollback | WAL |
|---|---|---|
| `point_lookup` (by primary key) | 31.4 µs | 38.6 µs |
| `full_scan` (5 000 rows, 2 columns) | 1.56 ms | 1.63 ms |
| `range_1pct` (50 rows via `WHERE id >= .. AND id < ..`) | 2.77 ms | 2.88 ms |
| `limit_100` (`SELECT id FROM t LIMIT 100`) | 1.42 ms | 1.46 ms |
| `secondary_index` lookup (via `CREATE INDEX`) | 172 µs | 170 µs |

Three things to read off this table:

- **`LIMIT 100` costs 92 % of a full scan** while returning 2 % of the rows. Nothing
  is streamed: `open_table_cursor` decodes and buffers the entire table into a
  `Vec<StoredRow>`, `read_rows_with_ids` clones every row again, and the cursor sorts
  the result — all before the first row is returned.
- **A 1 % range scan costs *more* than a full scan** (2.77 ms vs 1.56 ms), because it
  pays the same materialization and then evaluates a predicate over every row.
- **A secondary-index lookup is 5.5× slower than a primary-key lookup**, despite being
  the access path that exists to be faster.

Full scan works out to 311 ns per row, against 110 ns for a single cached page read.

### Large rows

250 rows, each with a 2 KiB `TEXT` column that spills to an overflow chain.

| | rollback | WAL |
|---|---|---|
| scan *without* selecting the large column | 293 µs | 311 µs |
| scan *with* the large column | 320 µs | 332 µs |

Skipping the large column saves only 9 %. With `MIN_LOCAL_PAYLOAD = 489`, a leaf page
holds roughly 8 of these rows, so the scan reads nearly as many pages either way.

### Delete

1 250 rows deleted by predicate.

| rollback | WAL |
|---|---|
| 85.6 ms | 26.8 ms |

Rollback mode is 3.2× slower, consistent with 4 fsyncs per commit against WAL's 1.

---

## Not yet measurable

*Superseded — see the phase S3 section below, which lands `PagerStats` and the
counter assertions.* The allocation half is still outstanding:
`benches/support/mod.rs` carries a counting global allocator (`CountingAllocator`,
`measure_allocs`) that nothing uses yet.

## Incidental findings

- `Pager::begin_transaction` immediately followed by `commit_transaction`, with no page
  written, fails with `Illegal pager state transition from WriterLocked to WriterDbMod`.
  The state machine has no path for a writer that never dirtied a page. Not reachable
  from SQL — the connection layer skips the pager commit when nothing is pending — so
  it is an API sharp edge rather than a user-visible bug. The guard types in the
  rewrite remove the state machine, and with it this case.
- The repository root accumulates `_test_pager_legacy_sidecar_metadata_rejected_*.db.pager_checksums`
  files. The test at `src/storage/tests.rs:1174` asserts the sidecar still exists after
  the rejection it is testing, and `TestDbFile::drop` does not clean `.pager_checksums`.
  Untracked, but it grows one file per test run. *(Fixed in S1.)*

---

## Phase B2 — v4 record and index-key encodings

### Disk space

| fixture | before | after | change |
|---|---|---|---|
| five small integers | 99.1 bytes/row | **87.7** | **−11.5 %** |
| `int + 2 text + bool` | 123.7 bytes/row | **105.7** | **−14.6 %** |

Below the 25 bytes/row the design targets, because the record is only one of three
stacked layers. Still on top of every row: the 11-byte `StoredValueLayout` wrapper
(removed in B7), the B-tree cell's two `u16` lengths plus a 2-byte pointer (B3/B4),
and the row-id key's length prefix (B5, when table trees become natively `u64`-keyed).

### Time

Old and new built and run alternately in one session, so these are comparable.

| benchmark | old | new | change |
|---|---|---|---|
| `append_heavy/rollback_batch100` | 30.72 ms | 29.39 ms | **−4.3 %** |
| `append_heavy/wal_batch100` | 17.89 ms | 17.00 ms | **−5.0 %** |
| `append_heavy/rollback_batch1` | 113.13 ms | 111.24 ms | −1.7 % |
| `point_lookup/rollback` | 81.78 µs | 81.62 µs | ~0 |
| `secondary_index/rollback` | 461.7 µs | 453.0 µs | −1.9 % |
| `delete_heavy/wal` | 56.28 ms | 56.42 ms | ~0 |
| `delete_heavy/rollback` | 165.69 ms | 173.77 ms | +4.9 % |
| **`full_scan/rollback`** | **3.97 ms** | **4.60 ms** | **+15.9 %** |
| **`full_scan/wal`** | **4.14 ms** | **4.74 ms** | **+14.5 %** |

Writes got cheaper — fewer bytes to encode, journal and copy. Point and index lookups
did not move. The full scan is the one real regression, and it is the honest cost of a
variable-width format: decoding a field is now a varint read plus a classification,
against reading a fixed-width tag and a fixed-width body. The benchmark holds the whole
table in the page cache, so it charges the extra CPU and never collects the 12 % fewer
pages, which is the pessimal case for this trade. Once pages come from disk, 12 % fewer
of them repays a sub-nanosecond-per-field decode cost many times over.

Three things were fixed while chasing this, each worth roughly a third of an initially
much larger regression:

- `decode` walked the header twice, once to build values and once to validate that the
  body ended where the header said. Both now happen in one pass.
- `to_values` collected into a `Vec` with no capacity hint, so every row went through a
  growth sequence. The header length is an upper bound on the field count and an exact
  one for small records, so it now preallocates.
- Each field was classified twice — once for its body length while walking the header,
  once to build the value. `classify` now returns a `Serial` enum that carries both, so
  two chains of range comparisons per field became one match.

`encode` also stopped staging serial types in a temporary `Vec`, which was a heap
allocation on every row written.

### Correctness

Three index-key ordering bugs fixed. All three produced wrong query results:

- text and blob keys were prefixed with a little-endian `u32` length, so `"b"` sorted
  before `"ab"` and every text index was ordered by length-low-byte first;
- `Integer`/`BigInt`/`UInteger`/`UBigInt` had four separate tags, so in a mixed column
  every `i32` sorted below every `i64` regardless of magnitude;
- decimal keys emitted scale before digits, so `1.5` sorted after `2`.

Also: `-0.0` and `0.0` produced different keys despite being equal, so a unique index
would have accepted both.

---

## Phase S3 (partial) — measurement infrastructure and pool groundwork

### `PagerStats`: counters instead of a clock

The most useful thing in this phase is not a speedup. Repeated benchmarking on the
development machine showed drift large enough to make wall-clock comparison useless
at the resolution this rewrite operates in:

| what was measured | reading A | reading B |
|---|---|---|
| `full_scan`, unchanged code, hours apart | 1.55 ms | 3.97 ms |
| `delete_heavy/rollback`, unchanged code, ~20 min apart | 78 ms | 92 ms |

A ~12 % "regression" was chased for an hour before an alternating A/B with the order
reversed showed it was drift that happened to line up with which build ran first.

`Pager::stats()` now reports fsyncs, page reads and writes, pool hits and misses,
spills and WAL frames. Subtracting two readings gives the work one operation did —
a number identical on every machine under any load. Eight tests in
`storage::tests::work_performed` assert on them, several pinning behaviour that is
known to be bad so the phase that improves it has something to move:

```
rollback commit issues exactly 4 fsyncs      (measured, not inferred)
wal commit issues exactly 1 fsync
wal commit appends frames, writes 0 db pages
a 100-mutation transaction writes each page once
20 reads of a cached page cause 0 backend reads and 20 pool hits
reads perform no writes and no fsyncs
```

**Rule for the rest of this rewrite: compare alternating builds inside one session,
and prefer a counter to a stopwatch wherever the change is structural.**

### Snapshot cost

`PagerSnapshot::Full` cloned the entire `PageCache` — including a deep copy of the
recycled-buffer pool — and `run_atomically` takes one snapshot per row. It now keeps
shared handles to just the dirty pages, since only those differ from durable storage.
The variant is also boxed: the enum is as large as its biggest variant, so every
snapshot moved **424 bytes**, including the `RollbackSavepoint` case that carries
nothing but an id. Now two words.

This was a prerequisite, not a cleanup — the frame arena cannot be `Clone`, so the
plan's S3-before-S4 ordering is wrong on this point.

### Page-id hashing

The pool keyed its map with SipHash-1-3. Replaced with Fibonacci hashing, measured
alternating in one session:

| pass | fib-hash | SipHash |
|---|---|---|
| 1 | 112.65 ns | 115.27 ns |
| 2 | 105.98 ns | 109.68 ns |

**About 3 %** — small, and the number is the point. The cache-hit path is a lock
acquire, a map probe and an `Arc` clone; only the probe got cheaper. The remaining
cost sits in the `RwLock` and the refcount, so the frame arena will only pay for
itself together with the collapse of `Arc<RwLock<Pager>>` to `Arc<Pager>`, which is
the largest single piece of work left in the storage plan.

---

## Phase B8 (partial) — streaming table scans

`read_rows_with_ids` built a `TableCursor`, which already decodes every row into a
`Vec`, and then walked it cloning each row into a second `Vec`. Every caller paid for
the whole table twice. Two streaming primitives replace that where callers filter:

- `visit_table_rows` — decodes one row at a time.
- `visit_table_row_ids` — decodes **no records at all**. Row ids are the tree keys, so
  collecting them for a `DELETE`/`UPDATE` full scan never needs a row's value; that
  path used to decode every column of every row and read only `row_id` from each.

Measured alternating in one session, both passes agreeing on sign and rough size:

| benchmark | materializing | streaming | change |
|---|---|---|---|
| `full_scan/rollback` | 1.924 / 1.868 ms | 1.716 / 1.620 ms | **−12 %** |
| `full_scan/wal` | 1.945 / 1.919 ms | 1.803 / 1.698 ms | **−9 %** |
| `limit_100/rollback` | 1.720 / 1.754 ms | 1.571 / 1.429 ms | **−14 %** |
| `range_1pct/rollback` | 3.045 / 3.024 ms | 2.986 / 2.783 ms | −5 % |
| `delete_heavy` | 88.6 / 89.2 ms | 89.3 / 91.0 ms | unchanged |

`delete_heavy` not moving is the expected result, not a disappointment: at ~139 µs per
row it is bound by four fsyncs per commit, and no amount of decode work matters at
that scale. It is S6's number, not this phase's.

### What is still materialized, and why it is not a one-liner

`LIMIT 100` still costs about 90 % of a full scan. The remaining cost is
`SelectAccessPath::FullTableScan`, which calls `read_from_table` and applies the limit
afterwards, and `locate_rows_for_access_path`, which collects every row before
filtering.

Both resist the callback treatment for the same reason: the scan borrows
`ctx.engine` mutably for its whole duration, and the predicate — `conditions_match` —
needs `ctx` too, because a `WHERE` clause may contain a subquery that runs its own
query. Materializing is how the current code sidesteps that aliasing.

Fixing it properly needs one of: splitting `ExecutionContext` so the scan and the
predicate borrow disjoint fields, or pushing the limit down into the access path so
the scan itself can stop early. Both are query-layer design changes rather than
storage ones, and neither is a safe incremental edit.

---

## Phase B9 — pushing `LIMIT` into the scan

The original baseline recorded that **`LIMIT 100` cost 92 % of a full scan** while
returning 2 % of the rows. `LIMIT` is applied by `apply_select_window` at the very
end, on rows already filtered, grouped and sorted, so the scan decoded all 5 000 rows
of the fixture to return 100.

`SelectExecutor::scan_row_cap` now decides when a prefix of the table is a correct
prefix of the result, and `read_from_table_capped` stops the scan there.

Measured against the previous commit in a git worktree, so both versions were built
and run without ever touching the live tree:

| benchmark | before | after | change |
|---|---|---|---|
| `limit_100/rollback` | 1.503 / 1.489 ms | **59.95 / 56.89 µs** | **25×** |
| `limit_100/wal` | 1.521 / 1.527 ms | **60.67 / 61.28 µs** | **25×** |
| `range_1pct/rollback` | 2.929 / 2.786 ms | 3.050 / 2.787 ms | unchanged |

`range_1pct` not moving is the guard working: it has a `WHERE`, so the scan must
still produce `limit` *matching* rows and pushdown is refused. `LIMIT 100` now costs
about 3.5 % of a full scan, roughly proportional to the 2 % of rows it returns.

### The guards, and how they are policed

Five are load-bearing — `WHERE`, `ORDER BY`, `GROUP BY`, `DISTINCT`, and aggregate
projections. Each is a way a later row can displace an earlier one. Each has a test
in `sql::tests::limit_pushdown_tests` that fails when its guard is deleted, verified
by deleting them.

One of those tests did not work at first: the `DISTINCT` case passed with its guard
removed, because the fixture cycled buckets `0..4` and the first five rows already
covered every distinct value. The fixture now has a column whose second value only
appears past row ten, so a short prefix genuinely produces the wrong answer.

`having_clause` and `set_operation` are defensive — no query could be constructed
that reaches this path with either set. They are kept because they cost nothing and
dropping them would make correctness depend on that routing detail; the `UNION` test
documents the behaviour and states that it does not police the guard.

### Note on methodology

The two earlier A/B runs used `git stash` to swap versions, and twice an interrupted
run left the working tree clean with the changes stranded in a stash. This comparison
used `git worktree add --detach HEAD~1` instead: the old version is built in its own
directory and the live tree is never modified. That is the pattern to use.

---

## Phase S6 (partial) — one fewer fsync per rollback commit

`commit_rollback_transaction` synced the journal, then called `flush`, which stages
the persisted-state page — journaling it — and syncs again. Only the staging happened
in between, and it writes no database page, so the first sync was redundant.

**Counter: 4 fsyncs → 3.** The `rollback_commit_fsync_count_is_pinned` test failed on
the change and its assertion moved with it, which is the workflow those counters exist
for.

**Clock: no measurable change.** Measured against the previous commit in a worktree,
two passes each:

| benchmark | 4 fsyncs | 3 fsyncs |
|---|---|---|
| `append_heavy/rollback_batch1` | 42.4 / 43.2 ms | 43.8 / 41.3 ms |
| `append_heavy/rollback_batch100` | 11.6 / 11.7 ms | 12.8 / 11.6 ms |
| `delete_heavy/rollback` | 82.5 / 76.2 ms | 78.0 / 75.2 ms |

Medians overlap and the sign flips between passes. That is a real result, not a
failed measurement: the sync that was removed was the *second* sync of the same
journal file with a few hundred bytes appended in between, so the kernel had almost
nothing left to flush and the call was nearly free on this hardware. On a device that
honours barriers, or a filesystem without an SSD's write cache underneath, the same
call is not free — which is exactly why the count is tracked separately from the time.

Claiming a speedup here would be overclaiming. The honest statement is that a
redundant syscall is gone and the workload numbers did not move.

### Getting to two

The last fsync is the journal's `Committed` marker, and it cannot be removed by
reordering: it is what stops recovery rolling back a transaction that already
completed. Replacing it needs the per-page `gen` stamp from the format-v4 page
trailer, so recovery can decide whether a commit landed by comparing page generations
instead of reading a durable flag.

### A testing gap, and closing it

Nothing in the suite could test fsync *ordering*. Dropping a `Pager` is a process
exit, not a power loss: unsynced writes are still in the OS page cache, so reopening
reads them back regardless. Moving the journal sync to after the page writes —
breaking the write-ahead rule outright — left every recovery test passing.

That is now closed for the journal. `JournalFile` remembers what the file contained
at the last `sync_all`, and `lose_unsynced_writes` discards everything written since.
`a_power_loss_that_drops_unsynced_journal_writes_still_recovers` stops a commit once
one page has reached the database file, throws away the unsynced journal bytes, and
requires that reopening restores both originals. It passes today and fails when the
sync is moved after the page writes — both directions checked.

It is the first test in the codebase that can observe fsync ordering, which is what
makes the remaining 3→2 fsync work and the format-v4 flag day verifiable rather than
argued. The database file has no equivalent yet, so only journal ordering is covered;
the WAL has none at all.

---

## Phase S6 (rest) + B7 — two fsyncs, and large values out of the leaf

### Rollback commits now issue two fsyncs

The third existed only so recovery could tell a finished commit from an abandoned
one: it wrote a "committed" flag into the journal header and synced it, a whole fsync
for one byte. The journal now records a **commit intent** instead — every page the
commit is about to write, with the checksum that page must hold once it has — synced
before any database page is overwritten. Recovery reads the database and compares.

Comparing against the *post* image rather than asking "did this page change" is what
makes it sound: a page whose new contents equal its old is indistinguishable by the
latter, and a torn page matches neither image, so it reads as partial.

**The plan expected this to require the format-v4 per-page `gen` stamp, and therefore
a flag day. It does not** — the journal alone can answer the question.

Two fsyncs is the floor for a rollback journal: one to make the undo information
durable before the database is touched, one to make the new contents durable.

**Clock: still no measurable change**, consistent with the 4→3 result. `delete_heavy`
and `append_heavy` are flat across both passes. The syncs being removed are of a small,
already-mostly-flushed journal file, and on this hardware they are close to free. On a
device that honours barriers they are not, which is why the count is tracked separately.

### Large values

`MIN_LOCAL_PAYLOAD` drops from 489 to 32, and overflow chains are read in one pass
rather than two. Measured against the commit before this round, two passes each:

| benchmark | before | after | change |
|---|---|---|---|
| `large_row/rollback_scan_without_body` | 229.9 / 231.0 µs | 199.3 / 192.9 µs | **−15 %** |
| `large_row/rollback_scan_with_body` | 249.4 / 258.5 µs | 216.4 / 207.2 µs | **−16 %** |
| `large_row/wal_scan_without_body` | 242.8 µs | 212.5 / 199.1 µs | ~−15 % |
| `delete_heavy`, `append_heavy` | — | — | flat |

### What did not improve, and why

The baseline observed that skipping an unprojected 2 KB column saves only 9 %, and
predicted denser leaves would turn that into roughly 12×. **It did not.** The saving
is still about 8 %: both figures dropped by 15 %, but their ratio barely moved.

The reason is that not projecting a column does not avoid reading it. The executor
materialises whole rows and projects afterwards, so `SELECT id, tag` hydrates `body`
anyway — chain walk included. Denser leaves make every scan cheaper; they cannot make
an unread column free while it is still being decoded.

Getting that win needs projection pushdown: deciding which fields to decode before
decoding them. `RecordRef` was built for exactly this — field offsets come from a
header walk with no body access — but nothing uses it yet. That is the piece of work,
not the spill rule.

---

# Cumulative: the whole branch against pre-rewrite `main`

Measured by building `main` in a git worktree — with this branch's bench harness
copied in, since `main` predates it — and running both alternately, twice each. These
are the numbers to quote; per-phase deltas cannot be multiplied together, because the
machine drifts more between sessions than most of the individual changes are worth.

Medians of two passes. 5 000-row fixture.

## Disk space

Deterministic, so no drift caveat applies.

| fixture | before | after | change |
|---|---|---|---|
| five small integers | 99.1 bytes/row | **87.7** | **−11.5 %** |
| `int + 2 text + bool` | 123.7 bytes/row | **105.7** | **−14.6 %** |

## Time

| benchmark | before | after | change |
|---|---|---|---|
| `limit_100/rollback` | 1 386 µs | **54.7 µs** | **25×** |
| `limit_100/wal` | 1 473 µs | **59.1 µs** | **25×** |
| `append_heavy/wal_batch100` | 9.90 ms | 6.05 ms | **−39 %** |
| `append_heavy/wal_batch1` | 49.1 ms | 33.5 ms | **−32 %** |
| `append_heavy/rollback_batch1` | 62.8 ms | 43.5 ms | **−31 %** |
| `append_heavy/rollback_batch100` | 16.1 ms | 11.4 ms | **−29 %** |
| `large_row/rollback_scan_with_body` | 321.6 µs | 209 µs | **−35 %** |
| `large_row/rollback_scan_without_body` | 292.3 µs | 197 µs | **−33 %** |
| `large_row/wal_scan_*` | ~310 µs | ~220 µs | **−29 %** |
| `delete_heavy/wal` | 25.2 ms | 22.6 ms | −10 % |
| `delete_heavy/rollback` | 87.5 ms | 82.1 ms | −6 % |
| `full_scan/rollback` | 1.550 ms | 1.577 ms | flat |
| `full_scan/wal` | 1.576 ms | 1.610 ms | flat |
| `point_lookup` (both modes) | 29.9 / 34.8 µs | 30.4 / 35.0 µs | flat |
| `range_1pct` (both modes) | ~2.76 ms | ~2.78 ms | flat |
| **`secondary_index/rollback`** | 168.2 µs | 174.9 µs | **+4 %** |
| **`secondary_index/wal`** | 173.6 µs | 186.4 µs | **+7 %** |

## Reading this honestly

**Full scans did not get faster.** B8's streaming was worth −12 %, and B2's
variable-width record decoding cost about +16 %; they cancel. The rewrite bought disk
space and `LIMIT` on that path, not raw scan throughput. Making scans genuinely faster
needs projection pushdown and the B-tree page format, neither of which has landed.

**Secondary-index lookups got slower**, consistently and in both modes. The cause is
known: index keys carry integers as a tag plus eight big-endian bytes where the old
format used four, because one order now covers `i64::MIN..=u64::MAX` — which is what
fixed the bug where every `i32` sorted below every `i64`. Dropping the redundant rowid
copy from the entry value pays some of that back, but not all. Correctness was worth
it; the size is recoverable later by varint-encoding the integer key body.

**Where the wins actually came from.** Appends: fewer bytes to encode and journal,
plus two fewer fsyncs. Large rows: values out of the leaf and single-pass chain reads.
`LIMIT`: the scan stops instead of materialising the table.

---

## Small-footprint batch

Five changes of a few lines each, measured against the commit before them, two
alternating passes in a worktree.

| benchmark | before | after | change |
|---|---|---|---|
| `delete_heavy/rollback` | 86.7 / 81.6 ms | 74.6 / 74.0 ms | **−12 %** |
| `delete_heavy/wal` | 21.3 / 20.8 ms | 23.2 / 19.9 ms | flat |
| `point_lookup`, `full_scan` | — | — | inconclusive |
| `secondary_index` | 169 / 165 µs | 183 / 166 µs | **inconclusive** |
| bytes/row | 87.7 | 87.7 | **unchanged** |

The delete result is the real one and has an obvious cause: `is_child_underflow`
decoded the child node in full — materialising every key and value into owned `Vec`s
— to answer a question that `serialized_size_on_page` computes from cell byte ranges
on the page. That ran on every rebalance check of every delete.

### What did not show up, and what I do not know

The first `AFTER` pass ran slow across every benchmark in it — `point_lookup` 33.2 µs
against 29.4 in the second pass, `full_scan` 1.73 ms against 1.58 — so the
`secondary_index` and `full_scan` columns are noise, not signal. Alternating passes
caught that; a single run would have reported a regression.

**The index-key change did not move bytes/row at all**, and that is a fact rather than
noise: the figure is deterministic and identical to the digit. Integer keys provably
shrank — a unit test pins three bytes where nine were used, and 0 and −1 cost two — so
either the primary-key index is a smaller share of the file than assumed, or an
`INT PRIMARY KEY` on this engine does not populate a separate index tree whose keys
would shrink. I have not established which, and the honest summary is that the
encoding is smaller while the file is not.

The change still stands on correctness: the fixed eight bytes existed to give one
order spanning `i64::MIN..=u64::MAX`, and shortening it keeps that order while
undoing the size cost the rewrite introduced. But it should not be quoted as a
measured space win until the fixture question is answered.

---

## Medium batch — page and node clones on the write path

Two costs removed, both paying a page- or node-sized copy for information already
available:

- all four in-place B-tree fast paths ended by cloning the whole 4 KiB page they had
  just edited, then handed the same page to `write_page`, which wrapped it in an `Arc`
  again — two page-sized allocations for a single-cell change;
- `can_merge_with` cloned both nodes, decoded the clones into owned `Vec`s and
  concatenated them purely to measure the result, and `merge_leaf` called it a second
  time, so one rebalance decision cost four clones and four full decodes.

Measured against the previous commit in a worktree, two passes each:

| benchmark | before | after | change |
|---|---|---|---|
| `append_heavy/wal_batch100` | 6.14 / 6.06 ms | 5.58 / 5.72 ms | **−7 %** |
| `append_heavy/rollback_batch100` | 12.02 / 11.58 ms | 11.43 / 11.26 ms | **−4 %** |
| `append_heavy/wal_batch1` | 33.94 / 33.31 ms | 32.06 / 33.04 ms | −3 % |
| `delete_heavy` (both modes) | — | — | flat |

Deletes not moving is expected: that path already cleared the node's page view by hand
before mutating, for exactly this reason, so only the clone afterwards was left — and
`can_merge_with` runs on a rebalance, not on every delete.

**The first attempt at this measurement was thrown away.** Its confidence intervals
spanned two to four times the median — `[41.9, 95.7, 165.9] ms` on one benchmark —
which means the machine was loaded, not that the change did something dramatic. Even
the re-run has two intervals wide enough to ignore, which is why the table quotes only
benchmarks that agreed across both passes.

## The third medium item, declined

`save_table_metadata` does a 4 KiB read and a 4 KiB write on every commit, and
`load_table_metadata` a 4 KiB read on every transaction begin. Both are real, and both
are ~300 ns of memcpy against a commit that costs tens of microseconds — under a
tenth of a percent, well below the noise floor everything here is measured at.

Removing the copies properly means partial page writes, so the catalog section of the
metadata page can be updated without reading and rewriting the whole page. That is a
larger change than the payoff justifies today, and shaving only the clone would be
churn for something unmeasurable. Same reasoning as declining `read_reserved_blob`
earlier, and noted here so the next person does not have to rediscover it.
