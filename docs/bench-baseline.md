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
