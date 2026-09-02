# Measuring Hematite

How to find out what the engine costs, and what it currently costs.

## Why not wall clock

On the machine this was developed on, the *same unchanged binary* measured 1.55 ms
and 3.97 ms for the same full-scan benchmark hours apart, and 76 ms against 97 ms
for another twenty minutes apart. A change worth 10 % is invisible under that much
drift, and drift that happens to line up with which build ran first reads exactly
like a real regression.

So wall clock is the last instrument here, not the first, and it is only ever used
by building both versions and running them **alternately in one session**. Never
against a stored number from another day.

## The three instruments, and what each is blind to

| instrument | measures | blind to |
|---|---|---|
| `valgrind --tool=callgrind` | instructions executed | syscalls — a `pread` is a handful of client instructions whatever it costs |
| `valgrind --tool=dhat` | allocations and bytes | memcpy, and any copy into a buffer the page pool recycles |
| `strace -c` | syscalls | everything in userspace |
| `PagerStats` counters | fsyncs, page reads and writes, pool hits and misses, journal bytes, page-image copies, trailer verifications, leaf rotations | nothing it does not already count |

Every one of those blind spots has hidden a real cost at some point. Instruction
counts said a warm page cache was 1 % *worse* than a cold one, because a pool hit
costs more client instructions than a `pread` stub; the counters said it removed
1 400 page reads per 200 transactions. Allocation counts showed nothing when four
4 KiB copies per savepoint were removed, because the pool hands the same recycled
buffers back.

Use at least two. When they disagree, that disagreement is the finding.

## Counters as tests

`PagerStats` exists so that "this commit issues exactly two fsyncs" and "this
transaction writes each dirty page exactly once" can be ordinary `#[test]`s. They
are identical on every machine under any load, they run in CI without a benchmark
harness, and they fail loudly when a refactor quietly adds work back.

`storage::tests::work_performed_tests` is where they live. Several assert things
no other instrument can see — that opening a read scope verifies no page trailers,
that freeing pages does not stat the file, that a savepoint copies a page image
once.

**A counter test is worth writing only if it fails when the behaviour is reverted.**
Check that. This codebase has produced four tests that passed either way, and
each was caught by deliberately backing the fix out and watching them stay green.

## The harnesses

`examples/scan_profile.rs` — bulk insert then repeated full scans, sized by
`SCANS` and `MODE=wal`. Instructions per scanned row:

```
SCANS=0  valgrind --tool=callgrind --callgrind-out-file=.scratch/base.out \
    --cache-sim=no --branch-sim=no ./target/release/examples/scan_profile
SCANS=13 valgrind --tool=callgrind --callgrind-out-file=.scratch/s13.out \
    --cache-sim=no --branch-sim=no ./target/release/examples/scan_profile
```

Subtract the two totals and divide by `13 * ROWS`. The baseline run exists to net
out the inserts, which cost far more than the scans do.

`examples/storage_profile.rs` — one workload per run, selected by `PHASE`:
`setup`, `scan`, `lookup`, `update`, `delete`, `rollback`, `append_txns`. Sized by
`ROWS` and `REPS`, with `MODE=rollback|wal`. `PHASE=setup` builds the fixture and
stops, so any other phase can be isolated by subtraction. Every run prints the
storage counters for the measured phase.

Syscalls, which callgrind cannot see:

```
PHASE=append_txns strace -c -f ./target/release/examples/storage_profile
```

`benches/storage_micro.rs` and `benches/storage_workloads.rs` are the criterion
benchmarks, for when wall clock is genuinely the question.

## Cost by layer

A per-function list says which function is hot. It does not say which *layer* is, and
that is the question a design change starts from — a scan that is three quarters record
decode and a point lookup that is half allocator want completely different fixes.

`scripts/layer-attribution.sh` answers it: it buckets `callgrind_annotate`'s self costs
by the `hematite::<layer>::` prefix in each symbol, so generic instantiations land where
the work happens rather than in the standard library. Give it two profiles and it prints
the difference, which is what every phase measurement needs, since each harness builds a
fixture before the workload it measures.

    scripts/layer-attribution.sh .scratch/lookup.out .scratch/setup.out
    scripts/layer-attribution.sh .scratch/s13.out    .scratch/s0.out

It checks its own parse against `PROGRAM TOTALS` and complains if the two disagree.

Its blind spot is the one self cost always has: `~libc-alloc` and `~libc-mem` are
attributed to nobody. A layer that allocates heavily reads small here and large in those
buckets, which is exactly what a point lookup does — 40 % of it is `malloc` and `free`
called from the query layer. Reading them needs `--separate-callers=2`, which attributes
the allocator to its callers, or `dhat`.

Shares of each workload, net of the fixture:

| layer | scan | lookup | insert | update | delete |
|---|---|---|---|---|---|
| catalog | 49.0 % | 7.5 % | 11.6 % | 15.1 % | 12.9 % |
| btree | 32.3 % | 10.7 % | 23.2 % | 32.5 % | 34.6 % |
| storage | 0.2 % | 3.1 % | 9.6 % | 14.7 % | 16.5 % |
| query | 0.1 % | 17.2 % | 6.6 % | 4.3 % | 1.3 % |
| parser | 0.0 % | 10.3 % | 18.5 % | 0.0 % | 0.0 % |
| sql | 0.0 % | 3.7 % | 0.0 % | 0.0 % | 0.0 % |
| malloc/free | 0.4 % | 28.8 % | 13.5 % | 18.7 % | 20.6 % |
| memcpy/memset | 11.9 % | 9.1 % | 6.9 % | 8.8 % | 10.7 % |
| std/hashbrown | 0.1 % | 8.6 % | 8.2 % | 5.6 % | 3.5 % |

There is no one bottleneck layer. A scan is bottlenecked in `catalog`, on turning record
bytes into `Value`s — the pager is free, because the cache is warm and the node hands out
borrowed slices. A point lookup is bottlenecked in `query`, on per-statement setup: the
B-tree descent and page read together are 10 %, and the other 90 % is deciding what to
read. Writes are bottlenecked in `btree` and `storage`.

Function names come from the ELF symbol table, so a plain release build profiles
fine. `debug = true` in `[profile.release]` adds *source-line* detail, and is worth
setting only when you want `callgrind_annotate`'s per-line annotation — note that it
also splits each function across every file it inlined from, and makes the tool emit
auto-annotation sections that a naive parse will double-count.

## Where the cost is now

Instructions, 5 000-row workloads net of the fixture, and per scanned row:

| workload | per operation | was |
|---|---|---|
| insert (batched) | 29 400 | 29 700 |
| delete | 26 300 | 32 600 |
| update | 51 500 | 58 300 |
| point lookup | 50 400 | 72 800 |
| full scan | 492 per row | 938 |

The right-hand column is the same measurement before the changes listed below. A scan
more than halved because a projecting decode stopped re-deriving a record layout that
every row of a table shares.

Allocations per scanned row: **0.018**. A scanned row is a slice of one buffer,
not a `Vec` of its own.

Disk, five small integer columns: **27.0 bytes per row**, of which the table tree
is 26.4 — a table keyed by a single integer column keeps no separate index for
that key, because the key *is* the row id.

Syscalls, 200 single-row transactions:

| | rollback | WAL |
|---|---|---|
| fsync | 411 (2 per commit) | 207 (1 per commit) |
| total | 4 486 | 3 017 |

## What is left, measured

- A point lookup spends 29 % of itself in `malloc` and `free`. What is left is the
  per-statement `Vec`s themselves: validation still builds a `SourceBinding` per source
  and execution a `ResolvedSource`, each with a vector of names, of types and of
  collations, and drops them at the end of the statement. The names inside are shared
  now, so what remains is the vectors and the structs, not the strings. Sharing the
  vectors too means caching a derived list per table and keeping it in step with DDL —
  the one thing the `Arc<str>` migration was chosen to avoid.
- `extract_literal_equalities` builds a `HashMap<String, Value>` per statement under
  SipHash, 392 instructions of a lookup. Deliberately left: unlike the schema maps this
  one is keyed by the *query's own* predicates, so a query can grow it, and a weak
  hasher would make a long enough list of colliding condition names quadratic. It is
  the case `catalog::name_hash` is documented as not covering.
- `read_page_from_wal_file_at_offset` reopens the WAL on every on-demand frame read:
  213 `openat` and 613 `lseek` per 200 WAL transactions, about 12 % of WAL syscall
  time. It wants a kept handle and positional reads.
- SQL lexing and parsing is 18.5 % of an insert, the largest share of one after the
  B-tree. An insert has no bottleneck left otherwise: nothing else in it is above 5 %.
- `BTreeNode::to_page` still clears the gap between the cell pointers and the first
  cell, 945 instructions per deleted row, because after a rebalance that gap is most of
  the page. Note that `remove_cell` does *not* clear a freed cell body — it links it
  into the freeblock chain — so free space already carries deleted bytes on the common
  path, and what `to_page` spends is not upholding an invariant the engine keeps
  elsewhere. Dropping it would be worth about 3.5 % of a delete, and a deliberate
  decision about what free space may contain.
- A scanned row spends 185 instructions in `decode_fields_into_planned` and 56 in
  `read_table_columns`. Decode is 241 of 493, from 621 of 938.

### Not a bug: a delete is linear

A delete costs 26 500 instructions per row over 5 000 rows and 29 200 over 10 000, and
that 10 % is not an algorithmic term. The counters scale exactly: `pool_hits` 44 438
against 89 662, `undo_page_copies` 34 against 66, `page_reads` 2 either way, and no
misses, evictions or spills at all. Per-function costs per row are flat — record decode
identical, `BTreePageHeader::parse` within 1 %, `snapshot_original_page` within 1 %. What
grows is B-tree descent depth, which is `log n` and shows as `LazyKeyReader::key` rising
9 %, and glibc's allocator on a larger heap, which shows as `_int_malloc` rising 49 %.
There is no quadratic left to find here.

### What was done

Each measured on its own:

- `ProjectionPlan` in `src/catalog/record_codec.rs` keeps the field layout a record
  shape implies and reuses it while the record header still matches, instead of
  re-walking and re-classifying that header for every row. Matching the whole header
  prefix rather than the serial types alone also settles where the bodies start, so a
  hit does not build a `RecordRef` at all. Scan: −37 %.
- `RecordRef::value_from_body` reads a field straight into an owned `Value` rather than
  building a `ValueRef` and immediately destructuring it, which matched on the serial
  type twice. Scan: a further −16 %, for −48 % in total.
- `Column::name` is an `Arc<str>`. Every statement naming a source cloned it — into a
  `SourceBinding` for validation and a `ResolvedSource` for execution — so a `SELECT`
  over five columns allocated ten strings before reading a row. The alternative was a
  derived name list cached per table, which would have needed to be invalidated on
  every schema change; sharing the names themselves means there is nothing derived to
  go stale. Lookup: −10 %.
- `Schema` holds its tables as `Arc<Table>`. A `SELECT` cloned a whole `Table` — every
  column with its name, character set, collation and default — per statement, only to
  escape a borrow. Lookup: −11 %.
- `catalog::name_hash` keys the schema's name maps on a multiply-and-rotate hash rather
  than SipHash. `Schema::table_names` and `Table::column_indices` cannot be grown by a
  query, only probed, so the attack SipHash defends against has nowhere to go — the same
  argument `storage::page_hash` makes for page ids. SipHash fell from 4 317 instructions
  of a lookup to 1 435.
- The free-page state a savepoint captures is shared under `Arc` rather than copied. A
  savepoint is taken per row and used to copy the whole free list, which a `DELETE`
  grows as it runs. `PageSet::insert` fell from 6.4 % of a delete to 0.6 %.
- `ResolvedSource` resolves a column name by scanning its column list rather than
  through a `HashMap<String, usize>` rebuilt per statement. Resolution happens once per
  statement, not per row, so the map cost a cloned key per column to answer a handful
  of probes. Lookup: −8 %.
- `FreeList` and the two WAL page maps key on `PageIdHasher` rather than SipHash.
  `page_is_live_at_savepoint` runs on every page the write path captures, and reached a
  `HashMap<PageId, usize>` under the default hasher. Delete: −3 %.
- `page_format`'s free functions take the header the caller already parsed, and
  `defragment_page` returns the one it leaves behind, instead of re-reading it from the
  page bytes. Worth 0.4 %, against the 7 % the header's share of the profile suggested:
  `parse` is cheap and the calls removed were per cell operation, not per descent level.
- `BTreeNode::to_page` clears only the gap between the cell pointers and the first cell
  rather than the whole page, since the header, pointer array and cell region are all
  written over. Worth 0.5 %, not the 4 % expected, because after a rebalance the gap is
  most of the page.
