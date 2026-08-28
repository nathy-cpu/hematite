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

Set `debug = true` in `[profile.release]` before a callgrind run, or the profile
comes back without symbols.

## Where the cost is now

Instructions, 5 000-row workloads net of the fixture, and per scanned row:

| workload | per operation |
|---|---|
| insert (batched) | 29 700 |
| delete | 32 600 |
| update | 58 300 |
| point lookup | 73 600 |
| full scan | 942 per row |

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

- `read_page_from_wal_file_at_offset` reopens the WAL on every on-demand frame
  read: 213 `openat` and 613 `lseek` per 200 WAL transactions, about 12 % of WAL
  syscall time. It wants a kept handle and positional reads.
- `captured_page_ids` allocates a fresh set per savepoint, and a savepoint is
  taken per row. `PageSet::insert` and the malloc/free either side of it are
  around 12 % of a delete.
- SQL lexing and parsing is 5.6 % of an insert and 2.5 % of a point lookup.
- `materialize_filtered_rows` clones a `Table` per statement, and query
  validation allocates around 60 times per point lookup. Both are in
  `src/query/`, above the storage layer.
