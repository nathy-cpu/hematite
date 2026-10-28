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

## The four instruments, and what each is blind to

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
that taking a snapshot does not stat the file, that a savepoint copies a page image
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

`append_txns` commits `REPS` single-row transactions; `ROWS` only sizes the fixture.

`examples/wal_profile.rs` — the same method in WAL mode. `PHASE=scan` scans `SCANS`
times (default 13, with `SCANS=0` as the baseline); `PHASE=append` commits
`SCANS * 100` rows one transaction at a time, which is the WAL write path.

`examples/parse_profile.rs` — one parser workload per run, selected by `PHASE`,
sized by `REPS`, with `REPS=0` as the subtraction baseline. Unlike the others it
counts its own allocations: the binary installs a counting `#[global_allocator]`, so
every run prints allocations and bytes for the measured phase alone. That number
needs no tooling and does not drift, which matters because the parser's cost is
mostly allocator traffic and `~libc-alloc` is attributed to nobody by construction.

```
PHASE=insert_values REPS=200 ./target/release/examples/parse_profile
PHASE=insert_values REPS=0   valgrind --tool=callgrind --cache-sim=no --branch-sim=no \
    --callgrind-out-file=.scratch/p-0.out   ./target/release/examples/parse_profile
PHASE=insert_values REPS=200 valgrind --tool=callgrind --cache-sim=no --branch-sim=no \
    --callgrind-out-file=.scratch/p-200.out ./target/release/examples/parse_profile
scripts/layer-attribution.sh .scratch/p-200.out .scratch/p-0.out
```

`benches/storage_micro.rs` and `benches/storage_workloads.rs` are the criterion
benchmarks, for when wall clock is genuinely the question. `HEMATITE_BENCH_ROWS`
sizes `storage_workloads` (default 20 000), which also prints a disk-space report
before criterion starts.

## What parsing allocates

Measured by `examples/parse_profile.rs`, per repetition. The first seven parse a
string and drop the tree; the rest go through a real `Connection`, because the cost
they exist to expose only happens there. The right-hand columns are the same harness
against the engine as of `0684e9e`.

| phase | what it parses | allocs | bytes | allocs at `0684e9e` | bytes at `0684e9e` |
|---|---|---|---|---|---|
| `script_split` | 360-statement DDL script, completeness only | **0** | **0** | 8 | 522 240 |
| `repl_paste` | 120 lines pasted, checked after each | **8** | **19 635** | 248 | 1 780 755 |
| `point_lookup` | `SELECT a FROM t WHERE id = 4711;` | 19 | **1 168** | 9 | 2 484 |
| `blob_strings` | INSERT with a 4 KiB hex blob and two escapes | 24 | 25 943 | 25 | 8 750 |
| `check_fragment` | one CHECK condition | 46 | 3 448 | 36 | 3 189 |
| `select` | 4-join SELECT with GROUP BY / HAVING / ORDER BY | **71** | 8 672 | 81 | 8 018 |
| `insert_values` | `INSERT … VALUES` × 400 rows | 860 | 441 467 | 821 | 364 186 |
| `lookup` | end-to-end point lookup | 112 | **6 540** | 113 | 8 187 |
| `prepared_rebind` | bind + execute a prepared INSERT | **117** | 42 292 | 126 | 42 544 |
| `check_rows` | 50-row INSERT into a table with two CHECKs | **2 516** | **167 698** | 4 584 | 435 640 |
| `no_check_rows` | the same INSERT with no CHECK constraints | 1 654 | 159 946 | 1 628 | 153 595 |
| `correlated` | one correlated subquery over 500 rows | **59 190** | **3 135 987** | 69 191 | 3 590 632 |

What this says that the layer table does not:

- **The parse-only phases measure the bridge, not the arena.** Every one goes through
  `parse_sql_statement`, which builds a fresh arena *and* materialises the owned tree
  from it (`src/parser/bridge.rs`), so a statement pays for both forms. That is why
  a point lookup's parse allocates 19 times against 9, and a 400-row `INSERT` 441 KB
  against 364 KB. These numbers fall when the query layer reads the arena directly
  and the bridge comes out.
- **What does not go through the bridge has already fallen.** Checking a script or a
  REPL buffer for completeness is a byte scan that allocates nothing of its own.
  CHECK constraints are parsed once per statement rather than once per row, which
  halves `check_rows`.
- **A correlated subquery costs 118 allocations per outer row.**
  `bind_correlated_subquery` deep-clones the subquery's AST for every row of the outer
  query. Planning it per row is unavoidable — binding is what turns `u.id = t.id` into
  a literal equality, and that is what lets the subquery use an index rather than
  scan — but the *clone* is the arena's to remove, once the query layer binds against
  it.

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
  the one thing sharing the names as `Arc<str>` was chosen to avoid.
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
  The parser now builds into an arena, but the query layer still reads the owned
  tree, so `src/parser/bridge.rs` rebuilds it after every parse: a 400-row `INSERT`
  allocates 860 times, one `Vec` per row and one `String` per text literal, plus the
  arena itself. Removing that cost means every consumer in `query/` reading the
  arena directly — a change to the query layer, not to the parser.
- A correlated subquery costs **118 allocations per outer row**, 59 190 for one over
  500 rows. `SelectExecutor::bind_correlated_subquery` deep-clones the subquery's AST
  and then re-plans it — optimize, rewrite, choose an access path — for every row of
  the outer query, when only the substituted literals differ between rows. The
  planner validates the subquery once per statement, but `SelectExecutor::execute`
  still validates what it runs, so each row pays one validation pass. It is the
  largest AST-copy site in the engine, and no profiled workload runs one, so it
  appears nowhere in the layer table. Binding against the arena would make the clone
  a handful of `memcpy`s; the re-plan wants hoisting regardless.
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

A delete costs 26 300 instructions per row over 5 000 rows and 29 200 over 10 000, and
that 10 % is not an algorithmic term. The counters scale exactly: `pool_hits` 44 438
against 89 662, `undo_page_copies` 34 against 66, `page_reads` 2 either way, and no
misses at all. Per-function costs per row are flat — record decode
identical, `BTreePageHeader::parse` within 1 %, `snapshot_original_page` within 1 %. What
grows is B-tree descent depth, which is `log n` and shows as `LazyKeyReader::key` rising
9 %, and glibc's allocator on a larger heap, which shows as `_int_malloc` rising 49 %.
There is no quadratic left to find here.

### What was done

Each measured on its own:

- The parser builds into a reusable arena (`src/parser/arena.rs`). An expression node
  is 16 bytes and an index into a flat buffer rather than 48 bytes and a `Box` per
  child; a statement is 12 bytes rather than 336; a list is a run in one shared
  buffer rather than a `Vec`; a name is a byte range into the statement's own SQL
  rather than a `String`. `Ast::reset` empties the buffers without releasing them, so
  a warm arena parses without reaching the allocator.

  **This is not yet visible in the numbers above, and currently makes them worse.**
  The query layer still reads the owned AST in `src/parser/ast.rs`, so
  `src/parser/bridge.rs` materializes one from the arena after every parse — the
  arena is built *and* the owned tree is built from it. A point lookup's parse
  allocates 19 times where it allocated 9. The bridge exists to make the arena parser
  checkable rather than to stay: with it in place the whole existing suite, 212 of
  whose tests drive real SQL end to end, exercises the arena parser against the owned
  trees those tests were written for. It comes out when the query layer reads the
  arena directly, and the allocations go with it. Nothing in the engine reuses an
  `Ast` across statements yet: `parse_sql_statement` builds a fresh one per call.
- `script_is_complete` scans bytes for a semicolon outside a literal rather than
  lexing the whole buffer. The REPL asks it after every line of a statement it is
  still collecting, so lexing made pasting quadratic: a 120-line paste cost 248
  allocations and 1.7 MB, and now costs none of its own. It also cannot fail any
  more — the old one returned the lexer's error and `main` propagated it, so typing
  `SELECT 'oops` ended the session, when an unterminated literal just means the
  statement is not finished.
- `QueryPlan::node` is derived when `EXPLAIN` asks for it instead of built by the
  planner. A `PlanNode` is a *description* — owned table and column names, a
  `Vec<String>` of ORDER BY columns — and nothing but `EXPLAIN` and `Debug` ever reads one.
  Worth 3 allocations of every SELECT, and 3 per row of an outer query whose
  subquery is correlated.
- The planner validates a correlated subquery's shape once per statement rather than
  once per outer row. Binding substitutes literals at the same positions every row,
  and validation reads column references and structure, never a literal's value — it
  was half of what re-planning cost. Correlated subquery over 500 rows: −7 %.
  `SelectExecutor::execute` still validates the bound subquery each row.
- A `Token` is a kind and a byte range into the input rather than a `Cow<str>` or a
  `Vec<u8>`, which makes it `Copy` and 12 bytes instead of 32. Escaped literals and
  identifiers — the only text whose value is not its source — report a distinct kind
  carrying the raw span and are unescaped by the parser, on the path that had to own
  a `String` anyway. Nothing in the lexer allocates but the token buffer, and that
  buffer is now sized from the input instead of a fixed 64 tokens: a point lookup's
  parse allocates 580 bytes where it allocated 2 484, and a 400-row `INSERT` 220 KiB
  where it allocated 356.
- A SQL script splits into ranges over one token buffer rather than a `Vec<Token>`
  per statement. A 360-statement DDL script lexes in 2 allocations of 87 KiB, from 8
  of 522 KiB.
- `validate_statement` is reached through the per-statement validator the caller
  already knows it wants. Nine executors used to deep-clone their whole statement —
  a 336-byte `Statement` with ten `Vec`s under it — purely to wrap it in the enum
  that function matches on. Worth 8 allocations of every statement, and it is a
  25-line change: the validators already had the right signatures and were private.
- A table's CHECK constraints are parsed once per statement, not once per row. The
  catalog stores a check as SQL text, so every row written re-lexed and re-parsed
  every constraint on the table, and rebuilt the scan executor the conditions are
  evaluated against. Checking a row against two constraints: 59 allocations of
  5 641 bytes, to 17 of 219.
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
- `page_format` reuses a page header it has already parsed instead of re-reading it
  from the page bytes: `insert_cell` and `remove_cell` parse it once and keep it,
  `total_free_space_with_header` takes the caller's, and `defragment_page` returns the
  one it leaves behind. Worth 0.4 %, against the 7 % the header's share of the profile suggested:
  `parse` is cheap and the calls removed were per cell operation, not per descent level.
- `BTreeNode::to_page` clears only the gap between the cell pointers and the first cell
  rather than the whole page, since the header, pointer array and cell region are all
  written over. Worth 0.5 %, not the 4 % expected, because after a rebalance the gap is
  most of the page.
