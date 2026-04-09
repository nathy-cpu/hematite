# SQLite B-Tree and Pager: Implementation Notes and Rust Reimplementation Guide

## Scope

This document focuses on the SQLite storage engine below the SQL layer:

- `src/btree.c`
- `src/btree.h`
- `src/btreeInt.h`
- `src/pager.c`
- `src/pager.h`
- `src/pcache.h`
- `src/pcache.c`
- `src/pcache1.c`
- `doc/pager-invariants.txt`
- focused tests in `test/` and `src/test_btree.c`

It intentionally avoids the parser, code generator, VDBE, and platform VFS details except where they directly shape btree or pager behavior.

The target reader is someone building an embedded database in Rust who suspects the main bottlenecks are:

- the btree implementation,
- page cache/pager behavior,
- read/write paths through large rows and overflow pages,
- and the lack of SQLite-style crash-safety and adversarial testing.

## High-Level Architecture

SQLite’s storage stack is layered roughly like this:

1. `Pager`
   Owns the database file, rollback journal or WAL integration, file locks, page cache, savepoints, crash recovery, and page-level durability rules.

2. `PCache`
   Caches `PgHdr` page objects in memory, tracks dirty pages, decides when to spill dirty pages, and provides page pin/unpin semantics.

3. `Btree`
   Understands database page format, cell format, overflow chains, freelist pages, pointer-map pages, cursor navigation, insertion, deletion, balancing, and integrity checking.

The important boundary is:

- pager knows page identity, durability, journaling, locking, cache state;
- btree knows page contents and tree algorithms.

That separation is one of SQLite’s biggest strengths. A Rust reimplementation should preserve it.

## Core Design Ideas Worth Copying

SQLite is fast and robust here because it combines:

- a compact page format with mostly contiguous I/O,
- lazy decoding of cells,
- explicit free-space accounting on each page,
- careful overflow handling,
- a page cache that separates "dirty", "journaled", and "writeable",
- two-phase commit through the pager,
- strong corruption checks at every low-level boundary,
- savepoints implemented at the pager layer,
- and a huge amount of fault-injection and integrity testing.

The system is not "just a btree". It is a btree plus a correctness protocol.

## Part I: The B-Tree

### On-disk page types

SQLite database files are page-oriented. A page may be:

- a btree page,
- a freelist trunk page,
- a freelist leaf page,
- an overflow page,
- or, when auto-vacuum is enabled, a pointer-map page.

Page 1 is special:

- first 100 bytes are the database header,
- the remainder is a normal btree page image.

### Btree page layout

A btree page has:

1. page header,
2. cell pointer array,
3. unallocated gap,
4. cell content area at the end of the page.

Important properties:

- cell pointers are kept in key order,
- cell bodies are not required to be in key order,
- freeblocks inside the cell-content area form a linked list,
- tiny unusable gaps become "fragments",
- total fragment bytes are tracked in the page header,
- defragmentation repacks cells tightly when needed.

This layout is a big reason SQLite is efficient:

- searching mostly touches the cell pointer array and a few cell headers,
- mutation can often reuse fragmented/freeblock space without full page rebuild,
- full defragmentation is available but not the common path.

### Page type flags and tree flavors

SQLite supports multiple logical btree flavors on the same low-level machinery:

- rowid tables: integer key (`PTF_INTKEY`) with row payload in leaves,
- indexes and `WITHOUT ROWID` tables: blob/composite keys, usually no separate payload.

Main flags:

- `PTF_INTKEY`
- `PTF_ZERODATA`
- `PTF_LEAFDATA`
- `PTF_LEAF`

This lets one engine support:

- table interior pages,
- table leaf pages,
- index interior pages,
- index leaf pages.

### Key in-memory structures

#### `BtShared`

Represents the shared state for one database file:

- `Pager *pPager`
- page-size derived limits like `maxLocal`, `minLocal`, `maxLeaf`, `minLeaf`
- current page count
- page 1 handle
- auto-vacuum configuration
- schema pointer
- mutex
- shared-cache lock state

This is the "database-wide btree state".

#### `Btree`

Represents one connection’s handle onto a `BtShared`:

- transaction state,
- shared-cache participation,
- connection pointer,
- per-handle data version adjustment.

This is the "connection-facing" wrapper.

#### `MemPage`

Represents a decoded btree page in memory. It contains:

- flags like `leaf`, `intKey`, `intKeyLeaf`,
- page number,
- header offset,
- child pointer size,
- cell offset,
- free space count,
- number of cells,
- `aData` pointer to raw page bytes,
- `xCellSize` and `xParseCell` function pointers for page-type-specific cell decoding,
- temporary overflow-cell arrays for pages that became overfull during mutation.

This is a key performance choice: SQLite caches page-derived metadata in `MemPage` instead of repeatedly re-decoding raw bytes.

#### `BtCursor`

A cursor stores:

- current state (`VALID`, `INVALID`, `SKIPNEXT`, `REQUIRESEEK`, `FAULT`),
- a stack of ancestor pages,
- current page and cell index,
- cached parsed cell info,
- cached overflow-page list,
- saved key/rowid for restoration.

SQLite’s cursor is stateful and heavily optimized for repeated nearby operations.

#### `CellInfo`

Decoded cell summary:

- key or payload size,
- payload pointer,
- total payload length,
- locally stored payload length,
- total on-page cell size.

This is how SQLite avoids fully decoding record payloads during navigation.

### Cell formats

Cells vary by page type, but conceptually contain:

- optional child pointer,
- varints for payload length and/or integer key,
- payload bytes stored locally,
- optional overflow pointer.

Important consequence:

- navigation usually needs only child pointer and key prefix,
- full payload access is deferred,
- large rows pay overflow costs only when actually touched.

### Local payload vs overflow payload

SQLite does not simply store `min(page_free, payload)` bytes locally.

It uses carefully chosen formulas:

- `maxLocal = ((usableSize - 12) * 64 / 255) - 23`
- `minLocal = ((usableSize - 12) * 32 / 255) - 23`
- `maxLeaf = usableSize - 35`
- `minLeaf = ((usableSize - 12) * 32 / 255) - 23`

When payload exceeds local capacity, SQLite chooses a local payload size that:

- stays between `minLocal` and `maxLocal`,
- minimizes wasted space on overflow pages,
- is stable as part of the file format.

The overflow split logic is one of the details a Rust port should copy exactly if it wants SQLite-compatible behavior and good space utilization.

### Overflow chains

If payload does not fit locally:

- the cell stores an overflow page number,
- each overflow page stores a 4-byte next pointer followed by data,
- all intermediate overflow pages are full,
- only the last may be partial.

SQLite optimizes overflow access in two important ways:

1. `BtCursor.aOverflow`
   It lazily caches overflow page numbers so later reads do not have to walk the chain again from the start.

2. direct overflow reads
   When conditions are safe, large read-only overflow payload can be read directly from the database file, bypassing the cache.

For a Rust database that is currently slow on large rows, this is a major lesson:

- do not re-walk overflow chains from page 1 for every access,
- do not eagerly copy entire payloads if the caller only needs slices,
- add a fast path for aligned direct reads when the cache is clean.

### Page initialization and decoding

When a page is first loaded, SQLite initializes `MemPage` from raw bytes:

- validates header fields,
- chooses cell parsing functions based on flags,
- computes free-space metrics,
- records `maxLocal` and `minLocal` for that page type.

This early validation is also a corruption defense. Many malformed page offsets are rejected during page initialization, before deeper logic runs.

### Free-space management on a page

SQLite tracks page space in three forms:

- unallocated gap between cell pointer array and cell content area,
- freeblocks inside cell content area,
- fragments too small for freeblocks.

Important routines:

- `pageFindSlot()`
  searches freeblocks for reusable space,
- `allocateSpace()`
  allocates from freeblocks or gap, possibly after defragmentation,
- `defragmentPage()`
  repacks cells to eliminate freeblocks and fragments.

This is another place where naive Rust implementations often lose badly:

- frequent full-page rewrites,
- repeated allocations and memmoves,
- no fragmentation model,
- no distinction between pointer-array growth and cell-content compaction.

SQLite does the minimum work most of the time, and only rebuilds when necessary.

### Navigation and search

Search is cursor-centric:

- `moveToRoot()`
- binary search within current page
- descend with `moveToChild()`
- `moveToLeftmost()` / `moveToRightmost()`

Optimizations worth copying:

- stay-on-last optimization for append-heavy rowid workloads,
- reuse current cursor position when searching for nearby keys,
- fast next/previous transitions without full reseek,
- save/restore cursor positions across rebalance and rollback.

The cursor state machine is not incidental. It is part of performance.

### Insertion path

High-level insert sequence:

1. position cursor near insertion point,
2. build cell image with `fillInCell()`,
3. if replacing and size matches, overwrite in place,
4. otherwise remove old cell if replacing,
5. insert cell into page,
6. if page overflows, balance upward.

Two details matter a lot:

#### 1. Overflow-cell staging

If a page has enough logical free space but not enough contiguous space, or a parent split temporarily produces too many divider cells, SQLite can stage a few overflow cells in `MemPage.apOvfl[]` instead of immediately forcing a full rebuild.

That lets balancing operate on a logical set of cells before repacking physical page bytes.

#### 2. Cheap overwrite path

If old and new cell sizes match, SQLite often overwrites in place instead of delete + insert.

That avoids:

- free-list surgery,
- page defragmentation,
- and unnecessary rebalance risk.

### Deletion path

High-level delete sequence:

1. ensure cursor is valid,
2. if deleting from an internal page, move to predecessor leaf entry,
3. free overflow pages,
4. drop the cell,
5. if needed, move predecessor cell up,
6. rebalance underfull pages upward,
7. preserve cursor position if requested.

SQLite uses the predecessor from the subtree below the deleted interior cell. That choice simplifies repair because replacement data comes from within the correct subtree.

### Balancing

SQLite has three balancing modes:

#### `balance_quick()`

Special-case optimization for append-at-rightmost-leaf:

- allocate a new right sibling,
- move only the overflow cell there,
- insert one divider into the parent.

This is a huge practical win for append-heavy rowid inserts. A Rust MVP that always does full sibling redistribution will usually benchmark much worse.

#### `balance_nonroot()`

General case for overfull or underfull non-root pages:

- choose target page plus up to two siblings,
- remove divider cells from parent,
- gather cells from siblings and dividers,
- repartition into nearly full pages,
- rebuild pages,
- update parent dividers,
- free or allocate sibling pages if necessary.

This routine is the heart of SQLite’s btree maintenance.

Key traits:

- it balances by bytes, not just by cell count,
- it may change sibling count,
- it handles both overflow and underflow,
- it cooperates with auto-vacuum pointer-map updates,
- it assumes rollback will clean up if anything fails mid-way.

#### `balance_deeper()`

Used when the root itself overflows:

- allocate a new child page,
- copy current root contents into child,
- empty the root,
- make the root an interior page pointing to the child,
- then continue balancing below.

This avoids changing the root page number, which is critical because root page numbers are stable identifiers for tables and indexes.

### Freelist management

SQLite keeps a database-level freelist:

- page 1 stores first trunk page and total free page count,
- freelist trunk pages point to next trunk and a set of leaf free pages.

`allocateBtreePage()`:

- first reuses freelist pages,
- optionally tries to find pages near a target page number,
- otherwise extends the file.

`freePage2()`:

- increments freelist count,
- optionally zeroes content for secure delete,
- links freed page as a leaf or new trunk.

Performance lesson:

- SQLite aggressively reuses pages and can place related pages near each other,
- but correctness of freelist metadata is always maintained under journaling.

### Auto-vacuum and pointer maps

When auto-vacuum is enabled, SQLite adds pointer-map pages. Each non-pointer-map page gets an entry telling SQLite who points to it:

- root page,
- free page,
- first overflow page,
- later overflow page,
- ordinary btree child.

Why this exists:

- auto-vacuum needs to move pages,
- moving a page means updating the parent pointer to its new location,
- pointer maps make parent lookup cheap.

This is a good example of SQLite spending a little space to avoid expensive global searches.

### Integrity checking and corruption defenses

SQLite is extremely defensive in low-level btree code:

- checks page bounds before dereferencing offsets,
- validates cell sizes against page end,
- validates overflow chains,
- validates freelist/trunk layout,
- validates pointer-map consistency,
- rejects impossible tree depths,
- tracks duplicate page references during integrity check,
- uses corruption returns instead of undefined behavior.

`PRAGMA integrity_check` is backed by real structural validation, not a superficial scan.

If your Rust database is intended to be reliable, you should treat corruption detection as a first-class feature, not an afterthought.

## Part II: The Pager

### Pager responsibilities

SQLite’s pager is responsible for:

- opening the database file,
- page cache ownership,
- rollback journal or WAL coordination,
- file locking,
- transaction boundaries,
- savepoints and subjournals,
- crash recovery,
- page-size aware aligned reads and writes,
- change-counter management,
- deciding when dirty pages may be written to the database file.

The pager is the core of SQLite’s durability story.

### Pager invariants

`doc/pager-invariants.txt` and the top of `pager.c` define the rules. The most important ones are:

- database pages are not overwritten unless they are safely rollbackable,
- journal content must match original page content,
- writes are page-aligned,
- database writes are synced before journal finalization,
- rollback restores original logical state,
- the database is well-formed before and after every transaction,
- read requires shared lock,
- write requires exclusive lock on the database file in rollback mode.

These invariants are the contract the entire storage engine is built around.

### Pager state machine

Important states:

- `PAGER_OPEN`
- `PAGER_READER`
- `PAGER_WRITER_LOCKED`
- `PAGER_WRITER_CACHEMOD`
- `PAGER_WRITER_DBMOD`
- `PAGER_WRITER_FINISHED`
- `PAGER_ERROR`

This is not bookkeeping fluff. It encodes what is already safe and what is not.

Examples:

- `WRITER_LOCKED`
  lock acquired, but no page content modified yet.

- `WRITER_CACHEMOD`
  cache changed, journal header written, database file not yet changed.

- `WRITER_DBMOD`
  journal synced, database file may now be written.

- `ERROR`
  cache may be inconsistent; further access must fail until state is discarded.

For Rust, modeling pager state as an enum with explicit transitions will pay off immediately.

### Core pager data structures

#### `Pager`

Tracks:

- state and lock level,
- journal mode,
- sync policy,
- db size at different times (`dbSize`, `dbOrigSize`, `dbFileSize`),
- dirty/journal bookkeeping bitvecs,
- savepoint array,
- change-counter tracking,
- page cache,
- WAL handle if used.

#### `PgHdr`

Represents one cached page:

- raw data pointer,
- extra area,
- page number,
- dirty/writeable/sync-needed flags,
- refcount,
- dirty-list links.

Important flags:

- `PGHDR_DIRTY`
- `PGHDR_WRITEABLE`
- `PGHDR_NEED_SYNC`
- `PGHDR_DONT_WRITE`
- `PGHDR_MMAP`

#### `PCache`

Maintains:

- dirty list in LRU order,
- a `pSynced` pointer to help choose spill candidates,
- cache size and spill thresholds,
- reference counts,
- pluggable backend storage.

### Transaction begin

`sqlite3PagerBegin()`:

- assumes caller already has a read transaction,
- in rollback mode acquires RESERVED or EXCLUSIVE lock,
- in WAL mode begins a WAL write transaction,
- records original sizes,
- moves pager to `WRITER_LOCKED`.

Notice the separation:

- taking locks does not yet imply journaling,
- journaling begins only when a page is first dirtied.

This reduces overhead for transactions that end up doing no writes.

### First write to a page

`sqlite3PagerWrite()` / `pager_write()` is the critical moment.

SQLite does all of this before the caller may mutate bytes:

1. if needed, opens rollback journal and writes first journal header,
2. marks page dirty,
3. if page existed at txn start and is not yet journaled, writes original page image into rollback journal,
4. if page is new past EOF, marks it `NEED_SYNC` when appropriate,
5. marks page `WRITEABLE`,
6. if savepoints are active, records it in subjournal as needed,
7. updates logical db size.

That ordering is extremely important. A Rust port should copy the state transitions, not just the final effects.

### Sector-aware journaling

If sector size exceeds page size, SQLite journals all pages sharing the sector before allowing any of them to be written.

Why:

- hardware may tear writes at sector granularity, not page granularity.

SQLite handles this in `pagerWriteLargeSector()` and uses `PGHDR_NEED_SYNC` plus `SPILLFLAG_NOSYNC` to stop unsafe intermediate sync behavior.

This is easy to omit in a reimplementation and easy to regret later.

### Rollback journal format and syncing

Journal contains:

- header with magic, page size, original db size, checksum seed, etc.,
- page records of `(pgno, page bytes, checksum)`,
- optional super-journal marker.

`syncJournal()` ensures journal durability before database-file writes:

- may update journal header record count,
- syncs journal unless no-sync or in-memory journal,
- clears `PGHDR_NEED_SYNC`,
- transitions pager to `WRITER_DBMOD`.

SQLite also contains logic for device capabilities like:

- safe append,
- sequential writes,
- atomic writes,
- batch atomic writes.

A Rust MVP can start simpler, but the ordering rules must stay intact.

### Commit path

SQLite commit is explicitly split:

#### Phase 1

`sqlite3PagerCommitPhaseOne()` / `sqlite3BtreeCommitPhaseOne()`

Does the real work:

- auto-vacuum relocation if needed,
- change-counter update,
- optional super-journal pointer write,
- journal sync,
- dirty page writeback,
- truncate/grow db file if needed,
- database file sync.

At this point, the transaction is durable but not yet logically committed, because the journal still exists.

#### Phase 2

`sqlite3PagerCommitPhaseTwo()` / `sqlite3BtreeCommitPhaseTwo()`

Finalizes journal:

- delete,
- truncate,
- zero header,
- or close in-memory journal.

That is the point of no return.

This is a very good design for Rust too:

- it clarifies recovery semantics,
- it simplifies testing,
- and it makes failure injection much more precise.

### Rollback path

`sqlite3PagerRollback()` handles:

- explicit rollback of current transaction,
- or recovery from write failures,
- or recovery after hot-journal detection on open.

Rollback journal playback:

- truncates database back to original size,
- restores page images from journal,
- handles partial journals safely,
- finalizes journal only after successful rollback.

If rollback itself fails, SQLite moves pager to `PAGER_ERROR` so no one trusts the cache.

That error-state idea is extremely valuable. Rust’s type safety does not eliminate the need for a poisoned state after partial I/O failure.

### Savepoints and subjournals

Savepoints live largely in the pager.

For each savepoint SQLite tracks:

- original database size,
- main-journal offset,
- subjournal start record,
- bitvec of pages touched in that savepoint,
- WAL savepoint data if in WAL mode.

Rollback to savepoint:

- restore affected main-journal records,
- then subjournal records,
- ensure each page is restored once using a temporary bitvec,
- restore db size to savepoint size.

This design is subtle and worth copying. Savepoints are not implemented as nested full transactions. They are page-image restoration scopes.

### Page cache and spilling

`PCache` keeps dirty pages in LRU order and spills them through `pagerStress()` when memory pressure requires it.

Spill behavior is careful:

- disallowed in some rollback-sensitive states,
- may require journal sync first,
- writes a page cleanly back to DB or WAL,
- turns page clean only after successful write.

The important lesson is that eviction is part of correctness, not just memory management.

### WAL notes

This document focuses on rollback-journal mode because that is where the pager invariants and most of the page overwrite rules live.

Still, a Rust implementation should note:

- pager supports both rollback and WAL,
- WAL mode never enters `WRITER_DBMOD`/`WRITER_FINISHED`,
- dirty pages are written as WAL frames rather than directly to the database,
- savepoint rollback in WAL uses WAL savepoint metadata.

If your Rust MVP is struggling, implement rollback-journal correctly first, then add WAL later.

### Change counter and cache invalidation

SQLite updates bytes 24..39 of page 1 to signal file changes.

Other connections compare this to decide whether to invalidate caches.

This small mechanism is part of making multi-connection correctness cheap.

## What the Tests Reveal About SQLite’s Priorities

SQLite’s tests around btree and pager are not just unit tests. They are behavior and failure-model tests.

### Btree-focused tests

`test/btree01.test`

- regression tests for difficult balancing cases,
- especially large payload updates and page rebalance behavior,
- includes fuzz-found cursor/overflow-cache regression.

`test/btree02.test`

- exercises repeated save/restore cursor position behavior,
- especially around `CURSOR_SKIPNEXT`.

`test/btreefault.test`

- injects OOM/faults into btree-related operations,
- validates integrity after failure and recovery.

`test/corruptD.test`, `test/corruptG.test`

- deliberately corrupt page offsets, cell headers, and payload descriptors,
- check that SQLite reports corruption instead of reading out of bounds.

### Pager-focused tests

`test/pager1.test`

- locking semantics across multiple clients,
- savepoints,
- hot journals,
- backup interactions,
- multiple journal modes,
- invalid page access behavior.

`test/pager2.test`

- savepoint-heavy sequences across many sector sizes and modes,
- rollback with `journal_mode=off`,
- shared in-memory database cases.

`test/pager3.test`

- journal file existence behavior around exclusive locking.

`test/pager4.test`

- renamed/unlinked database edge cases yielding `READONLY_DBMOVED`.

`test/pagerfault.test`, `test/pagerfault2.test`

- hot-journal rollback under injected faults,
- page-size changes,
- multi-file transactions,
- persistent/truncate journal failure handling,
- slow-path OOM scenarios.

`test/pageropt.test`

- performance-sensitive pager behaviors,
- avoiding unnecessary reads of last overflow page on delete,
- not reading freelist page contents unnecessarily,
- cache reuse and invalidation behavior.

`test/malloc3.test`

- system-wide malloc failure handling,
- especially tricky btree balance and pager rollback/error-state interactions.

`test/avtrans.test`, `test/pagesize.test`

- auto-vacuum stress,
- page-size variation,
- rollback correctness across large payloads and VACUUM paths.

### Quality assurance pattern to copy

SQLite’s storage layer QA combines:

- regression tests for known bugs,
- integrity-check assertions after many operations,
- corruption-oriented tests,
- OOM fault injection,
- I/O fault injection,
- concurrency/locking tests,
- mode-matrix testing across page sizes, sector sizes, journal modes, and vacuum modes,
- and performance regression tests at the storage boundary.

That test philosophy matters as much as the algorithms.

## Rust Reimplementation Guidance

### 1. Preserve the layering

Use separate modules for:

- `pager`
- `pcache`
- `btree`
- `page_format`
- `journal`
- `wal` later
- `integrity_check`

Do not merge page-format knowledge into the pager.

### 2. Use a stable page abstraction

Suggested split:

- `PageId(u32)`
- `CachedPage { id, data: Box<[u8]>, extra, flags, refcount }`
- `MemPageView` or `BtreePage<'a>` for decoded btree metadata

Important: do not repeatedly allocate decoded structures for hot paths. Cache page-derived metadata similarly to `MemPage`.

### 3. Separate raw bytes from decoded metadata

SQLite’s pattern is excellent:

- raw bytes live in the page cache,
- derived fields are cached alongside,
- parsing is lazy and page-type-specific.

In Rust, prefer:

- raw `&[u8]` / `&mut [u8]` for storage,
- small decoded structs for cell/page metadata,
- no serde, no generic object trees on the hot path.

### 4. Reproduce the local/overflow payload formulas exactly

Do not invent a simpler split rule.

If you do, you will likely get:

- worse space usage,
- more overflow pages,
- different balancing pressure,
- and incompatibility with SQLite-style page economics.

### 5. Make cursor locality a first-class optimization

A slow Rust btree often suffers from:

- re-searching from root too often,
- decoding whole records during search,
- losing sibling/ancestor context between operations.

Copy SQLite’s approach:

- keep cursor stacks,
- cache current parsed cell info,
- support `next`/`prev` without reseek,
- support cursor save/restore after mutation.

### 6. Optimize append-heavy insertions

Implement a right-edge fast path like `balance_quick()`.

Without it, append workloads pay full sibling redistribution far too often.

### 7. Avoid per-operation allocations in hot paths

SQLite uses:

- page-local temp space,
- cursor overflow caches,
- bounded overflow-cell staging arrays,
- stack/scratch buffers during balancing.

In Rust:

- reuse scratch buffers in the btree context,
- avoid `Vec` growth inside every insert/delete,
- pre-size temporary cell arrays during balancing,
- use small fixed-capacity arrays where SQLite uses bounded counts.

### 8. Model pager page states explicitly

Each cached page should track at least:

- clean vs dirty,
- journaled vs not,
- writeable vs not,
- needs-journal-sync vs not,
- dont-write optimization.

Do not collapse these into one boolean like `dirty`.

SQLite’s behavior depends on the distinction.

### 9. Add a poisoned/error state to the pager

After partial failure during rollback or spill, the cache may be untrustworthy.

Rust safety will not save you from logical inconsistency.

Have a pager state like:

- `Open`
- `Reader`
- `WriterLocked`
- `WriterCacheMod`
- `WriterDbMod`
- `WriterFinished`
- `Error(PagerError)`

Once poisoned, reject reads/writes until the cache is discarded and reopened.

### 10. Start with rollback journal before WAL

Suggested implementation order:

1. page cache,
2. rollback-journal pager,
3. btree page format + cursors + search,
4. insert/delete + rebalance,
5. freelist,
6. savepoints/subjournal,
7. auto-vacuum pointer maps,
8. integrity checker,
9. WAL.

WAL is valuable but should not be the first rescue strategy for a slow MVP.

### 11. Be deliberate about byte-level APIs

SQLite uses:

- big-endian fixed integers,
- custom varint parsing,
- pointer arithmetic with strict bounds checks.

In Rust:

- write small hand-optimized helpers for `get_u16_be`, `put_u16_be`, `get_u32_be`, varints,
- keep them inline,
- expose checked and unchecked/internal variants if profiling justifies it,
- prefer slices plus explicit indices over generic readers.

### 12. Make integrity checking a built-in feature

Implement a full storage checker that validates:

- page reachability,
- duplicate page references,
- freelist correctness,
- overflow chain correctness,
- btree key order,
- cell coverage/no overlap,
- pointer-map correctness if auto-vacuum exists,
- page-count/header consistency.

Run it constantly in tests.

### 13. Mirror SQLite’s fault model in tests

You should add:

- deterministic allocation-failure injection,
- deterministic I/O failure injection,
- crash simulation between journal sync and db write,
- crash simulation before and after journal finalization,
- corruption corpus tests for page headers and cells,
- long random insert/update/delete sequences with periodic integrity checks.

If you only benchmark and do correctness tests on happy paths, you will not catch the real storage bugs.

### 14. Likely reasons the Rust MVP is slow

Based on what SQLite does well, common causes are:

- full-page decode/encode on every access,
- allocations during every cell parse or rebalance,
- reseeking from root too often,
- naive overflow reads that walk the chain repeatedly,
- page splits that rebuild too much,
- lack of right-edge insert optimization,
- copying payload eagerly instead of slicing,
- page cache without dirty/journal/writeable distinctions,
- no cheap overwrite-in-place path,
- freelist reuse that still reads old content unnecessarily,
- poor cache spill policy.

### 15. Practical performance tactics for Rust

- Store raw page bytes in fixed-size buffers and mutate in place.
- Cache decoded page header fields.
- Cache current cell parse in the cursor.
- Cache overflow page numbers lazily per cursor.
- Keep comparator work on index keys minimal; avoid materializing full keys when prefix comparison is enough.
- Use specialized fast paths for rowid tables.
- Make balancing scratch space reusable.
- Distinguish logical free space from contiguous free space.
- Avoid copying overflow payload unless caller really needs owned bytes.
- Benchmark append-heavy, point-lookup, range-scan, large-row, and delete-heavy workloads separately.

## What Can Be Simplified, and What Should Not

### Safe to simplify early

- shared-cache table locking,
- WAL,
- exotic VFS capabilities,
- mmap/direct-read fast paths,
- some auto-vacuum variants.

### Do not simplify if you want SQLite-like robustness

- journaling order rules,
- page-state distinctions,
- overflow split formulas,
- rebalance correctness,
- cursor save/restore semantics,
- corruption detection,
- fault-injection tests.

## Recommended Rust Module Shape

One workable shape:

- `pager/mod.rs`
- `pager/journal.rs`
- `pager/savepoint.rs`
- `pager/cache.rs`
- `btree/mod.rs`
- `btree/page.rs`
- `btree/cell.rs`
- `btree/cursor.rs`
- `btree/balance.rs`
- `btree/freelist.rs`
- `btree/autovacuum.rs`
- `format/varint.rs`
- `format/header.rs`
- `integrity/mod.rs`
- `tests/fault_injection.rs`
- `tests/corruption.rs`
- `tests/storage_randomized.rs`

## Bottom Line

SQLite’s btree and pager are fast because they are:

- page-oriented,
- mutation-aware,
- cursor-locality-aware,
- byte-precise,
- and built around strict crash-recovery invariants.

They are reliable because every delicate boundary is defended by:

- explicit state machines,
- journaling rules,
- pointer-map maintenance,
- integrity checks,
- and aggressive fault/corruption testing.

If you want SQLite-like behavior in Rust, the right target is not "rewrite a btree" but:

- build a pager that enforces recovery invariants,
- build a btree that manipulates page images with minimal copying,
- and build a test harness that assumes storage code is guilty until proven durable.
