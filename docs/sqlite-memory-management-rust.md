# SQLite Memory Management: Implementation Notes and Rust Reimplementation Guide

## Scope

This document analyzes how SQLite manages memory, minimizes allocation overhead, and maintains bounded memory usage. The key source files are:

- `src/malloc.c` — central allocation wrapper with heap limits and statistics
- `src/mem1.c` — default system `malloc()`-based allocator
- `src/mem2.c` — debug allocator with overrun/underrun detection
- `src/mem3.c` — memsys3 pool allocator (chunk-based)
- `src/mem5.c` — memsys5 buddy allocator (zero-fragmentation guarantee)
- `src/pcache.c` — logical page cache with dirty-list management
- `src/pcache1.c` — default page cache implementation with bulk allocation
- `src/memjournal.c` — in-memory journal with spill-to-disk
- `src/vdbeInt.h` — VDBE register (Mem) and cursor memory layout
- `src/sqliteInt.h` — lookaside allocator structs and global config

The target reader is building a database engine in Rust and wants to reproduce SQLite's memory discipline:

- no hidden `malloc()` calls on hot paths,
- bounded total memory with graceful degradation,
- per-connection allocation pools to avoid contention,
- lazy allocation and deferred computation,
- robust OOM handling that never corrupts data.

## The Main Lesson

SQLite does not just call `malloc()` and hope for the best. It has a **layered memory architecture** where each layer serves a different allocation pattern:

```
┌─────────────────────────────────────────────────┐
│   Application (sqlite3_malloc / sqlite3_free)   │
├─────────────────────────────────────────────────┤
│   sqlite3DbMallocRaw() — per-connection layer   │
│   ┌──────────────┐  ┌─────────────────────┐     │
│   │  Lookaside   │  │  Fallback to heap   │     │
│   │  (128B/1200B │  │  (sqlite3Malloc)    │     │
│   │   fixed-sz)  │  │                     │     │
│   └──────────────┘  └─────────────────────┘     │
├─────────────────────────────────────────────────┤
│   Page Cache (pcache1)                          │
│   ┌──────────┐  ┌──────────┐  ┌───────────┐    │
│   │  Bulk    │  │  Config  │  │  Heap     │    │
│   │  alloc   │  │  pool    │  │  fallback │    │
│   └──────────┘  └──────────┘  └───────────┘    │
├─────────────────────────────────────────────────┤
│   Low-level allocator (sqlite3_mem_methods)     │
│   mem1 (system) │ mem3 (pool) │ mem5 (buddy)   │
└─────────────────────────────────────────────────┘
```

Each layer avoids calling into the layer below when possible. The result: most allocations during query execution never touch the system allocator.

## Part I: The Allocation Wrapper (`malloc.c`)

### `sqlite3Malloc()` vs `sqlite3DbMallocRaw()`

SQLite has **two** primary allocation entry points:

1. **`sqlite3Malloc(n)`** (`malloc.c:296`) — Global allocator. Goes through `mallocWithAlarm()` which checks soft/hard heap limits, updates statistics, and calls the low-level `xMalloc`.

2. **`sqlite3DbMallocRaw(db, n)`** (`malloc.c:636`) — Per-connection allocator. Tries the **lookaside** first, falls back to `sqlite3Malloc()`. This is the one used by the parser, code generator, and expression evaluator.

The separation matters because `sqlite3DbMallocRaw()` can service most small allocations from a pre-allocated pool with **zero mutex contention** and **zero fragmentation**.

### The `mallocWithAlarm()` mechanism (`malloc.c:246`)

Every heap allocation goes through this function when `bMemstat` is enabled:

```c
static void mallocWithAlarm(int n, void **pp){
    nFull = sqlite3GlobalConfig.m.xRoundup(n);  // round to allocator's granularity
    if( mem0.alarmThreshold > 0 ){
        nUsed = sqlite3StatusValue(SQLITE_STATUS_MEMORY_USED);
        if( nUsed >= mem0.alarmThreshold - nFull ){
            AtomicStore(&mem0.nearlyFull, 1);
            sqlite3MallocAlarm(nFull);          // try to release cache pages
            if( mem0.hardLimit ){
                if( nUsed >= mem0.hardLimit - nFull ){
                    *pp = 0;                    // hard limit: deny allocation
                    return;
                }
            }
        }
    }
    p = sqlite3GlobalConfig.m.xMalloc(nFull);
    // SQLITE_ENABLE_MEMORY_MANAGEMENT: retry after releasing cache
    if( p == 0 && mem0.alarmThreshold > 0 ){
        sqlite3MallocAlarm(nFull);
        p = sqlite3GlobalConfig.m.xMalloc(nFull);
    }
}
```

Key behaviors:
- **Soft heap limit** (`sqlite3_soft_heap_limit64`): When approached, triggers `sqlite3_release_memory()` which evicts clean cache pages. The allocation still succeeds.
- **Hard heap limit** (`sqlite3_hard_heap_limit64`): When reached, the allocation **fails** — returns NULL. This is enforced via `PRAGMA hard_heap_limit`.
- **`nearlyFull` flag** (`malloc.c:180`): Checked by the page cache to decide whether to recycle aggressively. Set atomically — no mutex needed for the read path.

### OOM handling philosophy

SQLite uses a "sticky OOM" pattern (`malloc.c:617–631`):

```c
// Once malloc fails, ALL subsequent mallocs on the same connection also fail:
int *a = sqlite3DbMallocRaw(db, 100);
int *b = sqlite3DbMallocRaw(db, 200);
if( b ) a[10] = 9;  // Safe! If b succeeded, a must have too
```

When `sqlite3DbMallocRaw()` fails, it sets `db->mallocFailed = 1`. All subsequent allocations on the same connection immediately return NULL without even trying. This dramatically simplifies error-handling code — you only need to check the last allocation, not every intermediate one.

For Rust: model this as a per-connection `Result`-like sticky error state. Once OOM is hit, all operations return early until the transaction is rolled back.

## Part II: The Lookaside Allocator

### Design rationale

The lookaside is a per-connection pool of fixed-size buffers that services small, transient allocations — primarily AST nodes (`Expr`, `Token`, `SrcList`) during SQL parsing and code generation. The comment at `sqliteInt.h:1546` states it provides **~10% performance improvement** by avoiding `malloc()`/`free()` churn.

### Two-size lookaside (`sqliteInt.h:1575–1618`)

Since 2019, SQLite uses a **two-tier** lookaside with small (128-byte) and large (1200-byte) pools:

```c
struct Lookaside {
    u32 bDisable;           // Disable lookaside when > 0
    u16 sz;                 // Current effective size (0 when disabled)
    u16 szTrue;             // True size, restored when re-enabled
    u8 bMalloced;           // True if memory was sqlite3_malloc'd
    u32 nSlot;              // Total number of large slots
    u32 anStat[3];          // [0]=hits  [1]=size-misses  [2]=full-misses
    LookasideSlot *pInit;   // Large buffers not yet used (for high-water mark)
    LookasideSlot *pFree;   // Large buffers returned after use
    LookasideSlot *pSmallInit; // Small (128B) buffers not yet used
    LookasideSlot *pSmallFree; // Small (128B) buffers returned after use
    void *pMiddle;          // Boundary between large and small zones
    void *pStart;           // First byte of the pool
    void *pEnd;             // First byte past large zone
    void *pTrueEnd;         // First byte past entire pool
};

#define LOOKASIDE_SMALL  128    // Small slot size
// Default: 100 slots of 1200 bytes = 120KB per connection
```

### Allocation fast path (`malloc.c:643–690`)

```c
void *sqlite3DbMallocRawNN(sqlite3 *db, u64 n){
    // Step 1: Too large for lookaside?
    if( n > db->lookaside.sz ){
        if( !db->lookaside.bDisable ) db->lookaside.anStat[1]++;  // size miss
        return dbMallocRawFinish(db, n);  // fall back to heap
    }
    // Step 2: Try small pool first (if n <= 128)
    if( n <= LOOKASIDE_SMALL ){
        if( (pBuf = db->lookaside.pSmallFree) != 0 ){
            db->lookaside.pSmallFree = pBuf->pNext;
            db->lookaside.anStat[0]++;  // hit
            return (void*)pBuf;
        }
        if( (pBuf = db->lookaside.pSmallInit) != 0 ){
            db->lookaside.pSmallInit = pBuf->pNext;
            db->lookaside.anStat[0]++;
            return (void*)pBuf;
        }
    }
    // Step 3: Try large pool
    if( (pBuf = db->lookaside.pFree) != 0 ){
        db->lookaside.pFree = pBuf->pNext;
        db->lookaside.anStat[0]++;
        return (void*)pBuf;
    }
    if( (pBuf = db->lookaside.pInit) != 0 ){
        db->lookaside.pInit = pBuf->pNext;
        db->lookaside.anStat[0]++;
        return (void*)pBuf;
    }
    // Step 4: Pool exhausted
    db->lookaside.anStat[2]++;  // full miss
    return dbMallocRawFinish(db, n);
}
```

### Free fast path (`malloc.c:419–494`)

Free checks if the pointer falls within the lookaside memory range using simple pointer comparison — `O(1)` with no hash lookup:

```c
void sqlite3DbFreeNN(sqlite3 *db, void *p){
    if( (uptr)p < (uptr)db->lookaside.pEnd ){
        if( (uptr)p >= (uptr)db->lookaside.pMiddle ){
            // Return to small free list
            pBuf->pNext = db->lookaside.pSmallFree;
            db->lookaside.pSmallFree = pBuf;
            return;
        }
        if( (uptr)p >= (uptr)db->lookaside.pStart ){
            // Return to large free list
            pBuf->pNext = db->lookaside.pFree;
            db->lookaside.pFree = pBuf;
            return;
        }
    }
    // Not lookaside — use heap free
    sqlite3_free(p);
}
```

### Disable/Enable mechanism

Lookaside is disabled during schema parsing (shared-cache safety) and some internal operations:

```c
#define DisableLookaside  db->lookaside.bDisable++;db->lookaside.sz=0
#define EnableLookaside   db->lookaside.bDisable--;\
   db->lookaside.sz=db->lookaside.bDisable?0:db->lookaside.szTrue
```

Setting `sz=0` causes all allocations to fail the `n > db->lookaside.sz` check without adding a separate branch for `bDisable`.

### Rust takeaway

A per-connection bump/slab allocator with two size classes would replicate this. The key insight is: most SQL parsing allocations are small (< 128 bytes), short-lived, and belong to one connection. A Rust `Arena` or `bumpalo`-style allocator tied to the connection lifetime would capture most of the benefit.

## Part III: Page Cache Memory Management

### Page cache line layout (`pcache1.c:19–31`)

Each cached page is a contiguous allocation with this layout:

```
|------------------------------------------------------|
| database page content  | PgHdr1 | MemPage  | PgHdr  |
|------------------------------------------------------|
  ^                        ^                     ^
  pBuf                     (internal)            pExtra
  szPage bytes             routing info          btree extension
```

Page content is first so buffer overreads (corruption defense) flow harmlessly into the metadata headers rather than into other pages.

### Three sources of page memory (`pcache1.c:56–82`)

1. **General-purpose heap** — `sqlite3Malloc()`. Always available as fallback.
2. **Global page pool** — Pre-allocated via `sqlite3_config(SQLITE_CONFIG_PAGECACHE, pBuf, sz, N)`. Fixed-size slots, mutex-protected free list.
3. **Per-cache bulk allocation** — A single `sqlite3Malloc()` call at cache creation that pre-allocates `nInitPage` pages (default 20, set via `SQLITE_DEFAULT_PCACHE_INITSZ`). ~5% performance improvement for common workloads.

Allocation preference: bulk local → global pool → heap fallback.

### Bulk allocation (`pcache1.c:297–330`)

When a page cache is first used, `pcache1InitBulk()` does one large allocation:

```c
static int pcache1InitBulk(PCache1 *pCache){
    if( pcache1.nInitPage == 0 ) return 0;
    if( pCache->nMax < 3 ) return 0;       // don't bother for tiny caches
    szBulk = pCache->szAlloc * (i64)pcache1.nInitPage;
    if( szBulk > pCache->szAlloc * pCache->nMax ){
        szBulk = pCache->szAlloc * pCache->nMax;  // cap at max cache size
    }
    zBulk = pCache->pBulk = sqlite3Malloc(szBulk);
    // Divide into individual page slots and chain on pFree list
}
```

`szAlloc = szPage + szExtra + ROUND8(sizeof(PgHdr1))` — each slot is the full cache line. For a 4096-byte page, `szAlloc ≈ 4096 + 272 + 48 = ~4416` bytes. Bulk allocation for 20 pages = ~88KB.

### LRU recycling and memory pressure

Unpinned pages form a **circular doubly-linked LRU list** (via `PgHdr1.pLruNext`/`pLruPrev`). When a new page is needed and the cache is full:

1. **`pcache1UnderMemoryPressure()`** (`pcache1.c:518–524`) checks:
   - If using global page pool: is `nFreeSlot < nReserve`?
   - If using heap: is `sqlite3HeapNearlyFull()`? (the soft-limit alarm)
2. **`pcache1EnforceMaxPage()`** (`pcache1.c:619–635`) recycles the LRU unpinned page when `nPurgeable > nMaxPage`.
3. **Bulk memory is freed** when `nPage` drops to zero (all pages evicted).

### Spill strategy: `pSynced` optimization (`pcache.c:25–39`)

When the cache needs to evict a dirty page, it strongly prefers one that does **not** require a journal sync (`PGHDR_NEED_SYNC` is clear). The `pSynced` pointer tracks the best candidate:

```c
// In sqlite3PcacheFetchStress():
for(pPg = pCache->pSynced;
    pPg && (pPg->nRef || (pPg->flags & PGHDR_NEED_SYNC));
    pPg = pPg->pDirtyPrev);
```

Why this matters: syncing the journal is expensive (fsync). Evicting a page that has already been synced avoids an extra fsync. This optimization alone can prevent unnecessary I/O spikes under memory pressure.

### `PGroup` modes (`pcache1.c:136–165`)

The page cache operates in one of two modes:

| Mode | When | Mutex | Memory sharing |
|---|---|---|---|
| **Separate** (mode 1) | Multithreaded, no pre-allocated pool | None | Each PCache is independent |
| **Shared** (mode 2) | `SQLITE_ENABLE_MEMORY_MANAGEMENT`, or single-threaded with global pool | `SQLITE_MUTEX_STATIC_LRU` | All PCaches share one LRU, can steal each other's unpinned pages |

Mode 2 is more memory-efficient (shared pool) but requires a mutex. Mode 1 is faster (no mutex) but uses more total memory across connections.

## Part IV: VDBE Memory — Registers and Cursors

### The `Mem` (sqlite3_value) struct (`vdbeInt.h:232–256`)

Every VDBE register is a `Mem`:

```c
struct sqlite3_value {
    union MemValue {
        double r;              // MEM_Real
        i64 i;                 // MEM_Int
        int nZero;             // MEM_Zero (append zeroes to blob)
        const char *zPType;    // pointer type string
        FuncDef *pDef;         // MEM_Agg context
    } u;
    char *z;                   // String or BLOB data
    int n;                     // Length of z
    u16 flags;                 // Type flags (see below)
    u8 enc;                    // Encoding (UTF-8/16)
    u8 eSubtype;               // Subtype
    sqlite3 *db;               // Associated connection (for lookaside)
    int szMalloc;              // Size of zMalloc buffer
    u32 uTemp;                 // Transient scratch
    char *zMalloc;             // Dynamically allocated storage for z
    void (*xDel)(void*);       // Destructor for MEM_Dyn
};
```

**Storage ownership flags** (`vdbeInt.h:331–334`):

| Flag | Value | Meaning |
|---|---|---|
| `MEM_Dyn` | `0x1000` | `z` points to `malloc`'d memory, must call `xDel()` |
| `MEM_Static` | `0x2000` | `z` points to static/permanent memory |
| `MEM_Ephem` | `0x4000` | `z` points to ephemeral memory (page buffer, may be invalidated) |
| `MEM_Agg` | `0x8000` | `z` points to aggregate function context |

**Key memory optimization**: `Mem.zMalloc` is a **separate** buffer from `Mem.z`. When a register holds a value from a btree page (`MEM_Ephem`), `z` points directly into the page buffer — no copy. Only when the value must outlive the page (e.g., crossing a yield boundary) does SQLite copy into `zMalloc`.

### Lazy deserialization: `aRow` in `VdbeCursor` (`vdbeInt.h:128–130`)

```c
const u8 *aRow;         // Data for the current row, if all on one page
u32 payloadSize;        // Total number of bytes in the record
u32 szRow;              // Bytes available in aRow
```

When a record fits entirely on a single page, `aRow` points directly to the page buffer. `OP_Column` can then extract fields by decoding in-place — **zero copies**. Only when the record overflows to multiple pages does SQLite allocate a buffer and assemble the full record.

### Column cache (`VdbeCursor.cacheStatus`)

Column parsing results (header offsets, types) are cached per-cursor and invalidated by incrementing `Vdbe.cacheCtr`. This avoids re-parsing the record header when multiple `OP_Column` opcodes access the same row.

### VDBE register allocation

Registers are pre-allocated as a flat `Mem[]` array of size `nMem`:

```c
// In Vdbe struct:
Mem *aMem;       // The memory locations (registers)
int nMem;        // Number allocated
```

The compiler knows at prepare-time exactly how many registers are needed. No dynamic allocation during execution. Sub-programs (`OP_Program`) use `VdbeFrame` which captures the parent's register array and allocates a new one inline:

```c
#define VdbeFrameMem(p) ((Mem *)&((u8 *)p)[ROUND8(sizeof(VdbeFrame))])
```

## Part V: In-Memory Journals (`memjournal.c`)

### Spill-to-disk pattern

Statement journals start in memory as a linked list of `FileChunk` nodes:

```c
struct FileChunk {
    FileChunk *pNext;       // Next chunk in chain
    u8 zChunk[8];           // Content (actual size = nChunkSize)
};
#define MEMJOURNAL_DFLT_FILECHUNKSIZE 1024  // default chunk size
```

The journal behavior depends on `nSpill`:

| `nSpill` value | Behavior |
|---|---|
| `0` | Always on disk (use VFS directly) |
| `> 0` | In-memory until size exceeds `nSpill`, then flush to disk |
| `< 0` | Always in-memory (`:memory:` databases, `journal_mode=MEMORY`) |

Configured via `SQLITE_DEFAULT_JOURNAL_SIZE_LIMIT` and `sqlite3GlobalConfig.nStmtSpill`.

**Why this exists**: Most statement journals are small (a few KB). Creating a temporary file for each is expensive. By buffering in memory and only spilling to disk when the journal exceeds the threshold, SQLite avoids thousands of unnecessary file I/O operations.

## Part VI: Memsys5 — The Zero-Fragmentation Buddy Allocator (`mem5.c`)

### When to use memsys5

Memsys5 is designed for embedded systems with a **fixed memory budget** and no system `malloc()`. It provides a **mathematical guarantee** against fragmentation failure (Robson's theorem):

```
N >= M*(1 + log2(n)/2) - n + 1
```

Where N = total pool size, M = peak usage, n = ratio of largest to smallest allocation.

### Data structures (`mem5.c:92–132`)

```c
static struct Mem5Global {
    int szAtom;               // Smallest allocation (power of 2, >= 8)
    int nBlock;               // Number of szAtom-sized blocks
    u8 *zPool;                // The memory pool
    sqlite3_mutex *mutex;
    int aiFreelist[LOGMAX+1]; // Free lists for each power-of-2 size class
    u8 *aCtrl;                // One byte per block: CTRL_LOGSIZE | CTRL_FREE
} mem5;

#define LOGMAX 30             // Maximum: 2^30 * szAtom bytes
#define CTRL_LOGSIZE  0x1f    // Low 5 bits: log2(block_count)
#define CTRL_FREE     0x20    // Bit 5: free flag
```

### Allocation algorithm (`mem5.c:220–285`)

Classic buddy allocation:

1. Round `nByte` up to next power-of-2 multiple of `szAtom`
2. Walk `aiFreelist[]` upward from the target size until a free block is found
3. **Split**: repeatedly halve the found block, linking the unused halves to smaller free lists, until the block matches the requested size class
4. Return the block and mark it as allocated in `aCtrl[]`

### Deallocation with coalescing (`mem5.c:290–351`)

1. Mark block as free
2. Find buddy block (XOR the index bit at the current level)
3. If buddy is free and same size class: **coalesce** — merge into parent block
4. Repeat upward until buddy is not free or max size reached

### Why Rust ports should care

If your Rust database targets embedded systems or wants deterministic memory behavior, implementing a buddy allocator in Rust gives you:
- **Bounded memory**: total usage is fixed at initialization
- **No fragmentation failure**: the Robson bound guarantees success
- **Zero external dependencies**: no system allocator needed

## Part VII: Global Configuration and Memory Limits

### `Sqlite3Config` memory-related fields (`sqliteInt.h:4280–4306`)

```c
struct Sqlite3Config {
    // Allocator interfaces
    sqlite3_mem_methods m;           // Low-level allocator (mem1/mem3/mem5)
    sqlite3_pcache_methods2 pcache2; // Page cache allocator

    // Heap configuration (for memsys3/memsys5)
    void *pHeap;                     // Pre-allocated heap buffer
    int nHeap;                       // Size of pHeap
    int mnReq, mxReq;               // Min and max request sizes

    // Page cache configuration
    void *pPage;                     // Pre-allocated page cache buffer
    int szPage;                      // Size of each page slot
    int nPage;                       // Number of page slots

    // Lookaside defaults
    int szLookaside;                 // Default lookaside buffer size (1200)
    int nLookaside;                  // Default lookaside buffer count (100)

    // Memory management
    int nStmtSpill;                  // Statement journal spill threshold
    u8 bSmallMalloc;                 // Avoid large allocations if true

    // Memory-mapped I/O
    sqlite3_int64 szMmap;            // mmap space per file
    sqlite3_int64 mxMmap;            // Maximum mmap size
};
```

### `sqlite3_status()` memory counters

| Counter | Meaning |
|---|---|
| `SQLITE_STATUS_MEMORY_USED` | Current heap bytes in use |
| `SQLITE_STATUS_MALLOC_COUNT` | Current number of outstanding allocations |
| `SQLITE_STATUS_MALLOC_SIZE` | Largest single allocation ever |
| `SQLITE_STATUS_PAGECACHE_USED` | Pages from `SQLITE_CONFIG_PAGECACHE` pool |
| `SQLITE_STATUS_PAGECACHE_OVERFLOW` | Bytes spilled from page cache to heap |
| `SQLITE_STATUS_PAGECACHE_SIZE` | Largest page cache allocation |

These counters are tracked with minimal overhead using atomic operations and are essential for tuning memory budgets.

## Part VIII: Memory Minimization Patterns

### Pattern 1: Deferred allocation

SQLite allocates lazily:
- **Page cache**: bulk allocation happens on first page fetch, not at `sqlite3_open()`
- **Overflow arrays**: `BtCursor.aOverflow` is allocated only when an overflow chain is actually traversed
- **Journal files**: statement journals start in-memory, spill to disk only if needed
- **MemPage.nFree**: set to `-1` (unknown) initially, computed only when needed

### Pattern 2: Pointer reuse instead of copying

- `Mem.z` with `MEM_Ephem` points directly into page buffers — no copy until needed
- `VdbeCursor.aRow` reads directly from a pinned page
- `OP_Column` with `MEM_Ephem` returns a pointer into the record, not a copy
- `cellSizePtr()` returns cell size by reading raw page bytes, not by copying into a struct

### Pattern 3: Size-class specialization

- Lookaside: two fixed sizes (128B and 1200B) cover >90% of parser allocations
- Page cache: all pages are the same size — perfect for pool allocation
- `pTmpSpace` in `BtShared`: one scratch buffer sized for one maximum cell, reused for every insert/balance

### Pattern 4: Compile-time sizing

- VDBE register count is known at prepare time — one flat array, no reallocation
- `VdbeCursor` size is computed with `SZ_VDBECURSOR(N)` — includes space for `aType[N]` and `aOffset[N+1]` inline
- `BtCursor` ancestor stack is fixed at `BTCURSOR_MAX_DEPTH = 20` — no dynamic stack

### Pattern 5: Memory pressure back-propagation

When heap is near the soft limit:
1. `sqlite3HeapNearlyFull()` returns true
2. Page cache avoids allocating new pages (`pcache1UnderMemoryPressure`)
3. Instead, LRU clean pages are recycled
4. If still over limit, `sqlite3_release_memory()` is called which aggressively evicts cache pages
5. Hard limit: allocation simply fails, triggering sticky OOM

### Pattern 6: `bSmallMalloc` mode

When `sqlite3GlobalConfig.bSmallMalloc` is true, SQLite avoids large temporary allocations throughout the codebase. This is useful for extremely memory-constrained environments where a single large allocation might fail even though total memory usage is acceptable.

## Summary: Rust Reimplementation Priorities

| Priority | What to implement | Why |
|---|---|---|
| **1. Per-connection arena** | Replace lookaside with a Rust arena allocator (e.g., `bumpalo`) for parser/codegen allocations | ~10% speedup, eliminates malloc contention |
| **2. Typed page pool** | Pre-allocate page cache as a flat array of fixed-size slots | Eliminates fragmentation, enables O(1) alloc/free |
| **3. Lazy page content access** | Return pointers into pinned pages instead of copying | Eliminates the biggest memory waste in naive implementations |
| **4. Sticky OOM** | Model OOM as a per-connection error flag that blocks all subsequent allocations | Dramatically simplifies error handling |
| **5. In-memory statement journals** | Buffer small journals in memory with configurable spill threshold | Avoids temporary file creation for most statements |
| **6. Compile-time register sizing** | Pre-allocate all VDBE registers as a `Vec<Value>` at prepare time | No dynamic allocation during execution |
| **7. Memory pressure signaling** | Implement soft/hard heap limits with cache eviction callbacks | Bounded memory usage without crashes |
