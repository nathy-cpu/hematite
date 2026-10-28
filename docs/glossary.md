# Database Engine Glossary

This glossary defines core concepts and terminology used in the Hematite database engine. It is designed to help novices understand the fundamental architecture and design patterns of database engines.

---

### Buffer Pool / Pager
The memory management layer of the database storage engine. Since reading data from or writing data to disk is extremely slow compared to CPU operations, the engine cannot interact with disk files directly for every query. Instead, it reads file data in fixed-size blocks (typically **4096 bytes**, called **Pages**) and stores them in an in-memory cache called the **Buffer Pool**. The **Pager** is the component that coordinates this cache, tracking which pages are loaded, which are modified, and when pages should be flushed to disk.

### Page
The fundamental unit of data storage in a page-oriented database engine. In Hematite, a Page is a fixed-size block of **4096 bytes**, and the database file is a sequence of them. The last 4 bytes of every page are a checksum of the rest, so 4092 bytes are usable. The rollback journal and write-ahead log are not sequences of pages: each is a small header followed by records that carry whole page images.

### Dirty Page
A page cached in the buffer pool (in memory) that has been modified by a write transaction but has not yet been written back to the physical database file on disk. The pager must track dirty pages to ensure they are synchronized to disk safely before the transaction is marked as committed.

### Eviction / Page Replacement
When the buffer pool runs out of memory slots (frames) for caching pages and a new page needs to be read from disk, the pager must choose an existing page to remove ("evict") from memory. Hematite uses an **LRU (Least Recently Used)** eviction strategy, meaning it evicts the page that has not been accessed for the longest time. It never evicts a dirty page or one still in use: if every cached page is one of those, the cache grows past its target instead. In rollback mode, a cache full of dirty pages is relieved by *spilling* them — syncing the journal, then writing them to the database file before the commit.

### Volcano Iterator Model vs. Materialization
The **Volcano** (or **Iterator**) model implements each operator in a query plan — scan, filter, join, sort, projection — as an iterator with a `next()` method. The top operator calls `next()` on its child, pulling rows up through the pipeline one at a time, so intermediate results are streamed rather than held in memory.

The alternative is **materialization**: each step runs to completion and hands its whole output to the next. Hematite's executor materializes. A `SELECT` reads its source rows into a `Vec`, filters that into another, sorts, groups, and projects, step by step. It costs memory proportional to the intermediate results, and buys a simple executor with no per-row call chain.

### Slotted Page Layout
A physical layout design used to organize data records (cells) inside a single fixed-size page. If a database only stored fixed-size rows, they could be placed in a simple array. However, SQL supports variable-length columns (like `VARCHAR` or `BLOB`).
To handle variable-length rows without wasting space or causing page fragmentation, the slotted page layout splits the page into two regions:
1. **Cell Pointer Array**: Starts just after the page header and grows *forward* (left-to-right). Each pointer is a small 2-byte integer offset indicating where a record starts.
2. **Cell Content Area**: Starts at the end of the page's usable area (before the 4-byte checksum trailer) and grows *backward* (right-to-left), storing the actual key and value bytes.
3. **Unallocated Gap**: The free space in the middle. When a new row is inserted, its pointer is added to the front array, and its data is written to the end of the content area, shrinking the gap in the middle.

```text
+--------------------------------------------------------+
| Page Header | Pointer 1 | Pointer 2 | -> [GAP] <- | Cell 2 | Cell 1 |
+--------------------------------------------------------+
```

### B+ Tree
A self-balancing search tree data structure optimized for systems reading and writing large blocks of data. 
* **Leaf Nodes**: Store the actual database rows (keys and values).
* **Interior (Internal) Nodes**: Store only separator keys and child page IDs, acting as routing markers to direct lookups to the correct leaf node.
B+ Trees are excellent for databases because they have a high fan-out (each node points to many children), keeping the tree shallow (typically 3–4 levels deep even for millions of rows), which minimizes disk I/O during lookups.

### Rollback Journal (Undo Log)
A crash-safety mechanism. When a transaction modifies database pages, Hematite writes the *original, unmodified* page images to a separate journal file (`<database>.journal`) before writing the new pages to the database file. If the transaction is aborted (`ROLLBACK`), the original images are restored. If the system crashes, the journal is examined when the database is next opened: unless its recorded *commit intents* show that the commit had fully reached the disk, the original page images are copied back, restoring the state before the transaction.

### Write-Ahead Log (WAL / Redo Log)
A modern crash-safety and concurrency mechanism. Instead of modifying pages directly in the database file, all updates are appended sequentially to a separate WAL file (`<database>.wal`).
* **Durability**: A transaction is committed by syncing its redone frames to the WAL.
* **Concurrency**: Readers can access unmodified pages in the main database file or historical snapshots in the WAL concurrently while a writer is appending new frames to the end of the WAL. This is known as **Single-Writer, Multiple-Reader (SWMR)** concurrency.
* **Checkpointing**: Once the WAL holds 1000 frames, the pager copies the latest version of every page in it back into the main database file, syncs that file, and deletes the WAL, so it never grows indefinitely.
