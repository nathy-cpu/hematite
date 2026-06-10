# Database Engine Glossary

This glossary defines core concepts and terminology used in the Hematite database engine. It is designed to help novices understand the fundamental architecture and design patterns of database engines.

---

### Buffer Pool / Pager
The memory management layer of the database storage engine. Since reading data from or writing data to disk is extremely slow compared to CPU operations, the engine cannot interact with disk files directly for every query. Instead, it reads file data in fixed-size blocks (typically **4096 bytes**, called **Pages**) and stores them in an in-memory cache called the **Buffer Pool**. The **Pager** is the component that coordinates this cache, tracking which pages are loaded, which are modified, and when pages should be flushed to disk.

### Page
The fundamental unit of data storage in a page-oriented database engine. In Hematite, a Page is a fixed-size block of **4096 bytes**. The database file, rollback journal, and write-ahead log are all structured as sequences of these contiguous pages.

### Dirty Page
A page cached in the buffer pool (in memory) that has been modified by a write transaction but has not yet been written back to the physical database file on disk. The pager must track dirty pages to ensure they are synchronized to disk safely before the transaction is marked as committed.

### Eviction / Page Replacement
When the buffer pool runs out of memory slots (frames) for caching pages and a new page needs to be read from disk, the pager must choose an existing page to remove ("evict") from memory. Hematite uses an **LRU (Least Recently Used)** eviction strategy, meaning it evicts the page that has not been accessed for the longest time. If the chosen page is a *dirty page*, it must be flushed to disk (or the journal/WAL) before it can be evicted.

### Volcano Iterator Model
A classic query execution architecture (also known as the **Iterator Model** or **Pipeline Model**). In this model, each operator in the physical query plan (e.g., scan, filter, join, sort, projection) is implemented as an iterator that exposes a simple interface containing a `next()` method. 
* To run the query, the top operator calls `next()`.
* This operator then calls `next()` on its child operator, pulling data tuples up through the execution pipeline one at a time.
* This model is memory-efficient because it streams rows through the operators without materializing the entire intermediate result sets in memory.

### Slotted Page Layout
A physical layout design used to organize data records (cells) inside a single fixed-size page. If a database only stored fixed-size rows, they could be placed in a simple array. However, SQL supports variable-length columns (like `VARCHAR` or `BLOB`).
To handle variable-length rows without wasting space or causing page fragmentation, the slotted page layout splits the page into two regions:
1. **Cell Pointer Array**: Starts at the beginning of the page and grows *forward* (left-to-right). Each pointer is a small 2-byte integer offset indicating where a record starts.
2. **Cell Content Area**: Starts at the end of the page and grows *backward* (right-to-left), storing the actual key and value bytes.
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
A crash-safety mechanism. When a transaction modifies database pages, Hematite writes the *original, unmodified* page images to a separate journal file (`.jrnl`) before writing the new pages to the database file. If the system crashes mid-transaction or the transaction is aborted (`ROLLBACK`), the recovery manager reads the rollback journal and copies the original page images back into the database file, restoring it to a consistent state.

### Write-Ahead Log (WAL / Redo Log)
A modern crash-safety and concurrency mechanism. Instead of modifying pages directly in the database file, all updates are appended sequentially to a separate WAL file (`.wal`). 
* **Durability**: A transaction is committed by syncing its redone frames to the WAL.
* **Concurrency**: Readers can access unmodified pages in the main database file or historical snapshots in the WAL concurrently while a writer is appending new frames to the end of the WAL. This is known as **Single-Writer, Multiple-Reader (SWMR)** concurrency.
* **Checkpointing**: Periodically, the pager copies the latest page frames from the WAL back into the main database file to prevent the WAL from growing indefinitely.
