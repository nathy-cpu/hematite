# Architecture

Hematite is a layered, embeddable SQL database engine. It follows a strict unidirectional dependency model, where higher-level SQL abstractions are progressively lowered into lower-level storage primitives.

```text
sql -> parser -> query -> catalog -> btree -> storage
```

## Core Layers

### 1. `storage` (The Foundation)

The storage layer is responsible for translating the concept of "pages" into physical disk bytes. It manages the lifecycle of the database file and its associated journals.

- **The Pager**: Manages an in-memory page cache. It uses a least-recently-used (LRU) eviction policy and ensures that "dirty" pages are only written to disk after the appropriate journal entries have been flushed.
- **Journaling (Atomicity & Durability)**:
    - **Rollback Journal**: The default mode. Before a page is modified, its original content is written to a journal file. In the event of a crash or rollback, these "undo" records are replayed to restore the database.
    - **Write-Ahead Log (WAL)**: An optimized mode where changes are appended to a separate log. This allows for concurrent readers and a single writer, as readers can access the stable database file while the writer appends to the log.
- **Page Allocation**: Maintains a "freelist" of pages that have been deleted and can be reused, preventing fragmentation and uncontrolled file growth.

### 2. `btree` (The Indexing Engine)

Everything in Hematite is stored in B-Trees. Tables are "clustered" (the data lives in the primary key tree), and secondary indexes are separate trees.

- **B+ Tree Structure**: Internal nodes store only keys (for routing), while leaf nodes store both keys and values (the actual row data). This ensures efficient range scans.
- **Byte Trees vs. Typed Trees**: The engine handles two variants:
    - **Byte Trees**: Store arbitrary `Vec<u8>` keys and values, used for low-level storage.
    - **Typed Trees**: Overlay semantic meaning (e.g., "this byte range is a 64-bit integer") onto the byte trees.
- **Overflow Pages**: When a key or value exceeds the size of a single page, the B-Tree automatically spills it into a chain of overflow pages.

### 3. `catalog` (The Relational Model)

This layer translates the generic B-Trees into structured tables and columns.

- **Schema Registry**: Manages the persistence of `CREATE TABLE`, `CREATE INDEX`, and `CREATE VIEW` metadata. This metadata is itself stored in a special "master" table tree.
- **Logical Row Encoding**: Hematite uses a compact binary format for rows. It handles null optimization and varint-encoded headers to minimize the storage footprint of each record.
- **Runtime Type System**: Defines the behavior of `INT`, `DECIMAL`, `TEXT`, `BLOB`, `DATE`, and `INTERVAL` values, including their serialization rules.

### 4. `query` (The Brain)

The query layer is responsible for the "meaning" of SQL.

- **Lowering**: Translates parser-owned syntax (like a literal string '2023-01-01') into a runtime `Value` (like a `DateValue`).
- **Validation**: Performs semantic checks against the schema (e.g., "Does this column exist?", "Are these types comparable?").
- **Cost-Based Optimizer**: Although currently simplified, the planner chooses between full table scans and index-based lookups based on available metadata.
- **The Executor**: An iterator-based execution engine. Operators like `Filter`, `Project`, `Join`, and `Sort` are chained together, pulling rows from the B-tree cursors one at a time.

### 5. `parser` & `sql` (The Interface)

- **Strict Parsing**: The parser is hand-rolled for maximum clarity. It requires uppercase keywords and follows a predictable recursive-descent pattern.
- **Connection Facade**: Coordinates the overall state. It owns the `Catalog` instance and manages the lifecycle of `Transaction` objects.

---

## Concurrency & Safety

Hematite uses a **Single-Writer, Multiple-Reader (SWMR)** model when in WAL mode.

- **Catalog Locking**: Access to the database metadata and B-trees is protected by an `Arc<Mutex<Catalog>>`. This ensures that only one thread can modify the database structure at a time.
- **Memory Safety**: Being written in Rust, Hematite naturally avoids common pitfalls like dangling pointers or buffer overflows in the pager and B-tree implementations.
- **Transaction Isolation**: Supports `SERIALIZABLE` isolation within a single session, and `READ COMMITTED` or better across multiple connections (depending on the journal mode).

## Data Flow: The Journey of a Query

1. **Entry**: SQL text is passed to `Connection::execute`.
2. **Lex & Parse**: The `parser` generates an AST.
3. **Normalize**: The `query` layer expands `SELECT *` and resolves view definitions.
4. **Validate**: The AST is checked for semantic correctness against the active `Schema`.
5. **Plan**: The `planner` generates a `PhysicalPlan`.
6. **Execute**: The `executor` steps through the plan, opening B-Tree `Cursor`s and performing arithmetic/comparisons.
7. **Commit**: If a mutation occurred, the `storage` layer flushes pages to disk according to the current journaling protocol.

## Persistence Guarantees (ACID)

Hematite is designed to provide full ACID compliance:

## Design Intent

The project aims to remain:

- embeddable
- understandable
- hackable
- small enough for experimentation

That means preferring straightforward code and clear boundaries over trying to mimic every behavior of a larger database system.
