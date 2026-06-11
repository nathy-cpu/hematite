# Hematite Storage Format Specification v1

This document defines the physical on-disk format for Hematite v1 database files, rollback journals, and write-ahead logs (WAL).

Hematite uses a page-based storage engine with a fixed page size of **4096 bytes** (`PAGE_SIZE`), utilizing slotted-page allocations and clustered B+ Trees.

---

## 1. Database File Layout

A database file consists of contiguous, fixed-size pages. Unlike SQLite, which overlays the database header on Page 1 along with the schema tree root, Hematite enforces a strict separation between database headers, transaction metadata, and allocatable data B-trees.

| Page 0<br>`DB_HEADER_PAGE_ID` | Page 1<br>`STORAGE_METADATA_PAGE_ID` | Page 2+<br>`FIRST_ALLOCATABLE_PAGE_ID` |
| --- | --- | --- |
| **Database Header** | **Storage Metadata** | **Allocatable Trees** |

### Page 0: Database Header Layout

The first 100 bytes of Page 0 are reserved (`DATABASE_HEADER_SIZE = 100`). The actual database header occupies **20 bytes** at the start; the remaining 80 bytes are zero padding reserved for future use. This 100-byte reservation also serves as the B-tree header offset — when Page 0 is also the schema root, the B-tree page header begins at offset 100.

All scalar integers in the database header are stored in **little-endian** byte order.

| Offset | Size (Bytes) | Type | Field | Description / Value |
| --- | --- | --- | --- | --- |
| `0` | `4` | `[u8; 4]` | Magic | ASCII `HMTD`. Identifies the file as a Hematite database. |
| `4` | `4` | `u32` | Version | Database format version. Current value: `1`. |
| `8` | `4` | `u32` | Schema Root Page | Root page ID of the schema catalog B-Tree. |
| `12` | `4` | `u32` | Next Table ID | Incremental counter to allocate unique table IDs. |
| `16` | `4` | `u32` | Header Checksum | Checksum (`DefaultHasher`) computed over bytes `0..16`. |
| `20` | `80` | `[u8; 80]` | Reserved | Zero padding. Reserved for future header fields. |

> **Note**: Page size is not stored in the header — it is a compile-time constant (`PAGE_SIZE = 4096`).

### Page 1: Storage Metadata Page

Page 1 is a dedicated metadata container starting with the container magic `HMD1` (`METADATA_PAGE_MAGIC`). It stores two variable-length sections:

1. **Pager Metadata** (magic `HPM1`): Persists journal mode, free page list, and per-page checksums.
2. **Catalog Metadata**: Persists runtime table metadata (row counts, next rowid, root page IDs).

---

## 2. B-Tree Page Format

Hematite uses B+ Trees for storage. Tables use B-Trees with `rowid`-derived keys and row payload values. Indexes use B-Trees with composite key columns.

> [!NOTE]
> **Why Slotted Pages?** 
> When database rows contain variable-length columns (e.g., text or blobs), a simple array of rows makes insertion, deletion, and updates extremely expensive. Deleting a row would require shifting all subsequent rows to fill the gap, and inserting a larger row would require rewriting the entire page.
>
> A **Slotted Page Layout** solves this by separating pointers from the actual data. Pointers grow forward from the start of the page, while the data cells grow backward from the end of the page. This leaves a flexible "unallocated gap" in the middle. If a row is deleted, we mark its space as a freeblock and remove its pointer, avoiding expensive shifts.

All B-Tree pages are structured using a **Slotted Page Layout**:

| Component | Description |
| :--- | :--- |
| **Page Header** | 8 bytes for Leaf, 12 bytes for Interior |
| **Cell Pointer Array** | 2 bytes per cell, sorted by key (grows forward `====>`) |
| **UNALLOCATED GAP** | Free space between pointer array and content |
| **Cell Content Area** | Holds cells and freeblocks (grows backward `<====`) |

### B-Tree Page Headers

The page type identifier is the first byte:

* `0x02`: Interior Index Page
* `0x05`: Interior Table Page
* `0x0A`: Leaf Index Page
* `0x0D`: Leaf Table Page

#### Leaf Page Header Layout (`0x0A` or `0x0D`)

* **Offset 0 (1 byte)**: Page Type
* **Offset 1 (2 bytes)**: First Freeblock Offset (`0` if none)
* **Offset 3 (2 bytes)**: Cell Count
* **Offset 5 (2 bytes)**: Start of Cell Content Area (offset from page start)
* **Offset 7 (1 byte)**: Fragmented Free Byte Count

#### Interior Page Header Layout (`0x02` or `0x05`)

* **Offset 0 (1 byte)**: Page Type
* **Offset 1 (2 bytes)**: First Freeblock Offset
* **Offset 3 (2 bytes)**: Cell Count
* **Offset 5 (2 bytes)**: Start of Cell Content Area
* **Offset 7 (1 byte)**: Fragmented Free Byte Count
* **Offset 8 (4 bytes)**: Rightmost Child Page ID

### Cell Pointer Array

Directly following the B-Tree header is the cell pointer array. Each entry is a **2-byte big-endian offset** indicating the starting position of a cell within the cell content area. The array is sorted according to the B-Tree keys, allowing in-place binary searching.

---

## 3. Cell Payload Formats

Hematite cells use **fixed-width u16 big-endian** lengths for keys and values (no varints). The B-tree operates on opaque byte keys and values — rowid encoding and record serialization are handled by the catalog layer above.

### Leaf Cell Layout

Stores a key-value pair:

| Key Length (2 bytes, BE) | Value Length (2 bytes, BE) | Key Bytes | Value Bytes |
| --- | --- | --- | --- |

* Key length: `u16` big-endian. Maximum `MAX_KEY_SIZE = 256` bytes.
* Value length: `u16` big-endian. Maximum `MAX_VALUE_SIZE = 1024` bytes.
* Total cell header overhead: **4 bytes**.

### Interior Cell Layout

Stores a separator key with a left child pointer:

| Left Child Page ID (4 bytes, BE) | Key Length (2 bytes, BE) | Key Bytes |
| --- | --- | --- |

* Left child: `u32` big-endian page ID.
* Key length: `u16` big-endian. Maximum `MAX_KEY_SIZE = 256` bytes.
* Total cell header overhead: **6 bytes**.

---

## 4. Value Overflow and Large Payloads

> [!NOTE]
> **Why Value Overflow?**
> A B-Tree is most efficient when its nodes have a high fan-out (meaning each page points to many children), keeping the tree shallow. If we store extremely large rows (e.g., a 10KB text block) directly inside the B-Tree node, a single row would occupy multiple pages, reducing the node capacity and forcing the tree to grow much deeper.
>
> To prevent this, Hematite limits the size of data stored "inline" (directly in the node cell). When a row's value exceeds a certain threshold, the database "spills" the excess data into a linked list of **Overflow Pages** (each holding up to 4088 bytes of payload). The main leaf cell retains only a small prefix of the data (the local payload) and a 4-byte pointer to the first overflow page.

When a value exceeds the inline capacity of a leaf cell, the B-tree value store layer wraps it in a `StoredValueLayout` that manages inline and overflow portions.

### StoredValueLayout

Each value stored in a leaf cell uses the following wrapper format:

| Tag (1 byte) | Total Length (4 bytes, LE) | Local Payload | Overflow Page ID (4 bytes, optional) | Padding (2 bytes) |
|---|---|---|---|---|

* **Tag**: `0x00` = inline, `0x01` = has overflow.
* **Header size**: `STORED_VALUE_HEADER_SIZE = 11` bytes.

### Spilling Formulas (SQLite-style)

* `MAX_LOCAL_PAYLOAD` = `((4096 - 12) * 64 / 255) - 23` = **1001 bytes**
* `MIN_LOCAL_PAYLOAD` = `((4096 - 12) * 32 / 255) - 23` = **489 bytes**

If the payload fits in `MAX_LOCAL_PAYLOAD`, it is stored entirely inline. Otherwise, the local portion is computed as:

```
surplus = MIN_LOCAL_PAYLOAD + (total_len - MIN_LOCAL_PAYLOAD) % OVERFLOW_PAYLOAD_CAPACITY
local = surplus if surplus <= MAX_LOCAL_PAYLOAD else MIN_LOCAL_PAYLOAD
```

The remaining bytes are written into a chain of overflow pages.

### Overflow Page Structure (`0x20`)

Each overflow page has an 8-byte header followed by payload bytes:

* **Offset 0 (1 byte)**: Page Type (`0x20`)
* **Offset 1 (3 bytes)**: Reserved (must be `0`)
* **Offset 4 (4 bytes)**: Next Overflow Page ID (big-endian, `0` if last page in the chain)
* **Offset 8 onwards**: Payload bytes (up to `OVERFLOW_PAYLOAD_CAPACITY = 4088` bytes)

---

## 5. Transaction Journal Formats

### Rollback Journal Format

A rollback journal (file extension `.jrnl`) enables "undo" recovery. It uses the `HTJ3` binary format.

#### Rollback Journal Header (36 bytes)

| Offset | Size (Bytes) | Type | Field | Description |
|---|---|---|---|---|
| `0` | `4` | `[u8; 4]` | Magic | ASCII `HTJ3` |
| `4` | `4` | `u32` | Version | Journal format version. Current: `1`. |
| `8` | `1` | `u8` | State | `1` = Active, `2` = Committed |
| `9` | `2` | `u16` | Page Size | Database page size (`4096`). Big-endian. |
| `11` | `1` | `u8` | Reserved | Must be `0`. |
| `12` | `4` | `u32` | Original Page Count | Database page count before the transaction. |
| `16` | `4` | `u32` | Sector Size Hint | Sector alignment hint. |
| `20` | `4` | `u32` | Checksum Seed | Seed for page record checksums. |
| `24` | `4` | `u32` | Free Page Count | Count of free pages serialized after the header. |
| `28` | `4` | `u32` | Checksum Count | Count of page checksums serialized after free pages. |
| `32` | `4` | `u32` | Record Count | Number of page records following the metadata sections. |

All multi-byte integers in the journal header are big-endian.

#### Journal Body Layout

After the 36-byte header, the journal body contains three sections in order:

1. **Free page list**: Variable-length encoded list of `free_page_count` page IDs.
2. **Page checksums**: Variable-length encoded list of `checksum_count` `(page_id, checksum)` pairs.
3. **Page records**: Each record is an 8-byte prefix (`page_id: u32` + `checksum: u32`) followed by `PAGE_SIZE` bytes of original page content.

---

### Write-Ahead Log (WAL) Format

A WAL file (file extension `.wal`) enables "redo" recovery and SWMR concurrency. It uses the `HTW3` binary format.

| WAL Header (24 bytes) | WAL Frame 1 (28-byte prefix + 4096-byte payload) | WAL Frame 2 ... |
|---|---|---|

#### WAL Header Layout (24 bytes)

| Offset | Size (Bytes) | Type | Field | Description |
|---|---|---|---|---|
| `0` | `4` | `[u8; 4]` | Magic | ASCII `HTW3` |
| `4` | `4` | `u32` | Version | WAL format version. Current: `1`. |
| `8` | `2` | `u16` | Page Size | Database page size (`4096`). |
| `10` | `2` | `u16` | Reserved | Must be `0`. |
| `12` | `4` | `u32` | Checkpoint Sequence | Monotonically increasing checkpoint counter. |
| `16` | `4` | `u32` | Salt-1 | Randomized validation salt. |
| `20` | `4` | `u32` | Salt-2 | Randomized validation salt. |

All multi-byte integers in the WAL header are big-endian.

#### WAL Frame Layout (28-byte prefix + 4096-byte payload)

| Offset | Size (Bytes) | Type | Field | Description |
|---|---|---|---|---|
| `0` | `4` | `u32` | Page ID | The database page this frame replaces. |
| `4` | `4` | `u32` | Database Page Count | Non-zero indicates a commit boundary frame. |
| `8` | `8` | `u64` | Commit Sequence | Monotonic transaction commit counter. |
| `16` | `4` | `u32` | Salt-1 | Must match the WAL header salt. |
| `20` | `4` | `u32` | Salt-2 | Must match the WAL header salt. |
| `24` | `4` | `u32` | Checksum | Frame integrity checksum over header + page bytes. |
| `28` | `4096` | `[u8]` | Page Payload | The full replacement page image. |

All multi-byte integers in WAL frames are big-endian.
