# Hematite Storage Format Specification

This document defines the physical on-disk format for Hematite database files, rollback journals, and write-ahead logs (WAL). Every format carries version `1`.

Hematite uses a page-based storage engine with a fixed page size of **4096 bytes** (`PAGE_SIZE`), utilizing slotted-page allocations and clustered B+ Trees.

The last **4 bytes** of every page (`PAGE_TRAILER_SIZE`) belong to the pager, which stamps a checksum of the page's own contents there on the way out. Nothing above the pager may write into them, so the space a page has for its own use is **4092 bytes** (`USABLE_PAGE_SIZE`).

The trailer is `hash32(seed = page_id, bytes 0..4092)`, stored little-endian, where `hash32` is the high 32 bits of XXH64 (`storage/checksum.rs`). An all-zero trailer means "not stamped" and is not verified; any other value must match when a page's contents are read for use.

---

## 1. Database File Layout

A database file consists of contiguous, fixed-size pages. Unlike SQLite, which overlays the database header on Page 1 along with the schema tree root, Hematite enforces a strict separation between database headers, transaction metadata, and allocatable data B-trees.

| Page 0<br>`DB_HEADER_PAGE_ID` | Page 1<br>`STORAGE_METADATA_PAGE_ID` | Page 2+<br>`FIRST_ALLOCATABLE_PAGE_ID` |
| --- | --- | --- |
| **Database Header** | **Storage Metadata** | **Allocatable Trees** |

### Page 0: Database Header Layout

The first 100 bytes of Page 0 are reserved (`DATABASE_HEADER_SIZE = 100`). The actual database header occupies **20 bytes** at the start; the remaining 80 bytes are zero padding reserved for future use. No B-tree is ever rooted on page 0: the schema tree, like every other tree, is allocated from page 2 onward.

All scalar integers in the database header are stored in **little-endian** byte order.

| Offset | Size (Bytes) | Type | Field | Description / Value |
| --- | --- | --- | --- | --- |
| `0` | `4` | `[u8; 4]` | Magic | ASCII `HMTD`. Identifies the file as a Hematite database. |
| `4` | `4` | `u32` | Version | Database format version. Current value: `1`. A file carrying anything else is refused at open. |
| `8` | `4` | `u32` | Schema Root Page | Root page ID of the schema catalog B-Tree. |
| `12` | `4` | `u32` | Next Table ID | Incremental counter to allocate unique table IDs. |
| `16` | `4` | `u32` | Header Checksum | `hash32(seed = 0, bytes 0..16)`: the high 32 bits of XXH64. |
| `20` | `80` | `[u8; 80]` | Reserved | Zero padding. Reserved for future header fields. |

> **Note**: Page size is not stored in the header — it is a compile-time constant (`PAGE_SIZE = 4096`).

### Page 1: Storage Metadata Page

Page 1 is a dedicated metadata container shared by the pager and the catalog. Each owner rewrites the whole page but carries the other's section across unchanged. A page whose usable bytes are all zero holds neither section.

| Offset | Size (Bytes) | Field |
| --- | --- | --- |
| `0` | `4` | Magic, ASCII `HMD1` (`METADATA_PAGE_MAGIC`) |
| `4` | `4` | Version, `u32` LE, `1` |
| `8` | `4` | Pager section length, `u32` LE |
| `12` | `4` | Catalog section length, `u32` LE |
| `16` | … | Pager section, then catalog section; both must end by byte 4092 |

1. **Pager section** (magic `HPM1`): the journal mode and the free page list. Per-page checksums are *not* here — each page carries its own in its trailer, which has no ceiling on the number of pages it can describe.

   | Offset | Size (Bytes) | Field |
   | --- | --- | --- |
   | `0` | `4` | Magic, ASCII `HPM1` |
   | `4` | `4` | Version, `u32` LE, `1` |
   | `8` | `1` | Journal mode: `0` rollback, `1` WAL |
   | `9` | `4` | Free page count, `u32` LE |
   | `13` | `4` | Checksum entry count, `u32` LE, always `0` |
   | `17` | … | Free page ids, sorted, each a LEB128 varint delta from the previous |

2. **Catalog section**: UTF-8 text, one line per field — `version=1`, `table_count=N`, then one `table|<name>|<root_page_id>|<row_count>|<next_row_id>` line per table, sorted by name.

---

## 2. B-Tree Page Format

Hematite uses B+ Trees for storage. Tables use B-Trees keyed by an encoded row id, with a record as the value. Indexes use B-Trees keyed by encoded column values; section 6 gives the key and value encodings.

> [!NOTE]
> **Why Slotted Pages?** 
> When database rows contain variable-length columns (e.g., text or blobs), a simple array of rows makes insertion, deletion, and updates extremely expensive. Deleting a row would require shifting all subsequent rows to fill the gap, and inserting a larger row would require rewriting the entire page.
>
> A **Slotted Page Layout** solves this by separating pointers from the actual data. Pointers grow forward from the start of the page, while the data cells grow backward from the end of the page's usable area (byte 4092, before the trailer). This leaves a flexible "unallocated gap" in the middle. If a row is deleted, we mark its space as a freeblock and remove its pointer, avoiding expensive shifts.

All B-Tree pages are structured using a **Slotted Page Layout**:

| Component | Description |
| :--- | :--- |
| **Page Header** | 8 bytes for Leaf, 12 bytes for Interior |
| **Cell Pointer Array** | 2 bytes per cell, sorted by key (grows forward `====>`) |
| **UNALLOCATED GAP** | Free space between pointer array and content |
| **Cell Content Area** | Holds cells and freeblocks (grows backward `<====`) |

### B-Tree Page Headers

The page type identifier is the first byte:

* `0x05`: Interior Page
* `0x0D`: Leaf Page

These are the only two kinds `BTreeNode` writes or accepts, for index trees as well as table trees: the B-tree stores opaque byte keys and values, so a tree does not need to know which it is. The kind values `0x02` (interior index) and `0x0A` (leaf index) are defined in `PageKind` and refused when read as a B-tree node.

#### Leaf Page Header Layout (`0x0D`)

* **Offset 0 (1 byte)**: Page Type
* **Offset 1 (2 bytes)**: First Freeblock Offset (`0` if none)
* **Offset 3 (2 bytes)**: Cell Count
* **Offset 5 (2 bytes)**: Start of Cell Content Area (offset from page start; `4092` on an empty page)
* **Offset 7 (1 byte)**: Fragmented Free Byte Count

#### Interior Page Header Layout (`0x05`)

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

* Key length: `u16` big-endian.
* Value length: `u16` big-endian.
* Total cell header overhead: **4 bytes** (`LEAF_CELL_HEADER_SIZE`).

A page holds at most `MAX_KEYS` = 399 cells (`BTREE_ORDER` − 1). That cap binds before the bytes do only for cells whose key and stored value together take 4 bytes or less; any larger cell runs out of bytes first. Keys are capped at 256 bytes.

### Interior Cell Layout

Stores a separator key with a left child pointer:

| Left Child Page ID (4 bytes, BE) | Key Length (2 bytes, BE) | Key Bytes |
| --- | --- | --- |

* Left child: `u32` big-endian page ID.
* Key length: `u16` big-endian.
* Total cell header overhead: **6 bytes** (`INTERNAL_CELL_HEADER_SIZE`).

---

## 4. Value Overflow and Large Payloads

> [!NOTE]
> **Why Value Overflow?**
> A B-Tree is most efficient when its nodes have a high fan-out (meaning each page points to many children), keeping the tree shallow. If we store extremely large rows (e.g., a 10KB text block) directly inside the B-Tree node, a single row would occupy multiple pages, reducing the node capacity and forcing the tree to grow much deeper.
>
> To prevent this, Hematite limits the size of data stored "inline" (directly in the node cell). When a row's value exceeds a certain threshold, the database "spills" the excess data into a linked list of **Overflow Pages** (each holding up to 4084 bytes of payload). The main leaf cell retains only a small prefix of the data (the local payload) and a 4-byte pointer to the first overflow page.

Only values spill. Keys are capped at 256 bytes and always stay in the cell.

When a value exceeds the inline capacity of a leaf cell, the B-tree value store layer wraps it in a `StoredValueLayout` that manages inline and overflow portions.

### StoredValueLayout

Each value stored in a leaf cell takes one of two forms, told apart by its first byte:

| Form | Layout | Header |
|---|---|---|
| Inline | `[tag 0x00][payload]` | 1 byte (`STORED_VALUE_INLINE_HEADER_SIZE`) |
| Overflow | `[tag 0x01][total length u32 LE][first overflow page u32 LE][local payload]` | 9 bytes (`STORED_VALUE_OVERFLOW_HEADER_SIZE`) |

An inline value's length is the cell's value length minus the tag, so it is not stored again. An overflow value's local length is never stored either: it follows from the rule below.

### Spilling Rule

* `MAX_LOCAL_PAYLOAD` = `((4092 - 12) * 64 / 255) - 23` = **1001 bytes**, SQLite's formula over the usable page size.
* `MIN_LOCAL_PAYLOAD` = **32 bytes**.

A payload of up to `MAX_LOCAL_PAYLOAD` bytes is stored entirely inline. A larger one keeps exactly `MIN_LOCAL_PAYLOAD` bytes in the cell and writes the rest into a chain of overflow pages.

### Overflow Page Structure (`0x20`)

Each overflow page has an 8-byte header followed by payload bytes:

* **Offset 0 (1 byte)**: Page Type (`0x20`)
* **Offset 1 (3 bytes)**: Reserved (must be `0`)
* **Offset 4 (4 bytes)**: Next Overflow Page ID (big-endian, `0` if last page in the chain)
* **Offset 8 onwards**: Payload bytes (up to `OVERFLOW_PAYLOAD_CAPACITY = 4084` bytes, ending at the trailer)

---

## 5. Transaction Journal Formats

### Rollback Journal Format

A rollback journal enables "undo" recovery. It sits beside the database, named after the whole database filename plus `.journal` (`app.db` → `app.db.journal`).

A committed or rolled-back transaction empties its journal rather than deleting it: an empty file means "nothing to recover", and the file and its open handle are reused by the next transaction. Recovery at open, and switching to WAL mode, delete it instead. A non-empty file that does not parse is corruption, and is an error.

#### Rollback Journal Header (40 bytes)

| Offset | Size (Bytes) | Type | Field | Description |
|---|---|---|---|---|
| `0` | `4` | `[u8; 4]` | Magic | ASCII `HTJ1` |
| `4` | `4` | `u32` | Version | Journal format version. Current: `1`. |
| `8` | `1` | `u8` | State | `1` = Active, `2` = Committed. The engine writes only `1`; recovery discards a `2` journal without replaying it. |
| `9` | `2` | `u16` | Page Size | Database page size (`4096`). Big-endian. |
| `11` | `1` | `u8` | Reserved | Must be `0`. |
| `12` | `4` | `u32` | Original Page Count | Database page count before the transaction. |
| `16` | `4` | `u32` | Sector Size Hint | Always `4096`; not used. |
| `20` | `4` | `u32` | Checksum Seed | Fresh for every journal, mixed into every record checksum. A record left over from an earlier journal therefore fails to verify. |
| `24` | `4` | `u32` | Free Page Count | Count of free pages serialized after the header. |
| `28` | `4` | `u32` | Checksum Count | Always `0`: page checksums live in page trailers. The field is kept so the header layout is fixed. |
| `32` | `4` | `u32` | Record Count | Number of page records following the metadata sections. |
| `36` | `4` | `u32` | Intent Count | Number of commit-intent entries following the page records. |

All multi-byte integers in the journal header are big-endian.

#### Journal Body Layout

After the 40-byte header, the journal body contains four sections in order:

1. **Free page list**: `free_page_count` page IDs as fixed 4-byte `u32`s: the free list as it stood before the transaction.
2. **Page checksums**: Empty; `checksum_count` is always `0`.
3. **Page records** (4104 bytes each): an 8-byte prefix (`page_id: u32` + `checksum: u32`) followed by `PAGE_SIZE` bytes of original page content. The checksum is `hash32(hash64(0, page_id ‖ seed), page image)`.
4. **Commit intents**: `intent_count` entries of `(page_id: u32, checksum: u32)`, naming what each page the commit touches must contain if the commit reached the disk. An intent's checksum is `hash32(0, bytes 0..4092)` of the post-commit page — deliberately not its trailer value, which is seeded with the page id.

The commit intents are what keep a rollback commit to two fsyncs. Recovery finding an active journal does not mean the transaction was abandoned: if every page listed already holds its post-commit checksum, the commit landed and the journal is discarded. Comparing against the *post*-commit image rather than asking "did this page change" is what makes it sound — a page whose new contents equal its old contents is indistinguishable by the latter test, and a torn page matches neither image, so it is correctly treated as a partial commit.

#### Rollback Commit Order

1. Stage page 1 (journalling its pre-image like any other page).
2. Append the commit intents and rewrite the intent count.
3. **fsync the journal** (fsync 1).
4. Write the dirty pages to the database, page 1 last.
5. **fsync the database** (fsync 2).
6. Truncate the journal to zero length, without an fsync: a crash before the truncate reaches disk leaves a journal whose intents all match, which recovery discards.

When the page cache must spill dirty pages before commit, the journal is fsynced before any of them is written to the database.

#### Recovery at Open

1. If another connection in this process holds the write lock, recovery is skipped: the journal is live.
2. Read the journal, tolerating a body cut short by the crash. The header and free list must parse. Page records are kept up to the first one that is short or fails its checksum, and anything after the last one kept is ignored: the database is written only after the journal is fsynced, so a record that is not intact was appended after the last sync, and neither it nor any later record covers a page the database has seen. Commit intents are kept only if every record before them is intact.
3. If the journal lists commit intents and every listed page holds its intended checksum, the commit landed: delete the journal.
4. Otherwise roll back: truncate the database to its original page count, restore the original free list, write every page record's image back, fsync the database, then delete the journal.

---

### Write-Ahead Log (WAL) Format

A WAL file enables "redo" recovery and single-writer / multi-reader concurrency within one process. It is named after the whole database filename plus `.wal` (`app.db` → `app.db.wal`).

| WAL Header (24 bytes) | WAL Frame 1 (28-byte prefix + 4096-byte payload) | WAL Frame 2 ... |
|---|---|---|

#### WAL Header Layout (24 bytes)

| Offset | Size (Bytes) | Type | Field | Description |
|---|---|---|---|---|
| `0` | `4` | `[u8; 4]` | Magic | ASCII `HTW1` |
| `4` | `4` | `u32` | Version | WAL format version. Current: `1`. |
| `8` | `2` | `u16` | Page Size | Database page size (`4096`). |
| `10` | `2` | `u16` | Reserved | Must be `0`. |
| `12` | `4` | `u32` | Checkpoint Sequence | Always `0`: a checkpoint deletes the WAL, so there is never a second one to number. |
| `16` | `4` | `u32` | Salt-1 | Randomized validation salt. |
| `20` | `4` | `u32` | Salt-2 | Randomized validation salt. |

All multi-byte integers in the WAL header are big-endian.

#### WAL Frame Layout (28-byte prefix + 4096-byte payload)

| Offset | Size (Bytes) | Type | Field | Description |
|---|---|---|---|---|
| `0` | `4` | `u32` | Page ID | The database page this frame replaces. |
| `4` | `4` | `u32` | Database Page Count | Page count of the database after this frame's commit; every frame of a commit carries the same value. |
| `8` | `8` | `u64` | Commit Sequence | Monotonic transaction commit counter; all frames of one commit share it. |
| `16` | `4` | `u32` | Salt-1 | Must match the WAL header salt. |
| `20` | `4` | `u32` | Salt-2 | Must match the WAL header salt. |
| `24` | `4` | `u32` | Checksum | `hash32(hash64(0, prefix bytes 0..24), page image)`. |
| `28` | `4096` | `[u8]` | Page Payload | The full replacement page image, trailer stamped (page 1's frame carries a zero trailer). |

All multi-byte integers in WAL frames are big-endian.

#### Commits, Recovery and Checkpoints

* **Commit**: the commit's frames, sorted by page id and followed by a frame for page 1, are appended in one `writev`, then the WAL is fsynced. That is the commit's only fsync; the database file is not written.
* **Commit boundary**: a run of frames sharing a commit sequence is committed only if it contains a page-1 frame, which is always written last. A run without one is an incomplete commit and is ignored.
* **Damage**: a partial frame at the end of the file is a torn append and is ignored. A complete frame whose salts or checksum do not match fails the load.
* **Checkpoint**: every page the WAL overrides is written to the database, the database is fsynced, the WAL is deleted, and page 1 is rewritten and fsynced. It runs after a commit once the WAL holds 1000 frames or more, on an explicit `checkpoint_wal`, and when switching back to rollback mode.

---

## 6. Catalog Encodings

The B-tree stores opaque bytes; the catalog decides what they mean. All are version 1.

* **Schema tree**: holds one entry, key `__schema__`, whose value is the serialized schema: `next_table_id` (`u32` LE), `next_column_id` (`u32` LE), `table_count` (`u32` LE), then each table's definition (`Table::serialize`).
* **Table tree key**: the row id as a tag byte followed by its big-endian magnitude, so keys sort in row-id order: tags `0x81..=0x88` for non-negative ids of 1–8 bytes, `0x78..=0x7F` for negative ids of 8–1 bytes (`catalog/serialization.rs`).
* **Table tree value**: a self-describing record (`catalog/record_codec.rs`): a varint header length, one varint serial type per column giving its type and width, then the column bodies. Integers take their minimal width; text, blob and decimal fold their length into the serial type.
* **Primary-key index**: key is the order-preserving encoding of the key columns (`catalog/index_key_codec.rs`), value is the row id as a varint. A table whose primary key is a single integer column of any type but `UINT64` has no primary-key index: the key *is* the row id.
* **Secondary index**: key is the order-preserving encoding of the indexed columns followed by the row id as `[len][big-endian bytes][len]`, so equal values stay distinct and sort by row id; the value is empty.
* **Varints** (records and index values): the length is carried in the trailing zero bits of the first byte — `xxxxxxx1` is one byte holding 7 bits, `xxxxxx10` two bytes holding 14, up to `00000000` followed by eight bytes — so decoding is a load and a shift (`btree/varint.rs`). B-tree cells use fixed `u16` lengths, not varints, and page 1's free-list deltas use plain LEB128.
