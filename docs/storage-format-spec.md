# Hematite Storage Format Specification v3 Draft

This document defines the target on-disk format for the next Hematite storage generation.

It is intentionally much closer to SQLite's storage model than the current Hematite format, while
still leaving room for Hematite-specific simplifications where those do not materially harm page
density, write behavior, or crash safety.

This specification is the implementation target for `F1` and the phases that follow it in
[storage-refactor-plan.md](./storage-refactor-plan.md).

## Status

- format generation: `v3`
- compatibility with current Hematite files: none
- compatibility with SQLite files: none
- design inspiration: strong

This is a SQLite-inspired format, not a byte-for-byte SQLite clone.

## Core Goals

The format is designed to achieve the following:

- much denser B-tree pages than the current contiguous key/value layout
- less page rewrite amplification during insert and delete
- a cleaner separation between pager and B-tree concerns
- rollback and WAL formats that scale with dirty pages rather than whole visible-state snapshots
- elimination of hot-path sidecar metadata files

## Main Database File

### Page Size

- fixed page size for the first implementation: `4096`
- all page numbers are 1-based in the main database file
- page `1` is special and contains the database header plus the first B-tree page payload region

This intentionally moves away from the current Hematite layout that uses a 64-byte prelude and
logical page `0`.

### Database Header Placement

- the first `100` bytes of page `1` are the database header
- the remainder of page `1` is usable page content for the root B-tree page stored there
- all pages after page `1` are fully usable except for their own per-page headers

This follows SQLite's most important header-placement choice because it improves compactness and
removes the need for a separate reserved header page.

### Database Header Fields

All integer fields are stored big-endian.

| Offset | Size | Field | Notes |
|---|---:|---|---|
| `0` | `16` | magic | ASCII `Hematite format 3` truncated/padded to 16 bytes |
| `16` | `2` | page size | fixed to `4096` for v3 |
| `18` | `1` | format write version | `3` |
| `19` | `1` | format read version | `3` |
| `20` | `1` | reserved space per page | `0` initially |
| `21` | `1` | max embedded payload fraction | `64` |
| `22` | `1` | min embedded payload fraction | `32` |
| `23` | `1` | leaf payload fraction | `32` |
| `24` | `4` | file change counter | incremented on durable commit |
| `28` | `4` | page count | durable page count |
| `32` | `4` | first freelist trunk page | `0` if none |
| `36` | `4` | total freelist page count | includes trunk and freelist leaf pages |
| `40` | `4` | schema root page | root of the schema table |
| `44` | `4` | schema format version | Hematite schema generation number |
| `48` | `4` | default cache hint | optional tuning hint |
| `52` | `4` | largest root page | reserved for future auto-vacuum work |
| `56` | `4` | text encoding | `1 = UTF-8` |
| `60` | `4` | user version | library-managed or user-managed metadata |
| `64` | `4` | incremental vacuum flag | reserved |
| `68` | `4` | application id | optional |
| `72` | `20` | reserved | zero for now |
| `92` | `4` | header checksum | checksum over bytes `0..92` |
| `96` | `4` | next table id | Hematite catalog convenience field |

The `next table id` field is the main intentional divergence from SQLite's header, since it keeps
one piece of catalog state cheaply accessible without inventing a separate metadata page.

## Page Types

Each page begins with a page-type-specific header.

The first byte identifies page type:

- `0x02`: interior index page
- `0x05`: interior table page
- `0x0A`: leaf index page
- `0x0D`: leaf table page
- `0x20`: overflow page
- `0x30`: freelist trunk page
- `0x31`: freelist leaf page

The first implementation can omit pointer-map pages and auto-vacuum support.

## B-tree Page Format

### Shared Layout

Every B-tree page uses a slotted-page design:

1. page header
2. cell pointer array growing forward
3. unallocated gap
4. cell content area growing backward from the end of the page
5. freeblocks within the cell content area

This replaces the current Hematite layout that serializes all keys and values as contiguous
sections.

### B-tree Page Header

For leaf pages:

| Offset | Size | Field |
|---|---:|---|
| `0` | `1` | page type |
| `1` | `2` | first freeblock offset |
| `3` | `2` | number of cells |
| `5` | `2` | start of cell content area |
| `7` | `1` | fragmented free byte count |

For interior pages:

| Offset | Size | Field |
|---|---:|---|
| `0` | `1` | page type |
| `1` | `2` | first freeblock offset |
| `3` | `2` | number of cells |
| `5` | `2` | start of cell content area |
| `7` | `1` | fragmented free byte count |
| `8` | `4` | rightmost child page |

On page `1`, the usable B-tree page header begins at offset `100` rather than `0`.

### Cell Pointer Array

- one 2-byte big-endian offset per cell
- offsets are stored in sorted key order
- cell bodies themselves may appear anywhere inside the cell content area

This is one of the key performance features we want from SQLite's layout.

### Cell Layouts

#### Table Leaf Cell

- varint payload size
- varint rowid
- local payload bytes
- optional 4-byte overflow page number when payload spills

#### Table Interior Cell

- 4-byte left child page number
- varint rowid separator key

#### Index Leaf Cell

- varint payload size
- local payload bytes
- optional 4-byte overflow page number when payload spills

#### Index Interior Cell

- 4-byte left child page number
- varint payload size
- local payload bytes
- optional 4-byte overflow page number when payload spills

For the first migration, rowid table pages should be implemented first. Index pages can follow the
same general model with a smaller amount of special casing than the current Hematite page format.

### Payload Split Rules

The new format should use SQLite-style local payload formulas:

- `maxLocal = ((usableSize - 12) * 64 / 255) - 23`
- `minLocal = ((usableSize - 12) * 32 / 255) - 23`
- `maxLeaf = usableSize - 35`
- `minLeaf = ((usableSize - 12) * 32 / 255) - 23`

When payload does not fit locally:

- choose a local payload size between `minLocal` and `maxLocal`
- maximize overflow page utilization
- keep the rule deterministic as part of the format

This should be copied directly because it is one of SQLite's most important space-efficiency
choices.

### Freeblocks And Fragments

Free space is represented in three ways:

- the unallocated gap
- freeblocks in the cell content area
- fragments smaller than the minimum freeblock size

The B-tree layer must support:

- allocate from the unallocated gap when possible
- reuse freeblocks before defragmenting
- defragment only when needed

The goal is to stop rebuilding whole pages for ordinary insert and delete paths.

## Overflow Page Format

Overflow pages are page-type `0x20`.

| Offset | Size | Field |
|---|---:|---|
| `0` | `1` | page type |
| `1` | `3` | reserved |
| `4` | `4` | next overflow page number, `0` if last |
| `8` | `N` | payload bytes |

Overflow payload bytes occupy the rest of the usable page.

Rules:

- intermediate overflow pages should be filled completely
- only the final overflow page may be partial
- cursor code should lazily cache overflow page chains once traversed

## Freelist Format

Freelist pages should move into the main database file rather than sidecar metadata.

### Freelist Trunk Page

Page type `0x30`:

- next trunk page number
- count of freelist leaf entries stored on this page
- array of free page numbers

### Freelist Leaf Page

Page type `0x31`:

- no extra structure required in the first implementation beyond the page-type marker

This is simpler than the current externally persisted free-page list and puts page reuse under the
same durability model as the rest of the database.

## Checksums

The v3 format should not persist a separate `.pager_checksums` sidecar.

For the first implementation:

- main database pages do not carry a per-page checksum by default
- rollback journal and WAL records may carry record-level checksums
- the database header carries a header checksum

This is a deliberate tradeoff. The current sidecar checksum model adds write amplification and
recovery complexity without delivering SQLite-like benefits.

## Rollback Journal Format

The rollback journal becomes page-image oriented and self-contained.

### Journal Header

| Field | Notes |
|---|---|
| magic | format identifier |
| format version | journal format version |
| page size | must match main database |
| original database page count | page count before transaction |
| sector size hint | reserved for later |
| checksum seed | for record validation |

### Journal Records

Each record stores:

- page number
- original page bytes
- record checksum

Rules:

- original page image is journaled before a page becomes writeable
- each page is journaled at most once per transaction
- commit phase one syncs the journal before main-file overwrite
- commit phase two finalizes or removes the journal

This is explicitly closer to SQLite than the current Hematite journal, which still drags along
broader metadata state.

## WAL Format

The new WAL must be frame-oriented rather than whole-visible-state oriented.

### WAL Header

| Field | Notes |
|---|---|
| magic | WAL identifier |
| format version | WAL format version |
| page size | must match main database |
| salt values | for frame validation |
| checkpoint sequence | optional |

### WAL Frame

Each frame stores:

- page number
- database page count after commit boundary this frame belongs to
- page bytes
- frame checksum

### Commit Boundary

- a transaction is considered committed when a commit-marking frame boundary is durable
- readers select a stable end mark when they begin
- checkpoint copies frames back to the main database in page-number order or other pager-approved
  order

This replaces the current Hematite WAL design that serializes entire visible-state transitions per
commit.

## Endianness

For v3:

- database header fields use big-endian
- page-header scalar fields use big-endian
- journal and WAL headers use big-endian
- varints use SQLite-style variable-length integer encoding

This choice is made to stay close to SQLite and keep the format easy to inspect with existing
storage intuition.

## Catalog Mapping

The catalog and higher layers should not need a separate metadata page for durable storage
coordination.

The schema should live in normal B-tree pages:

- page `1` should be the root page of the schema table where practical
- table metadata should be stored as ordinary rows in the schema/catalog structure
- runtime-only metadata must stay in memory and be reconstructable at open time

This does not mean Hematite must adopt SQLite's catalog schema verbatim, only that metadata should
live in ordinary storage structures rather than custom out-of-band pages and sidecars.

## Migration Policy

Databases created with the current Hematite format are not readable as v3 databases.

`F10` resolves this by choosing explicit old-format retirement for the current release line:

- Hematite opens only databases created with the current post-reset storage generation
- older on-disk generations are rejected at open time with an explicit migration/retirement error
- no offline migrator is shipped yet

No in-place compatibility layer should be built into the hot storage path.

## Implementation Order Hints

This spec suggests the following implementation order:

1. new header and page-numbering model
2. slotted table leaf pages
3. slotted table interior pages
4. freelist in main file
5. overflow pages
6. rollback journal
7. WAL
8. index-page variants and read-path tuning

## Non-Goals For v3

These are intentionally out of scope for the first new format:

- byte-for-byte SQLite compatibility
- pointer-map pages
- auto-vacuum
- mmap-specific page states
- WITHOUT ROWID table layout
- sector-aware large-sector journaling on day one

Those can come later once the main structural rewrite is complete.
