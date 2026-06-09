//! # Slotted B-Tree Page Layout and Allocation (`page_format`)
//!
//! This module manages the physical memory layout of individual B-Tree pages using a **Slotted Page Architecture**.
//! It provides helper structures to allocate, delete, defragment, and format cells within the page buffer.
//!
//! ---
//!
//! ## 1. Core Database Slotted Page Concepts
//!
//! ### The Need for Slotted Pages
//! In a database, records (cells) are often variable in size (due to variable-length text, binary data,
//! or optional NULL columns). If cells are serialized sequentially on a page, deleting a cell in the middle
//! leaves a hole. Inserting new cells of varying sizes eventually causes fragmentation.
//!
//! Shifting all subsequent cells to plug holes is CPU-intensive. Instead, a **Slotted Page** architecture splits
//! the page into three areas:
//! 1. **Cell Pointer Array**: Starts at the front of the page and grows *forward*. Contains 2-byte offsets
//!    pointing to the actual cells. The logical index in this array is stable, even if the cell is moved.
//! 2. **Cell Content Area**: Starts at the end of the page and grows *backward*. Contains the raw cell payloads.
//! 3. **Unallocated Gap**: The empty space in the middle.
//!
//! ### Physical Layout of a Slotted Page
//!
//! ```text
//! +-----------------------------------------------------------------------+
//! | Page Header (Kind, Freeblock ptr, Cell count, Content start, Frags)   |
//! +-----------------------------------------------------------------------+
//! | Cell Pointer Array (growing forward)                                  |
//! | [Offset to Cell 0] [Offset to Cell 1] ...                             |
//! +-----------------------------------------------------------------------+
//! |                        UNALLOCATED GAP                                |
//! |                                                                       |
//! +-----------------------------------------------------------------------+
//! | Cell Content Area (growing backward)                                  |
//! | ... [Cell 1 Payload] [Freeblock] [Cell 0 Payload]                     |
//! +-----------------------------------------------------------------------+
//! ```
//!
//! ### Freeblocks and Fragmented Bytes
//! When a cell is deleted:
//! * Its slot pointer is removed.
//! * Its space in the Cell Content Area is added to a linked list of **Freeblocks** (if it is $\ge 4$ bytes).
//!   The first 2 bytes of a freeblock store the offset to the next freeblock; the next 2 bytes store the block size.
//! * If the deleted space is $< 4$ bytes, it is too small to form a freeblock. It is classified as **Fragmented Free Bytes**.
//!
//! ### Defragmentation (Compaction)
//! If a cell allocation fails because the unallocated gap is too small, but the total free space (gap + freeblocks + fragments)
//! is sufficient, the page is **defragmented**:
//! 1. All active cells are read into memory.
//! 2. The Cell Content Area is cleared, wiping out all freeblocks and fragments.
//! 3. The cells are written back contiguously from the end of the page.
//! 4. The unallocated gap becomes one large, unified contiguous region.
//!
//! ---
//!
//! ## 2. Hematite Slotted Page Header Layouts
//!
//! Hematite supports four B-Tree page types: Leaf/Interior Table pages, and Leaf/Interior Index pages.
//! On Page 1 (the first physical page containing the B-Tree schema), the page header starts at offset 100
//! (`DATABASE_HEADER_SIZE`), directly following the database file header.
//!
//! ### Header Offset Calculation
//!
//! ```text
//!              Page 1 (Schema Root)                  Page 2+ (Standard)
//!            +---------------------+               +---------------------+
//!            |  Database Header    |               |  B-Tree Page Header |
//!   Offset 0 |  (100 bytes)        |      Offset 0 |  (8 or 12 bytes)    |
//!            +---------------------+               +---------------------+
//! Offset 100 |  B-Tree Page Header |               |  Cell Pointer Array |
//!            |  (8 or 12 bytes)    |               |  (growing forward)  |
//!            +---------------------+               +---------------------+
//! ```
//!
//! ### Header Fields
//!
//! | Offset | Size (Bytes) | Field Name | Description |
//! |---|---|---|---|
//! | `0` | `1` | `page_kind` | One of `0x02` (Interior Index), `0x05` (Interior Table), `0x0A` (Leaf Index), or `0x0D` (Leaf Table). |
//! | `1` | `2` | `first_freeblock` | Offset to the first freeblock in the page. `0` if none. |
//! | `3` | `2` | `cell_count` | Number of cells stored on the page. |
//! | `5` | `2` | `cell_content_start`| Start of the cell content area. Grows backward; `PAGE_SIZE` if empty. |
//! | `7` | `1` | `fragmented_free_bytes`| Wasted space in bytes that could not form freeblocks. |
//! | `8` | `4` | `rightmost_child` | (Interior pages only) The page ID of the rightmost routing child. |
//!

#![allow(dead_code)]

use crate::error::{HematiteError, Result};
use crate::storage::format::{PageKind, DATABASE_HEADER_SIZE};
use crate::storage::{Page, PAGE_SIZE};

const LEAF_HEADER_SIZE: usize = 8;
const INTERIOR_HEADER_SIZE: usize = 12;
const OFFSET_PAGE_KIND: usize = 0;
const OFFSET_FIRST_FREEBLOCK: usize = 1;
const OFFSET_CELL_COUNT: usize = 3;
const OFFSET_CELL_CONTENT_START: usize = 5;
const OFFSET_FRAGMENTED_FREE_BYTES: usize = 7;
const OFFSET_RIGHTMOST_CHILD: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BTreePageHeaderV3 {
    pub(crate) kind: PageKind,
    pub(crate) header_offset: usize,
    pub(crate) header_size: usize,
    pub(crate) first_freeblock: u16,
    pub(crate) cell_count: u16,
    pub(crate) cell_content_start: u16,
    pub(crate) fragmented_free_bytes: u8,
    pub(crate) rightmost_child: Option<u32>,
}

impl BTreePageHeaderV3 {
    pub(crate) fn parse(page: &Page, is_page_one: bool) -> Result<Self> {
        let header_offset = header_offset(is_page_one);
        if header_offset + LEAF_HEADER_SIZE > page.data.len() {
            return Err(HematiteError::CorruptedData(
                "v3 b-tree page header is truncated".to_string(),
            ));
        }

        let kind = PageKind::from_byte(page.data[header_offset + OFFSET_PAGE_KIND])?;
        let header_size = page_header_size(kind)?;
        if header_offset + header_size > page.data.len() {
            return Err(HematiteError::CorruptedData(
                "v3 b-tree page header exceeds page bounds".to_string(),
            ));
        }

        let header = Self {
            kind,
            header_offset,
            header_size,
            first_freeblock: read_u16_be(&page.data, header_offset + OFFSET_FIRST_FREEBLOCK),
            cell_count: read_u16_be(&page.data, header_offset + OFFSET_CELL_COUNT),
            cell_content_start: read_u16_be(&page.data, header_offset + OFFSET_CELL_CONTENT_START),
            fragmented_free_bytes: page.data[header_offset + OFFSET_FRAGMENTED_FREE_BYTES],
            rightmost_child: kind
                .is_interior()
                .then(|| read_u32_be(&page.data, header_offset + OFFSET_RIGHTMOST_CHILD)),
        };

        if header.cell_content_start as usize > PAGE_SIZE {
            return Err(HematiteError::CorruptedData(
                "v3 b-tree cell content start exceeds page bounds".to_string(),
            ));
        }

        let pointer_area_end = header.pointer_area_end();
        if pointer_area_end > header.cell_content_start as usize {
            return Err(HematiteError::CorruptedData(
                "v3 b-tree pointer area overlaps cell content".to_string(),
            ));
        }

        Ok(header)
    }

    pub(crate) fn pointer_area_start(&self) -> usize {
        self.header_offset + self.header_size
    }

    pub(crate) fn pointer_area_end(&self) -> usize {
        self.pointer_area_start() + self.cell_count as usize * 2
    }

    pub(crate) fn writable_gap_size(&self) -> usize {
        self.cell_content_start as usize - self.pointer_area_end()
    }
}

#[cfg(test)]
pub(crate) fn initialize_btree_page(
    page: &mut Page,
    kind: PageKind,
    is_page_one: bool,
) -> Result<()> {
    let header_offset = header_offset(is_page_one);
    let header_size = page_header_size(kind)?;
    if header_offset + header_size > PAGE_SIZE {
        return Err(HematiteError::StorageError(
            "v3 b-tree header does not fit on page".to_string(),
        ));
    }

    if !matches!(
        kind,
        PageKind::LeafTable
            | PageKind::InteriorTable
            | PageKind::LeafIndex
            | PageKind::InteriorIndex
    ) {
        return Err(HematiteError::StorageError(format!(
            "Page kind {:?} is not a v3 b-tree page",
            kind
        )));
    }

    if !is_page_one {
        page.data.fill(0);
    } else {
        for byte in page.data.iter_mut().skip(DATABASE_HEADER_SIZE) {
            *byte = 0;
        }
    }

    page.data[header_offset + OFFSET_PAGE_KIND] = kind as u8;
    write_u16_be(&mut page.data, header_offset + OFFSET_FIRST_FREEBLOCK, 0);
    write_u16_be(&mut page.data, header_offset + OFFSET_CELL_COUNT, 0);
    write_u16_be(
        &mut page.data,
        header_offset + OFFSET_CELL_CONTENT_START,
        PAGE_SIZE as u16,
    );
    page.data[header_offset + OFFSET_FRAGMENTED_FREE_BYTES] = 0;
    if kind.is_interior() {
        write_u32_be(&mut page.data, header_offset + OFFSET_RIGHTMOST_CHILD, 0);
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn cell_pointer(page: &Page, is_page_one: bool, index: usize) -> Result<u16> {
    let header = BTreePageHeaderV3::parse(page, is_page_one)?;
    if index >= header.cell_count as usize {
        return Err(HematiteError::StorageError(format!(
            "Cell index {} out of bounds for {} cells",
            index, header.cell_count
        )));
    }
    Ok(read_u16_be(
        &page.data,
        header.pointer_area_start() + index * 2,
    ))
}

#[cfg(test)]
pub(crate) fn set_rightmost_child(page: &mut Page, is_page_one: bool, child: u32) -> Result<()> {
    let header = BTreePageHeaderV3::parse(page, is_page_one)?;
    if !header.kind.is_interior() {
        return Err(HematiteError::StorageError(
            "Cannot set rightmost child on a leaf page".to_string(),
        ));
    }
    write_u32_be(
        &mut page.data,
        header.header_offset + OFFSET_RIGHTMOST_CHILD,
        child,
    );
    Ok(())
}

/// Minimum freeblock size: 4 bytes (2 for next pointer + 2 for size).
const MIN_FREEBLOCK_SIZE: usize = 4;

/// Calculate total usable free space on a page: gap + freeblock chain + fragments.
pub(crate) fn total_free_space(page: &Page, is_page_one: bool) -> Result<usize> {
    let header = BTreePageHeaderV3::parse(page, is_page_one)?;
    let gap = header.writable_gap_size();
    let fragments = header.fragmented_free_bytes as usize;

    let mut freeblock_space = 0usize;
    let mut fb_offset = header.first_freeblock as usize;
    let mut visited = 0u16;
    while fb_offset != 0 {
        if fb_offset + MIN_FREEBLOCK_SIZE > PAGE_SIZE {
            return Err(HematiteError::CorruptedData(
                "Freeblock pointer exceeds page bounds".to_string(),
            ));
        }
        let fb_size = read_u16_be(&page.data, fb_offset + 2) as usize;
        freeblock_space += fb_size;
        fb_offset = read_u16_be(&page.data, fb_offset) as usize;
        visited += 1;
        if visited > PAGE_SIZE as u16 / MIN_FREEBLOCK_SIZE as u16 {
            return Err(HematiteError::CorruptedData(
                "Freeblock chain cycle detected".to_string(),
            ));
        }
    }

    Ok(gap + freeblock_space + fragments)
}

/// Try to allocate `needed` bytes from the freeblock chain.
/// Returns the offset of the allocated space, or `None` if no suitable freeblock exists.
fn allocate_from_freeblocks(data: &mut [u8], header_offset: usize, needed: usize) -> Option<usize> {
    // Walk the freeblock chain looking for a block >= needed.
    let mut prev_ptr_offset = header_offset + OFFSET_FIRST_FREEBLOCK;
    let mut fb_offset = read_u16_be(data, prev_ptr_offset) as usize;

    while fb_offset != 0 {
        if fb_offset + MIN_FREEBLOCK_SIZE > data.len() {
            return None;
        }
        let fb_size = read_u16_be(data, fb_offset + 2) as usize;
        let next_fb = read_u16_be(data, fb_offset) as usize;

        if fb_size >= needed {
            let remainder = fb_size - needed;
            if remainder >= MIN_FREEBLOCK_SIZE {
                // Shrink: keep the tail as a smaller freeblock.
                let new_fb_offset = fb_offset + needed;
                write_u16_be(data, new_fb_offset, next_fb as u16);
                write_u16_be(data, new_fb_offset + 2, remainder as u16);
                write_u16_be(data, prev_ptr_offset, new_fb_offset as u16);
            } else {
                // Use the whole freeblock; add remainder to fragment count.
                write_u16_be(data, prev_ptr_offset, next_fb as u16);
                if remainder > 0 {
                    let frag_byte = header_offset + OFFSET_FRAGMENTED_FREE_BYTES;
                    let old_frags = data[frag_byte] as usize;
                    let new_frags = (old_frags + remainder).min(255);
                    data[frag_byte] = new_frags as u8;
                }
            }
            return Some(fb_offset);
        }

        prev_ptr_offset = fb_offset; // next-pointer field is at the start of freeblock
        fb_offset = next_fb;
    }

    None
}

/// Insert a cell onto a page, reusing freeblock space when possible.
/// Falls back to the gap, defragmenting if needed.
pub(crate) fn insert_cell(
    page: &mut Page,
    is_page_one: bool,
    sorted_index: usize,
    cell_bytes: &[u8],
) -> Result<u16> {
    let mut header = BTreePageHeaderV3::parse(page, is_page_one)?;
    if sorted_index > header.cell_count as usize {
        return Err(HematiteError::StorageError(format!(
            "Cannot insert cell pointer at index {} for {} existing cells",
            sorted_index, header.cell_count
        )));
    }

    let cell_len = cell_bytes.len();
    // We always need 2 bytes for the new cell pointer in the pointer array.
    let pointer_space_needed = 2;

    // Check total free space first (gap + freeblocks + fragments).
    let total_free = total_free_space(page, is_page_one)?;
    if total_free < cell_len + pointer_space_needed {
        return Err(HematiteError::StorageError(
            "v3 b-tree page has insufficient total space for new cell".to_string(),
        ));
    }

    // Ensure the gap has room for the new pointer entry (2 bytes).
    if header.writable_gap_size() < pointer_space_needed {
        defragment_page(page, is_page_one)?;
        header = BTreePageHeaderV3::parse(page, is_page_one)?;
    }

    // Try allocating from freeblocks first.
    let cell_start = if let Some(offset) =
        allocate_from_freeblocks(&mut page.data, header.header_offset, cell_len)
    {
        page.data[offset..offset + cell_len].copy_from_slice(cell_bytes);
        offset
    } else {
        // Fall back to the gap. If the gap is too small for cell + pointer, defragment.
        let required_in_gap = cell_len + pointer_space_needed;
        if header.writable_gap_size() < required_in_gap {
            defragment_page(page, is_page_one)?;
            header = BTreePageHeaderV3::parse(page, is_page_one)?;
        }
        if header.writable_gap_size() < cell_len + pointer_space_needed {
            return Err(HematiteError::StorageError(
                "v3 b-tree page has insufficient contiguous space for new cell".to_string(),
            ));
        }
        let start = header.cell_content_start as usize - cell_len;
        page.data[start..header.cell_content_start as usize].copy_from_slice(cell_bytes);

        // Update cell_content_start.
        write_u16_be(
            &mut page.data,
            header.header_offset + OFFSET_CELL_CONTENT_START,
            start as u16,
        );
        start
    };

    // Insert the cell pointer at sorted_index, shifting later pointers right.
    let pointer_start = header.pointer_area_start();
    let insertion_offset = pointer_start + sorted_index * 2;
    let pointer_end = pointer_start + header.cell_count as usize * 2;
    page.data
        .copy_within(insertion_offset..pointer_end, insertion_offset + 2);
    write_u16_be(&mut page.data, insertion_offset, cell_start as u16);

    // Update cell count.
    write_u16_be(
        &mut page.data,
        header.header_offset + OFFSET_CELL_COUNT,
        header.cell_count + 1,
    );

    Ok(cell_start as u16)
}

/// Remove a cell at `index` and add the freed space to the freeblock chain
/// instead of defragmenting the entire page.
pub(crate) fn remove_cell(page: &mut Page, is_page_one: bool, index: usize) -> Result<()> {
    let header = BTreePageHeaderV3::parse(page, is_page_one)?;
    if index >= header.cell_count as usize {
        return Err(HematiteError::StorageError(format!(
            "Cell index {} out of bounds for {} cells",
            index, header.cell_count
        )));
    }

    // Read the cell pointer to know where the cell body lives.
    let cell_offset = read_u16_be(&page.data, header.pointer_area_start() + index * 2) as usize;

    // Determine the cell size by finding the next cell boundary.
    let cell_size = compute_cell_size(page, &header, cell_offset)?;

    // Remove the cell pointer by shifting later pointers left.
    let pointer_start = header.pointer_area_start();
    let removal_offset = pointer_start + index * 2;
    let pointer_end = pointer_start + header.cell_count as usize * 2;
    page.data
        .copy_within(removal_offset + 2..pointer_end, removal_offset);
    // Zero out the vacated trailing pointer slot.
    write_u16_be(&mut page.data, pointer_end - 2, 0);

    // Update cell count.
    write_u16_be(
        &mut page.data,
        header.header_offset + OFFSET_CELL_COUNT,
        header.cell_count - 1,
    );

    // Handle freed body bytes:
    // - tiny freed regions become fragments (tracked in fragmented_free_bytes)
    // - if the freed region is exactly at the current cell_content_start, advance it
    // - otherwise, insert the freed region into the freeblock chain
    if cell_size < MIN_FREEBLOCK_SIZE {
        let frag_byte = header.header_offset + OFFSET_FRAGMENTED_FREE_BYTES;
        let old_frags = page.data[frag_byte] as usize;
        let new_frags = (old_frags + cell_size).min(255);
        page.data[frag_byte] = new_frags as u8;
    } else if cell_offset == header.cell_content_start as usize {
        // advance cell_content_start past the freed region
        let new_start = cell_offset + cell_size;
        write_u16_be(
            &mut page.data,
            header.header_offset + OFFSET_CELL_CONTENT_START,
            new_start as u16,
        );
    } else {
        // normal-sized freed region not at the head of the content area:
        // add it to the freeblock chain so it can be reused later.
        add_to_freeblock_chain(&mut page.data, header.header_offset, cell_offset, cell_size);
    }

    // Trigger defragmentation only if fragmentation is excessive (>60 bytes).
    let updated_header = BTreePageHeaderV3::parse(page, is_page_one)?;
    if updated_header.fragmented_free_bytes > 60 {
        defragment_page(page, is_page_one)?;
    }

    Ok(())
}

/// Add a freed region to the freeblock chain, maintaining sorted order by offset.
fn add_to_freeblock_chain(data: &mut [u8], header_offset: usize, offset: usize, size: usize) {
    let first_fb_ptr = header_offset + OFFSET_FIRST_FREEBLOCK;
    let mut prev_ptr_offset = first_fb_ptr;
    let mut fb_offset = read_u16_be(data, prev_ptr_offset) as usize;

    // Walk until we find the right insertion point (sorted by offset).
    while fb_offset != 0 && fb_offset < offset {
        prev_ptr_offset = fb_offset; // next-pointer is at the start
        fb_offset = read_u16_be(data, fb_offset) as usize;
    }

    // Insert new freeblock: point to current successor.
    write_u16_be(data, offset, fb_offset as u16);
    write_u16_be(data, offset + 2, size as u16);

    // Link predecessor to new freeblock.
    write_u16_be(data, prev_ptr_offset, offset as u16);
}

pub(crate) fn compute_cell_size(
    page: &Page,
    header: &BTreePageHeaderV3,
    cell_offset: usize,
) -> Result<usize> {
    if cell_offset >= PAGE_SIZE {
        return Err(HematiteError::CorruptedData(
            "Cell offset out of bounds".to_string(),
        ));
    }

    match header.kind {
        PageKind::LeafTable => {
            if cell_offset + 4 > PAGE_SIZE {
                return Err(HematiteError::CorruptedData(
                    "Leaf cell header truncated".to_string(),
                ));
            }
            let key_len =
                u16::from_be_bytes([page.data[cell_offset], page.data[cell_offset + 1]]) as usize;
            let value_len =
                u16::from_be_bytes([page.data[cell_offset + 2], page.data[cell_offset + 3]])
                    as usize;
            let total = 4 + key_len + value_len;
            if cell_offset + total > PAGE_SIZE {
                return Err(HematiteError::CorruptedData(
                    "Leaf cell content exceeds page bounds".to_string(),
                ));
            }
            Ok(total)
        }
        PageKind::InteriorTable => {
            if cell_offset + 6 > PAGE_SIZE {
                return Err(HematiteError::CorruptedData(
                    "Internal cell header truncated".to_string(),
                ));
            }
            let key_len =
                u16::from_be_bytes([page.data[cell_offset + 4], page.data[cell_offset + 5]])
                    as usize;
            let total = 6 + key_len;
            if cell_offset + total > PAGE_SIZE {
                return Err(HematiteError::CorruptedData(
                    "Internal cell key exceeds page bounds".to_string(),
                ));
            }
            Ok(total)
        }
        _ => Err(HematiteError::CorruptedData(
            "Unsupported page kind for exact cell sizing".to_string(),
        )),
    }
}

/// Defragment a page by repacking all cells tightly from the end of the page.
/// Clears the freeblock chain and fragment count.
pub(crate) fn defragment_page(page: &mut Page, is_page_one: bool) -> Result<()> {
    let header = BTreePageHeaderV3::parse(page, is_page_one)?;
    let cell_count = header.cell_count as usize;
    let pointer_start = header.pointer_area_start();

    // Extract cell bodies by exact size computed independently
    let mut cells: Vec<(usize, Vec<u8>)> = Vec::with_capacity(cell_count);
    for i in 0..cell_count {
        let offset = read_u16_be(&page.data, pointer_start + i * 2) as usize;
        let size = compute_cell_size(page, &header, offset)?;
        cells.push((i, page.data[offset..offset + size].to_vec()));
    }

    // Clear the cell content area.
    let pointer_area_end = pointer_start + cell_count * 2;
    page.data[pointer_area_end..PAGE_SIZE].fill(0);

    // Repack cells from the end.
    let mut content_start = PAGE_SIZE;
    // Process in reverse offset order so the largest offset is written first.
    for (ptr_index, cell_data) in cells.iter().rev() {
        content_start -= cell_data.len();
        page.data[content_start..content_start + cell_data.len()].copy_from_slice(cell_data);
        write_u16_be(
            &mut page.data,
            pointer_start + ptr_index * 2,
            content_start as u16,
        );
    }

    // Update header: new content start, clear freeblocks and fragments.
    write_u16_be(
        &mut page.data,
        header.header_offset + OFFSET_CELL_CONTENT_START,
        content_start as u16,
    );
    write_u16_be(
        &mut page.data,
        header.header_offset + OFFSET_FIRST_FREEBLOCK,
        0,
    );
    page.data[header.header_offset + OFFSET_FRAGMENTED_FREE_BYTES] = 0;
    Ok(())
}

fn header_offset(is_page_one: bool) -> usize {
    if is_page_one {
        DATABASE_HEADER_SIZE
    } else {
        0
    }
}

fn page_header_size(kind: PageKind) -> Result<usize> {
    match kind {
        PageKind::LeafIndex | PageKind::LeafTable => Ok(LEAF_HEADER_SIZE),
        PageKind::InteriorIndex | PageKind::InteriorTable => Ok(INTERIOR_HEADER_SIZE),
        _ => Err(HematiteError::StorageError(format!(
            "Page kind {:?} is not a v3 b-tree page",
            kind
        ))),
    }
}

fn read_u16_be(bytes: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes([bytes[offset], bytes[offset + 1]])
}

fn write_u16_be(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn read_u32_be(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn write_u32_be(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}
