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

pub(crate) fn initialize_btree_page(page: &mut Page, kind: PageKind, is_page_one: bool) -> Result<()> {
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
    write_u16_be(&mut page.data, header_offset + OFFSET_CELL_CONTENT_START, PAGE_SIZE as u16);
    page.data[header_offset + OFFSET_FRAGMENTED_FREE_BYTES] = 0;
    if kind.is_interior() {
        write_u32_be(&mut page.data, header_offset + OFFSET_RIGHTMOST_CHILD, 0);
    }
    Ok(())
}

pub(crate) fn cell_pointer(page: &Page, is_page_one: bool, index: usize) -> Result<u16> {
    let header = BTreePageHeaderV3::parse(page, is_page_one)?;
    if index >= header.cell_count as usize {
        return Err(HematiteError::StorageError(format!(
            "Cell index {} out of bounds for {} cells",
            index, header.cell_count
        )));
    }
    Ok(read_u16_be(&page.data, header.pointer_area_start() + index * 2))
}

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

    let required = cell_bytes.len() + 2;
    if header.writable_gap_size() < required {
        defragment_page(page, is_page_one)?;
        header = BTreePageHeaderV3::parse(page, is_page_one)?;
    }
    if header.writable_gap_size() < required {
        return Err(HematiteError::StorageError(
            "v3 b-tree page has insufficient contiguous space for new cell".to_string(),
        ));
    }

    let cell_start = header.cell_content_start as usize - cell_bytes.len();
    page.data[cell_start..header.cell_content_start as usize].copy_from_slice(cell_bytes);

    let pointer_start = header.pointer_area_start();
    let insertion_offset = pointer_start + sorted_index * 2;
    let pointer_end = pointer_start + header.cell_count as usize * 2;
    page.data.copy_within(insertion_offset..pointer_end, insertion_offset + 2);
    write_u16_be(&mut page.data, insertion_offset, cell_start as u16);

    write_u16_be(
        &mut page.data,
        header.header_offset + OFFSET_CELL_COUNT,
        header.cell_count + 1,
    );
    write_u16_be(
        &mut page.data,
        header.header_offset + OFFSET_CELL_CONTENT_START,
        cell_start as u16,
    );
    Ok(cell_start as u16)
}

pub(crate) fn remove_cell(page: &mut Page, is_page_one: bool, index: usize) -> Result<()> {
    let header = BTreePageHeaderV3::parse(page, is_page_one)?;
    if index >= header.cell_count as usize {
        return Err(HematiteError::StorageError(format!(
            "Cell index {} out of bounds for {} cells",
            index, header.cell_count
        )));
    }

    let pointer_start = header.pointer_area_start();
    let removal_offset = pointer_start + index * 2;
    let pointer_end = pointer_start + header.cell_count as usize * 2;
    page.data
        .copy_within(removal_offset + 2..pointer_end, removal_offset);
    write_u16_be(
        &mut page.data,
        header.header_offset + OFFSET_CELL_COUNT,
        header.cell_count - 1,
    );
    page.data[pointer_end - 2] = 0;
    page.data[pointer_end - 1] = 0;
    defragment_page(page, is_page_one)
}

pub(crate) fn defragment_page(page: &mut Page, is_page_one: bool) -> Result<()> {
    let header = BTreePageHeaderV3::parse(page, is_page_one)?;
    let mut cells = Vec::with_capacity(header.cell_count as usize);
    for index in 0..header.cell_count as usize {
        let start = cell_pointer(page, is_page_one, index)? as usize;
        let end = if index == 0 {
            PAGE_SIZE
        } else {
            cell_pointer(page, is_page_one, index - 1)? as usize
        };
        if start >= end || end > PAGE_SIZE {
            return Err(HematiteError::CorruptedData(
                "v3 b-tree cell pointer ordering is invalid".to_string(),
            ));
        }
        cells.push(page.data[start..end].to_vec());
    }

    let clear_from = header.pointer_area_start();
    page.data[clear_from..PAGE_SIZE].fill(0);

    let mut content_start = PAGE_SIZE;
    for (index, cell) in cells.iter().enumerate().rev() {
        content_start -= cell.len();
        page.data[content_start..content_start + cell.len()].copy_from_slice(cell);
        write_u16_be(
            &mut page.data,
            header.pointer_area_start() + index * 2,
            content_start as u16,
        );
    }

    write_u16_be(
        &mut page.data,
        header.header_offset + OFFSET_CELL_CONTENT_START,
        content_start as u16,
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

#[cfg(test)]
mod tests {
    use super::{
        cell_pointer, defragment_page, initialize_btree_page, insert_cell, remove_cell,
        set_rightmost_child, BTreePageHeaderV3,
    };
    use crate::storage::format::PageKind;
    use crate::storage::Page;

    #[test]
    fn initializes_leaf_page_with_page_one_offset() {
        let mut page = Page::new(1);
        initialize_btree_page(&mut page, PageKind::LeafTable, true).unwrap();

        let header = BTreePageHeaderV3::parse(&page, true).unwrap();
        assert_eq!(header.header_offset, 100);
        assert_eq!(header.cell_count, 0);
        assert_eq!(header.cell_content_start, crate::storage::PAGE_SIZE as u16);
    }

    #[test]
    fn initializes_interior_page_with_rightmost_child_slot() {
        let mut page = Page::new(2);
        initialize_btree_page(&mut page, PageKind::InteriorTable, false).unwrap();
        set_rightmost_child(&mut page, false, 99).unwrap();

        let header = BTreePageHeaderV3::parse(&page, false).unwrap();
        assert_eq!(header.rightmost_child, Some(99));
    }

    #[test]
    fn inserts_cells_and_tracks_pointer_order() {
        let mut page = Page::new(2);
        initialize_btree_page(&mut page, PageKind::LeafTable, false).unwrap();

        let first = vec![1u8; 8];
        let second = vec![2u8; 5];
        insert_cell(&mut page, false, 0, &first).unwrap();
        insert_cell(&mut page, false, 1, &second).unwrap();

        let header = BTreePageHeaderV3::parse(&page, false).unwrap();
        assert_eq!(header.cell_count, 2);
        let first_ptr = cell_pointer(&page, false, 0).unwrap();
        let second_ptr = cell_pointer(&page, false, 1).unwrap();
        assert!(first_ptr > second_ptr);
    }

    #[test]
    fn remove_cell_defragments_page() {
        let mut page = Page::new(2);
        initialize_btree_page(&mut page, PageKind::LeafTable, false).unwrap();
        insert_cell(&mut page, false, 0, &[1u8; 8]).unwrap();
        insert_cell(&mut page, false, 1, &[2u8; 6]).unwrap();
        insert_cell(&mut page, false, 2, &[3u8; 4]).unwrap();

        remove_cell(&mut page, false, 1).unwrap();

        let header = BTreePageHeaderV3::parse(&page, false).unwrap();
        assert_eq!(header.cell_count, 2);
        assert_eq!(header.fragmented_free_bytes, 0);
        assert!(header.pointer_area_end() <= header.cell_content_start as usize);
    }

    #[test]
    fn explicit_defragmentation_keeps_cell_count_stable() {
        let mut page = Page::new(2);
        initialize_btree_page(&mut page, PageKind::LeafTable, false).unwrap();
        insert_cell(&mut page, false, 0, &[1u8; 7]).unwrap();
        insert_cell(&mut page, false, 1, &[2u8; 9]).unwrap();

        defragment_page(&mut page, false).unwrap();

        let header = BTreePageHeaderV3::parse(&page, false).unwrap();
        assert_eq!(header.cell_count, 2);
        assert_eq!(header.fragmented_free_bytes, 0);
    }
}
