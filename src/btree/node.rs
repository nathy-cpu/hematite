//! # B-Tree Node Layout, Lazy Decoding, and Mutations (`node`)
//!
//! This module coordinates in-memory representations of B-Tree pages, binary search lookups, and page mutation
//! algorithms such as node splits, cell insertions, deletions, and balance operations.
//!
//! ---
//!
//! ## 1. Core Database B-Tree Node Concepts
//!
//! ### B-Tree Structure Overview
//! Clustered indexes and tables in a database are represented as B+ Trees:
//! * **Interior Nodes (Routing)**: Contain separator keys and child page IDs. They do not store data rows.
//!   Their purpose is to route the search path to the correct leaf page.
//! * **Leaf Nodes (Data)**: Contain actual data rows (keys and values). They do not contain children pointers.
//!
//! ```text
//!                              +------------------------+
//!                              |     [Interior Node]    |
//!                              | Key 20 | Key 50 | P3   |
//!                              +---+--------+--------+--+
//!                                 /         |         \
//!                         Keys < 20     20 <= Keys < 50 \ Keys >= 50
//!                               /           |             \
//!                              v            v              v
//!                 +----------------+ +----------------+ +----------------+
//!                 |  [Leaf Node]   | |  [Leaf Node]   | |  [Leaf Node]   |
//!                 | Row 10 | Row 15| | Row 20 | Row 45| | Row 50 | Row 80|
//!                 +----------------+ +----------------+ +----------------+
//! ```
//!
//! ### Lazy Node Decoding
//! Creating high-level Rust structs with allocated vectors for every page read is computationally
//! expensive and increases GC/allocation pressure.
//! To avoid this, Hematite implements **Lazy Node Decoding**:
//! * **Lazy Read State**: The `BTreeNode` struct is constructed by wrapping a raw page buffer. It does *not*
//!   parse the cells. To check if a key exists or to route an interior node, it performs a binary search directly
//!   on the page bytes using the cell pointer array.
//! * **Decoded Mutation State**: Only when a cell is inserted, updated, or deleted does the node transition
//!   into a fully-materialized state. Cells are parsed into memory vectors, mutated, and then serialized back
//!   into a packed slotted page layout.
//!
//! ### B-Tree Node Mutations
//! * **Cell Insertion**: Inserts a key-value cell. If the page lacks space (even after defragmentation), a **Split** is triggered.
//! * **Node Split**: Creates a new sibling page. Half of the cells are moved to the new page, and a separator key is propagated to the parent node.
//! * **Node Borrow / Merge**: When a deletion causes a page to drop below its minimum fill factor (underflow),
//!   it attempts to borrow cells from a adjacent sibling. If the sibling is also near underflow, the two pages are merged.
//!
//! ---
//!
//! ## 2. In-Memory Struct Transitions
//!
//! ```text
//!               +--------------------------------------+
//!               |       Page buffer read from Pager    |
//!               +-------------------+------------------+
//!                                   |
//!                         BTreeNode::from_page()
//!                                   v
//!               +--------------------------------------+
//!               |           BTreeNode (Lazy)           |
//!               |    * Performs binary searches on     |
//!               |      in-place page buffer.           |
//!               |    * Zero memory allocations.        |
//!               +-------------------+------------------+
//!                                   |
//!                             Cell Mutation
//!                                   v
//!               +--------------------------------------+
//!               |          Decoded Cell List           |
//!               |    * Cells materialized into vectors |
//!               |    * Mutation performed in memory.   |
//!               +-------------------+------------------+
//!                                   |
//!                         Write / Serialize
//!                                   v
//!               +--------------------------------------+
//!               |        Encoded Page Buffer           |
//!               |    * Slotted page layout re-packed.  |
//!               |    * Written back to Pager.          |
//!               +--------------------------------------+
//! ```
//!
//! ---
//!
//! ## 3. B-Tree Invariants
//!
//! 1. **Sorted Order**: All cell keys within a node must be sorted monotonically.
//! 2. **Routing Integrity**: In an interior node, the subtree pointed to by Child ID $I$ must contain only keys
//!    greater than or equal to Separator Key $I-1$ and strictly less than Separator Key $I$.
//! 3. **Size Limits**: Cell keys must not exceed `MAX_KEY_SIZE` (256 bytes) and values must not exceed `MAX_VALUE_SIZE` (1024 bytes).
//!

use crate::btree::page_format::{
    compute_cell_size, insert_cell as pf_insert_cell, remove_cell as pf_remove_cell,
    total_free_space as pf_total_free_space, BTreePageHeaderV3,
};
use crate::btree::varint;
use crate::btree::{BTreeKey, BTreeValue, NodeType, BTREE_ORDER};
use crate::error::{HematiteError, Result};
use crate::storage::format::{PageKind, DATABASE_HEADER_SIZE};
#[cfg(test)]
use crate::storage::INVALID_PAGE_ID;
use crate::storage::{Page, PageId, Pager, PAGE_SIZE};
use std::collections::VecDeque;
use std::sync::Arc;

pub const MAX_KEY_SIZE: usize = 256;
pub const MAX_VALUE_SIZE: usize = 1024;
pub const CHILD_ID_SIZE: usize = 4;
pub const MAX_KEYS: usize = BTREE_ORDER - 1;
#[cfg(test)]
pub const BTREE_PAGE_HEADER_SIZE: usize = LEAF_HEADER_SIZE;

const LEAF_HEADER_SIZE: usize = 8;
const INTERIOR_HEADER_SIZE: usize = 12;

#[derive(Debug, Clone)]
pub struct BTreeNode {
    pub page_id: PageId,
    pub node_type: NodeType,
    pub keys: VecDeque<BTreeKey>,
    pub children: VecDeque<PageId>,
    pub values: VecDeque<BTreeValue>,

    pub key_count: usize,
    pub payload_len: usize,
    pub raw_page: Option<Arc<Page>>,
    pub is_decoded: bool,
    /// The parsed header of `raw_page`, kept so per-cell access does not re-parse it.
    ///
    /// `cell_range_for_index` needs the header, and both `get_key_view` and
    /// `get_value_view` call it, so parsing on demand cost two header parses per row
    /// on a scan — enough to cancel out what removing the eager whole-page pass
    /// saved. Every construction path already parses the header, so holding on to it
    /// is free.
    pub(crate) header: Option<BTreePageHeaderV3>,
}

impl BTreeNode {
    pub fn new_internal(page_id: PageId) -> Self {
        Self {
            page_id,
            node_type: NodeType::Internal,
            keys: VecDeque::new(),
            children: VecDeque::new(),
            values: VecDeque::new(),
            key_count: 0,
            payload_len: 0,
            raw_page: None,
            is_decoded: true,
            header: None,
        }
    }

    pub fn new_leaf(page_id: PageId) -> Self {
        Self {
            page_id,
            node_type: NodeType::Leaf,
            keys: VecDeque::new(),
            children: VecDeque::new(),
            values: VecDeque::new(),
            key_count: 0,
            payload_len: 0,
            raw_page: None,
            is_decoded: true,
            header: None,
        }
    }

    pub fn validate_key_size(key: &BTreeKey) -> Result<()> {
        if key.data.len() > MAX_KEY_SIZE {
            return Err(HematiteError::StorageError(format!(
                "Key size {} exceeds maximum allowed size {}",
                key.data.len(),
                MAX_KEY_SIZE
            )));
        }
        Ok(())
    }

    pub fn validate_value_size(value: &BTreeValue) -> Result<()> {
        if value.data.len() > MAX_VALUE_SIZE {
            return Err(HematiteError::StorageError(format!(
                "Value size {} exceeds maximum allowed size {}",
                value.data.len(),
                MAX_VALUE_SIZE
            )));
        }
        Ok(())
    }

    pub fn estimate_serialized_size(&self) -> usize {
        let header_size = match self.node_type {
            NodeType::Leaf => LEAF_HEADER_SIZE,
            NodeType::Internal => INTERIOR_HEADER_SIZE,
        };
        let cell_body_bytes = match self.node_type {
            NodeType::Leaf => self
                .keys
                .iter()
                .zip(self.values.iter())
                .map(|(key, value)| leaf_cell_size(key, value))
                .sum::<usize>(),
            NodeType::Internal => self.keys.iter().map(internal_cell_size).sum::<usize>(),
        };
        header_size + self.keys.len() * 2 + cell_body_bytes
    }

    pub fn serialized_size_on_page(&self) -> Result<usize> {
        if self.is_decoded {
            return Ok(self.estimate_serialized_size());
        }

        let header_size = match self.node_type {
            NodeType::Leaf => LEAF_HEADER_SIZE,
            NodeType::Internal => INTERIOR_HEADER_SIZE,
        };

        // Sum cell body bytes without allocating an intermediate Vec.
        let mut cell_body_bytes = 0usize;
        for index in 0..self.key_count {
            let (start, end) = self.cell_range_for_index(index)?;
            cell_body_bytes += end - start;
        }

        Ok(header_size + self.key_count * 2 + cell_body_bytes)
    }

    /// Whether this node's decoded contents fit in a page.
    ///
    /// Test-only now. `can_merge_with` used to reach it by building the merged node;
    /// it answers from byte counts instead, and this survives as the slow, obvious
    /// reference implementation that the differential merge test checks against.
    #[cfg(test)]
    pub fn will_fit_in_page(&self) -> bool {
        self.estimate_serialized_size() + page_header_offset(self.page_id) <= PAGE_SIZE
    }

    pub fn can_insert_key_value(&self, key: &BTreeKey, value: &BTreeValue) -> bool {
        if !matches!(self.node_type, NodeType::Leaf) {
            return false;
        }
        self.estimate_serialized_size()
            + leaf_cell_size(key, value)
            + 2
            + page_header_offset(self.page_id)
            <= PAGE_SIZE
    }

    pub fn can_insert_key_child(&self, key: &BTreeKey) -> bool {
        if !matches!(self.node_type, NodeType::Internal) {
            return false;
        }
        self.estimate_serialized_size()
            + internal_cell_size(key)
            + 2
            + page_header_offset(self.page_id)
            <= PAGE_SIZE
    }

    pub fn from_page(page: Page) -> Result<Self> {
        Self::from_shared_page(Arc::new(page))
    }

    pub fn from_shared_page(page: Arc<Page>) -> Result<Self> {
        if page.data.len() != PAGE_SIZE {
            return Err(HematiteError::InvalidPage(page.id));
        }

        let is_page_one = is_page_one(page.id);
        let header = BTreePageHeaderV3::parse(&page, is_page_one)?;
        let node_type = match header.kind {
            PageKind::LeafTable => NodeType::Leaf,
            PageKind::InteriorTable => NodeType::Internal,
            _ => {
                return Err(HematiteError::CorruptedData(format!(
                    "Unsupported B-tree page kind {:?}",
                    header.kind
                )))
            }
        };
        if header.cell_count as usize > MAX_KEYS {
            return Err(HematiteError::CorruptedData(format!(
                "Key count {} exceeds maximum {}",
                header.cell_count, MAX_KEYS
            )));
        }

        let mut node = match node_type {
            NodeType::Leaf => Self::new_leaf(page.id),
            NodeType::Internal => Self::new_internal(page.id),
        };
        node.key_count = header.cell_count as usize;
        node.payload_len = PAGE_SIZE.saturating_sub(header.cell_content_start as usize);
        node.raw_page = Some(page.clone());
        node.is_decoded = false;
        node.header = Some(header);
        // Whole-page structural validation used to run here, on every page loaded,
        // including pages a cursor merely navigates past — ~28-30 % of a batched
        // insert. It stood in for `key_view`/`value_view`, which swallowed per-cell
        // decode errors with `.ok()`; those propagate now, so a cell that is read is
        // checked at the read. The whole-page form is [`BTreeNode::verify`], for
        // callers that want the page examined rather than used.
        Ok(node)
    }

    pub fn from_page_decoded(page: Page) -> Result<Self> {
        let mut node = Self::from_page(page)?;
        node.decode()?;
        Ok(node)
    }

    pub fn from_shared_page_decoded(page: Arc<Page>) -> Result<Self> {
        let mut node = Self::from_shared_page(page)?;
        node.decode()?;
        Ok(node)
    }

    pub fn decode(&mut self) -> Result<()> {
        if self.is_decoded {
            return Ok(());
        }

        let page = self
            .raw_page
            .as_ref()
            .ok_or_else(|| HematiteError::CorruptedData("Missing raw page".to_string()))?;
        let header = BTreePageHeaderV3::parse(page, is_page_one(self.page_id))?;

        match self.node_type {
            NodeType::Leaf => {
                for i in 0..self.key_count {
                    let (start, end) = self.cell_range_for_index(i)?;
                    let (key, value) = parse_leaf_cell(&page.data[start..end])?;
                    self.keys.push_back(key);
                    self.values.push_back(value);
                }
            }
            NodeType::Internal => {
                for i in 0..self.key_count {
                    let (start, end) = self.cell_range_for_index(i)?;
                    let (left_child, key) = parse_internal_cell(&page.data[start..end])?;
                    self.children.push_back(left_child);
                    self.keys.push_back(key);
                }
                let rightmost = header.rightmost_child.unwrap_or(0);
                if rightmost == 0 {
                    return Err(HematiteError::CorruptedData(
                        "Internal node is missing its rightmost child".to_string(),
                    ));
                }
                self.children.push_back(rightmost);
            }
        }
        self.is_decoded = true;
        Ok(())
    }

    pub(crate) fn validate_cell_layouts(&self) -> Result<()> {
        let page = self.raw_page.as_ref().ok_or_else(|| {
            HematiteError::CorruptedData("Missing raw page for lazy node".to_string())
        })?;

        match self.node_type {
            NodeType::Leaf => {
                for index in 0..self.key_count {
                    let (start, end) = self.cell_range_for_index(index)?;
                    parse_leaf_cell(&page.data[start..end])?;
                }
            }
            NodeType::Internal => {
                for index in 0..self.key_count {
                    let (start, end) = self.cell_range_for_index(index)?;
                    parse_internal_cell(&page.data[start..end])?;
                }
                let _ = self.get_child_procedural(self.key_count)?;
            }
        }

        Ok(())
    }

    pub fn get_key_view(&self, target_index: usize) -> Result<&[u8]> {
        if target_index >= self.key_count {
            return Err(HematiteError::StorageError(
                "Index out of bounds".to_string(),
            ));
        }

        if self.is_decoded {
            return Ok(&self.keys[target_index].data);
        }

        let page = self.raw_page.as_ref().ok_or_else(|| {
            HematiteError::CorruptedData("Missing raw page for lazy node".to_string())
        })?;
        let (start, end) = self.cell_range_for_index(target_index)?;
        let key_range = match self.node_type {
            NodeType::Leaf => leaf_cell_key_range(&page.data[start..end])?,
            NodeType::Internal => internal_cell_key_range(&page.data[start..end])?,
        };
        Ok(&page.data[start + key_range.0..start + key_range.1])
    }

    pub fn get_child_procedural(&self, target_index: usize) -> Result<PageId> {
        if self.node_type != NodeType::Internal || target_index > self.key_count {
            return Err(HematiteError::StorageError(
                "Index out of bounds".to_string(),
            ));
        }

        if self.is_decoded {
            return Ok(self.children[target_index]);
        }

        let page = self.raw_page.as_ref().ok_or_else(|| {
            HematiteError::CorruptedData("Missing raw page for lazy node".to_string())
        })?;
        if target_index == self.key_count {
            let header = BTreePageHeaderV3::parse(page, is_page_one(self.page_id))?;
            let rightmost = header.rightmost_child.ok_or_else(|| {
                HematiteError::CorruptedData(
                    "Internal node is missing its rightmost child".to_string(),
                )
            })?;
            if rightmost <= 1 {
                return Err(HematiteError::CorruptedData(
                    "Internal node rightmost child cannot reference reserved pages".to_string(),
                ));
            }
            return Ok(rightmost);
        }
        let (start, end) = self.cell_range_for_index(target_index)?;
        let (child, _) = parse_internal_cell(&page.data[start..end])?;
        Ok(child)
    }

    pub fn get_value_view(&self, target_index: usize) -> Result<&[u8]> {
        if self.node_type != NodeType::Leaf || target_index >= self.key_count {
            return Err(HematiteError::StorageError(
                "Index out of bounds".to_string(),
            ));
        }

        if self.is_decoded {
            return Ok(&self.values[target_index].data);
        }

        let page = self.raw_page.as_ref().ok_or_else(|| {
            HematiteError::CorruptedData("Missing raw page for lazy node".to_string())
        })?;
        let (start, end) = self.cell_range_for_index(target_index)?;
        let value_range = leaf_cell_value_range(&page.data[start..end])?;
        Ok(&page.data[start + value_range.0..start + value_range.1])
    }

    pub fn get_value_procedural(&self, target_index: usize) -> Result<BTreeValue> {
        self.get_value_view(target_index)
            .map(|v| BTreeValue::new(v.to_vec()))
    }

    pub fn to_page(&self, page: &mut Page) -> Result<()> {
        if self.keys.len() > MAX_KEYS {
            return Err(HematiteError::StorageError(format!(
                "Node has {} keys, exceeds max {}",
                self.keys.len(),
                MAX_KEYS
            )));
        }
        if matches!(self.node_type, NodeType::Internal)
            && self.children.len() != self.keys.len() + 1
        {
            return Err(HematiteError::StorageError(
                "Internal node children must equal keys + 1".to_string(),
            ));
        }
        if matches!(self.node_type, NodeType::Leaf) && self.values.len() != self.keys.len() {
            return Err(HematiteError::StorageError(
                "Leaf node values must equal keys".to_string(),
            ));
        }

        let kind = match self.node_type {
            NodeType::Leaf => PageKind::LeafTable,
            NodeType::Internal => PageKind::InteriorTable,
        };
        initialize_page_bytes(page, kind)?;

        let header_offset = page_header_offset(page.id);
        let header_size = match self.node_type {
            NodeType::Leaf => LEAF_HEADER_SIZE,
            NodeType::Internal => INTERIOR_HEADER_SIZE,
        };
        let pointer_area_start = header_offset + header_size;

        // Compute total bytes required for all cells without allocating a Vec per-cell.
        let mut total_cell_bytes: usize = 0;
        for index in 0..self.keys.len() {
            let cell_len = match self.node_type {
                NodeType::Leaf => {
                    Self::validate_key_size(&self.keys[index])?;
                    Self::validate_value_size(&self.values[index])?;
                    leaf_cell_size(&self.keys[index], &self.values[index])
                }
                NodeType::Internal => {
                    Self::validate_key_size(&self.keys[index])?;
                    internal_cell_size(&self.keys[index])
                }
            };
            total_cell_bytes += cell_len;
        }

        let total_size =
            page_header_offset(page.id) + header_size + self.keys.len() * 2 + total_cell_bytes;
        if total_size > PAGE_SIZE {
            return Err(HematiteError::StorageError(format!(
                "Serialized B-tree node exceeds page size: {} bytes",
                total_size
            )));
        }

        // Fill cells directly into the page buffer from the end backwards to avoid
        // allocating per-cell Vec<u8>. For each cell, compute its length and write
        // header + key (+ value) bytes straight into the page.data slice.
        let mut content_start = PAGE_SIZE;
        for index in (0..self.keys.len()).rev() {
            // Must agree exactly with `leaf_cell_size`/`internal_cell_size`, which is
            // what reserved the space above and what `compute_cell_size` will read
            // back. Duplicating the arithmetic here is how the fixed-width framing
            // survived in one place after the rest moved to varints.
            let cell_len = match self.node_type {
                NodeType::Leaf => leaf_cell_size(&self.keys[index], &self.values[index]),
                NodeType::Internal => internal_cell_size(&self.keys[index]),
            };

            content_start -= cell_len;
            let dest_start = content_start;
            let mut offs = 0usize;

            match self.node_type {
                NodeType::Leaf => {
                    // varint key len, varint value len, key bytes, value bytes
                    let key_bytes_len = self.keys[index].data.len();
                    let val_bytes_len = self.values[index].data.len();
                    offs +=
                        varint::write(&mut page.data[dest_start + offs..], key_bytes_len as u64);
                    offs +=
                        varint::write(&mut page.data[dest_start + offs..], val_bytes_len as u64);
                    let key_bytes = &self.keys[index].data;
                    page.data[dest_start + offs..dest_start + offs + key_bytes.len()]
                        .copy_from_slice(key_bytes);
                    offs += key_bytes.len();
                    let val_bytes = &self.values[index].data;
                    page.data[dest_start + offs..dest_start + offs + val_bytes.len()]
                        .copy_from_slice(val_bytes);
                }
                NodeType::Internal => {
                    // 4 bytes left child, varint key len, then key bytes
                    let child = self.children[index];
                    dest_copy_u32_be(&mut page.data, dest_start + offs, child);
                    offs += CHILD_ID_SIZE;
                    let key_len = self.keys[index].data.len();
                    offs += varint::write(&mut page.data[dest_start + offs..], key_len as u64);
                    let key_bytes = &self.keys[index].data;
                    page.data[dest_start + offs..dest_start + offs + key_bytes.len()]
                        .copy_from_slice(key_bytes);
                }
            }

            // Write cell pointer into pointer array.
            write_u16_be(
                &mut page.data,
                pointer_area_start + index * 2,
                content_start as u16,
            );
        }

        write_u16_be(&mut page.data, header_offset + 3, self.keys.len() as u16);
        write_u16_be(&mut page.data, header_offset + 5, content_start as u16);
        page.data[header_offset + 7] = 0;
        if self.node_type == NodeType::Internal {
            write_u32_be(
                &mut page.data,
                header_offset + 8,
                self.children.back().copied().ok_or_else(|| {
                    HematiteError::StorageError(
                        "Internal node is missing its rightmost child".to_string(),
                    )
                })?,
            );
        }
        Ok(())
    }

    pub fn try_update_leaf_in_place(
        &mut self,
        page: &mut Page,
        key: &BTreeKey,
        new_value: &BTreeValue,
    ) -> Result<bool> {
        if self.node_type != NodeType::Leaf {
            return Ok(false);
        }

        // Binary search, not a scan. Keys on a page are sorted and `exact_key_index`
        // has always been here; walking every key to find one was O(n) on the hottest
        // update path there is — a same-length overwrite, which is what an UPDATE of a
        // fixed-width column does to every row it touches.
        let Some(index) = self.exact_key_index(key)? else {
            return Ok(false);
        };

        let current_len = self.get_value_view(index)?.len();
        if current_len != new_value.data.len() {
            return Ok(false);
        }

        let (start, end) = self.cell_range_for_index(index)?;
        let cell = &mut page.data[start..end];
        let (key_range, value_range) = leaf_cell_ranges(cell)?;
        cell[key_range.0..key_range.1].copy_from_slice(key.as_bytes());
        cell[value_range.0..value_range.1].copy_from_slice(new_value.as_bytes());

        if self.is_decoded {
            self.values[index] = new_value.clone();
        }
        // The page has been mutated, so this node's borrowed view of it is stale. It is
        // dropped rather than refreshed: refreshing meant `Arc::new(page.clone())`, a
        // full 4 KiB copy plus an allocation, immediately after the byte edit this
        // whole path exists to make cheap — and the caller then handed the same page
        // to `write_page`, which wraps it in an `Arc` again. Two page-sized
        // allocations for a single-cell change.
        //
        // Every caller writes the page and returns, so nothing reads through the node
        // afterwards. Anything that tries gets a clean error from the view accessors
        // rather than stale bytes, and the failure paths above return before mutating,
        // so the `decode()` fallback still has its page.
        self.raw_page = None;
        Ok(true)
    }

    /// Try to insert a key-value pair directly into the page bytes without a
    /// full decode + rebuild cycle.  Returns `true` if successful, `false` if
    /// there isn't enough space on the page (caller should fall back to split).
    pub fn try_insert_leaf_in_place(
        &mut self,
        page: &mut Page,
        key: &BTreeKey,
        value: &BTreeValue,
    ) -> Result<bool> {
        if self.node_type != NodeType::Leaf {
            return Ok(false); // Only supported for leaf nodes currently
        }

        if self.key_count >= MAX_KEYS {
            return Ok(false);
        }

        Self::validate_key_size(key)?;
        Self::validate_value_size(value)?;

        // Check for duplicate key — updates must go through the normal path.
        if self.exact_key_index(key)?.is_some() {
            return Ok(false);
        }

        let is_p1 = is_page_one(page.id);
        let cell_bytes = encode_leaf_cell(key, value);
        let needed = cell_bytes.len() + 2; // +2 for cell pointer

        // Check total free space on the page.
        let free = pf_total_free_space(page, is_p1)?;
        if free < needed {
            return Ok(false);
        }

        // Find insertion position via binary search on the lazy node.
        let pos = self.lower_bound_index(key)?;

        // Use the page_format insert_cell to do the work.
        pf_insert_cell(page, is_p1, pos, &cell_bytes)?;

        // Invalidate the lazy node state so the next read re-parses.
        // Dropped, not refreshed; see `try_update_leaf_in_place` for why.
        self.raw_page = None;
        self.is_decoded = false;
        let header = BTreePageHeaderV3::parse(page, is_p1)?;
        self.key_count = header.cell_count as usize;
        self.payload_len = PAGE_SIZE.saturating_sub(header.cell_content_start as usize);
        self.header = Some(header);
        self.keys.clear();
        self.values.clear();
        self.children.clear();
        Ok(true)
    }

    /// Try to remove a cell at `index` directly on the page bytes without
    /// a full decode + rebuild.  Returns `true` if successful.
    pub fn try_remove_cell_in_place(&mut self, page: &mut Page, index: usize) -> Result<bool> {
        if index >= self.key_count {
            return Ok(false);
        }
        let is_p1 = is_page_one(page.id);

        pf_remove_cell(page, is_p1, index)?;

        // Refresh lazy state from the updated page.
        // Dropped, not refreshed; see `try_update_leaf_in_place` for why.
        self.raw_page = None;
        self.is_decoded = false;
        let header = BTreePageHeaderV3::parse(page, is_p1)?;
        self.key_count = header.cell_count as usize;
        self.payload_len = PAGE_SIZE.saturating_sub(header.cell_content_start as usize);
        self.header = Some(header);
        self.keys.clear();
        self.values.clear();
        self.children.clear();
        Ok(true)
    }

    /// balance_quick: Append-optimized leaf split.
    ///
    /// When the page is full and the new key is strictly greater than every
    /// existing key (append pattern), we skip the full decode → redistribute →
    /// rebuild cycle and instead:
    /// 1. allocate one fresh leaf page
    /// 2. write only the new cell into it
    /// 3. return (split_key, new_page_id) — the original page is untouched
    ///
    /// Returns `Ok(None)` when preconditions are not met (caller falls back to
    /// the full `split_leaf` path).
    pub fn try_split_leaf_quick(
        &mut self,
        storage: &mut Pager,
        key: BTreeKey,
        value: BTreeValue,
    ) -> Result<Option<(BTreeKey, PageId)>> {
        // Only applicable to leaf nodes.
        if self.node_type != NodeType::Leaf {
            return Ok(None);
        }
        // Must have at least one existing key to compare against.
        if self.key_count == 0 {
            return Ok(None);
        }
        // Check append condition: new key must be greater than every existing key.
        // Uses the lazy zero-copy accessor — no decode required.
        let last_key_bytes = self.get_key_view(self.key_count - 1)?;
        if key.as_bytes() <= last_key_bytes {
            return Ok(None);
        }

        Self::validate_key_size(&key)?;
        Self::validate_value_size(&value)?;

        let new_page_id = storage.allocate_page()?;
        let mut new_page = storage.new_page_buffer(new_page_id)?;
        // Initialize page bytes as an empty leaf.
        initialize_page_bytes(&mut new_page, PageKind::LeafTable)?;

        // Write the single new cell into the fresh page.
        let cell_bytes = encode_leaf_cell(&key, &value);
        let is_p1 = is_page_one(new_page_id);
        pf_insert_cell(&mut new_page, is_p1, 0, &cell_bytes)?;

        // The split key for an append is the new key itself (the minimum key of
        // the right sibling, following the B+ tree convention).
        let split_key = key;

        storage.write_page(new_page)?;
        Ok(Some((split_key, new_page_id)))
    }

    /// Try to insert a separator key + child pointer directly into the page
    /// bytes of an internal node without a full decode + rebuild cycle.
    ///
    /// Internal cell format: `[4-byte left_child][2-byte key_len][key_bytes]`
    /// The rightmost child is stored in the page header.
    ///
    /// When a child at position `pos` splits, the split result is
    /// `(split_key, new_right_child)`. We insert a new cell at `pos` whose
    /// left-child pointer is the existing child-at-pos, and then overwrite
    /// the left-child pointer of the cell that shifted to `pos+1` (or the
    /// rightmost-child header slot) with `new_right_child`.
    ///
    /// Returns `true` if successful, `false` if no space (caller does full
    /// decode + split_internal).
    pub fn try_insert_internal_in_place(
        &mut self,
        page: &mut Page,
        key: &BTreeKey,
        new_child: PageId,
    ) -> Result<bool> {
        if self.node_type != NodeType::Internal {
            return Ok(false);
        }
        if self.key_count >= MAX_KEYS {
            return Ok(false);
        }

        Self::validate_key_size(key)?;

        let is_p1 = is_page_one(page.id);
        let old_key_count = self.key_count;

        // Find insertion position (same as insert_internal: upper_bound).
        let pos = self.upper_bound_index(key)?;

        // Determine the left-child pointer for the new cell.
        // This is the child pointer that currently occupies position `pos` in
        // the logical children array.
        let left_child: PageId = if pos < old_key_count {
            // Read from the cell currently at `pos` — first 4 bytes are child ptr.
            // Parsed from the `page` argument rather than `self.header`: this reads
            // into that buffer, and nothing here guarantees it is the same one
            // `raw_page` holds. One parse per internal in-place insert, which only
            // happens when a split propagates.
            let page_header = BTreePageHeaderV3::parse(page, is_p1)?;
            let cell_offset = cell_pointer_from_header(page, &page_header, pos)? as usize;
            u32::from_be_bytes([
                page.data[cell_offset],
                page.data[cell_offset + 1],
                page.data[cell_offset + 2],
                page.data[cell_offset + 3],
            ])
        } else {
            // pos == key_count: the child here is the rightmost child in header.
            let header = BTreePageHeaderV3::parse(page, is_p1)?;
            header.rightmost_child.ok_or_else(|| {
                HematiteError::StorageError(
                    "Internal page missing rightmost child during in-place insert".to_string(),
                )
            })?
        };

        // Encode the new internal cell: [left_child][key_len][key_bytes]
        let cell_bytes = encode_internal_cell_raw(left_child, key);

        let needed = cell_bytes.len() + 2; // +2 for the new cell pointer
        let free = pf_total_free_space(page, is_p1)?;
        if free < needed {
            return Ok(false);
        }

        // Insert the cell at position `pos` (shifts later pointers right).
        pf_insert_cell(page, is_p1, pos, &cell_bytes)?;

        // After insertion, the cell that was at `pos` is now at `pos + 1`.
        // Its left-child pointer still holds `left_child` but should be
        // `new_child` (the right half of the split).
        let new_header = BTreePageHeaderV3::parse(page, is_p1)?;
        if (pos + 1) < new_header.cell_count as usize {
            // Overwrite the first 4 bytes of the shifted cell.
            let shifted_offset = cell_pointer_from_header(page, &new_header, pos + 1)? as usize;
            page.data[shifted_offset..shifted_offset + 4].copy_from_slice(&new_child.to_be_bytes());
        } else {
            // The new cell was appended as the last cell; update rightmost child.
            let header_offset = page_header_offset(page.id);
            write_u32_be(&mut page.data, header_offset + 8, new_child);
        }

        // Refresh lazy state (same pattern as try_insert_leaf_in_place).
        // Dropped, not refreshed; see `try_update_leaf_in_place` for why.
        self.raw_page = None;
        self.is_decoded = false;
        self.key_count = new_header.cell_count as usize;
        self.payload_len = PAGE_SIZE.saturating_sub(new_header.cell_content_start as usize);
        self.header = Some(new_header);
        self.keys.clear();
        self.values.clear();
        self.children.clear();
        Ok(true)
    }

    #[cfg(test)]
    pub fn search(&self, key: &BTreeKey) -> Result<SearchResult> {
        match self.node_type {
            NodeType::Leaf => self.search_leaf(key),
            NodeType::Internal => self.search_internal(key),
        }
    }

    #[cfg(test)]
    fn search_leaf(&self, key: &BTreeKey) -> Result<SearchResult> {
        if let Some(index) = self.exact_key_index(key)? {
            Ok(SearchResult::Found(self.get_value_procedural(index)?))
        } else {
            Ok(SearchResult::NotFound(INVALID_PAGE_ID))
        }
    }

    #[cfg(test)]
    fn search_internal(&self, key: &BTreeKey) -> Result<SearchResult> {
        Ok(SearchResult::NotFound(
            self.get_child_procedural(self.upper_bound_index(key)?)?,
        ))
    }

    pub fn find_child(&self, key: &BTreeKey) -> Result<PageId> {
        self.get_child_procedural(self.upper_bound_index(key)?)
    }

    pub fn insert_leaf(&mut self, key: BTreeKey, value: BTreeValue) -> Result<()> {
        self.decode()?;
        Self::validate_key_size(&key)?;
        Self::validate_value_size(&value)?;

        if let Some(pos) = self.keys.iter().position(|k| k == &key) {
            self.values[pos] = value;
            self.key_count = self.keys.len();
            return Ok(());
        }

        let pos = self
            .keys
            .iter()
            .position(|k| k > &key)
            .unwrap_or(self.keys.len());
        self.keys.insert(pos, key);
        self.values.insert(pos, value);
        self.key_count = self.keys.len();
        Ok(())
    }

    pub fn insert_internal(&mut self, key: BTreeKey, child_page_id: PageId) -> Result<()> {
        self.decode()?;
        Self::validate_key_size(&key)?;
        let pos = self
            .keys
            .iter()
            .position(|k| k >= &key)
            .unwrap_or(self.keys.len());
        self.keys.insert(pos, key);
        self.children.insert(pos + 1, child_page_id);
        self.key_count = self.keys.len();
        Ok(())
    }

    pub fn split_leaf(
        &mut self,
        storage: &mut Pager,
        new_key: BTreeKey,
        new_value: BTreeValue,
    ) -> Result<(BTreeKey, PageId)> {
        self.decode()?;
        let pos = self
            .keys
            .iter()
            .position(|k| k > &new_key)
            .unwrap_or(self.keys.len());
        let is_append = pos == self.keys.len();
        self.keys.insert(pos, new_key);
        self.values.insert(pos, new_value);
        self.key_count = self.keys.len();

        if self.keys.len() < 2 {
            return Err(HematiteError::StorageError(
                "Cannot split leaf with fewer than 2 keys".to_string(),
            ));
        }

        let new_page_id = storage.allocate_page()?;
        let mut new_page = storage.new_page_buffer(new_page_id)?;
        let mut new_node = Self::new_leaf(new_page_id);
        let split_pos = if is_append {
            self.keys.len() - 1
        } else {
            self.best_leaf_split_pos()
        };

        // Move right-side keys/values into the new node using VecDeque drain.
        let right_keys: VecDeque<BTreeKey> = self.keys.drain(split_pos..).collect();
        let right_values: VecDeque<BTreeValue> = self.values.drain(split_pos..).collect();
        new_node.keys = right_keys;
        new_node.values = right_values;
        new_node.key_count = new_node.keys.len();
        self.key_count = self.keys.len();
        let split_key = new_node.keys[0].clone();

        let mut current_page = storage.take_page_for_write(self.page_id)?;
        self.to_page(&mut current_page)?;
        storage.write_page(current_page)?;
        new_node.to_page(&mut new_page)?;
        storage.write_page(new_page)?;
        Ok((split_key, new_page_id))
    }

    pub fn split_internal(
        &mut self,
        storage: &mut Pager,
        new_key: BTreeKey,
        new_child: PageId,
    ) -> Result<(BTreeKey, PageId)> {
        self.decode()?;
        let pos = self
            .keys
            .iter()
            .position(|k| k >= &new_key)
            .unwrap_or(self.keys.len());
        let is_append = pos == self.keys.len();
        self.keys.insert(pos, new_key);
        self.children.insert(pos + 1, new_child);
        self.key_count = self.keys.len();

        if self.keys.len() < 2 {
            return Err(HematiteError::StorageError(
                "Cannot split internal node with fewer than 2 keys".to_string(),
            ));
        }

        let new_page_id = storage.allocate_page()?;
        let mut new_page = storage.new_page_buffer(new_page_id)?;
        let mut new_node = Self::new_internal(new_page_id);

        let split_pos = if is_append {
            self.keys.len() - 2
        } else {
            self.best_internal_split_pos()
        };
        let split_key = self.keys.get(split_pos).cloned().expect("split key");
        // Drain the ranges for keys and children into the new node.
        let right_keys: VecDeque<BTreeKey> = self.keys.drain((split_pos + 1)..).collect();
        let right_children: VecDeque<PageId> = self.children.drain((split_pos + 1)..).collect();
        new_node.keys = right_keys;
        new_node.children = right_children;
        // Remove the separator key from the left node (was at split_pos)
        // For VecDeque remove the element at split_pos.
        let _ = self.keys.remove(split_pos);
        self.key_count = self.keys.len();
        new_node.key_count = new_node.keys.len();

        let mut current_page = storage.take_page_for_write(self.page_id)?;
        self.to_page(&mut current_page)?;
        storage.write_page(current_page)?;
        new_node.to_page(&mut new_page)?;
        storage.write_page(new_page)?;
        Ok((split_key, new_page_id))
    }

    fn best_leaf_split_pos(&self) -> usize {
        let len = self.keys.len();
        let mut prefix = vec![0usize; len + 1];
        for i in 0..len {
            prefix[i + 1] = prefix[i] + leaf_cell_size(&self.keys[i], &self.values[i]);
        }
        let total = prefix[len];
        let mut best_pos = len / 2;
        let mut best_score = usize::MAX;
        let mut best_min = 0usize;
        for (pos, left) in prefix.iter().enumerate().take(len).skip(1) {
            let left = *left;
            let right = total - left;
            let score = left.abs_diff(right);
            let min_side = left.min(right);
            if score < best_score || (score == best_score && min_side > best_min) {
                best_score = score;
                best_min = min_side;
                best_pos = pos;
            }
        }
        best_pos
    }

    fn best_internal_split_pos(&self) -> usize {
        let key_len = self.keys.len();
        let mut key_prefix = vec![0usize; key_len + 1];
        for i in 0..key_len {
            key_prefix[i + 1] = key_prefix[i] + internal_cell_size(&self.keys[i]);
        }
        let total_key_bytes = key_prefix[key_len];
        let mut best_pos = key_len / 2;
        let mut best_score = usize::MAX;
        let mut best_min = 0usize;

        for split_pos in 1..key_len - 1 {
            let left_key_bytes = key_prefix[split_pos];
            let right_key_bytes = total_key_bytes - key_prefix[split_pos + 1];
            let left_children = split_pos + 1;
            let right_children = key_len - split_pos;
            let left_payload = left_key_bytes + left_children * CHILD_ID_SIZE;
            let right_payload = right_key_bytes + right_children * CHILD_ID_SIZE;
            let score = left_payload.abs_diff(right_payload);
            let min_side = left_payload.min(right_payload);
            if score < best_score || (score == best_score && min_side > best_min) {
                best_score = score;
                best_min = min_side;
                best_pos = split_pos;
            }
        }
        best_pos
    }

    pub fn find_child_index(&self, key: &BTreeKey) -> Result<usize> {
        self.upper_bound_index(key)
    }

    pub fn lower_bound_index(&self, key: &BTreeKey) -> Result<usize> {
        let mut left = 0;
        let mut right = self.key_count;
        while left < right {
            let mid = (left + right) / 2;
            let mid_key_bytes = self.get_key_view(mid)?;
            if mid_key_bytes < key.as_bytes() {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        Ok(left)
    }

    pub fn exact_key_index(&self, key: &BTreeKey) -> Result<Option<usize>> {
        let index = self.lower_bound_index(key)?;
        if index < self.key_count && self.get_key_view(index)? == key.as_bytes() {
            Ok(Some(index))
        } else {
            Ok(None)
        }
    }

    pub fn upper_bound_index(&self, key: &BTreeKey) -> Result<usize> {
        if self.is_decoded {
            let mut left = 0;
            let mut right = self.keys.len();
            while left < right {
                let mid = (left + right) / 2;
                if self.keys[mid].as_bytes() <= key.as_bytes() {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }
            return Ok(left);
        }

        let mut left = 0;
        let mut right = self.key_count;
        while left < right {
            let mid = (left + right) / 2;
            let mid_key_bytes = self.get_key_view(mid)?;
            if mid_key_bytes <= key.as_bytes() {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        Ok(left)
    }

    /// Space the two nodes would occupy merged, from their byte counts alone.
    ///
    /// A merged page keeps one header and carries both nodes' pointer arrays and cell
    /// bodies, so its size is the two sizes less one header. `serialized_size_on_page`
    /// answers for a lazy node by summing cell ranges off the page, so neither node
    /// has to be decoded — or cloned.
    ///
    /// `None` when either size cannot be read, which callers treat as "do not merge".
    /// Refusing to merge is always safe; merging when it does not fit is not.
    fn merged_size_with(&self, other: &BTreeNode, header_size: usize) -> Option<usize> {
        let mine = self.serialized_size_on_page().ok()?;
        let theirs = other.serialized_size_on_page().ok()?;
        mine.checked_add(theirs)?.checked_sub(header_size)
    }

    /// Whether two leaves fit in one page.
    ///
    /// This used to clone both nodes, decode the clones — materialising every key and
    /// value into owned `Vec`s — and concatenate them, purely to measure the result.
    /// `merge_leaf` then called it a second time, so a single rebalance decision cost
    /// four node clones and four full decodes.
    pub fn can_merge_with(&self, other: &BTreeNode) -> bool {
        if self.node_type != other.node_type || self.node_type != NodeType::Leaf {
            return false;
        }
        if self.key_count + other.key_count > MAX_KEYS {
            return false;
        }
        let Some(merged) = self.merged_size_with(other, LEAF_HEADER_SIZE) else {
            return false;
        };
        merged + page_header_offset(self.page_id) <= PAGE_SIZE
    }

    /// Whether two interior nodes plus the separator between them fit in one page.
    ///
    /// The separator becomes an ordinary cell in the merged node, so it costs its own
    /// cell plus the two-byte pointer that indexes it.
    pub fn can_merge_internal_with_separator(
        &self,
        other: &BTreeNode,
        separator_key: &BTreeKey,
    ) -> bool {
        if self.node_type != NodeType::Internal || other.node_type != NodeType::Internal {
            return false;
        }
        if self.key_count + other.key_count + 1 > MAX_KEYS {
            return false;
        }
        let Some(merged) = self.merged_size_with(other, INTERIOR_HEADER_SIZE) else {
            return false;
        };
        merged + internal_cell_size(separator_key) + 2 + page_header_offset(self.page_id)
            <= PAGE_SIZE
    }

    pub fn merge_leaf(&mut self, other: &mut BTreeNode, storage: &mut Pager) -> Result<()> {
        self.decode()?;
        other.decode()?;
        if self.node_type != NodeType::Leaf || other.node_type != NodeType::Leaf {
            return Err(HematiteError::StorageError(
                "Can only merge leaf nodes".to_string(),
            ));
        }
        if !self.can_merge_with(other) {
            return Err(HematiteError::StorageError(
                "Nodes cannot be merged".to_string(),
            ));
        }
        self.keys.append(&mut other.keys);
        self.values.append(&mut other.values);
        self.key_count = self.keys.len();
        storage.deallocate_page(other.page_id)?;
        Ok(())
    }

    pub fn merge_internal(
        &mut self,
        other: &mut BTreeNode,
        separator_key: BTreeKey,
        storage: &mut Pager,
    ) -> Result<()> {
        self.decode()?;
        other.decode()?;
        if self.node_type != NodeType::Internal || other.node_type != NodeType::Internal {
            return Err(HematiteError::StorageError(
                "Can only merge internal nodes".to_string(),
            ));
        }
        // BUG-06 fix: removed redundant can_merge_internal_with_separator check.
        // The caller already checked eligibility; repeating it here clones and
        // decodes both nodes a second time for no benefit.
        self.keys.push_back(separator_key);
        self.keys.append(&mut other.keys);
        self.children.append(&mut other.children);
        self.key_count = self.keys.len();
        storage.deallocate_page(other.page_id)?;
        Ok(())
    }

    pub fn is_underflow(&self) -> bool {
        // BUG-07 fix: base underflow on page-space utilisation instead of the
        // MAX_KEYS constant, which is higher than the realistic per-page key
        // capacity and would trigger rebalancing on almost every delete.
        let used = self.serialized_size_on_page().unwrap_or(PAGE_SIZE);
        let capacity = PAGE_SIZE - page_header_offset(self.page_id);
        // Underflow when the node occupies less than 25 % of its available space.
        used * 4 < capacity
    }

    /// The byte range of one cell.
    ///
    /// O(1): `compute_cell_size` reads the cell's own `key_len`/`value_len` at its
    /// offset, so a single range needs neither the other cells nor an allocation.
    ///
    /// This used to build — and cache — the exact ranges of *every* cell on the page,
    /// which meant three `Vec` allocations, a sort and an overlap sweep to answer a
    /// question about one cell. `from_shared_page` ran the same pass eagerly on every
    /// page it loaded, and together they were ~28-30 % of a batched insert. The
    /// whole-page pass was doing two jobs; only the structural half of it needed the
    /// whole page, and that half now lives in [`verify`](Self::verify).
    fn cell_range_for_index(&self, index: usize) -> Result<(usize, usize)> {
        // BUG-01 fix: the parameter is always a cell *index*, never a byte
        // offset.  The previous dual-interpretation (index if < len, else
        // treated as offset) would silently return the wrong cell when the
        // cell count exceeded a cell's byte offset value.
        let (page, header) = self.raw_page_and_header()?;
        let start = cell_pointer_from_header(page, header, index)? as usize;
        let size = compute_cell_size(page, header, start)?;
        Ok((start, start + size))
    }

    /// The raw page and its already-parsed header.
    fn raw_page_and_header(&self) -> Result<(&Page, &BTreePageHeaderV3)> {
        let page = self.raw_page.as_ref().ok_or_else(|| {
            HematiteError::CorruptedData("Missing raw page for lazy node".to_string())
        })?;
        let header = self.header.as_ref().ok_or_else(|| {
            HematiteError::CorruptedData("Missing parsed header for lazy node".to_string())
        })?;
        Ok((page, header))
    }

    /// Checks the structural properties of the whole page: that no two cell pointers
    /// coincide, that every cell's declared size is readable, and that no two cells
    /// overlap.
    ///
    /// This ran on every page load, including pages a cursor merely navigated past.
    /// It was defence-in-depth for `BTreeCursor::key_view`/`value_view`, which used to
    /// swallow per-cell decode errors with `.ok()` and report corruption as "no
    /// entry" — so the eager pass was the only thing that surfaced it. Those
    /// accessors now propagate, which means a cell that is actually read is checked
    /// where it is read, and this is left for callers that want the whole page
    /// examined: `validate_integrity` and debug assertions.
    pub fn verify(&self) -> Result<()> {
        let (page, header) = self.raw_page_and_header()?;
        let mut offsets = Vec::with_capacity(self.key_count);
        for index in 0..self.key_count {
            offsets.push(cell_pointer_from_header(page, header, index)?);
        }
        compute_cell_ranges_exact(page, header, &offsets)?;
        Ok(())
    }
}

/// Exact-sizing variant of compute_cell_ranges that reads key_len/value_len
/// from the page bytes for each cell, rather than inferring the last cell's
/// end from PAGE_SIZE.  This prevents the last cell's range from including
/// trailing freeblock/fragment bytes, which would mask corruption.
///
/// BUG-02 fix: called from from_shared_page and try_remove_cell_in_place.
fn compute_cell_ranges_exact(
    page: &Page,
    header: &BTreePageHeaderV3,
    cell_offsets: &[u16],
) -> Result<Vec<(usize, usize)>> {
    // Sort (offset, original_index) pairs once so both the duplicate check and
    // the overlap check can be done against this single ordering, instead of
    // computing exact ranges in cell-index order and then cloning the whole
    // vec just to re-sort it for validation.
    let mut indexed_offsets: Vec<(usize, usize)> = cell_offsets
        .iter()
        .copied()
        .enumerate()
        .map(|(index, offset)| (offset as usize, index))
        .collect();
    indexed_offsets.sort_unstable_by_key(|(offset, _)| *offset);

    if indexed_offsets
        .windows(2)
        .any(|window| window[0].0 == window[1].0)
    {
        return Err(HematiteError::CorruptedData(
            "Duplicate B-tree cell pointers".to_string(),
        ));
    }

    let mut ranges = vec![(0usize, 0usize); cell_offsets.len()];
    for &(start, original_index) in &indexed_offsets {
        let size = compute_cell_size(page, header, start)?;
        ranges[original_index] = (start, start + size);
    }

    for window in indexed_offsets.windows(2) {
        let (_, left_index) = window[0];
        let (right_start, _) = window[1];
        if ranges[left_index].1 > right_start {
            return Err(HematiteError::CorruptedData(
                "Overlapping B-tree cells or duplicate pointers".to_string(),
            ));
        }
    }

    Ok(ranges)
}

fn page_header_offset(page_id: PageId) -> usize {
    if is_page_one(page_id) {
        DATABASE_HEADER_SIZE
    } else {
        0
    }
}

fn is_page_one(page_id: PageId) -> bool {
    page_id == 0
}

fn initialize_page_bytes(page: &mut Page, kind: PageKind) -> Result<()> {
    let header_offset = page_header_offset(page.id);
    let header_size = match kind {
        PageKind::LeafTable => LEAF_HEADER_SIZE,
        PageKind::InteriorTable => INTERIOR_HEADER_SIZE,
        _ => {
            return Err(HematiteError::StorageError(format!(
                "Unsupported B-tree page kind {:?}",
                kind
            )))
        }
    };

    if header_offset == 0 {
        page.data.fill(0);
    } else {
        for byte in page.data.iter_mut().skip(DATABASE_HEADER_SIZE) {
            *byte = 0;
        }
    }

    page.data[header_offset] = kind as u8;
    write_u16_be(&mut page.data, header_offset + 1, 0);
    write_u16_be(&mut page.data, header_offset + 3, 0);
    write_u16_be(&mut page.data, header_offset + 5, PAGE_SIZE as u16);
    page.data[header_offset + 7] = 0;
    if header_size == INTERIOR_HEADER_SIZE {
        write_u32_be(&mut page.data, header_offset + 8, 0);
    }
    Ok(())
}

fn encode_leaf_cell(key: &BTreeKey, value: &BTreeValue) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(leaf_cell_size(key, value));
    varint::write_to_vec(&mut bytes, key.data.len() as u64);
    varint::write_to_vec(&mut bytes, value.data.len() as u64);
    bytes.extend_from_slice(&key.data);
    bytes.extend_from_slice(&value.data);
    bytes
}

/// `[varint key_len][varint value_len][key][value]`.
///
/// The two lengths were fixed 16-bit fields, four bytes on every cell whatever it
/// held. A table row's key is a three-byte row id and its value around eighteen
/// bytes, so both lengths fit the varint's one-byte form and the framing halves.
fn leaf_cell_size(key: &BTreeKey, value: &BTreeValue) -> usize {
    varint::len_of(key.data.len() as u64)
        + varint::len_of(value.data.len() as u64)
        + key.data.len()
        + value.data.len()
}

/// `[u32 left_child][varint key_len][key]`. The child pointer stays fixed-width: it
/// is a page id, so it is rarely small, and interior cells are few.
fn internal_cell_size(key: &BTreeKey) -> usize {
    CHILD_ID_SIZE + varint::len_of(key.data.len() as u64) + key.data.len()
}

/// Encode a raw internal cell: `[4-byte child_page_id][2-byte key_len][key_bytes]`.
/// Used by `try_insert_internal_in_place` to build a cell directly from raw
/// components without going through the VecDeque-based decode path.
fn encode_internal_cell_raw(child: PageId, key: &BTreeKey) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(internal_cell_size(key));
    bytes.extend_from_slice(&child.to_be_bytes());
    varint::write_to_vec(&mut bytes, key.data.len() as u64);
    bytes.extend_from_slice(&key.data);
    bytes
}

fn leaf_cell_ranges(cell: &[u8]) -> Result<((usize, usize), (usize, usize))> {
    let truncated = || HematiteError::CorruptedData("Leaf cell header is truncated".to_string());
    // See `compute_cell_size`: the one-byte form for both lengths is the common case
    // and worth reading directly.
    let (key_len, value_len, key_start) = match cell.get(..2) {
        Some(&[a, b]) if a & 1 == 1 && b & 1 == 1 => ((a >> 1) as usize, (b >> 1) as usize, 2),
        _ => {
            let (key_len, key_len_bytes) = varint::read(cell, 0).ok_or_else(truncated)?;
            let (value_len, value_len_bytes) =
                varint::read(cell, key_len_bytes).ok_or_else(truncated)?;
            (
                key_len as usize,
                value_len as usize,
                key_len_bytes + value_len_bytes,
            )
        }
    };
    let key_end = key_start + key_len;
    let value_end = key_end + value_len;
    if key_len > MAX_KEY_SIZE || value_len > MAX_VALUE_SIZE || value_end > cell.len() {
        return Err(HematiteError::CorruptedData(
            "Leaf cell content exceeds page bounds".to_string(),
        ));
    }
    Ok(((key_start, key_end), (key_end, value_end)))
}

fn leaf_cell_key_range(cell: &[u8]) -> Result<(usize, usize)> {
    leaf_cell_ranges(cell).map(|ranges| ranges.0)
}

fn leaf_cell_value_range(cell: &[u8]) -> Result<(usize, usize)> {
    leaf_cell_ranges(cell).map(|ranges| ranges.1)
}

fn internal_cell_key_range(cell: &[u8]) -> Result<(usize, usize)> {
    let truncated =
        || HematiteError::CorruptedData("Internal cell header is truncated".to_string());
    let (key_len, key_len_bytes) = varint::read(cell, CHILD_ID_SIZE).ok_or_else(truncated)?;
    let key_len = key_len as usize;
    let key_start = CHILD_ID_SIZE + key_len_bytes;
    let key_end = key_start + key_len;
    if key_len > MAX_KEY_SIZE || key_end > cell.len() {
        return Err(HematiteError::CorruptedData(
            "Internal cell key exceeds page bounds".to_string(),
        ));
    }
    Ok((key_start, key_end))
}

fn parse_leaf_cell(cell: &[u8]) -> Result<(BTreeKey, BTreeValue)> {
    let (key_range, value_range) = leaf_cell_ranges(cell)?;
    Ok((
        BTreeKey::new(cell[key_range.0..key_range.1].to_vec()),
        BTreeValue::new(cell[value_range.0..value_range.1].to_vec()),
    ))
}

fn parse_internal_cell(cell: &[u8]) -> Result<(PageId, BTreeKey)> {
    let key_range = internal_cell_key_range(cell)?;
    let child = u32::from_be_bytes([cell[0], cell[1], cell[2], cell[3]]);
    if child <= 1 {
        return Err(HematiteError::CorruptedData(
            "Internal cell child pointer cannot reference reserved pages".to_string(),
        ));
    }
    Ok((
        child,
        BTreeKey::new(cell[key_range.0..key_range.1].to_vec()),
    ))
}

fn cell_pointer_from_header(page: &Page, header: &BTreePageHeaderV3, index: usize) -> Result<u16> {
    if index >= header.cell_count as usize {
        return Err(HematiteError::StorageError(format!(
            "Cell index {} out of bounds for {} cells",
            index, header.cell_count
        )));
    }
    let offset = header.pointer_area_start() + index * 2;
    Ok(u16::from_be_bytes([
        page.data[offset],
        page.data[offset + 1],
    ]))
}

fn write_u16_be(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn write_u32_be(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

// Helpers that write directly into a destination slice starting at `offset`.
// These are thin wrappers used by `to_page()` to avoid allocating temporary
// buffers and to make intent explicit.
fn dest_copy_u16_be(dest: &mut [u8], offset: usize, value: u16) {
    dest[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn dest_copy_u32_be(dest: &mut [u8], offset: usize, value: u32) {
    dest[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

#[cfg(test)]
#[derive(Debug)]
pub enum SearchResult {
    Found(BTreeValue),
    NotFound(PageId),
}
