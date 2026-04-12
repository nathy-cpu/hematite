//! B-tree node layout and node-local algorithms.
//!
//! The live on-page format is now a slotted-page layout:
//! - a compact page header
//! - a cell pointer array in key order
//! - cell bodies packed from the end of the page backward
//! - a rightmost-child slot for internal pages
//!
//! This keeps the existing `BTreeNode` API that the rest of the tree code uses, but removes the
//! old contiguous key/value section format.

use crate::btree::page_format::{
    insert_cell as pf_insert_cell, remove_cell as pf_remove_cell,
    total_free_space as pf_total_free_space, BTreePageHeaderV3,
};
use crate::btree::{BTreeKey, BTreeValue, NodeType, BTREE_ORDER};
use crate::error::{HematiteError, Result};
use crate::storage::format::{PageKind, DATABASE_HEADER_SIZE};
use crate::storage::{Page, PageId, Pager, PAGE_SIZE};
use std::sync::Arc;
#[cfg(test)]
use crate::storage::INVALID_PAGE_ID;

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
    pub keys: Vec<BTreeKey>,
    pub children: Vec<PageId>,
    pub values: Vec<BTreeValue>,

    pub key_count: usize,
    pub payload_len: usize,
    pub raw_page: Option<Arc<Page>>,
    pub is_decoded: bool,
    pub cell_offsets: Vec<u16>,
}

impl BTreeNode {
    pub fn new_internal(page_id: PageId) -> Self {
        Self {
            page_id,
            node_type: NodeType::Internal,
            keys: Vec::new(),
            children: Vec::new(),
            values: Vec::new(),
            key_count: 0,
            payload_len: 0,
            raw_page: None,
            is_decoded: true,
            cell_offsets: Vec::new(),
        }
    }

    pub fn new_leaf(page_id: PageId) -> Self {
        Self {
            page_id,
            node_type: NodeType::Leaf,
            keys: Vec::new(),
            children: Vec::new(),
            values: Vec::new(),
            key_count: 0,
            payload_len: 0,
            raw_page: None,
            is_decoded: true,
            cell_offsets: Vec::new(),
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
        let cell_body_bytes = (0..self.key_count)
            .map(|index| {
                let (start, end) = self.cell_range_for_index(index)?;
                Ok(end - start)
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .sum::<usize>();

        Ok(header_size + self.key_count * 2 + cell_body_bytes)
    }

    pub fn will_fit_in_page(&self) -> bool {
        self.estimate_serialized_size() + page_header_offset(self.page_id) <= PAGE_SIZE
    }

    pub fn can_insert_key_value(&self, key: &BTreeKey, value: &BTreeValue) -> bool {
        if !matches!(self.node_type, NodeType::Leaf) {
            return false;
        }
        self.estimate_serialized_size() + leaf_cell_size(key, value) + 2 + page_header_offset(self.page_id)
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
        node.cell_offsets = (0..header.cell_count as usize)
            .map(|index| cell_pointer_from_header(&page, &header, index))
            .collect::<Result<Vec<_>>>()?;

        // Validate the cells before returning a lazy node.
        let cell_ranges = node.cell_ranges()?;
        match node.node_type {
            NodeType::Leaf => {
                for &offset in &node.cell_offsets {
                    let (start, end) = cell_ranges[offset as usize];
                    parse_leaf_cell(&page.data[start..end])?;
                }
            }
            NodeType::Internal => {
                for &offset in &node.cell_offsets {
                    let (start, end) = cell_ranges[offset as usize];
                    parse_internal_cell(&page.data[start..end])?;
                }
                if header.rightmost_child.unwrap_or(0) == 0 {
                    return Err(HematiteError::CorruptedData(
                        "Internal node is missing its rightmost child".to_string(),
                    ));
                }
            }
        }

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
        let cell_ranges = self.cell_ranges()?;

        self.keys.clear();
        self.children.clear();
        self.values.clear();

        match self.node_type {
            NodeType::Leaf => {
                for &offset in &self.cell_offsets {
                    let (start, end) = cell_ranges[offset as usize];
                    let (key, value) = parse_leaf_cell(&page.data[start..end])?;
                    self.keys.push(key);
                    self.values.push(value);
                }
            }
            NodeType::Internal => {
                for &offset in &self.cell_offsets {
                    let (start, end) = cell_ranges[offset as usize];
                    let (left_child, key) = parse_internal_cell(&page.data[start..end])?;
                    self.children.push(left_child);
                    self.keys.push(key);
                }
                self.children.push(header.rightmost_child.unwrap_or(0));
            }
        }

        self.key_count = self.keys.len();
        self.is_decoded = true;
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

    pub fn get_key_procedural(&self, target_index: usize) -> Result<BTreeKey> {
        self.get_key_view(target_index)
            .map(|v| BTreeKey::new(v.to_vec()))
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
            return Ok(header.rightmost_child.unwrap_or(0));
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

        let mut cell_bytes = Vec::with_capacity(self.keys.len());
        for index in 0..self.keys.len() {
            let bytes = match self.node_type {
                NodeType::Leaf => {
                    Self::validate_key_size(&self.keys[index])?;
                    Self::validate_value_size(&self.values[index])?;
                    encode_leaf_cell(&self.keys[index], &self.values[index])
                }
                NodeType::Internal => {
                    Self::validate_key_size(&self.keys[index])?;
                    encode_internal_cell(self.children[index], &self.keys[index])?
                }
            };
            cell_bytes.push(bytes);
        }

        let total_cell_bytes = cell_bytes.iter().map(Vec::len).sum::<usize>();
        let total_size =
            page_header_offset(page.id) + header_size + self.keys.len() * 2 + total_cell_bytes;
        if total_size > PAGE_SIZE {
            return Err(HematiteError::StorageError(format!(
                "Serialized B-tree node exceeds page size: {} bytes",
                total_size
            )));
        }

        let mut content_start = PAGE_SIZE;
        for (index, cell) in cell_bytes.iter().enumerate().rev() {
            content_start -= cell.len();
            page.data[content_start..content_start + cell.len()].copy_from_slice(cell);
            write_u16_be(&mut page.data, pointer_area_start + index * 2, content_start as u16);
        }

        write_u16_be(
            &mut page.data,
            header_offset + 3,
            self.keys.len() as u16,
        );
        write_u16_be(
            &mut page.data,
            header_offset + 5,
            content_start as u16,
        );
        page.data[header_offset + 7] = 0;
        if self.node_type == NodeType::Internal {
            write_u32_be(
                &mut page.data,
                header_offset + 8,
                *self.children.last().ok_or_else(|| {
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

        let mut target_index = None;
        for i in 0..self.key_count {
            if self.get_key_view(i)? == key.as_bytes() {
                target_index = Some(i);
                break;
            }
        }
        let Some(index) = target_index else {
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
        self.raw_page = Some(Arc::new(page.clone()));
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
        if self.exact_key_index(key).is_some() {
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
        let pos = self.lower_bound_index(key);

        // Use the page_format insert_cell to do the work.
        pf_insert_cell(page, is_p1, pos, &cell_bytes)?;

        // Invalidate the lazy node state so the next read re-parses.
        self.raw_page = Some(Arc::new(page.clone()));
        self.is_decoded = false;
        let header = BTreePageHeaderV3::parse(page, is_p1)?;
        self.key_count = header.cell_count as usize;
        self.cell_offsets = (0..header.cell_count as usize)
            .map(|i| cell_pointer_from_header(page, &header, i))
            .collect::<Result<Vec<_>>>()?;
        self.payload_len = PAGE_SIZE.saturating_sub(header.cell_content_start as usize);
        self.keys.clear();
        self.values.clear();
        self.children.clear();
        Ok(true)
    }

    /// Try to remove a cell at `index` directly on the page bytes without
    /// a full decode + rebuild.  Returns `true` if successful.
    pub fn try_remove_cell_in_place(
        &mut self,
        page: &mut Page,
        index: usize,
    ) -> Result<bool> {
        if index >= self.key_count {
            return Ok(false);
        }
        let is_p1 = is_page_one(page.id);

        pf_remove_cell(page, is_p1, index)?;

        // Refresh lazy state from the updated page.
        self.raw_page = Some(Arc::new(page.clone()));
        self.is_decoded = false;
        let header = BTreePageHeaderV3::parse(page, is_p1)?;
        self.key_count = header.cell_count as usize;
        self.cell_offsets = (0..header.cell_count as usize)
            .map(|i| cell_pointer_from_header(page, &header, i))
            .collect::<Result<Vec<_>>>()?;
        self.payload_len = PAGE_SIZE.saturating_sub(header.cell_content_start as usize);
        self.keys.clear();
        self.values.clear();
        self.children.clear();
        Ok(true)
    }

    #[cfg(test)]
    pub fn search(&self, key: &BTreeKey) -> SearchResult {
        match self.node_type {
            NodeType::Leaf => self.search_leaf(key),
            NodeType::Internal => self.search_internal(key),
        }
    }

    #[cfg(test)]
    fn search_leaf(&self, key: &BTreeKey) -> SearchResult {
        if let Some(index) = self.exact_key_index(key) {
            SearchResult::Found(self.get_value_procedural(index).unwrap())
        } else {
            SearchResult::NotFound(INVALID_PAGE_ID)
        }
    }

    #[cfg(test)]
    fn search_internal(&self, key: &BTreeKey) -> SearchResult {
        SearchResult::NotFound(self.get_child_procedural(self.upper_bound_index(key)).unwrap())
    }

    pub fn find_child(&self, key: &BTreeKey) -> PageId {
        self.get_child_procedural(self.upper_bound_index(key)).unwrap()
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
        let mut new_page = Page::new(new_page_id);
        let mut new_node = Self::new_leaf(new_page_id);
        let split_pos = if is_append {
            self.keys.len() - 1
        } else {
            self.best_leaf_split_pos()
        };

        new_node.keys = self.keys.split_off(split_pos);
        new_node.values = self.values.split_off(split_pos);
        new_node.key_count = new_node.keys.len();
        self.key_count = self.keys.len();
        let split_key = new_node.keys[0].clone();

        let mut current_page = storage.read_page(self.page_id)?;
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
        let mut new_page = Page::new(new_page_id);
        let mut new_node = Self::new_internal(new_page_id);

        let split_pos = if is_append {
            self.keys.len() - 2
        } else {
            self.best_internal_split_pos()
        };
        let split_key = self.keys[split_pos].clone();
        new_node.keys = self.keys.split_off(split_pos + 1);
        new_node.children = self.children.split_off(split_pos + 1);
        self.keys.pop();
        self.key_count = self.keys.len();
        new_node.key_count = new_node.keys.len();

        let mut current_page = storage.read_page(self.page_id)?;
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
        for pos in 1..len {
            let left = prefix[pos];
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

    pub fn delete_from_leaf(&mut self, key: &BTreeKey) -> Result<Option<BTreeValue>> {
        self.decode()?;
        if self.node_type != NodeType::Leaf {
            return Err(HematiteError::StorageError("Not a leaf node".to_string()));
        }
        for (i, k) in self.keys.iter().enumerate() {
            if k == key {
                let value = self.values.remove(i);
                self.keys.remove(i);
                self.key_count = self.keys.len();
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    pub fn find_child_index(&self, key: &BTreeKey) -> usize {
        self.upper_bound_index(key)
    }

    pub fn lower_bound_index(&self, key: &BTreeKey) -> usize {
        let mut left = 0;
        let mut right = self.key_count;
        while left < right {
            let mid = (left + right) / 2;
            let mid_key_bytes = self.get_key_view(mid).unwrap();
            if mid_key_bytes < key.as_bytes() {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        left
    }

    pub fn exact_key_index(&self, key: &BTreeKey) -> Option<usize> {
        let index = self.lower_bound_index(key);
        if index < self.key_count && self.get_key_view(index).unwrap() == key.as_bytes() {
            Some(index)
        } else {
            None
        }
    }

    pub fn upper_bound_index(&self, key: &BTreeKey) -> usize {
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
            return left;
        }

        let mut left = 0;
        let mut right = self.key_count;
        while left < right {
            let mid = (left + right) / 2;
            let mid_key_bytes = self.get_key_view(mid).unwrap();
            if mid_key_bytes <= key.as_bytes() {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        left
    }

    pub fn can_merge_with(&self, other: &BTreeNode) -> bool {
        if self.node_type != other.node_type {
            return false;
        }
        match self.node_type {
            NodeType::Leaf => {
                let mut merged = self.clone();
                let mut other = other.clone();
                merged.decode().ok();
                other.decode().ok();
                merged.keys.extend(other.keys);
                merged.values.extend(other.values);
                merged.key_count = merged.keys.len();
                merged.keys.len() <= MAX_KEYS && merged.will_fit_in_page()
            }
            NodeType::Internal => false,
        }
    }

    pub fn can_merge_internal_with_separator(
        &self,
        other: &BTreeNode,
        separator_key: &BTreeKey,
    ) -> bool {
        if self.node_type != NodeType::Internal || other.node_type != NodeType::Internal {
            return false;
        }
        let mut merged = self.clone();
        let mut other = other.clone();
        merged.decode().ok();
        other.decode().ok();
        merged.keys.push(separator_key.clone());
        merged.keys.extend(other.keys);
        merged.children.extend(other.children);
        merged.key_count = merged.keys.len();
        merged.keys.len() <= MAX_KEYS && merged.will_fit_in_page()
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
        if !self.can_merge_internal_with_separator(other, &separator_key) {
            return Err(HematiteError::StorageError(
                "Nodes cannot be merged".to_string(),
            ));
        }
        self.keys.push(separator_key);
        self.keys.append(&mut other.keys);
        self.children.append(&mut other.children);
        self.key_count = self.keys.len();
        storage.deallocate_page(other.page_id)?;
        Ok(())
    }

    pub fn is_underflow(&self) -> bool {
        match self.node_type {
            NodeType::Leaf => self.key_count < (MAX_KEYS / 2),
            NodeType::Internal => self.key_count < ((MAX_KEYS - 1) / 2),
        }
    }

    fn cell_ranges(&self) -> Result<Vec<(usize, usize)>> {
        let page = self.raw_page.as_ref().ok_or_else(|| {
            HematiteError::CorruptedData("Missing raw page for lazy node".to_string())
        })?;
        let mut starts = self
            .cell_offsets
            .iter()
            .map(|offset| *offset as usize)
            .collect::<Vec<_>>();
        starts.sort_unstable();
        starts.dedup();
        if starts.len() != self.cell_offsets.len() {
            return Err(HematiteError::CorruptedData(
                "Duplicate B-tree cell pointers".to_string(),
            ));
        }

        let mut ranges = vec![(0usize, 0usize); PAGE_SIZE];
        for (index, start) in starts.iter().enumerate() {
            let end = starts.get(index + 1).copied().unwrap_or(PAGE_SIZE);
            if *start >= end || end > PAGE_SIZE {
                return Err(HematiteError::CorruptedData(
                    "B-tree cell pointer ordering is invalid".to_string(),
                ));
            }
            ranges[*start] = (*start, end);
        }
        let _ = page; // keeps the method tied to a real page for consistency checks above.
        Ok(ranges)
    }

    fn cell_range_for_index(&self, index: usize) -> Result<(usize, usize)> {
        if index >= self.cell_offsets.len() {
            return Err(HematiteError::StorageError(
                "Index out of bounds".to_string(),
            ));
        }
        let ranges = self.cell_ranges()?;
        Ok(ranges[self.cell_offsets[index] as usize])
    }
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
    let mut bytes = Vec::with_capacity(4 + key.data.len() + value.data.len());
    bytes.extend_from_slice(&(key.data.len() as u16).to_be_bytes());
    bytes.extend_from_slice(&(value.data.len() as u16).to_be_bytes());
    bytes.extend_from_slice(&key.data);
    bytes.extend_from_slice(&value.data);
    bytes
}

fn encode_internal_cell(left_child: PageId, key: &BTreeKey) -> Result<Vec<u8>> {
    if left_child <= 1 {
        return Err(HematiteError::StorageError(
            "Internal node child ids cannot reference reserved pages".to_string(),
        ));
    }
    let mut bytes = Vec::with_capacity(6 + key.data.len());
    bytes.extend_from_slice(&left_child.to_be_bytes());
    bytes.extend_from_slice(&(key.data.len() as u16).to_be_bytes());
    bytes.extend_from_slice(&key.data);
    Ok(bytes)
}

fn leaf_cell_size(key: &BTreeKey, value: &BTreeValue) -> usize {
    4 + key.data.len() + value.data.len()
}

fn internal_cell_size(key: &BTreeKey) -> usize {
    6 + key.data.len()
}

fn leaf_cell_ranges(cell: &[u8]) -> Result<((usize, usize), (usize, usize))> {
    if cell.len() < 4 {
        return Err(HematiteError::CorruptedData(
            "Leaf cell header is truncated".to_string(),
        ));
    }
    let key_len = u16::from_be_bytes([cell[0], cell[1]]) as usize;
    let value_len = u16::from_be_bytes([cell[2], cell[3]]) as usize;
    let key_start = 4;
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
    if cell.len() < 6 {
        return Err(HematiteError::CorruptedData(
            "Internal cell header is truncated".to_string(),
        ));
    }
    let key_len = u16::from_be_bytes([cell[4], cell[5]]) as usize;
    let key_start = 6;
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
    Ok((child, BTreeKey::new(cell[key_range.0..key_range.1].to_vec())))
}

fn cell_pointer_from_header(page: &Page, header: &BTreePageHeaderV3, index: usize) -> Result<u16> {
    if index >= header.cell_count as usize {
        return Err(HematiteError::StorageError(format!(
            "Cell index {} out of bounds for {} cells",
            index, header.cell_count
        )));
    }
    let offset = header.pointer_area_start() + index * 2;
    Ok(u16::from_be_bytes([page.data[offset], page.data[offset + 1]]))
}

fn write_u16_be(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn write_u32_be(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

#[cfg(test)]
#[derive(Debug)]
pub enum SearchResult {
    Found(BTreeValue),
    NotFound(PageId),
}
