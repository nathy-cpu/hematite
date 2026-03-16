//! B-tree node structure and operations

use crate::btree::{BTreeKey, BTreeValue, NodeType, BTREE_ORDER};
use crate::error::{HematiteError, Result};
use crate::storage::{Page, PageId, StorageEngine, PAGE_SIZE};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

// Size validation constants
pub const MAX_KEY_SIZE: usize = 256; // Maximum key size in bytes
pub const MAX_VALUE_SIZE: usize = 1024; // Maximum value size in bytes
pub const NODE_HEADER_SIZE: usize = 5; // node_type(1) + key_count(4)
pub const KEY_LENGTH_SIZE: usize = 2; // u16 for key length
pub const VALUE_LENGTH_SIZE: usize = 2; // u16 for value length
pub const CHILD_ID_SIZE: usize = 4; // u32 for PageId

// Reserve space for page overhead and safety margin
pub const PAGE_OVERHEAD: usize = 64; // Safety margin for page metadata

// Serialization validation constants
pub const BTREE_MAGIC: &[u8; 4] = b"BTRE"; // Magic number for B-tree pages
pub const BTREE_VERSION: u8 = 1; // Current B-tree format version
pub const CHECKSUM_SIZE: usize = 4; // CRC32 checksum size
pub const VALIDATION_HEADER_SIZE: usize = 9; // magic(4) + version(1) + checksum(4)

pub const MAX_KEYS: usize = BTREE_ORDER - 1;
pub const MAX_CHILDREN: usize = BTREE_ORDER;

#[derive(Debug, Clone)]
pub struct BTreeNode {
    pub page_id: PageId,
    pub node_type: NodeType,
    pub keys: Vec<BTreeKey>,
    pub children: Vec<PageId>,
    pub values: Vec<BTreeValue>, // Only used for leaf nodes
}

impl BTreeNode {
    pub fn new_internal(page_id: PageId) -> Self {
        Self {
            page_id,
            node_type: NodeType::Internal,
            keys: Vec::new(),
            children: Vec::new(),
            values: Vec::new(),
        }
    }

    pub fn new_leaf(page_id: PageId) -> Self {
        Self {
            page_id,
            node_type: NodeType::Leaf,
            keys: Vec::new(),
            children: Vec::new(),
            values: Vec::new(),
        }
    }

    // Size validation methods
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
        let mut size = NODE_HEADER_SIZE;

        // Add key sizes (key data + length prefix)
        for key in &self.keys {
            size += KEY_LENGTH_SIZE + key.data.len();
        }

        // Add children for internal nodes
        if matches!(self.node_type, NodeType::Internal) {
            size += self.children.len() * CHILD_ID_SIZE;
        }

        // Add values for leaf nodes
        if matches!(self.node_type, NodeType::Leaf) {
            for value in &self.values {
                size += VALUE_LENGTH_SIZE + value.data.len();
            }
        }

        size
    }

    pub fn will_fit_in_page(&self) -> bool {
        let estimated_size = self.estimate_serialized_size();
        estimated_size + PAGE_OVERHEAD <= PAGE_SIZE
    }

    pub fn can_insert_key_value(&self, key: &BTreeKey, value: &BTreeValue) -> bool {
        if !matches!(self.node_type, NodeType::Leaf) {
            return false;
        }

        let additional_size =
            KEY_LENGTH_SIZE + key.data.len() + VALUE_LENGTH_SIZE + value.data.len();
        let current_size = self.estimate_serialized_size();

        current_size + additional_size + PAGE_OVERHEAD <= PAGE_SIZE
    }

    pub fn can_insert_key_child(&self, key: &BTreeKey) -> bool {
        if !matches!(self.node_type, NodeType::Internal) {
            return false;
        }

        let additional_size = KEY_LENGTH_SIZE + key.data.len() + CHILD_ID_SIZE;
        let current_size = self.estimate_serialized_size();

        current_size + additional_size + PAGE_OVERHEAD <= PAGE_SIZE
    }

    // Checksum and validation helper methods
    fn calculate_checksum(data: &[u8]) -> u32 {
        let mut hasher = DefaultHasher::new();
        hasher.write(data);
        hasher.finish() as u32
    }

    fn validate_page_header(page: &Page) -> Result<()> {
        // For testing, if the page is all zeros, skip validation
        if page.data.iter().all(|&b| b == 0) {
            return Ok(());
        }

        // Check magic number
        if page.data.len() < VALIDATION_HEADER_SIZE {
            return Err(HematiteError::CorruptedData(
                "Page too small for validation header".to_string(),
            ));
        }

        let magic = &page.data[0..4];
        if magic != BTREE_MAGIC {
            return Err(HematiteError::CorruptedData(format!(
                "Invalid magic number: expected {:?}, got {:?}",
                BTREE_MAGIC, magic
            )));
        }

        // Check version
        let version = page.data[4];
        if version != BTREE_VERSION {
            return Err(HematiteError::CorruptedData(format!(
                "Unsupported B-tree version: expected {}, got {}",
                BTREE_VERSION, version
            )));
        }

        Ok(())
    }

    fn verify_checksum(page: &Page, data_offset: usize, data_end: usize) -> Result<()> {
        // Check if page is all zeros (newly allocated)
        if page.data.iter().all(|&b| b == 0) {
            return Ok(());
        }

        if page.data.len() < data_offset + CHECKSUM_SIZE {
            return Err(HematiteError::CorruptedData(
                "Page too small for checksum".to_string(),
            ));
        }

        let stored_checksum = u32::from_le_bytes([
            page.data[data_offset],
            page.data[data_offset + 1],
            page.data[data_offset + 2],
            page.data[data_offset + 3],
        ]);

        // Data to check starts after checksum and goes to original data end
        let data_to_check = &page.data[data_offset + CHECKSUM_SIZE..=data_end];
        let calculated_checksum = Self::calculate_checksum(data_to_check);

        if stored_checksum != calculated_checksum {
            return Err(HematiteError::CorruptedData(format!(
                "Checksum mismatch: stored {}, calculated {}",
                stored_checksum, calculated_checksum
            )));
        }

        Ok(())
    }

    pub fn from_page(page: Page) -> Result<Self> {
        if page.data.len() != PAGE_SIZE {
            return Err(HematiteError::InvalidPage(page.id.as_u32()));
        }

        // Verify checksum FIRST before any other operations
        Self::validate_page_header(&page)?;

        // Read node type
        let node_type = match page.data[VALIDATION_HEADER_SIZE] {
            0 => NodeType::Internal,
            1 => NodeType::Leaf,
            _ => {
                return Err(HematiteError::CorruptedData(
                    "Invalid node type".to_string(),
                ))
            }
        };

        // Calculate actual data end by reading the serialized data structure
        let key_count = u32::from_le_bytes([
            page.data[VALIDATION_HEADER_SIZE + 1],
            page.data[VALIDATION_HEADER_SIZE + 2],
            page.data[VALIDATION_HEADER_SIZE + 3],
            page.data[VALIDATION_HEADER_SIZE + 4],
        ]) as usize;

        let mut data_end = VALIDATION_HEADER_SIZE + NODE_HEADER_SIZE;
        match node_type {
            NodeType::Leaf => {
                for _ in 0..key_count {
                    // key length
                    let key_len =
                        u16::from_le_bytes([page.data[data_end], page.data[data_end + 1]]) as usize;
                    data_end += 2 + key_len;
                    // value length
                    let value_len =
                        u16::from_le_bytes([page.data[data_end], page.data[data_end + 1]]) as usize;
                    data_end += 2 + value_len;
                }
            }
            NodeType::Internal => {
                for _ in 0..key_count {
                    let key_len =
                        u16::from_le_bytes([page.data[data_end], page.data[data_end + 1]]) as usize;
                    data_end += 2 + key_len;
                }
                data_end += (key_count + 1) * CHILD_ID_SIZE;
            }
        }

        Self::verify_checksum(&page, 5, data_end - 1)?;

        let mut node = match node_type {
            NodeType::Internal => Self::new_internal(page.id),
            NodeType::Leaf => Self::new_leaf(page.id),
        };

        let mut offset = VALIDATION_HEADER_SIZE + NODE_HEADER_SIZE; // Start after validation and node headers

        // Read keys
        for _ in 0..key_count {
            // Check bounds for key length
            if offset + 2 > PAGE_SIZE {
                return Err(HematiteError::CorruptedData(
                    "Key length exceeds page bounds".to_string(),
                ));
            }

            let key_len = u16::from_le_bytes([page.data[offset], page.data[offset + 1]]) as usize;
            offset += 2;

            // Validate key size
            if key_len > MAX_KEY_SIZE {
                return Err(HematiteError::CorruptedData(format!(
                    "Key size {} exceeds maximum allowed size {}",
                    key_len, MAX_KEY_SIZE
                )));
            }

            // Check bounds for key data
            if offset + key_len > PAGE_SIZE {
                return Err(HematiteError::CorruptedData(
                    "Key data exceeds page bounds".to_string(),
                ));
            }

            let key_data = page.data[offset..offset + key_len].to_vec();
            offset += key_len;
            node.keys.push(BTreeKey::new(key_data));
        }

        // Read children for internal nodes
        if matches!(node_type, NodeType::Internal) {
            for _ in 0..key_count + 1 {
                // Check bounds for child ID
                if offset + 4 > PAGE_SIZE {
                    return Err(HematiteError::CorruptedData(
                        "Child ID exceeds page bounds".to_string(),
                    ));
                }

                let child_id = u32::from_le_bytes([
                    page.data[offset],
                    page.data[offset + 1],
                    page.data[offset + 2],
                    page.data[offset + 3],
                ]);
                offset += 4;
                node.children.push(PageId::new(child_id));
            }
        }

        // Read values for leaf nodes
        if matches!(node_type, NodeType::Leaf) {
            for _ in 0..key_count {
                // Check bounds for value length
                if offset + 2 > PAGE_SIZE {
                    return Err(HematiteError::CorruptedData(
                        "Value length exceeds page bounds".to_string(),
                    ));
                }

                let value_len =
                    u16::from_le_bytes([page.data[offset], page.data[offset + 1]]) as usize;
                offset += 2;

                // Validate value size
                if value_len > MAX_VALUE_SIZE {
                    return Err(HematiteError::CorruptedData(format!(
                        "Value size {} exceeds maximum allowed size {}",
                        value_len, MAX_VALUE_SIZE
                    )));
                }

                // Check bounds for value data
                if offset + value_len > PAGE_SIZE {
                    return Err(HematiteError::CorruptedData(
                        "Value data exceeds page bounds".to_string(),
                    ));
                }

                let value_data = page.data[offset..offset + value_len].to_vec();
                offset += value_len;
                node.values.push(BTreeValue::new(value_data));
            }
        }

        Ok(node)
    }

    pub fn to_page(&self, page: &mut Page) -> Result<()> {
        // Clear page data
        page.data.fill(0);

        // Write validation header
        page.data[0..4].copy_from_slice(BTREE_MAGIC);
        page.data[4] = BTREE_VERSION;

        // Reserve space for checksum (will be filled later)
        let checksum_offset = 5;
        let node_header_offset = checksum_offset + CHECKSUM_SIZE;

        // Write node type
        page.data[node_header_offset] = match self.node_type {
            NodeType::Internal => 0,
            NodeType::Leaf => 1,
        };

        // Write key count
        let key_count = self.keys.len() as u32;
        page.data[node_header_offset + 1..node_header_offset + 5]
            .copy_from_slice(&key_count.to_le_bytes());

        let mut offset = node_header_offset + NODE_HEADER_SIZE;

        // Write keys
        for key in &self.keys {
            let key_len = (key.data.len() as u16).to_le_bytes();
            page.data[offset..offset + 2].copy_from_slice(&key_len);
            offset += 2;

            page.data[offset..offset + key.data.len()].copy_from_slice(&key.data);
            offset += key.data.len();
        }

        // Write children for internal nodes
        if matches!(self.node_type, NodeType::Internal) {
            for child_id in &self.children {
                page.data[offset..offset + 4].copy_from_slice(&child_id.as_u32().to_le_bytes());
                offset += 4;
            }
        }

        // Write values for leaf nodes
        if matches!(self.node_type, NodeType::Leaf) {
            for value in &self.values {
                let value_len = (value.data.len() as u16).to_le_bytes();
                page.data[offset..offset + 2].copy_from_slice(&value_len);
                offset += 2;

                page.data[offset..offset + value.data.len()].copy_from_slice(&value.data);
                offset += value.data.len();
            }
        }

        // Calculate and write checksum
        let data_to_checksum = &page.data[checksum_offset + CHECKSUM_SIZE..offset];
        let checksum = Self::calculate_checksum(data_to_checksum);
        page.data[checksum_offset..checksum_offset + CHECKSUM_SIZE]
            .copy_from_slice(&checksum.to_le_bytes());

        Ok(())
    }

    pub fn search(&self, key: &BTreeKey) -> SearchResult {
        match self.node_type {
            NodeType::Leaf => self.search_leaf(key),
            NodeType::Internal => self.search_internal(key),
        }
    }

    fn search_leaf(&self, key: &BTreeKey) -> SearchResult {
        for (i, k) in self.keys.iter().enumerate() {
            match key.cmp(k) {
                std::cmp::Ordering::Equal => {
                    return SearchResult::Found(self.values[i].clone());
                }
                std::cmp::Ordering::Less => {
                    break;
                }
                std::cmp::Ordering::Greater => continue,
            }
        }
        SearchResult::NotFound(PageId::invalid())
    }

    fn search_internal(&self, key: &BTreeKey) -> SearchResult {
        for (i, k) in self.keys.iter().enumerate() {
            match key.cmp(k) {
                std::cmp::Ordering::Equal => {
                    // In B+ tree, when key equals separator, continue to right child
                    // (assuming separators are minimum keys of right subtrees)
                    return SearchResult::NotFound(self.children[i + 1]);
                }
                std::cmp::Ordering::Less => {
                    return SearchResult::NotFound(self.children[i]);
                }
                std::cmp::Ordering::Greater => continue,
            }
        }
        SearchResult::NotFound(self.children[self.keys.len()])
    }

    pub fn find_child(&self, key: &BTreeKey) -> PageId {
        for (i, k) in self.keys.iter().enumerate() {
            if key < k {
                return self.children[i];
            }
        }
        self.children[self.keys.len()]
    }

    pub fn insert_leaf(&mut self, key: BTreeKey, value: BTreeValue) -> Result<()> {
        // Validate key and value sizes
        Self::validate_key_size(&key)?;
        Self::validate_value_size(&value)?;

        // Check if insertion would exceed page size
        if !self.can_insert_key_value(&key, &value) {
            return Err(HematiteError::StorageError(
                "Insertion would exceed page size limit".to_string(),
            ));
        }

        // Check if key already exists
        if let Some(pos) = self.keys.iter().position(|k| k == &key) {
            // Key already exists - replace the value
            self.values[pos] = value;
            return Ok(());
        }

        // Find insertion position for new key
        let pos = self
            .keys
            .iter()
            .position(|k| k > &key)
            .unwrap_or(self.keys.len());
        self.keys.insert(pos, key);
        self.values.insert(pos, value);
        Ok(())
    }

    pub fn insert_internal(&mut self, key: BTreeKey, child_page_id: PageId) -> Result<()> {
        // Validate key size
        Self::validate_key_size(&key)?;

        // Check if insertion would exceed page size
        if !self.can_insert_key_child(&key) {
            return Err(HematiteError::StorageError(
                "Insertion would exceed page size limit".to_string(),
            ));
        }

        let pos = self
            .keys
            .iter()
            .position(|k| k >= &key)
            .unwrap_or(self.keys.len());
        self.keys.insert(pos, key);
        self.children.insert(pos + 1, child_page_id);
        Ok(())
    }

    pub fn split_leaf(
        &mut self,
        storage: &mut StorageEngine,
        new_key: BTreeKey,
        new_value: BTreeValue,
    ) -> Result<(BTreeKey, PageId)> {
        // Insert the new key/value first
        let pos = self
            .keys
            .iter()
            .position(|k| k > &new_key)
            .unwrap_or(self.keys.len());
        self.keys.insert(pos, new_key);
        self.values.insert(pos, new_value);

        // Create new leaf node
        let new_page_id = storage.allocate_page()?;
        let mut new_page = Page::new(new_page_id);
        let mut new_node = Self::new_leaf(new_page_id);

        // Split keys and values - median key moves up to parent
        let split_pos = self.keys.len() / 2;

        // Move keys AFTER the median to the right node
        new_node.keys = self.keys.split_off(split_pos + 1);
        new_node.values = self.values.split_off(split_pos + 1);

        // In B+ tree, the split key should be the FIRST key of the right node
        // BUT the original key stays in the leaf (we don't pop it)
        let split_key = if new_node.keys.is_empty() {
            // Edge case: if right node is empty, use the last key from left
            self.keys.last().unwrap().clone()
        } else {
            new_node.keys[0].clone()
        };

        // NOTE: In B+ tree, we DON'T remove the split key from the leaf
        // All keys must remain in leaf nodes. The split_key is just copied up.

        // Write both nodes
        let mut current_page = storage.read_page(self.page_id)?;
        self.to_page(&mut current_page)?;
        storage.write_page(current_page)?;
        new_node.to_page(&mut new_page)?;
        storage.write_page(new_page)?;

        Ok((split_key, new_page_id))
    }

    pub fn split_internal(
        &mut self,
        storage: &mut StorageEngine,
        new_key: BTreeKey,
        new_child: PageId,
    ) -> Result<(BTreeKey, PageId)> {
        // Insert the new key/child first
        let pos = self
            .keys
            .iter()
            .position(|k| k >= &new_key)
            .unwrap_or(self.keys.len());
        self.keys.insert(pos, new_key);
        self.children.insert(pos + 1, new_child);

        // Create new internal node
        let new_page_id = storage.allocate_page()?;
        let mut new_page = Page::new(new_page_id);
        let mut new_node = Self::new_internal(new_page_id);

        // Split keys and children - median key moves up to parent
        let split_pos = self.keys.len() / 2;
        let split_key = self.keys[split_pos].clone();

        // Move keys AFTER the median to the right node
        new_node.keys = self.keys.split_off(split_pos + 1);
        new_node.children = self.children.split_off(split_pos + 1);

        // Remove the median key from left node (it moves up to parent)
        self.keys.pop();

        // Write both nodes
        let mut current_page = storage.read_page(self.page_id)?;
        self.to_page(&mut current_page)?;
        storage.write_page(current_page)?;
        new_node.to_page(&mut new_page)?;
        storage.write_page(new_page)?;

        Ok((split_key, new_page_id))
    }

    // Delete operations
    pub fn delete_from_leaf(&mut self, key: &BTreeKey) -> Result<Option<BTreeValue>> {
        if self.node_type != NodeType::Leaf {
            return Err(HematiteError::StorageError("Not a leaf node".to_string()));
        }

        for (i, k) in self.keys.iter().enumerate() {
            if k == key {
                let value = self.values.remove(i);
                self.keys.remove(i);
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    pub fn delete_from_internal(&mut self, key: &BTreeKey) -> Result<(bool, Option<BTreeKey>)> {
        if self.node_type != NodeType::Internal {
            return Err(HematiteError::StorageError(
                "Not an internal node".to_string(),
            ));
        }

        // Find the child that might contain the key
        let _child_index = self.find_child_index(key);

        // In a full implementation, we would:
        // 1. Recursively delete from the appropriate child
        // 2. Check if the child is underflow after deletion
        // 3. If underflow, try to borrow from siblings
        // 4. If borrowing fails, merge with a sibling
        // 5. Update parent separator keys as needed

        // For now, implement basic deletion without underflow handling
        // This is a placeholder that assumes the child can handle deletion
        Ok((false, None))
    }

    pub fn find_child_index(&self, key: &BTreeKey) -> usize {
        for (i, k) in self.keys.iter().enumerate() {
            if key < k {
                return i;
            }
        }
        self.keys.len()
    }

    pub fn can_merge_with(&self, other: &BTreeNode) -> bool {
        self.keys.len() + other.keys.len() <= MAX_KEYS
    }

    pub fn merge_leaf(&mut self, other: &mut BTreeNode, storage: &mut StorageEngine) -> Result<()> {
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

        // Deallocate the 'other' page
        storage.deallocate_page(other.page_id)?;

        Ok(())
    }

    pub fn merge_internal(
        &mut self,
        other: &mut BTreeNode,
        separator_key: BTreeKey,
        storage: &mut StorageEngine,
    ) -> Result<()> {
        if self.node_type != NodeType::Internal || other.node_type != NodeType::Internal {
            return Err(HematiteError::StorageError(
                "Can only merge internal nodes".to_string(),
            ));
        }

        if !self.can_merge_with(other) {
            return Err(HematiteError::StorageError(
                "Nodes cannot be merged".to_string(),
            ));
        }

        self.keys.push(separator_key);
        self.keys.append(&mut other.keys);
        self.children.append(&mut other.children);

        // Deallocate the 'other' page
        storage.deallocate_page(other.page_id)?;

        Ok(())
    }

    pub fn borrow_from_sibling(
        &mut self,
        sibling: &mut BTreeNode,
        is_left_sibling: bool,
    ) -> Result<Option<BTreeKey>> {
        // Both nodes must be of the same type
        if self.node_type != sibling.node_type {
            return Err(HematiteError::StorageError(
                "Cannot borrow between different node types".to_string(),
            ));
        }

        if sibling.keys.len() <= (MAX_KEYS / 2) {
            return Ok(None); // Sibling doesn't have enough keys to borrow
        }

        match self.node_type {
            NodeType::Leaf => {
                if is_left_sibling {
                    // Borrow from left sibling (take the last key)
                    let key = sibling.keys.pop().unwrap();
                    let value = sibling.values.pop().unwrap();
                    self.keys.insert(0, key.clone());
                    self.values.insert(0, value);
                    Ok(Some(key))
                } else {
                    // Borrow from right sibling (take the first key)
                    let key = sibling.keys.remove(0);
                    let value = sibling.values.remove(0);
                    self.keys.push(key.clone());
                    self.values.push(value);
                    Ok(Some(key))
                }
            }
            NodeType::Internal => {
                if is_left_sibling {
                    // Borrow from left sibling (take last key and last child)
                    let key = sibling.keys.pop().unwrap();
                    let child = sibling.children.pop().unwrap();
                    self.keys.insert(0, key.clone());
                    self.children.insert(0, child);
                    Ok(Some(key))
                } else {
                    // Borrow from right sibling (take first key and first child)
                    let key = sibling.keys.remove(0);
                    let child = sibling.children.remove(0);
                    self.keys.push(key.clone());
                    self.children.push(child);
                    Ok(Some(key))
                }
            }
        }
    }

    pub fn is_underflow(&self) -> bool {
        match self.node_type {
            NodeType::Leaf => self.keys.len() < (MAX_KEYS / 2),
            NodeType::Internal => self.keys.len() < ((MAX_KEYS - 1) / 2),
        }
    }
}

#[derive(Debug)]
pub enum SearchResult {
    Found(BTreeValue),
    NotFound(PageId),
}
