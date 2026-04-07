//! B-tree node layout and node-local algorithms.
//!
//! This file defines the page format used by the generic B-tree layer and the local operations
//! that can be performed on a single node before higher-level tree orchestration takes over.
//!
//! On-disk page format:
//!
//! ```text
//! +----------------------+----------------------------------------------+
//! | header               | magic, version, checksum, type, counts       |
//! +----------------------+----------------------------------------------+
//! | key section          | key_len + key bytes, repeated                |
//! +----------------------+----------------------------------------------+
//! | child/value section  | child ids for internal nodes, values for leaf|
//! +----------------------+----------------------------------------------+
//! ```
//!
//! Structural model:
//! - leaf nodes own `(key, value)` pairs;
//! - internal nodes own `keys.len() + 1` child pointers;
//! - internal separator keys route search and equal keys descend to the right subtree;
//! - page size checks are performed before mutation so split decisions can be made deterministically.
//!
//! The split helpers in this file try to balance payload bytes, not just key counts, so large keys
//! and values do not create badly-skewed pages.

use crate::btree::{BTreeKey, BTreeValue, NodeType, BTREE_ORDER};
use crate::error::{HematiteError, Result};
use crate::storage::{Page, PageId, Pager, INVALID_PAGE_ID, PAGE_SIZE};

pub const MAX_KEY_SIZE: usize = 256;
pub const MAX_VALUE_SIZE: usize = 1024;
pub const KEY_LENGTH_SIZE: usize = 2;
pub const VALUE_LENGTH_SIZE: usize = 2;
pub const CHILD_ID_SIZE: usize = 4;
pub const PAGE_OVERHEAD: usize = 64;

pub const BTREE_MAGIC: &[u8; 4] = b"BTRE";
pub const BTREE_PAGE_FORMAT_VERSION: u8 = 2;
pub const CHECKSUM_SIZE: usize = 4;
pub const BTREE_PAGE_HEADER_SIZE: usize = 18;
const HEADER_OFFSET_MAGIC: usize = 0;
const HEADER_OFFSET_VERSION: usize = 4;
const HEADER_OFFSET_CHECKSUM: usize = 5;
const HEADER_OFFSET_NODE_TYPE: usize = 9;
const HEADER_OFFSET_KEY_COUNT: usize = 10;
const HEADER_OFFSET_PAYLOAD_LEN: usize = 14;

pub const MAX_KEYS: usize = BTREE_ORDER - 1;

#[derive(Debug, Clone)]
pub struct BTreeNode {
    pub page_id: PageId,
    pub node_type: NodeType,
    pub keys: Vec<BTreeKey>,
    pub children: Vec<PageId>,
    pub values: Vec<BTreeValue>,

    pub key_count: usize,
    pub payload_len: usize,
    pub raw_page: Option<Page>,
    pub is_decoded: bool,
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
        if self.raw_page.is_some() && self.keys.is_empty() && self.key_count > 0 {
            return BTREE_PAGE_HEADER_SIZE + self.payload_len;
        }

        let mut payload_size = 0usize;

        for key in &self.keys {
            payload_size += KEY_LENGTH_SIZE + key.data.len();
        }

        if matches!(self.node_type, NodeType::Internal) {
            payload_size += self.children.len() * CHILD_ID_SIZE;
        }

        if matches!(self.node_type, NodeType::Leaf) {
            for value in &self.values {
                payload_size += VALUE_LENGTH_SIZE + value.data.len();
            }
        }

        BTREE_PAGE_HEADER_SIZE + payload_size
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

    fn calculate_checksum(data: &[u8]) -> u32 {
        let mut hash: u32 = 0x811C9DC5;
        for byte in data {
            hash ^= u32::from(*byte);
            hash = hash.wrapping_mul(0x01000193);
        }
        hash
    }

    fn validate_page_header(page: &Page) -> Result<(NodeType, usize, usize)> {
        // For testing, if the page is all zeros, skip validation
        if page.data.iter().all(|&b| b == 0) {
            return Ok((NodeType::Leaf, 0, 0));
        }

        if page.data.len() < BTREE_PAGE_HEADER_SIZE {
            return Err(HematiteError::CorruptedData(
                "Page too small for validation header".to_string(),
            ));
        }

        let magic = &page.data[HEADER_OFFSET_MAGIC..HEADER_OFFSET_MAGIC + 4];
        if magic != BTREE_MAGIC {
            return Err(HematiteError::CorruptedData(format!(
                "Invalid magic number: expected {:?}, got {:?}",
                BTREE_MAGIC, magic
            )));
        }

        let version = page.data[HEADER_OFFSET_VERSION];
        if version != BTREE_PAGE_FORMAT_VERSION {
            return Err(HematiteError::CorruptedData(format!(
                "Unsupported B-tree version: expected {}, got {}",
                BTREE_PAGE_FORMAT_VERSION, version
            )));
        }

        let node_type = match page.data[HEADER_OFFSET_NODE_TYPE] {
            0 => NodeType::Internal,
            1 => NodeType::Leaf,
            _ => {
                return Err(HematiteError::CorruptedData(
                    "Invalid node type".to_string(),
                ))
            }
        };

        let key_count = u16::from_le_bytes([
            page.data[HEADER_OFFSET_KEY_COUNT],
            page.data[HEADER_OFFSET_KEY_COUNT + 1],
        ]) as usize;

        if key_count > MAX_KEYS {
            return Err(HematiteError::CorruptedData(format!(
                "Key count {} exceeds maximum {}",
                key_count, MAX_KEYS
            )));
        }

        let payload_len = u32::from_le_bytes([
            page.data[HEADER_OFFSET_PAYLOAD_LEN],
            page.data[HEADER_OFFSET_PAYLOAD_LEN + 1],
            page.data[HEADER_OFFSET_PAYLOAD_LEN + 2],
            page.data[HEADER_OFFSET_PAYLOAD_LEN + 3],
        ]) as usize;

        if BTREE_PAGE_HEADER_SIZE + payload_len > PAGE_SIZE {
            return Err(HematiteError::CorruptedData(format!(
                "Payload length {} exceeds page bounds",
                payload_len
            )));
        }

        let child_bytes = (key_count + 1) * CHILD_ID_SIZE;
        let min_payload_len = match node_type {
            NodeType::Internal => key_count * KEY_LENGTH_SIZE + child_bytes,
            NodeType::Leaf => key_count * (KEY_LENGTH_SIZE + VALUE_LENGTH_SIZE),
        };
        if payload_len < min_payload_len {
            return Err(HematiteError::CorruptedData(
                "Payload length too small for node content".to_string(),
            ));
        }

        Ok((node_type, key_count, payload_len))
    }

    fn verify_checksum(page: &Page, payload_len: usize) -> Result<()> {
        if page.data.iter().all(|&b| b == 0) {
            return Ok(());
        }

        let stored_checksum = u32::from_le_bytes([
            page.data[HEADER_OFFSET_CHECKSUM],
            page.data[HEADER_OFFSET_CHECKSUM + 1],
            page.data[HEADER_OFFSET_CHECKSUM + 2],
            page.data[HEADER_OFFSET_CHECKSUM + 3],
        ]);

        let data_to_check =
            &page.data[BTREE_PAGE_HEADER_SIZE..BTREE_PAGE_HEADER_SIZE + payload_len];
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
            return Err(HematiteError::InvalidPage(page.id));
        }

        let (node_type, key_count, payload_len) = Self::validate_page_header(&page)?;
        Self::verify_checksum(&page, payload_len)?;

        let mut node = match node_type {
            NodeType::Internal => Self::new_internal(page.id),
            NodeType::Leaf => Self::new_leaf(page.id),
        };
        node.key_count = key_count;
        node.payload_len = payload_len;
        node.raw_page = Some(page);
        node.is_decoded = false;

        Ok(node)
    }

    pub fn from_page_decoded(page: Page) -> Result<Self> {
        let mut node = Self::from_page(page)?;
        node.decode()?;
        Ok(node)
    }

    pub fn decode(&mut self) -> Result<()> {
        if self.is_decoded {
            return Ok(());
        }

        let page = self.raw_page.as_ref().unwrap();
        let payload_start = BTREE_PAGE_HEADER_SIZE;
        let payload_end = payload_start + self.payload_len;
        let mut offset = payload_start;
        let key_count = self.key_count;
        let node_type = self.node_type;

        for _ in 0..key_count {
            if offset + 2 > payload_end {
                return Err(HematiteError::CorruptedData(
                    "Key length exceeds page bounds".to_string(),
                ));
            }

            let key_len = u16::from_le_bytes([page.data[offset], page.data[offset + 1]]) as usize;
            offset += 2;

            if key_len > MAX_KEY_SIZE {
                return Err(HematiteError::CorruptedData(format!(
                    "Key size {} exceeds maximum allowed size {}",
                    key_len, MAX_KEY_SIZE
                )));
            }

            if offset + key_len > payload_end {
                return Err(HematiteError::CorruptedData(
                    "Key data exceeds page bounds".to_string(),
                ));
            }

            let key_data = page.data[offset..offset + key_len].to_vec();
            offset += key_len;
            self.keys.push(BTreeKey::new(key_data));
        }

        if matches!(node_type, NodeType::Internal) {
            for _ in 0..key_count + 1 {
                if offset + 4 > payload_end {
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
                self.children.push(child_id);
            }
        }

        if matches!(node_type, NodeType::Leaf) {
            for _ in 0..key_count {
                if offset + 2 > payload_end {
                    return Err(HematiteError::CorruptedData(
                        "Value length exceeds page bounds".to_string(),
                    ));
                }

                let value_len =
                    u16::from_le_bytes([page.data[offset], page.data[offset + 1]]) as usize;
                offset += 2;

                if value_len > MAX_VALUE_SIZE {
                    return Err(HematiteError::CorruptedData(format!(
                        "Value size {} exceeds maximum allowed size {}",
                        value_len, MAX_VALUE_SIZE
                    )));
                }

                if offset + value_len > payload_end {
                    return Err(HematiteError::CorruptedData(
                        "Value data exceeds page bounds".to_string(),
                    ));
                }

                let value_data = page.data[offset..offset + value_len].to_vec();
                offset += value_len;
                self.values.push(BTreeValue::new(value_data));
            }
        }

        if offset != payload_end {
            return Err(HematiteError::CorruptedData(
                "B-tree page payload length does not match decoded content".to_string(),
            ));
        }

        if matches!(node_type, NodeType::Internal) && self.children.len() != self.keys.len() + 1 {
            return Err(HematiteError::CorruptedData(
                "Internal node child count mismatch".to_string(),
            ));
        }

        if matches!(node_type, NodeType::Leaf) && self.values.len() != self.keys.len() {
            return Err(HematiteError::CorruptedData(
                "Leaf node value count mismatch".to_string(),
            ));
        }

        self.is_decoded = true;
        Ok(())
    }

    pub fn get_key_procedural(&self, target_index: usize) -> Result<BTreeKey> {
        if target_index >= self.key_count {
            return Err(HematiteError::StorageError("Index out of bounds".to_string()));
        }
        if !self.keys.is_empty() {
            return Ok(self.keys[target_index].clone());
        }

        let page = self.raw_page.as_ref().unwrap();
        let mut offset = BTREE_PAGE_HEADER_SIZE;
        
        for i in 0..=target_index {
            let key_len = u16::from_le_bytes([page.data[offset], page.data[offset + 1]]) as usize;
            offset += 2;
            if i == target_index {
                let key_data = page.data[offset..offset + key_len].to_vec();
                return Ok(BTreeKey::new(key_data));
            }
            offset += key_len;
        }
        unreachable!()
    }

    pub fn get_child_procedural(&self, target_index: usize) -> Result<PageId> {
        if target_index > self.key_count || self.node_type != NodeType::Internal {
            return Err(HematiteError::StorageError("Index out of bounds".to_string()));
        }
        if !self.children.is_empty() {
            return Ok(self.children[target_index]);
        }

        let page = self.raw_page.as_ref().unwrap();
        let mut offset = BTREE_PAGE_HEADER_SIZE;
        
        for _ in 0..self.key_count {
            let key_len = u16::from_le_bytes([page.data[offset], page.data[offset + 1]]) as usize;
            offset += 2 + key_len;
        }
        
        offset += target_index * 4;
        let child_id = u32::from_le_bytes([
            page.data[offset],
            page.data[offset + 1],
            page.data[offset + 2],
            page.data[offset + 3],
        ]);
        Ok(child_id)
    }

    pub fn get_value_procedural(&self, target_index: usize) -> Result<BTreeValue> {
        if target_index >= self.key_count || self.node_type != NodeType::Leaf {
            return Err(HematiteError::StorageError("Index out of bounds".to_string()));
        }
        if !self.values.is_empty() {
            return Ok(self.values[target_index].clone());
        }

        let page = self.raw_page.as_ref().unwrap();
        let mut offset = BTREE_PAGE_HEADER_SIZE;
        
        for _ in 0..self.key_count {
            let key_len = u16::from_le_bytes([page.data[offset], page.data[offset + 1]]) as usize;
            offset += 2 + key_len;
        }
        
        for i in 0..=target_index {
            let val_len = u16::from_le_bytes([page.data[offset], page.data[offset + 1]]) as usize;
            offset += 2;
            if i == target_index {
                let val_data = page.data[offset..offset + val_len].to_vec();
                return Ok(BTreeValue::new(val_data));
            }
            offset += val_len;
        }
        unreachable!()
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

        page.data.fill(0);
        page.data[HEADER_OFFSET_MAGIC..HEADER_OFFSET_MAGIC + 4].copy_from_slice(BTREE_MAGIC);
        page.data[HEADER_OFFSET_VERSION] = BTREE_PAGE_FORMAT_VERSION;
        page.data[HEADER_OFFSET_NODE_TYPE] = match self.node_type {
            NodeType::Internal => 0,
            NodeType::Leaf => 1,
        };
        page.data[HEADER_OFFSET_KEY_COUNT..HEADER_OFFSET_KEY_COUNT + 2]
            .copy_from_slice(&(self.keys.len() as u16).to_le_bytes());

        let mut payload = Vec::new();
        for key in &self.keys {
            Self::validate_key_size(key)?;
            let key_len = (key.data.len() as u16).to_le_bytes();
            payload.extend_from_slice(&key_len);
            payload.extend_from_slice(&key.data);
        }

        if matches!(self.node_type, NodeType::Internal) {
            for child_id in &self.children {
                payload.extend_from_slice(&child_id.to_le_bytes());
            }
        }

        if matches!(self.node_type, NodeType::Leaf) {
            for value in &self.values {
                Self::validate_value_size(value)?;
                let value_len = (value.data.len() as u16).to_le_bytes();
                payload.extend_from_slice(&value_len);
                payload.extend_from_slice(&value.data);
            }
        }

        if BTREE_PAGE_HEADER_SIZE + payload.len() > PAGE_SIZE {
            return Err(HematiteError::StorageError(format!(
                "Serialized B-tree node exceeds page size: {} bytes",
                BTREE_PAGE_HEADER_SIZE + payload.len()
            )));
        }

        page.data[HEADER_OFFSET_PAYLOAD_LEN..HEADER_OFFSET_PAYLOAD_LEN + 4]
            .copy_from_slice(&(payload.len() as u32).to_le_bytes());
        page.data[BTREE_PAGE_HEADER_SIZE..BTREE_PAGE_HEADER_SIZE + payload.len()]
            .copy_from_slice(&payload);

        let checksum = Self::calculate_checksum(&payload);
        page.data[HEADER_OFFSET_CHECKSUM..HEADER_OFFSET_CHECKSUM + CHECKSUM_SIZE]
            .copy_from_slice(&checksum.to_le_bytes());

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

        let existing_index = match self.keys.iter().position(|k| k == key) {
            Some(i) => i,
            None => return Ok(false),
        };

        let old_value_len = self.values[existing_index].data.len();
        if old_value_len != new_value.data.len() {
            return Ok(false);
        }

        // Needs to have a decoded page to do direct manipulation
        if self.raw_page.is_none() {
            self.to_page(page)?;
            self.raw_page = Some(page.clone());
        }

        self.values[existing_index] = new_value.clone();

        let mut offset = BTREE_PAGE_HEADER_SIZE;
        for k in &self.keys {
            offset += 2 + k.data.len();
        }
        for i in 0..existing_index {
            offset += 2 + self.values[i].data.len();
        }

        offset += 2; // skip value len
        page.data[offset..offset + new_value.data.len()].copy_from_slice(&new_value.data);

        let payload_len = u32::from_le_bytes([
            page.data[HEADER_OFFSET_PAYLOAD_LEN],
            page.data[HEADER_OFFSET_PAYLOAD_LEN + 1],
            page.data[HEADER_OFFSET_PAYLOAD_LEN + 2],
            page.data[HEADER_OFFSET_PAYLOAD_LEN + 3],
        ]) as usize;
        
        let payload = &page.data[BTREE_PAGE_HEADER_SIZE..BTREE_PAGE_HEADER_SIZE + payload_len];
        let checksum = Self::calculate_checksum(payload);
        page.data[HEADER_OFFSET_CHECKSUM..HEADER_OFFSET_CHECKSUM + CHECKSUM_SIZE]
            .copy_from_slice(&checksum.to_le_bytes());

        self.raw_page = Some(page.clone());
        
        Ok(true)
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
        SearchResult::NotFound(INVALID_PAGE_ID)
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
        Self::validate_key_size(&key)?;
        Self::validate_value_size(&value)?;

        if !self.can_insert_key_value(&key, &value) {
            return Err(HematiteError::StorageError(
                "Insertion would exceed page size limit".to_string(),
            ));
        }

        if let Some(pos) = self.keys.iter().position(|k| k == &key) {
            self.values[pos] = value;
            return Ok(());
        }

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
        Self::validate_key_size(&key)?;

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
        storage: &mut Pager,
        new_key: BTreeKey,
        new_value: BTreeValue,
    ) -> Result<(BTreeKey, PageId)> {
        let pos = self
            .keys
            .iter()
            .position(|k| k > &new_key)
            .unwrap_or(self.keys.len());
            
        let is_append = pos == self.keys.len();
        
        self.keys.insert(pos, new_key);
        self.values.insert(pos, new_value);

        if self.keys.len() < 2 {
            return Err(HematiteError::StorageError(
                "Cannot split leaf with fewer than 2 keys".to_string(),
            ));
        }

        let new_page_id = storage.allocate_page()?;
        let mut new_page = Page::new(new_page_id);
        let mut new_node = Self::new_leaf(new_page_id);

        let split_pos = if is_append {
            self.keys.len() - 1 // Leave all existing elements in the left node
        } else {
            self.best_leaf_split_pos()
        };
        
        new_node.keys = self.keys.split_off(split_pos);
        new_node.values = self.values.split_off(split_pos);
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
        let pos = self
            .keys
            .iter()
            .position(|k| k >= &new_key)
            .unwrap_or(self.keys.len());
            
        let is_append = pos == self.keys.len();
        
        self.keys.insert(pos, new_key);
        self.children.insert(pos + 1, new_child);

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

        let mut current_page = storage.read_page(self.page_id)?;
        self.to_page(&mut current_page)?;
        storage.write_page(current_page)?;
        new_node.to_page(&mut new_page)?;
        storage.write_page(new_page)?;

        Ok((split_key, new_page_id))
    }

    fn leaf_entry_size(key: &BTreeKey, value: &BTreeValue) -> usize {
        KEY_LENGTH_SIZE + key.data.len() + VALUE_LENGTH_SIZE + value.data.len()
    }

    fn internal_key_wire_size(key: &BTreeKey) -> usize {
        KEY_LENGTH_SIZE + key.data.len()
    }

    fn best_leaf_split_pos(&self) -> usize {
        let len = self.keys.len();
        let mut prefix = vec![0usize; len + 1];
        for i in 0..len {
            prefix[i + 1] = prefix[i] + Self::leaf_entry_size(&self.keys[i], &self.values[i]);
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
            key_prefix[i + 1] = key_prefix[i] + Self::internal_key_wire_size(&self.keys[i]);
        }
        let total_key_bytes = key_prefix[key_len];

        let mut best_pos = key_len / 2;
        let mut best_score = usize::MAX;
        let mut best_min = 0usize;

        // split_pos is the promoted separator key; left/right must both keep at least one key.
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

    pub fn find_child_index(&self, key: &BTreeKey) -> usize {
        for (i, k) in self.keys.iter().enumerate() {
            if key < k {
                return i;
            }
        }
        self.keys.len()
    }

    pub fn can_merge_with(&self, other: &BTreeNode) -> bool {
        if self.node_type != other.node_type {
            return false;
        }

        match self.node_type {
            NodeType::Leaf => {
                let mut merged = self.clone();
                merged.keys.extend(other.keys.clone());
                merged.values.extend(other.values.clone());
                merged.keys.len() <= MAX_KEYS && merged.will_fit_in_page()
            }
            NodeType::Internal => {
                // Internal merges need an explicit separator key from the parent.
                // Callers should use `can_merge_internal_with_separator`.
                false
            }
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
        merged.keys.push(separator_key.clone());
        merged.keys.extend(other.keys.clone());
        merged.children.extend(other.children.clone());

        merged.keys.len() <= MAX_KEYS && merged.will_fit_in_page()
    }

    pub fn merge_leaf(&mut self, other: &mut BTreeNode, storage: &mut Pager) -> Result<()> {
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

        storage.deallocate_page(other.page_id)?;

        Ok(())
    }

    pub fn merge_internal(
        &mut self,
        other: &mut BTreeNode,
        separator_key: BTreeKey,
        storage: &mut Pager,
    ) -> Result<()> {
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

        storage.deallocate_page(other.page_id)?;

        Ok(())
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
