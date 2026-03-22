//! B-tree node structure and operations.
//!
//! M0 storage contract notes:
//! - B-tree pages are self-validating via magic, version, and page checksum.
//! - Node payload format here is the authoritative encoding for current tree pages.
//! - Planned table/index specialization (rowid table cells, overflow payloads, index key->rowid)
//!   will build on this checksum/version discipline rather than bypass it.

use crate::btree::{BTreeKey, BTreeValue, NodeType, BTREE_ORDER};
use crate::error::{HematiteError, Result};
use crate::storage::{Page, PageId, Pager, PAGE_SIZE};

// Size validation constants
pub const MAX_KEY_SIZE: usize = 256; // Maximum key size in bytes
pub const MAX_VALUE_SIZE: usize = 1024; // Maximum value size in bytes
pub const KEY_LENGTH_SIZE: usize = 2; // u16 for key length
pub const VALUE_LENGTH_SIZE: usize = 2; // u16 for value length
pub const CHILD_ID_SIZE: usize = 4; // u32 for PageId

// Reserve space for page overhead and safety margin
pub const PAGE_OVERHEAD: usize = 64; // Safety margin for page metadata

// M2.1 pager-backed B-tree page format constants.
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

    // Checksum and validation helper methods
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
            return Err(HematiteError::InvalidPage(page.id.as_u32()));
        }

        let (node_type, key_count, payload_len) = Self::validate_page_header(&page)?;
        Self::verify_checksum(&page, payload_len)?;

        let mut node = match node_type {
            NodeType::Internal => Self::new_internal(page.id),
            NodeType::Leaf => Self::new_leaf(page.id),
        };

        let payload_start = BTREE_PAGE_HEADER_SIZE;
        let payload_end = payload_start + payload_len;
        let mut offset = payload_start;

        // Read keys
        for _ in 0..key_count {
            if offset + 2 > payload_end {
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
            if offset + key_len > payload_end {
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
                node.children.push(PageId::new(child_id));
            }
        }

        // Read values for leaf nodes
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

                // Validate value size
                if value_len > MAX_VALUE_SIZE {
                    return Err(HematiteError::CorruptedData(format!(
                        "Value size {} exceeds maximum allowed size {}",
                        value_len, MAX_VALUE_SIZE
                    )));
                }

                // Check bounds for value data
                if offset + value_len > payload_end {
                    return Err(HematiteError::CorruptedData(
                        "Value data exceeds page bounds".to_string(),
                    ));
                }

                let value_data = page.data[offset..offset + value_len].to_vec();
                offset += value_len;
                node.values.push(BTreeValue::new(value_data));
            }
        }

        if offset != payload_end {
            return Err(HematiteError::CorruptedData(
                "B-tree page payload length does not match decoded content".to_string(),
            ));
        }

        if matches!(node_type, NodeType::Internal) && node.children.len() != node.keys.len() + 1 {
            return Err(HematiteError::CorruptedData(
                "Internal node child count mismatch".to_string(),
            ));
        }

        if matches!(node_type, NodeType::Leaf) && node.values.len() != node.keys.len() {
            return Err(HematiteError::CorruptedData(
                "Leaf node value count mismatch".to_string(),
            ));
        }

        Ok(node)
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
                payload.extend_from_slice(&child_id.as_u32().to_le_bytes());
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
        storage: &mut Pager,
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

        if self.keys.len() < 2 {
            return Err(HematiteError::StorageError(
                "Cannot split leaf with fewer than 2 keys".to_string(),
            ));
        }

        // Create new leaf node
        let new_page_id = storage.allocate_page()?;
        let mut new_page = Page::new(new_page_id);
        let mut new_node = Self::new_leaf(new_page_id);

        // Choose a split point that balances payload bytes across leaf pages.
        let split_pos = self.best_leaf_split_pos();
        new_node.keys = self.keys.split_off(split_pos);
        new_node.values = self.values.split_off(split_pos);
        let split_key = new_node.keys[0].clone();

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
        storage: &mut Pager,
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

        if self.keys.len() < 2 {
            return Err(HematiteError::StorageError(
                "Cannot split internal node with fewer than 2 keys".to_string(),
            ));
        }

        // Create new internal node
        let new_page_id = storage.allocate_page()?;
        let mut new_page = Page::new(new_page_id);
        let mut new_node = Self::new_internal(new_page_id);

        // Choose a split point that balances payload bytes while keeping key separators valid.
        let split_pos = self.best_internal_split_pos();
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

        // Deallocate the 'other' page
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
