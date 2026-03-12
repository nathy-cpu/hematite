//! B-tree node structure and operations

use crate::btree::{BTreeKey, BTreeValue, NodeType, BTREE_ORDER};
use crate::error::{HematiteError, Result};
use crate::storage::{Page, PageId, PAGE_SIZE, StorageEngine};

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

    pub fn from_page(page: Page) -> Result<Self> {
        if page.data.len() != PAGE_SIZE {
            return Err(HematiteError::InvalidPage(page.id.as_u32()));
        }

        // Read node type from first byte
        let node_type = match page.data[0] {
            0 => NodeType::Internal,
            1 => NodeType::Leaf,
            _ => return Err(HematiteError::CorruptedData("Invalid node type".to_string())),
        };

        // Read key count from next 4 bytes
        let key_count = u32::from_le_bytes([
            page.data[1], page.data[2], page.data[3], page.data[4]
        ]) as usize;

        let mut node = match node_type {
            NodeType::Internal => Self::new_internal(page.id),
            NodeType::Leaf => Self::new_leaf(page.id),
        };

        let mut offset = 5; // Start after header

        // Read keys
        for _ in 0..key_count {
            let key_len = u16::from_le_bytes([page.data[offset], page.data[offset + 1]]) as usize;
            offset += 2;
            
            let key_data = page.data[offset..offset + key_len].to_vec();
            offset += key_len;
            node.keys.push(BTreeKey::new(key_data));
        }

        // Read children for internal nodes
        if matches!(node_type, NodeType::Internal) {
            for _ in 0..key_count + 1 {
                let child_id = u32::from_le_bytes([
                    page.data[offset], page.data[offset + 1], 
                    page.data[offset + 2], page.data[offset + 3]
                ]);
                offset += 4;
                node.children.push(PageId::new(child_id));
            }
        }

        // Read values for leaf nodes
        if matches!(node_type, NodeType::Leaf) {
            for _ in 0..key_count {
                let value_len = u16::from_le_bytes([page.data[offset], page.data[offset + 1]]) as usize;
                offset += 2;
                
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

        // Write node type
        page.data[0] = match self.node_type {
            NodeType::Internal => 0,
            NodeType::Leaf => 1,
        };

        // Write key count
        let key_count = self.keys.len() as u32;
        page.data[1..5].copy_from_slice(&key_count.to_le_bytes());

        let mut offset = 5;

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
                    return SearchResult::Found(BTreeValue::new(vec![])); // Internal nodes don't store values
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
        let pos = self.keys.iter().position(|k| k >= &key).unwrap_or(self.keys.len());
        self.keys.insert(pos, key);
        self.values.insert(pos, value);
        Ok(())
    }

    pub fn insert_internal(&mut self, key: BTreeKey, child_page_id: PageId) -> Result<()> {
        let pos = self.keys.iter().position(|k| k >= &key).unwrap_or(self.keys.len());
        self.keys.insert(pos, key);
        self.children.insert(pos + 1, child_page_id);
        Ok(())
    }

    pub fn split_leaf(&mut self, storage: &mut StorageEngine, new_key: BTreeKey, new_value: BTreeValue) -> Result<(BTreeKey, PageId)> {
        // Insert the new key/value first
        self.insert_leaf(new_key, new_value)?;

        // Create new leaf node
        let new_page_id = storage.allocate_page()?;
        let mut new_page = Page::new(new_page_id);
        let mut new_node = Self::new_leaf(new_page_id);

        // Split keys and values
        let split_pos = self.keys.len() / 2;
        let split_key = self.keys[split_pos].clone();

        new_node.keys = self.keys.split_off(split_pos);
        new_node.values = self.values.split_off(split_pos);

        // Remove the split key from left node (it moves up)
        self.keys.pop();
        self.values.pop();

        // Write both nodes
        self.to_page(&mut storage.read_page(self.page_id)?)?;
        new_node.to_page(&mut new_page)?;
        storage.write_page(new_page)?;

        Ok((split_key, new_page_id))
    }

    pub fn split_internal(&mut self, storage: &mut StorageEngine, new_key: BTreeKey, new_child: PageId) -> Result<(BTreeKey, PageId)> {
        // Insert the new key/child first
        self.insert_internal(new_key, new_child)?;

        // Create new internal node
        let new_page_id = storage.allocate_page()?;
        let mut new_page = Page::new(new_page_id);
        let mut new_node = Self::new_internal(new_page_id);

        // Split keys and children
        let split_pos = self.keys.len() / 2;
        let split_key = self.keys[split_pos].clone();

        new_node.keys = self.keys.split_off(split_pos + 1); // +1 because split key moves up
        new_node.children = self.children.split_off(split_pos + 1);

        // Remove the split key from left node (it moves up)
        self.keys.pop();

        // Write both nodes
        self.to_page(&mut storage.read_page(self.page_id)?)?;
        new_node.to_page(&mut new_page)?;
        storage.write_page(new_page)?;

        Ok((split_key, new_page_id))
    }
}

#[derive(Debug)]
pub enum SearchResult {
    Found(BTreeValue),
    NotFound(PageId),
}
