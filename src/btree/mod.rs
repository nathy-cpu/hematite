//! B-tree index module for efficient data indexing and retrieval

pub mod cursor;
pub mod node;
pub mod tree;

use crate::error::{HematiteError, Result};
use crate::storage::{PageId, StorageEngine};

pub use node::BTreeNode;

pub const BTREE_ORDER: usize = 100; // Maximum children per node

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    Internal,
    Leaf,
}

#[derive(Debug, Clone)]
pub struct BTreeKey {
    pub data: Vec<u8>,
}

impl BTreeKey {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

impl PartialEq for BTreeKey {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for BTreeKey {}

impl PartialOrd for BTreeKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BTreeKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.data.cmp(&other.data)
    }
}

#[derive(Debug, Clone)]
pub struct BTreeValue {
    pub data: Vec<u8>,
}

impl BTreeValue {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

/// Main B-tree index interface
pub struct BTreeIndex {
    storage: StorageEngine,
    root_page_id: PageId,
}

impl BTreeIndex {
    pub fn new(storage: StorageEngine, root_page_id: PageId) -> Self {
        Self {
            storage,
            root_page_id,
        }
    }

    pub fn search(&mut self, key: &BTreeKey) -> Result<Option<BTreeValue>> {
        let mut current_page_id = self.root_page_id;

        loop {
            let page = self.storage.read_page(current_page_id)?;
            let node = node::BTreeNode::from_page(page)?;

            match node.search(key) {
                node::SearchResult::Found(value) => return Ok(Some(value)),
                node::SearchResult::NotFound(child_page_id) => match node.node_type {
                    NodeType::Leaf => return Ok(None),
                    NodeType::Internal => current_page_id = child_page_id,
                },
            }
        }
    }

    pub fn insert(&mut self, key: BTreeKey, value: BTreeValue) -> Result<()> {
        // TODO: Implement B-tree insertion with splitting
        let result = self.insert_recursive(self.root_page_id, key, value)?;

        if let Some((new_key, new_page_id)) = result {
            // Root split needed - create new root
            self.create_new_root(new_key, new_page_id)?;
        }

        Ok(())
    }

    fn insert_recursive(
        &mut self,
        page_id: PageId,
        key: BTreeKey,
        value: BTreeValue,
    ) -> Result<Option<(BTreeKey, PageId)>> {
        let page = self.storage.read_page(page_id)?;
        let mut node = node::BTreeNode::from_page(page.clone())?;

        match node.node_type {
            NodeType::Leaf => {
                if node.keys.len() < node::MAX_KEYS {
                    node.insert_leaf(key, value)?;
                    self.storage.write_page(page.clone())?;
                    Ok(None)
                } else {
                    // Leaf split needed
                    let (new_key, new_page_id) = node.split_leaf(&mut self.storage, key, value)?;
                    Ok(Some((new_key, new_page_id)))
                }
            }
            NodeType::Internal => {
                let child_page_id = node.find_child(&key);
                let split_result = self.insert_recursive(child_page_id, key, value)?;

                if let Some((split_key, split_page_id)) = split_result {
                    if node.keys.len() < node::MAX_KEYS {
                        node.insert_internal(split_key, split_page_id)?;
                        self.storage.write_page(page)?;
                        Ok(None)
                    } else {
                        // Internal split needed
                        let (new_key, new_page_id) =
                            node.split_internal(&mut self.storage, split_key, split_page_id)?;
                        Ok(Some((new_key, new_page_id)))
                    }
                } else {
                    self.storage.write_page(page)?;
                    Ok(None)
                }
            }
        }
    }

    fn create_new_root(&mut self, key: BTreeKey, right_page_id: PageId) -> Result<()> {
        let new_root_page_id = self.storage.allocate_page()?;
        let mut new_root_page = crate::storage::Page::new(new_root_page_id);

        let mut new_root = node::BTreeNode::new_internal(new_root_page_id);
        new_root.keys.push(key);
        new_root.children.push(self.root_page_id);
        new_root.children.push(right_page_id);

        node::BTreeNode::to_page(&new_root, &mut new_root_page)?;
        self.storage.write_page(new_root_page)?;

        self.root_page_id = new_root_page_id;
        Ok(())
    }

    pub fn delete(&mut self, _key: &BTreeKey) -> Result<Option<BTreeValue>> {
        // TODO: Implement B-tree deletion with merging
        Err(HematiteError::StorageError(
            "Delete not implemented yet".to_string(),
        ))
    }
}
