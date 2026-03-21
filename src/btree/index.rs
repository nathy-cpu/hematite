//! B-tree index implementation

use crate::btree::cursor::BTreeCursor;
use crate::btree::node::SearchResult;
use crate::btree::KeyValueCodec;
use crate::btree::{BTreeKey, BTreeNode, BTreeValue, NodeType};
use crate::error::{HematiteError, Result};
use crate::storage::{Page, PageId, StorageEngine};
use std::sync::{Arc, Mutex};

use super::node;

/// Main B-tree index interface
pub struct BTreeIndex {
    storage: Arc<Mutex<StorageEngine>>,
    root_page_id: PageId,
}

impl BTreeIndex {
    fn min_keys_for(node_type: NodeType) -> usize {
        match node_type {
            NodeType::Leaf => node::MAX_KEYS / 2,
            NodeType::Internal => (node::MAX_KEYS - 1) / 2,
        }
    }

    pub fn new(storage: StorageEngine, root_page_id: PageId) -> Self {
        Self {
            storage: Arc::new(Mutex::new(storage)),
            root_page_id,
        }
    }

    pub fn from_shared_storage(storage: Arc<Mutex<StorageEngine>>, root_page_id: PageId) -> Self {
        Self {
            storage,
            root_page_id,
        }
    }

    pub fn new_with_init(storage: StorageEngine) -> Result<Self> {
        // Allocate a page for root
        let storage_arc = Arc::new(Mutex::new(storage));
        let root_page_id = storage_arc.lock().unwrap().allocate_page()?;

        // Initialize as empty leaf node - create the node first, then serialize it
        let root_node = BTreeNode::new_leaf(root_page_id);

        // Create a fresh page and write the node to it
        let mut root_page = Page::new(root_page_id);
        root_node.to_page(&mut root_page)?;
        storage_arc.lock().unwrap().write_page(root_page)?;

        Ok(Self {
            storage: storage_arc,
            root_page_id,
        })
    }

    pub fn search(&mut self, key: &BTreeKey) -> Result<Option<BTreeValue>> {
        let mut current_page_id = self.root_page_id;

        loop {
            let page = self.storage.lock().unwrap().read_page(current_page_id)?;
            let node = BTreeNode::from_page(page)?;

            match node.search(key) {
                SearchResult::Found(value) => return Ok(Some(value)),
                SearchResult::NotFound(child_page_id) => match node.node_type {
                    NodeType::Leaf => return Ok(None),
                    NodeType::Internal => current_page_id = child_page_id,
                },
            }
        }
    }

    pub fn search_typed<C: KeyValueCodec>(&mut self, key: &C::Key) -> Result<Option<C::Value>> {
        let encoded_key = C::encode_key(key)?;
        let raw = self.search(&BTreeKey::new(encoded_key))?;
        match raw {
            Some(value) => Ok(Some(C::decode_value(value.as_bytes())?)),
            None => Ok(None),
        }
    }

    pub fn insert(&mut self, key: BTreeKey, value: BTreeValue) -> Result<()> {
        // Validate key and value sizes at the top level
        BTreeNode::validate_key_size(&key)?;
        BTreeNode::validate_value_size(&value)?;

        // TODO: Implement B-tree insertion with splitting
        let result = self.insert_recursive(self.root_page_id, key, value)?;

        if let Some((new_key, new_page_id)) = result {
            // Root split needed - create new root
            self.create_new_root(new_key, new_page_id)?;
        }

        Ok(())
    }

    pub fn insert_typed<C: KeyValueCodec>(&mut self, key: &C::Key, value: &C::Value) -> Result<()> {
        let encoded_key = C::encode_key(key)?;
        let encoded_value = C::encode_value(value)?;
        self.insert(BTreeKey::new(encoded_key), BTreeValue::new(encoded_value))
    }

    fn insert_recursive(
        &mut self,
        page_id: PageId,
        key: BTreeKey,
        value: BTreeValue,
    ) -> Result<Option<(BTreeKey, PageId)>> {
        let mut page = self.storage.lock().unwrap().read_page(page_id)?;
        let mut node = BTreeNode::from_page(page.clone())?;

        match node.node_type {
            NodeType::Leaf => {
                if let Some(existing_index) = node.keys.iter().position(|k| k == &key) {
                    node.values[existing_index] = value;
                    if !node.will_fit_in_page() {
                        return Err(HematiteError::StorageError(
                            "Updated value exceeds page size limit".to_string(),
                        ));
                    }
                    node.to_page(&mut page)?;
                    self.storage.lock().unwrap().write_page(page)?;
                    return Ok(None);
                }

                if node.keys.len() < node::MAX_KEYS && node.can_insert_key_value(&key, &value) {
                    node.insert_leaf(key, value)?;
                    node.to_page(&mut page)?;
                    self.storage.lock().unwrap().write_page(page)?;
                    Ok(None)
                } else {
                    // Leaf split needed
                    let (new_key, new_page_id) =
                        node.split_leaf(&mut self.storage.lock().unwrap(), key, value)?;
                    Ok(Some((new_key, new_page_id)))
                }
            }
            NodeType::Internal => {
                let child_page_id = node.find_child(&key);
                let split_result = self.insert_recursive(child_page_id, key, value)?;

                if let Some((split_key, split_page_id)) = split_result {
                    if node.keys.len() < node::MAX_KEYS && node.can_insert_key_child(&split_key) {
                        node.insert_internal(split_key, split_page_id)?;
                        node.to_page(&mut page)?;
                        self.storage.lock().unwrap().write_page(page)?;
                        Ok(None)
                    } else {
                        // Internal split needed
                        let (new_key, new_page_id) = node.split_internal(
                            &mut self.storage.lock().unwrap(),
                            split_key,
                            split_page_id,
                        )?;
                        Ok(Some((new_key, new_page_id)))
                    }
                } else {
                    node.to_page(&mut page)?;
                    self.storage.lock().unwrap().write_page(page)?;
                    Ok(None)
                }
            }
        }
    }

    fn create_new_root(&mut self, key: BTreeKey, right_page_id: PageId) -> Result<()> {
        let new_root_page_id = self.storage.lock().unwrap().allocate_page()?;
        let mut new_root_page = Page::new(new_root_page_id);

        let mut new_root = BTreeNode::new_internal(new_root_page_id);
        new_root.keys.push(key);
        new_root.children.push(self.root_page_id);
        new_root.children.push(right_page_id);

        BTreeNode::to_page(&new_root, &mut new_root_page)?;
        self.storage.lock().unwrap().write_page(new_root_page)?;

        self.root_page_id = new_root_page_id;
        Ok(())
    }

    pub fn delete(&mut self, key: &BTreeKey) -> Result<Option<BTreeValue>> {
        let result = self.delete_recursive(self.root_page_id, key)?;
        if let Some(new_root) = self.check_root_underflow()? {
            self.root_page_id = new_root;
        }
        Ok(result)
    }

    pub fn delete_typed<C: KeyValueCodec>(&mut self, key: &C::Key) -> Result<Option<C::Value>> {
        let encoded_key = C::encode_key(key)?;
        let raw = self.delete(&BTreeKey::new(encoded_key))?;
        match raw {
            Some(value) => Ok(Some(C::decode_value(value.as_bytes())?)),
            None => Ok(None),
        }
    }

    pub fn cursor(&self) -> Result<BTreeCursor> {
        // Create a new cursor with the shared storage
        // Note: This is a simplified implementation
        // In practice, we'd need to clone the storage or use a different approach
        BTreeCursor::new(self.storage.clone(), self.root_page_id)
    }

    fn delete_recursive(&mut self, page_id: PageId, key: &BTreeKey) -> Result<Option<BTreeValue>> {
        let mut page = self.storage.lock().unwrap().read_page(page_id)?;
        let mut node = BTreeNode::from_page(page.clone())?;

        let result = match node.node_type {
            NodeType::Leaf => {
                let value = node.delete_from_leaf(key)?;
                node.to_page(&mut page)?;
                self.storage.lock().unwrap().write_page(page)?;
                value
            }
            NodeType::Internal => {
                let child_index = node.find_child_index(key);
                let child_page_id = node.children[child_index];

                // Recursively delete from child
                let deleted_value = self.delete_recursive(child_page_id, key)?;

                // Check if child is underflow and handle rebalancing
                if self.is_child_underflow(child_page_id)? {
                    self.rebalance_node(&mut node, child_index)?;
                }

                node.to_page(&mut page)?;
                self.storage.lock().unwrap().write_page(page)?;
                deleted_value
            }
        };

        Ok(result)
    }

    fn check_root_underflow(&mut self) -> Result<Option<PageId>> {
        let page = self.storage.lock().unwrap().read_page(self.root_page_id)?;
        let node = BTreeNode::from_page(page)?;

        if node.keys.is_empty() && !node.children.is_empty() {
            // Root is empty but has children, make first child the new root
            let new_root_page_id = node.children[0];

            // Deallocate the old root page
            self.storage
                .lock()
                .unwrap()
                .deallocate_page(self.root_page_id)?;

            Ok(Some(new_root_page_id))
        } else {
            Ok(None)
        }
    }

    fn is_child_underflow(&mut self, child_page_id: PageId) -> Result<bool> {
        let page = self.storage.lock().unwrap().read_page(child_page_id)?;
        let node = BTreeNode::from_page(page)?;
        Ok(node.is_underflow())
    }

    fn rebalance_node(&mut self, parent: &mut BTreeNode, child_index: usize) -> Result<()> {
        // Try to borrow from left sibling
        if child_index > 0 {
            let left_sibling_id = parent.children[child_index - 1];
            if self.try_borrow_from_left_sibling(parent, child_index, left_sibling_id)? {
                return Ok(());
            }
        }

        // Try to borrow from right sibling
        if child_index < parent.children.len() - 1 {
            let right_sibling_id = parent.children[child_index + 1];
            if self.try_borrow_from_right_sibling(parent, child_index, right_sibling_id)? {
                return Ok(());
            }
        }

        // If borrowing failed, try to merge
        if child_index > 0 {
            self.merge_with_left_sibling(parent, child_index)?;
        } else {
            self.merge_with_right_sibling(parent, child_index)?;
        }

        Ok(())
    }

    fn try_borrow_from_left_sibling(
        &mut self,
        parent: &mut BTreeNode,
        child_index: usize,
        left_sibling_id: PageId,
    ) -> Result<bool> {
        let mut left_page = self.storage.lock().unwrap().read_page(left_sibling_id)?;
        let mut left_sibling = BTreeNode::from_page(left_page.clone())?;

        let child_id = parent.children[child_index];
        let mut child_page = self.storage.lock().unwrap().read_page(child_id)?;
        let mut child_node = BTreeNode::from_page(child_page.clone())?;

        if left_sibling.node_type != child_node.node_type {
            return Err(HematiteError::StorageError(
                "Sibling node type mismatch during left borrow".to_string(),
            ));
        }

        let min_keys = Self::min_keys_for(left_sibling.node_type);
        if left_sibling.keys.len() <= min_keys {
            return Ok(false);
        }

        match child_node.node_type {
            NodeType::Leaf => {
                let key = left_sibling.keys.pop().ok_or_else(|| {
                    HematiteError::StorageError("Left leaf sibling missing key".to_string())
                })?;
                let value = left_sibling.values.pop().ok_or_else(|| {
                    HematiteError::StorageError("Left leaf sibling missing value".to_string())
                })?;
                child_node.keys.insert(0, key);
                child_node.values.insert(0, value);
                parent.keys[child_index - 1] = child_node.keys[0].clone();
            }
            NodeType::Internal => {
                let rotate_up_key = left_sibling.keys.pop().ok_or_else(|| {
                    HematiteError::StorageError("Left internal sibling missing key".to_string())
                })?;
                let rotate_child = left_sibling.children.pop().ok_or_else(|| {
                    HematiteError::StorageError("Left internal sibling missing child".to_string())
                })?;
                let parent_separator = parent.keys[child_index - 1].clone();

                child_node.keys.insert(0, parent_separator);
                child_node.children.insert(0, rotate_child);
                parent.keys[child_index - 1] = rotate_up_key;
            }
        }

        // Write back changes
        left_sibling.to_page(&mut left_page)?;
        child_node.to_page(&mut child_page)?;
        self.storage.lock().unwrap().write_page(left_page)?;
        self.storage.lock().unwrap().write_page(child_page)?;

        Ok(true)
    }

    fn try_borrow_from_right_sibling(
        &mut self,
        parent: &mut BTreeNode,
        child_index: usize,
        right_sibling_id: PageId,
    ) -> Result<bool> {
        let mut right_page = self.storage.lock().unwrap().read_page(right_sibling_id)?;
        let mut right_sibling = BTreeNode::from_page(right_page.clone())?;

        let child_id = parent.children[child_index];
        let mut child_page = self.storage.lock().unwrap().read_page(child_id)?;
        let mut child_node = BTreeNode::from_page(child_page.clone())?;

        if right_sibling.node_type != child_node.node_type {
            return Err(HematiteError::StorageError(
                "Sibling node type mismatch during right borrow".to_string(),
            ));
        }

        let min_keys = Self::min_keys_for(right_sibling.node_type);
        if right_sibling.keys.len() <= min_keys {
            return Ok(false);
        }

        match child_node.node_type {
            NodeType::Leaf => {
                let key = right_sibling.keys.remove(0);
                let value = right_sibling.values.remove(0);
                child_node.keys.push(key);
                child_node.values.push(value);

                let new_separator = right_sibling.keys.first().ok_or_else(|| {
                    HematiteError::StorageError(
                        "Right leaf sibling became empty after borrow".to_string(),
                    )
                })?;
                parent.keys[child_index] = new_separator.clone();
            }
            NodeType::Internal => {
                let parent_separator = parent.keys[child_index].clone();
                let rotate_child = right_sibling.children.remove(0);
                let rotate_up_key = right_sibling.keys.remove(0);

                child_node.keys.push(parent_separator);
                child_node.children.push(rotate_child);
                parent.keys[child_index] = rotate_up_key;
            }
        }

        // Write back changes
        right_sibling.to_page(&mut right_page)?;
        child_node.to_page(&mut child_page)?;
        self.storage.lock().unwrap().write_page(right_page)?;
        self.storage.lock().unwrap().write_page(child_page)?;

        Ok(true)
    }

    fn merge_with_left_sibling(
        &mut self,
        parent: &mut BTreeNode,
        child_index: usize,
    ) -> Result<()> {
        let left_sibling_id = parent.children[child_index - 1];
        let child_id = parent.children[child_index];

        let mut left_page = self.storage.lock().unwrap().read_page(left_sibling_id)?;
        let mut left_sibling = BTreeNode::from_page(left_page.clone())?;

        let child_page = self.storage.lock().unwrap().read_page(child_id)?;
        let mut child_node = BTreeNode::from_page(child_page.clone())?;

        let separator_key = parent.keys.remove(child_index - 1);
        parent.children.remove(child_index);

        match (left_sibling.node_type, child_node.node_type) {
            (NodeType::Leaf, NodeType::Leaf) => {
                left_sibling.merge_leaf(&mut child_node, &mut self.storage.lock().unwrap())?;
            }
            (NodeType::Internal, NodeType::Internal) => {
                left_sibling.merge_internal(
                    &mut child_node,
                    separator_key,
                    &mut self.storage.lock().unwrap(),
                )?;
            }
            _ => {
                return Err(HematiteError::StorageError(
                    "Cannot merge different node types".to_string(),
                ));
            }
        }

        // Write back changes
        left_sibling.to_page(&mut left_page)?;
        self.storage.lock().unwrap().write_page(left_page)?;
        // Note: child_page becomes unused and could be deallocated

        Ok(())
    }

    fn merge_with_right_sibling(
        &mut self,
        parent: &mut BTreeNode,
        child_index: usize,
    ) -> Result<()> {
        let child_id = parent.children[child_index];
        let right_sibling_id = parent.children[child_index + 1];

        let mut child_page = self.storage.lock().unwrap().read_page(child_id)?;
        let mut child_node = BTreeNode::from_page(child_page.clone())?;

        let right_page = self.storage.lock().unwrap().read_page(right_sibling_id)?;
        let mut right_sibling = BTreeNode::from_page(right_page.clone())?;

        let separator_key = parent.keys.remove(child_index);
        parent.children.remove(child_index + 1);

        match (child_node.node_type, right_sibling.node_type) {
            (NodeType::Leaf, NodeType::Leaf) => {
                child_node.merge_leaf(&mut right_sibling, &mut self.storage.lock().unwrap())?;
            }
            (NodeType::Internal, NodeType::Internal) => {
                child_node.merge_internal(
                    &mut right_sibling,
                    separator_key,
                    &mut self.storage.lock().unwrap(),
                )?;
            }
            _ => {
                return Err(HematiteError::StorageError(
                    "Cannot merge different node types".to_string(),
                ));
            }
        }

        // Write back changes
        child_node.to_page(&mut child_page)?;
        self.storage.lock().unwrap().write_page(child_page)?;
        // Note: right_page becomes unused and could be deallocated

        Ok(())
    }
}
