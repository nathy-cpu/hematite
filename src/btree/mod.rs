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

#[derive(Debug, Clone, PartialEq)]
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
        let mut page = self.storage.read_page(page_id)?;
        let mut node = node::BTreeNode::from_page(page.clone())?;

        match node.node_type {
            NodeType::Leaf => {
                if node.keys.len() < node::MAX_KEYS {
                    node.insert_leaf(key, value)?;
                    node.to_page(&mut page)?;
                    self.storage.write_page(page)?;
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

    pub fn delete(&mut self, key: &BTreeKey) -> Result<Option<BTreeValue>> {
        let result = self.delete_recursive(self.root_page_id, key)?;

        // If the root became empty and has children, make the first child the new root
        if let Some(root_page_id) = self.check_root_underflow()? {
            self.root_page_id = root_page_id;
        }

        Ok(result)
    }

    fn delete_recursive(&mut self, page_id: PageId, key: &BTreeKey) -> Result<Option<BTreeValue>> {
        let mut page = self.storage.read_page(page_id)?;
        let mut node = node::BTreeNode::from_page(page.clone())?;

        let result = match node.node_type {
            NodeType::Leaf => {
                let value = node.delete_from_leaf(key)?;
                node.to_page(&mut page)?;
                self.storage.write_page(page)?;
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
                self.storage.write_page(page)?;
                deleted_value
            }
        };

        Ok(result)
    }

    fn check_root_underflow(&mut self) -> Result<Option<PageId>> {
        let page = self.storage.read_page(self.root_page_id)?;
        let node = node::BTreeNode::from_page(page)?;

        if node.keys.is_empty() && !node.children.is_empty() {
            // Root is empty but has children, make first child the new root
            Ok(Some(node.children[0]))
        } else {
            Ok(None)
        }
    }

    fn is_child_underflow(&mut self, child_page_id: PageId) -> Result<bool> {
        let page = self.storage.read_page(child_page_id)?;
        let node = node::BTreeNode::from_page(page)?;
        Ok(node.is_underflow())
    }

    fn rebalance_node(&mut self, parent: &mut node::BTreeNode, child_index: usize) -> Result<()> {
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
        parent: &mut node::BTreeNode,
        child_index: usize,
        left_sibling_id: PageId,
    ) -> Result<bool> {
        let mut left_page = self.storage.read_page(left_sibling_id)?;
        let mut left_sibling = node::BTreeNode::from_page(left_page.clone())?;

        let child_id = parent.children[child_index];
        let mut child_page = self.storage.read_page(child_id)?;
        let mut child_node = node::BTreeNode::from_page(child_page.clone())?;

        if let Some(borrowed_key) = child_node.borrow_from_sibling(&mut left_sibling, true)? {
            // Update parent separator key
            parent.keys[child_index - 1] = borrowed_key;

            // Write back changes
            left_sibling.to_page(&mut left_page)?;
            child_node.to_page(&mut child_page)?;
            self.storage.write_page(left_page)?;
            self.storage.write_page(child_page)?;

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn try_borrow_from_right_sibling(
        &mut self,
        parent: &mut node::BTreeNode,
        child_index: usize,
        right_sibling_id: PageId,
    ) -> Result<bool> {
        let mut right_page = self.storage.read_page(right_sibling_id)?;
        let mut right_sibling = node::BTreeNode::from_page(right_page.clone())?;

        let child_id = parent.children[child_index];
        let mut child_page = self.storage.read_page(child_id)?;
        let mut child_node = node::BTreeNode::from_page(child_page.clone())?;

        if let Some(borrowed_key) = child_node.borrow_from_sibling(&mut right_sibling, false)? {
            // Update parent separator key
            parent.keys[child_index] = borrowed_key;

            // Write back changes
            right_sibling.to_page(&mut right_page)?;
            child_node.to_page(&mut child_page)?;
            self.storage.write_page(right_page)?;
            self.storage.write_page(child_page)?;

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn merge_with_left_sibling(
        &mut self,
        parent: &mut node::BTreeNode,
        child_index: usize,
    ) -> Result<()> {
        let left_sibling_id = parent.children[child_index - 1];
        let child_id = parent.children[child_index];

        let mut left_page = self.storage.read_page(left_sibling_id)?;
        let mut left_sibling = node::BTreeNode::from_page(left_page.clone())?;

        let mut child_page = self.storage.read_page(child_id)?;
        let mut child_node = node::BTreeNode::from_page(child_page.clone())?;

        let separator_key = parent.keys.remove(child_index - 1);
        parent.children.remove(child_index);

        match (left_sibling.node_type, child_node.node_type) {
            (NodeType::Leaf, NodeType::Leaf) => {
                left_sibling.merge_leaf(&mut child_node)?;
            }
            (NodeType::Internal, NodeType::Internal) => {
                left_sibling.merge_internal(&mut child_node, separator_key)?;
            }
            _ => {
                return Err(HematiteError::StorageError(
                    "Cannot merge different node types".to_string(),
                ))
            }
        }

        // Write back changes
        left_sibling.to_page(&mut left_page)?;
        self.storage.write_page(left_page)?;
        // Note: child_page becomes unused and could be deallocated

        Ok(())
    }

    fn merge_with_right_sibling(
        &mut self,
        parent: &mut node::BTreeNode,
        child_index: usize,
    ) -> Result<()> {
        let child_id = parent.children[child_index];
        let right_sibling_id = parent.children[child_index + 1];

        let mut child_page = self.storage.read_page(child_id)?;
        let mut child_node = node::BTreeNode::from_page(child_page.clone())?;

        let mut right_page = self.storage.read_page(right_sibling_id)?;
        let mut right_sibling = node::BTreeNode::from_page(right_page.clone())?;

        let separator_key = parent.keys.remove(child_index);
        parent.children.remove(child_index + 1);

        match (child_node.node_type, right_sibling.node_type) {
            (NodeType::Leaf, NodeType::Leaf) => {
                child_node.merge_leaf(&mut right_sibling)?;
            }
            (NodeType::Internal, NodeType::Internal) => {
                child_node.merge_internal(&mut right_sibling, separator_key)?;
            }
            _ => {
                return Err(HematiteError::StorageError(
                    "Cannot merge different node types".to_string(),
                ))
            }
        }

        // Write back changes
        child_node.to_page(&mut child_page)?;
        self.storage.write_page(child_page)?;
        // Note: right_page becomes unused and could be deallocated

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_simple_debug() -> Result<()> {
        println!("=== Starting B-tree debug test ===");

        let mut storage = StorageEngine::new(":memory:".to_string())?;
        println!("✓ Storage engine created");

        let mut btree = BTreeIndex::new(storage, PageId::new(1));
        println!(
            "✓ B-tree created with root page ID: {:?}",
            btree.root_page_id
        );

        // Try to read the root page directly
        let root_page = btree.storage.read_page(btree.root_page_id)?;
        println!(
            "✓ Root page read successfully, data len: {}",
            root_page.data.len()
        );

        // Check if root page is initialized
        let node_type_byte = root_page.data[0];
        println!(
            "✓ Root node type byte: {} (0=Internal, 1=Leaf)",
            node_type_byte
        );

        // Insert a simple key-value pair
        let key = BTreeKey::new(vec![1]);
        let value = BTreeValue::new(vec![42]);
        println!("✓ Inserting key: {:?}, value: {:?}", key, value);

        btree.insert(key.clone(), value.clone())?;
        println!("✓ Insert completed");

        // Read the root page again to see what changed
        let root_page_after = btree.storage.read_page(btree.root_page_id)?;
        println!(
            "✓ Root page after insert, node type byte: {}",
            root_page_after.data[0]
        );
        let key_count_after = u32::from_le_bytes([
            root_page_after.data[1],
            root_page_after.data[2],
            root_page_after.data[3],
            root_page_after.data[4],
        ]);
        println!("✓ Root page key count after insert: {}", key_count_after);

        // Try to search for the key
        println!("✓ Searching for key: {:?}", key);
        let found = btree.search(&key)?;
        println!("✓ Search result: {:?}", found);

        if let Some(found_value) = found {
            println!("✓ SUCCESS: Found value: {:?}", found_value);
            assert_eq!(found_value, value);
        } else {
            println!("✗ FAILURE: Key not found!");
            panic!("Key not found after insertion");
        }

        println!("=== B-tree debug test completed ===");
        Ok(())
    }

    #[test]
    fn test_btree_delete_debug() -> Result<()> {
        println!("=== Starting B-tree delete debug test ===");

        let mut storage = StorageEngine::new(":memory:".to_string())?;
        let mut btree = BTreeIndex::new(storage, PageId::new(1));

        // Use a unique key to avoid conflicts with other tests
        let key = BTreeKey::new(vec![255, 255, 255]); // Unique key
        let value = BTreeValue::new(vec![42]);
        println!("✓ Inserting unique key: {:?}", key);
        btree.insert(key.clone(), value.clone())?;

        // Verify it exists
        let found_before = btree.search(&key)?;
        println!("✓ Key exists before delete: {:?}", found_before);

        // Check root page key count before delete
        let root_page_before = btree.storage.read_page(btree.root_page_id)?;
        let key_count_before = u32::from_le_bytes([
            root_page_before.data[1],
            root_page_before.data[2],
            root_page_before.data[3],
            root_page_before.data[4],
        ]);
        println!("✓ Root page key count before delete: {}", key_count_before);

        // Delete the key
        println!("✓ Deleting key: {:?}", key);
        let deleted_value = btree.delete(&key)?;
        println!("✓ Delete result: {:?}", deleted_value);

        // Check root page key count after delete
        let root_page_after = btree.storage.read_page(btree.root_page_id)?;
        let key_count_after = u32::from_le_bytes([
            root_page_after.data[1],
            root_page_after.data[2],
            root_page_after.data[3],
            root_page_after.data[4],
        ]);
        println!("✓ Root page key count after delete: {}", key_count_after);

        // Verify it's gone
        let found_after = btree.search(&key)?;
        println!("✓ Key exists after delete: {:?}", found_after);

        if found_after.is_none() {
            println!("✓ SUCCESS: Key was deleted");
        } else {
            println!("✗ FAILURE: Key still exists after delete");
            panic!("Key still exists after deletion");
        }

        println!("=== B-tree delete debug test completed ===");
        Ok(())
    }

    #[test]
    fn test_btree_insert_and_search() -> Result<()> {
        let mut storage = StorageEngine::new(":memory:".to_string())?;
        let mut btree = BTreeIndex::new(storage, PageId::new(1));

        // Insert some key-value pairs with unique keys
        let key1 = BTreeKey::new(vec![101, 0, 0]);
        let value1 = BTreeValue::new(vec![10, 20, 30]);
        btree.insert(key1.clone(), value1.clone())?;

        let key2 = BTreeKey::new(vec![102, 0, 0]);
        let value2 = BTreeValue::new(vec![40, 50, 60]);
        btree.insert(key2.clone(), value2.clone())?;

        let key3 = BTreeKey::new(vec![103, 0, 0]);
        let value3 = BTreeValue::new(vec![70, 80, 90]);
        btree.insert(key3.clone(), value3.clone())?;

        // Search for inserted keys
        let found1 = btree.search(&key1)?;
        assert!(found1.is_some());
        assert_eq!(found1.unwrap(), value1);

        let found2 = btree.search(&key2)?;
        assert!(found2.is_some());
        assert_eq!(found2.unwrap(), value2);

        let found3 = btree.search(&key3)?;
        assert!(found3.is_some());
        assert_eq!(found3.unwrap(), value3);

        // Search for non-existent key
        let key_missing = BTreeKey::new(vec![99, 0, 0]);
        let found_missing = btree.search(&key_missing)?;
        assert!(found_missing.is_none());

        Ok(())
    }

    #[test]
    fn test_btree_delete() -> Result<()> {
        let mut storage = StorageEngine::new(":memory:".to_string())?;
        let mut btree = BTreeIndex::new(storage, PageId::new(1));

        // Insert some key-value pairs with unique keys
        let key1 = BTreeKey::new(vec![201, 0, 0]);
        let value1 = BTreeValue::new(vec![10, 20, 30]);
        btree.insert(key1.clone(), value1.clone())?;

        let key2 = BTreeKey::new(vec![202, 0, 0]);
        let value2 = BTreeValue::new(vec![40, 50, 60]);
        btree.insert(key2.clone(), value2.clone())?;

        let key3 = BTreeKey::new(vec![203, 0, 0]);
        let value3 = BTreeValue::new(vec![70, 80, 90]);
        btree.insert(key3.clone(), value3.clone())?;

        // Verify all keys exist
        assert!(btree.search(&key1)?.is_some());
        assert!(btree.search(&key2)?.is_some());
        assert!(btree.search(&key3)?.is_some());

        // Delete middle key
        let deleted_value = btree.delete(&key2)?;
        assert!(deleted_value.is_some());
        assert_eq!(deleted_value.unwrap(), value2);

        // Verify deletion
        assert!(btree.search(&key2)?.is_none());
        assert!(btree.search(&key1)?.is_some());
        assert!(btree.search(&key3)?.is_some());

        // Delete non-existent key
        let key_missing = BTreeKey::new(vec![99, 0, 0]);
        let deleted_missing = btree.delete(&key_missing)?;
        assert!(deleted_missing.is_none());

        Ok(())
    }

    #[test]
    fn test_btree_delete_all_keys() -> Result<()> {
        let mut storage = StorageEngine::new(":memory:".to_string())?;
        let mut btree = BTreeIndex::new(storage, PageId::new(1));

        // Insert multiple keys with unique range
        for i in 51..=55 {
            let key = BTreeKey::new(vec![i, 0, 0]);
            let value = BTreeValue::new(vec![i * 2, i * 3, i * 4]);
            btree.insert(key, value)?;
        }

        // Delete all keys one by one
        for i in 51..=55 {
            let key = BTreeKey::new(vec![i, 0, 0]);
            let deleted_value = btree.delete(&key)?;
            assert!(deleted_value.is_some());
        }

        // Verify all keys are deleted
        for i in 51..=55 {
            let key = BTreeKey::new(vec![i, 0, 0]);
            let found = btree.search(&key)?;
            assert!(found.is_none());
        }

        Ok(())
    }
}
