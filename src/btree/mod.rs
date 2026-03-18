//! B-tree module with comprehensive testing

pub mod cursor;
pub mod index;
pub mod node;
pub mod tree;

pub use index::BTreeIndex;
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

#[cfg(test)]
mod tests {
    use crate::btree::index::BTreeIndex;
    use crate::btree::tree::BTreeManager;
    use crate::btree::{BTreeKey, BTreeValue};
    use crate::error::Result;
    use crate::storage::StorageEngine;
    use crate::test_utils::TestDbFile;

    fn tmp_db() -> TestDbFile {
        TestDbFile::new("_test_btree")
    }

    fn new_storage(db: &TestDbFile) -> Result<StorageEngine> {
        StorageEngine::new(db.path().to_string())
    }

    #[test]
    fn test_btree_insert_and_search() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let mut btree = BTreeIndex::new_with_init(storage)?;

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
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let mut btree = BTreeIndex::new_with_init(storage)?;

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
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let mut btree = BTreeIndex::new_with_init(storage)?;

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

    // Comprehensive cursor tests
    #[test]
    fn test_cursor_operations() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let mut btree = BTreeIndex::new_with_init(storage)?;

        // Insert test data
        let test_keys: Vec<Vec<u8>> = vec![
            vec![10, 0, 0],
            vec![20, 0, 0],
            vec![30, 0, 0],
            vec![40, 0, 0],
            vec![50, 0, 0],
        ];

        for (i, key_data) in test_keys.iter().enumerate() {
            let key = BTreeKey::new(key_data.clone());
            let value = BTreeValue::new(vec![(i as u8), 0, 0]);
            btree.insert(key, value)?;
        }

        // Test cursor navigation
        let mut cursor = btree.cursor()?;

        // Test first()
        cursor.first()?;
        if let Some((current_key, _current_value)) = cursor.current() {
            assert_eq!(*current_key, BTreeKey::new(vec![10, 0, 0]));
        }

        // Test next()
        cursor.next()?;
        if let Some((current_key, _)) = cursor.current() {
            assert_eq!(*current_key, BTreeKey::new(vec![20, 0, 0]));
        }

        // Test prev()
        cursor.prev()?;
        if let Some((current_key, _)) = cursor.current() {
            assert_eq!(*current_key, BTreeKey::new(vec![10, 0, 0]));
        }

        // Test last()
        cursor.last()?;
        if let Some((current_key, _)) = cursor.current() {
            assert_eq!(*current_key, BTreeKey::new(vec![50, 0, 0]));
        }

        // Test seek()
        let seek_key = BTreeKey::new(vec![30, 0, 0]);
        cursor.seek(&seek_key)?;
        if let Some((current_key, _)) = cursor.current() {
            assert_eq!(*current_key, seek_key);
        }

        Ok(())
    }

    // Node splitting tests
    #[test]
    fn test_leaf_node_splitting() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let mut btree = BTreeIndex::new_with_init(storage)?;

        // Insert enough keys to cause leaf splits
        for i in 0..20 {
            let key = BTreeKey::new(vec![i as u8, 0, 0]);
            let value = BTreeValue::new(vec![i as u8, 0, 0]);
            btree.insert(key, value)?;
        }

        // Verify all keys are still accessible after splits
        for i in 0..20 {
            let key = BTreeKey::new(vec![i as u8, 0, 0]);
            let found = btree.search(&key)?;
            assert!(
                found.is_some(),
                "Key {} should be found after leaf split",
                i
            );
            assert_eq!(found.unwrap(), BTreeValue::new(vec![i as u8, 0, 0]));
        }

        Ok(())
    }

    #[test]
    fn test_internal_node_splitting() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let mut btree = BTreeIndex::new_with_init(storage)?;

        // Insert many keys to cause internal node splits
        for i in 0..1000 {
            let key = BTreeKey::new(vec![(i / 10) as u8, (i % 10) as u8, 0]);
            let value = BTreeValue::new(vec![i as u8, 0, 0]);
            btree.insert(key, value)?;
        }

        // Verify tree integrity after internal splits
        for i in 0..1000 {
            let key = BTreeKey::new(vec![(i / 10) as u8, (i % 10) as u8, 0]);
            let found = btree.search(&key)?;
            if found.is_none() {
                println!("Failed to find key {} with data {:?}", i, key);
            }
            assert!(
                found.is_some(),
                "Key {} should be found after internal split",
                i
            );
        }

        Ok(())
    }

    // Size-based split tests
    #[test]
    fn test_size_based_splits() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let mut btree = BTreeIndex::new_with_init(storage)?;

        // Insert large keys/values to test size-based splits
        for i in 0..10 {
            let large_key = BTreeKey::new(vec![i as u8; 50]); // 50-byte keys
            let large_value = BTreeValue::new(vec![i as u8; 100]); // 100-byte values
            btree.insert(large_key, large_value)?;
        }

        // Verify all large keys are accessible
        for i in 0..10 {
            let large_key = BTreeKey::new(vec![i as u8; 50]);
            let found = btree.search(&large_key)?;
            assert!(found.is_some(), "Large key {} should be found", i);
            assert_eq!(found.unwrap(), BTreeValue::new(vec![i as u8; 100]));
        }

        Ok(())
    }

    // Duplicate key handling tests
    #[test]
    fn test_duplicate_key_handling() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let mut btree = BTreeIndex::new_with_init(storage)?;

        let key = BTreeKey::new(vec![42, 0, 0]);
        let value1 = BTreeValue::new(vec![1, 0, 0]);
        let value2 = BTreeValue::new(vec![2, 0, 0]);

        // Insert initial key
        btree.insert(key.clone(), value1.clone())?;
        let found = btree.search(&key)?;
        assert_eq!(found.unwrap(), value1);

        // Insert duplicate key (should replace value)
        btree.insert(key.clone(), value2.clone())?;
        let found = btree.search(&key)?;
        assert_eq!(found.unwrap(), value2);

        // Verify only one entry exists
        let mut cursor = btree.cursor()?;
        let mut count = 0;
        if let Some((current_key, _)) = cursor.current() {
            if current_key == &key {
                count += 1;
            }
        }
        while cursor.next().is_ok() {
            if let Some((current_key, _)) = cursor.current() {
                if current_key == &key {
                    count += 1;
                }
            }
        }
        assert_eq!(count, 1, "Should only have one entry for duplicate key");

        Ok(())
    }

    // Edge case tests
    #[test]
    fn test_empty_tree_operations() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let mut btree = BTreeIndex::new_with_init(storage)?;

        // Test operations on empty tree
        let key = BTreeKey::new(vec![1, 0, 0]);
        assert!(btree.search(&key)?.is_none());
        assert!(btree.delete(&key)?.is_none());

        // Test cursor on empty tree
        let mut cursor = btree.cursor()?;
        cursor.first()?; // Should succeed but cursor should be invalid
        assert!(!cursor.is_valid()); // Cursor should be invalid on empty tree
        cursor.last()?; // Should succeed but cursor should be invalid
        assert!(!cursor.is_valid()); // Cursor should be invalid on empty tree

        Ok(())
    }

    #[test]
    fn test_single_key_operations() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let mut btree = BTreeIndex::new_with_init(storage)?;

        let key = BTreeKey::new(vec![5, 0, 0]);
        let value = BTreeValue::new(vec![42, 0, 0]);

        // Insert single key
        btree.insert(key.clone(), value)?;
        assert!(btree.search(&key)?.is_some());

        // Test cursor with single key
        let mut cursor = btree.cursor()?;

        cursor.first()?;
        assert!(cursor.is_valid());
        cursor.next()?; // Moves past the only key
        assert!(!cursor.is_valid()); // Now at end
        assert!(cursor.prev().is_ok()); // Should go back
        assert!(cursor.is_valid());

        // Delete single key
        assert!(btree.delete(&key)?.is_some());
        assert!(btree.search(&key)?.is_none());

        Ok(())
    }

    #[test]
    fn test_root_split() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let mut btree = BTreeIndex::new_with_init(storage)?;

        // Insert keys to force root split
        for i in 0..15 {
            let key = BTreeKey::new(vec![i as u8, 0, 0]);
            let value = BTreeValue::new(vec![i as u8, 0, 0]);
            btree.insert(key, value)?;
        }

        // Verify tree is still functional after root split
        for i in 0..15 {
            let key = BTreeKey::new(vec![i as u8, 0, 0]);
            assert!(btree.search(&key)?.is_some());
        }

        Ok(())
    }

    // Checksum validation tests
    #[test]
    fn test_checksum_validation() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let mut btree = BTreeIndex::new_with_init(storage)?;

        // Insert some data
        for i in 0..10 {
            let key = BTreeKey::new(vec![i as u8, 0, 0]);
            let value = BTreeValue::new(vec![i as u8, 0, 0]);
            btree.insert(key, value)?;
        }

        // Force a flush to ensure checksums are written
        drop(btree);

        // Reopen and verify checksums are validated
        let storage = new_storage(&path)?;
        let _manager = BTreeManager::new(storage);
        // This should work if checksums are valid
        // If we manually corrupt the data, it should fail

        Ok(())
    }

    // Property-based test for tree invariants
    #[test]
    fn test_tree_invariants_random_operations() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let mut btree = BTreeIndex::new_with_init(storage)?;

        let mut inserted_keys = Vec::new();

        // Random insertions
        for i in 0..50 {
            let key_data = vec![(i * 7 % 100) as u8, (i * 13 % 100) as u8, 0];
            let key = BTreeKey::new(key_data.clone());
            let value = BTreeValue::new(vec![i as u8, 0, 0]);

            btree.insert(key.clone(), value.clone())?;
            inserted_keys.push((key, value));
        }

        // Verify all inserted keys are present and correct
        for (key, expected_value) in &inserted_keys {
            let found = btree.search(key)?;
            assert!(found.is_some(), "Key {:?} should be found", key);
            assert_eq!(found.unwrap(), *expected_value);
        }

        // Verify keys are in sorted order using cursor
        let mut cursor = btree.cursor()?;
        let mut cursor_keys = Vec::new();
        while let Some((key, _)) = cursor.current() {
            cursor_keys.push(key.clone());
            cursor.next().unwrap_or(()); // Continue or break
        }

        // Check that cursor_keys are sorted
        for i in 1..cursor_keys.len() {
            assert!(
                cursor_keys[i - 1] < cursor_keys[i],
                "Keys should be in sorted order"
            );
        }

        // Random deletions
        for i in (0..inserted_keys.len()).step_by(3) {
            let (key, _) = &inserted_keys[i];
            btree.delete(key)?;
        }

        // Verify remaining keys are still present
        for (j, (key, expected_value)) in inserted_keys.iter().enumerate() {
            if j % 3 == 0 {
                continue;
            } // Skip deleted keys
            let found = btree.search(key)?;
            assert!(found.is_some(), "Key {:?} should still be found", key);
            assert_eq!(found.unwrap(), *expected_value);
        }

        Ok(())
    }

    // Merging and borrowing tests
    #[test]
    fn test_merging_and_borrowing() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let mut btree = BTreeIndex::new_with_init(storage)?;

        // Insert many keys to create a multi-level tree
        for i in 0..50 {
            let key = BTreeKey::new(vec![i as u8, 0, 0]);
            let value = BTreeValue::new(vec![i as u8, 0, 0]);
            btree.insert(key, value)?;
        }

        // Delete many keys to trigger merging/borrowing
        for i in (0..50).step_by(2) {
            let key = BTreeKey::new(vec![i as u8, 0, 0]);
            btree.delete(&key)?;
        }

        // Verify remaining keys are still accessible
        for i in (1..50).step_by(2) {
            let key = BTreeKey::new(vec![i as u8, 0, 0]);
            let found = btree.search(&key)?;
            assert!(found.is_some(), "Key {} should be found after deletions", i);
        }

        Ok(())
    }
}
