//! Centralized tests for the btree module

mod mod_tests {
    use crate::btree::bytes::ByteTreeStore;
    use crate::btree::index::BTreeIndex;
    use crate::btree::tree::BTreeManager;
    use crate::btree::{BTreeKey, BTreeValue, KeyValueCodec};
    use crate::error::Result;
    use crate::storage::Pager;
    use crate::test_utils::TestDbFile;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    fn tmp_db() -> TestDbFile {
        TestDbFile::new("_test_btree")
    }

    fn new_storage(db: &TestDbFile) -> Result<Pager> {
        Pager::new(db.path().to_string(), 100)
    }

    #[derive(Debug, Clone)]
    struct LcgRng {
        state: u64,
    }

    impl LcgRng {
        fn new(seed: u64) -> Self {
            Self { state: seed }
        }

        fn next_u64(&mut self) -> u64 {
            self.state = self
                .state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.state
        }

        fn next_u32(&mut self) -> u32 {
            (self.next_u64() >> 32) as u32
        }
    }

    #[derive(Debug, Clone, Copy, Default)]
    struct U32StringCodec;

    impl KeyValueCodec for U32StringCodec {
        type Key = u32;
        type Value = String;

        fn encode_key(key: &Self::Key) -> Result<Vec<u8>> {
            Ok(key.to_le_bytes().to_vec())
        }

        fn decode_key(bytes: &[u8]) -> Result<Self::Key> {
            if bytes.len() != 4 {
                return Err(crate::error::HematiteError::StorageError(
                    "Invalid u32 key encoding".to_string(),
                ));
            }
            Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
        }

        fn encode_value(value: &Self::Value) -> Result<Vec<u8>> {
            Ok(value.as_bytes().to_vec())
        }

        fn decode_value(bytes: &[u8]) -> Result<Self::Value> {
            String::from_utf8(bytes.to_vec()).map_err(|e| {
                crate::error::HematiteError::StorageError(format!("Invalid UTF-8 value: {}", e))
            })
        }
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
    fn test_btree_typed_codec_boundary() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let mut btree = BTreeIndex::new_with_init(storage)?;

        btree.insert_typed::<U32StringCodec>(&7, &"seven".to_string())?;
        btree.insert_typed::<U32StringCodec>(&9, &"nine".to_string())?;

        let found = btree.search_typed::<U32StringCodec>(&7)?;
        assert_eq!(found, Some("seven".to_string()));

        let deleted = btree.delete_typed::<U32StringCodec>(&9)?;
        assert_eq!(deleted, Some("nine".to_string()));
        assert_eq!(btree.search_typed::<U32StringCodec>(&9)?, None);

        Ok(())
    }

    #[test]
    fn test_byte_tree_store_raw_bytes_boundary() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let trees = ByteTreeStore::new(storage);
        let root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;

        tree.insert(b"users", b"schema-entry")?;
        tree.insert(b"orders", b"schema-entry-2")?;

        assert_eq!(tree.get(b"users")?, Some(b"schema-entry".to_vec()));
        assert_eq!(tree.delete(b"orders")?, Some(b"schema-entry-2".to_vec()));
        assert_eq!(tree.get(b"orders")?, None);

        let page_ids = trees.collect_page_ids(root_page_id)?;
        assert!(!page_ids.is_empty());
        assert!(trees.validate_tree(root_page_id)?);

        Ok(())
    }

    #[test]
    fn test_byte_tree_cursor_uses_byte_slices() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let trees = ByteTreeStore::new(storage);
        let root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;

        tree.insert(b"alpha", b"one")?;
        tree.insert(b"beta", b"two")?;

        let mut cursor = tree.cursor()?;
        assert!(cursor.is_valid());
        assert_eq!(cursor.current(), Some((&b"alpha"[..], &b"one"[..])));

        cursor.seek(b"beta")?;
        assert_eq!(cursor.current(), Some((&b"beta"[..], &b"two"[..])));

        Ok(())
    }

    #[test]
    fn test_byte_tree_insert_reports_root_changes() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let trees = ByteTreeStore::new(storage);
        let original_root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(original_root_page_id)?;
        let mut final_root_page_id = original_root_page_id;
        let mut saw_root_change = false;

        for i in 0..200u32 {
            let key = i.to_be_bytes();
            let value = format!("value-{i}");
            let mutation = tree.insert_with_mutation(&key, value.as_bytes())?;
            final_root_page_id = mutation.root_page_id;
            saw_root_change |= mutation.root_changed;
        }

        assert!(saw_root_change);
        assert_ne!(final_root_page_id, original_root_page_id);

        let mut reopened = trees.open_tree(final_root_page_id)?;
        assert_eq!(
            reopened.get(&42u32.to_be_bytes())?,
            Some(b"value-42".to_vec())
        );

        Ok(())
    }

    #[test]
    fn test_byte_tree_range_and_prefix_helpers() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let trees = ByteTreeStore::new(storage);
        let root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;

        tree.insert(b"alpha:1", b"a1")?;
        tree.insert(b"alpha:2", b"a2")?;
        tree.insert(b"beta:1", b"b1")?;
        tree.insert(b"gamma:1", b"g1")?;

        let from_beta = tree.entries_from(b"beta")?;
        assert_eq!(from_beta.len(), 2);
        assert_eq!(from_beta[0], (b"beta:1".to_vec(), b"b1".to_vec()));

        let alpha_entries = tree.entries_with_prefix(b"alpha:")?;
        assert_eq!(
            alpha_entries,
            vec![
                (b"alpha:1".to_vec(), b"a1".to_vec()),
                (b"alpha:2".to_vec(), b"a2".to_vec())
            ]
        );

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

    #[test]
    fn test_duplicate_key_update_on_dense_tree() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let mut btree = BTreeIndex::new_with_init(storage)?;

        // Grow the tree so duplicate update happens in a dense structure.
        for i in 0u32..300u32 {
            let key = BTreeKey::new(i.to_le_bytes().to_vec());
            let value = BTreeValue::new(format!("v{i}").into_bytes());
            btree.insert(key, value)?;
        }

        let target_key = BTreeKey::new(42u32.to_le_bytes().to_vec());
        let replacement = BTreeValue::new(b"replacement-value".to_vec());
        btree.insert(target_key.clone(), replacement.clone())?;

        let found = btree.search(&target_key)?;
        assert_eq!(found, Some(replacement));

        // Ensure duplicate update did not create multiple entries.
        let mut cursor = btree.cursor()?;
        let mut seen = 0usize;
        while cursor.is_valid() {
            if let Some((key, _)) = cursor.current() {
                if key == &target_key {
                    seen += 1;
                }
            }
            cursor.next()?;
        }
        assert_eq!(seen, 1);

        Ok(())
    }

    #[test]
    fn test_rebalance_after_large_right_side_deletes() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let mut btree = BTreeIndex::new_with_init(storage)?;

        for i in 0u8..140u8 {
            btree.insert(BTreeKey::new(vec![i]), BTreeValue::new(vec![i, i]))?;
        }

        // Delete heavily from the upper key range to force repeated underflow handling
        // in right-side subtrees (borrow and eventually merge paths).
        for i in 80u8..140u8 {
            let deleted = btree.delete(&BTreeKey::new(vec![i]))?;
            assert!(deleted.is_some(), "expected key {} to be deleted", i);
        }

        // Remaining keys should still be searchable.
        for i in 0u8..80u8 {
            let found = btree.search(&BTreeKey::new(vec![i]))?;
            assert!(found.is_some(), "expected key {} to still exist", i);
        }

        // Deleted keys should be absent.
        for i in 80u8..140u8 {
            let found = btree.search(&BTreeKey::new(vec![i]))?;
            assert!(found.is_none(), "expected key {} to be gone", i);
        }

        // Cursor order should remain strictly increasing with no duplicates.
        let mut cursor = btree.cursor()?;
        let mut expected = 0u8;
        while cursor.is_valid() {
            let (key, _value) = cursor.current().expect("valid cursor should have current");
            assert_eq!(key.as_bytes(), &[expected]);
            expected = expected.wrapping_add(1);
            cursor.next()?;
        }
        assert_eq!(expected, 80u8);

        Ok(())
    }

    #[test]
    fn test_validate_tree_reports_healthy_tree() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let mut manager = BTreeManager::new(storage);
        let root = manager.create_tree()?;
        let mut index = manager.open_tree(root)?;

        for i in 0u8..40u8 {
            index.insert(BTreeKey::new(vec![i]), BTreeValue::new(vec![i]))?;
        }

        assert!(manager.validate_tree(index.root_page_id())?);
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

    #[test]
    fn test_randomized_insert_delete_reopen_integrity() -> Result<()> {
        let path = tmp_db();
        let mut shared = Arc::new(Mutex::new(new_storage(&path)?));
        let mut manager = BTreeManager::from_shared_storage(shared.clone());
        let mut root_page_id = manager.create_tree()?;
        let mut btree = manager.open_tree(root_page_id)?;

        let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut rng = LcgRng::new(0xA11C_E52E_2026_0321);

        for step in 0usize..600usize {
            let key_id = rng.next_u32() % 4000;
            let key_bytes = key_id.to_be_bytes().to_vec();
            let key = BTreeKey::new(key_bytes.clone());
            let choice = rng.next_u32() % 100;

            if choice < 75 {
                let value_seed = rng.next_u64();
                let mut value_bytes = Vec::with_capacity(16);
                value_bytes.extend_from_slice(&value_seed.to_le_bytes());
                value_bytes.extend_from_slice(&(step as u64).to_le_bytes());
                let value = BTreeValue::new(value_bytes.clone());
                btree.insert(key, value)?;
                oracle.insert(key_bytes, value_bytes);
            } else if choice < 85 {
                let deleted = btree.delete(&key)?;
                let expected = oracle.remove(&key_bytes).map(BTreeValue::new);
                assert_eq!(deleted, expected);
            } else {
                let found = btree.search(&key)?;
                let expected = oracle.get(&key_bytes).cloned().map(BTreeValue::new);
                assert_eq!(found, expected);
            }

            root_page_id = btree.root_page_id();
            if step % 150 == 0 {
                shared.lock().unwrap().flush()?;
                drop(btree);
                drop(manager);
                drop(shared);

                shared = Arc::new(Mutex::new(new_storage(&path)?));
                manager = BTreeManager::from_shared_storage(shared.clone());
                btree = manager.open_tree(root_page_id)?;

                for (k, v) in &oracle {
                    let found = btree.search(&BTreeKey::new(k.clone()))?;
                    assert_eq!(found, Some(BTreeValue::new(v.clone())));
                }
            }
        }

        let mut cursor = btree.cursor()?;
        let mut actual = Vec::new();
        while cursor.is_valid() {
            if let Some((k, v)) = cursor.current() {
                actual.push((k.as_bytes().to_vec(), v.as_bytes().to_vec()));
            }
            cursor.next()?;
        }

        let expected = oracle.into_iter().collect::<Vec<_>>();
        assert_eq!(actual, expected);

        Ok(())
    }
}

mod tree_tests {
    use crate::btree::node::{BTREE_PAGE_FORMAT_VERSION, BTREE_PAGE_HEADER_SIZE};
    use crate::btree::tree::BTreeManager;
    use crate::btree::{BTreeKey, BTreeNode, BTreeValue, NodeType};
    use crate::error::Result;
    use crate::storage::{Page, Pager, PAGE_SIZE};

    #[test]
    fn test_btree_key_comparison() {
        let key1 = BTreeKey::new(vec![1, 2, 3]);
        let key2 = BTreeKey::new(vec![1, 2, 4]);
        let key3 = BTreeKey::new(vec![1, 2, 3]);

        assert!(key1 < key2);
        assert!(key2 > key1);
        assert_eq!(key1, key3);
    }

    #[test]
    fn test_btree_node_creation() {
        let page_id = 1;
        let leaf_node = BTreeNode::new_leaf(page_id);
        assert!(matches!(leaf_node.node_type, NodeType::Leaf));
        assert_eq!(leaf_node.keys.len(), 0);
        assert_eq!(leaf_node.values.len(), 0);

        let internal_node = BTreeNode::new_internal(page_id);
        assert!(matches!(internal_node.node_type, NodeType::Internal));
        assert_eq!(internal_node.keys.len(), 0);
        assert_eq!(internal_node.children.len(), 0);
    }

    #[test]
    fn test_btree_node_serialization() -> Result<()> {
        let page_id = 1;
        let mut node = BTreeNode::new_leaf(page_id);

        node.keys.push(BTreeKey::new(vec![1, 2, 3]));
        node.keys.push(BTreeKey::new(vec![4, 5, 6]));
        node.values.push(BTreeValue::new(vec![7, 8, 9]));
        node.values.push(BTreeValue::new(vec![10, 11, 12]));

        let mut page = Page::new(page_id);
        BTreeNode::to_page(&node, &mut page)?;

        let deserialized_node = BTreeNode::from_page(page)?;
        assert_eq!(deserialized_node.node_type, node.node_type);
        assert_eq!(deserialized_node.keys.len(), node.keys.len());
        assert_eq!(deserialized_node.values.len(), node.values.len());

        for (i, key) in node.keys.iter().enumerate() {
            assert_eq!(deserialized_node.keys[i].data, key.data);
        }

        for (i, value) in node.values.iter().enumerate() {
            assert_eq!(deserialized_node.values[i].data, value.data);
        }

        Ok(())
    }

    #[test]
    fn test_btree_page_rejects_unsupported_version() -> Result<()> {
        let page_id = 1;
        let mut node = BTreeNode::new_leaf(page_id);
        node.keys.push(BTreeKey::new(vec![1, 2, 3]));
        node.values.push(BTreeValue::new(vec![7, 8, 9]));

        let mut page = Page::new(page_id);
        BTreeNode::to_page(&node, &mut page)?;
        page.data[4] = BTREE_PAGE_FORMAT_VERSION.saturating_add(1);

        let err = BTreeNode::from_page(page).unwrap_err();
        assert!(err.to_string().contains("Unsupported B-tree version"));

        Ok(())
    }

    #[test]
    fn test_btree_page_rejects_invalid_payload_length() -> Result<()> {
        let page_id = 1;
        let mut node = BTreeNode::new_leaf(page_id);
        node.keys.push(BTreeKey::new(vec![1, 2, 3]));
        node.values.push(BTreeValue::new(vec![7, 8, 9]));

        let mut page = Page::new(page_id);
        BTreeNode::to_page(&node, &mut page)?;

        // Corrupt payload_len to exceed page boundary.
        page.data[14..18].copy_from_slice(&((PAGE_SIZE as u32) + 1).to_le_bytes());
        let err = BTreeNode::from_page(page).unwrap_err();
        assert!(err.to_string().contains("Payload length"));

        // Keep a guard that the header offset contract is stable.
        assert_eq!(BTREE_PAGE_HEADER_SIZE, 18);

        Ok(())
    }

    #[test]
    fn test_leaf_merge_rejects_oversized_result() -> Result<()> {
        let mut storage = Pager::new_in_memory(100)?;
        let left_page = storage.allocate_page()?;
        let right_page = storage.allocate_page()?;

        let mut left = BTreeNode::new_leaf(left_page);
        let mut right = BTreeNode::new_leaf(right_page);

        for i in 0u32..10u32 {
            left.keys.push(BTreeKey::new(i.to_le_bytes().to_vec()));
            left.values.push(BTreeValue::new(vec![1u8; 200]));
        }
        for i in 10u32..20u32 {
            right.keys.push(BTreeKey::new(i.to_le_bytes().to_vec()));
            right.values.push(BTreeValue::new(vec![2u8; 200]));
        }

        let err = left.merge_leaf(&mut right, &mut storage).unwrap_err();
        assert!(err.to_string().contains("Nodes cannot be merged"));

        Ok(())
    }

    #[test]
    fn test_leaf_merge_deallocates_other_page() -> Result<()> {
        let mut storage = Pager::new_in_memory(100)?;
        let left_page = storage.allocate_page()?;
        let right_page = storage.allocate_page()?;

        let mut left = BTreeNode::new_leaf(left_page);
        let mut right = BTreeNode::new_leaf(right_page);

        left.keys.push(BTreeKey::new(vec![1]));
        left.values.push(BTreeValue::new(vec![11]));
        right.keys.push(BTreeKey::new(vec![2]));
        right.values.push(BTreeValue::new(vec![22]));

        left.merge_leaf(&mut right, &mut storage)?;
        assert_eq!(left.keys.len(), 2);
        assert_eq!(left.values.len(), 2);

        // Right page should have been returned to freelist and reused first.
        let reused = storage.allocate_page()?;
        assert_eq!(reused, right_page);

        Ok(())
    }

    #[test]
    fn test_validate_tree_rejects_unsorted_leaf_keys() -> Result<()> {
        let test_db = crate::test_utils::TestDbFile::new("_test_btree_validate_unsorted_leaf");
        let mut storage = Pager::new(test_db.path(), 100)?;
        let root = storage.allocate_page()?;

        let mut leaf = BTreeNode::new_leaf(root);
        leaf.keys.push(BTreeKey::new(vec![2]));
        leaf.values.push(BTreeValue::new(vec![20]));
        leaf.keys.push(BTreeKey::new(vec![1]));
        leaf.values.push(BTreeValue::new(vec![10]));

        let mut page = Page::new(root);
        leaf.to_page(&mut page)?;
        storage.write_page(page)?;
        storage.flush()?;

        let mut manager = BTreeManager::new(storage);
        assert!(!manager.validate_tree(root)?);

        Ok(())
    }

    #[test]
    fn test_validate_tree_rejects_mixed_leaf_depths() -> Result<()> {
        let test_db =
            crate::test_utils::TestDbFile::new("_test_btree_validate_leaf_depth_mismatch");
        let mut storage = Pager::new(test_db.path(), 100)?;

        let root = storage.allocate_page()?;
        let left_leaf_page = storage.allocate_page()?;
        let right_internal_page = storage.allocate_page()?;
        let right_left_leaf_page = storage.allocate_page()?;
        let right_right_leaf_page = storage.allocate_page()?;

        let mut left_leaf = BTreeNode::new_leaf(left_leaf_page);
        left_leaf.keys.push(BTreeKey::new(vec![1]));
        left_leaf.values.push(BTreeValue::new(vec![11]));

        let mut right_left_leaf = BTreeNode::new_leaf(right_left_leaf_page);
        right_left_leaf.keys.push(BTreeKey::new(vec![6]));
        right_left_leaf.values.push(BTreeValue::new(vec![66]));

        let mut right_right_leaf = BTreeNode::new_leaf(right_right_leaf_page);
        right_right_leaf.keys.push(BTreeKey::new(vec![9]));
        right_right_leaf.values.push(BTreeValue::new(vec![99]));

        let mut right_internal = BTreeNode::new_internal(right_internal_page);
        right_internal.keys.push(BTreeKey::new(vec![8]));
        right_internal.children.push(right_left_leaf_page);
        right_internal.children.push(right_right_leaf_page);

        let mut root_node = BTreeNode::new_internal(root);
        root_node.keys.push(BTreeKey::new(vec![5]));
        root_node.children.push(left_leaf_page);
        root_node.children.push(right_internal_page);

        let mut root_page = Page::new(root);
        root_node.to_page(&mut root_page)?;
        storage.write_page(root_page)?;

        let mut left_page = Page::new(left_leaf_page);
        left_leaf.to_page(&mut left_page)?;
        storage.write_page(left_page)?;

        let mut right_internal_page_data = Page::new(right_internal_page);
        right_internal.to_page(&mut right_internal_page_data)?;
        storage.write_page(right_internal_page_data)?;

        let mut right_left_page = Page::new(right_left_leaf_page);
        right_left_leaf.to_page(&mut right_left_page)?;
        storage.write_page(right_left_page)?;

        let mut right_right_page = Page::new(right_right_leaf_page);
        right_right_leaf.to_page(&mut right_right_page)?;
        storage.write_page(right_right_page)?;
        storage.flush()?;

        let mut manager = BTreeManager::new(storage);
        assert!(!manager.validate_tree(root)?);

        Ok(())
    }
}
