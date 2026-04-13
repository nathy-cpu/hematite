//! Centralized tests for the btree module

mod mod_tests {
    use crate::btree::bytes::ByteTreeStore;
    use crate::btree::codec::RawBytesCodec;
    use crate::btree::index::BTreeIndex;
    use crate::btree::tree::BTreeManager;
    use crate::btree::value_store::StoredValueLayout;
    use crate::btree::{BTreeKey, BTreeValue, KeyValueCodec, TypedTreeStore};
    use crate::error::Result;
    use crate::storage::overflow::collect_overflow_page_ids;
    use crate::storage::Pager;
    use crate::test_utils::TestDbFile;
    use std::collections::BTreeMap;
    use std::sync::{Arc, RwLock};

    fn tmp_db() -> TestDbFile {
        TestDbFile::new("_test_btree")
    }

    fn new_storage(db: &TestDbFile) -> Result<Pager> {
        Pager::new(db.path(), 100)
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
    fn test_typed_tree_store_codec_boundary() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let trees = TypedTreeStore::<U32StringCodec>::from_storage(storage);
        let root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;

        tree.insert(&7, &"seven".to_string())?;
        tree.insert(&9, &"nine".to_string())?;

        assert_eq!(tree.get(&7)?, Some("seven".to_string()));
        assert_eq!(tree.delete(&9)?, Some("nine".to_string()));

        let cursor = tree.cursor()?;
        assert!(cursor.is_valid());
        assert_eq!(cursor.current()?, Some((7, "seven".to_string())));
        assert!(trees.validate_tree(root_page_id)?);

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
        assert_eq!(
            cursor.current()?,
            Some((b"alpha".to_vec(), b"one".to_vec()))
        );

        cursor.seek(b"beta")?;
        assert_eq!(cursor.current()?, Some((b"beta".to_vec(), b"two".to_vec())));

        Ok(())
    }

    #[test]
    fn test_btree_cursor_materializes_key_and_value_lazily() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let mut btree = BTreeIndex::new_with_init(storage)?;

        btree.insert(
            BTreeKey::new(b"alpha".to_vec()),
            BTreeValue::new(b"one".to_vec()),
        )?;
        btree.insert(
            BTreeKey::new(b"beta".to_vec()),
            BTreeValue::new(b"two".to_vec()),
        )?;

        let mut cursor = btree.cursor()?;
        assert_eq!(cursor.cache_materialized(), (false, false));

        assert_eq!(
            cursor.key().map(|key| key.as_bytes()),
            Some(b"alpha".as_slice())
        );
        assert_eq!(cursor.cache_materialized(), (true, false));

        assert_eq!(
            cursor.value().map(|value| value.as_bytes()),
            Some(b"one".as_slice())
        );
        assert_eq!(cursor.cache_materialized(), (true, true));

        cursor.next()?;
        assert_eq!(cursor.cache_materialized(), (false, false));
        assert_eq!(
            cursor
                .current()
                .map(|(key, value)| (key.as_bytes(), value.as_bytes())),
            Some((b"beta".as_slice(), b"two".as_slice()))
        );
        assert_eq!(cursor.cache_materialized(), (true, true));

        Ok(())
    }

    #[test]
    fn test_byte_tree_cursor_reuses_overflow_cache_for_repeated_current_reads() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let trees = ByteTreeStore::new(storage);
        let root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;

        let large_value = vec![0x51; crate::storage::PAGE_SIZE * 2];
        tree.insert(b"blob", &large_value)?;

        let cursor = tree.cursor()?;
        assert_eq!(cursor.overflow_cache_stats(), (0, 0));

        assert_eq!(
            cursor.current()?,
            Some((b"blob".to_vec(), large_value.clone()))
        );
        assert_eq!(cursor.overflow_cache_stats(), (0, 1));

        assert_eq!(cursor.current()?, Some((b"blob".to_vec(), large_value)));
        assert_eq!(cursor.overflow_cache_stats(), (1, 1));

        Ok(())
    }

    #[test]
    fn test_byte_tree_keeps_root_page_stable_across_splits() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let trees = ByteTreeStore::new(storage);
        let original_root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(original_root_page_id)?;
        let mut final_root_page_id = original_root_page_id;

        for i in 0..200u32 {
            let key = i.to_be_bytes();
            let value = format!("value-{i}");
            let mutation = tree.insert_with_mutation(&key, value.as_bytes())?;
            final_root_page_id = mutation.root_page_id;
            assert!(!mutation.root_changed);
        }

        assert_eq!(final_root_page_id, original_root_page_id);

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
    fn test_byte_tree_large_values_roundtrip_and_cursor() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let trees = ByteTreeStore::new(storage);
        let root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;

        let large_value = vec![0x5A; crate::storage::PAGE_SIZE * 2];
        tree.insert(b"blob", &large_value)?;
        assert_eq!(tree.get(b"blob")?, Some(large_value.clone()));

        let cursor = tree.cursor()?;
        assert_eq!(cursor.current()?, Some((b"blob".to_vec(), large_value)));

        Ok(())
    }

    #[test]
    fn test_byte_tree_failed_large_insert_restores_snapshot_and_reclaims_overflow_pages(
    ) -> Result<()> {
        let trees = ByteTreeStore::new(Pager::new_in_memory(100)?);
        let root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;
        let large_value = vec![0x77; crate::storage::PAGE_SIZE * 2];
        let baseline_allocated = trees.allocated_page_count()?;
        let baseline_free_pages = trees.free_page_ids()?;

        trees
            .shared_storage()
            .write()
            .unwrap()
            .inject_io_failure_after(1);

        let err = tree.insert(b"blob", &large_value).unwrap_err();
        assert!(err.to_string().contains("Injected IO error"));
        assert_eq!(tree.get(b"blob")?, None);
        assert_eq!(trees.allocated_page_count()?, baseline_allocated);
        assert_eq!(trees.free_page_ids()?, baseline_free_pages);
        assert!(trees.validate_tree(root_page_id)?);
        assert!(trees.validate_tree_overflow(root_page_id).is_ok());

        Ok(())
    }

    #[test]
    fn test_byte_tree_failed_large_update_restores_previous_value_and_overflow_state(
    ) -> Result<()> {
        let trees = ByteTreeStore::new(Pager::new_in_memory(100)?);
        let root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;
        let original_value = vec![0x31; crate::storage::PAGE_SIZE * 2];
        let replacement_value = vec![0x52; crate::storage::PAGE_SIZE * 2];

        tree.insert(b"blob", &original_value)?;
        let baseline_allocated = trees.allocated_page_count()?;
        let baseline_free_pages = trees.free_page_ids()?;

        trees
            .shared_storage()
            .write()
            .unwrap()
            .inject_io_failure_after(1);

        let err = tree.insert(b"blob", &replacement_value).unwrap_err();
        assert!(err.to_string().contains("Injected IO error"));
        assert_eq!(tree.get(b"blob")?, Some(original_value));
        assert_eq!(trees.allocated_page_count()?, baseline_allocated);
        assert_eq!(trees.free_page_ids()?, baseline_free_pages);
        assert!(trees.validate_tree(root_page_id)?);
        assert!(trees.validate_tree_overflow(root_page_id).is_ok());

        Ok(())
    }

    #[test]
    fn test_byte_tree_failed_large_delete_restores_previous_value_and_overflow_state(
    ) -> Result<()> {
        let path = tmp_db();
        let trees = ByteTreeStore::new(Pager::new(path.path(), 1)?);
        let root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;
        let large_value = vec![0x63; crate::storage::PAGE_SIZE * 2];

        tree.insert(b"blob", &large_value)?;
        trees.flush()?;
        let baseline_allocated = trees.allocated_page_count()?;
        let baseline_free_pages = trees.free_page_ids()?;

        trees.begin_transaction()?;
        trees
            .shared_storage()
            .write()
            .unwrap()
            .inject_io_failure_after(0);

        let err = tree.delete(b"blob").unwrap_err();
        assert!(err.to_string().contains("Injected IO error"));
        assert_eq!(tree.get(b"blob")?, Some(large_value));
        assert_eq!(trees.allocated_page_count()?, baseline_allocated);
        assert_eq!(trees.free_page_ids()?, baseline_free_pages);
        assert!(trees.transaction_active()?);

        trees.rollback_transaction()?;
        assert!(trees.validate_tree(root_page_id)?);
        assert!(trees.validate_tree_overflow(root_page_id).is_ok());

        Ok(())
    }

    #[test]
    fn test_byte_tree_delete_reclaims_overflow_pages() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let trees = ByteTreeStore::new(storage);
        let root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;
        let large_value = vec![0x6B; crate::storage::PAGE_SIZE * 2];

        tree.insert(b"blob", &large_value)?;

        let shared = trees.shared_storage();
        let mut raw_tree = BTreeIndex::from_shared_storage(shared.clone(), root_page_id);
        let stored_value = raw_tree
            .search_typed::<RawBytesCodec>(&b"blob".to_vec())?
            .expect("stored value should exist");
        let layout = StoredValueLayout::decode(&stored_value)?;
        let overflow_ids = {
            let mut pager = shared.write().unwrap();
            collect_overflow_page_ids(&mut pager, Some(layout.overflow_first_page))?
        };

        assert_eq!(tree.delete(b"blob")?, Some(large_value));

        let reused_page_id = shared.write().unwrap().allocate_page()?;
        assert!(overflow_ids.contains(&reused_page_id));
        Ok(())
    }

    #[test]
    fn test_byte_tree_reset_reclaims_overflow_pages() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let trees = ByteTreeStore::new(storage);
        let root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;
        let large_value = vec![0x33; crate::storage::PAGE_SIZE * 2];

        tree.insert(b"blob", &large_value)?;

        let shared = trees.shared_storage();
        let mut raw_tree = BTreeIndex::from_shared_storage(shared.clone(), root_page_id);
        let stored_value = raw_tree
            .search_typed::<RawBytesCodec>(&b"blob".to_vec())?
            .expect("stored value should exist");
        let layout = StoredValueLayout::decode(&stored_value)?;
        let overflow_ids = {
            let mut pager = shared.write().unwrap();
            collect_overflow_page_ids(&mut pager, Some(layout.overflow_first_page))?
        };

        trees.reset_tree(root_page_id)?;

        let reused_page_id = shared.write().unwrap().allocate_page()?;
        assert!(overflow_ids.contains(&reused_page_id));
        Ok(())
    }

    #[test]
    fn test_byte_tree_failed_reset_restores_previous_value_and_overflow_state() -> Result<()> {
        let path = tmp_db();
        let trees = ByteTreeStore::new(Pager::new(path.path(), 1)?);
        let root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;
        let large_value = vec![0x47; crate::storage::PAGE_SIZE * 2];

        tree.insert(b"blob", &large_value)?;
        trees.flush()?;
        let baseline_allocated = trees.allocated_page_count()?;
        let baseline_free_pages = trees.free_page_ids()?;

        trees.begin_transaction()?;

        trees
            .shared_storage()
            .write()
            .unwrap()
            .inject_io_failure_after(0);

        let err = trees.reset_tree(root_page_id).unwrap_err();
        assert!(err.to_string().contains("Injected IO error"));
        assert!(trees.transaction_active()?);

        assert_eq!(tree.get(b"blob")?, Some(large_value));

        trees.rollback_transaction()?;

        assert_eq!(trees.allocated_page_count()?, baseline_allocated);
        assert_eq!(trees.free_page_ids()?, baseline_free_pages);
        assert!(trees.validate_tree(root_page_id)?);
        assert!(trees.validate_tree_overflow(root_page_id).is_ok());

        Ok(())
    }

    #[test]
    fn test_byte_tree_validate_tree_detects_overflow_cycle() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let trees = ByteTreeStore::new(storage);
        let root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;
        let large_value = vec![0x41; crate::storage::PAGE_SIZE * 2];

        tree.insert(b"blob", &large_value)?;

        let shared = trees.shared_storage();
        let mut raw_tree = BTreeIndex::from_shared_storage(shared.clone(), root_page_id);
        let stored_value = raw_tree
            .search_typed::<RawBytesCodec>(&b"blob".to_vec())?
            .expect("stored value should exist");
        let layout = StoredValueLayout::decode(&stored_value)?;
        let mut pager = shared.write().unwrap();
        let mut overflow_page = pager.read_page(layout.overflow_first_page)?;
        overflow_page.data[4..8].copy_from_slice(&layout.overflow_first_page.to_le_bytes());
        pager.write_page(overflow_page)?;
        drop(pager);

        assert!(!trees.validate_tree(root_page_id)?);
        assert!(trees.validate_tree_overflow(root_page_id).is_err());
        Ok(())
    }

    #[test]
    fn test_byte_tree_validate_tree_detects_truncated_overflow_chain() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let trees = ByteTreeStore::new(storage);
        let root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;
        let large_value = vec![0x51; crate::storage::PAGE_SIZE * 2];

        tree.insert(b"blob", &large_value)?;

        let shared = trees.shared_storage();
        let mut raw_tree = BTreeIndex::from_shared_storage(shared.clone(), root_page_id);
        let stored_value = raw_tree
            .search_typed::<RawBytesCodec>(&b"blob".to_vec())?
            .expect("stored value should exist");
        let layout = StoredValueLayout::decode(&stored_value)?;
        let mut pager = shared.write().unwrap();
        let mut overflow_page = pager.read_page(layout.overflow_first_page)?;
        overflow_page.data[4..8].copy_from_slice(&crate::storage::INVALID_PAGE_ID.to_le_bytes());
        pager.write_page(overflow_page)?;
        drop(pager);

        assert!(!trees.validate_tree(root_page_id)?);
        assert!(trees.validate_tree_overflow(root_page_id).is_err());
        Ok(())
    }

    #[test]
    fn test_byte_tree_large_values_survive_root_splits() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let trees = ByteTreeStore::new(storage);
        let mut root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;

        for i in 0..64u32 {
            let key = i.to_be_bytes();
            let value = vec![i as u8; crate::storage::PAGE_SIZE + 257];
            let mutation = tree.insert_with_mutation(&key, &value)?;
            root_page_id = mutation.root_page_id;
        }

        drop(tree);
        let mut reopened = trees.open_tree(root_page_id)?;
        for key in [0u32, 7, 31, 63] {
            let value = reopened
                .get(&key.to_be_bytes())?
                .expect("value should exist");
            assert_eq!(value, vec![key as u8; crate::storage::PAGE_SIZE + 257]);
        }
        assert!(trees.validate_tree_overflow(root_page_id).is_ok());
        assert!(trees.validate_tree(root_page_id)?);

        Ok(())
    }

    #[test]
    fn test_byte_tree_large_value_deletes_survive_rebalance() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let trees = ByteTreeStore::new(storage);
        let mut root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;

        for i in 0..48u32 {
            let key = i.to_be_bytes();
            let value = vec![i as u8; crate::storage::PAGE_SIZE + 193];
            let mutation = tree.insert_with_mutation(&key, &value)?;
            root_page_id = mutation.root_page_id;
        }

        for i in 0..40u32 {
            let key = i.to_be_bytes();
            let (_deleted, mutation) = tree.delete_with_mutation(&key)?;
            root_page_id = mutation.root_page_id;
        }

        drop(tree);
        let mut reopened = trees.open_tree(root_page_id)?;
        for key in 40u32..48 {
            let value = reopened
                .get(&key.to_be_bytes())?
                .expect("value should remain after deletes");
            assert_eq!(value, vec![key as u8; crate::storage::PAGE_SIZE + 193]);
        }
        assert!(trees.validate_tree_overflow(root_page_id).is_ok());
        assert!(trees.validate_tree(root_page_id)?);

        Ok(())
    }

    #[test]
    fn test_byte_tree_randomized_large_value_reopen_integrity() -> Result<()> {
        let path = tmp_db();
        let storage = new_storage(&path)?;
        let trees = ByteTreeStore::new(storage);
        let mut root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;
        let mut expected = BTreeMap::<u32, Vec<u8>>::new();
        let mut rng = LcgRng::new(0xB71E_2026);

        for step in 0..160u32 {
            let key = rng.next_u32() % 48;
            if rng.next_u32() % 3 == 0 {
                let (deleted, mutation) = tree.delete_with_mutation(&key.to_be_bytes())?;
                root_page_id = mutation.root_page_id;
                let expected_deleted = expected.remove(&key);
                assert_eq!(deleted, expected_deleted);
            } else {
                let logical_len = if rng.next_u32() % 2 == 0 {
                    32 + (rng.next_u32() as usize % 96)
                } else {
                    crate::storage::PAGE_SIZE + 64 + (rng.next_u32() as usize % 256)
                };
                let value = vec![(step % 251) as u8; logical_len];
                let mutation = tree.insert_with_mutation(&key.to_be_bytes(), &value)?;
                root_page_id = mutation.root_page_id;
                expected.insert(key, value);
            }

            if step % 20 == 0 {
                drop(tree);
                assert!(trees.validate_tree(root_page_id)?);
                tree = trees.open_tree(root_page_id)?;
            }
        }

        drop(tree);
        assert!(trees.validate_tree(root_page_id)?);
        let mut reopened = trees.open_tree(root_page_id)?;
        for (key, value) in expected {
            let res = reopened.get(&key.to_be_bytes())?;
            if res != Some(value.clone()) {
                println!("FAIL on key {}. res is {:?}", key, res);
                assert_eq!(res, Some(value));
            }
        }

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
        let mut shared = Arc::new(RwLock::new(new_storage(&path)?));
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
                shared.write().unwrap().flush()?;
                drop(btree);
                drop(manager);
                drop(shared);

                shared = Arc::new(RwLock::new(new_storage(&path)?));
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

mod value_store_tests {
    use crate::btree::value_store::{
        StoredValueLayout, STORED_VALUE_HEADER_SIZE, STORED_VALUE_LOCAL_CAPACITY,
    };
    use crate::storage::INVALID_PAGE_ID;

    #[test]
    fn test_stored_value_layout_inline_roundtrip() -> crate::error::Result<()> {
        let layout = StoredValueLayout::new_inline(b"hello world")?;
        let encoded = layout.encode()?;
        let decoded = StoredValueLayout::decode(&encoded)?;

        assert_eq!(decoded, layout);
        assert_eq!(decoded.overflow_first_page, INVALID_PAGE_ID);
        Ok(())
    }

    #[test]
    fn test_stored_value_layout_overflow_roundtrip() -> crate::error::Result<()> {
        let local_payload = vec![0xAB; STORED_VALUE_LOCAL_CAPACITY];
        let layout = StoredValueLayout::new_overflow(
            STORED_VALUE_LOCAL_CAPACITY + 123,
            local_payload.clone(),
            77,
        )?;
        let encoded = layout.encode()?;
        assert_eq!(
            encoded.len(),
            STORED_VALUE_HEADER_SIZE + local_payload.len()
        );

        let decoded = StoredValueLayout::decode(&encoded)?;
        assert_eq!(decoded, layout);
        assert_eq!(decoded.overflow_len(), 123);
        Ok(())
    }

    #[test]
    fn test_stored_value_layout_rejects_invalid_headers() {
        assert!(StoredValueLayout::decode(&[0u8; STORED_VALUE_HEADER_SIZE - 1]).is_err());

        let mut bad_inline = vec![0u8; STORED_VALUE_HEADER_SIZE];
        bad_inline[0] = 0;
        bad_inline[1..5].copy_from_slice(&1u32.to_le_bytes());
        bad_inline[5..7].copy_from_slice(&0u16.to_le_bytes());
        bad_inline[7..11].copy_from_slice(&7u32.to_le_bytes());
        assert!(StoredValueLayout::decode(&bad_inline).is_err());
    }
}

mod tree_tests {
    use crate::btree::bytes::ByteTreeStore;
    use crate::btree::node::BTreeNode;
    use crate::btree::node::BTREE_PAGE_HEADER_SIZE;
    use crate::btree::tree::BTreeManager;
    use crate::btree::{BTreeKey, BTreeValue, NodeType};
    use crate::error::Result;
    use crate::storage::format::DATABASE_HEADER_SIZE;
    use crate::storage::{Page, Pager};

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

        node.keys.push_back(BTreeKey::new(vec![1, 2, 3]));
        node.keys.push_back(BTreeKey::new(vec![4, 5, 6]));
        node.values.push_back(BTreeValue::new(vec![7, 8, 9]));
        node.values.push_back(BTreeValue::new(vec![10, 11, 12]));

        let mut page = Page::new(page_id);
        BTreeNode::to_page(&node, &mut page)?;

        let deserialized_node = BTreeNode::from_page_decoded(page)?;
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
    fn test_btree_lazy_page_views_do_not_force_decode() -> Result<()> {
        let page_id = 1;
        let mut node = BTreeNode::new_leaf(page_id);
        node.keys.push_back(BTreeKey::new(vec![1, 2, 3]));
        node.keys.push_back(BTreeKey::new(vec![4, 5, 6]));
        node.values.push_back(BTreeValue::new(vec![7, 8, 9]));
        node.values.push_back(BTreeValue::new(vec![10, 11, 12]));

        let mut page = Page::new(page_id);
        BTreeNode::to_page(&node, &mut page)?;

        let lazy = BTreeNode::from_page(page)?;
        assert!(!lazy.is_decoded);
        assert_eq!(lazy.key_count, 2);
        assert!(lazy.keys.is_empty());
        assert!(lazy.values.is_empty());

        assert_eq!(lazy.get_key_view(0)?, &[1, 2, 3]);
        assert_eq!(lazy.get_value_view(1)?, &[10, 11, 12]);
        assert!(!lazy.is_decoded);

        match lazy.search(&BTreeKey::new(vec![4, 5, 6]))? {
            crate::btree::node::SearchResult::Found(value) => {
                assert_eq!(value.data, vec![10, 11, 12]);
            }
            crate::btree::node::SearchResult::NotFound(_) => {
                panic!("expected exact key to be found from lazy page view");
            }
        }
        assert!(!lazy.is_decoded);

        Ok(())
    }

    #[test]
    fn test_btree_lazy_search_reports_duplicate_cell_offsets_as_corruption() -> Result<()> {
        let page_id = 2;
        let mut node = BTreeNode::new_leaf(page_id);
        node.keys.push_back(BTreeKey::new(vec![1]));
        node.values.push_back(BTreeValue::new(vec![10]));
        node.keys.push_back(BTreeKey::new(vec![2]));
        node.values.push_back(BTreeValue::new(vec![20]));

        let mut page = Page::new(page_id);
        BTreeNode::to_page(&node, &mut page)?;

        let mut lazy = BTreeNode::from_page(page)?;
        lazy.cell_offsets[1] = lazy.cell_offsets[0];
        *lazy.cell_ranges_cache.borrow_mut() = None;

        let err = lazy.search(&BTreeKey::new(vec![2])).unwrap_err();
        assert!(matches!(err, crate::error::HematiteError::CorruptedData(_)));
        assert!(!lazy.is_decoded);

        Ok(())
    }

    #[test]
    fn test_btree_lazy_internal_navigation_reports_missing_rightmost_child() -> Result<()> {
        let page_id = 2;
        let mut node = BTreeNode::new_internal(page_id);
        node.keys.push_back(BTreeKey::new(vec![5]));
        node.children.push_back(3);
        node.children.push_back(4);

        let mut page = Page::new(page_id);
        BTreeNode::to_page(&node, &mut page)?;
        page.data[8..12].copy_from_slice(&0u32.to_be_bytes());

        let lazy = BTreeNode::from_page(page)?;
        let err = lazy.find_child(&BTreeKey::new(vec![9])).unwrap_err();
        assert!(matches!(
            err,
            crate::error::HematiteError::CorruptedData(message)
                if message.contains("rightmost child")
        ));

        Ok(())
    }

    #[test]
    fn test_btree_lazy_open_skips_unread_internal_child_validation_until_needed() -> Result<()> {
        let page_id = 2;
        let mut node = BTreeNode::new_internal(page_id);
        node.keys.push_back(BTreeKey::new(vec![10]));
        node.keys.push_back(BTreeKey::new(vec![20]));
        node.children.push_back(3);
        node.children.push_back(4);
        node.children.push_back(5);

        let mut page = Page::new(page_id);
        BTreeNode::to_page(&node, &mut page)?;

        let probe = BTreeNode::from_page(page.clone())?;
        let second_offset = probe.cell_offsets[1] as usize;
        page.data[second_offset..second_offset + 4].copy_from_slice(&1u32.to_be_bytes());

        let lazy = BTreeNode::from_page(page)?;
        assert_eq!(lazy.find_child(&BTreeKey::new(vec![5]))?, 3);

        let err = lazy.validate_cell_layouts().unwrap_err();
        assert!(matches!(err, crate::error::HematiteError::CorruptedData(_)));

        Ok(())
    }

    #[test]
    fn test_byte_tree_corrupted_root_returns_errors_for_lookup_seek_and_scan() -> Result<()> {
        let trees = ByteTreeStore::new(Pager::new_in_memory(100)?);
        let root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;
        tree.insert(b"alpha", b"one")?;
        tree.insert(b"beta", b"two")?;

        let mut cursor = tree.cursor()?;
        let shared = trees.shared_storage();
        let mut pager = shared.write().unwrap();
        let mut page = pager.read_page(root_page_id)?;
        let first_pointer = [page.data[8], page.data[9]];
        page.data[10..12].copy_from_slice(&first_pointer);
        pager.write_page(page)?;
        drop(pager);

        let lookup_err = tree.get(b"beta").unwrap_err();
        assert!(matches!(
            lookup_err,
            crate::error::HematiteError::CorruptedData(_)
        ));

        let seek_err = cursor.seek(b"beta").unwrap_err();
        assert!(matches!(seek_err, crate::error::HematiteError::CorruptedData(_)));

        let scan_err = cursor.first().unwrap_err();
        assert!(matches!(scan_err, crate::error::HematiteError::CorruptedData(_)));

        Ok(())
    }

    #[test]
    fn test_btree_leaf_in_place_update_preserves_lazy_state() -> Result<()> {
        let page_id = 1;
        let key = BTreeKey::new(vec![1, 2, 3]);
        let mut node = BTreeNode::new_leaf(page_id);
        node.keys.push_back(key.clone());
        node.values.push_back(BTreeValue::new(vec![7, 8, 9]));

        let mut page = Page::new(page_id);
        BTreeNode::to_page(&node, &mut page)?;

        let mut lazy = BTreeNode::from_page(page.clone())?;
        assert!(!lazy.is_decoded);

        let replacement = BTreeValue::new(vec![4, 5, 6]);
        assert!(lazy.try_update_leaf_in_place(&mut page, &key, &replacement)?);
        assert!(!lazy.is_decoded);

        let decoded = BTreeNode::from_page_decoded(page)?;
        assert_eq!(decoded.values.len(), 1);
        assert_eq!(decoded.values[0].data, replacement.data);

        Ok(())
    }

    #[test]
    fn test_btree_append_split_keeps_existing_keys_left() -> Result<()> {
        let mut storage = Pager::new_in_memory(100)?;
        let page_id = storage.allocate_page()?;

        let mut node = BTreeNode::new_leaf(page_id);
        for i in 0u32..3u32 {
            node.keys.push_back(BTreeKey::new(i.to_be_bytes().to_vec()));
            node.values
                .push_back(BTreeValue::new(format!("v{i}").into_bytes()));
        }

        let mut page = Page::new(page_id);
        node.to_page(&mut page)?;
        storage.write_page(page)?;

        let mut lazy = BTreeNode::from_page(storage.read_page(page_id)?)?;
        let append_key = BTreeKey::new(99u32.to_be_bytes().to_vec());
        let append_value = BTreeValue::new(b"v99".to_vec());
        let (split_key, right_page_id) =
            lazy.split_leaf(&mut storage, append_key.clone(), append_value.clone())?;

        assert_eq!(split_key, append_key);

        let left = BTreeNode::from_page_decoded(storage.read_page(page_id)?)?;
        let right = BTreeNode::from_page_decoded(storage.read_page(right_page_id)?)?;

        assert_eq!(left.keys.len(), 3);
        assert_eq!(right.keys.len(), 1);
        assert_eq!(left.keys[0].data, 0u32.to_be_bytes().to_vec());
        assert_eq!(left.keys[1].data, 1u32.to_be_bytes().to_vec());
        assert_eq!(left.keys[2].data, 2u32.to_be_bytes().to_vec());
        assert_eq!(right.keys[0], append_key);
        assert_eq!(right.values[0], append_value);

        Ok(())
    }

    #[test]
    fn test_btree_page_rejects_unsupported_version() -> Result<()> {
        let page_id = 2;
        let mut node = BTreeNode::new_leaf(page_id);
        node.keys.push_back(BTreeKey::new(vec![1, 2, 3]));
        node.values.push_back(BTreeValue::new(vec![7, 8, 9]));

        let mut page = Page::new(page_id);
        BTreeNode::to_page(&node, &mut page)?;
        page.data[0] = 0xFF;

        let err = BTreeNode::from_page_decoded(page).unwrap_err();
        assert!(
            err.to_string().contains("Unknown v3 page kind byte")
                || err.to_string().contains("Unsupported B-tree page kind")
        );

        Ok(())
    }

    #[test]
    fn test_btree_page_rejects_invalid_payload_length() -> Result<()> {
        let page_id = 2;
        let mut node = BTreeNode::new_leaf(page_id);
        node.keys.push_back(BTreeKey::new(vec![1, 2, 3]));
        node.values.push_back(BTreeValue::new(vec![7, 8, 9]));

        let mut page = Page::new(page_id);
        BTreeNode::to_page(&node, &mut page)?;

        // Corrupt cell-content start so the pointer area overlaps it.
        page.data[5..7].copy_from_slice(&1u16.to_be_bytes());
        let err = BTreeNode::from_page_decoded(page).unwrap_err();
        assert!(err
            .to_string()
            .contains("pointer area overlaps cell content"));

        // Keep a guard that the header offset contract is stable.
        assert_eq!(BTREE_PAGE_HEADER_SIZE, 8);
        assert_eq!(DATABASE_HEADER_SIZE, 100);

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
            left.keys.push_back(BTreeKey::new(i.to_le_bytes().to_vec()));
            left.values.push_back(BTreeValue::new(vec![1u8; 200]));
        }
        for i in 10u32..20u32 {
            right
                .keys
                .push_back(BTreeKey::new(i.to_le_bytes().to_vec()));
            right.values.push_back(BTreeValue::new(vec![2u8; 200]));
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

        left.keys.push_back(BTreeKey::new(vec![1]));
        left.values.push_back(BTreeValue::new(vec![11]));
        right.keys.push_back(BTreeKey::new(vec![2]));
        right.values.push_back(BTreeValue::new(vec![22]));

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
        leaf.keys.push_back(BTreeKey::new(vec![2]));
        leaf.values.push_back(BTreeValue::new(vec![20]));
        leaf.keys.push_back(BTreeKey::new(vec![1]));
        leaf.values.push_back(BTreeValue::new(vec![10]));

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
        left_leaf.keys.push_back(BTreeKey::new(vec![1]));
        left_leaf.values.push_back(BTreeValue::new(vec![11]));

        let mut right_left_leaf = BTreeNode::new_leaf(right_left_leaf_page);
        right_left_leaf.keys.push_back(BTreeKey::new(vec![6]));
        right_left_leaf.values.push_back(BTreeValue::new(vec![66]));

        let mut right_right_leaf = BTreeNode::new_leaf(right_right_leaf_page);
        right_right_leaf.keys.push_back(BTreeKey::new(vec![9]));
        right_right_leaf.values.push_back(BTreeValue::new(vec![99]));

        let mut right_internal = BTreeNode::new_internal(right_internal_page);
        right_internal.keys.push_back(BTreeKey::new(vec![8]));
        right_internal.children.push_back(right_left_leaf_page);
        right_internal.children.push_back(right_right_leaf_page);

        let mut root_node = BTreeNode::new_internal(root);
        root_node.keys.push_back(BTreeKey::new(vec![5]));
        root_node.children.push_back(left_leaf_page);
        root_node.children.push_back(right_internal_page);

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
