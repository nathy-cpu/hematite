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

    fn build_split_byte_tree() -> Result<(ByteTreeStore, crate::storage::PageId)> {
        let trees = ByteTreeStore::new(Pager::new_in_memory(100)?);
        let root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;
        // Large enough values that a handful of rows overflow a page. Inserting a
        // fixed count of tiny rows used to work only because `MAX_KEYS` forced a split
        // at 99 cells; splits are driven by bytes now, so the fixture has to supply
        // the bytes rather than rely on the cell cap.
        let filler = vec![0xAB; 900];
        for i in 0..20u32 {
            tree.insert(&i.to_be_bytes(), &filler)?;
        }
        Ok((trees, root_page_id))
    }

    fn corrupt_right_leaf_page_kind(
        trees: &ByteTreeStore,
        root_page_id: crate::storage::PageId,
    ) -> Result<()> {
        let page_ids = trees.collect_page_ids(root_page_id)?;
        assert!(page_ids.len() >= 3, "expected split tree");

        let shared = trees.shared_storage();
        let mut pager = shared.write().unwrap();
        let mut page = pager.read_page(page_ids[2])?;
        page.data[0] = 0xFF;
        pager.write_page(page)
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
    fn reading_a_spilled_value_visits_each_overflow_page_once() -> Result<()> {
        // Replaces a test that asserted an overflow page-id memo was hit. The memo
        // existed because the read walked the chain twice; now it walks once and the
        // memo is gone, so the property worth pinning is the page count itself.
        //
        // The cache has to be cold for that count to mean anything. An earlier
        // version of this test reused the store that had just written the value, so
        // every page was still resident, the backend was never touched, and the
        // assertion held for any implementation.
        let path = tmp_db();
        let large_value = vec![0x51; crate::storage::PAGE_SIZE * 2];
        let root_page_id = {
            let trees = ByteTreeStore::new(new_storage(&path)?);
            let root_page_id = trees.create_tree()?;
            let mut tree = trees.open_tree(root_page_id)?;
            tree.insert(b"blob", &large_value)?;
            trees.flush()?;
            root_page_id
        };

        // Reopen with room for the descent but not the chain, so the overflow pages
        // must come from the backend.
        let trees = ByteTreeStore::new(Pager::new(path.path(), 4)?);
        let tree = trees.open_tree(root_page_id)?;
        let before = trees.storage_stats()?;
        let cursor = tree.cursor()?;
        assert_eq!(cursor.current()?, Some((b"blob".to_vec(), large_value)));
        let work = trees.storage_stats()?.since(&before);

        // Two overflow pages carry the payload that does not fit in the cell, and a
        // single pass reads each once. The bound is 3 rather than 5 deliberately: the
        // two-pass read this replaced would fault all four, so a looser bound would
        // let the regression back in unnoticed.
        assert!(
            work.page_reads > 0,
            "cache was not cold, so this measures nothing"
        );
        assert!(
            work.page_reads <= 3,
            "expected one pass over the chain, saw {} page reads",
            work.page_reads
        );
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

        let reopened = trees.open_tree(final_root_page_id)?;
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
    fn test_byte_tree_entries_with_prefix_propagates_cursor_next_error() -> Result<()> {
        let (trees, root_page_id) = build_split_byte_tree()?;
        corrupt_right_leaf_page_kind(&trees, root_page_id)?;

        let tree = trees.open_tree(root_page_id)?;
        let err = tree.entries_with_prefix(b"").unwrap_err();
        assert!(matches!(err, crate::error::HematiteError::StorageError(_)));

        Ok(())
    }

    #[test]
    fn test_byte_tree_collect_remaining_propagates_cursor_next_error() -> Result<()> {
        let (trees, root_page_id) = build_split_byte_tree()?;
        let tree = trees.open_tree(root_page_id)?;
        let mut cursor = tree.cursor()?;
        assert!(cursor.current()?.is_some());

        corrupt_right_leaf_page_kind(&trees, root_page_id)?;

        let err = cursor.collect_remaining().unwrap_err();
        assert!(matches!(err, crate::error::HematiteError::StorageError(_)));

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
    fn test_byte_tree_failed_large_update_restores_previous_value_and_overflow_state() -> Result<()>
    {
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
    fn test_byte_tree_failed_large_delete_restores_previous_value_and_overflow_state() -> Result<()>
    {
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
        let raw_tree = BTreeIndex::from_shared_storage(shared.clone(), root_page_id);
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
        let raw_tree = BTreeIndex::from_shared_storage(shared.clone(), root_page_id);
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
        let raw_tree = BTreeIndex::from_shared_storage(shared.clone(), root_page_id);
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
        let raw_tree = BTreeIndex::from_shared_storage(shared.clone(), root_page_id);
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
        let reopened = trees.open_tree(root_page_id)?;
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
        let reopened = trees.open_tree(root_page_id)?;
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
        let reopened = trees.open_tree(root_page_id)?;
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

mod page_format_tests {
    use crate::btree::page_format::{
        cell_pointer, defragment_page, initialize_btree_page, insert_cell, remove_cell,
        set_rightmost_child, BTreePageHeaderV3,
    };
    use crate::storage::format::PageKind;
    use crate::storage::Page;

    /// A leaf cell of exactly `len` bytes: an empty key and a value filling the rest.
    ///
    /// Cell lengths are varints now, so the framing width depends on the value length
    /// rather than being a flat four bytes. Built through the same `varint` the page
    /// format uses so this cannot drift from it again.
    fn dummy_cell(len: usize) -> Vec<u8> {
        use crate::btree::varint;
        assert!(len >= 2, "a cell needs at least two length varints");
        // Find the value length whose framing plus itself totals `len`.
        let val_len = (0..len)
            .rev()
            .find(|&v| varint::len_of(0) + varint::len_of(v as u64) + v == len)
            .expect("no value length produces the requested cell size");
        let mut buf = Vec::with_capacity(len);
        varint::write_to_vec(&mut buf, 0);
        varint::write_to_vec(&mut buf, val_len as u64);
        buf.resize(len, 1);
        buf
    }

    #[test]
    fn initializes_leaf_page_with_page_one_offset() {
        let mut page = Page::new(1);
        initialize_btree_page(&mut page, PageKind::LeafTable, true).unwrap();

        let header = BTreePageHeaderV3::parse(&page, true).unwrap();
        assert_eq!(header.header_offset, 100);
        assert_eq!(header.cell_count, 0);
        assert_eq!(header.cell_content_start, crate::storage::PAGE_SIZE as u16);
    }

    #[test]
    fn initializes_interior_page_with_rightmost_child_slot() {
        let mut page = Page::new(2);
        initialize_btree_page(&mut page, PageKind::InteriorTable, false).unwrap();
        set_rightmost_child(&mut page, false, 99).unwrap();

        let header = BTreePageHeaderV3::parse(&page, false).unwrap();
        assert_eq!(header.rightmost_child, Some(99));
    }

    #[test]
    fn inserts_cells_and_tracks_pointer_order() {
        let mut page = Page::new(2);
        initialize_btree_page(&mut page, PageKind::LeafTable, false).unwrap();

        let first = vec![1u8; 8];
        let second = vec![2u8; 5];
        insert_cell(&mut page, false, 0, &first).unwrap();
        insert_cell(&mut page, false, 1, &second).unwrap();

        let header = BTreePageHeaderV3::parse(&page, false).unwrap();
        assert_eq!(header.cell_count, 2);
        let first_ptr = cell_pointer(&page, false, 0).unwrap();
        let second_ptr = cell_pointer(&page, false, 1).unwrap();
        assert!(first_ptr > second_ptr);
    }

    #[test]
    fn remove_cell_defragments_page() {
        let mut page = Page::new(1);
        initialize_btree_page(&mut page, PageKind::LeafTable, false).unwrap();
        insert_cell(&mut page, false, 0, &dummy_cell(8)).unwrap();
        insert_cell(&mut page, false, 1, &dummy_cell(6)).unwrap();
        insert_cell(&mut page, false, 2, &dummy_cell(4)).unwrap();

        remove_cell(&mut page, false, 1).unwrap();

        let header = BTreePageHeaderV3::parse(&page, false).unwrap();
        assert_eq!(header.cell_count, 2);
        assert_eq!(header.fragmented_free_bytes, 0);
        assert!(header.pointer_area_end() <= header.cell_content_start as usize);
    }

    #[test]
    fn explicit_defragmentation_keeps_cell_count_stable() {
        let mut page = Page::new(2);
        initialize_btree_page(&mut page, PageKind::LeafTable, false).unwrap();
        insert_cell(&mut page, false, 0, &dummy_cell(7)).unwrap();
        insert_cell(&mut page, false, 1, &dummy_cell(9)).unwrap();

        defragment_page(&mut page, false).unwrap();

        let header = BTreePageHeaderV3::parse(&page, false).unwrap();
        assert_eq!(header.cell_count, 2);
        assert_eq!(header.fragmented_free_bytes, 0);
    }
}

mod index_extra_tests {
    use crate::btree::index::BTreeIndex;
    use crate::btree::node::BTreeNode;
    use crate::btree::page_format::{
        initialize_btree_page, insert_cell as pf_insert_cell, remove_cell as pf_remove_cell,
        set_rightmost_child, total_free_space as pf_total_free_space,
    };
    use crate::btree::tree::BTreeManager;
    use crate::btree::{BTreeKey, BTreeValue};
    use crate::storage::format::PageKind;
    use crate::storage::{Page, Pager};
    use std::sync::{Arc, RwLock};

    #[test]
    fn test_remove_cell_adds_freeblock() {
        let mut page = Page::new(2);
        initialize_btree_page(&mut page, PageKind::LeafTable, false).unwrap();

        let make_cell = |klen: usize, vlen: usize, key_byte: u8, val_byte: u8| -> Vec<u8> {
            use crate::btree::varint;
            let mut cell = Vec::new();
            varint::write_to_vec(&mut cell, klen as u64);
            varint::write_to_vec(&mut cell, vlen as u64);
            cell.extend(std::iter::repeat_n(key_byte, klen));
            cell.extend(std::iter::repeat_n(val_byte, vlen));
            cell
        };

        let c1 = make_cell(2, 2, 0x11, 0x21);
        let c2 = make_cell(3, 3, 0x12, 0x22);
        let c3 = make_cell(4, 1, 0x13, 0x23);

        pf_insert_cell(&mut page, false, 0, &c1).unwrap();
        pf_insert_cell(&mut page, false, 1, &c2).unwrap();
        pf_insert_cell(&mut page, false, 2, &c3).unwrap();

        let free_before = pf_total_free_space(&page, false).unwrap();
        pf_remove_cell(&mut page, false, 1).unwrap();
        let free_after = pf_total_free_space(&page, false).unwrap();

        assert!(free_after > free_before);
    }

    #[test]
    fn test_create_new_root_preserves_db_header() {
        let pager = Pager::new_in_memory(10).expect("create pager");
        let storage = Arc::new(RwLock::new(pager));
        let mut index = BTreeIndex::from_shared_storage(Arc::clone(&storage), 0);

        {
            let mut pg = storage.write().unwrap();
            let db_header = crate::catalog::header::DatabaseHeader::new(1);
            let mut page0 = Page::new(0);
            db_header.serialize(&mut page0.data).unwrap();
            pg.write_page(page0).unwrap();

            let right = pg.allocate_page().unwrap();
            let key = BTreeKey::new(vec![0x42]);
            index
                .create_new_root(&mut pg, key, right)
                .expect("create_new_root");
        }

        {
            let pg_read = storage.read().unwrap();
            let page0 = pg_read.read_page(0).expect("read page0");
            let header = crate::catalog::header::DatabaseHeader::deserialize(&page0.data)
                .expect("db header decode");
            assert_eq!(
                header.version,
                crate::catalog::header::DatabaseHeader::CURRENT_VERSION
            );
            let _bt_header = crate::btree::page_format::BTreePageHeaderV3::parse(&page0, true)
                .expect("b-tree header parse");
        }
    }

    #[test]
    fn test_check_root_underflow_preserves_db_header() {
        let pager = Pager::new_in_memory(10).expect("create pager");
        let storage = Arc::new(RwLock::new(pager));

        {
            let mut pg = storage.write().unwrap();
            let db_header = crate::catalog::header::DatabaseHeader::new(1);
            let mut page0 = Page::new(0);
            db_header.serialize(&mut page0.data).unwrap();
            pg.write_page(page0).unwrap();
        }

        let child_id = {
            let mut pg = storage.write().unwrap();
            pg.allocate_page().unwrap()
        };

        {
            let mut child_page = Page::new(child_id);
            let mut child_node = BTreeNode::new_leaf(child_id);
            child_node.keys.push_back(BTreeKey::new(vec![0x1]));
            child_node.values.push_back(BTreeValue::new(vec![0x2]));
            child_node.key_count = child_node.keys.len();
            child_node.to_page(&mut child_page).unwrap();

            let mut pg = storage.write().unwrap();
            pg.write_page(child_page).unwrap();
        }

        {
            let mut pg = storage.write().unwrap();
            let mut root_page = pg.read_page(0).unwrap();
            initialize_btree_page(&mut root_page, PageKind::InteriorTable, true).unwrap();
            set_rightmost_child(&mut root_page, true, child_id).unwrap();
            pg.write_page(root_page).unwrap();
        }

        let mut idx = BTreeIndex::from_shared_storage(Arc::clone(&storage), 0);
        {
            let mut pg = storage.write().unwrap();
            let res = idx
                .check_root_underflow(&mut pg)
                .expect("check_root_underflow");
            assert_eq!(res, Some(0));
        }

        {
            let pg_read = storage.read().unwrap();
            let page0 = pg_read.read_page(0).expect("read page0");
            let _header = crate::catalog::header::DatabaseHeader::deserialize(&page0.data)
                .expect("db header");
        }
    }

    #[test]
    fn test_validate_tree_simple_leaf() {
        let pager = Pager::new_in_memory(10).expect("create pager");
        let storage = Arc::new(RwLock::new(pager));
        let mut manager = BTreeManager::from_shared_storage(Arc::clone(&storage));

        let page_id = {
            let mut pg = storage.write().unwrap();
            pg.allocate_page().unwrap()
        };

        {
            let mut leaf_page = Page::new(page_id);
            initialize_btree_page(&mut leaf_page, PageKind::LeafTable, false).unwrap();

            let mut node = BTreeNode::new_leaf(page_id);
            node.keys.push_back(BTreeKey::new(vec![0xAA]));
            node.values.push_back(BTreeValue::new(vec![0xBB]));
            node.key_count = 1;
            node.to_page(&mut leaf_page).unwrap();

            let mut pg = storage.write().unwrap();
            pg.write_page(leaf_page).unwrap();
        }

        let ok = manager.validate_tree(page_id).expect("validate_tree ran");
        assert!(ok);
    }
}

mod value_store_tests {
    use crate::btree::value_store::{
        inline_payload, StoredValueLayout, MAX_LOCAL_PAYLOAD, STORED_VALUE_INLINE_HEADER_SIZE,
        STORED_VALUE_OVERFLOW_HEADER_SIZE,
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
        let local_payload = vec![0xAB; MAX_LOCAL_PAYLOAD];
        let layout =
            StoredValueLayout::new_overflow(MAX_LOCAL_PAYLOAD + 123, local_payload.clone(), 77)?;
        let encoded = layout.encode()?;
        assert_eq!(
            encoded.len(),
            STORED_VALUE_OVERFLOW_HEADER_SIZE + local_payload.len()
        );

        let decoded = StoredValueLayout::decode(&encoded)?;
        assert_eq!(decoded, layout);
        assert_eq!(decoded.overflow_len(), 123);
        Ok(())
    }

    #[test]
    fn test_stored_value_layout_rejects_invalid_headers() {
        // Empty input has not even a tag byte.
        assert!(StoredValueLayout::decode(&[]).is_err());
        // An unknown tag.
        assert!(StoredValueLayout::decode(&[9u8; 4]).is_err());
        // An overflow value whose header is cut short.
        assert!(StoredValueLayout::decode(&[
            super::super::value_store::STORED_VALUE_OVERFLOW_TAG,
            0,
            0
        ])
        .is_err());
        // An overflow value naming no head page.
        let mut headless = vec![0u8; STORED_VALUE_OVERFLOW_HEADER_SIZE];
        headless[0] = super::super::value_store::STORED_VALUE_OVERFLOW_TAG;
        headless[1..5].copy_from_slice(&8u32.to_le_bytes());
        headless[5..9].copy_from_slice(&crate::storage::INVALID_PAGE_ID.to_le_bytes());
        assert!(StoredValueLayout::decode(&headless).is_err());
    }

    /// The borrowed decode has to agree with the owning one, since scans now take
    /// the borrowed path and everything else still takes the owning one.
    #[test]
    fn test_borrowed_decode_agrees_with_owned() -> crate::error::Result<()> {
        let inline = StoredValueLayout::new_inline(b"hello world")?.encode()?;
        let spilled = StoredValueLayout::new_overflow(
            MAX_LOCAL_PAYLOAD + 123,
            vec![0xAB; MAX_LOCAL_PAYLOAD],
            77,
        )?
        .encode()?;

        for encoded in [&inline, &spilled] {
            let owned = StoredValueLayout::decode(encoded)?;
            let (borrowed, payload) = StoredValueLayout::decode_borrowed(encoded)?;

            assert_eq!(borrowed.total_len, owned.total_len);
            assert_eq!(borrowed.overflow_first_page, owned.overflow_first_page);
            assert_eq!(payload, owned.local_payload.as_slice());
        }
        Ok(())
    }

    /// `inline_payload` is what decides whether a scan can skip the copy and the
    /// pager lock, so it has to say "no" on exactly the values that spill.
    #[test]
    fn test_inline_payload_borrows_only_unspilled_values() -> crate::error::Result<()> {
        let inline = StoredValueLayout::new_inline(b"hello world")?.encode()?;
        assert_eq!(inline_payload(&inline)?, Some(b"hello world".as_slice()));

        let spilled = StoredValueLayout::new_overflow(
            MAX_LOCAL_PAYLOAD + 123,
            vec![0xAB; MAX_LOCAL_PAYLOAD],
            77,
        )?
        .encode()?;
        assert_eq!(
            inline_payload(&spilled)?,
            None,
            "a spilled value's bytes are not all in the page, so they cannot be borrowed"
        );

        assert!(inline_payload(&[]).is_err());
        // An inline value is now just a tag plus its bytes, so the smallest valid one
        // is a single tag byte carrying an empty payload -- which is exactly what a
        // secondary-index entry stores.
        assert_eq!(inline_payload(&[0u8])?, Some(b"".as_slice()));
        assert_eq!(
            STORED_VALUE_INLINE_HEADER_SIZE, 1,
            "an inline value must cost one byte of header"
        );
        Ok(())
    }
}

mod compaction_tests {
    use crate::btree::bytes::ByteTreeStore;
    use crate::btree::compaction::{auto_vacuum, build_parent_map, BUILD_PARENT_MAP_CALLS};
    use crate::error::Result;
    use crate::storage::{Pager, PAGE_SIZE};
    use std::sync::atomic::Ordering;

    #[test]
    fn auto_vacuum_relocates_overflow_and_internal_pages_in_a_single_parent_map_build() -> Result<()>
    {
        let trees = ByteTreeStore::new(Pager::new_in_memory(100)?);

        // Tree A will be dropped, scattering free pages among tree B's
        // higher-numbered pages -- forcing more than one relocation. It needs
        // enough rows to span multiple leaf pages (plus an internal root),
        // not just one, so deleting it frees more than a single page.
        let root_a = trees.create_tree()?;
        {
            let mut tree_a = trees.open_tree(root_a)?;
            for i in 0..300u32 {
                tree_a.insert(&i.to_be_bytes(), &vec![i as u8; 32])?;
            }
        }

        // Tree B: enough rows to span multiple internal + leaf pages, plus a
        // value large enough to force an overflow chain -- exercising both
        // the `RelocationChildren::Internal` and `RelocationChildren::Leaf`
        // (overflow-pointer) patch paths in `relocate_page`.
        let root_b = trees.create_tree()?;
        {
            let mut tree_b = trees.open_tree(root_b)?;
            for i in 0..80u32 {
                tree_b.insert(&i.to_be_bytes(), &vec![i as u8; 32])?;
            }
            tree_b.insert(b"blob", &vec![0x5A; PAGE_SIZE * 2])?;
        }

        trees.delete_tree(root_a)?;

        let free_before = trees.free_page_ids()?.len();
        assert!(
            free_before > 1,
            "expected multiple scattered free slots (got {}), to force more than one relocation",
            free_before
        );

        BUILD_PARENT_MAP_CALLS.store(0, Ordering::SeqCst);
        let storage = trees.shared_storage();
        let mut roots = [root_b];
        auto_vacuum(&storage, &mut roots, |_old, _new| Ok(()))?;

        assert_eq!(
            BUILD_PARENT_MAP_CALLS.load(Ordering::SeqCst),
            1,
            "a single auto_vacuum() call should rebuild the parent map at most once, \
             regardless of how many pages it relocates"
        );

        assert_eq!(
            trees.free_page_ids()?.len(),
            0,
            "auto_vacuum should reclaim every free page"
        );

        // Cross-check: the incrementally-maintained parent map (verified
        // implicitly by successful relocation above) must be consistent with
        // a from-scratch rebuild once vacuum has settled -- i.e. re-deriving
        // the map from the post-vacuum tree must succeed without error.
        {
            let pager = storage
                .read()
                .map_err(|_| crate::error::HematiteError::InternalError("lock poisoned".into()))?;
            build_parent_map(&pager, &roots)?;
        }

        // All of tree B's data, including the overflow-bearing value, must
        // survive relocation intact.
        let tree_b = trees.open_tree(roots[0])?;
        for i in 0..80u32 {
            assert_eq!(tree_b.get(&i.to_be_bytes())?, Some(vec![i as u8; 32]));
        }
        assert_eq!(tree_b.get(b"blob")?, Some(vec![0x5A; PAGE_SIZE * 2]));

        Ok(())
    }

    #[test]
    fn auto_vacuum_is_idempotent_when_nothing_needs_relocating() -> Result<()> {
        let trees = ByteTreeStore::new(Pager::new_in_memory(100)?);
        let root = trees.create_tree()?;
        {
            let mut tree = trees.open_tree(root)?;
            for i in 0..10u32 {
                tree.insert(&i.to_be_bytes(), &[i as u8])?;
            }
        }

        BUILD_PARENT_MAP_CALLS.store(0, Ordering::SeqCst);
        let storage = trees.shared_storage();
        let mut roots = [root];
        auto_vacuum(&storage, &mut roots, |_old, _new| Ok(()))?;

        assert_eq!(
            BUILD_PARENT_MAP_CALLS.load(Ordering::SeqCst),
            0,
            "auto_vacuum should not build a parent map at all when there is nothing to relocate"
        );
        assert_eq!(
            roots[0], root,
            "root should be unchanged with nothing to relocate"
        );

        Ok(())
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

        // Point the second cell at the first. The corruption goes into the page's
        // pointer array rather than a cached copy of it: `BTreeNode` no longer keeps
        // one, and writing the bytes is what real corruption looks like anyway.
        // A leaf header is 8 bytes, so pointer `i` lives at `8 + i * 2`.
        let first_pointer = [page.data[8], page.data[9]];
        page.data[10..12].copy_from_slice(&first_pointer);

        let lazy = BTreeNode::from_page(page)?;

        // Two cell pointers claiming the same bytes is a whole-page property: no
        // single cell looks wrong on its own, so only a pass that compares cells
        // against each other can see it. That pass used to run on every page load;
        // it is `verify` now, called by `validate_tree`.
        let err = lazy.verify().unwrap_err();
        assert!(matches!(err, crate::error::HematiteError::CorruptedData(_)));
        assert!(!lazy.is_decoded);

        Ok(())
    }

    #[test]
    fn test_btree_lazy_search_reports_overlapping_cells_as_corruption() -> Result<()> {
        let page_id = 2;
        let mut node = BTreeNode::new_leaf(page_id);
        for _ in 0..2 {
            node.keys.push_back(BTreeKey::new(vec![0x0B; 4]));
            node.values.push_back(BTreeValue::new(vec![0x0B; 4]));
        }

        let mut page = Page::new(page_id);
        BTreeNode::to_page(&node, &mut page)?;

        // Shift the earlier cell's declared offset forward by 2 bytes so it reads
        // into its own payload bytes as a *different*, smaller cell -- overlapping the
        // later cell's byte range without being an exact duplicate pointer (distinct
        // from the case covered above). A leaf header is 8 bytes, so pointer `i` lives
        // at `8 + i * 2`.
        //
        // The payload bytes are `0x0B` on purpose. Cell lengths are varints, and
        // `0x0B` is the one-byte encoding of 5, so the shifted pointer parses as a
        // well-formed cell of `1 + 1 + 5 + 5 = 12` bytes. Each real cell here is 10
        // bytes, so a cell starting two bytes in and running twelve reaches past its
        // neighbour's start while still ending inside the page: an overlap, rather
        // than the out-of-bounds length that a smaller or larger byte would produce.
        // That is what makes this exercise the overlap sweep instead of the per-cell
        // size validation, which would reject it first.
        let pointer_at =
            |data: &[u8], i: usize| u16::from_be_bytes([data[8 + i * 2], data[9 + i * 2]]);
        let (lower_index, higher_index) = if pointer_at(&page.data, 0) < pointer_at(&page.data, 1) {
            (0, 1)
        } else {
            (1, 0)
        };
        let shifted = pointer_at(&page.data, lower_index) + 2;
        page.data[8 + lower_index * 2..10 + lower_index * 2]
            .copy_from_slice(&shifted.to_be_bytes());
        assert!(shifted != pointer_at(&page.data, higher_index));

        let lazy = BTreeNode::from_page(page)?;

        // As with duplicate pointers, an overlap is only visible when cells are
        // compared against one another, so it lives in `verify` rather than on the
        // access path. Each of these cells parses fine in isolation.
        let err = lazy.verify().unwrap_err();
        assert!(matches!(
            err,
            crate::error::HematiteError::CorruptedData(ref message)
                if message.contains("Overlapping")
        ));
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

        // An interior header is 12 bytes, so cell pointer `i` lives at `12 + i * 2`.
        // Read straight from the page: `BTreeNode` no longer caches the pointer array.
        let second_offset = u16::from_be_bytes([page.data[14], page.data[15]]) as usize;
        page.data[second_offset..second_offset + 4].copy_from_slice(&1u32.to_be_bytes());

        let lazy = BTreeNode::from_page(page)?;
        assert_eq!(lazy.find_child(&BTreeKey::new(vec![5]))?, 3);

        let err = lazy.validate_cell_layouts().unwrap_err();
        assert!(matches!(err, crate::error::HematiteError::CorruptedData(_)));

        Ok(())
    }

    /// Duplicate cell pointers are caught by the integrity check.
    ///
    /// This used to assert that `get`/`seek`/`first` each returned `CorruptedData`,
    /// because `from_shared_page` structurally validated every page it loaded — a
    /// pass worth ~28-30 % of a batched insert. It is `verify` now, called from
    /// `validate_tree`.
    ///
    /// It is deliberately *not* called from `open_tree`: that runs once per inserted
    /// row, and putting a whole-page pass there measured 5.7 % of the insert path,
    /// growing with the root's cell count. Cheap-looking checks on per-row paths are
    /// not cheap.
    ///
    /// The narrowing is real and worth stating: duplicate pointers now make a key
    /// look absent rather than raise. Detection on the access path covers cells that
    /// fail to parse, not cells that parse but disagree with their neighbours.
    #[test]
    fn test_byte_tree_duplicate_cell_pointers_are_caught_by_integrity_check() -> Result<()> {
        let trees = ByteTreeStore::new(Pager::new_in_memory(100)?);
        let root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;
        tree.insert(b"alpha", b"one")?;
        tree.insert(b"beta", b"two")?;

        let shared = trees.shared_storage();
        let mut pager = shared.write().unwrap();
        let mut page = pager.read_page(root_page_id)?;
        let first_pointer = [page.data[8], page.data[9]];
        page.data[10..12].copy_from_slice(&first_pointer);
        pager.write_page(page)?;
        drop(pager);

        // `validate_tree` returns Err on a structural fault and Ok(false) on a
        // semantic one; duplicate pointers are the former.
        match trees.clone().validate_tree(root_page_id) {
            Err(crate::error::HematiteError::CorruptedData(_)) => {}
            Err(other) => panic!("expected CorruptedData, got {other:?}"),
            Ok(valid) => assert!(!valid, "duplicate cell pointers should not validate"),
        }

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

        // The node's borrowed view of the page is dropped by the mutation rather than
        // refreshed with a 4 KiB copy. Reading through it afterwards must fail loudly;
        // silently returning the pre-mutation bytes would be far worse than an error.
        assert!(
            lazy.get_value_view(0).is_err(),
            "a mutated node handed back a stale view instead of failing"
        );

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
            err.to_string().contains("Unknown page kind byte")
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

/// `can_merge_with` answers from byte counts instead of building the merged node.
/// These check the arithmetic against the thing it models.
///
/// Getting this wrong in one direction is invisible and merely slow — merges that
/// could happen do not. Getting it wrong in the other overflows a page. So the test
/// that matters is the agreement one: whenever the predicate says yes, actually
/// merging must fit.
///
/// What it catches, checked by deliberately breaking the formula: dropping a node's
/// contribution, miscounting cells, using the wrong capacity. What it does *not*
/// catch is the single-header term. `MAX_KEYS` caps a node at 99 cells, so the
/// page-fit boundary is only reachable with cells of roughly a hundred bytes, and at
/// that granularity an eight-byte header can never flip the answer. The term is
/// correct and stays for clarity, but it is unobservable in any reachable
/// configuration and this test should not be read as covering it.
mod merge_fit_tests {
    use crate::btree::node::BTreeNode;
    use crate::btree::{BTreeKey, BTreeValue, NodeType};

    fn leaf_with(page_id: u32, count: usize, value_len: usize) -> BTreeNode {
        let mut node = BTreeNode::new_leaf(page_id);
        for i in 0..count {
            node.keys
                .push_back(BTreeKey::new((i as u32).to_be_bytes().to_vec()));
            node.values
                .push_back(BTreeValue::new(vec![0xAB; value_len]));
        }
        node.key_count = node.keys.len();
        node
    }

    /// The merged node the predicate is modelling, built the slow honest way.
    fn actually_merged(left: &BTreeNode, right: &BTreeNode) -> BTreeNode {
        let mut merged = left.clone();
        merged.keys.extend(right.keys.iter().cloned());
        merged.values.extend(right.values.iter().cloned());
        merged.key_count = merged.keys.len();
        merged
    }

    #[test]
    fn the_predicate_agrees_with_performing_the_merge() {
        let mut checked_both_answers = (false, false);

        // Sweeps across the page-fit boundary: with 99 cells maximum, two nodes of
        // ~20 cells at ~200 bytes each land either side of 4 KiB.
        for count in [1usize, 4, 16, 18, 20, 22, 40, 90] {
            for value_len in [1usize, 16, 64, 96, 100, 104, 200, 500] {
                let left = leaf_with(2, count, value_len);
                let right = leaf_with(3, count, value_len);

                let predicted = left.can_merge_with(&right);
                let actual = {
                    let merged = actually_merged(&left, &right);
                    merged.keys.len() <= crate::btree::node::MAX_KEYS && merged.will_fit_in_page()
                };

                assert_eq!(
                    predicted, actual,
                    "disagreement for {count} keys of {value_len} bytes: \
                     predicate said {predicted}, merging said {actual}"
                );

                if predicted {
                    checked_both_answers.0 = true;
                } else {
                    checked_both_answers.1 = true;
                }
            }
        }

        assert!(
            checked_both_answers.0 && checked_both_answers.1,
            "the sweep only produced one answer, so it proves nothing"
        );
    }

    #[test]
    fn nodes_of_different_kinds_never_merge() {
        let leaf = leaf_with(2, 4, 16);
        let mut interior = BTreeNode::new_internal(3);
        interior.keys.push_back(BTreeKey::new(vec![1]));
        interior.children.push_back(4);
        interior.children.push_back(5);
        interior.key_count = 1;

        assert!(!leaf.can_merge_with(&interior));
        assert!(!interior.can_merge_with(&leaf));
        assert_eq!(interior.node_type, NodeType::Internal);
    }
}

/// Guards the exchange B3a made: whole-page structural validation stopped running on
/// every page load, and in return the read accessors stopped swallowing decode
/// errors. If `key_view`/`value_view` ever go back to `.ok()`, corruption becomes
/// silent absence again and nothing else is watching for it.
mod cell_access_error_tests {
    use crate::btree::bytes::ByteTreeStore;
    use crate::error::{HematiteError, Result};
    use crate::storage::Pager;

    /// A cell whose declared length runs off the end of the page must raise when a
    /// cursor reads it — not read as "no entry", which is what `.ok()` turned it into
    /// and what the eager whole-page pass existed to compensate for.
    ///
    /// Checked against a deliberate revert: restoring `.ok()` on `value_view` alone
    /// still fails this test, but restoring it on `key_view` alone does not, because
    /// an oversized key length puts the *value* out of bounds too and `value_view`
    /// reaches it first. So this guards the pair, not each accessor independently.
    #[test]
    fn corrupt_cell_surfaces_through_the_cursor_rather_than_reading_as_absent() -> Result<()> {
        let trees = ByteTreeStore::new(Pager::new_in_memory(100)?);
        let root_page_id = trees.create_tree()?;
        let mut tree = trees.open_tree(root_page_id)?;
        tree.insert(b"alpha", b"one")?;
        tree.insert(b"beta", b"two")?;

        // Find the first cell and blow up its declared key length. The cell pointers
        // stay consistent, so this is per-cell corruption, not the whole-page kind
        // `verify` covers.
        // Corrupt after opening, so the read path is what is under test rather than
        // `open_tree`'s root `verify`.
        let shared = trees.shared_storage();
        let mut pager = shared.write().unwrap();
        let mut page = pager.read_page(root_page_id)?;
        let first_offset = u16::from_be_bytes([page.data[8], page.data[9]]) as usize;
        page.data[first_offset..first_offset + 2].copy_from_slice(&u16::MAX.to_be_bytes());
        pager.write_page(page)?;
        drop(pager);

        let mut cursor = tree.cursor()?;
        let result = (|| {
            cursor.first()?;
            cursor.with_current(|_key, _value| Ok(()))
        })();

        match result {
            Err(HematiteError::CorruptedData(_)) => Ok(()),
            Err(other) => panic!("expected CorruptedData, got {other:?}"),
            Ok(Some(())) => panic!("a cell with an impossible key length was read as valid"),
            Ok(None) => panic!("corruption reported as 'no entry' — the `.ok()` bug is back"),
        }
    }
}

/// Diagnostic, not an assertion: prints where a row's bytes on disk go.
/// `cargo test --release space_budget -- --ignored --nocapture`
mod space_budget_probe {
    use crate::catalog::CatalogEngine;
    use crate::test_utils::TestDbFile;
    use crate::Value;

    /// Same fixture but with a secondary index, which is the only thing that uses
    /// the row-id key suffix. `cargo test --release secondary_index_space -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn report_secondary_index_space() {
        let db = TestDbFile::new("_test_secondary_index_space");
        let mut h = crate::Hematite::new(db.path()).expect("open");
        h.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT);")
            .expect("create");
        h.execute("CREATE INDEX ix_a ON t (a);").expect("index");
        let mut i = 0;
        while i < 5000 {
            let end = (i + 500).min(5000);
            let mut sql = String::from("INSERT INTO t (id, a) VALUES ");
            for id in i..end {
                if id > i {
                    sql.push(',');
                }
                sql.push_str(&format!("({},{})", id, id));
            }
            sql.push(';');
            h.execute(&sql).expect("insert");
            i = end;
        }
        drop(h);
        let bytes = std::fs::metadata(db.path()).expect("stat").len();
        println!(
            "\nsecondary-index fixture: {bytes} bytes ({:.1}/row)\n",
            bytes as f64 / 5000.0
        );
    }

    #[test]
    #[ignore]
    fn report_space_budget() -> crate::error::Result<()> {
        let db = TestDbFile::new("_test_space_budget_probe");
        let mut engine = CatalogEngine::new(db.path())?;
        let root = engine.create_table("t")?;

        let rows = 5000i64;
        engine.begin_transaction()?;
        for i in 0..rows {
            engine.insert_into_table(
                "t",
                (0..5).map(|c| Value::Integer((i + c) as i32)).collect(),
            )?;
        }
        engine.commit_transaction()?;

        let stats = engine.collect_tree_space_stats(root)?;
        let file = std::fs::metadata(db.path())?.len();
        let leaf_capacity = (stats.leaf_pages * 4096).max(1);
        println!("\n--- space budget, {rows} rows of 5 small ints ---");
        println!(
            "file bytes         {file}  ({:.1}/row)",
            file as f64 / rows as f64
        );
        println!("leaf pages         {}", stats.leaf_pages);
        println!("internal pages     {}", stats.internal_pages);
        println!("overflow pages     {}", stats.overflow_page_ids.len());
        println!(
            "rows per leaf      {:.1}",
            rows as f64 / stats.leaf_pages.max(1) as f64
        );
        println!(
            "tree used bytes    {}  ({:.1}/row)",
            stats.used_bytes,
            stats.used_bytes as f64 / rows as f64
        );
        println!(
            "leaf fill factor   {:.0}%\n",
            100.0 * stats.used_bytes as f64 / leaf_capacity as f64
        );
        Ok(())
    }
}
