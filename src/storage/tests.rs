//! Centralized tests for the storage module

mod buffer_pool_tests {
    use crate::storage::buffer_pool::*;
    use crate::storage::{Page, PageId};

    #[test]
    fn test_buffer_pool_basic_operations() {
        let mut pool = BufferPool::new(3);
        let page_id = PageId::new(1);
        let page = Page::new(page_id);

        // Test empty pool
        assert!(pool.get(page_id).is_none());
        assert_eq!(pool.len(), 0);

        // Test put and get
        pool.put(page.clone());
        assert!(pool.get(page_id).is_some());
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn test_buffer_pool_lru_eviction() {
        let mut pool = BufferPool::new(2);

        // Fill pool to capacity
        let page1 = Page::new(PageId::new(1));
        let page2 = Page::new(PageId::new(2));
        pool.put(page1.clone());
        pool.put(page2.clone());

        assert_eq!(pool.len(), 2);

        // Add third page (should evict first)
        let page3 = Page::new(PageId::new(3));
        pool.put(page3.clone());

        assert_eq!(pool.len(), 2);
        assert!(pool.get(PageId::new(1)).is_none()); // Evicted
        assert!(pool.get(PageId::new(2)).is_some()); // Still present
        assert!(pool.get(PageId::new(3)).is_some()); // New page
    }

    #[test]
    fn test_buffer_pool_lru_update() {
        let mut pool = BufferPool::new(3);

        let page1 = Page::new(PageId::new(1));
        let page2 = Page::new(PageId::new(2));
        let page3 = Page::new(PageId::new(3));

        // Add pages
        pool.put(page1);
        pool.put(page2);
        pool.put(page3);

        // Access page1 (should make it most recently used)
        pool.get(PageId::new(1));

        // Add page4 (should evict page2, not page1)
        let page4 = Page::new(PageId::new(4));
        pool.put(page4);

        assert!(pool.get(PageId::new(1)).is_some()); // Still present (accessed)
        assert!(pool.get(PageId::new(2)).is_none()); // Evicted (least recently used)
        assert!(pool.get(PageId::new(3)).is_some()); // Still present
        assert!(pool.get(PageId::new(4)).is_some()); // New page
    }

    #[test]
    fn test_buffer_pool_update_existing() {
        let mut pool = BufferPool::new(2);

        let page_id = PageId::new(1);
        let page1 = Page::new(page_id);
        let mut page2 = Page::new(page_id);
        page2.data[0] = 42; // Modified page

        // Add first page
        pool.put(page1);
        assert_eq!(pool.get(page_id).unwrap().data[0], 0);

        // Update with modified page
        pool.put(page2);
        assert_eq!(pool.get(page_id).unwrap().data[0], 42);
        assert_eq!(pool.len(), 1); // Still only one page
    }

    #[test]
    fn test_buffer_pool_remove() {
        let mut pool = BufferPool::new(3);

        let page1 = Page::new(PageId::new(1));
        let page2 = Page::new(PageId::new(2));

        pool.put(page1);
        pool.put(page2);

        assert_eq!(pool.len(), 2);

        // Remove page1
        pool.remove(PageId::new(1));
        assert_eq!(pool.len(), 1);
        assert!(pool.get(PageId::new(1)).is_none());
        assert!(pool.get(PageId::new(2)).is_some());

        // Remove non-existent page
        pool.remove(PageId::new(999));
        assert_eq!(pool.len(), 1); // No change
    }

    #[test]
    fn test_buffer_pool_capacity_zero() {
        let mut pool = BufferPool::new(0);

        let page = Page::new(PageId::new(1));
        pool.put(page);

        // Pool should remain empty since capacity is 0
        assert_eq!(pool.len(), 0);
        assert!(pool.get(PageId::new(1)).is_none());
    }
}

mod freelist_tests {
    use crate::storage::free_list::FreeList;
    use crate::storage::PageId;

    #[test]
    fn test_freelist_push_is_idempotent() {
        let mut freelist = FreeList::new();
        freelist.push_free_page(PageId::new(10));
        freelist.push_free_page(PageId::new(10));
        assert_eq!(freelist.as_slice(), &[PageId::new(10)]);
    }

    #[test]
    fn test_freelist_compacts_trailing_pages() {
        let mut freelist = FreeList::new();
        freelist.push_free_page(PageId::new(8));
        freelist.push_free_page(PageId::new(9));
        freelist.push_free_page(PageId::new(11));
        freelist.push_free_page(PageId::new(12));

        let mut next_page_id = 13;
        freelist.compact_trailing_pages(&mut next_page_id, 2);

        assert_eq!(next_page_id, 11);
        assert_eq!(freelist.as_slice(), &[PageId::new(8), PageId::new(9)]);
    }

    #[test]
    fn test_freelist_metadata_count_validation() {
        let result = FreeList::deserialize_metadata_lines(
            FreeList::METADATA_VERSION,
            2,
            &vec!["freelist|8".to_string()],
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Freelist metadata count mismatch"));
    }
}

mod pager_tests {
    use crate::storage::pager::Pager;
    use crate::storage::Page;
    use crate::test_utils::TestDbFile;

    #[test]
    fn test_pager_write_is_buffered_until_flush() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_write_is_buffered");
        let mut pager = Pager::new(test_db.path(), 8)?;

        let page_id = pager.allocate_page()?;
        let mut page = Page::new(page_id);
        page.data[0] = 99;
        pager.write_page(page)?;

        // Without flush, dirty write should not be visible to a fresh pager.
        let mut before_flush = Pager::new(test_db.path(), 8)?;
        let on_disk_before = before_flush.read_page(page_id)?;
        assert_eq!(on_disk_before.data[0], 0);

        pager.flush()?;

        let mut after_flush = Pager::new(test_db.path(), 8)?;
        let on_disk_after = after_flush.read_page(page_id)?;
        assert_eq!(on_disk_after.data[0], 99);

        Ok(())
    }

    #[test]
    fn test_pager_flush_clears_dirty_tracking() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_flush_clears_dirty");
        let mut pager = Pager::new(test_db.path(), 8)?;
        let page_id = pager.allocate_page()?;

        let mut page = Page::new(page_id);
        page.data[0..4].copy_from_slice(&[1, 2, 3, 4]);
        pager.write_page(page)?;
        assert_eq!(pager.dirty_page_count(), 1);

        pager.flush()?;
        assert_eq!(pager.dirty_page_count(), 0);

        Ok(())
    }
}

mod database_tests {
    use crate::error::Result;
    use crate::storage::database::*;
    use crate::test_utils::TestDbFile;

    #[test]
    fn test_database_creation_and_close() -> Result<()> {
        let test_db = TestDbFile::new("_test_database");

        {
            let mut db = Database::open(test_db.path())?;
            // Database is created successfully
            db.close()?;
        }

        Ok(())
    }

    #[test]
    fn test_database_storage_access() -> Result<()> {
        let test_db = TestDbFile::new("_test_database_storage");

        let mut db = Database::open(test_db.path())?;

        // Test storage access
        let storage = db.storage();
        assert_eq!(storage.get_table_metadata().len(), 0);
        Ok(())
    }
}

mod mod_tests {
    use crate::catalog::Value;
    use crate::storage::*;
    use crate::test_utils::TestDbFile;
    use std::io::{Seek, SeekFrom, Write};

    // ... (rest of the code remains the same)

    #[test]
    fn test_concurrent_page_access() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_concurrent");

        let mut storage = StorageEngine::new(test_db.path())?;
        let page_id = storage.allocate_page()?;

        // Write initial data
        let mut page = Page::new(page_id);
        page.data[0..8].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        storage.write_page(page)?;

        // Read initial data
        let read_page = storage.read_page(page_id)?;
        assert_eq!(read_page.data[0..8], [1, 2, 3, 4, 5, 6, 7, 8]);

        // Modify and write back (this should update the cache)
        let mut mod_page = Page::new(page_id);
        mod_page.data[0..8].copy_from_slice(&[5, 2, 3, 4, 5, 6, 7, 8]);
        storage.write_page(mod_page)?;

        // Read again (should get the updated data from cache)
        let updated_page = storage.read_page(page_id)?;
        assert_eq!(updated_page.data[0..8], [5, 2, 3, 4, 5, 6, 7, 8]);

        Ok(())
    }

    #[test]
    fn test_free_pages_persist_across_reopen() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_free_pages_persist");

        let deallocated_page = {
            let mut storage = StorageEngine::new(test_db.path())?;
            let page_1 = storage.allocate_page()?;
            let page_2 = storage.allocate_page()?;
            assert_ne!(page_1, page_2);
            storage.deallocate_page(page_1)?;
            storage.flush()?;
            page_1
        };

        let mut reopened = StorageEngine::new(test_db.path())?;
        let reused = reopened.allocate_page()?;
        assert_eq!(reused, deallocated_page);

        Ok(())
    }

    #[test]
    fn test_trailing_free_pages_compact_storage() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_trailing_free_pages_compact");

        let (highest_page, size_before, size_after_compaction) = {
            let mut storage = StorageEngine::new(test_db.path())?;
            let _page_1 = storage.allocate_page()?;
            let _page_2 = storage.allocate_page()?;
            let page_3 = storage.allocate_page()?;
            let size_before = std::fs::metadata(test_db.path())?.len();

            storage.deallocate_page(page_3)?;
            storage.flush()?;
            let size_after_compaction = std::fs::metadata(test_db.path())?.len();

            (page_3, size_before, size_after_compaction)
        };

        assert!(size_after_compaction < size_before);

        let mut reopened = StorageEngine::new(test_db.path())?;
        let reused = reopened.allocate_page()?;
        assert_eq!(reused, highest_page);

        Ok(())
    }

    #[test]
    fn test_storage_stats_reflect_tables_rows_and_free_pages() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_stats");
        let mut storage = StorageEngine::new(test_db.path())?;

        let _ = storage.create_table("users")?;
        let _ = storage.create_table("notes")?;
        let _ = storage.insert_into_table("users", vec![Value::Integer(1)])?;
        let _ = storage.insert_into_table("users", vec![Value::Integer(2)])?;
        let free_page = storage.allocate_page()?;
        let _tail_page = storage.allocate_page()?;
        storage.deallocate_page(free_page)?;

        let stats = storage.get_storage_stats();
        assert_eq!(stats.table_count, 2);
        assert_eq!(stats.total_rows, 2);
        assert_eq!(stats.free_page_count, 1);

        Ok(())
    }

    #[test]
    fn test_storage_integrity_validates_healthy_state() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_integrity_healthy");
        let mut storage = StorageEngine::new(test_db.path())?;

        let _ = storage.create_table("users")?;
        let _ = storage.insert_into_table("users", vec![Value::Integer(1)])?;
        let _ = storage.insert_into_table("users", vec![Value::Integer(2)])?;

        let report = storage.validate_integrity()?;
        assert_eq!(report.table_count, 1);
        assert_eq!(report.live_page_count, 1);
        assert_eq!(report.total_rows, 2);
        assert_eq!(report.pager.free_page_count, 0);
        assert!(report.pager.verified_checksum_pages >= 1);

        Ok(())
    }

    #[test]
    fn test_storage_integrity_rejects_live_free_page_overlap() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_integrity_overlap");
        let mut storage = StorageEngine::new(test_db.path())?;

        let root_page_id = storage.create_table("users")?;
        let _extra_page_id = storage.allocate_page()?;
        storage.flush()?;
        storage.deallocate_page(root_page_id)?;

        let err = storage.validate_integrity().unwrap_err();
        assert!(err.to_string().contains("is both live and free"));

        Ok(())
    }

    #[test]
    fn test_storage_integrity_rejects_corrupt_table_row_count() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_integrity_row_count_corrupt");
        let mut storage = StorageEngine::new(test_db.path())?;

        let root_page_id = storage.create_table("users")?;
        let _ = storage.insert_into_table("users", vec![Value::Integer(1)])?;

        let mut page = storage.read_page(root_page_id)?;
        page.data[1..5].copy_from_slice(&2u32.to_le_bytes());
        storage.write_page(page)?;

        let err = storage.validate_integrity().unwrap_err();
        assert!(err.to_string().contains("Corrupted data"));

        Ok(())
    }

    #[test]
    fn test_versioned_storage_metadata_persists_across_reopen() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_versioned_storage_metadata");

        {
            let mut storage = StorageEngine::new(test_db.path())?;
            let _ = storage.create_table("users")?;
            let _ = storage.insert_into_table("users", vec![Value::Integer(1)])?;
            let page_1 = storage.allocate_page()?;
            let _page_2 = storage.allocate_page()?;
            storage.deallocate_page(page_1)?;
            storage.flush()?;
        }

        let reopened = StorageEngine::new(test_db.path())?;
        let stats = reopened.get_storage_stats();
        assert_eq!(stats.table_count, 1);
        assert_eq!(stats.total_rows, 1);
        assert_eq!(stats.free_page_count, 1);

        Ok(())
    }

    #[test]
    fn test_storage_metadata_rejects_unsupported_version() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_metadata_unsupported_version");
        let mut pager = crate::storage::pager::Pager::new(test_db.path(), 8)?;

        let payload = b"version=2\n";
        let mut page = Page::new(STORAGE_METADATA_PAGE_ID);
        page.data[0..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        page.data[4..4 + payload.len()].copy_from_slice(payload);
        pager.write_page(page)?;
        pager.flush()?;
        drop(pager);

        let reopened = StorageEngine::new(test_db.path());
        assert!(reopened.is_err());
        assert!(reopened
            .unwrap_err()
            .to_string()
            .contains("Unsupported storage metadata version"));

        Ok(())
    }

    #[test]
    fn test_page_checksum_detects_on_disk_corruption() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_page_checksum_detects_corruption");

        let corrupted_page_id = {
            let mut storage = StorageEngine::new(test_db.path())?;
            let page_id = storage.allocate_page()?;
            let mut page = Page::new(page_id);
            page.data[0..4].copy_from_slice(&[7, 7, 7, 7]);
            storage.write_page(page)?;
            storage.flush()?;
            page_id
        };

        {
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(test_db.path())?;
            let offset = 64 + (corrupted_page_id.as_u32() as u64 * PAGE_SIZE as u64);
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&[9])?;
            file.flush()?;
        }

        let mut reopened = StorageEngine::new(test_db.path())?;
        let err = reopened.read_page(corrupted_page_id).unwrap_err();
        assert!(err.to_string().contains("Page checksum mismatch"));

        Ok(())
    }

    #[test]
    fn test_storage_integrity_detects_checksum_corruption() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_integrity_checksum_corrupt");

        let corrupted_page_id = {
            let mut storage = StorageEngine::new(test_db.path())?;
            let page_id = storage.allocate_page()?;
            let mut page = Page::new(page_id);
            page.data[0..4].copy_from_slice(&[7, 7, 7, 7]);
            storage.write_page(page)?;
            storage.flush()?;
            page_id
        };

        {
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(test_db.path())?;
            let offset = 64 + (corrupted_page_id.as_u32() as u64 * PAGE_SIZE as u64);
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&[9])?;
            file.flush()?;
        }

        let mut reopened = StorageEngine::new(test_db.path())?;
        let err = reopened.validate_integrity().unwrap_err();
        assert!(err.to_string().contains("Page checksum mismatch"));

        Ok(())
    }

    #[test]
    fn test_table_storage_spans_multiple_pages() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_multi_page");
        let mut storage = StorageEngine::new(test_db.path())?;

        let _ = storage.create_table("users")?;

        let payload = "x".repeat(500);
        for i in 0..12 {
            let _ = storage.insert_into_table(
                "users",
                vec![Value::Integer(i), Value::Text(format!("{payload}-{i}"))],
            )?;
        }

        let metadata = storage
            .get_table_metadata()
            .get("users")
            .expect("table metadata should exist")
            .clone();
        let root_page = storage.read_page(metadata.root_page_id)?;
        let root_header = storage.read_page_header(&root_page)?;

        assert_ne!(root_header.next_page_id, PageId::invalid());
        assert_eq!(metadata.row_count, 12);

        let rows = storage.read_from_table("users")?;
        assert_eq!(rows.len(), 12);
        assert_eq!(
            rows.first(),
            Some(&vec![
                Value::Integer(0),
                Value::Text(format!("{payload}-0"))
            ])
        );
        assert_eq!(
            rows.last(),
            Some(&vec![
                Value::Integer(11),
                Value::Text(format!("{payload}-11"))
            ])
        );

        Ok(())
    }

    #[test]
    fn test_row_ids_survive_table_rewrite() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_row_ids_survive_rewrite");
        let mut storage = StorageEngine::new(test_db.path())?;

        let _ = storage.create_table("users")?;
        let first_id = storage.insert_into_table(
            "users",
            vec![Value::Integer(1), Value::Text("Alice".to_string())],
        )?;
        let second_id = storage.insert_into_table(
            "users",
            vec![Value::Integer(2), Value::Text("Bob".to_string())],
        )?;

        let mut rows = storage.read_rows_with_ids("users")?;
        assert_eq!(rows[0].row_id, first_id);
        assert_eq!(rows[1].row_id, second_id);

        rows[0].values[1] = Value::Text("Alice Updated".to_string());
        rows.remove(1);
        storage.replace_table_rows("users", rows)?;

        let rewritten = storage.read_rows_with_ids("users")?;
        assert_eq!(rewritten.len(), 1);
        assert_eq!(rewritten[0].row_id, first_id);
        assert_eq!(
            rewritten[0].values,
            vec![Value::Integer(1), Value::Text("Alice Updated".to_string()),]
        );

        let metadata = storage
            .get_table_metadata()
            .get("users")
            .expect("table metadata should exist");
        assert_eq!(metadata.next_row_id, second_id + 1);

        Ok(())
    }

    #[test]
    fn test_lookup_row_by_primary_key() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_lookup_by_primary_key");
        let mut storage = StorageEngine::new(test_db.path())?;

        let _ = storage.create_table("users")?;
        let first_id = storage.insert_into_table(
            "users",
            vec![Value::Integer(1), Value::Text("Alice".to_string())],
        )?;
        let _ = storage.insert_into_table(
            "users",
            vec![Value::Integer(2), Value::Text("Bob".to_string())],
        )?;

        let table = crate::catalog::Table::new(
            crate::catalog::TableId::new(1),
            "users".to_string(),
            vec![
                crate::catalog::Column::new(
                    crate::catalog::ColumnId::new(1),
                    "id".to_string(),
                    crate::catalog::DataType::Integer,
                )
                .primary_key(true),
                crate::catalog::Column::new(
                    crate::catalog::ColumnId::new(2),
                    "name".to_string(),
                    crate::catalog::DataType::Text,
                ),
            ],
            storage
                .get_table_metadata()
                .get("users")
                .expect("table metadata should exist")
                .root_page_id,
        )?;

        let found = storage.lookup_row_by_primary_key(&table, &[Value::Integer(1)])?;
        assert!(found.is_some());
        let found = found.unwrap();
        assert_eq!(found.row_id, first_id);
        assert_eq!(
            found.values,
            vec![Value::Integer(1), Value::Text("Alice".to_string()),]
        );

        let missing = storage.lookup_row_by_primary_key(&table, &[Value::Integer(99)])?;
        assert!(missing.is_none());

        Ok(())
    }

    #[test]
    fn test_secondary_index_lookup_and_rebuild() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_secondary_index_lookup");
        let mut storage = StorageEngine::new(test_db.path())?;

        let root_page_id = storage.create_table("users")?;
        let mut table = crate::catalog::Table::new(
            crate::catalog::TableId::new(1),
            "users".to_string(),
            vec![
                crate::catalog::Column::new(
                    crate::catalog::ColumnId::new(1),
                    "id".to_string(),
                    crate::catalog::DataType::Integer,
                )
                .primary_key(true),
                crate::catalog::Column::new(
                    crate::catalog::ColumnId::new(2),
                    "email".to_string(),
                    crate::catalog::DataType::Text,
                ),
            ],
            root_page_id,
        )?;
        table.add_secondary_index(crate::catalog::SecondaryIndex {
            name: "idx_users_email".to_string(),
            column_indices: vec![1],
            root_page_id: crate::storage::PageId::new(77),
        })?;

        let row_id_1 = storage.insert_into_table(
            "users",
            vec![Value::Integer(1), Value::Text("a@example.com".to_string())],
        )?;
        let row_1 = crate::storage::StoredRow {
            row_id: row_id_1,
            values: vec![Value::Integer(1), Value::Text("a@example.com".to_string())],
        };
        storage.register_secondary_index_row(&table, row_1.clone())?;

        let row_id_2 = storage.insert_into_table(
            "users",
            vec![Value::Integer(2), Value::Text("a@example.com".to_string())],
        )?;
        let row_2 = crate::storage::StoredRow {
            row_id: row_id_2,
            values: vec![Value::Integer(2), Value::Text("a@example.com".to_string())],
        };
        storage.register_secondary_index_row(&table, row_2.clone())?;

        let matched = storage.lookup_rows_by_secondary_index(
            &table,
            "idx_users_email",
            &[Value::Text("a@example.com".to_string())],
        )?;
        assert_eq!(matched.len(), 2);

        let rewritten_rows = vec![crate::storage::StoredRow {
            row_id: row_id_1,
            values: vec![Value::Integer(1), Value::Text("b@example.com".to_string())],
        }];
        storage.replace_table_rows("users", rewritten_rows.clone())?;
        storage.rebuild_secondary_indexes(&table, &rewritten_rows)?;

        let old_key_rows = storage.lookup_rows_by_secondary_index(
            &table,
            "idx_users_email",
            &[Value::Text("a@example.com".to_string())],
        )?;
        assert!(old_key_rows.is_empty());

        let new_key_rows = storage.lookup_rows_by_secondary_index(
            &table,
            "idx_users_email",
            &[Value::Text("b@example.com".to_string())],
        )?;
        assert_eq!(new_key_rows.len(), 1);
        assert_eq!(new_key_rows[0].row_id, row_id_1);

        Ok(())
    }
}

mod randomized_pager_lifecycle_tests {
    use crate::storage::StorageEngine;
    use crate::test_utils::TestDbFile;
    use std::collections::HashSet;

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

        fn choose_index(&mut self, len: usize) -> usize {
            (self.next_u64() % len as u64) as usize
        }

        fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
            debug_assert!(denominator > 0);
            (self.next_u64() % denominator) < numerator
        }
    }

    fn remove_random(live_pages: &mut HashSet<u32>, rng: &mut LcgRng) -> Option<u32> {
        if live_pages.is_empty() {
            return None;
        }

        let idx = rng.choose_index(live_pages.len());
        let page_id = *live_pages
            .iter()
            .nth(idx)
            .expect("index from set length should be valid");
        live_pages.remove(&page_id);
        Some(page_id)
    }

    #[test]
    fn test_randomized_allocation_reuse_reopen_cycles() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_randomized_alloc_reuse_reopen");
        let mut storage = StorageEngine::new(test_db.path())?;
        let mut rng = LcgRng::new(0xD1CE_BA5E_2026_0317);

        let mut live_pages: HashSet<u32> = HashSet::new();
        let mut retired_pages: HashSet<u32> = HashSet::new();
        let mut reuse_hits = 0usize;

        for step in 0..600usize {
            let should_allocate = live_pages.is_empty() || rng.chance(3, 5);
            if should_allocate {
                let page_id = storage.allocate_page()?;
                let id = page_id.as_u32();

                assert!(id >= 2, "allocator returned reserved page {}", id);
                assert!(
                    !live_pages.contains(&id),
                    "allocator returned an already-live page {}",
                    id
                );

                if retired_pages.remove(&id) {
                    reuse_hits += 1;
                }
                live_pages.insert(id);
            } else if let Some(id) = remove_random(&mut live_pages, &mut rng) {
                storage.deallocate_page(crate::storage::PageId::new(id))?;
                retired_pages.insert(id);
            }

            if rng.chance(1, 10) {
                storage.flush()?;
                storage = StorageEngine::new(test_db.path())?;
                let _ = storage.validate_integrity()?;
            }

            if step % 50 == 0 {
                let _ = storage.validate_integrity()?;
            }
        }

        storage.flush()?;
        storage = StorageEngine::new(test_db.path())?;
        let report = storage.validate_integrity()?;
        assert!(report.pager.free_page_count <= retired_pages.len());
        assert!(reuse_hits > 0, "expected at least one reused page id");

        Ok(())
    }
}

mod rowid_table_tests {
    use crate::storage::rowid_table::{RowidInternalCell, RowidLeafCell};
    use crate::storage::PageId;

    #[test]
    fn test_rowid_leaf_cell_roundtrip() -> crate::error::Result<()> {
        let cell = RowidLeafCell {
            rowid: 42,
            payload: vec![1, 2, 3, 4, 5],
        };

        let encoded = cell.encode();
        let decoded = RowidLeafCell::decode(&encoded)?;
        assert_eq!(decoded, cell);

        Ok(())
    }

    #[test]
    fn test_rowid_leaf_cell_rejects_length_mismatch() {
        let bad = vec![0u8; RowidLeafCell::HEADER_SIZE + 3];
        assert!(RowidLeafCell::decode(&bad).is_err());
    }

    #[test]
    fn test_rowid_internal_cell_roundtrip() -> crate::error::Result<()> {
        let cell = RowidInternalCell {
            separator_rowid: 144,
            child_page_id: PageId::new(99),
        };
        let encoded = cell.encode();
        let decoded = RowidInternalCell::decode(&encoded)?;
        assert_eq!(decoded, cell);
        Ok(())
    }

    #[test]
    fn test_rowid_internal_cell_rejects_wrong_size() {
        assert!(RowidInternalCell::decode(&[0u8; 11]).is_err());
    }
}

mod serialization_tests {
    use crate::catalog::Value;
    use crate::error::{HematiteError, Result};
    use crate::storage::serialization::*;

    #[test]
    fn test_roundtrip_row() -> Result<()> {
        let row = vec![
            Value::Integer(1),
            Value::Text("hello".to_string()),
            Value::Boolean(true),
            Value::Float(3.5),
            Value::Null,
        ];

        let bytes = RowSerializer::serialize(&row)?;
        // Stored length includes everything after the 4-byte length prefix.
        let len = RowSerializer::read_row_length(&bytes[0..4])?;
        let decoded = RowSerializer::deserialize(&bytes[4..4 + len])?;
        assert_eq!(decoded.len(), row.len());
        Ok(())
    }

    #[test]
    fn test_deserialize_truncated_returns_error() {
        // Integer marker without enough payload bytes.
        let truncated = vec![1u8, 0, 1];
        let err = RowSerializer::deserialize(&truncated).unwrap_err();
        assert!(matches!(err, HematiteError::CorruptedData(_)));
    }
}
