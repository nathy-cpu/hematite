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
