//! Centralized tests for the storage module

mod buffer_pool_tests {
    use crate::storage::buffer_pool::*;
    use crate::storage::Page;

    #[test]
    fn test_buffer_pool_basic_operations() {
        let mut pool = BufferPool::new(3);
        let page_id = 1;
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
        let page1 = Page::new(1);
        let page2 = Page::new(2);
        pool.put(page1.clone());
        pool.put(page2.clone());

        assert_eq!(pool.len(), 2);

        // Add third page (should evict first)
        let page3 = Page::new(3);
        pool.put(page3.clone());

        assert_eq!(pool.len(), 2);
        assert!(pool.get(1).is_none()); // Evicted
        assert!(pool.get(2).is_some()); // Still present
        assert!(pool.get(3).is_some()); // New page
    }

    #[test]
    fn test_buffer_pool_lru_update() {
        let mut pool = BufferPool::new(3);

        let page1 = Page::new(1);
        let page2 = Page::new(2);
        let page3 = Page::new(3);

        // Add pages
        pool.put(page1);
        pool.put(page2);
        pool.put(page3);

        // Access page1 (should make it most recently used)
        pool.get(1);

        // Add page4 (should evict page2, not page1)
        let page4 = Page::new(4);
        pool.put(page4);

        assert!(pool.get(1).is_some()); // Still present (accessed)
        assert!(pool.get(2).is_none()); // Evicted (least recently used)
        assert!(pool.get(3).is_some()); // Still present
        assert!(pool.get(4).is_some()); // New page
    }

    #[test]
    fn test_buffer_pool_update_existing() {
        let mut pool = BufferPool::new(2);

        let page_id = 1;
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

        let page1 = Page::new(1);
        let page2 = Page::new(2);

        pool.put(page1);
        pool.put(page2);

        assert_eq!(pool.len(), 2);

        // Remove page1
        pool.remove(1);
        assert_eq!(pool.len(), 1);
        assert!(pool.get(1).is_none());
        assert!(pool.get(2).is_some());

        // Remove non-existent page
        pool.remove(999);
        assert_eq!(pool.len(), 1); // No change
    }

    #[test]
    fn test_buffer_pool_capacity_zero() {
        let mut pool = BufferPool::new(0);

        let page = Page::new(1);
        pool.put(page);

        // Pool should remain empty since capacity is 0
        assert_eq!(pool.len(), 0);
        assert!(pool.get(1).is_none());
    }
}

mod freelist_tests {
    use crate::storage::free_list::FreeList;

    #[test]
    fn test_freelist_push_is_idempotent() {
        let mut freelist = FreeList::new();
        freelist.push_free_page(10);
        freelist.push_free_page(10);
        assert_eq!(freelist.as_slice(), &[10]);
    }

    #[test]
    fn test_freelist_compacts_trailing_pages() {
        let mut freelist = FreeList::new();
        freelist.push_free_page(8);
        freelist.push_free_page(9);
        freelist.push_free_page(11);
        freelist.push_free_page(12);

        let mut next_page_id = 13;
        freelist.compact_trailing_pages(&mut next_page_id, 2);

        assert_eq!(next_page_id, 11);
        assert_eq!(freelist.as_slice(), &[8, 9]);
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
    use std::sync::{Arc, Barrier};
    use std::thread;

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

    #[test]
    fn test_pager_transaction_commit_persists_changes() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_transaction_commit");
        let page_id = {
            let mut pager = Pager::new(test_db.path(), 8)?;
            let page_id = pager.allocate_page()?;
            let mut page = Page::new(page_id);
            page.data[0] = 10;
            pager.write_page(page)?;
            pager.flush()?;

            pager.begin_transaction()?;
            let mut updated = pager.read_page(page_id)?;
            updated.data[0] = 99;
            pager.write_page(updated)?;
            pager.commit_transaction()?;
            page_id
        };

        let mut reopened = Pager::new(test_db.path(), 8)?;
        let page = reopened.read_page(page_id)?;
        assert_eq!(page.data[0], 99);
        Ok(())
    }

    #[test]
    fn test_pager_recovery_rolls_back_active_journal_on_open() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_recovery_rolls_back_active_journal");
        let page_id = {
            let mut pager = Pager::new(test_db.path(), 8)?;
            let page_id = pager.allocate_page()?;
            let mut page = Page::new(page_id);
            page.data[0] = 10;
            pager.write_page(page)?;
            pager.flush()?;

            pager.begin_transaction()?;
            let mut updated = pager.read_page(page_id)?;
            updated.data[0] = 99;
            pager.write_page(updated)?;
            page_id
        };

        let mut reopened = Pager::new(test_db.path(), 8)?;
        let page = reopened.read_page(page_id)?;
        assert_eq!(page.data[0], 10);
        Ok(())
    }

    #[test]
    fn test_pager_allows_multiple_readers() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_multiple_readers");
        let mut first = Pager::new(test_db.path(), 8)?;
        let mut second = Pager::new(test_db.path(), 8)?;

        first.begin_read()?;
        second.begin_read()?;

        second.end_read()?;
        first.end_read()?;
        Ok(())
    }

    #[test]
    fn test_pager_blocks_writer_while_reader_is_active() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_writer_blocked_by_reader");
        let mut reader = Pager::new(test_db.path(), 8)?;
        let mut writer = Pager::new(test_db.path(), 8)?;

        reader.begin_read()?;
        let err = writer.begin_transaction().unwrap_err();
        assert!(err.to_string().contains("locked"));
        reader.end_read()?;

        writer.begin_transaction()?;
        writer.rollback_transaction()?;
        Ok(())
    }

    #[test]
    fn test_pager_blocks_reader_while_writer_is_active() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_reader_blocked_by_writer");
        let mut writer = Pager::new(test_db.path(), 8)?;
        let mut reader = Pager::new(test_db.path(), 8)?;

        writer.begin_transaction()?;
        let err = reader.begin_read().unwrap_err();
        assert!(err.to_string().contains("locked"));
        writer.rollback_transaction()?;

        reader.begin_read()?;
        reader.end_read()?;
        Ok(())
    }

    #[test]
    fn test_pager_commit_releases_writer_lock_and_persists_changes() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_commit_releases_writer_lock");
        let mut writer = Pager::new(test_db.path(), 8)?;
        let page_id = writer.allocate_page()?;

        let mut page = Page::new(page_id);
        page.data[0] = 10;
        writer.write_page(page)?;
        writer.flush()?;

        writer.begin_transaction()?;
        let mut updated = writer.read_page(page_id)?;
        updated.data[0] = 77;
        writer.write_page(updated)?;

        let mut reader = Pager::new(test_db.path(), 8)?;
        let err = reader.begin_read().unwrap_err();
        assert!(err.to_string().contains("locked"));

        writer.commit_transaction()?;

        reader.begin_read()?;
        let persisted = reader.read_page(page_id)?;
        assert_eq!(persisted.data[0], 77);
        reader.end_read()?;
        Ok(())
    }

    #[test]
    fn test_pager_rollback_releases_writer_lock_and_restores_original_data(
    ) -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_rollback_releases_writer_lock");
        let mut writer = Pager::new(test_db.path(), 8)?;
        let page_id = writer.allocate_page()?;

        let mut page = Page::new(page_id);
        page.data[0] = 10;
        writer.write_page(page)?;
        writer.flush()?;

        writer.begin_transaction()?;
        let mut updated = writer.read_page(page_id)?;
        updated.data[0] = 55;
        writer.write_page(updated)?;

        let mut second_writer = Pager::new(test_db.path(), 8)?;
        let err = second_writer.begin_transaction().unwrap_err();
        assert!(err.to_string().contains("locked"));

        writer.rollback_transaction()?;

        second_writer.begin_transaction()?;
        let restored = second_writer.read_page(page_id)?;
        assert_eq!(restored.data[0], 10);
        second_writer.rollback_transaction()?;
        Ok(())
    }

    #[test]
    fn test_pager_allows_concurrent_readers_and_blocks_writer_until_they_exit(
    ) -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_concurrent_readers_block_writer");
        let db_path = std::path::PathBuf::from(test_db.path());
        let barrier = Arc::new(Barrier::new(3));

        let first_barrier = Arc::clone(&barrier);
        let first_path = db_path.clone();
        let first_reader = thread::spawn(move || -> crate::error::Result<()> {
            let mut pager = Pager::new(&first_path, 8)?;
            pager.begin_read()?;
            first_barrier.wait();
            first_barrier.wait();
            pager.end_read()?;
            Ok(())
        });

        let second_barrier = Arc::clone(&barrier);
        let second_path = db_path.clone();
        let second_reader = thread::spawn(move || -> crate::error::Result<()> {
            let mut pager = Pager::new(&second_path, 8)?;
            pager.begin_read()?;
            second_barrier.wait();
            second_barrier.wait();
            pager.end_read()?;
            Ok(())
        });

        barrier.wait();

        let mut writer = Pager::new(&db_path, 8)?;
        let err = writer.begin_transaction().unwrap_err();
        assert!(err.to_string().contains("locked"));

        barrier.wait();

        first_reader.join().unwrap()?;
        second_reader.join().unwrap()?;

        writer.begin_transaction()?;
        writer.rollback_transaction()?;
        Ok(())
    }
}

mod mod_tests {
    use crate::btree::value_store::StoredValueLayout;
    use crate::btree::{BTreeKey, BTreeNode, NodeType};
    use crate::catalog::{CatalogEngine, Value};
    use crate::storage::overflow::collect_overflow_page_ids;
    use crate::storage::{Page, Pager, PAGE_SIZE, STORAGE_METADATA_PAGE_ID};
    use crate::test_utils::TestDbFile;
    use std::io::{Seek, SeekFrom, Write};

    // ... (rest of the code remains the same)

    #[test]
    fn test_concurrent_page_access() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_concurrent");

        let mut storage = Pager::new(test_db.path(), 100)?;
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
            let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;
            let page_1 = storage.allocate_page()?;
            let page_2 = storage.allocate_page()?;
            assert_ne!(page_1, page_2);
            storage.deallocate_page(page_1)?;
            storage.flush()?;
            page_1
        };

        let reopened = crate::catalog::CatalogEngine::new(test_db.path())?;
        let reused = reopened.allocate_page()?;
        assert_eq!(reused, deallocated_page);

        Ok(())
    }

    #[test]
    fn test_trailing_free_pages_compact_storage() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_trailing_free_pages_compact");

        let (highest_page, size_before, size_after_compaction) = {
            let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;
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

        let reopened = crate::catalog::CatalogEngine::new(test_db.path())?;
        let reused = reopened.allocate_page()?;
        assert_eq!(reused, highest_page);

        Ok(())
    }

    #[test]
    fn test_storage_stats_reflect_tables_rows_and_free_pages() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_stats");
        let mut storage = CatalogEngine::new(test_db.path())?;

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
        assert_eq!(stats.fragmented_free_page_count, 1);
        assert_eq!(stats.trailing_free_page_count, 0);
        assert!(stats.allocated_page_count >= stats.live_table_page_count + stats.free_page_count);
        assert!(
            stats.file_bytes >= 64 + (stats.allocated_page_count as u64 + 2) * PAGE_SIZE as u64
        );
        assert!(stats.table_used_bytes > 0);
        assert!(stats.table_unused_bytes < stats.live_table_page_count * PAGE_SIZE);

        Ok(())
    }

    #[test]
    fn test_storage_stats_and_integrity_report_live_overflow_pages() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_overflow_stats");
        let mut storage = CatalogEngine::new(test_db.path())?;

        let _ = storage.create_table("docs")?;
        let large_text = "x".repeat(PAGE_SIZE * 3);
        let _ = storage.insert_into_table("docs", vec![Value::Text(large_text)])?;

        let stats = storage.get_storage_stats();
        assert_eq!(stats.table_count, 1);
        assert_eq!(stats.total_rows, 1);
        assert!(stats.overflow_page_count > 0);
        assert!(stats.table_used_bytes > PAGE_SIZE);
        assert!(
            stats.table_unused_bytes
                < (stats.live_table_page_count + stats.overflow_page_count) * PAGE_SIZE
        );

        let report = storage.validate_integrity()?;
        assert_eq!(report.table_count, 1);
        assert_eq!(report.total_rows, 1);
        assert!(report.overflow_page_count > 0);

        Ok(())
    }

    #[test]
    fn test_storage_integrity_validates_healthy_state() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_integrity_healthy");
        let mut storage = CatalogEngine::new(test_db.path())?;

        let _ = storage.create_table("users")?;
        let _ = storage.insert_into_table("users", vec![Value::Integer(1)])?;
        let _ = storage.insert_into_table("users", vec![Value::Integer(2)])?;

        let report = storage.validate_integrity()?;
        assert_eq!(report.table_count, 1);
        assert_eq!(report.live_page_count, 1);
        assert_eq!(report.index_page_count, 0);
        assert_eq!(report.overflow_page_count, 0);
        assert_eq!(report.total_rows, 2);
        assert!(report.pager.allocated_page_count >= 1);
        assert_eq!(report.pager.free_page_count, 0);
        assert_eq!(report.pager.fragmented_free_page_count, 0);
        assert!(report.pager.verified_checksum_pages >= 1);

        Ok(())
    }

    #[test]
    fn test_storage_integrity_rejects_live_free_page_overlap() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_integrity_overlap");
        let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;

        let root_page_id = storage.create_table("users")?;
        let _extra_page_id = storage.allocate_page()?;
        storage.flush()?;
        storage.deallocate_page(root_page_id)?;

        let err = storage.validate_integrity().unwrap_err();
        assert!(err.to_string().contains("is both live and free"));

        Ok(())
    }

    #[test]
    fn test_storage_integrity_rejects_live_overflow_free_page_overlap() -> crate::error::Result<()>
    {
        let test_db = TestDbFile::new("_test_storage_integrity_overflow_overlap");
        let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;

        let root_page_id = storage.create_table("docs")?;
        let _ = storage.insert_into_table("docs", vec![Value::Text("x".repeat(PAGE_SIZE * 3))])?;

        let root_page = storage.read_page(root_page_id)?;
        let root_node = BTreeNode::from_page(root_page)?;
        let layout = StoredValueLayout::decode(root_node.values[0].as_bytes())?;
        assert_ne!(layout.overflow_first_page, crate::storage::INVALID_PAGE_ID);

        storage.deallocate_page(layout.overflow_first_page)?;

        let err = storage.validate_integrity().unwrap_err();
        let message = err.to_string();
        assert!(message.contains("Overflow page"));
        assert!(
            message.contains("both live and free")
                || message.contains("also on the freelist")
                || message.contains("magic mismatch"),
            "unexpected overlap message: {message}"
        );

        Ok(())
    }

    #[test]
    fn test_storage_integrity_rejects_corrupt_table_row_count() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_integrity_row_count_corrupt");
        let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;

        let root_page_id = storage.create_table("users")?;
        let _ = storage.insert_into_table("users", vec![Value::Integer(1)])?;

        let mut page = storage.read_page(root_page_id)?;
        page.data[1..5].copy_from_slice(&2u32.to_le_bytes());
        storage.write_page(page)?;

        let err = storage.validate_integrity().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Corrupted data")
                || msg.contains("Unsupported B-tree version")
                || msg.contains("Invalid magic number")
                || msg.contains("Invalid page type"),
            "unexpected corruption error message: {msg}"
        );

        Ok(())
    }

    #[test]
    fn test_storage_integrity_rejects_cursor_rowid_order_violation() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_integrity_cursor_rowid_order");
        let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;

        let root_page_id = storage.create_table("users")?;
        let _ = storage.insert_into_table("users", vec![Value::Integer(1)])?;
        let _ = storage.insert_into_table("users", vec![Value::Integer(2)])?;

        let mut page = storage.read_page(root_page_id)?;
        let mut node = BTreeNode::from_page(page.clone())?;
        assert_eq!(node.node_type, NodeType::Leaf);
        assert!(node.keys.len() >= 2);

        let first_row_id = u64::from_be_bytes(node.keys[0].data.as_slice().try_into().unwrap());

        // Corrupt the second rowid key to be <= the first rowid key.
        node.keys[1] = BTreeKey::new(first_row_id.to_be_bytes().to_vec());
        node.to_page(&mut page)?;
        storage.write_page(page)?;

        let err = storage.validate_integrity().unwrap_err();
        assert!(err
            .to_string()
            .contains("Cursor-visible rowid order violation"));
        Ok(())
    }

    #[test]
    fn test_versioned_storage_metadata_persists_across_reopen() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_versioned_storage_metadata");

        {
            let mut storage = CatalogEngine::new(test_db.path())?;
            let _ = storage.create_table("users")?;
            let _ = storage.insert_into_table("users", vec![Value::Integer(1)])?;
            let page_1 = storage.allocate_page()?;
            let _page_2 = storage.allocate_page()?;
            storage.deallocate_page(page_1)?;
            storage.flush()?;
        }

        let reopened = crate::catalog::CatalogEngine::new(test_db.path())?;
        let stats = reopened.get_storage_stats();
        assert_eq!(stats.table_count, 1);
        assert_eq!(stats.total_rows, 1);
        assert_eq!(stats.free_page_count, 1);

        Ok(())
    }

    #[test]
    fn test_delete_reclaims_large_row_overflow_pages() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_delete_reclaims_large_row_overflow");
        let mut storage = CatalogEngine::new(test_db.path())?;

        let root_page_id = storage.create_table("docs")?;
        let rowid =
            storage.insert_into_table("docs", vec![Value::Text("x".repeat(PAGE_SIZE * 3))])?;

        let root_page = storage.read_page(root_page_id)?;
        let root_node = BTreeNode::from_page(root_page)?;
        let layout = StoredValueLayout::decode(root_node.values[0].as_bytes())?;
        let overflow_ids = collect_overflow_page_ids(
            &mut storage.pager.lock().unwrap(),
            Some(layout.overflow_first_page),
        )?;
        assert!(!overflow_ids.is_empty());

        assert!(storage.delete_from_table_by_rowid("docs", rowid)?);
        let stats = storage.get_storage_stats();
        assert_eq!(stats.total_rows, 0);
        assert_eq!(stats.overflow_page_count, 0);

        let reused = storage.allocate_page()?;
        assert!(overflow_ids.contains(&reused));

        Ok(())
    }

    #[test]
    fn test_replace_table_rows_reclaims_previous_large_row_overflow_pages(
    ) -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_replace_reclaims_large_row_overflow");
        let mut storage = CatalogEngine::new(test_db.path())?;

        let root_page_id = storage.create_table("docs")?;
        let _ = storage.insert_into_table("docs", vec![Value::Text("x".repeat(PAGE_SIZE * 3))])?;

        let root_page = storage.read_page(root_page_id)?;
        let root_node = BTreeNode::from_page(root_page)?;
        let layout = StoredValueLayout::decode(root_node.values[0].as_bytes())?;
        let overflow_ids = collect_overflow_page_ids(
            &mut storage.pager.lock().unwrap(),
            Some(layout.overflow_first_page),
        )?;

        storage.replace_table_rows(
            "docs",
            vec![crate::catalog::StoredRow {
                row_id: 1,
                values: vec![Value::Text("small".to_string())],
            }],
        )?;

        let stats = storage.get_storage_stats();
        assert_eq!(stats.total_rows, 1);
        assert_eq!(stats.overflow_page_count, 0);

        let reused = storage.allocate_page()?;
        assert!(overflow_ids.contains(&reused));

        Ok(())
    }

    #[test]
    fn test_drop_table_reclaims_large_row_overflow_pages() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_drop_reclaims_large_row_overflow");
        let mut storage = CatalogEngine::new(test_db.path())?;

        let root_page_id = storage.create_table("docs")?;
        let _ = storage.insert_into_table("docs", vec![Value::Text("x".repeat(PAGE_SIZE * 3))])?;

        let root_page = storage.read_page(root_page_id)?;
        let root_node = BTreeNode::from_page(root_page)?;
        let layout = StoredValueLayout::decode(root_node.values[0].as_bytes())?;
        let overflow_ids = collect_overflow_page_ids(
            &mut storage.pager.lock().unwrap(),
            Some(layout.overflow_first_page),
        )?;

        storage.drop_table("docs")?;
        assert_eq!(storage.get_storage_stats().table_count, 0);

        let reused = storage.allocate_page()?;
        assert!(overflow_ids.contains(&reused) || reused == root_page_id);

        Ok(())
    }

    #[test]
    fn test_repeated_large_row_churn_keeps_file_growth_bounded() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_large_row_churn_bounded_growth");
        let mut first_peak = None;

        {
            let mut storage = CatalogEngine::new(test_db.path())?;
            let _ = storage.create_table("docs")?;

            for cycle in 0..6 {
                let rowid = storage.insert_into_table(
                    "docs",
                    vec![Value::Text(format!(
                        "cycle-{cycle}-{}",
                        "x".repeat(PAGE_SIZE * 3)
                    ))],
                )?;
                storage.flush()?;

                let size_after_insert = std::fs::metadata(test_db.path())?.len();
                first_peak.get_or_insert(size_after_insert);

                assert!(
                    size_after_insert <= first_peak.unwrap() + (PAGE_SIZE as u64),
                    "large-row churn grew file unexpectedly: first_peak={}, current={}",
                    first_peak.unwrap(),
                    size_after_insert
                );

                assert!(storage.delete_from_table_by_rowid("docs", rowid)?);
                storage.flush()?;
            }
        }

        let final_size = std::fs::metadata(test_db.path())?.len();
        assert!(
            final_size <= first_peak.unwrap(),
            "expected final file size {} to be <= first peak {}",
            final_size,
            first_peak.unwrap()
        );

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

        let reopened = crate::catalog::CatalogEngine::new(test_db.path());
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
            let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;
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
            let offset = 64 + (corrupted_page_id as u64 * PAGE_SIZE as u64);
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&[9])?;
            file.flush()?;
        }

        let reopened = crate::catalog::CatalogEngine::new(test_db.path())?;
        let err = reopened.read_page(corrupted_page_id).unwrap_err();
        assert!(err.to_string().contains("Page checksum mismatch"));

        Ok(())
    }

    #[test]
    fn test_storage_integrity_detects_checksum_corruption() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_integrity_checksum_corrupt");

        let corrupted_page_id = {
            let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;
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
            let offset = 64 + (corrupted_page_id as u64 * PAGE_SIZE as u64);
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&[9])?;
            file.flush()?;
        }

        let mut reopened = crate::catalog::CatalogEngine::new(test_db.path())?;
        let err = reopened.validate_integrity().unwrap_err();
        assert!(err.to_string().contains("Page checksum mismatch"));

        Ok(())
    }

    #[test]
    fn test_table_storage_spans_multiple_pages() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_multi_page");
        let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;

        let _ = storage.create_table("users")?;

        let payload = "x".repeat(500);
        for i in 0..220 {
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
        let root_node = BTreeNode::from_page(root_page)?;

        assert_eq!(root_node.node_type, NodeType::Internal);
        assert_eq!(metadata.row_count, 220);

        let rows = storage.read_from_table("users")?;
        assert_eq!(rows.len(), 220);
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
                Value::Integer(219),
                Value::Text(format!("{payload}-219"))
            ])
        );

        Ok(())
    }

    #[test]
    fn test_new_table_root_uses_btree_page_format() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_table_root_is_btree");
        let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;

        let root_page_id = storage.create_table("users")?;
        let root_page = storage.read_page(root_page_id)?;
        let root_node = BTreeNode::from_page(root_page)?;

        assert_eq!(root_node.page_id, root_page_id);
        assert_eq!(root_node.node_type, NodeType::Leaf);
        assert!(root_node.keys.is_empty());
        assert!(root_node.values.is_empty());

        Ok(())
    }

    #[test]
    fn test_row_ids_survive_table_rewrite() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_row_ids_survive_rewrite");
        let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;

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
        let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;

        let _ = storage.create_table("users")?;
        let primary_key_root_page_id = storage.create_empty_btree()?;
        let first_id = storage.insert_into_table(
            "users",
            vec![Value::Integer(1), Value::Text("Alice".to_string())],
        )?;
        let second_id = storage.insert_into_table(
            "users",
            vec![Value::Integer(2), Value::Text("Bob".to_string())],
        )?;

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
        table.primary_key_index_root_page_id = primary_key_root_page_id.into();
        storage.register_primary_key_row(
            &table,
            crate::catalog::StoredRow {
                row_id: first_id,
                values: vec![Value::Integer(1), Value::Text("Alice".to_string())],
            },
        )?;
        storage.register_primary_key_row(
            &table,
            crate::catalog::StoredRow {
                row_id: second_id,
                values: vec![Value::Integer(2), Value::Text("Bob".to_string())],
            },
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
        let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;

        let root_page_id = storage.create_table("users")?;
        let primary_key_root_page_id = storage.create_empty_btree()?;
        let secondary_index_root_page_id = storage.create_empty_btree()?;
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
        table.primary_key_index_root_page_id = primary_key_root_page_id.into();
        table.add_secondary_index(crate::catalog::SecondaryIndex {
            name: "idx_users_email".to_string(),
            column_indices: vec![1],
            root_page_id: secondary_index_root_page_id.into(),
        })?;

        let row_id_1 = storage.insert_into_table(
            "users",
            vec![Value::Integer(1), Value::Text("a@example.com".to_string())],
        )?;
        let row_1 = crate::catalog::StoredRow {
            row_id: row_id_1,
            values: vec![Value::Integer(1), Value::Text("a@example.com".to_string())],
        };
        storage.register_primary_key_row(&table, row_1.clone())?;
        storage.register_secondary_index_row(&table, row_1.clone())?;

        let row_id_2 = storage.insert_into_table(
            "users",
            vec![Value::Integer(2), Value::Text("a@example.com".to_string())],
        )?;
        let row_2 = crate::catalog::StoredRow {
            row_id: row_id_2,
            values: vec![Value::Integer(2), Value::Text("a@example.com".to_string())],
        };
        storage.register_primary_key_row(&table, row_2.clone())?;
        storage.register_secondary_index_row(&table, row_2.clone())?;

        let matched = storage.lookup_rows_by_secondary_index(
            &table,
            "idx_users_email",
            &[Value::Text("a@example.com".to_string())],
        )?;
        assert_eq!(matched.len(), 2);

        let rewritten_rows = vec![crate::catalog::StoredRow {
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

    #[test]
    fn test_durable_indexes_survive_reopen() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_durable_indexes_survive_reopen");

        let (table, row_id) = {
            let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;
            let root_page_id = storage.create_table("users")?;
            let primary_key_root_page_id = storage.create_empty_btree()?;
            let secondary_index_root_page_id = storage.create_empty_btree()?;

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
            table.primary_key_index_root_page_id = primary_key_root_page_id.into();
            table.add_secondary_index(crate::catalog::SecondaryIndex {
                name: "idx_users_email".to_string(),
                column_indices: vec![1],
                root_page_id: secondary_index_root_page_id.into(),
            })?;

            let row_id = storage.insert_into_table(
                "users",
                vec![
                    Value::Integer(7),
                    Value::Text("persist@example.com".to_string()),
                ],
            )?;
            let row = crate::catalog::StoredRow {
                row_id,
                values: vec![
                    Value::Integer(7),
                    Value::Text("persist@example.com".to_string()),
                ],
            };
            storage.register_primary_key_row(&table, row.clone())?;
            storage.register_secondary_index_row(&table, row)?;
            storage.flush()?;
            (table, row_id)
        };

        let mut reopened = crate::catalog::CatalogEngine::new(test_db.path())?;
        let found = reopened.lookup_row_by_primary_key(&table, &[Value::Integer(7)])?;
        assert_eq!(found.map(|row| row.row_id), Some(row_id));

        let secondary = reopened.lookup_rows_by_secondary_index(
            &table,
            "idx_users_email",
            &[Value::Text("persist@example.com".to_string())],
        )?;
        assert_eq!(secondary.len(), 1);
        assert_eq!(secondary[0].row_id, row_id);
        reopened.validate_table_indexes(&table)?;
        Ok(())
    }

    #[test]
    fn test_delete_updates_durable_indexes() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_delete_updates_durable_indexes");
        let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;
        let root_page_id = storage.create_table("users")?;
        let primary_key_root_page_id = storage.create_empty_btree()?;
        let secondary_index_root_page_id = storage.create_empty_btree()?;

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
        table.primary_key_index_root_page_id = primary_key_root_page_id.into();
        table.add_secondary_index(crate::catalog::SecondaryIndex {
            name: "idx_users_email".to_string(),
            column_indices: vec![1],
            root_page_id: secondary_index_root_page_id.into(),
        })?;

        let row_id = storage.insert_into_table(
            "users",
            vec![
                Value::Integer(11),
                Value::Text("gone@example.com".to_string()),
            ],
        )?;
        let row = crate::catalog::StoredRow {
            row_id,
            values: vec![
                Value::Integer(11),
                Value::Text("gone@example.com".to_string()),
            ],
        };
        storage.register_primary_key_row(&table, row.clone())?;
        storage.register_secondary_index_row(&table, row.clone())?;

        assert!(storage.delete_primary_key_row(&table, &row)?);
        storage.delete_secondary_index_row(&table, &row)?;
        assert!(storage.delete_from_table_by_rowid("users", row_id)?);

        assert!(storage
            .lookup_row_by_primary_key(&table, &[Value::Integer(11)])?
            .is_none());
        assert!(storage
            .lookup_rows_by_secondary_index(
                &table,
                "idx_users_email",
                &[Value::Text("gone@example.com".to_string())],
            )?
            .is_empty());

        Ok(())
    }

    #[test]
    fn test_table_scan_via_cursor_matches_row_reads() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_table_scan_via_cursor");
        let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;
        let _ = storage.create_table("users")?;
        let _ = storage.insert_into_table("users", vec![Value::Integer(2)])?;
        let _ = storage.insert_into_table("users", vec![Value::Integer(1)])?;
        let _ = storage.insert_into_table("users", vec![Value::Integer(3)])?;

        let rows = storage.read_rows_with_ids("users")?;

        let mut cursor = storage.open_table_cursor("users")?;
        let mut cursor_rows = Vec::new();
        if cursor.first() {
            loop {
                cursor_rows.push(cursor.current().cloned().expect("cursor row"));
                if !cursor.next() {
                    break;
                }
            }
        }

        assert_eq!(cursor_rows, rows);
        Ok(())
    }

    #[test]
    fn test_rowid_lookup_via_cursor_seek() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_rowid_lookup_via_cursor");
        let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;
        let _ = storage.create_table("users")?;

        let first = storage.insert_into_table("users", vec![Value::Integer(10)])?;
        let _second = storage.insert_into_table("users", vec![Value::Integer(20)])?;

        let found = storage.lookup_row_by_rowid("users", first)?;
        assert!(found.is_some());
        assert_eq!(found.unwrap().values, vec![Value::Integer(10)]);

        let missing = storage.lookup_row_by_rowid("users", first + 99)?;
        assert!(missing.is_none());
        Ok(())
    }

    #[test]
    fn test_table_cursor_order_survives_reopen() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_cursor_reopen_order");

        {
            let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;
            let _ = storage.create_table("users")?;
            let _ = storage.insert_into_table("users", vec![Value::Integer(1)])?;
            let _ = storage.insert_into_table("users", vec![Value::Integer(2)])?;
            let _ = storage.insert_into_table("users", vec![Value::Integer(3)])?;
            storage.flush()?;
        }

        let mut reopened = crate::catalog::CatalogEngine::new(test_db.path())?;
        let mut cursor = reopened.open_table_cursor("users")?;
        let mut seen = Vec::new();
        if cursor.first() {
            loop {
                seen.push(cursor.current().expect("row").row_id);
                if !cursor.next() {
                    break;
                }
            }
        }

        let mut sorted = seen.clone();
        sorted.sort_unstable();
        assert_eq!(seen, sorted);
        assert_eq!(seen.len(), 3);
        Ok(())
    }

    #[test]
    fn test_delete_from_table_by_rowid_removes_only_target_row() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_delete_by_rowid");
        let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;
        let _ = storage.create_table("users")?;

        let row1 = storage.insert_into_table("users", vec![Value::Integer(1)])?;
        let row2 = storage.insert_into_table("users", vec![Value::Integer(2)])?;
        let row3 = storage.insert_into_table("users", vec![Value::Integer(3)])?;

        assert!(storage.delete_from_table_by_rowid("users", row2)?);
        assert!(!storage.delete_from_table_by_rowid("users", row2)?);

        let remaining = storage.read_rows_with_ids("users")?;
        let remaining_rowids = remaining.iter().map(|row| row.row_id).collect::<Vec<_>>();
        let remaining_values = remaining
            .iter()
            .map(|row| row.values[0].clone())
            .collect::<Vec<_>>();

        assert_eq!(remaining_rowids, vec![row1, row3]);
        assert_eq!(remaining_values, vec![Value::Integer(1), Value::Integer(3)]);

        let metadata = storage.get_table_metadata().get("users").unwrap();
        assert_eq!(metadata.row_count, 2);
        Ok(())
    }
}

mod randomized_pager_lifecycle_tests {
    use crate::catalog::{CatalogEngine, Value};
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
        let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;
        let mut rng = LcgRng::new(0xD1CE_BA5E_2026_0317);

        let mut live_pages: HashSet<u32> = HashSet::new();
        let mut retired_pages: HashSet<u32> = HashSet::new();
        let mut reuse_hits = 0usize;

        for step in 0..600usize {
            let should_allocate = live_pages.is_empty() || rng.chance(3, 5);
            if should_allocate {
                let page_id = storage.allocate_page()?;
                let id = page_id;

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
                storage.deallocate_page(id)?;
                retired_pages.insert(id);
            }

            if rng.chance(1, 10) {
                storage.flush()?;
                storage = crate::catalog::CatalogEngine::new(test_db.path())?;
                let _ = storage.validate_integrity()?;
            }

            if step % 50 == 0 {
                let _ = storage.validate_integrity()?;
            }
        }

        storage.flush()?;
        storage = crate::catalog::CatalogEngine::new(test_db.path())?;
        let report = storage.validate_integrity()?;
        assert!(report.pager.free_page_count <= retired_pages.len());
        assert!(reuse_hits > 0, "expected at least one reused page id");

        Ok(())
    }

    #[test]
    fn test_randomized_large_row_reopen_churn_integrity() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_randomized_large_row_reopen_churn");
        let mut rng = LcgRng::new(0xC0FFEE55AA);
        let mut live_row_ids = Vec::new();

        {
            let mut storage = CatalogEngine::new(test_db.path())?;
            let _ = storage.create_table("docs")?;

            for step in 0..120usize {
                let do_insert = live_row_ids.is_empty() || rng.chance(2, 3);
                if do_insert {
                    let is_large = rng.chance(1, 2);
                    let payload = if is_large {
                        format!("L{}-{}", step, "x".repeat(crate::storage::PAGE_SIZE * 3))
                    } else {
                        format!("S{}-{}", step, "y".repeat(32))
                    };
                    let rowid = storage.insert_into_table("docs", vec![Value::Text(payload)])?;
                    live_row_ids.push(rowid);
                } else {
                    let index = rng.choose_index(live_row_ids.len());
                    let rowid = live_row_ids.swap_remove(index);
                    let deleted = storage.delete_from_table_by_rowid("docs", rowid)?;
                    assert!(deleted, "expected rowid {} to delete", rowid);
                }

                if step % 8 == 0 {
                    storage.flush()?;
                    assert!(storage.validate_integrity().is_ok());
                }

                if step % 15 == 14 {
                    storage.flush()?;
                    drop(storage);
                    storage = CatalogEngine::new(test_db.path())?;

                    let rows = storage.read_rows_with_ids("docs")?;
                    let mut actual_row_ids =
                        rows.into_iter().map(|row| row.row_id).collect::<Vec<_>>();
                    actual_row_ids.sort_unstable();
                    live_row_ids.sort_unstable();
                    assert_eq!(actual_row_ids, live_row_ids);
                    assert!(storage.validate_integrity().is_ok());
                }
            }

            storage.flush()?;
            assert!(storage.validate_integrity().is_ok());
        }

        let mut reopened = CatalogEngine::new(test_db.path())?;
        let mut actual_row_ids = reopened
            .read_rows_with_ids("docs")?
            .into_iter()
            .map(|row| row.row_id)
            .collect::<Vec<_>>();
        actual_row_ids.sort_unstable();
        live_row_ids.sort_unstable();
        assert_eq!(actual_row_ids, live_row_ids);
        assert!(reopened.validate_integrity().is_ok());

        Ok(())
    }
}

mod rowid_table_tests {
    use crate::catalog::row_id::{
        decode_stored_row_record, encode_stored_row_record, free_row_record_overflow,
        hydrate_row_record_cell, materialize_row_record_cell, RowidInternalCell, RowidLeafCell,
        RowidLeafCellLayout, ROWID_LEAF_FIXED_HEADER_SIZE,
    };
    use crate::storage::overflow::{
        collect_overflow_page_ids, free_overflow_chain, read_overflow_chain,
        validate_overflow_chain, write_overflow_chain,
    };
    use crate::storage::Pager;
    use crate::storage::INVALID_PAGE_ID;

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
            child_page_id: 99,
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

    #[test]
    fn test_rowid_fixed_leaf_cell_roundtrip() -> crate::error::Result<()> {
        let cell = RowidLeafCellLayout {
            rowid: 77,
            total_payload_len: 1000,
            local_payload: vec![9, 8, 7, 6, 5],
            overflow_first_page: 120,
        };
        let encoded = cell.encode()?;
        assert_eq!(
            encoded.len(),
            ROWID_LEAF_FIXED_HEADER_SIZE + cell.local_payload.len()
        );
        let decoded = RowidLeafCellLayout::decode(&encoded)?;
        assert_eq!(decoded, cell);
        Ok(())
    }

    #[test]
    fn test_local_payload_accounting_clamps_to_limit() {
        assert_eq!(RowidLeafCellLayout::local_payload_len_for(100, 64), 64);
        assert_eq!(RowidLeafCellLayout::local_payload_len_for(40, 64), 40);
    }

    #[test]
    fn test_overflow_chain_roundtrip_and_free() -> crate::error::Result<()> {
        let test_db = crate::test_utils::TestDbFile::new("_test_rowid_overflow_chain");
        let mut storage = Pager::new(test_db.path(), 100)?;

        let payload = vec![0xAB; crate::storage::PAGE_SIZE * 2 + 57];
        let first = write_overflow_chain(&mut storage, &payload)?;
        assert!(first.is_some());

        let read_back = read_overflow_chain(&mut storage, first, payload.len())?;
        assert_eq!(read_back, payload);
        let report = validate_overflow_chain(&mut storage, first, payload.len())?;
        assert!(report.page_count >= 3);
        assert!(report.payload_len >= payload.len());

        free_overflow_chain(&mut storage, first)?;
        let reused = storage.allocate_page()?;
        assert_eq!(Some(reused), first);
        Ok(())
    }

    #[test]
    fn test_overflow_chain_validation_detects_cycle() -> crate::error::Result<()> {
        let test_db = crate::test_utils::TestDbFile::new("_test_rowid_overflow_cycle");
        let mut storage = Pager::new(test_db.path(), 100)?;
        let payload = vec![0x44; crate::storage::PAGE_SIZE + 5];
        let first = write_overflow_chain(&mut storage, &payload)?
            .expect("non-empty payload should allocate overflow chain");

        let mut first_page = storage.read_page(first)?;
        first_page.data[4..8].copy_from_slice(&first.to_le_bytes());
        storage.write_page(first_page)?;

        let err = validate_overflow_chain(&mut storage, Some(first), payload.len()).unwrap_err();
        assert!(err.to_string().contains("cycle"));
        Ok(())
    }

    #[test]
    fn test_overflow_chain_validation_detects_truncation() -> crate::error::Result<()> {
        let test_db = crate::test_utils::TestDbFile::new("_test_rowid_overflow_truncation");
        let mut storage = Pager::new(test_db.path(), 100)?;
        let payload = vec![0x55; crate::storage::PAGE_SIZE + 50];
        let first = write_overflow_chain(&mut storage, &payload)?
            .expect("non-empty payload should allocate overflow chain");

        let mut first_page = storage.read_page(first)?;
        first_page.data[4..8].copy_from_slice(&INVALID_PAGE_ID.to_le_bytes());
        storage.write_page(first_page)?;

        let err = validate_overflow_chain(&mut storage, Some(first), payload.len()).unwrap_err();
        assert!(err.to_string().contains("shorter"));
        Ok(())
    }

    #[test]
    fn test_rowid_record_encode_decode_with_local_split() -> crate::error::Result<()> {
        let row = crate::catalog::StoredRow {
            row_id: 501,
            values: vec![
                crate::catalog::Value::Integer(9),
                crate::catalog::Value::Text("x".repeat(300)),
                crate::catalog::Value::Boolean(true),
            ],
        };

        let encoded = encode_stored_row_record(&row, 64)?;
        assert_eq!(encoded.cell.rowid, row.row_id);
        assert_eq!(encoded.cell.local_payload.len(), 64);
        assert!(!encoded.overflow_payload.is_empty());

        let mut full_payload = encoded.cell.local_payload.clone();
        full_payload.extend_from_slice(&encoded.overflow_payload);
        let decoded = decode_stored_row_record(row.row_id, &full_payload)?;
        assert_eq!(decoded, row);
        Ok(())
    }

    #[test]
    fn test_large_row_overflow_reopen_and_reuse() -> crate::error::Result<()> {
        let test_db = crate::test_utils::TestDbFile::new("_test_rowid_large_row_reopen_reuse");

        let cell_page_id = {
            let mut storage = Pager::new(test_db.path(), 100)?;
            let row = crate::catalog::StoredRow {
                row_id: 9001,
                values: vec![
                    crate::catalog::Value::Integer(123),
                    crate::catalog::Value::Text("payload".repeat(1500)),
                    crate::catalog::Value::Boolean(false),
                ],
            };

            let cell_bytes = materialize_row_record_cell(&row, 64, |payload| {
                write_overflow_chain(&mut storage, payload)
            })?;
            let cell = RowidLeafCellLayout::decode(&cell_bytes)?;
            let overflow_ids = collect_overflow_page_ids(
                &mut storage,
                if cell.overflow_first_page == INVALID_PAGE_ID {
                    None
                } else {
                    Some(cell.overflow_first_page)
                },
            )?;
            assert!(!overflow_ids.is_empty());

            let cell_page_id = storage.allocate_page()?;
            let mut page = crate::storage::Page::new(cell_page_id);
            page.data[0..4].copy_from_slice(&(cell_bytes.len() as u32).to_le_bytes());
            page.data[4..4 + cell_bytes.len()].copy_from_slice(&cell_bytes);
            storage.write_page(page)?;
            storage.flush()?;
            cell_page_id
        };

        let mut reopened = Pager::new(test_db.path(), 100)?;
        let page = reopened.read_page(cell_page_id)?;
        let size =
            u32::from_le_bytes([page.data[0], page.data[1], page.data[2], page.data[3]]) as usize;
        let cell_bytes = page.data[4..4 + size].to_vec();
        let restored = hydrate_row_record_cell(&cell_bytes, |first, len| {
            read_overflow_chain(&mut reopened, first, len)
        })?;
        assert_eq!(restored.row_id, 9001);
        assert_eq!(restored.values[0], crate::catalog::Value::Integer(123));

        let cell = RowidLeafCellLayout::decode(&cell_bytes)?;
        let overflow_ids = collect_overflow_page_ids(
            &mut reopened,
            if cell.overflow_first_page == INVALID_PAGE_ID {
                None
            } else {
                Some(cell.overflow_first_page)
            },
        )?;
        free_row_record_overflow(&cell_bytes, |first| {
            free_overflow_chain(&mut reopened, first)
        })?;
        let reused = reopened.allocate_page()?;
        assert!(overflow_ids.contains(&reused));

        Ok(())
    }
}

mod cursor_tests {
    use crate::catalog::cursor::{IndexCursor, IndexEntry, TableCursor};
    use crate::catalog::StoredRow;

    #[test]
    fn test_table_cursor_first_next_seek_current() {
        let rows = vec![
            StoredRow {
                row_id: 20,
                values: vec![crate::catalog::Value::Integer(2)],
            },
            StoredRow {
                row_id: 10,
                values: vec![crate::catalog::Value::Integer(1)],
            },
            StoredRow {
                row_id: 30,
                values: vec![crate::catalog::Value::Integer(3)],
            },
        ];
        let mut cursor = TableCursor::new(rows);

        assert!(cursor.first());
        assert_eq!(cursor.current().map(|r| r.row_id), Some(10));
        assert!(cursor.next());
        assert_eq!(cursor.current().map(|r| r.row_id), Some(20));
        assert!(cursor.seek_rowid(30));
        assert_eq!(cursor.current().map(|r| r.row_id), Some(30));
        assert!(!cursor.next());
        assert!(!cursor.is_valid());
    }

    #[test]
    fn test_index_cursor_first_next_seek_current() {
        let entries = vec![
            IndexEntry {
                key: b"b".to_vec(),
                row_id: 2,
            },
            IndexEntry {
                key: b"a".to_vec(),
                row_id: 1,
            },
            IndexEntry {
                key: b"c".to_vec(),
                row_id: 3,
            },
        ];
        let mut cursor = IndexCursor::new(entries);

        assert!(cursor.first());
        assert_eq!(cursor.current().map(|e| e.key.clone()), Some(b"a".to_vec()));
        assert!(cursor.next());
        assert_eq!(cursor.current().map(|e| e.key.clone()), Some(b"b".to_vec()));
        assert!(cursor.seek_key(b"c"));
        assert_eq!(cursor.current().map(|e| e.row_id), Some(3));
        assert!(!cursor.next());
        assert!(!cursor.is_valid());
    }

    #[test]
    fn test_index_cursor_seek_key_lands_on_first_duplicate() {
        let entries = vec![
            IndexEntry {
                key: b"k".to_vec(),
                row_id: 30,
            },
            IndexEntry {
                key: b"k".to_vec(),
                row_id: 10,
            },
            IndexEntry {
                key: b"k".to_vec(),
                row_id: 20,
            },
        ];
        let mut cursor = IndexCursor::new(entries);

        assert!(cursor.seek_key(b"k"));
        assert_eq!(cursor.current().map(|e| e.row_id), Some(10));
        assert!(cursor.next());
        assert_eq!(cursor.current().map(|e| e.row_id), Some(20));
    }

    #[test]
    fn test_cursor_invariants_for_empty_and_seek_miss() {
        let mut table = TableCursor::new(Vec::new());
        assert!(!table.first());
        assert!(!table.is_valid());
        assert!(!table.seek_rowid(1));
        assert!(table.current().is_none());

        let entries = vec![IndexEntry {
            key: b"k1".to_vec(),
            row_id: 10,
        }];
        let mut index = IndexCursor::new(entries);
        assert!(!index.seek_key(b"missing"));
        assert!(index.current().is_none());
        assert!(!index.next());
    }
}

mod serialization_tests {
    use crate::catalog::serialization::*;
    use crate::catalog::Value;
    use crate::error::{HematiteError, Result};

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
