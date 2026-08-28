//! Centralized tests for the storage module

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

mod wal_tests {
    use crate::storage::file_len_for_next_page_id;
    use crate::storage::metadata_page;
    use crate::storage::pager_metadata::PersistedPagerState;
    use crate::storage::wal::{
        load_visible_state_from_path_with_base, VisibleWalState, WalFile, WalFrame, WalHeader,
        WalPageCache,
    };
    use crate::storage::{JournalMode, PAGE_SIZE, STORAGE_METADATA_PAGE_ID};
    use crate::test_utils::TestDbFile;
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;

    #[test]
    fn test_wal_visible_state_rejects_malformed_committed_metadata_frame(
    ) -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_wal_rejects_malformed_committed_metadata");
        let wal_path = std::path::PathBuf::from(format!("{}.wal", test_db.path()));

        let malformed_payload = vec![0xAA, 0xBB, 0xCC, 0xDD];
        let metadata_page_bytes =
            metadata_page::write_pager_metadata(&vec![0u8; PAGE_SIZE], &malformed_payload)?;

        let wal = WalFile {
            header: WalHeader::default(),
            frames: vec![WalFrame {
                page_id: STORAGE_METADATA_PAGE_ID,
                database_page_count: 2,
                commit_sequence: 1,
                data: metadata_page_bytes,
            }],
        };
        fs::write(&wal_path, wal.encode()?)?;

        let err = load_visible_state_from_path_with_base(
            &wal_path,
            file_len_for_next_page_id(2),
            Vec::new(),
            &vec![0u8; PAGE_SIZE],
        )
        .unwrap_err();
        assert!(matches!(err, crate::error::HematiteError::CorruptedData(_)));
        assert!(err.to_string().contains("malformed"));
        Ok(())
    }

    fn valid_metadata_frame_bytes() -> crate::error::Result<Vec<u8>> {
        let payload = PersistedPagerState {
            journal_mode: JournalMode::Wal,
            free_pages: vec![],
        }
        .encode(1);
        metadata_page::write_pager_metadata(&vec![0u8; PAGE_SIZE], &payload)
    }

    #[test]
    fn wal_page_cache_evicts_least_recently_used_entry() {
        let mut cache = WalPageCache::new(2);
        cache.put(1, Arc::new(vec![1u8; 4]));
        cache.put(2, Arc::new(vec![2u8; 4]));

        // Touch page 1 so it becomes MRU and page 2 becomes the LRU entry.
        assert!(cache.get(1).is_some());
        // Inserting a third page at capacity 2 should evict page 2, not page 1.
        cache.put(3, Arc::new(vec![3u8; 4]));

        assert!(
            cache.peek(2).is_none(),
            "page 2 should have been evicted as the least-recently-used entry"
        );
        assert!(cache.peek(1).is_some());
        assert!(cache.peek(3).is_some());
    }

    #[test]
    fn wal_offset_read_returns_latest_committed_frame_across_commits() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_wal_offset_read_returns_latest_frame");
        let wal_path = std::path::PathBuf::from(format!("{}.wal", test_db.path()));
        let page_id = 5;

        let mut first_page = vec![0u8; PAGE_SIZE];
        first_page[0] = 1;
        let mut second_page = vec![0u8; PAGE_SIZE];
        second_page[0] = 2;

        let wal = WalFile {
            header: WalHeader::default(),
            frames: vec![
                WalFrame::committed(page_id, 6, 1, first_page),
                WalFrame::committed(
                    STORAGE_METADATA_PAGE_ID,
                    6,
                    1,
                    valid_metadata_frame_bytes()?,
                ),
                WalFrame::committed(page_id, 6, 2, second_page.clone()),
                WalFrame::committed(
                    STORAGE_METADATA_PAGE_ID,
                    6,
                    2,
                    valid_metadata_frame_bytes()?,
                ),
            ],
        };
        fs::write(&wal_path, wal.encode()?)?;

        let mut state = load_visible_state_from_path_with_base(
            &wal_path,
            file_len_for_next_page_id(6),
            Vec::new(),
            &vec![0u8; PAGE_SIZE],
        )?
        .expect("visible state should be loaded from the WAL file");

        let offset = state.page_override_offsets[&page_id];
        assert_ne!(
            offset, 0,
            "a real frame offset should be recorded, not the sentinel"
        );

        // The recorded offset must point at the *second* (later) commit's
        // frame, not the first -- i.e. "latest wins" for offsets, matching
        // the existing "latest wins" semantics for cached page data.
        let frame_size = crate::storage::wal::WAL_FRAME_PREFIX_SIZE as u64 + PAGE_SIZE as u64;
        let first_frame_offset = crate::storage::wal::WAL_HEADER_SIZE as u64;
        let second_frame_offset = first_frame_offset + frame_size;
        assert!(
            offset >= second_frame_offset,
            "offset {} should point at the second commit's frame (>= {})",
            offset,
            second_frame_offset
        );

        assert_eq!(
            state.read_page_data_readonly(page_id)?,
            Some(second_page.clone())
        );
        assert_eq!(state.read_page_data(page_id)?, Some(second_page));
        Ok(())
    }

    #[test]
    fn wal_read_page_data_falls_back_to_scan_and_detects_offset_mismatch(
    ) -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_wal_offset_mismatch_detection");
        let wal_path = std::path::PathBuf::from(format!("{}.wal", test_db.path()));
        let page_id = 7;

        let mut page_bytes = vec![0u8; PAGE_SIZE];
        page_bytes[0] = 42;

        let wal = WalFile {
            header: WalHeader::default(),
            frames: vec![
                WalFrame::committed(page_id, 8, 1, page_bytes.clone()),
                WalFrame::committed(
                    STORAGE_METADATA_PAGE_ID,
                    8,
                    1,
                    valid_metadata_frame_bytes()?,
                ),
            ],
        };
        fs::write(&wal_path, wal.encode()?)?;

        // Sentinel offset (0) should still work via the legacy full-file scan.
        let mut sentinel_state =
            VisibleWalState::from_database_state(file_len_for_next_page_id(8), Vec::new());
        sentinel_state.wal_path = Some(wal_path.clone());
        sentinel_state.page_override_offsets.insert(page_id, 0);
        assert_eq!(sentinel_state.read_page_data(page_id)?, Some(page_bytes));

        // An out-of-bounds offset should be treated as a miss, not a panic/error.
        let mut oob_state =
            VisibleWalState::from_database_state(file_len_for_next_page_id(8), Vec::new());
        oob_state.wal_path = Some(wal_path.clone());
        oob_state.page_override_offsets.insert(page_id, 10_000_000);
        assert_eq!(oob_state.read_page_data(page_id)?, None);

        // An in-bounds offset that doesn't actually point at `page_id`'s frame
        // (here, the metadata frame's offset) must be reported as corruption,
        // not silently returned as if it were the requested page's data.
        let frame_size = crate::storage::wal::WAL_FRAME_PREFIX_SIZE as u64 + PAGE_SIZE as u64;
        let metadata_frame_offset = crate::storage::wal::WAL_HEADER_SIZE as u64 + frame_size;
        let mut mismatched_state =
            VisibleWalState::from_database_state(file_len_for_next_page_id(8), Vec::new());
        mismatched_state.wal_path = Some(wal_path);
        mismatched_state
            .page_override_offsets
            .insert(page_id, metadata_frame_offset);
        let err = mismatched_state.read_page_data(page_id).unwrap_err();
        assert!(matches!(err, crate::error::HematiteError::CorruptedData(_)));
        assert!(err.to_string().contains("mismatch"));

        Ok(())
    }
}

mod pager_tests {
    use crate::storage::journal::{JournalRecord, JournalState, RollbackJournal};
    use crate::storage::pager::Pager;
    use crate::storage::pager_metadata::PersistedPagerState;
    use crate::storage::wal::{
        load_visible_state_from_path_with_base, WalFile, WalFrame, WalHeader,
    };
    use crate::storage::{
        metadata_page, JournalMode, Page, PageId, DB_HEADER_PAGE_ID, PAGE_SIZE,
        STORAGE_METADATA_PAGE_ID,
    };
    use crate::test_utils::TestDbFile;
    use std::collections::HashMap;

    use std::fs;

    use std::io::{Seek, SeekFrom, Write};

    use std::sync::{Arc, Barrier, RwLock};
    use std::thread;

    /// The retired FNV-1a checksum, kept only to fabricate a plausible *legacy*
    /// sidecar file. Current checksums are XXH64 (`storage::checksum`); this
    /// deliberately does not track them, because the file it builds is meant to look
    /// like one written by an older version.

    #[test]
    fn test_pager_write_is_buffered_until_flush() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_write_is_buffered");
        let mut pager = Pager::new(test_db.path(), 8)?;

        let page_id = pager.allocate_page()?;
        let mut page = Page::new(page_id);
        page.data[0] = 99;
        pager.write_page(page)?;

        // Without flush, dirty write should not be visible to a fresh pager.
        let before_flush = Pager::new(test_db.path(), 8)?;
        let on_disk_before = before_flush.read_page(page_id)?;
        assert_eq!(on_disk_before.data[0], 0);

        pager.flush()?;

        let after_flush = Pager::new(test_db.path(), 8)?;
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
    fn test_pager_rollback_defers_physical_file_growth_until_flush() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_rollback_defers_file_growth");
        let mut pager = Pager::new(test_db.path(), 32)?;
        let original_len = fs::metadata(test_db.path())?.len();

        pager.begin_transaction()?;
        let page_id = pager.allocate_page()?;
        let mut page = Page::new(page_id);
        page.data[0] = 0x5A;
        pager.write_page(page)?;

        assert_eq!(
            fs::metadata(test_db.path())?.len(),
            original_len,
            "rollback transaction mutated file length before journal sync barrier"
        );

        pager.flush()?;
        assert!(
            fs::metadata(test_db.path())?.len() > original_len,
            "file should grow only after rollback journal sync + flush path"
        );

        pager.rollback_transaction()?;
        assert_eq!(fs::metadata(test_db.path())?.len(), original_len);
        Ok(())
    }

    #[test]
    fn test_pager_rejects_reserved_page_deallocation() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_rejects_reserved_deallocate");
        let mut pager = Pager::new(test_db.path(), 8)?;

        let err = pager.deallocate_page(DB_HEADER_PAGE_ID).unwrap_err();
        assert!(err
            .to_string()
            .contains("Cannot deallocate reserved page 0"));

        let err = pager.deallocate_page(STORAGE_METADATA_PAGE_ID).unwrap_err();
        assert!(err
            .to_string()
            .contains("Cannot deallocate reserved page 1"));

        pager.begin_transaction()?;
        let err = pager.deallocate_page(DB_HEADER_PAGE_ID).unwrap_err();
        assert!(err
            .to_string()
            .contains("Cannot deallocate reserved page 0"));
        pager.rollback_transaction()?;
        Ok(())
    }

    #[test]
    fn test_pager_wal_commit_updates_visible_state_without_full_reload() -> crate::error::Result<()>
    {
        let test_db = TestDbFile::new("_test_pager_wal_commit_incremental_visible_state");
        let mut pager = Pager::new(test_db.path(), 16)?;
        let page_id = pager.allocate_page()?;
        let mut page = Page::new(page_id);
        page.data[0] = 1;
        pager.write_page(page)?;
        pager.flush()?;
        pager.set_journal_mode(JournalMode::Wal)?;

        for step in 0..6u8 {
            pager.begin_transaction()?;
            let mut updated = pager.read_page(page_id)?;
            updated.data[0] = step.saturating_add(2);
            pager.write_page(updated)?;

            let reloads_before_commit = pager.wal_visible_state_reload_count();
            pager.commit_transaction()?;
            let reloads_after_commit = pager.wal_visible_state_reload_count();

            assert_eq!(
                reloads_after_commit, reloads_before_commit,
                "WAL commit should not reload full visible state from disk"
            );
        }

        Ok(())
    }

    #[test]
    fn test_pager_wal_commit_handles_large_checksum_metadata() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_wal_commit_large_checksum_metadata");
        let target_page_id = {
            let mut pager = Pager::new(test_db.path(), 64)?;
            let mut target_page_id = None;

            for index in 0..900u32 {
                let page_id = pager.allocate_page()?;
                let mut page = Page::new(page_id);
                page.data[0] = (index % 251) as u8;
                pager.write_page(page)?;
                if index == 450 {
                    target_page_id = Some(page_id);
                }
            }

            pager.flush()?;
            pager.set_journal_mode(JournalMode::Wal)?;

            let target_page_id = target_page_id.expect("target page should be assigned");
            pager.begin_transaction()?;
            let mut updated = pager.read_page(target_page_id)?;
            updated.data[0] = 222;
            pager.write_page(updated)?;
            pager.commit_transaction()?;
            target_page_id
        };

        let mut reopened = Pager::new(test_db.path(), 64)?;
        reopened.begin_read()?;
        assert_eq!(reopened.read_page(target_page_id)?.data[0], 222);
        reopened.end_read()?;
        Ok(())
    }

    #[test]
    fn test_pager_wal_commit_beyond_cache_capacity_reads_correctly_after_eviction(
    ) -> crate::error::Result<()> {
        // WAL_PAGE_CACHE_CAPACITY is 64; committing more distinct dirty pages
        // than that in one transaction forces the earliest-committed pages
        // out of the in-memory WAL page cache. Reading them back must fall
        // through to an on-demand read from the WAL file at the real byte
        // offset recorded during commit, not the page's stale pre-transaction
        // content.
        let test_db = TestDbFile::new("_test_pager_wal_commit_beyond_cache_capacity");
        let mut pager = Pager::new(test_db.path(), 64)?;
        let page_ids: Vec<PageId> = (0..100)
            .map(|_| pager.allocate_page())
            .collect::<crate::error::Result<_>>()?;
        pager.flush()?;
        pager.set_journal_mode(JournalMode::Wal)?;

        pager.begin_transaction()?;
        for (index, &page_id) in page_ids.iter().enumerate() {
            let mut page = pager.read_page(page_id)?;
            page.data[0] = (index % 251) as u8;
            pager.write_page(page)?;
        }
        pager.commit_transaction()?;

        pager.begin_read()?;
        for (index, &page_id) in page_ids.iter().enumerate() {
            assert_eq!(
                pager.read_page(page_id)?.data[0],
                (index % 251) as u8,
                "page {} (dirtied {}th in a {}-page commit) should read back its \
                 committed value even after eviction from the WAL page cache",
                page_id,
                index,
                page_ids.len()
            );
        }
        pager.end_read()?;
        Ok(())
    }

    #[test]
    fn test_pager_wal_begin_transaction_skips_reload_when_wal_unchanged() -> crate::error::Result<()>
    {
        let test_db = TestDbFile::new("_test_pager_wal_begin_transaction_skips_reload");
        let mut pager = Pager::new(test_db.path(), 16)?;
        let page_id = pager.allocate_page()?;
        let mut page = Page::new(page_id);
        page.data[0] = 7;
        pager.write_page(page)?;
        pager.flush()?;
        pager.set_journal_mode(JournalMode::Wal)?;

        pager.begin_transaction()?;
        let mut updated = pager.read_page(page_id)?;
        updated.data[0] = 8;
        pager.write_page(updated)?;
        pager.commit_transaction()?;

        let reloads_before = pager.wal_visible_state_reload_count();
        pager.begin_transaction()?;
        let reloads_after_begin = pager.wal_visible_state_reload_count();
        assert_eq!(reloads_after_begin, reloads_before);
        assert_eq!(pager.read_page(page_id)?.data[0], 8);
        pager.rollback_transaction()?;
        let reloads_after_rollback = pager.wal_visible_state_reload_count();
        assert_eq!(reloads_after_rollback, reloads_after_begin);

        Ok(())
    }

    #[test]
    fn test_pager_large_transaction_stays_cache_bounded_under_pressure() -> crate::error::Result<()>
    {
        let test_db = TestDbFile::new("_test_pager_large_tx_cache_pressure");
        let mut pager = Pager::new(test_db.path(), 8)?;
        pager.begin_transaction()?;

        let mut peak_cached_pages = 0usize;
        for i in 0..160u32 {
            let page_id = pager.allocate_page()?;
            let mut page = Page::new(page_id);
            page.data[0..4].copy_from_slice(&i.to_le_bytes());
            pager.write_page(page)?;
            peak_cached_pages = peak_cached_pages.max(pager.cached_page_count());
        }

        assert!(
            peak_cached_pages <= 24,
            "cache grew unexpectedly under spill pressure: {} pages",
            peak_cached_pages
        );

        pager.rollback_transaction()?;
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

        let reopened = Pager::new(test_db.path(), 8)?;
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

        let reopened = Pager::new(test_db.path(), 8)?;
        let page = reopened.read_page(page_id)?;
        assert_eq!(page.data[0], 10);
        Ok(())
    }

    #[test]
    fn test_pager_rollback_journal_is_created_on_begin_transaction() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_rollback_journal_created_on_begin");
        let journal_path = std::path::PathBuf::from(format!("{}.journal", test_db.path()));

        let mut pager = Pager::new(test_db.path(), 8)?;
        let page_id = pager.allocate_page()?;
        let mut page = Page::new(page_id);
        page.data[0] = 33;
        pager.write_page(page)?;
        pager.flush()?;

        pager.begin_transaction()?;
        assert!(journal_path.exists());

        let journal = RollbackJournal::decode(&fs::read(&journal_path)?)?;
        assert_eq!(journal.state, JournalState::Active);
        assert_eq!(journal.page_records.len(), 0);
        assert!(journal.original_file_len >= crate::storage::file_len_for_next_page_id(3));

        pager.rollback_transaction()?;
        Ok(())
    }

    #[test]
    fn test_pager_rollback_journal_records_original_pages_once() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_rollback_journal_records_original_pages_once");
        let journal_path = std::path::PathBuf::from(format!("{}.journal", test_db.path()));

        let mut pager = Pager::new(test_db.path(), 8)?;
        let first_page_id = pager.allocate_page()?;
        let second_page_id = pager.allocate_page()?;

        let mut first_page = Page::new(first_page_id);
        first_page.data[0] = 11;
        pager.write_page(first_page)?;

        let mut second_page = Page::new(second_page_id);
        second_page.data[0] = 22;
        pager.write_page(second_page)?;
        pager.flush()?;

        pager.begin_transaction()?;

        let mut updated_first = pager.read_page(first_page_id)?;
        updated_first.data[0] = 99;
        pager.write_page(updated_first)?;

        let mut updated_first_again = pager.read_page(first_page_id)?;
        updated_first_again.data[1] = 88;
        pager.write_page(updated_first_again)?;

        let mut updated_second = pager.read_page(second_page_id)?;
        updated_second.data[0] = 77;
        pager.write_page(updated_second)?;

        let journal = RollbackJournal::decode(&fs::read(&journal_path)?)?;
        assert_eq!(journal.state, JournalState::Active);
        assert_eq!(journal.page_records.len(), 2);

        let first_record = journal
            .page_records
            .iter()
            .find(|record| record.page_id == first_page_id)
            .expect("first page should be journaled");
        let second_record = journal
            .page_records
            .iter()
            .find(|record| record.page_id == second_page_id)
            .expect("second page should be journaled");

        assert_eq!(first_record.data[0], 11);
        assert_eq!(second_record.data[0], 22);

        pager.rollback_transaction()?;
        Ok(())
    }

    #[test]
    fn test_pager_recovery_rolls_back_multiple_pages_from_active_journal(
    ) -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_recovery_rolls_back_multiple_pages");
        let (first_page_id, second_page_id) = {
            let mut pager = Pager::new(test_db.path(), 8)?;
            let first_page_id = pager.allocate_page()?;
            let second_page_id = pager.allocate_page()?;

            let mut first_page = Page::new(first_page_id);
            first_page.data[0] = 10;
            pager.write_page(first_page)?;

            let mut second_page = Page::new(second_page_id);
            second_page.data[0] = 20;
            pager.write_page(second_page)?;
            pager.flush()?;

            pager.begin_transaction()?;
            let mut updated_first = pager.read_page(first_page_id)?;
            updated_first.data[0] = 50;
            pager.write_page(updated_first)?;

            let mut updated_second = pager.read_page(second_page_id)?;
            updated_second.data[0] = 60;
            pager.write_page(updated_second)?;

            (first_page_id, second_page_id)
        };

        let reopened = Pager::new(test_db.path(), 8)?;
        assert_eq!(reopened.read_page(first_page_id)?.data[0], 10);
        assert_eq!(reopened.read_page(second_page_id)?.data[0], 20);
        Ok(())
    }

    #[test]
    fn test_pager_open_discards_committed_journal_without_rollback() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_open_discards_committed_journal");
        let journal_path = std::path::PathBuf::from(format!("{}.journal", test_db.path()));

        let (page_id, file_len, free_pages) = {
            let mut pager = Pager::new(test_db.path(), 8)?;
            let page_id = pager.allocate_page()?;
            let mut page = Page::new(page_id);
            page.data[0] = 55;
            pager.write_page(page)?;
            pager.flush()?;
            (page_id, pager.file_len()?, pager.free_pages().to_vec())
        };

        let journal = RollbackJournal {
            state: JournalState::Committed,
            original_file_len: file_len,
            original_free_pages: free_pages,
            commit_intents: Vec::new(),
            page_records: vec![JournalRecord {
                page_id,
                data: vec![1u8; crate::storage::PAGE_SIZE],
            }],
        };
        fs::write(&journal_path, journal.encode()?)?;

        let reopened = Pager::new(test_db.path(), 8)?;
        assert_eq!(reopened.read_page(page_id)?.data[0], 55);
        assert!(!journal_path.exists());
        Ok(())
    }

    /// A committed transaction empties its journal rather than deleting it, and
    /// an empty journal means "nothing to recover".
    ///
    /// Deleting it cost an `unlink` at every commit and an `openat` plus a
    /// truncating `open` at the next `BEGIN` — three syscalls per transaction to
    /// arrive back at a file this pager already had open, and `unlink` was the
    /// most expensive of the three.
    #[test]
    fn a_committed_transaction_empties_its_journal_and_keeps_it() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_journal_retired_not_removed");
        let journal_path = std::path::PathBuf::from(format!("{}.journal", test_db.path()));

        let mut pager = Pager::new(test_db.path(), 8)?;
        let page_id = pager.allocate_page()?;
        pager.begin_transaction()?;
        let mut page = Page::new(page_id);
        page.data[0] = 7;
        pager.write_page(page)?;
        pager.commit_transaction()?;

        pager.begin_transaction()?;
        let mut page = Page::new(page_id);
        page.data[0] = 8;
        pager.write_page(page)?;
        pager.commit_transaction()?;

        assert!(
            journal_path.exists(),
            "the journal should survive a commit, to be reused by the next transaction"
        );
        assert_eq!(
            fs::metadata(&journal_path)?.len(),
            0,
            "a retired journal should be empty"
        );

        // And an empty one is not something to recover from: reopening must leave
        // the committed page alone rather than replaying anything.
        drop(pager);
        let mut reopened = Pager::new(test_db.path(), 8)?;
        reopened.begin_read()?;
        assert_eq!(reopened.read_page(page_id)?.data[0], 8);
        reopened.end_read()?;
        Ok(())
    }

    /// An empty journal is ignored; a non-empty one that does not parse is still
    /// an error. Retiring a journal must not have turned corruption into silence.
    #[test]
    fn an_empty_journal_is_ignored_but_a_corrupt_one_is_not() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_empty_journal_ignored");
        let journal_path = std::path::PathBuf::from(format!("{}.journal", test_db.path()));

        let mut pager = Pager::new(test_db.path(), 8)?;
        let page_id = pager.allocate_page()?;
        pager.begin_transaction()?;
        let mut page = Page::new(page_id);
        page.data[0] = 42;
        pager.write_page(page)?;
        pager.commit_transaction()?;
        drop(pager);

        fs::write(&journal_path, b"")?;
        let mut reopened = Pager::new(test_db.path(), 8)?;
        reopened.begin_read()?;
        assert_eq!(reopened.read_page(page_id)?.data[0], 42);
        reopened.end_read()?;
        drop(reopened);

        fs::write(&journal_path, b"not a journal at all")?;
        assert!(
            Pager::new(test_db.path(), 8).is_err(),
            "a journal with unrecognisable contents should still be rejected"
        );
        Ok(())
    }

    #[test]
    fn test_pager_open_rejects_truncated_rollback_journal() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_open_rejects_truncated_journal");
        let journal_path = std::path::PathBuf::from(format!("{}.journal", test_db.path()));

        let mut pager = Pager::new(test_db.path(), 8)?;
        pager.flush()?;
        drop(pager);

        fs::write(&journal_path, b"HTJ3\x00\x00")?;

        let err = Pager::new(test_db.path(), 8).unwrap_err();
        assert!(err.to_string().contains("truncated"));
        Ok(())
    }

    #[test]
    fn test_pager_restore_snapshot_keeps_rollback_records_needed_for_later_rollback(
    ) -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_restore_snapshot_rewrites_journal");
        let journal_path = std::path::PathBuf::from(format!("{}.journal", test_db.path()));

        let mut pager = Pager::new(test_db.path(), 8)?;
        let first_page_id = pager.allocate_page()?;
        let second_page_id = pager.allocate_page()?;

        let mut first_page = Page::new(first_page_id);
        first_page.data[0] = 10;
        pager.write_page(first_page)?;

        let mut second_page = Page::new(second_page_id);
        second_page.data[0] = 20;
        pager.write_page(second_page)?;
        pager.flush()?;

        pager.begin_transaction()?;

        let mut updated_first = pager.read_page(first_page_id)?;
        updated_first.data[0] = 50;
        pager.write_page(updated_first)?;

        let snapshot = pager.snapshot()?;

        let mut updated_second = pager.read_page(second_page_id)?;
        updated_second.data[0] = 60;
        pager.write_page(updated_second)?;

        let journal = RollbackJournal::decode(&fs::read(&journal_path)?)?;
        assert_eq!(journal.page_records.len(), 2);

        pager.restore_snapshot(snapshot)?;

        let rewritten_journal = RollbackJournal::decode(&fs::read(&journal_path)?)?;
        assert_eq!(rewritten_journal.page_records.len(), 2);
        assert_eq!(rewritten_journal.page_records[0].page_id, first_page_id);
        assert_eq!(rewritten_journal.page_records[1].page_id, second_page_id);

        pager.rollback_transaction()?;
        assert_eq!(pager.read_page(first_page_id)?.data[0], 10);
        assert_eq!(pager.read_page(second_page_id)?.data[0], 20);
        Ok(())
    }

    #[test]
    fn test_pager_open_does_not_recover_live_writer_journal() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_open_skips_live_writer_journal");
        let journal_path = std::path::PathBuf::from(format!("{}.journal", test_db.path()));

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

        let journal_before_open = RollbackJournal::decode(&fs::read(&journal_path)?)?;
        assert_eq!(journal_before_open.page_records.len(), 1);

        let _observer = Pager::new(test_db.path(), 8)?;

        let journal_after_open = RollbackJournal::decode(&fs::read(&journal_path)?)?;
        assert_eq!(journal_after_open.page_records.len(), 1);

        writer.commit_transaction()?;

        let reopened = Pager::new(test_db.path(), 8)?;
        assert_eq!(reopened.read_page(page_id)?.data[0], 55);
        Ok(())
    }

    #[test]
    fn test_pager_restore_snapshot_recovers_flushed_post_savepoint_page_changes(
    ) -> crate::error::Result<()> {
        let test_db =
            TestDbFile::new("_test_pager_restore_snapshot_recovers_flushed_post_savepoint_changes");
        let mut pager = Pager::new(test_db.path(), 8)?;
        let page_id = pager.allocate_page()?;

        let mut page = Page::new(page_id);
        page.data[0] = 10;
        pager.write_page(page)?;
        pager.flush()?;

        pager.begin_transaction()?;
        let snapshot = pager.snapshot()?;

        let mut updated = pager.read_page(page_id)?;
        updated.data[0] = 42;
        pager.write_page(updated)?;
        pager.flush()?;

        pager.restore_snapshot(snapshot)?;
        assert_eq!(pager.read_page(page_id)?.data[0], 10);

        pager.commit_transaction()?;

        let reopened = Pager::new(test_db.path(), 8)?;
        assert_eq!(reopened.read_page(page_id)?.data[0], 10);
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
    fn test_shared_pager_rwlock_allows_parallel_read_guards_for_page_reads(
    ) -> crate::error::Result<()> {
        let shared = Arc::new(RwLock::new(Pager::new_in_memory(8)?));
        let page_id = {
            let mut pager = shared.write().map_err(|_| {
                crate::error::HematiteError::InternalError(
                    "Shared pager write lock is poisoned".to_string(),
                )
            })?;
            let page_id = pager.allocate_page()?;
            let mut page = Page::new(page_id);
            page.data[0] = 42;
            pager.write_page(page)?;
            pager.flush()?;
            page_id
        };

        let start = Arc::new(Barrier::new(3));
        let overlap = Arc::new(Barrier::new(3));
        let mut handles = Vec::new();

        for _ in 0..2 {
            let shared = Arc::clone(&shared);
            let start = Arc::clone(&start);
            let overlap = Arc::clone(&overlap);
            handles.push(thread::spawn(move || -> crate::error::Result<u8> {
                start.wait();
                let pager = shared.read().map_err(|_| {
                    crate::error::HematiteError::InternalError(
                        "Shared pager read lock is poisoned".to_string(),
                    )
                })?;
                overlap.wait();
                Ok(pager.read_page(page_id)?.data[0])
            }));
        }

        start.wait();
        overlap.wait();

        for handle in handles {
            assert_eq!(handle.join().expect("reader thread panicked")?, 42);
        }

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
    fn test_pager_journal_mode_persists_across_reopen() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_journal_mode_persists");

        let mut pager = Pager::new(test_db.path(), 8)?;
        assert_eq!(pager.journal_mode(), JournalMode::Rollback);
        pager.set_journal_mode(JournalMode::Wal)?;
        drop(pager);

        let reopened = Pager::new(test_db.path(), 8)?;
        assert_eq!(reopened.journal_mode(), JournalMode::Wal);
        Ok(())
    }

    #[test]
    fn test_pager_invalid_journal_mode_metadata_is_rejected() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_invalid_journal_mode_metadata");
        let mut pager = Pager::new(test_db.path(), 8)?;
        pager.flush()?;
        drop(pager);

        let mut invalid_payload = PersistedPagerState {
            journal_mode: JournalMode::Rollback,
            free_pages: vec![],
        }
        .encode(1);
        if invalid_payload.len() > 8 {
            invalid_payload[8] = 9;
        }
        let invalid = metadata_page::write_pager_metadata(&vec![0; PAGE_SIZE], &invalid_payload)?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(test_db.path())?;
        file.seek(SeekFrom::Start(
            STORAGE_METADATA_PAGE_ID as u64 * PAGE_SIZE as u64,
        ))?;
        file.write_all(&invalid)?;
        file.sync_all()?;

        let err = Pager::new(test_db.path(), 8).unwrap_err();
        assert!(err.to_string().contains("Unsupported pager journal mode"));
        Ok(())
    }

    #[test]
    fn test_pager_open_rejects_metadata_page_without_magic() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_legacy_reserved_metadata_layout");
        let pager = Pager::new(test_db.path(), 8)?;
        drop(pager);

        let payload = b"not a metadata page";
        let mut unmagicked_page = vec![0; PAGE_SIZE];
        unmagicked_page[0..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        unmagicked_page[4..4 + payload.len()].copy_from_slice(payload);

        let mut db_file = std::fs::OpenOptions::new()
            .write(true)
            .open(test_db.path())?;
        db_file.seek(SeekFrom::Start(
            STORAGE_METADATA_PAGE_ID as u64 * PAGE_SIZE as u64,
        ))?;
        db_file.write_all(&unmagicked_page)?;
        db_file.sync_all()?;

        let err = Pager::new(test_db.path(), 8).unwrap_err();
        assert!(err
            .to_string()
            .contains("does not carry the expected magic"));
        Ok(())
    }

    #[test]
    fn test_pager_open_rejects_malformed_committed_wal_metadata_frame() -> crate::error::Result<()>
    {
        let test_db = TestDbFile::new("_test_pager_rejects_malformed_committed_wal_metadata");
        {
            let mut pager = Pager::new(test_db.path(), 8)?;
            pager.set_journal_mode(JournalMode::Wal)?;
            pager.flush()?;
        }

        let wal_path = std::path::PathBuf::from(format!("{}.wal", test_db.path()));
        let malformed_payload = vec![0xAA, 0xBB, 0xCC, 0xDD];
        let metadata_page_bytes =
            metadata_page::write_pager_metadata(&vec![0u8; PAGE_SIZE], &malformed_payload)?;
        let wal = WalFile {
            header: WalHeader::default(),
            frames: vec![WalFrame {
                page_id: STORAGE_METADATA_PAGE_ID,
                database_page_count: 2,
                commit_sequence: 1,
                data: metadata_page_bytes,
            }],
        };
        fs::write(&wal_path, wal.encode()?)?;

        let err = Pager::new(test_db.path(), 8).unwrap_err();
        assert!(err.to_string().contains("malformed"));
        Ok(())
    }

    #[test]
    fn test_pager_wal_mode_allows_reader_while_writer_is_active() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_wal_reader_while_writer");
        let mut writer = Pager::new(test_db.path(), 8)?;
        let mut reader = Pager::new(test_db.path(), 8)?;

        writer.set_journal_mode(JournalMode::Wal)?;
        reader.set_journal_mode(JournalMode::Wal)?;

        writer.begin_transaction()?;
        reader.begin_read()?;
        assert_eq!(reader.wal_snapshot_sequence(), Some(0));
        reader.end_read()?;
        writer.rollback_transaction()?;
        Ok(())
    }

    #[test]
    fn test_pager_wal_mode_blocks_second_writer() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_wal_second_writer");
        let mut first_writer = Pager::new(test_db.path(), 8)?;
        let mut second_writer = Pager::new(test_db.path(), 8)?;

        first_writer.set_journal_mode(JournalMode::Wal)?;
        second_writer.set_journal_mode(JournalMode::Wal)?;

        first_writer.begin_transaction()?;
        let err = second_writer.begin_transaction().unwrap_err();
        assert!(err.to_string().contains("locked"));
        first_writer.rollback_transaction()?;
        Ok(())
    }

    #[test]
    fn test_pager_wal_refreshes_stale_writer_state_before_committing() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_wal_refreshes_stale_writer_state");
        let wal_path = std::path::PathBuf::from(format!("{}.wal", test_db.path()));

        let mut first_writer = Pager::new(test_db.path(), 8)?;
        let mut second_writer = Pager::new(test_db.path(), 8)?;
        first_writer.set_journal_mode(JournalMode::Wal)?;
        second_writer.set_journal_mode(JournalMode::Wal)?;

        let mut pinned_reader = Pager::new(test_db.path(), 8)?;
        pinned_reader.begin_read()?;

        first_writer.begin_transaction()?;
        let page_id = first_writer.allocate_page()?;
        let mut first_page = Page::new(page_id);
        first_page.data[0] = 1;
        first_writer.write_page(first_page)?;
        first_writer.commit_transaction()?;

        second_writer.begin_transaction()?;
        let mut updated_page = second_writer.read_page(page_id)?;
        assert_eq!(updated_page.data[0], 1);
        updated_page.data[0] = 2;
        second_writer.write_page(updated_page)?;
        second_writer.commit_transaction()?;

        let state = load_visible_state_from_path_with_base(
            &wal_path,
            crate::storage::file_len_for_next_page_id(3),
            Vec::new(),
            &vec![0u8; PAGE_SIZE],
        )?
        .expect("WAL state should exist after committed transactions");
        assert_eq!(state.visible_sequence, 2);

        pinned_reader.end_read()?;

        let mut reader = Pager::new(test_db.path(), 8)?;
        reader.begin_read()?;
        assert_eq!(reader.read_page(page_id)?.data[0], 2);
        reader.end_read()?;
        Ok(())
    }

    #[test]
    fn test_pager_reads_committed_wal_page_images() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_reads_committed_wal_page_images");
        let wal_path = std::path::PathBuf::from(format!("{}.wal", test_db.path()));

        let page_id = {
            let mut pager = Pager::new(test_db.path(), 8)?;
            let page_id = pager.allocate_page()?;
            let mut page = Page::new(page_id);
            page.data[0] = 10;
            pager.write_page(page)?;
            pager.flush()?;
            pager.set_journal_mode(JournalMode::Wal)?;
            page_id
        };

        let metadata_page = {
            let pager = Pager::new(test_db.path(), 8)?;
            pager
                .read_page(crate::storage::STORAGE_METADATA_PAGE_ID)?
                .data
                .to_vec()
        };
        let mut wal_page = vec![0u8; crate::storage::PAGE_SIZE];
        wal_page[0] = 20;
        let frame = WalFrame::committed(page_id, 4, 1, wal_page);
        let metadata_frame = WalFrame::committed(
            crate::storage::STORAGE_METADATA_PAGE_ID,
            4,
            1,
            metadata_page,
        );
        let wal = WalFile {
            header: WalHeader::default(),
            frames: vec![frame, metadata_frame],
        };
        fs::write(&wal_path, wal.encode()?)?;

        let mut reader = Pager::new(test_db.path(), 8)?;
        reader.begin_read()?;
        let page = reader.read_page(page_id)?;
        assert_eq!(page.data[0], 20);
        reader.end_read()?;
        Ok(())
    }

    #[test]
    fn test_pager_wal_reader_snapshot_stays_stable_across_newer_commits() -> crate::error::Result<()>
    {
        let test_db = TestDbFile::new("_test_pager_wal_reader_snapshot_stability");
        let wal_path = std::path::PathBuf::from(format!("{}.wal", test_db.path()));

        let page_id = {
            let mut pager = Pager::new(test_db.path(), 8)?;
            let page_id = pager.allocate_page()?;
            let mut page = Page::new(page_id);
            page.data[0] = 10;
            pager.write_page(page)?;
            pager.flush()?;
            pager.set_journal_mode(JournalMode::Wal)?;
            page_id
        };

        let metadata_page = {
            let pager = Pager::new(test_db.path(), 8)?;
            pager
                .read_page(crate::storage::STORAGE_METADATA_PAGE_ID)?
                .data
                .to_vec()
        };
        let mut first_wal_page = vec![0u8; crate::storage::PAGE_SIZE];
        first_wal_page[0] = 20;
        let frame1 = WalFrame::committed(page_id, 4, 1, first_wal_page);
        let metadata_frame_1 = WalFrame::committed(
            crate::storage::STORAGE_METADATA_PAGE_ID,
            4,
            1,
            metadata_page.clone(),
        );
        let wal1 = WalFile {
            header: WalHeader::default(),
            frames: vec![frame1.clone(), metadata_frame_1.clone()],
        };
        fs::write(&wal_path, wal1.encode()?)?;

        let mut first_reader = Pager::new(test_db.path(), 8)?;
        first_reader.begin_read()?;
        assert_eq!(first_reader.read_page(page_id)?.data[0], 20);

        let mut second_wal_page = vec![0u8; crate::storage::PAGE_SIZE];
        second_wal_page[0] = 30;
        let frame2 = WalFrame::committed(page_id, 4, 2, second_wal_page);
        let metadata_frame_2 = WalFrame::committed(
            crate::storage::STORAGE_METADATA_PAGE_ID,
            4,
            2,
            metadata_page,
        );
        let wal2 = WalFile {
            header: WalHeader::default(),
            frames: vec![frame1, metadata_frame_1, frame2, metadata_frame_2],
        };
        fs::write(&wal_path, wal2.encode()?)?;

        let mut second_reader = Pager::new(test_db.path(), 8)?;
        second_reader.begin_read()?;
        assert_eq!(second_reader.read_page(page_id)?.data[0], 30);
        second_reader.end_read()?;

        assert_eq!(first_reader.read_page(page_id)?.data[0], 20);
        first_reader.end_read()?;
        Ok(())
    }

    #[test]
    fn test_pager_wal_reader_keeps_cache_between_identical_read_scopes() -> crate::error::Result<()>
    {
        let test_db = TestDbFile::new("_test_pager_wal_reader_keeps_cache_between_scopes");

        let page_id = {
            let mut pager = Pager::new(test_db.path(), 8)?;
            pager.set_journal_mode(JournalMode::Wal)?;
            pager.begin_transaction()?;
            let page_id = pager.allocate_page()?;
            let mut page = Page::new(page_id);
            page.data[0] = 55;
            pager.write_page(page)?;
            pager.commit_transaction()?;
            page_id
        };

        let mut reader = Pager::new(test_db.path(), 8)?;
        reader.set_journal_mode(JournalMode::Wal)?;

        reader.begin_read()?;
        assert_eq!(reader.read_page(page_id)?.data[0], 55);
        reader.end_read()?;

        let retained_entries = reader.cached_page_count();
        assert!(retained_entries > 0);

        reader.begin_read()?;
        assert_eq!(reader.cached_page_count(), retained_entries);
        assert_eq!(reader.read_page(page_id)?.data[0], 55);
        reader.end_read()?;
        Ok(())
    }

    #[test]
    fn test_pager_wal_begin_read_skips_reload_when_wal_unchanged() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_wal_begin_read_skips_reload");

        let page_id = {
            let mut pager = Pager::new(test_db.path(), 8)?;
            pager.set_journal_mode(JournalMode::Wal)?;
            pager.begin_transaction()?;
            let page_id = pager.allocate_page()?;
            let mut page = Page::new(page_id);
            page.data[0] = 42;
            pager.write_page(page)?;
            pager.commit_transaction()?;
            page_id
        };

        let mut reader = Pager::new(test_db.path(), 8)?;
        reader.set_journal_mode(JournalMode::Wal)?;

        reader.begin_read()?;
        assert_eq!(reader.read_page(page_id)?.data[0], 42);
        reader.end_read()?;

        let reloads_before = reader.wal_visible_state_reload_count();
        reader.begin_read()?;
        let reloads_after_begin = reader.wal_visible_state_reload_count();
        assert_eq!(reloads_after_begin, reloads_before);
        assert_eq!(reader.read_page(page_id)?.data[0], 42);
        reader.end_read()?;

        Ok(())
    }

    #[test]
    fn test_pager_wal_reader_reloads_page_after_sequence_advance_without_cache_reset(
    ) -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_wal_reader_reloads_after_sequence_advance");

        let mut writer = Pager::new(test_db.path(), 8)?;
        writer.set_journal_mode(JournalMode::Wal)?;
        writer.begin_transaction()?;
        let page_id = writer.allocate_page()?;
        let mut page = Page::new(page_id);
        page.data[0] = 10;
        writer.write_page(page)?;
        writer.commit_transaction()?;

        let mut reader = Pager::new(test_db.path(), 8)?;
        reader.set_journal_mode(JournalMode::Wal)?;
        reader.begin_read()?;
        assert_eq!(reader.read_page(page_id)?.data[0], 10);
        reader.end_read()?;
        assert!(reader.cached_page_count() > 0);

        writer.begin_transaction()?;
        let mut updated = writer.read_page(page_id)?;
        updated.data[0] = 99;
        writer.write_page(updated)?;
        writer.commit_transaction()?;

        reader.begin_read()?;
        assert_eq!(reader.read_page(page_id)?.data[0], 99);
        reader.end_read()?;
        Ok(())
    }

    #[test]
    fn test_pager_wal_commit_persists_committed_state_for_reopen() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_wal_commit_persists");
        let wal_path = std::path::PathBuf::from(format!("{}.wal", test_db.path()));

        let page_id = {
            let mut pager = Pager::new(test_db.path(), 8)?;
            let page_id = pager.allocate_page()?;
            let mut page = Page::new(page_id);
            page.data[0] = 10;
            pager.write_page(page)?;
            pager.flush()?;
            pager.set_journal_mode(JournalMode::Wal)?;

            pager.begin_transaction()?;
            let mut updated = pager.read_page(page_id)?;
            updated.data[0] = 99;
            pager.write_page(updated)?;
            pager.commit_transaction()?;
            page_id
        };

        assert!(wal_path.exists());

        let mut reopened = Pager::new(test_db.path(), 8)?;
        reopened.begin_read()?;
        let page = reopened.read_page(page_id)?;
        assert_eq!(page.data[0], 99);
        reopened.end_read()?;
        Ok(())
    }

    #[test]
    fn test_pager_wal_checkpoint_waits_for_active_readers() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_wal_checkpoint_waits_for_readers");
        let wal_path = std::path::PathBuf::from(format!("{}.wal", test_db.path()));

        let page_id = {
            let mut pager = Pager::new(test_db.path(), 8)?;
            let page_id = pager.allocate_page()?;
            let mut page = Page::new(page_id);
            page.data[0] = 10;
            pager.write_page(page)?;
            pager.flush()?;
            pager.set_journal_mode(JournalMode::Wal)?;
            page_id
        };

        let mut reader = Pager::new(test_db.path(), 8)?;
        reader.begin_read()?;

        let mut writer = Pager::new(test_db.path(), 8)?;
        writer.begin_transaction()?;
        let mut updated = writer.read_page(page_id)?;
        updated.data[0] = 99;
        writer.write_page(updated)?;
        writer.commit_transaction()?;

        assert!(wal_path.exists());
        let err = writer.checkpoint_wal().unwrap_err();
        assert!(err.to_string().contains("readers are active"));

        assert_eq!(reader.read_page(page_id)?.data[0], 10);
        reader.end_read()?;

        writer.checkpoint_wal()?;
        assert!(!wal_path.exists());

        let mut reopened = Pager::new(test_db.path(), 8)?;
        reopened.begin_read()?;
        assert_eq!(reopened.read_page(page_id)?.data[0], 99);
        reopened.end_read()?;
        Ok(())
    }

    #[test]
    fn test_pager_wal_checkpoint_allows_reader_on_latest_sequence() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_wal_checkpoint_allows_latest_reader");
        let wal_path = std::path::PathBuf::from(format!("{}.wal", test_db.path()));

        let page_id = {
            let mut pager = Pager::new(test_db.path(), 8)?;
            let page_id = pager.allocate_page()?;
            let mut page = Page::new(page_id);
            page.data[0] = 10;
            pager.write_page(page)?;
            pager.flush()?;
            pager.set_journal_mode(JournalMode::Wal)?;
            page_id
        };

        let mut stale_reader = Pager::new(test_db.path(), 8)?;
        stale_reader.begin_read()?;

        let mut writer = Pager::new(test_db.path(), 8)?;
        writer.begin_transaction()?;
        let mut updated = writer.read_page(page_id)?;
        updated.data[0] = 44;
        writer.write_page(updated)?;
        writer.commit_transaction()?;
        assert!(wal_path.exists());

        stale_reader.end_read()?;

        let mut reader = Pager::new(test_db.path(), 8)?;
        reader.begin_read()?;
        assert_eq!(reader.wal_snapshot_sequence(), Some(1));
        assert_eq!(reader.read_page(page_id)?.data[0], 44);

        writer.checkpoint_wal()?;
        assert!(!wal_path.exists());

        assert_eq!(reader.read_page(page_id)?.data[0], 44);
        reader.end_read()?;
        Ok(())
    }

    #[test]
    fn test_pager_switches_from_wal_to_rollback_after_checkpointing() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_switches_wal_to_rollback");
        let wal_path = std::path::PathBuf::from(format!("{}.wal", test_db.path()));

        let page_id = {
            let mut pager = Pager::new(test_db.path(), 8)?;
            let page_id = pager.allocate_page()?;
            let mut page = Page::new(page_id);
            page.data[0] = 10;
            pager.write_page(page)?;
            pager.flush()?;
            pager.set_journal_mode(JournalMode::Wal)?;
            page_id
        };

        let mut reader = Pager::new(test_db.path(), 8)?;
        reader.begin_read()?;

        let mut writer = Pager::new(test_db.path(), 8)?;
        writer.begin_transaction()?;
        let mut updated = writer.read_page(page_id)?;
        updated.data[0] = 88;
        writer.write_page(updated)?;
        writer.commit_transaction()?;
        assert!(wal_path.exists());

        reader.end_read()?;
        writer.set_journal_mode(JournalMode::Rollback)?;
        assert_eq!(writer.journal_mode(), JournalMode::Rollback);
        assert!(!wal_path.exists());

        let reopened = Pager::new(test_db.path(), 8)?;
        assert_eq!(reopened.journal_mode(), JournalMode::Rollback);
        let reopened = reopened;
        let page = reopened.read_page(page_id)?;
        assert_eq!(page.data[0], 88);
        Ok(())
    }

    #[test]
    fn test_pager_refuses_wal_to_rollback_switch_with_active_reader() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_refuses_wal_to_rollback_with_reader");

        let page_id = {
            let mut pager = Pager::new(test_db.path(), 8)?;
            let page_id = pager.allocate_page()?;
            let mut page = Page::new(page_id);
            page.data[0] = 10;
            pager.write_page(page)?;
            pager.flush()?;
            pager.set_journal_mode(JournalMode::Wal)?;
            page_id
        };

        let mut reader = Pager::new(test_db.path(), 8)?;
        reader.begin_read()?;

        let mut writer = Pager::new(test_db.path(), 8)?;
        writer.begin_transaction()?;
        let mut updated = writer.read_page(page_id)?;
        updated.data[0] = 44;
        writer.write_page(updated)?;
        writer.commit_transaction()?;

        let err = writer.set_journal_mode(JournalMode::Rollback).unwrap_err();
        assert!(err.to_string().contains("while readers are active"));

        reader.end_read()?;
        writer.set_journal_mode(JournalMode::Rollback)?;
        Ok(())
    }

    #[test]
    fn test_pager_wal_recovery_discards_uncommitted_transaction() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_wal_recovery_discards_uncommitted");

        let page_id = {
            let mut pager = Pager::new(test_db.path(), 8)?;
            let page_id = pager.allocate_page()?;
            let mut page = Page::new(page_id);
            page.data[0] = 10;
            pager.write_page(page)?;
            pager.flush()?;
            pager.set_journal_mode(JournalMode::Wal)?;

            pager.begin_transaction()?;
            let mut updated = pager.read_page(page_id)?;
            updated.data[0] = 77;
            pager.write_page(updated)?;
            page_id
        };

        let mut reopened = Pager::new(test_db.path(), 8)?;
        reopened.begin_read()?;
        let page = reopened.read_page(page_id)?;
        assert_eq!(page.data[0], 10);
        reopened.end_read()?;
        Ok(())
    }

    #[test]
    fn test_pager_wal_transaction_does_not_create_rollback_journal() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_wal_transaction_does_not_create_journal");
        let journal_path = std::path::PathBuf::from(format!("{}.journal", test_db.path()));

        let mut pager = Pager::new(test_db.path(), 8)?;
        let page_id = pager.allocate_page()?;
        let mut page = Page::new(page_id);
        page.data[0] = 7;
        pager.write_page(page)?;
        pager.flush()?;
        pager.set_journal_mode(JournalMode::Wal)?;

        pager.begin_transaction()?;
        let mut updated = pager.read_page(page_id)?;
        updated.data[0] = 99;
        pager.write_page(updated)?;
        assert!(!journal_path.exists());

        pager.rollback_transaction()?;
        assert!(!journal_path.exists());

        let mut reopened = Pager::new(test_db.path(), 8)?;
        reopened.begin_read()?;
        let restored = reopened.read_page(page_id)?;
        assert_eq!(restored.data[0], 7);
        reopened.end_read()?;
        Ok(())
    }

    #[test]
    fn test_pager_wal_reader_keeps_trailing_page_visible_until_checkpoint(
    ) -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_wal_reader_keeps_trailing_page_visible");

        let trailing_page_id = {
            let mut pager = Pager::new(test_db.path(), 8)?;
            let first_page_id = pager.allocate_page()?;
            let trailing_page_id = pager.allocate_page()?;

            let mut first_page = Page::new(first_page_id);
            first_page.data[0] = 11;
            pager.write_page(first_page)?;

            let mut trailing_page = Page::new(trailing_page_id);
            trailing_page.data[0] = 33;
            pager.write_page(trailing_page)?;
            pager.flush()?;
            pager.set_journal_mode(JournalMode::Wal)?;
            trailing_page_id
        };

        let mut reader = Pager::new(test_db.path(), 8)?;
        reader.begin_read()?;
        assert_eq!(reader.read_page(trailing_page_id)?.data[0], 33);

        let mut writer = Pager::new(test_db.path(), 8)?;
        writer.begin_transaction()?;
        writer.deallocate_page(trailing_page_id)?;
        writer.commit_transaction()?;

        assert_eq!(reader.read_page(trailing_page_id)?.data[0], 33);
        reader.end_read()?;

        writer.checkpoint_wal()?;
        let reopened = Pager::new(test_db.path(), 8)?;
        assert!(reopened.read_page(trailing_page_id).is_err());
        Ok(())
    }

    #[test]
    fn test_pager_latest_wal_reader_does_not_see_freed_page_after_commit(
    ) -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_latest_wal_reader_does_not_see_freed_page");

        let trailing_page_id = {
            let mut pager = Pager::new(test_db.path(), 8)?;
            let first_page_id = pager.allocate_page()?;
            let trailing_page_id = pager.allocate_page()?;

            let mut first_page = Page::new(first_page_id);
            first_page.data[0] = 11;
            pager.write_page(first_page)?;

            let mut trailing_page = Page::new(trailing_page_id);
            trailing_page.data[0] = 33;
            pager.write_page(trailing_page)?;
            pager.flush()?;
            pager.set_journal_mode(JournalMode::Wal)?;
            trailing_page_id
        };

        let mut stale_reader = Pager::new(test_db.path(), 8)?;
        stale_reader.begin_read()?;
        assert_eq!(stale_reader.read_page(trailing_page_id)?.data[0], 33);

        let mut writer = Pager::new(test_db.path(), 8)?;
        writer.begin_transaction()?;
        writer.deallocate_page(trailing_page_id)?;
        writer.commit_transaction()?;

        let mut latest_reader = Pager::new(test_db.path(), 8)?;
        latest_reader.begin_read()?;
        assert!(latest_reader.read_page(trailing_page_id).is_err());
        latest_reader.end_read()?;

        assert_eq!(stale_reader.read_page(trailing_page_id)?.data[0], 33);
        stale_reader.end_read()?;
        Ok(())
    }

    #[test]
    fn test_pager_validate_integrity_uses_wal_visible_state() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_pager_validate_integrity_uses_wal_visible_state");
        let wal_path = std::path::PathBuf::from(format!("{}.wal", test_db.path()));

        let page_id = {
            let mut pager = Pager::new(test_db.path(), 8)?;
            let page_id = pager.allocate_page()?;
            let mut page = Page::new(page_id);
            page.data[0] = 10;
            pager.write_page(page)?;
            pager.flush()?;
            pager.set_journal_mode(JournalMode::Wal)?;
            page_id
        };

        let metadata_page = {
            let pager = Pager::new(test_db.path(), 8)?;
            pager
                .read_page(crate::storage::STORAGE_METADATA_PAGE_ID)?
                .data
                .to_vec()
        };
        // Hand-built frames have to stamp the trailer the pager would have written,
        // or the page reads back as never-written and nothing verifies it.
        let mut wal_page = Page::new(page_id);
        wal_page.data[0] = 99;
        crate::storage::file_manager::stamp_page_trailer(&mut wal_page);
        let frame = WalFrame::committed(page_id, 4, 1, wal_page.take_data());
        let metadata_frame = WalFrame::committed(
            crate::storage::STORAGE_METADATA_PAGE_ID,
            4,
            1,
            metadata_page,
        );
        let wal = WalFile {
            header: WalHeader::default(),
            frames: vec![frame, metadata_frame],
        };
        fs::write(&wal_path, wal.encode()?)?;

        let mut pager = Pager::new(test_db.path(), 8)?;
        let report = pager.validate_integrity()?;
        assert_eq!(report.verified_checksum_pages, 1);
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
    use crate::btree::node::BTreeNode;
    use crate::btree::value_store::StoredValueLayout;
    use crate::btree::{BTreeKey, NodeType};
    use crate::catalog::{CatalogEngine, Value};
    use crate::storage::overflow::{
        collect_overflow_page_ids, validate_overflow_chain, write_overflow_chain,
        OVERFLOW_CHUNK_CAPACITY,
    };
    use crate::storage::{Page, Pager, PAGE_SIZE, STORAGE_METADATA_PAGE_ID};
    use crate::test_utils::TestDbFile;
    use std::io::{Seek, SeekFrom, Write};

    #[test]
    fn test_concurrent_page_access() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_concurrent");

        let mut storage = Pager::new(test_db.path(), 100)?;
        let page_id = storage.allocate_page()?;

        let mut page = Page::new(page_id);
        page.data[0..8].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        storage.write_page(page)?;

        let read_page = storage.read_page(page_id)?;
        assert_eq!(read_page.data[0..8], [1, 2, 3, 4, 5, 6, 7, 8]);

        let mut mod_page = Page::new(page_id);
        mod_page.data[0..8].copy_from_slice(&[5, 2, 3, 4, 5, 6, 7, 8]);
        storage.write_page(mod_page)?;

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

        let stats = storage.get_storage_stats()?;
        assert_eq!(stats.table_count, 2);
        assert_eq!(stats.total_rows, 2);
        assert_eq!(stats.free_page_count, 1);
        assert_eq!(stats.fragmented_free_page_count, 1);
        assert_eq!(stats.trailing_free_page_count, 0);
        assert!(stats.allocated_page_count >= stats.live_table_page_count + stats.free_page_count);
        assert!(stats.file_bytes >= (stats.allocated_page_count as u64 + 2) * PAGE_SIZE as u64);
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

        let stats = storage.get_storage_stats()?;
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
    fn test_overflow_validation_rejects_trailing_chain_pages() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_overflow_trailing_chain_rejected");
        let mut pager = Pager::new(test_db.path(), 32)?;

        let payload = vec![0x2A; OVERFLOW_CHUNK_CAPACITY + 128];
        let first_page = write_overflow_chain(&mut pager, &payload)?
            .expect("non-empty payload should produce overflow pages");
        pager.flush()?;

        let err =
            validate_overflow_chain(&pager, Some(first_page), OVERFLOW_CHUNK_CAPACITY).unwrap_err();
        assert!(err.to_string().contains("trailing pages"));
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
        let root_node = BTreeNode::from_page_decoded(root_page)?;
        let layout = StoredValueLayout::decode(root_node.values[0].as_bytes())?;
        assert_ne!(layout.overflow_first_page, crate::storage::INVALID_PAGE_ID);

        storage.deallocate_page(layout.overflow_first_page)?;

        let err = storage.validate_integrity().unwrap_err();
        let message = err.to_string();
        assert!(message.contains("Overflow page"));
        assert!(
            message.contains("both live and free")
                || message.contains("also on the freelist")
                || message.contains("magic mismatch")
                || message.contains("kind mismatch"),
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
        let mut node = BTreeNode::from_page_decoded(page.clone())?;
        assert_eq!(node.node_type, NodeType::Leaf);
        assert!(node.keys.len() >= 2);

        let first_row_id =
            crate::catalog::serialization::RowCodec::decode_row_id_key(&node.keys[0].data)?;

        // Corrupt the second rowid key to be <= the first rowid key.
        node.keys[1] = BTreeKey::new(crate::catalog::serialization::RowCodec::encode_row_id_key(
            first_row_id,
        ));
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
        let stats = reopened.get_storage_stats()?;
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
        let root_node = BTreeNode::from_page_decoded(root_page)?;
        let layout = StoredValueLayout::decode(root_node.values[0].as_bytes())?;
        let overflow_ids = storage.with_pager(|pager| {
            collect_overflow_page_ids(pager, Some(layout.overflow_first_page))
        })?;
        assert!(!overflow_ids.is_empty());

        assert!(storage.delete_from_table_by_rowid("docs", rowid)?);
        let stats = storage.get_storage_stats()?;
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
        let root_node = BTreeNode::from_page_decoded(root_page)?;
        let layout = StoredValueLayout::decode(root_node.values[0].as_bytes())?;
        let overflow_ids = storage.with_pager(|pager| {
            collect_overflow_page_ids(pager, Some(layout.overflow_first_page))
        })?;

        storage.replace_table_rows(
            "docs",
            vec![crate::catalog::StoredRow {
                row_id: 1,
                values: vec![Value::Text("small".to_string())],
            }],
        )?;

        let stats = storage.get_storage_stats()?;
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
        let root_node = BTreeNode::from_page_decoded(root_page)?;
        let layout = StoredValueLayout::decode(root_node.values[0].as_bytes())?;
        let overflow_ids = storage.with_pager(|pager| {
            collect_overflow_page_ids(pager, Some(layout.overflow_first_page))
        })?;

        storage.drop_table("docs")?;
        assert_eq!(storage.get_storage_stats()?.table_count, 0);

        let reused = storage.allocate_page()?;
        assert!(overflow_ids.contains(&reused) || reused == root_page_id);

        Ok(())
    }

    #[test]
    fn test_failed_drop_table_restores_runtime_metadata_and_rows() -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_storage_failed_drop_table_restores_state");

        {
            let mut storage = CatalogEngine::new(test_db.path())?;
            storage.create_table("docs")?;
            let row_id =
                storage.insert_into_table("docs", vec![Value::Text("keep".to_string())])?;
            assert_eq!(row_id, 1);

            storage.with_pager(|pager| {
                pager.inject_io_failure_after(0);
                Ok(())
            })?;

            let err = storage.drop_table("docs").unwrap_err();
            assert!(err.to_string().contains("Injected IO error"));

            assert_eq!(storage.get_storage_stats()?.table_count, 1);
            assert_eq!(
                storage.read_from_table("docs")?,
                vec![vec![Value::Text("keep".to_string())]]
            );
        }

        let mut reopened = CatalogEngine::new(test_db.path())?;
        assert_eq!(reopened.get_storage_stats()?.table_count, 1);
        assert_eq!(
            reopened.read_from_table("docs")?,
            vec![vec![Value::Text("keep".to_string())]]
        );

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

        let payload = crate::storage::metadata_page::write_catalog_metadata(
            &vec![0; PAGE_SIZE],
            b"version=2\n",
        )?;
        let mut page = Page::new(STORAGE_METADATA_PAGE_ID);
        page.data.copy_from_slice(&payload);
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
            let offset = corrupted_page_id as u64 * PAGE_SIZE as u64;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&[9])?;
            file.flush()?;
        }

        let reopened = crate::catalog::CatalogEngine::new(test_db.path())?;
        // Caught by the page's own trailer now, at read time, before the checksum map
        // is consulted at all. The map could only cover a database small enough for it
        // to fit one metadata page; the trailer has no such ceiling.
        let err = reopened.read_page(corrupted_page_id).unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("checksum in its trailer")
                || message.contains("Page checksum mismatch"),
            "corruption went undetected: {message}"
        );

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
            let offset = corrupted_page_id as u64 * PAGE_SIZE as u64;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&[9])?;
            file.flush()?;
        }

        let mut reopened = crate::catalog::CatalogEngine::new(test_db.path())?;
        // As above: the trailer catches this while `validate_integrity` is reading the
        // page, so the error names the trailer rather than the map.
        let err = reopened.validate_integrity().unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("checksum in its trailer")
                || message.contains("Page checksum mismatch"),
            "corruption went undetected: {message}"
        );

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
        let root_node = BTreeNode::from_page_decoded(root_page)?;

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
        let root_node = BTreeNode::from_page_decoded(root_page)?;

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
                    crate::catalog::DataType::Int,
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
                    crate::catalog::DataType::Int,
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
            unique: false,
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
                        crate::catalog::DataType::Int,
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
                unique: false,
            })?;

            // `insert_table_row`, not `insert_into_table`: this table is keyed by
            // a single integer column, so the row id is the key rather than the
            // next counter value.
            let row_id = storage.insert_table_row(
                &table,
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
    fn test_failed_secondary_index_registration_restores_previous_indexes(
    ) -> crate::error::Result<()> {
        let test_db = TestDbFile::new("_test_failed_secondary_index_registration_restores_state");
        let mut storage = crate::catalog::CatalogEngine::new(test_db.path())?;
        let root_page_id = storage.create_table("users")?;

        let mut table = crate::catalog::Table::new(
            crate::catalog::TableId::new(1),
            "users".to_string(),
            vec![
                crate::catalog::Column::new(
                    crate::catalog::ColumnId::new(1),
                    "id".to_string(),
                    crate::catalog::DataType::Int,
                )
                .primary_key(true),
                crate::catalog::Column::new(
                    crate::catalog::ColumnId::new(2),
                    "email".to_string(),
                    crate::catalog::DataType::Text,
                ),
                crate::catalog::Column::new(
                    crate::catalog::ColumnId::new(3),
                    "username".to_string(),
                    crate::catalog::DataType::Text,
                ),
            ],
            root_page_id,
        )?;
        table.add_secondary_index(crate::catalog::SecondaryIndex {
            name: "idx_users_email".to_string(),
            column_indices: vec![1],
            root_page_id: storage.create_empty_btree()?.into(),
            unique: false,
        })?;
        table.add_secondary_index(crate::catalog::SecondaryIndex {
            name: "idx_users_username_unique".to_string(),
            column_indices: vec![2],
            root_page_id: storage.create_empty_btree()?.into(),
            unique: true,
        })?;

        let existing_row_id = storage.insert_into_table(
            "users",
            vec![
                Value::Integer(1),
                Value::Text("alice@example.com".to_string()),
                Value::Text("alice".to_string()),
            ],
        )?;
        let existing_row = crate::catalog::StoredRow {
            row_id: existing_row_id,
            values: vec![
                Value::Integer(1),
                Value::Text("alice@example.com".to_string()),
                Value::Text("alice".to_string()),
            ],
        };
        storage.register_secondary_index_row(&table, existing_row)?;

        let conflicting_row = crate::catalog::StoredRow {
            row_id: 99,
            values: vec![
                Value::Integer(2),
                Value::Text("shadow@example.com".to_string()),
                Value::Text("alice".to_string()),
            ],
        };

        let err = storage
            .register_secondary_index_row(&table, conflicting_row)
            .unwrap_err();
        assert!(err.to_string().contains("UNIQUE index"));

        assert!(storage
            .lookup_secondary_index_rowids(
                &table,
                "idx_users_email",
                &[Value::Text("shadow@example.com".to_string())],
            )?
            .is_empty());
        assert_eq!(
            storage.lookup_secondary_index_rowids(
                &table,
                "idx_users_username_unique",
                &[Value::Text("alice".to_string())],
            )?,
            vec![existing_row_id]
        );

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
                    crate::catalog::DataType::Int,
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
            unique: false,
        })?;

        // See `test_durable_indexes_survive_reopen`: the row id is the key here.
        let row_id = storage.insert_table_row(
            &table,
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

// Row serialization used to be tested here, reaching up into `catalog` from a
// storage test module. Those two tests were written against the old format's
// four-byte length prefix, which no longer exists, and everything they covered is
// now covered where the format lives: `catalog::record_codec` round-trips every value
// type and rejects truncation at every length, and `catalog::serialization` tests
// the facade.

mod file_manager_tests {
    use crate::storage::file_manager::FileManager;

    #[test]
    fn deallocate_reserved_pages_are_rejected() {
        let mut manager = FileManager::new_in_memory().unwrap();

        let page_zero_err = manager.deallocate_page(0).unwrap_err();
        assert!(page_zero_err
            .to_string()
            .contains("Cannot deallocate reserved page 0"));

        let page_one_err = manager.deallocate_page(1).unwrap_err();
        assert!(page_one_err
            .to_string()
            .contains("Cannot deallocate reserved page 1"));
    }
}

mod format_tests {
    use crate::storage::format::PageKind;

    #[test]
    fn page_kind_roundtrip() {
        for kind in [
            PageKind::InteriorIndex,
            PageKind::InteriorTable,
            PageKind::LeafIndex,
            PageKind::LeafTable,
            PageKind::Overflow,
            PageKind::FreelistTrunk,
            PageKind::FreelistLeaf,
        ] {
            assert_eq!(PageKind::from_byte(kind as u8).unwrap(), kind);
        }
    }
}

mod metadata_page_tests {
    use crate::storage::metadata_page::{
        read_catalog_metadata, read_pager_metadata, write_catalog_metadata, write_pager_metadata,
    };
    use crate::storage::PAGE_SIZE;

    #[test]
    fn metadata_page_preserves_both_sections() {
        let empty = vec![0; PAGE_SIZE];
        let page = write_pager_metadata(&empty, b"pager=yes").unwrap();
        let page = write_catalog_metadata(&page, b"catalog=yes").unwrap();

        assert_eq!(read_pager_metadata(&page).unwrap().unwrap(), b"pager=yes");
        assert_eq!(
            read_catalog_metadata(&page).unwrap().unwrap(),
            b"catalog=yes"
        );
    }

    #[test]
    fn metadata_page_rejects_a_page_without_magic() {
        let payload = b"not a metadata page";
        let mut unmagicked_page = vec![0; PAGE_SIZE];
        unmagicked_page[0..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        unmagicked_page[4..4 + payload.len()].copy_from_slice(payload);

        let err = write_pager_metadata(&unmagicked_page, b"pager=yes").unwrap_err();
        assert!(err
            .to_string()
            .contains("does not carry the expected magic"));
    }

    #[test]
    fn metadata_page_rejects_btree_root_bytes() {
        let mut page = vec![0; PAGE_SIZE];
        page[0..4].copy_from_slice(b"BTRE");

        let err = read_catalog_metadata(&page).unwrap_err();
        assert!(err
            .to_string()
            .contains("does not carry the expected magic"));
        let err = read_pager_metadata(&page).unwrap_err();
        assert!(err
            .to_string()
            .contains("does not carry the expected magic"));
    }
}

mod overflow_unit_tests {
    use crate::storage::overflow::{
        encode_overflow_chain, split_payload_into_overflow_chunks, OverflowPage,
        OVERFLOW_CHUNK_CAPACITY,
    };
    use crate::storage::{Page, Pager};

    #[test]
    fn overflow_chunk_split_uses_full_intermediate_pages() {
        let payload = vec![0x7A; OVERFLOW_CHUNK_CAPACITY * 2 + 19];
        let chunks = split_payload_into_overflow_chunks(&payload);

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), OVERFLOW_CHUNK_CAPACITY);
        assert_eq!(chunks[1].len(), OVERFLOW_CHUNK_CAPACITY);
        assert_eq!(chunks[2].len(), 19);
    }

    #[test]
    fn overflow_chain_roundtrip() {
        let payload = vec![0x44; OVERFLOW_CHUNK_CAPACITY + 37];
        let mut storage = Pager::new_in_memory(8).unwrap();
        let pages = encode_overflow_chain(&[8, 9], &payload, &mut storage).unwrap();
        let mut decoded = Vec::new();

        for (index, page) in pages.iter().enumerate() {
            let remaining = payload.len() - decoded.len();
            let expected_chunk_len = remaining.min(OVERFLOW_CHUNK_CAPACITY);
            let overflow = OverflowPage::decode(page, expected_chunk_len).unwrap();
            let expected_next_page_id = pages.get(index + 1).map(|page| page.id).unwrap_or(0);
            assert_eq!(overflow.next_page_id, expected_next_page_id);
            decoded.extend_from_slice(&overflow.payload_chunk);
        }

        assert_eq!(decoded, payload);
    }

    #[test]
    fn overflow_page_rejects_wrong_kind() {
        let mut page = Page::new(5);
        page.data[0] = 0xFF;

        let error = OverflowPage::decode(&page, 10).unwrap_err();
        assert!(error.to_string().contains("overflow page kind mismatch"));
    }
}

mod journal_codec_tests {
    use crate::storage::journal::{
        EncodedRollbackJournal, JournalFileRecord, JournalFileState, JournalHeader,
    };
    use crate::storage::PAGE_SIZE;

    const JOURNAL_HEADER_SIZE: usize = 36;

    #[test]
    fn v3_rollback_journal_roundtrip() {
        let journal = EncodedRollbackJournal {
            commit_intents: Vec::new(),
            header: JournalHeader {
                state: JournalFileState::Committed,
                original_database_page_count: 12,
                checksum_seed: 0xDEADBEEF,
                ..JournalHeader::default()
            },
            original_free_pages: vec![5, 9],
            original_checksums: vec![(2, 100), (7, 200)],
            records: vec![
                JournalFileRecord {
                    page_number: 2,
                    page_bytes: vec![0x11; PAGE_SIZE],
                },
                JournalFileRecord {
                    page_number: 9,
                    page_bytes: vec![0xAB; PAGE_SIZE],
                },
            ],
        };

        let encoded = journal.encode().expect("encode journal");
        let decoded = EncodedRollbackJournal::decode(&encoded).expect("decode journal");

        assert_eq!(decoded.header.state, JournalFileState::Committed);
        assert_eq!(decoded.header.original_database_page_count, 12);
        assert_eq!(decoded.header.checksum_seed, 0xDEADBEEF);
        assert_eq!(decoded.header.free_page_count, 2);
        assert_eq!(decoded.header.checksum_count, 2);
        assert_eq!(decoded.header.record_count, 2);
        assert_eq!(decoded.original_free_pages, journal.original_free_pages);
        assert_eq!(decoded.original_checksums, journal.original_checksums);
        assert_eq!(decoded.records, journal.records);
    }

    #[test]
    fn v3_rollback_journal_rejects_checksum_corruption() {
        let journal = EncodedRollbackJournal {
            commit_intents: Vec::new(),
            header: JournalHeader::default(),
            original_free_pages: vec![],
            original_checksums: vec![],
            records: vec![JournalFileRecord {
                page_number: 3,
                page_bytes: vec![0x55; PAGE_SIZE],
            }],
        };

        let mut encoded = journal.encode().expect("encode journal");
        let checksum_index = JOURNAL_HEADER_SIZE + 4;
        encoded[checksum_index] ^= 0x01;

        let error = EncodedRollbackJournal::decode(&encoded).expect_err("corruption should fail");
        assert!(error
            .to_string()
            .contains("rollback journal record checksum mismatch"));
    }

    #[test]
    fn v3_rollback_journal_rejects_duplicate_pages() {
        let journal = EncodedRollbackJournal {
            commit_intents: Vec::new(),
            header: JournalHeader::default(),
            original_free_pages: vec![],
            original_checksums: vec![],
            records: vec![
                JournalFileRecord {
                    page_number: 4,
                    page_bytes: vec![0x22; PAGE_SIZE],
                },
                JournalFileRecord {
                    page_number: 4,
                    page_bytes: vec![0x33; PAGE_SIZE],
                },
            ],
        };

        let error = journal.encode().expect_err("duplicate pages should fail");
        assert!(error
            .to_string()
            .contains("rollback journal contains duplicate page 4"));
    }

    #[test]
    fn v3_rollback_journal_rejects_truncated_metadata_sections() {
        let journal = EncodedRollbackJournal {
            commit_intents: Vec::new(),
            header: JournalHeader::default(),
            original_free_pages: vec![2],
            original_checksums: vec![(3, 10)],
            records: vec![],
        };

        let mut encoded = journal.encode().expect("encode journal");
        encoded.truncate(encoded.len() - 2);

        let error = EncodedRollbackJournal::decode(&encoded).expect_err("truncation should fail");
        assert!(error.to_string().contains("truncated"));
    }
}

mod wal_codec_tests {
    use crate::storage::wal::{WalFile, WalFrame, WalHeader};

    #[test]
    fn v3_wal_roundtrip() {
        let wal = WalFile {
            header: WalHeader::default(),
            frames: vec![
                WalFrame::committed(1, 3, 1, vec![7u8; crate::storage::PAGE_SIZE]),
                WalFrame::committed(2, 3, 1, vec![9u8; crate::storage::PAGE_SIZE]),
                WalFrame::committed(2, 4, 2, vec![11u8; crate::storage::PAGE_SIZE]),
            ],
        };

        let encoded = wal.encode().unwrap();
        let decoded = WalFile::decode(&encoded).unwrap();
        assert_eq!(decoded, wal);
    }

    #[test]
    fn v3_wal_reconstructs_visible_pages_for_commit_boundary() {
        let wal = WalFile {
            header: WalHeader::default(),
            frames: vec![
                WalFrame::committed(1, 3, 1, vec![1u8; crate::storage::PAGE_SIZE]),
                WalFrame::committed(2, 3, 1, vec![2u8; crate::storage::PAGE_SIZE]),
                WalFrame::committed(2, 4, 2, vec![3u8; crate::storage::PAGE_SIZE]),
            ],
        };

        let (page_count_1, visible_1) = wal.visible_pages_at(1).unwrap();
        assert_eq!(page_count_1, 3);
        assert_eq!(visible_1.get(&2).unwrap()[0], 2);

        let (page_count_2, visible_2) = wal.visible_pages_at(2).unwrap();
        assert_eq!(page_count_2, 4);
        assert_eq!(visible_2.get(&2).unwrap()[0], 3);
    }

    #[test]
    fn v3_wal_rejects_checksum_corruption() {
        let wal = WalFile {
            header: WalHeader::default(),
            frames: vec![WalFrame::committed(
                1,
                3,
                1,
                vec![7u8; crate::storage::PAGE_SIZE],
            )],
        };

        let mut encoded = wal.encode().unwrap();
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF;

        let err = WalFile::decode(&encoded).unwrap_err();
        assert!(err.to_string().contains("checksum mismatch"));
    }

    #[test]
    fn v3_wal_decode_ignores_truncated_tail_frame() {
        let wal = WalFile {
            header: WalHeader::default(),
            frames: vec![
                WalFrame::committed(1, 3, 1, vec![7u8; crate::storage::PAGE_SIZE]),
                WalFrame::committed(2, 3, 2, vec![9u8; crate::storage::PAGE_SIZE]),
            ],
        };

        let mut encoded = wal.encode().unwrap();
        encoded.truncate(encoded.len() - 19);

        let decoded = WalFile::decode(&encoded).unwrap();
        assert_eq!(decoded.frames.len(), 1);
        assert_eq!(decoded.frames[0].commit_sequence, 1);
    }
}

/// Assertions about *work performed*, not time taken.
///
/// These exist because wall-clock benchmarking cannot police this rewrite. On the
/// development machine the same unchanged code measured 76 ms and 97 ms for one
/// benchmark twenty minutes apart, and 1.55 ms against 3.97 ms for another. A 10 %
/// change is invisible under that, and drift that happens to correlate with which
/// build ran first is indistinguishable from a real regression.
///
/// Syscall and page-write counts have none of that problem: they are identical on
/// every machine under any load, they run in CI without a benchmark harness, and
/// they fail loudly when a refactor quietly adds an fsync back.
///
/// Several of these lock in behaviour that is *known to be bad* — four fsyncs per
/// rollback commit, for instance. That is deliberate. They are the before-picture
/// the storage rewrite is measured against, and the phase that improves a number is
/// expected to update the assertion in the same commit.
mod work_performed_tests {
    use crate::storage::{JournalMode, Page, Pager};
    use crate::test_utils::TestDbFile;

    /// Commits one dirty page and reports the storage work that took.
    fn commit_one_page(pager: &mut Pager, page_id: u32, fill: u8) -> crate::storage::StatsSnapshot {
        let before = pager.stats();
        pager.begin_transaction().expect("begin");
        let mut page = Page::new(page_id);
        page.data[0] = fill;
        pager.write_page(page).expect("write");
        pager.commit_transaction().expect("commit");
        pager.stats().since(&before)
    }

    #[test]
    fn rollback_commit_fsync_count_is_pinned() {
        let db = TestDbFile::new("_test_work_rollback_fsyncs");
        let mut pager = Pager::new(db.path(), 64).expect("open");
        let page_id = pager.allocate_page().expect("allocate");

        // Warm up: the first commit of a fresh database also initialises metadata.
        let _ = commit_one_page(&mut pager, page_id, 1);
        let work = commit_one_page(&mut pager, page_id, 2);

        // Two: the journal, once, after the persisted-state page has been staged and
        // the commit intent recorded; then the database file. That is the floor for a
        // rollback journal — one sync to make the undo information durable before the
        // database is touched, one to make the new contents durable.
        //
        // It was four. One went with the redundant pre-`flush` journal sync; the
        // other with the commit marker, which the journal's commit intent replaced.
        assert_eq!(
            work.fsyncs, 2,
            "rollback commit fsync count changed: {work:?}"
        );
    }

    /// Taking a snapshot used to cost a `statx`.
    ///
    /// `ByteTree::run_atomically` takes one pager snapshot per row it writes, and
    /// each one recorded the database file's length by asking the operating system
    /// for it — a number the pager had just computed itself, since it is the only
    /// writer of that file. Under `strace`, 200 single-row transactions issued
    /// 12 031 `statx` calls, more than any other syscall including `fsync`, and more
    /// than 60 per transaction.
    ///
    /// The length is now maintained in memory by the paths that change it, and the
    /// OS is asked only at open and during recovery.
    #[test]
    fn taking_a_snapshot_does_not_stat_the_file() {
        let db = TestDbFile::new("_test_work_snapshot_statx");
        let mut pager = Pager::new(db.path(), 64).expect("open");
        let page_id = pager.allocate_page().expect("allocate");

        pager.begin_transaction().expect("begin");
        let mut page = Page::new(page_id);
        page.data[0] = 1;
        pager.write_page(page).expect("write");

        let before = pager.stats();
        for _ in 0..32 {
            let _ = pager.snapshot().expect("snapshot");
        }
        let work = pager.stats().since(&before);
        pager.commit_transaction().expect("commit");

        assert_eq!(
            work.file_stats, 0,
            "32 snapshots asked the OS for the file length: {work:?}"
        );
    }

    /// Preserving a page's pre-image costs exactly one copy: the journal's.
    ///
    /// `snapshot_original_page` has two customers. The rollback journal needs the
    /// bytes, once per page per transaction, because they go into a file. A
    /// savepoint needs the page as it stood at the mark, and takes a shared handle
    /// to the version the cache already holds — the write path replaces pages
    /// rather than mutating them while a handle is out, so the handle stays valid
    /// and nothing is copied.
    ///
    /// It used to make three copies beyond the journal's: it cloned the cached page
    /// it was only reading from, cloned the finished journal record so the
    /// in-memory list and the journal writer could each hold one, and copied the
    /// whole page again for every savepoint that had not yet captured it. None of
    /// them showed up in an allocation profile, because the page-buffer pool hands
    /// the same recycled buffers back — they were pure memcpy, and a savepoint is
    /// taken per row.
    #[test]
    fn preserving_a_page_image_copies_it_once_for_the_journal() {
        let db = TestDbFile::new("_test_work_undo_copies");
        let mut pager = Pager::new(db.path(), 64).expect("open");
        let page_id = pager.allocate_page().expect("allocate");

        // Give the page a durable pre-image, so the writes below have something to
        // preserve rather than being treated as pages the transaction created.
        pager.begin_transaction().expect("begin");
        let mut page = Page::new(page_id);
        page.data[0] = 1;
        pager.write_page(page).expect("write");
        pager.commit_transaction().expect("commit");

        pager.begin_transaction().expect("begin");
        let before = pager.stats();
        for fill in 2..=6u8 {
            // A savepoint per write, as `run_atomically` takes one per row.
            let savepoint = pager.snapshot().expect("snapshot");
            let mut page = Page::new(page_id);
            page.data[0] = fill;
            pager.write_page(page).expect("write");
            pager.release_snapshot(savepoint).expect("release");
        }
        let work = pager.stats().since(&before);
        pager.commit_transaction().expect("commit");

        // One: the journal, on the first write. The four writes after it find the
        // page already journalled, and the five savepoints each take a handle.
        assert_eq!(
            work.undo_page_copies, 1,
            "page pre-image copy count changed: {work:?}"
        );
    }

    #[test]
    fn a_rollback_transaction_journals_one_record_per_page() {
        fn journal_bytes_for(label: &str, pages: usize) -> u64 {
            let db = TestDbFile::new(label);
            let mut pager = Pager::new(db.path(), 64).expect("open");
            let page_ids: Vec<u32> = (0..pages)
                .map(|_| pager.allocate_page().expect("allocate"))
                .collect();

            // Give every page a durable pre-image, so the writes below have
            // something to journal rather than being pages the transaction created.
            pager.begin_transaction().expect("begin");
            for &page_id in &page_ids {
                let mut page = Page::new(page_id);
                page.data[0] = 1;
                pager.write_page(page).expect("write");
            }
            pager.commit_transaction().expect("commit");

            let before = pager.stats();
            pager.begin_transaction().expect("begin");
            for &page_id in &page_ids {
                // Twice each: the second write finds the page already journalled.
                for fill in 2..=3u8 {
                    let mut page = Page::new(page_id);
                    page.data[0] = fill;
                    pager.write_page(page).expect("write");
                }
            }
            pager.commit_transaction().expect("commit");
            pager.stats().since(&before).journal_bytes
        }

        let two = journal_bytes_for("_test_work_journal_bytes_two", 2);
        let five = journal_bytes_for("_test_work_journal_bytes_five", 5);

        // Per page: the record — an 8-byte prefix and the page — the four-byte
        // record count the append rewrites in the header, and the commit intent,
        // a page id and the checksum the page must carry if the commit landed.
        let per_page = (8 + crate::storage::PAGE_SIZE + 4 + 8) as u64;
        assert_eq!(
            five - two,
            3 * per_page,
            "journal cost per page changed: {two} bytes for 2 pages, {five} for 5"
        );
    }

    /// A rollback commit leaves the page cache alone.
    ///
    /// Rollback mode has no WAL to version reads, so a transaction that starts
    /// after someone else committed has to drop every cached page. Telling their
    /// commit from ours is the whole problem, and the answer used to be a
    /// fingerprint of the persisted metadata page as last read from disk. But that
    /// page is shared with the catalog, whose section carries row counts, so our own
    /// commits changed it too: the fingerprint never matched and the cache was
    /// emptied at the start of every single transaction — 205 resets over 205
    /// one-row transactions, and seven of the nine page reads each of those cost.
    ///
    /// The cached copy of that page is the better witness. A commit writes it
    /// through the cache, so bytes on disk that differ from it are bytes we did not
    /// write.
    #[test]
    fn a_rollback_commit_does_not_empty_the_page_cache() {
        let db = TestDbFile::new("_test_work_cache_survives_commit");
        let mut pager = Pager::new(db.path(), 64).expect("open");
        let page_ids: Vec<u32> = (0..4)
            .map(|_| pager.allocate_page().expect("allocate"))
            .collect();

        pager.begin_transaction().expect("begin");
        for &page_id in &page_ids {
            let mut page = Page::new(page_id);
            page.data[0] = 1;
            pager.write_page(page).expect("write");
        }
        pager.commit_transaction().expect("commit");

        // Every page is resident and clean. Reading them back in a fresh
        // transaction must not touch the file.
        let before = pager.stats();
        pager.begin_transaction().expect("begin");
        for &page_id in &page_ids {
            pager.read_page(page_id).expect("read");
        }
        let mut page = Page::new(page_ids[0]);
        page.data[0] = 2;
        pager.write_page(page).expect("write");
        pager.commit_transaction().expect("commit");
        let work = pager.stats().since(&before);

        assert_eq!(
            work.pool_misses, 0,
            "the commit before this one emptied the page cache: {work:?}"
        );
    }

    /// Opening a read scope does not hash a page it already has.
    ///
    /// Every read scope re-reads the persisted state page to find out whether
    /// another process has committed, and every page read verified its trailer:
    /// an xxHash64 over 4 KiB, twice per statement, on a page that had not
    /// changed. It was 11 % of a point-lookup workload.
    ///
    /// The bytes are compared against the cached copy first now. A page that
    /// matches one this pager itself wrote is not being trusted, it is being
    /// recognised, so there is nothing to verify — or to parse, or to apply.
    #[test]
    fn opening_a_read_scope_verifies_no_trailers() {
        let db = TestDbFile::new("_test_read_scope_no_verify");
        let mut pager = Pager::new(db.path(), 64).expect("open");
        let page_id = pager.allocate_page().expect("allocate");
        pager.begin_transaction().expect("begin");
        let mut page = Page::new(page_id);
        page.data[0] = 1;
        pager.write_page(page).expect("write");
        pager.commit_transaction().expect("commit");

        // Warm: the first scope after a commit has a cached copy to compare to.
        pager.begin_read().expect("begin read");
        pager.end_read().expect("end read");

        let before = pager.stats();
        for _ in 0..8 {
            pager.begin_read().expect("begin read");
            let _ = pager.read_page(page_id).expect("read");
            pager.end_read().expect("end read");
        }
        let work = pager.stats().since(&before);

        assert_eq!(
            work.trailer_verifications, 0,
            "a read scope hashed a page it already had: {work:?}"
        );
    }

    /// The same in WAL mode, which answers the question a different way.
    ///
    /// Rollback mode compares the persisted state page against its cached copy.
    /// WAL mode has a cheaper witness — the WAL file's own stamp — and used to
    /// read, verify, parse and apply that 4 KiB page *before* consulting it. The
    /// stamp is asked first now, so a read scope that finds the WAL unmoved
    /// touches the file for nothing at all.
    #[test]
    fn opening_a_wal_read_scope_reads_no_pages() {
        let db = TestDbFile::new("_test_wal_read_scope_no_reads");
        let mut pager = Pager::new(db.path(), 64).expect("open");
        pager
            .set_journal_mode(JournalMode::Wal)
            .expect("switch to wal");
        let page_id = pager.allocate_page().expect("allocate");
        pager.begin_transaction().expect("begin");
        let mut page = Page::new(page_id);
        page.data[0] = 1;
        pager.write_page(page).expect("write");
        pager.commit_transaction().expect("commit");

        // Warm: the first scope after a commit establishes the stamp.
        pager.begin_read().expect("begin read");
        pager.end_read().expect("end read");

        let before = pager.stats();
        for _ in 0..8 {
            pager.begin_read().expect("begin read");
            let _ = pager.read_page(page_id).expect("read");
            pager.end_read().expect("end read");
        }
        let work = pager.stats().since(&before);

        assert_eq!(
            work.page_reads, 0,
            "a WAL read scope read from the file: {work:?}"
        );
        assert_eq!(
            work.trailer_verifications, 0,
            "a WAL read scope hashed a page: {work:?}"
        );
    }

    #[test]
    fn wal_commit_issues_exactly_one_fsync() {
        let db = TestDbFile::new("_test_work_wal_fsyncs");
        let mut pager = Pager::new(db.path(), 64).expect("open");
        pager
            .set_journal_mode(JournalMode::Wal)
            .expect("switch to wal");
        let page_id = pager.allocate_page().expect("allocate");

        let _ = commit_one_page(&mut pager, page_id, 1);
        let work = commit_one_page(&mut pager, page_id, 2);

        assert_eq!(work.fsyncs, 1, "wal commit fsync count changed: {work:?}");
    }

    /// WAL commits append frames instead of writing the database file, which is the
    /// property that makes them cheap. If a change starts writing pages through on
    /// commit, this catches it.
    #[test]
    fn wal_commit_appends_frames_without_writing_the_database_file() {
        let db = TestDbFile::new("_test_work_wal_frames");
        let mut pager = Pager::new(db.path(), 64).expect("open");
        pager
            .set_journal_mode(JournalMode::Wal)
            .expect("switch to wal");
        let page_id = pager.allocate_page().expect("allocate");

        let _ = commit_one_page(&mut pager, page_id, 1);
        let work = commit_one_page(&mut pager, page_id, 2);

        assert!(work.wal_frames > 0, "no frames appended: {work:?}");
        assert_eq!(
            work.page_writes, 0,
            "wal commit wrote the database file: {work:?}"
        );
    }

    /// A transaction touching many pages must write each one once, not once per
    /// statement and not repeatedly as it is re-dirtied.
    #[test]
    fn a_multi_page_transaction_writes_each_dirty_page_once() {
        let db = TestDbFile::new("_test_work_write_amplification");
        let mut pager = Pager::new(db.path(), 512).expect("open");

        let mut page_ids = Vec::new();
        pager.begin_transaction().expect("begin");
        for _ in 0..50 {
            let page_id = pager.allocate_page().expect("allocate");
            // Allocating alone does not dirty the cache, and a transaction that
            // never dirties a page cannot commit — see the note on the pager's
            // WriterLocked state in `docs/bench-baseline.md`.
            pager.write_page(Page::new(page_id)).expect("initialise");
            page_ids.push(page_id);
        }
        pager.commit_transaction().expect("commit");

        let before = pager.stats();
        pager.begin_transaction().expect("begin");
        // Dirty every page twice; the second write must not cost a second flush.
        for pass in 0..2u8 {
            for &page_id in &page_ids {
                let mut page = Page::new(page_id);
                page.data[0] = pass;
                pager.write_page(page).expect("write");
            }
        }
        pager.commit_transaction().expect("commit");
        let work = pager.stats().since(&before);

        // 50 data pages plus the metadata page, written once each despite 100
        // in-transaction mutations.
        assert!(
            work.page_writes <= page_ids.len() as u64 + 1,
            "write amplification: {} writes for {} pages ({work:?})",
            work.page_writes,
            page_ids.len()
        );
        assert_eq!(work.spills, 0, "unexpected spill: {work:?}");
    }

    /// A cached page must not go back to the file backend.
    #[test]
    fn repeated_reads_of_a_cached_page_do_not_reach_the_backend() {
        let db = TestDbFile::new("_test_work_cache_hits");
        let mut pager = Pager::new(db.path(), 64).expect("open");
        let page_id = pager.allocate_page().expect("allocate");
        let _ = commit_one_page(&mut pager, page_id, 7);

        // Prime the cache, then measure only the repeats.
        let _ = pager.read_page(page_id).expect("prime");
        let before = pager.stats();
        for _ in 0..20 {
            let page = pager.read_page(page_id).expect("read");
            assert_eq!(page.data[0], 7);
        }
        let work = pager.stats().since(&before);

        assert_eq!(
            work.page_reads, 0,
            "cached page was re-read from the backend: {work:?}"
        );
        assert_eq!(work.pool_hits, 20, "expected 20 pool hits: {work:?}");
        assert_eq!(work.pool_misses, 0, "unexpected pool miss: {work:?}");
    }

    /// Reading a page that was evicted must fault it back in exactly once.
    #[test]
    fn an_evicted_page_faults_in_once_per_read() {
        let db = TestDbFile::new("_test_work_cache_misses");
        // Capacity 1 guarantees eviction between reads of different pages.
        let mut pager = Pager::new(db.path(), 1).expect("open");
        let first = pager.allocate_page().expect("allocate");
        let second = pager.allocate_page().expect("allocate");
        let _ = commit_one_page(&mut pager, first, 1);
        let _ = commit_one_page(&mut pager, second, 2);

        let before = pager.stats();
        for _ in 0..5 {
            let _ = pager.read_page(first).expect("read first");
            let _ = pager.read_page(second).expect("read second");
        }
        let work = pager.stats().since(&before);

        assert!(
            work.page_reads > 0,
            "expected faults with a 1-page cache: {work:?}"
        );
        assert!(
            work.page_reads <= 10,
            "more faults than reads issued: {work:?}"
        );
    }

    /// Reading never writes, and never syncs.
    #[test]
    fn reads_perform_no_writes_and_no_fsyncs() {
        let db = TestDbFile::new("_test_work_reads_are_clean");
        let mut pager = Pager::new(db.path(), 64).expect("open");
        let page_id = pager.allocate_page().expect("allocate");
        let _ = commit_one_page(&mut pager, page_id, 3);

        let before = pager.stats();
        for _ in 0..10 {
            let _ = pager.read_page(page_id).expect("read");
        }
        let work = pager.stats().since(&before);

        assert_eq!(work.page_writes, 0, "read wrote a page: {work:?}");
        assert_eq!(work.fsyncs, 0, "read issued an fsync: {work:?}");
    }

    /// A rolled-back transaction must not leave its pages in the database file.
    #[test]
    fn rollback_does_not_write_transaction_pages_to_the_database() {
        let db = TestDbFile::new("_test_work_rollback_writes");
        let mut pager = Pager::new(db.path(), 512).expect("open");
        let page_id = pager.allocate_page().expect("allocate");
        let _ = commit_one_page(&mut pager, page_id, 1);

        let before = pager.stats();
        pager.begin_transaction().expect("begin");
        let mut page = Page::new(page_id);
        page.data[0] = 99;
        pager.write_page(page).expect("write");
        pager.rollback_transaction().expect("rollback");
        let work = pager.stats().since(&before);

        assert_eq!(
            pager.read_page(page_id).expect("read").data[0],
            1,
            "rollback did not restore the page"
        );
        assert_eq!(
            work.spills, 0,
            "a small rolled-back transaction spilled: {work:?}"
        );
    }
}

/// Crash recovery, including the write-ahead ordering a rollback commit depends on.
///
/// The rule: a page's original image must be in a *synced* journal before that page
/// is overwritten in the database file. Overwrite first and a power loss leaves a
/// half-applied transaction that can no longer be undone.
///
/// Two of these tests cannot see that rule at all, and it is worth being precise
/// about why. Dropping a `Pager` is a process exit, not a power loss: writes that
/// were never synced are still in the OS page cache and reopening reads them back.
/// Moving the journal sync to after the page writes leaves both of them passing —
/// checked deliberately. They cover recovery *logic*: a half-applied commit is undone,
/// a dead transaction's allocations are truncated away.
///
/// `a_power_loss_that_drops_unsynced_journal_writes_still_recovers` is the one that
/// pins the ordering. `JournalFile` remembers what was durable at the last `sync_all`,
/// so the test can throw away everything written since — exactly what a power loss
/// does — and then demand that recovery still works. It fails when the sync is moved
/// after the page writes.
///
/// The database file has no equivalent yet, so only journal ordering is covered.
mod crash_recovery_ordering_tests {
    use crate::storage::{Page, Pager};
    use crate::test_utils::TestDbFile;

    /// Fills `page_id` with `fill` and commits.
    fn commit_fill(pager: &mut Pager, page_id: u32, fill: u8) {
        pager.begin_transaction().expect("begin");
        let mut page = Page::new(page_id);
        page.data.fill(fill);
        pager.write_page(page).expect("write");
        pager.commit_transaction().expect("commit");
    }

    /// A commit that dies partway through writing pages must leave the database
    /// exactly as it was, not half-updated.
    ///
    /// The failure is injected after one page has already reached the file, so
    /// recovery has real work to undo rather than merely finding an untouched
    /// database.
    #[test]
    fn a_commit_that_dies_midway_is_undone_on_reopen() {
        let db = TestDbFile::new("_test_crash_midway_rollback");

        let (first, second) = {
            let mut pager = Pager::new(db.path(), 64).expect("open");
            let first = pager.allocate_page().expect("allocate");
            let second = pager.allocate_page().expect("allocate");
            commit_fill(&mut pager, first, 0xA1);
            commit_fill(&mut pager, second, 0xA2);

            // Overwrite both, then fail after the first page write reaches the file.
            pager.begin_transaction().expect("begin");
            for (page_id, fill) in [(first, 0xB1), (second, 0xB2)] {
                let mut page = Page::new(page_id);
                page.data.fill(fill);
                pager.write_page(page).expect("write");
            }
            pager.inject_io_failure_after(1);
            assert!(
                pager.commit_transaction().is_err(),
                "injected failure did not surface"
            );

            // Dropping without rollback is the stand-in for the process dying: the
            // journal stays on disk in its active state.
            (first, second)
        };

        let pager = Pager::new(db.path(), 64).expect("reopen");
        assert_eq!(
            pager.read_page(first).expect("read first").data[0],
            0xA1,
            "first page kept its half-committed value"
        );
        assert_eq!(
            pager.read_page(second).expect("read second").data[0],
            0xA2,
            "second page kept its half-committed value"
        );
    }

    /// The same, for a transaction that only allocates pages and journals none of
    /// them. The journal still has to be durable, because its header carries the
    /// file length recovery truncates back to.
    #[test]
    fn a_dead_allocating_transaction_does_not_grow_the_file() {
        let db = TestDbFile::new("_test_crash_allocating");

        let baseline_len = {
            let mut pager = Pager::new(db.path(), 64).expect("open");
            let page_id = pager.allocate_page().expect("allocate");
            commit_fill(&mut pager, page_id, 0x5A);
            let baseline_len = pager.file_len().expect("len");

            pager.begin_transaction().expect("begin");
            for _ in 0..8 {
                let fresh = pager.allocate_page().expect("allocate");
                let mut page = Page::new(fresh);
                page.data.fill(0xCC);
                pager.write_page(page).expect("write");
            }
            pager.inject_io_failure_after(1);
            assert!(
                pager.commit_transaction().is_err(),
                "injected failure did not surface"
            );
            baseline_len
        };

        let pager = Pager::new(db.path(), 64).expect("reopen");
        assert_eq!(
            pager.file_len().expect("len"),
            baseline_len,
            "recovery did not truncate the pages the dead transaction allocated"
        );
    }

    /// The discriminating test: a power loss that throws away every journal write not
    /// yet synced must still leave the database recoverable.
    ///
    /// This is the one that actually pins the write-ahead ordering. If the journal is
    /// synced before the database pages are written, the pre-images survive the power
    /// loss and recovery undoes the half-applied commit. If the sync happens after
    /// the page writes, the pre-images are among the bytes thrown away, and the
    /// database is left with a partially-applied transaction it can no longer undo.
    ///
    /// Verified to fail when the sync is moved after the page writes.
    #[test]
    fn a_power_loss_that_drops_unsynced_journal_writes_still_recovers() {
        let db = TestDbFile::new("_test_power_loss_unsynced_journal");

        let (first, second) = {
            let mut pager = Pager::new(db.path(), 64).expect("open");
            let first = pager.allocate_page().expect("allocate");
            let second = pager.allocate_page().expect("allocate");
            commit_fill(&mut pager, first, 0xA1);
            commit_fill(&mut pager, second, 0xA2);

            pager.begin_transaction().expect("begin");
            for (page_id, fill) in [(first, 0xB1), (second, 0xB2)] {
                let mut page = Page::new(page_id);
                page.data.fill(fill);
                pager.write_page(page).expect("write");
            }

            // Stop the commit once one page has reached the database file, then take
            // away every journal byte that was never synced.
            pager.inject_io_failure_after(1);
            assert!(
                pager.commit_transaction().is_err(),
                "injected failure did not surface"
            );
            pager
                .simulate_journal_power_loss()
                .expect("simulate power loss");

            (first, second)
        };

        let pager = Pager::new(db.path(), 64).expect("reopen");
        assert_eq!(
            pager.read_page(first).expect("read first").data[0],
            0xA1,
            "pre-image was not durable when the database page was overwritten"
        );
        assert_eq!(
            pager.read_page(second).expect("read second").data[0],
            0xA2,
            "second page was not restored"
        );
    }

    /// A power loss that discards unsynced *database* writes must also recover.
    ///
    /// The mirror of the journal case. Here the journal is intact and it is the
    /// half-written database that has to be repaired, which is the situation the
    /// rollback journal exists for in the first place.
    #[test]
    fn a_power_loss_that_drops_unsynced_database_writes_still_recovers() {
        let db = TestDbFile::new("_test_power_loss_unsynced_database");

        let (first, second) = {
            let mut pager = Pager::new(db.path(), 64).expect("open");
            let first = pager.allocate_page().expect("allocate");
            let second = pager.allocate_page().expect("allocate");
            commit_fill(&mut pager, first, 0xA1);
            commit_fill(&mut pager, second, 0xA2);

            pager.begin_transaction().expect("begin");
            for (page_id, fill) in [(first, 0xB1), (second, 0xB2)] {
                let mut page = Page::new(page_id);
                page.data.fill(fill);
                pager.write_page(page).expect("write");
            }
            pager.inject_io_failure_after(1);
            assert!(
                pager.commit_transaction().is_err(),
                "injected failure did not surface"
            );
            pager
                .simulate_database_power_loss()
                .expect("simulate power loss");

            (first, second)
        };

        let pager = Pager::new(db.path(), 64).expect("reopen");
        assert_eq!(pager.read_page(first).expect("read first").data[0], 0xA1);
        assert_eq!(pager.read_page(second).expect("read second").data[0], 0xA2);
    }

    /// Losing both at once — neither the journal nor the database reached the disk
    /// past their last sync — must still land on the pre-transaction state.
    #[test]
    fn a_power_loss_that_drops_both_still_recovers() {
        let db = TestDbFile::new("_test_power_loss_both");

        let page_id = {
            let mut pager = Pager::new(db.path(), 64).expect("open");
            let page_id = pager.allocate_page().expect("allocate");
            commit_fill(&mut pager, page_id, 0x11);

            pager.begin_transaction().expect("begin");
            let mut page = Page::new(page_id);
            page.data.fill(0x22);
            pager.write_page(page).expect("write");
            pager.inject_io_failure_after(0);
            assert!(
                pager.commit_transaction().is_err(),
                "failure did not surface"
            );
            pager.simulate_journal_power_loss().expect("journal loss");
            pager.simulate_database_power_loss().expect("database loss");
            page_id
        };

        let pager = Pager::new(db.path(), 64).expect("reopen");
        assert_eq!(pager.read_page(page_id).expect("read").data[0], 0x11);
    }

    /// A WAL append interrupted partway must not be treated as committed data.
    ///
    /// Frames are written and only then synced, so a power loss leaves a partial
    /// frame at the end of the file. Recovery has to ignore everything past the last
    /// complete commit — here, the transaction that was torn.
    #[test]
    fn a_torn_wal_append_is_ignored_on_reopen() {
        let db = TestDbFile::new("_test_torn_wal_append");

        let (page_id, committed_len) = {
            let mut pager = Pager::new(db.path(), 64).expect("open");
            pager
                .set_journal_mode(crate::storage::JournalMode::Wal)
                .expect("switch to wal");
            let page_id = pager.allocate_page().expect("allocate");
            commit_fill(&mut pager, page_id, 0x33);
            let committed_len = pager.wal_len().expect("wal len");

            // A second committed transaction, then tear the file back so that the
            // second commit is only partly present.
            commit_fill(&mut pager, page_id, 0x44);
            let torn_len = committed_len + (pager.wal_len().expect("wal len") - committed_len) / 2;
            pager
                .simulate_torn_wal_append(torn_len)
                .expect("tear the wal");
            (page_id, committed_len)
        };
        assert!(committed_len > 0, "no wal was written");

        let pager = Pager::new(db.path(), 64).expect("reopen");
        assert_eq!(
            pager.read_page(page_id).expect("read").data[0],
            0x33,
            "a partially appended commit was treated as durable"
        );
    }

    /// A commit that finished, then a crash before the journal was removed, must not
    /// be rolled back.
    ///
    /// This is the durability property the third fsync used to buy. The journal is
    /// still on disk, full of pre-images, and marked active — the exact shape
    /// recovery treats as "roll this back". What stops it is the commit intent: every
    /// page listed already holds the contents the commit meant to write, so recovery
    /// concludes the commit landed and discards the journal instead of replaying it.
    ///
    /// Verified to fail if the intent check is forced to report "not fulfilled".
    #[test]
    fn a_finished_commit_survives_a_crash_before_the_journal_is_removed() {
        let db = TestDbFile::new("_test_commit_then_crash_before_cleanup");

        let page_id = {
            let mut pager = Pager::new(db.path(), 64).expect("open");
            let page_id = pager.allocate_page().expect("allocate");
            commit_fill(&mut pager, page_id, 0x11);

            // Commit for real, but leave the journal behind as a crash would.
            pager.keep_journal_after_commit();
            commit_fill(&mut pager, page_id, 0x22);
            page_id
        };

        let pager = Pager::new(db.path(), 64).expect("reopen");
        assert_eq!(
            pager.read_page(page_id).expect("read").data[0],
            0x22,
            "recovery rolled back a committed transaction"
        );
    }

    /// The other side of the same coin: an *unfinished* commit that left a journal
    /// behind must still be rolled back, even though the journal now looks the same.
    ///
    /// Together with the test above this pins both directions of the oracle. If it
    /// answered "fulfilled" unconditionally, this one fails; if it answered "not
    /// fulfilled" unconditionally, the one above fails.
    #[test]
    fn an_unfinished_commit_is_still_rolled_back() {
        let db = TestDbFile::new("_test_unfinished_commit_rolled_back");

        let (first, second) = {
            let mut pager = Pager::new(db.path(), 64).expect("open");
            let first = pager.allocate_page().expect("allocate");
            let second = pager.allocate_page().expect("allocate");
            commit_fill(&mut pager, first, 0xA1);
            commit_fill(&mut pager, second, 0xA2);

            pager.begin_transaction().expect("begin");
            for (page_id, fill) in [(first, 0xB1), (second, 0xB2)] {
                let mut page = Page::new(page_id);
                page.data.fill(fill);
                pager.write_page(page).expect("write");
            }
            pager.inject_io_failure_after(1);
            assert!(
                pager.commit_transaction().is_err(),
                "injected failure did not surface"
            );
            (first, second)
        };

        let pager = Pager::new(db.path(), 64).expect("reopen");
        assert_eq!(pager.read_page(first).expect("read first").data[0], 0xA1);
        assert_eq!(pager.read_page(second).expect("read second").data[0], 0xA2);
    }
}

/// The WAL has to stop growing on its own.
///
/// Nothing called `checkpoint_wal` unless the embedding application did, so a
/// long-running writer grew the file without bound — and every reader whose snapshot
/// went stale re-read and re-decoded the whole thing to rebuild its page index.
mod wal_autocheckpoint_tests {
    use crate::storage::{JournalMode, Page, Pager};
    use crate::test_utils::TestDbFile;

    #[test]
    fn a_long_run_of_commits_does_not_grow_the_wal_without_bound() {
        let db = TestDbFile::new("_test_wal_autocheckpoint");
        let mut pager = Pager::new(db.path(), 256).expect("open");
        pager
            .set_journal_mode(JournalMode::Wal)
            .expect("switch to wal");

        let mut page_ids = Vec::new();
        pager.begin_transaction().expect("begin");
        for _ in 0..16 {
            let page_id = pager.allocate_page().expect("allocate");
            pager.write_page(Page::new(page_id)).expect("initialise");
            page_ids.push(page_id);
        }
        pager.commit_transaction().expect("commit");

        // Enough commits to push well past the threshold if nothing checks in.
        let mut peak = 0u64;
        for round in 0..120u8 {
            pager.begin_transaction().expect("begin");
            for &page_id in &page_ids {
                let mut page = Page::new(page_id);
                page.data[0] = round;
                pager.write_page(page).expect("write");
            }
            pager.commit_transaction().expect("commit");
            peak = peak.max(pager.wal_len().expect("wal len"));
        }

        // Roughly 2000 frames' worth of writes. Without a checkpoint the file holds
        // all of them; the bound here is generous enough not to be brittle and tight
        // enough that never checkpointing fails it.
        let frame_size = 28 + crate::storage::PAGE_SIZE as u64;
        let peak_frames = peak / frame_size;
        assert!(
            peak_frames < 1600,
            "wal reached {peak_frames} frames, so it is not being checked in"
        );
    }
}

/// Salts separate one WAL generation from the next.
///
/// Every frame carries the header's pair, and a mismatch means the frame predates
/// this generation. They used to be the fixed constants 0x48454D41 and 0x54495445 —
/// "HEMA" and "TITE" — so every comparison against them was tautological and the
/// guard could never fire.
mod wal_salt_tests {
    use crate::storage::wal::WalHeader;

    #[test]
    fn each_wal_generation_gets_its_own_salts() {
        let first = WalHeader::default();
        let second = WalHeader::default();

        assert!(
            first.salt_1 != second.salt_1 || first.salt_2 != second.salt_2,
            "two generations produced identical salts, so the stale-frame guard \
             cannot distinguish them"
        );

        // The retired constants, in case someone reintroduces them as a default.
        assert_ne!((first.salt_1, first.salt_2), (0x48454D41, 0x54495445));
    }
}

/// Savepoints keep shared handles to the dirty pages rather than copying them,
/// because `run_atomically` takes one per row and copying every already-dirty page
/// each time is quadratic across a transaction.
///
/// Sharing is only safe because a later write copies the page into a fresh buffer
/// instead of mutating the one the savepoint holds — the outstanding `Arc` is what
/// forces that. This checks the property that argument rests on.
mod savepoint_sharing_tests {
    use crate::storage::{JournalMode, Page, Pager};
    use crate::test_utils::TestDbFile;

    /// WAL mode has to roll back to a savepoint too.
    ///
    /// It could not until the undo log stopped being rollback-specific: WAL
    /// transactions had no savepoints, so `snapshot` fell back to copying every dirty
    /// page. Nothing in the suite covered this — when WAL restore was briefly a silent
    /// no-op, every existing test still passed.
    #[test]
    fn wal_mode_rolls_back_to_a_savepoint() {
        let db = TestDbFile::new("_test_savepoint_wal_restore");
        let mut pager = Pager::new(db.path(), 64).expect("open");
        pager
            .set_journal_mode(JournalMode::Wal)
            .expect("journal mode");

        let page_id = pager.allocate_page().expect("allocate");
        pager.begin_transaction().expect("begin");
        let mut page = Page::new(page_id);
        page.data.fill(0x11);
        pager.write_page(page).expect("write");
        pager.commit_transaction().expect("commit");

        pager.begin_transaction().expect("begin");
        let mut page = Page::new(page_id);
        page.data.fill(0x22);
        pager.write_page(page).expect("write");

        let snapshot = pager.snapshot().expect("snapshot");

        let mut page = Page::new(page_id);
        page.data.fill(0x33);
        pager.write_page(page).expect("write");

        pager.restore_snapshot(snapshot).expect("restore");
        assert_eq!(
            pager.read_page(page_id).expect("read").data[0],
            0x22,
            "restoring a WAL savepoint did not undo the write after it"
        );

        pager.commit_transaction().expect("commit");
        assert_eq!(pager.read_page(page_id).expect("read").data[0], 0x22);
    }

    /// A page the transaction created after the savepoint has no pre-image, so
    /// rolling back has to drop it rather than restore anything. Rollback used to
    /// reset the whole cache, which handled this bluntly; it now undoes only what
    /// changed, so created pages are tracked explicitly.
    #[test]
    fn rolling_back_drops_pages_created_after_the_savepoint() {
        let db = TestDbFile::new("_test_savepoint_created_pages");
        let mut pager = Pager::new(db.path(), 64).expect("open");

        let existing = pager.allocate_page().expect("allocate");
        pager.begin_transaction().expect("begin");
        let mut page = Page::new(existing);
        page.data.fill(0x11);
        pager.write_page(page).expect("write");
        pager.commit_transaction().expect("commit");

        pager.begin_transaction().expect("begin");
        let snapshot = pager.snapshot().expect("snapshot");

        let created = pager.allocate_page().expect("allocate");
        let mut page = Page::new(created);
        page.data.fill(0x99);
        pager.write_page(page).expect("write");

        pager.restore_snapshot(snapshot).expect("restore");

        // The page allocated after the mark must not survive as a dirty cache entry
        // holding 0x99.
        assert!(
            pager
                .read_page(created)
                .map(|page| page.data[0] != 0x99)
                .unwrap_or(true),
            "a page created after the savepoint survived the rollback"
        );
        // And the page that predates the transaction is untouched.
        assert_eq!(pager.read_page(existing).expect("read").data[0], 0x11);

        pager.rollback_transaction().expect("rollback");
    }

    /// The case removing the eager dirty-page copy could have broken: a page dirtied
    /// *before* the savepoint and never written again. Its contents live only in the
    /// cache, so an undo that resets the cache would lose them.
    #[test]
    fn a_page_dirtied_before_the_savepoint_and_untouched_after_survives() {
        let db = TestDbFile::new("_test_savepoint_untouched_dirty");
        let mut pager = Pager::new(db.path(), 64).expect("open");

        let quiet = pager.allocate_page().expect("allocate");
        let noisy = pager.allocate_page().expect("allocate");
        pager.begin_transaction().expect("begin");
        for (id, fill) in [(quiet, 0x11u8), (noisy, 0x22)] {
            let mut page = Page::new(id);
            page.data.fill(fill);
            pager.write_page(page).expect("write");
        }
        pager.commit_transaction().expect("commit");

        pager.begin_transaction().expect("begin");
        // Dirty both, then mark. `quiet` is never written again.
        for (id, fill) in [(quiet, 0xAAu8), (noisy, 0xBB)] {
            let mut page = Page::new(id);
            page.data.fill(fill);
            pager.write_page(page).expect("write");
        }
        let snapshot = pager.snapshot().expect("snapshot");

        let mut page = Page::new(noisy);
        page.data.fill(0xCC);
        pager.write_page(page).expect("write");

        pager.restore_snapshot(snapshot).expect("restore");
        assert_eq!(
            pager.read_page(quiet).expect("read").data[0],
            0xAA,
            "a page dirtied before the savepoint and untouched after was lost"
        );
        assert_eq!(pager.read_page(noisy).expect("read").data[0], 0xBB);

        pager.rollback_transaction().expect("rollback");
    }

    #[test]
    fn a_savepoint_image_survives_later_writes_to_the_same_page() {
        let db = TestDbFile::new("_test_savepoint_shared_image");
        let mut pager = Pager::new(db.path(), 64).expect("open");

        let page_id = pager.allocate_page().expect("allocate");
        pager.begin_transaction().expect("begin");
        let mut page = Page::new(page_id);
        page.data.fill(0x11);
        pager.write_page(page).expect("write");
        pager.commit_transaction().expect("commit");

        pager.begin_transaction().expect("begin");
        let mut page = Page::new(page_id);
        page.data.fill(0x22);
        pager.write_page(page).expect("write");

        // Savepoint here: the page holds 0x22.
        let snapshot = pager.snapshot().expect("snapshot");

        // Overwrite it twice more. If the savepoint shared a page the pager then
        // mutated in place, its image would follow along to 0x44.
        for fill in [0x33u8, 0x44] {
            let mut page = Page::new(page_id);
            page.data.fill(fill);
            pager.write_page(page).expect("write");
        }

        pager.restore_snapshot(snapshot).expect("restore");
        assert_eq!(
            pager.read_page(page_id).expect("read").data[0],
            0x22,
            "the savepoint image was mutated by later writes"
        );

        pager.rollback_transaction().expect("rollback");
        assert_eq!(
            pager.read_page(page_id).expect("read").data[0],
            0x11,
            "rollback did not restore the committed value"
        );
    }
}

/// Coordination between handles is in memory, so it leaves nothing on disk.
///
/// It used to leave four things: `.rollback.lock`, `.wal.write.lock`, a
/// `.wal.readers/` directory, and a lock file inside it per live read transaction.
/// Between them they cost about eight syscalls per commit to support cross-process
/// access the engine does not offer.
mod no_lock_sidecar_tests {
    use crate::storage::{JournalMode, Page, Pager};
    use crate::test_utils::TestDbFile;

    /// Every path that used to create a lock file: a rollback write, a rollback
    /// read, a WAL write, and a WAL read.
    fn exercise_every_lock_path(path: &str) {
        let mut pager = Pager::new(path, 8).expect("open");
        let page_id = pager.allocate_page().expect("allocate");

        pager.begin_transaction().expect("begin rollback write");
        pager.write_page(Page::new(page_id)).expect("write");
        pager.commit_transaction().expect("commit rollback");

        pager.begin_read().expect("begin rollback read");
        pager.end_read().expect("end rollback read");

        pager.set_journal_mode(JournalMode::Wal).expect("wal mode");
        pager.begin_transaction().expect("begin wal write");
        pager.write_page(Page::new(page_id)).expect("write");
        pager.commit_transaction().expect("commit wal");

        pager.begin_read().expect("begin wal read");
        pager.end_read().expect("end wal read");
    }

    #[test]
    fn locking_creates_no_sidecar_files() {
        let db = TestDbFile::new("_test_no_lock_sidecars");
        let path = db.path().to_string();
        exercise_every_lock_path(&path);

        for suffix in [".rollback.lock", ".wal.write.lock"] {
            let sidecar = format!("{path}{suffix}");
            assert!(
                !std::path::Path::new(&sidecar).exists(),
                "{suffix} was created; locking is supposed to be in memory"
            );
        }
        let readers_dir = format!("{path}.wal.readers");
        assert!(
            !std::path::Path::new(&readers_dir).exists(),
            "the reader lock directory was created; locking is supposed to be in memory"
        );
    }

    /// Two handles on one database still exclude each other, which is the whole
    /// point of keeping the state shared rather than per-pager.
    #[test]
    fn two_handles_on_one_database_still_exclude_each_other() {
        let db = TestDbFile::new("_test_two_handles_exclude");
        let mut first = Pager::new(db.path(), 8).expect("open first");
        let mut second = Pager::new(db.path(), 8).expect("open second");

        first.begin_transaction().expect("first writer begins");
        let err = second
            .begin_transaction()
            .expect_err("a second writer must not get in");
        assert!(
            err.to_string().contains("locked"),
            "unexpected error for a blocked writer: {err}"
        );

        first.rollback_transaction().expect("first writer ends");
        second
            .begin_transaction()
            .expect("the lock should be free once the first writer is done");
        second.rollback_transaction().expect("second writer ends");
    }
}

/// A rollback savepoint holds a shared handle to every page that was dirty when it was
/// taken, and `run_atomically` takes one per row. If they are not released on success
/// they accumulate for the length of the transaction: memory grows linearly in rows,
/// and — because the handles are shared — the page cache can never reclaim a buffer,
/// so every page write allocates a fresh zeroed 4 KiB one.
mod savepoint_release_tests {
    use crate::catalog::CatalogEngine;
    use crate::test_utils::TestDbFile;
    use crate::Value;

    /// Held savepoints after `rows` successful inserts in one transaction.
    fn savepoints_held_after(rows: i32, name: &str) -> usize {
        let db = TestDbFile::new(name);
        let mut engine = CatalogEngine::new(db.path()).expect("open");
        engine.create_table("t").expect("create");

        engine.begin_transaction().expect("begin");
        for i in 0..rows {
            engine
                .insert_into_table("t", vec![Value::Integer(i)])
                .expect("insert");
        }
        let held = engine
            .tree_store()
            .shared_storage()
            .read()
            .unwrap()
            .active_rollback_savepoint_count();
        engine.commit_transaction().expect("commit");
        held
    }

    /// The property is that the count does not grow with rows, not that it is zero:
    /// one savepoint stays outstanding for the statement in progress, which is
    /// bounded. Before releasing on success it was two per row — 401 held after 200
    /// rows, which is what this fails with if the release in `CatalogEngine::
    /// run_atomically` is removed.
    ///
    /// It does *not* cover the matching release in `ByteTreeStore::run_atomically`:
    /// removing that one alone leaves this passing, because inserts reach the tree
    /// through the catalog. That release is kept for the same correctness reason and
    /// for direct `ByteTreeStore` users, but it is unexercised here.
    #[test]
    fn savepoints_do_not_accumulate_across_rows_in_one_transaction() {
        let few = savepoints_held_after(10, "_test_savepoint_release_few");
        let many = savepoints_held_after(200, "_test_savepoint_release_many");

        assert_eq!(
            few, many,
            "savepoints held grew from {few} at 10 rows to {many} at 200 -- they are \
             accumulating again, and each pins every dirty page"
        );
        assert!(
            many <= 1,
            "expected at most one outstanding savepoint, found {many}"
        );
    }
}

/// Diagnostic: at what size does per-page integrity checking silently switch off?
/// `cargo test --release checksum_cliff -- --ignored --nocapture`
mod checksum_cliff_probe {
    use crate::storage::{JournalMode, Page, Pager};
    use crate::test_utils::TestDbFile;

    #[test]
    #[ignore]
    fn report_checksum_cliff() {
        let db = TestDbFile::new("_test_checksum_cliff");
        let mut pager = Pager::new(db.path(), 64).expect("open");
        pager.set_journal_mode(JournalMode::Rollback).expect("mode");

        let mut last_reported = 0usize;
        for round in 1..=4000u32 {
            let id = pager.allocate_page().expect("allocate");
            pager.begin_transaction().expect("begin");
            let mut page = Page::new(id);
            page.data[0] = (round % 251) as u8;
            pager.write_page(page).expect("write");
            pager.commit_transaction().expect("commit");

            let report = pager.validate_integrity().expect("validate");
            if report.checksummed_page_count < last_reported {
                println!(
                    "\nchecksums dropped after {round} pages: {} -> {}\n",
                    last_reported, report.checksummed_page_count
                );
                return;
            }
            last_reported = report.checksummed_page_count;
        }
        println!("\nno cliff within 4000 pages (peak {last_reported} checksummed)\n");
    }
}

/// The trailer exists so a page carries its own checksum, with no central map to
/// outgrow a metadata page. These check it is written, that it detects a page
/// corrupted on disk, and that it is seeded per page so a page written to the wrong
/// offset does not read as valid.
mod page_trailer_tests {
    use crate::storage::file_manager::{FileManager, PageTrailerState};
    use crate::storage::{Page, Pager, PAGE_TRAILER_SIZE, USABLE_PAGE_SIZE};
    use crate::test_utils::TestDbFile;
    use std::io::{Read, Seek, SeekFrom, Write};

    fn commit_page(pager: &mut Pager, fill: u8) -> u32 {
        let id = pager.allocate_page().expect("allocate");
        pager.begin_transaction().expect("begin");
        let mut page = Page::new(id);
        page.data[..USABLE_PAGE_SIZE].fill(fill);
        pager.write_page(page).expect("write");
        pager.commit_transaction().expect("commit");
        id
    }

    #[test]
    fn a_written_page_carries_a_valid_trailer() {
        let db = TestDbFile::new("_test_trailer_written");
        let mut pager = Pager::new(db.path(), 32).expect("open");
        let id = commit_page(&mut pager, 0x5A);
        drop(pager);

        let mut file = std::fs::File::open(db.path()).expect("open file");
        let mut bytes = vec![0u8; crate::storage::PAGE_SIZE];
        file.seek(SeekFrom::Start(
            id as u64 * crate::storage::PAGE_SIZE as u64,
        ))
        .expect("seek");
        file.read_exact(&mut bytes).expect("read");

        let page = Page::from_bytes(id, bytes).expect("page");
        assert!(
            page.data[USABLE_PAGE_SIZE..].iter().any(|&b| b != 0),
            "the trailer was never stamped"
        );
        assert_eq!(
            FileManager::page_trailer_state(&page),
            PageTrailerState::Valid
        );
    }

    /// The case the central map could not cover past 685 pages: a byte flipped on
    /// disk, in a page nothing has cached.
    #[test]
    fn a_flipped_byte_on_disk_fails_its_trailer() {
        let db = TestDbFile::new("_test_trailer_detects_flip");
        let mut pager = Pager::new(db.path(), 32).expect("open");
        let id = commit_page(&mut pager, 0x5A);
        drop(pager);

        let offset = id as u64 * crate::storage::PAGE_SIZE as u64;
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(db.path())
            .expect("open file");
        file.seek(SeekFrom::Start(offset + 100)).expect("seek");
        file.write_all(&[0x5B]).expect("flip");
        file.seek(SeekFrom::Start(offset)).expect("seek");
        let mut bytes = vec![0u8; crate::storage::PAGE_SIZE];
        file.read_exact(&mut bytes).expect("read");

        let page = Page::from_bytes(id, bytes).expect("page");
        assert_eq!(
            FileManager::page_trailer_state(&page),
            PageTrailerState::Mismatch,
            "a flipped content byte was not caught by the trailer"
        );
    }

    /// Seeded with the page id, so the same bytes at the wrong offset are not valid.
    /// Without the seed a page relocated by compaction would verify against its old
    /// contents.
    #[test]
    fn the_trailer_is_bound_to_its_page_id() {
        let db = TestDbFile::new("_test_trailer_seeded");
        let mut pager = Pager::new(db.path(), 32).expect("open");
        let id = commit_page(&mut pager, 0x77);
        drop(pager);

        let mut file = std::fs::File::open(db.path()).expect("open file");
        let mut bytes = vec![0u8; crate::storage::PAGE_SIZE];
        file.seek(SeekFrom::Start(
            id as u64 * crate::storage::PAGE_SIZE as u64,
        ))
        .expect("seek");
        file.read_exact(&mut bytes).expect("read");

        let as_written = Page::from_bytes(id, bytes.clone()).expect("page");
        assert_eq!(
            FileManager::page_trailer_state(&as_written),
            PageTrailerState::Valid
        );

        let relocated = Page::from_bytes(id + 1, bytes).expect("page");
        assert_eq!(
            FileManager::page_trailer_state(&relocated),
            PageTrailerState::Mismatch,
            "the same bytes verified under a different page id"
        );
    }

    /// A page in a freshly extended file has never been written and has nothing to
    /// verify against; that is reported as unstamped rather than as corruption.
    #[test]
    fn an_unwritten_page_is_unstamped_not_corrupt() {
        let page = Page::new(9);
        assert_eq!(page.data[USABLE_PAGE_SIZE..].len(), PAGE_TRAILER_SIZE);
        assert_eq!(
            FileManager::page_trailer_state(&page),
            PageTrailerState::Unstamped
        );
    }
}
