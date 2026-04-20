mod cache_tests {
    use super::super::cache::PageCache;
    use crate::storage::Page;

    #[test]
    fn dirty_pages_are_not_evicted_under_capacity_pressure() {
        let mut cache = PageCache::new(1);

        let mut first = Page::new(1);
        first.data[0] = 10;
        cache.put(first);
        cache.mark_dirty(1);

        let mut second = Page::new(2);
        second.data[0] = 20;
        cache.put(second);

        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_some());
        assert!(cache.is_dirty(1));
    }

    #[test]
    fn pinned_pages_are_not_evicted() {
        let mut cache = PageCache::new(1);
        cache.put(Page::new(1));
        let held = cache.get(1).expect("page should be cached");
        cache.put(Page::new(2));

        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_some());
        drop(held);
    }

    #[test]
    fn shared_handles_count_as_live_pins_until_dropped() {
        let mut cache = PageCache::new(2);
        cache.put(Page::new(1));

        let held = cache.get(1).expect("page should be cached");
        assert_eq!(cache.pin_count(1), 1);

        cache.put(Page::new(2));
        assert!(cache.peek(1).is_some());
        assert!(cache.peek(2).is_some());

        drop(held);
        assert_eq!(cache.pin_count(1), 0);

        cache.put(Page::new(3));
        assert!(cache.peek(1).is_none());
        assert!(cache.peek(2).is_some());
        assert!(cache.peek(3).is_some());
    }

    #[test]
    fn dirty_pages_keep_first_dirty_order() {
        let mut cache = PageCache::new(4);
        cache.put(Page::new(1));
        cache.put(Page::new(2));
        cache.put(Page::new(3));

        cache.mark_dirty(2);
        cache.mark_dirty(1);
        cache.mark_dirty(2);
        cache.mark_dirty(3);

        assert_eq!(cache.dirty_page_ids(), vec![2, 1, 3]);
    }

    #[test]
    fn cache_hits_do_not_reorder_dirty_writeback_order() {
        let mut cache = PageCache::new(4);
        cache.put(Page::new(1));
        cache.put(Page::new(2));
        cache.put(Page::new(3));

        cache.mark_dirty(1);
        cache.mark_dirty(2);
        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_some());
        assert!(cache.get(1).is_some());

        assert_eq!(cache.dirty_page_ids(), vec![1, 2]);
    }

    #[test]
    fn peek_does_not_update_lru_order() {
        let mut cache = PageCache::new(2);
        cache.put(Page::new(1));
        cache.put(Page::new(2));

        assert!(cache.peek(1).is_some());
        cache.put(Page::new(3));

        assert!(cache.get(1).is_none());
        assert!(cache.get(2).is_some());
        assert!(cache.get(3).is_some());
    }

    #[test]
    fn cache_metadata_flags_track_page_state() {
        let mut cache = PageCache::new(2);
        cache.put(Page::new(1));

        cache.pin(1);
        cache.mark_dirty(1);
        cache.mark_journaled(1);
        cache.mark_need_sync(1);
        cache.set_dont_write(1, true);

        let meta = cache.meta(1).expect("page metadata should exist");
        assert_eq!(meta.manual_pin_count, 1);
        assert!(meta.dirty);
        assert!(meta.writeable);
        assert!(meta.journaled);
        assert!(meta.need_sync);
        assert!(meta.dont_write);
        assert!(meta.dirty_sequence.is_some());
        assert_eq!(cache.pin_count(1), 1);

        cache.unpin(1);
        cache.clear_dirty(1);

        let meta = cache.meta(1).expect("page metadata should still exist");
        assert_eq!(meta.manual_pin_count, 0);
        assert!(!meta.dirty);
        assert!(!meta.writeable);
        assert!(!meta.journaled);
        assert!(!meta.need_sync);
        assert!(!meta.dont_write);
        assert!(meta.dirty_sequence.is_none());
        assert_eq!(cache.pin_count(1), 0);
    }

    #[test]
    fn view_tokens_filter_stale_read_hits_without_resetting_entries() {
        let mut cache = PageCache::new(2);
        let mut page = Page::new(1);
        page.data[0] = 7;
        cache.put_with_view(page, 11);

        assert!(cache.get_for_view(1, 11).is_some());
        assert!(cache.get_for_view(1, 12).is_none());

        cache.set_view_token(1, 12);
        let page = cache.get_for_view(1, 12).expect("page should be visible");
        assert_eq!(page.data[0], 7);
    }
}

mod fault_tests {
    use super::super::{Pager, PagerState};
    use crate::error::Result;
    use crate::storage::types::{Page, DB_HEADER_PAGE_ID, FIRST_ALLOCATABLE_PAGE_ID};
    use crate::storage::wal::{WalFrame, WalRecord};
    use crate::storage::JournalMode;
    use crate::test_utils::TestDbFile;
    use std::fs;

    #[test]
    fn test_pager_enters_error_state_on_flush_failure() -> Result<()> {
        let test_db = TestDbFile::new("pager_fault_flush");
        let mut pager = Pager::new(&test_db, 10)?;

        assert_eq!(pager.state(), PagerState::Open);

        let mut page = Page::new(DB_HEADER_PAGE_ID);
        page.data[0] = 0xAA;
        pager.write_page(page)?;

        pager.inject_io_failure();
        let result = pager.flush();

        assert!(result.is_err());
        assert_eq!(pager.state(), PagerState::Error);

        let read_result = pager.read_page(DB_HEADER_PAGE_ID);
        assert!(read_result.is_err());
        assert!(read_result
            .unwrap_err()
            .to_string()
            .contains("Pager is in an error state"));

        Ok(())
    }

    #[test]
    fn test_pager_recovery_from_error_state_via_rollback() -> Result<()> {
        let test_db = TestDbFile::new("pager_fault_rollback");

        {
            let mut pager = Pager::new(&test_db, 10)?;
            let mut page = Page::new(FIRST_ALLOCATABLE_PAGE_ID);
            page.data[0] = 1;
            pager.write_page(page)?;
            pager.flush()?;
        }

        let mut pager = Pager::new(&test_db, 10)?;
        pager.begin_transaction()?;

        let mut page = Page::new(FIRST_ALLOCATABLE_PAGE_ID);
        page.data[0] = 2;
        pager.write_page(page)?;

        pager.inject_io_failure();
        let commit_result = pager.commit_transaction();
        assert!(commit_result.is_err());
        assert_eq!(pager.state(), PagerState::Error);

        pager.rollback_transaction()?;
        assert_eq!(pager.state(), PagerState::Open);

        let restored_page = pager.read_page(FIRST_ALLOCATABLE_PAGE_ID)?;
        assert_eq!(restored_page.data[0], 1);

        Ok(())
    }

    #[test]
    fn test_pager_restore_snapshot_clears_error_state_after_spill_failure() -> Result<()> {
        let test_db = TestDbFile::new("pager_fault_restore_snapshot_error_state");
        let mut pager = Pager::new(&test_db, 1)?;
        let page_id = pager.allocate_page()?;

        let mut initial = Page::new(page_id);
        initial.data[0] = 1;
        pager.write_page(initial)?;
        pager.flush()?;

        pager.begin_transaction()?;
        let snapshot = pager.snapshot()?;

        let mut updated = pager.read_page(page_id)?;
        updated.data[0] = 2;

        pager.inject_io_failure_after(0);
        let err = pager.write_page(updated).unwrap_err();
        assert!(err.to_string().contains("Injected IO error"));
        assert_eq!(pager.state(), PagerState::Error);

        pager.restore_snapshot(snapshot)?;
        assert!(matches!(
            pager.state(),
            PagerState::WriterLocked | PagerState::WriterCacheMod
        ));
        assert_eq!(pager.read_page(page_id)?.data[0], 1);

        pager.rollback_transaction()?;
        assert_eq!(pager.state(), PagerState::Open);

        Ok(())
    }

    #[test]
    fn test_pager_state_tracks_reader_scope() -> Result<()> {
        let test_db = TestDbFile::new("pager_state_reader_scope");
        let mut pager = Pager::new(&test_db, 10)?;

        assert_eq!(pager.state(), PagerState::Open);
        pager.begin_read()?;
        assert_eq!(pager.state(), PagerState::Reader);
        pager.end_read()?;
        assert_eq!(pager.state(), PagerState::Open);

        Ok(())
    }

    #[test]
    fn test_pager_state_tracks_writer_progression() -> Result<()> {
        let test_db = TestDbFile::new("pager_state_writer_progression");
        let mut pager = Pager::new(&test_db, 10)?;

        let page_id = pager.allocate_page()?;
        let mut initial = Page::new(page_id);
        initial.data[0] = 1;
        pager.write_page(initial)?;
        pager.flush()?;
        assert_eq!(pager.state(), PagerState::Open);

        pager.begin_transaction()?;
        assert_eq!(pager.state(), PagerState::WriterLocked);

        let mut updated = pager.read_page(page_id)?;
        updated.data[0] = 2;
        pager.write_page(updated)?;
        assert_eq!(pager.state(), PagerState::WriterCacheMod);

        pager.flush()?;
        assert_eq!(pager.state(), PagerState::WriterDbMod);

        pager.rollback_transaction()?;
        assert_eq!(pager.state(), PagerState::Open);
        assert_eq!(pager.read_page(page_id)?.data[0], 1);

        Ok(())
    }

    #[test]
    fn test_pager_reader_scope_cannot_upgrade_to_writer_transaction() -> Result<()> {
        let test_db = TestDbFile::new("pager_state_reader_cannot_upgrade");
        let mut pager = Pager::new(&test_db, 10)?;

        pager.begin_read()?;
        assert_eq!(pager.state(), PagerState::Reader);

        let err = pager.begin_transaction().unwrap_err();
        assert!(err
            .to_string()
            .contains("cannot upgrade a shared database lock to a write lock"));
        assert_eq!(pager.state(), PagerState::Reader);

        pager.end_read()?;
        assert_eq!(pager.state(), PagerState::Open);

        Ok(())
    }

    #[test]
    fn test_pager_begin_read_during_write_transaction_keeps_writer_state() -> Result<()> {
        let test_db = TestDbFile::new("pager_state_reader_inside_writer");
        let mut pager = Pager::new(&test_db, 10)?;

        pager.begin_transaction()?;
        assert_eq!(pager.state(), PagerState::WriterLocked);

        pager.begin_read()?;
        assert_eq!(pager.state(), PagerState::WriterLocked);

        let page_id = pager.allocate_page()?;
        let mut page = Page::new(page_id);
        page.data[0] = 7;
        pager.write_page(page)?;
        assert_eq!(pager.state(), PagerState::WriterCacheMod);

        pager.end_read()?;
        assert_eq!(pager.state(), PagerState::WriterCacheMod);

        pager.rollback_transaction()?;
        assert_eq!(pager.state(), PagerState::Open);

        Ok(())
    }

    #[test]
    fn test_pager_checkpoint_failure_enters_error_state_and_reopen_uses_wal_state() -> Result<()> {
        let test_db = TestDbFile::new("pager_fault_checkpoint_failure");
        let wal_path = std::path::PathBuf::from(format!("{}.wal", test_db.path()));

        let (first_page_id, second_page_id) = {
            let mut setup = Pager::new(&test_db, 10)?;
            let first_page_id = setup.allocate_page()?;
            let second_page_id = setup.allocate_page()?;

            let mut first_page = Page::new(first_page_id);
            first_page.data[0] = 10;
            setup.write_page(first_page)?;

            let mut second_page = Page::new(second_page_id);
            second_page.data[0] = 20;
            setup.write_page(second_page)?;
            setup.flush()?;
            setup.set_journal_mode(JournalMode::Wal)?;
            (first_page_id, second_page_id)
        };

        let mut pinned_reader = Pager::new(&test_db, 10)?;
        pinned_reader.begin_read()?;

        let mut writer = Pager::new(&test_db, 10)?;
        writer.begin_transaction()?;
        let mut first_page = writer.read_page(first_page_id)?;
        first_page.data[0] = 11;
        writer.write_page(first_page)?;

        let mut second_page = writer.read_page(second_page_id)?;
        second_page.data[0] = 21;
        writer.write_page(second_page)?;
        writer.commit_transaction()?;
        assert!(wal_path.exists());

        pinned_reader.end_read()?;

        writer.inject_io_failure_after(1);
        let checkpoint_err = writer.checkpoint_wal().unwrap_err();
        assert!(checkpoint_err.to_string().contains("Injected IO error"));
        assert_eq!(writer.state(), PagerState::Error);
        assert!(wal_path.exists());

        let mut reopened = Pager::new(&test_db, 10)?;
        reopened.begin_read()?;
        assert_eq!(reopened.read_page(first_page_id)?.data[0], 11);
        assert_eq!(reopened.read_page(second_page_id)?.data[0], 21);
        reopened.end_read()?;

        Ok(())
    }

    #[test]
    fn test_pager_reopen_ignores_truncated_wal_tail_after_committed_state() -> Result<()> {
        let test_db = TestDbFile::new("pager_fault_truncated_wal_tail");
        let wal_path = std::path::PathBuf::from(format!("{}.wal", test_db.path()));

        let page_id = {
            let mut setup = Pager::new(&test_db, 10)?;
            let page_id = setup.allocate_page()?;
            let mut page = Page::new(page_id);
            page.data[0] = 7;
            setup.write_page(page)?;
            setup.flush()?;
            setup.set_journal_mode(JournalMode::Wal)?;
            page_id
        };

        let mut pinned_reader = Pager::new(&test_db, 10)?;
        pinned_reader.begin_read()?;

        let mut writer = Pager::new(&test_db, 10)?;
        writer.begin_transaction()?;
        let mut page = writer.read_page(page_id)?;
        page.data[0] = 99;
        writer.write_page(page)?;
        writer.commit_transaction()?;
        assert!(wal_path.exists());

        pinned_reader.end_read()?;

        let partial_tail = WalRecord::encode_file(&[WalRecord {
            sequence: 2,
            file_len: crate::storage::file_len_for_next_page_id(page_id + 1),
            free_pages: vec![],
            checksums: vec![(page_id, 123)],
            frames: vec![WalFrame::new(
                page_id,
                vec![42u8; crate::storage::PAGE_SIZE],
            )],
        }])?;
        let mut wal_bytes = fs::read(&wal_path)?;
        wal_bytes.extend_from_slice(&partial_tail[24..partial_tail.len() - 17]);
        fs::write(&wal_path, wal_bytes)?;

        let mut reopened = Pager::new(&test_db, 10)?;
        reopened.begin_read()?;
        assert_eq!(reopened.wal_snapshot_sequence(), Some(1));
        assert_eq!(reopened.read_page(page_id)?.data[0], 99);
        reopened.end_read()?;

        Ok(())
    }
}
