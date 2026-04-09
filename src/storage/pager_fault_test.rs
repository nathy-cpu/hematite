use crate::storage::pager::{Pager, PagerState};
use crate::storage::wal::{WalFrame, WalRecord};
use crate::storage::types::{Page, DB_HEADER_PAGE_ID};
use crate::error::Result;
use crate::test_utils::TestDbFile;
use crate::storage::JournalMode;
use std::fs;

#[test]
fn test_pager_enters_error_state_on_flush_failure() -> Result<()> {
    let test_db = TestDbFile::new("pager_fault_flush");
    let mut pager = Pager::new(&test_db, 10)?;

    // 1. Initial state should be Open
    assert_eq!(pager.state(), PagerState::Open);

    // 2. Perform a write
    let mut page = Page::new(DB_HEADER_PAGE_ID);
    page.data[0] = 0xAA;
    pager.write_page(page)?;

    // 3. Inject failure and try to flush
    pager.inject_io_failure();
    let result = pager.flush();

    // 4. Flush should fail
    assert!(result.is_err());

    // 5. State should now be Error
    assert_eq!(pager.state(), PagerState::Error);

    // 6. Subsequent operations should fail immediately
    let read_result = pager.read_page(DB_HEADER_PAGE_ID);
    assert!(read_result.is_err());
    assert!(read_result.unwrap_err().to_string().contains("Pager is in an error state"));

    Ok(())
}

#[test]
fn test_pager_recovery_from_error_state_via_rollback() -> Result<()> {
    let test_db = TestDbFile::new("pager_fault_rollback");
    
    // Create initial DB
    {
        let mut pager = Pager::new(&test_db, 10)?;
        let mut page = Page::new(2); // Some payload page
        page.data[0] = 1;
        pager.write_page(page)?;
        pager.flush()?;
    }

    let mut pager = Pager::new(&test_db, 10)?;
    pager.begin_transaction()?;
    
    let mut page = Page::new(2);
    page.data[0] = 2;
    pager.write_page(page)?;

    // Inject failure during flush (which happens during commit in some modes or explicit flush)
    // For Rollback mode, commit flushes.
    pager.inject_io_failure();
    let commit_result = pager.commit_transaction();
    assert!(commit_result.is_err());
    assert_eq!(pager.state(), PagerState::Error);

    // Rollback should be possible? Actually, the plan says:
    // "reject all reads and mutations until a hard reset or rollback is performed."
    
    // Let's check if rollback_transaction clears the error state.
    // Looking at pager.rs, rollback might need to be updated to clear Error state if it's safe.
    // Usually, SQLite requires a rollback to clear the error.
    
    pager.rollback_transaction()?;
    assert_eq!(pager.state(), PagerState::Open);

    // Should be able to read the original value now
    let restored_page = pager.read_page(2)?;
    assert_eq!(restored_page.data[0], 1);

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
    assert!(
        err.to_string()
            .contains("cannot upgrade a shared database lock to a write lock")
    );
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
        file_len: 64 + 3 * crate::storage::PAGE_SIZE as u64,
        free_pages: vec![],
        checksums: vec![(page_id, 123)],
        frames: vec![WalFrame {
            page_id,
            data: vec![42u8; crate::storage::PAGE_SIZE],
        }],
    }])?;
    let mut wal_bytes = fs::read(&wal_path)?;
    wal_bytes.extend_from_slice(&partial_tail[8..partial_tail.len() - 17]);
    fs::write(&wal_path, wal_bytes)?;

    let mut reopened = Pager::new(&test_db, 10)?;
    reopened.begin_read()?;
    assert_eq!(reopened.wal_snapshot_sequence(), Some(1));
    assert_eq!(reopened.read_page(page_id)?.data[0], 99);
    reopened.end_read()?;

    Ok(())
}
