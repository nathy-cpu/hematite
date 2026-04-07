use crate::storage::pager::{Pager, PagerState};
use crate::storage::types::{Page, DB_HEADER_PAGE_ID};
use crate::error::Result;
use crate::test_utils::TestDbFile;

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
