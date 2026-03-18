//! Storage engine module for Hematite database

pub mod buffer_pool;
pub mod database;
pub mod engine;
pub mod file_manager;
pub mod page_manager;
pub mod serialization;
pub mod table;
pub mod types;

// Re-export commonly used types
pub use database::Database;
pub use engine::StorageEngine;
pub use serialization::RowSerializer;
pub use table::TableManager;
pub use types::{
    Page, PageId, PageType, TableMetadata, TablePageHeader, DB_HEADER_PAGE_ID,
    MAX_ROWS_PER_PAGE, PAGE_SIZE, STORAGE_METADATA_PAGE_ID,
};

#[cfg(test)]
mod tests {
    use super::*;
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
}

// ... (rest of the code remains the same)
