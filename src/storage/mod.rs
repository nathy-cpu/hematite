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
    Page, PageId, PageType, StoredRow, TableMetadata, TablePageHeader, DB_HEADER_PAGE_ID,
    MAX_ROWS_PER_PAGE, PAGE_SIZE, STORAGE_METADATA_PAGE_ID, TABLE_PAGE_HEADER_SIZE,
};

#[cfg(test)]
mod tests;
