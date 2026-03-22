//! Storage engine module for Hematite database

pub mod buffer_pool;
pub mod cursor;
pub mod database;
pub mod engine;
pub mod file_manager;
pub mod free_list;
pub mod index_cache;
pub mod overflow;
pub mod pager;
pub mod row_id;
pub mod serialization;
pub mod table_btree;
pub mod types;

// Re-export commonly used types
pub use database::Database;
pub use engine::StorageEngine;
pub use serialization::RowSerializer;
pub use types::{
    Page, PageId, PagerIntegrityReport, StorageIntegrityReport, StorageStats, StoredRow,
    TableMetadata, DB_HEADER_PAGE_ID, PAGE_SIZE, STORAGE_METADATA_PAGE_ID,
};

#[cfg(test)]
mod tests;
