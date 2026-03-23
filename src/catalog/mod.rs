//! Catalog and schema management for database objects

pub mod catalog;
pub mod column;
pub mod cursor;
pub mod engine;
pub(crate) mod engine_metadata;
pub mod header;
pub mod ids;
pub(crate) mod index_store;
pub(crate) mod integrity;
pub mod row_id;
pub(crate) mod runtime_metadata;
pub mod schema;
pub(crate) mod schema_store;
pub mod serialization;
pub mod table;
pub(crate) mod table_store;
pub mod tests;
pub mod types;

// Re-export main types for easier access
pub use catalog::Catalog;
pub use column::Column;
pub use cursor::{IndexCursor, TableCursor};
pub use engine::{
    CatalogEngine, CatalogIntegrityReport, CatalogStorageStats, StoredRow, TableRuntimeMetadata,
};
pub use header::DatabaseHeader;
pub use ids::{ColumnId, TableId};
pub use schema::Schema;
pub use serialization::RowSerializer;
pub use table::{SecondaryIndex, Table};
pub use types::{DataType, JournalMode, Value};
