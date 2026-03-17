//! Catalog and schema management for database objects

pub mod catalog;
pub mod catalog_new;
pub mod column;
pub mod header;
pub mod ids;
pub mod schema;
pub mod table;
pub mod tests;
pub mod types;

// Re-export main types for easier access
pub use catalog::Catalog;
pub use column::Column;
pub use header::DatabaseHeader;
pub use ids::{ColumnId, TableId};
pub use schema::Schema;
pub use table::Table;
pub use types::{DataType, Value};
