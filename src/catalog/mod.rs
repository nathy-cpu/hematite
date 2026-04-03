//! Relational catalog and access-method layer.
//!
//! Boundary:
//! - This module owns relational meaning: schemas, rows, primary keys, secondary indexes, and
//!   relational codecs.
//! - It builds on the generic [`crate::storage`] and [`crate::btree`] layers but should not be
//!   part of the future generic fork.
//! - The intended fork point is below this module: keep `storage` + `btree`, replace `catalog`,
//!   `query`, and `sql` with the next database model.

pub mod catalog;
pub mod column;
pub mod cursor;
pub mod engine;
pub(crate) mod engine_metadata;
pub mod header;
pub mod ids;
pub(crate) mod index_store;
pub(crate) mod integrity;
pub mod object;
pub mod record;
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
    CatalogEngine, CatalogIntegrityReport, CatalogStorageStats, TableRuntimeMetadata,
};
pub use header::DatabaseHeader;
pub use ids::{ColumnId, TableId};
pub use object::{NamedConstraint, NamedConstraintKind, Trigger, TriggerEvent, View};
pub use record::StoredRow;
pub use schema::Schema;
pub use serialization::{IndexKeyCodec, RowCodec, RowSerializer};
pub use table::{SecondaryIndex, Table};
pub use types::{
    DataType, DateTimeValue, DateValue, DecimalValue, Float128Value, IntervalDaySecondValue,
    IntervalYearMonthValue, JournalMode, TimeValue, TimeWithTimeZoneValue, TimestampValue, Value,
};
