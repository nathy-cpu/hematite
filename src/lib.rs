//! Hematite is a small embeddable SQL database written in Rust.
//!
//! It is designed to stay lightweight enough to repurpose and extend while still offering a
//! surprisingly broad SQL surface: DDL, transactions, views, triggers, joins, aggregates, window
//! functions, recursive CTEs, savepoints, rich scalar expressions, and a custom type system.
//!
//! # Quick Start
//!
//! ```no_run
//! use hematite::Hematite;
//!
//! fn main() -> hematite::Result<()> {
//!     let mut db = Hematite::new_in_memory()?;
//!     db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")?;
//!     db.execute("INSERT INTO users (id, name) VALUES (1, 'Ada');")?;
//!
//!     let names = db.query("SELECT name FROM users ORDER BY id;")?;
//!     assert_eq!(names.columns, vec!["name"]);
//!     assert_eq!(names.rows.len(), 1);
//!     Ok(())
//! }
//! ```
//!
//! # Main Entry Points
//!
//! - [`Hematite`] is the high-level library facade.
//! - [`Connection`] is the lower-level SQL connection boundary.
//! - [`PreparedStatement`] supports repeated execution with parameters.
//! - [`Transaction`] wraps explicit transactions.
//! - [`ResultSet`], [`Row`], and [`StatementResult`] are the primary result types.
//!
//! # Project Layout
//!
//! The core architecture is layered:
//!
//! ```text
//! sql -> parser -> query -> catalog -> btree -> storage
//! ```
//!
//! - `sql`: user-facing API, script stepping, transactions, CLI-facing behavior
//! - `parser`: lexer, AST, and syntax validation
//! - `query`: planning, execution, coercion, metadata shaping
//! - `catalog`: schema, row typing, metadata persistence, logical encoding
//! - `btree`: generic key/value tree over byte payloads
//! - `storage`: pager, WAL/rollback journal, page and row primitives
//!
//! # More Documentation
//!
//! - Repository quick start: `README.md`
//! - Internal architecture guide: `docs/architecture.md`
//! - Module and codebase guide: `docs/codebase-guide.md`
//! - SQL dialect and support matrix: `docs/sql-dialect.md`

pub mod btree;
pub mod catalog;
pub mod error;
pub mod parser;
pub mod query;
pub mod sql;
pub mod storage;

#[cfg(test)]
mod architecture_tests;

#[cfg(test)]
pub mod test_utils;

pub use catalog::{Catalog, Column, DataType, Schema, StoredRow, Table, TableCursor, Value};
pub use error::{HematiteError, Result};
pub use parser::parser::Parser;
pub use parser::{ast::*, Lexer};
pub use sql::{
    script_is_complete, Connection, Database, ExecutedStatement, FromRow, FromValue, Hematite,
    PreparedStatement, ResultSet, Row, StatementResult, Transaction,
};
