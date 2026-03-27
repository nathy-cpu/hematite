//! Hematite - Embedded Relational Database Management System
//!
//! A lightweight, type-safe embedded database with MySQL-compatible syntax.
//! Built entirely with the standard Rust library.

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

pub use catalog::{
    Catalog, CatalogEngine, CatalogIntegrityReport, CatalogStorageStats, Column, DataType, Schema,
    StoredRow, Table, TableCursor, Value,
};
pub use error::{HematiteError, Result};
pub use parser::parser::Parser;
pub use parser::{ast::*, Lexer};
pub use query::{ExecutionContext, QueryExecutor, QueryPlanner, QueryResult};
pub use sql::{
    Connection, Database, Hematite, PreparedStatement, ResultSet, Row, StatementResult, Transaction,
};
pub use storage::Pager;
