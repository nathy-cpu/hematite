//! Hematite - Embedded Relational Database Management System
//!
//! A lightweight, type-safe embedded database with MySQL-compatible syntax.
//! Built entirely with the standard Rust library.

pub mod btree;
pub mod catalog;
pub mod error;
pub mod parser;
pub mod storage;

pub use btree::tree::BTreeManager;
pub use catalog::{Catalog, Column, DataType, Schema, Table, Value};
pub use error::{HematiteError, Result};
pub use parser::parser::Parser;
pub use parser::{ast::*, Lexer};
pub use storage::{Database, StorageEngine};
