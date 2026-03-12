//! Hematite - Embedded Relational Database Management System
//! 
//! A lightweight, type-safe embedded database with MySQL-compatible syntax.
//! Built entirely with the standard Rust library.

pub mod storage;
pub mod error;

pub use storage::{Database, StorageEngine};
pub use error::{HematiteError, Result};
