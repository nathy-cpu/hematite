//! Relational row container used by catalog access methods.

use super::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct StoredRow {
    pub row_id: u64,
    pub values: Vec<Value>,
}
