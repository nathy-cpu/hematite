//! Schema-level relational objects beyond base tables.

use crate::error::{HematiteError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct View {
    pub name: String,
    pub query_sql: String,
    pub column_names: Vec<String>,
    pub dependencies: Vec<String>,
}

impl View {
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(HematiteError::StorageError(
                "View name cannot be empty".to_string(),
            ));
        }
        if self.query_sql.trim().is_empty() {
            return Err(HematiteError::StorageError(format!(
                "View '{}' must store a query",
                self.name
            )));
        }
        if self.column_names.is_empty() {
            return Err(HematiteError::StorageError(format!(
                "View '{}' must expose at least one column",
                self.name
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trigger {
    pub name: String,
    pub table_name: String,
    pub event: TriggerEvent,
    pub body_sql: String,
    pub old_alias: Option<String>,
    pub new_alias: Option<String>,
}

impl Trigger {
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(HematiteError::StorageError(
                "Trigger name cannot be empty".to_string(),
            ));
        }
        if self.table_name.is_empty() {
            return Err(HematiteError::StorageError(format!(
                "Trigger '{}' must reference a table",
                self.name
            )));
        }
        if self.body_sql.trim().is_empty() {
            return Err(HematiteError::StorageError(format!(
                "Trigger '{}' must store a body statement",
                self.name
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamedConstraintKind {
    Check,
    ForeignKey,
    Unique,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedConstraint {
    pub table_name: String,
    pub name: String,
    pub kind: NamedConstraintKind,
}
