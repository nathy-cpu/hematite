//! Schema-level relational objects beyond base tables.

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
