//! ID types for database objects

/// Unique identifier for database tables
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableId(u32);

impl TableId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

/// Unique identifier for table columns
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnId(u32);

impl ColumnId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_id() {
        let table_id = TableId::new(42);
        assert_eq!(table_id.as_u32(), 42);
    }

    #[test]
    fn test_column_id() {
        let column_id = ColumnId::new(123);
        assert_eq!(column_id.as_u32(), 123);
    }

    #[test]
    fn test_table_id_hash() {
        use std::collections::HashSet;

        let id1 = TableId::new(1);
        let id2 = TableId::new(1);
        let id3 = TableId::new(2);

        let mut set = HashSet::new();
        set.insert(id1);
        set.insert(id2); // Same value, shouldn't increase size
        set.insert(id3);

        assert_eq!(set.len(), 2);
        assert!(set.contains(&TableId::new(1)));
        assert!(set.contains(&TableId::new(2)));
    }

    #[test]
    fn test_column_id_hash() {
        use std::collections::HashSet;

        let id1 = ColumnId::new(1);
        let id2 = ColumnId::new(1);
        let id3 = ColumnId::new(2);

        let mut set = HashSet::new();
        set.insert(id1);
        set.insert(id2); // Same value, shouldn't increase size
        set.insert(id3);

        assert_eq!(set.len(), 2);
        assert!(set.contains(&ColumnId::new(1)));
        assert!(set.contains(&ColumnId::new(2)));
    }

    #[test]
    fn test_table_id_equality() {
        let id1 = TableId::new(42);
        let id2 = TableId::new(42);
        let id3 = TableId::new(43);

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_column_id_equality() {
        let id1 = ColumnId::new(42);
        let id2 = ColumnId::new(42);
        let id3 = ColumnId::new(43);

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_table_id_debug() {
        let id = TableId::new(42);
        let debug_str = format!("{:?}", id);
        assert!(debug_str.contains("42"));
    }

    #[test]
    fn test_column_id_debug() {
        let id = ColumnId::new(42);
        let debug_str = format!("{:?}", id);
        assert!(debug_str.contains("42"));
    }
}
