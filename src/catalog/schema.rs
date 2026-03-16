//! Schema management for database metadata

use super::column::Column;
use super::table::Table;
use super::TableId;
use crate::error::{HematiteError, Result};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Schema {
    tables: HashMap<TableId, Table>,
    table_names: HashMap<String, TableId>,
    next_table_id: u32,
    next_column_id: u32,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            table_names: HashMap::new(),
            next_table_id: 1,
            next_column_id: 1,
        }
    }

    pub fn create_table(&mut self, name: String, columns: Vec<Column>) -> Result<TableId> {
        // Check for duplicate table name
        if self.table_names.contains_key(&name) {
            return Err(HematiteError::ParseError(format!(
                "Table '{}' already exists",
                name
            )));
        }

        // Validate column names are unique
        let mut column_names = std::collections::HashSet::new();
        for col in &columns {
            if column_names.contains(&col.name) {
                return Err(HematiteError::ParseError(format!(
                    "Duplicate column name '{}'",
                    col.name
                )));
            }
            column_names.insert(col.name.clone());
        }

        let table_id = TableId::new(self.next_table_id);
        self.next_table_id += 1;

        // For now, we'll use a placeholder root page ID
        // This will be assigned when the table is actually created in storage
        let root_page_id = crate::storage::PageId::new(0);

        let table = Table::new(table_id, name.clone(), columns, root_page_id)?;

        self.tables.insert(table_id, table);
        self.table_names.insert(name, table_id);

        Ok(table_id)
    }

    pub fn get_table(&self, table_id: TableId) -> Option<&Table> {
        self.tables.get(&table_id)
    }

    pub fn get_table_by_name(&self, name: &str) -> Option<&Table> {
        self.table_names
            .get(name)
            .and_then(|&id| self.tables.get(&id))
    }

    pub fn drop_table(&mut self, table_id: TableId) -> Result<()> {
        let table = self
            .tables
            .get(&table_id)
            .ok_or_else(|| HematiteError::StorageError("Table not found".to_string()))?;

        let name = table.name.clone();
        self.tables.remove(&table_id);
        self.table_names.remove(&name);

        Ok(())
    }

    pub fn list_tables(&self) -> Vec<(TableId, String)> {
        self.tables
            .iter()
            .map(|(&id, table)| (id, table.name.clone()))
            .collect()
    }

    pub fn get_table_count(&self) -> usize {
        self.tables.len()
    }

    pub fn get_total_column_count(&self) -> usize {
        self.tables.values().map(|table| table.column_count()).sum()
    }

    pub fn validate(&self) -> Result<()> {
        // Check for orphaned table names
        for (name, &table_id) in &self.table_names {
            if !self.tables.contains_key(&table_id) {
                return Err(HematiteError::CorruptedData(format!(
                    "Orphaned table name '{}' references non-existent table",
                    name
                )));
            }
        }

        // Check for tables without names
        for (&table_id, table) in &self.tables {
            if !self.table_names.contains_key(&table.name) {
                return Err(HematiteError::CorruptedData(format!(
                    "Table '{}' ({}) has no name entry",
                    table.name,
                    table_id.as_u32()
                )));
            }
        }

        Ok(())
    }

    pub fn serialize(&self, buffer: &mut Vec<u8>) -> Result<()> {
        // Next table ID (4 bytes)
        buffer.extend_from_slice(&self.next_table_id.to_le_bytes());

        // Next column ID (4 bytes)
        buffer.extend_from_slice(&self.next_column_id.to_le_bytes());

        // Table count (4 bytes)
        buffer.extend_from_slice(&(self.tables.len() as u32).to_le_bytes());

        // Tables
        for table in self.tables.values() {
            table.serialize(buffer)?;
        }

        Ok(())
    }

    pub fn deserialize(buffer: &[u8]) -> Result<Self> {
        let mut offset = 0;

        if buffer.len() < 12 {
            return Err(HematiteError::CorruptedData(
                "Invalid schema header".to_string(),
            ));
        }

        // Next table ID
        let next_table_id = u32::from_le_bytes([
            buffer[offset],
            buffer[offset + 1],
            buffer[offset + 2],
            buffer[offset + 3],
        ]);
        offset += 4;

        // Next column ID
        let next_column_id = u32::from_le_bytes([
            buffer[offset],
            buffer[offset + 1],
            buffer[offset + 2],
            buffer[offset + 3],
        ]);
        offset += 4;

        // Table count
        let table_count = u32::from_le_bytes([
            buffer[offset],
            buffer[offset + 1],
            buffer[offset + 2],
            buffer[offset + 3],
        ]) as usize;
        offset += 4;

        let mut schema = Self {
            tables: HashMap::new(),
            table_names: HashMap::new(),
            next_table_id,
            next_column_id,
        };

        // Tables
        for _ in 0..table_count {
            let table = Table::deserialize(buffer, &mut offset)?;
            let name = table.name.clone();
            let id = table.id;

            schema.table_names.insert(name, id);
            schema.tables.insert(id, table);
        }

        Ok(schema)
    }

    pub fn set_table_root_page(
        &mut self,
        table_id: TableId,
        root_page_id: crate::storage::PageId,
    ) -> Result<()> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| HematiteError::StorageError("Table not found".to_string()))?;

        table.root_page_id = root_page_id;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::types::DataType;
    use crate::catalog::{Column, ColumnId, TableId};
    use crate::Result;
    use crate::Schema;

    fn create_test_columns() -> Vec<Column> {
        vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(2), "name".to_string(), DataType::Text),
            Column::new(ColumnId::new(3), "active".to_string(), DataType::Boolean),
        ]
    }

    #[test]
    fn test_schema_creation() {
        let schema = Schema::new();
        assert_eq!(schema.get_table_count(), 0);
        assert_eq!(schema.next_table_id, 1);
        assert_eq!(schema.next_column_id, 1);
        assert_eq!(schema.get_total_column_count(), 0);
    }

    #[test]
    fn test_create_table() -> Result<()> {
        let mut schema = Schema::new();

        let columns = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(2), "name".to_string(), DataType::Text),
        ];

        let table_id = schema.create_table("users".to_string(), columns)?;
        assert_eq!(schema.get_table_count(), 1);
        assert!(schema.get_table(table_id).is_some());
        assert!(schema.get_table_by_name("users").is_some());

        Ok(())
    }

    #[test]
    fn test_duplicate_table_name() {
        let mut schema = Schema::new();

        let columns = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
        ];

        schema
            .create_table("users".to_string(), columns.clone())
            .unwrap();

        let result = schema.create_table("users".to_string(), columns);
        assert!(result.is_err());
    }

    #[test]
    fn test_duplicate_column_names() {
        let mut schema = Schema::new();

        let columns = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(2), "id".to_string(), DataType::Text), // Duplicate name
        ];

        let result = schema.create_table("users".to_string(), columns);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Duplicate column name"));
    }

    #[test]
    fn test_drop_table() -> Result<()> {
        let mut schema = Schema::new();

        let columns = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
        ];

        let table_id = schema.create_table("users".to_string(), columns)?;
        assert_eq!(schema.get_table_count(), 1);

        schema.drop_table(table_id)?;
        assert_eq!(schema.get_table_count(), 0);
        assert!(schema.get_table(table_id).is_none());
        assert!(schema.get_table_by_name("users").is_none());

        Ok(())
    }

    #[test]
    fn test_drop_nonexistent_table() {
        let mut schema = Schema::new();
        let table_id = TableId::new(999);
        let result = schema.drop_table(table_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_tables() -> Result<()> {
        let mut schema = Schema::new();

        let columns1 = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
        ];
        let columns2 = vec![
            Column::new(ColumnId::new(2), "id".to_string(), DataType::Integer).primary_key(true),
        ];

        let table1_id = schema.create_table("users".to_string(), columns1)?;
        let table2_id = schema.create_table("products".to_string(), columns2)?;

        let tables = schema.list_tables();
        assert_eq!(tables.len(), 2);

        // Check that both tables are listed
        let table_ids: Vec<TableId> = tables.iter().map(|(id, _)| *id).collect();
        assert!(table_ids.contains(&table1_id));
        assert!(table_ids.contains(&table2_id));

        // Check table names
        let table_names: Vec<String> = tables.iter().map(|(_, name)| name.clone()).collect();
        assert!(table_names.contains(&"users".to_string()));
        assert!(table_names.contains(&"products".to_string()));

        Ok(())
    }

    #[test]
    fn test_get_table_by_name() -> Result<()> {
        let mut schema = Schema::new();

        let columns = create_test_columns();
        let table_id = schema.create_table("users".to_string(), columns)?;

        let table = schema.get_table_by_name("users");
        assert!(table.is_some());
        assert_eq!(table.unwrap().id, table_id);

        let nonexistent = schema.get_table_by_name("nonexistent");
        assert!(nonexistent.is_none());

        Ok(())
    }

    #[test]
    fn test_table_id_assignment() -> Result<()> {
        let mut schema = Schema::new();

        let columns1 = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
        ];
        let columns2 = vec![
            Column::new(ColumnId::new(2), "id".to_string(), DataType::Integer).primary_key(true),
        ];

        let table1_id = schema.create_table("table1".to_string(), columns1)?;
        let table2_id = schema.create_table("table2".to_string(), columns2)?;

        assert_eq!(table1_id.as_u32(), 1);
        assert_eq!(table2_id.as_u32(), 2);
        assert_eq!(schema.next_table_id, 3);

        Ok(())
    }

    #[test]
    fn test_get_total_column_count() -> Result<()> {
        let mut schema = Schema::new();

        assert_eq!(schema.get_total_column_count(), 0);

        let columns1 = create_test_columns(); // 3 columns
        schema.create_table("users".to_string(), columns1)?;

        assert_eq!(schema.get_total_column_count(), 3);

        let columns2 = vec![
            Column::new(ColumnId::new(4), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(5), "name".to_string(), DataType::Text),
        ]; // 2 columns
        schema.create_table("products".to_string(), columns2)?;

        assert_eq!(schema.get_total_column_count(), 5);

        Ok(())
    }

    #[test]
    fn test_schema_validation() -> Result<()> {
        let mut schema = Schema::new();

        // Valid schema should pass validation
        let columns = create_test_columns();
        schema.create_table("users".to_string(), columns)?;
        assert!(schema.validate().is_ok());

        Ok(())
    }

    #[test]
    fn test_schema_serialization_roundtrip() -> Result<()> {
        let mut original_schema = Schema::new();

        // Add some tables
        let columns1 = create_test_columns();
        let table1_id = original_schema.create_table("users".to_string(), columns1)?;

        let columns2 = vec![
            Column::new(ColumnId::new(4), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(5), "name".to_string(), DataType::Text),
        ];
        let table2_id = original_schema.create_table("products".to_string(), columns2)?;

        // Serialize
        let mut buffer = Vec::new();
        original_schema.serialize(&mut buffer)?;

        // Deserialize
        let deserialized_schema = Schema::deserialize(&buffer)?;

        // Verify structure
        assert_eq!(deserialized_schema.get_table_count(), 2);
        assert_eq!(deserialized_schema.next_table_id, 3);
        assert_eq!(deserialized_schema.next_column_id, 1); // Column IDs are not auto-assigned

        // Verify tables
        assert!(deserialized_schema.get_table(table1_id).is_some());
        assert!(deserialized_schema.get_table(table2_id).is_some());
        assert!(deserialized_schema.get_table_by_name("users").is_some());
        assert!(deserialized_schema.get_table_by_name("products").is_some());

        Ok(())
    }

    #[test]
    fn test_schema_serialization_empty() -> Result<()> {
        let schema = Schema::new();

        let mut buffer = Vec::new();
        schema.serialize(&mut buffer)?;

        let deserialized = Schema::deserialize(&buffer)?;
        assert_eq!(deserialized.get_table_count(), 0);
        assert_eq!(deserialized.next_table_id, 1);
        assert_eq!(deserialized.next_column_id, 1);

        Ok(())
    }

    #[test]
    fn test_schema_deserialization_errors() {
        let buffer = vec![]; // Empty buffer
        let result = Schema::deserialize(&buffer);
        assert!(result.is_err());

        let buffer = vec![1, 2, 3]; // Too short for header
        let result = Schema::deserialize(&buffer);
        assert!(result.is_err());
    }

    #[test]
    fn test_set_table_root_page() -> Result<()> {
        let mut schema = Schema::new();

        let columns = create_test_columns();
        let table_id = schema.create_table("users".to_string(), columns)?;

        let new_root_page = crate::storage::PageId::new(42);
        schema.set_table_root_page(table_id, new_root_page)?;

        let table = schema.get_table(table_id).unwrap();
        assert_eq!(table.root_page_id, new_root_page);

        Ok(())
    }

    #[test]
    fn test_set_table_root_page_nonexistent() {
        let mut schema = Schema::new();
        let table_id = TableId::new(999);
        let root_page = crate::storage::PageId::new(42);

        let result = schema.set_table_root_page(table_id, root_page);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_clone() -> Result<()> {
        let mut original = Schema::new();

        let columns = create_test_columns();
        original.create_table("users".to_string(), columns)?;

        let cloned = original.clone();
        assert_eq!(cloned.get_table_count(), original.get_table_count());
        assert_eq!(cloned.next_table_id, original.next_table_id);
        assert_eq!(cloned.next_column_id, original.next_column_id);

        // Verify tables are cloned
        assert!(cloned.get_table_by_name("users").is_some());
        assert_eq!(
            cloned.get_total_column_count(),
            original.get_total_column_count()
        );

        Ok(())
    }

    #[test]
    fn test_schema_debug() {
        let schema = Schema::new();
        let debug_str = format!("{:?}", schema);
        assert!(debug_str.contains("Schema"));
    }
}
