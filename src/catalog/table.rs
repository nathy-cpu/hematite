//! Table definitions for database tables

use super::column::Column;
use super::types::Value;
use super::TableId;
use crate::error::{HematiteError, Result};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Table {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<Column>,
    pub column_indices: HashMap<String, usize>,
    pub primary_key_columns: Vec<usize>,
    pub root_page_id: crate::storage::PageId,
}

impl Table {
    pub fn new(
        id: TableId,
        name: String,
        columns: Vec<Column>,
        root_page_id: crate::storage::PageId,
    ) -> Result<Self> {
        let mut column_indices = HashMap::new();
        let mut primary_key_columns = Vec::new();

        for (index, column) in columns.iter().enumerate() {
            column_indices.insert(column.name.clone(), index);
            if column.primary_key {
                primary_key_columns.push(index);
            }
        }

        // Validate that at least one column exists
        if columns.is_empty() {
            return Err(HematiteError::StorageError(
                "Table must have at least one column".to_string(),
            ));
        }

        // Validate primary key
        if primary_key_columns.is_empty() {
            return Err(HematiteError::StorageError(
                "Table must have at least one primary key column".to_string(),
            ));
        }

        Ok(Self {
            id,
            name,
            columns,
            column_indices,
            primary_key_columns,
            root_page_id,
        })
    }

    pub fn get_column_by_name(&self, name: &str) -> Option<&Column> {
        self.column_indices
            .get(name)
            .map(|&index| &self.columns[index])
    }

    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.column_indices.get(name).copied()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn primary_key_count(&self) -> usize {
        self.primary_key_columns.len()
    }

    pub fn validate_row(&self, values: &[Value]) -> Result<()> {
        if values.len() != self.columns.len() {
            return Err(HematiteError::StorageError(format!(
                "Expected {} values, got {}",
                self.columns.len(),
                values.len()
            )));
        }

        for (column, value) in self.columns.iter().zip(values.iter()) {
            if !column.validate_value(value) {
                return Err(HematiteError::StorageError(format!(
                    "Invalid value for column '{}': {:?}",
                    column.name, value
                )));
            }
        }

        Ok(())
    }

    pub fn get_primary_key_values(&self, values: &[Value]) -> Result<Vec<Value>> {
        self.primary_key_columns
            .iter()
            .map(|&index| {
                if index < values.len() {
                    Ok(values[index].clone())
                } else {
                    Err(HematiteError::StorageError(
                        "Primary key value not found".to_string(),
                    ))
                }
            })
            .collect()
    }

    pub fn row_size(&self) -> usize {
        self.columns.iter().map(|col| col.size()).sum()
    }

    pub fn serialize(&self, buffer: &mut Vec<u8>) -> Result<()> {
        // Table ID (4 bytes)
        buffer.extend_from_slice(&self.id.as_u32().to_le_bytes());

        // Name length (4 bytes) + name
        let name_bytes = self.name.as_bytes();
        buffer.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        buffer.extend_from_slice(name_bytes);

        // Root page ID (4 bytes)
        buffer.extend_from_slice(&self.root_page_id.as_u32().to_le_bytes());

        // Column count (4 bytes)
        buffer.extend_from_slice(&(self.columns.len() as u32).to_le_bytes());

        // Columns
        for column in &self.columns {
            column.serialize(buffer)?;
        }

        // Primary key column count (4 bytes)
        buffer.extend_from_slice(&(self.primary_key_columns.len() as u32).to_le_bytes());

        // Primary key column indices
        for &index in &self.primary_key_columns {
            buffer.extend_from_slice(&(index as u32).to_le_bytes());
        }

        Ok(())
    }

    pub fn deserialize(buffer: &[u8], offset: &mut usize) -> Result<Self> {
        if *offset + 12 > buffer.len() {
            return Err(HematiteError::CorruptedData(
                "Invalid table header".to_string(),
            ));
        }

        // Table ID
        let id = TableId::new(u32::from_le_bytes([
            buffer[*offset],
            buffer[*offset + 1],
            buffer[*offset + 2],
            buffer[*offset + 3],
        ]));
        *offset += 4;

        // Name
        let name_len = u32::from_le_bytes([
            buffer[*offset],
            buffer[*offset + 1],
            buffer[*offset + 2],
            buffer[*offset + 3],
        ]) as usize;
        *offset += 4;

        if *offset + name_len > buffer.len() {
            return Err(HematiteError::CorruptedData(
                "Invalid table name".to_string(),
            ));
        }
        let name = String::from_utf8(buffer[*offset..*offset + name_len].to_vec())
            .map_err(|_| HematiteError::CorruptedData("Invalid UTF-8 in table name".to_string()))?;
        *offset += name_len;

        // Root page ID
        let root_page_id = crate::storage::PageId::new(u32::from_le_bytes([
            buffer[*offset],
            buffer[*offset + 1],
            buffer[*offset + 2],
            buffer[*offset + 3],
        ]));
        *offset += 4;

        // Column count
        let column_count = u32::from_le_bytes([
            buffer[*offset],
            buffer[*offset + 1],
            buffer[*offset + 2],
            buffer[*offset + 3],
        ]) as usize;
        *offset += 4;

        // Columns
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            columns.push(Column::deserialize(buffer, offset)?);
        }

        // Primary key column count
        if *offset + 4 > buffer.len() {
            return Err(HematiteError::CorruptedData(
                "Invalid primary key count".to_string(),
            ));
        }
        let pk_count = u32::from_le_bytes([
            buffer[*offset],
            buffer[*offset + 1],
            buffer[*offset + 2],
            buffer[*offset + 3],
        ]) as usize;
        *offset += 4;

        // Primary key column indices
        let mut primary_key_columns = Vec::with_capacity(pk_count);
        for _ in 0..pk_count {
            if *offset + 4 > buffer.len() {
                return Err(HematiteError::CorruptedData(
                    "Invalid primary key index".to_string(),
                ));
            }
            let index = u32::from_le_bytes([
                buffer[*offset],
                buffer[*offset + 1],
                buffer[*offset + 2],
                buffer[*offset + 3],
            ]) as usize;
            *offset += 4;
            primary_key_columns.push(index);
        }

        Self::new(id, name, columns, root_page_id)
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::types::{DataType, Value};
    use crate::catalog::TableId;
    use crate::catalog::{Column, ColumnId};
    use crate::HematiteError;
    use crate::Table;

    fn create_test_columns() -> Vec<Column> {
        vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(2), "name".to_string(), DataType::Text),
            Column::new(ColumnId::new(3), "active".to_string(), DataType::Boolean),
        ]
    }

    #[test]
    fn test_table_creation() -> Result<(), HematiteError> {
        let columns = create_test_columns();
        let table = Table::new(
            TableId::new(1),
            "users".to_string(),
            columns,
            crate::storage::PageId::new(42),
        )?;

        assert_eq!(table.id.as_u32(), 1);
        assert_eq!(table.name, "users");
        assert_eq!(table.column_count(), 3);
        assert_eq!(table.primary_key_count(), 1);
        assert_eq!(table.root_page_id.as_u32(), 42);

        Ok(())
    }

    #[test]
    fn test_table_validation_no_columns() {
        let result = Table::new(
            TableId::new(1),
            "empty".to_string(),
            vec![],
            crate::storage::PageId::new(1),
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("at least one column"));
    }

    #[test]
    fn test_table_validation_no_primary_key() {
        let columns = vec![
            Column::new(ColumnId::new(1), "name".to_string(), DataType::Text),
            Column::new(ColumnId::new(2), "age".to_string(), DataType::Integer),
        ];

        let result = Table::new(
            TableId::new(1),
            "no_pk".to_string(),
            columns,
            crate::storage::PageId::new(1),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("primary key"));
    }

    #[test]
    fn test_table_get_column_by_name() -> Result<(), HematiteError> {
        let columns = create_test_columns();
        let table = Table::new(
            TableId::new(1),
            "users".to_string(),
            columns,
            crate::storage::PageId::new(42),
        )?;

        let id_column = table.get_column_by_name("id");
        assert!(id_column.is_some());
        assert_eq!(id_column.unwrap().name, "id");

        let name_column = table.get_column_by_name("name");
        assert!(name_column.is_some());
        assert_eq!(name_column.unwrap().data_type, DataType::Text);

        let nonexistent = table.get_column_by_name("nonexistent");
        assert!(nonexistent.is_none());

        Ok(())
    }

    #[test]
    fn test_table_get_column_index() -> Result<(), HematiteError> {
        let columns = create_test_columns();
        let table = Table::new(
            TableId::new(1),
            "users".to_string(),
            columns,
            crate::storage::PageId::new(42),
        )?;

        assert_eq!(table.get_column_index("id"), Some(0));
        assert_eq!(table.get_column_index("name"), Some(1));
        assert_eq!(table.get_column_index("active"), Some(2));
        assert_eq!(table.get_column_index("nonexistent"), None);

        Ok(())
    }

    #[test]
    fn test_table_validate_row() -> Result<(), HematiteError> {
        let columns = create_test_columns();
        let table = Table::new(
            TableId::new(1),
            "users".to_string(),
            columns,
            crate::storage::PageId::new(42),
        )?;

        // Valid row
        let valid_row = vec![
            Value::Integer(1),
            Value::Text("Alice".to_string()),
            Value::Boolean(true),
        ];
        assert!(table.validate_row(&valid_row).is_ok());

        // Invalid row length
        let short_row = vec![Value::Integer(1), Value::Text("Alice".to_string())];
        assert!(table.validate_row(&short_row).is_err());

        let long_row = vec![
            Value::Integer(1),
            Value::Text("Alice".to_string()),
            Value::Boolean(true),
            Value::Float(3.14),
        ];
        assert!(table.validate_row(&long_row).is_err());

        // Invalid value types
        let invalid_types = vec![
            Value::Text("not an integer".to_string()),
            Value::Text("Alice".to_string()),
            Value::Boolean(true),
        ];
        assert!(table.validate_row(&invalid_types).is_err());

        Ok(())
    }

    #[test]
    fn test_table_get_primary_key_values() -> Result<(), HematiteError> {
        let columns = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(2), "name".to_string(), DataType::Text),
            Column::new(
                ColumnId::new(3),
                "created_at".to_string(),
                DataType::Integer,
            )
            .primary_key(true),
        ];
        let table = Table::new(
            TableId::new(1),
            "logs".to_string(),
            columns,
            crate::storage::PageId::new(42),
        )?;

        let row = vec![
            Value::Integer(123),
            Value::Text("log entry".to_string()),
            Value::Integer(456),
        ];

        let pk_values = table.get_primary_key_values(&row)?;
        assert_eq!(pk_values.len(), 2);
        assert_eq!(pk_values[0], Value::Integer(123));
        assert_eq!(pk_values[1], Value::Integer(456));

        Ok(())
    }

    #[test]
    fn test_table_get_primary_key_values_invalid() -> Result<(), HematiteError> {
        let columns = create_test_columns();
        let table = Table::new(
            TableId::new(1),
            "users".to_string(),
            columns,
            crate::storage::PageId::new(42),
        )?;

        // Row too short for primary key extraction
        let short_row = vec![];
        let result = table.get_primary_key_values(&short_row);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_table_row_size() -> Result<(), HematiteError> {
        let columns = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(2), "name".to_string(), DataType::Text),
            Column::new(ColumnId::new(3), "active".to_string(), DataType::Boolean),
            Column::new(ColumnId::new(4), "price".to_string(), DataType::Float),
        ];
        let table = Table::new(
            TableId::new(1),
            "products".to_string(),
            columns,
            crate::storage::PageId::new(42),
        )?;

        // Integer (4) + Text (255) + Boolean (1) + Float (8) = 268
        assert_eq!(table.row_size(), 268);

        Ok(())
    }

    #[test]
    fn test_table_serialization_roundtrip() -> Result<(), HematiteError> {
        let columns = create_test_columns();
        let original = Table::new(
            TableId::new(42),
            "test_table".to_string(),
            columns,
            crate::storage::PageId::new(123),
        )?;

        let mut buffer = Vec::new();
        original.serialize(&mut buffer)?;

        let mut offset = 0;
        let deserialized = Table::deserialize(&buffer, &mut offset)?;

        assert_eq!(original.id, deserialized.id);
        assert_eq!(original.name, deserialized.name);
        assert_eq!(original.root_page_id, deserialized.root_page_id);
        assert_eq!(original.column_count(), deserialized.column_count());
        assert_eq!(
            original.primary_key_count(),
            deserialized.primary_key_count()
        );

        // Check columns
        assert_eq!(deserialized.column_count(), 3);
        assert!(deserialized.get_column_by_name("id").is_some());
        assert!(deserialized.get_column_by_name("name").is_some());
        assert!(deserialized.get_column_by_name("active").is_some());

        // Check primary key columns
        assert_eq!(deserialized.primary_key_columns.len(), 1);
        assert_eq!(deserialized.primary_key_columns[0], 0); // First column is primary key

        Ok(())
    }

    #[test]
    fn test_table_serialization_multiple_primary_keys() -> Result<(), HematiteError> {
        let columns = vec![
            Column::new(ColumnId::new(1), "user_id".to_string(), DataType::Integer)
                .primary_key(true),
            Column::new(ColumnId::new(2), "post_id".to_string(), DataType::Integer)
                .primary_key(true),
            Column::new(ColumnId::new(3), "content".to_string(), DataType::Text),
        ];
        let original = Table::new(
            TableId::new(1),
            "user_posts".to_string(),
            columns,
            crate::storage::PageId::new(42),
        )?;

        let mut buffer = Vec::new();
        original.serialize(&mut buffer)?;

        let mut offset = 0;
        let deserialized = Table::deserialize(&buffer, &mut offset)?;

        assert_eq!(deserialized.primary_key_columns.len(), 2);
        assert_eq!(deserialized.primary_key_columns[0], 0); // First column
        assert_eq!(deserialized.primary_key_columns[1], 1); // Second column

        Ok(())
    }

    #[test]
    fn test_table_deserialization_errors() {
        let buffer = vec![]; // Empty buffer
        let mut offset = 0;
        assert!(Table::deserialize(&buffer, &mut offset).is_err());

        let buffer = vec![1, 2, 3]; // Too short for table header
        let mut offset = 0;
        assert!(Table::deserialize(&buffer, &mut offset).is_err());
    }

    #[test]
    fn test_table_clone() -> Result<(), HematiteError> {
        let columns = create_test_columns();
        let original = Table::new(
            TableId::new(1),
            "users".to_string(),
            columns,
            crate::storage::PageId::new(42),
        )?;

        let cloned = original.clone();
        assert_eq!(original.id, cloned.id);
        assert_eq!(original.name, cloned.name);
        assert_eq!(original.column_count(), cloned.column_count());
        assert_eq!(original.primary_key_count(), cloned.primary_key_count());

        // Verify column indices are preserved
        assert_eq!(
            original.get_column_index("id"),
            cloned.get_column_index("id")
        );
        assert_eq!(
            original.get_column_index("name"),
            cloned.get_column_index("name")
        );

        Ok(())
    }

    #[test]
    fn test_table_debug() -> Result<(), HematiteError> {
        let columns = create_test_columns();
        let table = Table::new(
            TableId::new(1),
            "users".to_string(),
            columns,
            crate::storage::PageId::new(42),
        )?;

        let debug_str = format!("{:?}", table);
        assert!(debug_str.contains("Table"));
        assert!(debug_str.contains("users"));

        Ok(())
    }
}
