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
