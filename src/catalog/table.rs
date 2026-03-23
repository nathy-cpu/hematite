//! Table definitions for database tables

use super::column::Column;
use super::ids::TableId;
use super::types::Value;
use crate::HematiteError;
use crate::Result;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Table {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<Column>,
    pub column_indices: HashMap<String, usize>,
    pub primary_key_columns: Vec<usize>,
    pub secondary_indexes: Vec<SecondaryIndex>,
    pub root_page_id: u32,
    pub primary_key_index_root_page_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecondaryIndex {
    pub name: String,
    pub column_indices: Vec<usize>,
    pub root_page_id: u32,
}

impl Table {
    pub fn new(
        id: TableId,
        name: String,
        mut columns: Vec<Column>,
        root_page_id: u32,
    ) -> Result<Self> {
        let mut column_indices = HashMap::new();
        let mut primary_key_columns = Vec::new();

        for column in &mut columns {
            if column.primary_key {
                column.nullable = false;
            }
        }

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
            secondary_indexes: Vec::new(),
            root_page_id,
            primary_key_index_root_page_id: 0,
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

    pub fn get_secondary_index(&self, name: &str) -> Option<&SecondaryIndex> {
        self.secondary_indexes
            .iter()
            .find(|index| index.name == name)
    }

    pub fn add_secondary_index(&mut self, index: SecondaryIndex) -> Result<()> {
        if self.get_secondary_index(&index.name).is_some() {
            return Err(HematiteError::StorageError(format!(
                "Secondary index '{}' already exists on table '{}'",
                index.name, self.name
            )));
        }

        if index.column_indices.is_empty() {
            return Err(HematiteError::StorageError(
                "Secondary index must reference at least one column".to_string(),
            ));
        }

        for &column_index in &index.column_indices {
            if column_index >= self.columns.len() {
                return Err(HematiteError::StorageError(format!(
                    "Secondary index '{}' references invalid column index {}",
                    index.name, column_index
                )));
            }
        }

        self.secondary_indexes.push(index);
        Ok(())
    }

    pub fn drop_secondary_index(&mut self, name: &str) -> Result<SecondaryIndex> {
        let index = self
            .secondary_indexes
            .iter()
            .position(|index| index.name == name)
            .ok_or_else(|| {
                HematiteError::StorageError(format!(
                    "Secondary index '{}' does not exist on table '{}'",
                    name, self.name
                ))
            })?;

        Ok(self.secondary_indexes.remove(index))
    }

    pub fn serialize(&self, buffer: &mut Vec<u8>) -> Result<()> {
        // Table ID (4 bytes)
        buffer.extend_from_slice(&self.id.as_u32().to_le_bytes());

        // Name length (4 bytes) + name
        let name_bytes = self.name.as_bytes();
        buffer.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        buffer.extend_from_slice(name_bytes);

        // Root page ID (4 bytes)
        buffer.extend_from_slice(&self.root_page_id.to_le_bytes());
        buffer.extend_from_slice(&self.primary_key_index_root_page_id.to_le_bytes());

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

        // Secondary indexes
        buffer.extend_from_slice(&(self.secondary_indexes.len() as u32).to_le_bytes());
        for index in &self.secondary_indexes {
            let name_bytes = index.name.as_bytes();
            buffer.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buffer.extend_from_slice(name_bytes);
            buffer.extend_from_slice(&index.root_page_id.to_le_bytes());
            buffer.extend_from_slice(&(index.column_indices.len() as u32).to_le_bytes());
            for &column_index in &index.column_indices {
                buffer.extend_from_slice(&(column_index as u32).to_le_bytes());
            }
        }

        Ok(())
    }

    pub fn deserialize(buffer: &[u8], offset: &mut usize) -> Result<Self> {
        if *offset + 16 > buffer.len() {
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
        let root_page_id = u32::from_le_bytes([
            buffer[*offset],
            buffer[*offset + 1],
            buffer[*offset + 2],
            buffer[*offset + 3],
        ]);
        *offset += 4;

        let primary_key_index_root_page_id = u32::from_le_bytes([
            buffer[*offset],
            buffer[*offset + 1],
            buffer[*offset + 2],
            buffer[*offset + 3],
        ]);
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

        let mut table = Self::new(id, name, columns, root_page_id)?;
        table.primary_key_index_root_page_id = primary_key_index_root_page_id;

        if *offset == buffer.len() {
            return Ok(table);
        }

        if *offset + 4 > buffer.len() {
            return Err(HematiteError::CorruptedData(
                "Invalid secondary index count".to_string(),
            ));
        }
        let secondary_index_count = u32::from_le_bytes([
            buffer[*offset],
            buffer[*offset + 1],
            buffer[*offset + 2],
            buffer[*offset + 3],
        ]) as usize;
        *offset += 4;

        for _ in 0..secondary_index_count {
            if *offset + 4 > buffer.len() {
                return Err(HematiteError::CorruptedData(
                    "Invalid secondary index name length".to_string(),
                ));
            }
            let name_len = u32::from_le_bytes([
                buffer[*offset],
                buffer[*offset + 1],
                buffer[*offset + 2],
                buffer[*offset + 3],
            ]) as usize;
            *offset += 4;

            if *offset + name_len > buffer.len() {
                return Err(HematiteError::CorruptedData(
                    "Invalid secondary index name".to_string(),
                ));
            }
            let name =
                String::from_utf8(buffer[*offset..*offset + name_len].to_vec()).map_err(|_| {
                    HematiteError::CorruptedData(
                        "Invalid UTF-8 in secondary index name".to_string(),
                    )
                })?;
            *offset += name_len;

            if *offset + 8 > buffer.len() {
                return Err(HematiteError::CorruptedData(
                    "Invalid secondary index metadata".to_string(),
                ));
            }
            let index_root_page_id = u32::from_le_bytes([
                buffer[*offset],
                buffer[*offset + 1],
                buffer[*offset + 2],
                buffer[*offset + 3],
            ]);
            *offset += 4;

            let column_count = u32::from_le_bytes([
                buffer[*offset],
                buffer[*offset + 1],
                buffer[*offset + 2],
                buffer[*offset + 3],
            ]) as usize;
            *offset += 4;

            let mut column_indices = Vec::with_capacity(column_count);
            for _ in 0..column_count {
                if *offset + 4 > buffer.len() {
                    return Err(HematiteError::CorruptedData(
                        "Invalid secondary index column index".to_string(),
                    ));
                }
                let column_index = u32::from_le_bytes([
                    buffer[*offset],
                    buffer[*offset + 1],
                    buffer[*offset + 2],
                    buffer[*offset + 3],
                ]) as usize;
                *offset += 4;
                column_indices.push(column_index);
            }

            table.add_secondary_index(SecondaryIndex {
                name,
                column_indices,
                root_page_id: index_root_page_id,
            })?;
        }

        Ok(table)
    }

    /// Convert table to bytes for storage in schema B-tree
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        self.serialize(&mut buffer)?;
        Ok(buffer)
    }

    /// Create table from bytes stored in schema B-tree
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut offset = 0;
        Self::deserialize(bytes, &mut offset)
    }
}
