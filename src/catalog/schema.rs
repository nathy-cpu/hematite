//! Schema management for database metadata

use super::column::Column;
use super::ids::TableId;
use super::table::Table;
use crate::error::HematiteError;
use crate::Result;
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

    /// Insert a fully-defined table (used when loading persisted schema).
    pub fn insert_table(&mut self, table: Table) -> Result<()> {
        // Check for duplicate table name
        if self.table_names.contains_key(&table.name) {
            return Err(HematiteError::CorruptedData(format!(
                "Duplicate table name '{}' while loading schema",
                table.name
            )));
        }
        // Check for duplicate table id
        if self.tables.contains_key(&table.id) {
            return Err(HematiteError::CorruptedData(format!(
                "Duplicate table id {} while loading schema",
                table.id.as_u32()
            )));
        }

        // Advance ID generators to avoid collisions on subsequent creates.
        self.next_table_id = self.next_table_id.max(table.id.as_u32().saturating_add(1));
        for col in &table.columns {
            self.next_column_id = self
                .next_column_id
                .max(col.id.as_u32().saturating_add(1));
        }

        self.table_names.insert(table.name.clone(), table.id);
        self.tables.insert(table.id, table);
        Ok(())
    }
}
