//! Schema management for database metadata

use super::column::Column;
use super::ids::TableId;
use super::object::{NamedConstraintKind, Trigger, View};
use super::table::{CheckConstraint, ForeignKeyConstraint, SecondaryIndex, Table};
use crate::error::HematiteError;
use crate::Result;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Schema {
    tables: HashMap<TableId, Table>,
    table_names: HashMap<String, TableId>,
    views: HashMap<String, View>,
    triggers: HashMap<String, Trigger>,
    next_table_id: u32,
    next_column_id: u32,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            table_names: HashMap::new(),
            views: HashMap::new(),
            triggers: HashMap::new(),
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
        let root_page_id = 0u32;

        let table = Table::new(table_id, name.clone(), columns, root_page_id)?;

        self.tables.insert(table_id, table);
        self.table_names.insert(name, table_id);

        Ok(table_id)
    }

    pub fn create_table_with_roots(
        &mut self,
        name: String,
        columns: Vec<Column>,
        table_root_page_id: u32,
        primary_key_root_page_id: u32,
    ) -> Result<TableId> {
        if self.table_names.contains_key(&name) {
            return Err(HematiteError::ParseError(format!(
                "Table '{}' already exists",
                name
            )));
        }

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

        let mut table = Table::new(table_id, name.clone(), columns, table_root_page_id)?;
        table.primary_key_index_root_page_id = primary_key_root_page_id;

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

    pub fn view(&self, name: &str) -> Option<&View> {
        self.views.get(name)
    }

    pub fn trigger(&self, name: &str) -> Option<&Trigger> {
        self.triggers.get(name)
    }

    pub fn create_view(&mut self, view: View) -> Result<()> {
        view.validate()?;
        if self.table_names.contains_key(&view.name) || self.views.contains_key(&view.name) {
            return Err(HematiteError::ParseError(format!(
                "Schema object '{}' already exists",
                view.name
            )));
        }
        self.views.insert(view.name.clone(), view);
        Ok(())
    }

    pub fn drop_view(&mut self, name: &str) -> Result<View> {
        self.views
            .remove(name)
            .ok_or_else(|| HematiteError::StorageError(format!("View '{}' does not exist", name)))
    }

    pub fn create_trigger(&mut self, trigger: Trigger) -> Result<()> {
        trigger.validate()?;
        if self.table_names.contains_key(&trigger.name)
            || self.views.contains_key(&trigger.name)
            || self.triggers.contains_key(&trigger.name)
        {
            return Err(HematiteError::ParseError(format!(
                "Schema object '{}' already exists",
                trigger.name
            )));
        }
        self.triggers.insert(trigger.name.clone(), trigger);
        Ok(())
    }

    pub fn drop_trigger(&mut self, name: &str) -> Result<Trigger> {
        self.triggers.remove(name).ok_or_else(|| {
            HematiteError::StorageError(format!("Trigger '{}' does not exist", name))
        })
    }

    pub(crate) fn tables(&self) -> &HashMap<TableId, Table> {
        &self.tables
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

    pub fn rename_table(&mut self, table_id: TableId, new_name: String) -> Result<()> {
        if self.table_names.contains_key(&new_name) {
            return Err(HematiteError::ParseError(format!(
                "Table '{}' already exists",
                new_name
            )));
        }

        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| HematiteError::StorageError("Table not found".to_string()))?;
        let old_name = std::mem::replace(&mut table.name, new_name.clone());
        self.table_names.remove(&old_name);
        self.table_names.insert(new_name, table_id);
        Ok(())
    }

    pub fn add_secondary_index(&mut self, table_id: TableId, index: SecondaryIndex) -> Result<()> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| HematiteError::StorageError("Table not found".to_string()))?;
        table.add_secondary_index(index)
    }

    pub fn add_column(&mut self, table_id: TableId, column: Column) -> Result<()> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| HematiteError::StorageError("Table not found".to_string()))?;
        self.next_column_id = self
            .next_column_id
            .max(column.id.as_u32().saturating_add(1));
        table.add_column(column)
    }

    pub fn rename_column(
        &mut self,
        table_id: TableId,
        old_name: &str,
        new_name: String,
    ) -> Result<()> {
        let table_name = self
            .tables
            .get(&table_id)
            .ok_or_else(|| HematiteError::StorageError("Table not found".to_string()))?
            .name
            .clone();
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| HematiteError::StorageError("Table not found".to_string()))?;
        table.rename_column(old_name, new_name.clone())?;

        for other_table in self.tables.values_mut() {
            other_table.rewrite_inbound_referenced_column(&table_name, old_name, &new_name);
        }

        Ok(())
    }

    pub fn drop_column(&mut self, table_id: TableId, column_name: &str) -> Result<usize> {
        let table_name = self
            .tables
            .get(&table_id)
            .ok_or_else(|| HematiteError::StorageError("Table not found".to_string()))?
            .name
            .clone();
        if self.tables.values().any(|other_table| {
            other_table.id != table_id
                && other_table.foreign_keys.iter().any(|foreign_key| {
                    foreign_key.referenced_table == table_name
                        && foreign_key
                            .referenced_columns
                            .iter()
                            .any(|referenced_column| referenced_column == column_name)
                })
        }) {
            return Err(HematiteError::StorageError(format!(
                "Column '{}' is referenced by a foreign key",
                column_name
            )));
        }
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| HematiteError::StorageError("Table not found".to_string()))?;
        table.drop_column(column_name)
    }

    pub fn set_column_default(
        &mut self,
        table_id: TableId,
        column_name: &str,
        default_value: Option<super::types::Value>,
    ) -> Result<()> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| HematiteError::StorageError("Table not found".to_string()))?;
        table.set_column_default(column_name, default_value)
    }

    pub fn set_column_nullable(
        &mut self,
        table_id: TableId,
        column_name: &str,
        nullable: bool,
    ) -> Result<()> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| HematiteError::StorageError("Table not found".to_string()))?;
        table.set_column_nullable(column_name, nullable)
    }

    pub fn add_check_constraint(
        &mut self,
        table_id: TableId,
        constraint: CheckConstraint,
    ) -> Result<()> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| HematiteError::StorageError("Table not found".to_string()))?;
        table.add_check_constraint(constraint)
    }

    pub fn add_foreign_key(
        &mut self,
        table_id: TableId,
        constraint: ForeignKeyConstraint,
    ) -> Result<()> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| HematiteError::StorageError("Table not found".to_string()))?;
        table.add_foreign_key(constraint)
    }

    pub fn drop_secondary_index(&mut self, table_id: TableId, index_name: &str) -> Result<()> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| HematiteError::StorageError("Table not found".to_string()))?;
        let _ = table.drop_secondary_index(index_name)?;
        Ok(())
    }

    pub fn drop_named_constraint(
        &mut self,
        table_id: TableId,
        constraint_name: &str,
    ) -> Result<NamedConstraintKind> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| HematiteError::StorageError("Table not found".to_string()))?;
        table.drop_named_constraint(constraint_name)
    }

    pub fn set_table_primary_key_root_page(
        &mut self,
        table_id: TableId,
        root_page_id: u32,
    ) -> Result<()> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| HematiteError::StorageError("Table not found".to_string()))?;
        table.primary_key_index_root_page_id = root_page_id;
        Ok(())
    }

    pub fn set_table_storage_roots(
        &mut self,
        table_id: TableId,
        table_root_page_id: u32,
        primary_key_root_page_id: u32,
    ) -> Result<()> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| HematiteError::StorageError("Table not found".to_string()))?;
        table.root_page_id = table_root_page_id;
        table.primary_key_index_root_page_id = primary_key_root_page_id;
        Ok(())
    }

    pub fn list_tables(&self) -> Vec<(TableId, String)> {
        self.tables
            .iter()
            .map(|(&id, table)| (id, table.name.clone()))
            .collect()
    }

    pub fn list_views(&self) -> Vec<String> {
        self.views.keys().cloned().collect()
    }

    pub fn list_triggers(&self) -> Vec<String> {
        self.triggers.keys().cloned().collect()
    }

    pub fn get_table_count(&self) -> usize {
        self.tables.len()
    }

    pub fn get_total_column_count(&self) -> usize {
        self.tables.values().map(|table| table.column_count()).sum()
    }

    pub fn next_table_id(&self) -> u32 {
        self.next_table_id
    }

    pub fn next_column_id(&self) -> u32 {
        self.next_column_id
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

        buffer.extend_from_slice(&(self.views.len() as u32).to_le_bytes());
        for view in self.views.values() {
            view.serialize(buffer);
        }

        buffer.extend_from_slice(&(self.triggers.len() as u32).to_le_bytes());
        for trigger in self.triggers.values() {
            trigger.serialize(buffer);
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
            views: HashMap::new(),
            triggers: HashMap::new(),
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

        if offset == buffer.len() {
            return Ok(schema);
        }

        if offset + 4 > buffer.len() {
            return Err(HematiteError::CorruptedData(
                "Invalid view count".to_string(),
            ));
        }
        let view_count = u32::from_le_bytes([
            buffer[offset],
            buffer[offset + 1],
            buffer[offset + 2],
            buffer[offset + 3],
        ]) as usize;
        offset += 4;

        for _ in 0..view_count {
            let view = View::deserialize(buffer, &mut offset)?;
            schema.views.insert(view.name.clone(), view);
        }

        if offset == buffer.len() {
            return Ok(schema);
        }

        if offset + 4 > buffer.len() {
            return Err(HematiteError::CorruptedData(
                "Invalid trigger count".to_string(),
            ));
        }
        let trigger_count = u32::from_le_bytes([
            buffer[offset],
            buffer[offset + 1],
            buffer[offset + 2],
            buffer[offset + 3],
        ]) as usize;
        offset += 4;

        for _ in 0..trigger_count {
            let trigger = Trigger::deserialize(buffer, &mut offset)?;
            schema.triggers.insert(trigger.name.clone(), trigger);
        }

        Ok(schema)
    }

    pub fn set_table_root_page(&mut self, table_id: TableId, root_page_id: u32) -> Result<()> {
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
            self.next_column_id = self.next_column_id.max(col.id.as_u32().saturating_add(1));
        }

        self.table_names.insert(table.name.clone(), table.id);
        self.tables.insert(table.id, table);
        Ok(())
    }

    pub fn insert_view(&mut self, view: View) -> Result<()> {
        if self.table_names.contains_key(&view.name) || self.views.contains_key(&view.name) {
            return Err(HematiteError::CorruptedData(format!(
                "Duplicate schema object name '{}' while loading schema",
                view.name
            )));
        }
        self.views.insert(view.name.clone(), view);
        Ok(())
    }

    pub fn insert_trigger(&mut self, trigger: Trigger) -> Result<()> {
        if self.triggers.contains_key(&trigger.name) {
            return Err(HematiteError::CorruptedData(format!(
                "Duplicate trigger name '{}' while loading schema",
                trigger.name
            )));
        }
        self.triggers.insert(trigger.name.clone(), trigger);
        Ok(())
    }
}
