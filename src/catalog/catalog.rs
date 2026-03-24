//! Relational catalog manager.
//!
//! The catalog owns schema-level state and coordinates it with the catalog engine.
//!
//! ```text
//! in-memory schema --------------------+
//!                                      |
//! create/drop/alter style operations   |
//!                                      v
//!                               schema B-tree
//!                                      |
//!                                database header
//! ```
//!
//! Core invariants:
//! - the in-memory schema is authoritative while a catalog operation is running;
//! - `schema_root` always names the durable schema tree recorded in page 0;
//! - schema contents are written before the header is repointed at a new schema root;
//! - transaction rollback restores both the schema snapshot and the engine snapshot.

use crate::catalog::column::Column;
use crate::catalog::engine::{CatalogEngine, CatalogEngineSnapshot, CatalogIntegrityReport};
use crate::catalog::ids::TableId;
use crate::catalog::schema::Schema;
use crate::catalog::table::{SecondaryIndex, Table};
use crate::catalog::JournalMode;
use crate::error::Result;
use std::collections::HashMap;
#[derive(Debug)]
pub struct Catalog {
    engine: CatalogEngine,
    schema: Schema,
    schema_root: u32,
    schema_dirty: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogSnapshot {
    schema: Schema,
    schema_root: u32,
    schema_dirty: bool,
    engine: CatalogEngineSnapshot,
}

impl Catalog {
    /// Open or create a database with SQLite-style schema management
    pub fn open_or_create<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        Self::open_with_engine(CatalogEngine::new(path)?)
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::open_with_engine(CatalogEngine::new_in_memory()?)
    }

    fn open_with_engine(mut engine: CatalogEngine) -> Result<Self> {
        let existing_header = engine.read_database_header()?;

        let header = match existing_header {
            Some(header) => header,
            None => {
                // New database - create header and schema B-tree
                let schema_root = engine.create_tree()?;
                engine.initialize_database_header(schema_root)?
            }
        };

        // Load schema from B-tree
        let schema = engine.load_schema(header.schema_root_page)?;

        Ok(Self {
            engine,
            schema,
            schema_root: header.schema_root_page,
            schema_dirty: false,
        })
    }

    /// Save schema to the B-tree (transactional)
    fn save_schema_to_btree(&mut self) -> Result<()> {
        if !self.schema_dirty {
            return Ok(());
        }

        let current_schema_root = self.engine.save_schema(&self.schema, self.schema_root)?;

        let transaction_active = self.engine.transaction_active()?;
        self.engine.update_database_header(|header| {
            header.schema_root_page = current_schema_root;
        })?;
        if !transaction_active {
            self.engine.flush()?;
        }

        self.schema_root = current_schema_root;
        self.schema_dirty = false;
        Ok(())
    }

    fn get_next_table_id(&mut self) -> Result<TableId> {
        self.engine.allocate_table_id()
    }

    pub fn create_table(&mut self, name: &str, columns: Vec<Column>) -> Result<TableId> {
        if self.schema.get_table_by_name(name).is_some() {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Table '{}' already exists",
                name
            )));
        }

        let table_id = self.get_next_table_id()?;
        let table = Table::new(table_id, name.to_string(), columns, 0u32)?;

        self.schema.insert_table(table.clone())?;
        self.schema_dirty = true;
        self.save_schema_to_btree()?;

        Ok(table_id)
    }

    pub fn create_table_with_roots(
        &mut self,
        name: &str,
        columns: Vec<Column>,
        table_root_page_id: u32,
        primary_key_root_page_id: u32,
    ) -> Result<TableId> {
        if self.schema.get_table_by_name(name).is_some() {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Table '{}' already exists",
                name
            )));
        }

        let table_id = self.get_next_table_id()?;
        let mut table = Table::new(table_id, name.to_string(), columns, table_root_page_id)?;
        table.primary_key_index_root_page_id = primary_key_root_page_id;

        self.schema.insert_table(table)?;
        self.schema_dirty = true;
        self.save_schema_to_btree()?;

        Ok(table_id)
    }

    pub fn get_table(&self, table_id: TableId) -> Result<Option<Table>> {
        Ok(self.schema.get_table(table_id).cloned())
    }

    pub fn get_table_by_name(&self, name: &str) -> Result<Option<Table>> {
        Ok(self.schema.get_table_by_name(name).cloned())
    }

    pub fn drop_table(&mut self, table_id: TableId) -> Result<()> {
        let table = self.schema.get_table(table_id).cloned();
        if table.is_none() {
            return Err(crate::error::HematiteError::StorageError(
                "Table not found".to_string(),
            ));
        }
        self.schema.drop_table(table_id)?;
        self.schema_dirty = true;
        self.save_schema_to_btree()?;

        Ok(())
    }

    pub fn rename_table(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        let table = self.schema.get_table_by_name(old_name).ok_or_else(|| {
            crate::error::HematiteError::StorageError(format!("Table '{}' not found", old_name))
        })?;

        self.schema.rename_table(table.id, new_name.to_string())?;
        self.schema_dirty = true;
        self.save_schema_to_btree()?;
        Ok(())
    }

    pub fn add_column(&mut self, table_id: TableId, column: Column) -> Result<()> {
        self.schema.add_column(table_id, column)?;
        self.schema_dirty = true;
        self.save_schema_to_btree()?;
        Ok(())
    }

    pub fn list_tables(&self) -> Result<Vec<(TableId, String)>> {
        Ok(self.schema.list_tables())
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn clone_schema(&self) -> Schema {
        self.schema.clone()
    }

    pub fn with_engine<F, T>(&mut self, f: F) -> Result<T>
    where
        F: FnOnce(&mut CatalogEngine) -> Result<T>,
    {
        f(&mut self.engine)
    }

    pub(crate) fn with_read_engine<F, T>(&mut self, f: F) -> Result<T>
    where
        F: FnOnce(&mut CatalogEngine) -> Result<T>,
    {
        self.engine.begin_read()?;
        let result = f(&mut self.engine);
        let release = self.engine.end_read();
        match (result, release) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(err), _) => Err(err),
            (Ok(_), Err(err)) => Err(err),
        }
    }

    pub(crate) fn snapshot(&self) -> CatalogSnapshot {
        CatalogSnapshot {
            schema: self.schema.clone(),
            schema_root: self.schema_root,
            schema_dirty: self.schema_dirty,
            engine: self.engine.snapshot(),
        }
    }

    pub(crate) fn restore_snapshot(&mut self, snapshot: CatalogSnapshot) {
        self.schema = snapshot.schema;
        self.schema_root = snapshot.schema_root;
        self.schema_dirty = snapshot.schema_dirty;
        self.engine.restore_snapshot(snapshot.engine);
    }

    pub(crate) fn begin_transaction(&mut self) -> Result<()> {
        self.engine.begin_transaction()
    }

    pub(crate) fn commit_transaction(&mut self) -> Result<()> {
        self.save_schema_to_btree()?;
        self.engine.commit_transaction()
    }

    pub(crate) fn rollback_transaction(&mut self) -> Result<()> {
        self.engine.rollback_transaction()
    }

    pub fn flush_schema(&mut self) -> Result<()> {
        self.save_schema_to_btree()
    }

    pub fn flush(&mut self) -> Result<()> {
        self.save_schema_to_btree()?;
        self.engine.flush()
    }

    pub fn journal_mode(&self) -> Result<JournalMode> {
        self.engine.journal_mode()
    }

    pub fn set_journal_mode(&mut self, journal_mode: JournalMode) -> Result<()> {
        self.save_schema_to_btree()?;
        self.engine.set_journal_mode(journal_mode)
    }

    pub fn checkpoint_wal(&mut self) -> Result<()> {
        self.save_schema_to_btree()?;
        self.engine.checkpoint_wal()
    }

    pub fn replace_schema(&mut self, schema: Schema) -> Result<()> {
        self.schema = schema;
        self.schema_dirty = true;
        self.save_schema_to_btree()?;
        self.engine.set_next_table_id(self.schema.next_table_id())
    }

    pub fn set_table_root_page(&mut self, table_id: TableId, root_page: u32) -> Result<()> {
        if self.schema.get_table(table_id).is_none() {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Table ID {} not found",
                table_id.as_u32()
            )));
        }

        if root_page == 0 {
            return Err(crate::error::HematiteError::StorageError(
                "Root page 0 is reserved for database header".to_string(),
            ));
        }

        self.schema.set_table_root_page(table_id, root_page)?;
        self.schema_dirty = true;
        self.save_schema_to_btree()?;

        Ok(())
    }

    pub fn get_table_root_page(&self, table_id: TableId) -> Result<Option<u32>> {
        if let Some(table) = self.schema.get_table(table_id) {
            if table.root_page_id == 0 {
                Ok(None)
            } else {
                Ok(Some(table.root_page_id))
            }
        } else {
            Ok(None)
        }
    }

    pub fn add_secondary_index(&mut self, table_id: TableId, index: SecondaryIndex) -> Result<()> {
        self.schema.add_secondary_index(table_id, index)?;
        self.schema_dirty = true;
        self.save_schema_to_btree()?;

        Ok(())
    }

    pub fn set_table_primary_key_root_page(
        &mut self,
        table_id: TableId,
        root_page_id: u32,
    ) -> Result<()> {
        if root_page_id == 0 {
            return Err(crate::error::HematiteError::StorageError(
                "Root page 0 is reserved for database header".to_string(),
            ));
        }

        self.schema
            .set_table_primary_key_root_page(table_id, root_page_id)?;
        self.schema_dirty = true;
        self.save_schema_to_btree()?;

        Ok(())
    }

    pub fn set_table_storage_roots(
        &mut self,
        table_id: TableId,
        table_root_page_id: u32,
        primary_key_root_page_id: u32,
    ) -> Result<()> {
        if table_root_page_id == 0 || primary_key_root_page_id == 0 {
            return Err(crate::error::HematiteError::StorageError(
                "Root page 0 is reserved for database header".to_string(),
            ));
        }

        self.schema.set_table_storage_roots(
            table_id,
            table_root_page_id,
            primary_key_root_page_id,
        )?;
        self.schema_dirty = true;
        self.save_schema_to_btree()?;

        Ok(())
    }

    pub fn validate_schema(&self) -> Result<()> {
        let schema_result = self.schema.validate();

        for (table_id, table_name) in self.list_tables()? {
            let table = self.schema.get_table(table_id).ok_or_else(|| {
                crate::error::HematiteError::StorageError(format!(
                    "Table {} found in list but not in schema",
                    table_name
                ))
            })?;

            if table.root_page_id == 0 {
                continue;
            }
        }

        schema_result
    }

    pub fn validate_integrity(&mut self) -> Result<CatalogIntegrityReport> {
        self.validate_schema()?;

        let schema_tables = self
            .schema
            .list_tables()
            .into_iter()
            .filter_map(|(table_id, table_name)| {
                self.schema
                    .get_table(table_id)
                    .map(|table| (table_name, table.root_page_id))
            })
            .collect::<HashMap<_, _>>();

        let storage_tables = self
            .engine
            .get_table_metadata()
            .iter()
            .map(|(name, metadata)| (name.clone(), metadata.root_page_id))
            .collect::<HashMap<_, _>>();

        for (table_name, root_page_id) in &schema_tables {
            let storage_root = storage_tables.get(table_name).ok_or_else(|| {
                crate::error::HematiteError::CorruptedData(format!(
                    "Catalog table '{}' is missing from storage metadata",
                    table_name
                ))
            })?;

            if storage_root != root_page_id {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Catalog/storage root mismatch for table '{}': catalog={}, storage={}",
                    table_name, root_page_id, storage_root
                )));
            }
        }

        for table_name in storage_tables.keys() {
            if !schema_tables.contains_key(table_name) {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Storage metadata contains table '{}' missing from catalog schema",
                    table_name
                )));
            }
        }

        let tables = self
            .schema
            .list_tables()
            .into_iter()
            .filter_map(|(table_id, _)| self.schema.get_table(table_id).cloned())
            .collect::<Vec<_>>();
        let mut report = self.engine.validate_integrity()?;
        let usage = self.engine.validate_catalog_layout(&tables)?;
        report.live_page_count = usage.live_table_pages;
        report.index_page_count = usage.live_index_pages;
        Ok(report)
    }

    pub fn get_total_column_count(&self) -> usize {
        self.schema.get_total_column_count()
    }

    pub fn get_table_stats(&self, table_id: TableId) -> Result<Option<TableStats>> {
        if let Some(table) = self.schema.get_table(table_id) {
            Ok(Some(TableStats {
                id: table.id,
                name: table.name.clone(),
                column_count: table.column_count(),
                primary_key_count: table.primary_key_count(),
                root_page_id: table.root_page_id,
                row_size: table.row_size(),
            }))
        } else {
            Ok(None)
        }
    }

    pub fn get_all_table_stats(&self) -> Result<Vec<TableStats>> {
        let tables = self.list_tables()?;
        let mut stats = Vec::new();

        for (table_id, _name) in tables {
            if let Some(table_stat) = self.get_table_stats(table_id)? {
                stats.push(table_stat);
            }
        }

        Ok(stats)
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.schema.get_table_by_name(name).is_some()
    }

    pub fn table_exists_by_id(&self, table_id: TableId) -> bool {
        self.schema.get_table(table_id).is_some()
    }

    pub fn peek_next_table_id(&self) -> Result<TableId> {
        self.engine.peek_next_table_id()
    }

    pub fn create_table_with_root(
        &mut self,
        name: &str,
        columns: Vec<Column>,
        root_page: u32,
    ) -> Result<TableId> {
        if self.schema.get_table_by_name(name).is_some() {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Table '{}' already exists",
                name
            )));
        }

        let table_id = self.get_next_table_id()?;

        let table = Table::new(table_id, name.to_string(), columns, root_page)?;

        self.schema.insert_table(table.clone())?;
        self.schema_dirty = true;
        self.save_schema_to_btree()?;

        Ok(table_id)
    }

    pub fn get_table_columns(&self, table_id: TableId) -> Result<Option<Vec<Column>>> {
        if let Some(table) = self.schema.get_table(table_id) {
            Ok(Some(table.columns.clone()))
        } else {
            Ok(None)
        }
    }

    pub fn get_table_columns_by_name(&self, name: &str) -> Result<Option<Vec<Column>>> {
        if let Some(table) = self.schema.get_table_by_name(name) {
            Ok(Some(table.columns.clone()))
        } else {
            Ok(None)
        }
    }

    pub fn get_primary_key_columns(&self, table_id: TableId) -> Result<Option<Vec<Column>>> {
        if let Some(table) = self.schema.get_table(table_id) {
            let pk_columns = table
                .primary_key_columns
                .iter()
                .map(|&index| table.columns[index].clone())
                .collect();
            Ok(Some(pk_columns))
        } else {
            Ok(None)
        }
    }
}

/// Statistics for a table
#[derive(Debug, Clone)]
pub struct TableStats {
    pub id: TableId,
    pub name: String,
    pub column_count: usize,
    pub primary_key_count: usize,
    pub root_page_id: u32,
    pub row_size: usize,
}
