//! # Relational Catalog Manager (`catalog`)
//!
//! The Catalog is the metadata manager of the database. It translates relational schema concepts
//! (tables, columns, types, indexes, constraints, views, and triggers) into physical B-Tree structures,
//! managing metadata serialization and coordinating transactional schema modifications.
//!
//! ---
//!
//! ## 1. Core Database Catalog Concepts
//!
//! ### The Relational Catalog
//! A database engine needs to know *what* tables and columns exist, their data types, and what constraints
//! (e.g. check constraints, foreign keys, secondary indexes) apply to them. The Relational Catalog stores
//! this structural information. In mature database engines, the catalog is stored in "system tables" (special
//! internal tables that are queried just like normal user tables).
//!
//! ### Schema Serialization
//! In Hematite, all catalog definitions are serialized into bytes and stored within the **Schema B-Tree**
//! rooted at the Page ID specified in the database header (on Page 0). When the database is opened, the catalog
//! coordinator traverses this B-Tree, reads the serialized metadata rows, and reconstructs the active in-memory
//! schema representation (`Schema` registry).
//!
//! ### Transactional DDL (Data Definition Language)
//! Altering a database structure (creating/dropping tables, altering columns, adding indexes) must be transactional:
//! 1. **Begin**: When a transaction starts, the catalog captures a **Snapshot** of the in-memory schema state.
//! 2. **Mutate**: Structural changes update both the in-memory registry and write new serialized rows to the
//!    durable schema B-Tree.
//! 3. **Commit**: The transaction finishes, making the new schema pointer in the database header durable.
//! 4. **Rollback**: If the transaction aborts, the catalog discards the in-flight schema mutations and reinstates
//!    the saved snapshot, restoring both in-memory schema structures and the underlying storage engines to the
//!    pre-transaction baseline.
//!
//! ---
//!
//! ## 2. Catalog Structural Interactions
//!
//! ```text
//!    +------------------------------------------+
//!    |             In-Memory Schema             | <---+ Reader threads fetch schema definitions
//!    |         (Authoritative at Runtime)       |
//!    +--------------------+---------------------+
//!                         |
//!                 CREATE / DROP / ALTER
//!                         |
//!                         v
//!    +--------------------+---------------------+
//!    |           Relational Catalog             |
//!    |    * Serializes metadata into row bytes. |
//!    |    * Performs constraint validations.    |
//!    +--------------------+---------------------+
//!                         |
//!                Write Catalog Records
//!                         |
//!                         v
//!    +--------------------+---------------------+
//!    |              Schema B-Tree               | (Root page tracked on Page 0 Header)
//!    |        (Physical Catalog Tables)         |
//!    +------------------------------------------+
//! ```
//!
//! ---
//!
//! ## 3. Transactional Schema Rollback Flow
//!
//! ```text
//!   Idle State (Active Schema V1)
//!         |
//!    BEGIN TRANSACTION
//!         v
//!   Capture Schema Snapshot V1 (CatalogEngineSnapshot)
//!         |
//!    CREATE TABLE users (id INT, name TEXT)
//!         v
//!   Active Schema modified in-memory (Schema V2)
//!   Metadata rows written to Schema B-Tree
//!         |
//!   +-----+-----+
//!   |           |
//!   v           v
//! COMMIT     ROLLBACK
//!   |           |
//!   |           +---> Discard Schema V2 in-memory.
//!   |                 Restore Snapshot V1.
//!   |                 Revert CatalogEngine changes.
//!   v                 (Durable rollback journal/WAL cleans physical changes)
//! Commit V2           |
//! durably.            v
//!             Back to Schema V1
//! ```
//!
//! ---
//!
//! ## 4. Core Catalog Invariants
//!
//! 1. **Authoritative Snapshot**: The in-memory schema registry must be kept in perfect synchronization with the
//!    durable catalog B-Tree cells during query planning and validation.
//! 2. **Durable Catalog Repointing**: When the schema B-Tree changes, its new root Page ID must be serialized
//!    and updated in the Page 0 database header *after* all modified catalog pages are written, preventing pointers
//!    from referencing unwritten pages.
//! 3. **Isolation Integrity**: During transaction rollback, catalog state reverts must synchronize both in-memory
//!    snapshots and storage page states to avoid inconsistent catalog pointer states.
//!

use crate::catalog::column::Column;
use crate::catalog::engine::{CatalogEngine, CatalogEngineSnapshot, CatalogIntegrityReport};
use crate::catalog::ids::TableId;
use crate::catalog::object::{NamedConstraintKind, Trigger, View};
use crate::catalog::schema::Schema;
use crate::catalog::table::{CheckConstraint, ForeignKeyConstraint, SecondaryIndex, Table};
use crate::catalog::JournalMode;
use crate::error::Result;
use std::collections::HashMap;
#[derive(Debug)]
pub struct Catalog {
    pub(crate) engine: CatalogEngine,
    pub(crate) schema: Schema,
    pub(crate) schema_root: u32,
    pub(crate) schema_dirty: bool,
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

    fn restore_state(&mut self, schema: Schema, schema_root: u32, schema_dirty: bool) {
        self.schema = schema;
        self.schema_root = schema_root;
        self.schema_dirty = schema_dirty;
    }

    fn rollback_failed_atomic_operation(
        &mut self,
        schema: Schema,
        schema_root: u32,
        schema_dirty: bool,
    ) -> Result<()> {
        self.restore_state(schema, schema_root, schema_dirty);
        self.engine.rollback_transaction()
    }

    fn run_atomically<T>(&mut self, operation: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        let schema = self.schema.clone();
        let schema_root = self.schema_root;
        let schema_dirty = self.schema_dirty;
        let transaction_active = self.engine.transaction_active()?;

        if transaction_active {
            let engine_snapshot = self.engine.snapshot()?;
            match operation(self) {
                Ok(result) => Ok(result),
                Err(err) => {
                    self.restore_state(schema, schema_root, schema_dirty);
                    self.engine.restore_snapshot(engine_snapshot)?;
                    Err(err)
                }
            }
        } else {
            self.engine.begin_transaction()?;
            match operation(self) {
                Ok(result) => match self.commit_transaction() {
                    Ok(()) => Ok(result),
                    Err(err) => {
                        self.rollback_failed_atomic_operation(schema, schema_root, schema_dirty)?;
                        Err(err)
                    }
                },
                Err(err) => {
                    self.rollback_failed_atomic_operation(schema, schema_root, schema_dirty)?;
                    Err(err)
                }
            }
        }
    }

    fn run_schema_mutation<T>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        self.run_atomically(|catalog| {
            let result = operation(catalog)?;
            catalog.schema_dirty = true;
            catalog.save_schema_to_btree()?;
            Ok(result)
        })
    }

    fn get_next_table_id(&mut self) -> Result<TableId> {
        self.engine.allocate_table_id()
    }

    pub fn create_table(&mut self, name: &str, columns: Vec<Column>) -> Result<TableId> {
        self.run_schema_mutation(|catalog| {
            if catalog.schema.get_table_by_name(name).is_some() {
                return Err(crate::error::HematiteError::StorageError(format!(
                    "Table '{}' already exists",
                    name
                )));
            }

            let table_id = catalog.get_next_table_id()?;
            let table = Table::new(table_id, name.to_string(), columns, 0u32)?;

            catalog.schema.insert_table(table)?;
            Ok(table_id)
        })
    }

    pub fn create_table_with_roots(
        &mut self,
        name: &str,
        columns: Vec<Column>,
        table_root_page_id: u32,
        primary_key_root_page_id: u32,
    ) -> Result<TableId> {
        self.run_schema_mutation(|catalog| {
            if catalog.schema.get_table_by_name(name).is_some() {
                return Err(crate::error::HematiteError::StorageError(format!(
                    "Table '{}' already exists",
                    name
                )));
            }

            let table_id = catalog.get_next_table_id()?;
            let mut table = Table::new(table_id, name.to_string(), columns, table_root_page_id)?;
            table.primary_key_index_root_page_id = primary_key_root_page_id;

            catalog.schema.insert_table(table)?;
            Ok(table_id)
        })
    }

    pub fn get_table(&self, table_id: TableId) -> Result<Option<Table>> {
        Ok(self.schema.get_table(table_id).cloned())
    }

    pub fn get_table_by_name(&self, name: &str) -> Result<Option<Table>> {
        Ok(self.schema.get_table_by_name(name).cloned())
    }

    pub fn drop_table(&mut self, table_id: TableId) -> Result<()> {
        self.run_schema_mutation(|catalog| {
            let table = catalog.schema.get_table(table_id).cloned().ok_or_else(|| {
                crate::error::HematiteError::StorageError("Table not found".to_string())
            })?;
            if let Some(view_name) = catalog.first_view_dependency_on(&table.name, None) {
                return Err(crate::error::HematiteError::ParseError(format!(
                    "Cannot drop table '{}' because view '{}' depends on it",
                    table.name, view_name
                )));
            }
            catalog.schema.drop_table(table_id)?;
            Ok(())
        })
    }

    pub fn rename_table(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        self.run_schema_mutation(|catalog| {
            let table = catalog.schema.get_table_by_name(old_name).ok_or_else(|| {
                crate::error::HematiteError::StorageError(format!("Table '{}' not found", old_name))
            })?;

            catalog.schema.rename_table(table.id, new_name.to_string())
        })
    }

    pub fn add_column(&mut self, table_id: TableId, column: Column) -> Result<()> {
        self.run_schema_mutation(|catalog| catalog.schema.add_column(table_id, column))
    }

    pub fn rename_column(
        &mut self,
        table_id: TableId,
        old_name: &str,
        new_name: String,
    ) -> Result<()> {
        self.run_schema_mutation(|catalog| {
            catalog.schema.rename_column(table_id, old_name, new_name)
        })
    }

    pub fn drop_column(&mut self, table_id: TableId, column_name: &str) -> Result<usize> {
        self.run_schema_mutation(|catalog| catalog.schema.drop_column(table_id, column_name))
    }

    pub fn set_column_default(
        &mut self,
        table_id: TableId,
        column_name: &str,
        default_value: Option<crate::catalog::Value>,
    ) -> Result<()> {
        self.run_schema_mutation(|catalog| {
            catalog
                .schema
                .set_column_default(table_id, column_name, default_value)
        })
    }

    pub fn set_column_nullable(
        &mut self,
        table_id: TableId,
        column_name: &str,
        nullable: bool,
    ) -> Result<()> {
        self.run_schema_mutation(|catalog| {
            catalog
                .schema
                .set_column_nullable(table_id, column_name, nullable)
        })
    }

    pub fn add_check_constraint(
        &mut self,
        table_id: TableId,
        constraint: CheckConstraint,
    ) -> Result<()> {
        self.run_schema_mutation(|catalog| {
            catalog.schema.add_check_constraint(table_id, constraint)
        })
    }

    pub fn add_foreign_key(
        &mut self,
        table_id: TableId,
        constraint: ForeignKeyConstraint,
    ) -> Result<()> {
        self.run_schema_mutation(|catalog| catalog.schema.add_foreign_key(table_id, constraint))
    }

    pub fn list_tables(&self) -> Result<Vec<(TableId, String)>> {
        Ok(self.schema.list_tables())
    }

    pub fn create_view(&mut self, view: View) -> Result<()> {
        self.run_schema_mutation(|catalog| {
            if view
                .dependencies
                .iter()
                .any(|dependency| dependency.eq_ignore_ascii_case(&view.name))
            {
                return Err(crate::error::HematiteError::ParseError(format!(
                    "View '{}' cannot depend on itself",
                    view.name
                )));
            }
            for dependency in &view.dependencies {
                if catalog.view_depends_on(dependency, &view.name) {
                    return Err(crate::error::HematiteError::ParseError(format!(
                        "Creating view '{}' would introduce a recursive view cycle through '{}'",
                        view.name, dependency
                    )));
                }
            }
            catalog.schema.create_view(view)
        })
    }

    pub fn drop_view(&mut self, name: &str) -> Result<View> {
        self.run_schema_mutation(|catalog| {
            if let Some(view_name) = catalog.first_view_dependency_on(name, Some(name)) {
                return Err(crate::error::HematiteError::ParseError(format!(
                    "Cannot drop view '{}' because view '{}' depends on it",
                    name, view_name
                )));
            }
            catalog.schema.drop_view(name)
        })
    }

    pub fn get_view(&self, name: &str) -> Result<Option<View>> {
        Ok(self.schema.view(name).cloned())
    }

    pub fn list_views(&self) -> Result<Vec<String>> {
        Ok(self.schema.list_views())
    }

    pub fn create_trigger(&mut self, trigger: Trigger) -> Result<()> {
        self.run_schema_mutation(|catalog| catalog.schema.create_trigger(trigger))
    }

    pub fn drop_trigger(&mut self, name: &str) -> Result<Trigger> {
        self.run_schema_mutation(|catalog| catalog.schema.drop_trigger(name))
    }

    pub fn get_trigger(&self, name: &str) -> Result<Option<Trigger>> {
        Ok(self.schema.trigger(name).cloned())
    }

    pub fn list_triggers(&self) -> Result<Vec<String>> {
        Ok(self.schema.list_triggers())
    }

    pub fn drop_named_constraint(
        &mut self,
        table_id: TableId,
        constraint_name: &str,
    ) -> Result<NamedConstraintKind> {
        self.run_schema_mutation(|catalog| {
            catalog
                .schema
                .drop_named_constraint(table_id, constraint_name)
        })
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    fn first_view_dependency_on(
        &self,
        object_name: &str,
        skip_view: Option<&str>,
    ) -> Option<String> {
        self.schema
            .list_views()
            .into_iter()
            .filter(|view_name| !skip_view.is_some_and(|skip| view_name.eq_ignore_ascii_case(skip)))
            .find(|view_name| {
                self.schema.view(view_name).is_some_and(|view| {
                    view.dependencies
                        .iter()
                        .any(|dependency| dependency.eq_ignore_ascii_case(object_name))
                })
            })
    }

    fn view_depends_on(&self, view_name: &str, target_name: &str) -> bool {
        let Some(view) = self.schema.view(view_name) else {
            return false;
        };
        view.dependencies.iter().any(|dependency| {
            dependency.eq_ignore_ascii_case(target_name)
                || self.view_depends_on(dependency, target_name)
        })
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

    pub(crate) fn snapshot(&self) -> Result<CatalogSnapshot> {
        Ok(CatalogSnapshot {
            schema: self.schema.clone(),
            schema_root: self.schema_root,
            schema_dirty: self.schema_dirty,
            engine: self.engine.snapshot()?,
        })
    }

    pub(crate) fn transaction_entry_snapshot(&self) -> Result<CatalogSnapshot> {
        Ok(CatalogSnapshot {
            schema: self.schema.clone(),
            schema_root: self.schema_root,
            schema_dirty: self.schema_dirty,
            engine: self.engine.snapshot()?.into_transaction_baseline(),
        })
    }

    pub(crate) fn restore_snapshot(&mut self, snapshot: CatalogSnapshot) -> Result<()> {
        self.schema = snapshot.schema;
        self.schema_root = snapshot.schema_root;
        self.schema_dirty = snapshot.schema_dirty;
        self.engine.restore_snapshot(snapshot.engine)
    }

    pub(crate) fn begin_transaction(&mut self) -> Result<()> {
        self.engine.begin_transaction()
    }

    pub(crate) fn refresh_from_storage(&mut self) -> Result<()> {
        if self.schema_dirty {
            return Ok(());
        }

        let transaction_active = self.engine.transaction_active()?;
        if !transaction_active {
            self.engine.begin_read()?;
        }

        let refresh = (|| -> Result<()> {
            let Some(header) = self.engine.read_database_header()? else {
                return Ok(());
            };

            self.engine.refresh_runtime_metadata()?;
            self.schema = self.engine.load_schema(header.schema_root_page)?;
            self.schema_root = header.schema_root_page;
            self.schema_dirty = false;
            Ok(())
        })();

        if transaction_active {
            return refresh;
        }

        let release = self.engine.end_read();
        match (refresh, release) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(err), _) => Err(err),
            (Ok(()), Err(err)) => Err(err),
        }
    }

    pub fn auto_vacuum(&mut self) -> Result<()> {
        let mut root_pages = Vec::new();
        if self.schema_root >= 2 {
            root_pages.push(self.schema_root);
        }
        for table in self.schema.tables().values() {
            if table.root_page_id >= 2 {
                root_pages.push(table.root_page_id);
            }
            if table.primary_key_index_root_page_id >= 2 {
                root_pages.push(table.primary_key_index_root_page_id);
            }
            for index in &table.secondary_indexes {
                if index.root_page_id >= 2 {
                    root_pages.push(index.root_page_id);
                }
            }
        }

        let storage = self.engine.tree_store.shared_storage();
        crate::btree::compaction::auto_vacuum(&storage, &mut root_pages, |old_id, new_id| -> Result<()> {
            if self.schema_root == old_id {
                self.schema_root = new_id;
                self.engine.update_database_header(|h| h.schema_root_page = new_id)?;
            }
            for table in self.schema.tables_mut().values_mut() {
                if table.root_page_id == old_id {
                    table.root_page_id = new_id;
                    if let Some(metadata) = self.engine.table_metadata.get_mut(&table.name) {
                        metadata.root_page_id = new_id;
                    }
                    self.schema_dirty = true;
                }
                if table.primary_key_index_root_page_id == old_id {
                    table.primary_key_index_root_page_id = new_id;
                    self.schema_dirty = true;
                }
                for index in &mut table.secondary_indexes {
                    if index.root_page_id == old_id {
                        index.root_page_id = new_id;
                        self.schema_dirty = true;
                    }
                }
            }
            Ok(())
        })
    }

    pub(crate) fn commit_transaction(&mut self) -> Result<()> {
        self.save_schema_to_btree()?;
        self.auto_vacuum()?;
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

    pub(crate) fn has_pending_changes(&self) -> Result<bool> {
        Ok(self.schema_dirty || self.engine.has_pending_changes()?)
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
        self.run_atomically(|catalog| {
            catalog.schema = schema;
            catalog.schema_dirty = true;
            catalog.save_schema_to_btree()?;
            catalog
                .engine
                .set_next_table_id(catalog.schema.next_table_id())
        })
    }

    pub fn set_table_root_page(&mut self, table_id: TableId, root_page: u32) -> Result<()> {
        self.run_schema_mutation(|catalog| {
            if catalog.schema.get_table(table_id).is_none() {
                return Err(crate::error::HematiteError::StorageError(format!(
                    "Table ID {} not found",
                    table_id.as_u32()
                )));
            }

            if root_page <= 1 {
                return Err(crate::error::HematiteError::StorageError(
                    "Root pages 0 and 1 are reserved".to_string(),
                ));
            }

            catalog.schema.set_table_root_page(table_id, root_page)
        })
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
        self.run_schema_mutation(|catalog| catalog.schema.add_secondary_index(table_id, index))
    }

    pub fn set_table_primary_key_root_page(
        &mut self,
        table_id: TableId,
        root_page_id: u32,
    ) -> Result<()> {
        self.run_schema_mutation(|catalog| {
            if root_page_id <= 1 {
                return Err(crate::error::HematiteError::StorageError(
                    "Root pages 0 and 1 are reserved".to_string(),
                ));
            }

            catalog
                .schema
                .set_table_primary_key_root_page(table_id, root_page_id)
        })
    }

    pub fn set_table_storage_roots(
        &mut self,
        table_id: TableId,
        table_root_page_id: u32,
        primary_key_root_page_id: u32,
    ) -> Result<()> {
        self.run_schema_mutation(|catalog| {
            if table_root_page_id <= 1 || primary_key_root_page_id <= 1 {
                return Err(crate::error::HematiteError::StorageError(
                    "Root pages 0 and 1 are reserved".to_string(),
                ));
            }

            catalog.schema.set_table_storage_roots(
                table_id,
                table_root_page_id,
                primary_key_root_page_id,
            )
        })
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
        self.run_schema_mutation(|catalog| {
            if catalog.schema.get_table_by_name(name).is_some() {
                return Err(crate::error::HematiteError::StorageError(format!(
                    "Table '{}' already exists",
                    name
                )));
            }

            let table_id = catalog.get_next_table_id()?;
            let table = Table::new(table_id, name.to_string(), columns, root_page)?;

            catalog.schema.insert_table(table)?;
            Ok(table_id)
        })
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
