//! Catalog - SQLite-style schema management with B-tree persistence

use crate::btree::tree::BTreeManager;
use crate::btree::BTreeIndex;
use crate::catalog::column::Column;
use crate::catalog::header::DatabaseHeader;
use crate::catalog::ids::TableId;
use crate::catalog::schema::Schema;
use crate::catalog::table::{SecondaryIndex, Table};
use crate::error::Result;
use crate::storage::{Page, PageId, StorageEngine, StorageIntegrityReport, DB_HEADER_PAGE_ID};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// SQLite-style catalog manager with B-tree schema persistence
#[derive(Debug)]
pub struct Catalog {
    storage: Arc<Mutex<StorageEngine>>,
    schema: Schema,
    schema_root: PageId,
    schema_dirty: bool,
}

impl Catalog {
    /// Open or create a database with SQLite-style schema management
    pub fn open_or_create<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let storage = StorageEngine::new(path)?;
        Self::open_with_storage(storage)
    }

    pub fn open_in_memory() -> Result<Self> {
        let storage = StorageEngine::new_in_memory()?;
        Self::open_with_storage(storage)
    }

    fn open_with_storage(storage: StorageEngine) -> Result<Self> {
        let storage = Arc::new(Mutex::new(storage));

        // Try to read existing database header
        let mut storage_guard = storage.lock().unwrap();
        let header = match storage_guard.read_page(DB_HEADER_PAGE_ID) {
            Ok(page) => {
                // Existing database - read header
                DatabaseHeader::deserialize(&page)?
            }
            Err(_) => {
                // New database - create header and schema B-tree
                let header = DatabaseHeader::new(PageId::new(2)); // Start schema at page 2

                // Create empty schema B-tree
                let schema_root = storage_guard.create_empty_btree()?;
                let mut new_header = header;
                new_header.schema_root_page = schema_root;
                new_header.checksum = new_header.calculate_checksum();

                // Write header to page 0
                let mut header_page = Page::new(DB_HEADER_PAGE_ID);
                new_header.serialize(&mut header_page)?;
                storage_guard.write_page(header_page)?;

                new_header
            }
        };

        drop(storage_guard);

        // Load schema from B-tree
        let schema = Self::load_schema_from_btree(&storage, header.schema_root_page)?;

        Ok(Self {
            storage,
            schema,
            schema_root: header.schema_root_page,
            schema_dirty: false,
        })
    }

    /// Load schema from the schema B-tree
    fn load_schema_from_btree(
        storage: &Arc<Mutex<StorageEngine>>,
        schema_root: PageId,
    ) -> Result<Schema> {
        let btree = BTreeIndex::from_shared_storage(storage.clone(), schema_root);
        let mut cursor = btree.cursor()?;

        let mut schema = Schema::new();

        while cursor.is_valid() {
            if let (Some(key), Some(value)) = (cursor.key(), cursor.value()) {
                let table_name = String::from_utf8(key.data.clone()).map_err(|e| {
                    crate::error::HematiteError::StorageError(format!("Invalid table name: {}", e))
                })?;
                let mut table = Table::from_bytes(&value.data)?;
                // Ensure the persisted name matches the key to avoid inconsistencies.
                table.name = table_name;
                schema.insert_table(table)?;
            }
            cursor.next()?;
        }

        Ok(schema)
    }

    /// Save schema to the B-tree (transactional)
    fn save_schema_to_btree(&mut self) -> Result<()> {
        if !self.schema_dirty {
            return Ok(());
        }

        let table_entries = self
            .schema
            .list_tables()
            .into_iter()
            .filter_map(|(table_id, _name)| self.schema.get_table(table_id).cloned())
            .map(|table| {
                Ok((
                    crate::btree::BTreeKey::new(table.name.as_bytes().to_vec()),
                    crate::btree::BTreeValue::new(table.to_bytes()?),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let old_schema_root = self.schema_root;
        let mut manager = BTreeManager::from_shared_storage(self.storage.clone());
        manager.delete_tree(old_schema_root)?;
        let new_schema_root = manager.create_tree()?;

        let mut btree =
            crate::btree::BTreeIndex::from_shared_storage(self.storage.clone(), new_schema_root);

        for (key, value) in table_entries {
            btree.insert(key, value)?;
        }

        let mut storage_guard = self.storage.lock().unwrap();
        let header_page = storage_guard.read_page(DB_HEADER_PAGE_ID)?;
        let mut header = DatabaseHeader::deserialize(&header_page)?;
        header.schema_root_page = new_schema_root;
        header.checksum = header.calculate_checksum();
        let mut updated = Page::new(DB_HEADER_PAGE_ID);
        header.serialize(&mut updated)?;
        storage_guard.write_page(updated)?;

        self.schema_root = new_schema_root;
        self.schema_dirty = false;
        Ok(())
    }

    /// Get the next table ID from database header
    fn get_next_table_id(&self) -> Result<TableId> {
        let mut storage_guard = self.storage.lock().unwrap();
        let header_page = storage_guard.read_page(DB_HEADER_PAGE_ID)?;
        let mut header = DatabaseHeader::deserialize(&header_page)?;
        let table_id = header.increment_table_id();

        // Update header with new table ID
        let mut updated_page = Page::new(DB_HEADER_PAGE_ID);
        header.serialize(&mut updated_page)?;
        storage_guard.write_page(updated_page)?;

        Ok(table_id)
    }

    /// Create a new table in the catalog
    pub fn create_table(&mut self, name: &str, columns: Vec<Column>) -> Result<TableId> {
        // Validate table name doesn't exist
        if self.schema.get_table_by_name(name).is_some() {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Table '{}' already exists",
                name
            )));
        }

        // Get next table ID
        let table_id = self.get_next_table_id()?;

        // Create table with placeholder root page (will be set when storage/B-tree is created).
        let table = Table::new(table_id, name.to_string(), columns, PageId::new(0))?;

        self.schema.insert_table(table.clone())?;
        self.schema_dirty = true;

        Ok(table_id)
    }

    /// Get a table by ID
    pub fn get_table(&self, table_id: TableId) -> Result<Option<Table>> {
        Ok(self.schema.get_table(table_id).cloned())
    }

    /// Get a table by name
    pub fn get_table_by_name(&self, name: &str) -> Result<Option<Table>> {
        Ok(self.schema.get_table_by_name(name).cloned())
    }

    /// Drop a table from the catalog
    pub fn drop_table(&mut self, table_id: TableId) -> Result<()> {
        // Check if table exists
        let table = self.schema.get_table(table_id).cloned();
        if table.is_none() {
            return Err(crate::error::HematiteError::StorageError(
                "Table not found".to_string(),
            ));
        }
        // Remove from in-memory schema
        self.schema.drop_table(table_id)?;
        self.schema_dirty = true;

        Ok(())
    }

    /// List all tables
    pub fn list_tables(&self) -> Result<Vec<(TableId, String)>> {
        Ok(self.schema.list_tables())
    }

    /// Get the in-memory schema (for testing purposes)
    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    /// Clone the current in-memory schema snapshot.
    pub fn clone_schema(&self) -> Schema {
        self.schema.clone()
    }

    /// Run a storage operation against the catalog's backing storage.
    pub fn with_storage<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut StorageEngine) -> Result<T>,
    {
        let mut storage_guard = self.storage.lock().unwrap();
        f(&mut storage_guard)
    }

    /// Force schema persistence to B-tree
    pub fn flush_schema(&mut self) -> Result<()> {
        self.save_schema_to_btree()
    }

    /// Flush both schema metadata and storage pages.
    pub fn flush(&mut self) -> Result<()> {
        self.save_schema_to_btree()?;
        let mut storage_guard = self.storage.lock().unwrap();
        storage_guard.flush()
    }

    /// Replace the entire in-memory schema and persist it as the durable catalog state.
    pub fn replace_schema(&mut self, schema: Schema) -> Result<()> {
        self.schema = schema;
        self.schema_dirty = true;
        self.save_schema_to_btree()?;

        let mut storage_guard = self.storage.lock().unwrap();
        let header_page = storage_guard.read_page(DB_HEADER_PAGE_ID)?;
        let mut header = DatabaseHeader::deserialize(&header_page)?;
        header.next_table_id = self.schema.next_table_id();
        header.checksum = header.calculate_checksum();

        let mut updated = Page::new(DB_HEADER_PAGE_ID);
        header.serialize(&mut updated)?;
        storage_guard.write_page(updated)?;

        Ok(())
    }

    /// Set the root page for a table's B-tree
    pub fn set_table_root_page(&mut self, table_id: TableId, root_page: PageId) -> Result<()> {
        // Validate table exists
        if self.schema.get_table(table_id).is_none() {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Table ID {} not found",
                table_id.as_u32()
            )));
        }

        // Validate root page is not page 0 (reserved for database header)
        if root_page.as_u32() == 0 {
            return Err(crate::error::HematiteError::StorageError(
                "Root page 0 is reserved for database header".to_string(),
            ));
        }

        // Update in-memory schema
        self.schema.set_table_root_page(table_id, root_page)?;
        self.schema_dirty = true;

        Ok(())
    }

    /// Get the root page for a table's B-tree
    pub fn get_table_root_page(&self, table_id: TableId) -> Result<Option<PageId>> {
        if let Some(table) = self.schema.get_table(table_id) {
            // Validate that root page is properly set
            if table.root_page_id.as_u32() == 0 {
                // Table exists but has no B-tree yet
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

        Ok(())
    }

    /// Validate the entire schema
    pub fn validate_schema(&self) -> Result<()> {
        let schema_result = self.schema.validate();

        // Additional catalog-specific validations
        for (table_id, table_name) in self.list_tables()? {
            let table = self.schema.get_table(table_id).ok_or_else(|| {
                crate::error::HematiteError::StorageError(format!(
                    "Table {} found in list but not in schema",
                    table_name
                ))
            })?;

            // Validate root page consistency
            if table.root_page_id.as_u32() == 0 {
                // This is OK for newly created tables without B-trees
                continue;
            }

            // For tables with B-trees, ensure root page is valid
            // (Additional validation could be added here to check if page exists in storage)
        }

        schema_result
    }

    pub fn validate_integrity(&self) -> Result<StorageIntegrityReport> {
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

        let mut storage_guard = self.storage.lock().unwrap();
        let storage_tables = storage_guard
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
                    table_name,
                    root_page_id.as_u32(),
                    storage_root.as_u32()
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

        storage_guard.validate_integrity()
    }

    // ... (rest of the code remains the same)
    pub fn get_total_column_count(&self) -> usize {
        self.schema.get_total_column_count()
    }

    /// Get table statistics
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

    /// Get all table statistics
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

    /// Check if a table exists by name
    pub fn table_exists(&self, name: &str) -> bool {
        self.schema.get_table_by_name(name).is_some()
    }

    /// Check if a table exists by ID
    pub fn table_exists_by_id(&self, table_id: TableId) -> bool {
        self.schema.get_table(table_id).is_some()
    }

    /// Get the next available table ID without incrementing
    pub fn peek_next_table_id(&self) -> Result<TableId> {
        let mut storage_guard = self.storage.lock().unwrap();
        let header_page = storage_guard.read_page(PageId::new(0))?;
        let header = DatabaseHeader::deserialize(&header_page)?;
        drop(storage_guard); // Release lock
        Ok(TableId::new(header.next_table_id))
    }

    /// Create a table with a specific root page (useful for B-tree setup)
    pub fn create_table_with_root(
        &mut self,
        name: &str,
        columns: Vec<Column>,
        root_page: PageId,
    ) -> Result<TableId> {
        // Validate table name doesn't exist
        if self.schema.get_table_by_name(name).is_some() {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Table '{}' already exists",
                name
            )));
        }

        // Get next table ID
        let table_id = self.get_next_table_id()?;

        let table = Table::new(table_id, name.to_string(), columns, root_page)?;

        self.schema.insert_table(table.clone())?;
        self.schema_dirty = true;

        Ok(table_id)
    }

    /// Get column information for a table
    pub fn get_table_columns(&self, table_id: TableId) -> Result<Option<Vec<Column>>> {
        if let Some(table) = self.schema.get_table(table_id) {
            Ok(Some(table.columns.clone()))
        } else {
            Ok(None)
        }
    }

    /// Get column information for a table by name
    pub fn get_table_columns_by_name(&self, name: &str) -> Result<Option<Vec<Column>>> {
        if let Some(table) = self.schema.get_table_by_name(name) {
            Ok(Some(table.columns.clone()))
        } else {
            Ok(None)
        }
    }

    /// Get primary key columns for a table
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
    pub root_page_id: PageId,
    pub row_size: usize,
}
