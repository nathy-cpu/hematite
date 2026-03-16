//! Catalog

use crate::catalog::column::Column;
use crate::catalog::ids::TableId;
use crate::catalog::schema::Schema;
use crate::catalog::table::Table;
use crate::error::Result;
use crate::storage::{PageId, StorageEngine};

/// Catalog manager for all database objects
#[derive(Debug)]
pub struct Catalog {
    storage: StorageEngine,
    schema_page_id: PageId,
}

impl Catalog {
    pub fn new(mut storage: StorageEngine) -> Result<Self> {
        // Initialize schema if it doesn't exist
        let schema_page_id = storage.allocate_page()?;
        let mut schema_page = crate::storage::Page::new(schema_page_id);

        let schema = Schema::new();
        schema.serialize(&mut schema_page.data)?;
        storage.write_page(schema_page)?;

        Ok(Self {
            storage,
            schema_page_id,
        })
    }

    pub fn create_table(&mut self, name: &str, columns: Vec<Column>) -> Result<TableId> {
        let mut schema = self.load_schema()?;
        let table_id = schema.create_table(name.to_string(), columns)?;
        self.save_schema(&schema)?;
        Ok(table_id)
    }

    pub fn get_table(&mut self, table_id: TableId) -> Result<Option<Table>> {
        let schema = self.load_schema()?;
        Ok(schema.get_table(table_id).cloned())
    }

    pub fn drop_table(&mut self, table_id: TableId) -> Result<()> {
        let mut schema = self.load_schema()?;
        schema.drop_table(table_id)?;
        self.save_schema(&schema)?;
        Ok(())
    }

    pub fn list_tables(&mut self) -> Result<Vec<(TableId, String)>> {
        let schema = self.load_schema()?;
        Ok(schema.list_tables())
    }

    fn load_schema(&mut self) -> Result<Schema> {
        let page = self.storage.read_page(self.schema_page_id)?;
        Schema::deserialize(&page.data)
    }

    fn save_schema(&mut self, schema: &Schema) -> Result<()> {
        let mut page = self.storage.read_page(self.schema_page_id)?;
        schema.serialize(&mut page.data)?;
        self.storage.write_page(page)?;
        Ok(())
    }

    /// Load schema from B-tree (for future use in Phase 3)
    fn load_schema_from_btree(&mut self, schema_root: PageId) -> Result<Schema> {
        // Note: This is a placeholder for Phase 3 implementation
        // For now, we'll create an empty schema since StorageEngine doesn't implement Clone
        let _schema_root = schema_root;

        // In Phase 3, this will be implemented as:
        // let storage = Arc::new(Mutex::new(self.storage));
        // let btree = BTreeIndex::from_shared_storage(storage, schema_root);
        // let mut cursor = btree.cursor()?;
        //
        // let mut schema = Schema::new();
        // while cursor.is_valid() {
        //     if let (Some(key), Some(value)) = (cursor.key(), cursor.value()) {
        //         let table_name = String::from_utf8(key.data.clone())
        //             .map_err(|e| HematiteError::StorageError(format!("Invalid table name: {}", e)))?;
        //         let table = Table::from_bytes(&value.data)?;
        //
        //         // Use Schema's public methods to add tables
        //         schema.create_table(table_name, table.columns)?;
        //     }
        //     cursor.next()?;
        // }
        //
        // Ok(schema)

        Ok(Schema::new())
    }
}
