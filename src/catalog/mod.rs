//! Catalog and schema management for database objects

pub mod column;
pub mod schema;
pub mod table;
pub mod types;

use crate::error::Result;
use crate::storage::{PageId, StorageEngine};

pub use column::Column;
pub use schema::Schema;
pub use table::Table;
pub use types::{DataType, Value};

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableId(u32);

impl TableId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnId(u32);

impl ColumnId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}
