//! Relational storage engine built on top of the pager and generic B-trees.

use crate::btree::tree::create_tree_root;
use crate::catalog::{Table, Value};
use crate::error::{HematiteError, Result};
use crate::storage::{
    Page, PageId, Pager, PagerIntegrityReport, DB_HEADER_PAGE_ID, STORAGE_METADATA_PAGE_ID,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use super::cursor::{IndexCursor, TableCursor};
use super::{engine_metadata, index_store, integrity, table_store};

#[derive(Debug, Clone)]
pub struct TableRuntimeMetadata {
    pub name: String,
    pub root_page_id: PageId,
    pub row_count: u64,
    pub next_row_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogStorageStats {
    pub table_count: usize,
    pub total_rows: u64,
    pub file_bytes: u64,
    pub allocated_page_count: usize,
    pub free_page_count: usize,
    pub fragmented_free_page_count: usize,
    pub trailing_free_page_count: usize,
    pub live_table_page_count: usize,
    pub table_used_bytes: usize,
    pub table_unused_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogIntegrityReport {
    pub table_count: usize,
    pub live_page_count: usize,
    pub index_page_count: usize,
    pub overflow_page_count: usize,
    pub free_page_count: usize,
    pub total_rows: u64,
    pub pager: PagerIntegrityReport,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredRow {
    pub row_id: u64,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone)]
pub struct CatalogEngineSnapshot {
    table_metadata: HashMap<String, TableRuntimeMetadata>,
}

#[derive(Debug)]
pub struct CatalogEngine {
    pub(crate) pager: Arc<Mutex<Pager>>,
    pub(crate) table_metadata: HashMap<String, TableRuntimeMetadata>,
}

impl CatalogEngine {
    pub(crate) const STORAGE_METADATA_VERSION: u32 = 3;

    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let pager = Arc::new(Mutex::new(Pager::new(path, 100)?));
        Self::from_shared_pager(pager)
    }

    pub fn new_in_memory() -> Result<Self> {
        let pager = Arc::new(Mutex::new(Pager::new_in_memory(100)?));
        Self::from_shared_pager(pager)
    }

    pub fn from_shared_pager(pager: Arc<Mutex<Pager>>) -> Result<Self> {
        let mut engine = Self {
            pager,
            table_metadata: HashMap::new(),
        };
        engine_metadata::load_table_metadata(&mut engine)?;
        Ok(engine)
    }

    pub fn shared_pager(&self) -> Arc<Mutex<Pager>> {
        self.pager.clone()
    }

    pub fn read_page(&self, page_id: PageId) -> Result<Page> {
        self.pager.lock().unwrap().read_page(page_id)
    }

    pub fn write_page(&self, page: Page) -> Result<()> {
        self.pager.lock().unwrap().write_page(page)
    }

    pub fn allocate_page(&self) -> Result<PageId> {
        let page_id = self.pager.lock().unwrap().allocate_page()?;
        if page_id == DB_HEADER_PAGE_ID || page_id == STORAGE_METADATA_PAGE_ID {
            return self.allocate_page();
        }
        Ok(page_id)
    }

    pub fn deallocate_page(&self, page_id: PageId) -> Result<()> {
        if page_id == DB_HEADER_PAGE_ID || page_id == STORAGE_METADATA_PAGE_ID {
            return Err(HematiteError::StorageError(
                "Cannot deallocate reserved page".to_string(),
            ));
        }
        self.pager.lock().unwrap().deallocate_page(page_id)
    }

    pub fn flush(&mut self) -> Result<()> {
        engine_metadata::save_table_metadata(self)?;
        self.pager.lock().unwrap().flush()
    }

    pub fn begin_transaction(&mut self) -> Result<()> {
        self.pager.lock().unwrap().begin_transaction()
    }

    pub fn commit_transaction(&mut self) -> Result<()> {
        engine_metadata::save_table_metadata(self)?;
        self.pager.lock().unwrap().commit_transaction()
    }

    pub fn rollback_transaction(&mut self) -> Result<()> {
        self.pager.lock().unwrap().rollback_transaction()
    }

    pub fn transaction_active(&self) -> bool {
        self.pager.lock().unwrap().transaction_active()
    }

    pub fn snapshot(&self) -> CatalogEngineSnapshot {
        CatalogEngineSnapshot {
            table_metadata: self.table_metadata.clone(),
        }
    }

    pub fn restore_snapshot(&mut self, snapshot: CatalogEngineSnapshot) {
        self.table_metadata = snapshot.table_metadata;
    }

    pub fn create_empty_btree(&self) -> Result<PageId> {
        let mut pager = self.pager.lock().unwrap();
        create_tree_root(&mut pager)
    }

    pub fn get_table_metadata(&self) -> &HashMap<String, TableRuntimeMetadata> {
        &self.table_metadata
    }

    pub fn get_storage_stats(&self) -> CatalogStorageStats {
        table_store::get_storage_stats(self)
    }

    pub fn create_table(&mut self, table_name: &str) -> Result<PageId> {
        table_store::create_table(self, table_name)
    }

    pub fn insert_into_table(&mut self, table_name: &str, row: Vec<Value>) -> Result<u64> {
        table_store::insert_into_table(self, table_name, row)
    }

    pub fn replace_table_rows(&mut self, table_name: &str, rows: Vec<StoredRow>) -> Result<()> {
        table_store::replace_table_rows(self, table_name, rows)
    }

    pub fn insert_row_with_rowid(&mut self, table_name: &str, row: StoredRow) -> Result<()> {
        table_store::insert_row_with_rowid(self, table_name, row)
    }

    pub fn delete_from_table_by_rowid(&mut self, table_name: &str, rowid: u64) -> Result<bool> {
        table_store::delete_from_table_by_rowid(self, table_name, rowid)
    }

    pub fn drop_table(&mut self, table_name: &str) -> Result<()> {
        table_store::drop_table(self, table_name)
    }

    pub fn drop_table_with_indexes(&mut self, table: &Table) -> Result<()> {
        index_store::drop_table_with_indexes(self, table)
    }

    pub fn open_table_cursor(&mut self, table_name: &str) -> Result<TableCursor> {
        table_store::open_table_cursor(self, table_name)
    }

    pub fn read_rows_with_ids(&mut self, table_name: &str) -> Result<Vec<StoredRow>> {
        table_store::read_rows_with_ids(self, table_name)
    }

    pub fn read_from_table(&mut self, table_name: &str) -> Result<Vec<Vec<Value>>> {
        table_store::read_from_table(self, table_name)
    }

    pub fn lookup_row_by_rowid(
        &mut self,
        table_name: &str,
        rowid: u64,
    ) -> Result<Option<StoredRow>> {
        table_store::lookup_row_by_rowid(self, table_name, rowid)
    }

    pub fn lookup_row_by_primary_key(
        &mut self,
        table: &Table,
        key_values: &[Value],
    ) -> Result<Option<StoredRow>> {
        index_store::lookup_row_by_primary_key(self, table, key_values)
    }

    pub fn lookup_primary_key_rowid(
        &mut self,
        table: &Table,
        key_values: &[Value],
    ) -> Result<Option<u64>> {
        index_store::lookup_primary_key_rowid(self, table, key_values)
    }

    pub fn register_primary_key_row(&mut self, table: &Table, row: StoredRow) -> Result<()> {
        index_store::register_primary_key_row(self, table, row)
    }

    pub fn lookup_rows_by_secondary_index(
        &mut self,
        table: &Table,
        index_name: &str,
        key_values: &[Value],
    ) -> Result<Vec<StoredRow>> {
        index_store::lookup_rows_by_secondary_index(self, table, index_name, key_values)
    }

    pub fn lookup_secondary_index_rowids(
        &mut self,
        table: &Table,
        index_name: &str,
        key_values: &[Value],
    ) -> Result<Vec<u64>> {
        index_store::lookup_secondary_index_rowids(self, table, index_name, key_values)
    }

    pub fn register_secondary_index_row(&mut self, table: &Table, row: StoredRow) -> Result<()> {
        index_store::register_secondary_index_row(self, table, row)
    }

    pub fn rebuild_primary_key_index(&mut self, table: &Table, rows: &[StoredRow]) -> Result<()> {
        index_store::rebuild_primary_key_index(self, table, rows)
    }

    pub fn rebuild_secondary_indexes(&mut self, table: &Table, rows: &[StoredRow]) -> Result<()> {
        index_store::rebuild_secondary_indexes(self, table, rows)
    }

    pub fn delete_primary_key_row(&mut self, table: &Table, row: &StoredRow) -> Result<bool> {
        index_store::delete_primary_key_row(self, table, row)
    }

    pub fn delete_secondary_index_row(&mut self, table: &Table, row: &StoredRow) -> Result<()> {
        index_store::delete_secondary_index_row(self, table, row)
    }

    pub fn encode_primary_key(&self, key_values: &[Value]) -> Result<Vec<u8>> {
        index_store::encode_primary_key(key_values)
    }

    pub fn encode_secondary_index_key(&self, key_values: &[Value]) -> Result<Vec<u8>> {
        index_store::encode_secondary_index_key(key_values)
    }

    pub fn open_primary_key_cursor(&mut self, table: &Table) -> Result<IndexCursor> {
        index_store::open_primary_key_cursor(self, table)
    }

    pub fn open_secondary_index_cursor(
        &mut self,
        table: &Table,
        index_name: &str,
    ) -> Result<IndexCursor> {
        index_store::open_secondary_index_cursor(self, table, index_name)
    }

    pub fn validate_table_indexes(&mut self, table: &Table) -> Result<()> {
        integrity::validate_table_indexes(self, table)
    }

    pub fn validate_integrity(&mut self) -> Result<CatalogIntegrityReport> {
        integrity::validate_integrity(self)
    }
}
