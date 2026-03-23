//! Relational storage engine built on top of the pager and generic B-trees.

use crate::btree::{ByteTree, ByteTreeStore, KeyValueCodec, TreeSpaceStats, TypedTreeStore};
use crate::catalog::{DatabaseHeader, JournalMode, Table, TableId, Value};
use crate::error::{HematiteError, Result};
use crate::storage::{
    JournalMode as PagerJournalMode, Page, PageId, Pager, PagerIntegrityReport, DB_HEADER_PAGE_ID,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use super::cursor::{IndexCursor, TableCursor};
use super::{
    engine_metadata, index_store, integrity, runtime_metadata, schema_store, table_store, Schema,
};

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
    pub overflow_page_count: usize,
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
    pub(crate) const PAGE_SIZE: usize = crate::storage::PAGE_SIZE;
    pub(crate) const INVALID_PAGE_ID: PageId = crate::storage::INVALID_PAGE_ID;
    pub(crate) const STORAGE_METADATA_VERSION: u32 = 3;

    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let pager = Arc::new(Mutex::new(Pager::new(path, 100)?));
        Self::from_shared_pager(pager)
    }

    pub fn new_in_memory() -> Result<Self> {
        let pager = Arc::new(Mutex::new(Pager::new_in_memory(100)?));
        Self::from_shared_pager(pager)
    }

    pub(crate) fn from_shared_pager(pager: Arc<Mutex<Pager>>) -> Result<Self> {
        let mut engine = Self {
            pager,
            table_metadata: HashMap::new(),
        };
        engine_metadata::load_table_metadata(&mut engine)?;
        Ok(engine)
    }

    pub fn read_database_header(&self) -> Result<Option<DatabaseHeader>> {
        let mut pager = self.pager.lock().unwrap();
        match pager.read_page(DB_HEADER_PAGE_ID) {
            Ok(page) => Ok(Some(DatabaseHeader::deserialize(&page.data)?)),
            Err(_) => Ok(None),
        }
    }

    pub fn initialize_database_header(&mut self, schema_root_page: u32) -> Result<DatabaseHeader> {
        let header = DatabaseHeader::new(schema_root_page);
        let mut page = Page::new(DB_HEADER_PAGE_ID);
        header.serialize(&mut page.data)?;

        let mut pager = self.pager.lock().unwrap();
        pager.write_page(page)?;
        pager.flush()?;
        Ok(header)
    }

    pub fn allocate_table_id(&mut self) -> Result<TableId> {
        let mut pager = self.pager.lock().unwrap();
        let header_page = pager.read_page(DB_HEADER_PAGE_ID)?;
        let mut header = DatabaseHeader::deserialize(&header_page.data)?;
        let table_id = header.increment_table_id();

        let mut updated_page = Page::new(DB_HEADER_PAGE_ID);
        header.serialize(&mut updated_page.data)?;
        pager.write_page(updated_page)?;
        Ok(table_id)
    }

    pub fn set_next_table_id(&mut self, next_table_id: u32) -> Result<()> {
        self.update_database_header(|header| {
            header.next_table_id = next_table_id;
        })
    }

    pub fn peek_next_table_id(&self) -> Result<TableId> {
        let header = self
            .read_database_header()?
            .ok_or_else(|| HematiteError::StorageError("Database header is missing".to_string()))?;
        Ok(TableId::new(header.next_table_id))
    }

    pub fn update_database_header<F>(&mut self, update: F) -> Result<()>
    where
        F: FnOnce(&mut DatabaseHeader),
    {
        let mut pager = self.pager.lock().unwrap();
        let header_page = pager.read_page(DB_HEADER_PAGE_ID)?;
        let mut header = DatabaseHeader::deserialize(&header_page.data)?;
        update(&mut header);
        header.checksum = header.calculate_checksum();

        let mut updated_page = Page::new(DB_HEADER_PAGE_ID);
        header.serialize(&mut updated_page.data)?;
        pager.write_page(updated_page)
    }

    #[cfg(test)]
    pub(crate) fn read_page(&self, page_id: PageId) -> Result<Page> {
        self.pager.lock().unwrap().read_page(page_id)
    }

    #[cfg(test)]
    pub(crate) fn write_page(&self, page: Page) -> Result<()> {
        self.pager.lock().unwrap().write_page(page)
    }

    #[cfg(test)]
    pub(crate) fn allocate_page(&self) -> Result<PageId> {
        let page_id = self.pager.lock().unwrap().allocate_page()?;
        if page_id == DB_HEADER_PAGE_ID || page_id == crate::storage::STORAGE_METADATA_PAGE_ID {
            return self.allocate_page();
        }
        Ok(page_id)
    }

    #[cfg(test)]
    pub(crate) fn deallocate_page(&self, page_id: PageId) -> Result<()> {
        if page_id == DB_HEADER_PAGE_ID || page_id == crate::storage::STORAGE_METADATA_PAGE_ID {
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

    pub fn journal_mode(&self) -> Result<JournalMode> {
        Ok(match self.pager.lock().unwrap().journal_mode() {
            PagerJournalMode::Rollback => JournalMode::Rollback,
            PagerJournalMode::Wal => JournalMode::Wal,
        })
    }

    pub fn set_journal_mode(&mut self, journal_mode: JournalMode) -> Result<()> {
        let mode = match journal_mode {
            JournalMode::Rollback => PagerJournalMode::Rollback,
            JournalMode::Wal => PagerJournalMode::Wal,
        };
        self.pager.lock().unwrap().set_journal_mode(mode)
    }

    pub fn checkpoint_wal(&mut self) -> Result<()> {
        self.pager.lock().unwrap().checkpoint_wal()
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

    pub(crate) fn begin_read(&mut self) -> Result<()> {
        self.pager.lock().unwrap().begin_read()
    }

    pub(crate) fn end_read(&mut self) -> Result<()> {
        self.pager.lock().unwrap().end_read()
    }

    pub fn snapshot(&self) -> CatalogEngineSnapshot {
        CatalogEngineSnapshot {
            table_metadata: self.table_metadata.clone(),
        }
    }

    pub fn restore_snapshot(&mut self, snapshot: CatalogEngineSnapshot) {
        self.table_metadata = snapshot.table_metadata;
    }

    pub(crate) fn create_empty_btree(&self) -> Result<PageId> {
        self.tree_store().create_tree()
    }

    pub(crate) fn get_table_metadata(&self) -> &HashMap<String, TableRuntimeMetadata> {
        &self.table_metadata
    }

    pub(crate) fn load_schema(&self, schema_root: PageId) -> Result<Schema> {
        schema_store::load_schema(self, schema_root)
    }

    pub(crate) fn save_schema(&mut self, schema: &Schema, current_root: PageId) -> Result<PageId> {
        schema_store::save_schema(self, schema, current_root)
    }

    pub(crate) fn tree_store(&self) -> ByteTreeStore {
        ByteTreeStore::from_shared_storage(self.pager.clone())
    }

    pub(crate) fn typed_tree_store<C: KeyValueCodec>(&self) -> TypedTreeStore<C> {
        TypedTreeStore::from_shared_storage(self.pager.clone())
    }

    pub(crate) fn open_tree(&self, root_page_id: PageId) -> Result<ByteTree> {
        self.tree_store().open_tree(root_page_id)
    }

    pub(crate) fn create_tree(&self) -> Result<PageId> {
        self.tree_store().create_tree()
    }

    pub(crate) fn delete_tree(&self, root_page_id: PageId) -> Result<()> {
        self.tree_store().delete_tree(root_page_id)
    }

    pub(crate) fn reset_tree(&self, root_page_id: PageId) -> Result<()> {
        self.tree_store().reset_tree(root_page_id)
    }

    pub(crate) fn read_tree_entries(
        &self,
        root_page_id: PageId,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.open_tree(root_page_id)?.entries()
    }

    pub(crate) fn visit_tree_entries<F>(&self, root_page_id: PageId, mut visit: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        let tree = self.open_tree(root_page_id)?;
        let mut cursor = tree.cursor()?;
        cursor.first()?;
        while let Some((key, value)) = cursor.current()? {
            visit(&key, &value)?;
            if cursor.next().is_err() {
                break;
            }
        }
        Ok(())
    }

    pub(crate) fn insert_tree_entry(
        &self,
        root_page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<PageId> {
        let mut tree = self.open_tree(root_page_id)?;
        Ok(tree.insert_with_mutation(key, value)?.root_page_id)
    }

    pub(crate) fn collect_tree_page_ids(&self, root_page_id: PageId) -> Result<Vec<PageId>> {
        self.tree_store().collect_page_ids(root_page_id)
    }

    pub(crate) fn collect_tree_space_stats(&self, root_page_id: PageId) -> Result<TreeSpaceStats> {
        self.tree_store().collect_space_stats(root_page_id)
    }

    pub(crate) fn pager_integrity_report(&mut self) -> Result<PagerIntegrityReport> {
        self.pager.lock().unwrap().validate_integrity()
    }

    pub(crate) fn free_page_ids(&self) -> Vec<PageId> {
        self.pager.lock().unwrap().free_pages().to_vec()
    }

    pub(crate) fn is_reserved_page(page_id: PageId) -> bool {
        page_id == DB_HEADER_PAGE_ID || page_id == crate::storage::STORAGE_METADATA_PAGE_ID
    }

    pub fn get_storage_stats(&self) -> CatalogStorageStats {
        table_store::get_storage_stats(self)
    }

    pub(crate) fn create_runtime_table_metadata(
        &mut self,
        table_name: &str,
        root_page_id: PageId,
    ) -> Result<()> {
        runtime_metadata::create_table_metadata(self, table_name, root_page_id)
    }

    pub(crate) fn table_runtime_metadata(&self, table_name: &str) -> Result<&TableRuntimeMetadata> {
        runtime_metadata::lookup_table_metadata(self, table_name)
    }

    pub(crate) fn remove_runtime_table_metadata(
        &mut self,
        table_name: &str,
    ) -> Result<TableRuntimeMetadata> {
        runtime_metadata::remove_table_metadata(self, table_name)
    }

    pub(crate) fn record_generated_row_insert(
        &mut self,
        table_name: &str,
        new_root_page_id: PageId,
        row_id: u64,
    ) {
        runtime_metadata::apply_insert(self, table_name, new_root_page_id, Some(row_id + 1));
    }

    pub(crate) fn record_explicit_row_insert(
        &mut self,
        table_name: &str,
        new_root_page_id: PageId,
    ) {
        runtime_metadata::apply_insert(self, table_name, new_root_page_id, None);
    }

    pub(crate) fn record_row_delete(
        &mut self,
        table_name: &str,
        new_root_page_id: PageId,
        deleted: bool,
    ) {
        runtime_metadata::apply_delete(self, table_name, new_root_page_id, deleted);
    }

    pub(crate) fn prepare_table_replace(&mut self, table_name: &str, rows: &[StoredRow]) {
        runtime_metadata::prepare_replace(self, table_name, rows);
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

    pub(crate) fn validate_catalog_layout(
        &mut self,
        tables: &[Table],
    ) -> Result<integrity::CatalogTreeUsage> {
        integrity::validate_catalog_layout(self, tables)
    }

    pub fn validate_integrity(&mut self) -> Result<CatalogIntegrityReport> {
        integrity::validate_integrity(self)
    }
}
