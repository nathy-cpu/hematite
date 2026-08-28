//! Catalog storage engine.
//!
//! This is the narrow bridge between relational concepts and the generic lower layers.
//!
//! ```text
//! SQL / planner / executor
//!          |
//!          v
//!      CatalogEngine
//!          |
//!   +------+------+
//!   |             |
//! rows/indexes  schema metadata
//!   |             |
//!   +-------> generic B-tree
//!                    |
//!                    v
//!                  pager
//! ```
//!
//! Responsibilities:
//! - hold runtime metadata such as row counts and next rowid;
//! - persist schema roots and table metadata;
//! - expose relational operations in terms of rows, keys, and cursors;
//! - prevent page- and node-level details from leaking into query/catalog code.
//!
//! This file should coordinate access methods, not define relational byte formats. Row codecs and
//! index-key codecs live beside the catalog model so the generic lower layers remain reusable.

use crate::btree::{
    ByteTree, ByteTreeStore, ByteTreeStoreSnapshot, JournalMode as BTreeJournalMode, KeyValueCodec,
    PageId, PagerIntegrityReport, TreeSpaceStats, TypedTreeStore,
};
use crate::catalog::{DatabaseHeader, JournalMode, Table, TableId, Value};
use crate::error::{HematiteError, Result};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use super::cursor::{IndexCursor, TableCursor};
use super::{
    engine_metadata, index_store, integrity, record::StoredRow, runtime_metadata, schema_store,
    table_store, Schema,
};

/// Runtime metadata for every open table, keyed by name.
///
/// Held behind an `Arc` by the engine. `run_atomically` wraps each single-row
/// insert and delete and snapshots this so a failed operation can put the row
/// counts back, and there are three such wrappers per inserted row — the table,
/// the primary-key index, and each secondary index. Cloning the map at each one
/// was eleven allocations a row. Only the first mutation after a snapshot copies
/// it now, via `Arc::make_mut`.
pub type TableMetadataMap = HashMap<Arc<str>, TableRuntimeMetadata>;

#[derive(Debug, Clone)]
pub struct TableRuntimeMetadata {
    /// The same handle the map is keyed by, not a second copy of the string.
    ///
    /// Cloning the map costs one allocation for its table plus one per `String` it
    /// owns, and it owned two per table: the key and this. `Arc<str>` borrows as
    /// `str`, so lookups by `&str` are unchanged.
    pub name: Arc<str>,
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

#[derive(Debug, Clone)]
pub struct CatalogEngineSnapshot {
    table_metadata: Arc<TableMetadataMap>,
    tree_store: ByteTreeStoreSnapshot,
}

impl CatalogEngineSnapshot {
    pub(crate) fn into_transaction_baseline(mut self) -> Self {
        self.tree_store = self.tree_store.into_transaction_baseline();
        self
    }
}

#[derive(Debug)]
pub struct CatalogEngine {
    pub(crate) tree_store: ByteTreeStore,
    pub(crate) table_metadata: Arc<TableMetadataMap>,
    /// The durable-state token as of the last [`Self::refresh_runtime_metadata`].
    metadata_state_token: Option<u64>,
}

impl CatalogEngine {
    pub(crate) const PAGE_SIZE: usize = ByteTreeStore::PAGE_SIZE;
    pub(crate) const INVALID_PAGE_ID: PageId = ByteTreeStore::INVALID_PAGE_ID;
    pub(crate) const STORAGE_METADATA_VERSION: u32 = 1;

    fn unsupported_storage_format_error(detail: impl Into<String>) -> HematiteError {
        HematiteError::StorageError(format!(
            "Unsupported on-disk database format: {}. This build only opens databases created \
with the current post-reset storage format. Create a new database file; an offline migrator is \
not available yet.",
            detail.into()
        ))
    }

    /// Default page-cache capacity (in pages) used when a caller does not specify one.
    ///
    /// At the 4 KiB page size this is ~8 MiB, comparable to SQLite's default cache. The
    /// previous value of 100 pages (~400 KiB) was far smaller than typical working sets and
    /// caused the cache to thrash — hot pages were evicted and re-read from the OS on nearly
    /// every operation. See the benchmark investigation for details.
    pub const DEFAULT_PAGE_CACHE_CAPACITY: usize = 2000;

    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::with_cache_capacity(path, Self::DEFAULT_PAGE_CACHE_CAPACITY)
    }

    pub fn with_cache_capacity<P: AsRef<Path>>(path: P, cache_capacity: usize) -> Result<Self> {
        Self::from_tree_store(ByteTreeStore::open_path(path, cache_capacity)?)
    }

    pub fn new_in_memory() -> Result<Self> {
        Self::new_in_memory_with_cache_capacity(Self::DEFAULT_PAGE_CACHE_CAPACITY)
    }

    pub fn new_in_memory_with_cache_capacity(cache_capacity: usize) -> Result<Self> {
        Self::from_tree_store(ByteTreeStore::new_in_memory(cache_capacity)?)
    }

    pub(crate) fn from_tree_store(tree_store: ByteTreeStore) -> Result<Self> {
        let mut engine = Self {
            tree_store,
            table_metadata: Arc::default(),
            metadata_state_token: None,
        };
        engine_metadata::load_table_metadata(&mut engine)?;
        Ok(engine)
    }

    pub fn read_database_header(&self) -> Result<Option<DatabaseHeader>> {
        let Some(page) = self
            .tree_store()
            .read_reserved_blob(ByteTreeStore::DB_HEADER_PAGE_ID)?
        else {
            return Ok(None);
        };

        if page.iter().all(|&byte| byte == 0) {
            return Ok(None);
        }

        if page.len() < 8 {
            return Err(Self::unsupported_storage_format_error(
                "reserved header page is too short",
            ));
        }

        let magic = [page[0], page[1], page[2], page[3]];
        if magic != DatabaseHeader::MAGIC {
            let printable_magic = String::from_utf8_lossy(&magic);
            return Err(Self::unsupported_storage_format_error(format!(
                "header magic {:?} ({}) does not match the current Hematite header",
                magic, printable_magic
            )));
        }

        let version = u32::from_le_bytes([page[4], page[5], page[6], page[7]]);
        if version != DatabaseHeader::CURRENT_VERSION {
            return Err(Self::unsupported_storage_format_error(format!(
                "header version {} is retired; expected version {}",
                version,
                DatabaseHeader::CURRENT_VERSION
            )));
        }

        Ok(Some(DatabaseHeader::deserialize(&page)?))
    }

    pub fn initialize_database_header(&mut self, schema_root_page: u32) -> Result<DatabaseHeader> {
        let header = DatabaseHeader::new(schema_root_page);
        let mut page = vec![0; ByteTreeStore::PAGE_SIZE];
        header.serialize(&mut page)?;
        self.tree_store()
            .write_reserved_blob(ByteTreeStore::DB_HEADER_PAGE_ID, &page)?;
        self.tree_store().flush()?;
        Ok(header)
    }

    pub fn allocate_table_id(&mut self) -> Result<TableId> {
        let header_page = self
            .tree_store()
            .read_reserved_blob(ByteTreeStore::DB_HEADER_PAGE_ID)?
            .ok_or_else(|| HematiteError::StorageError("Database header is missing".to_string()))?;
        let mut header = DatabaseHeader::deserialize(&header_page)?;
        let table_id = header.increment_table_id();

        let mut updated_page = vec![0; ByteTreeStore::PAGE_SIZE];
        header.serialize(&mut updated_page)?;
        self.tree_store()
            .write_reserved_blob(ByteTreeStore::DB_HEADER_PAGE_ID, &updated_page)?;
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
        let header_page = self
            .tree_store()
            .read_reserved_blob(ByteTreeStore::DB_HEADER_PAGE_ID)?
            .ok_or_else(|| HematiteError::StorageError("Database header is missing".to_string()))?;
        let mut header = DatabaseHeader::deserialize(&header_page)?;
        update(&mut header);
        header.checksum = header.calculate_checksum();

        let mut updated_page = vec![0; ByteTreeStore::PAGE_SIZE];
        header.serialize(&mut updated_page)?;
        self.tree_store()
            .write_reserved_blob(ByteTreeStore::DB_HEADER_PAGE_ID, &updated_page)
    }

    #[cfg(test)]
    pub(crate) fn read_page(&self, page_id: PageId) -> Result<crate::storage::Page> {
        let storage = self.tree_store().shared_storage();
        let pager = storage.read().map_err(|_| {
            HematiteError::InternalError("Catalog engine pager lock is poisoned".to_string())
        })?;
        pager.read_page(page_id)
    }

    #[cfg(test)]
    pub(crate) fn write_page(&self, page: crate::storage::Page) -> Result<()> {
        let storage = self.tree_store().shared_storage();
        let mut pager = storage.write().map_err(|_| {
            HematiteError::InternalError("Catalog engine pager lock is poisoned".to_string())
        })?;
        pager.write_page(page)
    }

    #[cfg(test)]
    pub(crate) fn allocate_page(&self) -> Result<PageId> {
        let storage = self.tree_store().shared_storage();
        let page_id = storage
            .write()
            .map_err(|_| {
                HematiteError::InternalError("Catalog engine pager lock is poisoned".to_string())
            })?
            .allocate_page()?;
        if Self::is_reserved_page(page_id) {
            return self.allocate_page();
        }
        Ok(page_id)
    }

    #[cfg(test)]
    pub(crate) fn deallocate_page(&self, page_id: PageId) -> Result<()> {
        if Self::is_reserved_page(page_id) {
            return Err(HematiteError::StorageError(
                "Cannot deallocate reserved page".to_string(),
            ));
        }
        let storage = self.tree_store().shared_storage();
        let mut pager = storage.write().map_err(|_| {
            HematiteError::InternalError("Catalog engine pager lock is poisoned".to_string())
        })?;
        pager.deallocate_page(page_id)
    }

    #[cfg(test)]
    pub(crate) fn with_pager<T>(
        &self,
        callback: impl FnOnce(&mut crate::storage::Pager) -> Result<T>,
    ) -> Result<T> {
        let storage = self.tree_store().shared_storage();
        let mut pager = storage.write().map_err(|_| {
            HematiteError::InternalError("Catalog engine pager lock is poisoned".to_string())
        })?;
        callback(&mut pager)
    }

    pub fn flush(&mut self) -> Result<()> {
        engine_metadata::save_table_metadata(self)?;
        self.tree_store().flush()
    }

    pub(crate) fn has_pending_changes(&self) -> Result<bool> {
        self.tree_store().has_pending_changes()
    }

    pub fn journal_mode(&self) -> Result<JournalMode> {
        Ok(match self.tree_store().journal_mode()? {
            BTreeJournalMode::Rollback => JournalMode::Rollback,
            BTreeJournalMode::Wal => JournalMode::Wal,
        })
    }

    pub fn set_journal_mode(&mut self, journal_mode: JournalMode) -> Result<()> {
        let mode = match journal_mode {
            JournalMode::Rollback => BTreeJournalMode::Rollback,
            JournalMode::Wal => BTreeJournalMode::Wal,
        };
        self.tree_store().set_journal_mode(mode)
    }

    pub fn checkpoint_wal(&mut self) -> Result<()> {
        self.tree_store().checkpoint_wal()
    }

    pub fn begin_transaction(&mut self) -> Result<()> {
        self.tree_store().begin_transaction()?;
        self.refresh_runtime_metadata()
    }

    pub fn commit_transaction(&mut self) -> Result<()> {
        engine_metadata::save_table_metadata(self)?;
        self.tree_store().commit_transaction()
    }

    pub fn rollback_transaction(&mut self) -> Result<()> {
        self.tree_store().rollback_transaction()
    }

    pub fn transaction_active(&self) -> Result<bool> {
        self.tree_store().transaction_active()
    }

    pub(crate) fn refresh_runtime_metadata(&mut self) -> Result<()> {
        Arc::make_mut(&mut self.table_metadata).clear();
        let loaded = engine_metadata::load_table_metadata(self);
        self.metadata_state_token = self.durable_state_token().ok();
        loaded
    }

    /// Re-reads the runtime metadata only if durable state has moved since the last
    /// time it was read.
    ///
    /// This runs at the start of every read scope, which is once per statement, and
    /// re-read and re-parsed the whole table-metadata section each time. Between two
    /// statements with no commit in between there is nothing new to read.
    fn refresh_runtime_metadata_if_stale(&mut self) -> Result<()> {
        if let Ok(token) = self.durable_state_token() {
            if self.metadata_state_token == Some(token) {
                return Ok(());
            }
        }
        self.refresh_runtime_metadata()
    }

    pub(crate) fn begin_read(&mut self) -> Result<()> {
        // Opening the read scope first: that is what notices another process's
        // commit, and it is what moves the token the check below reads.
        self.tree_store().begin_read()?;
        self.refresh_runtime_metadata_if_stale()
    }

    pub(crate) fn end_read(&mut self) -> Result<()> {
        self.tree_store().end_read()
    }

    pub fn snapshot(&self) -> Result<CatalogEngineSnapshot> {
        Ok(CatalogEngineSnapshot {
            table_metadata: Arc::clone(&self.table_metadata),
            tree_store: self.tree_store.snapshot()?,
        })
    }

    pub fn restore_snapshot(&mut self, snapshot: CatalogEngineSnapshot) -> Result<()> {
        self.table_metadata = snapshot.table_metadata;
        self.tree_store.restore_snapshot(snapshot.tree_store)
    }

    /// Discards a snapshot, keeping the work done since it was taken.
    pub(crate) fn release_snapshot(&mut self, snapshot: CatalogEngineSnapshot) -> Result<()> {
        self.tree_store.release_snapshot(snapshot.tree_store)
    }

    fn restore_runtime_metadata(&mut self, table_metadata: Arc<TableMetadataMap>) {
        self.table_metadata = table_metadata;
    }

    fn rollback_failed_atomic_operation(
        &mut self,
        table_metadata: Arc<TableMetadataMap>,
    ) -> Result<()> {
        self.restore_runtime_metadata(table_metadata);
        self.rollback_transaction()
    }

    fn run_atomically<T>(&mut self, operation: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        let transaction_active = self.transaction_active()?;

        if transaction_active {
            // The metadata pre-image lives in the snapshot on this path. It used to be
            // cloned a second time above, unconditionally, and then dropped unread —
            // a whole `HashMap<String, TableRuntimeMetadata>` per row, since
            // `run_atomically` wraps every single-row insert and delete.
            let snapshot = self.snapshot()?;
            match operation(self) {
                // Released, not dropped — see `Pager::release_savepoint`.
                Ok(result) => {
                    self.release_snapshot(snapshot)?;
                    Ok(result)
                }
                Err(err) => {
                    self.restore_snapshot(snapshot)?;
                    Err(err)
                }
            }
        } else {
            // No snapshot on this path: the transaction is ours, so rolling it back
            // undoes the storage side. Only the in-memory metadata needs a pre-image.
            let table_metadata = Arc::clone(&self.table_metadata);
            self.begin_transaction()?;
            match operation(self) {
                Ok(result) => match self.commit_transaction() {
                    Ok(()) => Ok(result),
                    Err(err) => {
                        self.rollback_failed_atomic_operation(table_metadata)?;
                        Err(err)
                    }
                },
                Err(err) => {
                    self.rollback_failed_atomic_operation(table_metadata)?;
                    Err(err)
                }
            }
        }
    }

    pub(crate) fn create_empty_btree(&self) -> Result<PageId> {
        self.tree_store().create_tree()
    }

    /// A shared handle to the runtime metadata, for callers that only read it.
    pub fn shared_table_metadata(&self) -> Arc<TableMetadataMap> {
        Arc::clone(&self.table_metadata)
    }

    pub(crate) fn get_table_metadata(&self) -> &TableMetadataMap {
        &self.table_metadata
    }

    pub(crate) fn load_schema(&self, schema_root: PageId) -> Result<Schema> {
        schema_store::load_schema(self, schema_root)
    }

    pub(crate) fn save_schema(&mut self, schema: &Schema, current_root: PageId) -> Result<PageId> {
        schema_store::save_schema(self, schema, current_root)
    }

    /// Borrows the tree store rather than cloning it.
    ///
    /// This returned a clone, and it is called from roughly forty places, several of
    /// them once per row. `ByteTreeStore` is a handle around an `Arc`, so each call
    /// was a refcount pair for something the caller only reads through.
    pub(crate) fn tree_store(&self) -> &ByteTreeStore {
        &self.tree_store
    }

    pub(crate) fn typed_tree_store<C: KeyValueCodec>(&self) -> TypedTreeStore<C> {
        TypedTreeStore::new(self.tree_store().clone())
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
        self.visit_tree_entries_while(root_page_id, |key, value| {
            visit(key, value)?;
            Ok(true)
        })
    }

    /// Walks tree entries until the visitor returns `false`.
    ///
    /// The early exit is what lets a capped scan stop after the rows it needs
    /// instead of walking the whole tree and discarding the tail.
    /// Walks a tree's values, without locating each entry's key.
    ///
    /// A projecting scan never reads the key, and finding it costs a second cell range
    /// and view per row.
    pub(crate) fn visit_tree_values_while<F>(
        &self,
        root_page_id: PageId,
        mut visit: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<bool>,
    {
        let tree = self.open_tree(root_page_id)?;
        let mut cursor = tree.cursor()?;
        cursor.first()?;
        while let Some(keep_going) = cursor.with_current_value(&mut visit)? {
            if !keep_going {
                break;
            }
            cursor.next()?;
        }
        Ok(())
    }

    pub(crate) fn visit_tree_entries_while<F>(
        &self,
        root_page_id: PageId,
        mut visit: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let tree = self.open_tree(root_page_id)?;
        let mut cursor = tree.cursor()?;
        cursor.first()?;
        // `with_current` rather than `current`: the visitor only borrows, and
        // `current` would allocate a key copy and a hydrated value per entry to
        // produce the same bytes.
        while let Some(keep_going) = cursor.with_current(&mut visit)? {
            if !keep_going {
                break;
            }
            cursor.next()?;
        }
        Ok(())
    }

    pub(crate) fn collect_tree_page_ids(&self, root_page_id: PageId) -> Result<Vec<PageId>> {
        self.tree_store().collect_page_ids(root_page_id)
    }

    pub(crate) fn collect_tree_space_stats(&self, root_page_id: PageId) -> Result<TreeSpaceStats> {
        self.tree_store().collect_space_stats(root_page_id)
    }

    pub(crate) fn pager_integrity_report(&mut self) -> Result<PagerIntegrityReport> {
        self.tree_store().validate_storage()
    }

    pub(crate) fn free_page_ids(&self) -> Result<Vec<PageId>> {
        self.tree_store().free_page_ids()
    }

    pub(crate) fn is_reserved_page(page_id: PageId) -> bool {
        page_id == ByteTreeStore::DB_HEADER_PAGE_ID
            || page_id == ByteTreeStore::RESERVED_METADATA_PAGE_ID
    }

    /// A token that changes whenever durable state does, so a caller can tell
    /// whether anything it derived from the file needs rebuilding.
    pub(crate) fn durable_state_token(&self) -> Result<u64> {
        self.tree_store().durable_state_token()
    }

    /// Live counters for the storage work this engine has caused.
    ///
    /// Distinct from [`Self::get_storage_stats`], which describes the file's shape.
    /// These are cumulative counts of syscalls and copies — what a benchmark or a
    /// profiling run wants to attribute, and what the storage layer's own tests
    /// assert on. Exposed so a workload outside the crate can do the same.
    pub fn storage_counters(&self) -> Result<crate::storage::StatsSnapshot> {
        self.tree_store().storage_stats()
    }

    pub fn get_storage_stats(&self) -> Result<CatalogStorageStats> {
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

    pub(crate) fn rename_table_runtime_metadata(
        &mut self,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        runtime_metadata::rename_table_metadata(self, old_name, new_name)
    }

    pub(crate) fn record_generated_row_insert(
        &mut self,
        table_name: &str,
        new_root_page_id: PageId,
        row_id: u64,
    ) {
        runtime_metadata::apply_insert(self, table_name, new_root_page_id, Some(row_id + 1));
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
        self.run_atomically(|engine| table_store::create_table(engine, table_name))
    }

    pub fn insert_into_table(&mut self, table_name: &str, row: Vec<Value>) -> Result<u64> {
        self.run_atomically(|engine| table_store::insert_into_table(engine, table_name, row))
    }

    pub fn replace_table_rows(&mut self, table_name: &str, rows: Vec<StoredRow>) -> Result<()> {
        self.run_atomically(|engine| table_store::replace_table_rows(engine, table_name, rows))
    }

    pub fn insert_row_with_rowid(&mut self, table_name: &str, row: StoredRow) -> Result<()> {
        self.run_atomically(|engine| table_store::insert_row_with_rowid(engine, table_name, row))
    }

    /// See [`table_store::insert_new_stored_row`].
    pub fn insert_new_row_with_rowid(
        &mut self,
        table_name: &str,
        row: StoredRow,
    ) -> Result<()> {
        self.run_atomically(|engine| table_store::insert_new_stored_row(engine, table_name, row))
    }

    pub fn delete_from_table_by_rowid(&mut self, table_name: &str, rowid: u64) -> Result<bool> {
        self.run_atomically(|engine| {
            table_store::delete_from_table_by_rowid(engine, table_name, rowid)
        })
    }

    pub fn drop_table(&mut self, table_name: &str) -> Result<()> {
        self.run_atomically(|engine| table_store::drop_table(engine, table_name))
    }

    pub fn drop_table_with_indexes(&mut self, table: &Table) -> Result<()> {
        self.run_atomically(|engine| index_store::drop_table_with_indexes(engine, table))
    }

    pub fn open_table_cursor(&mut self, table_name: &str) -> Result<TableCursor> {
        table_store::open_table_cursor(self, table_name)
    }

    pub fn read_rows_with_ids(&mut self, table_name: &str) -> Result<Vec<StoredRow>> {
        table_store::read_rows_with_ids(self, table_name)
    }

    /// Streams a table's rows instead of materialising them.
    ///
    /// Prefer this wherever the caller filters, counts, or stops early: it decodes
    /// one row at a time rather than building a `Vec` of the whole table first.
    pub fn visit_table_rows<F>(&mut self, table_name: &str, visit: F) -> Result<()>
    where
        F: FnMut(StoredRow) -> Result<()>,
    {
        table_store::visit_rows(self, table_name, visit)
    }

    /// Streams a table's row ids without decoding any records.
    pub fn visit_table_row_ids<F>(&mut self, table_name: &str, visit: F) -> Result<()>
    where
        F: FnMut(u64) -> Result<()>,
    {
        table_store::visit_row_ids(self, table_name, visit)
    }

    pub fn read_from_table(&mut self, table_name: &str) -> Result<Vec<Vec<Value>>> {
        table_store::read_from_table(self, table_name)
    }

    /// Reads at most `max_rows` rows, stopping the scan once it has them.
    ///
    /// The caller is responsible for establishing that a prefix of the table is a
    /// correct answer; see `SelectExecutor::scan_row_cap`.
    /// Reads a table decoding only the given column indices, in that order.
    pub fn read_table_columns(
        &mut self,
        table_name: &str,
        indices: &[usize],
        max_rows: Option<usize>,
    ) -> Result<Vec<Value>> {
        table_store::read_table_columns(self, table_name, indices, max_rows)
    }

    pub fn read_from_table_capped(
        &mut self,
        table_name: &str,
        max_rows: usize,
    ) -> Result<Vec<Vec<Value>>> {
        table_store::read_from_table_capped(self, table_name, max_rows)
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

    /// Writes a row under the row id this table's shape requires, and returns it.
    ///
    /// For a table whose primary key is a single integer column, the key *is* the
    /// row id and the caller does not get to choose. Otherwise the id comes from
    /// the table's counter, as it always did.
    ///
    /// Prefer this over `insert_into_table` followed by `register_primary_key_row`:
    /// that pair lets a caller store a row under a counter id and then claim a
    /// different primary key for it, leaving a row nothing can look up. Three tests
    /// in this repository did exactly that before the two were joined here.
    pub fn insert_table_row(&mut self, table: &Table, values: Vec<Value>) -> Result<u64> {
        match self.aliased_rowid_for_row(table, &values)? {
            Some(row_id) => {
                self.insert_new_row_with_rowid(&table.name, StoredRow { row_id, values })?;
                Ok(row_id)
            }
            None => self.insert_into_table(&table.name, values),
        }
    }

    /// See [`index_store::aliased_rowid_for_row`].
    pub fn aliased_rowid_for_row(&self, table: &Table, values: &[Value]) -> Result<Option<u64>> {
        index_store::aliased_rowid_for_row(table, values)
    }

    pub fn register_primary_key_row(&mut self, table: &Table, row: StoredRow) -> Result<()> {
        self.run_atomically(|engine| index_store::register_primary_key_row(engine, table, row))
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
        self.run_atomically(|engine| index_store::register_secondary_index_row(engine, table, row))
    }

    pub fn rebuild_primary_key_index(&mut self, table: &Table, rows: &[StoredRow]) -> Result<()> {
        self.run_atomically(|engine| index_store::rebuild_primary_key_index(engine, table, rows))
    }

    pub fn rebuild_secondary_indexes(&mut self, table: &Table, rows: &[StoredRow]) -> Result<()> {
        self.run_atomically(|engine| index_store::rebuild_secondary_indexes(engine, table, rows))
    }

    pub fn delete_primary_key_row(&mut self, table: &Table, row: &StoredRow) -> Result<bool> {
        self.run_atomically(|engine| index_store::delete_primary_key_row(engine, table, row))
    }

    pub fn delete_secondary_index_row(&mut self, table: &Table, row: &StoredRow) -> Result<()> {
        self.run_atomically(|engine| index_store::delete_secondary_index_row(engine, table, row))
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
