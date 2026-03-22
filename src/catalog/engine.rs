//! Relational storage engine built on top of the pager and generic B-trees.

use crate::btree::BTreeNode;
use crate::catalog::{Table, Value};
use crate::error::{HematiteError, Result};
use crate::storage::{
    Page, PageId, Pager, PagerIntegrityReport, DB_HEADER_PAGE_ID, STORAGE_METADATA_PAGE_ID,
};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};

use super::cursor::{IndexCursor, TableCursor};
use super::{index_btree, table_btree};

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
    pub free_page_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogIntegrityReport {
    pub table_count: usize,
    pub live_page_count: usize,
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
    pager: Arc<Mutex<Pager>>,
    table_metadata: HashMap<String, TableRuntimeMetadata>,
}

impl CatalogEngine {
    const STORAGE_METADATA_VERSION: u32 = 3;

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
        engine.load_table_metadata()?;
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
        self.save_table_metadata()?;
        self.pager.lock().unwrap().flush()
    }

    pub fn begin_transaction(&mut self) -> Result<()> {
        self.pager.lock().unwrap().begin_transaction()
    }

    pub fn commit_transaction(&mut self) -> Result<()> {
        self.save_table_metadata()?;
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
        let root_page_id = self.allocate_page()?;
        let root_node = BTreeNode::new_leaf(root_page_id);
        let mut root_page = Page::new(root_page_id);
        BTreeNode::to_page(&root_node, &mut root_page)?;
        self.write_page(root_page)?;
        Ok(root_page_id)
    }

    pub fn get_table_metadata(&self) -> &HashMap<String, TableRuntimeMetadata> {
        &self.table_metadata
    }

    pub fn get_storage_stats(&self) -> CatalogStorageStats {
        let free_page_count = self.pager.lock().unwrap().free_pages().len();
        CatalogStorageStats {
            table_count: self.table_metadata.len(),
            total_rows: self.table_metadata.values().map(|m| m.row_count).sum(),
            free_page_count,
        }
    }

    pub fn create_table(&mut self, table_name: &str) -> Result<PageId> {
        let root_page_id = self.allocate_page()?;
        self.create_table_metadata(table_name, root_page_id)?;

        let mut root_page = Page::new(root_page_id);
        let root = BTreeNode::new_leaf(root_page_id);
        root.to_page(&mut root_page)?;
        self.write_page(root_page)?;
        Ok(root_page_id)
    }

    pub fn insert_into_table(&mut self, table_name: &str, row: Vec<Value>) -> Result<u64> {
        let (root_page_id, row_id) = {
            let metadata = self.lookup_table_metadata(table_name)?;
            (metadata.root_page_id, metadata.next_row_id)
        };

        let new_root_page_id = {
            let mut pager = self.pager.lock().unwrap();
            table_btree::insert_row(&mut pager, root_page_id, row_id, row)?
        };

        if let Some(new_root_page_id) = new_root_page_id {
            if let Some(metadata) = self.table_metadata.get_mut(table_name) {
                metadata.root_page_id = new_root_page_id;
            }
        }
        if let Some(metadata) = self.table_metadata.get_mut(table_name) {
            metadata.row_count += 1;
            metadata.next_row_id += 1;
        }
        Ok(row_id)
    }

    pub fn replace_table_rows(&mut self, table_name: &str, rows: Vec<StoredRow>) -> Result<()> {
        let root_page_id = self.lookup_table_metadata(table_name)?.root_page_id;
        {
            let mut pager = self.pager.lock().unwrap();
            table_btree::reset_tree(&mut pager, root_page_id)?;
        }

        let next_row_id = self
            .table_metadata
            .get(table_name)
            .map(|metadata| metadata.next_row_id)
            .unwrap_or(1);

        if let Some(metadata) = self.table_metadata.get_mut(table_name) {
            metadata.row_count = 0;
            metadata.next_row_id =
                next_row_id.max(rows.iter().map(|row| row.row_id).max().unwrap_or(0) + 1);
        }

        for row in rows {
            self.insert_stored_row(table_name, row)?;
        }

        Ok(())
    }

    pub fn delete_from_table_by_rowid(&mut self, table_name: &str, rowid: u64) -> Result<bool> {
        let root_page_id = self.lookup_table_metadata(table_name)?.root_page_id;
        let deleted = {
            let mut pager = self.pager.lock().unwrap();
            table_btree::delete_row(&mut pager, root_page_id, rowid)?
        };
        if deleted {
            if let Some(metadata) = self.table_metadata.get_mut(table_name) {
                metadata.row_count = metadata.row_count.saturating_sub(1);
            }
        }
        Ok(deleted)
    }

    pub fn drop_table(&mut self, table_name: &str) -> Result<()> {
        let metadata = self.table_metadata.remove(table_name).ok_or_else(|| {
            HematiteError::StorageError(format!("Table '{}' does not exist", table_name))
        })?;

        let mut page_ids = Vec::new();
        {
            let mut pager = self.pager.lock().unwrap();
            table_btree::collect_page_ids(&mut pager, metadata.root_page_id, &mut page_ids)?;
            for page_id in page_ids {
                pager.deallocate_page(page_id)?;
            }
        }

        Ok(())
    }

    pub fn drop_table_with_indexes(&mut self, table: &Table) -> Result<()> {
        self.drop_table(&table.name)?;
        let mut pager = self.pager.lock().unwrap();

        if table.primary_key_index_root_page_id.as_u32() != 0 {
            let mut page_ids = Vec::new();
            index_btree::collect_page_ids(
                &mut pager,
                table.primary_key_index_root_page_id,
                &mut page_ids,
            )?;
            for page_id in page_ids {
                pager.deallocate_page(page_id)?;
            }
        }

        for index in &table.secondary_indexes {
            if index.root_page_id.as_u32() == 0 {
                continue;
            }
            let mut page_ids = Vec::new();
            index_btree::collect_page_ids(&mut pager, index.root_page_id, &mut page_ids)?;
            for page_id in page_ids {
                pager.deallocate_page(page_id)?;
            }
        }

        Ok(())
    }

    pub fn open_table_cursor(&mut self, table_name: &str) -> Result<TableCursor> {
        let root_page_id = self.lookup_table_metadata(table_name)?.root_page_id;
        let rows = {
            let mut pager = self.pager.lock().unwrap();
            table_btree::read_rows(&mut pager, root_page_id)?
        };
        Ok(TableCursor::new(rows))
    }

    pub fn read_rows_with_ids(&mut self, table_name: &str) -> Result<Vec<StoredRow>> {
        let mut cursor = self.open_table_cursor(table_name)?;
        let mut rows = Vec::new();
        if cursor.first() {
            loop {
                if let Some(row) = cursor.current() {
                    rows.push(row.clone());
                }
                if !cursor.next() {
                    break;
                }
            }
        }
        Ok(rows)
    }

    pub fn read_from_table(&mut self, table_name: &str) -> Result<Vec<Vec<Value>>> {
        Ok(self
            .read_rows_with_ids(table_name)?
            .into_iter()
            .map(|row| row.values)
            .collect())
    }

    pub fn lookup_row_by_rowid(
        &mut self,
        table_name: &str,
        rowid: u64,
    ) -> Result<Option<StoredRow>> {
        let root_page_id = self.lookup_table_metadata(table_name)?.root_page_id;
        let mut pager = self.pager.lock().unwrap();
        table_btree::lookup_row(&mut pager, root_page_id, rowid)
    }

    pub fn lookup_row_by_primary_key(
        &mut self,
        table: &Table,
        key_values: &[Value],
    ) -> Result<Option<StoredRow>> {
        let rowid = self.lookup_primary_key_rowid(table, key_values)?;
        match rowid {
            Some(rowid) => self.lookup_row_by_rowid(&table.name, rowid),
            None => Ok(None),
        }
    }

    pub fn lookup_primary_key_rowid(
        &mut self,
        table: &Table,
        key_values: &[Value],
    ) -> Result<Option<u64>> {
        let root_page_id = self.require_index_root_page(
            table.primary_key_index_root_page_id,
            &format!("primary-key index for table '{}'", table.name),
        )?;
        let mut pager = self.pager.lock().unwrap();
        index_btree::lookup_primary_key(&mut pager, root_page_id, key_values)
    }

    pub fn register_primary_key_row(&mut self, table: &Table, row: StoredRow) -> Result<()> {
        let root_page_id = self.require_index_root_page(
            table.primary_key_index_root_page_id,
            &format!("primary-key index for table '{}'", table.name),
        )?;
        let key_values = table.get_primary_key_values(&row.values)?;
        let mut pager = self.pager.lock().unwrap();
        if index_btree::lookup_primary_key(&mut pager, root_page_id, &key_values)?.is_some() {
            return Err(HematiteError::StorageError(format!(
                "Duplicate primary key for table '{}'",
                table.name
            )));
        }
        index_btree::insert_primary_key(&mut pager, root_page_id, &key_values, row.row_id)?;
        Ok(())
    }

    pub fn lookup_rows_by_secondary_index(
        &mut self,
        table: &Table,
        index_name: &str,
        key_values: &[Value],
    ) -> Result<Vec<StoredRow>> {
        let rowids = self.lookup_secondary_index_rowids(table, index_name, key_values)?;
        let mut rows = Vec::with_capacity(rowids.len());
        for rowid in rowids {
            if let Some(row) = self.lookup_row_by_rowid(&table.name, rowid)? {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    pub fn lookup_secondary_index_rowids(
        &mut self,
        table: &Table,
        index_name: &str,
        key_values: &[Value],
    ) -> Result<Vec<u64>> {
        let index = table.get_secondary_index(index_name).ok_or_else(|| {
            HematiteError::StorageError(format!(
                "Secondary index '{}' does not exist on table '{}'",
                index_name, table.name
            ))
        })?;
        let root_page_id = self.require_index_root_page(
            index.root_page_id,
            &format!("secondary index '{}' on table '{}'", index.name, table.name),
        )?;
        let rowids = {
            let mut pager = self.pager.lock().unwrap();
            index_btree::lookup_secondary_rowids(&mut pager, root_page_id, key_values)?
        };
        Ok(rowids)
    }

    pub fn register_secondary_index_row(&mut self, table: &Table, row: StoredRow) -> Result<()> {
        let mut pager = self.pager.lock().unwrap();
        for index in &table.secondary_indexes {
            let root_page_id = self.require_index_root_page(
                index.root_page_id,
                &format!("secondary index '{}' on table '{}'", index.name, table.name),
            )?;
            let key_values = index
                .column_indices
                .iter()
                .map(|&column_index| row.values[column_index].clone())
                .collect::<Vec<_>>();
            index_btree::insert_secondary_key(&mut pager, root_page_id, &key_values, row.row_id)?;
        }
        Ok(())
    }

    pub fn rebuild_primary_key_index(&mut self, table: &Table, rows: &[StoredRow]) -> Result<()> {
        let root_page_id = self.require_index_root_page(
            table.primary_key_index_root_page_id,
            &format!("primary-key index for table '{}'", table.name),
        )?;
        let mut pager = self.pager.lock().unwrap();
        index_btree::reset_tree(&mut pager, root_page_id)?;
        let mut seen = HashSet::new();
        for row in rows {
            let key_values = table.get_primary_key_values(&row.values)?;
            let encoded = index_btree::encode_index_key(&key_values)?;
            if !seen.insert(encoded) {
                return Err(HematiteError::StorageError(format!(
                    "Duplicate primary key encountered while rebuilding table '{}'",
                    table.name
                )));
            }
            index_btree::insert_primary_key(&mut pager, root_page_id, &key_values, row.row_id)?;
        }
        Ok(())
    }

    pub fn rebuild_secondary_indexes(&mut self, table: &Table, rows: &[StoredRow]) -> Result<()> {
        let mut pager = self.pager.lock().unwrap();
        for index in &table.secondary_indexes {
            let root_page_id = self.require_index_root_page(
                index.root_page_id,
                &format!("secondary index '{}' on table '{}'", index.name, table.name),
            )?;
            index_btree::reset_tree(&mut pager, root_page_id)?;
            for row in rows {
                let key_values = index
                    .column_indices
                    .iter()
                    .map(|&column_index| row.values[column_index].clone())
                    .collect::<Vec<_>>();
                index_btree::insert_secondary_key(
                    &mut pager,
                    root_page_id,
                    &key_values,
                    row.row_id,
                )?;
            }
        }
        Ok(())
    }

    pub fn delete_primary_key_row(&mut self, table: &Table, row: &StoredRow) -> Result<bool> {
        let root_page_id = self.require_index_root_page(
            table.primary_key_index_root_page_id,
            &format!("primary-key index for table '{}'", table.name),
        )?;
        let key_values = table.get_primary_key_values(&row.values)?;
        let mut pager = self.pager.lock().unwrap();
        index_btree::delete_primary_key(&mut pager, root_page_id, &key_values)
    }

    pub fn delete_secondary_index_row(&mut self, table: &Table, row: &StoredRow) -> Result<()> {
        let mut pager = self.pager.lock().unwrap();
        for index in &table.secondary_indexes {
            let key_values = index
                .column_indices
                .iter()
                .map(|&column_index| row.values[column_index].clone())
                .collect::<Vec<_>>();
            index_btree::delete_secondary_key(
                &mut pager,
                index.root_page_id,
                &key_values,
                row.row_id,
            )?;
        }
        Ok(())
    }

    pub fn encode_primary_key(&self, key_values: &[Value]) -> Result<Vec<u8>> {
        index_btree::encode_index_key(key_values)
    }

    pub fn encode_secondary_index_key(&self, key_values: &[Value]) -> Result<Vec<u8>> {
        index_btree::encode_index_key(key_values)
    }

    pub fn open_primary_key_cursor(&mut self, table: &Table) -> Result<IndexCursor> {
        let root_page_id = self.require_index_root_page(
            table.primary_key_index_root_page_id,
            &format!("primary-key index for table '{}'", table.name),
        )?;
        let entries = {
            let mut pager = self.pager.lock().unwrap();
            index_btree::read_primary_entries(&mut pager, root_page_id)?
        };
        Ok(IndexCursor::new(entries))
    }

    pub fn open_secondary_index_cursor(
        &mut self,
        table: &Table,
        index_name: &str,
    ) -> Result<IndexCursor> {
        let index = table.get_secondary_index(index_name).ok_or_else(|| {
            HematiteError::StorageError(format!(
                "Secondary index '{}' does not exist on table '{}'",
                index_name, table.name
            ))
        })?;
        let root_page_id = self.require_index_root_page(
            index.root_page_id,
            &format!("secondary index '{}' on table '{}'", index.name, table.name),
        )?;
        let entries = {
            let mut pager = self.pager.lock().unwrap();
            index_btree::read_secondary_entries(&mut pager, root_page_id)?
        };
        Ok(IndexCursor::new(entries))
    }

    pub fn validate_table_indexes(&mut self, table: &Table) -> Result<()> {
        let rows = self.read_rows_with_ids(&table.name)?;
        for row in &rows {
            let key_values = table.get_primary_key_values(&row.values)?;
            let stored_rowid = {
                let mut pager = self.pager.lock().unwrap();
                index_btree::lookup_primary_key(
                    &mut pager,
                    table.primary_key_index_root_page_id,
                    &key_values,
                )?
            }
            .ok_or_else(|| {
                HematiteError::CorruptedData(format!(
                    "Primary-key index is missing a row for table '{}'",
                    table.name
                ))
            })?;

            if stored_rowid != row.row_id {
                return Err(HematiteError::CorruptedData(format!(
                    "Primary-key index rowid mismatch for table '{}': expected {}, got {}",
                    table.name, row.row_id, stored_rowid
                )));
            }
        }

        for index in &table.secondary_indexes {
            for row in &rows {
                let key_values = index
                    .column_indices
                    .iter()
                    .map(|&column_index| row.values[column_index].clone())
                    .collect::<Vec<_>>();
                let rowids = {
                    let mut pager = self.pager.lock().unwrap();
                    index_btree::lookup_secondary_rowids(
                        &mut pager,
                        index.root_page_id,
                        &key_values,
                    )?
                };
                if !rowids.contains(&row.row_id) {
                    return Err(HematiteError::CorruptedData(format!(
                        "Secondary index '{}' is missing rowid {} for table '{}'",
                        index.name, row.row_id, table.name
                    )));
                }
            }
        }

        Ok(())
    }

    pub fn validate_integrity(&mut self) -> Result<CatalogIntegrityReport> {
        let pager_report = self.pager.lock().unwrap().validate_integrity()?;
        let metadata_entries = self
            .table_metadata
            .iter()
            .map(|(name, metadata)| (name.clone(), metadata.clone()))
            .collect::<Vec<_>>();

        let free_pages = self
            .pager
            .lock()
            .unwrap()
            .free_pages()
            .iter()
            .copied()
            .collect::<HashSet<_>>();

        let mut live_pages = HashSet::new();
        let mut total_rows = 0u64;

        for (table_name, metadata) in metadata_entries {
            if metadata.root_page_id == PageId::invalid()
                || metadata.root_page_id == DB_HEADER_PAGE_ID
                || metadata.root_page_id == STORAGE_METADATA_PAGE_ID
            {
                return Err(HematiteError::CorruptedData(format!(
                    "Table '{}' has invalid root page {}",
                    table_name,
                    metadata.root_page_id.as_u32()
                )));
            }

            let (table_pages, counted_rows, max_row_id) = {
                let mut pager = self.pager.lock().unwrap();
                table_btree::validate_pages(&mut pager, &table_name, metadata.root_page_id)?
            };

            for page_id in table_pages {
                if free_pages.contains(&page_id) {
                    return Err(HematiteError::CorruptedData(format!(
                        "Page {} for table '{}' is both live and free",
                        page_id.as_u32(),
                        table_name
                    )));
                }
                if !live_pages.insert(page_id) {
                    return Err(HematiteError::CorruptedData(format!(
                        "Page {} is shared by multiple tables",
                        page_id.as_u32()
                    )));
                }
            }

            if counted_rows != metadata.row_count {
                return Err(HematiteError::CorruptedData(format!(
                    "Table '{}' row count mismatch: metadata={}, actual={}",
                    table_name, metadata.row_count, counted_rows
                )));
            }

            if metadata.next_row_id <= max_row_id {
                return Err(HematiteError::CorruptedData(format!(
                    "Table '{}' next_row_id {} is not ahead of max row_id {}",
                    table_name, metadata.next_row_id, max_row_id
                )));
            }

            total_rows += counted_rows;
        }

        Ok(CatalogIntegrityReport {
            table_count: self.table_metadata.len(),
            live_page_count: live_pages.len(),
            free_page_count: pager_report.free_page_count,
            total_rows,
            pager: pager_report,
        })
    }

    fn create_table_metadata(&mut self, table_name: &str, root_page_id: PageId) -> Result<()> {
        if self.table_metadata.contains_key(table_name) {
            return Err(HematiteError::StorageError(format!(
                "Table '{}' already exists",
                table_name
            )));
        }

        self.table_metadata.insert(
            table_name.to_string(),
            TableRuntimeMetadata {
                name: table_name.to_string(),
                root_page_id,
                row_count: 0,
                next_row_id: 1,
            },
        );
        Ok(())
    }

    fn lookup_table_metadata(&self, table_name: &str) -> Result<&TableRuntimeMetadata> {
        self.table_metadata.get(table_name).ok_or_else(|| {
            HematiteError::StorageError(format!("Table '{}' does not exist", table_name))
        })
    }

    fn insert_stored_row(&mut self, table_name: &str, row: StoredRow) -> Result<()> {
        let root_page_id = self.lookup_table_metadata(table_name)?.root_page_id;
        let new_root_page_id = {
            let mut pager = self.pager.lock().unwrap();
            table_btree::insert_row(&mut pager, root_page_id, row.row_id, row.values)?
        };

        if let Some(new_root_page_id) = new_root_page_id {
            if let Some(metadata) = self.table_metadata.get_mut(table_name) {
                metadata.root_page_id = new_root_page_id;
            }
        }
        if let Some(metadata) = self.table_metadata.get_mut(table_name) {
            metadata.row_count += 1;
        }
        Ok(())
    }

    fn require_index_root_page(&self, root_page_id: PageId, label: &str) -> Result<PageId> {
        if root_page_id == PageId::new(0) || root_page_id == PageId::invalid() {
            return Err(HematiteError::StorageError(format!(
                "Missing durable {} root page",
                label
            )));
        }
        Ok(root_page_id)
    }

    fn serialize_storage_metadata(&self) -> Result<String> {
        let mut lines = vec![
            format!("version={}", Self::STORAGE_METADATA_VERSION),
            format!("table_count={}", self.table_metadata.len()),
        ];

        let mut table_entries = self.table_metadata.values().cloned().collect::<Vec<_>>();
        table_entries.sort_by(|left, right| left.name.cmp(&right.name));

        for table in table_entries {
            lines.push(format!(
                "table|{}|{}|{}|{}",
                table.name,
                table.root_page_id.as_u32(),
                table.row_count,
                table.next_row_id
            ));
        }

        Ok(lines.join("\n"))
    }

    fn parse_storage_metadata(&mut self, metadata_str: &str) -> Result<()> {
        let mut lines = metadata_str.lines();
        let version_line = lines.next().ok_or_else(|| {
            HematiteError::StorageError("Missing storage metadata version".to_string())
        })?;
        let version = version_line
            .strip_prefix("version=")
            .ok_or_else(|| {
                HematiteError::StorageError(
                    "Storage metadata is missing version prefix".to_string(),
                )
            })?
            .parse::<u32>()
            .map_err(|_| {
                HematiteError::StorageError("Invalid storage metadata version".to_string())
            })?;

        if version != Self::STORAGE_METADATA_VERSION {
            return Err(HematiteError::StorageError(format!(
                "Unsupported storage metadata version: expected {}, got {}",
                Self::STORAGE_METADATA_VERSION,
                version
            )));
        }

        for line in metadata_str.lines().skip(1) {
            if line.is_empty() || line.starts_with("table_count=") {
                continue;
            }
            if let Some(payload) = line.strip_prefix("table|") {
                let parts = payload.split('|').collect::<Vec<_>>();
                if parts.len() != 4 {
                    return Err(HematiteError::StorageError(
                        "Invalid table metadata record".to_string(),
                    ));
                }
                let name = parts[0];
                let root_page_id = PageId::new(parts[1].parse::<u32>().map_err(|_| {
                    HematiteError::StorageError("Invalid table root page metadata".to_string())
                })?);
                let row_count = parts[2].parse::<u64>().map_err(|_| {
                    HematiteError::StorageError("Invalid table row count metadata".to_string())
                })?;
                let next_row_id = parts[3].parse::<u64>().map_err(|_| {
                    HematiteError::StorageError("Invalid table next_row_id metadata".to_string())
                })?;

                self.create_table_metadata(name, root_page_id)?;
                if let Some(metadata) = self.table_metadata.get_mut(name) {
                    metadata.row_count = row_count;
                    metadata.next_row_id = next_row_id;
                }
                continue;
            }
            return Err(HematiteError::StorageError(
                "Unknown storage metadata record".to_string(),
            ));
        }
        Ok(())
    }

    fn load_table_metadata(&mut self) -> Result<()> {
        let maybe_page = {
            self.pager
                .lock()
                .unwrap()
                .read_page(STORAGE_METADATA_PAGE_ID)
        };
        match maybe_page {
            Ok(page) => {
                if page.data.len() >= 4 {
                    if page.data.len() >= 9 && &page.data[0..4] == b"BTRE" {
                        return Ok(());
                    }
                    if page.data.iter().all(|&b| b == 0) {
                        return Ok(());
                    }
                    let metadata_size = u32::from_le_bytes([
                        page.data[0],
                        page.data[1],
                        page.data[2],
                        page.data[3],
                    ]) as usize;

                    if metadata_size > 0 && metadata_size + 4 <= crate::storage::PAGE_SIZE {
                        let metadata_bytes = &page.data[4..4 + metadata_size];
                        let metadata_str =
                            String::from_utf8(metadata_bytes.to_vec()).map_err(|_| {
                                HematiteError::StorageError("Invalid metadata encoding".to_string())
                            })?;
                        self.parse_storage_metadata(&metadata_str)?;
                    }
                }
            }
            Err(_) => {}
        }

        Ok(())
    }

    fn save_table_metadata(&mut self) -> Result<()> {
        let metadata_str = self.serialize_storage_metadata()?;
        let metadata_bytes = metadata_str.as_bytes();

        if metadata_bytes.len() > crate::storage::PAGE_SIZE - 4 {
            return Err(HematiteError::StorageError(
                "Table metadata too large".to_string(),
            ));
        }

        let mut page = Page::new(STORAGE_METADATA_PAGE_ID);
        page.data[0..4].copy_from_slice(&(metadata_bytes.len() as u32).to_le_bytes());
        page.data[4..4 + metadata_bytes.len()].copy_from_slice(metadata_bytes);
        self.pager.lock().unwrap().write_page(page)?;
        Ok(())
    }
}
