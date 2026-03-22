//! Main storage engine implementation.
//!
//! M0 storage contract notes:
//! - This layer is the relational storage facade above page IO.
//! - On-disk metadata versioning is strict; older metadata formats are rejected.
//! - Tables are stored as rowid-keyed B-trees and the catalog persists root-page metadata.
//! - The storage file is organized as a forest of B-trees (catalog/table/index).

use crate::btree::BTreeNode;
use crate::catalog::{Table, Value};
use crate::error::Result;
use crate::storage::free_list::FreeList;
use crate::storage::index_cache::TransientIndexStore;
use crate::storage::{
    cursor::TableCursor, pager::Pager, table_btree, Page, PageId, StorageIntegrityReport,
    StoredRow, TableMetadata, DB_HEADER_PAGE_ID, STORAGE_METADATA_PAGE_ID,
};
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// Main storage engine interface
#[derive(Debug)]
pub struct StorageEngine {
    pager: Pager,
    table_metadata: HashMap<String, TableMetadata>,
    transient_indexes: TransientIndexStore,
}

impl StorageEngine {
    const STORAGE_METADATA_VERSION: u32 = 3;

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

        let freelist = FreeList::from_page_ids(self.pager.free_pages().to_vec());
        lines.extend(freelist.serialize_metadata_lines());

        let mut checksum_entries = self.pager.checksum_entries();
        checksum_entries.sort_by_key(|(page_id, _)| page_id.as_u32());
        lines.push(format!(
            "checksum_version={}",
            Pager::CHECKSUM_METADATA_VERSION
        ));
        lines.push(format!("checksum_count={}", checksum_entries.len()));
        for (page_id, checksum) in checksum_entries {
            lines.push(format!("checksum|{}|{}", page_id.as_u32(), checksum));
        }

        Ok(lines.join("\n"))
    }

    fn parse_storage_metadata(&mut self, metadata_str: &str) -> Result<()> {
        let mut lines = metadata_str.lines();
        let version_line = lines.next().ok_or_else(|| {
            crate::error::HematiteError::StorageError(
                "Missing storage metadata version".to_string(),
            )
        })?;
        let version = version_line
            .strip_prefix("version=")
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Storage metadata is missing version prefix".to_string(),
                )
            })?
            .parse::<u32>()
            .map_err(|_| {
                crate::error::HematiteError::StorageError(
                    "Invalid storage metadata version".to_string(),
                )
            })?;

        if version != Self::STORAGE_METADATA_VERSION {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Unsupported storage metadata version: expected {}, got {}",
                Self::STORAGE_METADATA_VERSION,
                version
            )));
        }

        let mut freelist_version = None;
        let mut freelist_count = None;
        let mut freelist_records = Vec::new();
        let mut checksum_version = None;
        let mut checksum_count = None;
        let mut checksum_records: Vec<(PageId, u32)> = Vec::new();

        for line in metadata_str.lines().skip(1) {
            if line.is_empty() || line.starts_with("table_count=") {
                continue;
            }

            if let Some(payload) = line.strip_prefix("table|") {
                let parts = payload.split('|').collect::<Vec<_>>();
                if parts.len() != 4 {
                    return Err(crate::error::HematiteError::StorageError(
                        "Invalid table metadata record".to_string(),
                    ));
                }

                let name = parts[0];
                let root_page_id = PageId::new(parts[1].parse::<u32>().map_err(|_| {
                    crate::error::HematiteError::StorageError(
                        "Invalid table root page metadata".to_string(),
                    )
                })?);
                let row_count = parts[2].parse::<u64>().map_err(|_| {
                    crate::error::HematiteError::StorageError(
                        "Invalid table row count metadata".to_string(),
                    )
                })?;
                let next_row_id = parts[3].parse::<u64>().map_err(|_| {
                    crate::error::HematiteError::StorageError(
                        "Invalid table next_row_id metadata".to_string(),
                    )
                })?;

                self.create_table_metadata(name, root_page_id)?;
                if let Some(metadata) = self.table_metadata.get_mut(name) {
                    metadata.row_count = row_count;
                    metadata.next_row_id = next_row_id;
                }
                continue;
            }

            if let Some(payload) = line.strip_prefix("freelist_version=") {
                let parsed = payload.parse::<u32>().map_err(|_| {
                    crate::error::HematiteError::StorageError(
                        "Invalid freelist metadata version".to_string(),
                    )
                })?;
                freelist_version = Some(parsed);
                continue;
            }

            if let Some(payload) = line.strip_prefix("freelist_count=") {
                let parsed = payload.parse::<usize>().map_err(|_| {
                    crate::error::HematiteError::StorageError(
                        "Invalid freelist metadata count".to_string(),
                    )
                })?;
                freelist_count = Some(parsed);
                continue;
            }

            if line.starts_with("freelist|") {
                freelist_records.push(line.to_string());
                continue;
            }

            if let Some(payload) = line.strip_prefix("checksum_version=") {
                let parsed = payload.parse::<u32>().map_err(|_| {
                    crate::error::HematiteError::StorageError(
                        "Invalid checksum metadata version".to_string(),
                    )
                })?;
                checksum_version = Some(parsed);
                continue;
            }

            if let Some(payload) = line.strip_prefix("checksum_count=") {
                let parsed = payload.parse::<usize>().map_err(|_| {
                    crate::error::HematiteError::StorageError(
                        "Invalid checksum metadata count".to_string(),
                    )
                })?;
                checksum_count = Some(parsed);
                continue;
            }

            if let Some(payload) = line.strip_prefix("checksum|") {
                let parts = payload.split('|').collect::<Vec<_>>();
                if parts.len() != 2 {
                    return Err(crate::error::HematiteError::StorageError(
                        "Invalid checksum metadata record".to_string(),
                    ));
                }
                let page_id = parts[0].parse::<u32>().map(PageId::new).map_err(|_| {
                    crate::error::HematiteError::StorageError(
                        "Invalid checksum page id metadata".to_string(),
                    )
                })?;
                let checksum = parts[1].parse::<u32>().map_err(|_| {
                    crate::error::HematiteError::StorageError(
                        "Invalid checksum value metadata".to_string(),
                    )
                })?;
                checksum_records.push((page_id, checksum));
                continue;
            }

            return Err(crate::error::HematiteError::StorageError(
                "Unknown storage metadata record".to_string(),
            ));
        }

        let freelist = FreeList::deserialize_metadata_lines(
            freelist_version.ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Missing freelist metadata version".to_string(),
                )
            })?,
            freelist_count.ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Missing freelist metadata count".to_string(),
                )
            })?,
            &freelist_records,
        )?;
        self.pager.set_free_pages(freelist.into_page_ids());

        let checksum_version = checksum_version.ok_or_else(|| {
            crate::error::HematiteError::StorageError(
                "Missing checksum metadata version".to_string(),
            )
        })?;
        if checksum_version != Pager::CHECKSUM_METADATA_VERSION {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Unsupported checksum metadata version: expected {}, got {}",
                Pager::CHECKSUM_METADATA_VERSION,
                checksum_version
            )));
        }

        let expected_checksum_count = checksum_count.ok_or_else(|| {
            crate::error::HematiteError::StorageError("Missing checksum metadata count".to_string())
        })?;
        if expected_checksum_count != checksum_records.len() {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Checksum metadata count mismatch: expected {}, got {}",
                expected_checksum_count,
                checksum_records.len()
            )));
        }

        let mut checksum_map = HashMap::new();
        for (page_id, checksum) in checksum_records {
            if checksum_map.insert(page_id, checksum).is_some() {
                return Err(crate::error::HematiteError::StorageError(format!(
                    "Duplicate checksum metadata entry for page {}",
                    page_id.as_u32()
                )));
            }
        }
        self.pager.replace_checksums(checksum_map);
        Ok(())
    }

    pub fn get_storage_stats(&self) -> crate::storage::StorageStats {
        crate::storage::StorageStats {
            table_count: self.table_metadata.len(),
            total_rows: self
                .table_metadata
                .values()
                .map(|metadata| metadata.row_count)
                .sum(),
            free_page_count: self.pager.free_pages().len(),
        }
    }

    fn create_table_metadata(&mut self, table_name: &str, root_page_id: PageId) -> Result<()> {
        if self.table_metadata.contains_key(table_name) {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Table '{}' already exists",
                table_name
            )));
        }

        self.table_metadata.insert(
            table_name.to_string(),
            TableMetadata {
                name: table_name.to_string(),
                root_page_id,
                row_count: 0,
                next_row_id: 1,
            },
        );
        Ok(())
    }

    fn lookup_table_metadata(&self, table_name: &str) -> Result<&TableMetadata> {
        self.table_metadata.get(table_name).ok_or_else(|| {
            crate::error::HematiteError::StorageError(format!(
                "Table '{}' does not exist",
                table_name
            ))
        })
    }

    pub fn validate_integrity(&mut self) -> Result<StorageIntegrityReport> {
        let pager_report = self.pager.validate_integrity()?;

        let metadata_entries = self
            .table_metadata
            .iter()
            .map(|(name, metadata)| (name.clone(), metadata.clone()))
            .collect::<Vec<_>>();

        let free_pages = self
            .pager
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
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Table '{}' has invalid root page {}",
                    table_name,
                    metadata.root_page_id.as_u32()
                )));
            }

            let (table_pages, counted_rows, max_row_id) =
                table_btree::validate_pages(self, &table_name, metadata.root_page_id)?;

            for page_id in table_pages {
                if free_pages.contains(&page_id) {
                    return Err(crate::error::HematiteError::CorruptedData(format!(
                        "Page {} for table '{}' is both live and free",
                        page_id.as_u32(),
                        table_name
                    )));
                }

                if !live_pages.insert(page_id) {
                    return Err(crate::error::HematiteError::CorruptedData(format!(
                        "Page {} is shared by multiple tables",
                        page_id.as_u32()
                    )));
                }
            }

            if counted_rows != metadata.row_count {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Table '{}' row count mismatch: metadata={}, actual={}",
                    table_name, metadata.row_count, counted_rows
                )));
            }

            if metadata.next_row_id <= max_row_id {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Table '{}' next_row_id {} is not ahead of max row_id {}",
                    table_name, metadata.next_row_id, max_row_id
                )));
            }

            let mut cursor = self.open_table_cursor(&table_name)?;
            let mut cursor_rows = 0u64;
            let mut previous_rowid: Option<u64> = None;
            if cursor.first() {
                loop {
                    let row = cursor.current().ok_or_else(|| {
                        crate::error::HematiteError::CorruptedData(format!(
                            "Cursor became invalid while scanning table '{}'",
                            table_name
                        ))
                    })?;
                    if let Some(prev) = previous_rowid {
                        if row.row_id <= prev {
                            return Err(crate::error::HematiteError::CorruptedData(format!(
                                "Cursor-visible rowid order violation for table '{}': {} then {}",
                                table_name, prev, row.row_id
                            )));
                        }
                    }
                    previous_rowid = Some(row.row_id);
                    cursor_rows += 1;
                    if !cursor.next() {
                        break;
                    }
                }
            }

            if cursor_rows != counted_rows {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Cursor-visible row count mismatch for table '{}': chain={}, cursor={}",
                    table_name, counted_rows, cursor_rows
                )));
            }

            total_rows += counted_rows;
        }

        Ok(StorageIntegrityReport {
            table_count: self.table_metadata.len(),
            live_page_count: live_pages.len(),
            free_page_count: pager_report.free_page_count,
            total_rows,
            pager: pager_report,
        })
    }

    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let pager = Pager::new(path, 100)?;
        Self::from_pager(pager)
    }

    pub fn new_in_memory() -> Result<Self> {
        let pager = Pager::new_in_memory(100)?;
        Self::from_pager(pager)
    }

    fn from_pager(pager: Pager) -> Result<Self> {
        // Load existing table metadata
        {
            let mut engine = Self {
                pager,
                table_metadata: HashMap::new(),
                transient_indexes: TransientIndexStore::default(),
            };
            engine.load_table_metadata()?;
            Ok(engine)
        }
    }

    pub fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        self.pager.read_page(page_id)
    }

    pub fn write_page(&mut self, page: Page) -> Result<()> {
        self.pager.write_page(page)
    }

    pub fn allocate_page(&mut self) -> Result<PageId> {
        let page_id = self.pager.allocate_page()?;

        // Never allocate reserved pages.
        if page_id == DB_HEADER_PAGE_ID || page_id == STORAGE_METADATA_PAGE_ID {
            return self.allocate_page(); // Recursive call to get next page
        }

        Ok(page_id)
    }

    pub fn deallocate_page(&mut self, page_id: PageId) -> Result<()> {
        // Never deallocate reserved pages.
        if page_id == DB_HEADER_PAGE_ID || page_id == STORAGE_METADATA_PAGE_ID {
            return Err(crate::error::HematiteError::StorageError(
                "Cannot deallocate database header page".to_string(),
            ));
        }

        self.pager.deallocate_page(page_id)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.save_table_metadata()?;
        self.pager.flush()
    }

    // Table metadata persistence
    fn load_table_metadata(&mut self) -> Result<()> {
        // Try to read table metadata from a special page (e.g., page 1)
        match self.pager.read_page(STORAGE_METADATA_PAGE_ID) {
            Ok(page) => {
                // Check if this page contains table metadata
                if page.data.len() >= 4 {
                    // First check if this might be a B-tree page by looking for magic number
                    if page.data.len() >= 9 && &page.data[0..4] == b"BTRE" {
                        // This is a B-tree page, not table metadata, skip it
                        return Ok(());
                    }

                    // Check if page is all zeros (newly allocated)
                    if page.data.iter().all(|&b| b == 0) {
                        // This is a fresh page, no metadata yet
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
                                crate::error::HematiteError::StorageError(
                                    "Invalid metadata encoding".to_string(),
                                )
                            })?;

                        self.parse_storage_metadata(&metadata_str)?;
                    }
                }
            }
            Err(_) => {
                // Page doesn't exist or can't be read, that's ok for new databases
            }
        }

        Ok(())
    }

    pub fn get_table_metadata(&self) -> &std::collections::HashMap<String, TableMetadata> {
        &self.table_metadata
    }

    fn save_table_metadata(&mut self) -> Result<()> {
        // Serialize table metadata
        let metadata_str = self.serialize_storage_metadata()?;
        let metadata_bytes = metadata_str.as_bytes();

        if metadata_bytes.len() > crate::storage::PAGE_SIZE - 4 {
            return Err(crate::error::HematiteError::StorageError(
                "Table metadata too large".to_string(),
            ));
        }

        // Create or update metadata page
        let mut page = Page::new(STORAGE_METADATA_PAGE_ID);

        // Write metadata size
        let size_bytes = (metadata_bytes.len() as u32).to_le_bytes();
        page.data[0..4].copy_from_slice(&size_bytes);

        // Write metadata data
        page.data[4..4 + metadata_bytes.len()].copy_from_slice(metadata_bytes);

        // Write page to disk
        self.pager.write_page(page)?;

        Ok(())
    }

    // Proper table operations using page-based storage
    pub fn create_table(&mut self, table_name: &str) -> Result<PageId> {
        let root_page_id = self.allocate_page()?;
        self.create_table_metadata(table_name, root_page_id)?;

        let mut root_page = Page::new(root_page_id);
        let root = BTreeNode::new_leaf(root_page_id);
        root.to_page(&mut root_page)?;
        self.write_page(root_page)?;

        Ok(root_page_id)
    }

    pub fn insert_into_table(
        &mut self,
        table_name: &str,
        row: Vec<crate::catalog::Value>,
    ) -> Result<u64> {
        let (root_page_id, row_id) = {
            let metadata = self.lookup_table_metadata(table_name)?;
            (metadata.root_page_id, metadata.next_row_id)
        };

        let new_root_page_id = table_btree::insert_row(self, root_page_id, row_id, row)?;

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
        let root_page_id = {
            let metadata = self.lookup_table_metadata(table_name)?;
            metadata.root_page_id
        };

        table_btree::reset_tree(self, root_page_id)?;

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
        let root_page_id = {
            let metadata = self.lookup_table_metadata(table_name)?;
            metadata.root_page_id
        };

        let deleted = table_btree::delete_row(self, root_page_id, rowid)?;
        if !deleted {
            return Ok(false);
        }
        if let Some(metadata) = self.table_metadata.get_mut(table_name) {
            metadata.row_count = metadata.row_count.saturating_sub(1);
        }
        Ok(true)
    }

    fn insert_stored_row(&mut self, table_name: &str, row: StoredRow) -> Result<()> {
        let root_page_id = {
            let metadata = self.lookup_table_metadata(table_name)?;
            metadata.root_page_id
        };

        let new_root_page_id = table_btree::insert_row(self, root_page_id, row.row_id, row.values)?;

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

    pub fn drop_table(&mut self, table_name: &str) -> Result<()> {
        let metadata = self.table_metadata.remove(table_name).ok_or_else(|| {
            crate::error::HematiteError::StorageError(format!(
                "Table '{}' does not exist",
                table_name
            ))
        })?;

        let mut page_ids = Vec::new();
        table_btree::collect_page_ids(self, metadata.root_page_id, &mut page_ids)?;
        for page_id in page_ids {
            self.deallocate_page(page_id)?;
        }

        self.transient_indexes.remove_table(table_name);

        Ok(())
    }

    pub fn open_table_cursor(&mut self, table_name: &str) -> Result<TableCursor> {
        let root_page_id = self.lookup_table_metadata(table_name)?.root_page_id;
        let rows = table_btree::read_rows(self, root_page_id)?;
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

    pub fn read_from_table(&mut self, table_name: &str) -> Result<Vec<Vec<crate::catalog::Value>>> {
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
        table_btree::lookup_row(self, root_page_id, rowid)
    }

    pub fn table_exists(&self, table_name: &str) -> bool {
        self.table_metadata.contains_key(table_name)
    }

    pub fn lookup_row_by_primary_key(
        &mut self,
        table: &Table,
        key_values: &[Value],
    ) -> Result<Option<StoredRow>> {
        self.ensure_primary_key_index(table)?;
        let key = Self::encode_primary_key(key_values)?;
        Ok(self.transient_indexes.lookup_primary_key(&table.name, &key))
    }

    pub fn register_primary_key_row(&mut self, table: &Table, row: StoredRow) -> Result<()> {
        self.transient_indexes
            .register_primary_key_row(table, row, Self::encode_primary_key)
    }

    pub fn lookup_rows_by_secondary_index(
        &mut self,
        table: &Table,
        index_name: &str,
        key_values: &[Value],
    ) -> Result<Vec<StoredRow>> {
        self.ensure_secondary_indexes(table)?;
        let key = Self::encode_index_key(key_values)?;
        Ok(self
            .transient_indexes
            .lookup_secondary_index(&table.name, index_name, &key))
    }

    pub fn register_secondary_index_row(&mut self, table: &Table, row: StoredRow) -> Result<()> {
        self.transient_indexes
            .register_secondary_index_row(table, row, Self::encode_index_key)
    }

    pub fn rebuild_primary_key_index(&mut self, table: &Table, rows: &[StoredRow]) -> Result<()> {
        self.transient_indexes
            .rebuild_primary_key_index(table, rows, Self::encode_primary_key)
    }

    pub fn rebuild_secondary_indexes(&mut self, table: &Table, rows: &[StoredRow]) -> Result<()> {
        self.transient_indexes
            .rebuild_secondary_indexes(table, rows, Self::encode_index_key)
    }

    fn ensure_primary_key_index(&mut self, table: &Table) -> Result<()> {
        if self.transient_indexes.has_primary_key_index(&table.name) {
            return Ok(());
        }

        let rows = self.read_rows_with_ids(&table.name)?;
        self.rebuild_primary_key_index(table, &rows)
    }

    fn ensure_secondary_indexes(&mut self, table: &Table) -> Result<()> {
        if self.transient_indexes.has_secondary_indexes(&table.name) {
            return Ok(());
        }

        let rows = self.read_rows_with_ids(&table.name)?;
        self.rebuild_secondary_indexes(table, &rows)
    }

    fn encode_primary_key(values: &[Value]) -> Result<Vec<u8>> {
        crate::storage::serialization::RowSerializer::serialize(values)
    }

    fn encode_index_key(values: &[Value]) -> Result<Vec<u8>> {
        crate::storage::serialization::RowSerializer::serialize(values)
    }

    pub fn create_empty_btree(&mut self) -> Result<PageId> {
        use crate::btree::BTreeNode;

        let root_page_id = self.allocate_page()?;
        let root_node = BTreeNode::new_leaf(root_page_id);

        // Create a fresh page and write the node to it
        let mut root_page = Page::new(root_page_id);
        BTreeNode::to_page(&root_node, &mut root_page)?;

        self.write_page(root_page)?;
        Ok(root_page_id)
    }
}
