//! Main storage engine implementation.
//!
//! M0 storage contract notes:
//! - This layer is the relational storage facade above page IO.
//! - On-disk metadata versioning is strict; older metadata formats are rejected.
//! - Table metadata currently tracks root page, row count, and next rowid.
//! - During migration to table B-tree storage, rowid remains the physical table key.
//! - The storage file is expected to evolve into a forest of B-trees (catalog/table/index).

use crate::btree::node::SearchResult;
use crate::btree::{BTreeKey, BTreeNode, BTreeValue, NodeType};
use crate::catalog::{Table, Value};
use crate::error::Result;
use crate::storage::free_list::FreeList;
use crate::storage::{
    cursor::TableCursor, pager::Pager, Page, PageId, StorageIntegrityReport, StoredRow,
    TableMetadata, DB_HEADER_PAGE_ID, STORAGE_METADATA_PAGE_ID,
};
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// Main storage engine interface
#[derive(Debug)]
pub struct StorageEngine {
    pager: Pager,
    table_metadata: HashMap<String, TableMetadata>,
    primary_key_indexes: HashMap<String, HashMap<Vec<u8>, StoredRow>>,
    secondary_indexes: HashMap<String, HashMap<String, HashMap<Vec<u8>, Vec<StoredRow>>>>,
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
                self.validate_table_btree_pages(&table_name, metadata.root_page_id)?;

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

    fn validate_table_btree_pages(
        &mut self,
        table_name: &str,
        root_page_id: PageId,
    ) -> Result<(Vec<PageId>, u64, u64)> {
        let mut visited = HashSet::new();
        let mut row_count = 0u64;
        let mut max_row_id = 0u64;
        self.walk_table_btree(
            root_page_id,
            table_name,
            &mut visited,
            &mut row_count,
            &mut max_row_id,
        )?;
        Ok((visited.into_iter().collect(), row_count, max_row_id))
    }

    fn walk_table_btree(
        &mut self,
        page_id: PageId,
        table_name: &str,
        visited: &mut HashSet<PageId>,
        row_count: &mut u64,
        max_row_id: &mut u64,
    ) -> Result<()> {
        if !visited.insert(page_id) {
            return Err(crate::error::HematiteError::CorruptedData(format!(
                "Cycle detected in B-tree for table '{}'",
                table_name
            )));
        }

        let page = self.read_page(page_id)?;
        let node = BTreeNode::from_page(page)?;

        match node.node_type {
            NodeType::Leaf => {
                for value in node.values {
                    let row = crate::storage::serialization::RowSerializer::deserialize_stored_row(
                        &value.data,
                    )?;
                    *row_count += 1;
                    *max_row_id = (*max_row_id).max(row.row_id);
                }
            }
            NodeType::Internal => {
                for child in node.children {
                    self.walk_table_btree(child, table_name, visited, row_count, max_row_id)?;
                }
            }
        }

        Ok(())
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
                primary_key_indexes: HashMap::new(),
                secondary_indexes: HashMap::new(),
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

        self.insert_into_table_btree(table_name, root_page_id, row_id, row)
    }

    fn insert_into_table_btree(
        &mut self,
        table_name: &str,
        root_page_id: PageId,
        row_id: u64,
        row: Vec<crate::catalog::Value>,
    ) -> Result<u64> {
        self.insert_btree_row_with_id(table_name, root_page_id, row_id, row, true)?;
        Ok(row_id)
    }

    fn insert_btree_row_with_id(
        &mut self,
        table_name: &str,
        root_page_id: PageId,
        row_id: u64,
        row: Vec<crate::catalog::Value>,
        advance_next_rowid: bool,
    ) -> Result<()> {
        let key = BTreeKey::new(row_id.to_be_bytes().to_vec());
        let mut encoded =
            crate::storage::serialization::RowSerializer::serialize_stored_row(&StoredRow {
                row_id,
                values: row,
            })?;
        encoded.drain(0..4); // B-tree value payload stores row_id+values without length prefix.
        let value = BTreeValue::new(encoded);

        let split_result = self.insert_btree_recursive(root_page_id, key, value)?;
        if let Some((split_key, split_page_id)) = split_result {
            let new_root_page_id = self.allocate_page()?;
            let mut new_root = BTreeNode::new_internal(new_root_page_id);
            new_root.keys.push(split_key);
            new_root.children.push(root_page_id);
            new_root.children.push(split_page_id);

            let mut new_root_page = Page::new(new_root_page_id);
            new_root.to_page(&mut new_root_page)?;
            self.write_page(new_root_page)?;

            if let Some(metadata) = self.table_metadata.get_mut(table_name) {
                metadata.root_page_id = new_root_page_id;
            }
        }

        if let Some(metadata) = self.table_metadata.get_mut(table_name) {
            metadata.row_count += 1;
            if advance_next_rowid {
                metadata.next_row_id += 1;
            }
        }

        Ok(())
    }

    fn insert_btree_recursive(
        &mut self,
        page_id: PageId,
        key: BTreeKey,
        value: BTreeValue,
    ) -> Result<Option<(BTreeKey, PageId)>> {
        let mut page = self.read_page(page_id)?;
        let mut node = BTreeNode::from_page(page.clone())?;

        match node.node_type {
            NodeType::Leaf => {
                if let Some(existing_index) = node.keys.iter().position(|k| k == &key) {
                    node.values[existing_index] = value;
                    node.to_page(&mut page)?;
                    self.write_page(page)?;
                    return Ok(None);
                }

                if node.keys.len() < crate::btree::node::MAX_KEYS
                    && node.can_insert_key_value(&key, &value)
                {
                    node.insert_leaf(key, value)?;
                    node.to_page(&mut page)?;
                    self.write_page(page)?;
                    Ok(None)
                } else {
                    let (new_key, new_page_id) = node.split_leaf(self, key, value)?;
                    Ok(Some((new_key, new_page_id)))
                }
            }
            NodeType::Internal => {
                let child_page_id = node.find_child(&key);
                let split_result = self.insert_btree_recursive(child_page_id, key, value)?;

                if let Some((split_key, split_page_id)) = split_result {
                    if node.keys.len() < crate::btree::node::MAX_KEYS
                        && node.can_insert_key_child(&split_key)
                    {
                        node.insert_internal(split_key, split_page_id)?;
                        node.to_page(&mut page)?;
                        self.write_page(page)?;
                        Ok(None)
                    } else {
                        let (new_key, new_page_id) =
                            node.split_internal(self, split_key, split_page_id)?;
                        Ok(Some((new_key, new_page_id)))
                    }
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub fn replace_table_rows(&mut self, table_name: &str, rows: Vec<StoredRow>) -> Result<()> {
        let root_page_id = {
            let metadata = self.lookup_table_metadata(table_name)?;
            metadata.root_page_id
        };

        let mut page_ids = Vec::new();
        self.collect_btree_page_ids(root_page_id, &mut page_ids)?;
        for page_id in page_ids {
            if page_id != root_page_id {
                self.deallocate_page(page_id)?;
            }
        }
        let mut root_page = Page::new(root_page_id);
        let root = BTreeNode::new_leaf(root_page_id);
        root.to_page(&mut root_page)?;
        self.write_page(root_page)?;

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

        self.delete_from_table_by_rowid_btree(table_name, root_page_id, rowid)
    }

    fn delete_from_table_by_rowid_btree(
        &mut self,
        table_name: &str,
        root_page_id: PageId,
        rowid: u64,
    ) -> Result<bool> {
        let key = BTreeKey::new(rowid.to_be_bytes().to_vec());
        let deleted = self.delete_btree_recursive(root_page_id, &key)?;
        if !deleted {
            return Ok(false);
        }
        if let Some(metadata) = self.table_metadata.get_mut(table_name) {
            metadata.row_count = metadata.row_count.saturating_sub(1);
        }
        Ok(true)
    }

    fn delete_btree_recursive(&mut self, page_id: PageId, key: &BTreeKey) -> Result<bool> {
        let mut page = self.read_page(page_id)?;
        let mut node = BTreeNode::from_page(page.clone())?;

        match node.node_type {
            NodeType::Leaf => {
                let deleted = node.delete_from_leaf(key)?.is_some();
                if deleted {
                    node.to_page(&mut page)?;
                    self.write_page(page)?;
                }
                Ok(deleted)
            }
            NodeType::Internal => {
                let child_page_id = node.find_child(key);
                let deleted = self.delete_btree_recursive(child_page_id, key)?;
                if deleted {
                    node.to_page(&mut page)?;
                    self.write_page(page)?;
                }
                Ok(deleted)
            }
        }
    }

    fn insert_stored_row(&mut self, table_name: &str, row: StoredRow) -> Result<()> {
        let root_page_id = {
            let metadata = self.lookup_table_metadata(table_name)?;
            metadata.root_page_id
        };

        self.insert_btree_row_with_id(table_name, root_page_id, row.row_id, row.values, false)
    }

    pub fn drop_table(&mut self, table_name: &str) -> Result<()> {
        let metadata = self.table_metadata.remove(table_name).ok_or_else(|| {
            crate::error::HematiteError::StorageError(format!(
                "Table '{}' does not exist",
                table_name
            ))
        })?;

        let mut page_ids = Vec::new();
        self.collect_btree_page_ids(metadata.root_page_id, &mut page_ids)?;
        for page_id in page_ids {
            self.deallocate_page(page_id)?;
        }

        self.primary_key_indexes.remove(table_name);
        self.secondary_indexes.remove(table_name);

        Ok(())
    }

    fn collect_btree_page_ids(&mut self, page_id: PageId, out: &mut Vec<PageId>) -> Result<()> {
        out.push(page_id);
        let page = self.read_page(page_id)?;
        let node = BTreeNode::from_page(page)?;
        if node.node_type == NodeType::Internal {
            for child_page_id in node.children {
                self.collect_btree_page_ids(child_page_id, out)?;
            }
        }
        Ok(())
    }

    pub fn open_table_cursor(&mut self, table_name: &str) -> Result<TableCursor> {
        let root_page_id = self.lookup_table_metadata(table_name)?.root_page_id;
        let rows = self.read_rows_with_ids_from_btree(root_page_id)?;
        Ok(TableCursor::new(rows))
    }

    fn read_rows_with_ids_from_btree(&mut self, root_page_id: PageId) -> Result<Vec<StoredRow>> {
        let mut rows = Vec::new();
        self.collect_rows_from_btree(root_page_id, &mut rows)?;
        rows.sort_unstable_by_key(|row| row.row_id);
        Ok(rows)
    }

    fn collect_rows_from_btree(&mut self, page_id: PageId, out: &mut Vec<StoredRow>) -> Result<()> {
        let page = self.read_page(page_id)?;
        let node = BTreeNode::from_page(page)?;

        match node.node_type {
            NodeType::Leaf => {
                for value in node.values {
                    let row = crate::storage::serialization::RowSerializer::deserialize_stored_row(
                        &value.data,
                    )?;
                    out.push(row);
                }
            }
            NodeType::Internal => {
                for child_page_id in node.children {
                    self.collect_rows_from_btree(child_page_id, out)?;
                }
            }
        }

        Ok(())
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
        self.lookup_row_by_rowid_btree(root_page_id, rowid)
    }

    fn lookup_row_by_rowid_btree(
        &mut self,
        root_page_id: PageId,
        rowid: u64,
    ) -> Result<Option<StoredRow>> {
        let key = BTreeKey::new(rowid.to_be_bytes().to_vec());
        let mut current_page_id = root_page_id;
        loop {
            let page = self.read_page(current_page_id)?;
            let node = BTreeNode::from_page(page)?;
            match node.search(&key) {
                SearchResult::Found(value) => {
                    let row = crate::storage::serialization::RowSerializer::deserialize_stored_row(
                        &value.data,
                    )?;
                    return Ok(Some(row));
                }
                SearchResult::NotFound(next_child) => {
                    if node.node_type == NodeType::Leaf {
                        return Ok(None);
                    }
                    current_page_id = next_child;
                }
            }
        }
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
        Ok(self
            .primary_key_indexes
            .get(&table.name)
            .and_then(|index| index.get(&key).cloned()))
    }

    pub fn register_primary_key_row(&mut self, table: &Table, row: StoredRow) -> Result<()> {
        let key = Self::encode_primary_key(&table.get_primary_key_values(&row.values)?)?;
        self.primary_key_indexes
            .entry(table.name.clone())
            .or_default()
            .insert(key, row);
        Ok(())
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
            .secondary_indexes
            .get(&table.name)
            .and_then(|table_indexes| table_indexes.get(index_name))
            .and_then(|index| index.get(&key))
            .cloned()
            .unwrap_or_default())
    }

    pub fn register_secondary_index_row(&mut self, table: &Table, row: StoredRow) -> Result<()> {
        if table.secondary_indexes.is_empty() {
            return Ok(());
        }

        let table_indexes = self
            .secondary_indexes
            .entry(table.name.clone())
            .or_default();
        for index in &table.secondary_indexes {
            let key_values = index
                .column_indices
                .iter()
                .map(|&column_index| row.values[column_index].clone())
                .collect::<Vec<_>>();
            let key = Self::encode_index_key(&key_values)?;
            table_indexes
                .entry(index.name.clone())
                .or_default()
                .entry(key)
                .or_default()
                .push(row.clone());
        }

        Ok(())
    }

    pub fn rebuild_primary_key_index(&mut self, table: &Table, rows: &[StoredRow]) -> Result<()> {
        let mut index = HashMap::new();
        for row in rows {
            let key = Self::encode_primary_key(&table.get_primary_key_values(&row.values)?)?;
            index.insert(key, row.clone());
        }
        self.primary_key_indexes.insert(table.name.clone(), index);
        Ok(())
    }

    pub fn rebuild_secondary_indexes(&mut self, table: &Table, rows: &[StoredRow]) -> Result<()> {
        let mut table_indexes: HashMap<String, HashMap<Vec<u8>, Vec<StoredRow>>> = HashMap::new();

        for index in &table.secondary_indexes {
            let mut entries: HashMap<Vec<u8>, Vec<StoredRow>> = HashMap::new();
            for row in rows {
                let key_values = index
                    .column_indices
                    .iter()
                    .map(|&column_index| row.values[column_index].clone())
                    .collect::<Vec<_>>();
                let key = Self::encode_index_key(&key_values)?;
                entries.entry(key).or_default().push(row.clone());
            }
            table_indexes.insert(index.name.clone(), entries);
        }

        self.secondary_indexes
            .insert(table.name.clone(), table_indexes);
        Ok(())
    }

    fn ensure_primary_key_index(&mut self, table: &Table) -> Result<()> {
        if self.primary_key_indexes.contains_key(&table.name) {
            return Ok(());
        }

        let rows = self.read_rows_with_ids(&table.name)?;
        self.rebuild_primary_key_index(table, &rows)
    }

    fn ensure_secondary_indexes(&mut self, table: &Table) -> Result<()> {
        if self.secondary_indexes.contains_key(&table.name) {
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
