//! Main storage engine implementation.
//!
//! M0 storage contract notes:
//! - This layer is the relational storage facade above page IO.
//! - On-disk metadata versioning is strict; older metadata formats are rejected.
//! - Table metadata currently tracks root page, row count, and next rowid.
//! - During migration to table B-tree storage, rowid remains the physical table key.
//! - The storage file is expected to evolve into a forest of B-trees (catalog/table/index).

use crate::catalog::{Table, Value};
use crate::error::Result;
use crate::storage::table::{PageOperations, TableManager};
use crate::storage::{
    buffer_pool::BufferPool, file_manager::FileManager, Page, PageId, StorageIntegrityReport,
    StoredRow, TableMetadata, DB_HEADER_PAGE_ID, STORAGE_METADATA_PAGE_ID, TABLE_PAGE_HEADER_SIZE,
};
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// Main storage engine interface
#[derive(Debug)]
pub struct StorageEngine {
    file_manager: FileManager,
    buffer_pool: BufferPool,
    table_manager: TableManager,
    primary_key_indexes: HashMap<String, HashMap<Vec<u8>, StoredRow>>,
    secondary_indexes: HashMap<String, HashMap<String, HashMap<Vec<u8>, Vec<StoredRow>>>>,
}

impl StorageEngine {
    const STORAGE_METADATA_VERSION: u32 = 3;

    fn serialize_storage_metadata(&self) -> Result<String> {
        let mut lines = vec![
            format!("version={}", Self::STORAGE_METADATA_VERSION),
            format!(
                "table_count={}",
                self.table_manager.get_all_metadata().len()
            ),
            format!("free_page_count={}", self.file_manager.free_pages().len()),
        ];

        let mut table_entries = self
            .table_manager
            .get_all_metadata()
            .values()
            .cloned()
            .collect::<Vec<_>>();
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

        let mut free_pages = self.file_manager.free_pages().to_vec();
        free_pages.sort_by_key(|page_id| page_id.as_u32());
        for page_id in free_pages {
            lines.push(format!("free|{}", page_id.as_u32()));
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

        let mut free_pages = Vec::new();

        for line in metadata_str.lines().skip(1) {
            if line.is_empty()
                || line.starts_with("table_count=")
                || line.starts_with("free_page_count=")
            {
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

                self.table_manager.create_table(name, root_page_id)?;
                if let Some(metadata) = self.table_manager.get_table_metadata_mut(name) {
                    metadata.row_count = row_count;
                    metadata.next_row_id = next_row_id;
                }
                continue;
            }

            if let Some(payload) = line.strip_prefix("free|") {
                let page_id = payload.parse::<u32>().map(PageId::new).map_err(|_| {
                    crate::error::HematiteError::StorageError(
                        "Invalid free page metadata".to_string(),
                    )
                })?;
                free_pages.push(page_id);
                continue;
            }

            return Err(crate::error::HematiteError::StorageError(
                "Unknown storage metadata record".to_string(),
            ));
        }

        self.file_manager.set_free_pages(free_pages);
        Ok(())
    }

    pub fn get_storage_stats(&self) -> crate::storage::StorageStats {
        crate::storage::StorageStats {
            table_count: self.table_manager.get_all_metadata().len(),
            total_rows: self
                .table_manager
                .get_all_metadata()
                .values()
                .map(|metadata| metadata.row_count)
                .sum(),
            free_page_count: self.file_manager.free_pages().len(),
        }
    }

    pub fn validate_integrity(&mut self) -> Result<StorageIntegrityReport> {
        let metadata_entries = self
            .table_manager
            .get_all_metadata()
            .iter()
            .map(|(name, metadata)| (name.clone(), metadata.clone()))
            .collect::<Vec<_>>();

        let mut free_pages = HashSet::new();
        for &page_id in self.file_manager.free_pages() {
            if page_id == DB_HEADER_PAGE_ID || page_id == STORAGE_METADATA_PAGE_ID {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Reserved page {} cannot be marked free",
                    page_id.as_u32()
                )));
            }

            if !free_pages.insert(page_id) {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Duplicate free page {} detected",
                    page_id.as_u32()
                )));
            }
        }

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
                self.validate_table_page_chain(&table_name, &metadata)?;

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

            total_rows += counted_rows;
        }

        Ok(StorageIntegrityReport {
            table_count: self.table_manager.get_all_metadata().len(),
            live_page_count: live_pages.len(),
            free_page_count: free_pages.len(),
            total_rows,
        })
    }

    fn validate_table_page_chain(
        &mut self,
        table_name: &str,
        metadata: &TableMetadata,
    ) -> Result<(Vec<PageId>, u64, u64)> {
        let mut current_page_id = metadata.root_page_id;
        let mut previous_page_id = PageId::invalid();
        let mut visited = HashSet::new();
        let mut pages = Vec::new();
        let mut row_count = 0u64;
        let mut max_row_id = 0u64;

        loop {
            if !visited.insert(current_page_id) {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Cycle detected in page chain for table '{}'",
                    table_name
                )));
            }

            let page = self.read_page(current_page_id)?;
            let header = self.table_manager.read_page_header(&page)?;
            if header.page_type != crate::storage::PageType::TableData {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Table '{}' references non-table-data page {}",
                    table_name,
                    current_page_id.as_u32()
                )));
            }

            if header.prev_page_id != previous_page_id {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Broken prev_page_id chain for table '{}' at page {}",
                    table_name,
                    current_page_id.as_u32()
                )));
            }

            let mut offset = TABLE_PAGE_HEADER_SIZE;
            for _ in 0..header.row_count {
                if offset + 4 > crate::storage::PAGE_SIZE {
                    return Err(crate::error::HematiteError::CorruptedData(format!(
                        "Row length exceeds page bounds for table '{}' on page {}",
                        table_name,
                        current_page_id.as_u32()
                    )));
                }

                let row_length = crate::storage::serialization::RowSerializer::read_row_length(
                    &page.data[offset..offset + 4],
                )?;
                offset += 4;

                if offset + row_length > crate::storage::PAGE_SIZE {
                    return Err(crate::error::HematiteError::CorruptedData(format!(
                        "Row payload exceeds page bounds for table '{}' on page {}",
                        table_name,
                        current_page_id.as_u32()
                    )));
                }

                let row = crate::storage::serialization::RowSerializer::deserialize_stored_row(
                    &page.data[offset..offset + row_length],
                )?;
                max_row_id = max_row_id.max(row.row_id);
                row_count += 1;
                offset += row_length;
            }

            pages.push(current_page_id);

            if header.next_page_id == PageId::invalid() {
                break;
            }

            if header.next_page_id == DB_HEADER_PAGE_ID
                || header.next_page_id == STORAGE_METADATA_PAGE_ID
            {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Table '{}' points at reserved page {}",
                    table_name,
                    header.next_page_id.as_u32()
                )));
            }

            previous_page_id = current_page_id;
            current_page_id = header.next_page_id;
        }

        Ok((pages, row_count, max_row_id))
    }

    fn row_data_end(page: &Page, row_count: u32) -> Result<usize> {
        let mut offset = TABLE_PAGE_HEADER_SIZE;

        for _ in 0..row_count {
            if offset + 4 > crate::storage::PAGE_SIZE {
                return Err(crate::error::HematiteError::CorruptedData(
                    "Row length exceeds page bounds".to_string(),
                ));
            }

            let row_length = crate::storage::serialization::RowSerializer::read_row_length(
                &page.data[offset..offset + 4],
            )?;
            offset += 4 + row_length;

            if offset > crate::storage::PAGE_SIZE {
                return Err(crate::error::HematiteError::CorruptedData(
                    "Row payload exceeds page bounds".to_string(),
                ));
            }
        }

        Ok(offset)
    }

    fn initialize_table_page(
        &self,
        page_id: PageId,
        prev_page_id: PageId,
        next_page_id: PageId,
    ) -> Result<Page> {
        let mut page = Page::new(page_id);
        let header = crate::storage::TablePageHeader {
            page_type: crate::storage::PageType::TableData,
            row_count: 0,
            next_page_id,
            prev_page_id,
        };
        self.table_manager.write_page_header(&mut page, &header)?;
        Ok(page)
    }

    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file_manager = FileManager::new(path)?;
        Self::from_file_manager(file_manager)
    }

    pub fn new_in_memory() -> Result<Self> {
        let file_manager = FileManager::new_in_memory()?;
        Self::from_file_manager(file_manager)
    }

    fn from_file_manager(file_manager: FileManager) -> Result<Self> {
        let buffer_pool = BufferPool::new(100); // 100 pages in memory
        let table_manager = TableManager::new();

        // Load existing table metadata
        {
            let mut engine = Self {
                file_manager,
                buffer_pool,
                table_manager,
                primary_key_indexes: HashMap::new(),
                secondary_indexes: HashMap::new(),
            };
            engine.load_table_metadata()?;
            Ok(engine)
        }
    }

    pub fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        // Try to get from buffer pool first
        let page = if let Some(page) = self.buffer_pool.get(page_id) {
            page.clone()
        } else {
            // Read from file
            let page = self.file_manager.read_page(page_id)?;
            // Cache in buffer pool
            self.buffer_pool.put(page.clone());
            page
        };
        Ok(page)
    }

    pub fn write_page(&mut self, page: Page) -> Result<()> {
        // Write to file
        self.file_manager.write_page(&page)?;

        // Update buffer pool
        self.buffer_pool.put(page);

        Ok(())
    }

    pub fn allocate_page(&mut self) -> Result<PageId> {
        let page_id = self.file_manager.allocate_page()?;

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

        // Remove from buffer pool
        self.buffer_pool.remove(page_id);
        // Mark as free in file manager
        self.file_manager.deallocate_page(page_id)?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.save_table_metadata()?;
        self.file_manager.flush()?;
        Ok(())
    }

    // Table metadata persistence
    fn load_table_metadata(&mut self) -> Result<()> {
        // Try to read table metadata from a special page (e.g., page 1)
        match self.file_manager.read_page(STORAGE_METADATA_PAGE_ID) {
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
        self.table_manager.get_all_metadata()
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
        self.file_manager.write_page(&page)?;

        Ok(())
    }

    // Proper table operations using page-based storage
    pub fn create_table(&mut self, table_name: &str) -> Result<PageId> {
        // Allocate root page for the table
        let root_page_id = self.allocate_page()?;

        // Initialize table metadata
        self.table_manager.create_table(table_name, root_page_id)?;

        // Initialize root page as empty table data page
        let root_page =
            self.initialize_table_page(root_page_id, PageId::invalid(), PageId::invalid())?;
        self.write_page(root_page)?;

        Ok(root_page_id)
    }

    pub fn insert_into_table(
        &mut self,
        table_name: &str,
        row: Vec<crate::catalog::Value>,
    ) -> Result<u64> {
        let (root_page_id, row_id) = {
            let metadata = self
                .table_manager
                .get_table_metadata(table_name)
                .ok_or_else(|| {
                    crate::error::HematiteError::StorageError(format!(
                        "Table '{}' does not exist",
                        table_name
                    ))
                })?;
            (metadata.root_page_id, metadata.next_row_id)
        };

        let serialized_row =
            crate::storage::serialization::RowSerializer::serialize_stored_row(&StoredRow {
                row_id,
                values: row,
            })?;
        if TABLE_PAGE_HEADER_SIZE + serialized_row.len() > crate::storage::PAGE_SIZE {
            return Err(crate::error::HematiteError::StorageError(
                "Row too large to fit in a table page".to_string(),
            ));
        }

        let mut current_page_id = root_page_id;

        loop {
            let mut page = self.read_page(current_page_id)?;
            let mut header = self.table_manager.read_page_header(&page)?;
            let offset = Self::row_data_end(&page, header.row_count)?;

            if header.row_count < crate::storage::MAX_ROWS_PER_PAGE as u32
                && offset + serialized_row.len() <= crate::storage::PAGE_SIZE
            {
                page.data[offset..offset + serialized_row.len()].copy_from_slice(&serialized_row);
                header.row_count += 1;
                self.table_manager.write_page_header(&mut page, &header)?;
                self.write_page(page)?;

                // Update metadata
                if let Some(metadata) = self.table_manager.get_table_metadata_mut(table_name) {
                    metadata.row_count += 1;
                    metadata.next_row_id += 1;
                }

                return Ok(row_id);
            }

            if header.next_page_id != PageId::invalid() {
                current_page_id = header.next_page_id;
                continue;
            }

            let new_page_id = self.allocate_page()?;
            let mut new_page =
                self.initialize_table_page(new_page_id, current_page_id, PageId::invalid())?;

            header.next_page_id = new_page_id;
            self.table_manager.write_page_header(&mut page, &header)?;
            self.write_page(page)?;

            new_page.data[TABLE_PAGE_HEADER_SIZE..TABLE_PAGE_HEADER_SIZE + serialized_row.len()]
                .copy_from_slice(&serialized_row);
            let mut new_header = self.table_manager.read_page_header(&new_page)?;
            new_header.row_count = 1;
            self.table_manager
                .write_page_header(&mut new_page, &new_header)?;
            self.write_page(new_page)?;

            if let Some(metadata) = self.table_manager.get_table_metadata_mut(table_name) {
                metadata.row_count += 1;
                metadata.next_row_id += 1;
            }

            return Ok(row_id);
        }
    }

    pub fn replace_table_rows(&mut self, table_name: &str, rows: Vec<StoredRow>) -> Result<()> {
        let root_page_id = {
            let metadata = self
                .table_manager
                .get_table_metadata(table_name)
                .ok_or_else(|| {
                    crate::error::HematiteError::StorageError(format!(
                        "Table '{}' does not exist",
                        table_name
                    ))
                })?;
            metadata.root_page_id
        };

        let mut page_ids = vec![root_page_id];
        let mut current_page_id = root_page_id;
        loop {
            let page = self.read_page(current_page_id)?;
            let header = self.table_manager.read_page_header(&page)?;
            if header.next_page_id == PageId::invalid() {
                break;
            }
            current_page_id = header.next_page_id;
            page_ids.push(current_page_id);
        }

        let root_page =
            self.initialize_table_page(root_page_id, PageId::invalid(), PageId::invalid())?;
        self.write_page(root_page)?;

        for page_id in page_ids.into_iter().skip(1) {
            self.deallocate_page(page_id)?;
        }

        let next_row_id = self
            .table_manager
            .get_table_metadata(table_name)
            .map(|metadata| metadata.next_row_id)
            .unwrap_or(1);

        if let Some(metadata) = self.table_manager.get_table_metadata_mut(table_name) {
            metadata.row_count = 0;
            metadata.next_row_id =
                next_row_id.max(rows.iter().map(|row| row.row_id).max().unwrap_or(0) + 1);
        }

        for row in rows {
            self.insert_stored_row(table_name, row)?;
        }

        Ok(())
    }

    fn insert_stored_row(&mut self, table_name: &str, row: StoredRow) -> Result<()> {
        let root_page_id = {
            let metadata = self
                .table_manager
                .get_table_metadata(table_name)
                .ok_or_else(|| {
                    crate::error::HematiteError::StorageError(format!(
                        "Table '{}' does not exist",
                        table_name
                    ))
                })?;
            metadata.root_page_id
        };

        let serialized_row =
            crate::storage::serialization::RowSerializer::serialize_stored_row(&row)?;
        if TABLE_PAGE_HEADER_SIZE + serialized_row.len() > crate::storage::PAGE_SIZE {
            return Err(crate::error::HematiteError::StorageError(
                "Row too large to fit in a table page".to_string(),
            ));
        }

        let mut current_page_id = root_page_id;

        loop {
            let mut page = self.read_page(current_page_id)?;
            let mut header = self.table_manager.read_page_header(&page)?;
            let offset = Self::row_data_end(&page, header.row_count)?;

            if header.row_count < crate::storage::MAX_ROWS_PER_PAGE as u32
                && offset + serialized_row.len() <= crate::storage::PAGE_SIZE
            {
                page.data[offset..offset + serialized_row.len()].copy_from_slice(&serialized_row);
                header.row_count += 1;
                self.table_manager.write_page_header(&mut page, &header)?;
                self.write_page(page)?;

                if let Some(metadata) = self.table_manager.get_table_metadata_mut(table_name) {
                    metadata.row_count += 1;
                }

                return Ok(());
            }

            if header.next_page_id != PageId::invalid() {
                current_page_id = header.next_page_id;
                continue;
            }

            let new_page_id = self.allocate_page()?;
            let mut new_page =
                self.initialize_table_page(new_page_id, current_page_id, PageId::invalid())?;

            header.next_page_id = new_page_id;
            self.table_manager.write_page_header(&mut page, &header)?;
            self.write_page(page)?;

            new_page.data[TABLE_PAGE_HEADER_SIZE..TABLE_PAGE_HEADER_SIZE + serialized_row.len()]
                .copy_from_slice(&serialized_row);
            let mut new_header = self.table_manager.read_page_header(&new_page)?;
            new_header.row_count = 1;
            self.table_manager
                .write_page_header(&mut new_page, &new_header)?;
            self.write_page(new_page)?;

            if let Some(metadata) = self.table_manager.get_table_metadata_mut(table_name) {
                metadata.row_count += 1;
            }

            return Ok(());
        }
    }

    pub fn drop_table(&mut self, table_name: &str) -> Result<()> {
        let metadata = self.table_manager.remove_table(table_name).ok_or_else(|| {
            crate::error::HematiteError::StorageError(format!(
                "Table '{}' does not exist",
                table_name
            ))
        })?;

        let mut current_page_id = metadata.root_page_id;
        loop {
            let page = self.read_page(current_page_id)?;
            let header = self.table_manager.read_page_header(&page)?;
            let next_page_id = header.next_page_id;
            self.deallocate_page(current_page_id)?;

            if next_page_id == PageId::invalid() {
                break;
            }

            current_page_id = next_page_id;
        }

        self.primary_key_indexes.remove(table_name);
        self.secondary_indexes.remove(table_name);

        Ok(())
    }

    pub fn read_rows_with_ids(&mut self, table_name: &str) -> Result<Vec<StoredRow>> {
        let metadata = self
            .table_manager
            .get_table_metadata(table_name)
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(format!(
                    "Table '{}' does not exist",
                    table_name
                ))
            })?;

        let mut rows = Vec::new();
        let mut current_page_id = metadata.root_page_id;

        loop {
            let page = self.read_page(current_page_id)?;
            let header = self.table_manager.read_page_header(&page)?;
            let mut offset = TABLE_PAGE_HEADER_SIZE;

            for _ in 0..header.row_count {
                if offset + 4 > crate::storage::PAGE_SIZE {
                    return Err(crate::error::HematiteError::CorruptedData(
                        "Row length exceeds page bounds".to_string(),
                    ));
                }

                let row_length = crate::storage::serialization::RowSerializer::read_row_length(
                    &page.data[offset..offset + 4],
                )?;
                offset += 4;

                if offset + row_length > crate::storage::PAGE_SIZE {
                    return Err(crate::error::HematiteError::CorruptedData(
                        "Row payload exceeds page bounds".to_string(),
                    ));
                }

                let row_data = &page.data[offset..offset + row_length];
                let row =
                    crate::storage::serialization::RowSerializer::deserialize_stored_row(row_data)?;
                rows.push(row);
                offset += row_length;
            }

            if header.next_page_id == PageId::invalid() {
                break;
            }

            current_page_id = header.next_page_id;
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

    pub fn table_exists(&self, table_name: &str) -> bool {
        self.table_manager.table_exists(table_name)
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

    // Helper methods for page operations
    pub fn write_page_header(
        &self,
        page: &mut Page,
        header: &crate::storage::TablePageHeader,
    ) -> Result<()> {
        self.table_manager.write_page_header(page, header)
    }

    pub fn read_page_header(&self, page: &Page) -> Result<crate::storage::TablePageHeader> {
        self.table_manager.read_page_header(page)
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

// Implement PageOperations trait for StorageEngine
impl PageOperations for StorageEngine {
    fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        self.read_page(page_id)
    }

    fn write_page(&mut self, page: Page) -> Result<()> {
        self.write_page(page)
    }
}
