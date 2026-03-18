//! Table operations and metadata management

use crate::catalog::Value;
use crate::error::{HematiteError, Result};
use crate::storage::serialization::RowSerializer;
use crate::storage::{
    Page, PageId, PageType, TableMetadata, TablePageHeader, MAX_ROWS_PER_PAGE, PAGE_SIZE,
    TABLE_PAGE_HEADER_SIZE,
};
use std::collections::HashMap;

// Trait for page operations to allow dependency injection
pub trait PageOperations {
    fn read_page(&mut self, page_id: PageId) -> Result<Page>;
    fn write_page(&mut self, page: Page) -> Result<()>;
}

pub struct TableManager {
    table_metadata: HashMap<String, TableMetadata>,
}

impl std::fmt::Debug for TableManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableManager")
            .field("table_count", &self.table_metadata.len())
            .finish()
    }
}

impl TableManager {
    fn row_data_end(&self, page: &Page, row_count: u32) -> Result<usize> {
        let mut offset = TABLE_PAGE_HEADER_SIZE;

        for _ in 0..row_count {
            if offset + 4 > PAGE_SIZE {
                return Err(HematiteError::CorruptedData(
                    "Row length exceeds page bounds".to_string(),
                ));
            }

            let row_length = RowSerializer::read_row_length(&page.data[offset..offset + 4])?;
            offset += 4 + row_length;

            if offset > PAGE_SIZE {
                return Err(HematiteError::CorruptedData(
                    "Row payload exceeds page bounds".to_string(),
                ));
            }
        }

        Ok(offset)
    }

    pub fn new() -> Self {
        Self {
            table_metadata: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, table_name: &str, root_page_id: PageId) -> Result<()> {
        if self.table_metadata.contains_key(table_name) {
            return Err(HematiteError::StorageError(format!(
                "Table '{}' already exists",
                table_name
            )));
        }

        let metadata = TableMetadata {
            name: table_name.to_string(),
            root_page_id,
            row_count: 0,
            next_row_id: 1,
        };

        self.table_metadata.insert(table_name.to_string(), metadata);
        Ok(())
    }

    pub fn table_exists(&self, table_name: &str) -> bool {
        self.table_metadata.contains_key(table_name)
    }

    pub fn get_table_metadata(&self, table_name: &str) -> Option<&TableMetadata> {
        self.table_metadata.get(table_name)
    }

    pub fn get_table_metadata_mut(&mut self, table_name: &str) -> Option<&mut TableMetadata> {
        self.table_metadata.get_mut(table_name)
    }

    pub fn get_all_metadata(&self) -> &HashMap<String, TableMetadata> {
        &self.table_metadata
    }

    // Helper methods for page operations
    pub fn write_page_header(&self, page: &mut Page, header: &TablePageHeader) -> Result<()> {
        let mut offset = 0;

        // Write page type
        let page_type_byte = match header.page_type {
            PageType::TableData => 1,
            PageType::TableIndex => 2,
            PageType::Free => 3,
        };
        page.data[offset] = page_type_byte;
        offset += 1;

        // Write row count
        page.data[offset..offset + 4].copy_from_slice(&header.row_count.to_le_bytes());
        offset += 4;

        // Write next page ID
        page.data[offset..offset + 4].copy_from_slice(&header.next_page_id.as_u32().to_le_bytes());
        offset += 4;

        // Write prev page ID
        page.data[offset..offset + 4].copy_from_slice(&header.prev_page_id.as_u32().to_le_bytes());

        Ok(())
    }

    pub fn read_page_header(&self, page: &Page) -> Result<TablePageHeader> {
        let mut offset = 0;

        // Read page type
        let page_type_byte = page.data[offset];
        offset += 1;
        let page_type = match page_type_byte {
            1 => PageType::TableData,
            2 => PageType::TableIndex,
            3 => PageType::Free,
            _ => return Err(HematiteError::StorageError("Invalid page type".to_string())),
        };

        // Read row count
        let row_count = u32::from_le_bytes([
            page.data[offset],
            page.data[offset + 1],
            page.data[offset + 2],
            page.data[offset + 3],
        ]);
        offset += 4;

        // Read next page ID
        let next_page_id = PageId::new(u32::from_le_bytes([
            page.data[offset],
            page.data[offset + 1],
            page.data[offset + 2],
            page.data[offset + 3],
        ]));
        offset += 4;

        // Read prev page ID
        let prev_page_id = PageId::new(u32::from_le_bytes([
            page.data[offset],
            page.data[offset + 1],
            page.data[offset + 2],
            page.data[offset + 3],
        ]));

        Ok(TablePageHeader {
            page_type,
            row_count,
            next_page_id,
            prev_page_id,
        })
    }

    pub fn insert_into_table(
        &mut self,
        table_name: &str,
        row: Vec<Value>,
        page_ops: &mut dyn PageOperations,
    ) -> Result<()> {
        let root_page_id = {
            let metadata = self.table_metadata.get(table_name).ok_or_else(|| {
                HematiteError::StorageError(format!("Table '{}' does not exist", table_name))
            })?;
            metadata.root_page_id
        };

        let serialized_row = RowSerializer::serialize(&row)?;
        if TABLE_PAGE_HEADER_SIZE + serialized_row.len() > PAGE_SIZE {
            return Err(HematiteError::StorageError(
                "Row too large to fit in a table page".to_string(),
            ));
        }

        let mut page = page_ops.read_page(root_page_id)?;
        let mut header = self.read_page_header(&page)?;
        let offset = self.row_data_end(&page, header.row_count)?;

        if header.row_count < MAX_ROWS_PER_PAGE as u32 && offset + serialized_row.len() <= PAGE_SIZE
        {
            page.data[offset..offset + serialized_row.len()].copy_from_slice(&serialized_row);
            header.row_count += 1;
            self.write_page_header(&mut page, &header)?;
            page_ops.write_page(page)?;

            if let Some(metadata) = self.table_metadata.get_mut(table_name) {
                metadata.row_count += 1;
                metadata.next_row_id += 1;
            }

            Ok(())
        } else {
            Err(HematiteError::StorageError(
                "Page full - need page splitting".to_string(),
            ))
        }
    }

    // Metadata persistence
    pub fn serialize_metadata(&self) -> Result<String> {
        let mut result = String::new();

        for (name, metadata) in &self.table_metadata {
            if !result.is_empty() {
                result.push(';');
            }
            result.push_str(&format!(
                "{}:{},{},{}",
                name,
                metadata.root_page_id.as_u32(),
                metadata.row_count,
                metadata.next_row_id
            ));
        }

        Ok(result)
    }

    pub fn parse_metadata(&mut self, metadata_str: &str) -> Result<()> {
        if metadata_str.is_empty() {
            return Ok(());
        }

        for entry in metadata_str.split(';') {
            if entry.is_empty() {
                continue;
            }

            let parts: Vec<&str> = entry.split(':').collect();
            if parts.len() != 2 {
                continue;
            }

            let table_name = parts[0];
            let values: Vec<&str> = parts[1].split(',').collect();
            if values.len() != 3 {
                continue;
            }

            let root_page_id = PageId::new(
                values[0]
                    .parse::<u32>()
                    .map_err(|_| HematiteError::StorageError("Invalid page ID".to_string()))?,
            );
            let row_count = values[1]
                .parse::<u64>()
                .map_err(|_| HematiteError::StorageError("Invalid row count".to_string()))?;
            let next_row_id = values[2]
                .parse::<u64>()
                .map_err(|_| HematiteError::StorageError("Invalid next row ID".to_string()))?;

            self.table_metadata.insert(
                table_name.to_string(),
                TableMetadata {
                    name: table_name.to_string(),
                    root_page_id,
                    row_count,
                    next_row_id,
                },
            );
        }

        Ok(())
    }
}
