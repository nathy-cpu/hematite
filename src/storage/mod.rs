//! Storage engine module for Hematite database

pub mod buffer_pool;
pub mod file_manager;
pub mod page_manager;

use crate::catalog::Value;
use crate::error::{HematiteError, Result};
use std::path::Path;

pub const PAGE_SIZE: usize = 4096; // 4KB pages

// Table storage constants
pub const TABLE_METADATA_PAGE_ID: PageId = PageId::new(0);
pub const MAX_ROWS_PER_PAGE: usize = 100; // Approximate, depends on row size

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId(u32);

impl PageId {
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }

    pub const fn invalid() -> Self {
        Self(u32::MAX)
    }
}

#[derive(Debug, Clone)]
pub struct Page {
    pub id: PageId,
    pub data: Vec<u8>,
}

// Table storage structures
#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub name: String,
    pub root_page_id: PageId,
    pub row_count: u64,
    pub next_row_id: u64,
}

#[derive(Debug, Clone)]
pub struct TablePageHeader {
    pub page_type: PageType,
    pub row_count: u32,
    pub next_page_id: PageId,
    pub prev_page_id: PageId,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageType {
    TableData,
    TableIndex,
    Free,
}

impl Page {
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            data: vec![0u8; PAGE_SIZE],
        }
    }

    pub fn from_bytes(id: PageId, data: Vec<u8>) -> Result<Self> {
        if data.len() != PAGE_SIZE {
            return Err(HematiteError::InvalidPage(id.as_u32()));
        }
        Ok(Self { id, data })
    }
}

/// Main storage engine interface
#[derive(Debug)]
pub struct StorageEngine {
    file_manager: file_manager::FileManager,
    buffer_pool: buffer_pool::BufferPool,
    // Table metadata storage
    table_metadata: std::collections::HashMap<String, TableMetadata>,
}

impl StorageEngine {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file_manager = file_manager::FileManager::new(path)?;
        let buffer_pool = buffer_pool::BufferPool::new(100); // 100 pages in memory

        Ok(Self {
            file_manager,
            buffer_pool,
            table_metadata: std::collections::HashMap::new(),
        })
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
        Ok(page_id)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.file_manager.flush()?;
        Ok(())
    }

    // Proper table operations using page-based storage
    pub fn create_table(&mut self, table_name: &str) -> Result<()> {
        if self.table_metadata.contains_key(table_name) {
            return Err(HematiteError::StorageError(format!(
                "Table '{}' already exists",
                table_name
            )));
        }

        // Allocate root page for the table
        let root_page_id = self.allocate_page()?;

        // Initialize table metadata
        let metadata = TableMetadata {
            name: table_name.to_string(),
            root_page_id,
            row_count: 0,
            next_row_id: 1,
        };

        // Initialize root page as empty table data page
        let mut root_page = Page::new(root_page_id);
        let header = TablePageHeader {
            page_type: PageType::TableData,
            row_count: 0,
            next_page_id: PageId::invalid(),
            prev_page_id: PageId::invalid(),
        };
        self.write_page_header(&mut root_page, &header)?;
        self.write_page(root_page)?;

        self.table_metadata.insert(table_name.to_string(), metadata);
        Ok(())
    }

    pub fn insert_into_table(&mut self, table_name: &str, row: Vec<Value>) -> Result<()> {
        let root_page_id = {
            let metadata = self.table_metadata.get(table_name).ok_or_else(|| {
                HematiteError::StorageError(format!("Table '{}' does not exist", table_name))
            })?;
            metadata.root_page_id
        };

        // For now, simple implementation: serialize row and write to root page
        // In a real implementation, this would use B-tree for efficient storage
        let mut page = self.read_page(root_page_id)?;
        let mut header = self.read_page_header(&page)?;

        // Serialize the row (simplified)
        let serialized_row = self.serialize_row(&row)?;

        // Find space in the page (simplified - just append)
        if header.row_count < MAX_ROWS_PER_PAGE as u32 {
            let offset = 64 + (header.row_count as usize * serialized_row.len()); // Header + existing rows
            if offset + serialized_row.len() <= PAGE_SIZE {
                page.data[offset..offset + serialized_row.len()].copy_from_slice(&serialized_row);
                header.row_count += 1;
                self.write_page_header(&mut page, &header)?;
                self.write_page(page)?;

                // Update metadata
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
        } else {
            Err(HematiteError::StorageError(
                "Page full - need page splitting".to_string(),
            ))
        }
    }

    pub fn read_from_table(&mut self, table_name: &str) -> Result<Vec<Vec<Value>>> {
        let metadata = self.table_metadata.get(table_name).ok_or_else(|| {
            HematiteError::StorageError(format!("Table '{}' does not exist", table_name))
        })?;

        let page = self.read_page(metadata.root_page_id)?;
        let header = self.read_page_header(&page)?;

        let mut rows = Vec::new();
        let mut offset = 64; // Start after header

        for _ in 0..header.row_count {
            // Read row length (simplified - fixed size for now)
            let row_length = self.read_row_length(&page.data[offset..offset + 4])?;
            offset += 4;

            if offset + row_length <= PAGE_SIZE {
                let row_data = &page.data[offset..offset + row_length];
                let row = self.deserialize_row(row_data)?;
                rows.push(row);
                offset += row_length;
            } else {
                break;
            }
        }

        Ok(rows)
    }

    pub fn table_exists(&self, table_name: &str) -> bool {
        self.table_metadata.contains_key(table_name)
    }

    // Helper methods for page operations
    fn write_page_header(&self, page: &mut Page, header: &TablePageHeader) -> Result<()> {
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

    fn read_page_header(&self, page: &Page) -> Result<TablePageHeader> {
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

    fn serialize_row(&self, row: &[Value]) -> Result<Vec<u8>> {
        let mut data = Vec::new();

        // Write row length (placeholder, will be updated)
        data.extend_from_slice(&[0u8; 4]);

        // Serialize each value
        for value in row {
            match value {
                Value::Integer(i) => {
                    data.push(1); // Type marker for Integer
                    data.extend_from_slice(&i.to_le_bytes());
                }
                Value::Text(s) => {
                    data.push(2); // Type marker for Text
                    let bytes = s.as_bytes();
                    data.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    data.extend_from_slice(bytes);
                }
                Value::Boolean(b) => {
                    data.push(3); // Type marker for Boolean
                    data.push(*b as u8);
                }
                Value::Float(f) => {
                    data.push(4); // Type marker for Float
                    data.extend_from_slice(&f.to_le_bytes());
                }
                Value::Null => {
                    data.push(5); // Type marker for Null
                }
            }
        }

        // Update row length
        let row_length = (data.len() - 4) as u32;
        data[0..4].copy_from_slice(&row_length.to_le_bytes());

        Ok(data)
    }

    fn deserialize_row(&self, data: &[u8]) -> Result<Vec<Value>> {
        let mut values = Vec::new();
        let mut offset = 0;

        while offset < data.len() {
            let type_marker = data[offset];
            offset += 1;

            let value = match type_marker {
                1 => {
                    // Integer
                    let bytes = [
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ];
                    offset += 4;
                    Value::Integer(i32::from_le_bytes(bytes))
                }
                2 => {
                    // Text
                    let len_bytes = [
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ];
                    offset += 4;
                    let len = u32::from_le_bytes(len_bytes) as usize;
                    let text = String::from_utf8(data[offset..offset + len].to_vec())
                        .map_err(|_| HematiteError::StorageError("Invalid UTF-8".to_string()))?;
                    offset += len;
                    Value::Text(text)
                }
                3 => {
                    // Boolean
                    let b = data[offset] != 0;
                    offset += 1;
                    Value::Boolean(b)
                }
                4 => {
                    // Float
                    let bytes = [
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                        data[offset + 4],
                        data[offset + 5],
                        data[offset + 6],
                        data[offset + 7],
                    ];
                    offset += 8;
                    Value::Float(f64::from_le_bytes(bytes))
                }
                5 => {
                    // Null
                    Value::Null
                }
                _ => {
                    return Err(HematiteError::StorageError(
                        "Invalid value type".to_string(),
                    ))
                }
            };

            values.push(value);
        }

        Ok(values)
    }

    fn read_row_length(&self, data: &[u8]) -> Result<usize> {
        if data.len() < 4 {
            return Err(HematiteError::StorageError("Invalid row data".to_string()));
        }
        Ok(u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize)
    }
}

/// High-level database interface
pub struct Database {
    storage: StorageEngine,
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let storage = StorageEngine::new(path)?;
        Ok(Self { storage })
    }

    pub fn close(&mut self) -> Result<()> {
        self.storage.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Page, PageId};

    #[test]
    fn test_page_creation() {
        let page_id = PageId::new(1);
        let page = Page::new(page_id);

        assert_eq!(page.id, page_id);
        assert_eq!(page.data.len(), PAGE_SIZE);
        assert!(page.data.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_page_id() {
        let page_id = PageId::new(42);
        assert_eq!(page_id.as_u32(), 42);

        let invalid = PageId::invalid();
        assert_eq!(invalid.as_u32(), u32::MAX);
    }

    #[test]
    fn test_page_from_bytes() {
        let page_id = PageId::new(1);
        let data = vec![1u8; PAGE_SIZE];
        let page = Page::from_bytes(page_id, data.clone()).unwrap();

        assert_eq!(page.id, page_id);
        assert_eq!(page.data, data);
    }

    #[test]
    fn test_page_from_bytes_invalid_size() {
        let page_id = PageId::new(1);
        let data = vec![1u8; PAGE_SIZE - 1]; // Wrong size

        let result = Page::from_bytes(page_id, data);
        assert!(result.is_err());
    }
}
