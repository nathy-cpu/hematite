//! Storage engine module for Hematite database

pub mod buffer_pool;
pub mod file_manager;
pub mod page_manager;

use crate::error::{HematiteError, Result};
use std::path::Path;

pub const PAGE_SIZE: usize = 4096; // 4KB pages

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId(u32);

impl PageId {
    pub fn new(id: u32) -> Self {
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
}

impl StorageEngine {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file_manager = file_manager::FileManager::new(path)?;
        let buffer_pool = buffer_pool::BufferPool::new(100); // 100 pages in memory

        Ok(Self {
            file_manager,
            buffer_pool,
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
