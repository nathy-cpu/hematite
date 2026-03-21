//! Pager abstraction over page IO, cache, and allocation.
//!
//! M1.1 contract:
//! - All page reads/writes for the storage engine should flow through this type.
//! - Buffer pool behavior and file manager behavior are composed here.
//! - Allocation/deallocation remains file-manager-backed for now and is evolved in later M1 tasks.

use crate::error::Result;
use crate::storage::{buffer_pool::BufferPool, file_manager::FileManager, Page, PageId};
use std::path::Path;

#[derive(Debug)]
pub struct Pager {
    file_manager: FileManager,
    buffer_pool: BufferPool,
}

impl Pager {
    pub fn new<P: AsRef<Path>>(path: P, cache_capacity: usize) -> Result<Self> {
        let file_manager = FileManager::new(path)?;
        Ok(Self {
            file_manager,
            buffer_pool: BufferPool::new(cache_capacity),
        })
    }

    pub fn new_in_memory(cache_capacity: usize) -> Result<Self> {
        let file_manager = FileManager::new_in_memory()?;
        Ok(Self {
            file_manager,
            buffer_pool: BufferPool::new(cache_capacity),
        })
    }

    pub fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        if let Some(page) = self.buffer_pool.get(page_id) {
            return Ok(page.clone());
        }

        let page = self.file_manager.read_page(page_id)?;
        self.buffer_pool.put(page.clone());
        Ok(page)
    }

    pub fn write_page(&mut self, page: Page) -> Result<()> {
        self.file_manager.write_page(&page)?;
        self.buffer_pool.put(page);
        Ok(())
    }

    pub fn allocate_page(&mut self) -> Result<PageId> {
        self.file_manager.allocate_page()
    }

    pub fn deallocate_page(&mut self, page_id: PageId) -> Result<()> {
        self.buffer_pool.remove(page_id);
        self.file_manager.deallocate_page(page_id)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.file_manager.flush()
    }

    pub fn free_pages(&self) -> &[PageId] {
        self.file_manager.free_pages()
    }

    pub fn set_free_pages(&mut self, free_pages: Vec<PageId>) {
        self.file_manager.set_free_pages(free_pages);
    }
}
