//! Pager abstraction over page IO, cache, and allocation.
//!
//! M1.1 contract:
//! - All page reads/writes for the storage engine should flow through this type.
//! - Buffer pool behavior and file manager behavior are composed here.
//! - Allocation/deallocation remains file-manager-backed for now and is evolved in later M1 tasks.
//!
//! M1.2 contract:
//! - `write_page` is write-back into the cache and marks a page as dirty.
//! - `flush` is the persistence boundary that writes all dirty pages to disk and fsyncs.
//! - Dirty state is tracked by page id and cleared only after successful flush/deallocation.

use crate::error::Result;
use crate::storage::{buffer_pool::BufferPool, file_manager::FileManager, Page, PageId};
use std::collections::HashSet;
use std::path::Path;

#[derive(Debug)]
pub struct Pager {
    file_manager: FileManager,
    buffer_pool: BufferPool,
    dirty_pages: HashSet<PageId>,
}

impl Pager {
    pub fn new<P: AsRef<Path>>(path: P, cache_capacity: usize) -> Result<Self> {
        let file_manager = FileManager::new(path)?;
        Ok(Self {
            file_manager,
            buffer_pool: BufferPool::new(cache_capacity),
            dirty_pages: HashSet::new(),
        })
    }

    pub fn new_in_memory(cache_capacity: usize) -> Result<Self> {
        let file_manager = FileManager::new_in_memory()?;
        Ok(Self {
            file_manager,
            buffer_pool: BufferPool::new(cache_capacity),
            dirty_pages: HashSet::new(),
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
        let page_id = page.id;
        self.buffer_pool.put(page);
        self.dirty_pages.insert(page_id);
        Ok(())
    }

    pub fn allocate_page(&mut self) -> Result<PageId> {
        self.file_manager.allocate_page()
    }

    pub fn deallocate_page(&mut self, page_id: PageId) -> Result<()> {
        self.buffer_pool.remove(page_id);
        self.dirty_pages.remove(&page_id);
        self.file_manager.deallocate_page(page_id)
    }

    pub fn flush(&mut self) -> Result<()> {
        let dirty_ids = self.dirty_pages.iter().copied().collect::<Vec<_>>();
        for page_id in dirty_ids {
            if let Some(page) = self.buffer_pool.get(page_id) {
                self.file_manager.write_page(page)?;
            }
            self.dirty_pages.remove(&page_id);
        }
        self.file_manager.flush()
    }

    pub fn free_pages(&self) -> &[PageId] {
        self.file_manager.free_pages()
    }

    pub fn set_free_pages(&mut self, free_pages: Vec<PageId>) {
        self.file_manager.set_free_pages(free_pages);
    }

    #[cfg(test)]
    pub(crate) fn dirty_page_count(&self) -> usize {
        self.dirty_pages.len()
    }
}
