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
//!
//! M1.5 contract:
//! - Pager tracks deterministic page checksums for persisted pages.
//! - On cache-miss reads, persisted checksum records are verified before returning data.

use crate::error::Result;
use crate::storage::{
    buffer_pool::BufferPool, file_manager::FileManager, Page, PageId, PagerIntegrityReport,
    DB_HEADER_PAGE_ID, STORAGE_METADATA_PAGE_ID,
};
use std::collections::{HashMap, HashSet};
use std::path::Path;

#[derive(Debug)]
pub struct Pager {
    file_manager: FileManager,
    buffer_pool: BufferPool,
    dirty_pages: HashSet<PageId>,
    page_checksums: HashMap<PageId, u32>,
}

impl Pager {
    pub const CHECKSUM_METADATA_VERSION: u32 = 1;

    pub fn new<P: AsRef<Path>>(path: P, cache_capacity: usize) -> Result<Self> {
        let file_manager = FileManager::new(path)?;
        Ok(Self {
            file_manager,
            buffer_pool: BufferPool::new(cache_capacity),
            dirty_pages: HashSet::new(),
            page_checksums: HashMap::new(),
        })
    }

    pub fn new_in_memory(cache_capacity: usize) -> Result<Self> {
        let file_manager = FileManager::new_in_memory()?;
        Ok(Self {
            file_manager,
            buffer_pool: BufferPool::new(cache_capacity),
            dirty_pages: HashSet::new(),
            page_checksums: HashMap::new(),
        })
    }

    pub fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        if let Some(page) = self.buffer_pool.get(page_id) {
            return Ok(page.clone());
        }

        let page = self.file_manager.read_page(page_id)?;
        if let Some(expected_checksum) = self.page_checksums.get(&page_id) {
            let actual_checksum = Self::calculate_page_checksum(&page);
            if actual_checksum != *expected_checksum {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Page checksum mismatch for page {}: expected {}, got {}",
                    page_id.as_u32(),
                    expected_checksum,
                    actual_checksum
                )));
            }
        }
        self.buffer_pool.put(page.clone());
        Ok(page)
    }

    pub fn write_page(&mut self, page: Page) -> Result<()> {
        let page_id = page.id;
        if page_id != STORAGE_METADATA_PAGE_ID {
            self.page_checksums
                .insert(page_id, Self::calculate_page_checksum(&page));
        }
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
        self.page_checksums.remove(&page_id);
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

    pub fn checksum_entries(&self) -> Vec<(PageId, u32)> {
        self.page_checksums
            .iter()
            .map(|(page_id, checksum)| (*page_id, *checksum))
            .collect()
    }

    pub fn replace_checksums(&mut self, checksums: HashMap<PageId, u32>) {
        self.page_checksums = checksums;
    }

    pub fn validate_integrity(&mut self) -> Result<PagerIntegrityReport> {
        let max_page_id_exclusive = self.file_manager.next_page_id();
        let mut free_pages = HashSet::new();

        for &page_id in self.file_manager.free_pages() {
            if page_id == DB_HEADER_PAGE_ID || page_id == STORAGE_METADATA_PAGE_ID {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Reserved page {} cannot be marked free",
                    page_id.as_u32()
                )));
            }

            if page_id.as_u32() >= max_page_id_exclusive {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Free page {} exceeds allocated page range (next_page_id={})",
                    page_id.as_u32(),
                    max_page_id_exclusive
                )));
            }

            if !free_pages.insert(page_id) {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Duplicate free page {} detected",
                    page_id.as_u32()
                )));
            }
        }

        if self.page_checksums.contains_key(&STORAGE_METADATA_PAGE_ID) {
            return Err(crate::error::HematiteError::CorruptedData(format!(
                "Storage metadata page {} must not have pager checksum metadata",
                STORAGE_METADATA_PAGE_ID.as_u32()
            )));
        }

        let checksummed_pages = self
            .page_checksums
            .iter()
            .map(|(page_id, checksum)| (*page_id, *checksum))
            .collect::<Vec<_>>();

        let mut verified_checksum_pages = 0usize;
        for (page_id, expected_checksum) in checksummed_pages {
            if page_id.as_u32() >= max_page_id_exclusive {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Checksum entry for page {} exceeds allocated page range (next_page_id={})",
                    page_id.as_u32(),
                    max_page_id_exclusive
                )));
            }

            if free_pages.contains(&page_id) {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Page {} has checksum metadata but is marked free",
                    page_id.as_u32()
                )));
            }

            let page = if self.dirty_pages.contains(&page_id) {
                self.buffer_pool.get(page_id).cloned().ok_or_else(|| {
                    crate::error::HematiteError::StorageError(format!(
                        "Dirty page {} missing from buffer pool",
                        page_id.as_u32()
                    ))
                })?
            } else {
                self.file_manager.read_page(page_id)?
            };

            let actual_checksum = Self::calculate_page_checksum(&page);
            if actual_checksum != expected_checksum {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Page checksum mismatch for page {}: expected {}, got {}",
                    page_id.as_u32(),
                    expected_checksum,
                    actual_checksum
                )));
            }

            verified_checksum_pages += 1;
        }

        Ok(PagerIntegrityReport {
            free_page_count: free_pages.len(),
            checksummed_page_count: self.page_checksums.len(),
            verified_checksum_pages,
        })
    }

    fn calculate_page_checksum(page: &Page) -> u32 {
        // FNV-1a over page bytes for deterministic cross-process checksums using std only.
        let mut hash: u32 = 0x811C9DC5;
        for byte in &page.data {
            hash ^= u32::from(*byte);
            hash = hash.wrapping_mul(0x01000193);
        }
        hash
    }

    #[cfg(test)]
    pub(crate) fn dirty_page_count(&self) -> usize {
        self.dirty_pages.len()
    }
}
