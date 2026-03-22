//! File manager for handling single-file database operations.
//!
//! M0 storage contract notes:
//! - The first 64 bytes are a file-level header region.
//! - Logical page 0 and page 1 are reserved and never returned by allocation.
//! - This module currently stores freelist state as an in-memory list hydrated from
//!   storage metadata; M1 will move this toward a dedicated persisted freelist layout.
//! - `next_page_id` tracks high-water allocation and compaction can move this backward.

use crate::error::Result;
use crate::storage::free_list::FreeList;
use crate::storage::{Page, PageId, PAGE_SIZE, STORAGE_METADATA_PAGE_ID};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

#[derive(Debug)]
pub struct FileManager {
    backend: FileBackend,
    position: u64,
    next_page_id: u32,
    free_list: FreeList,
}

#[derive(Debug)]
enum FileBackend {
    Disk(File),
    Memory(Vec<u8>),
}

impl FileManager {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let mut manager = Self {
            backend: FileBackend::Disk(file),
            position: 0,
            next_page_id: 0,
            free_list: FreeList::new(),
        };

        manager.initialize()?;
        Ok(manager)
    }

    pub fn new_in_memory() -> Result<Self> {
        let mut manager = Self {
            backend: FileBackend::Memory(Vec::new()),
            position: 0,
            next_page_id: 0,
            free_list: FreeList::new(),
        };

        manager.initialize()?;
        Ok(manager)
    }

    fn initialize(&mut self) -> Result<()> {
        if self.len()? == 0 {
            self.write_header()?;
        } else {
            self.read_header()?;
        }
        Ok(())
    }

    fn write_header(&mut self) -> Result<()> {
        self.seek(SeekFrom::Start(0))?;
        self.write_all(&[0; 64])?; // Fixed file header region.
        self.next_page_id = 2; // Start page IDs from 2 (page 0 is header, page 1 is metadata)
        Ok(())
    }

    fn read_header(&mut self) -> Result<()> {
        let mut header = [0u8; 64];
        self.seek(SeekFrom::Start(0))?;
        self.read_exact(&mut header)?;

        // Derive the next allocatable page from the number of page-sized regions after the
        // 64-byte file header. Reserved pages 0 and 1 imply a minimum next page id of 2.
        let file_size = self.len()?;
        let page_regions = file_size.saturating_sub(64) / PAGE_SIZE as u64;
        self.next_page_id = (page_regions as u32).max(2);
        Ok(())
    }

    pub fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        let offset = 64 + (page_id as u64 * PAGE_SIZE as u64);

        let mut data = vec![0u8; PAGE_SIZE];
        self.seek(SeekFrom::Start(offset))?;
        self.read_exact(&mut data)?;

        Page::from_bytes(page_id, data)
    }

    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        let offset = 64 + (page.id as u64 * PAGE_SIZE as u64);

        self.seek(SeekFrom::Start(offset))?;
        self.write_all(&page.data)?;

        Ok(())
    }

    pub fn allocate_page(&mut self) -> Result<PageId> {
        // Try to reuse a free page first
        if let Some(free_page_id) = self.free_list.pop_free_page() {
            Ok(free_page_id)
        } else {
            // Allocate new page
            let page_id = self.next_page_id;
            self.next_page_id += 1;

            // Initialize new page with zeros
            let page = Page::new(page_id);
            self.write_page(&page)?;

            Ok(page_id)
        }
    }

    pub fn flush(&mut self) -> Result<()> {
        match &mut self.backend {
            FileBackend::Disk(file) => {
                file.sync_all()?;
            }
            FileBackend::Memory(_) => {}
        }
        Ok(())
    }

    pub fn deallocate_page(&mut self, page_id: PageId) -> Result<()> {
        // Add to free list for reuse
        self.free_list.push_free_page(page_id);
        self.compact_trailing_free_pages()?;
        Ok(())
    }

    pub fn free_pages(&self) -> &[PageId] {
        self.free_list.as_slice()
    }

    pub fn set_free_pages(&mut self, free_pages: Vec<PageId>) {
        self.free_list.replace(free_pages);
    }

    pub fn file_len(&self) -> Result<u64> {
        self.len()
    }

    pub fn restore_file_len(&mut self, len: u64) -> Result<()> {
        self.set_len(len)?;
        let page_regions = len.saturating_sub(64) / PAGE_SIZE as u64;
        self.next_page_id = (page_regions as u32).max(2);
        Ok(())
    }

    pub(crate) fn next_page_id(&self) -> u32 {
        self.next_page_id
    }

    fn compact_trailing_free_pages(&mut self) -> Result<()> {
        let minimum_next_page_id = STORAGE_METADATA_PAGE_ID + 1;
        self.free_list
            .compact_trailing_pages(&mut self.next_page_id, minimum_next_page_id);
        let target_next_page_id = self.next_page_id.max(minimum_next_page_id);
        let target_len = 64 + target_next_page_id as u64 * PAGE_SIZE as u64;
        let current_len = self.len()?;

        if target_len < current_len {
            self.set_len(target_len)?;
        }

        Ok(())
    }

    fn len(&self) -> Result<u64> {
        match &self.backend {
            FileBackend::Disk(file) => Ok(file.metadata()?.len()),
            FileBackend::Memory(buffer) => Ok(buffer.len() as u64),
        }
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match &mut self.backend {
            FileBackend::Disk(file) => {
                let position = file.seek(pos)?;
                self.position = position;
                Ok(position)
            }
            FileBackend::Memory(buffer) => {
                let len = buffer.len() as i64;
                let next = match pos {
                    SeekFrom::Start(offset) => offset as i64,
                    SeekFrom::End(offset) => len + offset,
                    SeekFrom::Current(offset) => self.position as i64 + offset,
                };

                if next < 0 {
                    return Err(crate::error::HematiteError::StorageError(
                        "Invalid negative seek in in-memory storage".to_string(),
                    ));
                }

                self.position = next as u64;
                Ok(self.position)
            }
        }
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        match &mut self.backend {
            FileBackend::Disk(file) => {
                file.read_exact(buf)?;
            }
            FileBackend::Memory(buffer) => {
                let offset = self.position as usize;
                let end = offset + buf.len();
                if end > buffer.len() {
                    return Err(crate::error::HematiteError::StorageError(
                        "Attempted to read beyond in-memory storage bounds".to_string(),
                    ));
                }
                buf.copy_from_slice(&buffer[offset..end]);
                self.position = end as u64;
            }
        }
        Ok(())
    }

    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        match &mut self.backend {
            FileBackend::Disk(file) => {
                file.write_all(buf)?;
            }
            FileBackend::Memory(buffer) => {
                let offset = self.position as usize;
                let end = offset + buf.len();
                if end > buffer.len() {
                    buffer.resize(end, 0);
                }
                buffer[offset..end].copy_from_slice(buf);
                self.position = end as u64;
            }
        }
        Ok(())
    }

    fn set_len(&mut self, len: u64) -> Result<()> {
        match &mut self.backend {
            FileBackend::Disk(file) => {
                file.set_len(len)?;
            }
            FileBackend::Memory(buffer) => {
                buffer.resize(len as usize, 0);
            }
        }

        if self.position > len {
            self.position = len;
        }

        Ok(())
    }
}
