//! Raw file backend for fixed-size logical pages.
//!
//! `FileManager` is intentionally dumber than `Pager`. It knows how to read and write page-sized
//! byte regions, track high-water allocation, and reuse freed page ids, but it does not know
//! about transactions, checksums, or tree structure.
//!
//! Addressing model:
//!
//! ```text
//! file offset
//!   = 64-byte file header
//!   + (page_id * PAGE_SIZE)
//! ```
//!
//! Reserved logical pages:
//! - page `0`: database header
//! - page `1`: storage metadata
//! - page `2+`: allocatable payload pages
//!
//! Allocation model:
//! - reuse a page id from the freelist if one exists;
//! - otherwise allocate `next_page_id` and advance the high-water mark;
//! - allow trailing free pages to pull the high-water mark backward during compaction.
//!
//! This module supports both disk-backed and in-memory backends so the pager can run identical
//! logic in tests and in the real database.

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
    #[cfg(test)]
    fail_next_write: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct FileManagerSnapshot {
    file_len: u64,
    free_pages: Vec<PageId>,
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

        let manager = Self {
            backend: FileBackend::Disk(file),
            position: 0,
            next_page_id: 0,
            free_list: FreeList::new(),
            #[cfg(test)]
            fail_next_write: false,
        };

        let mut manager = manager;
        manager.initialize()?;
        Ok(manager)
    }

    pub fn new_in_memory() -> Result<Self> {
        let manager = Self {
            backend: FileBackend::Memory(Vec::new()),
            position: 0,
            next_page_id: 0,
            free_list: FreeList::new(),
            #[cfg(test)]
            fail_next_write: false,
        };

        let mut manager = manager;
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

    pub fn deallocate_page_deferred(&mut self, page_id: PageId) {
        self.free_list.push_free_page(page_id);
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

    pub(crate) fn allocated_page_count(&self) -> usize {
        self.next_page_id
            .saturating_sub(STORAGE_METADATA_PAGE_ID + 1) as usize
    }

    pub(crate) fn trailing_free_page_count(&self) -> usize {
        if self.next_page_id <= STORAGE_METADATA_PAGE_ID + 1 {
            return 0;
        }

        let mut count = 0usize;
        let mut candidate = self.next_page_id;
        while candidate > STORAGE_METADATA_PAGE_ID + 1 {
            let page_id = candidate - 1;
            if self.free_list.as_slice().contains(&page_id) {
                count += 1;
                candidate -= 1;
            } else {
                break;
            }
        }

        count
    }

    pub(crate) fn fragmented_free_page_count(&self) -> usize {
        self.free_list
            .as_slice()
            .len()
            .saturating_sub(self.trailing_free_page_count())
    }

    pub(crate) fn compact_free_pages(&mut self) -> Result<()> {
        self.compact_trailing_free_pages()
    }

    pub(crate) fn snapshot(&self) -> Result<FileManagerSnapshot> {
        Ok(FileManagerSnapshot {
            file_len: self.file_len()?,
            free_pages: self.free_pages().to_vec(),
        })
    }

    pub(crate) fn restore_snapshot(&mut self, snapshot: FileManagerSnapshot) -> Result<()> {
        self.restore_file_len(snapshot.file_len)?;
        self.set_free_pages(snapshot.free_pages);
        Ok(())
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
        #[cfg(test)]
        if self.fail_next_write {
            self.fail_next_write = false;
            return Err(crate::error::HematiteError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Injected IO error",
            )));
        }
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

#[cfg(test)]
impl FileManager {
    pub fn inject_write_failure(&mut self) {
        self.fail_next_write = true;
    }
}
