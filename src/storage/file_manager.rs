//! File manager for handling single-file database operations

use crate::error::Result;
use crate::storage::{Page, PageId, PAGE_SIZE};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

#[derive(Debug)]
pub struct FileManager {
    backend: FileBackend,
    position: u64,
    next_page_id: u32,
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

        let mut manager = Self {
            backend: FileBackend::Disk(file),
            position: 0,
            next_page_id: 0,
            free_pages: Vec::new(),
        };

        manager.initialize()?;
        Ok(manager)
    }

    pub fn new_in_memory() -> Result<Self> {
        let mut manager = Self {
            backend: FileBackend::Memory(Vec::new()),
            position: 0,
            next_page_id: 0,
            free_pages: Vec::new(),
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
        self.write_all(&[0; 64])?; // 64-byte header
        self.next_page_id = 2; // Start page IDs from 2 (page 0 is header, page 1 is metadata)
        Ok(())
    }

    fn read_header(&mut self) -> Result<()> {
        let mut header = [0u8; 64];
        self.seek(SeekFrom::Start(0))?;
        self.read_exact(&mut header)?;

        // For now, just calculate next page ID from file size
        // Account for page 0 (header) and page 1 (metadata) being reserved
        let file_size = self.len()?;
        if file_size <= 64 {
            self.next_page_id = 2; // Only header exists
        } else {
            let data_pages = ((file_size - 64) / PAGE_SIZE as u64) as u32;
            self.next_page_id = if data_pages <= 1 { 2 } else { data_pages + 1 };
        }
        Ok(())
    }

    pub fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        let offset = 64 + (page_id.as_u32() as u64 * PAGE_SIZE as u64);

        let mut data = vec![0u8; PAGE_SIZE];
        self.seek(SeekFrom::Start(offset))?;
        self.read_exact(&mut data)?;

        Page::from_bytes(page_id, data)
    }

    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        let offset = 64 + (page.id.as_u32() as u64 * PAGE_SIZE as u64);

        self.seek(SeekFrom::Start(offset))?;
        self.write_all(&page.data)?;

        Ok(())
    }

    pub fn allocate_page(&mut self) -> Result<PageId> {
        // Try to reuse a free page first
        if let Some(free_page_id) = self.free_pages.pop() {
            Ok(free_page_id)
        } else {
            // Allocate new page
            let page_id = PageId::new(self.next_page_id);
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
        if !self.free_pages.contains(&page_id) {
            self.free_pages.push(page_id);
        }
        Ok(())
    }

    pub fn free_pages(&self) -> &[PageId] {
        &self.free_pages
    }

    pub fn set_free_pages(&mut self, free_pages: Vec<PageId>) {
        self.free_pages = free_pages;
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
}
