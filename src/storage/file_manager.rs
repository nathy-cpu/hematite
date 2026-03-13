//! File manager for handling single-file database operations

use crate::error::Result;
use crate::storage::{Page, PageId, PAGE_SIZE};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

#[derive(Debug)]
pub struct FileManager {
    file: File,
    next_page_id: u32,
}

impl FileManager {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let mut manager = Self {
            file,
            next_page_id: 0,
        };

        // Initialize if file is empty
        if manager.file.metadata()?.len() == 0 {
            manager.write_header()?;
        } else {
            manager.read_header()?;
        }

        Ok(manager)
    }

    fn write_header(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&[0; 64])?; // 64-byte header
        self.next_page_id = 2; // Start page IDs from 2 (page 0 is header, page 1 is metadata)
        Ok(())
    }

    fn read_header(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut header = [0u8; 64];
        self.file.read_exact(&mut header)?;

        // For now, just calculate next page ID from file size
        // Account for page 0 (header) and page 1 (metadata) being reserved
        let file_size = self.file.metadata()?.len();
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

        self.file.seek(SeekFrom::Start(offset))?;

        let mut data = vec![0u8; PAGE_SIZE];
        self.file.read_exact(&mut data)?;

        Page::from_bytes(page_id, data)
    }

    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        let offset = 64 + (page.id.as_u32() as u64 * PAGE_SIZE as u64);

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&page.data)?;
        self.file.sync_all()?;

        Ok(())
    }

    pub fn allocate_page(&mut self) -> Result<PageId> {
        let page_id = PageId::new(self.next_page_id);
        self.next_page_id += 1;

        // Initialize the new page with zeros
        let page = Page::new(page_id);
        self.write_page(&page)?;

        Ok(page_id)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }
}
