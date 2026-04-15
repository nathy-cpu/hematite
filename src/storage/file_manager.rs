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
//!   = page_id * PAGE_SIZE
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
use crate::storage::format::{
    bootstrap_database_page_one, detect_format_generation, DatabaseHeaderV3, FormatGeneration,
    PageKind,
};
use crate::storage::free_list::FreeList;
use crate::storage::{
    file_len_for_next_page_id, next_page_id_for_file_len, Page, PageId, DB_HEADER_PAGE_ID,
    FIRST_ALLOCATABLE_PAGE_ID, PAGE_SIZE, STORAGE_METADATA_PAGE_ID,
};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

#[cfg(not(any(unix, windows)))]
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

#[derive(Debug)]
pub struct FileManager {
    backend: FileBackend,
    position: u64,
    next_page_id: u32,
    free_list: FreeList,
    #[cfg(test)]
    fail_on_write_countdown: Option<usize>,
}

#[derive(Debug, Clone)]
pub(crate) struct FileManagerSnapshot {
    file_len: u64,
    free_pages: Vec<PageId>,
}

#[derive(Debug)]
enum FileBackend {
    Disk { file: File },
    Memory(Vec<u8>),
}

impl FileManager {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref())?;

        let manager = Self {
            backend: FileBackend::Disk { file },
            position: 0,
            next_page_id: 0,
            free_list: FreeList::new(),
            #[cfg(test)]
            fail_on_write_countdown: None,
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
            fail_on_write_countdown: None,
        };

        let mut manager = manager;
        manager.initialize()?;
        Ok(manager)
    }

    fn initialize(&mut self) -> Result<()> {
        let file_len = self.len()?;
        if file_len == 0 {
            self.initialize_new_file()?;
        } else {
            self.load_existing_file(file_len)?;
        }
        Ok(())
    }

    fn initialize_new_file(&mut self) -> Result<()> {
        self.set_len(file_len_for_next_page_id(FIRST_ALLOCATABLE_PAGE_ID))?;
        self.next_page_id = FIRST_ALLOCATABLE_PAGE_ID;
        self.free_list.replace(Vec::new());
        Ok(())
    }

    fn load_existing_file(&mut self, file_len: u64) -> Result<()> {
        if file_len % PAGE_SIZE as u64 != 0 {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Database file length {file_len} is not page aligned"
            )));
        }
        self.next_page_id = next_page_id_for_file_len(file_len);
        Ok(())
    }

    pub fn read_page(&self, page_id: PageId) -> Result<Page> {
        let offset = Self::page_offset(page_id)?;

        let mut data = vec![0u8; PAGE_SIZE];
        self.read_exact_at(offset, &mut data).map_err(|err| {
            crate::error::HematiteError::StorageError(format!(
                "Failed to read page {} at offset {}: {}",
                page_id, offset, err
            ))
        })?;

        Page::from_bytes(page_id, data)
    }

    #[allow(dead_code)]
    pub(crate) fn detect_format_generation(&self) -> Result<Option<FormatGeneration>> {
        let len = self.len()? as usize;
        if len == 0 {
            return Ok(None);
        }

        let probe_len = len.min(PAGE_SIZE);
        let bytes = self.read_region(0, probe_len)?;
        Ok(detect_format_generation(&bytes))
    }

    #[allow(dead_code)]
    pub(crate) fn bootstrap_v3_database(
        &mut self,
        header: &DatabaseHeaderV3,
        root_page_kind: PageKind,
    ) -> Result<()> {
        let page_one = bootstrap_database_page_one(header, root_page_kind)?;
        self.truncate_to(PAGE_SIZE as u64)?;
        self.write_region(0, &page_one)?;
        self.next_page_id = 2;
        self.free_list.replace(Vec::new());
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn read_region(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let mut bytes = vec![0u8; len];
        self.read_exact_at(offset, &mut bytes)?;
        Ok(bytes)
    }

    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        let offset = Self::page_offset(page.id)?;

        self.seek(SeekFrom::Start(offset))?;
        self.write_all(&page.data)?;

        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn write_region(&mut self, offset: u64, bytes: &[u8]) -> Result<()> {
        self.seek(SeekFrom::Start(offset))?;
        self.write_all(bytes)?;
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
            FileBackend::Disk { file, .. } => {
                file.sync_all()?;
            }
            FileBackend::Memory(_) => {}
        }
        Ok(())
    }

    pub fn deallocate_page(&mut self, page_id: PageId) -> Result<()> {
        if page_id == DB_HEADER_PAGE_ID || page_id == STORAGE_METADATA_PAGE_ID {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Cannot deallocate reserved page {}",
                page_id
            )));
        }
        // Add to free list for reuse
        self.free_list.push_free_page(page_id);
        self.compact_trailing_free_pages()?;
        Ok(())
    }

    pub fn deallocate_page_deferred(&mut self, page_id: PageId) {
        if page_id == DB_HEADER_PAGE_ID || page_id == STORAGE_METADATA_PAGE_ID {
            return;
        }
        self.free_list.push_free_page(page_id);
    }

    pub fn free_pages(&self) -> &[PageId] {
        self.free_list.as_slice()
    }

    pub fn set_free_pages(&mut self, free_pages: Vec<PageId>) {
        self.free_list.replace(free_pages);
    }

    pub fn is_free_page(&self, page_id: PageId) -> bool {
        self.free_list.contains(page_id)
    }

    pub fn file_len(&self) -> Result<u64> {
        self.len()
    }

    pub fn restore_file_len(&mut self, len: u64) -> Result<()> {
        self.set_len(len)?;
        self.next_page_id = next_page_id_for_file_len(len);
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn truncate_to(&mut self, len: u64) -> Result<()> {
        self.set_len(len)
    }

    pub(crate) fn next_page_id(&self) -> u32 {
        self.next_page_id
    }

    pub(crate) fn set_next_page_id(&mut self, next_page_id: u32) {
        self.next_page_id = next_page_id.max(FIRST_ALLOCATABLE_PAGE_ID);
    }

    pub(crate) fn allocated_page_count(&self) -> usize {
        self.next_page_id.saturating_sub(FIRST_ALLOCATABLE_PAGE_ID) as usize
    }

    pub(crate) fn trailing_free_page_count(&self) -> usize {
        if self.next_page_id <= FIRST_ALLOCATABLE_PAGE_ID {
            return 0;
        }

        let mut count = 0usize;
        let mut candidate = self.next_page_id;
        while candidate > FIRST_ALLOCATABLE_PAGE_ID {
            let page_id = candidate - 1;
            if self.free_list.contains(page_id) {
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
        let minimum_next_page_id = FIRST_ALLOCATABLE_PAGE_ID;
        self.free_list
            .compact_trailing_pages(&mut self.next_page_id, minimum_next_page_id);
        let target_next_page_id = self.next_page_id.max(minimum_next_page_id);
        let target_len = file_len_for_next_page_id(target_next_page_id);
        let current_len = self.len()?;

        if target_len < current_len {
            self.set_len(target_len)?;
        }

        Ok(())
    }

    fn page_offset(page_id: PageId) -> Result<u64> {
        Ok(page_id as u64 * PAGE_SIZE as u64)
    }

    fn len(&self) -> Result<u64> {
        match &self.backend {
            FileBackend::Disk { file, .. } => Ok(file.metadata()?.len()),
            FileBackend::Memory(buffer) => Ok(buffer.len() as u64),
        }
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        match &self.backend {
            FileBackend::Disk { file } => {
                #[cfg(any(unix, windows))]
                {
                    read_exact_at_position(file, offset, buf)?;
                }
                #[cfg(not(any(unix, windows)))]
                {
                    let mut clone = file.try_clone()?;
                    clone.seek(SeekFrom::Start(offset))?;
                    clone.read_exact(buf)?;
                }
            }
            FileBackend::Memory(buffer) => {
                let offset = offset as usize;
                let end = offset + buf.len();
                if end > buffer.len() {
                    return Err(crate::error::HematiteError::StorageError(
                        "Attempted to read beyond in-memory storage bounds".to_string(),
                    ));
                }
                buf.copy_from_slice(&buffer[offset..end]);
            }
        }
        Ok(())
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match &mut self.backend {
            FileBackend::Disk { file, .. } => {
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

    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        #[cfg(test)]
        if let Some(remaining_writes) = self.fail_on_write_countdown.as_mut() {
            if *remaining_writes == 0 {
                self.fail_on_write_countdown = None;
                return Err(crate::error::HematiteError::IoError(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Injected IO error",
                )));
            }
            *remaining_writes -= 1;
        }
        match &mut self.backend {
            FileBackend::Disk { file, .. } => {
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
            FileBackend::Disk { file, .. } => {
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

#[cfg(any(unix, windows))]
fn read_exact_at_position(file: &File, mut offset: u64, mut buf: &mut [u8]) -> std::io::Result<()> {
    while !buf.is_empty() {
        #[cfg(unix)]
        let bytes_read = file.read_at(buf, offset)?;
        #[cfg(windows)]
        let bytes_read = file.seek_read(buf, offset)?;
        if bytes_read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
        offset = offset.saturating_add(bytes_read as u64);
        let (_, tail) = buf.split_at_mut(bytes_read);
        buf = tail;
    }
    Ok(())
}

#[cfg(test)]
impl FileManager {
    pub fn inject_write_failure(&mut self) {
        self.inject_write_failure_after(0);
    }

    pub fn inject_write_failure_after(&mut self, writes_before_failure: usize) {
        self.fail_on_write_countdown = Some(writes_before_failure);
    }
}

#[cfg(test)]
mod tests {
    use super::FileManager;
    use crate::storage::format::{DatabaseHeaderV3, FormatGeneration, PageKind};

    #[test]
    fn raw_region_io_roundtrips_in_memory() {
        let mut manager = FileManager::new_in_memory().unwrap();
        manager.write_region(17, b"hematite").unwrap();

        let bytes = manager.read_region(17, 8).unwrap();
        assert_eq!(bytes, b"hematite");
    }

    #[test]
    fn raw_region_write_grows_in_memory_backend() {
        let mut manager = FileManager::new_in_memory().unwrap();
        manager.write_region(128, b"db").unwrap();

        assert!(manager.file_len().unwrap() >= 130);
        assert_eq!(manager.read_region(128, 2).unwrap(), b"db");
    }

    #[test]
    fn truncate_to_shrinks_in_memory_backend() {
        let mut manager = FileManager::new_in_memory().unwrap();
        manager.write_region(256, b"pager").unwrap();
        manager.truncate_to(64).unwrap();

        assert_eq!(manager.file_len().unwrap(), 64);
    }

    #[test]
    fn detect_format_generation_recognizes_v3_files() {
        let mut manager = FileManager::new_in_memory().unwrap();
        manager
            .bootstrap_v3_database(&DatabaseHeaderV3::default(), PageKind::LeafTable)
            .unwrap();

        assert_eq!(
            manager.detect_format_generation().unwrap(),
            Some(FormatGeneration::V3)
        );
    }

    #[test]
    fn bootstrap_v3_database_writes_page_one_image() {
        let mut manager = FileManager::new_in_memory().unwrap();
        manager
            .bootstrap_v3_database(&DatabaseHeaderV3::default(), PageKind::LeafTable)
            .unwrap();

        let page_one = manager.read_region(0, 4096).unwrap();
        assert_eq!(&page_one[..16], b"Hematite format3");
        assert_eq!(page_one[100], PageKind::LeafTable as u8);
        assert_eq!(manager.file_len().unwrap(), 4096);
    }

    #[test]
    fn deallocate_reserved_pages_are_rejected() {
        let mut manager = FileManager::new_in_memory().unwrap();

        let page_zero_err = manager.deallocate_page(0).unwrap_err();
        assert!(page_zero_err
            .to_string()
            .contains("Cannot deallocate reserved page 0"));

        let page_one_err = manager.deallocate_page(1).unwrap_err();
        assert!(page_one_err
            .to_string()
            .contains("Cannot deallocate reserved page 1"));
    }
}
