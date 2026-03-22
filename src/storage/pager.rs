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
//!
//! M7 contract:
//! - The pager owns rollback journaling and crash recovery for page/checksum state.
//! - Writes journal original page images before first modification in a transaction.
//! - Recovery is process-crash only and replays the rollback journal on open.

use crate::error::Result;
use crate::storage::journal::{JournalRecord, JournalState, RollbackJournal};
use crate::storage::{
    buffer_pool::BufferPool, file_manager::FileManager, Page, PageId, PagerIntegrityReport,
    DB_HEADER_PAGE_ID, STORAGE_METADATA_PAGE_ID,
};
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

#[derive(Debug, Clone)]
struct PagerTransaction {
    original_file_len: u64,
    original_free_pages: Vec<PageId>,
    original_checksums: HashMap<PageId, u32>,
    journaled_pages: HashSet<PageId>,
    page_records: Vec<JournalRecord>,
}

#[derive(Debug)]
pub struct Pager {
    file_manager: FileManager,
    buffer_pool: BufferPool,
    dirty_pages: HashSet<PageId>,
    page_checksums: HashMap<PageId, u32>,
    checksum_store_path: Option<PathBuf>,
    journal_path: Option<PathBuf>,
    transaction: Option<PagerTransaction>,
    buffer_pool_capacity: usize,
}

impl Pager {
    pub const CHECKSUM_METADATA_VERSION: u32 = 1;

    pub fn new<P: AsRef<Path>>(path: P, cache_capacity: usize) -> Result<Self> {
        let checksum_store_path = Some(Self::checksum_store_path(path.as_ref()));
        let journal_path = Some(Self::journal_path(path.as_ref()));
        let file_manager = FileManager::new(path)?;
        let mut pager = Self {
            file_manager,
            buffer_pool: BufferPool::new(cache_capacity),
            dirty_pages: HashSet::new(),
            page_checksums: HashMap::new(),
            checksum_store_path,
            journal_path,
            transaction: None,
            buffer_pool_capacity: cache_capacity,
        };
        pager.recover_if_needed()?;
        pager.load_persisted_state()?;
        Ok(pager)
    }

    pub fn new_in_memory(cache_capacity: usize) -> Result<Self> {
        let file_manager = FileManager::new_in_memory()?;
        Ok(Self {
            file_manager,
            buffer_pool: BufferPool::new(cache_capacity),
            dirty_pages: HashSet::new(),
            page_checksums: HashMap::new(),
            checksum_store_path: None,
            journal_path: None,
            transaction: None,
            buffer_pool_capacity: cache_capacity,
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
                    page_id, expected_checksum, actual_checksum
                )));
            }
        }
        self.buffer_pool.put(page.clone());
        Ok(page)
    }

    pub fn write_page(&mut self, page: Page) -> Result<()> {
        let page_id = page.id;
        self.snapshot_original_page(page_id)?;
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
        self.snapshot_original_page(page_id)?;
        self.buffer_pool.remove(page_id);
        self.dirty_pages.remove(&page_id);
        self.page_checksums.remove(&page_id);
        self.file_manager.deallocate_page(page_id)
    }

    pub fn flush(&mut self) -> Result<()> {
        let dirty_ids = self.dirty_pages.iter().copied().collect::<Vec<_>>();
        let mut metadata_page_dirty = false;

        // Persist all non-metadata dirty pages first.
        for page_id in dirty_ids.iter().copied() {
            if page_id == STORAGE_METADATA_PAGE_ID {
                metadata_page_dirty = true;
                continue;
            }

            if let Some(page) = self.buffer_pool.get(page_id) {
                self.file_manager.write_page(page)?;
            }
            self.dirty_pages.remove(&page_id);
        }

        // Persist metadata page last so it reflects already-persisted state.
        if metadata_page_dirty {
            if let Some(page) = self.buffer_pool.get(STORAGE_METADATA_PAGE_ID) {
                self.file_manager.write_page(page)?;
            }
            self.dirty_pages.remove(&STORAGE_METADATA_PAGE_ID);
        }
        self.file_manager.flush()?;
        self.persist_checksums()
    }

    pub fn begin_transaction(&mut self) -> Result<()> {
        if self.transaction.is_some() {
            return Err(crate::error::HematiteError::StorageError(
                "Pager transaction is already active".to_string(),
            ));
        }

        let transaction = PagerTransaction {
            original_file_len: self.file_manager.file_len()?,
            original_free_pages: self.file_manager.free_pages().to_vec(),
            original_checksums: self.page_checksums.clone(),
            journaled_pages: HashSet::new(),
            page_records: Vec::new(),
        };
        self.transaction = Some(transaction);
        self.persist_journal(JournalState::Active)
    }

    pub fn commit_transaction(&mut self) -> Result<()> {
        if self.transaction.is_none() {
            return Err(crate::error::HematiteError::StorageError(
                "Pager transaction is not active".to_string(),
            ));
        }

        self.flush()?;
        self.persist_journal(JournalState::Committed)?;
        self.remove_journal_file()?;
        self.transaction = None;
        Ok(())
    }

    pub fn rollback_transaction(&mut self) -> Result<()> {
        if self.transaction.is_none() {
            return Err(crate::error::HematiteError::StorageError(
                "Pager transaction is not active".to_string(),
            ));
        }

        self.rollback_from_active_transaction()?;
        self.remove_journal_file()?;
        self.transaction = None;
        Ok(())
    }

    pub fn transaction_active(&self) -> bool {
        self.transaction.is_some()
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
                    page_id
                )));
            }

            if page_id >= max_page_id_exclusive {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Free page {} exceeds allocated page range (next_page_id={})",
                    page_id, max_page_id_exclusive
                )));
            }

            if !free_pages.insert(page_id) {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Duplicate free page {} detected",
                    page_id
                )));
            }
        }

        if self.page_checksums.contains_key(&STORAGE_METADATA_PAGE_ID) {
            return Err(crate::error::HematiteError::CorruptedData(format!(
                "Storage metadata page {} must not have pager checksum metadata",
                STORAGE_METADATA_PAGE_ID
            )));
        }

        let checksummed_pages = self
            .page_checksums
            .iter()
            .map(|(page_id, checksum)| (*page_id, *checksum))
            .collect::<Vec<_>>();

        let mut verified_checksum_pages = 0usize;
        for (page_id, expected_checksum) in checksummed_pages {
            if page_id >= max_page_id_exclusive {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Checksum entry for page {} exceeds allocated page range (next_page_id={})",
                    page_id, max_page_id_exclusive
                )));
            }

            if free_pages.contains(&page_id) {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Page {} has checksum metadata but is marked free",
                    page_id
                )));
            }

            let page = if self.dirty_pages.contains(&page_id) {
                self.buffer_pool.get(page_id).cloned().ok_or_else(|| {
                    crate::error::HematiteError::StorageError(format!(
                        "Dirty page {} missing from buffer pool",
                        page_id
                    ))
                })?
            } else {
                self.file_manager.read_page(page_id)?
            };

            let actual_checksum = Self::calculate_page_checksum(&page);
            if actual_checksum != expected_checksum {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Page checksum mismatch for page {}: expected {}, got {}",
                    page_id, expected_checksum, actual_checksum
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

    fn checksum_store_path(db_path: &Path) -> PathBuf {
        let mut file_name = db_path
            .file_name()
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("hematite.db"));
        file_name.push(".pager_checksums");
        match db_path.parent() {
            Some(parent) => parent.join(file_name),
            None => PathBuf::from(file_name),
        }
    }

    fn journal_path(db_path: &Path) -> PathBuf {
        let mut file_name = db_path
            .file_name()
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("hematite.db"));
        file_name.push(".journal");
        match db_path.parent() {
            Some(parent) => parent.join(file_name),
            None => PathBuf::from(file_name),
        }
    }

    fn load_persisted_state(&mut self) -> Result<()> {
        let Some(path) = &self.checksum_store_path else {
            return Ok(());
        };

        let contents = match fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err.into()),
        };

        let mut lines = contents.lines();
        let version = lines
            .next()
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Missing pager checksum metadata version".to_string(),
                )
            })?
            .strip_prefix("version=")
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Pager checksum metadata is missing version prefix".to_string(),
                )
            })?
            .parse::<u32>()
            .map_err(|_| {
                crate::error::HematiteError::StorageError(
                    "Invalid pager checksum metadata version".to_string(),
                )
            })?;

        if version != Self::CHECKSUM_METADATA_VERSION {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Unsupported pager checksum metadata version: expected {}, got {}",
                Self::CHECKSUM_METADATA_VERSION,
                version
            )));
        }

        let expected_free_count = lines
            .next()
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Missing pager freelist metadata count".to_string(),
                )
            })?
            .strip_prefix("free_count=")
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Pager freelist metadata is missing count prefix".to_string(),
                )
            })?
            .parse::<usize>()
            .map_err(|_| {
                crate::error::HematiteError::StorageError(
                    "Invalid pager freelist metadata count".to_string(),
                )
            })?;

        let mut free_pages = Vec::with_capacity(expected_free_count);
        for _ in 0..expected_free_count {
            let line = lines.next().ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Pager freelist metadata ended early".to_string(),
                )
            })?;
            let page_id = line
                .strip_prefix("free|")
                .ok_or_else(|| {
                    crate::error::HematiteError::StorageError(
                        "Invalid pager freelist metadata record".to_string(),
                    )
                })?
                .parse::<u32>()
                .map(|page_id| page_id)
                .map_err(|_| {
                    crate::error::HematiteError::StorageError(
                        "Invalid pager freelist page id".to_string(),
                    )
                })?;
            free_pages.push(page_id);
        }

        let expected_count = lines
            .next()
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Missing pager checksum metadata count".to_string(),
                )
            })?
            .strip_prefix("checksum_count=")
            .ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Pager checksum metadata is missing count prefix".to_string(),
                )
            })?
            .parse::<usize>()
            .map_err(|_| {
                crate::error::HematiteError::StorageError(
                    "Invalid pager checksum metadata count".to_string(),
                )
            })?;

        let mut checksums = HashMap::new();
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let payload = line.strip_prefix("checksum|").ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Invalid pager checksum metadata record".to_string(),
                )
            })?;
            let parts = payload.split('|').collect::<Vec<_>>();
            if parts.len() != 2 {
                return Err(crate::error::HematiteError::StorageError(
                    "Invalid pager checksum metadata record".to_string(),
                ));
            }
            let page_id = parts[0].parse::<u32>().map_err(|_| {
                crate::error::HematiteError::StorageError(
                    "Invalid pager checksum page id".to_string(),
                )
            })?;
            let checksum = parts[1].parse::<u32>().map_err(|_| {
                crate::error::HematiteError::StorageError(
                    "Invalid pager checksum value".to_string(),
                )
            })?;
            if checksums.insert(page_id, checksum).is_some() {
                return Err(crate::error::HematiteError::StorageError(format!(
                    "Duplicate pager checksum entry for page {}",
                    page_id
                )));
            }
        }

        if checksums.len() != expected_count {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Pager checksum metadata count mismatch: expected {}, got {}",
                expected_count,
                checksums.len()
            )));
        }

        self.file_manager.set_free_pages(free_pages);
        self.page_checksums = checksums;
        Ok(())
    }

    fn persist_checksums(&self) -> Result<()> {
        let Some(path) = &self.checksum_store_path else {
            return Ok(());
        };

        let mut entries = self
            .page_checksums
            .iter()
            .map(|(page_id, checksum)| (*page_id, *checksum))
            .collect::<Vec<_>>();
        entries.sort_by_key(|(page_id, _)| *page_id);

        let mut lines = vec![
            format!("version={}", Self::CHECKSUM_METADATA_VERSION),
            format!("free_count={}", self.file_manager.free_pages().len()),
        ];
        for page_id in self.file_manager.free_pages() {
            lines.push(format!("free|{}", page_id));
        }
        lines.push(format!("checksum_count={}", entries.len()));
        for (page_id, checksum) in entries {
            lines.push(format!("checksum|{}|{}", page_id, checksum));
        }

        fs::write(path, lines.join("\n"))?;
        Ok(())
    }

    fn snapshot_original_page(&mut self, page_id: PageId) -> Result<()> {
        let Some(transaction) = &mut self.transaction else {
            return Ok(());
        };

        if transaction.journaled_pages.contains(&page_id) {
            return Ok(());
        }

        let page_end = 64 + ((page_id as u64 + 1) * crate::storage::PAGE_SIZE as u64);
        if page_end > transaction.original_file_len {
            return Ok(());
        }

        let page = self.file_manager.read_page(page_id)?;
        transaction.page_records.push(JournalRecord {
            page_id,
            data: page.data,
        });
        transaction.journaled_pages.insert(page_id);
        self.persist_journal(JournalState::Active)
    }

    fn persist_journal(&self, state: JournalState) -> Result<()> {
        let Some(transaction) = &self.transaction else {
            return Ok(());
        };
        let Some(path) = &self.journal_path else {
            return Ok(());
        };

        let journal = RollbackJournal {
            state,
            original_file_len: transaction.original_file_len,
            original_free_pages: transaction.original_free_pages.clone(),
            original_checksums: transaction
                .original_checksums
                .iter()
                .map(|(page_id, checksum)| (*page_id, *checksum))
                .collect(),
            page_records: transaction.page_records.clone(),
        };
        let bytes = journal.encode()?;
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(())
    }

    fn remove_journal_file(&self) -> Result<()> {
        let Some(path) = &self.journal_path else {
            return Ok(());
        };
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    fn recover_if_needed(&mut self) -> Result<()> {
        let Some(path) = &self.journal_path else {
            return Ok(());
        };
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err.into()),
        };

        let journal = RollbackJournal::decode(&bytes)?;
        match journal.state {
            JournalState::Active => {
                self.restore_from_journal(&journal)?;
                self.remove_journal_file()?;
            }
            JournalState::Committed => {
                self.remove_journal_file()?;
            }
        }
        Ok(())
    }

    fn rollback_from_active_transaction(&mut self) -> Result<()> {
        let transaction = self.transaction.clone().ok_or_else(|| {
            crate::error::HematiteError::StorageError("Pager transaction is not active".to_string())
        })?;
        let journal = RollbackJournal {
            state: JournalState::Active,
            original_file_len: transaction.original_file_len,
            original_free_pages: transaction.original_free_pages,
            original_checksums: transaction
                .original_checksums
                .into_iter()
                .collect::<Vec<_>>(),
            page_records: transaction.page_records,
        };
        self.restore_from_journal(&journal)
    }

    fn restore_from_journal(&mut self, journal: &RollbackJournal) -> Result<()> {
        self.buffer_pool = BufferPool::new(self.buffer_pool_capacity);
        self.dirty_pages.clear();
        self.file_manager
            .restore_file_len(journal.original_file_len)?;
        self.file_manager
            .set_free_pages(journal.original_free_pages.clone());

        for record in &journal.page_records {
            let page = Page::from_bytes(record.page_id, record.data.clone())?;
            self.file_manager.write_page(&page)?;
        }
        self.file_manager.flush()?;

        self.page_checksums = journal.original_checksums.iter().copied().collect();
        self.persist_checksums()
    }
}
