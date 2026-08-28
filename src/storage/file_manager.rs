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
use crate::storage::free_list::FreeList;
use crate::storage::stats::PagerStats;
use crate::storage::{
    file_len_for_next_page_id, next_page_id_for_file_len, Page, PageId, DB_HEADER_PAGE_ID,
    FIRST_ALLOCATABLE_PAGE_ID, PAGE_SIZE, PAGE_TRAILER_SIZE, STORAGE_METADATA_PAGE_ID,
    USABLE_PAGE_SIZE,
};
use std::fs::{File, OpenOptions};
#[cfg(any(test, not(any(unix, windows))))]
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

#[cfg(not(any(unix, windows)))]
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

#[derive(Debug)]
pub struct FileManager {
    backend: FileBackend,
    next_page_id: u32,
    /// The database file's length, as this manager last left it.
    ///
    /// The pager is the only writer of this file, so its length is always something
    /// this process just did. Reading it back from the OS is a `statx` — one per
    /// freed page if `deallocate_page` had to ask.
    file_len: u64,
    free_list: FreeList,
    /// Shared with the `Pager` that owns this backend, so both halves of the
    /// storage layer report into one set of counters.
    stats: Arc<PagerStats>,
    #[cfg(test)]
    fail_on_write_countdown: Option<usize>,
    /// Contents of the database file as of the last successful `flush`.
    ///
    /// Lets a test discard writes that were never synced, which is what a power loss
    /// does and what no process-level test can otherwise observe — dropping the
    /// pager is a process exit, and the kernel hands unsynced bytes straight back.
    #[cfg(test)]
    durable: Vec<u8>,
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
            next_page_id: 0,
            file_len: 0,
            free_list: FreeList::new(),
            stats: Arc::new(PagerStats::default()),
            #[cfg(test)]
            fail_on_write_countdown: None,
            #[cfg(test)]
            durable: Vec::new(),
        };

        let mut manager = manager;
        manager.initialize()?;
        Ok(manager)
    }

    pub fn new_in_memory() -> Result<Self> {
        let manager = Self {
            backend: FileBackend::Memory(Vec::new()),
            next_page_id: 0,
            file_len: 0,
            free_list: FreeList::new(),
            stats: Arc::new(PagerStats::default()),
            #[cfg(test)]
            fail_on_write_countdown: None,
            #[cfg(test)]
            durable: Vec::new(),
        };

        let mut manager = manager;
        manager.initialize()?;
        Ok(manager)
    }

    fn initialize(&mut self) -> Result<()> {
        let file_len = self.stat_len()?;
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
        self.file_len = file_len;
        Ok(())
    }

    /// Re-reads the file's length from the OS, for the one case where this process
    /// is not the only thing that has touched it: recovery, which may have replayed
    /// a journal written by a previous process.
    pub fn sync_with_disk(&mut self) -> Result<()> {
        let file_len = self.stat_len()?;
        self.load_existing_file(file_len)
    }

    /// The counter set this backend reports into, for the `Pager` to share.
    pub(crate) fn stats(&self) -> Arc<PagerStats> {
        Arc::clone(&self.stats)
    }

    pub fn read_page(&self, page_id: PageId) -> Result<Page> {
        // Unzeroed: `read_exact_at` fills the whole buffer or returns an error, so
        // the zeros were overwritten without ever being read. Zeroing a 4 KiB page
        // is about 4 100 instructions, and it happened on every page read.
        self.read_page_with_buf(
            page_id,
            crate::storage::types::page_pool::take_for_overwrite(),
        )
    }

    /// Read a page from disk into a caller-supplied buffer, reusing its allocation.
    /// The buffer must have a capacity of at least `PAGE_SIZE`; it will be resized.
    /// Whether a page read back matches the checksum in its trailer.
    ///
    /// A page that has never been written carries a zero trailer, which is what a
    /// freshly extended file gives. Those are reported as unverifiable rather than
    /// corrupt: there is nothing to check them against yet.
    pub(crate) fn page_trailer_state(page: &Page) -> PageTrailerState {
        let stored = &page.data[USABLE_PAGE_SIZE..];
        if stored.iter().all(|&byte| byte == 0) {
            return PageTrailerState::Unstamped;
        }
        if stored == page_trailer(page.id, &page.data[..USABLE_PAGE_SIZE]) {
            PageTrailerState::Valid
        } else {
            PageTrailerState::Mismatch
        }
    }

    /// Reads a page's bytes into a caller-owned buffer, without building a `Page`
    /// or checking its trailer.
    ///
    /// For a read whose only purpose is to compare the bytes against something the
    /// caller already has. Nothing is trusted on the strength of this read, so
    /// nothing is verified; a caller that goes on to *use* the contents reads it
    /// again through [`Self::read_page`], which does.
    pub(crate) fn read_page_content_into(&self, page_id: PageId, data: &mut [u8]) -> Result<()> {
        if data.len() != PAGE_SIZE {
            return Err(crate::error::HematiteError::InvalidPage(page_id));
        }
        let offset = Self::page_offset(page_id)?;
        PagerStats::bump(&self.stats.page_reads);
        self.read_exact_at(offset, data).map_err(|err| {
            crate::error::HematiteError::StorageError(format!(
                "Failed to read page {page_id} at offset {offset}: {err}"
            ))
        })
    }

    pub(crate) fn read_page_with_buf(&self, page_id: PageId, mut data: Vec<u8>) -> Result<Page> {
        let offset = Self::page_offset(page_id)?;
        PagerStats::bump(&self.stats.page_reads);
        data.resize(PAGE_SIZE, 0);
        self.read_exact_at(offset, &mut data).map_err(|err| {
            crate::error::HematiteError::StorageError(format!(
                "Failed to read page {} at offset {}: {}",
                page_id, offset, err
            ))
        })?;
        let page = Page::from_bytes(page_id, data)?;
        // Unstamped pages are pages this pager has never written -- a freshly extended
        // file reads as zeros -- so there is nothing to check them against.
        PagerStats::bump(&self.stats.trailer_verifications);
        if Self::page_trailer_state(&page) == PageTrailerState::Mismatch {
            return Err(crate::error::HematiteError::CorruptedData(format!(
                "Page {page_id} does not match the checksum in its trailer"
            )));
        }
        Ok(page)
    }

    /// Writes a page and stamps its trailer with a checksum of its own contents.
    ///
    /// The checksum covers `..USABLE_PAGE_SIZE` and is seeded with the page id, so a
    /// page written to the wrong offset fails verification rather than reading as
    /// valid. The trailer bytes in `page.data` are ignored: the checksum lives on disk
    /// only, and nothing above the pager may write there.
    ///
    /// Two positional writes rather than one. `pwritev` would make it one syscall but
    /// is not in `std`, and seeking to use the vectored path would cost the syscall it
    /// saved.
    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        self.write_page_data(page.id, &page.data)
    }

    /// Writes a page from bytes that are not already inside a [`Page`].
    ///
    /// Journal replay holds its pre-images as plain byte vectors, and building a
    /// `Page` around each one to call [`Self::write_page`] copied all 4 KiB for
    /// nothing.
    pub(crate) fn write_page_data(&mut self, page_id: PageId, data: &[u8]) -> Result<()> {
        if data.len() < USABLE_PAGE_SIZE {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Page {page_id} image is {} bytes, short of the usable {USABLE_PAGE_SIZE}",
                data.len()
            )));
        }
        let offset = Self::page_offset(page_id)?;
        PagerStats::bump(&self.stats.page_writes);
        let content = &data[..USABLE_PAGE_SIZE];
        let trailer = page_trailer(page_id, content);
        self.write_all_at(offset, content)?;
        self.write_all_at(offset + USABLE_PAGE_SIZE as u64, &trailer)
    }

    pub fn allocate_page(&mut self) -> Result<PageId> {
        // Try to reuse a free page first
        if let Some(free_page_id) = self.free_list.pop_free_page() {
            Ok(free_page_id)
        } else {
            let page_id = self.next_page_id;
            self.next_page_id += 1;

            let page = Page::new(page_id);
            self.write_page(&page)?;

            Ok(page_id)
        }
    }

    pub fn flush(&mut self) -> Result<()> {
        match &mut self.backend {
            FileBackend::Disk { file, .. } => {
                file.sync_all()?;
                PagerStats::bump(&self.stats.fsyncs);
            }
            FileBackend::Memory(_) => {}
        }
        #[cfg(test)]
        self.snapshot_durable_contents()?;
        Ok(())
    }

    /// Records what is now durable, for `lose_unsynced_writes` to restore to.
    ///
    /// Reads the file back rather than replaying a list of pending writes: databases
    /// in tests are a handful of pages, and this is obviously correct where tracking
    /// deltas would be one more thing to get wrong.
    #[cfg(test)]
    fn snapshot_durable_contents(&mut self) -> Result<()> {
        use std::io::Read;
        match &mut self.backend {
            FileBackend::Disk { file } => {
                let mut durable = Vec::new();
                let position = file.stream_position()?;
                file.seek(SeekFrom::Start(0))?;
                file.read_to_end(&mut durable)?;
                file.seek(SeekFrom::Start(position))?;
                self.durable = durable;
            }
            FileBackend::Memory(buffer) => self.durable = buffer.clone(),
        }
        Ok(())
    }

    /// Discards every database write since the last `flush`, as a power loss would.
    #[cfg(test)]
    pub(crate) fn lose_unsynced_writes(&mut self) -> Result<()> {
        let durable = std::mem::take(&mut self.durable);
        match &mut self.backend {
            FileBackend::Disk { file } => {
                file.set_len(durable.len() as u64)?;
                file.seek(SeekFrom::Start(0))?;
                file.write_all(&durable)?;
                file.sync_all()?;
            }
            FileBackend::Memory(buffer) => *buffer = durable.clone(),
        }
        self.file_len = durable.len() as u64;
        self.durable = durable;
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

    /// Removes `page_id` from the free list if present, in O(1) amortized
    /// time. Prefer this over `free_pages().to_vec()` + manual scan +
    /// `set_free_pages()`, which clones the whole list and rebuilds its
    /// position index just to drop one entry.
    pub(crate) fn remove_free_page(&mut self, page_id: PageId) -> bool {
        self.free_list.remove_page(page_id)
    }

    pub fn is_free_page(&self, page_id: PageId) -> bool {
        self.free_list.contains(page_id)
    }

    pub fn file_len(&self) -> Result<u64> {
        Ok(self.len())
    }

    pub fn restore_file_len(&mut self, len: u64) -> Result<()> {
        self.set_len(len)?;
        self.next_page_id = next_page_id_for_file_len(len);
        Ok(())
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
            file_len: self.len(),
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
        let current_len = self.len();

        if target_len < current_len {
            self.set_len(target_len)?;
        }

        Ok(())
    }

    fn page_offset(page_id: PageId) -> Result<u64> {
        Ok(page_id as u64 * PAGE_SIZE as u64)
    }

    fn len(&self) -> u64 {
        self.file_len
    }

    /// The file's length according to the OS, at the cost of a `statx`.
    ///
    /// Only open and recovery need this. Everything else reads [`Self::len`], which
    /// is maintained by the write paths below.
    fn stat_len(&self) -> Result<u64> {
        PagerStats::bump(&self.stats.file_stats);
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

    /// Writes at an absolute offset without moving a shared file position.
    ///
    /// Reads already used positional I/O; writes went through `seek` + `write_all`,
    /// which cost an extra syscall per page and made every write path depend on a
    /// mutable cursor that nothing else was allowed to disturb.
    fn write_all_at(&mut self, offset: u64, buf: &[u8]) -> Result<()> {
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
            FileBackend::Disk { file } => {
                write_all_at_position(file, offset, buf)?;
            }
            FileBackend::Memory(buffer) => {
                let offset = offset as usize;
                let end = offset + buf.len();
                if end > buffer.len() {
                    buffer.resize(end, 0);
                }
                buffer[offset..end].copy_from_slice(buf);
            }
        }
        // A positional write past the end extends the file, so this is one of the
        // two places the length can change.
        self.file_len = self.file_len.max(offset + buf.len() as u64);
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
        self.file_len = len;
        Ok(())
    }
}

/// Positional write, looping until the whole buffer lands.
///
/// On platforms without positional I/O this falls back to seeking a private handle
/// rather than the shared one, so it still leaves no cursor behind.
fn write_all_at_position(file: &mut File, offset: u64, buf: &[u8]) -> std::io::Result<()> {
    #[cfg(any(unix, windows))]
    {
        let mut written = 0usize;
        while written < buf.len() {
            #[cfg(unix)]
            let count = file.write_at(&buf[written..], offset + written as u64)?;
            #[cfg(windows)]
            let count = file.seek_write(&buf[written..], offset + written as u64)?;
            if count == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            written += count;
        }
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(buf)
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

/// A page's trailer bytes: a checksum of its contents, seeded with its id.
fn page_trailer(page_id: PageId, content: &[u8]) -> [u8; PAGE_TRAILER_SIZE] {
    crate::storage::checksum::hash32(page_id as u64, content).to_le_bytes()
}

/// Writes a page's trailer into its own buffer.
///
/// `write_page` stamps as it writes and leaves the in-memory buffer alone, because
/// stamping there would mean mutating a cached page. A WAL frame is different: the
/// frame *is* the page image that a reader will later verify, so the trailer has to be
/// in the bytes the frame carries. Without it, WAL-resident pages would be
/// unverifiable until a checkpoint wrote them to the file.
pub(crate) fn stamp_page_trailer(page: &mut Page) {
    let trailer = page_trailer(page.id, &page.data[..USABLE_PAGE_SIZE]);
    page.data[USABLE_PAGE_SIZE..].copy_from_slice(&trailer);
}

/// What a page's trailer says about the page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PageTrailerState {
    /// Contents match the stored checksum.
    Valid,
    /// Never written by this pager — a freshly extended file reads as zeros.
    Unstamped,
    /// Contents do not match the stored checksum.
    Mismatch,
}
