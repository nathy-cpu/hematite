/*! # Write-Ahead Logging (WAL) and Redo Recovery
//!
//! Write-Ahead Logging (WAL) is the default journaling mechanism of Hematite. It ensures transactional
//! Durability (the D in ACID) and enables Single-Writer, Multiple-Reader (SWMR) concurrency by logging
//! modifications out-of-place to a separate WAL file before checkpointing them back to the database.
//!
//! ---
//!
//! ## 1. Core Database WAL Concepts
//!
//! ### Write-Ahead Constraint
//! The fundamental rule of write-ahead logging states: *never write a database page directly to disk
//! without first flushing the log frame describing the modification to non-volatile storage.*
//! If the system crashes, the recovery coordinator can reconstruct the correct database state by
//! replaying the redo frames in the log.
//!
//! ### SWMR Concurrency
//! When WAL is enabled, writer threads write exclusively to the end of the WAL file (`.wal`). Reader threads
//! read pages from a read snapshot. When reading page $N$, the reader consults the WAL index:
//! * If page $N$ was modified in or before the reader's transaction snapshot, the reader overlays the page
//!   payload from the WAL file.
//! * If page $N$ was not modified in the WAL scope, the reader reads the base page from the main database file.
//!
//! Because readers do not access active writer offsets, writers can commit transactions without blocking
//! active read operations.
//!
//! ### Checkpointing
//! As transactions commit, the WAL file grows. To prevent the log from consuming too much disk space
//! and slowing down startup recovery, a **Checkpoint** operation is performed periodically. Checkpointing:
//! 1. Flushes modified page frames from the WAL back into their in-place offsets in the main database file.
//! 2. Truncates or resets the WAL file.
//!
//! ---
//!
//! ## 2. WAL File Layout & Specifications
//!
//! A Hematite WAL file consists of a 24-byte header followed by zero or more frames. Each frame contains a
//! 28-byte frame prefix followed by a `PAGE_SIZE` (4096 bytes) page image.
//!
//! ### WAL File Physical Layout
//!
//! ```text
//! +------------------+-----------------------------+-----------------------------+
//! |    WAL Header    |         WAL Frame 1         |         WAL Frame 2         |
//! |    (24 bytes)    | (28B prefix + 4096B payload)| (28B prefix + 4096B payload)|
//! +------------------+-----------------------------+-----------------------------+
//! ```
//!
//! ### WAL Header Layout (24 bytes)
//!
//! | Offset | Size (Bytes) | Field | Description / Value |
//! |---|---|---|---|
//! | `0` | `4` | Magic | Magic signature bytes `HTW3`. |
//! | `4` | `4` | Version | File format version. Fixed to `1`. |
//! | `8` | `2` | Page Size | Database page size. Fixed to `4096`. |
//! | `10`| `2` | Reserved | Zero-filled padding bytes. |
//! | `12`| `4` | Checkpoint Seq | Monotonically increasing checkpoint sequence number. |
//! | `16`| `4` | Salt 1 | Random salt used for validating frame checksums. |
//! | `20`| `4` | Salt 2 | Random salt used for validating frame checksums. |
//!
//! ### WAL Frame Layout (28-byte prefix + 4096-byte payload)
//!
//! | Offset | Size (Bytes) | Field | Description / Value |
//! |---|---|---|---|
//! | `0` | `4` | Page ID | Logical page ID (1-based index in the database file). |
//! | `4` | `4` | DB Page Count | Total page count of the database file after this frame's transaction commit. |
//! | `8` | `8` | Commit Sequence | Monotonically increasing transaction commit ID. |
//! | `16`| `4` | Salt 1 | Copy of Salt 1 from WAL header for frame verification. |
//! | `20`| `4` | Salt 2 | Copy of Salt 2 from WAL header for frame verification. |
//! | `24`| `4` | Checksum | CRC32 checksum computed over frame headers + page data. |
//! | `28`| `4096`| Page Data | Raw page bytes. |
//!
//! ---
//!
//! ## 3. Reader Snapshot and WAL Overlay Mapping
//!
//! ```text
//!            Reader Requests Page N
//!                      |
//!                      v
//!          +-----------+-----------+
//!          | Consult WAL Index     |
//!          | (VisibleWalState)     |
//!          +-----------+-----------+
//!                      |
//!           Does WAL contain Page N?
//!            (commit_seq <= snapshot_seq)
//!            /                   \
//!        Yes/                     \No
//!          v                       v
//!   +------+------+         +------+------+
//!   | Read frame  |         | Read page   |
//!   | from WAL    |         | from DB     |
//!   | file (.wal) |         | file (.db)  |
//!   +------+------+         +------+------+
//!          \                       /
//!           \                     /
//!            v                   v
//!           Serve reconstructed page bytes
//! ```
//!*/

use crate::error::{HematiteError, Result};
use crate::storage::metadata_page;
use crate::storage::pager::JournalMode;
use crate::storage::pager_metadata::PersistedPagerState;
use crate::storage::{
    file_len_for_next_page_id, next_page_id_for_file_len, PageId, PAGE_SIZE,
    STORAGE_METADATA_PAGE_ID,
};
#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Default capacity for the WAL page image LRU cache.
/// Only the N most recently accessed page images are held in memory;
/// all other WAL-committed pages are read from the WAL file on demand.
const WAL_PAGE_CACHE_CAPACITY: usize = 64;

/// Bounded LRU cache for WAL page images.
///
/// Holds up to `capacity` page images in memory. When a new page is
/// inserted and the cache is full, the least-recently-used entry is evicted.
/// Cache misses are serviced by reading from the WAL file at the stored
/// byte offset.
#[derive(Debug, Clone)]
pub(crate) struct WalPageCache {
    capacity: usize,
    /// Order tracks recency: front = most-recently-used, back = LRU.
    order: VecDeque<PageId>,
    entries: HashMap<PageId, Vec<u8>>,
}

impl WalPageCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            order: VecDeque::with_capacity(capacity),
            entries: HashMap::with_capacity(capacity),
        }
    }

    /// Get a page image from the cache, promoting it to most-recently-used.
    pub fn get(&mut self, page_id: PageId) -> Option<&[u8]> {
        if self.entries.contains_key(&page_id) {
            // Promote to MRU by removing and re-inserting at front
            self.order.retain(|&id| id != page_id);
            self.order.push_front(page_id);
            self.entries.get(&page_id).map(Vec::as_slice)
        } else {
            None
        }
    }

    /// Insert a page image into the cache, evicting the LRU entry if needed.
    pub fn put(&mut self, page_id: PageId, data: Vec<u8>) {
        if self.entries.contains_key(&page_id) {
            // Update existing entry and promote
            self.entries.insert(page_id, data);
            self.order.retain(|&id| id != page_id);
            self.order.push_front(page_id);
            return;
        }

        // Evict LRU if at capacity
        while self.entries.len() >= self.capacity {
            if let Some(evicted) = self.order.pop_back() {
                self.entries.remove(&evicted);
            } else {
                break;
            }
        }

        self.entries.insert(page_id, data);
        self.order.push_front(page_id);
    }

    /// Remove a specific page from the cache.
    pub fn remove(&mut self, page_id: PageId) {
        if self.entries.remove(&page_id).is_some() {
            self.order.retain(|&id| id != page_id);
        }
    }

    /// Remove all entries whose page_id does not satisfy the predicate.
    pub fn retain(&mut self, predicate: impl Fn(&PageId) -> bool) {
        self.entries.retain(|page_id, _| predicate(page_id));
        self.order.retain(|page_id| predicate(page_id));
    }

    /// Returns an iterator over all cached page IDs and their data.
    pub fn iter(&self) -> impl Iterator<Item = (&PageId, &Vec<u8>)> {
        self.entries.iter()
    }

    /// Returns the number of entries currently in the cache.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

impl PartialEq for WalPageCache {
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries
    }
}
impl Eq for WalPageCache {}

const V3_WAL_MAGIC: &[u8; 4] = b"HTW3";
const V3_WAL_VERSION: u32 = 1;
const V3_WAL_HEADER_SIZE: usize = 24;
const V3_WAL_FRAME_PREFIX_SIZE: usize = 28;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalHeader {
    pub(crate) page_size: u16,
    pub(crate) checkpoint_sequence: u32,
    pub(crate) salt_1: u32,
    pub(crate) salt_2: u32,
}

impl Default for WalHeader {
    fn default() -> Self {
        Self {
            page_size: PAGE_SIZE as u16,
            checkpoint_sequence: 0,
            salt_1: 0x48454D41,
            salt_2: 0x54495445,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFrame {
    pub page_id: PageId,
    pub data: Vec<u8>,
    pub(crate) database_page_count: PageId,
    pub(crate) commit_sequence: u64,
}

impl WalFrame {
    pub fn new(page_id: PageId, data: Vec<u8>) -> Self {
        Self {
            page_id,
            data,
            database_page_count: 0,
            commit_sequence: 0,
        }
    }

    pub(crate) fn committed(
        page_id: PageId,
        database_page_count: PageId,
        commit_sequence: u64,
        data: Vec<u8>,
    ) -> Self {
        Self {
            page_id,
            data,
            database_page_count,
            commit_sequence,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalFile {
    pub(crate) header: WalHeader,
    pub(crate) frames: Vec<WalFrame>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibleWalState {
    pub visible_sequence: u64,
    pub file_len: u64,
    pub free_pages: Vec<PageId>,
    pub free_page_set: HashSet<PageId>,
    pub page_checksums: HashMap<PageId, u32>,
    /// Byte offset into the WAL file for each overridden page.
    /// Used to read page data on-demand instead of storing full images.
    pub(crate) page_override_offsets: HashMap<PageId, u64>,
    /// Bounded LRU cache of recently-accessed WAL page images.
    pub(crate) page_cache: WalPageCache,
    /// Path to the WAL file for on-demand reads on cache miss.
    pub(crate) wal_path: Option<PathBuf>,
}

impl VisibleWalState {
    pub fn from_database_state(
        file_len: u64,
        free_pages: Vec<PageId>,
        page_checksums: HashMap<PageId, u32>,
    ) -> Self {
        let free_page_set = free_pages.iter().copied().collect();
        Self {
            visible_sequence: 0,
            file_len,
            free_pages,
            free_page_set,
            page_checksums,
            page_override_offsets: HashMap::new(),
            page_cache: WalPageCache::new(WAL_PAGE_CACHE_CAPACITY),
            wal_path: None,
        }
    }

    pub fn apply_committed_delta(
        &mut self,
        sequence: u64,
        file_len: u64,
        free_pages: Vec<PageId>,
        page_checksums: HashMap<PageId, u32>,
        frames: &[WalFrame],
    ) -> Result<()> {
        if sequence <= self.visible_sequence {
            return Err(HematiteError::StorageError(
                "WAL sequences must increase strictly".to_string(),
            ));
        }

        let visible_next_page_id = visible_next_page_id(file_len);
        let free_page_set = free_pages.iter().copied().collect::<HashSet<_>>();
        self.page_override_offsets.retain(|page_id, _| {
            *page_id < visible_next_page_id && !free_page_set.contains(page_id)
        });
        self.page_cache.retain(|page_id| {
            *page_id < visible_next_page_id && !free_page_set.contains(page_id)
        });

        for frame in frames {
            if frame.data.len() != PAGE_SIZE {
                return Err(HematiteError::StorageError(format!(
                    "WAL frame {} has invalid image size {}",
                    frame.page_id,
                    frame.data.len()
                )));
            }
            if frame.page_id >= visible_next_page_id {
                return Err(HematiteError::StorageError(format!(
                    "WAL frame {} exceeds visible page range",
                    frame.page_id
                )));
            }
            if free_page_set.contains(&frame.page_id) {
                return Err(HematiteError::StorageError(format!(
                    "WAL record {} marks page {} free and dirty",
                    sequence, frame.page_id
                )));
            }
            // Compute the byte offset where this frame's page data lives
            // in the WAL file. We calculate this from the WAL file structure:
            // frames are appended sequentially, so the offset for a newly
            // committed frame is determined by the WAL file layout.
            // For freshly committed frames, cache the data directly since
            // it's already in memory from the commit.
            self.page_cache.put(frame.page_id, frame.data.clone());
            // The offset will be set by the caller after frames are written
            // to disk. For now, mark the page as overridden with a sentinel.
            // The read path will check the cache first, then fall back to file.
            self.page_override_offsets.insert(frame.page_id, 0);
        }

        self.visible_sequence = sequence;
        self.file_len = file_len;
        self.free_pages = free_pages;
        self.free_page_set = free_page_set;
        self.page_checksums = page_checksums;
        Ok(())
    }

    pub fn visible_next_page_id(&self) -> PageId {
        visible_next_page_id(self.file_len)
    }

    pub fn contains_page(&self, page_id: PageId) -> bool {
        page_id < self.visible_next_page_id() && !self.is_page_free(page_id)
    }

    pub fn is_page_free(&self, page_id: PageId) -> bool {
        self.free_page_set.contains(&page_id)
    }

    /// Check if this state has an override for the given page.
    pub fn has_page_override(&self, page_id: PageId) -> bool {
        self.page_override_offsets.contains_key(&page_id)
    }

    /// Read a WAL-overridden page's data. Checks the in-memory LRU cache
    /// first; on cache miss, reads from the WAL file at the stored offset.
    /// Returns None if the page has no WAL override.
    pub fn read_page_data(&mut self, page_id: PageId) -> Result<Option<Vec<u8>>> {
        if !self.page_override_offsets.contains_key(&page_id) {
            return Ok(None);
        }

        // Check LRU cache first
        if let Some(data) = self.page_cache.get(page_id) {
            return Ok(Some(data.to_vec()));
        }

        // Cache miss: read from WAL file
        let Some(wal_path) = &self.wal_path else {
            return Err(HematiteError::StorageError(format!(
                "WAL page {} has override but no WAL file path for on-demand read",
                page_id
            )));
        };

        let data = read_page_from_wal_file(wal_path, page_id)?;
        match data {
            Some(page_data) => {
                self.page_cache.put(page_id, page_data.clone());
                Ok(Some(page_data))
            }
            None => {
                // Page not found in WAL file (possibly truncated after checkpoint)
                self.page_override_offsets.remove(&page_id);
                Ok(None)
            }
        }
    }

    /// Immutable read of WAL-overridden page data. Checks the LRU cache
    /// without promoting, and reads from the WAL file without caching.
    /// Used by read paths that only have `&self` (e.g. `read_page_shared`).
    pub fn read_page_data_readonly(&self, page_id: PageId) -> Result<Option<Vec<u8>>> {
        if !self.page_override_offsets.contains_key(&page_id) {
            return Ok(None);
        }

        // Check cache without promoting (peek, not get)
        if let Some(data) = self.page_cache.entries.get(&page_id) {
            return Ok(Some(data.clone()));
        }

        // Cache miss: read from WAL file (no cache update)
        let Some(wal_path) = &self.wal_path else {
            return Err(HematiteError::StorageError(format!(
                "WAL page {} has override but no WAL file path for on-demand read",
                page_id
            )));
        };

        read_page_from_wal_file(wal_path, page_id)
    }

    /// Iterate over all overridden page IDs.
    pub fn overridden_page_ids(&self) -> impl Iterator<Item = PageId> + '_ {
        self.page_override_offsets.keys().copied()
    }

    pub fn checksum_for_page(&self, page_id: PageId) -> Option<u32> {
        self.page_checksums.get(&page_id).copied()
    }
}

pub(crate) fn append_committed_frames_to_path<P: AsRef<Path>>(
    path: P,
    commit_sequence: u64,
    database_page_count: PageId,
    free_pages: &[PageId],
    checksums: &HashMap<PageId, u32>,
    base_metadata_page: &[u8],
    frames: &[WalFrame],
) -> Result<()> {
    let header = existing_or_default_header(path.as_ref())?;
    let frame_batch = synthesize_committed_frames(
        commit_sequence,
        database_page_count,
        free_pages,
        checksums,
        base_metadata_page,
        frames,
    )?;
    WalFile::append_frames_to_path(path, &header, &frame_batch)
}

pub(crate) fn load_visible_state_from_path_with_base<P: AsRef<Path>>(
    path: P,
    baseline_file_len: u64,
    baseline_free_pages: Vec<PageId>,
    baseline_checksums: HashMap<PageId, u32>,
    baseline_metadata_page: &[u8],
) -> Result<Option<VisibleWalState>> {
    let wal_path = path.as_ref().to_path_buf();
    let Some(wal) = WalFile::load_from_path(&wal_path)? else {
        return Ok(None);
    };
    let committed_groups = committed_frame_groups(&wal.frames);
    if committed_groups.is_empty() {
        return Ok(None);
    }

    let latest_sequence = committed_groups.last().unwrap()[0].commit_sequence;
    // We still need to read page data during loading for checksum computation
    // and metadata page parsing. But we store only the page IDs as override
    // markers, not the full page data.
    let (database_page_count, page_overrides_btree) =
        visible_pages_from_committed_groups(&committed_groups)?;
    let file_len = if database_page_count == 0 {
        baseline_file_len
    } else {
        file_len_for_next_page_id(database_page_count)
    };

    let metadata_frame_present = page_overrides_btree.contains_key(&STORAGE_METADATA_PAGE_ID);
    let (free_pages, mut checksums) = if metadata_frame_present {
        let metadata_page_bytes = page_overrides_btree
            .get(&STORAGE_METADATA_PAGE_ID)
            .map(Vec::as_slice)
            .unwrap_or(baseline_metadata_page);
        let persisted = match parse_metadata_page(metadata_page_bytes) {
            Ok(Some(persisted)) => persisted,
            Ok(None) => {
                return Err(HematiteError::CorruptedData(
                    "Committed WAL metadata frame is missing pager metadata".to_string(),
                ))
            }
            Err(err) => {
                return Err(HematiteError::CorruptedData(format!(
                    "Committed WAL metadata frame is malformed: {}",
                    err
                )))
            }
        };

        let mut checksums = baseline_checksums;
        checksums.extend(persisted.checksums);
        (persisted.free_pages, checksums)
    } else {
        (baseline_free_pages, baseline_checksums)
    };

    let visible_next_page_id = next_page_id_for_file_len(file_len);
    let free_page_set = free_pages.iter().copied().collect::<HashSet<_>>();

    // Compute checksums from the in-memory data (still needed for correctness)
    // but only store page IDs as override markers.
    let mut page_override_offsets = HashMap::new();
    let mut page_cache = WalPageCache::new(WAL_PAGE_CACHE_CAPACITY);

    for (page_id, bytes) in &page_overrides_btree {
        if *page_id >= visible_next_page_id || free_page_set.contains(page_id) {
            continue;
        }
        if *page_id != STORAGE_METADATA_PAGE_ID {
            checksums.insert(*page_id, page_checksum(bytes));
        }
        // Mark this page as having a WAL override (offset 0 = sentinel,
        // the read path will scan the WAL file to find the actual frame)
        page_override_offsets.insert(*page_id, 0u64);
        // Populate the LRU cache with the page data we already have
        page_cache.put(*page_id, bytes.clone());
    }

    checksums
        .retain(|page_id, _| *page_id < visible_next_page_id && !free_page_set.contains(page_id));

    Ok(Some(VisibleWalState {
        visible_sequence: latest_sequence,
        file_len,
        free_pages,
        free_page_set,
        page_checksums: checksums,
        page_override_offsets,
        page_cache,
        wal_path: Some(wal_path),
    }))
}

/// Read a specific page's data from the WAL file by scanning for the latest
/// frame that matches the given page_id. This is the on-demand read path
/// used when the WAL LRU cache misses.
///
/// Returns the page data if found, or None if the page is not in the WAL file.
fn read_page_from_wal_file(wal_path: &Path, target_page_id: PageId) -> Result<Option<Vec<u8>>> {
    let mut file = match OpenOptions::new().read(true).open(wal_path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };

    let file_len = file.metadata()?.len();
    if file_len < V3_WAL_HEADER_SIZE as u64 {
        return Ok(None);
    }

    // Read and validate the header
    let mut header_bytes = [0u8; V3_WAL_HEADER_SIZE];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut header_bytes)?;
    let header = WalHeader::decode(&header_bytes)?;

    // Scan all frames, keeping track of the latest match.
    // Frames for the same page_id can appear multiple times across different
    // transactions; the last one wins (latest commit sequence).
    let frame_size = (V3_WAL_FRAME_PREFIX_SIZE + PAGE_SIZE) as u64;
    let mut offset = V3_WAL_HEADER_SIZE as u64;
    let mut latest_data: Option<Vec<u8>> = None;

    while offset + frame_size <= file_len {
        // Read just the frame prefix to check the page_id
        let mut prefix = [0u8; V3_WAL_FRAME_PREFIX_SIZE];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut prefix)?;

        let frame_page_id = u32::from_be_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]);
        let salt_1 = u32::from_be_bytes([prefix[16], prefix[17], prefix[18], prefix[19]]);
        let salt_2 = u32::from_be_bytes([prefix[20], prefix[21], prefix[22], prefix[23]]);

        // Verify frame belongs to current WAL epoch
        if salt_1 != header.salt_1 || salt_2 != header.salt_2 {
            break; // Stale/corrupt frame, stop scanning
        }

        if frame_page_id == target_page_id {
            // Read the page data
            let mut page_data = vec![0u8; PAGE_SIZE];
            file.read_exact(&mut page_data)?;
            latest_data = Some(page_data);
        }

        offset += frame_size;
    }

    Ok(latest_data)
}

fn existing_or_default_header(path: &Path) -> Result<WalHeader> {
    Ok(WalFile::load_header_from_path(path)?.unwrap_or_default())
}

fn synthesize_committed_frames(
    commit_sequence: u64,
    database_page_count: PageId,
    free_pages: &[PageId],
    checksums: &HashMap<PageId, u32>,
    base_metadata_page: &[u8],
    frames: &[WalFrame],
) -> Result<Vec<WalFrame>> {
    let persisted = PersistedPagerState {
        journal_mode: JournalMode::Wal,
        free_pages: free_pages.to_vec(),
        checksums: checksums.clone(),
    };
    let metadata_payload = persisted.encode(1);

    let mut base_page = if let Some(frame) = frames
        .iter()
        .find(|frame| frame.page_id == STORAGE_METADATA_PAGE_ID)
    {
        frame.data.clone()
    } else {
        base_metadata_page.to_vec()
    };
    if base_page.len() != PAGE_SIZE {
        base_page = vec![0; PAGE_SIZE];
    }
    let metadata_page_bytes =
        match metadata_page::write_pager_metadata(&base_page, &metadata_payload) {
            Ok(bytes) => bytes,
            Err(HematiteError::StorageError(ref msg)) if msg.contains("exceeds page size") => {
                // Full checksum map can outgrow one reserved metadata page in WAL mode.
                // Keep free-page state, drop persisted checksum payload, reconstruct visible-page
                // checksums from WAL frames when reloading state.
                let fallback = PersistedPagerState {
                    journal_mode: JournalMode::Wal,
                    free_pages: free_pages.to_vec(),
                    checksums: HashMap::new(),
                };
                metadata_page::write_pager_metadata(&base_page, &fallback.encode(1))?
            }
            Err(err) => return Err(err),
        };

    let mut v3_frames = Vec::with_capacity(frames.len() + 1);
    for frame in frames {
        if frame.data.len() != PAGE_SIZE {
            return Err(HematiteError::StorageError(format!(
                "WAL frame {} has invalid image size {}",
                frame.page_id,
                frame.data.len()
            )));
        }
        if frame.page_id == STORAGE_METADATA_PAGE_ID {
            continue;
        }
        v3_frames.push(WalFrame::committed(
            frame.page_id,
            database_page_count,
            commit_sequence,
            frame.data.clone(),
        ));
    }

    v3_frames.push(WalFrame::committed(
        STORAGE_METADATA_PAGE_ID,
        database_page_count,
        commit_sequence,
        metadata_page_bytes,
    ));
    Ok(v3_frames)
}

fn parse_metadata_page(bytes: &[u8]) -> Result<Option<PersistedPagerState>> {
    let Some(metadata_bytes) = metadata_page::read_pager_metadata(bytes)? else {
        return Ok(None);
    };
    Ok(Some(PersistedPagerState::decode_bytes(&metadata_bytes, 1)?))
}

fn visible_next_page_id(file_len: u64) -> PageId {
    next_page_id_for_file_len(file_len)
}

fn page_checksum(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 0x811C9DC5;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

fn committed_frame_groups(frames: &[WalFrame]) -> Vec<Vec<WalFrame>> {
    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut current_sequence = None;

    for frame in frames {
        match current_sequence {
            Some(sequence) if sequence != frame.commit_sequence => {
                if current
                    .iter()
                    .any(|frame: &WalFrame| frame.page_id == STORAGE_METADATA_PAGE_ID)
                {
                    groups.push(current);
                }
                current = Vec::new();
                current_sequence = Some(frame.commit_sequence);
            }
            None => {
                current_sequence = Some(frame.commit_sequence);
            }
            _ => {}
        }
        current.push(frame.clone());
    }

    if current
        .iter()
        .any(|frame: &WalFrame| frame.page_id == STORAGE_METADATA_PAGE_ID)
    {
        groups.push(current);
    }

    groups
}

fn visible_pages_from_committed_groups(
    groups: &[Vec<WalFrame>],
) -> Result<(u32, HashMap<u32, Vec<u8>>)> {
    let mut database_page_count = 0u32;
    let mut pages = HashMap::new();

    for group in groups {
        for frame in group {
            database_page_count = database_page_count.max(frame.database_page_count);
            pages.insert(frame.page_id, frame.data.clone());
        }
    }

    Ok((database_page_count, pages))
}

impl WalHeader {
    pub(crate) fn encode(&self) -> [u8; V3_WAL_HEADER_SIZE] {
        let mut bytes = [0u8; V3_WAL_HEADER_SIZE];
        bytes[..4].copy_from_slice(V3_WAL_MAGIC);
        bytes[4..8].copy_from_slice(&V3_WAL_VERSION.to_be_bytes());
        bytes[8..10].copy_from_slice(&self.page_size.to_be_bytes());
        bytes[10..12].fill(0);
        bytes[12..16].copy_from_slice(&self.checkpoint_sequence.to_be_bytes());
        bytes[16..20].copy_from_slice(&self.salt_1.to_be_bytes());
        bytes[20..24].copy_from_slice(&self.salt_2.to_be_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < V3_WAL_HEADER_SIZE {
            return Err(HematiteError::StorageError(
                "v3 WAL header is truncated".to_string(),
            ));
        }
        if &bytes[..4] != V3_WAL_MAGIC {
            return Err(HematiteError::StorageError(
                "v3 WAL header magic mismatch".to_string(),
            ));
        }
        let version = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if version != V3_WAL_VERSION {
            return Err(HematiteError::StorageError(format!(
                "Unsupported v3 WAL version {version}"
            )));
        }

        let page_size = u16::from_be_bytes([bytes[8], bytes[9]]);
        if page_size as usize != PAGE_SIZE {
            return Err(HematiteError::StorageError(format!(
                "Unsupported v3 WAL page size {page_size}"
            )));
        }

        Ok(Self {
            page_size,
            checkpoint_sequence: u32::from_be_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
            salt_1: u32::from_be_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]),
            salt_2: u32::from_be_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]),
        })
    }
}

impl WalFrame {
    #[cfg(test)]
    pub(crate) fn encode(&self, header: &WalHeader) -> Result<Vec<u8>> {
        if self.data.len() != PAGE_SIZE {
            return Err(HematiteError::StorageError(format!(
                "v3 WAL frame for page {} has invalid image size {}",
                self.page_id,
                self.data.len()
            )));
        }

        let mut bytes = Vec::with_capacity(V3_WAL_FRAME_PREFIX_SIZE + PAGE_SIZE);
        bytes.extend_from_slice(&self.page_id.to_be_bytes());
        bytes.extend_from_slice(&self.database_page_count.to_be_bytes());
        bytes.extend_from_slice(&self.commit_sequence.to_be_bytes());
        bytes.extend_from_slice(&header.salt_1.to_be_bytes());
        bytes.extend_from_slice(&header.salt_2.to_be_bytes());
        let checksum = frame_checksum(
            self.page_id,
            self.database_page_count,
            self.commit_sequence,
            header.salt_1,
            header.salt_2,
            &self.data,
        );
        bytes.extend_from_slice(&checksum.to_be_bytes());
        bytes.extend_from_slice(&self.data);
        Ok(bytes)
    }

    pub(crate) fn write_to(&self, header: &WalHeader, file: &mut std::fs::File) -> Result<()> {
        if self.data.len() != PAGE_SIZE {
            return Err(HematiteError::StorageError(format!(
                "v3 WAL frame for page {} has invalid image size {}",
                self.page_id,
                self.data.len()
            )));
        }

        let checksum = frame_checksum(
            self.page_id,
            self.database_page_count,
            self.commit_sequence,
            header.salt_1,
            header.salt_2,
            &self.data,
        );

        let mut prefix = [0u8; V3_WAL_FRAME_PREFIX_SIZE];
        prefix[..4].copy_from_slice(&self.page_id.to_be_bytes());
        prefix[4..8].copy_from_slice(&self.database_page_count.to_be_bytes());
        prefix[8..16].copy_from_slice(&self.commit_sequence.to_be_bytes());
        prefix[16..20].copy_from_slice(&header.salt_1.to_be_bytes());
        prefix[20..24].copy_from_slice(&header.salt_2.to_be_bytes());
        prefix[24..28].copy_from_slice(&checksum.to_be_bytes());

        file.write_all(&prefix)?;
        file.write_all(&self.data)?;
        Ok(())
    }

    pub(crate) fn decode(bytes: &[u8], header: &WalHeader) -> Result<(Self, usize)> {
        if bytes.len() < V3_WAL_FRAME_PREFIX_SIZE + PAGE_SIZE {
            return Err(HematiteError::StorageError(
                "v3 WAL frame is truncated".to_string(),
            ));
        }

        let page_number = read_u32_be(bytes, 0);
        let database_page_count = read_u32_be(bytes, 4);
        let commit_sequence = read_u64_be(bytes, 8);
        let salt_1 = read_u32_be(bytes, 16);
        let salt_2 = read_u32_be(bytes, 20);
        let checksum = read_u32_be(bytes, 24);
        let page_bytes = bytes[28..28 + PAGE_SIZE].to_vec();

        if salt_1 != header.salt_1 || salt_2 != header.salt_2 {
            return Err(HematiteError::StorageError(
                "v3 WAL frame salt mismatch".to_string(),
            ));
        }

        let expected_checksum = frame_checksum(
            page_number,
            database_page_count,
            commit_sequence,
            salt_1,
            salt_2,
            &page_bytes,
        );
        if checksum != expected_checksum {
            return Err(HematiteError::StorageError(
                "v3 WAL frame checksum mismatch".to_string(),
            ));
        }

        Ok((
            Self::committed(
                page_number,
                database_page_count,
                commit_sequence,
                page_bytes,
            ),
            V3_WAL_FRAME_PREFIX_SIZE + PAGE_SIZE,
        ))
    }
}

impl WalFile {
    #[cfg(test)]
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(
            V3_WAL_HEADER_SIZE + self.frames.len() * (V3_WAL_FRAME_PREFIX_SIZE + PAGE_SIZE),
        );
        bytes.extend_from_slice(&self.header.encode());
        for frame in &self.frames {
            bytes.extend_from_slice(&frame.encode(&self.header)?);
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let header = WalHeader::decode(bytes)?;
        let mut offset = V3_WAL_HEADER_SIZE;
        let mut frames = Vec::new();
        while offset < bytes.len() {
            if bytes.len() - offset < V3_WAL_FRAME_PREFIX_SIZE + PAGE_SIZE {
                break;
            }
            let (frame, used) = WalFrame::decode(&bytes[offset..], &header)?;
            frames.push(frame);
            offset += used;
        }
        validate_frame_order(&frames)?;
        Ok(Self { header, frames })
    }

    #[cfg(test)]
    pub(crate) fn visible_pages_at(
        &self,
        commit_sequence: u64,
    ) -> Result<(u32, BTreeMap<u32, Vec<u8>>)> {
        let mut max_db_page_count = 0u32;
        let mut pages = BTreeMap::new();
        for frame in self
            .frames
            .iter()
            .filter(|frame| frame.commit_sequence <= commit_sequence)
        {
            max_db_page_count = max_db_page_count.max(frame.database_page_count);
            pages.insert(frame.page_id, frame.data.clone());
        }
        Ok((max_db_page_count, pages))
    }

    pub(crate) fn load_from_path<P: AsRef<Path>>(path: P) -> Result<Option<Self>> {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };

        Ok(Some(Self::decode(&bytes)?))
    }

    pub(crate) fn append_frames_to_path<P: AsRef<Path>>(
        path: P,
        header: &WalHeader,
        frames: &[WalFrame],
    ) -> Result<()> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        let metadata = file.metadata()?;
        if metadata.len() == 0 {
            file.write_all(&header.encode())?;
        } else if metadata.len() < V3_WAL_HEADER_SIZE as u64 {
            return Err(HematiteError::StorageError(
                "Existing v3 WAL file has a truncated header".to_string(),
            ));
        } else {
            let mut bytes = [0u8; V3_WAL_HEADER_SIZE];
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut bytes)?;
            let existing_header = WalHeader::decode(&bytes)?;
            if existing_header.page_size != header.page_size
                || existing_header.salt_1 != header.salt_1
                || existing_header.salt_2 != header.salt_2
            {
                return Err(HematiteError::StorageError(
                    "Existing v3 WAL header does not match append request".to_string(),
                ));
            }
            file.seek(SeekFrom::End(0))?;
        }

        for frame in frames {
            frame.write_to(header, &mut file)?;
        }
        file.sync_all()?;
        Ok(())
    }

    pub(crate) fn load_header_from_path<P: AsRef<Path>>(path: P) -> Result<Option<WalHeader>> {
        let mut file = match OpenOptions::new().read(true).open(path) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let metadata_len = file.metadata()?.len();
        if metadata_len == 0 {
            return Ok(None);
        }
        if metadata_len < V3_WAL_HEADER_SIZE as u64 {
            return Err(HematiteError::StorageError(
                "v3 WAL header is truncated".to_string(),
            ));
        }
        let mut bytes = [0u8; V3_WAL_HEADER_SIZE];
        file.read_exact(&mut bytes)?;
        Ok(Some(WalHeader::decode(&bytes)?))
    }
}

fn validate_frame_order(frames: &[WalFrame]) -> Result<()> {
    let mut previous_commit = 0u64;
    for frame in frames {
        if frame.commit_sequence < previous_commit {
            return Err(HematiteError::StorageError(
                "v3 WAL frames are not ordered by commit sequence".to_string(),
            ));
        }
        previous_commit = frame.commit_sequence;
    }
    Ok(())
}

fn frame_checksum(
    page_id: u32,
    database_page_count: u32,
    commit_sequence: u64,
    salt_1: u32,
    salt_2: u32,
    data: &[u8],
) -> u32 {
    let mut hash: u32 = 0x811C9DC5;
    macro_rules! feed_bytes {
        ($slice:expr) => {
            for b in $slice {
                hash ^= u32::from(*b);
                hash = hash.wrapping_mul(0x01000193);
            }
        };
    }

    feed_bytes!(&page_id.to_be_bytes());
    feed_bytes!(&database_page_count.to_be_bytes());
    feed_bytes!(&commit_sequence.to_be_bytes());
    feed_bytes!(&salt_1.to_be_bytes());
    feed_bytes!(&salt_2.to_be_bytes());
    feed_bytes!(data);

    hash
}

fn read_u32_be(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn read_u64_be(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}
