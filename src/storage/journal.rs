//! Rollback-journal semantic types and binary codec.
//!
//! All integers are big-endian. The file is a 40-byte header, the original free
//! list, the (always empty) original-checksum table, the page records, then the
//! commit intents:
//!
//! ```text
//! header   0..4 magic `HTJ1`        4..8 version (1)      8 state (1 active, 2 committed)
//!          9..11 page size (4096)   11 zero               12..16 original page count
//!          16..20 sector-size hint  20..24 checksum seed  24..28 free-page count
//!          28..32 checksum count    32..36 record count   36..40 intent count
//! free     4 bytes per page id
//! records  page id (4), checksum (4), page image (4096)
//! intents  page id (4), checksum the page must hold if the commit landed (4)
//! ```
//!
//! A zero-length file is a retired journal and means "nothing to recover".

use crate::error::{HematiteError, Result};
use crate::storage::checksum;
use crate::storage::next_page_id_for_file_len;
use crate::storage::{file_len_for_next_page_id, PageId, PAGE_SIZE};
use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::sync::Arc;

use crate::storage::stats::PagerStats;
#[cfg(test)]
use std::io::{Seek, SeekFrom};
use std::path::Path;

const JOURNAL_MAGIC: &[u8; 4] = b"HTJ1";
const JOURNAL_VERSION: u32 = 1;
const JOURNAL_HEADER_SIZE: usize = 40;
const JOURNAL_RECORD_PREFIX_SIZE: usize = 8;
const JOURNAL_RECORD_COUNT_OFFSET: u64 = 32;
const JOURNAL_INTENT_COUNT_OFFSET: u64 = 36;
/// One commit-intent entry: page id and the checksum that page must have if the
/// commit reached the disk.
const JOURNAL_INTENT_SIZE: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JournalFileState {
    Active = 1,
    Committed = 2,
}

impl JournalFileState {
    fn encode(self) -> u8 {
        self as u8
    }

    fn decode(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Active),
            2 => Ok(Self::Committed),
            _ => Err(HematiteError::StorageError(format!(
                "Unsupported rollback journal state {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JournalHeader {
    pub(crate) state: JournalFileState,
    pub(crate) page_size: u16,
    pub(crate) original_database_page_count: u32,
    pub(crate) sector_size_hint: u32,
    pub(crate) checksum_seed: u32,
    pub(crate) free_page_count: u32,
    pub(crate) checksum_count: u32,
    pub(crate) record_count: u32,
    /// Number of commit-intent entries following the page records.
    ///
    /// Non-zero only once a commit has declared its final page contents. Recovery
    /// uses it to decide whether the commit reached the disk, which is what removes
    /// the need for a separately synced "committed" flag.
    pub(crate) intent_count: u32,
}

impl Default for JournalHeader {
    fn default() -> Self {
        Self {
            state: JournalFileState::Active,
            page_size: PAGE_SIZE as u16,
            original_database_page_count: 0,
            sector_size_hint: PAGE_SIZE as u32,
            checksum_seed: 0x4A4F5552,
            free_page_count: 0,
            checksum_count: 0,
            record_count: 0,
            intent_count: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JournalFileRecord {
    pub(crate) page_number: PageId,
    pub(crate) page_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EncodedRollbackJournal {
    pub(crate) header: JournalHeader,
    pub(crate) original_free_pages: Vec<PageId>,
    pub(crate) original_checksums: Vec<(PageId, u32)>,
    pub(crate) records: Vec<JournalFileRecord>,
    pub(crate) commit_intents: Vec<(PageId, u32)>,
}

/// How a decode treats a journal whose body stops short of what its header says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Torn {
    /// Any mismatch is an error. Right for a journal this process is writing, which
    /// keeps its header in step with its body after every append.
    Reject,
    /// Keep the records up to the first one that is short or fails its checksum,
    /// keep the commit intents only if every record before them is intact, and
    /// ignore anything after that.
    KeepIntactPrefix,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalState {
    Active = 1,
    Committed = 2,
}

impl JournalState {
    #[cfg(test)]
    fn into_file_state(self) -> JournalFileState {
        match self {
            Self::Active => JournalFileState::Active,
            Self::Committed => JournalFileState::Committed,
        }
    }

    fn from_file_state(state: JournalFileState) -> Self {
        match state {
            JournalFileState::Active => Self::Active,
            JournalFileState::Committed => Self::Committed,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalRecord {
    pub page_id: PageId,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackJournal {
    pub state: JournalState,
    pub original_file_len: u64,
    pub original_free_pages: Vec<PageId>,
    pub page_records: Vec<JournalRecord>,
    /// What every page the commit touched must contain if the commit landed.
    ///
    /// Empty until a commit declares it. See [`write_commit_intents`].
    pub commit_intents: Vec<(PageId, u32)>,
}

impl RollbackJournal {
    #[cfg(test)]
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.to_encoded().encode()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(Self::from_encoded(EncodedRollbackJournal::decode(bytes)?))
    }

    /// Decodes a journal left by a process that may have died mid-write, keeping
    /// exactly what recovery can rely on.
    ///
    /// The pager writes the database file only after an fsync of the journal. So a
    /// record that did not survive intact was appended after the last sync: neither
    /// its page nor the page of any record after it has been written, and none of
    /// them is needed to undo anything. Records are read up to the first one that is
    /// short or fails its checksum, and whatever follows the last one kept — a record
    /// whose count update never landed, a torn tail — is ignored.
    ///
    /// Commit intents are kept only if every record before them is intact. A commit
    /// syncs its intents together with all of its records before writing anything,
    /// so a damaged record means the commit never started writing; dropping the
    /// intents makes recovery roll back, which is then right.
    ///
    /// The header and free list are still read strictly. They are written together
    /// before any record, so a journal the database depends on has them intact, and
    /// one that does not parse is treated as corruption rather than guessed at.
    pub fn decode_after_crash(bytes: &[u8]) -> Result<Self> {
        Ok(Self::from_encoded(
            EncodedRollbackJournal::decode_after_crash(bytes)?,
        ))
    }

    #[cfg(test)]
    fn to_encoded(&self) -> EncodedRollbackJournal {
        EncodedRollbackJournal {
            header: JournalHeader {
                state: self.state.into_file_state(),
                original_database_page_count: next_page_id_for_file_len(self.original_file_len),
                ..JournalHeader::default()
            },
            original_free_pages: self.original_free_pages.clone(),
            // Always empty: each page carries its own checksum in its trailer, so
            // the journal has no per-page checksums to save.
            original_checksums: Vec::new(),
            records: self
                .page_records
                .iter()
                .map(|record| JournalFileRecord {
                    page_number: record.page_id,
                    page_bytes: record.data.clone(),
                })
                .collect(),
            commit_intents: self.commit_intents.clone(),
        }
    }

    fn from_encoded(journal: EncodedRollbackJournal) -> Self {
        Self {
            state: JournalState::from_file_state(journal.header.state),
            original_file_len: file_len_for_next_page_id(
                journal.header.original_database_page_count,
            ),
            original_free_pages: journal.original_free_pages,
            page_records: journal
                .records
                .into_iter()
                .map(|record| JournalRecord {
                    page_id: record.page_number,
                    data: record.page_bytes,
                })
                .collect(),
            commit_intents: journal.commit_intents,
        }
    }
}

/// The rollback journal's file handle.
///
/// A thin wrapper over `File` in release builds. In test builds it also remembers
/// what the file contained at the last `sync_all`, so [`JournalFile::lose_unsynced_writes`]
/// can throw away everything written since — which is what a power loss does.
///
/// This exists because no ordinary test can tell a synced write from an unsynced one.
/// Dropping a `Pager` is a process exit: the kernel still has the unsynced bytes and
/// hands them straight back on reopen. Moving the journal sync after the page writes,
/// which breaks the write-ahead rule outright, therefore leaves every process-level
/// recovery test passing. Only discarding the unsynced bytes exposes it.
#[derive(Debug)]
pub(crate) struct JournalFile {
    file: File,
    /// Offset the next append lands at.
    ///
    /// Tracked so every write can be positional. Without it, updating the record
    /// count in the header costs a seek back, a four-byte write and a seek forward
    /// again — three syscalls to change one field, once per journalled page.
    len: u64,
    /// Reused buffer for framing a record before it is written.
    ///
    /// A record is an 8-byte prefix followed by a whole page, and it has to reach
    /// the file as one write. Framing it here rather than into a fresh `Vec` keeps
    /// that from being an allocation and a second copy of the page per record.
    scratch: Vec<u8>,
    /// The seed this journal's records are checksummed with, as written in its header.
    ///
    /// Fresh per journal. Retiring a journal truncates it without an fsync, so after a
    /// power loss the previous transaction's records can still sit at the offsets the
    /// current one writes to. Under a shared seed they would verify, and recovery
    /// would restore pre-images of a transaction that had already committed.
    checksum_seed: u32,
    /// Shared with the pager, so journal traffic lands in the same counter set as
    /// database-file traffic. Every write through this handle adds to
    /// `journal_bytes`; a counter nothing increments would let any assertion on it
    /// pass.
    stats: Arc<PagerStats>,
    /// Contents as of the last successful `sync_all`.
    ///
    /// Re-read whole rather than tracked incrementally: journals in tests are a few
    /// pages, and reading the file back is obviously correct where replaying a list
    /// of pending writes would be another thing to get wrong.
    #[cfg(test)]
    durable: Vec<u8>,
}

impl JournalFile {
    fn new(file: File, stats: Arc<PagerStats>) -> Self {
        Self {
            file,
            len: 0,
            scratch: Vec::new(),
            checksum_seed: JournalHeader::default().checksum_seed,
            stats,
            #[cfg(test)]
            durable: Vec::new(),
        }
    }

    /// Appends at the end of the journal, without a cursor.
    pub(crate) fn append(&mut self, bytes: &[u8]) -> Result<()> {
        write_all_at(&mut self.file, self.len, bytes)?;
        PagerStats::add(&self.stats.journal_bytes, bytes.len() as u64);
        self.len += bytes.len() as u64;
        Ok(())
    }

    /// Overwrites a fixed-position header field.
    pub(crate) fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<()> {
        write_all_at(&mut self.file, offset, bytes)?;
        PagerStats::add(&self.stats.journal_bytes, bytes.len() as u64);
        self.len = self.len.max(offset + bytes.len() as u64);
        Ok(())
    }

    /// Empties the journal, which is how a finished transaction retires it.
    ///
    /// An empty file means "no journal" to recovery, so this stands in for
    /// deleting it — and the file, and the open handle, survive to be reused by
    /// the next transaction. Deleting it would cost an `unlink` here and an
    /// `openat` plus a truncating `open` at the next `BEGIN`, three syscalls per
    /// commit, and `unlink` is the most expensive of them.
    ///
    /// Unsynced, as an `unlink` would be. A crash before it
    /// reaches the disk leaves a journal that looks live, and the commit intents
    /// recorded in it are what tell recovery the commit had already landed.
    pub(crate) fn retire(&mut self) -> Result<()> {
        // Already empty: the last transaction retired it and nothing has been
        // written since, so there is nothing for the kernel to do.
        if self.len == 0 {
            return Ok(());
        }
        self.set_len(0)
    }

    pub(crate) fn set_len(&mut self, len: u64) -> Result<()> {
        self.file.set_len(len)?;
        self.len = len;
        Ok(())
    }

    pub(crate) fn sync_all(&mut self) -> Result<()> {
        self.file.sync_all()?;
        #[cfg(test)]
        {
            use std::io::Read;
            let mut durable = Vec::new();
            self.file.seek(SeekFrom::Start(0))?;
            self.file.read_to_end(&mut durable)?;
            self.durable = durable;
        }
        Ok(())
    }

    /// Discards every write since the last `sync_all`, as a power loss would.
    #[cfg(test)]
    pub(crate) fn lose_unsynced_writes(&mut self) -> Result<()> {
        self.file.set_len(self.durable.len() as u64)?;
        write_all_at(&mut self.file, 0, &self.durable)?;
        self.file.sync_all()?;
        self.len = self.durable.len() as u64;
        Ok(())
    }
}

pub(crate) fn initialize_journal_file(
    path: &Path,
    original_file_len: u64,
    original_free_pages: &[PageId],
    original_checksums: &[(PageId, u32)],
    stats: Arc<PagerStats>,
    reuse: Option<JournalFile>,
) -> Result<(JournalFile, u64)> {
    let header = JournalHeader {
        state: JournalFileState::Active,
        original_database_page_count: next_page_id_for_file_len(original_file_len),
        checksum_seed: crate::storage::checksum::fresh_nonce() as u32,
        free_page_count: original_free_pages.len() as u32,
        checksum_count: original_checksums.len() as u32,
        record_count: 0,
        ..JournalHeader::default()
    };

    let mut bytes = Vec::with_capacity(
        JOURNAL_HEADER_SIZE + original_free_pages.len() * 4 + original_checksums.len() * 8,
    );
    bytes.extend_from_slice(&header.encode());
    for page_id in original_free_pages {
        bytes.extend_from_slice(&page_id.to_be_bytes());
    }
    for (page_id, checksum) in original_checksums {
        bytes.extend_from_slice(&page_id.to_be_bytes());
        bytes.extend_from_slice(&checksum.to_be_bytes());
    }

    let header_len = bytes.len() as u64;
    // Reuse the handle from the last transaction when there is one. Reopening would
    // cost an `openat` per `BEGIN` to arrive at a file this pager already has open.
    let mut journal = match reuse {
        Some(mut journal) => {
            journal.retire()?;
            journal
        }
        None => {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .truncate(true)
                .open(path)?;
            JournalFile::new(file, stats)
        }
    };
    journal.checksum_seed = header.checksum_seed;
    journal.append(&bytes)?;
    Ok((journal, header_len))
}

/// Frames one page pre-image and appends it.
///
/// The framing goes into the journal's own scratch buffer rather than a fresh
/// `Vec`. Building a `JournalFileRecord` first would cost a copy of the page to fill
/// it and another to encode it, on top of the copy that produced `record` — three
/// copies of 4 KiB to write one page, once per journalled page.
pub(crate) fn append_journal_record(file: &mut JournalFile, record: &JournalRecord) -> Result<()> {
    if record.data.len() != PAGE_SIZE {
        return Err(HematiteError::StorageError(format!(
            "rollback journal record for page {} has invalid image size {}",
            record.page_id,
            record.data.len()
        )));
    }
    let checksum = record_checksum(record.page_id, file.checksum_seed, &record.data);

    let mut scratch = std::mem::take(&mut file.scratch);
    scratch.clear();
    scratch.reserve(JOURNAL_RECORD_PREFIX_SIZE + PAGE_SIZE);
    scratch.extend_from_slice(&record.page_id.to_be_bytes());
    scratch.extend_from_slice(&checksum.to_be_bytes());
    scratch.extend_from_slice(&record.data);
    let result = file.append(&scratch);
    file.scratch = scratch;
    result
}

pub(crate) fn write_journal_record_count(file: &mut JournalFile, record_count: u32) -> Result<()> {
    file.write_at(JOURNAL_RECORD_COUNT_OFFSET, &record_count.to_be_bytes())
}

/// Appends the commit intent: what every page the commit touches must contain if
/// the commit reached the disk.
///
/// This is what keeps a commit to two fsyncs. The alternative is a "committed"
/// flag written into the journal header and synced, purely so recovery can tell a
/// completed commit from an abandoned one — a whole fsync spent on one byte.
///
/// With the intent recorded, recovery answers the same question by reading: if every
/// listed page already holds its post-commit checksum, the commit's writes reached
/// the disk and the journal can be discarded; if any does not, the commit was partial
/// and the pre-images are replayed.
///
/// Comparing against the *post* image rather than asking "did this page change" is
/// what makes it sound. A page whose new contents happen to equal its old contents is
/// indistinguishable by the latter test, and a torn page matches neither image, so it
/// is correctly treated as a partial commit.
pub(crate) fn write_commit_intents(
    file: &mut JournalFile,
    intents: &[(PageId, u32)],
) -> Result<()> {
    let mut bytes = Vec::with_capacity(intents.len() * JOURNAL_INTENT_SIZE);
    for (page_id, checksum) in intents {
        bytes.extend_from_slice(&page_id.to_be_bytes());
        bytes.extend_from_slice(&checksum.to_be_bytes());
    }
    file.append(&bytes)?;
    file.write_at(
        JOURNAL_INTENT_COUNT_OFFSET,
        &(intents.len() as u32).to_be_bytes(),
    )
}

impl JournalHeader {
    pub(crate) fn encode(&self) -> [u8; JOURNAL_HEADER_SIZE] {
        let mut bytes = [0u8; JOURNAL_HEADER_SIZE];
        bytes[..4].copy_from_slice(JOURNAL_MAGIC);
        bytes[4..8].copy_from_slice(&JOURNAL_VERSION.to_be_bytes());
        bytes[8] = self.state.encode();
        bytes[9..11].copy_from_slice(&self.page_size.to_be_bytes());
        bytes[11] = 0;
        bytes[12..16].copy_from_slice(&self.original_database_page_count.to_be_bytes());
        bytes[16..20].copy_from_slice(&self.sector_size_hint.to_be_bytes());
        bytes[20..24].copy_from_slice(&self.checksum_seed.to_be_bytes());
        bytes[24..28].copy_from_slice(&self.free_page_count.to_be_bytes());
        bytes[28..32].copy_from_slice(&self.checksum_count.to_be_bytes());
        bytes[32..36].copy_from_slice(&self.record_count.to_be_bytes());
        bytes[36..40].copy_from_slice(&self.intent_count.to_be_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < JOURNAL_HEADER_SIZE {
            return Err(HematiteError::StorageError(
                "rollback journal header is truncated".to_string(),
            ));
        }
        if &bytes[..4] != JOURNAL_MAGIC {
            return Err(HematiteError::StorageError(
                "rollback journal header magic mismatch".to_string(),
            ));
        }

        let version = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if version != JOURNAL_VERSION {
            return Err(HematiteError::StorageError(format!(
                "Unsupported rollback journal version {version}"
            )));
        }

        let page_size = u16::from_be_bytes([bytes[9], bytes[10]]);
        if page_size as usize != PAGE_SIZE {
            return Err(HematiteError::StorageError(format!(
                "Unsupported rollback journal page size {page_size}"
            )));
        }

        Ok(Self {
            state: JournalFileState::decode(bytes[8])?,
            page_size,
            original_database_page_count: read_u32_be(bytes, 12),
            sector_size_hint: read_u32_be(bytes, 16),
            checksum_seed: read_u32_be(bytes, 20),
            free_page_count: read_u32_be(bytes, 24),
            checksum_count: read_u32_be(bytes, 28),
            record_count: read_u32_be(bytes, 32),
            intent_count: read_u32_be(bytes, 36),
        })
    }
}

impl JournalFileRecord {
    /// Only the whole-journal encoder needs this; the live append path frames
    /// records straight into the journal's scratch buffer.
    #[cfg(test)]
    pub(crate) fn encode(&self, checksum_seed: u32) -> Result<Vec<u8>> {
        if self.page_bytes.len() != PAGE_SIZE {
            return Err(HematiteError::StorageError(format!(
                "rollback journal record for page {} has invalid image size {}",
                self.page_number,
                self.page_bytes.len()
            )));
        }

        let checksum = record_checksum(self.page_number, checksum_seed, &self.page_bytes);
        let mut bytes = Vec::with_capacity(JOURNAL_RECORD_PREFIX_SIZE + PAGE_SIZE);
        bytes.extend_from_slice(&self.page_number.to_be_bytes());
        bytes.extend_from_slice(&checksum.to_be_bytes());
        bytes.extend_from_slice(&self.page_bytes);
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8], checksum_seed: u32) -> Result<(Self, usize)> {
        if bytes.len() < JOURNAL_RECORD_PREFIX_SIZE + PAGE_SIZE {
            return Err(HematiteError::StorageError(
                "rollback journal record is truncated".to_string(),
            ));
        }

        let page_number = read_u32_be(bytes, 0);
        let checksum = read_u32_be(bytes, 4);
        let page_bytes = bytes[8..8 + PAGE_SIZE].to_vec();
        let expected = record_checksum(page_number, checksum_seed, &page_bytes);
        if checksum != expected {
            return Err(HematiteError::StorageError(
                "rollback journal record checksum mismatch".to_string(),
            ));
        }

        Ok((
            Self {
                page_number,
                page_bytes,
            },
            JOURNAL_RECORD_PREFIX_SIZE + PAGE_SIZE,
        ))
    }
}

impl EncodedRollbackJournal {
    #[cfg(test)]
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        validate_record_set(&self.records)?;

        let header = JournalHeader {
            free_page_count: self.original_free_pages.len() as u32,
            checksum_count: self.original_checksums.len() as u32,
            record_count: self.records.len() as u32,
            intent_count: self.commit_intents.len() as u32,
            ..self.header.clone()
        };
        let mut bytes = Vec::with_capacity(
            JOURNAL_HEADER_SIZE
                + self.original_free_pages.len() * std::mem::size_of::<PageId>()
                + self.original_checksums.len()
                    * (std::mem::size_of::<PageId>() + std::mem::size_of::<u32>())
                + self.records.len() * (JOURNAL_RECORD_PREFIX_SIZE + PAGE_SIZE),
        );
        bytes.extend_from_slice(&header.encode());

        for page_id in &self.original_free_pages {
            bytes.extend_from_slice(&page_id.to_be_bytes());
        }

        for (page_id, checksum) in &self.original_checksums {
            bytes.extend_from_slice(&page_id.to_be_bytes());
            bytes.extend_from_slice(&checksum.to_be_bytes());
        }

        for record in &self.records {
            bytes.extend_from_slice(&record.encode(header.checksum_seed)?);
        }

        for (page_id, checksum) in &self.commit_intents {
            bytes.extend_from_slice(&page_id.to_be_bytes());
            bytes.extend_from_slice(&checksum.to_be_bytes());
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        Self::decode_as(bytes, Torn::Reject)
    }

    /// Decodes a journal a crash may have cut short. See
    /// [`RollbackJournal::decode_after_crash`].
    pub(crate) fn decode_after_crash(bytes: &[u8]) -> Result<Self> {
        Self::decode_as(bytes, Torn::KeepIntactPrefix)
    }

    fn decode_as(bytes: &[u8], torn: Torn) -> Result<Self> {
        let header = JournalHeader::decode(bytes)?;
        let mut offset = JOURNAL_HEADER_SIZE;

        let mut original_free_pages = Vec::with_capacity(header.free_page_count as usize);
        for _ in 0..header.free_page_count {
            if offset + 4 > bytes.len() {
                return Err(HematiteError::StorageError(
                    "rollback journal freelist metadata is truncated".to_string(),
                ));
            }
            original_free_pages.push(read_u32_be(bytes, offset));
            offset += 4;
        }

        let mut original_checksums = Vec::with_capacity(header.checksum_count as usize);
        for _ in 0..header.checksum_count {
            if offset + 8 > bytes.len() {
                return Err(HematiteError::StorageError(
                    "rollback journal checksum metadata is truncated".to_string(),
                ));
            }
            original_checksums.push((read_u32_be(bytes, offset), read_u32_be(bytes, offset + 4)));
            offset += 8;
        }

        let mut records = Vec::with_capacity(header.record_count as usize);
        let mut records_intact = true;
        for _ in 0..header.record_count {
            match JournalFileRecord::decode(&bytes[offset..], header.checksum_seed) {
                Ok((record, used)) => {
                    records.push(record);
                    offset += used;
                }
                Err(_) if torn == Torn::KeepIntactPrefix => {
                    records_intact = false;
                    break;
                }
                Err(err) => return Err(err),
            }
        }

        let mut commit_intents = Vec::with_capacity(header.intent_count as usize);
        if records_intact {
            for _ in 0..header.intent_count {
                if offset + JOURNAL_INTENT_SIZE > bytes.len() {
                    if torn == Torn::KeepIntactPrefix {
                        commit_intents.clear();
                        break;
                    }
                    return Err(HematiteError::StorageError(
                        "rollback journal commit intent is truncated".to_string(),
                    ));
                }
                commit_intents.push((read_u32_be(bytes, offset), read_u32_be(bytes, offset + 4)));
                offset += JOURNAL_INTENT_SIZE;
            }
        }

        if torn == Torn::Reject && offset != bytes.len() {
            return Err(HematiteError::StorageError(
                "rollback journal has trailing bytes".to_string(),
            ));
        }

        validate_record_set(&records)?;
        Ok(Self {
            header,
            original_free_pages,
            original_checksums,
            records,
            commit_intents,
        })
    }
}

fn validate_record_set(records: &[JournalFileRecord]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for record in records {
        if !seen.insert(record.page_number) {
            return Err(HematiteError::StorageError(format!(
                "rollback journal contains duplicate page {}",
                record.page_number
            )));
        }
    }
    Ok(())
}

fn record_checksum(page_number: PageId, checksum_seed: u32, page_bytes: &[u8]) -> u32 {
    // The record header goes through the hash seed rather than being concatenated
    // with the page image, which would need a 4104-byte scratch `Vec` for every
    // journalled page.
    let mut header = [0u8; 8];
    header[0..4].copy_from_slice(&page_number.to_be_bytes());
    header[4..8].copy_from_slice(&checksum_seed.to_be_bytes());
    checksum::hash32(checksum::hash64(0, &header), page_bytes)
}

fn read_u32_be(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

/// Positional write, looping until the whole buffer lands.
///
/// Mirrors the database file's helper: no shared cursor, so no `seek` syscall per
/// write and no ordering dependency between the journal's writers.
fn write_all_at(file: &mut File, offset: u64, buf: &[u8]) -> Result<()> {
    #[cfg(unix)]
    use std::os::unix::fs::FileExt;
    #[cfg(windows)]
    use std::os::windows::fs::FileExt;

    #[cfg(any(unix, windows))]
    {
        let mut written = 0usize;
        while written < buf.len() {
            #[cfg(unix)]
            let count = file.write_at(&buf[written..], offset + written as u64)?;
            #[cfg(windows)]
            let count = file.seek_write(&buf[written..], offset + written as u64)?;
            if count == 0 {
                return Err(HematiteError::IoError(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write whole journal buffer",
                )));
            }
            written += count;
        }
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(buf)?;
        Ok(())
    }
}
