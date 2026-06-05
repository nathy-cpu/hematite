use crate::error::{HematiteError, Result};
use crate::storage::metadata_page;
use crate::storage::pager::JournalMode;
use crate::storage::pager_metadata::PersistedPagerState;
use crate::storage::{
    file_len_for_next_page_id, next_page_id_for_file_len, PageId,
    PAGE_SIZE, STORAGE_METADATA_PAGE_ID,
};
#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

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
    pub page_overrides: HashMap<PageId, Vec<u8>>,
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
            page_overrides: HashMap::new(),
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
        self.page_overrides.retain(|page_id, _| {
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
            self.page_overrides
                .insert(frame.page_id, frame.data.clone());
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

    pub fn page_bytes(&self, page_id: PageId) -> Option<&[u8]> {
        self.page_overrides.get(&page_id).map(Vec::as_slice)
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
    let Some(wal) = WalFile::load_from_path(path)? else {
        return Ok(None);
    };
    let committed_groups = committed_frame_groups(&wal.frames);
    if committed_groups.is_empty() {
        return Ok(None);
    }

    let latest_sequence = committed_groups.last().unwrap()[0].commit_sequence;
    let (database_page_count, mut page_overrides_btree) =
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
    page_overrides_btree
        .retain(|page_id, _| *page_id < visible_next_page_id && !free_page_set.contains(page_id));
    checksums
        .retain(|page_id, _| *page_id < visible_next_page_id && !free_page_set.contains(page_id));
    for (page_id, bytes) in &page_overrides_btree {
        if *page_id != STORAGE_METADATA_PAGE_ID {
            checksums.insert(*page_id, page_checksum(bytes));
        }
    }

    Ok(Some(VisibleWalState {
        visible_sequence: latest_sequence,
        file_len,
        free_pages,
        free_page_set,
        page_checksums: checksums,
        page_overrides: page_overrides_btree.into_iter().collect(),
    }))
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
