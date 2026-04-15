use crate::error::{HematiteError, Result};
use crate::storage::metadata_page;
use crate::storage::pager::JournalMode;
use crate::storage::pager_metadata::PersistedPagerState;
use crate::storage::wal_v3::{V3WalFile, V3WalFrame, V3WalHeader};
use crate::storage::{
    file_len_for_next_page_id, next_page_id_for_file_len, PageId, FIRST_ALLOCATABLE_PAGE_ID,
    PAGE_SIZE, STORAGE_METADATA_PAGE_ID,
};
use std::collections::{HashMap, HashSet};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFrame {
    pub page_id: PageId,
    pub data: Vec<u8>,
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

    #[allow(dead_code)]
    pub fn apply_record(&self, record: &WalRecord) -> Result<Self> {
        let mut next = self.clone();
        next.apply_committed_delta(
            record.sequence,
            record.file_len,
            record.free_pages.clone(),
            record.checksums.iter().copied().collect(),
            &record.frames,
        )?;
        Ok(next)
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub sequence: u64,
    pub file_len: u64,
    pub free_pages: Vec<PageId>,
    pub checksums: Vec<(PageId, u32)>,
    pub frames: Vec<WalFrame>,
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
    let frame_batch = synthesize_v3_frames(
        commit_sequence,
        database_page_count,
        free_pages,
        checksums,
        base_metadata_page,
        frames,
    )?;
    V3WalFile::append_frames_to_path(path, &header, &frame_batch)
}

pub(crate) fn load_visible_state_from_path_with_base<P: AsRef<Path>>(
    path: P,
    baseline_file_len: u64,
    baseline_free_pages: Vec<PageId>,
    baseline_checksums: HashMap<PageId, u32>,
    baseline_metadata_page: &[u8],
) -> Result<Option<VisibleWalState>> {
    let Some(wal) = V3WalFile::load_from_path(path)? else {
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
    let persisted = if metadata_frame_present {
        let metadata_page_bytes = page_overrides_btree
            .get(&STORAGE_METADATA_PAGE_ID)
            .map(Vec::as_slice)
            .unwrap_or(baseline_metadata_page);
        match parse_metadata_page(metadata_page_bytes) {
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
        }
    } else {
        PersistedPagerState {
            journal_mode: JournalMode::Wal,
            free_pages: baseline_free_pages,
            checksums: baseline_checksums,
        }
    };

    let visible_next_page_id = next_page_id_for_file_len(file_len);
    let free_page_set = persisted.free_pages.iter().copied().collect::<HashSet<_>>();
    page_overrides_btree
        .retain(|page_id, _| *page_id < visible_next_page_id && !free_page_set.contains(page_id));

    let PersistedPagerState {
        journal_mode: _,
        free_pages,
        checksums,
    } = persisted;
    let free_page_set = free_pages.iter().copied().collect();

    Ok(Some(VisibleWalState {
        visible_sequence: latest_sequence,
        file_len,
        free_pages,
        free_page_set,
        page_checksums: checksums,
        page_overrides: page_overrides_btree.into_iter().collect(),
    }))
}

impl WalRecord {
    #[cfg(test)]
    pub fn encode_file(records: &[Self]) -> Result<Vec<u8>> {
        let header = V3WalHeader::default();
        let mut metadata_page = vec![0; PAGE_SIZE];
        let mut frames = Vec::new();
        for record in records {
            let record_frames = synthesize_v3_frames(
                record.sequence,
                next_page_id_for_file_len(record.file_len),
                &record.free_pages,
                &record.checksums.iter().copied().collect(),
                &metadata_page,
                &record.frames,
            )?;
            if let Some(frame) = record_frames
                .iter()
                .find(|frame| frame.page_number == STORAGE_METADATA_PAGE_ID)
            {
                metadata_page = frame.page_bytes.clone();
            }
            frames.extend(record_frames);
        }

        V3WalFile { header, frames }.encode()
    }

    #[allow(dead_code)]
    pub fn decode_file(bytes: &[u8]) -> Result<Vec<Self>> {
        let wal = V3WalFile::decode(bytes)?;
        let mut records = Vec::new();
        let mut metadata_page = vec![0; PAGE_SIZE];
        for v3_frames in committed_frame_groups(&wal.frames) {
            let sequence = v3_frames[0].commit_sequence;
            let mut database_page_count = 0;
            let mut record_frames = Vec::new();
            for frame in &v3_frames {
                database_page_count = database_page_count.max(frame.database_page_count);
                if frame.page_number == STORAGE_METADATA_PAGE_ID {
                    metadata_page = frame.page_bytes.clone();
                    continue;
                }
                record_frames.push(WalFrame {
                    page_id: frame.page_number,
                    data: frame.page_bytes.clone(),
                });
            }

            let persisted = parse_metadata_page(&metadata_page)?.ok_or_else(|| {
                HematiteError::CorruptedData(
                    "Committed WAL metadata frame is missing pager metadata".to_string(),
                )
            })?;

            let mut checksums = persisted.checksums.into_iter().collect::<Vec<_>>();
            checksums.sort_by_key(|(page_id, _)| *page_id);

            records.push(Self {
                sequence,
                file_len: file_len_for_next_page_id(database_page_count),
                free_pages: persisted.free_pages,
                checksums,
                frames: record_frames,
            });
        }

        Self::validate_records(&records)?;
        Ok(records)
    }

    #[allow(dead_code)]
    pub fn append_to_path<P: AsRef<Path>>(path: P, record: &Self) -> Result<()> {
        append_committed_frames_to_path(
            path,
            record.sequence,
            next_page_id_for_file_len(record.file_len),
            &record.free_pages,
            &record.checksums.iter().copied().collect(),
            &vec![0; PAGE_SIZE],
            &record.frames,
        )
    }

    #[allow(dead_code)]
    pub fn load_visible_state_from_path<P: AsRef<Path>>(
        path: P,
    ) -> Result<Option<VisibleWalState>> {
        load_visible_state_from_path_with_base(
            path,
            file_len_for_next_page_id(FIRST_ALLOCATABLE_PAGE_ID),
            Vec::new(),
            HashMap::new(),
            &vec![0; PAGE_SIZE],
        )
    }

    #[allow(dead_code)]
    pub fn visible_state_from_records(records: &[Self]) -> Option<VisibleWalState> {
        let mut visible_state: Option<VisibleWalState> = None;
        for record in records {
            visible_state = Some(match &visible_state {
                Some(state) => state.apply_record(record).ok()?,
                None => VisibleWalState::from_database_state(
                    file_len_for_next_page_id(FIRST_ALLOCATABLE_PAGE_ID),
                    Vec::new(),
                    HashMap::new(),
                )
                .apply_record(record)
                .ok()?,
            });
        }
        visible_state
    }

    #[allow(dead_code)]
    fn validate_records(records: &[Self]) -> Result<()> {
        let mut previous_sequence = 0u64;
        for record in records {
            if record.sequence <= previous_sequence {
                return Err(HematiteError::StorageError(
                    "WAL sequences must increase strictly".to_string(),
                ));
            }
            previous_sequence = record.sequence;

            let mut seen_frames = HashSet::new();
            for frame in &record.frames {
                if !seen_frames.insert(frame.page_id) {
                    return Err(HematiteError::StorageError(format!(
                        "WAL record {} contains duplicate frame for page {}",
                        record.sequence, frame.page_id
                    )));
                }
            }
        }
        Ok(())
    }
}

fn existing_or_default_header(path: &Path) -> Result<V3WalHeader> {
    Ok(V3WalFile::load_header_from_path(path)?.unwrap_or_default())
}

fn synthesize_v3_frames(
    commit_sequence: u64,
    database_page_count: PageId,
    free_pages: &[PageId],
    checksums: &HashMap<PageId, u32>,
    base_metadata_page: &[u8],
    frames: &[WalFrame],
) -> Result<Vec<V3WalFrame>> {
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
    let metadata_page_bytes = metadata_page::write_pager_metadata(&base_page, &metadata_payload)?;

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
        v3_frames.push(V3WalFrame {
            page_number: frame.page_id,
            database_page_count,
            commit_sequence,
            page_bytes: frame.data.clone(),
        });
    }

    v3_frames.push(V3WalFrame {
        page_number: STORAGE_METADATA_PAGE_ID,
        database_page_count,
        commit_sequence,
        page_bytes: metadata_page_bytes,
    });
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

fn committed_frame_groups(frames: &[V3WalFrame]) -> Vec<Vec<V3WalFrame>> {
    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut current_sequence = None;

    for frame in frames {
        match current_sequence {
            Some(sequence) if sequence != frame.commit_sequence => {
                if current
                    .iter()
                    .any(|frame: &V3WalFrame| frame.page_number == STORAGE_METADATA_PAGE_ID)
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
        .any(|frame: &V3WalFrame| frame.page_number == STORAGE_METADATA_PAGE_ID)
    {
        groups.push(current);
    }

    groups
}

fn visible_pages_from_committed_groups(
    groups: &[Vec<V3WalFrame>],
) -> Result<(u32, HashMap<u32, Vec<u8>>)> {
    let mut database_page_count = 0u32;
    let mut pages = HashMap::new();

    for group in groups {
        for frame in group {
            database_page_count = database_page_count.max(frame.database_page_count);
            pages.insert(frame.page_number, frame.page_bytes.clone());
        }
    }

    Ok((database_page_count, pages))
}
