//! Rollback-journal compatibility types backed by the v3 journal format.
//!
//! The in-memory pager code still talks about `RollbackJournal`, `JournalRecord`, and
//! `JournalState`, but their on-disk encoding is now delegated to `journal_v3.rs`.

use crate::error::Result;
use crate::storage::journal_v3::{
    V3JournalHeader, V3JournalRecord, V3JournalState, V3RollbackJournal,
};
use crate::storage::{file_len_for_next_page_id, next_page_id_for_file_len, PageId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalState {
    Active = 1,
    Committed = 2,
}

impl JournalState {
    fn into_v3(self) -> V3JournalState {
        match self {
            Self::Active => V3JournalState::Active,
            Self::Committed => V3JournalState::Committed,
        }
    }

    fn from_v3(state: V3JournalState) -> Self {
        match state {
            V3JournalState::Active => Self::Active,
            V3JournalState::Committed => Self::Committed,
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
    pub original_checksums: Vec<(PageId, u32)>,
    pub page_records: Vec<JournalRecord>,
}

impl RollbackJournal {
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.into_v3().encode()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(Self::from_v3(V3RollbackJournal::decode(bytes)?))
    }

    fn into_v3(&self) -> V3RollbackJournal {
        V3RollbackJournal {
            header: V3JournalHeader {
                state: self.state.into_v3(),
                original_database_page_count: next_page_id_for_file_len(self.original_file_len),
                ..V3JournalHeader::default()
            },
            original_free_pages: self.original_free_pages.clone(),
            original_checksums: self.original_checksums.clone(),
            records: self
                .page_records
                .iter()
                .map(|record| V3JournalRecord {
                    page_number: record.page_id,
                    page_bytes: record.data.clone(),
                })
                .collect(),
        }
    }

    fn from_v3(journal: V3RollbackJournal) -> Self {
        Self {
            state: JournalState::from_v3(journal.header.state),
            original_file_len: file_len_for_next_page_id(journal.header.original_database_page_count),
            original_free_pages: journal.original_free_pages,
            original_checksums: journal.original_checksums,
            page_records: journal
                .records
                .into_iter()
                .map(|record| JournalRecord {
                    page_id: record.page_number,
                    data: record.page_bytes,
                })
                .collect(),
        }
    }
}
