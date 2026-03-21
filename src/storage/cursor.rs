//! Storage-layer cursor abstractions.

use crate::storage::StoredRow;

#[derive(Debug, Clone)]
pub struct TableCursor {
    rows: Vec<StoredRow>,
    position: Option<usize>,
}

impl TableCursor {
    pub fn new(mut rows: Vec<StoredRow>) -> Self {
        rows.sort_by_key(|row| row.row_id);
        Self {
            rows,
            position: None,
        }
    }

    pub fn first(&mut self) -> bool {
        if self.rows.is_empty() {
            self.position = None;
            return false;
        }
        self.position = Some(0);
        true
    }

    pub fn next(&mut self) -> bool {
        let Some(position) = self.position else {
            return false;
        };
        let next = position + 1;
        if next < self.rows.len() {
            self.position = Some(next);
            true
        } else {
            self.position = None;
            false
        }
    }

    pub fn seek_rowid(&mut self, rowid: u64) -> bool {
        let found = self
            .rows
            .binary_search_by_key(&rowid, |row| row.row_id)
            .ok();
        self.position = found;
        found.is_some()
    }

    pub fn current(&self) -> Option<&StoredRow> {
        self.position.and_then(|index| self.rows.get(index))
    }

    pub fn is_valid(&self) -> bool {
        self.current().is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    pub key: Vec<u8>,
    pub row_id: u64,
}

#[derive(Debug, Clone)]
pub struct IndexCursor {
    entries: Vec<IndexEntry>,
    position: Option<usize>,
}

impl IndexCursor {
    pub fn new(mut entries: Vec<IndexEntry>) -> Self {
        entries.sort_by(|l, r| l.key.cmp(&r.key).then(l.row_id.cmp(&r.row_id)));
        Self {
            entries,
            position: None,
        }
    }

    pub fn first(&mut self) -> bool {
        if self.entries.is_empty() {
            self.position = None;
            return false;
        }
        self.position = Some(0);
        true
    }

    pub fn next(&mut self) -> bool {
        let Some(position) = self.position else {
            return false;
        };
        let next = position + 1;
        if next < self.entries.len() {
            self.position = Some(next);
            true
        } else {
            self.position = None;
            false
        }
    }

    pub fn seek_key(&mut self, key: &[u8]) -> bool {
        let found = self
            .entries
            .binary_search_by(|entry| entry.key.as_slice().cmp(key))
            .ok();
        self.position = found;
        found.is_some()
    }

    pub fn current(&self) -> Option<&IndexEntry> {
        self.position.and_then(|index| self.entries.get(index))
    }

    pub fn is_valid(&self) -> bool {
        self.current().is_some()
    }
}
