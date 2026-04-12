//! Overflow page chains for large payloads.
//!
//! This is the live overflow API used by the tree/value-store layer. The public helper shape stays
//! the same, but the on-page bytes now use the v3 overflow page format.

use crate::error::{HematiteError, Result};
use crate::storage::overflow_v3::{
    encode_overflow_chain, split_payload_into_overflow_chunks, V3OverflowPage,
    V3_OVERFLOW_PAYLOAD_CAPACITY,
};
use crate::storage::{PageId, Pager};
use std::collections::HashSet;

pub const OVERFLOW_CHUNK_CAPACITY: usize = V3_OVERFLOW_PAYLOAD_CAPACITY;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverflowChainReport {
    pub page_count: usize,
    pub payload_len: usize,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct OverflowReadCache {
    first_page: Option<PageId>,
    expected_len: usize,
    page_ids: Vec<PageId>,
    #[cfg(test)]
    hits: usize,
    #[cfg(test)]
    misses: usize,
}

impl OverflowReadCache {
    fn clear(&mut self) {
        self.first_page = None;
        self.expected_len = 0;
        self.page_ids.clear();
    }

    fn cached_page_ids(&mut self, first_page: Option<PageId>, expected_len: usize) -> Option<&[PageId]> {
        if self.first_page == first_page && self.expected_len == expected_len {
            #[cfg(test)]
            {
                self.hits = self.hits.saturating_add(1);
            }
            Some(&self.page_ids)
        } else {
            None
        }
    }

    fn store(&mut self, first_page: Option<PageId>, expected_len: usize, page_ids: Vec<PageId>) {
        self.first_page = first_page;
        self.expected_len = expected_len;
        self.page_ids = page_ids;
        #[cfg(test)]
        {
            self.misses = self.misses.saturating_add(1);
        }
    }

    #[cfg(test)]
    pub(crate) fn stats(&self) -> (usize, usize) {
        (self.hits, self.misses)
    }
}

pub fn write_overflow_chain(storage: &mut Pager, payload: &[u8]) -> Result<Option<PageId>> {
    if payload.is_empty() {
        return Ok(None);
    }

    let page_count = split_payload_into_overflow_chunks(payload).len();
    let mut page_ids = Vec::with_capacity(page_count);
    for _ in 0..page_count {
        page_ids.push(storage.allocate_page()?);
    }

    let pages = encode_overflow_chain(&page_ids, payload)?;
    for page in pages {
        storage.write_page(page)?;
    }
    Ok(page_ids.first().copied())
}

pub fn read_overflow_chain(
    storage: &mut Pager,
    first_page: Option<PageId>,
    expected_len: usize,
) -> Result<Vec<u8>> {
    let mut cache = OverflowReadCache::default();
    read_overflow_chain_cached_with_cache(storage, first_page, expected_len, &mut cache)
}

pub fn free_overflow_chain(storage: &mut Pager, first_page: Option<PageId>) -> Result<()> {
    let page_ids = collect_overflow_page_ids(storage, first_page)?;
    for page_id in page_ids {
        storage.deallocate_page(page_id)?;
    }
    Ok(())
}

pub fn validate_overflow_chain(
    storage: &mut Pager,
    first_page: Option<PageId>,
    expected_len: usize,
) -> Result<OverflowChainReport> {
    if first_page.is_none() {
        if expected_len == 0 {
            return Ok(OverflowChainReport {
                page_count: 0,
                payload_len: 0,
            });
        }
        return Err(HematiteError::CorruptedData(
            "Missing overflow chain head for non-empty payload".to_string(),
        ));
    }

    let mut current = match first_page {
        Some(page_id) => page_id,
        None => unreachable!("handled above"),
    };
    let mut visited = HashSet::new();
    let mut remaining = expected_len;
    let mut page_count = 0usize;
    let mut payload_len = 0usize;

    while current != 0 && remaining > 0 {
        if !visited.insert(current) {
            return Err(HematiteError::CorruptedData(
                "Overflow chain cycle detected".to_string(),
            ));
        }

        let page = storage.read_page_shared(current)?;
        let expected_chunk_len = remaining.min(OVERFLOW_CHUNK_CAPACITY);
        let decoded = V3OverflowPage::decode(page.as_ref(), expected_chunk_len)?;
        let chunk_len = decoded.payload_chunk.len();
        remaining = remaining.saturating_sub(chunk_len);
        payload_len += chunk_len;
        page_count += 1;
        current = decoded.next_page_id;
    }

    if remaining > 0 {
        return Err(HematiteError::CorruptedData(
            "Overflow chain ended before expected payload length".to_string(),
        ));
    }

    Ok(OverflowChainReport {
        page_count,
        payload_len,
    })
}

pub fn collect_overflow_page_ids(
    storage: &mut Pager,
    first_page: Option<PageId>,
) -> Result<Vec<PageId>> {
    let mut ids = Vec::new();
    let mut current = match first_page {
        Some(page_id) => page_id,
        None => return Ok(ids),
    };
    let mut visited = HashSet::new();

    while current != 0 {
        if !visited.insert(current) {
            return Err(HematiteError::CorruptedData(
                "Overflow chain cycle detected while collecting page ids".to_string(),
            ));
        }

        let page = storage.read_page_shared(current)?;
        if page.data[0] != crate::storage::format::PageKind::Overflow as u8 {
            return Err(HematiteError::CorruptedData(
                "Overflow page kind mismatch while collecting page ids".to_string(),
            ));
        }
        ids.push(current);
        current = u32::from_be_bytes([page.data[4], page.data[5], page.data[6], page.data[7]]);
    }

    Ok(ids)
}

pub(crate) fn read_overflow_chain_cached_with_cache(
    storage: &mut Pager,
    first_page: Option<PageId>,
    expected_len: usize,
    cache: &mut OverflowReadCache,
) -> Result<Vec<u8>> {
    if expected_len == 0 {
        cache.clear();
        return Ok(Vec::new());
    }

    let page_ids = if let Some(page_ids) = cache.cached_page_ids(first_page, expected_len) {
        page_ids.to_vec()
    } else {
        let page_ids = collect_overflow_page_ids(storage, first_page)?;
        cache.store(first_page, expected_len, page_ids.clone());
        page_ids
    };

    let expected_page_count =
        (expected_len + V3_OVERFLOW_PAYLOAD_CAPACITY - 1) / V3_OVERFLOW_PAYLOAD_CAPACITY;
    if page_ids.len() != expected_page_count {
        return Err(HematiteError::StorageError(format!(
            "v3 overflow chain expected {} pages but received {}",
            expected_page_count,
            page_ids.len()
        )));
    }

    let mut payload = Vec::with_capacity(expected_len);
    for (index, page_id) in page_ids.iter().copied().enumerate() {
        let page = storage.read_page_shared(page_id)?;
        let remaining = expected_len - payload.len();
        let expected_chunk_len = remaining.min(V3_OVERFLOW_PAYLOAD_CAPACITY);
        let decoded = V3OverflowPage::decode(page.as_ref(), expected_chunk_len)?;
        let expected_next_page_id = page_ids.get(index + 1).copied().unwrap_or(0);
        if decoded.next_page_id != expected_next_page_id {
            return Err(HematiteError::StorageError(format!(
                "v3 overflow page {} points to {} but expected {}",
                page_id, decoded.next_page_id, expected_next_page_id
            )));
        }
        payload.extend_from_slice(&decoded.payload_chunk);
    }

    Ok(payload)
}
