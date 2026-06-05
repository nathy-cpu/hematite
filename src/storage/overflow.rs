//! Overflow page chains for large payloads.
//!
//! This is the live overflow API used by the tree/value-store layer. The public helper shape stays
//! the same, but the on-page bytes now use the v3 overflow page format.

use crate::error::{HematiteError, Result};
use crate::storage::format::PageKind;
use crate::storage::Page;
use crate::storage::{PageId, Pager};
use std::collections::HashSet;

const OVERFLOW_HEADER_SIZE: usize = 8;
pub const OVERFLOW_PAYLOAD_CAPACITY: usize = crate::storage::PAGE_SIZE - OVERFLOW_HEADER_SIZE;
pub const OVERFLOW_CHUNK_CAPACITY: usize = OVERFLOW_PAYLOAD_CAPACITY;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OverflowPage {
    pub(crate) next_page_id: u32,
    pub(crate) payload_chunk: Vec<u8>,
}

impl OverflowPage {
    pub(crate) fn encode(&self, page_id: PageId) -> Result<Page> {
        if self.payload_chunk.len() > OVERFLOW_PAYLOAD_CAPACITY {
            return Err(HematiteError::StorageError(format!(
                "overflow payload chunk {} exceeds capacity {}",
                self.payload_chunk.len(),
                OVERFLOW_PAYLOAD_CAPACITY
            )));
        }

        let mut page = Page::new(page_id);
        page.data[0] = PageKind::Overflow as u8;
        page.data[1..4].fill(0);
        page.data[4..8].copy_from_slice(&self.next_page_id.to_be_bytes());
        page.data[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + self.payload_chunk.len()]
            .copy_from_slice(&self.payload_chunk);
        Ok(page)
    }

    pub(crate) fn decode(page: &Page, expected_chunk_len: usize) -> Result<Self> {
        if page.data[0] != PageKind::Overflow as u8 {
            return Err(HematiteError::StorageError(
                "overflow page kind mismatch".to_string(),
            ));
        }
        if expected_chunk_len > OVERFLOW_PAYLOAD_CAPACITY {
            return Err(HematiteError::StorageError(format!(
                "overflow expected chunk length {} exceeds capacity {}",
                expected_chunk_len, OVERFLOW_PAYLOAD_CAPACITY
            )));
        }

        Ok(Self {
            next_page_id: read_u32_be(&page.data, 4),
            payload_chunk: page.data
                [OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + expected_chunk_len]
                .to_vec(),
        })
    }
}

pub(crate) fn split_payload_into_overflow_chunks(payload: &[u8]) -> Vec<Vec<u8>> {
    if payload.is_empty() {
        return Vec::new();
    }

    payload
        .chunks(OVERFLOW_PAYLOAD_CAPACITY)
        .map(|chunk| chunk.to_vec())
        .collect()
}

pub(crate) fn encode_overflow_chain(page_ids: &[PageId], payload: &[u8]) -> Result<Vec<Page>> {
    let chunks = split_payload_into_overflow_chunks(payload);
    if chunks.len() != page_ids.len() {
        return Err(HematiteError::StorageError(format!(
            "overflow chain needs {} page ids but received {}",
            chunks.len(),
            page_ids.len()
        )));
    }

    let mut pages = Vec::with_capacity(chunks.len());
    for (index, (page_id, payload_chunk)) in page_ids.iter().zip(chunks.into_iter()).enumerate() {
        let next_page_id = page_ids.get(index + 1).copied().unwrap_or(0);
        pages.push(
            OverflowPage {
                next_page_id,
                payload_chunk,
            }
            .encode(*page_id)?,
        );
    }
    Ok(pages)
}

fn read_u32_be(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

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

    fn cached_page_ids(
        &mut self,
        first_page: Option<PageId>,
        expected_len: usize,
    ) -> Option<&[PageId]> {
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

pub fn free_overflow_chain(storage: &mut Pager, first_page: Option<PageId>) -> Result<()> {
    let page_ids = collect_overflow_page_ids(storage, first_page)?;
    for page_id in page_ids {
        storage.deallocate_page(page_id)?;
    }
    Ok(())
}

pub fn validate_overflow_chain(
    storage: &Pager,
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
        let decoded = OverflowPage::decode(page.as_ref(), expected_chunk_len)?;
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
    if current != 0 {
        return Err(HematiteError::CorruptedData(
            "Overflow chain has trailing pages beyond expected payload length".to_string(),
        ));
    }

    Ok(OverflowChainReport {
        page_count,
        payload_len,
    })
}

pub fn collect_overflow_page_ids(
    storage: &Pager,
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
    storage: &Pager,
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

    let expected_page_count = expected_len.div_ceil(OVERFLOW_PAYLOAD_CAPACITY);
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
        let expected_chunk_len = remaining.min(OVERFLOW_PAYLOAD_CAPACITY);
        let decoded = OverflowPage::decode(page.as_ref(), expected_chunk_len)?;
        let expected_next_page_id = page_ids.get(index + 1).copied().unwrap_or(0);
        if decoded.next_page_id != expected_next_page_id {
            return Err(HematiteError::StorageError(format!(
                "overflow page {} points to {} but expected {}",
                page_id, decoded.next_page_id, expected_next_page_id
            )));
        }
        payload.extend_from_slice(&decoded.payload_chunk);
    }

    Ok(payload)
}

/// Read a sub-range `[offset .. offset+len)` from an overflow chain without
/// materializing the entire payload. Pages before the target range are skipped
/// (only the 8-byte header is read to follow the chain). Pages after the target
/// range are not touched at all.
///
/// `total_len` is the full logical payload length (used for bounds checking).
pub(crate) fn read_overflow_slice(
    storage: &Pager,
    first_page: Option<PageId>,
    total_len: usize,
    offset: usize,
    len: usize,
) -> Result<Vec<u8>> {
    if len == 0 || total_len == 0 {
        return Ok(Vec::new());
    }
    let end = offset.saturating_add(len).min(total_len);
    if offset >= total_len {
        return Ok(Vec::new());
    }
    let actual_len = end - offset;

    let start_page_index = offset / OVERFLOW_PAYLOAD_CAPACITY;
    let start_offset_in_page = offset % OVERFLOW_PAYLOAD_CAPACITY;

    // Walk the chain, skipping pages before the target range.
    let mut current = match first_page {
        Some(id) => id,
        None => return Ok(Vec::new()),
    };
    let mut page_index = 0usize;
    let mut visited = HashSet::new();

    while page_index < start_page_index {
        if !visited.insert(current) {
            return Err(HematiteError::CorruptedData(
                "Overflow chain cycle detected during slice read".to_string(),
            ));
        }
        let page = storage.read_page_shared(current)?;
        if page.data[0] != PageKind::Overflow as u8 {
            return Err(HematiteError::CorruptedData(
                "Overflow page kind mismatch during slice skip".to_string(),
            ));
        }
        current = read_u32_be(&page.data, 4);
        if current == 0 {
            return Err(HematiteError::CorruptedData(
                "Overflow chain ended before reaching slice start".to_string(),
            ));
        }
        page_index += 1;
    }

    // Now read from the target pages.
    let mut result = Vec::with_capacity(actual_len);
    let mut remaining = actual_len;
    let mut first_chunk_offset = start_offset_in_page;

    while remaining > 0 && current != 0 {
        if !visited.insert(current) {
            return Err(HematiteError::CorruptedData(
                "Overflow chain cycle detected during slice read".to_string(),
            ));
        }
        let page = storage.read_page_shared(current)?;
        // Compute how many payload bytes are on this page.
        let global_offset = page_index * OVERFLOW_PAYLOAD_CAPACITY;
        let page_payload_len = (total_len - global_offset).min(OVERFLOW_PAYLOAD_CAPACITY);

        let read_start = first_chunk_offset;
        let read_end = page_payload_len.min(read_start + remaining);
        if read_end > read_start {
            let data_start = OVERFLOW_HEADER_SIZE + read_start;
            let data_end = OVERFLOW_HEADER_SIZE + read_end;
            result.extend_from_slice(&page.data[data_start..data_end]);
            remaining -= read_end - read_start;
        }

        current = read_u32_be(&page.data, 4);
        page_index += 1;
        first_chunk_offset = 0; // Only the first page may have a non-zero offset.
    }

    Ok(result)
}

/// Cached variant of `read_overflow_slice`. If the `OverflowReadCache` already
/// has the page ID list, skips directly to the target page via index lookup
/// (O(1) seek instead of O(k) chain walk).
pub(crate) fn read_overflow_slice_cached(
    storage: &Pager,
    first_page: Option<PageId>,
    total_len: usize,
    offset: usize,
    len: usize,
    cache: &mut OverflowReadCache,
) -> Result<Vec<u8>> {
    if len == 0 || total_len == 0 {
        return Ok(Vec::new());
    }
    let end = offset.saturating_add(len).min(total_len);
    if offset >= total_len {
        return Ok(Vec::new());
    }
    let actual_len = end - offset;

    let start_page_index = offset / OVERFLOW_PAYLOAD_CAPACITY;

    // Try to use cached page IDs for O(1) seek.
    let page_ids = if let Some(ids) = cache.cached_page_ids(first_page, total_len) {
        Some(ids.to_vec())
    } else {
        let ids = collect_overflow_page_ids(storage, first_page)?;
        cache.store(first_page, total_len, ids.clone());
        Some(ids)
    };

    if let Some(page_ids) = page_ids {
        let mut result = Vec::with_capacity(actual_len);
        let mut remaining = actual_len;
        let mut chunk_offset = offset % OVERFLOW_PAYLOAD_CAPACITY;
        let mut idx = start_page_index;

        while remaining > 0 && idx < page_ids.len() {
            let page = storage.read_page_shared(page_ids[idx])?;
            let global_offset = idx * OVERFLOW_PAYLOAD_CAPACITY;
            let page_payload_len = (total_len - global_offset).min(OVERFLOW_PAYLOAD_CAPACITY);

            let read_start = chunk_offset;
            let read_end = page_payload_len.min(read_start + remaining);
            if read_end > read_start {
                let data_start = OVERFLOW_HEADER_SIZE + read_start;
                let data_end = OVERFLOW_HEADER_SIZE + read_end;
                result.extend_from_slice(&page.data[data_start..data_end]);
                remaining -= read_end - read_start;
            }

            idx += 1;
            chunk_offset = 0;
        }

        return Ok(result);
    }

    // Fallback to non-cached version.
    read_overflow_slice(storage, first_page, total_len, offset, len)
}
