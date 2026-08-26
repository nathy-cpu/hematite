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
/// An overflow page is a database page like any other, so its payload stops short of
/// the pager's trailer.
pub const OVERFLOW_PAYLOAD_CAPACITY: usize =
    crate::storage::USABLE_PAGE_SIZE - OVERFLOW_HEADER_SIZE;
pub const OVERFLOW_CHUNK_CAPACITY: usize = OVERFLOW_PAYLOAD_CAPACITY;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OverflowPage {
    pub(crate) next_page_id: u32,
    pub(crate) payload_chunk: Vec<u8>,
}

impl OverflowPage {
    pub(crate) fn encode(&self, page_id: PageId, storage: &mut Pager) -> Result<Page> {
        if self.payload_chunk.len() > OVERFLOW_PAYLOAD_CAPACITY {
            return Err(HematiteError::StorageError(format!(
                "overflow payload chunk {} exceeds capacity {}",
                self.payload_chunk.len(),
                OVERFLOW_PAYLOAD_CAPACITY
            )));
        }

        let mut page = storage.new_page_buffer(page_id)?;
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

pub(crate) fn encode_overflow_chain(
    page_ids: &[PageId],
    payload: &[u8],
    storage: &mut Pager,
) -> Result<Vec<Page>> {
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
            .encode(*page_id, storage)?,
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

pub fn write_overflow_chain(storage: &mut Pager, payload: &[u8]) -> Result<Option<PageId>> {
    if payload.is_empty() {
        return Ok(None);
    }

    let page_count = split_payload_into_overflow_chunks(payload).len();
    let mut page_ids = Vec::with_capacity(page_count);
    for _ in 0..page_count {
        page_ids.push(storage.allocate_page()?);
    }

    let pages = encode_overflow_chain(&page_ids, payload, storage)?;
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

/// Reads a spilled payload, visiting each overflow page once.
///
/// This used to walk the chain twice — `collect_overflow_page_ids` to learn the page
/// ids, then again to read the payload out of each — and memoise the id list to take
/// the edge off. Appending as it walks halves the page reads on a cold chain and
/// makes the memo pointless, so it is gone.
pub(crate) fn read_overflow_chain(
    storage: &Pager,
    first_page: Option<PageId>,
    expected_len: usize,
) -> Result<Vec<u8>> {
    if expected_len == 0 {
        return Ok(Vec::new());
    }

    let mut payload = Vec::with_capacity(expected_len);
    let mut current = first_page.unwrap_or(0);

    while payload.len() < expected_len {
        if current == 0 {
            return Err(HematiteError::CorruptedData(format!(
                "Overflow chain ended after {} of {} bytes",
                payload.len(),
                expected_len
            )));
        }

        let page = storage.read_page_shared(current)?;
        let remaining = expected_len - payload.len();
        let decoded =
            OverflowPage::decode(page.as_ref(), remaining.min(OVERFLOW_PAYLOAD_CAPACITY))?;
        payload.extend_from_slice(&decoded.payload_chunk);
        current = decoded.next_page_id;
    }

    // A cycle cannot spin forever: every hop appends a chunk, so the loop is bounded
    // by the expected length. It shows up here instead, as a chain that still has
    // pages after the payload is complete.
    if current != 0 {
        return Err(HematiteError::CorruptedData(
            "Overflow chain continues past the end of the payload".to_string(),
        ));
    }

    Ok(payload)
}
