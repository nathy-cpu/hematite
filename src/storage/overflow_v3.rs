#![allow(dead_code)]

use crate::error::{HematiteError, Result};
use crate::storage::format::PageKind;
use crate::storage::{Page, PageId, PAGE_SIZE};

pub(crate) const V3_OVERFLOW_HEADER_SIZE: usize = 8;
pub(crate) const V3_OVERFLOW_PAYLOAD_CAPACITY: usize = PAGE_SIZE - V3_OVERFLOW_HEADER_SIZE;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V3OverflowPage {
    pub(crate) next_page_id: u32,
    pub(crate) payload_chunk: Vec<u8>,
}

impl V3OverflowPage {
    pub(crate) fn encode(&self, page_id: PageId) -> Result<Page> {
        if self.payload_chunk.len() > V3_OVERFLOW_PAYLOAD_CAPACITY {
            return Err(HematiteError::StorageError(format!(
                "v3 overflow payload chunk {} exceeds capacity {}",
                self.payload_chunk.len(),
                V3_OVERFLOW_PAYLOAD_CAPACITY
            )));
        }

        let mut page = Page::new(page_id);
        page.data[0] = PageKind::Overflow as u8;
        page.data[1..4].fill(0);
        page.data[4..8].copy_from_slice(&self.next_page_id.to_be_bytes());
        page.data[V3_OVERFLOW_HEADER_SIZE..V3_OVERFLOW_HEADER_SIZE + self.payload_chunk.len()]
            .copy_from_slice(&self.payload_chunk);
        Ok(page)
    }

    pub(crate) fn decode(page: &Page, expected_chunk_len: usize) -> Result<Self> {
        if page.data[0] != PageKind::Overflow as u8 {
            return Err(HematiteError::StorageError(
                "v3 overflow page kind mismatch".to_string(),
            ));
        }
        if expected_chunk_len > V3_OVERFLOW_PAYLOAD_CAPACITY {
            return Err(HematiteError::StorageError(format!(
                "v3 overflow expected chunk length {} exceeds capacity {}",
                expected_chunk_len,
                V3_OVERFLOW_PAYLOAD_CAPACITY
            )));
        }

        Ok(Self {
            next_page_id: read_u32_be(&page.data, 4),
            payload_chunk: page.data
                [V3_OVERFLOW_HEADER_SIZE..V3_OVERFLOW_HEADER_SIZE + expected_chunk_len]
                .to_vec(),
        })
    }
}

pub(crate) fn split_payload_into_overflow_chunks(payload: &[u8]) -> Vec<Vec<u8>> {
    if payload.is_empty() {
        return Vec::new();
    }

    payload
        .chunks(V3_OVERFLOW_PAYLOAD_CAPACITY)
        .map(|chunk| chunk.to_vec())
        .collect()
}

pub(crate) fn encode_overflow_chain(page_ids: &[PageId], payload: &[u8]) -> Result<Vec<Page>> {
    let chunks = split_payload_into_overflow_chunks(payload);
    if chunks.len() != page_ids.len() {
        return Err(HematiteError::StorageError(format!(
            "v3 overflow chain needs {} page ids but received {}",
            chunks.len(),
            page_ids.len()
        )));
    }

    let mut pages = Vec::with_capacity(chunks.len());
    for (index, (page_id, payload_chunk)) in page_ids.iter().zip(chunks.into_iter()).enumerate() {
        let next_page_id = page_ids.get(index + 1).copied().unwrap_or(0);
        pages.push(
            V3OverflowPage {
                next_page_id,
                payload_chunk,
            }
            .encode(*page_id)?,
        );
    }
    Ok(pages)
}

pub(crate) fn decode_overflow_chain(pages: &[Page], expected_len: usize) -> Result<Vec<u8>> {
    if expected_len == 0 {
        return Ok(Vec::new());
    }

    let expected_page_count =
        (expected_len + V3_OVERFLOW_PAYLOAD_CAPACITY - 1) / V3_OVERFLOW_PAYLOAD_CAPACITY;
    if pages.len() != expected_page_count {
        return Err(HematiteError::StorageError(format!(
            "v3 overflow chain expected {} pages but received {}",
            expected_page_count,
            pages.len()
        )));
    }

    let mut payload = Vec::with_capacity(expected_len);
    for (index, page) in pages.iter().enumerate() {
        let remaining = expected_len - payload.len();
        let expected_chunk_len = remaining.min(V3_OVERFLOW_PAYLOAD_CAPACITY);
        let decoded = V3OverflowPage::decode(page, expected_chunk_len)?;
        let expected_next_page_id = pages.get(index + 1).map(|page| page.id).unwrap_or(0);
        if decoded.next_page_id != expected_next_page_id {
            return Err(HematiteError::StorageError(format!(
                "v3 overflow page {} points to {} but expected {}",
                page.id, decoded.next_page_id, expected_next_page_id
            )));
        }
        payload.extend_from_slice(&decoded.payload_chunk);
    }

    Ok(payload)
}

fn read_u32_be(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

#[cfg(test)]
mod tests {
    use super::{
        decode_overflow_chain, encode_overflow_chain, split_payload_into_overflow_chunks,
        V3OverflowPage, V3_OVERFLOW_PAYLOAD_CAPACITY,
    };
    use crate::storage::Page;

    #[test]
    fn overflow_chunk_split_uses_full_intermediate_pages() {
        let payload = vec![0x7A; V3_OVERFLOW_PAYLOAD_CAPACITY * 2 + 19];
        let chunks = split_payload_into_overflow_chunks(&payload);

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), V3_OVERFLOW_PAYLOAD_CAPACITY);
        assert_eq!(chunks[1].len(), V3_OVERFLOW_PAYLOAD_CAPACITY);
        assert_eq!(chunks[2].len(), 19);
    }

    #[test]
    fn overflow_chain_roundtrip() {
        let payload = vec![0x44; V3_OVERFLOW_PAYLOAD_CAPACITY + 37];
        let pages = encode_overflow_chain(&[8, 9], &payload).unwrap();
        let decoded = decode_overflow_chain(&pages, payload.len()).unwrap();

        assert_eq!(decoded, payload);
    }

    #[test]
    fn overflow_page_rejects_wrong_kind() {
        let mut page = Page::new(5);
        page.data[0] = 0xFF;

        let error = V3OverflowPage::decode(&page, 10).unwrap_err();
        assert!(
            error.to_string().contains("v3 overflow page kind mismatch"),
            "unexpected error: {error}"
        );
    }
}
