//! Overflow page chain support for large table-cell payloads.

use crate::error::{HematiteError, Result};
use crate::storage::{Page, PageId, Pager, PAGE_SIZE};

pub const OVERFLOW_MAGIC: &[u8; 4] = b"OVR1";
pub const OVERFLOW_HEADER_SIZE: usize = 12; // magic(4) + next_page_id(4) + chunk_len(4)
pub const OVERFLOW_CHUNK_CAPACITY: usize = PAGE_SIZE - OVERFLOW_HEADER_SIZE;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverflowChainReport {
    pub page_count: usize,
    pub payload_len: usize,
}

pub fn write_overflow_chain(storage: &mut Pager, payload: &[u8]) -> Result<Option<PageId>> {
    if payload.is_empty() {
        return Ok(None);
    }

    let mut pages = Vec::new();
    let mut offset = 0usize;

    while offset < payload.len() {
        let page_id = storage.allocate_page()?;
        pages.push(page_id);
        let take = OVERFLOW_CHUNK_CAPACITY.min(payload.len() - offset);
        offset += take;
    }

    let mut cursor = 0usize;
    let mut payload_offset = 0usize;
    while cursor < pages.len() {
        let page_id = pages[cursor];
        let next_page = if cursor + 1 < pages.len() {
            pages[cursor + 1]
        } else {
            PageId::invalid()
        };
        let chunk_len = OVERFLOW_CHUNK_CAPACITY.min(payload.len() - payload_offset);

        let mut page = Page::new(page_id);
        page.data[0..4].copy_from_slice(OVERFLOW_MAGIC);
        page.data[4..8].copy_from_slice(&next_page.as_u32().to_le_bytes());
        page.data[8..12].copy_from_slice(&(chunk_len as u32).to_le_bytes());
        page.data[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + chunk_len]
            .copy_from_slice(&payload[payload_offset..payload_offset + chunk_len]);
        storage.write_page(page)?;

        payload_offset += chunk_len;
        cursor += 1;
    }

    Ok(pages.first().copied())
}

pub fn read_overflow_chain(
    storage: &mut Pager,
    first_page: Option<PageId>,
    expected_len: usize,
) -> Result<Vec<u8>> {
    if first_page.is_none() {
        return Ok(Vec::new());
    }

    let mut out = Vec::with_capacity(expected_len);
    let mut current = first_page.unwrap_or(PageId::invalid());
    let mut visited = std::collections::HashSet::new();

    while current != PageId::invalid() && out.len() < expected_len {
        if !visited.insert(current) {
            return Err(HematiteError::CorruptedData(
                "Overflow chain cycle detected".to_string(),
            ));
        }
        let page = storage.read_page(current)?;
        if &page.data[0..4] != OVERFLOW_MAGIC {
            return Err(HematiteError::CorruptedData(
                "Overflow page magic mismatch".to_string(),
            ));
        }

        let next_page = PageId::new(u32::from_le_bytes([
            page.data[4],
            page.data[5],
            page.data[6],
            page.data[7],
        ]));
        let chunk_len =
            u32::from_le_bytes([page.data[8], page.data[9], page.data[10], page.data[11]]) as usize;
        if chunk_len > OVERFLOW_CHUNK_CAPACITY {
            return Err(HematiteError::CorruptedData(
                "Overflow chunk length exceeds page capacity".to_string(),
            ));
        }

        out.extend_from_slice(&page.data[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + chunk_len]);
        current = next_page;
    }

    if out.len() < expected_len {
        return Err(HematiteError::CorruptedData(
            "Overflow chain ended before expected payload length".to_string(),
        ));
    }
    out.truncate(expected_len);
    Ok(out)
}

pub fn free_overflow_chain(storage: &mut Pager, first_page: Option<PageId>) -> Result<()> {
    let mut current = match first_page {
        Some(page_id) => page_id,
        None => return Ok(()),
    };
    let mut visited = std::collections::HashSet::new();

    while current != PageId::invalid() {
        if !visited.insert(current) {
            return Err(HematiteError::CorruptedData(
                "Overflow chain cycle detected while freeing".to_string(),
            ));
        }
        let page = storage.read_page(current)?;
        if &page.data[0..4] != OVERFLOW_MAGIC {
            return Err(HematiteError::CorruptedData(
                "Overflow page magic mismatch while freeing".to_string(),
            ));
        }
        let next_page = PageId::new(u32::from_le_bytes([
            page.data[4],
            page.data[5],
            page.data[6],
            page.data[7],
        ]));
        storage.deallocate_page(current)?;
        current = next_page;
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

    let mut current = first_page.unwrap_or(PageId::invalid());
    let mut visited = std::collections::HashSet::new();
    let mut total_payload = 0usize;
    let mut pages = 0usize;

    while current != PageId::invalid() && total_payload < expected_len {
        if !visited.insert(current) {
            return Err(HematiteError::CorruptedData(
                "Overflow chain cycle detected during validation".to_string(),
            ));
        }
        let page = storage.read_page(current)?;
        if &page.data[0..4] != OVERFLOW_MAGIC {
            return Err(HematiteError::CorruptedData(
                "Overflow page magic mismatch during validation".to_string(),
            ));
        }
        let next_page = PageId::new(u32::from_le_bytes([
            page.data[4],
            page.data[5],
            page.data[6],
            page.data[7],
        ]));
        let chunk_len =
            u32::from_le_bytes([page.data[8], page.data[9], page.data[10], page.data[11]]) as usize;
        if chunk_len > OVERFLOW_CHUNK_CAPACITY {
            return Err(HematiteError::CorruptedData(
                "Overflow chunk length exceeds page capacity during validation".to_string(),
            ));
        }
        total_payload += chunk_len;
        pages += 1;
        current = next_page;
    }

    if total_payload < expected_len {
        return Err(HematiteError::CorruptedData(
            "Overflow chain payload shorter than expected length".to_string(),
        ));
    }

    Ok(OverflowChainReport {
        page_count: pages,
        payload_len: total_payload,
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
    let mut visited = std::collections::HashSet::new();

    while current != PageId::invalid() {
        if !visited.insert(current) {
            return Err(HematiteError::CorruptedData(
                "Overflow chain cycle detected while collecting page ids".to_string(),
            ));
        }
        let page = storage.read_page(current)?;
        if &page.data[0..4] != OVERFLOW_MAGIC {
            return Err(HematiteError::CorruptedData(
                "Overflow page magic mismatch while collecting page ids".to_string(),
            ));
        }
        ids.push(current);
        current = PageId::new(u32::from_le_bytes([
            page.data[4],
            page.data[5],
            page.data[6],
            page.data[7],
        ]));
    }

    Ok(ids)
}
