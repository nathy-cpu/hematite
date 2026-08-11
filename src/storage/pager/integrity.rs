use super::Pager;
use crate::error::Result;
use crate::storage::checksum;
use crate::storage::{
    next_page_id_for_file_len, Page, PageId, PagerIntegrityReport, DB_HEADER_PAGE_ID,
    STORAGE_METADATA_PAGE_ID,
};
use std::collections::HashSet;

impl Pager {
    pub fn validate_integrity(&mut self) -> Result<PagerIntegrityReport> {
        let (max_page_id_exclusive, logical_free_pages, logical_checksums, wal_override_page_ids) =
            if let Some(state) = &self.latest_wal_state {
                (
                    next_page_id_for_file_len(state.file_len),
                    state.free_pages.clone(),
                    state.page_checksums.clone(),
                    state.overridden_page_ids().collect::<HashSet<PageId>>(),
                )
            } else if let Some(transaction) = self.active_rollback_transaction() {
                (
                    transaction.rollback_next_page_id,
                    transaction.rollback_free_list.as_slice().to_vec(),
                    self.page_checksums.clone(),
                    HashSet::new(),
                )
            } else {
                (
                    self.file_manager.next_page_id(),
                    self.file_manager.free_pages().to_vec(),
                    self.page_checksums.clone(),
                    HashSet::new(),
                )
            };

        let mut free_pages = HashSet::new();

        for &page_id in &logical_free_pages {
            if page_id == DB_HEADER_PAGE_ID || page_id == STORAGE_METADATA_PAGE_ID {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Reserved page {} cannot be marked free",
                    page_id
                )));
            }

            if page_id >= max_page_id_exclusive {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Free page {} exceeds allocated page range (next_page_id={})",
                    page_id, max_page_id_exclusive
                )));
            }

            if !free_pages.insert(page_id) {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Duplicate free page {} detected",
                    page_id
                )));
            }
        }

        if logical_checksums.contains_key(&STORAGE_METADATA_PAGE_ID) {
            return Err(crate::error::HematiteError::CorruptedData(format!(
                "Storage metadata page {} must not have pager checksum metadata",
                STORAGE_METADATA_PAGE_ID
            )));
        }

        let checksummed_pages = logical_checksums.into_iter().collect::<Vec<_>>();
        let checksummed_page_count = checksummed_pages.len();

        let mut verified_checksum_pages = 0usize;
        for (page_id, expected_checksum) in checksummed_pages {
            if page_id >= max_page_id_exclusive {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Checksum entry for page {} exceeds allocated page range (next_page_id={})",
                    page_id, max_page_id_exclusive
                )));
            }

            if free_pages.contains(&page_id) {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Page {} has checksum metadata but is marked free",
                    page_id
                )));
            }

            let page = if self.cache_read()?.is_dirty(page_id) {
                self.cache_read()?.peek(page_id).cloned().ok_or_else(|| {
                    crate::error::HematiteError::StorageError(format!(
                        "Dirty page {} missing from page cache",
                        page_id
                    ))
                })?
            } else if wal_override_page_ids.contains(&page_id) {
                // Read WAL page data on-demand through the LRU cache
                if let Some(data) = self.read_wal_page_data(page_id)? {
                    Page::from_bytes(page_id, data)?
                } else {
                    self.file_manager.read_page(page_id)?
                }
            } else {
                self.file_manager.read_page(page_id)?
            };

            let actual_checksum = Self::calculate_page_checksum(&page);
            if actual_checksum != expected_checksum {
                return Err(crate::error::HematiteError::CorruptedData(format!(
                    "Page checksum mismatch for page {}: expected {}, got {}",
                    page_id, expected_checksum, actual_checksum
                )));
            }

            verified_checksum_pages += 1;
        }

        Ok(PagerIntegrityReport {
            allocated_page_count: self.file_manager.allocated_page_count(),
            free_page_count: free_pages.len(),
            fragmented_free_page_count: self.file_manager.fragmented_free_page_count(),
            trailing_free_page_count: self.file_manager.trailing_free_page_count(),
            checksummed_page_count,
            verified_checksum_pages,
        })
    }

    pub(super) fn calculate_page_checksum(page: &Page) -> u32 {
        checksum::hash32(0, &page.data)
    }
}
