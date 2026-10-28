use super::Pager;
use crate::error::Result;
use crate::storage::checksum;
use crate::storage::file_manager::{FileManager, PageTrailerState};
use crate::storage::{
    next_page_id_for_file_len, Page, PageId, PagerIntegrityReport, DB_HEADER_PAGE_ID,
    FIRST_ALLOCATABLE_PAGE_ID, STORAGE_METADATA_PAGE_ID,
};
use std::collections::HashSet;

impl Pager {
    /// A checksum of a page's contents, excluding its trailer.
    ///
    /// Not the trailer's own checksum: this is what the rollback journal's commit
    /// intents are compared against, which is a different question — whether a
    /// specific commit landed, rather than whether a page is intact.
    pub(super) fn calculate_page_checksum(page: &Page) -> u32 {
        checksum::hash32(0, &page.data[..crate::storage::USABLE_PAGE_SIZE])
    }

    pub fn validate_integrity(&mut self) -> Result<PagerIntegrityReport> {
        let (max_page_id_exclusive, logical_free_pages, wal_override_page_ids) =
            if let Some(state) = &self.latest_wal_state {
                (
                    next_page_id_for_file_len(state.file_len),
                    state.free_pages.clone(),
                    state.overridden_page_ids().collect::<HashSet<PageId>>(),
                )
            } else if let Some(transaction) = self.active_rollback_transaction() {
                (
                    transaction.rollback_next_page_id,
                    transaction.rollback_free_list.as_slice().to_vec(),
                    HashSet::new(),
                )
            } else {
                (
                    self.file_manager.next_page_id(),
                    self.file_manager.free_pages().to_vec(),
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

        // Walks every allocated page, because each page's checksum lives in its own
        // trailer and coverage does not depend on anything else fitting anywhere. A
        // checksum map serialised into one metadata page would cap coverage: it stops
        // fitting at 685 pages, about 2.8 MB.
        let mut checksummed_page_count = 0usize;
        let mut verified_checksum_pages = 0usize;
        for page_id in FIRST_ALLOCATABLE_PAGE_ID..max_page_id_exclusive {
            if free_pages.contains(&page_id) {
                continue;
            }
            // A dirty page has not reached storage, so nothing storage could have
            // corrupted has happened to it yet.
            if self.cache_read()?.is_dirty(page_id) {
                continue;
            }

            let page = if wal_override_page_ids.contains(&page_id) {
                if let Some(data) = self.read_wal_page_data(page_id)? {
                    Page::from_bytes(page_id, data)?
                } else {
                    self.file_manager.read_page(page_id)?
                }
            } else {
                // In WAL mode the logical page range runs ahead of the physical file:
                // a page allocated in the WAL has no image on disk until a checkpoint
                // writes one. There is nothing to verify yet.
                if page_id >= self.file_manager.next_page_id() {
                    continue;
                }
                self.file_manager.read_page(page_id)?
            };

            match FileManager::page_trailer_state(&page) {
                PageTrailerState::Valid => {
                    checksummed_page_count += 1;
                    verified_checksum_pages += 1;
                }
                // Never stamped: file space extended without the page being written
                // reads as zeros, and there is nothing to check it against. WAL frames
                // are stamped at commit, so a WAL-resident page lands in `Valid`.
                PageTrailerState::Unstamped => {}
                PageTrailerState::Mismatch => {
                    return Err(crate::error::HematiteError::CorruptedData(format!(
                        "Page {page_id} does not match the checksum in its trailer"
                    )));
                }
            }
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
}
