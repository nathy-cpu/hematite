use super::Pager;
use crate::error::Result;
use crate::storage::checksum;
use crate::storage::{
    next_page_id_for_file_len, Page, PageId, PagerIntegrityReport, DB_HEADER_PAGE_ID,
    STORAGE_METADATA_PAGE_ID,
};
use std::collections::HashSet;
use std::sync::Arc;

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
                    self.page_checksums.as_ref().clone(),
                    HashSet::new(),
                )
            } else {
                (
                    self.file_manager.next_page_id(),
                    self.file_manager.free_pages().to_vec(),
                    self.page_checksums.as_ref().clone(),
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

            // A dirty page has not reached storage, so a stored checksum cannot
            // describe it and there is nothing storage could have corrupted. This
            // used to compare the cached bytes against the map anyway, which always
            // matched: `write_page` computed that entry from those same bytes moments
            // earlier, so the comparison was tautological. Checksums are now computed
            // at flush, when the contents are final, and this page is simply not yet
            // verifiable — it is left out of `verified_checksum_pages` rather than
            // counted as checked. The structural checks below still cover it.
            if self.cache_read()?.is_dirty(page_id) {
                continue;
            }

            let page = if wal_override_page_ids.contains(&page_id) {
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

    /// Covers a page's contents, not its trailer.
    ///
    /// The trailer holds the page's own checksum, written by the file layer, so it is
    /// zero in memory and non-zero on disk. Hashing it would make every page fail
    /// verification the moment it was read back.
    pub(super) fn calculate_page_checksum(page: &Page) -> u32 {
        checksum::hash32(0, &page.data[..crate::storage::USABLE_PAGE_SIZE])
    }

    /// Recomputes the checksum of every dirty page.
    ///
    /// `write_page` used to do this on every call, so a transaction that wrote the
    /// same leaf once per row hashed 4 KiB once per row where once per commit would
    /// do — `hash64` was 5.8 % of a batched insert's instructions, most of it
    /// recomputing checksums that were about to be superseded. What a checksum has
    /// to describe is a page's *final* contents, and only a flush or a WAL commit
    /// makes them final.
    ///
    /// Deferring is safe because a dirty page is never evicted: `evict_if_needed`
    /// only takes clean, unpinned pages and would rather exceed its capacity than
    /// drop one. So between the write and the flush the page is always served from
    /// cache, and its checksum is never consulted.
    ///
    /// Must run before the metadata page is staged, since that page carries the
    /// serialized checksum map and would otherwise describe the previous contents.
    pub(super) fn refresh_dirty_page_checksums(&mut self) -> Result<()> {
        let dirty_ids = self.cache_read()?.dirty_page_ids();
        for page_id in dirty_ids {
            // The metadata page deliberately has no entry: it is where the map is
            // stored, so it cannot describe itself.
            if page_id == crate::storage::STORAGE_METADATA_PAGE_ID {
                continue;
            }
            let page = self.cache_read()?.peek_shared(page_id);
            if let Some(page) = page {
                let checksum = Self::calculate_page_checksum(&page);
                Arc::make_mut(&mut self.page_checksums).insert(page_id, checksum);
            }
        }
        Ok(())
    }
}
