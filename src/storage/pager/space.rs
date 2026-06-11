use super::{compact_transaction_free_pages, JournalMode, Pager};
use crate::error::Result;
use crate::storage::PageId;
use std::collections::HashMap;

impl Pager {
    pub fn allocate_page(&mut self) -> Result<PageId> {
        self.check_error_state()?;
        if let Some(transaction) = self.active_rollback_transaction_mut() {
            let page_id = if let Some(page_id) = transaction.rollback_free_list.pop_free_page() {
                page_id
            } else {
                let page_id = transaction.rollback_next_page_id;
                transaction.rollback_next_page_id =
                    transaction.rollback_next_page_id.saturating_add(1);
                page_id
            };
            transaction.rollback_uninitialized_pages.insert(page_id);
            return Ok(page_id);
        }
        if self.journal_mode == JournalMode::Wal {
            if let Some(transaction) = self.active_wal_transaction_mut() {
                if let Some(page_id) = transaction.wal_free_pages.pop() {
                    transaction.wal_free_page_set.remove(&page_id);
                    return Ok(page_id);
                }
                let page_id = transaction.wal_next_page_id;
                transaction.wal_next_page_id += 1;
                return Ok(page_id);
            }
        }
        self.file_manager.allocate_page()
    }

    pub fn deallocate_page(&mut self, page_id: PageId) -> Result<()> {
        self.check_error_state()?;
        Self::can_deallocate_page(page_id)?;
        self.snapshot_original_page(page_id)?;
        self.cache_mut()?.remove(page_id);
        self.page_checksums.remove(&page_id);
        if let Some(transaction) = self.active_rollback_transaction_mut() {
            transaction.rollback_uninitialized_pages.remove(&page_id);
            if !transaction.rollback_free_list.contains(page_id) {
                transaction.rollback_free_list.push_free_page(page_id);
            }
            transaction.rollback_free_list.compact_trailing_pages(
                &mut transaction.rollback_next_page_id,
                crate::storage::FIRST_ALLOCATABLE_PAGE_ID,
            );
            return Ok(());
        }
        if self.journal_mode == JournalMode::Wal {
            if let Some(transaction) = self.active_wal_transaction_mut() {
                if transaction.wal_free_page_set.insert(page_id) {
                    transaction.wal_free_pages.push(page_id);
                }
                compact_transaction_free_pages(transaction);
                return Ok(());
            }
            self.file_manager.deallocate_page_deferred(page_id);
            Ok(())
        } else {
            self.file_manager.deallocate_page(page_id)
        }
    }

    pub fn free_pages(&self) -> &[PageId] {
        self.file_manager.free_pages()
    }

    pub fn set_free_pages(&mut self, free_pages: Vec<PageId>) {
        self.file_manager.set_free_pages(free_pages);
    }

    pub fn checksum_entries(&self) -> Vec<(PageId, u32)> {
        self.page_checksums
            .iter()
            .map(|(page_id, checksum)| (*page_id, *checksum))
            .collect()
    }

    pub fn replace_checksums(&mut self, checksums: HashMap<PageId, u32>) {
        self.page_checksums = checksums;
    }

    pub fn file_len(&self) -> Result<u64> {
        self.file_manager.file_len()
    }

    pub fn allocated_page_count(&self) -> usize {
        self.file_manager.allocated_page_count()
    }

    pub fn fragmented_free_page_count(&self) -> usize {
        self.file_manager.fragmented_free_page_count()
    }

    pub fn trailing_free_page_count(&self) -> usize {
        self.file_manager.trailing_free_page_count()
    }

    pub fn mark_page_active(&mut self, page_id: PageId) -> Result<()> {
        self.check_error_state()?;
        if let Some(transaction) = self.active_rollback_transaction_mut() {
            transaction.rollback_free_list.remove_page(page_id);
            return Ok(());
        }
        if self.journal_mode == JournalMode::Wal {
            if let Some(transaction) = self.active_wal_transaction_mut() {
                if transaction.wal_free_page_set.remove(&page_id) {
                    if let Some(pos) = transaction.wal_free_pages.iter().position(|&x| x == page_id) {
                        transaction.wal_free_pages.remove(pos);
                    }
                }
                return Ok(());
            }
        }
        let mut free_pages = self.file_manager.free_pages().to_vec();
        if let Some(pos) = free_pages.iter().position(|&p| p == page_id) {
            free_pages.remove(pos);
            self.file_manager.set_free_pages(free_pages);
        }
        Ok(())
    }
}
