//! Core storage types and constants.
//!
//! This file defines the vocabulary shared by the storage layer:
//! - `PageId`: logical page address;
//! - `Page`: a full fixed-size page image;
//! - reserved page ids and page size;
//! - pager integrity reporting types used by higher layers.
//!
//! The key distinction is that a `Page` is just bytes plus an id. Any meaning attached to those
//! bytes belongs to higher layers such as the B-tree or catalog code.

use crate::error::Result;

pub const PAGE_SIZE: usize = 4096;

/// Bytes at the end of every page reserved for the pager.
///
/// Holds the page's own checksum. It used to live in a map serialised into a single
/// metadata page, and when that map outgrew the page the encoder silently wrote no
/// checksums at all — measured at 685 pages, so per-page integrity was off in any
/// database over about 2.8 MB. A checksum stored on the page it describes has no such
/// ceiling, needs no map to clone per savepoint, and needs no re-serialisation per
/// transaction.
pub const PAGE_TRAILER_SIZE: usize = 4;

/// Bytes of a page that content may use. Nothing above the pager may write past this.
pub const USABLE_PAGE_SIZE: usize = PAGE_SIZE - PAGE_TRAILER_SIZE;

pub const DB_HEADER_PAGE_ID: u32 = 0;
pub const STORAGE_METADATA_PAGE_ID: u32 = 1;
pub const FIRST_ALLOCATABLE_PAGE_ID: u32 = STORAGE_METADATA_PAGE_ID + 1;
pub const INVALID_PAGE_ID: u32 = u32::MAX;

pub type PageId = u32;

pub const fn next_page_id_for_file_len(file_len: u64) -> PageId {
    let page_count = (file_len / PAGE_SIZE as u64) as PageId;
    if page_count < FIRST_ALLOCATABLE_PAGE_ID {
        FIRST_ALLOCATABLE_PAGE_ID
    } else {
        page_count
    }
}

pub const fn file_len_for_next_page_id(next_page_id: PageId) -> u64 {
    next_page_id as u64 * PAGE_SIZE as u64
}

/// Recycled page-sized buffers.
///
/// A page write allocated a fresh `vec![0u8; PAGE_SIZE]` and freed the one it
/// displaced, so a batched insert made 40 690 `calloc`s. Almost all of that cost is
/// the zeroing rather than the allocator: glibc's `memset` uses `rep stosb`, one
/// instruction per byte, so a 4 KiB page costs ~4 100 instructions — 10 % of the
/// insert path.
///
/// Fixing it needs *both* halves, which is why two earlier attempts each measured
/// nothing. Recycling alone still pays the zeroing. Skipping the zeroing alone still
/// pays `calloc`, which zeroes anyway. Together, a page that is about to be
/// overwritten costs neither.
///
/// Reclaiming happens in `Page::drop`, which is the moment the last owner goes away.
/// Earlier attempts reclaimed in `PageCache::put_shared_with_view` and relied on
/// `Arc::try_unwrap`, which always failed because a `BTreeNode` holds an `Arc<Page>`
/// for the page it is mutating.
///
/// Thread-local so the hot path takes no lock. Buffers do not migrate between
/// threads: one thread's pool simply stays empty until it frees its own pages.
pub(crate) mod page_pool {
    use super::PAGE_SIZE;
    use std::cell::RefCell;

    /// Bounded so a burst of pages does not pin memory for the process lifetime.
    /// 256 buffers is 1 MiB per thread.
    const MAX_POOLED: usize = 256;

    thread_local! {
        static POOL: RefCell<Vec<Vec<u8>>> = const { RefCell::new(Vec::new()) };
    }

    /// A page-sized buffer whose contents are **undefined**; the caller must write
    /// every byte before the buffer is read.
    pub(crate) fn take_for_overwrite() -> Vec<u8> {
        POOL.with(|pool| pool.borrow_mut().pop())
            .unwrap_or_else(|| vec![0u8; PAGE_SIZE])
    }

    /// A zeroed page-sized buffer, for pages whose contents *are* the zeros.
    pub(super) fn take_zeroed() -> Vec<u8> {
        POOL.with(|pool| pool.borrow_mut().pop())
            .map(|mut data| {
                data.fill(0);
                data
            })
            .unwrap_or_else(|| vec![0u8; PAGE_SIZE])
    }

    pub(super) fn give_back(data: Vec<u8>) {
        if data.len() != PAGE_SIZE {
            return;
        }
        POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            if pool.len() < MAX_POOLED {
                pool.push(data);
            }
        });
    }

    #[cfg(test)]
    pub(crate) fn pooled_count() -> usize {
        POOL.with(|pool| pool.borrow().len())
    }

    #[cfg(test)]
    pub(crate) fn clear() {
        POOL.with(|pool| pool.borrow_mut().clear());
    }
}

#[cfg(test)]
pub(crate) use page_pool::{clear as clear_page_pool, pooled_count as pooled_page_buffers};

/// A page-sized buffer with **undefined** contents, for callers that hold a raw
/// `Vec<u8>` rather than a [`Page`] and will overwrite every byte.
pub(crate) fn take_page_buffer_for_overwrite() -> Vec<u8> {
    page_pool::take_for_overwrite()
}

#[derive(Debug)]
pub struct Page {
    pub id: PageId,
    pub data: Vec<u8>,
}

impl Page {
    /// A zeroed page.
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            data: page_pool::take_zeroed(),
        }
    }

    /// A page whose bytes are **undefined**; every one must be written before it is
    /// read. Skips the ~4 100-instruction zeroing that `new` pays.
    pub(crate) fn new_for_overwrite(id: PageId) -> Self {
        Self {
            id,
            data: page_pool::take_for_overwrite(),
        }
    }

    pub fn from_bytes(id: PageId, data: Vec<u8>) -> Result<Self> {
        if data.len() != PAGE_SIZE {
            return Err(crate::error::HematiteError::InvalidPage(id));
        }
        Ok(Self { id, data })
    }

    /// Takes the buffer, leaving the page empty.
    ///
    /// `Page` owns a pooled buffer and has a `Drop`, so the field cannot be moved out
    /// directly any more.
    pub(crate) fn take_data(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.data)
    }
}

impl Clone for Page {
    fn clone(&self) -> Self {
        let mut data = page_pool::take_for_overwrite();
        data.copy_from_slice(&self.data);
        Self { id: self.id, data }
    }
}

impl Drop for Page {
    fn drop(&mut self) {
        page_pool::give_back(std::mem::take(&mut self.data));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PagerIntegrityReport {
    pub allocated_page_count: usize,
    pub free_page_count: usize,
    pub fragmented_free_page_count: usize,
    pub trailing_free_page_count: usize,
    pub checksummed_page_count: usize,
    pub verified_checksum_pages: usize,
}
