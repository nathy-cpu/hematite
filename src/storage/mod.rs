//! Low-level storage primitives.
//!
//! Extraction boundary:
//! - External callers should treat [`Pager`] plus the page/value types re-exported here as the
//!   entire storage API.
//! - Everything else in this module is pager implementation detail and may change as long as the
//!   pager contract stays stable.
//! - This is the storage half of the future generic fork point.

pub(crate) mod buffer_pool;
pub(crate) mod file_manager;
pub(crate) mod free_list;
pub(crate) mod journal;
pub(crate) mod overflow;
pub(crate) mod pager;
pub(crate) mod types;
pub(crate) mod wal;

pub use pager::{JournalMode, Pager};
pub use types::{
    Page, PageId, PagerIntegrityReport, DB_HEADER_PAGE_ID, INVALID_PAGE_ID, PAGE_SIZE,
    STORAGE_METADATA_PAGE_ID,
};

#[cfg(test)]
mod tests;
