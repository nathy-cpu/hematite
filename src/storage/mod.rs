//! Low-level storage primitives.
//!
//! This module is the bottom of the stack. It is responsible for durable page IO and nothing
//! above it should need to understand file offsets, sidecar files, or crash-recovery record
//! layouts.
//!
//! The storage layer centers on a single abstraction:
//!
//! ```text
//!              Pager
//!                |
//!    +-----------+-----------+
//!    |           |           |
//! buffer      free pages   durability
//! pool        + file len   (rollback/WAL)
//! ```
//!
//! Responsibilities:
//! - map logical page ids to bytes on disk or bytes in the in-memory backend;
//! - allocate, reuse, and retire pages;
//! - maintain checksum metadata for durable pages;
//! - implement transaction visibility for rollback-journal and WAL modes;
//! - report integrity/accounting information upward without knowing what page contents mean.
//!
//! Extraction boundary:
//! - external callers should treat [`Pager`] plus the re-exported page/id types as the entire
//!   storage API;
//! - the other submodules are pager internals and are intentionally hidden so the on-disk
//!   representation can evolve without leaking into higher layers;
//! - this is the storage half of the future generic fork point.

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
#[cfg(test)]
mod pager_fault_test;
