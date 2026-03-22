//! Low-level storage primitives.

pub mod buffer_pool;
pub mod file_manager;
pub mod free_list;
pub mod overflow;
pub mod pager;
pub mod types;

pub use pager::Pager;
pub use types::{
    Page, PageId, PagerIntegrityReport, DB_HEADER_PAGE_ID, PAGE_SIZE, STORAGE_METADATA_PAGE_ID,
};

#[cfg(test)]
mod tests;
