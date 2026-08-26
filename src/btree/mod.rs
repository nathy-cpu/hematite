//! Generic B-tree module over opaque byte keys and values.
//!
//! This module is the structural layer between `storage` and `catalog`. It knows how to maintain
//! ordered trees on top of pages, but it does not know what keys or values mean.
//!
//! Public surfaces:
//! - [`ByteTreeStore`] / [`ByteTree`] / [`ByteTreeCursor`] for raw byte keys and values;
//! - [`TypedTreeStore`] / [`TypedTree`] / [`TypedTreeCursor`] for typed callers using codecs;
//! - [`KeyValueCodec`] as the only typed boundary.
//!
//! Internal surfaces:
//! - node serialization and validation;
//! - split / merge / rebalance logic;
//! - value overflow handling;
//! - tree validation and page-space accounting.
//!
//! This is the generic data-structure half of the future fork point.

pub mod bytes;
pub mod codec;
pub(crate) mod compaction;
pub(crate) mod cursor;
pub(crate) mod index;
pub(crate) mod node;
pub(crate) mod page_format;
pub(crate) mod tree;
pub mod typed;
pub(crate) mod value_store;
pub(crate) mod varint;

/// Re-exported so `catalog` can checksum the database header with the same
/// function everything else on disk uses. The layer matrix in `src/tests.rs` does
/// not permit `catalog` to name `crate::storage` directly, and page 0 is still
/// owned by `catalog`, so the route runs through here.
pub(crate) use crate::storage::checksum;
pub use crate::storage::{JournalMode, PageId, PagerIntegrityReport};
pub(crate) use bytes::ByteTreeStoreSnapshot;
pub use bytes::{ByteTree, ByteTreeCursor, ByteTreeStore};
pub use codec::{KeyValueCodec, RawBytesCodec};
pub use tree::TreeSpaceStats;
pub use typed::{TypedTree, TypedTreeCursor, TypedTreeStore};

/// Upper bound on cells per page — deliberately above what a page can physically
/// hold, so that **bytes are what actually limit a page**, not this number.
///
/// It was 100, and that was the binding constraint rather than page space: a leaf of
/// five-small-int rows filled to 98 cells and split at 86 % full, when the bytes would
/// have allowed ~113. Raising it took a table from 44.2 to 39.3 bytes per row, and the
/// same fixture through SQL — where the primary-key index roughly doubles the file, and
/// index cells are smaller so the cap bit harder — from 87.7 to 59.8.
///
/// The floor for a leaf cell is 2 bytes of pointer plus 4 of length prefix plus at
/// least one key byte and an 11-byte stored-value header, so a 4088-byte page cannot
/// hold more than ~227 cells. 400 is clear of that, which is the point: splitting is
/// decided by `total_free_space`, and this only exists to bound the in-memory decoded
/// vectors.
pub const BTREE_ORDER: usize = 400;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    Internal,
    Leaf,
}

#[derive(Debug, Clone)]
pub struct BTreeKey {
    pub data: Vec<u8>,
}

impl BTreeKey {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

impl PartialEq for BTreeKey {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for BTreeKey {}

impl PartialOrd for BTreeKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BTreeKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.data.cmp(&other.data)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BTreeValue {
    pub data: Vec<u8>,
}

impl BTreeValue {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

#[cfg(test)]
mod tests;
