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
pub(crate) mod cursor;
pub(crate) mod index;
pub(crate) mod node;
pub(crate) mod tree;
pub mod typed;
pub(crate) mod value_store;

pub use crate::storage::{JournalMode, PageId, PagerIntegrityReport};
pub use bytes::{ByteTree, ByteTreeCursor, ByteTreeStore};
pub use codec::{KeyValueCodec, RawBytesCodec};
pub use tree::TreeSpaceStats;
pub use typed::{TypedTree, TypedTreeCursor, TypedTreeStore};

pub const BTREE_ORDER: usize = 100;

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
