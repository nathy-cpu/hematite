//! Generic B-tree module over opaque byte keys and values.
//!
//! Extraction boundary:
//! - Higher layers should build on [`ByteTreeStore`], [`ByteTree`], [`ByteTreeCursor`], and
//!   [`KeyValueCodec`].
//! - Node/page/value-store mechanics remain internal so the tree layout can evolve without
//!   leaking implementation detail into relational code.
//! - This is the generic data-structure half of the future fork point.

pub mod bytes;
pub mod codec;
pub(crate) mod cursor;
pub(crate) mod index;
pub(crate) mod node;
pub(crate) mod tree;
pub mod typed;
pub(crate) mod value_store;

pub use bytes::{ByteTree, ByteTreeCursor, ByteTreeStore};
pub use codec::{KeyValueCodec, RawBytesCodec};
pub use tree::TreeSpaceStats;
pub use typed::{TypedTree, TypedTreeCursor, TypedTreeStore};

pub const BTREE_ORDER: usize = 100; // Maximum children per node

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
