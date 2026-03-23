//! Typed B-tree facade built on top of the raw byte-tree API.
//!
//! This is the convenience layer for callers that already have a stable key/value codec. It wires
//! a [`KeyValueCodec`] into the raw byte-tree API so typed code can work with domain objects while
//! still relying on the generic tree implementation underneath.

use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use crate::btree::bytes::{ByteTree, ByteTreeCursor, ByteTreeStore};
use crate::btree::{KeyValueCodec, TreeSpaceStats};
use crate::error::Result;
use crate::storage::{PageId, Pager};

#[derive(Clone)]
pub struct TypedTreeStore<C> {
    bytes: ByteTreeStore,
    marker: PhantomData<C>,
}

impl<C> TypedTreeStore<C> {
    pub fn new(bytes: ByteTreeStore) -> Self {
        Self {
            bytes,
            marker: PhantomData,
        }
    }

    pub fn from_storage(storage: Pager) -> Self {
        Self::new(ByteTreeStore::new(storage))
    }

    pub fn from_shared_storage(storage: Arc<Mutex<Pager>>) -> Self {
        Self::new(ByteTreeStore::from_shared_storage(storage))
    }

    pub fn byte_tree_store(&self) -> &ByteTreeStore {
        &self.bytes
    }
}

impl<C: KeyValueCodec> TypedTreeStore<C> {
    pub fn create_tree(&self) -> Result<PageId> {
        self.bytes.create_tree()
    }

    pub fn open_tree(&self, root_page_id: PageId) -> Result<TypedTree<C>> {
        Ok(TypedTree {
            bytes: self.bytes.open_tree(root_page_id)?,
            marker: PhantomData,
        })
    }

    pub fn delete_tree(&self, root_page_id: PageId) -> Result<()> {
        self.bytes.delete_tree(root_page_id)
    }

    pub fn validate_tree(&self, root_page_id: PageId) -> Result<bool> {
        self.bytes.validate_tree(root_page_id)
    }

    pub fn reset_tree(&self, root_page_id: PageId) -> Result<()> {
        self.bytes.reset_tree(root_page_id)
    }

    pub fn collect_page_ids(&self, root_page_id: PageId) -> Result<Vec<PageId>> {
        self.bytes.collect_page_ids(root_page_id)
    }

    pub fn collect_space_stats(&self, root_page_id: PageId) -> Result<TreeSpaceStats> {
        self.bytes.collect_space_stats(root_page_id)
    }
}

pub struct TypedTree<C> {
    bytes: ByteTree,
    marker: PhantomData<C>,
}

impl<C: KeyValueCodec> TypedTree<C> {
    pub fn root_page_id(&self) -> PageId {
        self.bytes.root_page_id()
    }

    pub fn get(&mut self, key: &C::Key) -> Result<Option<C::Value>> {
        let encoded_key = C::encode_key(key)?;
        self.bytes
            .get(&encoded_key)?
            .map(|value| C::decode_value(&value))
            .transpose()
    }

    pub fn insert(&mut self, key: &C::Key, value: &C::Value) -> Result<()> {
        let encoded_key = C::encode_key(key)?;
        let encoded_value = C::encode_value(value)?;
        self.bytes.insert(&encoded_key, &encoded_value)
    }

    pub fn delete(&mut self, key: &C::Key) -> Result<Option<C::Value>> {
        let encoded_key = C::encode_key(key)?;
        self.bytes
            .delete(&encoded_key)?
            .map(|value| C::decode_value(&value))
            .transpose()
    }

    pub fn entries(&self) -> Result<Vec<(C::Key, C::Value)>> {
        self.bytes
            .entries()?
            .into_iter()
            .map(|(key, value)| Ok((C::decode_key(&key)?, C::decode_value(&value)?)))
            .collect()
    }

    pub fn cursor(&self) -> Result<TypedTreeCursor<C>> {
        Ok(TypedTreeCursor {
            bytes: self.bytes.cursor()?,
            marker: PhantomData,
        })
    }
}

pub struct TypedTreeCursor<C> {
    bytes: ByteTreeCursor,
    marker: PhantomData<C>,
}

impl<C: KeyValueCodec> TypedTreeCursor<C> {
    pub fn is_valid(&self) -> bool {
        self.bytes.is_valid()
    }

    pub fn first(&mut self) -> Result<()> {
        self.bytes.first()
    }

    pub fn next(&mut self) -> Result<()> {
        self.bytes.next()
    }

    pub fn seek(&mut self, key: &C::Key) -> Result<()> {
        let encoded_key = C::encode_key(key)?;
        self.bytes.seek(&encoded_key)
    }

    pub fn current(&self) -> Result<Option<(C::Key, C::Value)>> {
        self.bytes
            .current()?
            .map(|(key, value)| Ok((C::decode_key(&key)?, C::decode_value(&value)?)))
            .transpose()
    }

    pub fn collect_all(&mut self) -> Result<Vec<(C::Key, C::Value)>> {
        self.bytes
            .collect_all()?
            .into_iter()
            .map(|(key, value)| Ok((C::decode_key(&key)?, C::decode_value(&value)?)))
            .collect()
    }
}
