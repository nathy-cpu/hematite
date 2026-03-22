//! Byte-oriented B-tree facade for higher layers.
//!
//! This is the main generic B-tree API that upper layers should use. It exposes
//! tree lifecycle and key/value operations over opaque bytes while hiding pager,
//! page, and node details inside the B-tree module.

use std::sync::{Arc, Mutex};

use crate::btree::codec::RawBytesCodec;
use crate::btree::cursor::BTreeCursor;
use crate::btree::index::{BTreeIndex, TreeMutation};
use crate::btree::tree::{
    collect_tree_page_ids, collect_tree_space_stats, reset_tree_pages, BTreeManager, TreeSpaceStats,
};
use crate::error::Result;
use crate::storage::{PageId, Pager};

#[derive(Clone)]
pub struct ByteTreeStore {
    storage: Arc<Mutex<Pager>>,
}

impl ByteTreeStore {
    pub fn new(storage: Pager) -> Self {
        Self {
            storage: Arc::new(Mutex::new(storage)),
        }
    }

    pub fn from_shared_storage(storage: Arc<Mutex<Pager>>) -> Self {
        Self { storage }
    }

    pub fn shared_storage(&self) -> Arc<Mutex<Pager>> {
        self.storage.clone()
    }

    pub fn create_tree(&self) -> Result<PageId> {
        let mut manager = BTreeManager::from_shared_storage(self.storage.clone());
        manager.create_tree()
    }

    pub fn open_tree(&self, root_page_id: PageId) -> Result<ByteTree> {
        let mut manager = BTreeManager::from_shared_storage(self.storage.clone());
        let index = manager.open_tree(root_page_id)?;
        Ok(ByteTree { index })
    }

    pub fn delete_tree(&self, root_page_id: PageId) -> Result<()> {
        let mut manager = BTreeManager::from_shared_storage(self.storage.clone());
        manager.delete_tree(root_page_id)
    }

    pub fn validate_tree(&self, root_page_id: PageId) -> Result<bool> {
        let mut manager = BTreeManager::from_shared_storage(self.storage.clone());
        manager.validate_tree(root_page_id)
    }

    pub fn reset_tree(&self, root_page_id: PageId) -> Result<()> {
        let mut pager = self.storage.lock().unwrap();
        reset_tree_pages(&mut pager, root_page_id)
    }

    pub fn collect_page_ids(&self, root_page_id: PageId) -> Result<Vec<PageId>> {
        let mut pager = self.storage.lock().unwrap();
        let mut page_ids = Vec::new();
        collect_tree_page_ids(&mut pager, root_page_id, &mut page_ids)?;
        Ok(page_ids)
    }

    pub fn collect_space_stats(&self, root_page_id: PageId) -> Result<TreeSpaceStats> {
        let mut pager = self.storage.lock().unwrap();
        collect_tree_space_stats(&mut pager, root_page_id)
    }
}

pub struct ByteTree {
    index: BTreeIndex,
}

impl ByteTree {
    pub fn root_page_id(&self) -> PageId {
        self.index.root_page_id()
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.index.search_typed::<RawBytesCodec>(&key.to_vec())
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.insert_with_mutation(key, value).map(|_| ())
    }

    pub fn insert_with_mutation(&mut self, key: &[u8], value: &[u8]) -> Result<TreeMutation> {
        self.index
            .insert_typed_with_mutation::<RawBytesCodec>(&key.to_vec(), &value.to_vec())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.delete_with_mutation(key).map(|(value, _)| value)
    }

    pub fn delete_with_mutation(&mut self, key: &[u8]) -> Result<(Option<Vec<u8>>, TreeMutation)> {
        self.index
            .delete_typed_with_mutation::<RawBytesCodec>(&key.to_vec())
    }

    pub fn cursor(&self) -> Result<ByteTreeCursor> {
        Ok(ByteTreeCursor {
            inner: self.index.cursor()?,
        })
    }
}

pub struct ByteTreeCursor {
    inner: BTreeCursor,
}

impl ByteTreeCursor {
    pub fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    pub fn first(&mut self) -> Result<()> {
        self.inner.first()
    }

    pub fn next(&mut self) -> Result<()> {
        self.inner.next()
    }

    pub fn seek(&mut self, key: &[u8]) -> Result<()> {
        self.inner.seek(&crate::btree::BTreeKey::new(key.to_vec()))
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.inner.key().map(|key| key.as_bytes())
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.inner.value().map(|value| value.as_bytes())
    }

    pub fn current(&self) -> Option<(&[u8], &[u8])> {
        self.inner
            .current()
            .map(|(key, value)| (key.as_bytes(), value.as_bytes()))
    }
}
