//! Byte-oriented B-tree facade for higher layers.
//!
//! This is the main generic B-tree API that upper layers should use. It exposes
//! tree lifecycle and key/value operations over opaque bytes while hiding pager,
//! page, and node details inside the B-tree module.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::btree::codec::RawBytesCodec;
use crate::btree::cursor::BTreeCursor;
use crate::btree::index::{BTreeIndex, TreeMutation};
use crate::btree::node::BTreeNode;
use crate::btree::tree::{
    collect_tree_page_ids, collect_tree_space_stats, reset_tree_pages, BTreeManager, TreeSpaceStats,
};
use crate::btree::value_store::{
    free_stored_value_overflow, hydrate_stored_value, materialize_stored_value, StoredValueLayout,
};
use crate::btree::NodeType;
use crate::error::Result;
use crate::storage::overflow::{collect_overflow_page_ids, validate_overflow_chain};
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
        Ok(ByteTree {
            storage: self.storage.clone(),
            index,
        })
    }

    pub fn delete_tree(&self, root_page_id: PageId) -> Result<()> {
        {
            let mut pager = self.storage.lock().unwrap();
            free_tree_overflow(&mut pager, root_page_id)?;
        }
        let mut manager = BTreeManager::from_shared_storage(self.storage.clone());
        manager.delete_tree(root_page_id)
    }

    pub fn validate_tree(&self, root_page_id: PageId) -> Result<bool> {
        let mut manager = BTreeManager::from_shared_storage(self.storage.clone());
        if !manager.validate_tree(root_page_id)? {
            return Ok(false);
        }
        Ok(self.validate_tree_overflow(root_page_id).is_ok())
    }

    pub fn validate_tree_overflow(&self, root_page_id: PageId) -> Result<()> {
        let mut pager = self.storage.lock().unwrap();
        let mut tree_page_ids = Vec::new();
        collect_tree_page_ids(&mut pager, root_page_id, &mut tree_page_ids)?;
        let tree_pages = tree_page_ids.into_iter().collect::<HashSet<_>>();
        let free_pages = pager.free_pages().iter().copied().collect::<HashSet<_>>();
        let mut owned_overflow_pages = HashSet::new();
        validate_tree_overflow_pages(
            &mut pager,
            root_page_id,
            &tree_pages,
            &free_pages,
            &mut owned_overflow_pages,
        )
    }

    pub fn reset_tree(&self, root_page_id: PageId) -> Result<()> {
        let mut pager = self.storage.lock().unwrap();
        free_tree_overflow(&mut pager, root_page_id)?;
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
    storage: Arc<Mutex<Pager>>,
    index: BTreeIndex,
}

impl ByteTree {
    pub fn root_page_id(&self) -> PageId {
        self.index.root_page_id()
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.index.search_typed::<RawBytesCodec>(&key.to_vec())? {
            Some(stored_value) => {
                let mut storage = self.storage.lock().unwrap();
                Ok(Some(hydrate_stored_value(&mut storage, &stored_value)?))
            }
            None => Ok(None),
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.insert_with_mutation(key, value).map(|_| ())
    }

    pub fn insert_with_mutation(&mut self, key: &[u8], value: &[u8]) -> Result<TreeMutation> {
        let existing_stored_value = self.index.search_typed::<RawBytesCodec>(&key.to_vec())?;
        let stored_value = {
            let mut storage = self.storage.lock().unwrap();
            materialize_stored_value(&mut storage, value)?
        };
        let mutation = self
            .index
            .insert_typed_with_mutation::<RawBytesCodec>(&key.to_vec(), &stored_value)?;

        if let Some(existing_stored_value) = existing_stored_value {
            let mut storage = self.storage.lock().unwrap();
            free_stored_value_overflow(&mut storage, &existing_stored_value)?;
        }

        Ok(mutation)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.delete_with_mutation(key).map(|(value, _)| value)
    }

    pub fn delete_with_mutation(&mut self, key: &[u8]) -> Result<(Option<Vec<u8>>, TreeMutation)> {
        let (stored_value, mutation) = self
            .index
            .delete_typed_with_mutation::<RawBytesCodec>(&key.to_vec())?;
        let logical_value = match stored_value {
            Some(stored_value) => {
                let mut storage = self.storage.lock().unwrap();
                let logical_value = hydrate_stored_value(&mut storage, &stored_value)?;
                free_stored_value_overflow(&mut storage, &stored_value)?;
                Some(logical_value)
            }
            None => None,
        };
        Ok((logical_value, mutation))
    }

    pub fn entry(&mut self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        Ok(self.get(key)?.map(|value| (key.to_vec(), value)))
    }

    pub fn entries(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut cursor = self.cursor()?;
        cursor.collect_all()
    }

    pub fn entries_from(&self, start_key: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut cursor = self.cursor()?;
        cursor.seek(start_key)?;
        cursor.collect_remaining()
    }

    pub fn entries_with_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut cursor = self.cursor()?;
        cursor.seek(prefix)?;
        let mut entries = Vec::new();
        while let Some((key, value)) = cursor.current()? {
            if !key.starts_with(prefix) {
                break;
            }
            entries.push((key, value));
            if cursor.next().is_err() {
                break;
            }
        }
        Ok(entries)
    }

    pub fn cursor(&self) -> Result<ByteTreeCursor> {
        Ok(ByteTreeCursor {
            storage: self.storage.clone(),
            inner: self.index.cursor()?,
        })
    }
}

fn free_tree_overflow(storage: &mut Pager, root_page_id: PageId) -> Result<()> {
    let page = storage.read_page(root_page_id)?;
    let node = BTreeNode::from_page(page)?;

    match node.node_type {
        NodeType::Leaf => {
            for value in node.values {
                free_stored_value_overflow(storage, value.as_bytes())?;
            }
        }
        NodeType::Internal => {
            for child_page_id in node.children {
                free_tree_overflow(storage, child_page_id)?;
            }
        }
    }

    Ok(())
}

fn validate_tree_overflow_pages(
    storage: &mut Pager,
    root_page_id: PageId,
    tree_pages: &HashSet<PageId>,
    free_pages: &HashSet<PageId>,
    owned_overflow_pages: &mut HashSet<PageId>,
) -> Result<()> {
    let page = storage.read_page(root_page_id)?;
    let node = BTreeNode::from_page(page)?;

    match node.node_type {
        NodeType::Leaf => {
            for value in node.values {
                let layout = StoredValueLayout::decode(value.as_bytes())?;
                if layout.overflow_first_page != crate::storage::INVALID_PAGE_ID {
                    let first_page = Some(layout.overflow_first_page);
                    validate_overflow_chain(storage, first_page, layout.overflow_len())?;
                    for overflow_page_id in collect_overflow_page_ids(storage, first_page)? {
                        if tree_pages.contains(&overflow_page_id) {
                            return Err(crate::error::HematiteError::CorruptedData(format!(
                                "Overflow page {} overlaps a B-tree page",
                                overflow_page_id
                            )));
                        }
                        if free_pages.contains(&overflow_page_id) {
                            return Err(crate::error::HematiteError::CorruptedData(format!(
                                "Overflow page {} is also on the freelist",
                                overflow_page_id
                            )));
                        }
                        if !owned_overflow_pages.insert(overflow_page_id) {
                            return Err(crate::error::HematiteError::CorruptedData(format!(
                                "Overflow page {} is shared by multiple values",
                                overflow_page_id
                            )));
                        }
                    }
                }
            }
        }
        NodeType::Internal => {
            for child_page_id in node.children {
                validate_tree_overflow_pages(
                    storage,
                    child_page_id,
                    tree_pages,
                    free_pages,
                    owned_overflow_pages,
                )?;
            }
        }
    }

    Ok(())
}

pub struct ByteTreeCursor {
    storage: Arc<Mutex<Pager>>,
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

    pub fn current(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        match self.inner.current() {
            Some((key, value)) => {
                let mut storage = self.storage.lock().unwrap();
                Ok(Some((
                    key.as_bytes().to_vec(),
                    hydrate_stored_value(&mut storage, value.as_bytes())?,
                )))
            }
            None => Ok(None),
        }
    }

    pub fn collect_all(&mut self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.first()?;
        self.collect_remaining()
    }

    pub fn collect_remaining(&mut self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut entries = Vec::new();
        while let Some(entry) = self.current()? {
            entries.push(entry);
            if self.next().is_err() {
                break;
            }
        }
        Ok(entries)
    }
}
