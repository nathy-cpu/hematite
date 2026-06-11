//! Generic byte-tree facade.
//!
//! This file is the main reusable interface of the B-tree layer. It lets callers treat a tree as
//! an ordered map from `&[u8]` to `&[u8]` while the implementation handles page layout, splitting,
//! merging, cursor navigation, and large-value overflow.
//!
//! Layer split:
//!
//! ```text
//! caller
//!   provides: ordered key bytes, opaque value bytes
//!   sees:     insert / delete / get / cursor / range helpers / stats
//!
//! byte tree
//!   owns:     root tracking, node mutation, structural validation, overflow-backed values
//!
//! pager
//!   owns:     page IO, free-page reuse, checksums, journaling, WAL, locking
//! ```
//!
//! Large values are represented with a B-tree-owned wrapper:
//!
//! ```text
//! logical value
//!      |
//!      v
//! StoredValueLayout
//!   local payload bytes
//!   total length
//!   first overflow page
//! ```
//!
//! That extra indirection is what keeps overflow handling generic instead of pushing it into the
//! catalog or table code.

use std::cell::RefCell;
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::btree::codec::RawBytesCodec;
use crate::btree::cursor::BTreeCursor;
use crate::btree::index::{BTreeIndex, TreeMutation};
use crate::btree::node::BTreeNode;
use crate::btree::tree::{
    collect_tree_page_ids, collect_tree_space_stats, reset_tree_pages, BTreeManager, TreeSpaceStats,
};
use crate::btree::value_store::{
    free_stored_value_overflow, hydrate_stored_value, hydrate_stored_value_with_cache,
    materialize_stored_value, StoredValueLayout,
};
use crate::btree::NodeType;
use crate::btree::{BTreeKey, BTreeValue};
use crate::error::{HematiteError, Result};
use crate::storage::overflow::{
    collect_overflow_page_ids, validate_overflow_chain, OverflowReadCache,
};
use crate::storage::{
    JournalMode, Page, PageId, Pager, PagerIntegrityReport, DB_HEADER_PAGE_ID, INVALID_PAGE_ID,
    PAGE_SIZE, STORAGE_METADATA_PAGE_ID,
};

#[derive(Debug, Clone)]
pub struct ByteTreeStore {
    storage: Arc<RwLock<Pager>>,
}

#[derive(Debug, Clone)]
pub(crate) struct ByteTreeStoreSnapshot {
    pager: crate::storage::pager::PagerSnapshot,
}

impl ByteTreeStoreSnapshot {
    pub(crate) fn into_transaction_baseline(mut self) -> Self {
        self.pager = self.pager.into_transaction_baseline();
        self
    }
}

impl ByteTreeStore {
    pub const PAGE_SIZE: usize = PAGE_SIZE;
    pub const INVALID_PAGE_ID: PageId = INVALID_PAGE_ID;
    pub const DB_HEADER_PAGE_ID: PageId = DB_HEADER_PAGE_ID;
    pub const RESERVED_METADATA_PAGE_ID: PageId = STORAGE_METADATA_PAGE_ID;

    fn lock_storage_read(&self) -> Result<RwLockReadGuard<'_, Pager>> {
        self.storage.read().map_err(|_| {
            HematiteError::InternalError("ByteTreeStore storage lock is poisoned".to_string())
        })
    }

    fn lock_storage(&self) -> Result<RwLockWriteGuard<'_, Pager>> {
        self.storage.write().map_err(|_| {
            HematiteError::InternalError("ByteTreeStore storage lock is poisoned".to_string())
        })
    }

    pub fn open_path<P: AsRef<Path>>(path: P, cache_capacity: usize) -> Result<Self> {
        Ok(Self::new(Pager::new(path, cache_capacity)?))
    }

    pub fn new_in_memory(cache_capacity: usize) -> Result<Self> {
        Ok(Self::new(Pager::new_in_memory(cache_capacity)?))
    }

    pub fn new(storage: Pager) -> Self {
        Self {
            storage: Arc::new(RwLock::new(storage)),
        }
    }

    pub fn from_shared_storage(storage: Arc<RwLock<Pager>>) -> Self {
        Self { storage }
    }

    pub fn shared_storage(&self) -> Arc<RwLock<Pager>> {
        self.storage.clone()
    }

    pub fn read_reserved_blob(&self, page_id: PageId) -> Result<Option<Vec<u8>>> {
        let pager = self.lock_storage_read()?;
        match pager.read_page_shared(page_id) {
            Ok(page) => Ok(Some(page.data.clone())),
            Err(err) => {
                // Only map clearly "page missing / deallocated" conditions to Ok(None).
                // For other errors (IO, checksum, corruption), propagate the error.
                use crate::error::HematiteError;
                match err {
                    HematiteError::StorageError(ref msg)
                        if msg.contains("not allocated")
                            || msg.contains("is deallocated")
                            || msg.contains("not allocated in the current WAL-visible state")
                            || msg.contains("is deallocated in the active WAL transaction") =>
                    {
                        Ok(None)
                    }
                    other => Err(other),
                }
            }
        }
    }

    pub fn write_reserved_blob(&self, page_id: PageId, bytes: &[u8]) -> Result<()> {
        if bytes.len() > PAGE_SIZE {
            return Err(HematiteError::StorageError(format!(
                "Reserved page payload exceeds page size: {} > {}",
                bytes.len(),
                PAGE_SIZE
            )));
        }
        let mut page = Page::new(page_id);
        page.data[..bytes.len()].copy_from_slice(bytes);
        self.lock_storage()?.write_page(page)
    }

    pub fn flush(&self) -> Result<()> {
        self.lock_storage()?.flush()
    }

    pub fn begin_transaction(&self) -> Result<()> {
        self.lock_storage()?.begin_transaction()
    }

    pub fn commit_transaction(&self) -> Result<()> {
        self.lock_storage()?.commit_transaction()
    }

    pub fn rollback_transaction(&self) -> Result<()> {
        self.lock_storage()?.rollback_transaction()
    }

    pub fn transaction_active(&self) -> Result<bool> {
        Ok(self.lock_storage_read()?.transaction_active())
    }

    pub(crate) fn has_pending_changes(&self) -> Result<bool> {
        self.lock_storage_read()?.has_pending_changes()
    }

    pub(crate) fn snapshot(&self) -> Result<ByteTreeStoreSnapshot> {
        Ok(ByteTreeStoreSnapshot {
            pager: self.lock_storage()?.snapshot()?,
        })
    }

    pub(crate) fn restore_snapshot(&self, snapshot: ByteTreeStoreSnapshot) -> Result<()> {
        self.lock_storage()?.restore_snapshot(snapshot.pager)
    }

    fn run_atomically<T>(&self, operation: impl FnOnce(&Self) -> Result<T>) -> Result<T> {
        let snapshot = self.snapshot()?;
        match operation(self) {
            Ok(result) => Ok(result),
            Err(err) => {
                self.restore_snapshot(snapshot)?;
                Err(err)
            }
        }
    }

    pub fn begin_read(&self) -> Result<()> {
        self.lock_storage()?.begin_read()
    }

    pub fn end_read(&self) -> Result<()> {
        self.lock_storage()?.end_read()
    }

    pub fn journal_mode(&self) -> Result<JournalMode> {
        Ok(self.lock_storage_read()?.journal_mode())
    }

    pub fn set_journal_mode(&self, journal_mode: JournalMode) -> Result<()> {
        self.lock_storage()?.set_journal_mode(journal_mode)
    }

    pub fn checkpoint_wal(&self) -> Result<()> {
        self.lock_storage()?.checkpoint_wal()
    }

    pub fn file_len(&self) -> Result<u64> {
        self.lock_storage_read()?.file_len()
    }

    pub fn allocated_page_count(&self) -> Result<usize> {
        Ok(self.lock_storage_read()?.allocated_page_count())
    }

    pub fn free_page_ids(&self) -> Result<Vec<PageId>> {
        Ok(self.lock_storage_read()?.logical_free_pages().to_vec())
    }

    pub fn next_page_id(&self) -> Result<PageId> {
        Ok(self.lock_storage_read()?.next_page_id())
    }

    pub fn fragmented_free_page_count(&self) -> Result<usize> {
        Ok(self.lock_storage_read()?.fragmented_free_page_count())
    }

    pub fn trailing_free_page_count(&self) -> Result<usize> {
        Ok(self.lock_storage_read()?.trailing_free_page_count())
    }

    pub fn validate_storage(&self) -> Result<PagerIntegrityReport> {
        self.lock_storage_read()?.validate_integrity()
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
        self.run_atomically(|store| {
            {
                let mut pager = store.lock_storage()?;
                free_tree_overflow(&mut pager, root_page_id)?;
            }
            let mut manager = BTreeManager::from_shared_storage(store.storage.clone());
            manager.delete_tree(root_page_id)
        })
    }

    pub fn validate_tree(&self, root_page_id: PageId) -> Result<bool> {
        let mut manager = BTreeManager::from_shared_storage(self.storage.clone());
        if !manager.validate_tree(root_page_id)? {
            return Ok(false);
        }
        Ok(self.validate_tree_overflow(root_page_id).is_ok())
    }

    pub fn validate_tree_overflow(&self, root_page_id: PageId) -> Result<()> {
        let pager = self.lock_storage_read()?;
        let mut tree_page_ids = Vec::new();
        collect_tree_page_ids(&pager, root_page_id, &mut tree_page_ids)?;
        let tree_pages = tree_page_ids.into_iter().collect::<HashSet<_>>();
        let free_pages = pager.free_pages().iter().copied().collect::<HashSet<_>>();
        let mut owned_overflow_pages = HashSet::new();
        validate_tree_overflow_pages(
            &pager,
            root_page_id,
            &tree_pages,
            &free_pages,
            &mut owned_overflow_pages,
        )
    }

    pub fn reset_tree(&self, root_page_id: PageId) -> Result<()> {
        self.run_atomically(|store| {
            let mut pager = store.lock_storage()?;
            free_tree_overflow(&mut pager, root_page_id)?;
            reset_tree_pages(&mut pager, root_page_id)
        })
    }

    pub fn collect_page_ids(&self, root_page_id: PageId) -> Result<Vec<PageId>> {
        let pager = self.lock_storage_read()?;
        let mut page_ids = Vec::new();
        collect_tree_page_ids(&pager, root_page_id, &mut page_ids)?;
        Ok(page_ids)
    }

    pub fn collect_space_stats(&self, root_page_id: PageId) -> Result<TreeSpaceStats> {
        let pager = self.lock_storage_read()?;
        collect_tree_space_stats(&pager, root_page_id)
    }
}

pub struct ByteTree {
    storage: Arc<RwLock<Pager>>,
    index: BTreeIndex,
}

impl ByteTree {
    fn lock_storage_read(&self) -> Result<RwLockReadGuard<'_, Pager>> {
        self.storage.read().map_err(|_| {
            HematiteError::InternalError("ByteTree storage lock is poisoned".to_string())
        })
    }

    fn lock_storage(&self) -> Result<RwLockWriteGuard<'_, Pager>> {
        self.storage.write().map_err(|_| {
            HematiteError::InternalError("ByteTree storage lock is poisoned".to_string())
        })
    }

    fn snapshot_storage(&self) -> Result<ByteTreeStoreSnapshot> {
        Ok(ByteTreeStoreSnapshot {
            pager: self.lock_storage()?.snapshot()?,
        })
    }

    fn restore_storage_snapshot(&self, snapshot: ByteTreeStoreSnapshot) -> Result<()> {
        self.lock_storage()?.restore_snapshot(snapshot.pager)
    }

    fn run_atomically<T>(&mut self, operation: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        let snapshot = self.snapshot_storage()?;
        match operation(self) {
            Ok(result) => Ok(result),
            Err(err) => {
                self.restore_storage_snapshot(snapshot)?;
                Err(err)
            }
        }
    }

    pub fn root_page_id(&self) -> PageId {
        self.index.root_page_id()
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.index.search_typed::<RawBytesCodec>(&key.to_vec())? {
            Some(stored_value) => {
                let storage = self.lock_storage_read()?;
                Ok(Some(hydrate_stored_value(&storage, &stored_value)?))
            }
            None => Ok(None),
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.insert_with_mutation(key, value).map(|_| ())
    }

    pub fn insert_with_mutation(&mut self, key: &[u8], value: &[u8]) -> Result<TreeMutation> {
        let encoded_key = key.to_vec();
        self.run_atomically(|tree| {
            let stored_value = {
                let mut storage = tree.lock_storage()?;
                materialize_stored_value(&mut storage, value)?
            };

            // Single-pass insert: insert_replacing_with_mutation returns the
            // old value (if any) alongside the mutation, eliminating the need
            // for a separate pre-search traversal.
            let (mutation, old_value) = tree.index.insert_replacing_with_mutation(
                BTreeKey::new(encoded_key),
                BTreeValue::new(stored_value),
            )?;

            // Free overflow pages from the old value, if present.
            if let Some(old_stored_value) = old_value {
                let mut storage = tree.lock_storage()?;
                free_stored_value_overflow(&mut storage, old_stored_value.as_bytes())?;
            }

            Ok(mutation)
        })
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.delete_with_mutation(key).map(|(value, _)| value)
    }

    pub fn delete_with_mutation(&mut self, key: &[u8]) -> Result<(Option<Vec<u8>>, TreeMutation)> {
        let encoded_key = key.to_vec();
        self.run_atomically(|tree| {
            let (stored_value, mutation) = tree
                .index
                .delete_typed_with_mutation::<RawBytesCodec>(&encoded_key)?;
            let logical_value = match stored_value {
                Some(stored_value) => {
                    let mut storage = tree.lock_storage()?;
                    let logical_value = hydrate_stored_value(&storage, &stored_value)?;
                    free_stored_value_overflow(&mut storage, &stored_value)?;
                    Some(logical_value)
                }
                None => None,
            };
            Ok((logical_value, mutation))
        })
    }

    pub fn entry(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
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
            cursor.next()?;
        }
        Ok(entries)
    }

    pub fn cursor(&self) -> Result<ByteTreeCursor> {
        Ok(ByteTreeCursor {
            storage: self.storage.clone(),
            inner: self.index.cursor()?,
            overflow_cache: RefCell::new(OverflowReadCache::default()),
        })
    }
}

fn free_tree_overflow(storage: &mut Pager, root_page_id: PageId) -> Result<()> {
    let page = storage.read_page_shared(root_page_id)?;
    let node = BTreeNode::from_shared_page(page)?;

    match node.node_type {
        NodeType::Leaf => {
            for index in 0..node.key_count {
                free_stored_value_overflow(storage, node.get_value_view(index)?)?;
            }
        }
        NodeType::Internal => {
            for child_index in 0..=node.key_count {
                let child_page_id = node.get_child_procedural(child_index)?;
                free_tree_overflow(storage, child_page_id)?;
            }
        }
    }

    Ok(())
}

fn validate_tree_overflow_pages(
    storage: &Pager,
    root_page_id: PageId,
    tree_pages: &HashSet<PageId>,
    free_pages: &HashSet<PageId>,
    owned_overflow_pages: &mut HashSet<PageId>,
) -> Result<()> {
    let page = storage.read_page_shared(root_page_id)?;
    let node = BTreeNode::from_shared_page(page)?;

    match node.node_type {
        NodeType::Leaf => {
            for index in 0..node.key_count {
                let layout = StoredValueLayout::decode(node.get_value_view(index)?)?;
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
            for child_index in 0..=node.key_count {
                let child_page_id = node.get_child_procedural(child_index)?;
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
    storage: Arc<RwLock<Pager>>,
    inner: BTreeCursor,
    overflow_cache: RefCell<OverflowReadCache>,
}

impl ByteTreeCursor {
    fn lock_storage(&self) -> Result<RwLockReadGuard<'_, Pager>> {
        self.storage.read().map_err(|_| {
            HematiteError::InternalError("ByteTreeCursor storage lock is poisoned".to_string())
        })
    }

    pub fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    pub fn first(&mut self) -> Result<()> {
        self.inner.first()
    }

    pub fn next(&mut self) -> Result<()> {
        self.inner.next()
    }

    pub fn prev(&mut self) -> Result<()> {
        self.inner.prev()
    }

    pub fn last(&mut self) -> Result<()> {
        self.inner.last()
    }

    /// Save the cursor position so it can survive tree mutations.
    /// Call `restore_position()` before continuing to use the cursor.
    pub fn save_position(&mut self) {
        self.inner.save_position();
    }

    /// Restore the cursor to the saved position.
    pub fn restore_position(&mut self) -> Result<()> {
        self.inner.restore_position()
    }

    pub fn seek(&mut self, key: &[u8]) -> Result<()> {
        self.inner.seek(&crate::btree::BTreeKey::new(key.to_vec()))
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.inner.key_view()
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.inner.value_view()
    }

    pub fn current(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        match (self.inner.key_view(), self.inner.value_view()) {
            (Some(key), Some(value)) => {
                let storage = self.lock_storage()?;
                let mut overflow_cache = self.overflow_cache.borrow_mut();
                Ok(Some((
                    key.to_vec(),
                    hydrate_stored_value_with_cache(&storage, value, &mut overflow_cache)?,
                )))
            }
            _ => Ok(None),
        }
    }

    #[cfg(test)]
    pub(crate) fn overflow_cache_stats(&self) -> (usize, usize) {
        self.overflow_cache.borrow().stats()
    }

    pub fn collect_all(&mut self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.first()?;
        self.collect_remaining()
    }

    pub fn collect_remaining(&mut self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut entries = Vec::new();
        while let Some(entry) = self.current()? {
            entries.push(entry);
            self.next()?;
        }
        Ok(entries)
    }
}
