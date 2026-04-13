//! Stack-based B-tree cursor for ordered scans and point seeks.
//!
//! The cursor keeps the search path from the root to the current leaf as a stack of frames:
//!
//! ```text
//! root frame
//!   -> internal frame
//!      -> internal frame
//!         -> leaf frame (current key/value index)
//! ```
//!
//! That stack makes `next` work without parent pointers in the on-disk format:
//! - move within the current leaf if possible;
//! - otherwise walk upward until a sibling subtree is available;
//! - descend again to the leftmost leaf of that subtree.
//!
//! The cursor reads nodes through the shared pager but exposes only logical key/value positions.
use std::cell::OnceCell;
use std::sync::{Arc, RwLock, RwLockWriteGuard};

use crate::btree::node::BTreeNode;
use crate::btree::{BTreeKey, BTreeValue, NodeType};
use crate::error::{HematiteError, Result};
use crate::storage::{PageId, Pager};

#[derive(Debug)]
pub struct BTreeCursor {
    storage: Arc<RwLock<Pager>>,
    stack: Vec<CursorFrame>,
    at_end: bool,

    cached_key: OnceCell<BTreeKey>,
    cached_value: OnceCell<BTreeValue>,

    #[cfg(test)]
    root_page_id: PageId,
}

#[derive(Debug, Clone)]
struct CursorFrame {
    page_id: PageId,
    node: BTreeNode,
    index: usize,
}

impl BTreeCursor {
    pub fn new(storage: Arc<RwLock<Pager>>, root_page_id: PageId) -> Result<Self> {
        let mut cursor = Self {
            storage,
            stack: Vec::new(),
            at_end: false,
            cached_key: OnceCell::new(),
            cached_value: OnceCell::new(),
            #[cfg(test)]
            root_page_id,
        };

        cursor.seek_to_first(root_page_id)?;
        Ok(cursor)
    }

    fn lock_storage(&self) -> Result<RwLockWriteGuard<'_, Pager>> {
        self.storage.write().map_err(|_| {
            HematiteError::InternalError("B-tree cursor storage lock is poisoned".to_string())
        })
    }

    fn invalidate_cache(&mut self) {
        let _ = self.cached_key.take();
        let _ = self.cached_value.take();
    }

    pub(crate) fn key_view(&self) -> Option<&[u8]> {
        if !self.is_valid() {
            return None;
        }
        let frame = self.stack.last()?;
        if frame.index >= frame.node.key_count {
            return None;
        }
        frame.node.get_key_view(frame.index).ok()
    }

    pub(crate) fn value_view(&self) -> Option<&[u8]> {
        if !self.is_valid() {
            return None;
        }
        let frame = self.stack.last()?;
        if frame.node.node_type != NodeType::Leaf || frame.index >= frame.node.key_count {
            return None;
        }
        frame.node.get_value_view(frame.index).ok()
    }

    pub fn is_valid(&self) -> bool {
        !self.at_end && !self.stack.is_empty()
    }

    pub fn key(&self) -> Option<&BTreeKey> {
        let key = self.key_view()?;
        Some(self.cached_key.get_or_init(|| BTreeKey::new(key.to_vec())))
    }

    pub fn value(&self) -> Option<&BTreeValue> {
        let value = self.value_view()?;
        Some(
            self.cached_value
                .get_or_init(|| BTreeValue::new(value.to_vec())),
        )
    }

    pub fn current(&self) -> Option<(&BTreeKey, &BTreeValue)> {
        Some((self.key()?, self.value()?))
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub fn save_position(&self) -> Option<BTreeKey> {
        self.key_view().map(|key| BTreeKey::new(key.to_vec()))
    }

    #[cfg(test)]
    pub(crate) fn cache_materialized(&self) -> (bool, bool) {
        (
            self.cached_key.get().is_some(),
            self.cached_value.get().is_some(),
        )
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub fn restore_position(&mut self, saved_position: Option<BTreeKey>) -> Result<()> {
        if let Some(key) = saved_position {
            self.seek(&key)
        } else {
            self.stack.clear();
            self.at_end = true;
            self.invalidate_cache();
            Ok(())
        }
    }

    pub fn first(&mut self) -> Result<()> {
        if self.stack.is_empty() {
            return Err(HematiteError::InternalError("No root page".to_string()));
        }

        let root_page_id = self.stack[0].page_id;
        self.seek_to_first(root_page_id)
    }

    pub fn seek(&mut self, key: &BTreeKey) -> Result<()> {
        if self.stack.is_empty() {
            return Err(HematiteError::InternalError("No root page".to_string()));
        }

        let root_page_id = self.stack[0].page_id;
        self.seek_to_key(root_page_id, key)
    }

    pub fn seek_near(&mut self, key: &BTreeKey) -> Result<()> {
        if let Some(frame) = self.stack.last_mut() {
            if frame.node.node_type == NodeType::Leaf && frame.node.key_count > 0 {
                let first = frame.node.get_key_view(0).ok();
                let last = frame.node.get_key_view(frame.node.key_count - 1).ok();
                if let (Some(first), Some(last)) = (first, last) {
                    if key.as_bytes() >= first && key.as_bytes() <= last {
                        let index = frame.node.lower_bound_index(key);
                        frame.index = index;
                        self.at_end = index >= frame.node.key_count;
                        self.invalidate_cache();
                        return Ok(());
                    }
                }
            }
        }
        self.seek(key)
    }

    #[cfg(test)]
    pub fn last(&mut self) -> Result<()> {
        if self.stack.is_empty() {
            return Err(HematiteError::InternalError("No root page".to_string()));
        }

        let root_page_id = self.stack[0].page_id;
        self.seek_to_last(root_page_id)
    }

    fn seek_to_first(&mut self, root_page_id: PageId) -> Result<()> {
        self.stack.clear();
        self.at_end = false;
        self.traverse_to_leftmost_leaf(root_page_id)?;
        if let Some(last) = self.stack.last() {
            if last.node.key_count == 0 {
                self.at_end = true;
            }
        }
        self.invalidate_cache();
        Ok(())
    }

    #[cfg(test)]
    fn seek_to_last(&mut self, root_page_id: PageId) -> Result<()> {
        self.stack.clear();
        self.at_end = false;
        self.traverse_to_rightmost_leaf(root_page_id)?;
        if let Some(last) = self.stack.last() {
            if last.node.key_count == 0 {
                self.at_end = true;
            }
        }
        self.invalidate_cache();
        Ok(())
    }

    fn seek_to_key(&mut self, page_id: PageId, key: &BTreeKey) -> Result<()> {
        let mut current_page_id = page_id;
        let mut frames = Vec::new();
        let at_end = {
            let mut storage = self.lock_storage()?;
            loop {
                let page = storage.read_page_shared(current_page_id)?;
                let node = BTreeNode::from_shared_page(page)?;

                match node.node_type {
                    NodeType::Leaf => {
                        let index = node.lower_bound_index(key);
                        let frame = CursorFrame {
                            page_id: current_page_id,
                            node,
                            index,
                        };
                        let reached_end = index >= frame.node.key_count;
                        frames.push(frame);
                        break reached_end;
                    }
                    NodeType::Internal => {
                        let child_index = node.upper_bound_index(key);
                        let next_child = node.get_child_procedural(child_index)?;
                        frames.push(CursorFrame {
                            page_id: current_page_id,
                            node,
                            index: child_index,
                        });
                        current_page_id = next_child;
                    }
                }
            }
        };

        self.stack = frames;
        self.at_end = at_end;
        self.invalidate_cache();
        Ok(())
    }

    pub fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Err(HematiteError::InternalError(
                "Cursor is at end or invalid".to_string(),
            ));
        }

        let current_frame = self.stack.last_mut().ok_or_else(|| {
            HematiteError::InternalError("B-tree cursor has no current frame".to_string())
        })?;
        current_frame.index += 1;

        // Check if we're still within the current leaf
        if current_frame.index < current_frame.node.key_count {
            self.invalidate_cache();
            return Ok(());
        }

        // Need to move to next leaf
        self.move_to_next_leaf()?;
        self.invalidate_cache();
        Ok(())
    }

    #[cfg(test)]
    pub fn prev(&mut self) -> Result<()> {
        if self.at_end {
            self.move_to_last_position()?;
            return Ok(());
        }

        if !self.is_valid() {
            return Err(HematiteError::InternalError(
                "Cursor is invalid".to_string(),
            ));
        }

        let current_frame = self.stack.last_mut().ok_or_else(|| {
            HematiteError::InternalError("B-tree cursor has no current frame".to_string())
        })?;

        if current_frame.index > 0 {
            current_frame.index -= 1;
            self.invalidate_cache();
            return Ok(());
        }

        self.move_to_previous_leaf()?;
        self.invalidate_cache();
        Ok(())
    }

    fn move_to_next_leaf(&mut self) -> Result<()> {
        // Find the next leaf by traversing up and then down
        while let Some(_frame) = self.stack.pop() {
            if self.stack.is_empty() {
                // We're at the root, no more leaves
                self.at_end = true;
                return Ok(());
            }

            let parent_frame = self.stack.last_mut().ok_or_else(|| {
                HematiteError::InternalError("B-tree cursor lost its parent frame".to_string())
            })?;
            // A node with N keys has N+1 children. The last child index is N.
            if parent_frame.index < parent_frame.node.key_count {
                // Move to next child in parent
                parent_frame.index += 1;
                let next_child_id = parent_frame.node.get_child_procedural(parent_frame.index)?;

                // Traverse down to the leftmost leaf of this subtree
                self.traverse_to_leftmost_leaf(next_child_id)?;
                return Ok(());
            }
        }

        // No more leaves
        self.at_end = true;
        Ok(())
    }

    #[cfg(test)]
    fn move_to_previous_leaf(&mut self) -> Result<()> {
        while let Some(_frame) = self.stack.pop() {
            if self.stack.is_empty() {
                self.at_end = true;
                return Ok(());
            }

            let parent_frame = self.stack.last_mut().ok_or_else(|| {
                HematiteError::InternalError("B-tree cursor lost its parent frame".to_string())
            })?;
            if parent_frame.index > 0 {
                parent_frame.index -= 1;
                let prev_child_id = parent_frame.node.get_child_procedural(parent_frame.index)?;
                self.traverse_to_rightmost_leaf(prev_child_id)?;
                return Ok(());
            }
        }

        self.at_end = true;
        Ok(())
    }

    #[cfg(test)]
    fn move_to_last_position(&mut self) -> Result<()> {
        self.at_end = false;

        if let Some(frame) = self.stack.last_mut() {
            if frame.index >= frame.node.key_count {
                frame.index = frame.node.key_count.saturating_sub(1);
            }
            return Ok(());
        }

        self.traverse_to_rightmost_leaf(self.root_page_id)?;
        Ok(())
    }

    fn traverse_to_leftmost_leaf(&mut self, page_id: PageId) -> Result<()> {
        let mut current_page_id = page_id;
        let mut frames = Vec::new();
        let mut storage = self.lock_storage()?;

        loop {
            let page = storage.read_page_shared(current_page_id)?;
            let node = BTreeNode::from_shared_page(page)?;
            let next_child = match node.node_type {
                NodeType::Leaf => None,
                NodeType::Internal => {
                    if node.key_count == 0 && node.get_child_procedural(0).is_err() {
                        return Err(HematiteError::CorruptedData(
                            "Internal node has no children".to_string(),
                        ));
                    }
                    Some(node.get_child_procedural(0)?)
                }
            };

            frames.push(CursorFrame {
                page_id: current_page_id,
                node,
                index: 0,
            });

            if let Some(next_child) = next_child {
                current_page_id = next_child;
            } else {
                break;
            }
        }

        drop(storage);
        self.stack.extend(frames);
        Ok(())
    }

    #[cfg(test)]
    fn traverse_to_rightmost_leaf(&mut self, page_id: PageId) -> Result<()> {
        let mut current_page_id = page_id;
        let mut frames = Vec::new();
        let mut storage = self.lock_storage()?;

        loop {
            let page = storage.read_page_shared(current_page_id)?;
            let node = BTreeNode::from_shared_page(page)?;

            let index = if node.node_type == NodeType::Leaf {
                node.key_count.saturating_sub(1)
            } else {
                node.key_count // Last child index is key_count
            };
            let next_child = match node.node_type {
                NodeType::Leaf => None,
                NodeType::Internal => {
                    if node.key_count == 0 && node.get_child_procedural(0).is_err() {
                        return Err(HematiteError::CorruptedData(
                            "Internal node has no children".to_string(),
                        ));
                    }
                    Some(node.get_child_procedural(node.key_count)?)
                }
            };

            frames.push(CursorFrame {
                page_id: current_page_id,
                node,
                index,
            });

            if let Some(next_child) = next_child {
                current_page_id = next_child;
            } else {
                break;
            }
        }

        drop(storage);
        self.stack.extend(frames);
        Ok(())
    }
}
