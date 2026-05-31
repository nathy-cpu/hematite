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
//!
//! ## Cursor State Machine
//!
//! ```text
//! Valid ──next/prev──> Valid
//!   │                    │
//!   ├──past-end────> Invalid
//!   │
//!   ├──save_position──> RequireSeek ──restore_position──> Valid
//!   │
//!   └──error──> Fault
//! ```
use std::cell::OnceCell;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use crate::btree::node::BTreeNode;
use crate::btree::{BTreeKey, BTreeValue, NodeType};
use crate::error::{HematiteError, Result};
use crate::storage::{PageId, Pager};

/// Cursor state machine.
///
/// Models the lifecycle of a cursor position with respect to tree mutations
/// and boundary conditions. Modeled after SQLite's BtCursor state flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CursorState {
    /// Points to a valid entry; key/value access is safe.
    Valid,
    /// Does not point to a valid entry (empty tree, past end, before first).
    Invalid,
    /// Tree was modified; cursor must be restored from saved key before use.
    RequireSeek,
    /// Unrecoverable error occurred; all operations except `first()`/`seek()` will fail.
    Fault,
}

#[derive(Debug)]
pub struct BTreeCursor {
    storage: Arc<RwLock<Pager>>,
    stack: Vec<CursorFrame>,
    state: CursorState,

    /// Saved key for RequireSeek restoration.
    saved_key: Option<Vec<u8>>,

    cached_key: OnceCell<BTreeKey>,
    cached_value: OnceCell<BTreeValue>,

    root_page_id: PageId,

    /// True when the cursor is known to be at the rightmost entry in the tree.
    /// Enables fast-path for append-pattern seeks (Change 4: AtLast flag).
    at_last: bool,
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
            state: CursorState::Invalid,
            saved_key: None,
            cached_key: OnceCell::new(),
            cached_value: OnceCell::new(),
            root_page_id,
            at_last: false,
        };

        cursor.seek_to_first(root_page_id)?;
        Ok(cursor)
    }

    fn lock_storage(&self) -> Result<RwLockReadGuard<'_, Pager>> {
        self.storage.read().map_err(|_| {
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
        matches!(self.state, CursorState::Valid)
    }

    #[cfg(test)]
    pub fn key(&self) -> Option<&BTreeKey> {
        let key = self.key_view()?;
        Some(self.cached_key.get_or_init(|| BTreeKey::new(key.to_vec())))
    }

    #[cfg(test)]
    pub fn value(&self) -> Option<&BTreeValue> {
        let value = self.value_view()?;
        Some(
            self.cached_value
                .get_or_init(|| BTreeValue::new(value.to_vec())),
        )
    }

    #[cfg(test)]
    pub fn current(&self) -> Option<(&BTreeKey, &BTreeValue)> {
        Some((self.key()?, self.value()?))
    }

    /// Save the current cursor position so it can be restored after tree
    /// mutations. The cursor releases its page references (stack is cleared)
    /// to allow those pages to be evicted or modified.
    ///
    /// After this call the cursor is in `RequireSeek` state and must have
    /// `restore_position()` called before any further navigation.
    pub fn save_position(&mut self) {
        if let Some(key_bytes) = self.key_view() {
            self.saved_key = Some(key_bytes.to_vec());
            self.state = CursorState::RequireSeek;
        } else {
            self.saved_key = None;
            self.state = CursorState::Invalid;
        }
        // Drop node references so pages can be evicted.
        self.stack.clear();
        self.at_last = false;
        self.invalidate_cache();
    }

    /// Restore the cursor to the position that was saved with `save_position()`.
    /// If the saved key still exists, the cursor will point to it. If it was
    /// deleted, the cursor will point to the next key >= the saved key.
    pub fn restore_position(&mut self) -> Result<()> {
        match self.state {
            CursorState::RequireSeek => {
                if let Some(key) = self.saved_key.take() {
                    self.seek_to_key(self.root_page_id, &BTreeKey::new(key))?;
                } else {
                    self.state = CursorState::Invalid;
                }
                Ok(())
            }
            CursorState::Valid => Ok(()),
            CursorState::Invalid => Ok(()),
            CursorState::Fault => Err(HematiteError::InternalError(
                "Cursor is in fault state and cannot be restored".to_string(),
            )),
        }
    }

    #[cfg(test)]
    pub(crate) fn cache_materialized(&self) -> (bool, bool) {
        (
            self.cached_key.get().is_some(),
            self.cached_value.get().is_some(),
        )
    }

    pub fn first(&mut self) -> Result<()> {
        let root_page_id = self.root_page_id;
        self.at_last = false;
        self.seek_to_first(root_page_id)
    }

    pub fn seek(&mut self, key: &BTreeKey) -> Result<()> {
        // AtLast fast path: if we're at the rightmost leaf and the target key
        // is >= current key, check the current leaf page first. This avoids
        // a full root-to-leaf descent for append-pattern workloads.
        if self.at_last && self.state == CursorState::Valid {
            if let Some(current_key) = self.key_view() {
                if key.as_bytes() >= current_key {
                    let frame = self.stack.last().unwrap();
                    let idx = frame.node.lower_bound_index(key)?;
                    if idx <= frame.node.key_count {
                        let frame = self.stack.last_mut().unwrap();
                        frame.index = idx;
                        if idx >= frame.node.key_count {
                            self.state = CursorState::Invalid;
                            self.at_last = false;
                        }
                        // else: state stays Valid, at_last stays true
                        self.invalidate_cache();
                        return Ok(());
                    }
                }
            }
        }

        let root_page_id = self.root_page_id;
        self.at_last = false;
        self.seek_to_key(root_page_id, key)
    }

    pub fn last(&mut self) -> Result<()> {
        let root_page_id = self.root_page_id;
        self.seek_to_last(root_page_id)?;
        self.at_last = self.state == CursorState::Valid;
        Ok(())
    }

    fn seek_to_first(&mut self, root_page_id: PageId) -> Result<()> {
        self.stack.clear();
        self.state = CursorState::Valid;
        self.traverse_to_leftmost_leaf(root_page_id)?;
        if let Some(last) = self.stack.last() {
            if last.node.key_count == 0 {
                self.state = CursorState::Invalid;
            }
        }
        self.invalidate_cache();
        Ok(())
    }

    fn seek_to_last(&mut self, root_page_id: PageId) -> Result<()> {
        self.stack.clear();
        self.state = CursorState::Valid;
        self.traverse_to_rightmost_leaf(root_page_id)?;
        if let Some(last) = self.stack.last() {
            if last.node.key_count == 0 {
                self.state = CursorState::Invalid;
            }
        }
        self.invalidate_cache();
        Ok(())
    }

    fn seek_to_key(&mut self, page_id: PageId, key: &BTreeKey) -> Result<()> {
        let mut current_page_id = page_id;
        let mut frames = Vec::new();
        let at_end = {
            let storage = self.lock_storage()?;
            loop {
                let page = storage.read_page_shared(current_page_id)?;
                let node = BTreeNode::from_shared_page(page)?;

                match node.node_type {
                    NodeType::Leaf => {
                        let index = node.lower_bound_index(key)?;
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
                        let child_index = node.upper_bound_index(key)?;
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
        self.state = if at_end {
            CursorState::Invalid
        } else {
            CursorState::Valid
        };
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
        self.at_last = false;
        self.move_to_next_leaf()?;
        self.invalidate_cache();
        Ok(())
    }

    pub fn prev(&mut self) -> Result<()> {
        if self.state == CursorState::Invalid {
            if !self.stack.is_empty() {
                // At end with stack intact — move to last valid position.
                self.move_to_last_position()?;
            } else {
                // Stack was drained (e.g. next() walked past the end).
                // Re-seek to the rightmost entry from the root.
                self.seek_to_last(self.root_page_id)?;
            }
            self.at_last = false;
            return Ok(());
        }

        if !self.is_valid() {
            return Err(HematiteError::InternalError(
                "Cursor is invalid".to_string(),
            ));
        }

        self.at_last = false;

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
                self.state = CursorState::Invalid;
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
        self.state = CursorState::Invalid;
        Ok(())
    }

    fn move_to_previous_leaf(&mut self) -> Result<()> {
        while let Some(_frame) = self.stack.pop() {
            if self.stack.is_empty() {
                self.state = CursorState::Invalid;
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

        self.state = CursorState::Invalid;
        Ok(())
    }

    fn move_to_last_position(&mut self) -> Result<()> {
        self.state = CursorState::Valid;

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
        let storage = self.lock_storage()?;

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

    fn traverse_to_rightmost_leaf(&mut self, page_id: PageId) -> Result<()> {
        let mut current_page_id = page_id;
        let mut frames = Vec::new();
        let storage = self.lock_storage()?;

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
