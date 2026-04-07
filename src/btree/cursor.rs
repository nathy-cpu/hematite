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
use crate::btree::node::BTreeNode;
use crate::btree::{BTreeKey, BTreeValue, NodeType};
use crate::error::{HematiteError, Result};
use crate::storage::{PageId, Pager};
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug)]
pub struct BTreeCursor {
    storage: Arc<Mutex<Pager>>,
    stack: Vec<CursorFrame>,
    at_end: bool,

    cached_key: Option<BTreeKey>,
    cached_value: Option<BTreeValue>,

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
    pub fn new(storage: Arc<Mutex<Pager>>, root_page_id: PageId) -> Result<Self> {
        let mut cursor = Self {
            storage,
            stack: Vec::new(),
            at_end: false,
            cached_key: None,
            cached_value: None,
            #[cfg(test)]
            root_page_id,
        };

        cursor.seek_to_first(root_page_id)?;
        Ok(cursor)
    }

    fn lock_storage(&self) -> Result<MutexGuard<'_, Pager>> {
        self.storage.lock().map_err(|_| {
            HematiteError::InternalError("B-tree cursor storage mutex is poisoned".to_string())
        })
    }

    fn sync_cache(&mut self) {
        self.cached_key = None;
        self.cached_value = None;
        if !self.is_valid() {
            return;
        }

        if let Some(frame) = self.stack.last() {
            if frame.index < frame.node.key_count {
                self.cached_key = frame.node.get_key_procedural(frame.index).ok();
                if frame.node.node_type == NodeType::Leaf {
                    self.cached_value = frame.node.get_value_procedural(frame.index).ok();
                }
            } else {
                self.at_end = true;
            }
        }
    }

    pub fn is_valid(&self) -> bool {
        !self.at_end && !self.stack.is_empty()
    }

    pub fn key(&self) -> Option<&BTreeKey> {
        if !self.is_valid() {
            return None;
        }
        self.cached_key.as_ref()
    }

    pub fn value(&self) -> Option<&BTreeValue> {
        if !self.is_valid() {
            return None;
        }
        self.cached_value.as_ref()
    }

    pub fn current(&self) -> Option<(&BTreeKey, &BTreeValue)> {
        if !self.is_valid() {
            return None;
        }
        if let (Some(k), Some(v)) = (self.cached_key.as_ref(), self.cached_value.as_ref()) {
            Some((k, v))
        } else {
            None
        }
    }

    pub fn save_position(&self) -> Option<BTreeKey> {
        self.cached_key.clone()
    }

    pub fn restore_position(&mut self, saved_position: Option<BTreeKey>) -> Result<()> {
        if let Some(key) = saved_position {
            self.seek(&key)
        } else {
            self.stack.clear();
            self.at_end = true;
            self.sync_cache();
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
        self.sync_cache();
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
        self.sync_cache();
        Ok(())
    }

    fn seek_to_key(&mut self, page_id: PageId, key: &BTreeKey) -> Result<()> {
        self.stack.clear();
        self.at_end = false;

        let mut current_page_id = page_id;

        loop {
            let page = self.lock_storage()?.read_page(current_page_id)?;
            let node = BTreeNode::from_page(page)?;

            match node.node_type {
                NodeType::Leaf => {
                    // Find the key in the leaf
                    let frame = CursorFrame {
                        page_id: current_page_id,
                        node: node.clone(),
                        index: 0,
                    };
                    self.stack.push(frame);

                    // Binary search for the key
                    let mut left = 0;
                    let mut right = node.key_count;

                    while left < right {
                        let mid = (left + right) / 2;
                        let mid_key_bytes = node.get_key_view(mid)?;
                        if mid_key_bytes < key.as_bytes() {
                            left = mid + 1;
                        } else {
                            right = mid;
                        }
                    }

                    let frame = self.stack.last_mut().ok_or_else(|| {
                        HematiteError::InternalError(
                            "B-tree cursor lost its leaf frame during seek".to_string(),
                        )
                    })?;
                    frame.index = left;
                    if left >= node.key_count {
                        self.at_end = true;
                    }
                    self.sync_cache();
                    return Ok(());
                }
                NodeType::Internal => {
                    // Find the correct child to traverse
                    let mut child_index = 0;
                    for i in 0..node.key_count {
                        let node_key_bytes = node.get_key_view(i)?;
                        if node_key_bytes < key.as_bytes() {
                            child_index = i + 1;
                        } else {
                            break;
                        }
                    }

                    let next_child = node.get_child_procedural(child_index)?;

                    let frame = CursorFrame {
                        page_id: current_page_id,
                        node: node.clone(),
                        index: child_index,
                    };
                    self.stack.push(frame);

                    current_page_id = next_child;
                }
            }
        }
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
            self.sync_cache();
            return Ok(());
        }

        // Need to move to next leaf
        self.move_to_next_leaf()?;
        self.sync_cache();
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
            self.sync_cache();
            return Ok(());
        }

        self.move_to_previous_leaf()?;
        self.sync_cache();
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

        loop {
            let page = self.lock_storage()?.read_page(current_page_id)?;
            let node = BTreeNode::from_page(page)?;

            let frame = CursorFrame {
                page_id: current_page_id,
                node: node.clone(),
                index: 0,
            };

            self.stack.push(frame);

            match node.node_type {
                NodeType::Leaf => {
                    break;
                }
                NodeType::Internal => {
                    if node.key_count == 0 && node.get_child_procedural(0).is_err() {
                        return Err(HematiteError::CorruptedData(
                            "Internal node has no children".to_string(),
                        ));
                    }
                    current_page_id = node.get_child_procedural(0)?;
                }
            }
        }

        Ok(())
    }

    #[cfg(test)]
    fn traverse_to_rightmost_leaf(&mut self, page_id: PageId) -> Result<()> {
        let mut current_page_id = page_id;

        loop {
            let page = self.lock_storage()?.read_page(current_page_id)?;
            let node = BTreeNode::from_page(page)?;

            let index = if node.node_type == NodeType::Leaf {
                node.key_count.saturating_sub(1)
            } else {
                node.key_count // Last child index is key_count
            };

            let frame = CursorFrame {
                page_id: current_page_id,
                node: node.clone(),
                index,
            };

            self.stack.push(frame);

            match node.node_type {
                NodeType::Leaf => break,
                NodeType::Internal => {
                    if node.key_count == 0 && node.get_child_procedural(0).is_err() {
                        return Err(HematiteError::CorruptedData(
                            "Internal node has no children".to_string(),
                        ));
                    }
                    current_page_id = node.get_child_procedural(node.key_count)?;
                }
            }
        }

        Ok(())
    }
}
