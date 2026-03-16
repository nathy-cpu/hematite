//! B-tree cursor for sequential navigation

use crate::btree::{BTreeKey, BTreeNode, BTreeValue, NodeType};
use crate::error::{HematiteError, Result};
use crate::storage::{PageId, StorageEngine};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct BTreeCursor {
    storage: Arc<Mutex<StorageEngine>>,
    stack: Vec<CursorFrame>,
    at_end: bool,
    root_page_id: PageId,
}

#[derive(Debug, Clone)]
struct CursorFrame {
    page_id: PageId,
    node: BTreeNode,
    index: usize,
}

impl BTreeCursor {
    pub fn new(storage: Arc<Mutex<StorageEngine>>, root_page_id: PageId) -> Result<Self> {
        let mut cursor = Self {
            storage: storage,
            stack: Vec::new(),
            at_end: false,
            root_page_id: root_page_id,
        };

        cursor.seek_to_first(root_page_id)?;
        Ok(cursor)
    }

    // pub fn seek_to_first_original(&mut self, root_page_id: PageId) -> Result<()> {
    //     self.stack.clear();
    //     self.at_end = false;
    //
    //     let mut current_page_id = root_page_id;
    //
    //     loop {
    //         let page = self.storage.lock().unwrap().read_page(current_page_id)?;
    //         let node = BTreeNode::from_page(page)?;
    //
    //         let node_type = node.node_type;
    //         let frame = CursorFrame {
    //             page_id: current_page_id,
    //             node: node.clone(),
    //             index: 0,
    //         };
    //
    //         self.stack.push(frame);
    //
    //         match node_type {
    //             NodeType::Leaf => {
    //                 // Check if the leaf has any keys
    //                 if node.keys.is_empty() {
    //                     // Empty tree - mark as at end
    //                     self.at_end = true;
    //                 }
    //                 break;
    //             }
    //             NodeType::Internal => {
    //                 if node.children.is_empty() {
    //                     return Err(HematiteError::CorruptedData(
    //                         "Internal node has no children".to_string(),
    //                     ));
    //                 }
    //                 current_page_id = node.children[0];
    //             }
    //         }
    //     }
    //
    //     Ok(())
    // }

    pub fn is_valid(&self) -> bool {
        !self.at_end && !self.stack.is_empty()
    }

    pub fn key(&self) -> Option<&BTreeKey> {
        if !self.is_valid() {
            return None;
        }

        let frame = self.stack.last().unwrap();
        if frame.index >= frame.node.keys.len() {
            return None;
        }

        Some(&frame.node.keys[frame.index])
    }

    pub fn value(&self) -> Option<&BTreeValue> {
        if !self.is_valid() {
            return None;
        }

        let frame = self.stack.last().unwrap();
        if frame.index >= frame.node.values.len() {
            return None;
        }

        Some(&frame.node.values[frame.index])
    }

    pub fn current(&self) -> Option<(&BTreeKey, &BTreeValue)> {
        if !self.is_valid() {
            return None;
        }

        let frame = self.stack.last().unwrap();
        if frame.index >= frame.node.keys.len() || frame.index >= frame.node.values.len() {
            return None;
        }

        Some((
            &frame.node.keys[frame.index],
            &frame.node.values[frame.index],
        ))
    }

    pub fn first(&mut self) -> Result<()> {
        if self.stack.is_empty() {
            return Err(HematiteError::InternalError("No root page".to_string()));
        }

        let root_page_id = self.stack[0].page_id;
        self.seek_to_first(root_page_id)
    }

    pub fn last(&mut self) -> Result<()> {
        if self.stack.is_empty() {
            return Err(HematiteError::InternalError("No root page".to_string()));
        }

        let root_page_id = self.stack[0].page_id;
        self.seek_to_last(root_page_id)
    }

    pub fn seek(&mut self, key: &BTreeKey) -> Result<()> {
        if self.stack.is_empty() {
            return Err(HematiteError::InternalError("No root page".to_string()));
        }

        let root_page_id = self.stack[0].page_id;
        self.seek_to_key(root_page_id, key)
    }

    fn seek_to_first(&mut self, root_page_id: PageId) -> Result<()> {
        self.stack.clear();
        self.at_end = false;
        self.traverse_to_leftmost_leaf(root_page_id)?;
        if let Some(last) = self.stack.last() {
            if last.node.keys.is_empty() {
                self.at_end = true;
            }
        }
        Ok(())
    }

    fn seek_to_last(&mut self, root_page_id: PageId) -> Result<()> {
        self.stack.clear();
        self.at_end = false;
        self.traverse_to_rightmost_leaf(root_page_id)?;
        if let Some(last) = self.stack.last() {
            if last.node.keys.is_empty() {
                self.at_end = true;
            }
        }
        Ok(())
    }

    fn seek_to_key(&mut self, page_id: PageId, key: &BTreeKey) -> Result<()> {
        self.stack.clear();
        self.at_end = false;

        let mut current_page_id = page_id;

        loop {
            let page = self.storage.lock().unwrap().read_page(current_page_id)?;
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
                    let mut right = node.keys.len();

                    while left < right {
                        let mid = (left + right) / 2;
                        if &node.keys[mid] < key {
                            left = mid + 1;
                        } else {
                            right = mid;
                        }
                    }

                    self.stack.last_mut().unwrap().index = left;
                    if left >= node.keys.len() {
                        self.at_end = true;
                    }
                    return Ok(());
                }
                NodeType::Internal => {
                    // Find the correct child to traverse
                    let mut child_index = 0;
                    for (i, node_key) in node.keys.iter().enumerate() {
                        if node_key < key {
                            child_index = i + 1;
                        } else {
                            break;
                        }
                    }

                    let frame = CursorFrame {
                        page_id: current_page_id,
                        node: node.clone(),
                        index: child_index,
                    };
                    self.stack.push(frame);

                    current_page_id = node.children[child_index];
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

        let current_frame = self.stack.last_mut().unwrap();
        current_frame.index += 1;

        // Check if we're still within the current leaf
        if current_frame.index < current_frame.node.keys.len() {
            return Ok(());
        }

        // Need to move to next leaf
        self.move_to_next_leaf()
    }

    pub fn prev(&mut self) -> Result<()> {
        if self.at_end {
            // We're at the end, move to the last valid position
            self.move_to_last_position()?;
            return Ok(());
        }

        if !self.is_valid() {
            return Err(HematiteError::InternalError(
                "Cursor is invalid".to_string(),
            ));
        }

        let current_frame = self.stack.last_mut().unwrap();

        // Check if we can move within current leaf
        if current_frame.index > 0 {
            current_frame.index -= 1;
            return Ok(());
        }

        // Need to move to previous leaf
        self.move_to_previous_leaf()
    }

    fn move_to_next_leaf(&mut self) -> Result<()> {
        // Find the next leaf by traversing up and then down
        while let Some(_frame) = self.stack.pop() {
            if self.stack.is_empty() {
                // We're at the root, no more leaves
                self.at_end = true;
                return Ok(());
            }

            let parent_frame = self.stack.last_mut().unwrap();
            if parent_frame.index < parent_frame.node.children.len() - 1 {
                // Move to next child in parent
                parent_frame.index += 1;
                let next_child_id = parent_frame.node.children[parent_frame.index];

                // Traverse down to the leftmost leaf of this subtree
                self.traverse_to_leftmost_leaf(next_child_id)?;
                return Ok(());
            }
        }

        // No more leaves
        self.at_end = true;
        Ok(())
    }

    fn move_to_previous_leaf(&mut self) -> Result<()> {
        // Find the previous leaf by traversing up and then down
        while let Some(_frame) = self.stack.pop() {
            if self.stack.is_empty() {
                // We're at the root, no previous leaves
                self.at_end = true;
                return Ok(());
            }

            let parent_frame = self.stack.last_mut().unwrap();
            if parent_frame.index > 0 {
                // Move to previous child in parent
                parent_frame.index -= 1;
                let prev_child_id = parent_frame.node.children[parent_frame.index];

                // Traverse down to the rightmost leaf of this subtree
                self.traverse_to_rightmost_leaf(prev_child_id)?;
                return Ok(());
            }
        }

        // No previous leaves
        self.at_end = true;
        Ok(())
    }

    fn move_to_last_position(&mut self) -> Result<()> {
        self.at_end = false;

        // If we have a current position, move to the last key in the current leaf
        if let Some(frame) = self.stack.last_mut() {
            if frame.index >= frame.node.keys.len() {
                frame.index = frame.node.keys.len().saturating_sub(1);
            }
            return Ok(());
        }

        // Otherwise, traverse to the rightmost leaf
        self.traverse_to_rightmost_leaf(self.root_page_id)?;

        Ok(())
    }

    fn traverse_to_leftmost_leaf(&mut self, page_id: PageId) -> Result<()> {
        let mut current_page_id = page_id;

        loop {
            let page = self.storage.lock().unwrap().read_page(current_page_id)?;
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
                    if node.children.is_empty() {
                        return Err(HematiteError::CorruptedData(
                            "Internal node has no children".to_string(),
                        ));
                    }
                    current_page_id = node.children[0];
                }
            }
        }

        Ok(())
    }

    fn traverse_to_rightmost_leaf(&mut self, page_id: PageId) -> Result<()> {
        let mut current_page_id = page_id;

        loop {
            let page = self.storage.lock().unwrap().read_page(current_page_id)?;
            let node = BTreeNode::from_page(page)?;

            let index = if node.node_type == NodeType::Leaf {
                node.keys.len().saturating_sub(1) // Point to last key in leaf
            } else {
                node.children.len() - 1 // Point to last child in internal node
            };

            let frame = CursorFrame {
                page_id: current_page_id,
                node: node.clone(),
                index,
            };

            self.stack.push(frame);

            match node.node_type {
                NodeType::Leaf => {
                    break;
                }
                NodeType::Internal => {
                    if node.children.is_empty() {
                        return Err(HematiteError::CorruptedData(
                            "Internal node has no children".to_string(),
                        ));
                    }
                    current_page_id = node.children[node.children.len() - 1];
                }
            }
        }

        Ok(())
    }
}
