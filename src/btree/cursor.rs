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
}

#[derive(Debug, Clone)]
struct CursorFrame {
    page_id: PageId,
    node: BTreeNode,
    index: usize,
}

impl BTreeCursor {
    pub fn new(storage: StorageEngine, root_page_id: PageId) -> Result<Self> {
        let mut cursor = Self {
            storage: Arc::new(Mutex::new(storage)),
            stack: Vec::new(),
            at_end: false,
        };

        cursor.seek_to_first(root_page_id)?;
        Ok(cursor)
    }

    pub fn seek_to_first(&mut self, root_page_id: PageId) -> Result<()> {
        self.stack.clear();
        self.at_end = false;

        let mut current_page_id = root_page_id;

        loop {
            let page = self.storage.lock().unwrap().read_page(current_page_id)?;
            let node = BTreeNode::from_page(page)?;

            let node_type = node.node_type;
            let frame = CursorFrame {
                page_id: current_page_id,
                node: node.clone(),
                index: 0,
            };

            self.stack.push(frame);

            match node_type {
                NodeType::Leaf => {
                    break;
                }
                NodeType::Internal => {
                    let last_frame = self.stack.last_mut().unwrap();
                    if last_frame.node.children.is_empty() {
                        return Err(HematiteError::CorruptedData(
                            "Internal node has no children".to_string(),
                        ));
                    }
                    current_page_id = last_frame.node.children[0];
                }
            }
        }

        Ok(())
    }

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
}
