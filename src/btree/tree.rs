//! B-tree operations and management.
//!
//! M0 storage contract notes:
//! - Each logical tree is identified by a root page id.
//! - The long-term storage model is a forest of trees (catalog + per-table + per-index).
//! - Tree lifecycle operations here (create/open/delete/validate) are the control plane that
//!   higher storage layers should use instead of direct page manipulation.

use crate::btree::{BTreeIndex, BTreeNode, NodeType};
use crate::error::Result;
use crate::storage::{Page, PageId, StorageEngine};
use std::sync::{Arc, Mutex};

pub struct BTreeManager {
    storage: Arc<Mutex<StorageEngine>>,
}

impl BTreeManager {
    pub fn new(storage: StorageEngine) -> Self {
        Self {
            storage: Arc::new(Mutex::new(storage)),
        }
    }

    pub fn from_shared_storage(storage: Arc<Mutex<StorageEngine>>) -> Self {
        Self { storage }
    }

    pub fn create_tree(&mut self) -> Result<PageId> {
        let root_page_id = self.storage.lock().unwrap().allocate_page()?;
        let mut root_page = Page::new(root_page_id);

        let root_node = BTreeNode::new_leaf(root_page_id);
        BTreeNode::to_page(&root_node, &mut root_page)?;

        self.storage.lock().unwrap().write_page(root_page)?;
        Ok(root_page_id)
    }

    pub fn open_tree(&mut self, root_page_id: PageId) -> Result<BTreeIndex> {
        // Verify that the root page exists and is a valid B-tree node
        let _page = self.storage.lock().unwrap().read_page(root_page_id)?;
        let _node = BTreeNode::from_page(_page)?; // Will error if invalid

        // Create a BTreeIndex with the shared storage engine
        let index = BTreeIndex::from_shared_storage(self.storage.clone(), root_page_id);
        Ok(index)
    }

    pub fn delete_tree(&mut self, root_page_id: PageId) -> Result<()> {
        // Recursively delete all pages in the tree
        self.delete_tree_recursive(root_page_id)?;
        Ok(())
    }

    fn delete_tree_recursive(&mut self, page_id: PageId) -> Result<()> {
        let page = self.storage.lock().unwrap().read_page(page_id)?;
        let node = BTreeNode::from_page(page)?;

        match node.node_type {
            NodeType::Leaf => {
                // Leaf nodes have no children, just deallocate the page
                self.storage.lock().unwrap().deallocate_page(page_id)?;
            }
            NodeType::Internal => {
                // Recursively delete all children
                for child_page_id in node.children {
                    self.delete_tree_recursive(child_page_id)?;
                }
                // Deallocate the internal node page
                self.storage.lock().unwrap().deallocate_page(page_id)?;
            }
        }
        Ok(())
    }

    pub fn validate_tree(&mut self, root_page_id: PageId) -> Result<bool> {
        let page = self.storage.lock().unwrap().read_page(root_page_id)?;
        let root_node = BTreeNode::from_page(page)?;

        self.validate_node_recursive(&root_node)
    }

    fn validate_node_recursive(&mut self, node: &BTreeNode) -> Result<bool> {
        // Check key ordering
        for i in 1..node.keys.len() {
            if node.keys[i - 1] >= node.keys[i] {
                return Ok(false);
            }
        }

        match node.node_type {
            NodeType::Leaf => {
                // Leaf nodes should have matching keys and values
                if node.keys.len() != node.values.len() {
                    return Ok(false);
                }
                Ok(true)
            }
            NodeType::Internal => {
                // Internal nodes should have children = keys + 1
                if node.children.len() != node.keys.len() + 1 {
                    return Ok(false);
                }

                // Recursively validate children
                for child_page_id in &node.children {
                    let page = self.storage.lock().unwrap().read_page(*child_page_id)?;
                    let child_node = BTreeNode::from_page(page)?;

                    if !self.validate_node_recursive(&child_node)? {
                        return Ok(false);
                    }
                }

                Ok(true)
            }
        }
    }

    pub fn get_tree_stats(&mut self, root_page_id: PageId) -> Result<TreeStats> {
        let page = self.storage.lock().unwrap().read_page(root_page_id)?;
        let root_node = BTreeNode::from_page(page)?;

        let mut stats = TreeStats::default();
        self.collect_stats_recursive(&root_node, &mut stats, 0)?;

        Ok(stats)
    }

    pub fn collect_stats_recursive(
        &mut self,
        node: &BTreeNode,
        stats: &mut TreeStats,
        depth: usize,
    ) -> Result<()> {
        stats.total_nodes += 1;
        stats.total_keys += node.keys.len();
        stats.max_depth = stats.max_depth.max(depth);

        match node.node_type {
            NodeType::Leaf => {
                stats.leaf_nodes += 1;
            }
            NodeType::Internal => {
                stats.internal_nodes += 1;

                for child_page_id in &node.children {
                    let page = self.storage.lock().unwrap().read_page(*child_page_id)?;
                    let child_node = BTreeNode::from_page(page)?;
                    self.collect_stats_recursive(&child_node, stats, depth + 1)?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct TreeStats {
    pub total_nodes: usize,
    pub leaf_nodes: usize,
    pub internal_nodes: usize,
    pub total_keys: usize,
    pub max_depth: usize,
}

impl TreeStats {
    pub fn average_keys_per_node(&self) -> f64 {
        if self.total_nodes == 0 {
            0.0
        } else {
            self.total_keys as f64 / self.total_nodes as f64
        }
    }
}
