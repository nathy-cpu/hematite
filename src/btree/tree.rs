//! B-tree operations and management.
//!
//! M0 storage contract notes:
//! - Each logical tree is identified by a root page id.
//! - The long-term storage model is a forest of trees (catalog + per-table + per-index).
//! - Tree lifecycle operations here (create/open/delete/validate) are the control plane that
//!   higher storage layers should use instead of direct page manipulation.

use crate::btree::index::BTreeIndex;
use crate::btree::node::BTreeNode;
use crate::btree::value_store::StoredValueLayout;
use crate::btree::NodeType;
use crate::error::Result;
use crate::storage::overflow::collect_overflow_page_ids;
use crate::storage::{
    Page, PageId, Pager, DB_HEADER_PAGE_ID, INVALID_PAGE_ID, STORAGE_METADATA_PAGE_ID,
};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

pub struct BTreeManager {
    storage: Arc<Mutex<Pager>>,
}

impl BTreeManager {
    pub fn new(storage: Pager) -> Self {
        Self {
            storage: Arc::new(Mutex::new(storage)),
        }
    }

    pub fn from_shared_storage(storage: Arc<Mutex<Pager>>) -> Self {
        Self { storage }
    }

    pub fn create_tree(&mut self) -> Result<PageId> {
        let mut pager = self.storage.lock().unwrap();
        create_tree_root(&mut pager)
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
        if root_page_id == INVALID_PAGE_ID
            || root_page_id == DB_HEADER_PAGE_ID
            || root_page_id == STORAGE_METADATA_PAGE_ID
        {
            return Ok(false);
        }

        let mut state = TreeValidationState {
            visited: HashSet::new(),
            leaf_depth: None,
        };

        self.validate_node_recursive(root_page_id, None, None, 0, &mut state)
    }

    fn validate_node_recursive(
        &mut self,
        page_id: PageId,
        lower_bound: Option<KeyBound>,
        upper_bound: Option<KeyBound>,
        depth: usize,
        state: &mut TreeValidationState,
    ) -> Result<bool> {
        if page_id == INVALID_PAGE_ID
            || page_id == DB_HEADER_PAGE_ID
            || page_id == STORAGE_METADATA_PAGE_ID
        {
            return Ok(false);
        }

        if !state.visited.insert(page_id) {
            return Ok(false);
        }

        let page = self.storage.lock().unwrap().read_page(page_id)?;
        let node = BTreeNode::from_page(page)?;

        // Check key ordering and per-node key bounds.
        for i in 1..node.keys.len() {
            if node.keys[i - 1] >= node.keys[i] {
                return Ok(false);
            }
        }
        for key in &node.keys {
            if let Some(lower) = &lower_bound {
                let below_lower = if lower.inclusive {
                    key.as_bytes() < lower.key.as_slice()
                } else {
                    key.as_bytes() <= lower.key.as_slice()
                };
                if below_lower {
                    return Ok(false);
                }
            }
            if let Some(upper) = &upper_bound {
                let above_upper = if upper.inclusive {
                    key.as_bytes() > upper.key.as_slice()
                } else {
                    key.as_bytes() >= upper.key.as_slice()
                };
                if above_upper {
                    return Ok(false);
                }
            }
        }

        match node.node_type {
            NodeType::Leaf => {
                // Leaf nodes should have matching keys and values
                if node.keys.len() != node.values.len() {
                    return Ok(false);
                }

                // All leaves should be at the same depth.
                if let Some(expected_depth) = state.leaf_depth {
                    if expected_depth != depth {
                        return Ok(false);
                    }
                } else {
                    state.leaf_depth = Some(depth);
                }

                Ok(true)
            }
            NodeType::Internal => {
                // Internal nodes should have children = keys + 1
                if node.children.len() != node.keys.len() + 1 {
                    return Ok(false);
                }
                if !node.values.is_empty() {
                    return Ok(false);
                }

                // Recursively validate children with tightened key ranges.
                for (child_index, child_page_id) in node.children.iter().copied().enumerate() {
                    let child_lower = if child_index == 0 {
                        lower_bound.clone()
                    } else {
                        Some(KeyBound {
                            key: node.keys[child_index - 1].as_bytes().to_vec(),
                            inclusive: true,
                        })
                    };
                    let child_upper = if child_index == node.keys.len() {
                        upper_bound.clone()
                    } else {
                        Some(KeyBound {
                            key: node.keys[child_index].as_bytes().to_vec(),
                            inclusive: false,
                        })
                    };

                    if !self.validate_node_recursive(
                        child_page_id,
                        child_lower,
                        child_upper,
                        depth + 1,
                        state,
                    )? {
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

pub fn create_tree_root(pager: &mut Pager) -> Result<PageId> {
    let root_page_id = pager.allocate_page()?;
    initialize_empty_tree_root(pager, root_page_id)?;
    Ok(root_page_id)
}

pub fn initialize_empty_tree_root(pager: &mut Pager, root_page_id: PageId) -> Result<()> {
    let mut root_page = Page::new(root_page_id);
    let root_node = BTreeNode::new_leaf(root_page_id);
    BTreeNode::to_page(&root_node, &mut root_page)?;
    pager.write_page(root_page)
}

pub fn reset_tree_pages(pager: &mut Pager, root_page_id: PageId) -> Result<()> {
    let mut page_ids = Vec::new();
    collect_tree_page_ids(pager, root_page_id, &mut page_ids)?;
    for page_id in page_ids {
        if page_id != root_page_id {
            pager.deallocate_page(page_id)?;
        }
    }
    initialize_empty_tree_root(pager, root_page_id)
}

pub fn collect_tree_page_ids(
    pager: &mut Pager,
    page_id: PageId,
    out: &mut Vec<PageId>,
) -> Result<()> {
    out.push(page_id);
    let page = pager.read_page(page_id)?;
    let node = BTreeNode::from_page(page)?;
    if node.node_type == NodeType::Internal {
        for child_page_id in node.children {
            collect_tree_page_ids(pager, child_page_id, out)?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TreeSpaceStats {
    pub page_ids: Vec<PageId>,
    pub overflow_page_ids: Vec<PageId>,
    pub used_bytes: usize,
    pub overflow_used_bytes: usize,
    pub leaf_pages: usize,
    pub internal_pages: usize,
}

pub fn collect_tree_space_stats(pager: &mut Pager, root_page_id: PageId) -> Result<TreeSpaceStats> {
    let mut visited = HashSet::new();
    let mut stats = TreeSpaceStats::default();
    collect_tree_space_stats_recursive(pager, root_page_id, &mut visited, &mut stats)?;
    Ok(stats)
}

fn collect_tree_space_stats_recursive(
    pager: &mut Pager,
    page_id: PageId,
    visited: &mut HashSet<PageId>,
    stats: &mut TreeSpaceStats,
) -> Result<()> {
    if !visited.insert(page_id) {
        return Err(crate::error::HematiteError::CorruptedData(format!(
            "Cycle detected while collecting tree space stats at page {}",
            page_id
        )));
    }

    let page = pager.read_page(page_id)?;
    let node = BTreeNode::from_page(page)?;
    stats.page_ids.push(page_id);
    stats.used_bytes += node.estimate_serialized_size();

    match node.node_type {
        NodeType::Leaf => {
            stats.leaf_pages += 1;
            for value in &node.values {
                let layout = StoredValueLayout::decode(value.as_bytes())?;
                if layout.overflow_first_page != INVALID_PAGE_ID {
                    let overflow_page_ids =
                        collect_overflow_page_ids(pager, Some(layout.overflow_first_page))?;
                    stats.overflow_used_bytes += layout.overflow_len();
                    for overflow_page_id in overflow_page_ids {
                        if visited.contains(&overflow_page_id)
                            || stats.overflow_page_ids.contains(&overflow_page_id)
                        {
                            return Err(crate::error::HematiteError::CorruptedData(format!(
                                "Duplicate overflow page {} encountered while collecting tree space stats",
                                overflow_page_id
                            )));
                        }
                        stats.overflow_page_ids.push(overflow_page_id);
                    }
                }
            }
        }
        NodeType::Internal => {
            stats.internal_pages += 1;
            for child_page_id in node.children {
                collect_tree_space_stats_recursive(pager, child_page_id, visited, stats)?;
            }
        }
    }

    Ok(())
}

#[derive(Debug, Default)]
struct TreeValidationState {
    visited: HashSet<PageId>,
    leaf_depth: Option<usize>,
}

#[derive(Debug, Clone)]
struct KeyBound {
    key: Vec<u8>,
    inclusive: bool,
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
