//! Generic tree lifecycle, validation, and space-accounting helpers.
//!
//! This file contains the control-plane operations around a tree root:
//! - create a fresh root page;
//! - open an existing root and validate it is a B-tree page;
//! - delete or reset an entire tree;
//! - walk all pages in a tree for validation or space accounting.
//!
//! It complements `index.rs`:
//! - `index.rs` mutates a single tree;
//! - `tree.rs` manages a tree as a durable object rooted at one page id.

use crate::btree::index::BTreeIndex;
use crate::btree::node::BTreeNode;
use crate::btree::value_store::StoredValueLayout;
use crate::btree::NodeType;
use crate::error::{HematiteError, Result};
use crate::storage::overflow::collect_overflow_page_ids;
use crate::storage::{
    Page, PageId, Pager, DB_HEADER_PAGE_ID, INVALID_PAGE_ID, STORAGE_METADATA_PAGE_ID,
};
use std::collections::HashSet;
use std::sync::{Arc, Mutex, MutexGuard};

pub struct BTreeManager {
    storage: Arc<Mutex<Pager>>,
}

impl BTreeManager {
    fn lock_storage(&self) -> Result<MutexGuard<'_, Pager>> {
        self.storage.lock().map_err(|_| {
            HematiteError::InternalError("B-tree manager storage mutex is poisoned".to_string())
        })
    }

    #[cfg(test)]
    pub fn new(storage: Pager) -> Self {
        Self {
            storage: Arc::new(Mutex::new(storage)),
        }
    }

    pub fn from_shared_storage(storage: Arc<Mutex<Pager>>) -> Self {
        Self { storage }
    }

    pub fn create_tree(&mut self) -> Result<PageId> {
        let mut pager = self.lock_storage()?;
        create_tree_root(&mut pager)
    }

    pub fn open_tree(&mut self, root_page_id: PageId) -> Result<BTreeIndex> {
        let page = self.lock_storage()?.read_page(root_page_id)?;
        let _node = BTreeNode::from_page(page)?;
        Ok(BTreeIndex::from_shared_storage(
            self.storage.clone(),
            root_page_id,
        ))
    }

    pub fn delete_tree(&mut self, root_page_id: PageId) -> Result<()> {
        self.delete_tree_recursive(root_page_id)?;
        Ok(())
    }

    fn delete_tree_recursive(&mut self, page_id: PageId) -> Result<()> {
        let page = self.lock_storage()?.read_page(page_id)?;
        let node = BTreeNode::from_page(page)?;

        match node.node_type {
            NodeType::Leaf => {
                self.lock_storage()?.deallocate_page(page_id)?;
            }
            NodeType::Internal => {
                for child_page_id in node.children {
                    self.delete_tree_recursive(child_page_id)?;
                }
                self.lock_storage()?.deallocate_page(page_id)?;
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

        let page = self.lock_storage()?.read_page(page_id)?;
        let node = BTreeNode::from_page(page)?;

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
                if node.keys.len() != node.values.len() {
                    return Ok(false);
                }

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
                if node.children.len() != node.keys.len() + 1 {
                    return Ok(false);
                }
                if !node.values.is_empty() {
                    return Ok(false);
                }

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
