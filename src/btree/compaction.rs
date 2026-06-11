use crate::storage::{PageId, Pager, Page, INVALID_PAGE_ID};
use crate::btree::node::BTreeNode;
use crate::btree::NodeType;
use crate::btree::value_store::StoredValueLayout;
use crate::btree::BTreeValue;
use crate::error::{Result, HematiteError};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PageParent {
    Root,
    BTreeInternal {
        parent_page_id: PageId,
        child_index: usize,
    },
    BTreeLeafCell {
        leaf_page_id: PageId,
        cell_index: usize,
    },
    OverflowPredecessor {
        pred_page_id: PageId,
    },
}

pub(crate) fn build_parent_map(
    pager: &Pager,
    roots: &[PageId],
) -> Result<HashMap<PageId, PageParent>> {
    let mut parent_map = HashMap::new();
    let mut visited = HashSet::new();

    for &root_page_id in roots {
        if root_page_id >= 2 {
            trace_tree(
                pager,
                root_page_id,
                PageParent::Root,
                &mut parent_map,
                &mut visited,
            )?;
        }
    }

    Ok(parent_map)
}

fn trace_tree(
    pager: &Pager,
    page_id: PageId,
    parent: PageParent,
    parent_map: &mut HashMap<PageId, PageParent>,
    visited: &mut HashSet<PageId>,
) -> Result<()> {
    if !visited.insert(page_id) {
        return Ok(());
    }

    parent_map.insert(page_id, parent);

    let page = pager.read_page(page_id)?;
    let node = BTreeNode::from_page_decoded(page)?;

    match node.node_type {
        NodeType::Internal => {
            for (i, &child_page_id) in node.children.iter().enumerate() {
                if child_page_id >= 2 {
                    trace_tree(
                        pager,
                        child_page_id,
                        PageParent::BTreeInternal {
                            parent_page_id: page_id,
                            child_index: i,
                        },
                        parent_map,
                        visited,
                    )?;
                }
            }
        }
        NodeType::Leaf => {
            for (i, value) in node.values.iter().enumerate() {
                let layout = StoredValueLayout::decode(&value.data)?;
                if layout.overflow_first_page != INVALID_PAGE_ID && layout.overflow_first_page >= 2 {
                    trace_overflow(
                        pager,
                        layout.overflow_first_page,
                        PageParent::BTreeLeafCell {
                            leaf_page_id: page_id,
                            cell_index: i,
                        },
                        parent_map,
                        visited,
                    )?;
                }
            }
        }
    }

    Ok(())
}

fn trace_overflow(
    pager: &Pager,
    page_id: PageId,
    parent: PageParent,
    parent_map: &mut HashMap<PageId, PageParent>,
    visited: &mut HashSet<PageId>,
) -> Result<()> {
    if !visited.insert(page_id) {
        return Ok(());
    }

    parent_map.insert(page_id, parent);

    let page = pager.read_page(page_id)?;
    let next_page_id = u32::from_be_bytes([page.data[4], page.data[5], page.data[6], page.data[7]]);
    if next_page_id != 0 && next_page_id >= 2 {
        trace_overflow(
            pager,
            next_page_id,
            PageParent::OverflowPredecessor { pred_page_id: page_id },
            parent_map,
            visited,
        )?;
    }

    Ok(())
}

fn relocate_page<F>(
    storage: &Arc<RwLock<Pager>>,
    old_page_id: PageId,
    new_page_id: PageId,
    parent_map: &mut HashMap<PageId, PageParent>,
    mut update_root: F,
) -> Result<()>
where
    F: FnMut(PageId, PageId) -> Result<()>,
{
    // 1. Read the old page data.
    let mut page = {
        let pager = storage.read().map_err(|_| {
            HematiteError::InternalError("Pager lock is poisoned".to_string())
        })?;
        pager.read_page(old_page_id)?
    };

    // 2. Change the page ID to new_page_id and write to pager.
    page.id = new_page_id;
    {
        let mut pager = storage.write().map_err(|_| {
            HematiteError::InternalError("Pager lock is poisoned".to_string())
        })?;
        pager.mark_page_active(new_page_id)?;
        pager.write_page(page)?;
    }

    // 3. Find parent of old_page_id in the parent map.
    let parent = parent_map.get(&old_page_id).cloned().ok_or_else(|| {
        HematiteError::StorageError(format!(
            "Page {} has no parent in the parent map during relocation",
            old_page_id
        ))
    })?;

    // 4. Update the parent reference to point to the new page ID.
    match parent {
        PageParent::Root => {
            update_root(old_page_id, new_page_id)?;
        }
        PageParent::BTreeInternal {
            parent_page_id,
            child_index,
        } => {
            let mut parent_node = {
                let pager = storage.read().map_err(|_| {
                    HematiteError::InternalError("Pager lock is poisoned".to_string())
                })?;
                let parent_page = pager.read_page(parent_page_id)?;
                BTreeNode::from_page_decoded(parent_page)?
            };
            parent_node.children[child_index] = new_page_id;
            let mut new_parent_page = Page::new(parent_page_id);
            parent_node.to_page(&mut new_parent_page)?;
            {
                let mut pager = storage.write().map_err(|_| {
                    HematiteError::InternalError("Pager lock is poisoned".to_string())
                })?;
                pager.write_page(new_parent_page)?;
            }
        }
        PageParent::BTreeLeafCell {
            leaf_page_id,
            cell_index,
        } => {
            let mut leaf_node = {
                let pager = storage.read().map_err(|_| {
                    HematiteError::InternalError("Pager lock is poisoned".to_string())
                })?;
                let leaf_page = pager.read_page(leaf_page_id)?;
                BTreeNode::from_page_decoded(leaf_page)?
            };
            let mut layout = StoredValueLayout::decode(&leaf_node.values[cell_index].data)?;
            layout.overflow_first_page = new_page_id;
            leaf_node.values[cell_index] = BTreeValue::new(layout.encode()?);
            let mut new_leaf_page = Page::new(leaf_page_id);
            leaf_node.to_page(&mut new_leaf_page)?;
            {
                let mut pager = storage.write().map_err(|_| {
                    HematiteError::InternalError("Pager lock is poisoned".to_string())
                })?;
                pager.write_page(new_leaf_page)?;
            }
        }
        PageParent::OverflowPredecessor { pred_page_id } => {
            let mut pred_page = {
                let pager = storage.read().map_err(|_| {
                    HematiteError::InternalError("Pager lock is poisoned".to_string())
                })?;
                pager.read_page(pred_page_id)?
            };
            pred_page.data[4..8].copy_from_slice(&new_page_id.to_be_bytes());
            {
                let mut pager = storage.write().map_err(|_| {
                    HematiteError::InternalError("Pager lock is poisoned".to_string())
                })?;
                pager.write_page(pred_page)?;
            }
        }
    }

    // 5. Update the parent map dynamically.
    if let Some(parent_info) = parent_map.remove(&old_page_id) {
        parent_map.insert(new_page_id, parent_info);
    }
    for val in parent_map.values_mut() {
        match val {
            PageParent::BTreeInternal {
                parent_page_id, ..
            } if *parent_page_id == old_page_id => {
                *parent_page_id = new_page_id;
            }
            PageParent::BTreeLeafCell { leaf_page_id, .. } if *leaf_page_id == old_page_id => {
                *leaf_page_id = new_page_id;
            }
            PageParent::OverflowPredecessor { pred_page_id } if *pred_page_id == old_page_id => {
                *pred_page_id = new_page_id;
            }
            _ => {}
        }
    }

    // 6. Deallocate the old page ID.
    {
        let mut pager = storage.write().map_err(|_| {
            HematiteError::InternalError("Pager lock is poisoned".to_string())
        })?;
        pager.deallocate_page(old_page_id)?;
    }

    Ok(())
}

pub fn auto_vacuum<F>(
    storage: &Arc<RwLock<Pager>>,
    roots: &mut [PageId],
    mut update_root: F,
) -> Result<()>
where
    F: FnMut(PageId, PageId) -> Result<()>,
{
    loop {
        // Read lock storage to check state
        let (next_page_id, mut free_pages) = {
            let pager = storage.read().map_err(|_| {
                HematiteError::InternalError("Pager lock is poisoned".to_string())
            })?;
            let next_page = pager.next_page_id();
            let free_pages = pager.logical_free_pages().to_vec();
            (next_page, free_pages)
        };

        if next_page_id <= 2 {
            break;
        }
        let highest_page_id = next_page_id - 1;

        free_pages.sort_unstable();

        // Check if the highest page is actually a free page.
        // If it is, the Pager itself should compact it.
        if free_pages.contains(&highest_page_id) {
            let mut pager = storage.write().map_err(|_| {
                HematiteError::InternalError("Pager lock is poisoned".to_string())
            })?;
            pager.deallocate_page(highest_page_id)?;
            continue;
        }

        // Find the lowest free page slot
        let Some(&first_free) = free_pages.first() else {
            break;
        };

        if first_free >= highest_page_id {
            break;
        }

        // We build parent map for this iteration.
        let parent_map = {
            let pager = storage.read().map_err(|_| {
                HematiteError::InternalError("Pager lock is poisoned".to_string())
            })?;
            build_parent_map(&pager, roots)?
        };

        let mut parent_map_mut = parent_map;
        relocate_page(
            storage,
            highest_page_id,
            first_free,
            &mut parent_map_mut,
            |old_id, new_id| {
                update_root(old_id, new_id)?;
                for r in roots.iter_mut() {
                    if *r == old_id {
                        *r = new_id;
                    }
                }
                Ok(())
            },
        )?;
    }

    Ok(())
}
