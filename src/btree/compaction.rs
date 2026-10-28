//! Auto-vacuum: moving pages down into free slots so free space collects at the end of
//! the file.
//!
//! `auto_vacuum` repeatedly relocates the highest allocated page into the lowest free
//! page id, until no free page sits below it. Pages carry no back pointers, so moving
//! one needs to know the single place that refers to it — a tree root, a parent's
//! child slot, a leaf cell's overflow head, or an overflow predecessor's next pointer.
//! `build_parent_map` records that for every page of every tree in one walk, and
//! `relocate_page` keeps the map current by patching only the moved page's own
//! outgoing edges, so the map is built once per call rather than once per move.

use crate::btree::node::BTreeNode;
use crate::btree::value_store::StoredValueLayout;
use crate::btree::BTreeValue;
use crate::btree::NodeType;
use crate::error::{HematiteError, Result};
use crate::storage::format::PageKind;
use crate::storage::{Page, PageId, Pager, INVALID_PAGE_ID};
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

/// Counts calls to `build_parent_map`, so tests can assert that a single
/// `auto_vacuum()` call rebuilds the parent map at most once regardless of
/// how many pages it relocates -- a regression guard for the O(1)-per-call
/// invariant this module relies on.
#[cfg(test)]
pub(crate) static BUILD_PARENT_MAP_CALLS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

pub(crate) fn build_parent_map(
    pager: &Pager,
    roots: &[PageId],
) -> Result<HashMap<PageId, PageParent>> {
    #[cfg(test)]
    BUILD_PARENT_MAP_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

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

    // Zero-copy lazy view: parent-map construction only needs child page ids
    // (internal nodes) or overflow pointers (leaf values), never full key/value
    // bytes, so avoid the full decode-into-owned-buffers path here.
    let page = pager.read_page_shared(page_id)?;
    let node = BTreeNode::from_shared_page(page)?;

    match node.node_type {
        NodeType::Internal => {
            for i in 0..=node.key_count {
                let child_page_id = node.get_child_procedural(i)?;
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
            for i in 0..node.key_count {
                let layout = StoredValueLayout::decode(node.get_value_view(i)?)?;
                if layout.overflow_first_page != INVALID_PAGE_ID && layout.overflow_first_page >= 2
                {
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
            PageParent::OverflowPredecessor {
                pred_page_id: page_id,
            },
            parent_map,
            visited,
        )?;
    }

    Ok(())
}

/// What a page being relocated points to -- the exact set of parent-map
/// entries that need patching after its id changes, bounded by `BTREE_ORDER`
/// (a page's own fan-out), not by database size.
enum RelocationChildren {
    Internal(Vec<PageId>),
    Leaf(Vec<(usize, PageId)>),
    Overflow(Option<PageId>),
}

/// Determines what `page` points to (its children, its leaf cells' overflow
/// pointers, or its overflow-chain successor) using the same zero-copy lazy
/// accessors as `trace_tree`, so this never decodes key/value bytes into
/// owned buffers just to discover topology.
fn collect_relocation_children(page: Arc<Page>) -> Result<RelocationChildren> {
    let kind = PageKind::from_byte(page.data[0])?;
    match kind {
        PageKind::LeafTable | PageKind::LeafIndex => {
            let node = BTreeNode::from_shared_page(page)?;
            let mut overflow_pointers = Vec::new();
            for i in 0..node.key_count {
                let layout = StoredValueLayout::decode(node.get_value_view(i)?)?;
                if layout.overflow_first_page != INVALID_PAGE_ID && layout.overflow_first_page >= 2
                {
                    overflow_pointers.push((i, layout.overflow_first_page));
                }
            }
            Ok(RelocationChildren::Leaf(overflow_pointers))
        }
        PageKind::InteriorTable | PageKind::InteriorIndex => {
            let node = BTreeNode::from_shared_page(page)?;
            let mut children = Vec::with_capacity(node.key_count + 1);
            for i in 0..=node.key_count {
                children.push(node.get_child_procedural(i)?);
            }
            Ok(RelocationChildren::Internal(children))
        }
        PageKind::Overflow => {
            let next_page_id =
                u32::from_be_bytes([page.data[4], page.data[5], page.data[6], page.data[7]]);
            Ok(RelocationChildren::Overflow(
                (next_page_id != 0 && next_page_id >= 2).then_some(next_page_id),
            ))
        }
        other => Err(HematiteError::CorruptedData(format!(
            "Unexpected page kind {:?} encountered during vacuum relocation",
            other
        ))),
    }
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
    // 1. Read the old page data and determine what it points to (children,
    //    overflow pointers, or overflow-chain successor) before consuming it
    //    into the id-flip below. Wrapping the freshly-read, uniquely-owned
    //    page in an `Arc` here costs only a refcount bump, not a copy: the
    //    lazy `BTreeNode` built inside `collect_relocation_children` only
    //    clones the small set of pointers a page can hold (bounded by
    //    `BTREE_ORDER`), so `Arc::try_unwrap` below reclaims the original
    //    page buffer with no 4KB copy.
    let old_page_arc = {
        let pager = storage
            .read()
            .map_err(|_| HematiteError::InternalError("Pager lock is poisoned".to_string()))?;
        Arc::new(pager.read_page(old_page_id)?)
    };
    let outgoing = collect_relocation_children(old_page_arc.clone())?;
    let mut page = Arc::try_unwrap(old_page_arc).map_err(|_| {
        HematiteError::InternalError(
            "Relocated page unexpectedly retained by another reference".to_string(),
        )
    })?;

    // 2. Change the page ID to new_page_id and write to pager.
    page.id = new_page_id;
    {
        let mut pager = storage
            .write()
            .map_err(|_| HematiteError::InternalError("Pager lock is poisoned".to_string()))?;
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

    // 5. Update the parent map: re-key the moved page's own entry (who
    //    points at it), then patch exactly the entries describing what the
    //    moved page points to (its children / overflow pointers / overflow
    //    successor, collected in step 1) -- bounded by the page's own fan-out,
    //    not by the size of the whole map.
    if let Some(parent_info) = parent_map.remove(&old_page_id) {
        parent_map.insert(new_page_id, parent_info);
    }
    match outgoing {
        RelocationChildren::Internal(children) => {
            for (child_index, child_id) in children.into_iter().enumerate() {
                if child_id >= 2 {
                    parent_map.insert(
                        child_id,
                        PageParent::BTreeInternal {
                            parent_page_id: new_page_id,
                            child_index,
                        },
                    );
                }
            }
        }
        RelocationChildren::Leaf(overflow_pointers) => {
            for (cell_index, overflow_page_id) in overflow_pointers {
                parent_map.insert(
                    overflow_page_id,
                    PageParent::BTreeLeafCell {
                        leaf_page_id: new_page_id,
                        cell_index,
                    },
                );
            }
        }
        RelocationChildren::Overflow(Some(next_page_id)) => {
            parent_map.insert(
                next_page_id,
                PageParent::OverflowPredecessor {
                    pred_page_id: new_page_id,
                },
            );
        }
        RelocationChildren::Overflow(None) => {}
    }

    // 6. Deallocate the old page ID.
    {
        let mut pager = storage
            .write()
            .map_err(|_| HematiteError::InternalError("Pager lock is poisoned".to_string()))?;
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
    // Built lazily on the first relocation actually needed, then reused for
    // every remaining iteration of this call: relocation never changes tree
    // topology (only a page's id and exactly one incoming pointer), so the
    // parent/child graph stays valid across the whole loop and only needs
    // patching at the moved page's own outgoing edges (done inside
    // `relocate_page`), not a full rebuild per relocation.
    let mut parent_map: Option<HashMap<PageId, PageParent>> = None;

    loop {
        // Read lock storage to check state
        let (next_page_id, mut free_pages) = {
            let pager = storage
                .read()
                .map_err(|_| HematiteError::InternalError("Pager lock is poisoned".to_string()))?;
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
            let mut pager = storage
                .write()
                .map_err(|_| HematiteError::InternalError("Pager lock is poisoned".to_string()))?;
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

        if parent_map.is_none() {
            let pager = storage
                .read()
                .map_err(|_| HematiteError::InternalError("Pager lock is poisoned".to_string()))?;
            parent_map = Some(build_parent_map(&pager, roots)?);
        }
        let map = parent_map
            .as_mut()
            .expect("parent_map was just initialized above");

        relocate_page(
            storage,
            highest_page_id,
            first_free,
            map,
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
