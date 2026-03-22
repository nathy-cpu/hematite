use crate::btree::node::SearchResult;
use crate::btree::tree::{
    collect_tree_page_ids, collect_tree_space_stats, reset_tree_pages, TreeSpaceStats,
};
use crate::btree::{BTreeKey, BTreeNode, BTreeValue, NodeType};
use crate::error::Result;
use crate::storage::{Page, PageId, Pager};
use std::collections::HashSet;

use super::engine::StoredRow;
use super::serialization::RowSerializer;

pub fn validate_pages(
    pager: &mut Pager,
    table_name: &str,
    root_page_id: PageId,
) -> Result<(Vec<PageId>, u64, u64)> {
    let mut visited = HashSet::new();
    let mut row_count = 0u64;
    let mut max_row_id = 0u64;
    let mut previous_row_id = None;
    walk_tree(
        pager,
        root_page_id,
        table_name,
        &mut visited,
        &mut row_count,
        &mut max_row_id,
        &mut previous_row_id,
    )?;
    Ok((visited.into_iter().collect(), row_count, max_row_id))
}

fn walk_tree(
    pager: &mut Pager,
    page_id: PageId,
    table_name: &str,
    visited: &mut HashSet<PageId>,
    row_count: &mut u64,
    max_row_id: &mut u64,
    previous_row_id: &mut Option<u64>,
) -> Result<()> {
    if !visited.insert(page_id) {
        return Err(crate::error::HematiteError::CorruptedData(format!(
            "Cycle detected in B-tree for table '{}'",
            table_name
        )));
    }

    let page = pager.read_page(page_id)?;
    let node = BTreeNode::from_page(page)?;

    match node.node_type {
        NodeType::Leaf => {
            for value in node.values {
                let row = RowSerializer::deserialize_stored_row(&value.data)?;
                if let Some(last_row_id) = previous_row_id {
                    if row.row_id <= *last_row_id {
                        return Err(crate::error::HematiteError::CorruptedData(format!(
                            "Cursor-visible rowid order violation in table '{}': {} followed by {}",
                            table_name, last_row_id, row.row_id
                        )));
                    }
                }
                *previous_row_id = Some(row.row_id);
                *row_count += 1;
                *max_row_id = (*max_row_id).max(row.row_id);
            }
        }
        NodeType::Internal => {
            for child in node.children {
                walk_tree(
                    pager,
                    child,
                    table_name,
                    visited,
                    row_count,
                    max_row_id,
                    previous_row_id,
                )?;
            }
        }
    }

    Ok(())
}

pub fn insert_row(
    pager: &mut Pager,
    root_page_id: PageId,
    row_id: u64,
    row: Vec<crate::catalog::Value>,
) -> Result<Option<PageId>> {
    let key = BTreeKey::new(row_id.to_be_bytes().to_vec());
    let encoded = RowSerializer::serialize_stored_row(&StoredRow {
        row_id,
        values: row,
    })?;
    let value = BTreeValue::new(encoded);

    let split_result = insert_recursive(pager, root_page_id, key, value)?;
    if let Some((split_key, split_page_id)) = split_result {
        let new_root_page_id = pager.allocate_page()?;
        let mut new_root = BTreeNode::new_internal(new_root_page_id);
        new_root.keys.push(split_key);
        new_root.children.push(root_page_id);
        new_root.children.push(split_page_id);

        let mut new_root_page = Page::new(new_root_page_id);
        new_root.to_page(&mut new_root_page)?;
        pager.write_page(new_root_page)?;
        Ok(Some(new_root_page_id))
    } else {
        Ok(None)
    }
}

fn insert_recursive(
    pager: &mut Pager,
    page_id: PageId,
    key: BTreeKey,
    value: BTreeValue,
) -> Result<Option<(BTreeKey, PageId)>> {
    let mut page = pager.read_page(page_id)?;
    let mut node = BTreeNode::from_page(page.clone())?;

    match node.node_type {
        NodeType::Leaf => {
            if let Some(existing_index) = node.keys.iter().position(|k| k == &key) {
                node.values[existing_index] = value;
                node.to_page(&mut page)?;
                pager.write_page(page)?;
                return Ok(None);
            }

            if node.keys.len() < crate::btree::node::MAX_KEYS
                && node.can_insert_key_value(&key, &value)
            {
                node.insert_leaf(key, value)?;
                node.to_page(&mut page)?;
                pager.write_page(page)?;
                Ok(None)
            } else {
                let (new_key, new_page_id) = node.split_leaf(pager, key, value)?;
                Ok(Some((new_key, new_page_id)))
            }
        }
        NodeType::Internal => {
            let child_page_id = node.find_child(&key);
            let split_result = insert_recursive(pager, child_page_id, key, value)?;

            if let Some((split_key, split_page_id)) = split_result {
                if node.keys.len() < crate::btree::node::MAX_KEYS
                    && node.can_insert_key_child(&split_key)
                {
                    node.insert_internal(split_key, split_page_id)?;
                    node.to_page(&mut page)?;
                    pager.write_page(page)?;
                    Ok(None)
                } else {
                    let (new_key, new_page_id) =
                        node.split_internal(pager, split_key, split_page_id)?;
                    Ok(Some((new_key, new_page_id)))
                }
            } else {
                Ok(None)
            }
        }
    }
}

pub fn delete_row(pager: &mut Pager, root_page_id: PageId, rowid: u64) -> Result<bool> {
    let key = BTreeKey::new(rowid.to_be_bytes().to_vec());
    delete_recursive(pager, root_page_id, &key)
}

fn delete_recursive(pager: &mut Pager, page_id: PageId, key: &BTreeKey) -> Result<bool> {
    let mut page = pager.read_page(page_id)?;
    let mut node = BTreeNode::from_page(page.clone())?;

    match node.node_type {
        NodeType::Leaf => {
            let deleted = node.delete_from_leaf(key)?.is_some();
            if deleted {
                node.to_page(&mut page)?;
                pager.write_page(page)?;
            }
            Ok(deleted)
        }
        NodeType::Internal => {
            let child_page_id = node.find_child(key);
            let deleted = delete_recursive(pager, child_page_id, key)?;
            if deleted {
                node.to_page(&mut page)?;
                pager.write_page(page)?;
            }
            Ok(deleted)
        }
    }
}

pub fn reset_tree(pager: &mut Pager, root_page_id: PageId) -> Result<()> {
    reset_tree_pages(pager, root_page_id)
}

pub fn collect_page_ids(pager: &mut Pager, page_id: PageId, out: &mut Vec<PageId>) -> Result<()> {
    collect_tree_page_ids(pager, page_id, out)
}

pub fn collect_space_stats(pager: &mut Pager, root_page_id: PageId) -> Result<TreeSpaceStats> {
    collect_tree_space_stats(pager, root_page_id)
}

pub fn read_rows(pager: &mut Pager, root_page_id: PageId) -> Result<Vec<StoredRow>> {
    let mut rows = Vec::new();
    collect_rows(pager, root_page_id, &mut rows)?;
    rows.sort_unstable_by_key(|row| row.row_id);
    Ok(rows)
}

fn collect_rows(pager: &mut Pager, page_id: PageId, out: &mut Vec<StoredRow>) -> Result<()> {
    let page = pager.read_page(page_id)?;
    let node = BTreeNode::from_page(page)?;

    match node.node_type {
        NodeType::Leaf => {
            for value in node.values {
                let row = RowSerializer::deserialize_stored_row(&value.data)?;
                out.push(row);
            }
        }
        NodeType::Internal => {
            for child_page_id in node.children {
                collect_rows(pager, child_page_id, out)?;
            }
        }
    }

    Ok(())
}

pub fn lookup_row(
    pager: &mut Pager,
    root_page_id: PageId,
    rowid: u64,
) -> Result<Option<StoredRow>> {
    let key = BTreeKey::new(rowid.to_be_bytes().to_vec());
    let mut current_page_id = root_page_id;
    loop {
        let page = pager.read_page(current_page_id)?;
        let node = BTreeNode::from_page(page)?;
        match node.search(&key) {
            SearchResult::Found(value) => {
                let row = RowSerializer::deserialize_stored_row(&value.data)?;
                return Ok(Some(row));
            }
            SearchResult::NotFound(next_child) => {
                if node.node_type == NodeType::Leaf {
                    return Ok(None);
                }
                current_page_id = next_child;
            }
        }
    }
}
