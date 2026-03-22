//! Durable index B-tree helpers.
//!
//! Primary-key indexes store `encoded_pk -> rowid`.
//! Secondary indexes store `encoded_index_key || rowid -> rowid`.

use crate::btree::node::SearchResult;
use crate::btree::{BTreeKey, BTreeNode, BTreeValue, NodeType};
use crate::catalog::Value;
use crate::error::{HematiteError, Result};
use crate::storage::cursor::IndexEntry;
use crate::storage::{Page, PageId, StorageEngine};

pub fn encode_index_key(values: &[Value]) -> Result<Vec<u8>> {
    crate::storage::serialization::RowSerializer::serialize(values)
}

pub fn insert_primary_key(
    storage: &mut StorageEngine,
    root_page_id: PageId,
    key_values: &[Value],
    rowid: u64,
) -> Result<Option<PageId>> {
    insert_entry(
        storage,
        root_page_id,
        BTreeKey::new(encode_index_key(key_values)?),
        BTreeValue::new(rowid.to_be_bytes().to_vec()),
    )
}

pub fn lookup_primary_key(
    storage: &mut StorageEngine,
    root_page_id: PageId,
    key_values: &[Value],
) -> Result<Option<u64>> {
    lookup_entry(storage, root_page_id, &encode_index_key(key_values)?)
        .and_then(|value| value.map(decode_rowid_value).transpose())
}

pub fn delete_primary_key(
    storage: &mut StorageEngine,
    root_page_id: PageId,
    key_values: &[Value],
) -> Result<bool> {
    delete_entry(storage, root_page_id, &encode_index_key(key_values)?)
}

pub fn insert_secondary_key(
    storage: &mut StorageEngine,
    root_page_id: PageId,
    key_values: &[Value],
    rowid: u64,
) -> Result<Option<PageId>> {
    let key = BTreeKey::new(encode_secondary_key(key_values, rowid)?);
    insert_entry(
        storage,
        root_page_id,
        key,
        BTreeValue::new(rowid.to_be_bytes().to_vec()),
    )
}

pub fn lookup_secondary_rowids(
    storage: &mut StorageEngine,
    root_page_id: PageId,
    key_values: &[Value],
) -> Result<Vec<u64>> {
    let prefix = encode_index_key(key_values)?;
    let mut entries = Vec::new();
    collect_entries(storage, root_page_id, &mut entries)?;
    let mut rowids = entries
        .into_iter()
        .filter(|entry| entry.key.starts_with(&prefix))
        .map(|entry| entry.row_id)
        .collect::<Vec<_>>();
    rowids.sort_unstable();
    Ok(rowids)
}

pub fn delete_secondary_key(
    storage: &mut StorageEngine,
    root_page_id: PageId,
    key_values: &[Value],
    rowid: u64,
) -> Result<bool> {
    delete_entry(
        storage,
        root_page_id,
        &encode_secondary_key(key_values, rowid)?,
    )
}

pub fn read_entries(storage: &mut StorageEngine, root_page_id: PageId) -> Result<Vec<IndexEntry>> {
    let mut entries = Vec::new();
    collect_entries(storage, root_page_id, &mut entries)?;
    entries.sort_by(|left, right| {
        left.key
            .cmp(&right.key)
            .then(left.row_id.cmp(&right.row_id))
    });
    Ok(entries)
}

pub fn reset_tree(storage: &mut StorageEngine, root_page_id: PageId) -> Result<()> {
    crate::storage::table_btree::reset_tree(storage, root_page_id)
}

pub fn collect_page_ids(
    storage: &mut StorageEngine,
    root_page_id: PageId,
    out: &mut Vec<PageId>,
) -> Result<()> {
    crate::storage::table_btree::collect_page_ids(storage, root_page_id, out)
}

fn insert_entry(
    storage: &mut StorageEngine,
    root_page_id: PageId,
    key: BTreeKey,
    value: BTreeValue,
) -> Result<Option<PageId>> {
    let split_result = insert_recursive(storage, root_page_id, key, value)?;
    if let Some((split_key, split_page_id)) = split_result {
        let left_child_page_id = storage.allocate_page()?;
        let root_snapshot = storage.read_page(root_page_id)?;
        let mut left_child_page = Page::new(left_child_page_id);
        left_child_page.data.copy_from_slice(&root_snapshot.data);
        storage.write_page(left_child_page)?;

        let mut new_root = BTreeNode::new_internal(root_page_id);
        new_root.keys.push(split_key);
        new_root.children.push(left_child_page_id);
        new_root.children.push(split_page_id);

        let mut new_root_page = Page::new(root_page_id);
        new_root.to_page(&mut new_root_page)?;
        storage.write_page(new_root_page)?;
        Ok(None)
    } else {
        Ok(None)
    }
}

fn insert_recursive(
    storage: &mut StorageEngine,
    page_id: PageId,
    key: BTreeKey,
    value: BTreeValue,
) -> Result<Option<(BTreeKey, PageId)>> {
    let mut page = storage.read_page(page_id)?;
    let mut node = BTreeNode::from_page(page.clone())?;

    match node.node_type {
        NodeType::Leaf => {
            if let Some(existing_index) = node.keys.iter().position(|candidate| candidate == &key) {
                node.values[existing_index] = value;
                node.to_page(&mut page)?;
                storage.write_page(page)?;
                return Ok(None);
            }

            if node.keys.len() < crate::btree::node::MAX_KEYS
                && node.can_insert_key_value(&key, &value)
            {
                node.insert_leaf(key, value)?;
                node.to_page(&mut page)?;
                storage.write_page(page)?;
                Ok(None)
            } else {
                let (new_key, new_page_id) = node.split_leaf(storage, key, value)?;
                Ok(Some((new_key, new_page_id)))
            }
        }
        NodeType::Internal => {
            let child_page_id = node.find_child(&key);
            let split_result = insert_recursive(storage, child_page_id, key, value)?;

            if let Some((split_key, split_page_id)) = split_result {
                if node.keys.len() < crate::btree::node::MAX_KEYS
                    && node.can_insert_key_child(&split_key)
                {
                    node.insert_internal(split_key, split_page_id)?;
                    node.to_page(&mut page)?;
                    storage.write_page(page)?;
                    Ok(None)
                } else {
                    let (new_key, new_page_id) =
                        node.split_internal(storage, split_key, split_page_id)?;
                    Ok(Some((new_key, new_page_id)))
                }
            } else {
                Ok(None)
            }
        }
    }
}

fn lookup_entry(
    storage: &mut StorageEngine,
    root_page_id: PageId,
    key: &[u8],
) -> Result<Option<Vec<u8>>> {
    let key = BTreeKey::new(key.to_vec());
    let mut current_page_id = root_page_id;
    loop {
        let page = storage.read_page(current_page_id)?;
        let node = BTreeNode::from_page(page)?;
        match node.search(&key) {
            SearchResult::Found(value) => return Ok(Some(value.data)),
            SearchResult::NotFound(next_child) => {
                if node.node_type == NodeType::Leaf {
                    return Ok(None);
                }
                current_page_id = next_child;
            }
        }
    }
}

fn delete_entry(storage: &mut StorageEngine, root_page_id: PageId, key: &[u8]) -> Result<bool> {
    let key = BTreeKey::new(key.to_vec());
    delete_recursive(storage, root_page_id, &key)
}

fn delete_recursive(storage: &mut StorageEngine, page_id: PageId, key: &BTreeKey) -> Result<bool> {
    let mut page = storage.read_page(page_id)?;
    let mut node = BTreeNode::from_page(page.clone())?;

    match node.node_type {
        NodeType::Leaf => {
            let deleted = node.delete_from_leaf(key)?.is_some();
            if deleted {
                node.to_page(&mut page)?;
                storage.write_page(page)?;
            }
            Ok(deleted)
        }
        NodeType::Internal => {
            let child_page_id = node.find_child(key);
            let deleted = delete_recursive(storage, child_page_id, key)?;
            if deleted {
                node.to_page(&mut page)?;
                storage.write_page(page)?;
            }
            Ok(deleted)
        }
    }
}

fn collect_entries(
    storage: &mut StorageEngine,
    page_id: PageId,
    out: &mut Vec<IndexEntry>,
) -> Result<()> {
    let page = storage.read_page(page_id)?;
    let node = BTreeNode::from_page(page)?;
    match node.node_type {
        NodeType::Leaf => {
            for (key, value) in node.keys.into_iter().zip(node.values.into_iter()) {
                out.push(IndexEntry {
                    row_id: decode_rowid_from_index_entry(&key.data, &value.data)?,
                    key: decode_logical_key(&key.data)?,
                });
            }
        }
        NodeType::Internal => {
            for child_page_id in node.children {
                collect_entries(storage, child_page_id, out)?;
            }
        }
    }
    Ok(())
}

fn decode_rowid_value(value: Vec<u8>) -> Result<u64> {
    if value.len() != 8 {
        return Err(HematiteError::CorruptedData(
            "Index rowid payload must be exactly 8 bytes".to_string(),
        ));
    }
    Ok(u64::from_be_bytes(value.try_into().unwrap()))
}

fn encode_secondary_key(values: &[Value], rowid: u64) -> Result<Vec<u8>> {
    let mut key = encode_index_key(values)?;
    key.extend_from_slice(&rowid.to_be_bytes());
    Ok(key)
}

fn decode_rowid_from_index_entry(key: &[u8], value: &[u8]) -> Result<u64> {
    if value.len() == 8 {
        return Ok(u64::from_be_bytes(value.try_into().unwrap()));
    }
    if key.len() >= 8 {
        return Ok(u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap()));
    }
    Err(HematiteError::CorruptedData(
        "Index entry is missing rowid bytes".to_string(),
    ))
}

fn decode_logical_key(key: &[u8]) -> Result<Vec<u8>> {
    if key.len() >= 8 {
        Ok(key[..key.len() - 8].to_vec())
    } else {
        Ok(key.to_vec())
    }
}
