//! Durable index storage operations for catalog-managed tables.

use std::collections::HashSet;

use crate::btree::ByteTreeStore;
use crate::catalog::{Table, Value};
use crate::error::{HematiteError, Result};
use crate::storage::{PageId, INVALID_PAGE_ID};

use super::cursor::IndexCursor;
use super::engine::{CatalogEngine, StoredRow};
use super::table_store;

pub(crate) fn drop_table_with_indexes(engine: &mut CatalogEngine, table: &Table) -> Result<()> {
    table_store::drop_table(engine, &table.name)?;
    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());

    if table.primary_key_index_root_page_id != 0 {
        trees.delete_tree(table.primary_key_index_root_page_id)?;
    }

    for index in &table.secondary_indexes {
        if index.root_page_id == 0 {
            continue;
        }
        trees.delete_tree(index.root_page_id)?;
    }

    Ok(())
}

pub(crate) fn lookup_row_by_primary_key(
    engine: &mut CatalogEngine,
    table: &Table,
    key_values: &[Value],
) -> Result<Option<StoredRow>> {
    let rowid = lookup_primary_key_rowid(engine, table, key_values)?;
    match rowid {
        Some(rowid) => table_store::lookup_row_by_rowid(engine, &table.name, rowid),
        None => Ok(None),
    }
}

pub(crate) fn lookup_primary_key_rowid(
    engine: &mut CatalogEngine,
    table: &Table,
    key_values: &[Value],
) -> Result<Option<u64>> {
    let root_page_id = require_index_root_page(
        table.primary_key_index_root_page_id,
        &format!("primary-key index for table '{}'", table.name),
    )?;
    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());
    let mut tree = trees.open_tree(root_page_id)?;
    match tree.get(&encode_index_key(key_values)?)? {
        Some(value) => decode_rowid_value(&value).map(Some),
        None => Ok(None),
    }
}

pub(crate) fn register_primary_key_row(
    engine: &mut CatalogEngine,
    table: &Table,
    row: StoredRow,
) -> Result<()> {
    let root_page_id = require_index_root_page(
        table.primary_key_index_root_page_id,
        &format!("primary-key index for table '{}'", table.name),
    )?;
    let key_values = table.get_primary_key_values(&row.values)?;
    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());
    let mut tree = trees.open_tree(root_page_id)?;
    let encoded_key = encode_index_key(&key_values)?;
    if tree.get(&encoded_key)?.is_some() {
        return Err(HematiteError::StorageError(format!(
            "Duplicate primary key for table '{}'",
            table.name
        )));
    }
    tree.insert_with_mutation(&encoded_key, &row.row_id.to_be_bytes())?;
    Ok(())
}

pub(crate) fn lookup_rows_by_secondary_index(
    engine: &mut CatalogEngine,
    table: &Table,
    index_name: &str,
    key_values: &[Value],
) -> Result<Vec<StoredRow>> {
    let rowids = lookup_secondary_index_rowids(engine, table, index_name, key_values)?;
    let mut rows = Vec::with_capacity(rowids.len());
    for rowid in rowids {
        if let Some(row) = table_store::lookup_row_by_rowid(engine, &table.name, rowid)? {
            rows.push(row);
        }
    }
    Ok(rows)
}

pub(crate) fn lookup_secondary_index_rowids(
    engine: &mut CatalogEngine,
    table: &Table,
    index_name: &str,
    key_values: &[Value],
) -> Result<Vec<u64>> {
    let index = table.get_secondary_index(index_name).ok_or_else(|| {
        HematiteError::StorageError(format!(
            "Secondary index '{}' does not exist on table '{}'",
            index_name, table.name
        ))
    })?;
    let root_page_id = require_index_root_page(
        index.root_page_id,
        &format!("secondary index '{}' on table '{}'", index.name, table.name),
    )?;
    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());
    let tree = trees.open_tree(root_page_id)?;
    tree.entries_with_prefix(&encode_index_key(key_values)?)?
        .into_iter()
        .map(|(_key, value)| decode_rowid_value(&value))
        .collect()
}

pub(crate) fn register_secondary_index_row(
    engine: &mut CatalogEngine,
    table: &Table,
    row: StoredRow,
) -> Result<()> {
    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());
    for index in &table.secondary_indexes {
        let root_page_id = require_index_root_page(
            index.root_page_id,
            &format!("secondary index '{}' on table '{}'", index.name, table.name),
        )?;
        let key_values = index
            .column_indices
            .iter()
            .map(|&column_index| row.values[column_index].clone())
            .collect::<Vec<_>>();
        let mut tree = trees.open_tree(root_page_id)?;
        tree.insert_with_mutation(
            &encode_secondary_key(&key_values, row.row_id)?,
            &row.row_id.to_be_bytes(),
        )?;
    }
    Ok(())
}

pub(crate) fn rebuild_primary_key_index(
    engine: &mut CatalogEngine,
    table: &Table,
    rows: &[StoredRow],
) -> Result<()> {
    let root_page_id = require_index_root_page(
        table.primary_key_index_root_page_id,
        &format!("primary-key index for table '{}'", table.name),
    )?;
    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());
    trees.reset_tree(root_page_id)?;
    let mut seen = HashSet::new();
    let mut tree = trees.open_tree(root_page_id)?;
    for row in rows {
        let key_values = table.get_primary_key_values(&row.values)?;
        let encoded = encode_index_key(&key_values)?;
        if !seen.insert(encoded) {
            return Err(HematiteError::StorageError(format!(
                "Duplicate primary key encountered while rebuilding table '{}'",
                table.name
            )));
        }
        tree.insert_with_mutation(&encode_index_key(&key_values)?, &row.row_id.to_be_bytes())?;
    }
    Ok(())
}

pub(crate) fn rebuild_secondary_indexes(
    engine: &mut CatalogEngine,
    table: &Table,
    rows: &[StoredRow],
) -> Result<()> {
    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());
    for index in &table.secondary_indexes {
        let root_page_id = require_index_root_page(
            index.root_page_id,
            &format!("secondary index '{}' on table '{}'", index.name, table.name),
        )?;
        trees.reset_tree(root_page_id)?;
        let mut tree = trees.open_tree(root_page_id)?;
        for row in rows {
            let key_values = index
                .column_indices
                .iter()
                .map(|&column_index| row.values[column_index].clone())
                .collect::<Vec<_>>();
            tree.insert_with_mutation(
                &encode_secondary_key(&key_values, row.row_id)?,
                &row.row_id.to_be_bytes(),
            )?;
        }
    }
    Ok(())
}

pub(crate) fn delete_primary_key_row(
    engine: &mut CatalogEngine,
    table: &Table,
    row: &StoredRow,
) -> Result<bool> {
    let root_page_id = require_index_root_page(
        table.primary_key_index_root_page_id,
        &format!("primary-key index for table '{}'", table.name),
    )?;
    let key_values = table.get_primary_key_values(&row.values)?;
    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());
    let mut tree = trees.open_tree(root_page_id)?;
    Ok(tree.delete(&encode_index_key(&key_values)?)?.is_some())
}

pub(crate) fn delete_secondary_index_row(
    engine: &mut CatalogEngine,
    table: &Table,
    row: &StoredRow,
) -> Result<()> {
    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());
    for index in &table.secondary_indexes {
        let key_values = index
            .column_indices
            .iter()
            .map(|&column_index| row.values[column_index].clone())
            .collect::<Vec<_>>();
        let mut tree = trees.open_tree(index.root_page_id)?;
        let _ = tree.delete(&encode_secondary_key(&key_values, row.row_id)?)?;
    }
    Ok(())
}

pub(crate) fn encode_primary_key(key_values: &[Value]) -> Result<Vec<u8>> {
    encode_index_key(key_values)
}

pub(crate) fn encode_secondary_index_key(key_values: &[Value]) -> Result<Vec<u8>> {
    encode_index_key(key_values)
}

pub(crate) fn open_primary_key_cursor(
    engine: &mut CatalogEngine,
    table: &Table,
) -> Result<IndexCursor> {
    let root_page_id = require_index_root_page(
        table.primary_key_index_root_page_id,
        &format!("primary-key index for table '{}'", table.name),
    )?;
    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());
    let tree = trees.open_tree(root_page_id)?;
    let entries = tree
        .entries()?
        .into_iter()
        .map(|(key, value)| {
            Ok(super::cursor::IndexEntry {
                row_id: decode_rowid_value(&value)?,
                key,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(IndexCursor::new(entries))
}

pub(crate) fn open_secondary_index_cursor(
    engine: &mut CatalogEngine,
    table: &Table,
    index_name: &str,
) -> Result<IndexCursor> {
    let index = table.get_secondary_index(index_name).ok_or_else(|| {
        HematiteError::StorageError(format!(
            "Secondary index '{}' does not exist on table '{}'",
            index_name, table.name
        ))
    })?;
    let root_page_id = require_index_root_page(
        index.root_page_id,
        &format!("secondary index '{}' on table '{}'", index.name, table.name),
    )?;
    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());
    let tree = trees.open_tree(root_page_id)?;
    let entries = tree
        .entries()?
        .into_iter()
        .map(|(key, value)| decode_secondary_entry(&key, &value))
        .collect::<Result<Vec<_>>>()?;
    Ok(IndexCursor::new(entries))
}

pub(crate) fn validate_table_indexes(engine: &mut CatalogEngine, table: &Table) -> Result<()> {
    let rows = table_store::read_rows_with_ids(engine, &table.name)?;
    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());
    for row in &rows {
        let key_values = table.get_primary_key_values(&row.values)?;
        let mut tree = trees.open_tree(table.primary_key_index_root_page_id)?;
        let stored_rowid = tree
            .get(&encode_index_key(&key_values)?)?
            .map(|value| decode_rowid_value(&value))
            .transpose()?
            .ok_or_else(|| {
                HematiteError::CorruptedData(format!(
                    "Primary-key index is missing a row for table '{}'",
                    table.name
                ))
            })?;

        if stored_rowid != row.row_id {
            return Err(HematiteError::CorruptedData(format!(
                "Primary-key index rowid mismatch for table '{}': expected {}, got {}",
                table.name, row.row_id, stored_rowid
            )));
        }
    }

    for index in &table.secondary_indexes {
        for row in &rows {
            let key_values = index
                .column_indices
                .iter()
                .map(|&column_index| row.values[column_index].clone())
                .collect::<Vec<_>>();
            let tree = trees.open_tree(index.root_page_id)?;
            let rowids = tree
                .entries_with_prefix(&encode_index_key(&key_values)?)?
                .into_iter()
                .map(|(_key, value)| decode_rowid_value(&value))
                .collect::<Result<Vec<_>>>()?;
            if !rowids.contains(&row.row_id) {
                return Err(HematiteError::CorruptedData(format!(
                    "Secondary index '{}' is missing rowid {} for table '{}'",
                    index.name, row.row_id, table.name
                )));
            }
        }
    }

    Ok(())
}

pub(crate) fn require_index_root_page(root_page_id: PageId, label: &str) -> Result<PageId> {
    if root_page_id == 0 || root_page_id == INVALID_PAGE_ID {
        return Err(HematiteError::StorageError(format!(
            "Missing durable {} root page",
            label
        )));
    }
    Ok(root_page_id)
}

fn encode_index_key(values: &[Value]) -> Result<Vec<u8>> {
    super::serialization::RowSerializer::serialize(values)
}

fn encode_secondary_key(values: &[Value], rowid: u64) -> Result<Vec<u8>> {
    let mut key = encode_index_key(values)?;
    key.extend_from_slice(&rowid.to_be_bytes());
    Ok(key)
}

fn decode_rowid_value(value: &[u8]) -> Result<u64> {
    if value.len() != 8 {
        return Err(HematiteError::CorruptedData(
            "Index rowid payload must be exactly 8 bytes".to_string(),
        ));
    }
    Ok(u64::from_be_bytes(value.try_into().unwrap()))
}

fn decode_secondary_entry(key: &[u8], value: &[u8]) -> Result<super::cursor::IndexEntry> {
    let row_id = if value.len() == 8 {
        decode_rowid_value(value)?
    } else if key.len() >= 8 {
        u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap())
    } else {
        return Err(HematiteError::CorruptedData(
            "Index entry is missing rowid bytes".to_string(),
        ));
    };

    let logical_key = if key.len() >= 8 {
        key[..key.len() - 8].to_vec()
    } else {
        key.to_vec()
    };

    Ok(super::cursor::IndexEntry {
        row_id,
        key: logical_key,
    })
}
