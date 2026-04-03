//! Durable index storage operations for catalog-managed tables.

use std::collections::HashSet;

use crate::catalog::{Column, Table, Value};
use crate::error::{HematiteError, Result};

use super::cursor::IndexCursor;
use super::engine::CatalogEngine;
use super::record::StoredRow;
use super::serialization::IndexKeyCodec;
use super::table_store;
use crate::catalog::column::{normalize_text_for_collation, pad_text_to_char_length};

const INVALID_ROOT_PAGE_ID: u32 = u32::MAX;

pub(crate) fn drop_table_with_indexes(engine: &mut CatalogEngine, table: &Table) -> Result<()> {
    table_store::drop_table(engine, &table.name)?;

    if table.primary_key_index_root_page_id != 0 {
        engine.delete_tree(table.primary_key_index_root_page_id)?;
    }

    for index in &table.secondary_indexes {
        if index.root_page_id == 0 {
            continue;
        }
        engine.delete_tree(index.root_page_id)?;
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
    let key_values = normalize_primary_key_values(table, key_values);
    let mut tree = open_required_tree(
        engine,
        table.primary_key_index_root_page_id,
        &format!("primary-key index for table '{}'", table.name),
    )?;
    match tree.get(&IndexKeyCodec::encode_key(&key_values)?)? {
        Some(value) => IndexKeyCodec::decode_row_id(&value).map(Some),
        None => Ok(None),
    }
}

pub(crate) fn register_primary_key_row(
    engine: &mut CatalogEngine,
    table: &Table,
    row: StoredRow,
) -> Result<()> {
    let key_values = table.get_primary_key_values(&row.values)?;
    let key_values = normalize_primary_key_values(table, &key_values);
    let mut tree = open_required_tree(
        engine,
        table.primary_key_index_root_page_id,
        &format!("primary-key index for table '{}'", table.name),
    )?;
    let encoded_key = IndexKeyCodec::encode_key(&key_values)?;
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
    let index = get_secondary_index(table, index_name)?;
    let key_values = normalize_secondary_index_values(table, index, key_values);
    let tree = open_required_tree(
        engine,
        index.root_page_id,
        &format!("secondary index '{}' on table '{}'", index.name, table.name),
    )?;
    tree.entries_with_prefix(&IndexKeyCodec::encode_key(&key_values)?)?
        .into_iter()
        .map(|(_key, value)| IndexKeyCodec::decode_row_id(&value))
        .collect()
}

pub(crate) fn register_secondary_index_row(
    engine: &mut CatalogEngine,
    table: &Table,
    row: StoredRow,
) -> Result<()> {
    for index in &table.secondary_indexes {
        let key_values = secondary_index_values(index, &row);
        let key_values = normalize_secondary_index_values(table, index, &key_values);
        let mut tree = open_required_tree(
            engine,
            index.root_page_id,
            &format!("secondary index '{}' on table '{}'", index.name, table.name),
        )?;
        if index.unique && tree_has_secondary_key(&tree, &key_values)? {
            return Err(unique_index_storage_error(&index.name, &table.name));
        }
        tree.insert_with_mutation(
            &IndexKeyCodec::encode_secondary_key(&key_values, row.row_id)?,
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
    engine.reset_tree(root_page_id)?;
    let mut seen = HashSet::new();
    let mut tree = engine.open_tree(root_page_id)?;
    for row in rows {
        let key_values = table.get_primary_key_values(&row.values)?;
        let key_values = normalize_primary_key_values(table, &key_values);
        let encoded = IndexKeyCodec::encode_key(&key_values)?;
        if !seen.insert(encoded) {
            return Err(HematiteError::StorageError(format!(
                "Duplicate primary key encountered while rebuilding table '{}'",
                table.name
            )));
        }
        tree.insert_with_mutation(
            &IndexKeyCodec::encode_key(&key_values)?,
            &row.row_id.to_be_bytes(),
        )?;
    }
    Ok(())
}

pub(crate) fn rebuild_secondary_indexes(
    engine: &mut CatalogEngine,
    table: &Table,
    rows: &[StoredRow],
) -> Result<()> {
    for index in &table.secondary_indexes {
        let root_page_id = require_index_root_page(
            index.root_page_id,
            &format!("secondary index '{}' on table '{}'", index.name, table.name),
        )?;
        engine.reset_tree(root_page_id)?;
        let mut seen = HashSet::new();
        let mut tree = engine.open_tree(root_page_id)?;
        for row in rows {
            let key_values = secondary_index_values(index, row);
            let key_values = normalize_secondary_index_values(table, index, &key_values);
            let encoded_key = IndexKeyCodec::encode_key(&key_values)?;
            if index.unique && !seen.insert(encoded_key) {
                return Err(HematiteError::StorageError(format!(
                    "Duplicate value encountered while rebuilding UNIQUE index '{}' on table '{}'",
                    index.name, table.name
                )));
            }
            tree.insert_with_mutation(
                &IndexKeyCodec::encode_secondary_key(&key_values, row.row_id)?,
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
    let key_values = table.get_primary_key_values(&row.values)?;
    let key_values = normalize_primary_key_values(table, &key_values);
    let mut tree = open_required_tree(
        engine,
        table.primary_key_index_root_page_id,
        &format!("primary-key index for table '{}'", table.name),
    )?;
    Ok(tree
        .delete(&IndexKeyCodec::encode_key(&key_values)?)?
        .is_some())
}

pub(crate) fn delete_secondary_index_row(
    engine: &mut CatalogEngine,
    table: &Table,
    row: &StoredRow,
) -> Result<()> {
    for index in &table.secondary_indexes {
        let key_values = secondary_index_values(index, row);
        let key_values = normalize_secondary_index_values(table, index, &key_values);
        let mut tree = engine.open_tree(index.root_page_id)?;
        let _ = tree.delete(&IndexKeyCodec::encode_secondary_key(
            &key_values,
            row.row_id,
        )?)?;
    }
    Ok(())
}

pub(crate) fn encode_primary_key(key_values: &[Value]) -> Result<Vec<u8>> {
    IndexKeyCodec::encode_key(key_values)
}

pub(crate) fn encode_secondary_index_key(key_values: &[Value]) -> Result<Vec<u8>> {
    IndexKeyCodec::encode_key(key_values)
}

pub(crate) fn open_primary_key_cursor(
    engine: &mut CatalogEngine,
    table: &Table,
) -> Result<IndexCursor> {
    let root_page_id = require_index_root_page(
        table.primary_key_index_root_page_id,
        &format!("primary-key index for table '{}'", table.name),
    )?;
    let mut entries = Vec::new();
    engine.visit_tree_entries(root_page_id, |key, value| {
        entries.push(super::cursor::IndexEntry {
            row_id: IndexKeyCodec::decode_row_id(value)?,
            key: key.to_vec(),
        });
        Ok(())
    })?;
    Ok(IndexCursor::new(entries))
}

pub(crate) fn open_secondary_index_cursor(
    engine: &mut CatalogEngine,
    table: &Table,
    index_name: &str,
) -> Result<IndexCursor> {
    let index = get_secondary_index(table, index_name)?;
    let root_page_id = require_index_root_page(
        index.root_page_id,
        &format!("secondary index '{}' on table '{}'", index.name, table.name),
    )?;
    let mut entries = Vec::new();
    engine.visit_tree_entries(root_page_id, |key, value| {
        entries.push(decode_secondary_entry(key, value)?);
        Ok(())
    })?;
    Ok(IndexCursor::new(entries))
}

pub(crate) fn validate_table_indexes(engine: &mut CatalogEngine, table: &Table) -> Result<()> {
    let rows = table_store::read_rows_with_ids(engine, &table.name)?;
    for row in &rows {
        let key_values = table.get_primary_key_values(&row.values)?;
        let key_values = normalize_primary_key_values(table, &key_values);
        let mut tree = open_required_tree(
            engine,
            table.primary_key_index_root_page_id,
            &format!("primary-key index for table '{}'", table.name),
        )?;
        let stored_rowid = tree
            .get(&IndexKeyCodec::encode_key(&key_values)?)?
            .map(|value| IndexKeyCodec::decode_row_id(&value))
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
            let key_values = secondary_index_values(index, row);
            let key_values = normalize_secondary_index_values(table, index, &key_values);
            let tree = open_required_tree(
                engine,
                index.root_page_id,
                &format!("secondary index '{}' on table '{}'", index.name, table.name),
            )?;
            let rowids = secondary_index_rowids_for_key(&tree, &key_values)?;
            if index.unique && rowids.len() > 1 {
                return Err(HematiteError::CorruptedData(format!(
                    "UNIQUE index '{}' contains duplicate entries for table '{}'",
                    index.name, table.name
                )));
            }
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

fn normalize_primary_key_values(table: &Table, key_values: &[Value]) -> Vec<Value> {
    table
        .primary_key_columns
        .iter()
        .zip(key_values.iter())
        .map(|(column_index, value)| normalize_index_value(&table.columns[*column_index], value))
        .collect()
}

fn normalize_secondary_index_values(
    table: &Table,
    index: &crate::catalog::SecondaryIndex,
    key_values: &[Value],
) -> Vec<Value> {
    index
        .column_indices
        .iter()
        .zip(key_values.iter())
        .map(|(column_index, value)| normalize_index_value(&table.columns[*column_index], value))
        .collect()
}

fn normalize_index_value(column: &Column, value: &Value) -> Value {
    match (&column.data_type, value) {
        (crate::catalog::DataType::Char(length), Value::Text(text)) => Value::Text(
            normalize_text_for_collation(
                &pad_text_to_char_length(text, *length),
                column.collation.as_deref(),
            ),
        ),
        (
            crate::catalog::DataType::Text | crate::catalog::DataType::VarChar(_),
            Value::Text(text),
        ) => Value::Text(normalize_text_for_collation(text, column.collation.as_deref())),
        _ => value.clone(),
    }
}

fn open_required_tree(
    engine: &CatalogEngine,
    root_page_id: u32,
    label: &str,
) -> Result<crate::btree::ByteTree> {
    engine.open_tree(require_index_root_page(root_page_id, label)?)
}

fn get_secondary_index<'a>(
    table: &'a Table,
    index_name: &str,
) -> Result<&'a crate::catalog::table::SecondaryIndex> {
    table.get_secondary_index(index_name).ok_or_else(|| {
        HematiteError::StorageError(format!(
            "Secondary index '{}' does not exist on table '{}'",
            index_name, table.name
        ))
    })
}

fn secondary_index_values(
    index: &crate::catalog::table::SecondaryIndex,
    row: &StoredRow,
) -> Vec<Value> {
    index
        .column_indices
        .iter()
        .map(|&column_index| row.values[column_index].clone())
        .collect()
}

fn tree_has_secondary_key(tree: &crate::btree::ByteTree, key_values: &[Value]) -> Result<bool> {
    Ok(!tree
        .entries_with_prefix(&IndexKeyCodec::encode_key(key_values)?)?
        .is_empty())
}

fn secondary_index_rowids_for_key(
    tree: &crate::btree::ByteTree,
    key_values: &[Value],
) -> Result<Vec<u64>> {
    tree.entries_with_prefix(&IndexKeyCodec::encode_key(key_values)?)?
        .into_iter()
        .map(|(_key, value)| IndexKeyCodec::decode_row_id(&value))
        .collect()
}

fn unique_index_storage_error(index_name: &str, table_name: &str) -> HematiteError {
    HematiteError::StorageError(format!(
        "Duplicate value for UNIQUE index '{}' on table '{}'",
        index_name, table_name
    ))
}

pub(crate) fn require_index_root_page(root_page_id: u32, label: &str) -> Result<u32> {
    if root_page_id == 0 || root_page_id == INVALID_ROOT_PAGE_ID {
        return Err(HematiteError::StorageError(format!(
            "Missing durable {} root page",
            label
        )));
    }
    Ok(root_page_id)
}

fn decode_secondary_entry(key: &[u8], value: &[u8]) -> Result<super::cursor::IndexEntry> {
    let logical_key = if key.len() >= 8 {
        IndexKeyCodec::split_secondary_key(key)?.0
    } else {
        key.to_vec()
    };

    let row_id = if value.len() == 8 {
        IndexKeyCodec::decode_row_id(value)?
    } else {
        IndexKeyCodec::split_secondary_key(key)?.1
    };

    Ok(super::cursor::IndexEntry {
        row_id,
        key: logical_key,
    })
}
