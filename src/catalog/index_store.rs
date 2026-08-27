//! Durable index storage operations for catalog-managed tables.

use std::collections::HashSet;

use crate::catalog::{Column, Table, Value};
use crate::error::{HematiteError, Result};

use super::cursor::IndexCursor;
use super::engine::CatalogEngine;
use super::record::StoredRow;
use super::serialization::{IndexKeyCodec, RowCodec};
use super::table_store;
use crate::catalog::column::normalize_text_for_collation;

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

/// The row id a table whose primary key *is* its row id stores this row under.
///
/// `None` when the table keeps a real primary-key index, which is every table
/// whose key is composite or not an integer.
fn aliased_rowid(table: &Table, values: &[Value]) -> Result<Option<u64>> {
    let Some(column_index) = table.rowid_alias_column() else {
        return Ok(None);
    };
    let value = values.get(column_index).ok_or_else(|| {
        HematiteError::StorageError(format!(
            "Primary key value not found for table '{}'",
            table.name
        ))
    })?;
    let row_id = RowCodec::rowid_for_primary_key(value).ok_or_else(|| {
        HematiteError::StorageError(format!(
            "Primary key for table '{}' is not an integer row id: {value:?}",
            table.name
        ))
    })?;
    Ok(Some(row_id))
}

/// The row id for a key given as the primary-key tuple rather than a whole row.
fn aliased_rowid_for_key(table: &Table, key_values: &[Value]) -> Result<Option<u64>> {
    if table.rowid_alias_column().is_none() {
        return Ok(None);
    }
    let value = key_values.first().ok_or_else(|| {
        HematiteError::StorageError(format!(
            "Primary key lookup for table '{}' supplied no value",
            table.name
        ))
    })?;
    let row_id = RowCodec::rowid_for_primary_key(value).ok_or_else(|| {
        HematiteError::StorageError(format!(
            "Primary key for table '{}' is not an integer row id: {value:?}",
            table.name
        ))
    })?;
    Ok(Some(row_id))
}

pub(crate) fn lookup_primary_key_rowid(
    engine: &mut CatalogEngine,
    table: &Table,
    key_values: &[Value],
) -> Result<Option<u64>> {
    if let Some(row_id) = aliased_rowid_for_key(table, key_values)? {
        // The key is the row id, so the table's own tree answers this. There is no
        // index to consult and nothing to decode: the question is whether the tree
        // holds that key.
        return Ok(table_store::rowid_exists(engine, &table.name, row_id)?.then_some(row_id));
    }

    let key_values = normalize_primary_key_values(table, key_values);
    let tree = open_required_tree(
        engine,
        table.primary_key_index_root_page_id,
        IndexLabel::PrimaryKey { table: &table.name },
    )?;
    match tree.get(&IndexKeyCodec::encode_key(&key_values)?)? {
        Some(value) => IndexKeyCodec::decode_row_id(&value).map(Some),
        None => Ok(None),
    }
}

/// The row id a row's primary key names, without asking whether it is free.
///
/// For the paths that are replacing a row they have already removed, where the id
/// being taken is the point rather than a collision.
pub(crate) fn aliased_rowid_for_row(table: &Table, values: &[Value]) -> Result<Option<u64>> {
    aliased_rowid(table, values)
}

pub(crate) fn register_primary_key_row(
    engine: &mut CatalogEngine,
    table: &Table,
    row: StoredRow,
) -> Result<()> {
    if let Some(expected_row_id) = aliased_rowid(table, &row.values)? {
        // Nothing to record: the row is already stored under its primary key.
        // Uniqueness was settled before the row was written -- see
        // `reserve_aliased_rowid`, which has to run first because inserting a
        // duplicate row id would overwrite the row it collided with.
        //
        // What is worth checking is that the row went in under the id its key
        // says it should have. A caller that allocated a row id from the counter
        // and then registered a different primary key would leave a row nothing
        // can find, and that is exactly what the low-level engine API does if it
        // is used the pre-alias way.
        if row.row_id != expected_row_id {
            return Err(HematiteError::StorageError(format!(
                "Row in table '{}' is stored under row id {} but its primary key is \
row id {expected_row_id}; a table keyed by a single integer column stores rows \
under that key",
                table.name, row.row_id
            )));
        }
        return Ok(());
    }

    let key_values = normalized_primary_key_values(table, &row.values)?;
    let mut tree = open_required_tree(
        engine,
        table.primary_key_index_root_page_id,
        IndexLabel::PrimaryKey { table: &table.name },
    )?;
    let encoded_key = IndexKeyCodec::encode_key(&key_values)?;
    if tree.get(&encoded_key)?.is_some() {
        return Err(HematiteError::StorageError(format!(
            "Duplicate primary key for table '{}'",
            table.name
        )));
    }
    tree.insert_with_mutation(
        &encoded_key,
        &IndexKeyCodec::encode_row_id_value(row.row_id),
    )?;
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
        IndexLabel::Secondary {
            index: &index.name,
            table: &table.name,
        },
    )?;
    tree.entries_with_prefix(&IndexKeyCodec::encode_key(&key_values)?)?
        .into_iter()
        .map(|(key, _value)| Ok(IndexKeyCodec::split_secondary_key(&key)?.1))
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
            IndexLabel::Secondary {
                index: &index.name,
                table: &table.name,
            },
        )?;
        if index.unique && tree_has_secondary_key(&tree, &key_values)? {
            return Err(unique_index_storage_error(&index.name, &table.name));
        }
        // No payload: the row id is already the key's trailing eight bytes, so
        // storing it again would be a second copy of the same number.
        tree.insert_with_mutation(
            &IndexKeyCodec::encode_secondary_key(&key_values, row.row_id)?,
            &[],
        )?;
    }
    Ok(())
}

pub(crate) fn rebuild_primary_key_index(
    engine: &mut CatalogEngine,
    table: &Table,
    rows: &[StoredRow],
) -> Result<()> {
    if table.has_rowid_alias() {
        // Nothing to rebuild. The rows arrive keyed by their primary key, so the
        // table tree they are written into *is* the index, and its keys are unique
        // by construction.
        return Ok(());
    }

    let root_page_id = require_index_root_page(
        table.primary_key_index_root_page_id,
        IndexLabel::PrimaryKey { table: &table.name },
    )?;
    engine.reset_tree(root_page_id)?;
    let mut seen = HashSet::new();
    let mut tree = engine.open_tree(root_page_id)?;
    for row in rows {
        let key_values = normalized_primary_key_values(table, &row.values)?;
        let encoded = IndexKeyCodec::encode_key(&key_values)?;
        if !seen.insert(encoded) {
            return Err(HematiteError::StorageError(format!(
                "Duplicate primary key encountered while rebuilding table '{}'",
                table.name
            )));
        }
        tree.insert_with_mutation(
            &IndexKeyCodec::encode_key(&key_values)?,
            &IndexKeyCodec::encode_row_id_value(row.row_id),
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
            IndexLabel::Secondary {
                index: &index.name,
                table: &table.name,
            },
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
                &[],
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
    if table.has_rowid_alias() {
        // The entry the caller means is the row itself, which it removes next.
        return Ok(true);
    }

    let key_values = normalized_primary_key_values(table, &row.values)?;
    let mut tree = open_required_tree(
        engine,
        table.primary_key_index_root_page_id,
        IndexLabel::PrimaryKey { table: &table.name },
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
        IndexLabel::PrimaryKey { table: &table.name },
    )?;
    let mut entries = Vec::new();
    engine.visit_tree_entries(root_page_id, |key, value| {
        entries.push(super::cursor::IndexEntry {
            row_id: IndexKeyCodec::decode_row_id(value)?,
            key: key.to_vec(),
        });
        Ok(())
    })?;
    Ok(IndexCursor::from_ordered_entries(entries))
}

pub(crate) fn open_secondary_index_cursor(
    engine: &mut CatalogEngine,
    table: &Table,
    index_name: &str,
) -> Result<IndexCursor> {
    let index = get_secondary_index(table, index_name)?;
    let root_page_id = require_index_root_page(
        index.root_page_id,
        IndexLabel::Secondary {
            index: &index.name,
            table: &table.name,
        },
    )?;
    let mut entries = Vec::new();
    engine.visit_tree_entries(root_page_id, |key, value| {
        entries.push(decode_secondary_entry(key, value)?);
        Ok(())
    })?;
    Ok(IndexCursor::from_ordered_entries(entries))
}

pub(crate) fn validate_table_indexes(engine: &mut CatalogEngine, table: &Table) -> Result<()> {
    let rows = table_store::read_rows_with_ids(engine, &table.name)?;

    // Opened once. This used to sit inside the loop, so validating a table re-opened
    // its index tree — and built the label `String` that names it — once per row.
    //
    // Guarded on there being rows because opening is also what *requires* the index
    // to exist: a table with no rows previously never entered the loop, so a missing
    // index root went unchallenged, and some callers rely on that.
    if table.has_rowid_alias() {
        // There is no index to check against. The invariant is that every row sits
        // under the row id its key names — the same one `register_primary_key_row`
        // enforces on the way in, checked here against what is actually stored.
        for row in &rows {
            let expected = aliased_rowid(table, &row.values)?
                .expect("a table with a row-id alias derives a row id from every row");
            if row.row_id != expected {
                return Err(HematiteError::CorruptedData(format!(
                    "Row in table '{}' is stored under row id {} but its primary key is \
row id {expected}",
                    table.name, row.row_id
                )));
            }
        }
        return validate_secondary_indexes(engine, table, &rows);
    }

    let primary_tree = if rows.is_empty() {
        None
    } else {
        Some(open_required_tree(
            engine,
            table.primary_key_index_root_page_id,
            IndexLabel::PrimaryKey { table: &table.name },
        )?)
    };
    for row in &rows {
        let primary_tree = primary_tree
            .as_ref()
            .expect("a non-empty row set opens the tree");
        let key_values = normalized_primary_key_values(table, &row.values)?;
        let stored_rowid = primary_tree
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

    validate_secondary_indexes(engine, table, &rows)
}

fn validate_secondary_indexes(
    engine: &mut CatalogEngine,
    table: &Table,
    rows: &[StoredRow],
) -> Result<()> {
    for index in &table.secondary_indexes {
        if rows.is_empty() {
            continue;
        }
        // Once per index, not once per row of every index.
        let tree = open_required_tree(
            engine,
            index.root_page_id,
            IndexLabel::Secondary {
                index: &index.name,
                table: &table.name,
            },
        )?;
        for row in rows {
            let key_values = secondary_index_values(index, row);
            let key_values = normalize_secondary_index_values(table, index, &key_values);
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

/// Pulls a row's primary-key columns out and normalises them in one pass.
///
/// Taking the key values first and normalising them after built two `Vec<Value>`
/// per row, and cloned each value twice: once out of the row, once into the
/// normalised copy. Every index registration and deletion did it, so it was four
/// allocations a row before the key was even encoded.
fn normalized_primary_key_values(table: &Table, row_values: &[Value]) -> Result<Vec<Value>> {
    table
        .primary_key_columns
        .iter()
        .map(|&index| {
            let value = row_values.get(index).ok_or_else(|| {
                HematiteError::StorageError("Primary key value not found".to_string())
            })?;
            Ok(normalize_index_value(&table.columns[index], value))
        })
        .collect()
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
        (
            crate::catalog::DataType::Text | crate::catalog::DataType::VarChar(_),
            Value::Text(text),
        ) => Value::Text(normalize_text_for_collation(
            text,
            column.collation.as_deref(),
        )),
        _ => value.clone(),
    }
}

fn open_required_tree(
    engine: &CatalogEngine,
    root_page_id: u32,
    label: IndexLabel<'_>,
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
        .map(|(key, _value)| Ok(IndexKeyCodec::split_secondary_key(&key)?.1))
        .collect()
}

fn unique_index_storage_error(index_name: &str, table_name: &str) -> HematiteError {
    HematiteError::StorageError(format!(
        "Duplicate value for UNIQUE index '{}' on table '{}'",
        index_name, table_name
    ))
}

pub(crate) fn require_index_root_page(root_page_id: u32, label: IndexLabel<'_>) -> Result<u32> {
    if root_page_id == 0 || root_page_id == INVALID_ROOT_PAGE_ID {
        return Err(HematiteError::StorageError(format!(
            "Missing durable {} root page",
            label
        )));
    }
    Ok(root_page_id)
}

/// Names an index for an error message, without building the message.
///
/// Every call that opens an index tree passes one of these to say what to call
/// the tree if it turns out not to be there. They used to be `&format!(...)`,
/// which allocated and formatted the sentence on every call — once per row on the
/// primary-key path — for an error that fires only on a corrupt catalog.
#[derive(Debug, Clone, Copy)]
pub(crate) enum IndexLabel<'a> {
    PrimaryKey { table: &'a str },
    Secondary { index: &'a str, table: &'a str },
}

impl std::fmt::Display for IndexLabel<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PrimaryKey { table } => write!(f, "primary-key index for table '{table}'"),
            Self::Secondary { index, table } => {
                write!(f, "secondary index '{index}' on table '{table}'")
            }
        }
    }
}

/// Secondary entries carry no payload, so both halves come from the key.
fn decode_secondary_entry(key: &[u8], _value: &[u8]) -> Result<super::cursor::IndexEntry> {
    let (logical_key, row_id) = IndexKeyCodec::split_secondary_key(key)?;
    Ok(super::cursor::IndexEntry {
        row_id,
        key: logical_key.to_vec(),
    })
}
