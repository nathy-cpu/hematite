//! Table storage operations for catalog-managed rowid tables.

use crate::catalog::Value;
use crate::error::Result;

use super::cursor::TableCursor;
use super::engine::{CatalogEngine, CatalogStorageStats};
use super::record::StoredRow;
use super::serialization::RowCodec;

pub(crate) fn get_storage_stats(engine: &CatalogEngine) -> Result<CatalogStorageStats> {
    let tree_store = engine.tree_store();
    let file_bytes = tree_store.file_len().unwrap_or(0);
    let allocated_page_count = tree_store.allocated_page_count()?;
    let free_page_ids = tree_store.free_page_ids()?;
    let free_page_count = free_page_ids.len();
    let fragmented_free_page_count = tree_store.fragmented_free_page_count()?;
    let trailing_free_page_count = tree_store.trailing_free_page_count()?;
    let mut live_table_page_count = 0usize;
    let mut overflow_page_count = 0usize;
    let mut table_used_bytes = 0usize;
    for metadata in engine.table_metadata.values() {
        if let Ok(space_stats) = engine.collect_tree_space_stats(metadata.root_page_id) {
            live_table_page_count += space_stats.page_ids.len();
            overflow_page_count += space_stats.overflow_page_ids.len();
            table_used_bytes += space_stats.used_bytes + space_stats.overflow_used_bytes;
        }
    }

    Ok(CatalogStorageStats {
        table_count: engine.table_metadata.len(),
        total_rows: engine.table_metadata.values().map(|m| m.row_count).sum(),
        file_bytes,
        allocated_page_count,
        free_page_count,
        fragmented_free_page_count,
        trailing_free_page_count,
        live_table_page_count,
        overflow_page_count,
        table_used_bytes,
        table_unused_bytes: (live_table_page_count + overflow_page_count)
            .saturating_mul(CatalogEngine::PAGE_SIZE)
            .saturating_sub(table_used_bytes),
    })
}

pub(crate) fn create_table(engine: &mut CatalogEngine, table_name: &str) -> Result<u32> {
    let root_page_id = engine.create_tree()?;
    engine.create_runtime_table_metadata(table_name, root_page_id)?;
    Ok(root_page_id)
}

pub(crate) fn insert_into_table(
    engine: &mut CatalogEngine,
    table_name: &str,
    row: Vec<Value>,
) -> Result<u64> {
    let (root_page_id, row_id) = {
        let metadata = engine.table_runtime_metadata(table_name)?;
        (metadata.root_page_id, metadata.next_row_id)
    };

    let mut tree = engine.open_tree(root_page_id)?;
    let encoded_row = RowCodec::encode_stored_row(&StoredRow {
        row_id,
        values: row,
    })?;
    let new_root_page_id = tree
        .insert_with_mutation(&RowCodec::encode_row_id_key(row_id), &encoded_row)?
        .root_page_id;

    engine.record_generated_row_insert(table_name, new_root_page_id, row_id);
    Ok(row_id)
}

pub(crate) fn replace_table_rows(
    engine: &mut CatalogEngine,
    table_name: &str,
    rows: Vec<StoredRow>,
) -> Result<()> {
    let root_page_id = engine.table_runtime_metadata(table_name)?.root_page_id;
    engine.reset_tree(root_page_id)?;
    engine.prepare_table_replace(table_name, &rows);

    for row in rows {
        insert_stored_row(engine, table_name, row)?;
    }

    Ok(())
}

pub(crate) fn insert_row_with_rowid(
    engine: &mut CatalogEngine,
    table_name: &str,
    row: StoredRow,
) -> Result<()> {
    insert_stored_row(engine, table_name, row)
}

pub(crate) fn delete_from_table_by_rowid(
    engine: &mut CatalogEngine,
    table_name: &str,
    rowid: u64,
) -> Result<bool> {
    let root_page_id = engine.table_runtime_metadata(table_name)?.root_page_id;
    let mut tree = engine.open_tree(root_page_id)?;
    let (deleted, mutation) = tree.delete_with_mutation(&RowCodec::encode_row_id_key(rowid))?;
    engine.record_row_delete(table_name, mutation.root_page_id, deleted.is_some());
    Ok(deleted.is_some())
}

pub(crate) fn drop_table(engine: &mut CatalogEngine, table_name: &str) -> Result<()> {
    let metadata = engine.remove_runtime_table_metadata(table_name)?;
    engine.delete_tree(metadata.root_page_id)
}

pub(crate) fn open_table_cursor(
    engine: &mut CatalogEngine,
    table_name: &str,
) -> Result<TableCursor> {
    let root_page_id = engine.table_runtime_metadata(table_name)?.root_page_id;
    let mut rows = Vec::new();
    engine.visit_tree_entries(root_page_id, |key, value| {
        let row_id = RowCodec::decode_row_id_key(key)?;
        rows.push(RowCodec::decode_stored_row(row_id, value)?);
        Ok(())
    })?;
    Ok(TableCursor::from_ordered_rows(rows))
}

/// Streams a table's rows in row-id order, decoding one at a time.
///
/// The building block for callers that only want *some* of the rows. Going through
/// [`read_rows_with_ids`] instead means decoding and holding the entire table before
/// the first row can be looked at, which for a filter is all wasted.
pub(crate) fn visit_rows<F>(
    engine: &mut CatalogEngine,
    table_name: &str,
    mut visit: F,
) -> Result<()>
where
    F: FnMut(StoredRow) -> Result<()>,
{
    let root_page_id = engine.table_runtime_metadata(table_name)?.root_page_id;
    engine.visit_tree_entries(root_page_id, |key, value| {
        let row_id = RowCodec::decode_row_id_key(key)?;
        visit(RowCodec::decode_stored_row(row_id, value)?)
    })
}

/// Streams a table's row ids without decoding any records.
///
/// The row id is the tree key, so answering "which rows are there" never needs to
/// look at a row's value at all. Callers that went through [`visit_rows`] for this
/// decoded every column of every row and then discarded all of them.
pub(crate) fn visit_row_ids<F>(
    engine: &mut CatalogEngine,
    table_name: &str,
    mut visit: F,
) -> Result<()>
where
    F: FnMut(u64) -> Result<()>,
{
    let root_page_id = engine.table_runtime_metadata(table_name)?.root_page_id;
    engine.visit_tree_entries(root_page_id, |key, _value| {
        visit(RowCodec::decode_row_id_key(key)?)
    })
}

pub(crate) fn read_rows_with_ids(
    engine: &mut CatalogEngine,
    table_name: &str,
) -> Result<Vec<StoredRow>> {
    // Collects the stream. This used to build a `TableCursor` — which already decodes
    // every row into a `Vec` — and then walk it cloning each row into a second `Vec`,
    // so every caller paid for the whole table twice.
    let mut rows = Vec::new();
    visit_rows(engine, table_name, |row| {
        rows.push(row);
        Ok(())
    })?;
    Ok(rows)
}

pub(crate) fn read_from_table(
    engine: &mut CatalogEngine,
    table_name: &str,
) -> Result<Vec<Vec<Value>>> {
    Ok(read_rows_with_ids(engine, table_name)?
        .into_iter()
        .map(|row| row.values)
        .collect())
}

/// Reads at most `max_rows` rows, stopping the scan once it has them.
///
/// Only correct where the first `max_rows` rows of the table *are* the answer — no
/// `WHERE`, no `ORDER BY`, no grouping or `DISTINCT` — which the caller decides. See
/// `SelectExecutor::scan_row_cap`.
pub(crate) fn read_from_table_capped(
    engine: &mut CatalogEngine,
    table_name: &str,
    max_rows: usize,
) -> Result<Vec<Vec<Value>>> {
    let root_page_id = engine.table_runtime_metadata(table_name)?.root_page_id;
    let mut rows = Vec::new();
    if max_rows == 0 {
        return Ok(rows);
    }
    engine.visit_tree_entries_while(root_page_id, |key, value| {
        let row_id = RowCodec::decode_row_id_key(key)?;
        rows.push(RowCodec::decode_stored_row(row_id, value)?.values);
        Ok(rows.len() < max_rows)
    })?;
    Ok(rows)
}

/// Reads a table decoding only `indices`, in that order.
///
/// The narrow path behind a `SELECT a, b FROM t` with nothing else in it: no
/// predicate, no ordering, no grouping. Rows come back already in projection order,
/// so the executor does not build a second row per result to reorder into.
pub(crate) fn read_table_columns(
    engine: &mut CatalogEngine,
    table_name: &str,
    indices: &[usize],
    max_rows: Option<usize>,
) -> Result<Vec<Vec<Value>>> {
    let root_page_id = engine.table_runtime_metadata(table_name)?.root_page_id;
    let mut rows = Vec::new();
    if max_rows == Some(0) {
        return Ok(rows);
    }
    engine.visit_tree_values_while(root_page_id, |value| {
        rows.push(RowCodec::decode_row_fields(value, indices)?);
        Ok(max_rows.is_none_or(|cap| rows.len() < cap))
    })?;
    Ok(rows)
}

pub(crate) fn lookup_row_by_rowid(
    engine: &mut CatalogEngine,
    table_name: &str,
    rowid: u64,
) -> Result<Option<StoredRow>> {
    let root_page_id = engine.table_runtime_metadata(table_name)?.root_page_id;
    let tree = engine.open_tree(root_page_id)?;
    match tree.get(&RowCodec::encode_row_id_key(rowid))? {
        Some(value) => Ok(Some(RowCodec::decode_stored_row(rowid, &value)?)),
        None => Ok(None),
    }
}

pub(crate) fn insert_stored_row(
    engine: &mut CatalogEngine,
    table_name: &str,
    row: StoredRow,
) -> Result<()> {
    let root_page_id = engine.table_runtime_metadata(table_name)?.root_page_id;
    let mut tree = engine.open_tree(root_page_id)?;
    let encoded_row = RowCodec::encode_stored_row(&row)?;
    let new_root_page_id = tree
        .insert_with_mutation(&RowCodec::encode_row_id_key(row.row_id), &encoded_row)?
        .root_page_id;

    engine.record_explicit_row_insert(table_name, new_root_page_id);
    Ok(())
}
