//! Table storage operations for catalog-managed rowid tables.

use crate::catalog::Value;
use crate::error::Result;

use super::cursor::TableCursor;
use super::engine::{CatalogEngine, CatalogStorageStats};
use super::record::StoredRow;
use super::serialization::RowCodec;

pub(crate) fn get_storage_stats(engine: &CatalogEngine) -> Result<CatalogStorageStats> {
    let pager = engine.lock_pager()?;
    let file_bytes = pager.file_len().unwrap_or(0);
    let allocated_page_count = pager.allocated_page_count();
    let free_page_count = pager.free_pages().len();
    let fragmented_free_page_count = pager.fragmented_free_page_count();
    let trailing_free_page_count = pager.trailing_free_page_count();
    let mut live_table_page_count = 0usize;
    let mut overflow_page_count = 0usize;
    let mut table_used_bytes = 0usize;
    drop(pager);
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
        .insert_with_mutation(&row_id.to_be_bytes(), &encoded_row)?
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
    let (deleted, mutation) = tree.delete_with_mutation(&rowid.to_be_bytes())?;
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
    engine.visit_tree_entries(root_page_id, |_key, value| {
        rows.push(RowCodec::decode_stored_row(value)?);
        Ok(())
    })?;
    Ok(TableCursor::new(rows))
}

pub(crate) fn read_rows_with_ids(
    engine: &mut CatalogEngine,
    table_name: &str,
) -> Result<Vec<StoredRow>> {
    let mut cursor = open_table_cursor(engine, table_name)?;
    let mut rows = Vec::new();
    if cursor.first() {
        loop {
            if let Some(row) = cursor.current() {
                rows.push(row.clone());
            }
            if !cursor.next() {
                break;
            }
        }
    }
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

pub(crate) fn lookup_row_by_rowid(
    engine: &mut CatalogEngine,
    table_name: &str,
    rowid: u64,
) -> Result<Option<StoredRow>> {
    let root_page_id = engine.table_runtime_metadata(table_name)?.root_page_id;
    let mut tree = engine.open_tree(root_page_id)?;
    match tree.get(&rowid.to_be_bytes())? {
        Some(value) => Ok(Some(RowCodec::decode_stored_row(&value)?)),
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
        .insert_with_mutation(&row.row_id.to_be_bytes(), &encoded_row)?
        .root_page_id;

    engine.record_explicit_row_insert(table_name, new_root_page_id);
    Ok(())
}
