//! Table storage operations for catalog-managed rowid tables.

use crate::btree::ByteTreeStore;
use crate::catalog::Value;
use crate::error::{HematiteError, Result};
use crate::storage::PageId;

use super::cursor::TableCursor;
use super::engine::{CatalogEngine, CatalogStorageStats, StoredRow};
use super::engine_metadata;
use super::serialization::RowSerializer;

pub(crate) fn get_storage_stats(engine: &CatalogEngine) -> CatalogStorageStats {
    let pager = engine.pager.lock().unwrap();
    let file_bytes = pager.file_len().unwrap_or(0);
    let allocated_page_count = pager.allocated_page_count();
    let free_page_count = pager.free_pages().len();
    let fragmented_free_page_count = pager.fragmented_free_page_count();
    let trailing_free_page_count = pager.trailing_free_page_count();
    let mut live_table_page_count = 0usize;
    let mut table_used_bytes = 0usize;
    drop(pager);
    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());

    for metadata in engine.table_metadata.values() {
        if let Ok(space_stats) = trees.collect_space_stats(metadata.root_page_id) {
            live_table_page_count += space_stats.page_ids.len();
            table_used_bytes += space_stats.used_bytes;
        }
    }

    CatalogStorageStats {
        table_count: engine.table_metadata.len(),
        total_rows: engine.table_metadata.values().map(|m| m.row_count).sum(),
        file_bytes,
        allocated_page_count,
        free_page_count,
        fragmented_free_page_count,
        trailing_free_page_count,
        live_table_page_count,
        table_used_bytes,
        table_unused_bytes: live_table_page_count
            .saturating_mul(crate::storage::PAGE_SIZE)
            .saturating_sub(table_used_bytes),
    }
}

pub(crate) fn create_table(engine: &mut CatalogEngine, table_name: &str) -> Result<PageId> {
    let root_page_id = engine.create_empty_btree()?;
    engine_metadata::create_table_metadata(engine, table_name, root_page_id)?;
    Ok(root_page_id)
}

pub(crate) fn insert_into_table(
    engine: &mut CatalogEngine,
    table_name: &str,
    row: Vec<Value>,
) -> Result<u64> {
    let (root_page_id, row_id) = {
        let metadata = engine_metadata::lookup_table_metadata(engine, table_name)?;
        (metadata.root_page_id, metadata.next_row_id)
    };

    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());
    let mut tree = trees.open_tree(root_page_id)?;
    let encoded_row = RowSerializer::serialize_stored_row(&StoredRow {
        row_id,
        values: row,
    })?;
    let new_root_page_id = tree
        .insert_with_mutation(&row_id.to_be_bytes(), &encoded_row)?
        .root_page_id;

    if let Some(metadata) = engine.table_metadata.get_mut(table_name) {
        metadata.root_page_id = new_root_page_id;
    }
    if let Some(metadata) = engine.table_metadata.get_mut(table_name) {
        metadata.row_count += 1;
        metadata.next_row_id += 1;
    }
    Ok(row_id)
}

pub(crate) fn replace_table_rows(
    engine: &mut CatalogEngine,
    table_name: &str,
    rows: Vec<StoredRow>,
) -> Result<()> {
    let root_page_id = engine_metadata::lookup_table_metadata(engine, table_name)?.root_page_id;
    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());
    trees.reset_tree(root_page_id)?;

    let next_row_id = engine
        .table_metadata
        .get(table_name)
        .map(|metadata| metadata.next_row_id)
        .unwrap_or(1);

    if let Some(metadata) = engine.table_metadata.get_mut(table_name) {
        metadata.row_count = 0;
        metadata.next_row_id =
            next_row_id.max(rows.iter().map(|row| row.row_id).max().unwrap_or(0) + 1);
    }

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
    let root_page_id = engine_metadata::lookup_table_metadata(engine, table_name)?.root_page_id;
    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());
    let mut tree = trees.open_tree(root_page_id)?;
    let (deleted, mutation) = tree.delete_with_mutation(&rowid.to_be_bytes())?;
    if let Some(metadata) = engine.table_metadata.get_mut(table_name) {
        metadata.root_page_id = mutation.root_page_id;
    }
    if deleted.is_some() {
        if let Some(metadata) = engine.table_metadata.get_mut(table_name) {
            metadata.row_count = metadata.row_count.saturating_sub(1);
        }
    }
    Ok(deleted.is_some())
}

pub(crate) fn drop_table(engine: &mut CatalogEngine, table_name: &str) -> Result<()> {
    let metadata = engine.table_metadata.remove(table_name).ok_or_else(|| {
        HematiteError::StorageError(format!("Table '{}' does not exist", table_name))
    })?;
    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());
    trees.delete_tree(metadata.root_page_id)
}

pub(crate) fn open_table_cursor(
    engine: &mut CatalogEngine,
    table_name: &str,
) -> Result<TableCursor> {
    let root_page_id = engine_metadata::lookup_table_metadata(engine, table_name)?.root_page_id;
    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());
    let tree = trees.open_tree(root_page_id)?;
    let rows = tree
        .entries()?
        .into_iter()
        .map(|(_key, value)| RowSerializer::deserialize_stored_row(&value))
        .collect::<Result<Vec<_>>>()?;
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
    let root_page_id = engine_metadata::lookup_table_metadata(engine, table_name)?.root_page_id;
    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());
    let mut tree = trees.open_tree(root_page_id)?;
    match tree.get(&rowid.to_be_bytes())? {
        Some(value) => Ok(Some(RowSerializer::deserialize_stored_row(&value)?)),
        None => Ok(None),
    }
}

pub(crate) fn insert_stored_row(
    engine: &mut CatalogEngine,
    table_name: &str,
    row: StoredRow,
) -> Result<()> {
    let root_page_id = engine_metadata::lookup_table_metadata(engine, table_name)?.root_page_id;
    let trees = ByteTreeStore::from_shared_storage(engine.shared_pager());
    let mut tree = trees.open_tree(root_page_id)?;
    let encoded_row = RowSerializer::serialize_stored_row(&row)?;
    let new_root_page_id = tree
        .insert_with_mutation(&row.row_id.to_be_bytes(), &encoded_row)?
        .root_page_id;

    if let Some(metadata) = engine.table_metadata.get_mut(table_name) {
        metadata.root_page_id = new_root_page_id;
    }
    if let Some(metadata) = engine.table_metadata.get_mut(table_name) {
        metadata.row_count += 1;
    }
    Ok(())
}
