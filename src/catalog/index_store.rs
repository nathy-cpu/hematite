//! Durable index storage operations for catalog-managed tables.

use std::collections::HashSet;

use crate::catalog::{Table, Value};
use crate::error::{HematiteError, Result};
use crate::storage::{PageId, INVALID_PAGE_ID};

use super::cursor::IndexCursor;
use super::engine::{CatalogEngine, StoredRow};
use super::index_btree;
use super::table_store;

pub(crate) fn drop_table_with_indexes(engine: &mut CatalogEngine, table: &Table) -> Result<()> {
    table_store::drop_table(engine, &table.name)?;
    let mut pager = engine.pager.lock().unwrap();

    if table.primary_key_index_root_page_id != 0 {
        let mut page_ids = Vec::new();
        index_btree::collect_page_ids(
            &mut pager,
            table.primary_key_index_root_page_id,
            &mut page_ids,
        )?;
        for page_id in page_ids {
            pager.deallocate_page(page_id)?;
        }
    }

    for index in &table.secondary_indexes {
        if index.root_page_id == 0 {
            continue;
        }
        let mut page_ids = Vec::new();
        index_btree::collect_page_ids(&mut pager, index.root_page_id, &mut page_ids)?;
        for page_id in page_ids {
            pager.deallocate_page(page_id)?;
        }
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
    let mut pager = engine.pager.lock().unwrap();
    index_btree::lookup_primary_key(&mut pager, root_page_id, key_values)
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
    let mut pager = engine.pager.lock().unwrap();
    if index_btree::lookup_primary_key(&mut pager, root_page_id, &key_values)?.is_some() {
        return Err(HematiteError::StorageError(format!(
            "Duplicate primary key for table '{}'",
            table.name
        )));
    }
    index_btree::insert_primary_key(&mut pager, root_page_id, &key_values, row.row_id)?;
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
    let mut pager = engine.pager.lock().unwrap();
    index_btree::lookup_secondary_rowids(&mut pager, root_page_id, key_values)
}

pub(crate) fn register_secondary_index_row(
    engine: &mut CatalogEngine,
    table: &Table,
    row: StoredRow,
) -> Result<()> {
    let mut pager = engine.pager.lock().unwrap();
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
        index_btree::insert_secondary_key(&mut pager, root_page_id, &key_values, row.row_id)?;
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
    let mut pager = engine.pager.lock().unwrap();
    index_btree::reset_tree(&mut pager, root_page_id)?;
    let mut seen = HashSet::new();
    for row in rows {
        let key_values = table.get_primary_key_values(&row.values)?;
        let encoded = index_btree::encode_index_key(&key_values)?;
        if !seen.insert(encoded) {
            return Err(HematiteError::StorageError(format!(
                "Duplicate primary key encountered while rebuilding table '{}'",
                table.name
            )));
        }
        index_btree::insert_primary_key(&mut pager, root_page_id, &key_values, row.row_id)?;
    }
    Ok(())
}

pub(crate) fn rebuild_secondary_indexes(
    engine: &mut CatalogEngine,
    table: &Table,
    rows: &[StoredRow],
) -> Result<()> {
    let mut pager = engine.pager.lock().unwrap();
    for index in &table.secondary_indexes {
        let root_page_id = require_index_root_page(
            index.root_page_id,
            &format!("secondary index '{}' on table '{}'", index.name, table.name),
        )?;
        index_btree::reset_tree(&mut pager, root_page_id)?;
        for row in rows {
            let key_values = index
                .column_indices
                .iter()
                .map(|&column_index| row.values[column_index].clone())
                .collect::<Vec<_>>();
            index_btree::insert_secondary_key(&mut pager, root_page_id, &key_values, row.row_id)?;
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
    let mut pager = engine.pager.lock().unwrap();
    index_btree::delete_primary_key(&mut pager, root_page_id, &key_values)
}

pub(crate) fn delete_secondary_index_row(
    engine: &mut CatalogEngine,
    table: &Table,
    row: &StoredRow,
) -> Result<()> {
    let mut pager = engine.pager.lock().unwrap();
    for index in &table.secondary_indexes {
        let key_values = index
            .column_indices
            .iter()
            .map(|&column_index| row.values[column_index].clone())
            .collect::<Vec<_>>();
        index_btree::delete_secondary_key(&mut pager, index.root_page_id, &key_values, row.row_id)?;
    }
    Ok(())
}

pub(crate) fn encode_primary_key(key_values: &[Value]) -> Result<Vec<u8>> {
    index_btree::encode_index_key(key_values)
}

pub(crate) fn encode_secondary_index_key(key_values: &[Value]) -> Result<Vec<u8>> {
    index_btree::encode_index_key(key_values)
}

pub(crate) fn open_primary_key_cursor(
    engine: &mut CatalogEngine,
    table: &Table,
) -> Result<IndexCursor> {
    let root_page_id = require_index_root_page(
        table.primary_key_index_root_page_id,
        &format!("primary-key index for table '{}'", table.name),
    )?;
    let entries = {
        let mut pager = engine.pager.lock().unwrap();
        index_btree::read_primary_entries(&mut pager, root_page_id)?
    };
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
    let entries = {
        let mut pager = engine.pager.lock().unwrap();
        index_btree::read_secondary_entries(&mut pager, root_page_id)?
    };
    Ok(IndexCursor::new(entries))
}

pub(crate) fn validate_table_indexes(engine: &mut CatalogEngine, table: &Table) -> Result<()> {
    let rows = table_store::read_rows_with_ids(engine, &table.name)?;
    for row in &rows {
        let key_values = table.get_primary_key_values(&row.values)?;
        let stored_rowid = {
            let mut pager = engine.pager.lock().unwrap();
            index_btree::lookup_primary_key(
                &mut pager,
                table.primary_key_index_root_page_id,
                &key_values,
            )?
        }
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
            let rowids = {
                let mut pager = engine.pager.lock().unwrap();
                index_btree::lookup_secondary_rowids(&mut pager, index.root_page_id, &key_values)?
            };
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
