//! In-memory table runtime metadata helpers.

use crate::error::{HematiteError, Result};

use super::engine::{CatalogEngine, TableRuntimeMetadata};
use super::record::StoredRow;

pub(crate) fn create_table_metadata(
    engine: &mut CatalogEngine,
    table_name: &str,
    root_page_id: u32,
) -> Result<()> {
    if engine.table_metadata.contains_key(table_name) {
        return Err(HematiteError::StorageError(format!(
            "Table '{}' already exists",
            table_name
        )));
    }

    engine.table_metadata.insert(
        table_name.to_string(),
        TableRuntimeMetadata {
            name: table_name.to_string(),
            root_page_id,
            row_count: 0,
            next_row_id: 1,
        },
    );
    Ok(())
}

pub(crate) fn lookup_table_metadata<'a>(
    engine: &'a CatalogEngine,
    table_name: &str,
) -> Result<&'a TableRuntimeMetadata> {
    engine.table_metadata.get(table_name).ok_or_else(|| {
        HematiteError::StorageError(format!("Table '{}' does not exist", table_name))
    })
}

pub(crate) fn remove_table_metadata(
    engine: &mut CatalogEngine,
    table_name: &str,
) -> Result<TableRuntimeMetadata> {
    engine.table_metadata.remove(table_name).ok_or_else(|| {
        HematiteError::StorageError(format!("Table '{}' does not exist", table_name))
    })
}

pub(crate) fn rename_table_metadata(
    engine: &mut CatalogEngine,
    old_name: &str,
    new_name: &str,
) -> Result<()> {
    if engine.table_metadata.contains_key(new_name) {
        return Err(HematiteError::StorageError(format!(
            "Table '{}' already exists",
            new_name
        )));
    }

    let mut metadata = remove_table_metadata(engine, old_name)?;
    metadata.name = new_name.to_string();
    engine.table_metadata.insert(new_name.to_string(), metadata);
    Ok(())
}

pub(crate) fn apply_insert(
    engine: &mut CatalogEngine,
    table_name: &str,
    new_root_page_id: u32,
    next_row_id: Option<u64>,
) {
    if let Some(metadata) = engine.table_metadata.get_mut(table_name) {
        metadata.root_page_id = new_root_page_id;
        metadata.row_count += 1;
        if let Some(next_row_id) = next_row_id {
            metadata.next_row_id = next_row_id;
        }
    }
}

pub(crate) fn apply_delete(
    engine: &mut CatalogEngine,
    table_name: &str,
    new_root_page_id: u32,
    deleted: bool,
) {
    if let Some(metadata) = engine.table_metadata.get_mut(table_name) {
        metadata.root_page_id = new_root_page_id;
        if deleted {
            metadata.row_count = metadata.row_count.saturating_sub(1);
        }
    }
}

pub(crate) fn prepare_replace(engine: &mut CatalogEngine, table_name: &str, rows: &[StoredRow]) {
    let preserved_next_row_id = engine
        .table_metadata
        .get(table_name)
        .map(|metadata| metadata.next_row_id)
        .unwrap_or(1);
    let next_row_id =
        preserved_next_row_id.max(rows.iter().map(|row| row.row_id).max().unwrap_or(0) + 1);

    if let Some(metadata) = engine.table_metadata.get_mut(table_name) {
        metadata.row_count = 0;
        metadata.next_row_id = next_row_id;
    }
}
