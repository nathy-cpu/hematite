//! Catalog integrity validation over table metadata and durable indexes.

use std::collections::HashSet;

use crate::catalog::Table;
use crate::error::{HematiteError, Result};

use super::engine::{CatalogEngine, CatalogIntegrityReport};
use super::index_store;
use super::serialization::RowCodec;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CatalogTreeUsage {
    pub live_table_pages: usize,
    pub live_index_pages: usize,
}

pub(crate) fn validate_integrity(engine: &mut CatalogEngine) -> Result<CatalogIntegrityReport> {
    let pager_report = engine.pager_integrity_report()?;
    let metadata_entries = engine
        .table_metadata
        .iter()
        .map(|(name, metadata)| (name.clone(), metadata.clone()))
        .collect::<Vec<_>>();

    let free_pages = engine.free_page_ids()?.into_iter().collect::<HashSet<_>>();

    let mut live_pages = HashSet::new();
    let mut overflow_pages = HashSet::new();
    let mut total_rows = 0u64;

    for (table_name, metadata) in metadata_entries {
        if metadata.root_page_id == CatalogEngine::INVALID_PAGE_ID
            || CatalogEngine::is_reserved_page(metadata.root_page_id)
        {
            return Err(HematiteError::CorruptedData(format!(
                "Table '{}' has invalid root page {}",
                table_name, metadata.root_page_id
            )));
        }

        let space_stats = engine.collect_tree_space_stats(metadata.root_page_id)?;
        let table_pages = space_stats.page_ids;
        let mut counted_rows = 0u64;
        let mut max_row_id = 0u64;
        let mut previous_row_id = None;
        for (key, value) in engine.read_tree_entries(metadata.root_page_id)? {
            if key.len() != 8 {
                return Err(HematiteError::CorruptedData(format!(
                    "Table '{}' contains a rowid key with invalid length {}",
                    table_name,
                    key.len()
                )));
            }

            let mut row_id_bytes = [0u8; 8];
            row_id_bytes.copy_from_slice(&key);
            let row_id = u64::from_be_bytes(row_id_bytes);
            if let Some(last_row_id) = previous_row_id {
                if row_id <= last_row_id {
                    return Err(HematiteError::CorruptedData(format!(
                        "Cursor-visible rowid order violation in table '{}': {} followed by {}",
                        table_name, last_row_id, row_id
                    )));
                }
            }
            let row = RowCodec::decode_stored_row(&value)?;
            if row.row_id != row_id {
                return Err(HematiteError::CorruptedData(format!(
                    "Stored rowid mismatch in table '{}': key={}, row={}",
                    table_name, row_id, row.row_id
                )));
            }

            previous_row_id = Some(row_id);
            counted_rows += 1;
            max_row_id = max_row_id.max(row_id);
        }

        for page_id in table_pages {
            if free_pages.contains(&page_id) {
                return Err(HematiteError::CorruptedData(format!(
                    "Page {} for table '{}' is both live and free",
                    page_id, table_name
                )));
            }
            if !live_pages.insert(page_id) {
                return Err(HematiteError::CorruptedData(format!(
                    "Page {} is shared by multiple tables",
                    page_id
                )));
            }
        }
        for overflow_page_id in space_stats.overflow_page_ids {
            if free_pages.contains(&overflow_page_id) {
                return Err(HematiteError::CorruptedData(format!(
                    "Overflow page {} for table '{}' is both live and free",
                    overflow_page_id, table_name
                )));
            }
            if live_pages.contains(&overflow_page_id) {
                return Err(HematiteError::CorruptedData(format!(
                    "Overflow page {} for table '{}' overlaps B-tree storage",
                    overflow_page_id, table_name
                )));
            }
            if !overflow_pages.insert(overflow_page_id) {
                return Err(HematiteError::CorruptedData(format!(
                    "Overflow page {} is shared by multiple rows",
                    overflow_page_id
                )));
            }
        }

        if counted_rows != metadata.row_count {
            return Err(HematiteError::CorruptedData(format!(
                "Table '{}' row count mismatch: metadata={}, actual={}",
                table_name, metadata.row_count, counted_rows
            )));
        }

        if metadata.next_row_id <= max_row_id {
            return Err(HematiteError::CorruptedData(format!(
                "Table '{}' next_row_id {} is not ahead of max row_id {}",
                table_name, metadata.next_row_id, max_row_id
            )));
        }

        total_rows += counted_rows;
    }

    Ok(CatalogIntegrityReport {
        table_count: engine.table_metadata.len(),
        live_page_count: live_pages.len(),
        index_page_count: 0,
        overflow_page_count: overflow_pages.len(),
        free_page_count: pager_report.free_page_count,
        total_rows,
        pager: pager_report,
    })
}

pub(crate) fn validate_table_indexes(
    engine: &mut CatalogEngine,
    table: &crate::catalog::Table,
) -> Result<()> {
    index_store::validate_table_indexes(engine, table)
}

pub(crate) fn validate_catalog_layout(
    engine: &mut CatalogEngine,
    tables: &[Table],
) -> Result<CatalogTreeUsage> {
    let free_pages = engine.free_page_ids()?.into_iter().collect::<HashSet<_>>();
    let mut table_pages = HashSet::new();
    let mut index_pages = HashSet::new();
    let mut overflow_pages = HashSet::new();

    for table in tables {
        let table_space_stats = engine.collect_tree_space_stats(table.root_page_id)?;
        for page_id in table_space_stats.page_ids {
            if free_pages.contains(&page_id) {
                return Err(HematiteError::CorruptedData(format!(
                    "Table page {} for '{}' is also present in the freelist",
                    page_id, table.name
                )));
            }
            if !table_pages.insert(page_id) {
                return Err(HematiteError::CorruptedData(format!(
                    "Table page {} is shared across multiple table trees",
                    page_id
                )));
            }
        }
        for overflow_page_id in table_space_stats.overflow_page_ids {
            if free_pages.contains(&overflow_page_id) {
                return Err(HematiteError::CorruptedData(format!(
                    "Overflow page {} for '{}' is also present in the freelist",
                    overflow_page_id, table.name
                )));
            }
            if table_pages.contains(&overflow_page_id) {
                return Err(HematiteError::CorruptedData(format!(
                    "Overflow page {} for '{}' overlaps table storage",
                    overflow_page_id, table.name
                )));
            }
            if !overflow_pages.insert(overflow_page_id) {
                return Err(HematiteError::CorruptedData(format!(
                    "Overflow page {} is shared across multiple table values",
                    overflow_page_id
                )));
            }
        }

        if table.primary_key_index_root_page_id != 0 {
            let index_page_ids =
                engine.collect_tree_page_ids(table.primary_key_index_root_page_id)?;
            for page_id in index_page_ids {
                if free_pages.contains(&page_id) {
                    return Err(HematiteError::CorruptedData(format!(
                        "Primary-key index page {} for '{}' is also present in the freelist",
                        page_id, table.name
                    )));
                }
                if table_pages.contains(&page_id) {
                    return Err(HematiteError::CorruptedData(format!(
                        "Primary-key index page {} for '{}' overlaps table storage",
                        page_id, table.name
                    )));
                }
                if overflow_pages.contains(&page_id) {
                    return Err(HematiteError::CorruptedData(format!(
                        "Primary-key index page {} for '{}' overlaps overflow storage",
                        page_id, table.name
                    )));
                }
                if !index_pages.insert(page_id) {
                    return Err(HematiteError::CorruptedData(format!(
                        "Index page {} is shared across multiple index trees",
                        page_id
                    )));
                }
            }
        }

        for index in &table.secondary_indexes {
            if index.root_page_id == 0 {
                return Err(HematiteError::CorruptedData(format!(
                    "Secondary index '{}' on '{}' is missing a root page",
                    index.name, table.name
                )));
            }
            let index_page_ids = engine.collect_tree_page_ids(index.root_page_id)?;
            for page_id in index_page_ids {
                if free_pages.contains(&page_id) {
                    return Err(HematiteError::CorruptedData(format!(
                        "Secondary index page {} for '{}.{}' is also present in the freelist",
                        page_id, table.name, index.name
                    )));
                }
                if table_pages.contains(&page_id) {
                    return Err(HematiteError::CorruptedData(format!(
                        "Secondary index page {} for '{}.{}' overlaps table storage",
                        page_id, table.name, index.name
                    )));
                }
                if overflow_pages.contains(&page_id) {
                    return Err(HematiteError::CorruptedData(format!(
                        "Secondary index page {} for '{}.{}' overlaps overflow storage",
                        page_id, table.name, index.name
                    )));
                }
                if !index_pages.insert(page_id) {
                    return Err(HematiteError::CorruptedData(format!(
                        "Index page {} is shared across multiple index trees",
                        page_id
                    )));
                }
            }
        }
    }

    for table in tables {
        validate_table_indexes(engine, table)?;
    }

    Ok(CatalogTreeUsage {
        live_table_pages: table_pages.len(),
        live_index_pages: index_pages.len(),
    })
}
