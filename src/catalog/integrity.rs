//! Catalog integrity validation over table metadata and durable indexes.

use std::collections::HashSet;

use crate::catalog::Table;
use crate::error::{HematiteError, Result};
use crate::storage::{DB_HEADER_PAGE_ID, INVALID_PAGE_ID, STORAGE_METADATA_PAGE_ID};

use super::engine::{CatalogEngine, CatalogIntegrityReport};
use super::{index_btree, index_store, table_btree};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CatalogTreeUsage {
    pub live_table_pages: usize,
    pub live_index_pages: usize,
}

pub(crate) fn validate_integrity(engine: &mut CatalogEngine) -> Result<CatalogIntegrityReport> {
    let pager_report = engine.pager.lock().unwrap().validate_integrity()?;
    let metadata_entries = engine
        .table_metadata
        .iter()
        .map(|(name, metadata)| (name.clone(), metadata.clone()))
        .collect::<Vec<_>>();

    let free_pages = engine
        .pager
        .lock()
        .unwrap()
        .free_pages()
        .iter()
        .copied()
        .collect::<HashSet<_>>();

    let mut live_pages = HashSet::new();
    let mut total_rows = 0u64;

    for (table_name, metadata) in metadata_entries {
        if metadata.root_page_id == INVALID_PAGE_ID
            || metadata.root_page_id == DB_HEADER_PAGE_ID
            || metadata.root_page_id == STORAGE_METADATA_PAGE_ID
        {
            return Err(HematiteError::CorruptedData(format!(
                "Table '{}' has invalid root page {}",
                table_name, metadata.root_page_id
            )));
        }

        let (table_pages, counted_rows, max_row_id) = {
            let mut pager = engine.pager.lock().unwrap();
            table_btree::validate_pages(&mut pager, &table_name, metadata.root_page_id)?
        };

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
        overflow_page_count: 0,
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
    let free_pages = engine
        .pager
        .lock()
        .unwrap()
        .free_pages()
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let mut table_pages = HashSet::new();
    let mut index_pages = HashSet::new();

    {
        let mut pager = engine.pager.lock().unwrap();
        for table in tables {
            let mut table_page_ids = Vec::new();
            table_btree::collect_page_ids(&mut pager, table.root_page_id, &mut table_page_ids)?;
            for page_id in table_page_ids {
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

            if table.primary_key_index_root_page_id != 0 {
                let mut index_page_ids = Vec::new();
                index_btree::collect_page_ids(
                    &mut pager,
                    table.primary_key_index_root_page_id,
                    &mut index_page_ids,
                )?;
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
                let mut index_page_ids = Vec::new();
                index_btree::collect_page_ids(&mut pager, index.root_page_id, &mut index_page_ids)?;
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
                    if !index_pages.insert(page_id) {
                        return Err(HematiteError::CorruptedData(format!(
                            "Index page {} is shared across multiple index trees",
                            page_id
                        )));
                    }
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
