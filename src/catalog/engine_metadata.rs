//! Catalog engine metadata persistence codec.

use crate::error::{HematiteError, Result};
use crate::storage::{Page, STORAGE_METADATA_PAGE_ID};

use super::engine::CatalogEngine;
use super::runtime_metadata;

pub(crate) fn load_table_metadata(engine: &mut CatalogEngine) -> Result<()> {
    let maybe_page = engine.lock_pager()?.read_page(STORAGE_METADATA_PAGE_ID);
    match maybe_page {
        Ok(page) => {
            if page.data.len() >= 4 {
                if page.data.len() >= 9 && &page.data[0..4] == b"BTRE" {
                    return Ok(());
                }
                if page.data.iter().all(|&b| b == 0) {
                    return Ok(());
                }
                let metadata_size =
                    u32::from_le_bytes([page.data[0], page.data[1], page.data[2], page.data[3]])
                        as usize;

                if metadata_size > 0 && metadata_size + 4 <= crate::storage::PAGE_SIZE {
                    let metadata_bytes = &page.data[4..4 + metadata_size];
                    let metadata_str =
                        String::from_utf8(metadata_bytes.to_vec()).map_err(|_| {
                            HematiteError::StorageError("Invalid metadata encoding".to_string())
                        })?;
                    parse_storage_metadata(engine, &metadata_str)?;
                }
            }
        }
        Err(_) => {}
    }

    Ok(())
}

pub(crate) fn save_table_metadata(engine: &mut CatalogEngine) -> Result<()> {
    let metadata_str = serialize_storage_metadata(engine)?;
    let metadata_bytes = metadata_str.as_bytes();

    if metadata_bytes.len() > crate::storage::PAGE_SIZE - 4 {
        return Err(HematiteError::StorageError(
            "Table metadata too large".to_string(),
        ));
    }

    let mut page = Page::new(STORAGE_METADATA_PAGE_ID);
    page.data[0..4].copy_from_slice(&(metadata_bytes.len() as u32).to_le_bytes());
    page.data[4..4 + metadata_bytes.len()].copy_from_slice(metadata_bytes);
    engine.lock_pager()?.write_page(page)?;
    Ok(())
}

fn serialize_storage_metadata(engine: &CatalogEngine) -> Result<String> {
    let mut lines = vec![
        format!("version={}", CatalogEngine::STORAGE_METADATA_VERSION),
        format!("table_count={}", engine.table_metadata.len()),
    ];

    let mut table_entries = engine.table_metadata.values().cloned().collect::<Vec<_>>();
    table_entries.sort_by(|left, right| left.name.cmp(&right.name));

    for table in table_entries {
        lines.push(format!(
            "table|{}|{}|{}|{}",
            table.name, table.root_page_id, table.row_count, table.next_row_id
        ));
    }

    Ok(lines.join("\n"))
}

fn parse_storage_metadata(engine: &mut CatalogEngine, metadata_str: &str) -> Result<()> {
    let mut lines = metadata_str.lines();
    let version_line = lines.next().ok_or_else(|| {
        HematiteError::StorageError("Missing storage metadata version".to_string())
    })?;
    let version = version_line
        .strip_prefix("version=")
        .ok_or_else(|| {
            HematiteError::StorageError("Storage metadata is missing version prefix".to_string())
        })?
        .parse::<u32>()
        .map_err(|_| HematiteError::StorageError("Invalid storage metadata version".to_string()))?;

    if version != CatalogEngine::STORAGE_METADATA_VERSION {
        return Err(HematiteError::StorageError(format!(
            "Unsupported storage metadata version: expected {}, got {}",
            CatalogEngine::STORAGE_METADATA_VERSION,
            version
        )));
    }

    for line in metadata_str.lines().skip(1) {
        if line.is_empty() || line.starts_with("table_count=") {
            continue;
        }
        if let Some(payload) = line.strip_prefix("table|") {
            let parts = payload.split('|').collect::<Vec<_>>();
            if parts.len() != 4 {
                return Err(HematiteError::StorageError(
                    "Invalid table metadata record".to_string(),
                ));
            }
            let name = parts[0];
            let root_page_id = parts[1].parse::<u32>().map_err(|_| {
                HematiteError::StorageError("Invalid table root page metadata".to_string())
            })?;
            let row_count = parts[2].parse::<u64>().map_err(|_| {
                HematiteError::StorageError("Invalid table row count metadata".to_string())
            })?;
            let next_row_id = parts[3].parse::<u64>().map_err(|_| {
                HematiteError::StorageError("Invalid table next_row_id metadata".to_string())
            })?;

            runtime_metadata::create_table_metadata(engine, name, root_page_id)?;
            if let Some(metadata) = engine.table_metadata.get_mut(name) {
                metadata.row_count = row_count;
                metadata.next_row_id = next_row_id;
            }
            continue;
        }
        return Err(HematiteError::StorageError(
            "Unknown storage metadata record".to_string(),
        ));
    }
    Ok(())
}
