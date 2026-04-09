use crate::error::{HematiteError, Result};
use crate::storage::pager::JournalMode;
use crate::storage::PageId;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PersistedPagerState {
    pub(crate) journal_mode: JournalMode,
    pub(crate) free_pages: Vec<PageId>,
    pub(crate) checksums: HashMap<PageId, u32>,
}

impl PersistedPagerState {
    pub(crate) fn encode(&self, version: u32) -> String {
        let mut entries = self
            .checksums
            .iter()
            .map(|(page_id, checksum)| (*page_id, *checksum))
            .collect::<Vec<_>>();
        entries.sort_by_key(|(page_id, _)| *page_id);

        let mut lines = vec![
            format!("version={version}"),
            format!("journal_mode={}", self.journal_mode.as_str()),
            format!("free_count={}", self.free_pages.len()),
        ];
        for page_id in &self.free_pages {
            lines.push(format!("free|{page_id}"));
        }
        lines.push(format!("checksum_count={}", entries.len()));
        for (page_id, checksum) in entries {
            lines.push(format!("checksum|{page_id}|{checksum}"));
        }
        lines.join("\n")
    }

    pub(crate) fn decode(contents: &str, expected_version: u32) -> Result<Self> {
        let mut lines = contents.lines();
        let version = lines
            .next()
            .ok_or_else(|| {
                HematiteError::StorageError("Missing pager checksum metadata version".to_string())
            })?
            .strip_prefix("version=")
            .ok_or_else(|| {
                HematiteError::StorageError(
                    "Pager checksum metadata is missing version prefix".to_string(),
                )
            })?
            .parse::<u32>()
            .map_err(|_| {
                HematiteError::StorageError("Invalid pager checksum metadata version".to_string())
            })?;

        if version != expected_version {
            return Err(HematiteError::StorageError(format!(
                "Unsupported pager checksum metadata version: expected {}, got {}",
                expected_version, version
            )));
        }

        let mut next_line = lines.next().ok_or_else(|| {
            HematiteError::StorageError("Missing pager freelist metadata count".to_string())
        })?;

        let journal_mode = if let Some(mode) = next_line.strip_prefix("journal_mode=") {
            let parsed = JournalMode::parse(mode)?;
            next_line = lines.next().ok_or_else(|| {
                HematiteError::StorageError("Missing pager freelist metadata count".to_string())
            })?;
            parsed
        } else {
            JournalMode::Rollback
        };

        let expected_free_count = next_line
            .strip_prefix("free_count=")
            .ok_or_else(|| {
                HematiteError::StorageError(
                    "Pager freelist metadata is missing count prefix".to_string(),
                )
            })?
            .parse::<usize>()
            .map_err(|_| {
                HematiteError::StorageError("Invalid pager freelist metadata count".to_string())
            })?;

        let mut free_pages = Vec::with_capacity(expected_free_count);
        for _ in 0..expected_free_count {
            let line = lines.next().ok_or_else(|| {
                HematiteError::StorageError("Pager freelist metadata ended early".to_string())
            })?;
            let page_id = line
                .strip_prefix("free|")
                .ok_or_else(|| {
                    HematiteError::StorageError(
                        "Invalid pager freelist metadata record".to_string(),
                    )
                })?
                .parse::<u32>()
                .map_err(|_| {
                    HematiteError::StorageError("Invalid pager freelist page id".to_string())
                })?;
            free_pages.push(page_id);
        }

        let expected_checksum_count = lines
            .next()
            .ok_or_else(|| {
                HematiteError::StorageError("Missing pager checksum metadata count".to_string())
            })?
            .strip_prefix("checksum_count=")
            .ok_or_else(|| {
                HematiteError::StorageError(
                    "Pager checksum metadata is missing count prefix".to_string(),
                )
            })?
            .parse::<usize>()
            .map_err(|_| {
                HematiteError::StorageError("Invalid pager checksum metadata count".to_string())
            })?;

        let mut checksums = HashMap::new();
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let payload = line.strip_prefix("checksum|").ok_or_else(|| {
                HematiteError::StorageError("Invalid pager checksum metadata record".to_string())
            })?;
            let parts = payload.split('|').collect::<Vec<_>>();
            if parts.len() != 2 {
                return Err(HematiteError::StorageError(
                    "Invalid pager checksum metadata record".to_string(),
                ));
            }
            let page_id = parts[0].parse::<u32>().map_err(|_| {
                HematiteError::StorageError("Invalid pager checksum page id".to_string())
            })?;
            let checksum = parts[1].parse::<u32>().map_err(|_| {
                HematiteError::StorageError("Invalid pager checksum value".to_string())
            })?;
            if checksums.insert(page_id, checksum).is_some() {
                return Err(HematiteError::StorageError(format!(
                    "Duplicate pager checksum entry for page {page_id}",
                )));
            }
        }

        if checksums.len() != expected_checksum_count {
            return Err(HematiteError::StorageError(format!(
                "Pager checksum metadata count mismatch: expected {}, got {}",
                expected_checksum_count,
                checksums.len()
            )));
        }

        Ok(Self {
            journal_mode,
            free_pages,
            checksums,
        })
    }
}
