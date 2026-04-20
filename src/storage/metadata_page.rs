use crate::error::{HematiteError, Result};
use crate::storage::PAGE_SIZE;

const METADATA_PAGE_MAGIC: &[u8; 4] = b"HMD1";
const METADATA_PAGE_VERSION: u32 = 1;
const HEADER_LEN: usize = 16;

pub(crate) fn read_pager_metadata(page: &[u8]) -> Result<Option<Vec<u8>>> {
    Ok(parse_sections(page)?.pager.map(|bytes| bytes.to_vec()))
}

pub(crate) fn read_catalog_metadata(page: &[u8]) -> Result<Option<Vec<u8>>> {
    Ok(parse_sections(page)?.catalog.map(|bytes| bytes.to_vec()))
}

pub(crate) fn write_pager_metadata(existing_page: &[u8], pager_metadata: &[u8]) -> Result<Vec<u8>> {
    let sections = parse_sections(existing_page)?;
    encode_sections(Some(pager_metadata), sections.catalog)
}

pub(crate) fn write_catalog_metadata(
    existing_page: &[u8],
    catalog_metadata: &[u8],
) -> Result<Vec<u8>> {
    let sections = parse_sections(existing_page)?;
    encode_sections(sections.pager, Some(catalog_metadata))
}

struct MetadataSections<'a> {
    pager: Option<&'a [u8]>,
    catalog: Option<&'a [u8]>,
}

fn parse_sections(page: &[u8]) -> Result<MetadataSections<'_>> {
    if page.len() != PAGE_SIZE {
        return Err(HematiteError::StorageError(format!(
            "Reserved metadata page must be exactly {} bytes",
            PAGE_SIZE
        )));
    }

    if page.iter().all(|&byte| byte == 0) {
        return Ok(MetadataSections {
            pager: None,
            catalog: None,
        });
    }

    if &page[0..4] == METADATA_PAGE_MAGIC {
        return parse_container_sections(page);
    }

    Err(HematiteError::StorageError(
        "Legacy reserved metadata layout is unsupported".to_string(),
    ))
}

fn parse_container_sections(page: &[u8]) -> Result<MetadataSections<'_>> {
    let version = u32::from_le_bytes(page[4..8].try_into().unwrap());
    if version != METADATA_PAGE_VERSION {
        return Err(HematiteError::StorageError(format!(
            "Unsupported reserved metadata page version: expected {}, got {}",
            METADATA_PAGE_VERSION, version
        )));
    }

    let pager_len = u32::from_le_bytes(page[8..12].try_into().unwrap()) as usize;
    let catalog_len = u32::from_le_bytes(page[12..16].try_into().unwrap()) as usize;
    let payload_len = pager_len.checked_add(catalog_len).ok_or_else(|| {
        HematiteError::StorageError("Reserved metadata page lengths overflow".to_string())
    })?;

    if HEADER_LEN + payload_len > PAGE_SIZE {
        return Err(HematiteError::StorageError(
            "Reserved metadata page payload exceeds page size".to_string(),
        ));
    }

    let pager = (pager_len > 0).then_some(&page[HEADER_LEN..HEADER_LEN + pager_len]);
    let catalog_start = HEADER_LEN + pager_len;
    let catalog = (catalog_len > 0).then_some(&page[catalog_start..catalog_start + catalog_len]);

    Ok(MetadataSections { pager, catalog })
}

fn encode_sections(pager: Option<&[u8]>, catalog: Option<&[u8]>) -> Result<Vec<u8>> {
    let pager_len = pager.map_or(0, <[u8]>::len);
    let catalog_len = catalog.map_or(0, <[u8]>::len);
    let payload_len = pager_len.checked_add(catalog_len).ok_or_else(|| {
        HematiteError::StorageError("Reserved metadata page lengths overflow".to_string())
    })?;

    if HEADER_LEN + payload_len > PAGE_SIZE {
        return Err(HematiteError::StorageError(
            "Reserved metadata page payload exceeds page size".to_string(),
        ));
    }

    let mut page = vec![0; PAGE_SIZE];
    page[0..4].copy_from_slice(METADATA_PAGE_MAGIC);
    page[4..8].copy_from_slice(&METADATA_PAGE_VERSION.to_le_bytes());
    page[8..12].copy_from_slice(&(pager_len as u32).to_le_bytes());
    page[12..16].copy_from_slice(&(catalog_len as u32).to_le_bytes());

    let mut offset = HEADER_LEN;
    if let Some(bytes) = pager {
        page[offset..offset + bytes.len()].copy_from_slice(bytes);
        offset += bytes.len();
    }
    if let Some(bytes) = catalog {
        page[offset..offset + bytes.len()].copy_from_slice(bytes);
    }

    Ok(page)
}
