#![allow(dead_code)]

use crate::error::{HematiteError, Result};
use crate::storage::PAGE_SIZE;

pub(crate) const V3_PAGE_SIZE: u16 = 4096;
pub(crate) const DATABASE_HEADER_SIZE: usize = 100;
pub(crate) const DATABASE_HEADER_MAGIC: &[u8; 16] = b"Hematite format3";
pub(crate) const DATABASE_HEADER_CHECKSUM_OFFSET: usize = 92;
pub(crate) const DATABASE_HEADER_NEXT_TABLE_ID_OFFSET: usize = 96;
pub(crate) const MAX_EMBEDDED_PAYLOAD_FRACTION: u8 = 64;
pub(crate) const MIN_EMBEDDED_PAYLOAD_FRACTION: u8 = 32;
pub(crate) const LEAF_PAYLOAD_FRACTION: u8 = 32;
const BTREE_LEAF_HEADER_SIZE: usize = 8;
const BTREE_INTERIOR_HEADER_SIZE: usize = 12;
const OFFSET_PAGE_KIND: usize = 0;
const OFFSET_FIRST_FREEBLOCK: usize = 1;
const OFFSET_CELL_COUNT: usize = 3;
const OFFSET_CELL_CONTENT_START: usize = 5;
const OFFSET_FRAGMENTED_FREE_BYTES: usize = 7;
const OFFSET_RIGHTMOST_CHILD: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FormatGeneration {
    V3 = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum PageKind {
    InteriorIndex = 0x02,
    InteriorTable = 0x05,
    LeafIndex = 0x0A,
    LeafTable = 0x0D,
    Overflow = 0x20,
    FreelistTrunk = 0x30,
    FreelistLeaf = 0x31,
}

impl PageKind {
    pub(crate) fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            0x02 => Ok(Self::InteriorIndex),
            0x05 => Ok(Self::InteriorTable),
            0x0A => Ok(Self::LeafIndex),
            0x0D => Ok(Self::LeafTable),
            0x20 => Ok(Self::Overflow),
            0x30 => Ok(Self::FreelistTrunk),
            0x31 => Ok(Self::FreelistLeaf),
            _ => Err(HematiteError::StorageError(format!(
                "Unknown v3 page kind byte {byte:#04x}"
            ))),
        }
    }

    pub(crate) fn is_leaf(self) -> bool {
        matches!(self, Self::LeafIndex | Self::LeafTable)
    }

    pub(crate) fn is_interior(self) -> bool {
        matches!(self, Self::InteriorIndex | Self::InteriorTable)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DatabaseHeaderV3 {
    pub(crate) page_size: u16,
    pub(crate) format_write_version: u8,
    pub(crate) format_read_version: u8,
    pub(crate) reserved_space: u8,
    pub(crate) max_embedded_payload_fraction: u8,
    pub(crate) min_embedded_payload_fraction: u8,
    pub(crate) leaf_payload_fraction: u8,
    pub(crate) file_change_counter: u32,
    pub(crate) page_count: u32,
    pub(crate) first_freelist_trunk_page: u32,
    pub(crate) freelist_page_count: u32,
    pub(crate) schema_root_page: u32,
    pub(crate) schema_format_version: u32,
    pub(crate) default_cache_hint: u32,
    pub(crate) largest_root_page: u32,
    pub(crate) text_encoding: u32,
    pub(crate) user_version: u32,
    pub(crate) incremental_vacuum_flag: u32,
    pub(crate) application_id: u32,
    pub(crate) next_table_id: u32,
}

impl Default for DatabaseHeaderV3 {
    fn default() -> Self {
        Self {
            page_size: V3_PAGE_SIZE,
            format_write_version: FormatGeneration::V3 as u8,
            format_read_version: FormatGeneration::V3 as u8,
            reserved_space: 0,
            max_embedded_payload_fraction: MAX_EMBEDDED_PAYLOAD_FRACTION,
            min_embedded_payload_fraction: MIN_EMBEDDED_PAYLOAD_FRACTION,
            leaf_payload_fraction: LEAF_PAYLOAD_FRACTION,
            file_change_counter: 0,
            page_count: 1,
            first_freelist_trunk_page: 0,
            freelist_page_count: 0,
            schema_root_page: 1,
            schema_format_version: 1,
            default_cache_hint: 0,
            largest_root_page: 0,
            text_encoding: 1,
            user_version: 0,
            incremental_vacuum_flag: 0,
            application_id: 0,
            next_table_id: 1,
        }
    }
}

impl DatabaseHeaderV3 {
    pub(crate) fn checksum(&self) -> u32 {
        checksum_bytes(&self.encode_without_checksum())
    }

    pub(crate) fn encode(&self) -> [u8; DATABASE_HEADER_SIZE] {
        let mut bytes = [0u8; DATABASE_HEADER_SIZE];
        bytes[..16].copy_from_slice(DATABASE_HEADER_MAGIC);
        bytes[16..18].copy_from_slice(&self.page_size.to_be_bytes());
        bytes[18] = self.format_write_version;
        bytes[19] = self.format_read_version;
        bytes[20] = self.reserved_space;
        bytes[21] = self.max_embedded_payload_fraction;
        bytes[22] = self.min_embedded_payload_fraction;
        bytes[23] = self.leaf_payload_fraction;
        bytes[24..28].copy_from_slice(&self.file_change_counter.to_be_bytes());
        bytes[28..32].copy_from_slice(&self.page_count.to_be_bytes());
        bytes[32..36].copy_from_slice(&self.first_freelist_trunk_page.to_be_bytes());
        bytes[36..40].copy_from_slice(&self.freelist_page_count.to_be_bytes());
        bytes[40..44].copy_from_slice(&self.schema_root_page.to_be_bytes());
        bytes[44..48].copy_from_slice(&self.schema_format_version.to_be_bytes());
        bytes[48..52].copy_from_slice(&self.default_cache_hint.to_be_bytes());
        bytes[52..56].copy_from_slice(&self.largest_root_page.to_be_bytes());
        bytes[56..60].copy_from_slice(&self.text_encoding.to_be_bytes());
        bytes[60..64].copy_from_slice(&self.user_version.to_be_bytes());
        bytes[64..68].copy_from_slice(&self.incremental_vacuum_flag.to_be_bytes());
        bytes[68..72].copy_from_slice(&self.application_id.to_be_bytes());
        bytes[DATABASE_HEADER_CHECKSUM_OFFSET..DATABASE_HEADER_CHECKSUM_OFFSET + 4]
            .copy_from_slice(&self.checksum().to_be_bytes());
        bytes[DATABASE_HEADER_NEXT_TABLE_ID_OFFSET..DATABASE_HEADER_NEXT_TABLE_ID_OFFSET + 4]
            .copy_from_slice(&self.next_table_id.to_be_bytes());
        bytes
    }

    fn encode_without_checksum(&self) -> [u8; DATABASE_HEADER_CHECKSUM_OFFSET] {
        let encoded = self.encode_without_tail();
        let mut bytes = [0u8; DATABASE_HEADER_CHECKSUM_OFFSET];
        bytes.copy_from_slice(&encoded[..DATABASE_HEADER_CHECKSUM_OFFSET]);
        bytes
    }

    fn encode_without_tail(&self) -> [u8; DATABASE_HEADER_SIZE] {
        let mut bytes = [0u8; DATABASE_HEADER_SIZE];
        bytes[..16].copy_from_slice(DATABASE_HEADER_MAGIC);
        bytes[16..18].copy_from_slice(&self.page_size.to_be_bytes());
        bytes[18] = self.format_write_version;
        bytes[19] = self.format_read_version;
        bytes[20] = self.reserved_space;
        bytes[21] = self.max_embedded_payload_fraction;
        bytes[22] = self.min_embedded_payload_fraction;
        bytes[23] = self.leaf_payload_fraction;
        bytes[24..28].copy_from_slice(&self.file_change_counter.to_be_bytes());
        bytes[28..32].copy_from_slice(&self.page_count.to_be_bytes());
        bytes[32..36].copy_from_slice(&self.first_freelist_trunk_page.to_be_bytes());
        bytes[36..40].copy_from_slice(&self.freelist_page_count.to_be_bytes());
        bytes[40..44].copy_from_slice(&self.schema_root_page.to_be_bytes());
        bytes[44..48].copy_from_slice(&self.schema_format_version.to_be_bytes());
        bytes[48..52].copy_from_slice(&self.default_cache_hint.to_be_bytes());
        bytes[52..56].copy_from_slice(&self.largest_root_page.to_be_bytes());
        bytes[56..60].copy_from_slice(&self.text_encoding.to_be_bytes());
        bytes[60..64].copy_from_slice(&self.user_version.to_be_bytes());
        bytes[64..68].copy_from_slice(&self.incremental_vacuum_flag.to_be_bytes());
        bytes[68..72].copy_from_slice(&self.application_id.to_be_bytes());
        bytes[DATABASE_HEADER_NEXT_TABLE_ID_OFFSET..DATABASE_HEADER_NEXT_TABLE_ID_OFFSET + 4]
            .copy_from_slice(&self.next_table_id.to_be_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < DATABASE_HEADER_SIZE {
            return Err(HematiteError::StorageError(
                "v3 database header is truncated".to_string(),
            ));
        }
        if &bytes[..16] != DATABASE_HEADER_MAGIC {
            return Err(HematiteError::StorageError(
                "v3 database header magic mismatch".to_string(),
            ));
        }

        let header = Self {
            page_size: u16::from_be_bytes([bytes[16], bytes[17]]),
            format_write_version: bytes[18],
            format_read_version: bytes[19],
            reserved_space: bytes[20],
            max_embedded_payload_fraction: bytes[21],
            min_embedded_payload_fraction: bytes[22],
            leaf_payload_fraction: bytes[23],
            file_change_counter: read_u32_be(bytes, 24),
            page_count: read_u32_be(bytes, 28),
            first_freelist_trunk_page: read_u32_be(bytes, 32),
            freelist_page_count: read_u32_be(bytes, 36),
            schema_root_page: read_u32_be(bytes, 40),
            schema_format_version: read_u32_be(bytes, 44),
            default_cache_hint: read_u32_be(bytes, 48),
            largest_root_page: read_u32_be(bytes, 52),
            text_encoding: read_u32_be(bytes, 56),
            user_version: read_u32_be(bytes, 60),
            incremental_vacuum_flag: read_u32_be(bytes, 64),
            application_id: read_u32_be(bytes, 68),
            next_table_id: read_u32_be(bytes, DATABASE_HEADER_NEXT_TABLE_ID_OFFSET),
        };

        if header.page_size as usize != PAGE_SIZE {
            return Err(HematiteError::StorageError(format!(
                "Unsupported v3 page size {}",
                header.page_size
            )));
        }
        if header.format_write_version != FormatGeneration::V3 as u8
            || header.format_read_version != FormatGeneration::V3 as u8
        {
            return Err(HematiteError::StorageError(format!(
                "Unsupported v3 format versions write={} read={}",
                header.format_write_version, header.format_read_version
            )));
        }

        let expected_checksum = read_u32_be(bytes, DATABASE_HEADER_CHECKSUM_OFFSET);
        if header.checksum() != expected_checksum {
            return Err(HematiteError::StorageError(
                "v3 database header checksum mismatch".to_string(),
            ));
        }

        Ok(header)
    }
}

pub(crate) fn detect_format_generation(bytes: &[u8]) -> Option<FormatGeneration> {
    if bytes.len() < DATABASE_HEADER_SIZE {
        return None;
    }
    if &bytes[..16] != DATABASE_HEADER_MAGIC {
        return None;
    }

    match (bytes[18], bytes[19]) {
        (3, 3) => Some(FormatGeneration::V3),
        _ => None,
    }
}

pub(crate) fn bootstrap_database_page_one(
    header: &DatabaseHeaderV3,
    root_page_kind: PageKind,
) -> Result<[u8; PAGE_SIZE]> {
    if !matches!(root_page_kind, PageKind::LeafTable | PageKind::InteriorTable) {
        return Err(HematiteError::StorageError(format!(
            "Unsupported page-one root kind {:?}",
            root_page_kind
        )));
    }

    let mut bytes = [0u8; PAGE_SIZE];
    bytes[..DATABASE_HEADER_SIZE].copy_from_slice(&header.encode());
    initialize_btree_page_header(
        &mut bytes,
        DATABASE_HEADER_SIZE,
        root_page_kind,
        PAGE_SIZE as u16,
    )?;
    Ok(bytes)
}

pub(crate) fn usable_space(page_size: usize, reserved_space: usize) -> usize {
    page_size.saturating_sub(reserved_space)
}

pub(crate) fn table_max_leaf_payload(usable_size: usize) -> usize {
    usable_size.saturating_sub(35)
}

pub(crate) fn min_local_payload(usable_size: usize) -> usize {
    ((usable_size.saturating_sub(12)) * 32 / 255).saturating_sub(23)
}

pub(crate) fn max_local_payload(usable_size: usize) -> usize {
    ((usable_size.saturating_sub(12)) * 64 / 255).saturating_sub(23)
}

pub(crate) fn choose_local_payload_size(usable_size: usize, payload_size: usize) -> usize {
    let max_local = max_local_payload(usable_size);
    if payload_size <= max_local {
        return payload_size;
    }

    let min_local = min_local_payload(usable_size);
    let overflow_usable = usable_size.saturating_sub(4);
    let mut local = min_local + (payload_size - min_local) % overflow_usable;
    if local > max_local {
        local = min_local;
    }
    local
}

pub(crate) fn encode_varint(mut value: u64) -> Vec<u8> {
    let mut buf = [0u8; 9];
    let mut i = 8;
    buf[i] = (value & 0x7f) as u8;
    while {
        value >>= 7;
        value != 0
    } {
        i -= 1;
        buf[i] = ((value & 0x7f) as u8) | 0x80;
    }
    buf[i..].to_vec()
}

pub(crate) fn decode_varint(bytes: &[u8]) -> Result<(u64, usize)> {
    let mut value = 0u64;
    for (index, byte) in bytes.iter().copied().enumerate().take(9) {
        if index == 8 {
            value = (value << 8) | u64::from(byte);
            return Ok((value, 9));
        }
        value = (value << 7) | u64::from(byte & 0x7f);
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
    }
    Err(HematiteError::StorageError(
        "Varint is truncated".to_string(),
    ))
}

fn read_u32_be(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn initialize_btree_page_header(
    bytes: &mut [u8],
    offset: usize,
    kind: PageKind,
    cell_content_start: u16,
) -> Result<()> {
    let header_size = match kind {
        PageKind::LeafTable | PageKind::LeafIndex => BTREE_LEAF_HEADER_SIZE,
        PageKind::InteriorTable | PageKind::InteriorIndex => BTREE_INTERIOR_HEADER_SIZE,
        _ => {
            return Err(HematiteError::StorageError(format!(
                "Page kind {:?} is not a b-tree page",
                kind
            )));
        }
    };

    if offset + header_size > bytes.len() {
        return Err(HematiteError::StorageError(
            "v3 b-tree page header exceeds page bounds".to_string(),
        ));
    }

    bytes[offset + OFFSET_PAGE_KIND] = kind as u8;
    write_u16_be(bytes, offset + OFFSET_FIRST_FREEBLOCK, 0);
    write_u16_be(bytes, offset + OFFSET_CELL_COUNT, 0);
    write_u16_be(bytes, offset + OFFSET_CELL_CONTENT_START, cell_content_start);
    bytes[offset + OFFSET_FRAGMENTED_FREE_BYTES] = 0;
    if matches!(kind, PageKind::InteriorTable | PageKind::InteriorIndex) {
        write_u32_be(bytes, offset + OFFSET_RIGHTMOST_CHILD, 0);
    }
    Ok(())
}

fn write_u16_be(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn write_u32_be(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn checksum_bytes(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 0x811C9DC5;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::{
        bootstrap_database_page_one, choose_local_payload_size, decode_varint,
        detect_format_generation, encode_varint, max_local_payload, min_local_payload,
        usable_space, DatabaseHeaderV3, FormatGeneration, PageKind, DATABASE_HEADER_SIZE,
    };

    #[test]
    fn v3_database_header_roundtrip() {
        let header = DatabaseHeaderV3 {
            page_count: 17,
            schema_root_page: 5,
            next_table_id: 42,
            user_version: 99,
            ..DatabaseHeaderV3::default()
        };

        let encoded = header.encode();
        assert_eq!(encoded.len(), DATABASE_HEADER_SIZE);
        assert_eq!(DatabaseHeaderV3::decode(&encoded).unwrap(), header);
    }

    #[test]
    fn v3_database_header_rejects_corrupted_checksum() {
        let header = DatabaseHeaderV3::default();
        let mut encoded = header.encode();
        encoded[24] ^= 0xFF;

        let err = DatabaseHeaderV3::decode(&encoded).unwrap_err();
        assert!(err.to_string().contains("checksum mismatch"));
    }

    #[test]
    fn format_detection_recognizes_v3_header() {
        let header = DatabaseHeaderV3::default();
        let encoded = header.encode();
        assert_eq!(
            detect_format_generation(&encoded),
            Some(FormatGeneration::V3)
        );
    }

    #[test]
    fn page_kind_roundtrip() {
        for kind in [
            PageKind::InteriorIndex,
            PageKind::InteriorTable,
            PageKind::LeafIndex,
            PageKind::LeafTable,
            PageKind::Overflow,
            PageKind::FreelistTrunk,
            PageKind::FreelistLeaf,
        ] {
            assert_eq!(PageKind::from_byte(kind as u8).unwrap(), kind);
        }
    }

    #[test]
    fn sqlite_style_local_payload_bounds_hold() {
        let usable = usable_space(4096, 0);
        let min_local = min_local_payload(usable);
        let max_local = max_local_payload(usable);
        assert!(min_local < max_local);

        let local = choose_local_payload_size(usable, 10_000);
        assert!(local >= min_local);
        assert!(local <= max_local);
    }

    #[test]
    fn varint_roundtrip_examples() {
        for value in [0, 1, 127, 128, 255, 16_384, u32::MAX as u64, u64::from(u32::MAX) + 1] {
            let encoded = encode_varint(value);
            let (decoded, used) = decode_varint(&encoded).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(used, encoded.len());
        }
    }

    #[test]
    fn bootstrap_page_one_writes_header_and_root_page_header() {
        let header = DatabaseHeaderV3::default();
        let page = bootstrap_database_page_one(&header, PageKind::LeafTable).unwrap();

        assert_eq!(&page[..DATABASE_HEADER_SIZE], &header.encode());
        assert_eq!(page[100], PageKind::LeafTable as u8);
        assert_eq!(u16::from_be_bytes([page[105], page[106]]), 4096);
    }
}
