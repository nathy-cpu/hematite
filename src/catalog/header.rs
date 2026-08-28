//! Database header management for Hematite database.
//!
//! Storage contract:
//! - The database header is always stored at reserved page 0.
//! - The version field is checked strictly: a file that does not carry the current
//!   version is refused at open rather than misread later.
//! - The header checksum covers all semantic header fields, so corruption is caught
//!   at open too.

use super::ids::TableId;
use crate::btree::checksum;
use crate::error::Result;

/// Database header structure stored on reserved page 0.
#[derive(Debug, Clone)]
pub struct DatabaseHeader {
    /// Magic bytes to identify Hematite database files
    pub magic: [u8; 4],
    /// Database format version
    pub version: u32,
    /// Root page of the schema B-tree
    pub schema_root_page: u32,
    /// Next available table ID
    pub next_table_id: u32,
    /// Header checksum for integrity verification
    pub checksum: u32,
}

impl DatabaseHeader {
    /// Magic bytes for Hematite database files
    pub const MAGIC: [u8; 4] = *b"HMTD";
    /// Current database format version.
    pub const CURRENT_VERSION: u32 = 1;
    /// Fixed reserved page ID for the database header.
    pub const HEADER_PAGE_ID: u32 = 0;

    /// Create a new database header with default values
    pub fn new(schema_root_page: u32) -> Self {
        let mut header = Self {
            magic: Self::MAGIC,
            version: Self::CURRENT_VERSION,
            schema_root_page,
            next_table_id: 1,
            checksum: 0,
        };
        header.checksum = header.calculate_checksum();
        header
    }

    /// Calculate checksum for header integrity.
    ///
    /// Hashes the semantic fields in their on-disk little-endian form — the first
    /// 16 bytes of the serialized header, everything up to the checksum itself.
    ///
    /// XXH64 rather than `DefaultHasher`, whose output is explicitly not guaranteed
    /// stable across Rust releases: a database written by one toolchain would fail
    /// header verification under another.
    pub fn calculate_checksum(&self) -> u32 {
        let mut fields = [0u8; 16];
        fields[0..4].copy_from_slice(&self.magic);
        fields[4..8].copy_from_slice(&self.version.to_le_bytes());
        fields[8..12].copy_from_slice(&self.schema_root_page.to_le_bytes());
        fields[12..16].copy_from_slice(&self.next_table_id.to_le_bytes());
        checksum::hash32(0, &fields)
    }

    /// Verify header integrity
    pub fn verify_checksum(&self) -> bool {
        self.checksum == self.calculate_checksum()
    }

    /// Serialize header to page data
    pub fn serialize(&self, bytes: &mut [u8]) -> Result<()> {
        let offset = 0;

        bytes[offset..offset + 4].copy_from_slice(&self.magic);
        bytes[offset + 4..offset + 8].copy_from_slice(&self.version.to_le_bytes());
        bytes[offset + 8..offset + 12].copy_from_slice(&self.schema_root_page.to_le_bytes());
        bytes[offset + 12..offset + 16].copy_from_slice(&self.next_table_id.to_le_bytes());
        bytes[offset + 16..offset + 20].copy_from_slice(&self.checksum.to_le_bytes());

        for byte in bytes.iter_mut().skip(20) {
            *byte = 0;
        }

        Ok(())
    }

    /// Deserialize header from page data
    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        let offset = 0;

        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[offset..offset + 4]);
        if magic != Self::MAGIC {
            return Err(crate::error::HematiteError::StorageError(
                "Invalid database file: wrong magic bytes".to_string(),
            ));
        }

        let version = u32::from_le_bytes([
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        if version != Self::CURRENT_VERSION {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Unsupported database header version: expected {}, got {}",
                Self::CURRENT_VERSION,
                version
            )));
        }

        let schema_root_page = u32::from_le_bytes([
            bytes[offset + 8],
            bytes[offset + 9],
            bytes[offset + 10],
            bytes[offset + 11],
        ]);

        let next_table_id = u32::from_le_bytes([
            bytes[offset + 12],
            bytes[offset + 13],
            bytes[offset + 14],
            bytes[offset + 15],
        ]);

        let checksum = u32::from_le_bytes([
            bytes[offset + 16],
            bytes[offset + 17],
            bytes[offset + 18],
            bytes[offset + 19],
        ]);

        let header = Self {
            magic,
            version,
            schema_root_page,
            next_table_id,
            checksum,
        };

        if !header.verify_checksum() {
            return Err(crate::error::HematiteError::StorageError(
                "Database header checksum verification failed".to_string(),
            ));
        }

        Ok(header)
    }

    /// Update next table ID and recalculate checksum
    pub fn increment_table_id(&mut self) -> TableId {
        let table_id = TableId::new(self.next_table_id);
        self.next_table_id += 1;
        self.checksum = self.calculate_checksum();
        table_id
    }
}
