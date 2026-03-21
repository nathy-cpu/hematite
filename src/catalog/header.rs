//! Database header management for Hematite database.
//!
//! M0 storage contract notes:
//! - The database header is always stored at page 0.
//! - Header versioning is strict: older on-disk versions are rejected.
//! - Header checksum covers all semantic header fields to detect corruption.

use super::ids::TableId;
use crate::error::Result;
use crate::storage::Page;
use crate::storage::PageId;

/// Database header structure stored on page 0.
#[derive(Debug, Clone)]
pub struct DatabaseHeader {
    /// Magic bytes to identify Hematite database files
    pub magic: [u8; 4],
    /// Database format version
    pub version: u32,
    /// Root page of the schema B-tree
    pub schema_root_page: PageId,
    /// Next available table ID
    pub next_table_id: u32,
    /// Header checksum for integrity verification
    pub checksum: u32,
}

impl DatabaseHeader {
    /// Magic bytes for Hematite database files
    pub const MAGIC: [u8; 4] = *b"HMTD";
    /// Current database format version.
    ///
    /// Version 2 is the first version after the M0 storage reset that intentionally
    /// breaks compatibility with previous files.
    pub const CURRENT_VERSION: u32 = 2;
    /// Fixed page ID for database header (consistent with existing implementation)
    pub const HEADER_PAGE_ID: PageId = PageId::new(0);

    /// Create a new database header with default values
    pub fn new(schema_root_page: PageId) -> Self {
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

    /// Calculate checksum for header integrity
    pub fn calculate_checksum(&self) -> u32 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        self.magic.hash(&mut hasher);
        self.version.hash(&mut hasher);
        self.schema_root_page.hash(&mut hasher);
        self.next_table_id.hash(&mut hasher);
        hasher.finish() as u32
    }

    /// Verify header integrity
    pub fn verify_checksum(&self) -> bool {
        self.checksum == self.calculate_checksum()
    }

    /// Serialize header to page data
    pub fn serialize(&self, page: &mut Page) -> Result<()> {
        let offset = 0;

        // Write magic bytes
        page.data[offset..offset + 4].copy_from_slice(&self.magic);

        // Write version
        page.data[offset + 4..offset + 8].copy_from_slice(&self.version.to_le_bytes());

        // Write schema root page ID
        page.data[offset + 8..offset + 12]
            .copy_from_slice(&self.schema_root_page.as_u32().to_le_bytes());

        // Write next table ID
        page.data[offset + 12..offset + 16].copy_from_slice(&self.next_table_id.to_le_bytes());

        // Write checksum
        page.data[offset + 16..offset + 20].copy_from_slice(&self.checksum.to_le_bytes());

        // Zero out the rest of the header page
        for byte in page.data.iter_mut().skip(20) {
            *byte = 0;
        }

        Ok(())
    }

    /// Deserialize header from page data
    pub fn deserialize(page: &Page) -> Result<Self> {
        let offset = 0;

        // Read magic bytes
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&page.data[offset..offset + 4]);

        // Verify magic bytes
        if magic != Self::MAGIC {
            return Err(crate::error::HematiteError::StorageError(
                "Invalid database file: wrong magic bytes".to_string(),
            ));
        }

        // Read version
        let version = u32::from_le_bytes([
            page.data[offset + 4],
            page.data[offset + 5],
            page.data[offset + 6],
            page.data[offset + 7],
        ]);
        if version != Self::CURRENT_VERSION {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Unsupported database header version: expected {}, got {}",
                Self::CURRENT_VERSION,
                version
            )));
        }

        // Read schema root page ID
        let schema_root_page = PageId::new(u32::from_le_bytes([
            page.data[offset + 8],
            page.data[offset + 9],
            page.data[offset + 10],
            page.data[offset + 11],
        ]));

        // Read next table ID
        let next_table_id = u32::from_le_bytes([
            page.data[offset + 12],
            page.data[offset + 13],
            page.data[offset + 14],
            page.data[offset + 15],
        ]);

        // Read checksum
        let checksum = u32::from_le_bytes([
            page.data[offset + 16],
            page.data[offset + 17],
            page.data[offset + 18],
            page.data[offset + 19],
        ]);

        let header = Self {
            magic,
            version,
            schema_root_page,
            next_table_id,
            checksum,
        };

        // Verify checksum
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
