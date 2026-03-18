//! High-level database interface

use crate::error::Result;
use crate::storage::StorageEngine;
use std::path::Path;

/// High-level database interface
pub struct Database {
    storage: StorageEngine,
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let storage = StorageEngine::new(path)?;
        Ok(Self { storage })
    }

    pub fn close(&mut self) -> Result<()> {
        self.storage.flush()?;
        Ok(())
    }

    pub fn storage(&mut self) -> &mut StorageEngine {
        &mut self.storage
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::TestDbFile;

    #[test]
    fn test_database_creation_and_close() -> Result<()> {
        let test_db = TestDbFile::new("_test_database");

        {
            let mut db = Database::open(test_db.path())?;
            // Database is created successfully
            db.close()?;
        }

        Ok(())
    }

    #[test]
    fn test_database_storage_access() -> Result<()> {
        let test_db = TestDbFile::new("_test_database_storage");

        let mut db = Database::open(test_db.path())?;
        
        // Test storage access
        let storage = db.storage();
        assert_eq!(storage.get_table_metadata().len(), 0);
        Ok(())
    }
}
