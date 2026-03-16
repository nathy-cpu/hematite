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
    use std::fs;

    #[test]
    fn test_database_creation_and_close() -> Result<()> {
        let test_path = "_test_database.db";
        let _ = fs::remove_file(test_path);

        {
            let mut db = Database::open(test_path)?;
            // Database is created successfully
            db.close()?;
        }

        // Clean up
        fs::remove_file(test_path)?;
        Ok(())
    }

    #[test]
    fn test_database_storage_access() -> Result<()> {
        let test_path = "_test_database_storage.db";
        let _ = fs::remove_file(test_path);

        let mut db = Database::open(test_path)?;
        
        // Test storage access
        let storage = db.storage();
        assert_eq!(storage.get_table_metadata().len(), 0);

        // Clean up
        fs::remove_file(test_path)?;
        Ok(())
    }
}
