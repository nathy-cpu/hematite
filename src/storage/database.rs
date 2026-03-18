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

