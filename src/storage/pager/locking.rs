use super::{JournalMode, Pager, PagerLockMode, PagerState};
use crate::error::Result;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, OnceLock};

#[derive(Debug, Clone, Default)]
pub(super) struct LockRegistryEntry {
    pub(super) readers: usize,
    pub(super) writer: bool,
    pub(super) wal_reader_sequences: HashMap<u64, usize>,
}

fn lock_registry() -> &'static Mutex<HashMap<PathBuf, LockRegistryEntry>> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, LockRegistryEntry>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn checksum_persist_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

impl Pager {
    pub(super) fn enter_reader_scope(&mut self) -> Result<PagerLockMode> {
        let previous_lock_mode = self.lock_mode;
        self.acquire_shared_lock()?;
        Ok(previous_lock_mode)
    }

    pub(super) fn leave_reader_scope(&mut self) -> Result<PagerLockMode> {
        self.release_shared_lock()?;
        Ok(self.lock_mode)
    }

    pub(super) fn enter_writer_scope(&mut self) -> Result<()> {
        self.acquire_write_lock()
    }

    pub(super) fn leave_writer_scope(&mut self) -> Result<PagerLockMode> {
        self.release_write_lock()?;
        Ok(self.lock_mode)
    }

    pub(super) fn exit_writer_scope_to_open(&mut self) -> Result<()> {
        let resulting_lock_mode = self.leave_writer_scope()?;
        debug_assert!(matches!(resulting_lock_mode, PagerLockMode::None));
        self.transition_state(PagerState::Open)
    }

    pub(super) fn lock_registry_map(
        &self,
    ) -> Result<MutexGuard<'static, HashMap<PathBuf, LockRegistryEntry>>> {
        lock_registry().lock().map_err(|_| {
            crate::error::HematiteError::InternalError(
                "Pager lock registry mutex is poisoned".to_string(),
            )
        })
    }

    pub(super) fn database_identity_path(&self) -> Result<&PathBuf> {
        self.database_identity.as_ref().ok_or_else(|| {
            crate::error::HematiteError::InternalError(
                "Pager database identity is not available".to_string(),
            )
        })
    }

    pub(super) fn has_live_writer(&self) -> Result<bool> {
        let Some(path) = self.database_identity.as_ref() else {
            return Ok(false);
        };
        let registry = self.lock_registry_map()?;
        Ok(registry.get(path).map(|entry| entry.writer).unwrap_or(false))
    }

    pub(super) fn acquire_shared_lock(&mut self) -> Result<()> {
        match self.lock_mode {
            PagerLockMode::Write if self.journal_mode == JournalMode::Wal => return Ok(()),
            PagerLockMode::Write => return Ok(()),
            PagerLockMode::Shared { depth } => {
                self.lock_mode = PagerLockMode::Shared { depth: depth + 1 };
                return Ok(());
            }
            PagerLockMode::None => {}
        }

        if self.database_identity.is_none() {
            self.lock_mode = PagerLockMode::Shared { depth: 1 };
            return Ok(());
        }

        let path = self.database_identity_path()?.clone();
        let mut registry = self.lock_registry_map()?;
        let entry = registry.entry(path).or_default();
        if entry.writer && self.journal_mode == JournalMode::Rollback {
            return Err(crate::error::HematiteError::StorageError(
                "database is locked for writing".to_string(),
            ));
        }
        entry.readers += 1;
        self.lock_mode = PagerLockMode::Shared { depth: 1 };
        Ok(())
    }

    pub(super) fn release_shared_lock(&mut self) -> Result<()> {
        match self.lock_mode {
            PagerLockMode::Write | PagerLockMode::None => return Ok(()),
            PagerLockMode::Shared { depth } if depth > 1 => {
                self.lock_mode = PagerLockMode::Shared { depth: depth - 1 };
                return Ok(());
            }
            PagerLockMode::Shared { .. } => {}
        }

        let Some(path) = self.database_identity.as_ref() else {
            self.lock_mode = PagerLockMode::None;
            return Ok(());
        };

        let mut registry = self.lock_registry_map()?;
        if let Some(entry) = registry.get_mut(path) {
            entry.readers = entry.readers.saturating_sub(1);
            if entry.readers == 0 && !entry.writer {
                registry.remove(path);
            }
        }
        self.lock_mode = PagerLockMode::None;
        Ok(())
    }

    pub(super) fn register_wal_reader_sequence(&self, sequence: u64) -> Result<()> {
        let Some(path) = self.database_identity.as_ref() else {
            return Ok(());
        };
        let mut registry = self.lock_registry_map()?;
        let entry = registry.entry(path.clone()).or_default();
        *entry.wal_reader_sequences.entry(sequence).or_insert(0) += 1;
        Ok(())
    }

    pub(super) fn unregister_wal_reader_sequence(&self, sequence: u64) -> Result<()> {
        let Some(path) = self.database_identity.as_ref() else {
            return Ok(());
        };
        let mut registry = self.lock_registry_map()?;
        if let Some(entry) = registry.get_mut(path) {
            if let Some(count) = entry.wal_reader_sequences.get_mut(&sequence) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    entry.wal_reader_sequences.remove(&sequence);
                }
            }
            if entry.readers == 0 && !entry.writer && entry.wal_reader_sequences.is_empty() {
                registry.remove(path);
            }
        }
        Ok(())
    }

    pub(super) fn acquire_write_lock(&mut self) -> Result<()> {
        if self.database_identity.is_none() {
            self.lock_mode = PagerLockMode::Write;
            return Ok(());
        }
        if self.lock_mode == PagerLockMode::Write {
            return Ok(());
        }
        if matches!(self.lock_mode, PagerLockMode::Shared { .. }) {
            return Err(crate::error::HematiteError::StorageError(
                "cannot upgrade a shared database lock to a write lock".to_string(),
            ));
        }

        let path = self.database_identity_path()?.clone();
        let mut registry = self.lock_registry_map()?;
        let entry = registry.entry(path).or_default();
        if entry.writer || (self.journal_mode == JournalMode::Rollback && entry.readers > 0) {
            return Err(crate::error::HematiteError::StorageError(
                "database is locked".to_string(),
            ));
        }
        entry.writer = true;
        self.lock_mode = PagerLockMode::Write;
        Ok(())
    }

    pub(super) fn release_write_lock(&mut self) -> Result<()> {
        let Some(path) = self.database_identity.as_ref() else {
            self.lock_mode = PagerLockMode::None;
            return Ok(());
        };
        if self.lock_mode != PagerLockMode::Write {
            return Ok(());
        }

        let mut registry = self.lock_registry_map()?;
        if let Some(entry) = registry.get_mut(path) {
            entry.writer = false;
            if entry.readers == 0 {
                registry.remove(path);
            }
        }
        self.lock_mode = PagerLockMode::None;
        Ok(())
    }
}
