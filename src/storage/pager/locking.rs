use super::{JournalMode, Pager, PagerLockMode, PagerState};
use crate::error::{HematiteError, Result};
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::fd::AsRawFd;

#[derive(Debug)]
pub(crate) struct WalReaderRegistration {
    pub(super) path: PathBuf,
    pub(super) sequence: u64,
    pub(super) file: File,
}

fn next_wal_reader_registration_id() -> u64 {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
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

    pub(super) fn database_identity_path(&self) -> Result<&PathBuf> {
        self.database_identity.as_ref().ok_or_else(|| {
            HematiteError::InternalError("Pager database identity is not available".to_string())
        })
    }

    pub(super) fn has_live_writer(&self) -> Result<bool> {
        if self.database_identity.is_none() {
            return Ok(false);
        }

        match self.try_exclusive_probe(&self.rollback_lock_path()?) {
            Ok(Some(file)) => {
                unlock_file(&file)?;
                Ok(false)
            }
            Ok(None) => Ok(true),
            Err(err) => Err(err),
        }
    }

    pub(super) fn wal_writer_active(&self) -> Result<bool> {
        if self.database_identity.is_none() {
            return Ok(false);
        }

        match self.try_exclusive_probe(&self.wal_write_lock_path()?) {
            Ok(Some(file)) => {
                unlock_file(&file)?;
                Ok(false)
            }
            Ok(None) => Ok(true),
            Err(err) => Err(err),
        }
    }

    pub(super) fn active_wal_reader_sequences(&self) -> Result<Vec<u64>> {
        let Some(dir_path) = self.wal_readers_dir_path()? else {
            return Ok(Vec::new());
        };
        if !dir_path.exists() {
            return Ok(Vec::new());
        }

        let mut sequences = Vec::new();
        for entry in fs::read_dir(&dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if !entry.file_type()?.is_file() {
                continue;
            }
            let Some(sequence) = Self::parse_wal_reader_sequence_path(&path) else {
                continue;
            };

            match self.try_exclusive_probe(&path)? {
                Some(file) => {
                    unlock_file(&file)?;
                    let _ = fs::remove_file(&path);
                }
                None => sequences.push(sequence),
            }
        }
        sequences.sort_unstable();
        sequences.dedup();
        Ok(sequences)
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

        if self.journal_mode == JournalMode::Rollback {
            let lock_path = self.rollback_lock_path()?;
            let file = open_lock_file(&lock_path)?;
            
            let start = Instant::now();
            let mut backoff = Duration::from_millis(1);
            let timeout = Duration::from_secs(5);

            loop {
                match try_lock_shared(&file) {
                    Ok(()) => {
                        self.rollback_lock_file = Some(file);
                        break;
                    }
                    Err(err) if is_lock_busy(&err) => {
                        if start.elapsed() >= timeout {
                            return Err(HematiteError::StorageError(
                                "database is locked for writing".to_string(),
                            ));
                        }
                        thread::sleep(backoff);
                        backoff = (backoff * 2).min(Duration::from_millis(100));
                    }
                    Err(err) => return Err(err.into()),
                }
            }
        }

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

        if let Some(registration) = self.wal_reader_registration.take() {
            unlock_file(&registration.file)?;
            self.wal_read_snapshot = None;
        }

        if let Some(file) = self.rollback_lock_file.take() {
            unlock_file(&file)?;
        }

        self.lock_mode = PagerLockMode::None;
        Ok(())
    }

    pub(super) fn register_wal_reader_sequence(&mut self, sequence: u64) -> Result<()> {
        let Some(dir_path) = self.wal_readers_dir_path()? else {
            return Ok(());
        };

        fs::create_dir_all(&dir_path)?;
        let path = self.wal_reader_sequence_path(sequence, next_wal_reader_registration_id())?;
        let file = open_lock_file(&path)?;
        try_lock_shared(&file)?;
        self.wal_reader_registration = Some(WalReaderRegistration {
            path,
            sequence,
            file,
        });
        Ok(())
    }

    pub(super) fn unregister_wal_reader_sequence(&mut self, sequence: u64) -> Result<()> {
        let Some(registration) = self.wal_reader_registration.take() else {
            return Ok(());
        };

        if registration.sequence != sequence {
            self.wal_reader_registration = Some(registration);
            return Ok(());
        }

        unlock_file(&registration.file)?;
        self.wal_read_snapshot = None;
        let _ = fs::remove_file(&registration.path);

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
            return Err(HematiteError::StorageError(
                "cannot upgrade a shared database lock to a write lock".to_string(),
            ));
        }

        let (lock_path, lock_slot) = if self.journal_mode == JournalMode::Rollback {
            (self.rollback_lock_path()?, &mut self.rollback_lock_file)
        } else {
            (self.wal_write_lock_path()?, &mut self.wal_write_lock_file)
        };

        let file = open_lock_file(&lock_path)?;
        let start = Instant::now();
        let mut backoff = Duration::from_millis(1);
        let timeout = Duration::from_secs(5);

        loop {
            match try_lock_exclusive(&file) {
                Ok(()) => {
                    *lock_slot = Some(file);
                    self.lock_mode = PagerLockMode::Write;
                    return Ok(());
                }
                Err(err) if is_lock_busy(&err) => {
                    if start.elapsed() >= timeout {
                        return Err(HematiteError::StorageError(
                            "database is locked".to_string(),
                        ));
                    }
                    thread::sleep(backoff);
                    backoff = (backoff * 2).min(Duration::from_millis(100));
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    pub(super) fn release_write_lock(&mut self) -> Result<()> {
        if self.lock_mode != PagerLockMode::Write {
            return Ok(());
        }

        if let Some(file) = self.rollback_lock_file.take() {
            unlock_file(&file)?;
        }
        if let Some(file) = self.wal_write_lock_file.take() {
            unlock_file(&file)?;
        }

        self.lock_mode = PagerLockMode::None;
        Ok(())
    }

    fn rollback_lock_path(&self) -> Result<PathBuf> {
        Self::sidecar_path(self.database_identity_path()?, ".rollback.lock")
    }

    fn wal_write_lock_path(&self) -> Result<PathBuf> {
        Self::sidecar_path(self.database_identity_path()?, ".wal.write.lock")
    }

    fn wal_readers_dir_path(&self) -> Result<Option<PathBuf>> {
        let Some(path) = self.database_identity.as_ref() else {
            return Ok(None);
        };
        Ok(Some(Self::sidecar_path(path, ".wal.readers")?))
    }

    fn wal_reader_sequence_path(&self, sequence: u64, registration_id: u64) -> Result<PathBuf> {
        let dir = self.wal_readers_dir_path()?.ok_or_else(|| {
            HematiteError::InternalError("Pager database identity is not available".to_string())
        })?;
        Ok(dir.join(format!("reader-{}-seq-{}.lock", registration_id, sequence)))
    }

    fn parse_wal_reader_sequence_path(path: &Path) -> Option<u64> {
        let file_name = path.file_name()?.to_str()?;
        let sequence = file_name.rsplit_once("-seq-")?.1.strip_suffix(".lock")?;
        sequence.parse::<u64>().ok()
    }

    fn sidecar_path(db_path: &Path, suffix: &str) -> Result<PathBuf> {
        let mut file_name = db_path
            .file_name()
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("hematite.db"));
        file_name.push(suffix);
        Ok(match db_path.parent() {
            Some(parent) => parent.join(file_name),
            None => PathBuf::from(file_name),
        })
    }

    fn try_exclusive_probe(&self, path: &Path) -> Result<Option<File>> {
        let file = open_lock_file(path)?;
        match try_lock_exclusive(&file) {
            Ok(()) => Ok(Some(file)),
            Err(err) if is_lock_busy(&err) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}

fn open_lock_file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)
        .map_err(Into::into)
}

#[cfg(unix)]
fn try_lock_shared(file: &File) -> std::io::Result<()> {
    flock(file, LOCK_SH | LOCK_NB)
}

#[cfg(unix)]
fn try_lock_exclusive(file: &File) -> std::io::Result<()> {
    flock(file, LOCK_EX | LOCK_NB)
}

#[cfg(unix)]
fn unlock_file(file: &File) -> std::io::Result<()> {
    flock(file, LOCK_UN)
}

#[cfg(unix)]
fn flock(file: &File, operation: i32) -> std::io::Result<()> {
    let rc = unsafe { libc_flock(file.as_raw_fd(), operation) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(unix)]
fn is_lock_busy(err: &std::io::Error) -> bool {
    matches!(err.raw_os_error(), Some(11) | Some(35))
}

#[cfg(unix)]
const LOCK_SH: i32 = 1;
#[cfg(unix)]
const LOCK_EX: i32 = 2;
#[cfg(unix)]
const LOCK_NB: i32 = 4;
#[cfg(unix)]
const LOCK_UN: i32 = 8;

#[cfg(unix)]
unsafe extern "C" {
    #[link_name = "flock"]
    fn libc_flock(fd: i32, operation: i32) -> i32;
}
