use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static TEST_DB_UNIQUIFIER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct TestDbFile {
    path: PathBuf,
}

impl TestDbFile {
    pub fn new(prefix: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let uniquifier = TEST_DB_UNIQUIFIER.fetch_add(1, Ordering::Relaxed);
        let path = PathBuf::from(format!("{}_{}_{}.db", prefix, nanos, uniquifier));
        Self::cleanup_paths_for(&path);
        Self { path }
    }

    pub fn path(&self) -> &str {
        self.path.to_str().unwrap_or_default()
    }

    pub fn as_path(&self) -> &Path {
        &self.path
    }

    fn wal_path(&self) -> PathBuf {
        Self::wal_path_for(&self.path)
    }

    fn wal_path_for(path: &Path) -> PathBuf {
        let mut file_name = path
            .file_name()
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("hematite.db"));
        file_name.push(".wal");
        match path.parent() {
            Some(parent) => parent.join(file_name),
            None => PathBuf::from(file_name),
        }
    }

    fn journal_path(&self) -> PathBuf {
        Self::journal_path_for(&self.path)
    }

    fn rollback_lock_path(&self) -> PathBuf {
        Self::rollback_lock_path_for(&self.path)
    }

    fn rollback_lock_path_for(path: &Path) -> PathBuf {
        let mut file_name = path
            .file_name()
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("hematite.db"));
        file_name.push(".rollback.lock");
        match path.parent() {
            Some(parent) => parent.join(file_name),
            None => PathBuf::from(file_name),
        }
    }

    fn wal_write_lock_path(&self) -> PathBuf {
        Self::wal_write_lock_path_for(&self.path)
    }

    fn wal_write_lock_path_for(path: &Path) -> PathBuf {
        let mut file_name = path
            .file_name()
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("hematite.db"));
        file_name.push(".wal.write.lock");
        match path.parent() {
            Some(parent) => parent.join(file_name),
            None => PathBuf::from(file_name),
        }
    }

    fn wal_readers_dir(&self) -> PathBuf {
        Self::wal_readers_dir_for(&self.path)
    }

    fn wal_readers_dir_for(path: &Path) -> PathBuf {
        let mut file_name = path
            .file_name()
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("hematite.db"));
        file_name.push(".wal.readers");
        match path.parent() {
            Some(parent) => parent.join(file_name),
            None => PathBuf::from(file_name),
        }
    }

    fn journal_path_for(path: &Path) -> PathBuf {
        let mut file_name = path
            .file_name()
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("hematite.db"));
        file_name.push(".journal");
        match path.parent() {
            Some(parent) => parent.join(file_name),
            None => PathBuf::from(file_name),
        }
    }

    fn cleanup_paths_for(path: &Path) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(Self::wal_path_for(path));
        let _ = fs::remove_file(Self::journal_path_for(path));
        let _ = fs::remove_file(Self::rollback_lock_path_for(path));
        let _ = fs::remove_file(Self::wal_write_lock_path_for(path));
        let _ = fs::remove_dir_all(Self::wal_readers_dir_for(path));
    }
}

impl Drop for TestDbFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        let _ = fs::remove_file(self.wal_path());
        let _ = fs::remove_file(self.journal_path());
        let _ = fs::remove_file(self.rollback_lock_path());
        let _ = fs::remove_file(self.wal_write_lock_path());
        let _ = fs::remove_dir_all(self.wal_readers_dir());
    }
}

impl AsRef<Path> for TestDbFile {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}
