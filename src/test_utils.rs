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

    /// Every file the pager may create alongside the database.
    ///
    /// Cleanup has to know about all of them, or every test run leaves another set
    /// behind in the repository root.
    const SIDECAR_FILE_SUFFIXES: [&'static str; 4] =
        [".wal", ".journal", ".rollback.lock", ".wal.write.lock"];

    const SIDECAR_DIR_SUFFIXES: [&'static str; 1] = [".wal.readers"];

    fn sidecar_for(path: &Path, suffix: &str) -> PathBuf {
        let mut file_name = path
            .file_name()
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("hematite.db"));
        file_name.push(suffix);
        match path.parent() {
            Some(parent) => parent.join(file_name),
            None => PathBuf::from(file_name),
        }
    }

    fn cleanup_paths_for(path: &Path) {
        let _ = fs::remove_file(path);
        for suffix in Self::SIDECAR_FILE_SUFFIXES {
            let _ = fs::remove_file(Self::sidecar_for(path, suffix));
        }
        for suffix in Self::SIDECAR_DIR_SUFFIXES {
            let _ = fs::remove_dir_all(Self::sidecar_for(path, suffix));
        }
    }
}

impl Drop for TestDbFile {
    fn drop(&mut self) {
        Self::cleanup_paths_for(&self.path);
    }
}

impl AsRef<Path> for TestDbFile {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}
