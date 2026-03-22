use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

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
        let path = PathBuf::from(format!("{}_{}.db", prefix, nanos));
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(Self::pager_checksum_path_for(&path));
        Self { path }
    }

    pub fn path(&self) -> &str {
        self.path.to_str().unwrap_or_default()
    }

    pub fn as_path(&self) -> &Path {
        &self.path
    }

    fn pager_checksum_path(&self) -> PathBuf {
        Self::pager_checksum_path_for(&self.path)
    }

    fn pager_checksum_path_for(path: &Path) -> PathBuf {
        let mut file_name = path
            .file_name()
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("hematite.db"));
        file_name.push(".pager_checksums");
        match path.parent() {
            Some(parent) => parent.join(file_name),
            None => PathBuf::from(file_name),
        }
    }
}

impl Drop for TestDbFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        let _ = fs::remove_file(self.pager_checksum_path());
    }
}

impl AsRef<Path> for TestDbFile {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}
