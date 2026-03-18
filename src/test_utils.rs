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
        Self { path }
    }

    pub fn path(&self) -> &str {
        self.path.to_str().unwrap_or_default()
    }

    pub fn as_path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDbFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

impl AsRef<Path> for TestDbFile {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}
