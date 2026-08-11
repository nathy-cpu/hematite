//! Shared helpers for the benchmark crates.
//!
//! Deliberately dependency-free: benchmarks must not need anything the library
//! itself refuses to depend on.

// Each bench is its own crate and uses a different subset of these helpers.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A database file in the system temp directory, removed along with every sidecar
/// the pager may have created when it drops.
pub struct TempDb {
    path: PathBuf,
}

impl TempDb {
    pub fn new(tag: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
            ^ COUNTER
                .fetch_add(1, Ordering::Relaxed)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut path = std::env::temp_dir();
        path.push(format!("hematite_bench_{tag}_{nonce:016x}.db"));
        let db = Self { path };
        db.cleanup();
        db
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn path_str(&self) -> &str {
        self.path.to_str().expect("temp path is valid utf-8")
    }

    /// Total bytes occupied by the database and every sidecar it has spawned.
    pub fn total_bytes(&self) -> u64 {
        let mut total = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);
        for suffix in [".wal", ".journal"] {
            let mut side = self.path.clone().into_os_string();
            side.push(suffix);
            total += std::fs::metadata(&side).map(|m| m.len()).unwrap_or(0);
        }
        total
    }

    pub fn main_bytes(&self) -> u64 {
        std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }

    fn cleanup(&self) {
        let _ = std::fs::remove_file(&self.path);
        for suffix in [
            ".wal",
            ".journal",
            ".rollback.lock",
            ".wal.write.lock",
            ".pager_checksums",
        ] {
            let mut side = self.path.clone().into_os_string();
            side.push(suffix);
            let _ = std::fs::remove_file(&side);
        }
        let mut readers = self.path.clone().into_os_string();
        readers.push(".wal.readers");
        let _ = std::fs::remove_dir_all(&readers);
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        self.cleanup();
    }
}

/// Counts live heap allocations so a benchmark can assert "this path allocates
/// nothing" rather than merely hoping so.
pub struct CountingAllocator;

pub static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
pub static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl std::alloc::GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { std::alloc::System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        unsafe { std::alloc::System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, new_size: usize) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { std::alloc::System.realloc(ptr, layout, new_size) }
    }
}

/// Allocation counts and bytes attributed to a single closure.
pub fn measure_allocs<T>(f: impl FnOnce() -> T) -> (T, u64, u64) {
    let count_before = ALLOC_COUNT.load(Ordering::Relaxed);
    let bytes_before = ALLOC_BYTES.load(Ordering::Relaxed);
    let value = f();
    (
        value,
        ALLOC_COUNT.load(Ordering::Relaxed) - count_before,
        ALLOC_BYTES.load(Ordering::Relaxed) - bytes_before,
    )
}
