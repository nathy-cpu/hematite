//! WAL-mode scan and append, for deterministic profiling under valgrind.
//!
//! Same method as `scan_profile`: two runs whose difference isolates the phase
//! under test. See that file's header for why instruction counts rather than wall
//! clock.
//!
//!     PHASE=scan  SCANS=13 valgrind --tool=callgrind ... ./target/release/examples/wal_profile
//!     PHASE=scan  SCANS=0  valgrind --tool=callgrind ... ./target/release/examples/wal_profile
//!
//! `PHASE=append` instead measures committing one row at a time, which is the WAL
//! write path: frame append, header validation, and the visible-state publish.

use hematite::query::JournalMode;
use hematite::Hematite;

const ROWS: usize = 20_000;

fn main() {
    let path = ".scratch/wal_profile.db";
    // Sidecars: `.wal`, the `.wal.readers/` lock directory, and two lock files.
    let _ = std::fs::remove_dir_all(format!("{path}.wal.readers"));
    for suffix in ["", ".wal", ".wal.write.lock", ".rollback.lock", ".journal"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }
    let mut h = Hematite::new(path).expect("open");
    h.set_journal_mode(JournalMode::Wal).expect("wal mode");
    h.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT, d INT);")
        .expect("create");

    let phase = std::env::var("PHASE").unwrap_or_else(|_| "scan".to_string());
    let reps = std::env::var("SCANS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(13);

    if phase == "append" {
        // One row per transaction: the per-commit WAL cost, undiluted.
        for id in 0..reps * 100 {
            h.execute(&format!(
                "INSERT INTO t (id, a, b, c, d) VALUES ({id},{id},{id},{id},{id});"
            ))
            .expect("insert");
        }
        println!("appended {} rows one per txn", reps * 100);
        return;
    }

    let mut i = 0;
    while i < ROWS {
        let end = (i + 500).min(ROWS);
        let mut sql = String::from("INSERT INTO t (id, a, b, c, d) VALUES ");
        for id in i..end {
            if id > i {
                sql.push(',');
            }
            sql.push_str(&format!("({},{},{},{},{})", id, id, id, id, id));
        }
        sql.push(';');
        h.execute(&sql).expect("insert");
        i = end;
    }

    let mut total = 0usize;
    for _ in 0..reps {
        total += h.query("SELECT id, a FROM t;").expect("scan").len();
    }
    println!("{total} rows over {reps} scans");
}
