//! A full scan in a tight loop, for deterministic profiling under valgrind.
//!
//! Wall-clock benchmarking on this machine is unreliable — the same unchanged code
//! measured 1.55 ms and 3.97 ms for `full_scan` hours apart, which is drift large
//! enough to pass for a regression. Instruction counts and allocation counts do not
//! drift, so they are what before/after comparisons here should be based on.
//!
//! `SCANS` sets the number of scans (default 60) and `MODE=wal` switches the journal
//! mode. Run from the repository root: the database goes in `.scratch/`.
//!
//! Instructions per row:
//!
//!     SCANS=0  valgrind --tool=callgrind --callgrind-out-file=.scratch/base.out \
//!         --cache-sim=no --branch-sim=no ./target/release/examples/scan_profile
//!     SCANS=13 valgrind --tool=callgrind --callgrind-out-file=.scratch/s13.out \
//!         --cache-sim=no --branch-sim=no ./target/release/examples/scan_profile
//!
//! Subtract the two totals and divide by `13 * ROWS`. The baseline run exists to
//! net out the inserts, which cost far more than the scans do.
//! `callgrind_annotate` on the two files, diffed per function, gives the breakdown.
//!
//! Allocations per row: the same two runs under `--tool=dhat`, diffing "blocks".

use hematite::query::JournalMode;
use hematite::Hematite;

const ROWS: usize = 20_000;

fn main() {
    let path = ".scratch/scan_profile.db";
    let _ = std::fs::remove_file(".scratch/scan_profile.db.wal");
    let _ = std::fs::remove_file(path);
    let mut h = Hematite::new(path).expect("open");
    if std::env::var("MODE").as_deref() == Ok("wal") {
        h.set_journal_mode(JournalMode::Wal).expect("wal");
    }
    h.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT, d INT);")
        .expect("create");

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

    let scans = std::env::var("SCANS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);
    let mut total = 0usize;
    for _ in 0..scans {
        total += h.query("SELECT id, a FROM t;").expect("scan").len();
    }
    println!("{total} rows over {scans} scans");
}
