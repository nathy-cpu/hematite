//! One workload per run, for deterministic profiling of the storage and catalog layers.
//!
//! [`scan_profile`](scan_profile.rs) covers bulk insert and full scan. This covers the
//! paths that one never reaches: point lookups, updates, deletes, and — the reason it
//! exists — small transactions, where syscall count rather than instruction count is
//! what dominates.
//!
//! Pick a workload with `PHASE`, size it with `ROWS` and `REPS`, and pick a durability
//! mode with `MODE=rollback|wal`. `PHASE=setup` runs the fixture build and stops, so
//! the measured phase can be isolated by subtraction the same way `scan_profile` nets
//! out its inserts.
//!
//!     PHASE=setup   valgrind --tool=callgrind --callgrind-out-file=.scratch/a.out ...
//!     PHASE=rollback valgrind --tool=callgrind --callgrind-out-file=.scratch/b.out ...
//!
//! Syscalls, which is the metric callgrind cannot see:
//!
//!     PHASE=append_txns strace -c -f ./target/release/examples/storage_profile
//!     PHASE=append_txns strace -f -e trace=pwrite64,pread64,fsync,fdatasync,ftruncate \
//!         ./target/release/examples/storage_profile 2>&1 | sort | uniq -c | sort -rn
//!
//! Allocations: the same pair of runs under `--tool=dhat`, diffing "blocks".
//!
//! Every run also prints the storage counters for the measured phase alone —
//! fsyncs, page reads and writes, pool hits and misses, journal bytes, page-image
//! copies. Those need no tooling at all, and they catch the two things the other
//! two instruments are blind to: a syscall, which callgrind does not see, and a
//! copy into a recycled buffer, which dhat does not see.

use hematite::query::JournalMode;
use hematite::Hematite;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let rows = env_usize("ROWS", 20_000);
    let reps = env_usize("REPS", 1);
    let phase = std::env::var("PHASE").unwrap_or_else(|_| "scan".to_string());

    let path = ".scratch/storage_profile.db";
    let _ = std::fs::remove_file(format!("{path}.wal"));
    let _ = std::fs::remove_file(path);
    let mut h = Hematite::new(path).expect("open");
    if std::env::var("MODE").as_deref() == Ok("wal") {
        h.set_journal_mode(JournalMode::Wal).expect("wal");
    }
    h.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT, d INT);")
        .expect("create");

    // The fixture is built in batches so that its own cost stays a small, constant
    // share of any phase measured against `PHASE=setup`.
    let mut i = 0;
    while i < rows {
        let end = (i + 500).min(rows);
        let mut sql = String::from("INSERT INTO t (id, a, b, c, d) VALUES ");
        for id in i..end {
            if id > i {
                sql.push(',');
            }
            sql.push_str(&format!("({id},{id},{id},{id},{id})"));
        }
        sql.push(';');
        h.execute(&sql).expect("insert");
        i = end;
    }

    let baseline = h.storage_counters().expect("counters");
    let mut sink = 0usize;
    match phase.as_str() {
        // The subtraction baseline: build the fixture and stop.
        "setup" => {}
        "scan" => {
            for _ in 0..reps {
                sink += h.query("SELECT id, a FROM t;").expect("scan").len();
            }
        }
        // Every row by primary key, one statement each. Descent and record decode
        // without the per-row cost being swamped by the scan's iteration.
        "lookup" => {
            for _ in 0..reps {
                for id in 0..rows {
                    sink += h
                        .query(&format!("SELECT a FROM t WHERE id = {id};"))
                        .expect("lookup")
                        .len();
                }
            }
        }
        // In-place value change: no key moves, so this isolates the update path from
        // tree restructuring.
        "update" => {
            for rep in 0..reps.max(1) {
                h.execute(&format!("UPDATE t SET a = a + {};", rep + 1))
                    .expect("update");
            }
        }
        // Builds a large transaction and throws it away, which is what exercises
        // the undo log and journal replay rather than the commit path.
        "rollback" => {
            for rep in 0..reps.max(1) {
                h.execute("BEGIN;").expect("begin");
                let base = rows + rep * rows;
                let mut i = 0;
                while i < rows {
                    let end = (i + 500).min(rows);
                    let mut sql = String::from("INSERT INTO t (id, a, b, c, d) VALUES ");
                    for offset in i..end {
                        if offset > i {
                            sql.push(',');
                        }
                        let id = base + offset;
                        sql.push_str(&format!("({id},{id},{id},{id},{id})"));
                    }
                    sql.push(';');
                    h.execute(&sql).expect("insert");
                    i = end;
                }
                h.execute("ROLLBACK;").expect("rollback");
            }
        }
        // Deletes every row in one transaction, exercising merge and free-page churn.
        "delete" => {
            h.execute("DELETE FROM t;").expect("delete");
        }
        // The syscall workload: one row per transaction, so per-commit costs are not
        // amortised over a batch. `REPS` is the number of commits; `ROWS` only sizes
        // the fixture underneath them, and can be small — 200 is plenty.
        "append_txns" => {
            for id in rows..rows + reps.max(1) {
                h.execute(&format!(
                    "INSERT INTO t (id, a, b, c, d) VALUES ({id},{id},{id},{id},{id});"
                ))
                .expect("append");
            }
        }
        other => panic!("unknown PHASE {other}"),
    }
    // The counters are the point of this harness as much as the workload is:
    // callgrind cannot see a syscall and dhat cannot see a memcpy into a recycled
    // buffer, and these see both.
    let work = h.storage_counters().expect("counters").since(&baseline);
    println!("phase={phase} rows={rows} reps={reps} sink={sink}");
    println!("{work:#?}");
}
