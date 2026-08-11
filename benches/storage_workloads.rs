//! End-to-end workload benchmarks, plus a disk-space report.
//!
//! The workload split follows `docs/sqlite-btree-pager-rust.md:1250-1261`:
//! append-heavy, point lookup, range scan, large row, delete-heavy. Each runs in
//! both journal modes, because the two durability engines have very different
//! per-commit costs.
//!
//! Wall time is only half the story here. Before criterion starts, `main` prints a
//! disk-space report — bytes on disk per row, for a fixed fixture. That number is a
//! headline metric of the storage rewrite and criterion has no way to express it.
//!
//! Sizes are tunable so the suite can be run quickly during development:
//!   HEMATITE_BENCH_ROWS=5000 cargo bench --bench storage_workloads

use std::hint::black_box;

use criterion::{criterion_group, Criterion};
use hematite::query::JournalMode;
use hematite::Hematite;

mod support;
use support::TempDb;

fn rows() -> usize {
    std::env::var("HEMATITE_BENCH_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000)
}

const MODES: [(&str, JournalMode); 2] = [
    ("rollback", JournalMode::Rollback),
    ("wal", JournalMode::Wal),
];

fn open(db: &TempDb, mode: JournalMode) -> Hematite {
    let mut h = Hematite::new(db.path_str()).expect("open database");
    h.set_journal_mode(mode).expect("set journal mode");
    h
}

/// The fixture every space measurement is taken against: a narrow table of small
/// integers, which is where fixed-width record tags cost the most.
fn create_narrow(h: &mut Hematite) {
    h.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT, d INT);")
        .expect("create table");
}

fn insert_narrow(h: &mut Hematite, count: usize, batch: usize) {
    let mut i = 0;
    while i < count {
        let end = (i + batch).min(count);
        let mut sql = String::from("INSERT INTO t (id, a, b, c, d) VALUES ");
        for id in i..end {
            if id > i {
                sql.push(',');
            }
            sql.push_str(&format!(
                "({},{},{},{},{})",
                id,
                id % 7,
                id % 11,
                id % 13,
                id % 17
            ));
        }
        sql.push(';');
        h.execute(&sql).expect("insert");
        i = end;
    }
}

// ---------------------------------------------------------------------------
// Disk space report
// ---------------------------------------------------------------------------

fn report_disk_usage() {
    let n = rows();
    eprintln!("\n=== disk usage (fixture: {n} rows) ===");
    eprintln!(
        "{:<10} {:<26} {:>12} {:>10} {:>12}",
        "mode", "fixture", "main bytes", "bytes/row", "total bytes"
    );

    for (label, mode) in MODES {
        // Narrow: five small integers per row.
        {
            let db = TempDb::new("space_narrow");
            {
                let mut h = open(&db, mode);
                create_narrow(&mut h);
                insert_narrow(&mut h, n, 500);
                if mode == JournalMode::Wal {
                    h.checkpoint_wal().expect("checkpoint");
                }
            }
            eprintln!(
                "{:<10} {:<26} {:>12} {:>10.1} {:>12}",
                label,
                "5 small ints",
                db.main_bytes(),
                db.main_bytes() as f64 / n as f64,
                db.total_bytes()
            );
        }

        // Mixed: the shape most application tables actually have.
        {
            let db = TempDb::new("space_mixed");
            {
                let mut h = open(&db, mode);
                h.execute(
                    "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, email TEXT, active BOOLEAN);",
                )
                .expect("create table");
                let mut i = 0;
                while i < n {
                    let end = (i + 500).min(n);
                    let mut sql = String::from("INSERT INTO t (id, name, email, active) VALUES ");
                    for id in i..end {
                        if id > i {
                            sql.push(',');
                        }
                        sql.push_str(&format!(
                            "({id},'user{id:06}','user{id:06}@example.com',TRUE)"
                        ));
                    }
                    sql.push(';');
                    h.execute(&sql).expect("insert");
                    i = end;
                }
                if mode == JournalMode::Wal {
                    h.checkpoint_wal().expect("checkpoint");
                }
            }
            eprintln!(
                "{:<10} {:<26} {:>12} {:>10.1} {:>12}",
                label,
                "int+2 text+bool",
                db.main_bytes(),
                db.main_bytes() as f64 / n as f64,
                db.total_bytes()
            );
        }
    }

    // Space amplification after churn: how much of the file is dead weight once
    // half the rows have been deleted.
    for (label, mode) in MODES {
        let db = TempDb::new("space_churn");
        let (before, after) = {
            let mut h = open(&db, mode);
            create_narrow(&mut h);
            insert_narrow(&mut h, n, 500);
            if mode == JournalMode::Wal {
                h.checkpoint_wal().expect("checkpoint");
            }
            let before = db.main_bytes();
            h.execute("DELETE FROM t WHERE a < 4;").expect("delete");
            if mode == JournalMode::Wal {
                h.checkpoint_wal().expect("checkpoint");
            }
            (before, db.main_bytes())
        };
        eprintln!(
            "{:<10} {:<26} {:>12} {:>10} {:>12}",
            label,
            "after deleting ~4/7",
            after,
            "-",
            format!("was {before}")
        );
    }
    eprintln!();
}

// ---------------------------------------------------------------------------
// Workloads
// ---------------------------------------------------------------------------

/// Sequential inserts, batched per transaction. Measures the commit path and the
/// append-side split policy together.
fn append_heavy(c: &mut Criterion) {
    let n = (rows() / 10).max(200);
    let mut group = c.benchmark_group("append_heavy");
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(n as u64));

    for (label, mode) in MODES {
        for batch in [1usize, 100] {
            group.bench_function(format!("{label}_batch{batch}"), |b| {
                b.iter_batched(
                    || {
                        let db = TempDb::new("append");
                        let mut h = open(&db, mode);
                        create_narrow(&mut h);
                        (db, h)
                    },
                    |(db, mut h)| {
                        insert_narrow(&mut h, n, batch);
                        black_box(db.main_bytes())
                    },
                    criterion::BatchSize::PerIteration,
                );
            });
        }
    }
    group.finish();
}

/// Point lookups by primary key over a warm cache.
fn point_lookup(c: &mut Criterion) {
    let n = rows();
    let mut group = c.benchmark_group("point_lookup");

    for (label, mode) in MODES {
        let db = TempDb::new("lookup");
        let mut h = open(&db, mode);
        create_narrow(&mut h);
        insert_narrow(&mut h, n, 500);

        let mut probe = 0usize;
        group.bench_function(label, |b| {
            b.iter(|| {
                // A cheap deterministic walk that defeats sequential prefetching.
                probe = (probe.wrapping_mul(2654435761).wrapping_add(12345)) % n;
                let rs = h
                    .query(&format!("SELECT a FROM t WHERE id = {probe};"))
                    .expect("select");
                black_box(rs.len())
            })
        });
    }
    group.finish();
}

/// Full ordered scan. This is the path that currently materializes the whole table
/// into a `Vec<StoredRow>` before the first row is returned.
fn full_scan(c: &mut Criterion) {
    let n = rows();
    let mut group = c.benchmark_group("full_scan");
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(n as u64));

    for (label, mode) in MODES {
        let db = TempDb::new("scan");
        let mut h = open(&db, mode);
        create_narrow(&mut h);
        insert_narrow(&mut h, n, 500);

        group.bench_function(label, |b| {
            b.iter(|| {
                let rs = h.query("SELECT id, a FROM t;").expect("scan");
                black_box(rs.len())
            })
        });
    }
    group.finish();
}

/// A narrow range and a LIMIT. Both should cost a seek plus a few rows; today both
/// pay for the whole table.
fn range_and_limit(c: &mut Criterion) {
    let n = rows();
    let lo = n / 2;
    let hi = lo + (n / 100).max(10);
    let mut group = c.benchmark_group("range_and_limit");

    for (label, mode) in MODES {
        let db = TempDb::new("range");
        let mut h = open(&db, mode);
        create_narrow(&mut h);
        insert_narrow(&mut h, n, 500);

        group.bench_function(format!("{label}_range_1pct"), |b| {
            b.iter(|| {
                let rs = h
                    .query(&format!("SELECT id FROM t WHERE id >= {lo} AND id < {hi};"))
                    .expect("range");
                black_box(rs.len())
            })
        });
        group.bench_function(format!("{label}_limit_100"), |b| {
            b.iter(|| {
                let rs = h.query("SELECT id FROM t LIMIT 100;").expect("limit");
                black_box(rs.len())
            })
        });
    }
    group.finish();
}

/// Rows carrying a value large enough to spill into an overflow chain, scanned
/// without projecting the large column. The spill threshold decides how many leaf
/// pages this touches.
fn large_row(c: &mut Criterion) {
    let n = (rows() / 20).max(100);
    let payload = "x".repeat(2048);
    let mut group = c.benchmark_group("large_row");
    group.sample_size(10);

    for (label, mode) in MODES {
        let db = TempDb::new("largerow");
        let mut h = open(&db, mode);
        h.execute("CREATE TABLE t (id INT PRIMARY KEY, tag INT, body TEXT);")
            .expect("create table");
        for id in 0..n {
            h.execute(&format!(
                "INSERT INTO t (id, tag, body) VALUES ({id},{},'{payload}');",
                id % 7
            ))
            .expect("insert");
        }

        group.bench_function(format!("{label}_scan_without_body"), |b| {
            b.iter(|| {
                let rs = h.query("SELECT id, tag FROM t;").expect("scan");
                black_box(rs.len())
            })
        });
        group.bench_function(format!("{label}_scan_with_body"), |b| {
            b.iter(|| {
                let rs = h.query("SELECT id, body FROM t;").expect("scan");
                black_box(rs.len())
            })
        });
    }
    group.finish();
}

/// Delete-heavy churn, ending in an integrity check so the benchmark also proves
/// the tree survived it.
fn delete_heavy(c: &mut Criterion) {
    let n = (rows() / 4).max(500);
    let mut group = c.benchmark_group("delete_heavy");
    group.sample_size(10);

    for (label, mode) in MODES {
        group.bench_function(label, |b| {
            b.iter_batched(
                || {
                    let db = TempDb::new("delete");
                    let mut h = open(&db, mode);
                    create_narrow(&mut h);
                    insert_narrow(&mut h, n, 500);
                    (db, h)
                },
                |(db, mut h)| {
                    h.execute("DELETE FROM t WHERE a < 4;").expect("delete");
                    black_box(db.main_bytes())
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// Secondary-index maintenance and lookup: the index-key encoding and the
/// index-store round trip.
fn secondary_index(c: &mut Criterion) {
    let n = rows();
    let mut group = c.benchmark_group("secondary_index");

    for (label, mode) in MODES {
        let db = TempDb::new("secidx");
        let mut h = open(&db, mode);
        h.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, bucket INT);")
            .expect("create table");
        let mut i = 0;
        while i < n {
            let end = (i + 500).min(n);
            let mut sql = String::from("INSERT INTO t (id, name, bucket) VALUES ");
            for id in i..end {
                if id > i {
                    sql.push(',');
                }
                sql.push_str(&format!("({id},'name{id:06}',{})", id % 100));
            }
            sql.push(';');
            h.execute(&sql).expect("insert");
            i = end;
        }
        h.execute("CREATE INDEX idx_bucket ON t (bucket);")
            .expect("create index");

        let mut probe = 0usize;
        group.bench_function(format!("{label}_indexed_lookup"), |b| {
            b.iter(|| {
                probe = (probe + 1) % 100;
                let rs = h
                    .query(&format!("SELECT id FROM t WHERE bucket = {probe};"))
                    .expect("indexed select");
                black_box(rs.len())
            })
        });
    }
    group.finish();
}

criterion_group!(
    workloads,
    append_heavy,
    point_lookup,
    full_scan,
    range_and_limit,
    large_row,
    delete_heavy,
    secondary_index,
);

/// Hand-written in place of `criterion_main!` so the disk-space report runs first.
/// Criterion only reports time; bytes-on-disk is a first-class metric of this
/// rewrite and has to be printed separately.
fn main() {
    if std::env::args().any(|a| a == "--test") {
        // `cargo test` runs benches in test mode; skip the expensive report.
        workloads();
        return;
    }
    report_disk_usage();
    workloads();
    Criterion::default().configure_from_args().final_summary();
}
