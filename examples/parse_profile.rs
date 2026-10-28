//! One parser workload per run, for deterministic profiling of the parser layer.
//!
//! `docs/performance.md` puts the parser at 10.3 % of a point lookup and 18.5 % of an
//! insert, and separately puts 28.8 % / 13.5 % in `malloc`/`free` — a bucket
//! `scripts/layer-attribution.sh` cannot attribute to a layer by construction. An
//! allocating layer therefore reads small in the layer table and large in a bucket that
//! names nobody. This harness exists to close that gap for the parser specifically.
//!
//! Pick a workload with `PHASE` and size it with `REPS`. `REPS=0` runs the fixture and
//! stops, so a phase is isolated by subtraction the same way `storage_profile` nets out
//! its inserts:
//!
//!     PHASE=insert_values REPS=0   valgrind --tool=callgrind \
//!         --callgrind-out-file=.scratch/p-ins-0.out   ./target/release/examples/parse_profile
//!     PHASE=insert_values REPS=200 valgrind --tool=callgrind \
//!         --callgrind-out-file=.scratch/p-ins-200.out ./target/release/examples/parse_profile
//!     scripts/layer-attribution.sh .scratch/p-ins-200.out .scratch/p-ins-0.out
//!
//! Where the allocator is called *from*, which is the layer script's documented blind
//! spot: add `--separate-callers=2`. Blocks and bytes: the same pair under
//! `--tool=dhat`, diffing "blocks".
//!
//! Every run also prints allocations and bytes for the measured phase alone, counted by
//! this binary's own global allocator. That number needs no tooling, does not drift the
//! way wall clock on this machine does (`docs/performance.md:5`), and is the one the
//! arena parser is judged on. The parse-only phases call `parse_sql_statement`,
//! which builds a fresh `Ast` and then the owned tree through `bridge.rs`, so they
//! count both; the arena alone, warm, should allocate nothing.
//!
//! Wall clock is deliberately not reported. It is the last instrument here, not the
//! first, and only ever by running two builds alternately in one session.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

use hematite::parser::parser::parse_expression_fragment;
use hematite::parser::{parse_sql_statement, tokenize_sql};
use hematite::{script_is_complete, Hematite, Value};

// ── Allocation counting ──────────────────────────────────────────────────────
//
// A `#[global_allocator]` is one per program and visible to everything linked in, so
// it belongs in a binary, never in the library. Here it covers the linked `hematite`
// code as well, which is the point.

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn allocs() -> (u64, u64) {
    (
        ALLOCS.load(Ordering::Relaxed),
        ALLOC_BYTES.load(Ordering::Relaxed),
    )
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ── Workload text ────────────────────────────────────────────────────────────
//
// The same strings as `src/parser/tests.rs`'s `perf_baseline_tests`, so the ignored
// wall-clock baselines and this harness measure the same thing.

/// Joins, aggregates, GROUP BY / HAVING / ORDER BY / LIMIT: the widest statement shape
/// the parser supports, and the deepest expression nesting.
fn large_select() -> String {
    "SELECT u.id, u.name, p.title, COUNT(c.id) AS comment_count \
     FROM users u \
     LEFT JOIN posts p ON u.id = p.user_id \
     LEFT JOIN comments c ON p.id = c.post_id \
     WHERE u.active = TRUE AND p.score > 10 \
     GROUP BY u.id, u.name, p.title \
     HAVING COUNT(c.id) > 1 \
     ORDER BY p.title DESC, u.name ASC LIMIT 200 OFFSET 10;"
        .to_string()
}

/// A batched multi-row INSERT. This is the shape `examples/storage_profile.rs` builds
/// its fixture from, and the one the documented 18.5 % of an insert actually is: in
/// the owned tree, one `Vec` per row and one `String` per text literal.
fn large_insert(rows: usize) -> String {
    let mut sql = String::from("INSERT INTO users (id, name, score, active) VALUES ");
    for i in 0..rows {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!(
            "({}, 'user{}', {}, TRUE)",
            i + 1,
            i + 1,
            (i % 97) + 1
        ));
    }
    sql.push(';');
    sql
}

/// A DDL script of many statements. `script_split` and `repl_paste` run
/// `script_is_complete` over it — a byte scan, not the lexer — which the REPL calls
/// after every input line.
fn ddl_script() -> String {
    let mut script = String::new();
    for i in 0..120 {
        script.push_str(&format!(
            "CREATE TABLE t{i} (id INT PRIMARY KEY, name TEXT, active BOOLEAN DEFAULT TRUE); \
             ALTER TABLE t{i} ADD COLUMN score INT; \
             CREATE INDEX idx_t{i}_score ON t{i} (score); "
        ));
    }
    script
}

/// The slow paths the lexer flags and the parser decodes: a 4 KiB hex blob, a
/// doubled-quote escape and a backslash escape, none of whose values is a slice of
/// the input.
fn string_blob_heavy() -> String {
    let blob = "AB".repeat(2_048);
    format!(
        "INSERT INTO files (name, payload, note) \
         VALUES ('alpha', X'{blob}', 'O\\'Brien''s report with unicode Alíce');"
    )
}

/// Rows per `check_rows` statement.
const CHECK_ROWS_PER_STATEMENT: usize = 50;

/// A CHECK condition. `parse_expression_fragment` re-parses one of these out of
/// catalog text once per statement that checks rows against the table
/// (`check_plan_for`), and again on `ALTER TABLE` paths that inspect or rename
/// columns, so it is parsed far more often than written.
const CHECK_FRAGMENT: &str =
    "(score BETWEEN 1 AND 100 AND (status = 'ACTIVE' OR status = 'PENDING')) \
     OR (expires_at IS NULL AND retries < 3)";

// ── End-to-end fixtures ──────────────────────────────────────────────────────
//
// The end-to-end phases go through a real `Connection` rather than
// `parse_sql_statement`, because the cost they exist to expose only happens there.

fn fresh_db(tag: &str) -> (Hematite, String) {
    let _ = std::fs::create_dir_all(".scratch");
    let path = format!(".scratch/parse_profile_{tag}.db");
    for suffix in ["", ".wal", ".journal"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }
    let h = Hematite::new(&path).expect("open");
    (h, path)
}

fn main() {
    let phase = std::env::var("PHASE").unwrap_or_else(|_| "insert_values".to_string());
    let reps = env_usize("REPS", 200);
    let rows = env_usize("ROWS", 400);

    // Build every fixture before the baseline is taken, so `REPS=0` subtracts all of
    // it and what remains is the measured phase alone.
    let select_sql = large_select();
    let insert_sql = large_insert(rows);
    let script_sql = ddl_script();
    let blob_sql = string_blob_heavy();
    let lookup_sql = "SELECT a FROM t WHERE id = 4711;";

    let mut db = None;
    let mut prepared = None;
    match phase.as_str() {
        // `check_rows` and `no_check_rows` are the same statement against the same
        // table shape, differing only in whether CHECK constraints are declared. The
        // difference between them is what checking a row costs.
        "check_rows" | "no_check_rows" => {
            let (mut h, path) = fresh_db(&phase);
            let ddl = if phase == "check_rows" {
                "CREATE TABLE t (id INT PRIMARY KEY, score INT CHECK (score BETWEEN 1 AND 100), \
                 status TEXT CHECK (status = 'ACTIVE' OR status = 'PENDING'));"
            } else {
                "CREATE TABLE t (id INT PRIMARY KEY, score INT, status TEXT);"
            };
            h.execute(ddl).expect("create");
            db = Some((h, path));
        }
        "prepared_rebind" => {
            let (mut h, path) = fresh_db("prepared");
            h.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT);")
                .expect("create");
            prepared = Some(
                h.prepare("INSERT INTO t (id, a, b) VALUES (?, ?, ?);")
                    .expect("prepare"),
            );
            db = Some((h, path));
        }
        "correlated" | "lookup" => {
            let (mut h, path) = fresh_db(&phase);
            h.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT);")
                .expect("create");
            h.execute("CREATE TABLE u (id INT PRIMARY KEY, z INT);")
                .expect("create");
            let mut sql = String::from("INSERT INTO t (id, a, b) VALUES ");
            let mut usql = String::from("INSERT INTO u (id, z) VALUES ");
            for i in 0..500 {
                if i > 0 {
                    sql.push(',');
                    usql.push(',');
                }
                sql.push_str(&format!("({i},{i},{i})"));
                usql.push_str(&format!("({i},{i})"));
            }
            sql.push(';');
            usql.push(';');
            h.execute(&sql).expect("seed t");
            h.execute(&usql).expect("seed u");
            db = Some((h, path));
        }
        _ => {}
    }

    let (base_allocs, base_bytes) = allocs();
    let mut sink = 0usize;

    for rep in 0..reps {
        match phase.as_str() {
            // ── Parse-only phases ────────────────────────────────────────────
            "select" => {
                sink += parse_sql_statement(&select_sql)
                    .expect("select")
                    .is_read_only() as usize
            }
            "insert_values" => {
                sink += parse_sql_statement(&insert_sql)
                    .expect("insert")
                    .is_read_only() as usize
            }
            "blob_strings" => {
                sink += parse_sql_statement(&blob_sql).expect("blob").is_read_only() as usize
            }
            "point_lookup" => {
                sink += parse_sql_statement(lookup_sql)
                    .expect("lookup")
                    .is_read_only() as usize
            }
            "tokenize" => sink += tokenize_sql(&select_sql).expect("tokenize").len(),
            "script_split" => sink += script_is_complete(&script_sql).expect("script") as usize,
            // What the REPL does while a long script is being pasted: ask after every
            // line whether the buffer ends on a statement boundary.
            "repl_paste" => {
                let mut buffer = String::new();
                for line in script_sql.split_inclusive(';').take(120) {
                    buffer.push_str(line);
                    sink += script_is_complete(&buffer).expect("repl") as usize;
                }
            }
            "check_fragment" => {
                sink += matches!(
                    parse_expression_fragment(CHECK_FRAGMENT).expect("fragment"),
                    hematite::Expression::Logical { .. }
                ) as usize
            }

            // ── End-to-end phases ────────────────────────────────────────────
            //
            // A fixed amount of work per rep, so the costs these exist to expose
            // scale with REPS the same way the parse-only phases do.
            //
            // One statement writing many rows, which is what makes the per-row
            // constraint work visible. A single-row INSERT pays the setup once and
            // gets nothing back from it.
            "check_rows" | "no_check_rows" => {
                let base = rep * CHECK_ROWS_PER_STATEMENT;
                let mut sql = String::from("INSERT INTO t (id, score, status) VALUES ");
                for i in 0..CHECK_ROWS_PER_STATEMENT {
                    if i > 0 {
                        sql.push(',');
                    }
                    sql.push_str(&format!("({}, 50, 'ACTIVE')", base + i));
                }
                sql.push(';');
                let (h, _) = db.as_mut().expect("fixture");
                h.execute(&sql).expect("insert");
                sink += CHECK_ROWS_PER_STATEMENT;
            }
            "prepared_rebind" => {
                let (h, _) = db.as_mut().expect("fixture");
                let stmt = prepared.as_mut().expect("prepared");
                stmt.bind_all(vec![
                    Value::Integer(rep as i32),
                    Value::Integer((rep % 97) as i32),
                    Value::Text(format!("row{rep}")),
                ])
                .expect("bind");
                h.execute_prepared(stmt).expect("execute");
                sink += 1;
            }
            "correlated" => {
                let (h, _) = db.as_mut().expect("fixture");
                sink += h
                    .query("SELECT id FROM t WHERE a = (SELECT z FROM u WHERE u.id = t.id);")
                    .expect("correlated")
                    .len();
            }
            "lookup" => {
                let (h, _) = db.as_mut().expect("fixture");
                sink += h
                    .query(&format!("SELECT a FROM t WHERE id = {};", rep % 500))
                    .expect("lookup")
                    .len();
            }

            other => {
                eprintln!("unknown PHASE '{other}'");
                std::process::exit(2);
            }
        }
    }

    let (end_allocs, end_bytes) = allocs();
    let phase_allocs = end_allocs - base_allocs;
    let phase_bytes = end_bytes - base_bytes;

    println!("phase        {phase}");
    println!("reps         {reps}");
    println!("allocs       {phase_allocs}");
    println!("alloc bytes  {phase_bytes}");
    if reps > 0 {
        println!("allocs/rep   {:.1}", phase_allocs as f64 / reps as f64);
        println!("bytes/rep    {:.1}", phase_bytes as f64 / reps as f64);
    }
    println!("sink         {sink}");

    if let Some((h, path)) = db {
        drop(prepared);
        drop(h);
        for suffix in ["", ".wal", ".journal"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
    }
}
