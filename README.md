# Hematite

Hematite is a small embeddable SQL database written in Rust.

The goal is not to be a giant general-purpose server. The goal is to stay small enough to
understand, repurpose, and extend, while still supporting a rich relational feature set.

## What It Is Good At

- embedding directly into a Rust application
- shipping as a lightweight local database
- experimentation with SQL and storage internals
- being readable enough to extend without fighting a huge codebase

## Quick Start

Add the crate:

```toml
[dependencies]
hematite = "0.1"
```

Use it:

```rust
use hematite::Hematite;

fn main() -> hematite::Result<()> {
    let mut db = Hematite::new_in_memory()?;

    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")?;
    db.execute("INSERT INTO users (id, name) VALUES (1, 'Ada');")?;

    let rows = db.query("SELECT id, name FROM users ORDER BY id;")?;
    for row in rows.iter() {
        println!("{:?}", row.values);
    }

    Ok(())
}
```

## CLI

This repo also ships a small CLI binary:

```bash
cargo run --bin hematite_cli -- demo.db
cargo run --bin hematite_cli -- demo.db "SELECT * FROM users;"
```

## Main API Surface

- `Hematite`: high-level facade
- `Connection`: lower-level SQL connection boundary
- `PreparedStatement`: prepared execution with parameters
- `Transaction`: explicit transaction wrapper
- `ResultSet`, `Row`, `StatementResult`: result types

## SQL Surface

Hematite currently supports a broad practical subset of SQL, including:

- transactions and savepoints
- tables, indexes, views, and triggers
- joins, aggregates, recursive CTEs, and window functions
- `INSERT ... SELECT`, `SELECT INTO`, and joined `UPDATE` / `DELETE`
- scalar functions, `CASE`, interval arithmetic, and rich type coercion
- introspection commands such as `SHOW ...`, `DESCRIBE`, and `EXPLAIN`

## Dialect Notes

Hematite is not trying to be a full PostgreSQL, SQLite, or MySQL clone.

Important dialect rules:

- SQL keywords must be uppercase
- the type system is custom and intentionally explicit
- some MySQL-inspired syntax is accepted, but compatibility is not the main goal

## Documentation

- [Architecture](docs/architecture.md)
- [Codebase Guide](docs/codebase-guide.md)
- [SQL Dialect](docs/sql-dialect.md)

## Status

Hematite is usable and well tested, but it is still an evolving library. Expect the dialect and
public API to continue being refined.

## License

MIT. See [LICENSE](LICENSE).
