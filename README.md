# Hematite

Hematite is a small embeddable SQL database written in Rust.

The goal is not to be a giant general-purpose server. The goal is to stay small enough to
understand, repurpose, and extend, while still supporting a robust relational feature set.

## What It Is Good At

- **Direct Embedding**: Seamlessly integrate into Rust applications without external dependencies.
- **Lightweight Storage**: Perfect for local-first applications and single-user tools.
- **Internals Exploration**: A readable codebase designed for experimentation with SQL and B-tree internals.
- **Explicit Semantics**: A strict, predictable SQL dialect that avoids the "magic" of larger RDBMSs.

## Quick Start

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
hematite = { package = "hematite-db", version = "0.1" }
```

The published package name is `hematite-db`, while the library is imported in Rust code as
`hematite`.

### Basic Usage

```rust
use hematite::Hematite;

fn main() -> hematite::Result<()> {
    let mut db = Hematite::new_in_memory()?;

    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, balance DECIMAL(10, 2));")?;
    db.execute("INSERT INTO users (id, name, balance) VALUES (1, 'Ada', 1000.50);")?;

    // Flexible query API
    let rows = db.query("SELECT id, name FROM users WHERE balance > 500;")?;
    for row in rows.iter() {
        println!("User {}: {}", row.get_int(0)?, row.get_string(1)?);
    }

    Ok(())
}
```

### Transactions and Savepoints

```rust
use hematite::Hematite;

fn transfer_funds(db: &mut Hematite, from: i32, to: i32, amount: f64) -> hematite::Result<()> {
    let mut tx = db.transaction()?;

    tx.execute(&format!("UPDATE users SET balance = balance - {amount} WHERE id = {from};"))?;
    
    // Create a savepoint for complex multi-step logic
    tx.execute("SAVEPOINT intermediate;")?;
    
    match tx.execute(&format!("UPDATE users SET balance = balance + {amount} WHERE id = {to};")) {
        Ok(_) => {
            tx.execute("RELEASE SAVEPOINT intermediate;")?;
            tx.commit()?;
        }
        Err(_) => {
            tx.execute("ROLLBACK TO SAVEPOINT intermediate;")?;
            // Handle error or commit the partial transaction
        }
    }
    
    Ok(())
}
```

### Prepared Statements with Parameters

```rust
use hematite::{Hematite, Value};

fn find_user(db: &mut Hematite, user_id: i32) -> hematite::Result<()> {
    let mut stmt = db.prepare("SELECT name FROM users WHERE id = ?;")?;

    // Bind parameters by index (starting at 1)
    stmt.bind(1, Value::Integer(user_id))?;

    // A prepared SELECT runs against the connection and returns its rows
    let result = stmt.query(&mut db.connection)?;
    for row in result.rows().iter() {
        println!("{:?}", row[0]); // each row is a `&[Value]`
    }

    Ok(())
}

fn rename_user(db: &mut Hematite, user_id: i32, name: &str) -> hematite::Result<()> {
    let mut stmt = db.prepare("UPDATE users SET name = ? WHERE id = ?;")?;
    stmt.bind_all(vec![Value::Text(name.to_string()), Value::Integer(user_id)])?;

    // `execute_prepared` is for statements that return no rows
    db.execute_prepared(&mut stmt)?;
    Ok(())
}
```

### Persistent Storage

By default, `Hematite::new_in_memory()` creates a transient database. For persistence, simply specify a file path:

```rust
let mut db = Hematite::new("prod_data.db")?;
```

### Struct Mapping

Map database rows directly to Rust structs by implementing the `FromRow` trait.

```rust
use hematite::{Hematite, FromRow, Row, Result};

struct User {
    id: i32,
    name: String,
}

impl FromRow for User {
    fn from_row(row: &Row) -> Result<Self> {
        Ok(Self {
            id: row.get_int(0)?,
            name: row.get_string(1)?,
        })
    }
}

fn list_users(db: &mut Hematite) -> Result<Vec<User>> {
    db.query_as("SELECT id, name FROM users;")
}
```

### ASCII Table Rendering

For CLI tools or debugging, you can render a result set as a formatted ASCII table:

```rust
let results = db.query("SELECT id, name, balance FROM users;")?;
println!("{}", results.render_ascii_table());
```

```text
+----+------+---------+
| id | name | balance |
+----+------+---------+
| 1  | Ada  | 1000.5  |
| 2  | Bob  | 250.75  |
+----+------+---------+
2 row(s)
```

## CLI Usage

Hematite ships with a lightweight CLI tool (`hematite_cli`) for schema exploration and ad-hoc queries.

### Interactive Shell

Start the interactive REPL with a database file:

```bash
cargo run --bin hematite_cli -- demo.db
```

A statement runs once the input ends in a `;` outside a string literal, so it can span several lines; the prompt changes from `db >` to `... >` while one is unfinished. `.exit` or `.quit` at the `db >` prompt, or end of input, leaves the shell.

### One-off Commands

Execute a single SQL command without entering the shell:

```bash
cargo run --bin hematite_cli -- demo.db "SELECT * FROM users;"
```

## Advanced SQL Features

### Common Table Expressions

Name a subquery and reuse it across the statement.

```sql
WITH high_earners AS (
    SELECT id, name, salary FROM employees WHERE salary > 100000
)
SELECT h.name, d.name AS department
FROM high_earners h
INNER JOIN departments d ON d.id = h.department_id;
```

### Precision Math

Handle financial calculations accurately with exact decimals.

```sql
SELECT
    price * 1.05 as price_with_tax,
    CAST('12.34' AS DECIMAL(5, 2)) as exact_amount
FROM products;
```

## Schema Introspection

Easily explore your database structure using built-in metadata commands:

```sql
SHOW TABLES;
DESCRIBE users;
SHOW CREATE TABLE users;
EXPLAIN SELECT * FROM users WHERE id = 1;
```

## Current Limitations

Hematite already supports a broad SQL surface, but there are still important things users often
expect from an embedded database that are not supported yet.

### Application Ergonomics

- **Manual struct mapping**: Typed row mapping exists, but users currently need to implement `FromRow` manually.
- **No derive macros yet**: There is no `#[derive(FromRow)]`-style support yet.
- **No schema-from-struct workflow**: Hematite does not yet generate tables from Rust structs.
- **No built-in JSON row/result API**: There is no first-class JSON export/import convenience layer yet.

### Embedded App Workflow

- **No migration framework**: DDL is supported, but there is no built-in migration/versioning workflow.
- **No backup / restore helpers**: There is not yet a high-level API for backups, dumps, or restore flows.
- **No import/export tools**: CSV import/export and similar convenience tooling are not built in.
- **No async API**: The crate is synchronous today.

### SQL Surface Not Supported

- **No temporal types**: `DATE`, `TIME`, `DATETIME`, `TIME WITH TIME ZONE`, and `INTERVAL` do not exist, and neither do temporal functions or clock access (`NOW()`, `CURRENT_DATE`). Model timestamps as `TEXT` or integer epoch offsets.
- **No fixed-length or enumerated types**: `CHAR(n)`, `BINARY(n)`, `VARBINARY(n)`, and `ENUM(...)` are rejected. Use `VARCHAR(n)`, `TEXT`, and `BLOB`; enumerations can be a `TEXT` column with a `CHECK` constraint.
- **No views or triggers**
- **No window functions**: there is no `OVER (...)` clause.
- **No recursive CTEs**: `WITH` works, `WITH RECURSIVE` does not.
- **No JSON type or JSON operators**
- **No stored procedures or user-defined functions**
- **No generated columns**
- **No full information-schema subsystem**

None of those type names are reserved words, so `date`, `char`, `enum`, and
`binary` are usable as ordinary column and table names.

### Deployment Model

- **Embedded only**: Hematite is not a client/server database.
- **No users, roles, or network protocol**
- **No replication or clustering**

These are known limits, not accidents. The project intentionally favors a small, understandable,
embedded-first design over chasing every feature found in larger database systems.

## Documentation

- [Architecture](docs/architecture.md)
- [Codebase Guide](docs/codebase-guide.md)
- [SQL Dialect](docs/sql-dialect.md)
- [Database Internals Glossary](docs/glossary.md)
- [Query Execution Walkthrough](docs/query-walkthrough.md)
- [Storage Format Specification](docs/storage-format-spec.md)
- [Measuring Hematite](docs/performance.md)
- [Contributing](CONTRIBUTING.md)

## Status

Hematite is an evolving library. While it is stable enough for experimentation, the dialect and public API are still being refined as we head toward a 1.0 release.

## License

MIT. See [LICENSE](LICENSE).
