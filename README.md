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
hematite = "0.1"
```

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
        println!("User {}: {}", row.get_as::<i32>(0)?, row.get_as::<String>(1)?);
    }

    Ok(())
}
```

### Transactions and Savepoints

```rust
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
fn find_user(db: &mut Hematite, user_id: i32) -> hematite::Result<()> {
    let mut stmt = db.prepare("SELECT name FROM users WHERE id = ?;")?;
    
    // Bind parameters by index (starting at 1)
    stmt.bind(1, hematite::Value::Integer(user_id))?;
    
    let result = db.execute_prepared(&mut stmt)?;
    // ... process result
    
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
+------+------+---------+
| id   | name | balance |
+------+------+---------+
| 1    | Ada  | 1000.50 |
| 2    | Bob  | 250.75  |
+------+------+---------+
2 row(s)
```

## CLI Usage

Hematite ships with a lightweight CLI tool (`hematite_cli`) for schema exploration and ad-hoc queries.

### Interactive Shell
Start the interactive REPL with a database file:

```bash
cargo run --bin hematite_cli -- demo.db
```

### One-off Commands
Execute a single SQL command without entering the shell:

```bash
cargo run --bin hematite_cli -- demo.db "SELECT * FROM users;"
```

## Advanced SQL Features

Hematite supports powerful SQL constructs that are often missing in "small" databases:

### Recursive CTEs
Find all subordinates in an organizational hierarchy.

```sql
WITH RECURSIVE subordinates AS (
    SELECT id, name, manager_id FROM employees WHERE name = 'Alice'
    UNION ALL
    SELECT e.id, e.name, e.manager_id 
    FROM employees e
    INNER JOIN subordinates s ON s.id = e.manager_id
)
SELECT * FROM subordinates;
```

### Window Functions
Calculate a running total of sales.

```sql
SELECT 
    sale_date, 
    amount, 
    SUM(amount) OVER (ORDER BY sale_date) as running_total
FROM sales;
```

### Precision Math and Intervals
Handle financial calculations and temporal offsets accurately.

```sql
SELECT 
    price * 1.05 as price_with_tax,
    current_date + INTERVAL '1-02' YEAR TO MONTH as expiry_date
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

## Documentation

- [Architecture](docs/architecture.md)
- [Codebase Guide](docs/codebase-guide.md)
- [SQL Dialect](docs/sql-dialect.md)

## Status

Hematite is an evolving library. While it is stable enough for experimentation, the dialect and public API are still being refined as we head toward a 1.0 release.

## License

MIT. See [LICENSE](LICENSE).
