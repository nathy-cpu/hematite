# SQL Dialect

Hematite has its own SQL dialect.

It borrows ideas from mainstream SQL databases, and it accepts some MySQL-inspired syntax, but it
is not trying to be a strict clone of MySQL.

## Key Dialect Rules

### Keywords must be uppercase

This is intentional.

- `SELECT * FROM users;` is valid
- `select * from users;` is rejected

The parser tries to tell the user which keyword needs capitalization.

### Library-first, embedded-first design

The dialect is chosen for a small embedded database, not for wire compatibility with external
database servers.

## Currently Supported

This is the high-level supported surface today.

### Core DDL

- `CREATE TABLE`
- `DROP TABLE`
- `ALTER TABLE`
- `CREATE INDEX`
- `DROP INDEX`

### Metadata Objects

- views
- triggers
- named constraints

### DML

- `INSERT`
- `INSERT ... SELECT`
- `INSERT ... ON DUPLICATE KEY UPDATE`
- `SELECT`
- joined `UPDATE`
- joined `DELETE`
- `SELECT INTO`

### Query Features

- joins: cross, inner, left, right, full
- subqueries
- recursive CTEs
- aggregates
- window functions
- `CASE`
- scalar subqueries
- interval arithmetic

### Transactions

- `BEGIN`
- `COMMIT`
- `ROLLBACK`
- savepoints

### Introspection

- `SHOW TABLES`
- `SHOW VIEWS`
- `SHOW INDEXES`
- `SHOW TRIGGERS`
- `SHOW CREATE TABLE`
- `SHOW CREATE VIEW`
- `DESCRIBE`
- `EXPLAIN`

## Type System

Hematite uses an explicit custom type system.

### Integer Types

- `INT8`
- `INT16`
- `INT`
- `INT32` as alias of `INT`
- `INT64`
- `INT128`
- `UINT8`
- `UINT16`
- `UINT`
- `UINT32` as alias of `UINT`
- `UINT64`
- `UINT128`

### Floating-Point Types

- `FLOAT32`
- `FLOAT`
- `FLOAT64` as alias of `FLOAT`

### Exact and Textual Types

- `DECIMAL`
- `TEXT`
- `CHAR(n)`
- `VARCHAR(n)`
- `ENUM(...)`
- `BOOLEAN` (`BOOL` as alias)

### Binary Types

- `BINARY(n)`
- `VARBINARY(n)`
- `BLOB`

### Temporal and Interval Types

- `DATE`
- `TIME`
- `DATETIME`
- `TIME WITH TIME ZONE`
- `INTERVAL YEAR TO MONTH`
- `INTERVAL DAY TO SECOND`

## Supported But Simplified

These exist, but their semantics are deliberately smaller than in some larger systems.

- `CHARACTER SET` metadata is persisted, but runtime behavior is centered on collation, not full
  encoding negotiation
- text collations are supported in a focused way, not as a full collation ecosystem
- trigger bodies are single statements
- views are read-only

## Not Supported Yet

Important things that are still absent:

- stored procedures
- user-defined functions
- full information-schema style system catalogs

## Things The Project Does Not Intend To Chase

- server/network protocol support
- user and privilege management
- full compatibility with another database’s parser quirks
- every admin command from a server RDBMS
- extremely broad SQL dialect compatibility at the cost of code size and clarity

The project should stay small, understandable, and embedded-first.
