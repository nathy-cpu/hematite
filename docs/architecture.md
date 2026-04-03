# Architecture

Hematite is organized as a layered embedded database:

```text
sql -> parser -> query -> catalog -> btree -> storage
```

The project intentionally tries to preserve that shape. Higher layers should not reach down past
their intended boundary, and lower layers should not depend on upper layers.

## Layers

### `storage`

Lowest-level persistence primitives.

Responsibilities:

- pager
- rollback journal and WAL handling
- page allocation and free-page reuse
- rowid-table page primitives
- on-disk integrity checks

This layer should know nothing about SQL, schemas, or query semantics.

### `btree`

Generic typed and byte-oriented B-tree helpers over storage pages.

Responsibilities:

- key/value tree operations
- split, merge, rebalance, and cursor movement
- overflow handling for larger payloads
- typed codec boundaries over byte storage

This layer should not know about SQL tables or query planning.

### `catalog`

Logical database metadata and row typing.

Responsibilities:

- schema objects: tables, columns, indexes, views, triggers
- value types and logical row encoding
- metadata persistence
- named constraints and index metadata

This is where database structure becomes relational, but it is still not responsible for parsing or
planning SQL.

### `query`

Planning, validation, execution, coercion, and metadata shaping.

Responsibilities:

- lower parser-owned types into runtime/catalog types
- validate statements against current schema
- build query plans
- execute expressions, joins, aggregates, windows, and mutations
- shape metadata/introspection output

This layer is where most SQL semantics live.

### `parser`

Owns SQL syntax.

Responsibilities:

- lexing
- AST construction
- parser-owned type and literal names
- structural parse errors

The parser should stay independent from catalog/runtime semantics. Semantic lowering belongs in
`query`.

### `sql`

Public library boundary.

Responsibilities:

- `Hematite` and `Connection`
- prepared statements
- transaction and savepoint coordination
- stepping through multi-statement scripts
- user-facing SQL error presentation
- CLI-facing execution helpers

## Execution Flow

For a typical statement:

1. SQL text enters through `sql::Connection`.
2. `parser` produces an AST.
3. `query::validation` checks the AST against the active schema.
4. `query::planner` builds an execution strategy.
5. `query::executor` performs the read or mutation.
6. `catalog` persists metadata and typed rows through `btree` and `storage`.

## Transactions

Transaction behavior is coordinated at the SQL connection boundary, while persistence and recovery
are implemented lower in the stack.

Current model:

- autocommit by default
- explicit `BEGIN` / `COMMIT` / `ROLLBACK`
- savepoints
- rollback-journal and WAL modes

## Design Intent

The project aims to remain:

- embeddable
- understandable
- hackable
- small enough for experimentation

That means preferring straightforward code and clear boundaries over trying to mimic every behavior
of a larger database system.
