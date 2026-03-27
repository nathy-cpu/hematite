# Objectives 1 To 5 Execution Tracker

This document tracks the ordered execution run for:

1. Architecture review
2. Architecture cleanup and refactor
3. SQL interface cleanup and stepped script execution
4. CLI update
5. Practical-core SQL completion and richer types

Commit rule for this run:
- Each task ends with targeted validation.
- Each validated task ends with a short one-sentence commit message.
- Each block ends with a broader test pass.

## Target Boundary Matrix

- `storage`: may depend only on `storage` and `error`
- `btree`: may depend only on `btree`, `storage`, and `error`
- `catalog`: may depend only on `catalog`, `btree`, and `error`
- `query`: may depend only on `query`, `parser`, `catalog`, and `error`
- `parser`: may depend only on `parser` and `error`
- `sql`: may depend only on `sql`, `query`, `parser`, and `error`
- `main`: may depend only on `sql`, `error`, and std

Upper-layer strictness for this run is facade-based:
- `query` is allowed to bridge parser AST to catalog behavior.
- `sql` is allowed to expose user-facing facades over query.
- Other upper-layer files should not reach downward ad hoc.

## Current Audit Snapshot

Known violations or review targets at the start of the run:

- `parser/ast.rs` imports `catalog::Value` and `catalog::DataType`
- `catalog/engine.rs` imports `storage`
- `catalog/engine_metadata.rs` imports `storage`
- `sql/interface.rs` imports `catalog` and parser details directly
- `sql/connection.rs` imports `catalog`, `parser`, and `query` directly
- `sql/result.rs` imports `catalog::Value` directly
- `main.rs` uses `Connection` directly instead of the higher-level SQL facade

Current lower-stack status that should be preserved:

- `storage` does not import upper modules
- most `btree` files depend only on `storage` and `btree`
- most `catalog` files already sit above `btree`, but metadata persistence still leaks into `storage`

## Acceptance Criteria

- No forbidden imports remain in production code.
- A source-level architecture guard test enforces the boundary matrix.
- The SQL interface exposes a stepped script execution API.
- The CLI uses the SQL interface and supports semicolon-driven consecutive statements.
- Practical-core SQL additions land without changing pager or B-tree public APIs.

## Phase Checklist

### Phase 1 — Review

#### Block 1A — Tracking And Rules

- [x] Create this tracker document.
- [x] Record the current dependency graph from source imports.
- [x] Record public API reach-through points that bypass intended layers.
- [x] Freeze the allowed dependency matrix in code and docs.

#### Block 1B — Baseline Validation

- [x] Run a compile-only baseline before cleanup.
- [x] Run a full test baseline before cleanup.

### Phase 2 — Cleanup And Refactor

#### Block 2A — Parser Independence

- [ ] Move parser AST literals and type names off catalog-owned types.
- [ ] Add query-owned lowering from parser types into catalog/runtime types.
- [ ] Re-run parser and query validation after the split.

#### Block 2B — Catalog Over B-Tree Only

- [ ] Add a B-tree-owned metadata/blob persistence interface.
- [ ] Move catalog metadata persistence off direct storage usage.
- [ ] Remove remaining direct storage imports from catalog production files.

#### Block 2C — Upper-Layer Facades

- [ ] Remove direct parser and catalog reach-through from `sql/interface.rs`.
- [ ] Tighten `sql/connection.rs` so it acts as the SQL/query boundary rather than a dependency grab-bag.
- [ ] Narrow public re-exports that encourage boundary bypassing.

#### Block 2D — Concrete Guardrails

- [x] Add automated dependency-lint coverage.
- [ ] Add or update short architectural comments only where they explain non-obvious boundaries.

### Phase 3 — SQL Interface

#### Block 3A — Result Surface

- [ ] Introduce a unified per-statement execution result enum.
- [ ] Rebuild eager helpers on top of one core execution path.

#### Block 3B — Script Iterator

- [ ] Add lexer-driven SQL statement splitting.
- [ ] Add a stepped iterator over semicolon-delimited statements.
- [ ] Rebuild batch execution on top of the iterator.

### Phase 4 — CLI

#### Block 4A — One-Shot Mode

- [ ] Parse `db_path` and optional SQL script from CLI arguments.
- [ ] Execute one-shot scripts through the stepped interface.

#### Block 4B — Interactive Mode

- [ ] Enter REPL when only the database path is provided.
- [ ] Accumulate input until semicolon-terminated statements are complete.
- [ ] Execute consecutive statements from one input buffer safely.

### Phase 5 — Practical-Core SQL And Types

#### Block 5A — Richer Types

- [ ] Add `BIGINT`, `DECIMAL`, `BLOB`, `DATE`, and `DATETIME`.
- [ ] Extend runtime values, schema persistence, and row encoding for those types.
- [ ] Add binding, result access, comparison, and ordering support.

#### Block 5B — Expression Completion

- [ ] Add `CAST(expr AS type)`.
- [ ] Add `%` modulo and finish numeric coercion behavior.

#### Block 5C — Important Missing SQL

- [ ] Add `INSERT ... SELECT`.
- [ ] Add `INSERT ... ON DUPLICATE KEY UPDATE`.
- [ ] Add `EXPLAIN`.
- [ ] Add `DESCRIBE`.
- [ ] Add `SHOW TABLES`.

#### Block 5D — Stabilization

- [ ] Expand regression coverage for new types and statements.
- [ ] Re-run architecture and full-suite validation.

## Commit Log

- _pending_
