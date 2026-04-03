# Codebase Guide

This document is the fast map of where things live and what they are responsible for.

## Crate Entry Points

- [src/lib.rs](/home/nathnael/dev/Rust/hematite/src/lib.rs)
  Public exports and crate-level overview.
- [src/main.rs](/home/nathnael/dev/Rust/hematite/src/main.rs)
  Small CLI wrapper around the library API.

## Public SQL API

- [src/sql/interface.rs](/home/nathnael/dev/Rust/hematite/src/sql/interface.rs)
  High-level `Hematite` facade and `FromValue` conversions.
- [src/sql/connection.rs](/home/nathnael/dev/Rust/hematite/src/sql/connection.rs)
  Main SQL boundary. Coordinates parsing, validation, planning, execution, transactions, and
  script stepping.
- [src/sql/result.rs](/home/nathnael/dev/Rust/hematite/src/sql/result.rs)
  Query results, rows, and statement result types.
- [src/sql/script.rs](/home/nathnael/dev/Rust/hematite/src/sql/script.rs)
  Token-aware SQL script splitting and iteration.

If you are integrating Hematite into an application, start here.

## Parsing

- [src/parser/lexer.rs](/home/nathnael/dev/Rust/hematite/src/parser/lexer.rs)
  Tokenization.
- [src/parser/parser.rs](/home/nathnael/dev/Rust/hematite/src/parser/parser.rs)
  AST parsing.
- [src/parser/ast.rs](/home/nathnael/dev/Rust/hematite/src/parser/ast.rs)
  SQL AST definitions.
- [src/parser/types.rs](/home/nathnael/dev/Rust/hematite/src/parser/types.rs)
  Parser-owned SQL type names and literal values.

If you are changing syntax or the SQL dialect, start here.

## Query Layer

- [src/query/validation.rs](/home/nathnael/dev/Rust/hematite/src/query/validation.rs)
  Schema-aware semantic checks.
- [src/query/planner.rs](/home/nathnael/dev/Rust/hematite/src/query/planner.rs)
  Query plan selection.
- [src/query/executor.rs](/home/nathnael/dev/Rust/hematite/src/query/executor.rs)
  Execution engine for expressions, scans, joins, aggregates, windows, and mutations.
- [src/query/lowering.rs](/home/nathnael/dev/Rust/hematite/src/query/lowering.rs)
  Parser-to-runtime lowering boundary.
- [src/query/metadata.rs](/home/nathnael/dev/Rust/hematite/src/query/metadata.rs)
  Shared helpers for `SHOW`, `DESCRIBE`, and related metadata output.

If you are changing SQL semantics, runtime coercion, or execution behavior, this is the core area.

## Catalog Layer

- [src/catalog/types.rs](/home/nathnael/dev/Rust/hematite/src/catalog/types.rs)
  Runtime data types and values.
- [src/catalog/column.rs](/home/nathnael/dev/Rust/hematite/src/catalog/column.rs)
  Column definitions, validation, defaults, text metadata.
- [src/catalog/table.rs](/home/nathnael/dev/Rust/hematite/src/catalog/table.rs)
  Table metadata and constraint structures.
- [src/catalog/schema.rs](/home/nathnael/dev/Rust/hematite/src/catalog/schema.rs)
  Schema object registry.
- [src/catalog/catalog.rs](/home/nathnael/dev/Rust/hematite/src/catalog/catalog.rs)
  Main catalog facade.
- [src/catalog/serialization.rs](/home/nathnael/dev/Rust/hematite/src/catalog/serialization.rs)
  Logical row and key encoding.

If you are changing schema objects, data types, or logical storage formats, start here.

## Lower Storage Stack

- [src/btree](/home/nathnael/dev/Rust/hematite/src/btree)
  Generic tree structures and typed/byte trees.
- [src/storage](/home/nathnael/dev/Rust/hematite/src/storage)
  Pager, journaling, page layout, and low-level persistence.

If you are working on durability, recovery, or physical structure, this is the area.

## Test Layout

Most tests live inline by subsystem:

- parser tests in `src/parser/tests.rs`
- query tests in `src/query/tests.rs`
- catalog tests in `src/catalog/tests.rs`
- storage tests in `src/storage/tests.rs`
- SQL integration-style tests in `src/sql/tests.rs`
- architecture guard in `src/architecture_tests.rs`

## Where To Start For Common Tasks

### Add new SQL syntax

1. update lexer tokens if needed
2. update parser AST / parser logic
3. update lowering if new type/value mapping is needed
4. update validation and executor
5. add parser and SQL tests

### Add a new runtime type

1. update parser-owned type names
2. update catalog `DataType` and `Value`
3. update column validation/defaults
4. update logical row/key serialization
5. update casts, coercion, functions, and result accessors
6. add round-trip and SQL tests

### Add a new metadata command

1. parse statement in `parser`
2. implement shared shaping in `query::metadata`
3. route execution in `sql::connection`
4. add SQL tests
