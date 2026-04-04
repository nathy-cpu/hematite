# Contributing to Hematite

Thank you for your interest in contributing to Hematite! This project aims to stay small, readable, and embeddable, making it a great place to experiment with database internals.

## Core Philosophy

- **Embeddable-First**: Prioritize code size and clear boundaries over full wire-compatibility with larger RDBMSs.
- **Explicit and Predictable**: Prefer straightforward SQL semantics and strict type handling.
- **Readable and Small Code**: Maintain high code quality and clear documentation so the project remains "hackable" for others.

## How to Contribute

1. **Report Issues**: If you find a bug or have a suggestion, please open an issue.
2. **Submit Pull Requests**:
    - Fork the repository and create a new branch.
    - Keep your changes focused and concise.
    - Include tests for any new features or bug fixes.
    - Ensure all existing tests pass (`cargo test`).
    - Follow the existing code style and naming conventions.

## Areas for Contribution

We are specifically looking for contributions in the following areas:

### 1. Application Ergonomics

- **Typed Mapping Ergonomics**: Improve the `FromRow` experience with derive macros, name-based field mapping, and support for flatter nested struct patterns.
- **Schema-from-Struct Tooling**: Add ways to define table schemas from Rust types without introducing a heavy ORM.
- **JSON Export Helpers**: Add optional row/result JSON conversion for API and tooling use cases.
- **Backup / Import / Export Utilities**: Add practical embedded-database tooling such as dump/restore and CSV import/export helpers.

### 2. Embedded Database Workflow

- **Migrations Story**: Build a small, explicit migration/versioning workflow suitable for embedded applications.
- **Observability**: Add query timing, tracing hooks, and useful runtime stats for application developers.
- **Async / Integration Surface**: Explore carefully scoped async or integration helpers without turning the project into a framework.

### 3. SQL Dialect & Semantics

- **Additional Scalar Functions**: Add high-value missing functions such as more date/time helpers or aggregation helpers.
- **Deeper Text Features**: Expand collation handling and charset behavior carefully, without ballooning complexity.
- **Advanced SQL Surface**: Add only features that make sense for a small embedded database and fit the project philosophy.

### 4. Storage & Persistence

- **Pager Performance**: Optimize page cache eviction or implement background page flushing.
- **WAL Improvements**: Support more configuration for Write-Ahead Log checkpointing.
- **On-disk Integrity**: Add more robust checksums and physical page verification.
- **Backup / Compaction Support**: Add practical maintenance operations that embedded users expect.

### 5. Performance, Refactoring, & Disk Efficiency

- **Executor Hot Paths**: Reduce repeated expression evaluation and unnecessary allocations in the query executor.
- **Planner Efficiency**: Improve simple cost selection, join ordering, and access-path choice without making the planner overly complex.
- **Row and Key Encoding Size**: Make logical row encoding and canonical key encoding more compact where that can be done cleanly.
- **Decimal / Numeric Fast Paths**: Reduce conversion overhead in arithmetic, casts, aggregates, and comparisons.
- **Text / Binary Storage Efficiency**: Improve handling of variable-width values so common cases use less space and less copying.
- **B-tree Rebalance Costs**: Reduce page churn and unnecessary movement during split/merge/rebalance operations.
- **File Growth and Reuse**: Improve free-page reuse, compaction behavior, and long-running churn behavior to keep database files tighter.
- **Read / Write Throughput Measurement**: Add and improve benchmarks around reads per second, writes per second, and file-size efficiency.
- **Targeted Refactors for Simplicity**: Simplify large modules where that improves maintainability and makes future optimization easier.

### 6. Testing & Reliability

- **Property-Based Testing**: Use `proptest` to verify data types, arithmetic edge cases, and planner/executor invariants.
- **Stress Testing**: Create benchmarks and stress tests for the pager and B-tree under load.
- **Application-Oriented Regression Coverage**: Add tests around persistence, typed mapping, CLI behavior, and real embedded workflows.

### 7. Documentation & DX

- **Library-Focused Docs**: Improve onboarding for library users first, not just database internals.
- **Examples**: Add small realistic examples for common embedded use cases.
- **CLI Enhancements**: Improve `hematite_cli` without letting it become a separate product focus.

---

## Feedback & Questions

If you have questions about the codebase or want to discuss a large feature before starting work, please reach out via the issue tracker.
