Upstream source:
- `duckdb/duckdb`
- focused raw mirror from `test/sql`

Current mirror status:
- mirrored successfully:
  - `test/sql/select`
  - `test/sql/filter`
- target directories identified for later expansion:
  - `test/sql/join`
  - `test/sql/order`
  - `test/sql/limit`
  - `test/sql/setops`
  - `test/sql/subquery`
  - `test/sql/cte`
  - `test/sql/window`
  - `test/sql/insert`
  - `test/sql/update`
  - `test/sql/delete`

Notes:
- DuckDB uses `.test` / `.test_slow` files rather than `.slt`.
- These files are kept as raw reference only; they are not executed by Hematite's
  sqllogictest runner.
- The mirrored DuckDB files include many engine-specific features and directives,
  so promotion will likely happen file-by-file or through adapted portable extracts.
