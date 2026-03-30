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

Promoted/adapted so far:
- `test/sql/select/test_select_empty_table.test`
  - adapted to:
    - `third_party/portable/select_empty_table_from_duckdb.slt`
- `test/sql/filter/test_constant_comparisons.test`
  - partially adapted to:
    - `third_party/portable/constant_filters_from_duckdb.slt`
  - note:
    - DuckDB cases that rely on bare `CASE ... END` as a `WHERE` predicate were not carried over because Hematite's current parser/executor requires a simpler predicate shape there
- `test/sql/filter/test_illegal_filters.test`
  - adapted to:
    - `third_party/portable/illegal_filters_from_duckdb.slt`
- `test/sql/filter/test_alias_filter.test`
  - intentionally not promoted
  - reason:
    - Hematite does not currently support using a SELECT-list alias inside `WHERE`
