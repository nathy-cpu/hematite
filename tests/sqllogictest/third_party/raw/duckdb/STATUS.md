Upstream source:
- `duckdb/duckdb`
- focused raw mirror from `test/sql`

Current mirror status:
- mirrored successfully:
  - `test/sql/select`
  - `test/sql/filter`
  - `test/sql/setops`
  - `test/sql/cte`
  - `test/sql/subquery`
  - `test/sql/join`
  - `test/sql/window`
  - `test/sql/order`
  - `test/sql/limit`
  - `test/sql/insert`
  - `test/sql/update`
  - `test/sql/delete`

This raw mirror is intentionally broader than the promoted portable bucket.
Only files explicitly adapted into `third_party/portable` are executed by Hematite's
sqllogictest runner.

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
- `test/sql/filter/filter_cache.test`
  - partially adapted to:
    - `third_party/portable/filter_cache_from_duckdb.slt`
  - notes:
    - rewritten to use explicit rows instead of DuckDB's `generate_series` and verification pragmas while preserving the nested-filter shape
- `test/sql/filter/test_obsolete_filters.test`
  - partially adapted to:
    - `third_party/portable/obsolete_filters_from_duckdb.slt`
  - notes:
    - trimmed to cases that fit Hematite's primary-key requirement and current predicate syntax
- `test/sql/select/test_select_locking.test`
  - adapted to:
    - `third_party/portable/select_locking_errors_from_duckdb.slt`
- `test/sql/select/test_select_into.test`
  - adapted to:
    - `third_party/portable/select_into_errors_from_duckdb.slt`
  - notes:
    - Hematite now supports `SELECT INTO`; the promoted case keeps the portable table-creation behavior and avoids DuckDB-specific extras
- `test/sql/select/test_positional_reference.test`
  - adapted to:
    - `third_party/portable/positional_reference_errors_from_duckdb.slt`
  - notes:
    - kept as an unsupported-syntax regression because Hematite does not implement `#n` positional references
- `test/sql/select/test_select_alias_prefix_colon.test`
  - adapted to:
    - `third_party/portable/select_alias_prefix_colon_errors_from_duckdb.slt`
  - notes:
    - kept as an unsupported-syntax regression because Hematite does not implement DuckDB's `alias : expr` or `alias : source` syntax
- `test/sql/filter/test_alias_filter.test`
  - partially adapted to:
    - `third_party/portable/alias_filter_from_duckdb.slt`
  - notes:
    - aggregate-alias and repeated-alias variants were left out
    - Hematite now supports unqualified SELECT-list aliases in `WHERE`, while still giving source columns precedence
- `test/sql/setops/test_setops.test`
  - partially adapted to:
    - `third_party/portable/setops_core_from_duckdb.slt`
  - notes:
    - kept to table-backed UNION and DISTINCT set-operation cases
    - bare constant-select set-operation forms were left out because Hematite's parser currently expects a regular select source shape there
- `test/sql/setops/test_except.test`
  - partially adapted to:
    - `third_party/portable/setops_core_from_duckdb.slt`
  - notes:
    - NOCASE collation cases were left out because Hematite does not implement collation semantics
- `test/sql/subquery/table/test_table_subquery.test`
  - partially adapted to:
    - `third_party/portable/table_subquery_from_duckdb.slt`
- `test/sql/join/cross_product/test_cross_product.test`
  - partially adapted to:
    - `third_party/portable/cross_product_from_duckdb.slt`
  - notes:
    - `range(...)`-based large-count cases were left out because Hematite does not expose DuckDB's table-function syntax
- `test/sql/cte/test_cte.test`
  - partially adapted to:
    - `third_party/portable/cte_basics_from_duckdb.slt`
  - notes:
    - recursive/materialized, alias-list-heavy, and chained-CTE variants were left out for a smaller portable baseline because Hematite does not yet resolve later CTEs from earlier CTE definitions in the same `WITH` list
- `test/sql/subquery/table/test_aliasing.test`
  - adapted to:
    - `third_party/portable/subquery_aliasing_from_duckdb.slt`
  - notes:
    - rewritten to group by the underlying column rather than the projection alias because Hematite does not currently resolve grouped aliases there
- `test/sql/order/test_order_by.test`
  - partially adapted to:
    - `third_party/portable/order_by_core_from_duckdb.slt`
  - notes:
    - kept to portable ordering and offset cases
    - left out positional ordering because Hematite does not yet accept `ORDER BY 2, 1`
    - left out grouped expression ordering because Hematite does not yet support the DuckDB-style grouped alias/expression form cleanly
    - left out DuckDB-specific `SET` controls, controversial union-order alias cases, and non-integer-literal ordering behavior
- `test/sql/window/test_rank.test`
  - partially adapted to:
    - `third_party/portable/ranking_windows_from_duckdb.slt`
- `test/sql/window/test_dense_rank.test`
  - partially adapted to:
    - `third_party/portable/ranking_windows_from_duckdb.slt`
  - notes:
    - the promoted cases use explicit rows instead of `range(...)` and skip `NULLS FIRST/LAST` and frame-clause variants that go beyond Hematite's current window surface
