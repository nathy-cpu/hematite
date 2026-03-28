Upstream source:
- `risinglightdb/sqllogictest-rs`
- mirrored from `tests/slt`

Triage status for the currently imported raw files:

- `basic.slt`
  - promoted in adapted form to:
    - `third_party/portable/basic_from_sqllogictest_rs.slt`
- `rowsort.slt`
  - covered in adapted form by:
    - `third_party/portable/basic_from_sqllogictest_rs.slt`
- `valuesort.slt`
  - promoted in adapted form to:
    - `third_party/portable/valuesort_from_sqllogictest_rs.slt`
- `condition.slt`
  - intentionally unsupported
  - reason: uses `onlyif` / `skipif` directives
- `file_level_sort_mode.slt`
  - intentionally unsupported
  - reason: uses `control sortmode ...` directive
- `error_sqlstate.slt`
  - partially adapted in:
    - `third_party/portable/errors_from_sqllogictest_rs.slt`
  - raw file itself is unsupported
  - reason: relies on SQLSTATE-aware error matching
- `error_sqlstate_parsing.slt`
  - intentionally unsupported
  - reason: parser/harness-level SQLSTATE matching coverage, not portable SQL behavior
- `include/include_1.slt`
  - intentionally unsupported
  - reason: uses `include ...` directives
- `include/include_2.slt.part`
  - intentionally unsupported
  - reason: dependency of `include`-directive tests
- `include/include/a.slt.part`
  - intentionally unsupported
  - reason: dependency of `include`-directive tests
- `include/include/b.slt.part`
  - intentionally unsupported
  - reason: dependency of `include`-directive tests
- `connection/counter.slt`
  - intentionally unsupported
  - reason: depends on multi-connection sqllogictest harness semantics and custom `counter()` function
- `retry.slt`
  - intentionally unsupported
  - reason: depends on `retry` and `system` harness directives plus custom `counter()` function

Promotion rule:
- promote only files whose behavior is portable SQL result checking
- adapt syntax only when needed for Hematite's current SQL surface
- keep harness-directive and harness-feature files in `raw` / `unsupported`
