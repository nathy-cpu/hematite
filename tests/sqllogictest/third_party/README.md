This directory contains sqllogictest cases adapted from external upstream sources.

Layout:
- `portable/`: imported or adapted cases that should pass in Hematite today
- `raw/`: bulk-imported upstream files kept as a reference mirror
- `unsupported/`: upstream cases kept for reference but intentionally skipped by the test runner

Current upstream source:
- `risinglightdb/sqllogictest-rs` test corpus

Notes:
- Some upstream files use harness directives or DB-specific conditions that Hematite does not support yet.
- Portable imports may be lightly adapted so they exercise the same behavior using Hematite's supported SQL surface.
- The sqllogictest runner reads `tests/sqllogictest/manifest.txt`, so `raw/` and `unsupported/` stay available for triage without being executed automatically.
