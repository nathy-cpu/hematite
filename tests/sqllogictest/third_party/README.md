This directory contains sqllogictest cases adapted from external upstream sources.

Layout:
- `portable/`: imported or adapted cases that should pass in Hematite today
- `unsupported/`: upstream cases kept for reference but intentionally skipped by the test runner

Current upstream source:
- `risinglightdb/sqllogictest-rs` test corpus

Notes:
- Some upstream files use harness directives or DB-specific conditions that Hematite does not support yet.
- Portable imports may be lightly adapted so they exercise the same behavior using Hematite's supported SQL surface.
