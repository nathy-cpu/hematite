# Hematite SQL Dialect Reference

This document defines the SQL dialect supported by Hematite, detailing the type system, supported functions, syntax rules, and limitations.

---

## 1. Key Dialect Rules

### Uppercase Keyword Casing

To keep lexing and parsing simple and unambiguous, Hematite enforces uppercase casing for all SQL keywords.

* **Valid**: `SELECT * FROM users;`
* **Rejected**: `select * from users;`
* The parser detects lowercased keywords and provides descriptive errors showing the expected uppercase keyword.

### Unified Exact Numeric System

Hematite handles all exact numeric calculations (`INT8` through `INT64`, unsigned counterparts, and `DECIMAL`) using an intermediate high-precision decimal representation:

1. **Promotion to Decimal**: Any arithmetic operation involving exact numerics (e.g. adding a `UINT8` to an `INT64`) automatically promotes both operands to `DecimalValue` before performing the calculation, preventing overflow or underflow.
2. **Automatic Down-scaling**: After the calculation, the engine evaluates the resulting value and down-scales it back to the smallest integer type capable of safely representing the value (e.g., trying to fit into `i32`, then `i64`).
3. **Rounding**: Divisions on exact decimal types employ nearest-neighbor rounding (round-half-up).

---

## 2. Supported Data Types

### Integer Types

* Signed: `INT8` (1 byte), `INT16` (2 bytes), `INT`/`INT32` (4 bytes), `INT64` (8 bytes).
* Unsigned: `UINT8` (1 byte), `UINT16` (2 bytes), `UINT`/`UINT32` (4 bytes), `UINT64` (8 bytes).

### Floating-Point Types

* `FLOAT32` (single-precision IEEE 754 float).
* `FLOAT`/`FLOAT64` (double-precision IEEE 754 float).

### Exact and Textual Types

* `DECIMAL` / `DECIMAL(precision, scale)`: Fixed-point exact decimals.
* `TEXT`: Variable-length UTF-8 encoded string.
* `VARCHAR(n)`: Variable-length character string with maximum length `n`.
* `BOOLEAN`/`BOOL`: Boolean states (`TRUE` or `FALSE`).

`TEXT` and `VARCHAR(n)` accept the optional `CHARACTER SET` and `COLLATE` column
modifiers.

### Binary Types

* `BLOB`: Binary object. Binary literals are written in hexadecimal form as `X'414243'`.

### Unsupported Types

Hematite has no temporal, fixed-length, or enumerated types. `DATE`, `TIME`,
`DATETIME`, `TIME WITH TIME ZONE`, `INTERVAL`, `CHAR(n)`, `ENUM(...)`,
`BINARY(n)`, and `VARBINARY(n)` are all rejected at parse time. None of these
words are reserved, so they remain available as ordinary identifiers — a column
may be named `date` or `binary`.

Model dates and times as `TEXT` (for example ISO-8601 strings) or as integer
epoch offsets, fixed-length strings as `VARCHAR(n)`, enumerations as `TEXT`
guarded by a `CHECK` constraint, and binary data as `BLOB`.

---

## 3. Supported SQL Functions

### Comparison & Conditional Functions

* **`COALESCE(val1, val2, ...)`**: Returns the first non-null argument.

  ```sql
  SELECT COALESCE(null_col, 'default_val');
  ```

* **`IFNULL(val, default)`**: Evaluates to `default` if `val` is null; equivalent to `COALESCE` with two arguments.
* **`NULLIF(val1, val2)`**: Returns `NULL` if `val1 = val2`; otherwise returns `val1`.
* **`GREATEST(val1, val2, ...)`**: Returns the largest value in the argument list.
* **`LEAST(val1, val2, ...)`**: Returns the smallest value in the argument list.

### String Functions

* **`LOWER(str)` / `UPPER(str)`**: Transforms characters to lower/upper case.
* **`TRIM(str)`**: Strips leading and trailing white space.
* **`LENGTH(str)`**: Returns character count (for text) or byte count (for blobs).
* **`OCTET_LENGTH(str)`**: Returns the byte length of the string.
* **`BIT_LENGTH(str)`**: Returns the bit length of the string.
* **`CONCAT(s1, s2, ...)`**: Concatenates arguments into a single string.
* **`CONCAT_WS(separator, s1, s2, ...)`**: Concatenates arguments using a custom separator.
* **`SUBSTRING(str, start_pos, [length])`**: Extracts a substring starting at index `start_pos` (1-indexed).
* **`LEFT(str, length)` / `RIGHT(str, length)`**: Extracts `length` characters from the start or end.
* **`REPLACE(str, target, replacement)`**: Replaces all instances of `target` with `replacement` in `str`.
* **`REPEAT(str, count)`**: Repeats `str` exactly `count` times.
* **`REVERSE(str)`**: Reverses the character order.
* **`LOCATE(substr, str, [start_pos])`**: Returns the 1-based index of the first occurrence of `substr` in `str`.
* **`HEX(blob)` / `UNHEX(hex_str)`**: Encodes binary values to hexadecimal text or decodes hexadecimal strings (little-endian representation).

### Math Functions

* **`ABS(val)`**: Returns the absolute value of `val`.
* **`ROUND(val, [decimal_places])`**: Rounds `val` to `decimal_places` (defaults to 0).
* **`CEIL(val)` / `FLOOR(val)`**: Rounds up or down to the nearest integer.
* **`POWER(base, exponent)`**: Raises `base` to the power of `exponent`.

### Aggregate Functions

* **`COUNT(*)` / `COUNT(column)`**
* **`SUM(column)`**
* **`AVG(column)`**
* **`MIN(column)`**
* **`MAX(column)`**

There are no temporal functions, and no clock access anywhere in the engine —
`NOW()`, `CURRENT_DATE`, `CURRENT_TIMESTAMP`, and `EXTRACT` do not exist.

---

## 4. Syntactic Constraints and Limitations

* **No Views or Triggers**: Neither can be created; `CREATE VIEW` and `CREATE TRIGGER` are rejected.
* **No Window Functions**: There is no `OVER (...)` clause, so `ROW_NUMBER`, `RANK`, and windowed aggregates are unavailable.
* **No Recursive CTEs**: `WITH` is supported, but `WITH RECURSIVE` is not.
* **No Temporal or Fixed-Length Types**: See "Unsupported Types" in section 2.
* **No Stored Procedures / User-Defined Functions**: Procedural languages and custom extensions are not supported. All logic must be coordinated via client queries.
