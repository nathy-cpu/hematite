# Hematite SQL Dialect Reference

This document defines the SQL dialect supported by Hematite, detailing the type system, supported functions, syntax rules, and limitations.

---

## 1. Key Dialect Rules

### Uppercase Keyword Casing

To keep lexing and parsing simple and unambiguous, Hematite enforces uppercase casing for all SQL keywords.

* **Valid**: `SELECT * FROM users;`
* **Rejected**: `select * from users;` fails with `Keyword 'select' must be capitalized as 'SELECT'`.

Keywords are matched case-sensitively, so a lowercase keyword lexes as an ordinary identifier; the parser reports it when it appears where a keyword belongs. What that means in practice:

* `TRUE`, `FALSE` and `NULL` must be uppercase. `WHERE true` reads `true` as a column name.
* Aggregate names (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) are keywords and must be uppercase.
* Scalar function names are not keywords and match case-insensitively: `lower(name)` and `LOWER(name)` are the same call.
* Clause words that appear only in one context — `ENGINE`, `CHARSET`, `CHARACTER`, `COLLATE`, `USING`, `BTREE`, `HASH` — are matched by their exact uppercase text.

### Statement Shape

* Every statement ends with `;`.
* `SELECT` always has a `FROM` clause; `SELECT 1;` is a parse error.

### Logical Operator Spellings

`&&`, `||` and `!` are accepted as `AND`, `OR` and `NOT`. In particular **`||` is logical OR, not string concatenation** — use `CONCAT(a, b)` to join strings.

### Unified Exact Numeric System

Hematite handles all exact numeric calculations (`INT8` through `INT64`, unsigned counterparts, and `DECIMAL`) using an intermediate high-precision decimal representation:

1. **Promotion to Decimal**: Any arithmetic operation involving exact numerics (e.g. adding a `UINT8` to an `INT64`) promotes both operands to `DecimalValue` before calculating, so a result outside its operands' type is not an error: two `INT8` values of 100 add to 200, and a `UINT8` of 0 minus 1 is -1.
2. **Automatic Down-scaling**: The result is then narrowed to the smallest integer type that holds it (`i32`, then `i64`), or kept as a decimal if it has a fractional part — `DECIMAL` `2.00 + 1` is `3`. A result that fits in no 64-bit integer type is an error (`Arithmetic overflowed the supported integer range`).
3. **Division**: Integer division truncates (`7 / 2` is `3`). Division involving a `DECIMAL` keeps 18 fractional digits and rounds half-up. A literal with a decimal point, like `2.0`, is a `FLOAT`, so `5 / 2.0` is floating-point division.

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
  SELECT COALESCE(nickname, 'default_val') FROM users;
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
* **`SUBSTRING(str, start_pos, [length])`** / **`SUBSTR`**: Extracts a substring starting at index `start_pos` (1-indexed).
* **`LEFT(str, length)` / `RIGHT(str, length)`**: Extracts `length` characters from the start or end.
* **`REPLACE(str, target, replacement)`**: Replaces all instances of `target` with `replacement` in `str`.
* **`REPEAT(str, count)`**: Repeats `str` exactly `count` times.
* **`REVERSE(str)`**: Reverses the character order.
* **`LOCATE(substr, str, [start_pos])`**: Returns the 1-based index of the first occurrence of `substr` in `str`.
* **`HEX(blob)` / `UNHEX(hex_str)`**: Encodes a value as hexadecimal text, or decodes hexadecimal text into a `BLOB`.

### Math Functions

* **`ABS(val)`**: Returns the absolute value of `val`.
* **`ROUND(val, [decimal_places])`**: Rounds `val` to `decimal_places` (defaults to 0).
* **`CEIL(val)` / `CEILING(val)` / `FLOOR(val)`**: Rounds up or down to the nearest integer.
* **`POWER(base, exponent)`** / **`POW`**: Raises `base` to the power of `exponent`.

### Aggregate Functions

* **`COUNT(*)` / `COUNT(column)`**
* **`SUM(column)`**
* **`AVG(column)`**
* **`MIN(column)`**
* **`MAX(column)`**

There are no temporal functions, and no clock access anywhere in the engine —
`NOW()`, `CURRENT_DATE`, `CURRENT_TIMESTAMP`, and `EXTRACT` do not exist.

---

## 4. Introspection

* `SHOW TABLES;`, `SHOW INDEXES;` / `SHOW INDEXES FROM t;`, `DESCRIBE t;` and `SHOW CREATE TABLE t;` return their answers as ordinary result rows.
* `EXPLAIN <statement>;` plans the statement without running it and returns two rows with columns `kind` and `detail`: `node`, the plan's description (the chosen access path — `RowIdLookup`, `PrimaryKeyLookup`, `SecondaryIndexLookup`, `FullTableScan` or `JoinScan` — with the projection, filter, ordering and limits), and `estimated_cost`, the planner's `LogEst` estimate.

---

## 5. Syntactic Constraints and Limitations

* **No Views or Triggers**: Neither can be created; `CREATE VIEW` and `CREATE TRIGGER` are rejected.
* **No Window Functions**: There is no `OVER (...)` clause, so `ROW_NUMBER`, `RANK`, and windowed aggregates are unavailable.
* **No Recursive CTEs**: `WITH` is supported, but `WITH RECURSIVE` is not.
* **No Temporal or Fixed-Length Types**: See "Unsupported Types" in section 2.
* **No Stored Procedures / User-Defined Functions**: Procedural languages and custom extensions are not supported. All logic must be coordinated via client queries.
