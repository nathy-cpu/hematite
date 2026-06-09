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

Hematite handles all exact numeric calculations (`INT8` through `INT128`, unsigned counterparts, and `DECIMAL`) using an intermediate high-precision decimal representation:

1. **Promotion to Decimal**: Any arithmetic operation involving exact numerics (e.g. adding a `UINT8` to an `INT128`) automatically promotes both operands to `DecimalValue` before performing the calculation, preventing overflow or underflow.
2. **Automatic Down-scaling**: After the calculation, the engine evaluates the resulting value and down-scales it back to the smallest integer type capable of safely representing the value (e.g., trying to fit into `i32`, then `i64`, then `i128`).
3. **Rounding**: Divisions on exact decimal types employ nearest-neighbor rounding (round-half-up).

---

## 2. Supported Data Types

### Integer Types

* Signed: `INT8` (1 byte), `INT16` (2 bytes), `INT`/`INT32` (4 bytes), `INT64` (8 bytes), `INT128` (16 bytes).
* Unsigned: `UINT8` (1 byte), `UINT16` (2 bytes), `UINT`/`UINT32` (4 bytes), `UINT64` (8 bytes), `UINT128` (16 bytes).

### Floating-Point Types

* `FLOAT32` (single-precision IEEE 754 float).
* `FLOAT`/`FLOAT64` (double-precision IEEE 754 float).

### Exact and Textual Types

* `DECIMAL` / `DECIMAL(precision, scale)`: Fixed-point exact decimals.
* `TEXT`: Variable-length UTF-8 encoded string.
* `CHAR(n)`: Fixed-length blank-padded character string of length `n`.
* `VARCHAR(n)`: Variable-length character string with maximum length `n`.
* `ENUM('val1', 'val2', ...)`: Internally persisted string enumeration.
* `BOOLEAN`/`BOOL`: Boolean states (`TRUE` or `FALSE`).

### Binary Types

* `BINARY(n)`: Fixed-length zero-padded binary array of length `n`.
* `VARBINARY(n)`: Variable-length binary array with maximum length `n`.
* `BLOB`: Large binary object.

### Temporal and Interval Types

* `DATE`: Calendar date (YYYY-MM-DD).
* `TIME`: Time of day (HH:MM:SS.FFF).
* `DATETIME`: Combined timestamp.
* `TIME WITH TIME ZONE`: Time with UTC offset.
* `INTERVAL YEAR TO MONTH`: Period in years and months.
* `INTERVAL DAY TO SECOND`: Period in days, hours, minutes, and seconds.

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

### Temporal Functions

* **`DATE(datetime)` / `TIME(datetime)`**: Extracts the date or time component from a `DATETIME` value.
* **`YEAR(val)` / `MONTH(val)` / `DAY(val)`**: Extracts the calendar component from a `DATE` or `DATETIME`.
* **`HOUR(val)` / `MINUTE(val)` / `SECOND(val)`**: Extracts the time component.
* **`TIME_TO_SEC(time)` / `SEC_TO_TIME(seconds)`**: Converts a time value to/from seconds since midnight.
* **`UNIX_TIMESTAMP([datetime])`**: Returns the epoch seconds value.

### Aggregate Functions

* **`COUNT(*)` / `COUNT(column)`**
* **`SUM(column)`**
* **`AVG(column)`**
* **`MIN(column)`**
* **`MAX(column)`**

### Window Functions

* **`ROW_NUMBER() OVER (...)`**: Assigns sequential integers starting at 1.
* **`RANK() OVER (...)`**: Assigns rank integers, leaving gaps in case of ties.
* **`DENSE_RANK() OVER (...)`**: Assigns dense rank integers, leaving no gaps.
* **`SUM/AVG/COUNT/MIN/MAX(...) OVER (...)`**: Computes the aggregate over the partition window.

---

## 4. Syntactic Constraints and Limitations

* **Single-Statement Triggers**: Trigger bodies are limited to a single SQL mutation statement; trigger logic cascades must be modeled without procedural blocks.
* **Read-Only Views**: Views can be created and queried, but they are read-only and cannot accept write modifications directly.
* **No Stored Procedures / User-Defined Functions**: Procedural languages and custom extensions are not supported. All logic must be coordinated via client queries.
