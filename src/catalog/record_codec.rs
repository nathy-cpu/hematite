//! The record encoding: how a row's values become bytes.
//!
//! Reached through [`crate::catalog::serialization::RowCodec`], which is the name the
//! rest of the catalog calls.
//!
//! # Why the width is per value
//!
//! Spending a fixed width on every value regardless of magnitude — a one-byte tag
//! plus four bytes for any `Integer`, plus a fixed record header — costs 58 bytes to
//! hold the ten bytes of information in a row of ten small integers. The serial type
//! carries the width, so a small value pays for a small value.
//!
//! # Layout
//!
//! ```text
//! record := [varint header_len] [serial_0] .. [serial_n-1] [body_0] .. [body_n-1]
//! ```
//!
//! `header_len` counts itself and every serial type, so it doubles as the offset of
//! the first body — and the field count falls out of walking the header rather than
//! being stored. Each serial type encodes both what the value is *and* how many
//! bytes its body occupies, so the offset of field `i` is computable from the header
//! alone, without reading a single body byte. That is what makes it possible to read
//! one column of a wide row without touching the rest, and it is why every
//! variable-length family folds its length into the code.
//!
//! # Serial types
//!
//! Fixed-size, codes 0 to 35:
//!
//! | code | value | body |
//! |---|---|---|
//! | 0 | `Null` | 0 |
//! | 1 | `Boolean(false)` | 0 |
//! | 2 | `Boolean(true)` | 0 |
//! | 3 | `Float32` | 4, big-endian bits |
//! | 4 | `Float` | 8, big-endian bits |
//! | 5 | `Float(+0.0)` | 0 |
//! | 6, 7 | reserved | — |
//! | 8 + w, w in 0..=4 | `Integer(i32)` | w, big-endian, sign-extended |
//! | 13 + w, w in 0..=8 | `BigInt(i64)` | w, big-endian, sign-extended |
//! | 22 + w, w in 0..=4 | `UInteger(u32)` | w, big-endian, zero-extended |
//! | 27 + w, w in 0..=8 | `UBigInt(u64)` | w, big-endian, zero-extended |
//!
//! Variable-size, codes 36 and up, three interleaved families:
//!
//! | code | value | body |
//! |---|---|---|
//! | 36 + 3L | `Text`, L bytes | L bytes of UTF-8 |
//! | 37 + 3L | `Blob`, L bytes | L bytes |
//! | 38 + 3L | `Decimal`, L bytes | L bytes, see [`encode_decimal_body`] |
//!
//! `Text` takes the lowest family because it is the most common, so any string up
//! to 30 bytes still has a one-byte serial type.
//!
//! Integer width is minimal but the *variant* is preserved, because `Value` treats
//! `Integer(5)` and `BigInt(5)` as different values (they compare unequal and report
//! different `data_type()`s), so collapsing them would change query results.
//!
//! # What it costs
//!
//! Ten small integers: header is 1 byte of `header_len` plus 10 one-byte serial
//! types, bodies are 1 byte each — 21 bytes, against 58 at a fixed width.

// `RecordRef`'s borrowed accessors (`field`, `iter`, `ValueRef`) are exercised by
// tests but have no production caller: every production read goes through `decode`
// or a `ProjectionPlan`, both of which hand back owned `Value`s.
#![allow(dead_code)]

use crate::btree::varint;
use crate::catalog::{DecimalValue, Value};
use crate::error::{HematiteError, Result};

const CODE_NULL: u64 = 0;
const CODE_FALSE: u64 = 1;
const CODE_TRUE: u64 = 2;
const CODE_F32: u64 = 3;
const CODE_F64: u64 = 4;
const CODE_F64_ZERO: u64 = 5;

const CODE_I32_BASE: u64 = 8;
const CODE_I64_BASE: u64 = 13;
const CODE_U32_BASE: u64 = 22;
const CODE_U64_BASE: u64 = 27;

/// First code of the variable-length range.
const CODE_VARIABLE_BASE: u64 = 36;
/// Offsets within each group of three variable-length codes.
const VARIABLE_TEXT: u64 = 0;
const VARIABLE_BLOB: u64 = 1;
const VARIABLE_DECIMAL: u64 = 2;

fn corrupt(message: impl Into<String>) -> HematiteError {
    HematiteError::CorruptedData(message.into())
}

/// Bytes needed to hold `value` in minimal two's-complement big-endian form.
///
/// Zero uses no bytes at all: the serial type already says the value is zero.
fn signed_width(value: i64) -> usize {
    if value == 0 {
        return 0;
    }
    // A negative value needs the same width as its bitwise complement, which is
    // what makes sign extension on the way back out lossless.
    let magnitude = if value < 0 { !value } else { value };
    let significant_bits = 64 - magnitude.leading_zeros() as usize;
    // +1 for the sign bit.
    (significant_bits + 1).div_ceil(8)
}

fn unsigned_width(value: u64) -> usize {
    if value == 0 {
        return 0;
    }
    (64 - value.leading_zeros() as usize).div_ceil(8)
}

fn write_signed(out: &mut Vec<u8>, value: i64, width: usize) {
    let bytes = value.to_be_bytes();
    out.extend_from_slice(&bytes[8 - width..]);
}

fn write_unsigned(out: &mut Vec<u8>, value: u64, width: usize) {
    let bytes = value.to_be_bytes();
    out.extend_from_slice(&bytes[8 - width..]);
}

fn read_signed(bytes: &[u8]) -> i64 {
    if bytes.is_empty() {
        return 0;
    }
    // Sign-extend from the top bit of the first byte.
    let mut buffer = if bytes[0] & 0x80 != 0 {
        [0xFFu8; 8]
    } else {
        [0u8; 8]
    };
    buffer[8 - bytes.len()..].copy_from_slice(bytes);
    i64::from_be_bytes(buffer)
}

fn read_unsigned(bytes: &[u8]) -> u64 {
    let mut buffer = [0u8; 8];
    buffer[8 - bytes.len()..].copy_from_slice(bytes);
    u64::from_be_bytes(buffer)
}

/// Packs decimal digits two per byte, `0x0F` marking an unused low nibble.
fn pack_digits(out: &mut Vec<u8>, digits: &[u8]) {
    for chunk in digits.chunks(2) {
        let high = chunk[0] & 0x0F;
        let low = if chunk.len() > 1 {
            chunk[1] & 0x0F
        } else {
            0x0F
        };
        out.push((high << 4) | low);
    }
}

fn unpack_digits(packed: &[u8], digit_count: usize) -> Result<Vec<u8>> {
    let mut digits = Vec::with_capacity(digit_count);
    for byte in packed {
        digits.push((byte >> 4) & 0x0F);
        if digits.len() == digit_count {
            break;
        }
        digits.push(byte & 0x0F);
        if digits.len() == digit_count {
            break;
        }
    }
    if digits.len() != digit_count || digits.iter().any(|digit| *digit > 9) {
        return Err(corrupt("Packed decimal digits are invalid"));
    }
    Ok(digits)
}

/// Decimal body: `[varint (scale << 1 | negative)] [varint digit_count] [packed BCD]`.
///
/// Sign rides in the low bit of the scale so the body needs no separate byte for it,
/// and the whole body length is carried by the serial type, which is what keeps the
/// header walk free of body reads.
fn encode_decimal_body(out: &mut Vec<u8>, decimal: &DecimalValue) {
    let digits = decimal.digit_bytes();
    varint::write_to_vec(
        out,
        ((decimal.scale() as u64) << 1) | u64::from(decimal.negative()),
    );
    varint::write_to_vec(out, digits.len() as u64);
    pack_digits(out, digits);
}

fn decode_decimal_body(body: &[u8]) -> Result<DecimalValue> {
    let (scale_and_sign, mut offset) =
        varint::read(body, 0).ok_or_else(|| corrupt("Decimal scale is truncated"))?;
    let negative = scale_and_sign & 1 == 1;
    let scale = usize::try_from(scale_and_sign >> 1)
        .map_err(|_| corrupt("Decimal scale is out of range"))?;

    let (digit_count, consumed) =
        varint::read(body, offset).ok_or_else(|| corrupt("Decimal digit count is truncated"))?;
    offset += consumed;
    let digit_count =
        usize::try_from(digit_count).map_err(|_| corrupt("Decimal digit count is out of range"))?;

    let packed = body
        .get(offset..)
        .ok_or_else(|| corrupt("Decimal digits are truncated"))?;
    if packed.len() != digit_count.div_ceil(2) {
        return Err(corrupt("Decimal digit body has the wrong length"));
    }
    let digits = unpack_digits(packed, digit_count)?;

    let decimal = DecimalValue::parse(&format_decimal(negative, &digits, scale))?;
    Ok(
        if decimal.digit_bytes().len() == 1 && decimal.digit_bytes()[0] == 0 {
            DecimalValue::zero()
        } else {
            decimal
        },
    )
}

fn format_decimal(negative: bool, digits: &[u8], scale: usize) -> String {
    let is_zero = digits.iter().all(|digit| *digit == 0);
    let mut out = String::new();
    if negative && !is_zero {
        out.push('-');
    }
    let digit_string: String = digits.iter().map(|d| char::from(b'0' + *d)).collect();

    if scale == 0 {
        out.push_str(&digit_string);
    } else if digit_string.len() <= scale {
        out.push_str("0.");
        for _ in 0..scale - digit_string.len() {
            out.push('0');
        }
        out.push_str(&digit_string);
    } else {
        let split = digit_string.len() - scale;
        out.push_str(&digit_string[..split]);
        out.push('.');
        out.push_str(&digit_string[split..]);
    }
    out
}

/// The serial type for `value`, and the number of body bytes it implies.
fn serial_type(value: &Value) -> (u64, usize) {
    match value {
        Value::Null => (CODE_NULL, 0),
        Value::Boolean(false) => (CODE_FALSE, 0),
        Value::Boolean(true) => (CODE_TRUE, 0),
        // Only *positive* zero takes the short form. Negative zero has distinct bits
        // and distinct behaviour under division, so it keeps its body.
        Value::Float(f) if f.to_bits() == 0f64.to_bits() => (CODE_F64_ZERO, 0),
        Value::Float(_) => (CODE_F64, 8),
        Value::Float32(_) => (CODE_F32, 4),
        Value::Integer(i) => {
            let width = signed_width(*i as i64);
            (CODE_I32_BASE + width as u64, width)
        }
        Value::BigInt(i) => {
            let width = signed_width(*i);
            (CODE_I64_BASE + width as u64, width)
        }
        Value::UInteger(u) => {
            let width = unsigned_width(*u as u64);
            (CODE_U32_BASE + width as u64, width)
        }
        Value::UBigInt(u) => {
            let width = unsigned_width(*u);
            (CODE_U64_BASE + width as u64, width)
        }
        Value::Text(s) => (
            CODE_VARIABLE_BASE + VARIABLE_TEXT + 3 * s.len() as u64,
            s.len(),
        ),
        Value::Blob(b) => (
            CODE_VARIABLE_BASE + VARIABLE_BLOB + 3 * b.len() as u64,
            b.len(),
        ),
        Value::Decimal(d) => {
            let len = decimal_body_len(d);
            (CODE_VARIABLE_BASE + VARIABLE_DECIMAL + 3 * len as u64, len)
        }
    }
}

fn decimal_body_len(decimal: &DecimalValue) -> usize {
    let digits = decimal.digit_bytes();
    varint::len_of(((decimal.scale() as u64) << 1) | u64::from(decimal.negative()))
        + varint::len_of(digits.len() as u64)
        + digits.len().div_ceil(2)
}

/// A serial type after classification: what the value is, and how wide its body is.
///
/// Classified once and passed along. Deciding it separately for the header walk
/// and again for building the value would be two chains of range comparisons per
/// field where one match does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Serial {
    Null,
    Boolean(bool),
    /// Positive zero, which carries no body.
    FloatZero,
    Float32,
    Float64,
    Integer(usize),
    BigInt(usize),
    UInteger(usize),
    UBigInt(usize),
    Text(usize),
    Blob(usize),
    Decimal(usize),
}

impl Serial {
    #[inline]
    fn body_len(self) -> usize {
        match self {
            Serial::Null | Serial::Boolean(_) | Serial::FloatZero => 0,
            Serial::Float32 => 4,
            Serial::Float64 => 8,
            Serial::Integer(w)
            | Serial::BigInt(w)
            | Serial::UInteger(w)
            | Serial::UBigInt(w)
            | Serial::Text(w)
            | Serial::Blob(w)
            | Serial::Decimal(w) => w,
        }
    }
}

/// Classifies a serial type, or `None` if the code is not defined.
#[inline]
fn classify(code: u64) -> Option<Serial> {
    if code >= CODE_VARIABLE_BASE {
        let offset = code - CODE_VARIABLE_BASE;
        let len = (offset / 3) as usize;
        return Some(match offset % 3 {
            VARIABLE_TEXT => Serial::Text(len),
            VARIABLE_BLOB => Serial::Blob(len),
            _ => Serial::Decimal(len),
        });
    }

    Some(match code {
        CODE_NULL => Serial::Null,
        CODE_FALSE => Serial::Boolean(false),
        CODE_TRUE => Serial::Boolean(true),
        CODE_F32 => Serial::Float32,
        CODE_F64 => Serial::Float64,
        CODE_F64_ZERO => Serial::FloatZero,
        6 | 7 => return None,
        c if c <= CODE_I32_BASE + 4 => Serial::Integer((c - CODE_I32_BASE) as usize),
        c if c <= CODE_I64_BASE + 8 => Serial::BigInt((c - CODE_I64_BASE) as usize),
        c if c <= CODE_U32_BASE + 4 => Serial::UInteger((c - CODE_U32_BASE) as usize),
        // Codes 8..36 are fully covered by the four integer families, so anything
        // that reaches here is the last of them.
        c => Serial::UBigInt((c - CODE_U64_BASE) as usize),
    })
}

/// Encodes `values` as one record.
pub(crate) fn encode(values: &[Value]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_into(&mut out, values);
    out
}

/// Appends one record to `out`, reusing its allocation.
pub(crate) fn encode_into(out: &mut Vec<u8>, values: &[Value]) {
    // Serial types are recomputed rather than staged in a temporary `Vec`: they are
    // a match and a leading-zero count, which is cheaper than the heap allocation
    // that staging them would cost on every row written.
    let mut types_len = 0usize;
    let mut body_len = 0usize;
    for value in values {
        let (code, len) = serial_type(value);
        types_len += varint::len_of(code);
        body_len += len;
    }

    // `header_len` counts itself, so it depends on its own encoded width. Widths
    // only ever grow by one here, and one step of the fixed point always settles it:
    // re-measuring with the larger width cannot make it larger again, because a
    // one-byte increase moves the total by one while a width boundary is 128 apart.
    let mut header_len = types_len + varint::len_of(types_len as u64);
    header_len = types_len + varint::len_of(header_len as u64);

    out.reserve(header_len + body_len);

    varint::write_to_vec(out, header_len as u64);
    for value in values {
        varint::write_to_vec(out, serial_type(value).0);
    }

    for value in values {
        match value {
            Value::Null | Value::Boolean(_) => {}
            Value::Float(f) if f.to_bits() == 0f64.to_bits() => {}
            Value::Float(f) => out.extend_from_slice(&f.to_bits().to_be_bytes()),
            Value::Float32(f) => out.extend_from_slice(&f.to_bits().to_be_bytes()),
            Value::Integer(i) => write_signed(out, *i as i64, signed_width(*i as i64)),
            Value::BigInt(i) => write_signed(out, *i, signed_width(*i)),
            Value::UInteger(u) => write_unsigned(out, *u as u64, unsigned_width(*u as u64)),
            Value::UBigInt(u) => write_unsigned(out, *u, unsigned_width(*u)),
            Value::Text(s) => out.extend_from_slice(s.as_bytes()),
            Value::Blob(b) => out.extend_from_slice(b),
            Value::Decimal(d) => encode_decimal_body(out, d),
        }
    }
}

/// A borrowed value read straight out of a record buffer.
///
/// `Text` and `Blob` borrow the buffer instead of copying it, which is the point:
/// a predicate over a text column can run without allocating a `String`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ValueRef<'a> {
    Null,
    Boolean(bool),
    Integer(i32),
    BigInt(i64),
    UInteger(u32),
    UBigInt(u64),
    Float32(f32),
    Float(f64),
    Text(&'a str),
    Blob(&'a [u8]),
    /// Decimals need digit unpacking to become a value, so they carry their raw
    /// body and materialise on demand.
    Decimal(&'a [u8]),
}

impl ValueRef<'_> {
    pub(crate) fn to_value(&self) -> Result<Value> {
        Ok(match self {
            ValueRef::Null => Value::Null,
            ValueRef::Boolean(b) => Value::Boolean(*b),
            ValueRef::Integer(i) => Value::Integer(*i),
            ValueRef::BigInt(i) => Value::BigInt(*i),
            ValueRef::UInteger(u) => Value::UInteger(*u),
            ValueRef::UBigInt(u) => Value::UBigInt(*u),
            ValueRef::Float32(f) => Value::Float32(*f),
            ValueRef::Float(f) => Value::Float(*f),
            ValueRef::Text(s) => Value::Text((*s).to_string()),
            ValueRef::Blob(b) => Value::Blob((*b).to_vec()),
            ValueRef::Decimal(body) => Value::Decimal(decode_decimal_body(body)?),
        })
    }
}

/// A record borrowed in place, with no decoding done up front.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RecordRef<'a> {
    /// The whole header: the length varint and every serial type after it.
    header: &'a [u8],
    /// Serial types only — `header_len` itself is already consumed.
    types: &'a [u8],
    body: &'a [u8],
}

impl<'a> RecordRef<'a> {
    /// Validates the header and borrows the record. Bodies are not touched.
    pub(crate) fn new(bytes: &'a [u8]) -> Result<Self> {
        let (header_len, prefix_len) =
            varint::read(bytes, 0).ok_or_else(|| corrupt("Record header length is truncated"))?;
        let header_len =
            usize::try_from(header_len).map_err(|_| corrupt("Record header is out of range"))?;

        if header_len < prefix_len || header_len > bytes.len() {
            return Err(corrupt(format!(
                "Record header length {header_len} is outside the {} byte record",
                bytes.len()
            )));
        }

        Ok(Self {
            header: &bytes[..header_len],
            types: &bytes[prefix_len..header_len],
            body: &bytes[header_len..],
        })
    }

    /// The header bytes, which are what a [`ProjectionPlan`] matches on.
    fn header(&self) -> &'a [u8] {
        self.header
    }

    /// Walks the serial types, yielding each code with the body range it covers.
    fn types(&self) -> TypeWalk<'a> {
        TypeWalk {
            types: self.types,
            type_offset: 0,
            body_offset: 0,
        }
    }

    pub(crate) fn field_count(&self) -> Result<usize> {
        let mut count = 0;
        for entry in self.types() {
            entry?;
            count += 1;
        }
        Ok(count)
    }

    /// Reads one field. Walks the header to field `index` — cheap, since serial
    /// types are one byte for anything small — and touches only that field's body.
    pub(crate) fn field(&self, index: usize) -> Result<ValueRef<'a>> {
        for (position, entry) in self.types().enumerate() {
            let (code, range) = entry?;
            if position == index {
                return self.value_at(code, range);
            }
        }
        Err(corrupt(format!("Record has no field at index {index}")))
    }

    pub(crate) fn iter(&self) -> RecordIter<'a> {
        RecordIter {
            record: *self,
            walk: self.types(),
        }
    }

    /// Takes `self` by value because [`RecordRef`] is `Copy` — three borrowed
    /// slices — and its callers are done with it once the values are out.
    ///
    /// Also returns where the bodies ended, so a caller validating that a record
    /// consumes its buffer exactly does not need a second pass over the header.
    fn to_values_and_body_end(self) -> Result<(Vec<Value>, usize)> {
        // Every serial type is at least one byte, so the header length bounds the
        // field count from above — and matches it exactly for the small codes that
        // dominate. One allocation instead of a growth sequence.
        let mut values = Vec::with_capacity(self.types.len());
        let mut body_end = 0;

        for entry in self.types() {
            let (serial, range) = entry?;
            body_end = range.end;
            values.push(self.value_at(serial, range)?.to_value()?);
        }
        Ok((values, body_end))
    }

    pub(crate) fn to_values(self) -> Result<Vec<Value>> {
        Ok(self.to_values_and_body_end()?.0)
    }

    /// Reads one field straight into an owned [`Value`].
    ///
    /// [`value_at`](Self::value_at) followed by [`ValueRef::to_value`] matches on the
    /// serial type twice and builds a borrowed value in between that is destructured
    /// immediately. A projecting scan wants the owned value and nothing else, so this
    /// does the one match the other two share, in
    /// [`value_from_body`](Self::value_from_body), which the planned scan calls
    /// directly — worth its own arm list because decode is the largest single cost in
    /// a scanned row.
    ///
    /// The two must agree, which `an_owned_read_matches_a_borrowed_one` checks over
    /// every variant. A missing arm is a compile error either way; what that test is
    /// for is the two drifting apart on what an arm *means*.
    #[inline]
    fn owned_value_at(&self, serial: Serial, range: std::ops::Range<usize>) -> Result<Value> {
        Self::value_from_body(self.body, serial, range)
    }

    /// Reads one field out of a record's bodies.
    ///
    /// Takes the bodies rather than `self`, because the planned decode path matches a
    /// cached header and never builds a `RecordRef` at all — it holds the bodies and
    /// the field layout, which is all this needs.
    #[inline]
    fn value_from_body(
        bodies: &[u8],
        serial: Serial,
        range: std::ops::Range<usize>,
    ) -> Result<Value> {
        let body = bodies
            .get(range)
            .ok_or_else(|| corrupt("Record body is truncated"))?;

        Ok(match serial {
            Serial::Null => Value::Null,
            Serial::Boolean(b) => Value::Boolean(b),
            Serial::FloatZero => Value::Float(0.0),
            Serial::Float32 => Value::Float32(f32::from_bits(u32::from_be_bytes(
                body.try_into()
                    .expect("Serial::body_len guarantees 4 bytes"),
            ))),
            Serial::Float64 => Value::Float(f64::from_bits(u64::from_be_bytes(
                body.try_into()
                    .expect("Serial::body_len guarantees 8 bytes"),
            ))),
            Serial::Integer(_) => Value::Integer(read_signed(body) as i32),
            Serial::BigInt(_) => Value::BigInt(read_signed(body)),
            Serial::UInteger(_) => Value::UInteger(read_unsigned(body) as u32),
            Serial::UBigInt(_) => Value::UBigInt(read_unsigned(body)),
            Serial::Text(_) => Value::Text(
                std::str::from_utf8(body)
                    .map_err(|_| corrupt("Invalid UTF-8 in text value"))?
                    .to_string(),
            ),
            Serial::Blob(_) => Value::Blob(body.to_vec()),
            Serial::Decimal(_) => Value::Decimal(decode_decimal_body(body)?),
        })
    }

    #[inline]
    fn value_at(&self, serial: Serial, range: std::ops::Range<usize>) -> Result<ValueRef<'a>> {
        let body = self
            .body
            .get(range)
            .ok_or_else(|| corrupt("Record body is truncated"))?;

        Ok(match serial {
            Serial::Null => ValueRef::Null,
            Serial::Boolean(b) => ValueRef::Boolean(b),
            Serial::FloatZero => ValueRef::Float(0.0),
            Serial::Float32 => ValueRef::Float32(f32::from_bits(u32::from_be_bytes(
                body.try_into()
                    .expect("Serial::body_len guarantees 4 bytes"),
            ))),
            Serial::Float64 => ValueRef::Float(f64::from_bits(u64::from_be_bytes(
                body.try_into()
                    .expect("Serial::body_len guarantees 8 bytes"),
            ))),
            Serial::Integer(_) => ValueRef::Integer(read_signed(body) as i32),
            Serial::BigInt(_) => ValueRef::BigInt(read_signed(body)),
            Serial::UInteger(_) => ValueRef::UInteger(read_unsigned(body) as u32),
            Serial::UBigInt(_) => ValueRef::UBigInt(read_unsigned(body)),
            Serial::Text(_) => ValueRef::Text(
                std::str::from_utf8(body).map_err(|_| corrupt("Invalid UTF-8 in text value"))?,
            ),
            Serial::Blob(_) => ValueRef::Blob(body),
            Serial::Decimal(_) => ValueRef::Decimal(body),
        })
    }
}

/// Walks serial types, tracking each one's body range.
struct TypeWalk<'a> {
    types: &'a [u8],
    type_offset: usize,
    body_offset: usize,
}

impl Iterator for TypeWalk<'_> {
    type Item = Result<(Serial, std::ops::Range<usize>)>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.type_offset >= self.types.len() {
            return None;
        }

        let Some((code, consumed)) = varint::read(self.types, self.type_offset) else {
            self.type_offset = self.types.len();
            return Some(Err(corrupt("Record serial type is truncated")));
        };
        self.type_offset += consumed;

        let Some(serial) = classify(code) else {
            self.type_offset = self.types.len();
            return Some(Err(corrupt(format!("Unknown record serial type {code}"))));
        };

        let start = self.body_offset;
        self.body_offset += serial.body_len();
        Some(Ok((serial, start..self.body_offset)))
    }
}

pub(crate) struct RecordIter<'a> {
    record: RecordRef<'a>,
    walk: TypeWalk<'a>,
}

impl<'a> Iterator for RecordIter<'a> {
    type Item = Result<ValueRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.walk.next()? {
            Ok((serial, range)) => Some(self.record.value_at(serial, range)),
            Err(err) => Some(Err(err)),
        }
    }
}

/// Where each projected field sits in a record, reused across rows of one shape.
///
/// A scan decodes thousands of records whose serial-type headers are identical byte
/// for byte — same columns, same widths. Re-deriving every body offset for each one
/// would repeat the same work per row: classifying those codes and walking the header
/// is the larger half of what a scanned row spends on decode without a plan. This
/// keeps the layout the last row produced and reuses it while the header still
/// matches.
///
/// Reuse is gated on a comparison of the serial types, not on an assumption about the
/// table. A row shaped differently from its neighbours — a `NULL` where they hold an
/// integer, an integer wide enough to need another byte — misses and rebuilds.
/// Correctness never depends on rows being uniform; only speed does.
///
/// The plan owns the indices it projects, so it cannot be applied to a projection it
/// was not built for. That is the whole reason the indices are not a parameter of
/// [`decode_fields_into_planned`]: a cached layout and a mismatched index list would
/// silently return the wrong columns.
pub(crate) struct ProjectionPlan {
    /// The fields wanted, in the order the caller wants them.
    indices: Vec<usize>,
    /// The whole record header the layout was derived from — the length varint and
    /// every serial type — owned so it outlives the record.
    ///
    /// The prefix rather than the serial types alone, because it also settles where the
    /// bodies start. Matching it means `RecordRef::new` has nothing left to work out:
    /// the header length is known, so its varint read and bounds checks are the same
    /// answer as last row's.
    header: Vec<u8>,
    /// One entry per wanted index, in projection order.
    fields: Vec<(Serial, std::ops::Range<usize>)>,
    /// Whether `header` and `fields` describe anything. A record of no fields has an empty header,
    /// which is a legitimate layout rather than "nothing cached yet".
    primed: bool,
}

impl ProjectionPlan {
    pub(crate) fn new(indices: &[usize]) -> Self {
        Self {
            indices: indices.to_vec(),
            header: Vec::new(),
            fields: Vec::new(),
            primed: false,
        }
    }

    /// The body of `bytes`, if its header is the one the layout was built from.
    ///
    /// One comparison answers both questions a hit needs: that the field offsets still
    /// apply, and where the bodies begin.
    #[inline]
    fn body_if_unchanged<'a>(&self, bytes: &'a [u8]) -> Option<&'a [u8]> {
        if !self.primed || bytes.len() < self.header.len() {
            return None;
        }
        let (prefix, body) = bytes.split_at(self.header.len());
        (prefix == self.header).then_some(body)
    }

    /// Rebuilds the layout from `record`.
    fn rebuild(&mut self, record: &RecordRef<'_>) -> Result<()> {
        let Self {
            indices,
            header,
            fields,
            primed,
        } = self;

        // Unprimed before the walk, so a layout left over from an earlier shape cannot
        // survive a failure here and then be reused against a record it does not
        // describe.
        *primed = false;
        fields.clear();

        // Stop at the last field anyone asked for. A `SELECT id, a` over five columns
        // has no reason to walk the last three serial types; only the fields that are
        // read need locating, and `value_at` bounds-checks each against the body, so a
        // field that is read is still checked.
        //
        // The cost of stopping is the whole-record check that the serial types account
        // for exactly the body length. That check covers fields nobody reads, which is
        // the same trade the B-tree makes for cell ranges: verify what is used, and
        // leave whole-object structure to the integrity check.
        let Some(&last_wanted) = indices.iter().max() else {
            header.clear();
            *primed = true;
            return Ok(());
        };

        fields.resize(indices.len(), (Serial::Null, 0..0));
        let mut filled = 0usize;

        for (position, entry) in record.types().enumerate() {
            let (code, range) = entry?;
            // Slots are filled as the single header walk reaches them, so the fields
            // can be asked for in any order — or twice — and still land where the
            // caller wants them.
            for (slot, wanted) in indices.iter().enumerate() {
                if *wanted == position {
                    fields[slot] = (code, range.clone());
                    filled += 1;
                }
            }
            if position == last_wanted {
                break;
            }
        }

        if filled != indices.len() {
            fields.clear();
            return Err(corrupt(format!(
                "Record has no field at index {last_wanted}"
            )));
        }

        header.clear();
        header.extend_from_slice(record.header());
        *primed = true;
        Ok(())
    }
}

/// Decodes only `indices`, in the order given.
///
/// A scan that selects two of five columns has no use for the other three, and
/// decode is a quarter of a scan's instructions. Field bodies are only
/// reachable by walking the serial-type header, but that walk is cheap (one byte per
/// small field) and it is the *bodies* that cost: allocating a `String`, copying a
/// blob. This walks the header once, records where each wanted field lives, and
/// touches nothing else.
///
/// Emitting in projection order rather than table order lets the caller skip building
/// a second row to reorder into.
pub(crate) fn decode_fields(bytes: &[u8], indices: &[usize]) -> Result<Vec<Value>> {
    let mut values = Vec::with_capacity(indices.len());
    decode_fields_into(bytes, indices, &mut values)?;
    Ok(values)
}

/// Appends the wanted fields to `out`, in `indices` order.
///
/// The one-shot form, for a caller decoding a single record. It builds a
/// [`ProjectionPlan`] and throws it away, so a caller decoding many records of one
/// shape — a scan — should hold the plan and call
/// [`decode_fields_into_planned`] instead.
pub(crate) fn decode_fields_into(
    bytes: &[u8],
    indices: &[usize],
    out: &mut Vec<Value>,
) -> Result<()> {
    decode_fields_into_planned(bytes, &mut ProjectionPlan::new(indices), out)
}

/// Appends the projected fields to `out`, in the plan's order.
///
/// The scan path, which lays every row into one buffer rather than giving each its
/// own vector. A vector per row would be the single allocation a scanned row makes,
/// and `malloc` and `free` for it measured 24 % of a full scan's instructions.
pub(crate) fn decode_fields_into_planned(
    bytes: &[u8],
    plan: &mut ProjectionPlan,
    out: &mut Vec<Value>,
) -> Result<()> {
    if plan.indices.is_empty() {
        return Ok(());
    }

    // A row whose header matches the last one needs no `RecordRef` at all: the field
    // layout still holds, and the header length that says where the bodies start is the
    // same one already matched. Only a row of a new shape pays for parsing it.
    let bodies = match plan.body_if_unchanged(bytes) {
        Some(bodies) => bodies,
        None => {
            let record = RecordRef::new(bytes)?;
            plan.rebuild(&record)?;
            record.body
        }
    };

    // Pushed, rather than written into slots reserved with `Null`: `Value` is 32
    // bytes and has a destructor, so pre-filling would write every field twice and run
    // drop glue on each placeholder it overwrote.
    let base = out.len();
    out.reserve(plan.fields.len());
    for (serial, range) in &plan.fields {
        match RecordRef::value_from_body(bodies, *serial, range.clone()) {
            Ok(value) => out.push(value),
            Err(err) => {
                // `out` holds whole rows laid end to end, so a half-decoded one would
                // shift every column of every row after it.
                out.truncate(base);
                return Err(err);
            }
        }
    }
    Ok(())
}

/// Decodes a whole record into owned values.
///
/// Stricter than [`RecordRef`]: the body must end exactly where the serial types
/// say it does. A caller decoding a whole record hands over exactly one record's
/// bytes, so anything left over is corruption — most likely a cell whose length
/// disagrees with its contents.
pub(crate) fn decode(bytes: &[u8]) -> Result<Vec<Value>> {
    let record = RecordRef::new(bytes)?;
    let (values, body_end) = record.to_values_and_body_end()?;

    if body_end != record.body.len() {
        return Err(corrupt(format!(
            "Record body is {} bytes but its serial types account for {body_end}",
            record.body.len()
        )));
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `decode_fields` answers projections, so it has to agree with `decode` on the
    /// fields it returns and honour the order it was asked for.
    #[test]
    fn decode_fields_matches_a_full_decode() {
        let values = vec![
            Value::Integer(7),
            Value::Text("hello".to_string()),
            Value::BigInt(-9_000_000_000),
            Value::Null,
            Value::Boolean(true),
        ];
        let bytes = encode(&values);
        let all = decode(&bytes).expect("decode");

        for indices in [
            vec![0usize],
            vec![4],
            vec![1, 3],
            vec![3, 1],
            vec![2, 2],
            vec![0, 1, 2, 3, 4],
        ] {
            let got = decode_fields(&bytes, &indices).expect("decode_fields");
            let want: Vec<Value> = indices.iter().map(|i| all[*i].clone()).collect();
            assert_eq!(got, want, "indices {indices:?}");
        }
    }

    #[test]
    fn decode_fields_rejects_an_index_past_the_end() {
        let bytes = encode(&[Value::Integer(1), Value::Integer(2)]);
        assert!(decode_fields(&bytes, &[2]).is_err());
        assert!(decode_fields(&bytes, &[0, 5]).is_err());
    }

    /// The owned and borrowed reads of a field must not drift apart.
    ///
    /// `owned_value_at` exists to skip building a `ValueRef` only to destructure it, so
    /// it repeats the arm list rather than sharing one. This is what keeps the copy
    /// honest.
    #[test]
    fn an_owned_read_matches_a_borrowed_one() {
        let values = vec![
            Value::Null,
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Integer(0),
            Value::Integer(-70_000),
            Value::BigInt(i64::MIN),
            Value::UInteger(u32::MAX),
            Value::UBigInt(u64::MAX),
            Value::Float(0.0),
            Value::Float(-0.0),
            Value::Float(1.5),
            Value::Float32(-2.25),
            Value::Text(String::new()),
            Value::Text("multi byte \u{e9}\u{4e16}".to_string()),
            Value::Blob(vec![]),
            Value::Blob(vec![0, 1, 255]),
            Value::Decimal(DecimalValue::zero()),
        ];
        let bytes = encode(&values);
        let record = RecordRef::new(&bytes).expect("record");

        for (position, entry) in record.types().enumerate() {
            let (serial, range) = entry.expect("serial type");
            let borrowed = record
                .value_at(serial, range.clone())
                .expect("borrowed")
                .to_value()
                .expect("to_value");
            let owned = record.owned_value_at(serial, range).expect("owned");
            assert_eq!(borrowed, owned, "field {position}");
            assert_eq!(owned, values[position], "field {position}");
        }
    }

    /// The plan is only allowed to skip its header walk when the shape really matches.
    ///
    /// Every other test here decodes through the one-shot form, which builds a fresh
    /// plan per record and so never exercises reuse at all. This drives rows of
    /// deliberately varying shape through one plan — differing widths, a `NULL` where
    /// its neighbours hold an integer, text of differing lengths — and checks each
    /// against a decode that shares nothing with the one before it.
    #[test]
    fn a_reused_plan_follows_a_change_of_shape() {
        let rows = vec![
            vec![
                Value::Integer(1),
                Value::Text("a".to_string()),
                Value::Integer(2),
            ],
            // Same shape as the first: this is the row that must hit the cache.
            vec![
                Value::Integer(3),
                Value::Text("b".to_string()),
                Value::Integer(4),
            ],
            // Wider integers, so every body offset after the first field moves.
            vec![
                Value::Integer(70_000),
                Value::Text("cc".to_string()),
                Value::Integer(80_000),
            ],
            // A null where the others hold an integer: a zero-width body.
            vec![
                Value::Null,
                Value::Text("ddd".to_string()),
                Value::Integer(5),
            ],
            // A different variant at the same position.
            vec![
                Value::BigInt(-9_000_000_000),
                Value::Text(String::new()),
                Value::Integer(6),
            ],
            // Back to the very first shape, after several misses.
            vec![
                Value::Integer(7),
                Value::Text("e".to_string()),
                Value::Integer(8),
            ],
        ];

        for indices in [vec![0usize], vec![2, 0], vec![1], vec![0, 1, 2], vec![2, 2]] {
            let mut plan = ProjectionPlan::new(&indices);
            let mut got = Vec::new();
            for row in &rows {
                decode_fields_into_planned(&encode(row), &mut plan, &mut got)
                    .expect("planned decode");
            }

            let want: Vec<Value> = rows
                .iter()
                .flat_map(|row| indices.iter().map(|i| row[*i].clone()))
                .collect();
            assert_eq!(got, want, "indices {indices:?}");
        }
    }

    /// A record the plan cannot describe must not leave it describing that record.
    ///
    /// The failure this guards against is a poisoned cache: the plan marks itself
    /// unprimed before rebuilding, so a walk that errors part way cannot leave a
    /// half-built layout behind for the next row to be decoded through.
    #[test]
    fn a_failed_rebuild_does_not_poison_the_plan() {
        let wanted = [1usize];
        let mut plan = ProjectionPlan::new(&wanted);
        let mut out = Vec::new();

        let good = encode(&[Value::Integer(1), Value::Integer(2)]);
        decode_fields_into_planned(&good, &mut plan, &mut out).expect("first");
        assert_eq!(out, vec![Value::Integer(2)]);

        // One field, so index 1 does not exist.
        let short = encode(&[Value::Integer(9)]);
        assert!(decode_fields_into_planned(&short, &mut plan, &mut out).is_err());
        // The failed row appended nothing.
        assert_eq!(out, vec![Value::Integer(2)]);

        // A row of the original shape still decodes, and one of a new shape does too.
        decode_fields_into_planned(&good, &mut plan, &mut out).expect("after failure");
        let wider = encode(&[Value::Integer(1), Value::Integer(70_000)]);
        decode_fields_into_planned(&wider, &mut plan, &mut out).expect("wider");
        assert_eq!(
            out,
            vec![Value::Integer(2), Value::Integer(2), Value::Integer(70_000)]
        );
    }

    /// A field that is read is still bounds-checked against the body.
    #[test]
    fn decode_fields_rejects_a_truncated_body_it_reads() {
        let bytes = encode(&[Value::BigInt(i64::MAX), Value::Integer(3)]);
        let truncated = &bytes[..bytes.len() - 1];
        assert!(
            decode_fields(truncated, &[1]).is_err(),
            "reading a field off the end of a truncated body must fail"
        );
    }

    /// The narrowing the early stop buys, stated so it is a decision rather than a
    /// surprise: the walk stops at the last field asked for, so damage confined to
    /// fields beyond it is not noticed here. `decode` still checks the whole record,
    /// and the integrity check still walks every page.
    #[test]
    fn decode_fields_does_not_inspect_fields_after_the_last_one_wanted() {
        let bytes = encode(&[Value::Integer(1), Value::Integer(2), Value::Integer(3)]);
        let mut damaged = bytes.clone();
        let last = damaged.len() - 1;
        damaged[last] ^= 0xFF;

        // Reading only the first field succeeds despite the damaged tail...
        assert_eq!(
            decode_fields(&damaged, &[0]).expect("first field"),
            vec![Value::Integer(1)]
        );
        // ...and the damage is visible to anything that reads that far.
        assert_ne!(
            decode_fields(&damaged, &[2]).expect("third field"),
            vec![Value::Integer(3)]
        );
    }

    use proptest::prelude::*;

    /// `Value` compares floats with `==`, so NaN never equals itself and this
    /// asserts on the bits instead. The encoding stores raw bits, so NaN payloads
    /// and negative zero both survive a round trip exactly.
    fn assert_same(left: &Value, right: &Value) {
        match (left, right) {
            (Value::Float(a), Value::Float(b)) => assert_eq!(a.to_bits(), b.to_bits()),
            (Value::Float32(a), Value::Float32(b)) => assert_eq!(a.to_bits(), b.to_bits()),
            _ => assert_eq!(left, right),
        }
    }

    fn roundtrip(values: &[Value]) -> Vec<u8> {
        let encoded = encode(values);
        let decoded = decode(&encoded).expect("decode");
        assert_eq!(decoded.len(), values.len(), "field count changed");
        for (original, result) in values.iter().zip(&decoded) {
            assert_same(original, result);
        }
        encoded
    }

    #[test]
    fn ten_small_integers_cost_21_bytes() {
        let values: Vec<Value> = (1..=10).map(Value::Integer).collect();
        let encoded = roundtrip(&values);
        // 1 byte header_len + 10 one-byte serial types + 10 one-byte bodies.
        assert_eq!(encoded.len(), 21);
    }

    /// The shape most application tables have. A fixed-width encoding spends 57
    /// bytes on it before the B-tree cell is added on top; this spends 36.
    #[test]
    fn a_typical_mixed_row_is_compact() {
        let values = vec![
            Value::Integer(5000),
            Value::Text("Alice Bailey".to_string()),
            Value::Text("alice@example.com".to_string()),
            Value::Boolean(true),
        ];
        let encoded = roundtrip(&values);
        // header: 1 (header_len) + 1 (i32, width 2) + 1 (text L=12, code 72)
        //         + 1 (text L=17, code 87) + 1 (bool) = 5
        // bodies: 2 + 12 + 17 + 0 = 31
        // Text codes stay one byte up to L = 30, which covers most short strings.
        assert_eq!(encoded.len(), 36);
    }

    #[test]
    fn null_and_boolean_have_no_body() {
        assert_eq!(roundtrip(&[Value::Null]).len(), 2);
        assert_eq!(roundtrip(&[Value::Boolean(true)]).len(), 2);
        assert_eq!(roundtrip(&[Value::Boolean(false)]).len(), 2);
        assert_eq!(roundtrip(&[Value::Integer(0)]).len(), 2);
        assert_eq!(roundtrip(&[Value::Float(0.0)]).len(), 2);
    }

    #[test]
    fn integer_width_tracks_magnitude() {
        let widths = [
            (0i32, 0usize),
            (1, 1),
            (127, 1),
            (-128, 1),
            (128, 2),
            (-129, 2),
            (32_767, 2),
            (32_768, 3),
            (i32::MAX, 4),
            (i32::MIN, 4),
        ];
        for (value, width) in widths {
            assert_eq!(signed_width(value as i64), width, "wrong width for {value}");
            roundtrip(&[Value::Integer(value)]);
        }
    }

    #[test]
    fn integer_variants_stay_distinct() {
        // `Integer(5) != BigInt(5)`, and they report different data types, so the
        // encoding must not collapse them onto one representation.
        let decoded = decode(&encode(&[
            Value::Integer(5),
            Value::BigInt(5),
            Value::UInteger(5),
            Value::UBigInt(5),
        ]))
        .expect("decode");
        assert_eq!(
            decoded,
            vec![
                Value::Integer(5),
                Value::BigInt(5),
                Value::UInteger(5),
                Value::UBigInt(5),
            ]
        );
    }

    #[test]
    fn extremes_survive() {
        roundtrip(&[
            Value::Integer(i32::MIN),
            Value::Integer(i32::MAX),
            Value::BigInt(i64::MIN),
            Value::BigInt(i64::MAX),
            Value::UInteger(u32::MAX),
            Value::UBigInt(u64::MAX),
            Value::Float(f64::INFINITY),
            Value::Float(f64::NEG_INFINITY),
            Value::Float(f64::NAN),
            Value::Float(-0.0),
            Value::Float32(f32::NAN),
            Value::Float32(-0.0),
        ]);
    }

    #[test]
    fn negative_zero_keeps_its_sign() {
        let encoded = encode(&[Value::Float(-0.0)]);
        let decoded = decode(&encoded).expect("decode");
        match decoded[0] {
            Value::Float(f) => assert!(f.is_sign_negative(), "-0.0 lost its sign"),
            ref other => panic!("expected a float, got {other:?}"),
        }
        // Positive zero takes the short form; negative zero does not.
        assert!(encoded.len() > encode(&[Value::Float(0.0)]).len());
    }

    #[test]
    fn text_and_blob_roundtrip_including_empty_and_multibyte() {
        roundtrip(&[
            Value::Text(String::new()),
            Value::Text("hello".to_string()),
            Value::Text("héllo wörld — ünïcode".to_string()),
            Value::Text("x".repeat(5000)),
            Value::Blob(Vec::new()),
            Value::Blob(vec![0, 1, 2, 255]),
            Value::Blob(vec![0xAB; 5000]),
        ]);
    }

    #[test]
    fn decimals_roundtrip() {
        for text in [
            "0",
            "1",
            "-1",
            "0.5",
            "-0.5",
            "123.456",
            "-123.456",
            "1000000",
            "0.000001",
            "99999999999999999999.99999999",
        ] {
            let decimal = DecimalValue::parse(text).expect("parse");
            roundtrip(&[Value::Decimal(decimal)]);
        }
    }

    #[test]
    fn empty_record_roundtrips() {
        let encoded = roundtrip(&[]);
        assert_eq!(encoded.len(), 1);
        assert_eq!(RecordRef::new(&encoded).unwrap().field_count().unwrap(), 0);
    }

    /// Field access must not depend on reading earlier bodies, which is what makes
    /// reading one column of a wide row cheap.
    #[test]
    fn random_access_matches_sequential_access() {
        let values = vec![
            Value::Integer(1),
            Value::Text("two".to_string()),
            Value::Null,
            Value::Blob(vec![3, 3, 3]),
            Value::BigInt(-4),
            Value::Boolean(true),
            Value::Float(5.5),
        ];
        let encoded = encode(&values);
        let record = RecordRef::new(&encoded).expect("parse");

        assert_eq!(record.field_count().unwrap(), values.len());
        for (index, expected) in values.iter().enumerate() {
            let field = record
                .field(index)
                .expect("field")
                .to_value()
                .expect("value");
            assert_same(expected, &field);
        }
        assert!(record.field(values.len()).is_err());
    }

    #[test]
    fn text_is_borrowed_not_copied() {
        let encoded = encode(&[Value::Text("borrowed".to_string())]);
        let record = RecordRef::new(&encoded).expect("parse");
        match record.field(0).expect("field") {
            ValueRef::Text(text) => {
                assert_eq!(text, "borrowed");
                // The str must point into the record buffer itself.
                let base = encoded.as_ptr() as usize;
                let borrowed = text.as_ptr() as usize;
                assert!((base..base + encoded.len()).contains(&borrowed));
            }
            other => panic!("expected text, got {other:?}"),
        }
    }

    #[test]
    fn truncation_is_rejected_at_every_length() {
        let encoded = encode(&[
            Value::Integer(70_000),
            Value::Text("abcdef".to_string()),
            Value::Blob(vec![9; 10]),
        ]);
        for short in 0..encoded.len() {
            let result = RecordRef::new(&encoded[..short]).and_then(|r| r.to_values());
            assert!(result.is_err(), "accepted a {short} byte truncation");
        }
        assert!(decode(&encoded).is_ok());
    }

    #[test]
    fn reserved_serial_types_are_rejected() {
        for code in [6u8, 7] {
            // header_len = 2 (one byte for itself, one serial type), then the code.
            let record = [
                {
                    let mut buffer = [0u8; 9];
                    varint::write(&mut buffer, 2);
                    buffer[0]
                },
                {
                    let mut buffer = [0u8; 9];
                    varint::write(&mut buffer, code as u64);
                    buffer[0]
                },
            ];
            assert!(decode(&record).is_err(), "accepted reserved code {code}");
        }
    }

    #[test]
    fn a_header_length_past_the_end_is_rejected() {
        let mut encoded = encode(&[Value::Integer(1)]);
        encoded[0] = {
            let mut buffer = [0u8; 9];
            varint::write(&mut buffer, 200);
            buffer[0]
        };
        assert!(RecordRef::new(&encoded).is_err());
    }

    fn arbitrary_value() -> impl Strategy<Value = Value> {
        prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Boolean),
            any::<i32>().prop_map(Value::Integer),
            any::<i64>().prop_map(Value::BigInt),
            any::<u32>().prop_map(Value::UInteger),
            any::<u64>().prop_map(Value::UBigInt),
            any::<f64>().prop_map(Value::Float),
            any::<f32>().prop_map(Value::Float32),
            ".{0,64}".prop_map(Value::Text),
            proptest::collection::vec(any::<u8>(), 0..64).prop_map(Value::Blob),
        ]
    }

    proptest! {
        #[test]
        fn any_row_roundtrips(values in proptest::collection::vec(arbitrary_value(), 0..20)) {
            let encoded = encode(&values);
            let decoded = decode(&encoded).expect("decode");
            prop_assert_eq!(decoded.len(), values.len());
            for (original, result) in values.iter().zip(&decoded) {
                match (original, result) {
                    (Value::Float(a), Value::Float(b)) => prop_assert_eq!(a.to_bits(), b.to_bits()),
                    (Value::Float32(a), Value::Float32(b)) => prop_assert_eq!(a.to_bits(), b.to_bits()),
                    _ => prop_assert_eq!(original, result),
                }
            }
        }

        /// Reading field `i` directly must agree with decoding the whole record.
        #[test]
        fn random_access_agrees(values in proptest::collection::vec(arbitrary_value(), 1..12)) {
            let encoded = encode(&values);
            let record = RecordRef::new(&encoded).expect("parse");
            prop_assert_eq!(record.field_count().expect("count"), values.len());
            let all = record.to_values().expect("values");
            for index in 0..values.len() {
                let direct = record.field(index).expect("field").to_value().expect("value");
                match (&all[index], &direct) {
                    (Value::Float(a), Value::Float(b)) => prop_assert_eq!(a.to_bits(), b.to_bits()),
                    (Value::Float32(a), Value::Float32(b)) => prop_assert_eq!(a.to_bits(), b.to_bits()),
                    (a, b) => prop_assert_eq!(a, b),
                }
            }
        }

        /// Nothing decodable may panic, whatever the bytes are.
        #[test]
        fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..128)) {
            let _ = decode(&bytes);
        }

        /// `RowCodec::encode_values` must add nothing on top of the record encoding.
        ///
        /// It delegates to [`encode`], so this holds the facade to the codec rather
        /// than comparing two encodings.
        #[test]
        fn never_larger_than_the_old_format(
            values in proptest::collection::vec(arbitrary_value(), 0..20)
        ) {
            let old = crate::catalog::serialization::RowCodec::encode_values(&values)
                .expect("old encode");
            prop_assert!(
                encode(&values).len() <= old.len(),
                "v4 encoding grew: {} > {}",
                encode(&values).len(),
                old.len()
            );
        }
    }
}
