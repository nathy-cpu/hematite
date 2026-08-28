//! The index-key encoding: how a set of values becomes one sortable byte string.
//!
//! Reached through [`crate::catalog::serialization::IndexKeyCodec`], which is the name
//! the rest of the catalog calls.
//!
//! # The contract
//!
//! Two properties, and everything else follows from them:
//!
//! 1. **Order preservation.** `encode(a) < encode(b)` as byte strings exactly when
//!    `a < b` as values. The B-tree compares keys with `memcmp` and nothing else, so
//!    any gap between the two orders is a wrong query result, not a slow one.
//! 2. **Prefix composition.** Each component is self-terminating, so `encode(&[a])`
//!    is a byte prefix of `encode(&[a, b])`. That is what makes a prefix scan over
//!    the leading columns of a composite index work.
//!
//! Index keys are never decoded back into values — callers only compare them and
//! split the trailing row id off — so the encoding is free to be lossy about which
//! Rust variant a number came from, and it uses that freedom to put all integers
//! into a single order.
//!
//! # Three ordering bugs this fixed
//!
//! - **Text and blob lengths.** The old encoding prefixes bytes with a
//!   *little-endian* `u32` length. Comparing `"b"` against `"ab"` compares `01` and
//!   `02` first, so `"b"` sorts before `"ab"`. Every text index was ordered by
//!   length-low-byte first. Replaced by escape-and-terminate, which is also two
//!   bytes shorter.
//! - **Integer variants.** The old encoding gave `Integer`, `BigInt`, `UInteger` and
//!   `UBigInt` four different tag bytes, so in a column holding a mix, every `i32`
//!   sorted before every `i64` regardless of magnitude. Now there are two integer
//!   tags — negative and non-negative — covering `i64::MIN..=u64::MAX` in one order.
//! - **Decimals.** The old encoding emitted scale before digits, so `1.5` (scale 1)
//!   sorted after `2` (scale 0). Now decimals are normalised to a
//!   sign/exponent/mantissa form that compares correctly.
//!
//! # Tags
//!
//! | tag | value | body |
//! |---|---|---|
//! | `0x00` | `Null` | — |
//! | `0x01` | `Boolean(false)` | — |
//! | `0x02` | `Boolean(true)` | — |
//! | `0x03` | integer `< 0` | `255 - len`, then `len` complemented bytes |
//! | `0x04` | integer `>= 0` | `len`, then `len` big-endian bytes |
//! | `0x05` | float | 8 bytes, total-order transform |
//! | `0x06` | decimal | see [`encode_decimal`] |
//! | `0x07` | text | escaped, terminated |
//! | `0x08` | blob | escaped, terminated |
//!
//! Tag order is the type order: nulls first, then booleans, then all numbers, then
//! text, then blobs. The tag byte puts every negative before every non-negative, so
//! the two integer forms never compare against each other.
//!
//! Integers carry only the bytes they need, with the length written first so that it
//! sorts the same way the digits do. See [`encode_integer`] for why the negative form
//! is mirrored.
//!
//! Floats and integers do not interleave: all integers sort before all floats. A
//! column is one or the other in practice, and making them interleave would mean
//! choosing a common representation that can hold `u64::MAX` and `f64` exactly,
//! which no eight-byte encoding does.

use crate::btree::varint;
use crate::catalog::{DecimalValue, Value};
use crate::error::{HematiteError, Result};

const TAG_NULL: u8 = 0x00;
const TAG_FALSE: u8 = 0x01;
const TAG_TRUE: u8 = 0x02;
const TAG_NEGATIVE_INT: u8 = 0x03;
const TAG_NON_NEGATIVE_INT: u8 = 0x04;
const TAG_FLOAT: u8 = 0x05;
const TAG_DECIMAL: u8 = 0x06;
const TAG_TEXT: u8 = 0x07;
const TAG_BLOB: u8 = 0x08;

/// Decimal sign classes, ordered so the class byte alone sorts negatives first.
const DECIMAL_NEGATIVE: u8 = 0;
const DECIMAL_ZERO: u8 = 1;
const DECIMAL_POSITIVE: u8 = 2;

/// Byte strings are escaped so they can be terminated unambiguously.
///
/// A literal `0x00` becomes `0x00 0xFF`; the terminator is `0x00 0x01`. Because
/// `0x01 < 0xFF`, a string that ends where another continues with an escaped zero
/// sorts first, which is the correct order. Because `0x00` is below every other
/// byte, a string that ends where another continues with any ordinary byte also
/// sorts first.
const ESCAPE: u8 = 0x00;
const ESCAPED_ZERO: u8 = 0xFF;
const TERMINATOR: u8 = 0x01;

fn corrupt(message: impl Into<String>) -> HematiteError {
    HematiteError::CorruptedData(message.into())
}

fn encode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.reserve(bytes.len() + 2);
    for byte in bytes {
        out.push(*byte);
        if *byte == ESCAPE {
            out.push(ESCAPED_ZERO);
        }
    }
    out.push(ESCAPE);
    out.push(TERMINATOR);
}

/// Maps a float onto a `u64` whose big-endian bytes sort in float order.
///
/// Positive values get their sign bit set so they sort above negatives; negative
/// values are inverted so that more-negative sorts lower. NaN lands above every
/// real value, consistently.
fn ordered_float_bits(value: f64) -> [u8; 8] {
    // `-0.0 == 0.0`, so they are one value and must produce one key — otherwise a
    // unique index would accept both. Their bit patterns differ, so collapse first.
    // The comparison catches both zeros.
    let value = if value == 0.0 { 0.0 } else { value };
    let bits = value.to_bits();
    let transformed = if bits >> 63 == 0 {
        bits ^ (1u64 << 63)
    } else {
        !bits
    };
    transformed.to_be_bytes()
}

/// Normalised decimal: sign, base-10 exponent, and significant digits.
///
/// The value is `0.d[0]d[1].. * 10^exponent`, with leading and trailing zeros
/// stripped so that `1.50` and `1.5` produce identical keys.
struct NormalizedDecimal<'a> {
    negative: bool,
    exponent: i64,
    digits: &'a [u8],
}

fn normalize_decimal(decimal: &DecimalValue) -> Option<NormalizedDecimal<'_>> {
    let digits = decimal.digit_bytes();
    let first = digits.iter().position(|digit| *digit != 0)?;
    let last = digits
        .iter()
        .rposition(|digit| *digit != 0)
        .expect("a first non-zero digit implies a last one");

    Some(NormalizedDecimal {
        negative: decimal.negative(),
        // Digits before the decimal point, less the leading zeros we dropped.
        exponent: digits.len() as i64 - decimal.scale() as i64 - first as i64,
        digits: &digits[first..=last],
    })
}

/// `[class] [exponent] [digits] [0x00]`, with everything after the class byte
/// complemented when the value is negative.
///
/// Digits are stored offset by one, so they occupy `1..=10` and the `0x00`
/// terminator stays below all of them: a shorter mantissa is a prefix of a longer
/// one and must sort first, since `0.12 < 0.123`. Complementing for negatives turns
/// the terminator into `0xFF`, above every complemented digit, which is again
/// correct: `-0.12 > -0.123`.
fn encode_decimal(out: &mut Vec<u8>, decimal: &DecimalValue) {
    let Some(normalized) = normalize_decimal(decimal) else {
        out.push(DECIMAL_ZERO);
        return;
    };

    let start = out.len();
    out.push(if normalized.negative {
        DECIMAL_NEGATIVE
    } else {
        DECIMAL_POSITIVE
    });

    // Flip the sign bit so the signed exponent sorts correctly as unsigned bytes.
    out.extend_from_slice(&((normalized.exponent as u64) ^ (1u64 << 63)).to_be_bytes());
    for digit in normalized.digits {
        out.push(digit + 1);
    }
    out.push(0x00);

    if normalized.negative {
        for byte in &mut out[start + 1..] {
            *byte = !*byte;
        }
    }
}

/// Appends one value's key encoding to `out`.
pub(crate) fn encode_value(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => out.push(TAG_NULL),
        Value::Boolean(false) => out.push(TAG_FALSE),
        Value::Boolean(true) => out.push(TAG_TRUE),
        Value::Integer(v) => encode_integer(out, *v as i64 as u64, *v < 0),
        Value::BigInt(v) => encode_integer(out, *v as u64, *v < 0),
        Value::UInteger(v) => encode_integer(out, *v as u64, false),
        Value::UBigInt(v) => encode_integer(out, *v, false),
        // Widening f32 to f64 is exact, so the two float types share one order.
        Value::Float32(v) => {
            out.push(TAG_FLOAT);
            out.extend_from_slice(&ordered_float_bits(*v as f64));
        }
        Value::Float(v) => {
            out.push(TAG_FLOAT);
            out.extend_from_slice(&ordered_float_bits(*v));
        }
        Value::Decimal(v) => {
            out.push(TAG_DECIMAL);
            encode_decimal(out, v);
        }
        Value::Text(v) => {
            out.push(TAG_TEXT);
            encode_bytes(out, v.as_bytes());
        }
        Value::Blob(v) => {
            out.push(TAG_BLOB);
            encode_bytes(out, v);
        }
    }
}

/// Writes an integer as a tag, a length, and only the bytes it needs.
///
/// Order survives because the length is written before the digits and sorts the same
/// way they do: a number needing more bytes is larger, and two numbers of equal width
/// compare correctly as big-endian bytes.
///
/// Negatives are mirrored. `!v` maps `-1` to 0 and `i64::MIN` to `2^63 - 1`, so more
/// negative means a *longer* encoding — the opposite of what the order needs. Writing
/// `255 - len` and complementing the digits reverses both comparisons, so more
/// negative sorts earlier. The tag still puts every negative before every
/// non-negative, so the two schemes never have to compare against each other.
///
/// Width-tagged rather than a fixed eight bytes per integer. A single order has to
/// span `i64::MIN..=u64::MAX`, and paying for that range on every key would make a
/// small integer nine bytes; here it costs three, and zero costs two.
fn encode_integer(out: &mut Vec<u8>, bits: u64, negative: bool) {
    if negative {
        out.push(TAG_NEGATIVE_INT);
        // Distance below -1, so -1 is the shortest negative and i64::MIN the longest.
        let magnitude = !bits;
        let len = significant_bytes(magnitude);
        out.push(255 - len as u8);
        for byte in &magnitude.to_be_bytes()[8 - len..] {
            out.push(!byte);
        }
    } else {
        out.push(TAG_NON_NEGATIVE_INT);
        let len = significant_bytes(bits);
        out.push(len as u8);
        out.extend_from_slice(&bits.to_be_bytes()[8 - len..]);
    }
}

/// Bytes needed to represent `value`, and zero for zero.
fn significant_bytes(value: u64) -> usize {
    if value == 0 {
        0
    } else {
        8 - (value.leading_zeros() as usize / 8)
    }
}

/// Encodes a composite key.
pub(crate) fn encode_key(values: &[Value]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_key_into(&mut out, values);
    out
}

/// Encodes into a caller-owned buffer, which lets a loop over many rows reuse one
/// allocation instead of making a fresh `Vec` per row.
pub(crate) fn encode_key_into(out: &mut Vec<u8>, values: &[Value]) {
    out.clear();
    for value in values {
        encode_value(out, value);
    }
}

/// A secondary-index key: the indexed values, then the row id.
///
/// The row id makes otherwise-equal keys unique, and it is recoverable from the key
/// itself — so unlike the old format, the entry's *value* stays empty rather than
/// storing a second copy of the same eight bytes.
/// Appends the row id that makes an index entry unique and orders duplicates.
///
/// Encoded as `[len][big-endian magnitude][len]`, not a fixed eight bytes. Both the
/// ordering and the parsing constrain the shape:
///
/// * The tree compares whole keys as bytes. Two entries only reach their row ids when
///   their value prefixes are equal, so both suffixes start at the same offset, and
///   the **leading** length is what makes the comparison correct: a smaller row id has
///   no more significant bytes than a larger one, so comparing lengths first and
///   magnitudes second reproduces integer order. A trailing length alone would sort
///   row id 5 after row id 256.
/// * `split_secondary_key` is handed a whole key and no column count, so it has to
///   find the suffix from the end — hence the **trailing** copy. It only affects
///   comparisons where the leading lengths were already equal, so it cannot change
///   the order.
///
/// Costs `2 + len` bytes against a flat 8: four bytes for row ids up to 65 535.
pub(crate) fn encode_secondary_key(values: &[Value], row_id: u64) -> Vec<u8> {
    let mut key = encode_key(values);
    let len = significant_bytes(row_id).max(1);
    key.push(len as u8);
    key.extend_from_slice(&row_id.to_be_bytes()[8 - len..]);
    key.push(len as u8);
    key
}

/// Splits a secondary-index key into its value prefix and trailing row id.
///
/// Borrows rather than copying; the old signature returned an owned `Vec` for the
/// prefix, which every caller then dropped.
pub(crate) fn split_secondary_key(key: &[u8]) -> Result<(&[u8], u64)> {
    let Some(&len) = key.last() else {
        return Err(corrupt("Index entry is missing rowid bytes"));
    };
    let len = len as usize;
    if len == 0 || len > 8 || key.len() < len + 2 {
        return Err(corrupt("Index entry has a malformed rowid suffix"));
    }
    let split = key.len() - len - 2;
    if key[split] as usize != len {
        return Err(corrupt("Index entry rowid length markers disagree"));
    }
    let mut bytes = [0u8; 8];
    bytes[8 - len..].copy_from_slice(&key[split + 1..key.len() - 1]);
    Ok((&key[..split], u64::from_be_bytes(bytes)))
}

/// A primary-key index entry's payload: the row id as a varint.
///
/// Was a fixed eight bytes; small row ids now cost one or two.
pub(crate) fn encode_row_id_value(row_id: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(varint::len_of(row_id));
    varint::write_to_vec(&mut out, row_id);
    out
}

pub(crate) fn decode_row_id_value(bytes: &[u8]) -> Result<u64> {
    let (row_id, consumed) =
        varint::read(bytes, 0).ok_or_else(|| corrupt("Index rowid payload is truncated"))?;
    if consumed != bytes.len() {
        return Err(corrupt(format!(
            "Index rowid payload has {} trailing bytes",
            bytes.len() - consumed
        )));
    }
    Ok(row_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn key(value: Value) -> Vec<u8> {
        encode_key(&[value])
    }

    fn assert_ordered(values: &[Value]) {
        for window in values.windows(2) {
            let left = key(window[0].clone());
            let right = key(window[1].clone());
            assert!(
                left < right,
                "order violated: {:?} should encode below {:?}\n  {:?}\n  {:?}",
                window[0],
                window[1],
                left,
                right
            );
        }
    }

    #[test]
    fn text_orders_lexicographically() {
        // The case a little-endian length prefix gets wrong: "b" has a shorter
        // length prefix than "ab", so it would sort first.
        assert_ordered(&[
            Value::Text(String::new()),
            Value::Text("a".to_string()),
            Value::Text("ab".to_string()),
            Value::Text("abc".to_string()),
            Value::Text("b".to_string()),
            Value::Text("ba".to_string()),
            Value::Text("z".to_string()),
        ]);
    }

    #[test]
    fn embedded_zero_bytes_keep_their_place() {
        assert_ordered(&[
            Value::Blob(vec![b'a']),
            Value::Blob(vec![b'a', 0]),
            Value::Blob(vec![b'a', 0, 0]),
            Value::Blob(vec![b'a', 1]),
            Value::Blob(vec![b'b']),
        ]);
    }

    /// Small integers should not cost what large ones do.
    ///
    /// A single order has to span `i64::MIN..=u64::MAX`, and a fixed-width body
    /// wide enough for that would make every key pay for the largest one.
    #[test]
    fn integer_keys_cost_only_the_bytes_they_need() {
        // tag + length + digits
        assert_eq!(key(Value::Integer(0)).len(), 2);
        assert_eq!(key(Value::Integer(1)).len(), 3);
        assert_eq!(key(Value::Integer(255)).len(), 3);
        assert_eq!(key(Value::Integer(256)).len(), 4);
        // -1 is the shortest negative: its distance below -1 is zero, so there are
        // no digits to write, exactly as for 0 on the other side.
        assert_eq!(key(Value::Integer(-1)).len(), 2);
        assert_eq!(key(Value::Integer(-2)).len(), 3);
        assert_eq!(key(Value::Integer(-256)).len(), 3);
        assert_eq!(key(Value::Integer(-257)).len(), 4);
        assert_eq!(key(Value::BigInt(i64::MAX)).len(), 10);
        assert_eq!(key(Value::UBigInt(u64::MAX)).len(), 10);

        // A typical indexed integer column, against the nine a fixed-width body
        // would take.
        assert!(key(Value::Integer(42)).len() < 9);
    }

    #[test]
    fn integer_variants_share_one_order() {
        // The old encoding tagged each Rust variant separately, so every i32 sorted
        // below every i64 no matter the magnitude.
        assert_ordered(&[
            Value::BigInt(i64::MIN),
            Value::Integer(i32::MIN),
            Value::Integer(-1),
            Value::Integer(0),
            Value::Integer(1),
            Value::BigInt(2),
            Value::UInteger(3),
            Value::Integer(i32::MAX),
            Value::UInteger(u32::MAX),
            Value::BigInt(i64::MAX),
            Value::UBigInt(u64::MAX),
        ]);
    }

    #[test]
    fn a_mixed_integer_column_sorts_by_value() {
        let mut keys: Vec<(Vec<u8>, i128)> = vec![
            (key(Value::BigInt(5_000_000_000)), 5_000_000_000),
            (key(Value::Integer(7)), 7),
            (key(Value::UBigInt(1)), 1),
            (key(Value::Integer(-3)), -3),
            (key(Value::BigInt(-9_000_000_000)), -9_000_000_000),
            (key(Value::UInteger(4_000_000_000)), 4_000_000_000),
        ];
        keys.sort();
        let sorted: Vec<i128> = keys.iter().map(|(_, value)| *value).collect();
        assert_eq!(
            sorted,
            vec![-9_000_000_000, -3, 1, 7, 4_000_000_000, 5_000_000_000]
        );
    }

    #[test]
    fn floats_order_including_infinities() {
        assert_ordered(&[
            Value::Float(f64::NEG_INFINITY),
            Value::Float(-1e300),
            Value::Float(-1.5),
            Value::Float(0.0),
            Value::Float(1.5),
            Value::Float(1e300),
            Value::Float(f64::INFINITY),
        ]);
        // f32 and f64 interleave, since widening is exact.
        assert!(key(Value::Float32(1.0)) < key(Value::Float(2.0)));
        assert!(key(Value::Float(1.0)) < key(Value::Float32(2.0)));
    }

    #[test]
    fn negative_and_positive_zero_compare_equal() {
        // They are the same number, so they must produce the same key — otherwise a
        // unique index would accept both.
        assert_eq!(key(Value::Float(0.0)), key(Value::Float(-0.0)));
    }

    #[test]
    fn decimals_order_by_magnitude_not_scale() {
        // The old encoding put scale before the digits, so 1.5 sorted after 2.
        let ordered = [
            "-1000", "-2", "-1.5", "-1", "-0.5", "-0.001", "0", "0.001", "0.5", "1", "1.5", "2",
            "10", "1000",
        ];
        let values: Vec<Value> = ordered
            .iter()
            .map(|text| Value::Decimal(DecimalValue::parse(text).expect("parse")))
            .collect();
        assert_ordered(&values);
    }

    #[test]
    fn decimals_that_are_equal_encode_equally() {
        // Trailing and leading zeros do not change the value, so they must not
        // change the key.
        for (left, right) in [("1.5", "1.50"), ("2", "2.000"), ("0.5", "0.50")] {
            assert_eq!(
                key(Value::Decimal(DecimalValue::parse(left).expect("parse"))),
                key(Value::Decimal(DecimalValue::parse(right).expect("parse"))),
                "{left} and {right} should encode identically"
            );
        }
    }

    #[test]
    fn types_sort_in_tag_order() {
        assert_ordered(&[
            Value::Null,
            Value::Boolean(false),
            Value::Boolean(true),
            Value::Integer(i32::MIN),
            Value::UBigInt(u64::MAX),
            Value::Float(f64::NEG_INFINITY),
            Value::Decimal(DecimalValue::parse("-99999").expect("parse")),
            Value::Text(String::new()),
            Value::Blob(Vec::new()),
        ]);
    }

    /// Prefix composition: without it, a scan over the leading columns of a
    /// composite index cannot be expressed as a byte-prefix scan.
    #[test]
    fn a_leading_column_key_is_a_prefix_of_the_full_key() {
        let cases: Vec<Vec<Value>> = vec![
            vec![Value::Integer(7), Value::Text("tail".to_string())],
            vec![Value::Text("lead".to_string()), Value::Integer(1)],
            vec![Value::Text(String::new()), Value::Null],
            vec![Value::Blob(vec![0, 0]), Value::Boolean(true)],
            vec![
                Value::Decimal(DecimalValue::parse("-1.25").expect("parse")),
                Value::Text("x".to_string()),
            ],
        ];
        for values in cases {
            let full = encode_key(&values);
            let prefix = encode_key(&values[..1]);
            assert!(
                full.starts_with(&prefix),
                "{:?} is not a prefix of {:?}",
                prefix,
                full
            );
        }
    }

    /// A prefix scan must match every key sharing the leading column and nothing
    /// else — in particular, not a different value whose encoding happens to start
    /// with the same bytes.
    #[test]
    fn a_prefix_scan_does_not_leak_into_neighbouring_values() {
        let prefix = encode_key(&[Value::Text("ab".to_string())]);
        let matching = encode_key(&[Value::Text("ab".to_string()), Value::Integer(1)]);
        let sibling = encode_key(&[Value::Text("abc".to_string()), Value::Integer(1)]);

        assert!(matching.starts_with(&prefix));
        assert!(!sibling.starts_with(&prefix));
    }

    #[test]
    fn secondary_keys_split_back_apart() {
        let values = vec![Value::Text("name".to_string()), Value::Integer(3)];
        for row_id in [0u64, 1, 255, 70_000, u64::MAX] {
            let key = encode_secondary_key(&values, row_id);
            let (prefix, decoded) = split_secondary_key(&key).expect("split");
            assert_eq!(decoded, row_id);
            assert_eq!(prefix, encode_key(&values).as_slice());
        }
    }

    #[test]
    fn secondary_keys_sort_by_value_then_row_id() {
        let a = encode_secondary_key(&[Value::Integer(1)], 2);
        let b = encode_secondary_key(&[Value::Integer(1)], 10);
        let c = encode_secondary_key(&[Value::Integer(2)], 0);
        assert!(a < b, "row id should break ties");
        assert!(b < c, "value should dominate row id");
    }

    #[test]
    fn short_keys_are_rejected_rather_than_panicking() {
        for len in 0..8usize {
            assert!(split_secondary_key(&vec![0u8; len]).is_err());
        }
    }

    #[test]
    fn row_id_values_roundtrip_and_shrink() {
        for row_id in [0u64, 1, 127, 128, 70_000, u64::MAX] {
            let encoded = encode_row_id_value(row_id);
            assert_eq!(decode_row_id_value(&encoded).expect("decode"), row_id);
        }
        // The old format spent eight bytes on every row id.
        assert_eq!(encode_row_id_value(1).len(), 1);
        assert_eq!(encode_row_id_value(70_000).len(), 3);
        assert!(decode_row_id_value(&[]).is_err());
    }

    fn comparable_value() -> impl Strategy<Value = Value> {
        // One type per run: cross-type ordering is fixed by tag and tested above.
        prop_oneof![
            any::<i64>().prop_map(Value::BigInt),
            ".{0,32}".prop_map(Value::Text),
            proptest::collection::vec(any::<u8>(), 0..32).prop_map(Value::Blob),
        ]
    }

    proptest! {
        #[test]
        fn integer_order_is_preserved(a in any::<i64>(), b in any::<i64>()) {
            let left = key(Value::BigInt(a));
            let right = key(Value::BigInt(b));
            prop_assert_eq!(left.cmp(&right), a.cmp(&b));
        }

        /// A secondary-index key is a value prefix plus the row id, and the tree
        /// compares the whole thing as bytes. So byte order has to match
        /// `(values, row_id)` order — including when the two entries share a value
        /// prefix, which is the only time the row id decides anything.
        #[test]
        fn secondary_key_order_matches_value_then_row_id(
            a in any::<i64>(),
            ra in any::<u64>(),
            b in any::<i64>(),
            rb in any::<u64>(),
        ) {
            let left = encode_secondary_key(&[Value::BigInt(a)], ra);
            let right = encode_secondary_key(&[Value::BigInt(b)], rb);
            prop_assert_eq!(left.cmp(&right), (a, ra).cmp(&(b, rb)));
        }

        /// And the row id has to survive the round trip, since callers read it back
        /// off the end of the key.
        #[test]
        fn secondary_key_row_id_round_trips(a in any::<i64>(), row_id in any::<u64>()) {
            let key = encode_secondary_key(&[Value::BigInt(a)], row_id);
            let (prefix, decoded) = split_secondary_key(&key).expect("split");
            let expected_prefix = encode_key(&[Value::BigInt(a)]);
            prop_assert_eq!(decoded, row_id);
            prop_assert_eq!(prefix, expected_prefix.as_slice());
        }

        #[test]
        fn unsigned_order_is_preserved(a in any::<u64>(), b in any::<u64>()) {
            let left = key(Value::UBigInt(a));
            let right = key(Value::UBigInt(b));
            prop_assert_eq!(left.cmp(&right), a.cmp(&b));
        }

        #[test]
        fn text_order_is_preserved(a in ".{0,40}", b in ".{0,40}") {
            let left = key(Value::Text(a.clone()));
            let right = key(Value::Text(b.clone()));
            prop_assert_eq!(left.cmp(&right), a.as_bytes().cmp(b.as_bytes()));
        }

        #[test]
        fn blob_order_is_preserved(
            a in proptest::collection::vec(any::<u8>(), 0..40),
            b in proptest::collection::vec(any::<u8>(), 0..40),
        ) {
            let left = key(Value::Blob(a.clone()));
            let right = key(Value::Blob(b.clone()));
            prop_assert_eq!(left.cmp(&right), a.cmp(&b));
        }

        #[test]
        fn float_order_is_preserved(a in any::<f64>(), b in any::<f64>()) {
            prop_assume!(!a.is_nan() && !b.is_nan());
            let left = key(Value::Float(a));
            let right = key(Value::Float(b));
            prop_assert_eq!(left.cmp(&right), a.partial_cmp(&b).expect("not NaN"));
        }

        /// Composite keys must order lexicographically by component.
        #[test]
        fn composite_order_follows_component_order(
            a in comparable_value(),
            b in comparable_value(),
            tail_a in any::<i64>(),
            tail_b in any::<i64>(),
        ) {
            prop_assume!(std::mem::discriminant(&a) == std::mem::discriminant(&b));
            let left = encode_key(&[a.clone(), Value::BigInt(tail_a)]);
            let right = encode_key(&[b.clone(), Value::BigInt(tail_b)]);
            let head = key(a).cmp(&key(b));
            let expected = if head == std::cmp::Ordering::Equal {
                tail_a.cmp(&tail_b)
            } else {
                head
            };
            prop_assert_eq!(left.cmp(&right), expected);
        }

        #[test]
        fn prefix_composition_always_holds(
            head in comparable_value(),
            tail in comparable_value(),
        ) {
            let full = encode_key(&[head.clone(), tail]);
            prop_assert!(full.starts_with(&key(head)));
        }

        #[test]
        fn secondary_keys_always_split(
            values in proptest::collection::vec(comparable_value(), 1..4),
            row_id in any::<u64>(),
        ) {
            let key = encode_secondary_key(&values, row_id);
            let expected_prefix = encode_key(&values);
            let (prefix, decoded) = split_secondary_key(&key).expect("split");
            prop_assert_eq!(decoded, row_id);
            prop_assert_eq!(prefix, expected_prefix.as_slice());
        }
    }
}
