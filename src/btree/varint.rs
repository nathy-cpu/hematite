//! Variable-length integers for the v4 on-disk format.
//!
//! Cell headers, record headers and serial type codes are all overwhelmingly small
//! numbers stored in fixed-width fields today: a row of ten small integers spends
//! four bytes on each one, and every B-tree cell spends four bytes on two `u16`
//! lengths. Encoding them by magnitude is where most of the disk-space win lives.
//!
//! # Format
//!
//! The length is carried in the number of trailing zero bits of the first byte, so
//! decoding is a load and a shift rather than a loop over continuation bits.
//!
//! | first byte | total bytes | payload bits | maximum value |
//! |---|---|---|---|
//! | `xxxxxxx1` | 1 | 7 | 127 |
//! | `xxxxxx10` | 2 | 14 | 16 383 |
//! | `xxxxx100` | 3 | 21 | 2 097 151 |
//! | `xxxx1000` | 4 | 28 | |
//! | `xxx10000` | 5 | 35 | |
//! | `xx100000` | 6 | 42 | |
//! | `x1000000` | 7 | 49 | |
//! | `10000000` | 8 | 56 | |
//! | `00000000` | 9 | 64 | `u64::MAX` |
//!
//! For the one-to-eight byte forms the value occupies the remaining bits of a
//! little-endian integer: `encoded = (value << len) | (1 << (len - 1))`. Since the
//! value has at most `len * 7` bits, `value << len` needs `len * 8` bits — exactly
//! the space available. The nine-byte form drops the packing entirely and stores a
//! plain little-endian `u64` after a zero tag byte.
//!
//! Chosen over LEB128 and over SQLite's big-endian varint, both of which decode
//! through a loop with a data-dependent branch per byte. All three are equally
//! readable; this one decodes in a handful of instructions with no branch that a
//! predictor can miss.
//!
//! # The eight-byte read
//!
//! [`read`] loads eight bytes unconditionally when they are available, which is why
//! v4 pages leave [`crate::storage::PAGE_SIZE`] minus `USABLE_PAGE_SIZE` bytes of
//! readable slack at the end of every page: a varint starting in the last few bytes
//! of the content area still has eight readable bytes after it. Buffers without
//! that slack — a standalone record buffer, say — take [`read`]'s tail path, which
//! copies into a staging array first. Both paths decode identically.

// Reachable only from the v4 codecs, which are not wired in yet.
#![allow(dead_code)]

/// Largest value representable in the eight-byte form; anything larger takes nine.
const MAX_PACKED: u64 = (1u64 << 56) - 1;

/// Number of bytes [`write`] will produce for `value`.
#[inline]
pub(crate) fn len_of(value: u64) -> usize {
    if value > MAX_PACKED {
        return 9;
    }
    // 1 for zero, otherwise ceil(significant_bits / 7).
    let bits = 64 - value.leading_zeros() as usize;
    bits.div_ceil(7).max(1)
}

/// Writes `value` at the start of `out`, returning the number of bytes used.
///
/// # Panics
///
/// If `out` is shorter than [`len_of(value)`](len_of). Callers size their buffers
/// from `len_of` before reserving space in a page, so a panic here means the caller
/// computed a cell size differently from the way it wrote the cell.
#[inline]
pub(crate) fn write(out: &mut [u8], value: u64) -> usize {
    let len = len_of(value);
    assert!(
        out.len() >= len,
        "varint needs {len} bytes, buffer has {}",
        out.len()
    );

    if len == 9 {
        out[0] = 0;
        out[1..9].copy_from_slice(&value.to_le_bytes());
    } else {
        let packed = (value << len) | (1u64 << (len - 1));
        out[..len].copy_from_slice(&packed.to_le_bytes()[..len]);
    }
    len
}

/// Appends `value` to `out`.
#[inline]
pub(crate) fn write_to_vec(out: &mut Vec<u8>, value: u64) {
    let start = out.len();
    out.resize(start + len_of(value), 0);
    write(&mut out[start..], value);
}

/// Reads a varint at `offset`, returning the value and its encoded length.
///
/// Returns `None` when the encoding runs past the end of `bytes`, which for on-disk
/// data means corruption rather than a programming error, so callers get a value to
/// branch on rather than a panic.
#[inline]
pub(crate) fn read(bytes: &[u8], offset: usize) -> Option<(u64, usize)> {
    let tag = *bytes.get(offset)?;

    if tag == 0 {
        let end = offset.checked_add(9)?;
        if end > bytes.len() {
            return None;
        }
        let value = u64::from_le_bytes(bytes[offset + 1..end].try_into().unwrap());
        return Some((value, 9));
    }

    let len = tag.trailing_zeros() as usize + 1;
    let end = offset.checked_add(len)?;
    if end > bytes.len() {
        return None;
    }

    // Fast path: the caller's buffer has eight readable bytes here, so the load is
    // unconditional and the bounds check above is the only branch.
    let raw = if offset + 8 <= bytes.len() {
        u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
    } else {
        let mut staging = [0u8; 8];
        staging[..len].copy_from_slice(&bytes[offset..end]);
        u64::from_le_bytes(staging)
    };

    let mask = u64::MAX >> (64 - len * 7);
    Some(((raw >> len) & mask, len))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(value: u64) -> usize {
        let mut buffer = [0u8; 9];
        let written = write(&mut buffer, value);
        assert_eq!(written, len_of(value), "len_of disagreed with write");

        let (decoded, read_len) = read(&buffer, 0).expect("decode");
        assert_eq!(decoded, value, "roundtrip failed for {value}");
        assert_eq!(read_len, written, "length disagreement for {value}");
        written
    }

    #[test]
    fn boundaries_use_the_expected_widths() {
        // Each row is the last value of one width and the first of the next.
        let expected: [(u64, usize); 18] = [
            (0, 1),
            (127, 1),
            (128, 2),
            (16_383, 2),
            (16_384, 3),
            (2_097_151, 3),
            (2_097_152, 4),
            ((1 << 28) - 1, 4),
            (1 << 28, 5),
            ((1 << 35) - 1, 5),
            (1 << 35, 6),
            ((1 << 42) - 1, 6),
            (1 << 42, 7),
            ((1 << 49) - 1, 7),
            (1 << 49, 8),
            ((1 << 56) - 1, 8),
            (1 << 56, 9),
            (u64::MAX, 9),
        ];

        for (value, width) in expected {
            assert_eq!(roundtrip(value), width, "wrong width for {value}");
        }
    }

    /// The sizes that motivate the format: a small integer, a page offset, and a
    /// text length all have to be cheap.
    #[test]
    fn common_values_are_one_or_two_bytes() {
        for value in [0u64, 1, 5, 42, 100, 127] {
            assert_eq!(len_of(value), 1);
        }
        for value in [128u64, 1_000, 4_096, 16_383] {
            assert_eq!(len_of(value), 2);
        }
    }

    #[test]
    fn exhaustive_roundtrip_over_small_values() {
        for value in 0..100_000u64 {
            roundtrip(value);
        }
    }

    #[test]
    fn roundtrip_across_the_whole_range() {
        let mut value = 1u64;
        while value < u64::MAX / 3 {
            roundtrip(value - 1);
            roundtrip(value);
            roundtrip(value + 1);
            value = value.saturating_mul(3);
        }
        roundtrip(u64::MAX - 1);
        roundtrip(u64::MAX);
    }

    /// The staging-array path must agree with the eight-byte-load path. Every
    /// encoding is decoded from a buffer trimmed to exactly its own length, which
    /// forces the slow path for anything shorter than eight bytes.
    #[test]
    fn tail_path_agrees_with_fast_path() {
        for value in [
            0u64,
            1,
            127,
            128,
            16_383,
            1 << 20,
            1 << 40,
            1 << 55,
            u64::MAX,
        ] {
            let mut wide = [0u8; 16];
            let len = write(&mut wide, value);

            let (fast, fast_len) = read(&wide, 0).expect("fast path");
            let (slow, slow_len) = read(&wide[..len], 0).expect("tail path");

            assert_eq!(fast, slow, "paths disagree for {value}");
            assert_eq!(fast_len, len);
            assert_eq!(slow_len, len);
        }
    }

    #[test]
    fn reads_at_a_nonzero_offset() {
        let mut buffer = vec![0xFFu8; 3];
        let mut encoded = [0u8; 9];
        let len = write(&mut encoded, 300_000);
        buffer.extend_from_slice(&encoded[..len]);
        buffer.extend_from_slice(&[0xEE; 8]);

        assert_eq!(read(&buffer, 3), Some((300_000, len)));
    }

    #[test]
    fn truncated_input_is_rejected_rather_than_panicking() {
        let mut buffer = [0u8; 9];

        let len = write(&mut buffer, 1 << 40);
        for short in 0..len {
            assert_eq!(read(&buffer[..short], 0), None, "accepted {short} bytes");
        }

        // The nine-byte form, whose length is not derivable from a bit pattern.
        let len = write(&mut buffer, u64::MAX);
        assert_eq!(len, 9);
        for short in 0..9 {
            assert_eq!(read(&buffer[..short], 0), None, "accepted {short} bytes");
        }

        assert_eq!(read(&buffer, 9), None, "read past the end");
    }

    #[test]
    fn every_encoding_is_unique() {
        let mut seen = std::collections::HashMap::new();
        for value in 0..30_000u64 {
            let mut buffer = [0u8; 9];
            let len = write(&mut buffer, value);
            let encoded = buffer[..len].to_vec();
            if let Some(previous) = seen.insert(encoded.clone(), value) {
                panic!("{value} and {previous} both encode to {encoded:?}");
            }
        }
    }
}
