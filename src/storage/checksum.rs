//! The one checksum in the engine.
//!
//! Every durable structure — page trailers, rollback-journal records, WAL frames,
//! the database header — is verified with the same function, so there is one
//! implementation to get right and one set of reference vectors pinning it. It is
//! not built on `DefaultHasher`, whose output is explicitly not stable across Rust
//! releases: a header written by one toolchain could fail verification under
//! another.
//!
//! The algorithm is XXH64. Compared with the alternatives considered:
//!
//! - byte-wise FNV-1a processes one byte per multiply, roughly 0.6 GB/s;
//! - CRC32C by slicing-by-8 is faster but wants an 8 KiB table, which competes for
//!   L1 with the very page data the buffer pool is trying to keep hot, and only
//!   yields 32 bits;
//! - hardware CRC32C is far faster still but needs `unsafe`, runtime feature
//!   detection, a separate aarch64 path, and a scalar fallback — three
//!   implementations to maintain in an engine whose stated goal is being readable.
//!
//! XXH64 consumes 32 bytes per iteration through four independent accumulators,
//! which LLVM vectorises on its own, needs no table, and gives 64 bits of output.
//! It is written here in entirely safe code.
//!
//! Corruption is *detected*, never corrected, and there is no cross-implementation
//! wire format to interoperate with, so CRC's bounded-burst guarantee buys nothing
//! that matters here.
//!
//! No on-disk field names the checksum function; the format versions (`HMTD`,
//! `HMD1`, `HPM1`, `HTJ1`, `HTW1`, all version 1) imply it. Adopting hardware CRC32C
//! would therefore be a format-version bump.

const PRIME64_1: u64 = 0x9E37_79B1_85EB_CA87;
const PRIME64_2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const PRIME64_3: u64 = 0x1656_67B1_9E37_79F9;
const PRIME64_4: u64 = 0x85EB_CA77_C2B2_AE63;
const PRIME64_5: u64 = 0x27D4_EB2F_1656_67C5;

#[inline(always)]
fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

#[inline(always)]
fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

#[inline(always)]
fn round(acc: u64, input: u64) -> u64 {
    acc.wrapping_add(input.wrapping_mul(PRIME64_2))
        .rotate_left(31)
        .wrapping_mul(PRIME64_1)
}

#[inline(always)]
fn merge_round(acc: u64, val: u64) -> u64 {
    let val = round(0, val);
    (acc ^ val).wrapping_mul(PRIME64_1).wrapping_add(PRIME64_4)
}

#[inline(always)]
fn avalanche(mut acc: u64) -> u64 {
    acc ^= acc >> 33;
    acc = acc.wrapping_mul(PRIME64_2);
    acc ^= acc >> 29;
    acc = acc.wrapping_mul(PRIME64_3);
    acc ^= acc >> 32;
    acc
}

/// XXH64 over `bytes`.
///
/// The seed is how callers separate contexts that would otherwise hash identical
/// bytes to identical values: a page checksum seeds with the page id, so a page
/// image relocated to a different id fails verification instead of silently
/// validating; a WAL frame or journal record seeds with a hash of its own header
/// fields, so the checksum covers them without copying them next to the 4 KiB
/// payload.
pub fn hash64(seed: u64, bytes: &[u8]) -> u64 {
    let len = bytes.len();
    let mut offset = 0usize;

    let mut acc = if len >= 32 {
        let mut v1 = seed.wrapping_add(PRIME64_1).wrapping_add(PRIME64_2);
        let mut v2 = seed.wrapping_add(PRIME64_2);
        let mut v3 = seed;
        let mut v4 = seed.wrapping_sub(PRIME64_1);

        // Four independent dependency chains; this is the loop that vectorises.
        while len - offset >= 32 {
            v1 = round(v1, read_u64(bytes, offset));
            v2 = round(v2, read_u64(bytes, offset + 8));
            v3 = round(v3, read_u64(bytes, offset + 16));
            v4 = round(v4, read_u64(bytes, offset + 24));
            offset += 32;
        }

        let mut acc = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        acc = merge_round(acc, v1);
        acc = merge_round(acc, v2);
        acc = merge_round(acc, v3);
        acc = merge_round(acc, v4);
        acc
    } else {
        seed.wrapping_add(PRIME64_5)
    };

    acc = acc.wrapping_add(len as u64);

    while len - offset >= 8 {
        acc ^= round(0, read_u64(bytes, offset));
        acc = acc
            .rotate_left(27)
            .wrapping_mul(PRIME64_1)
            .wrapping_add(PRIME64_4);
        offset += 8;
    }

    if len - offset >= 4 {
        acc ^= (read_u32(bytes, offset) as u64).wrapping_mul(PRIME64_1);
        acc = acc
            .rotate_left(23)
            .wrapping_mul(PRIME64_2)
            .wrapping_add(PRIME64_3);
        offset += 4;
    }

    while offset < len {
        acc ^= (bytes[offset] as u64).wrapping_mul(PRIME64_5);
        acc = acc.rotate_left(11).wrapping_mul(PRIME64_1);
        offset += 1;
    }

    avalanche(acc)
}

/// The high 32 bits of [`hash64`], for the many on-disk fields that are `u32`.
///
/// The high half is used rather than the low half because the final avalanche step
/// ends with `acc ^= acc >> 32`, which mixes the high bits into the low ones but
/// not the reverse; the top half is therefore the better-conditioned of the two.
#[inline]
pub fn hash32(seed: u64, bytes: &[u8]) -> u32 {
    (hash64(seed, bytes) >> 32) as u32
}

/// A value that differs between one call and the next, to tell one generation of an
/// on-disk structure from a leftover of the one before it.
///
/// WAL salts and journal checksum seeds both use it. A structure whose old contents
/// may survive in a reused or unsynced-truncated file can only reject them if the
/// current generation checks against something the old one did not have; a fixed
/// constant makes that check pass for both.
///
/// `RandomState` is seeded by the OS per process, and mixing in the wall clock
/// separates generations created within one process. Neither is a cryptographic
/// source and neither needs to be: the requirement is that two generations differ,
/// not that an adversary cannot guess them.
pub(crate) fn fresh_nonce() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    use std::time::{SystemTime, UNIX_EPOCH};

    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos() as u64)
            .unwrap_or(0),
    );
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Published XXH64 reference vectors. These are the whole point of the module:
    /// they pin the on-disk checksum to a specification instead of to whatever this
    /// file happens to compute, so a refactor that changes the values fails loudly.
    #[test]
    fn matches_reference_vectors() {
        assert_eq!(hash64(0, b""), 0xEF46_DB37_51D8_E999);
        assert_eq!(
            hash64(0, b"Nobody inspects the spammish repetition"),
            0xFBCE_A83C_8A37_8BF1
        );
    }

    #[test]
    fn seed_separates_identical_bytes() {
        let page = [0xA5u8; 4096];
        assert_ne!(hash64(1, &page), hash64(2, &page));
    }

    /// Every tail length must be reachable, since page trailers, journal records
    /// and WAL frames all hash buffers whose length is not a multiple of 32.
    #[test]
    fn all_tail_lengths_are_distinct() {
        let bytes = [0x7Eu8; 96];
        let mut seen = std::collections::HashSet::new();
        for len in 0..=bytes.len() {
            assert!(
                seen.insert(hash64(0, &bytes[..len])),
                "collision at length {len}"
            );
        }
    }

    #[test]
    fn single_bit_flips_are_detected() {
        let mut page = [0u8; 4096];
        for (i, byte) in page.iter_mut().enumerate() {
            *byte = (i % 251) as u8;
        }
        let baseline = hash32(7, &page);
        for offset in [0usize, 1, 31, 32, 2048, 4094, 4095] {
            let mut corrupted = page;
            corrupted[offset] ^= 0x01;
            assert_ne!(
                hash32(7, &corrupted),
                baseline,
                "flip at offset {offset} not detected"
            );
        }
    }
}
