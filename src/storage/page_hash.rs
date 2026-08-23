//! A hasher for page-id keyed maps.
//!
//! The standard library's default hasher is SipHash-1-3, chosen so that a program
//! taking untrusted keys cannot be pushed into quadratic behaviour by an attacker
//! who picks colliding ones. That is the right default for a `HashMap<String, _>`
//! fed by a network, and the wrong one here: page ids are dense integers the engine
//! generates itself, an attacker never chooses them, and the map is probed on every
//! single buffer-pool lookup.
//!
//! This replaces it with Fibonacci hashing — one multiply by the 64-bit golden-ratio
//! constant, which spreads sequential ids across the whole word. That is two or
//! three instructions against SipHash's dozens of rounds, and page ids are exactly
//! the input it handles best: consecutive allocations land in unrelated buckets
//! rather than clustering.
//!
//! The hasher deliberately supports only `write_u32`/`write_u64`. Anything else
//! panics rather than silently hashing badly, so a map keyed by something other than
//! a page id cannot pick this up by accident.

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};

/// 2^64 divided by the golden ratio, rounded to an odd number.
///
/// Odd matters: it makes the multiply invertible modulo 2^64, so distinct inputs
/// always produce distinct hashes and the map never sees an avoidable collision.
const GOLDEN_RATIO_64: u64 = 0x9E37_79B9_7F4A_7C15;

#[derive(Default)]
pub(crate) struct PageIdHasher {
    hash: u64,
}

impl Hasher for PageIdHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }

    #[inline]
    fn write_u32(&mut self, value: u32) {
        self.write_u64(u64::from(value));
    }

    #[inline]
    fn write_u64(&mut self, value: u64) {
        // The high bits carry the most mixing after a multiply, and `HashMap` takes
        // its bucket index from the top of the hash, so no extra shift is needed.
        self.hash = value.wrapping_mul(GOLDEN_RATIO_64);
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("PageIdHasher is only for u32/u64 page-id keys");
    }
}

/// A `HashMap` keyed by page id.
pub(crate) type PageMap<V> = HashMap<u32, V, BuildHasherDefault<PageIdHasher>>;

/// A `HashSet` of page ids.
///
/// The pager keeps seven of these — journaled pages, captured pages, free-page
/// sets — and several are rebuilt per row, because a rollback savepoint is taken per
/// row. Under `RandomState` that made `HashMap<u32, ()>::insert` 2.9 % of a batched
/// insert, all of it SipHash over four bytes.
pub(crate) type PageSet = HashSet<u32, BuildHasherDefault<PageIdHasher>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_ids_hash_distinctly() {
        // Multiplying by an odd constant is a bijection modulo 2^64, so this holds
        // for every input, not just the ones sampled here.
        let mut seen = std::collections::HashSet::new();
        for page_id in 0..100_000u32 {
            let mut hasher = PageIdHasher::default();
            hasher.write_u32(page_id);
            assert!(seen.insert(hasher.finish()), "collision at {page_id}");
        }
    }

    /// The property that matters for a buffer pool: pages allocated one after
    /// another must not land in adjacent buckets, or a sequential scan turns into
    /// one long probe chain.
    #[test]
    fn sequential_ids_scatter_across_buckets() {
        let buckets = 1024u64;
        let mut hit = std::collections::HashSet::new();
        for page_id in 0..64u32 {
            let mut hasher = PageIdHasher::default();
            hasher.write_u32(page_id);
            hit.insert(hasher.finish() >> (64 - buckets.trailing_zeros()));
        }
        // 64 consecutive ids over 1024 buckets: with good scattering nearly all
        // land somewhere different.
        assert!(
            hit.len() >= 60,
            "poor spread: {} distinct buckets",
            hit.len()
        );
    }

    #[test]
    fn a_page_map_round_trips() {
        let mut map: PageMap<&str> = PageMap::default();
        map.insert(1, "one");
        map.insert(4096, "many");
        assert_eq!(map.get(&1), Some(&"one"));
        assert_eq!(map.get(&4096), Some(&"many"));
        assert_eq!(map.get(&2), None);
        assert_eq!(map.len(), 2);
    }
}
