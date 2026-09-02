//! A hasher for maps keyed by a schema identifier.
//!
//! The standard library's default is SipHash-1-3, chosen so that a program taking
//! untrusted keys cannot be pushed into quadratic behaviour by an attacker who picks
//! colliding ones. That is the right default for a `HashMap<String, _>` fed by a
//! network, and the wrong one for the maps that hold table and column names.
//!
//! What makes it the wrong one is not that the names are trusted — they arrive in SQL
//! text — but that a query cannot *grow* these maps. Their contents come from DDL:
//! `Schema::table_names` has an entry per table, `Table::column_indices` one per
//! column. A query can probe them as often as it likes and never add a key, so the
//! collision chains an attacker would need to lengthen are fixed by the schema, and
//! the attack SipHash defends against has nowhere to go. Meanwhile the probes are on
//! the hot path: resolving one table name was 8 % of a point lookup, all of it SipHash
//! over a handful of bytes.
//!
//! The replacement is the multiply-and-rotate hash rustc uses for its own interned
//! symbols — eight bytes of the key per multiply, against SipHash's rounds per block.
//! Unlike [`crate::storage::page_hash::PageIdHasher`] it accepts `write`, because the
//! keys here really are strings.

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};

/// The odd 64-bit constant rustc's hasher multiplies by. Odd matters: it makes the
/// multiply invertible modulo 2^64, so no two inputs collide through the multiply
/// itself.
const MULTIPLIER: u64 = 0x51_7c_c1_b7_27_22_0a_95;

#[derive(Default)]
pub(crate) struct NameHasher {
    hash: u64,
}

impl NameHasher {
    #[inline]
    fn mix(&mut self, word: u64) {
        // Rotate before mixing so that bytes landing in the low bits of one word do
        // not keep landing there: without it a set of keys differing only in their
        // last byte would only ever disturb the bottom of the hash.
        self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(MULTIPLIER);
    }
}

impl Hasher for NameHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut rest = bytes;
        while rest.len() >= 8 {
            let (word, tail) = rest.split_at(8);
            self.mix(u64::from_le_bytes(word.try_into().expect("8 bytes")));
            rest = tail;
        }
        if rest.len() >= 4 {
            let (word, tail) = rest.split_at(4);
            self.mix(u64::from(u32::from_le_bytes(
                word.try_into().expect("4 bytes"),
            )));
            rest = tail;
        }
        for &byte in rest {
            self.mix(u64::from(byte));
        }
    }

    #[inline]
    fn write_u8(&mut self, value: u8) {
        self.mix(u64::from(value));
    }

    #[inline]
    fn write_usize(&mut self, value: usize) {
        self.mix(value as u64);
    }
}

/// A `HashMap` keyed by a schema identifier.
pub(crate) type NameMap<K, V> = HashMap<K, V, BuildHasherDefault<NameHasher>>;

/// A `HashSet` of schema identifiers.
pub(crate) type NameSet<K> = HashSet<K, BuildHasherDefault<NameHasher>>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::BuildHasher;

    fn hash(value: &str) -> u64 {
        BuildHasherDefault::<NameHasher>::default().hash_one(value)
    }

    /// The property a hash map needs: distinct keys almost never share a hash, and in
    /// particular the identifier shapes a schema actually contains do not.
    #[test]
    fn realistic_identifiers_do_not_collide() {
        let mut names = Vec::new();
        for prefix in ["id", "name", "created_at", "user_id", "order_total", "a"] {
            names.push(prefix.to_string());
            for i in 0..200 {
                names.push(format!("{prefix}_{i}"));
                names.push(format!("{i}_{prefix}"));
            }
        }
        let unique: HashSet<u64> = names.iter().map(|name| hash(name)).collect();
        assert_eq!(unique.len(), names.len(), "two identifiers shared a hash");
    }

    /// Keys differing only in their last byte must not differ only in the low bits of
    /// the hash, because a `HashMap` takes its bucket from the top of the word.
    #[test]
    fn a_trailing_difference_reaches_the_high_bits() {
        let base = hash("column_name_a");
        let other = hash("column_name_b");
        assert_ne!(base >> 32, other >> 32);
    }

    /// A one-word key and its truncation must not agree.
    #[test]
    fn length_is_part_of_the_hash() {
        assert_ne!(hash(""), hash("a"));
        assert_ne!(hash("abcdefgh"), hash("abcdefghi"));
    }
}
