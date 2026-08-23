//! ID types for database objects

/// Unique identifier for database tables
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableId(u32);

impl TableId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

/// Unique identifier for table columns
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnId(u32);

impl ColumnId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

/// A hasher for the `u32`-backed ids in this module.
///
/// `get_table_by_name` runs per row on the insert path and hashes twice — once for
/// the name, once for the resulting `TableId` — and under `RandomState` the second
/// of those measured 5.3 % of a batched insert: SipHash over four bytes.
///
/// The mix is a multiply by the 64-bit golden-ratio constant followed by taking the
/// high bits, which is what `HashMap` consumes. Ids here are dense small integers, so
/// the top bits of the product are what separate them; `RandomState`'s DoS resistance
/// buys nothing against keys the database assigns itself.
///
/// This duplicates the shape of `storage::page_hash::PageIdHasher` rather than
/// sharing it, because `catalog` may not import `storage` — see the layer matrix in
/// `src/tests.rs`. The id types live here, so their hashing does too.
#[derive(Default)]
pub struct IdHasher(u64);

impl std::hash::Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write_u32(&mut self, value: u32) {
        self.0 = (value as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("IdHasher is only for the u32-backed ids in this module");
    }
}

/// A `HashMap` keyed by one of this module's ids.
pub(crate) type IdMap<K, V> =
    std::collections::HashMap<K, V, std::hash::BuildHasherDefault<IdHasher>>;
