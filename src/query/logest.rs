//! Logarithmic cost estimation, ported from SQLite's LogEst.
//!
//! A `LogEst` is an `i16` approximating `10 * log2(x)`. This gives stable
//! integer arithmetic for cost comparisons without floating-point surprises.
//!
//! Key identities:
//!   - Multiplying quantities  → add LogEst values
//!   - Adding quantities       → `a.add(b)` (`LogEst::add`) ≈ log2(2^a + 2^b);
//!     the `+` operator multiplies
//!   - Converting back         → `to_u64()`

/// Compact logarithmic cost/cardinality estimate (SQLite-style).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogEst(pub i16);

/// Fractional correction table from SQLite (`a[]` in sqlite3LogEst).
const FRAC: [i16; 8] = [0, 2, 3, 5, 6, 7, 8, 9];

/// Correction table for LogEstAdd (from SQLite).
const ADD_CORRECTION: [i16; 32] = [
    10, 10, 9, 9, 8, 8, 7, 7, 7, 6, 6, 6, 5, 5, 5, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2,
    2,
];

impl LogEst {
    pub const ZERO: LogEst = LogEst(0);

    /// Convert an integer count into a LogEst.
    ///
    /// Faithful port of `sqlite3LogEst(u64)`.
    pub fn from_count(mut x: u64) -> Self {
        if x < 2 {
            return LogEst(0);
        }
        let mut y: i16 = 40;
        if x < 8 {
            while x < 8 {
                y -= 10;
                x <<= 1;
            }
        } else {
            // Use leading zeros for fast shift count.
            let bits = 63u32.saturating_sub(x.leading_zeros());
            let shift = bits.saturating_sub(3);
            y += shift as i16 * 10;
            x >>= shift;
        }
        LogEst(FRAC[(x & 7) as usize] + y - 10)
    }

    /// Approximate `log2(2^self + 2^other)`.
    ///
    /// Faithful port of `sqlite3LogEstAdd(a, b)`.
    pub fn add(self, other: LogEst) -> LogEst {
        let (a, b) = if self.0 >= other.0 {
            (self.0, other.0)
        } else {
            (other.0, self.0)
        };
        if a > b + 49 {
            return LogEst(a);
        }
        if a > b + 31 {
            return LogEst(a + 1);
        }
        LogEst(a + ADD_CORRECTION[(a - b) as usize])
    }

    /// Convert back to an approximate integer.
    ///
    /// Faithful port of `sqlite3LogEstToInt(x)`.
    /// A negative `LogEst` is a fraction of one, which rounds down to zero.
    pub fn to_u64(self) -> u64 {
        let x = self.0;
        if x < 0 {
            return 0;
        }
        // `x = 10 * e + r`, and `(r' + 8) / 8` approximates `2^(r / 10)`, so the value
        // is `(r' + 8) * 2^(e - 3)`.
        let mut n = (x % 10) as u64;
        let e = (x / 10) as u32;
        if n >= 5 {
            n -= 2;
        } else if n >= 1 {
            n -= 1;
        }
        if e > 60 {
            return i64::MAX as u64;
        }
        if e >= 3 {
            (n + 8) << (e - 3)
        } else {
            (n + 8) >> (3 - e)
        }
    }

    /// Convert from an `f64` cost, rounded to a count; zero or less is `LogEst(0)`.
    pub fn from_f64(v: f64) -> Self {
        if v <= 0.0 {
            return LogEst(0);
        }
        Self::from_count(v.round().max(1.0) as u64)
    }

    /// Convert to `f64`, by way of the approximate integer from `to_u64`.
    pub fn to_f64(self) -> f64 {
        self.to_u64() as f64
    }
}

impl std::fmt::Display for LogEst {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogEst({}≈{})", self.0, self.to_u64())
    }
}

impl std::ops::Add for LogEst {
    type Output = LogEst;
    /// Multiply quantities (add log values).
    fn add(self, rhs: LogEst) -> LogEst {
        LogEst(self.0.saturating_add(rhs.0))
    }
}

impl std::ops::Sub for LogEst {
    type Output = LogEst;
    /// Divide quantities (subtract log values).
    fn sub(self, rhs: LogEst) -> LogEst {
        LogEst(self.0.saturating_sub(rhs.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_conversions() {
        // SQLite's sqlite3LogEst() output: 10 * log2(x), rounded down.
        assert_eq!(LogEst::from_count(1).0, 0);
        assert_eq!(LogEst::from_count(2).0, 10);
        assert_eq!(LogEst::from_count(4).0, 20);
        assert_eq!(LogEst::from_count(8).0, 30);
        assert_eq!(LogEst::from_count(10).0, 33);
        assert_eq!(LogEst::from_count(100).0, 66);
        assert_eq!(LogEst::from_count(1000).0, 99);
        assert_eq!(LogEst::from_count(1024).0, 100);
        assert_eq!(LogEst::from_count(1_000_000).0, 199);
    }

    #[test]
    fn test_to_u64_inverts_from_count() {
        // SQLite's sqlite3LogEstToInt() output.
        assert_eq!(LogEst(0).to_u64(), 1);
        assert_eq!(LogEst(10).to_u64(), 2);
        assert_eq!(LogEst(30).to_u64(), 8);
        assert_eq!(LogEst(33).to_u64(), 10);
        assert_eq!(LogEst(66).to_u64(), 96);
        assert_eq!(LogEst(100).to_u64(), 1024);
        assert_eq!(LogEst(-10).to_u64(), 0);

        // Exact on powers of two, and within the table's rounding otherwise.
        for power in 0..40u32 {
            let count = 1u64 << power;
            assert_eq!(LogEst::from_count(count).to_u64(), count);
        }
        for count in [3u64, 10, 100, 1000, 12_345, 1_000_000] {
            let back = LogEst::from_count(count).to_u64() as f64;
            let ratio = back / count as f64;
            assert!((0.8..=1.1).contains(&ratio), "{count} -> {back}");
        }
    }

    #[test]
    fn test_ordering_preserved() {
        // The critical property: larger inputs produce larger LogEst.
        let values = [1u64, 2, 5, 10, 50, 100, 1000, 10000, 1_000_000];
        for window in values.windows(2) {
            let a = LogEst::from_count(window[0]);
            let b = LogEst::from_count(window[1]);
            assert!(
                a < b,
                "{} → {:?} should be < {} → {:?}",
                window[0],
                a,
                window[1],
                b
            );
        }
    }

    #[test]
    fn test_add_same() {
        // log2(2^a + 2^a) = a + 1, so add(x, x) ≈ x + 10
        let a = LogEst::from_count(1024);
        let sum = a.add(a);
        assert_eq!(sum.0, a.0 + ADD_CORRECTION[0]); // correction[0] = 10
    }

    #[test]
    fn test_add_far_apart() {
        let big = LogEst(200);
        let small = LogEst(10);
        let sum = big.add(small);
        // When far apart, sum ≈ big + 1 (or just big)
        assert!(sum.0 >= 200 && sum.0 <= 202, "sum={}", sum.0);
    }

    #[test]
    fn test_cost_comparison_pk_vs_scan() {
        // PK lookup should always be cheaper than a full table scan.
        let pk_cost = LogEst(10); // ~2 (locator cost for PK)
        let scan_cost = LogEst::from_count(1000); // ~1000 rows
        assert!(pk_cost < scan_cost);
    }
}
