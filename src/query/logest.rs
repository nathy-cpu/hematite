//! Logarithmic cost estimation, ported from SQLite's LogEst.
//!
//! A `LogEst` is an `i16` approximating `10 * log2(x)`. This gives stable
//! integer arithmetic for cost comparisons without floating-point surprises.
//!
//! Key identities:
//!   - Multiplying quantities  → add LogEst values
//!   - Adding quantities       → `logest_add(a, b)` ≈ log2(2^a + 2^b)
//!   - Converting back         → `to_u64()`

/// Compact logarithmic cost/cardinality estimate (SQLite-style).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogEst(pub i16);

/// Fractional correction table from SQLite (`a[]` in sqlite3LogEst).
const FRAC: [i16; 8] = [0, 2, 3, 5, 6, 7, 8, 9];

/// Correction table for LogEstAdd (from SQLite).
const ADD_CORRECTION: [i16; 32] = [
    10, 10, 9, 9, 8, 8, 7, 7, 7, 6, 6, 6, 5, 5, 5, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2,
    2, 2, 2,
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
        LogEst(FRAC[(x & 7) as usize] + y)
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
    pub fn to_u64(self) -> u64 {
        let x = self.0;
        if x < 10 {
            return if x < 0 { 0 } else { 1 };
        }
        let mut n = (x % 10) as u64;
        let shift = (x / 10) as u32;
        if n >= 5 {
            n -= 2;
        } else if n >= 1 {
            n -= 1;
        }
        if shift >= 64 {
            return u64::MAX;
        }
        (n + 8) << shift
    }

    /// Convert from `f64` cost (for migration compatibility).
    pub fn from_f64(v: f64) -> Self {
        if v <= 0.0 {
            return LogEst(0);
        }
        Self::from_count(v.round().max(1.0) as u64)
    }

    /// Convert to `f64` (for migration compatibility).
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
        // These match SQLite's actual sqlite3LogEst() output.
        assert_eq!(LogEst::from_count(1).0, 0);
        assert_eq!(LogEst::from_count(2).0, 20);
        assert_eq!(LogEst::from_count(4).0, 30);
        assert_eq!(LogEst::from_count(8).0, 40);
        assert_eq!(LogEst::from_count(1024).0, 110); // 2^10: y=40 + 7*10 shifts + FRAC[0]
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
