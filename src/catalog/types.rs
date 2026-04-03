//! Data types and runtime values for the relational layer.

use std::cmp::Ordering;
use std::fmt;

use crate::error::{HematiteError, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    Int8,
    Int16,
    Int,
    Int64,
    Int128,
    UInt8,
    UInt16,
    UInt,
    UInt64,
    UInt128,
    Text,
    Char(u32),
    VarChar(u32),
    Binary(u32),
    VarBinary(u32),
    Enum(Vec<String>),
    Boolean,
    Float32,
    Float,
    Float128,
    Decimal {
        precision: Option<u32>,
        scale: Option<u32>,
    },
    Numeric {
        precision: Option<u32>,
        scale: Option<u32>,
    },
    Blob,
    Date,
    Time,
    DateTime,
    Timestamp,
    TimeWithTimeZone,
}

impl DataType {
    pub fn size(&self) -> usize {
        match self {
            DataType::Int8 => 1,
            DataType::Int16 => 2,
            DataType::Int => 4,
            DataType::Int64 => 8,
            DataType::Int128 => 16,
            DataType::UInt8 => 1,
            DataType::UInt16 => 2,
            DataType::UInt => 4,
            DataType::UInt64 => 8,
            DataType::UInt128 => 16,
            DataType::Text => 255,
            DataType::Char(length) | DataType::VarChar(length) => *length as usize,
            DataType::Binary(length) | DataType::VarBinary(length) => *length as usize,
            DataType::Enum(values) => values.iter().map(|value| value.len()).max().unwrap_or(0),
            DataType::Boolean => 1,
            DataType::Float32 => 4,
            DataType::Float => 8,
            DataType::Float128 => 16,
            DataType::Decimal { precision, .. } | DataType::Numeric { precision, .. } => {
                precision.unwrap_or(32) as usize
            }
            DataType::Blob => 255,
            DataType::Date => 4,
            DataType::Time => 4,
            DataType::DateTime => 8,
            DataType::Timestamp => 8,
            DataType::TimeWithTimeZone => 6,
        }
    }

    pub fn name(&self) -> String {
        match self {
            DataType::Int8 => "INT8".to_string(),
            DataType::Int16 => "INT16".to_string(),
            DataType::Int => "INT".to_string(),
            DataType::Int64 => "INT64".to_string(),
            DataType::Int128 => "INT128".to_string(),
            DataType::UInt8 => "UINT8".to_string(),
            DataType::UInt16 => "UINT16".to_string(),
            DataType::UInt => "UINT".to_string(),
            DataType::UInt64 => "UINT64".to_string(),
            DataType::UInt128 => "UINT128".to_string(),
            DataType::Text => "TEXT".to_string(),
            DataType::Char(length) => format!("CHAR({length})"),
            DataType::VarChar(length) => format!("VARCHAR({length})"),
            DataType::Binary(length) => format!("BINARY({length})"),
            DataType::VarBinary(length) => format!("VARBINARY({length})"),
            DataType::Enum(values) => format!(
                "ENUM({})",
                values
                    .iter()
                    .map(|value| format!("'{}'", value.replace('\'', "''")))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Float32 => "FLOAT32".to_string(),
            DataType::Float => "FLOAT".to_string(),
            DataType::Float128 => "FLOAT128".to_string(),
            DataType::Decimal { precision, scale } => {
                format_numeric_type("DECIMAL", *precision, *scale)
            }
            DataType::Numeric { precision, scale } => {
                format_numeric_type("NUMERIC", *precision, *scale)
            }
            DataType::Blob => "BLOB".to_string(),
            DataType::Date => "DATE".to_string(),
            DataType::Time => "TIME".to_string(),
            DataType::DateTime => "DATETIME".to_string(),
            DataType::Timestamp => "TIMESTAMP".to_string(),
            DataType::TimeWithTimeZone => "TIME WITH TIME ZONE".to_string(),
        }
    }

    pub fn base_name(&self) -> &'static str {
        match self {
            DataType::Int8 => "INT8",
            DataType::Int16 => "INT16",
            DataType::Int => "INT",
            DataType::Int64 => "INT64",
            DataType::Int128 => "INT128",
            DataType::UInt8 => "UINT8",
            DataType::UInt16 => "UINT16",
            DataType::UInt => "UINT",
            DataType::UInt64 => "UINT64",
            DataType::UInt128 => "UINT128",
            DataType::Text => "TEXT",
            DataType::Char(_) => "CHAR",
            DataType::VarChar(_) => "VARCHAR",
            DataType::Binary(_) => "BINARY",
            DataType::VarBinary(_) => "VARBINARY",
            DataType::Enum(_) => "ENUM",
            DataType::Boolean => "BOOLEAN",
            DataType::Float32 => "FLOAT32",
            DataType::Float => "FLOAT",
            DataType::Float128 => "FLOAT128",
            DataType::Decimal { .. } => "DECIMAL",
            DataType::Numeric { .. } => "NUMERIC",
            DataType::Blob => "BLOB",
            DataType::Date => "DATE",
            DataType::Time => "TIME",
            DataType::DateTime => "DATETIME",
            DataType::Timestamp => "TIMESTAMP",
            DataType::TimeWithTimeZone => "TIME WITH TIME ZONE",
        }
    }

    pub fn decimal_constraints(&self) -> Option<(Option<u32>, Option<u32>)> {
        match self {
            DataType::Decimal { precision, scale } | DataType::Numeric { precision, scale } => {
                Some((*precision, *scale))
            }
            _ => None,
        }
    }
}

fn format_numeric_type(name: &str, precision: Option<u32>, scale: Option<u32>) -> String {
    match (precision, scale) {
        (Some(precision), Some(scale)) => format!("{name}({precision}, {scale})"),
        (Some(precision), None) => format!("{name}({precision})"),
        (None, _) => name.to_string(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalMode {
    Rollback,
    Wal,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DecimalValue {
    negative: bool,
    digits: Vec<u8>,
    scale: u32,
}

impl DecimalValue {
    pub fn zero() -> Self {
        Self {
            negative: false,
            digits: vec![0],
            scale: 0,
        }
    }

    pub fn parse(input: &str) -> Result<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(HematiteError::ParseError(
                "Decimal value cannot be empty".to_string(),
            ));
        }

        let (negative, digits) = match trimmed.as_bytes()[0] {
            b'+' => (false, &trimmed[1..]),
            b'-' => (true, &trimmed[1..]),
            _ => (false, trimmed),
        };

        if digits.is_empty() {
            return Err(HematiteError::ParseError(format!(
                "Invalid decimal value '{}'",
                input
            )));
        }

        let mut parts = digits.split('.');
        let integer = parts.next().unwrap_or_default();
        let fraction = parts.next();
        if parts.next().is_some()
            || !integer.chars().all(|ch| ch.is_ascii_digit())
            || fraction.is_some_and(|part| !part.chars().all(|ch| ch.is_ascii_digit()))
        {
            return Err(HematiteError::ParseError(format!(
                "Invalid decimal value '{}'",
                input
            )));
        }

        let integer = integer.trim_start_matches('0');
        let integer = if integer.is_empty() { "0" } else { integer };
        let mut fraction = fraction.unwrap_or_default().to_string();
        while fraction.ends_with('0') {
            fraction.pop();
        }

        let mut combined = String::with_capacity(integer.len() + fraction.len());
        combined.push_str(integer);
        combined.push_str(&fraction);
        let combined = combined.trim_start_matches('0');
        let digits = if combined.is_empty() {
            vec![0]
        } else {
            combined.bytes().map(|byte| byte - b'0').collect()
        };

        let negative = negative && !(digits.len() == 1 && digits[0] == 0);

        Ok(Self {
            negative,
            digits,
            scale: fraction.len() as u32,
        })
    }

    pub fn from_i32(value: i32) -> Self {
        Self::parse(&value.to_string()).expect("i32 string is always a valid decimal")
    }

    pub fn from_i64(value: i64) -> Self {
        Self::parse(&value.to_string()).expect("i64 string is always a valid decimal")
    }

    pub fn from_i128(value: i128) -> Self {
        Self::parse(&value.to_string()).expect("i128 string is always a valid decimal")
    }

    pub fn from_u32(value: u32) -> Self {
        Self::parse(&value.to_string()).expect("u32 string is always a valid decimal")
    }

    pub fn from_u64(value: u64) -> Self {
        Self::parse(&value.to_string()).expect("u64 string is always a valid decimal")
    }

    pub fn from_u128(value: u128) -> Self {
        Self::parse(&value.to_string()).expect("u128 string is always a valid decimal")
    }

    pub fn from_f64(value: f64) -> Result<Self> {
        if !value.is_finite() {
            return Err(HematiteError::ParseError(
                "Decimal value must be finite".to_string(),
            ));
        }
        Self::parse(&value.to_string())
    }

    pub fn precision(&self) -> u32 {
        self.digits.len() as u32
    }

    pub fn scale(&self) -> u32 {
        self.scale
    }

    pub fn is_zero(&self) -> bool {
        self.digits.len() == 1 && self.digits[0] == 0
    }

    pub fn fits_precision_scale(&self, precision: Option<u32>, scale: Option<u32>) -> bool {
        if let Some(scale) = scale {
            if self.scale > scale {
                return false;
            }
        }

        if let Some(precision) = precision {
            let max_digits = precision;
            let digits = self.precision();
            if digits > max_digits {
                return false;
            }
            if let Some(scale) = scale {
                let integer_digits = digits.saturating_sub(self.scale).max(1);
                let max_integer_digits = precision.saturating_sub(scale).max(1);
                if integer_digits > max_integer_digits {
                    return false;
                }
            }
        }

        true
    }

    pub fn digit_bytes(&self) -> &[u8] {
        &self.digits
    }

    pub fn negative(&self) -> bool {
        self.negative
    }
}

impl fmt::Display for DecimalValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.negative && !self.is_zero() {
            write!(f, "-")?;
        }

        let digits = self
            .digits
            .iter()
            .map(|digit| char::from(b'0' + *digit))
            .collect::<String>();

        if self.scale == 0 {
            return write!(f, "{digits}");
        }

        let split = digits.len().saturating_sub(self.scale as usize);
        if split == 0 {
            write!(f, "0.")?;
            for _ in 0..self.scale as usize - digits.len() {
                write!(f, "0")?;
            }
            write!(f, "{digits}")
        } else {
            write!(f, "{}.{}", &digits[..split], &digits[split..])
        }
    }
}

impl PartialOrd for DecimalValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DecimalValue {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.negative != other.negative {
            return if self.negative {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }

        let left_integer_digits = self.digits.len().saturating_sub(self.scale as usize).max(1);
        let right_integer_digits = other
            .digits
            .len()
            .saturating_sub(other.scale as usize)
            .max(1);

        let ordering = left_integer_digits
            .cmp(&right_integer_digits)
            .then_with(|| self.digits.cmp(&other.digits))
            .then_with(|| self.scale.cmp(&other.scale).reverse());

        if self.negative {
            ordering.reverse()
        } else {
            ordering
        }
    }
}

const FLOAT128_MAX_PRECISION: usize = 34;
const FLOAT128_EXPONENT_BIAS: i16 = 8192;
const FLOAT128_MIN_EXPONENT: i16 = -8192;
const FLOAT128_MAX_EXPONENT: i16 = 8191;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Float128Value {
    negative: bool,
    coefficient: u128,
    exponent: i16,
}

impl Float128Value {
    pub fn zero() -> Self {
        Self {
            negative: false,
            coefficient: 0,
            exponent: 0,
        }
    }

    pub fn parse(input: &str) -> Result<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(HematiteError::ParseError(
                "FLOAT128 value cannot be empty".to_string(),
            ));
        }

        let (negative, digits) = match trimmed.as_bytes()[0] {
            b'+' => (false, &trimmed[1..]),
            b'-' => (true, &trimmed[1..]),
            _ => (false, trimmed),
        };

        if digits.is_empty() {
            return Err(HematiteError::ParseError(format!(
                "Invalid FLOAT128 value '{}'",
                input
            )));
        }

        let mut parts = digits.split('.');
        let integer = parts.next().unwrap_or_default();
        let fraction = parts.next();
        if parts.next().is_some()
            || !integer.chars().all(|ch| ch.is_ascii_digit())
            || fraction.is_some_and(|part| !part.chars().all(|ch| ch.is_ascii_digit()))
        {
            return Err(HematiteError::ParseError(format!(
                "Invalid FLOAT128 value '{}'",
                input
            )));
        }

        let integer = integer.trim_start_matches('0');
        let integer = if integer.is_empty() { "0" } else { integer };
        let fraction = fraction.unwrap_or_default();
        let mut combined = String::with_capacity(integer.len() + fraction.len());
        combined.push_str(integer);
        combined.push_str(fraction);
        let combined = combined.trim_start_matches('0');

        if combined.is_empty() {
            return Ok(Self::zero());
        }

        let digits = combined.bytes().map(|byte| byte - b'0').collect::<Vec<_>>();
        let exponent = -(fraction.len() as i16);
        Self::from_parts(negative, digits, exponent)
    }

    pub fn from_integer(value: i128) -> Self {
        if value == 0 {
            return Self::zero();
        }
        let negative = value.is_negative();
        let coefficient = value.unsigned_abs();
        Self {
            negative,
            coefficient,
            exponent: 0,
        }
    }

    pub fn from_unsigned(value: u128) -> Self {
        if value == 0 {
            return Self::zero();
        }
        Self {
            negative: false,
            coefficient: value,
            exponent: 0,
        }
    }

    pub fn from_f64(value: f64) -> Result<Self> {
        if !value.is_finite() {
            return Err(HematiteError::ParseError(
                "FLOAT128 value must be finite".to_string(),
            ));
        }
        Self::parse(&value.to_string())
    }

    pub fn from_storage_bits(bits: u128) -> Result<Self> {
        let negative = (bits >> 127) != 0;
        let exponent_bits = ((bits >> 113) & 0x3fff) as u16;
        let coefficient = bits & ((1u128 << 113) - 1);
        if coefficient == 0 {
            return Ok(Self::zero());
        }
        let exponent = exponent_bits as i16 - FLOAT128_EXPONENT_BIAS;
        if !(FLOAT128_MIN_EXPONENT..=FLOAT128_MAX_EXPONENT).contains(&exponent) {
            return Err(HematiteError::CorruptedData(
                "FLOAT128 exponent is out of range".to_string(),
            ));
        }
        let digits = u128_to_digits(coefficient);
        Self::from_parts(negative, digits, exponent)
    }

    pub fn storage_bits(&self) -> u128 {
        if self.coefficient == 0 {
            return 0;
        }
        let sign = (self.negative as u128) << 127;
        let exponent = ((self.exponent + FLOAT128_EXPONENT_BIAS) as u16 as u128) << 113;
        sign | exponent | self.coefficient
    }

    pub fn negative(&self) -> bool {
        self.negative
    }

    pub fn is_zero(&self) -> bool {
        self.coefficient == 0
    }

    pub fn exponent(&self) -> i16 {
        self.exponent
    }

    pub fn coefficient(&self) -> u128 {
        self.coefficient
    }

    pub fn abs(&self) -> Self {
        let mut value = self.clone();
        value.negative = false;
        value
    }

    pub fn negated(&self) -> Self {
        if self.is_zero() {
            return Self::zero();
        }
        let mut value = self.clone();
        value.negative = !value.negative;
        value
    }

    pub fn to_f64(&self) -> Result<f64> {
        self.to_string().parse::<f64>().map_err(|_| {
            HematiteError::ParseError(format!(
                "FLOAT128 value '{}' cannot be represented as FLOAT",
                self
            ))
        })
    }

    pub fn to_decimal(&self) -> Result<DecimalValue> {
        DecimalValue::parse(&self.to_string())
    }

    pub fn to_i32(&self) -> Result<i32> {
        self.floor()?.to_string().parse::<i32>().map_err(|_| {
            HematiteError::ParseError(format!(
                "FLOAT128 value '{}' cannot be represented as INT",
                self
            ))
        })
    }

    pub fn to_i64(&self) -> Result<i64> {
        self.floor()?.to_string().parse::<i64>().map_err(|_| {
            HematiteError::ParseError(format!(
                "FLOAT128 value '{}' cannot be represented as INT64",
                self
            ))
        })
    }

    pub fn to_i128(&self) -> Result<i128> {
        self.floor()?.to_string().parse::<i128>().map_err(|_| {
            HematiteError::ParseError(format!(
                "FLOAT128 value '{}' cannot be represented as INT128",
                self
            ))
        })
    }

    pub fn to_u32(&self) -> Result<u32> {
        if self.negative {
            return Err(HematiteError::ParseError(format!(
                "FLOAT128 value '{}' cannot be represented as UINT",
                self
            )));
        }
        self.floor()?.to_string().parse::<u32>().map_err(|_| {
            HematiteError::ParseError(format!(
                "FLOAT128 value '{}' cannot be represented as UINT",
                self
            ))
        })
    }

    pub fn to_u64(&self) -> Result<u64> {
        if self.negative {
            return Err(HematiteError::ParseError(format!(
                "FLOAT128 value '{}' cannot be represented as UINT64",
                self
            )));
        }
        self.floor()?.to_string().parse::<u64>().map_err(|_| {
            HematiteError::ParseError(format!(
                "FLOAT128 value '{}' cannot be represented as UINT64",
                self
            ))
        })
    }

    pub fn to_u128(&self) -> Result<u128> {
        if self.negative {
            return Err(HematiteError::ParseError(format!(
                "FLOAT128 value '{}' cannot be represented as UINT128",
                self
            )));
        }
        self.floor()?.to_string().parse::<u128>().map_err(|_| {
            HematiteError::ParseError(format!(
                "FLOAT128 value '{}' cannot be represented as UINT128",
                self
            ))
        })
    }

    pub fn ceil(&self) -> Result<Self> {
        if self.is_zero() || self.exponent >= 0 {
            return Ok(self.clone());
        }

        let digits = self.coefficient_digits();
        let integer_len = digits.len() as i32 + self.exponent as i32;
        if integer_len <= 0 {
            return if self.negative {
                Ok(Self::zero())
            } else {
                Ok(Self::from_integer(1))
            };
        }

        let keep = integer_len as usize;
        let has_fraction = digits[keep..].iter().any(|digit| *digit != 0);
        let mut int_digits = digits[..keep].to_vec();
        if !self.negative && has_fraction {
            int_digits = add_digit_slices(&int_digits, &[1]);
        }
        Self::from_parts(self.negative, int_digits, 0)
    }

    pub fn floor(&self) -> Result<Self> {
        if self.is_zero() || self.exponent >= 0 {
            return Ok(self.clone());
        }

        let digits = self.coefficient_digits();
        let integer_len = digits.len() as i32 + self.exponent as i32;
        if integer_len <= 0 {
            return if self.negative {
                Ok(Self::from_integer(-1))
            } else {
                Ok(Self::zero())
            };
        }

        let keep = integer_len as usize;
        let has_fraction = digits[keep..].iter().any(|digit| *digit != 0);
        let mut int_digits = digits[..keep].to_vec();
        if self.negative && has_fraction {
            int_digits = add_digit_slices(&int_digits, &[1]);
        }
        Self::from_parts(self.negative, int_digits, 0)
    }

    pub fn round(&self, precision: i32) -> Result<Self> {
        if self.is_zero() {
            return Ok(Self::zero());
        }

        let target_exponent = -(precision as i16);
        if self.exponent >= target_exponent {
            return Ok(self.clone());
        }

        let drop = (target_exponent - self.exponent) as usize;
        let mut digits = self.coefficient_digits();
        if drop >= digits.len() {
            let should_round_up = digits.first().is_some_and(|digit| *digit >= 5);
            return if should_round_up {
                Self::from_parts(self.negative, vec![1], target_exponent)
            } else {
                Ok(Self::zero())
            };
        }

        let keep = digits.len() - drop;
        let round_digit = digits[keep];
        digits.truncate(keep);
        if round_digit >= 5 {
            digits = add_digit_slices(&digits, &[1]);
        }
        Self::from_parts(self.negative, digits, target_exponent)
    }

    pub fn add(&self, other: &Self) -> Result<Self> {
        if self.is_zero() {
            return Ok(other.clone());
        }
        if other.is_zero() {
            return Ok(self.clone());
        }

        if magnitude_digit_gap(self, other) > FLOAT128_MAX_PRECISION + 1 {
            return if self.abs_cmp(other).is_ge() {
                Ok(self.clone())
            } else {
                Ok(other.clone())
            };
        }

        let common_exponent = self.exponent.min(other.exponent);
        let left_digits = append_zeros(
            &self.coefficient_digits(),
            (self.exponent - common_exponent) as usize,
        );
        let right_digits = append_zeros(
            &other.coefficient_digits(),
            (other.exponent - common_exponent) as usize,
        );

        if self.negative == other.negative {
            return Self::from_parts(
                self.negative,
                add_digit_slices(&left_digits, &right_digits),
                common_exponent,
            );
        }

        match compare_digit_slices(&left_digits, &right_digits) {
            Ordering::Greater => Self::from_parts(
                self.negative,
                subtract_digit_slices(&left_digits, &right_digits),
                common_exponent,
            ),
            Ordering::Less => Self::from_parts(
                other.negative,
                subtract_digit_slices(&right_digits, &left_digits),
                common_exponent,
            ),
            Ordering::Equal => Ok(Self::zero()),
        }
    }

    pub fn subtract(&self, other: &Self) -> Result<Self> {
        self.add(&other.negated())
    }

    pub fn multiply(&self, other: &Self) -> Result<Self> {
        if self.is_zero() || other.is_zero() {
            return Ok(Self::zero());
        }
        let digits = multiply_digit_slices(&self.coefficient_digits(), &other.coefficient_digits());
        Self::from_parts(
            self.negative ^ other.negative,
            digits,
            self.exponent.checked_add(other.exponent).ok_or_else(|| {
                HematiteError::ParseError(
                    "FLOAT128 multiplication overflowed exponent range".to_string(),
                )
            })?,
        )
    }

    pub fn divide(&self, other: &Self) -> Result<Self> {
        if other.is_zero() {
            return Err(HematiteError::ParseError("Division by zero".to_string()));
        }
        if self.is_zero() {
            return Ok(Self::zero());
        }

        let extra_digits = FLOAT128_MAX_PRECISION + 1;
        let numerator = append_zeros(&self.coefficient_digits(), extra_digits);
        let denominator = other.coefficient_digits();
        let quotient = divide_digit_slices(&numerator, &denominator)?;
        let exponent = self
            .exponent
            .checked_sub(other.exponent)
            .and_then(|value| value.checked_sub(extra_digits as i16))
            .ok_or_else(|| {
                HematiteError::ParseError("FLOAT128 division overflowed exponent range".to_string())
            })?;
        Self::from_parts(self.negative ^ other.negative, quotient, exponent)
    }

    pub fn powi(&self, exponent: i32) -> Result<Self> {
        if exponent == 0 {
            return Ok(Self::from_integer(1));
        }
        if self.is_zero() {
            return if exponent < 0 {
                Err(HematiteError::ParseError("Division by zero".to_string()))
            } else {
                Ok(Self::zero())
            };
        }

        let mut base = self.clone();
        let mut exp = exponent.unsigned_abs();
        let mut result = Self::from_integer(1);
        while exp > 0 {
            if exp & 1 == 1 {
                result = result.multiply(&base)?;
            }
            exp >>= 1;
            if exp > 0 {
                base = base.multiply(&base)?;
            }
        }

        if exponent < 0 {
            Self::from_integer(1).divide(&result)
        } else {
            Ok(result)
        }
    }

    fn abs_cmp(&self, other: &Self) -> Ordering {
        let left = self.coefficient_digits();
        let right = other.coefficient_digits();
        let left_magnitude = left.len() as i32 + self.exponent as i32;
        let right_magnitude = right.len() as i32 + other.exponent as i32;
        match left_magnitude.cmp(&right_magnitude) {
            Ordering::Equal => {
                let common_exponent = self.exponent.min(other.exponent);
                let left = append_zeros(&left, (self.exponent - common_exponent) as usize);
                let right = append_zeros(&right, (other.exponent - common_exponent) as usize);
                compare_digit_slices(&left, &right)
            }
            ordering => ordering,
        }
    }

    fn coefficient_digits(&self) -> Vec<u8> {
        u128_to_digits(self.coefficient)
    }

    fn from_parts(negative: bool, digits: Vec<u8>, exponent: i16) -> Result<Self> {
        let (digits, exponent) = normalize_float128_parts(digits, exponent)?;
        if digits.len() == 1 && digits[0] == 0 {
            return Ok(Self::zero());
        }

        let (digits, exponent) = round_float128_digits(digits, exponent)?;
        let coefficient = digits_to_u128(&digits)?;
        Ok(Self {
            negative,
            coefficient,
            exponent,
        })
    }
}

impl fmt::Display for Float128Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_zero() {
            return write!(f, "0");
        }
        if self.negative {
            write!(f, "-")?;
        }

        let digits = self.coefficient_digits();
        if self.exponent >= 0 {
            let digit_string = digits
                .iter()
                .map(|digit| char::from(b'0' + *digit))
                .collect::<String>();
            write!(f, "{digit_string}")?;
            for _ in 0..self.exponent {
                write!(f, "0")?;
            }
            return Ok(());
        }

        let split = digits.len() as i32 + self.exponent as i32;
        let digit_string = digits
            .iter()
            .map(|digit| char::from(b'0' + *digit))
            .collect::<String>();
        if split <= 0 {
            write!(f, "0.")?;
            for _ in 0..-split {
                write!(f, "0")?;
            }
            write!(f, "{digit_string}")
        } else {
            let split = split as usize;
            write!(f, "{}.{}", &digit_string[..split], &digit_string[split..])
        }
    }
}

impl PartialOrd for Float128Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Float128Value {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.negative != other.negative {
            return if self.negative {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }

        let ordering = self.abs_cmp(other);
        if self.negative {
            ordering.reverse()
        } else {
            ordering
        }
    }
}

fn normalize_float128_parts(mut digits: Vec<u8>, mut exponent: i16) -> Result<(Vec<u8>, i16)> {
    trim_leading_zero_digits(&mut digits);
    if digits.is_empty() {
        return Ok((vec![0], 0));
    }
    while digits.len() > 1 && digits.last().copied() == Some(0) {
        digits.pop();
        exponent = exponent.checked_add(1).ok_or_else(|| {
            HematiteError::ParseError("FLOAT128 exponent overflowed supported range".to_string())
        })?;
    }
    if !(FLOAT128_MIN_EXPONENT..=FLOAT128_MAX_EXPONENT).contains(&exponent) {
        return Err(HematiteError::ParseError(
            "FLOAT128 exponent overflowed supported range".to_string(),
        ));
    }
    Ok((digits, exponent))
}

fn round_float128_digits(mut digits: Vec<u8>, mut exponent: i16) -> Result<(Vec<u8>, i16)> {
    if digits.len() <= FLOAT128_MAX_PRECISION {
        return Ok((digits, exponent));
    }

    let drop = digits.len() - FLOAT128_MAX_PRECISION;
    let round_digit = digits[FLOAT128_MAX_PRECISION];
    digits.truncate(FLOAT128_MAX_PRECISION);
    if round_digit >= 5 {
        digits = add_digit_slices(&digits, &[1]);
    }
    exponent = exponent.checked_add(drop as i16).ok_or_else(|| {
        HematiteError::ParseError("FLOAT128 exponent overflowed supported range".to_string())
    })?;
    normalize_float128_parts(digits, exponent)
}

fn trim_leading_zero_digits(digits: &mut Vec<u8>) {
    if let Some(first_non_zero) = digits.iter().position(|digit| *digit != 0) {
        if first_non_zero > 0 {
            digits.drain(0..first_non_zero);
        }
    } else {
        digits.clear();
    }
}

fn magnitude_digit_gap(left: &Float128Value, right: &Float128Value) -> usize {
    let left_magnitude = left.coefficient_digits().len() as i32 + left.exponent as i32;
    let right_magnitude = right.coefficient_digits().len() as i32 + right.exponent as i32;
    left_magnitude.abs_diff(right_magnitude) as usize
}

fn u128_to_digits(mut value: u128) -> Vec<u8> {
    if value == 0 {
        return vec![0];
    }
    let mut digits = Vec::new();
    while value > 0 {
        digits.push((value % 10) as u8);
        value /= 10;
    }
    digits.reverse();
    digits
}

fn digits_to_u128(digits: &[u8]) -> Result<u128> {
    let mut value = 0u128;
    for digit in digits {
        value = value
            .checked_mul(10)
            .and_then(|value| value.checked_add(*digit as u128))
            .ok_or_else(|| {
                HematiteError::ParseError(
                    "FLOAT128 coefficient overflowed supported range".to_string(),
                )
            })?;
    }
    Ok(value)
}

fn append_zeros(digits: &[u8], count: usize) -> Vec<u8> {
    if digits.len() == 1 && digits[0] == 0 {
        return vec![0];
    }
    let mut out = digits.to_vec();
    out.extend(std::iter::repeat_n(0, count));
    out
}

fn compare_digit_slices(left: &[u8], right: &[u8]) -> Ordering {
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

fn add_digit_slices(left: &[u8], right: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(left.len().max(right.len()) + 1);
    let mut carry = 0u8;
    let mut left_index = left.len() as isize - 1;
    let mut right_index = right.len() as isize - 1;
    while left_index >= 0 || right_index >= 0 || carry > 0 {
        let left_digit = if left_index >= 0 {
            left[left_index as usize]
        } else {
            0
        };
        let right_digit = if right_index >= 0 {
            right[right_index as usize]
        } else {
            0
        };
        let total = left_digit + right_digit + carry;
        out.push(total % 10);
        carry = total / 10;
        left_index -= 1;
        right_index -= 1;
    }
    out.reverse();
    out
}

fn subtract_digit_slices(left: &[u8], right: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(left.len());
    let mut borrow = 0i8;
    let mut left_index = left.len() as isize - 1;
    let mut right_index = right.len() as isize - 1;
    while left_index >= 0 {
        let mut left_digit = left[left_index as usize] as i8 - borrow;
        let right_digit = if right_index >= 0 {
            right[right_index as usize] as i8
        } else {
            0
        };
        if left_digit < right_digit {
            left_digit += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        out.push((left_digit - right_digit) as u8);
        left_index -= 1;
        right_index -= 1;
    }
    out.reverse();
    trim_leading_zero_digits(&mut out);
    if out.is_empty() {
        vec![0]
    } else {
        out
    }
}

fn multiply_digit_slices(left: &[u8], right: &[u8]) -> Vec<u8> {
    if (left.len() == 1 && left[0] == 0) || (right.len() == 1 && right[0] == 0) {
        return vec![0];
    }
    let mut out = vec![0u16; left.len() + right.len()];
    for (left_offset, left_digit) in left.iter().rev().enumerate() {
        for (right_offset, right_digit) in right.iter().rev().enumerate() {
            let index = out.len() - 1 - (left_offset + right_offset);
            out[index] += (*left_digit as u16) * (*right_digit as u16);
        }
    }
    for index in (1..out.len()).rev() {
        let carry = out[index] / 10;
        out[index] %= 10;
        out[index - 1] += carry;
    }
    let mut digits = out.into_iter().map(|digit| digit as u8).collect::<Vec<_>>();
    trim_leading_zero_digits(&mut digits);
    if digits.is_empty() {
        vec![0]
    } else {
        digits
    }
}

fn divide_digit_slices(numerator: &[u8], denominator: &[u8]) -> Result<Vec<u8>> {
    if denominator.len() == 1 && denominator[0] == 0 {
        return Err(HematiteError::ParseError("Division by zero".to_string()));
    }

    let mut quotient = Vec::with_capacity(numerator.len());
    let mut remainder = Vec::new();
    for digit in numerator {
        remainder.push(*digit);
        trim_leading_zero_digits(&mut remainder);
        let mut q = 0u8;
        while !remainder.is_empty()
            && compare_digit_slices(&remainder, denominator) != Ordering::Less
        {
            remainder = subtract_digit_slices(&remainder, denominator);
            q += 1;
        }
        quotient.push(q);
    }
    trim_leading_zero_digits(&mut quotient);
    if quotient.is_empty() {
        Ok(vec![0])
    } else {
        Ok(quotient)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DateValue {
    days_since_epoch: i32,
}

impl DateValue {
    pub fn epoch() -> Self {
        Self {
            days_since_epoch: 0,
        }
    }

    pub fn parse(input: &str) -> Result<Self> {
        let value = input.trim();
        let parts = value.split('-').collect::<Vec<_>>();
        if parts.len() != 3
            || parts[0].len() != 4
            || parts[1].len() != 2
            || parts[2].len() != 2
            || !parts
                .iter()
                .all(|part| part.chars().all(|ch| ch.is_ascii_digit()))
        {
            return Err(HematiteError::ParseError(format!(
                "Invalid DATE value '{}'",
                input
            )));
        }

        let year = parts[0]
            .parse::<i32>()
            .map_err(|_| HematiteError::ParseError(format!("Invalid DATE value '{}'", input)))?;
        let month = parts[1]
            .parse::<u32>()
            .map_err(|_| HematiteError::ParseError(format!("Invalid DATE value '{}'", input)))?;
        let day = parts[2]
            .parse::<u32>()
            .map_err(|_| HematiteError::ParseError(format!("Invalid DATE value '{}'", input)))?;
        validate_date_components(year, month, day, input)?;
        Ok(Self {
            days_since_epoch: days_from_civil(year, month, day),
        })
    }

    pub fn from_days_since_epoch(days_since_epoch: i32) -> Self {
        Self { days_since_epoch }
    }

    pub fn days_since_epoch(self) -> i32 {
        self.days_since_epoch
    }

    pub fn components(self) -> (i32, u32, u32) {
        civil_from_days(self.days_since_epoch)
    }
}

impl fmt::Display for DateValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (year, month, day) = self.components();
        write!(f, "{year:04}-{month:02}-{day:02}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimeValue {
    seconds_since_midnight: u32,
}

impl TimeValue {
    pub fn midnight() -> Self {
        Self {
            seconds_since_midnight: 0,
        }
    }

    pub fn parse(input: &str) -> Result<Self> {
        let value = input.trim();
        let (hour, minute, second) = parse_time_components(value, "TIME")?;
        Ok(Self {
            seconds_since_midnight: hour * 3_600 + minute * 60 + second,
        })
    }

    pub fn from_seconds_since_midnight(seconds_since_midnight: u32) -> Self {
        Self {
            seconds_since_midnight: seconds_since_midnight % 86_400,
        }
    }

    pub fn seconds_since_midnight(self) -> u32 {
        self.seconds_since_midnight
    }

    pub fn components(self) -> (u32, u32, u32) {
        let hour = self.seconds_since_midnight / 3_600;
        let minute = (self.seconds_since_midnight % 3_600) / 60;
        let second = self.seconds_since_midnight % 60;
        (hour, minute, second)
    }
}

impl fmt::Display for TimeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (hour, minute, second) = self.components();
        write!(f, "{hour:02}:{minute:02}:{second:02}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DateTimeValue {
    seconds_since_epoch: i64,
}

impl DateTimeValue {
    pub fn epoch() -> Self {
        Self {
            seconds_since_epoch: 0,
        }
    }

    pub fn parse(input: &str) -> Result<Self> {
        let value = input.trim();
        let mut parts = value.split(' ');
        let date = parts.next().unwrap_or_default();
        let time = parts.next().unwrap_or_default();
        if parts.next().is_some() {
            return Err(HematiteError::ParseError(format!(
                "Invalid DATETIME value '{}'",
                input
            )));
        }
        let date = DateValue::parse(date)?;
        let (hour, minute, second) = parse_time_components(time, "DATETIME")?;

        Ok(Self {
            seconds_since_epoch: date.days_since_epoch as i64 * 86_400
                + hour as i64 * 3_600
                + minute as i64 * 60
                + second as i64,
        })
    }

    pub fn from_seconds_since_epoch(seconds_since_epoch: i64) -> Self {
        Self {
            seconds_since_epoch,
        }
    }

    pub fn seconds_since_epoch(self) -> i64 {
        self.seconds_since_epoch
    }

    pub fn components(self) -> (DateValue, TimeValue) {
        let days = self.seconds_since_epoch.div_euclid(86_400) as i32;
        let seconds = self.seconds_since_epoch.rem_euclid(86_400) as u32;
        (
            DateValue::from_days_since_epoch(days),
            TimeValue::from_seconds_since_midnight(seconds),
        )
    }
}

impl fmt::Display for DateTimeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (date, time) = self.components();
        let (year, month, day) = date.components();
        let (hour, minute, second) = time.components();
        write!(
            f,
            "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}"
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimestampValue {
    seconds_since_epoch: i64,
}

impl TimestampValue {
    pub fn epoch() -> Self {
        Self {
            seconds_since_epoch: 0,
        }
    }

    pub fn parse(input: &str) -> Result<Self> {
        Ok(Self {
            seconds_since_epoch: DateTimeValue::parse(input)?.seconds_since_epoch(),
        })
    }

    pub fn from_seconds_since_epoch(seconds_since_epoch: i64) -> Self {
        Self {
            seconds_since_epoch,
        }
    }

    pub fn seconds_since_epoch(self) -> i64 {
        self.seconds_since_epoch
    }

    pub fn components(self) -> (DateValue, TimeValue) {
        DateTimeValue::from_seconds_since_epoch(self.seconds_since_epoch).components()
    }
}

impl fmt::Display for TimestampValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        DateTimeValue::from_seconds_since_epoch(self.seconds_since_epoch).fmt(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimeWithTimeZoneValue {
    seconds_since_midnight: u32,
    offset_minutes: i16,
}

impl TimeWithTimeZoneValue {
    pub fn utc_midnight() -> Self {
        Self {
            seconds_since_midnight: 0,
            offset_minutes: 0,
        }
    }

    pub fn parse(input: &str) -> Result<Self> {
        let value = input.trim();
        let split = value
            .rfind(['+', '-'])
            .filter(|index| *index > 0)
            .ok_or_else(|| {
                HematiteError::ParseError(format!("Invalid TIME WITH TIME ZONE value '{}'", input))
            })?;
        let (time, offset) = value.split_at(split);
        let time = TimeValue::parse(time).map_err(|_| {
            HematiteError::ParseError(format!("Invalid TIME WITH TIME ZONE value '{}'", input))
        })?;
        let offset_minutes = parse_timezone_offset(offset, input)?;
        Ok(Self {
            seconds_since_midnight: time.seconds_since_midnight(),
            offset_minutes,
        })
    }

    pub fn from_parts(seconds_since_midnight: u32, offset_minutes: i16) -> Self {
        Self {
            seconds_since_midnight: seconds_since_midnight % 86_400,
            offset_minutes,
        }
    }

    pub fn seconds_since_midnight(self) -> u32 {
        self.seconds_since_midnight
    }

    pub fn offset_minutes(self) -> i16 {
        self.offset_minutes
    }

    pub fn time(self) -> TimeValue {
        TimeValue::from_seconds_since_midnight(self.seconds_since_midnight)
    }
}

impl fmt::Display for TimeWithTimeZoneValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sign = if self.offset_minutes < 0 { '-' } else { '+' };
        let offset = self.offset_minutes.unsigned_abs();
        let offset_hours = offset / 60;
        let offset_minutes = offset % 60;
        write!(
            f,
            "{}{}{:02}:{:02}",
            self.time(),
            sign,
            offset_hours,
            offset_minutes
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IntervalYearMonthValue {
    total_months: i32,
}

impl IntervalYearMonthValue {
    pub fn new(total_months: i32) -> Self {
        Self { total_months }
    }

    pub fn parse(input: &str) -> Result<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(HematiteError::ParseError(
                "Invalid INTERVAL YEAR TO MONTH value ''".to_string(),
            ));
        }

        let (negative, digits) = match trimmed.as_bytes()[0] {
            b'+' => (false, &trimmed[1..]),
            b'-' => (true, &trimmed[1..]),
            _ => (false, trimmed),
        };
        let (years, months) = digits.split_once('-').ok_or_else(|| {
            HematiteError::ParseError(format!("Invalid INTERVAL YEAR TO MONTH value '{}'", input))
        })?;
        if years.is_empty()
            || months.len() != 2
            || !years.chars().all(|ch| ch.is_ascii_digit())
            || !months.chars().all(|ch| ch.is_ascii_digit())
        {
            return Err(HematiteError::ParseError(format!(
                "Invalid INTERVAL YEAR TO MONTH value '{}'",
                input
            )));
        }

        let years = years.parse::<i32>().map_err(|_| {
            HematiteError::ParseError(format!("Invalid INTERVAL YEAR TO MONTH value '{}'", input))
        })?;
        let months = months.parse::<i32>().map_err(|_| {
            HematiteError::ParseError(format!("Invalid INTERVAL YEAR TO MONTH value '{}'", input))
        })?;
        if !(0..12).contains(&months) {
            return Err(HematiteError::ParseError(format!(
                "Invalid INTERVAL YEAR TO MONTH value '{}'",
                input
            )));
        }

        let total_months = years
            .checked_mul(12)
            .and_then(|total| total.checked_add(months))
            .ok_or_else(|| {
                HematiteError::ParseError(
                    "INTERVAL YEAR TO MONTH value overflowed supported range".to_string(),
                )
            })?;
        Ok(Self {
            total_months: if negative {
                -total_months
            } else {
                total_months
            },
        })
    }

    pub fn total_months(self) -> i32 {
        self.total_months
    }
}

impl fmt::Display for IntervalYearMonthValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sign = if self.total_months < 0 { "-" } else { "" };
        let total_months = self.total_months.unsigned_abs();
        let years = total_months / 12;
        let months = total_months % 12;
        write!(f, "{sign}{years}-{months:02}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IntervalDaySecondValue {
    total_seconds: i64,
}

impl IntervalDaySecondValue {
    pub fn new(total_seconds: i64) -> Self {
        Self { total_seconds }
    }

    pub fn parse(input: &str) -> Result<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(HematiteError::ParseError(
                "Invalid INTERVAL DAY TO SECOND value ''".to_string(),
            ));
        }

        let (negative, digits) = match trimmed.as_bytes()[0] {
            b'+' => (false, &trimmed[1..]),
            b'-' => (true, &trimmed[1..]),
            _ => (false, trimmed),
        };
        let (days, time) = digits.split_once(' ').ok_or_else(|| {
            HematiteError::ParseError(format!("Invalid INTERVAL DAY TO SECOND value '{}'", input))
        })?;
        if days.is_empty() || !days.chars().all(|ch| ch.is_ascii_digit()) {
            return Err(HematiteError::ParseError(format!(
                "Invalid INTERVAL DAY TO SECOND value '{}'",
                input
            )));
        }
        let days = days.parse::<i64>().map_err(|_| {
            HematiteError::ParseError(format!("Invalid INTERVAL DAY TO SECOND value '{}'", input))
        })?;
        let (hour, minute, second) = parse_time_components(time, "INTERVAL DAY TO SECOND")?;
        let total_seconds = days
            .checked_mul(86_400)
            .and_then(|total| total.checked_add(hour as i64 * 3_600))
            .and_then(|total| total.checked_add(minute as i64 * 60))
            .and_then(|total| total.checked_add(second as i64))
            .ok_or_else(|| {
                HematiteError::ParseError(
                    "INTERVAL DAY TO SECOND value overflowed supported range".to_string(),
                )
            })?;
        Ok(Self {
            total_seconds: if negative {
                -total_seconds
            } else {
                total_seconds
            },
        })
    }

    pub fn total_seconds(self) -> i64 {
        self.total_seconds
    }
}

impl fmt::Display for IntervalDaySecondValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sign = if self.total_seconds < 0 { "-" } else { "" };
        let total_seconds = self.total_seconds.unsigned_abs();
        let days = total_seconds / 86_400;
        let remainder = total_seconds % 86_400;
        let hours = remainder / 3_600;
        let minutes = (remainder % 3_600) / 60;
        let seconds = remainder % 60;
        write!(f, "{sign}{days} {hours:02}:{minutes:02}:{seconds:02}")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(i32),
    BigInt(i64),
    Int128(i128),
    UInteger(u32),
    UBigInt(u64),
    UInt128(u128),
    Text(String),
    Enum(String),
    Boolean(bool),
    Float32(f32),
    Float(f64),
    Float128(Float128Value),
    Decimal(DecimalValue),
    Blob(Vec<u8>),
    Date(DateValue),
    Time(TimeValue),
    DateTime(DateTimeValue),
    Timestamp(TimestampValue),
    TimeWithTimeZone(TimeWithTimeZoneValue),
    IntervalYearMonth(IntervalYearMonthValue),
    IntervalDaySecond(IntervalDaySecondValue),
    Null,
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Integer(_) => DataType::Int,
            Value::BigInt(_) => DataType::Int64,
            Value::Int128(_) => DataType::Int128,
            Value::UInteger(_) => DataType::UInt,
            Value::UBigInt(_) => DataType::UInt64,
            Value::UInt128(_) => DataType::UInt128,
            Value::Text(_) => DataType::Text,
            Value::Enum(_) => DataType::Enum(Vec::new()),
            Value::Boolean(_) => DataType::Boolean,
            Value::Float32(_) => DataType::Float32,
            Value::Float(_) => DataType::Float,
            Value::Float128(_) => DataType::Float128,
            Value::Decimal(_) => DataType::Decimal {
                precision: None,
                scale: None,
            },
            Value::Blob(_) => DataType::Blob,
            Value::Date(_) => DataType::Date,
            Value::Time(_) => DataType::Time,
            Value::DateTime(_) => DataType::DateTime,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::TimeWithTimeZone(_) => DataType::TimeWithTimeZone,
            Value::IntervalYearMonth(_) | Value::IntervalDaySecond(_) => DataType::Text,
            Value::Null => DataType::Text,
        }
    }

    pub fn is_compatible_with(&self, data_type: DataType) -> bool {
        match (self, data_type) {
            (Value::Integer(_), DataType::Int8)
            | (Value::Integer(_), DataType::Int16)
            | (Value::Integer(_), DataType::Int) => true,
            (Value::BigInt(_), DataType::Int64) => true,
            (Value::Int128(_), DataType::Int128) => true,
            (Value::UInteger(_), DataType::UInt8)
            | (Value::UInteger(_), DataType::UInt16)
            | (Value::UInteger(_), DataType::UInt) => true,
            (Value::UBigInt(_), DataType::UInt64) => true,
            (Value::UInt128(_), DataType::UInt128) => true,
            (Value::Text(_), DataType::Text)
            | (Value::Text(_), DataType::Char(_))
            | (Value::Text(_), DataType::VarChar(_)) => true,
            (Value::Blob(_), DataType::Binary(_)) | (Value::Blob(_), DataType::VarBinary(_)) => {
                true
            }
            (Value::Enum(value), DataType::Enum(values)) => values.contains(value),
            (Value::Boolean(_), DataType::Boolean) => true,
            (Value::Float32(_), DataType::Float32) => true,
            (Value::Float(_), DataType::Float) => true,
            (Value::Float128(_), DataType::Float128) => true,
            (Value::Decimal(value), DataType::Decimal { precision, scale })
            | (Value::Decimal(value), DataType::Numeric { precision, scale }) => {
                value.fits_precision_scale(precision, scale)
            }
            (Value::Blob(_), DataType::Blob) => true,
            (Value::Date(_), DataType::Date) => true,
            (Value::Time(_), DataType::Time) => true,
            (Value::DateTime(_), DataType::DateTime) => true,
            (Value::Timestamp(_), DataType::Timestamp) => true,
            (Value::TimeWithTimeZone(_), DataType::TimeWithTimeZone) => true,
            (Value::Null, _) => true,
            _ => false,
        }
    }

    pub fn as_integer(&self) -> Option<i32> {
        match self {
            Value::Integer(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<String> {
        match self {
            Value::Text(s) => Some(s.clone()),
            Value::Enum(s) => Some(s.clone()),
            Value::Decimal(s) => Some(s.to_string()),
            Value::Date(s) => Some(s.to_string()),
            Value::Time(s) => Some(s.to_string()),
            Value::DateTime(s) => Some(s.to_string()),
            Value::Timestamp(s) => Some(s.to_string()),
            Value::TimeWithTimeZone(s) => Some(s.to_string()),
            Value::IntervalYearMonth(s) => Some(s.to_string()),
            Value::IntervalDaySecond(s) => Some(s.to_string()),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float32(f) => Some(*f as f64),
            Value::Float(f) => Some(*f),
            Value::Float128(f) => f.to_f64().ok(),
            _ => None,
        }
    }

    pub fn as_bigint(&self) -> Option<i64> {
        match self {
            Value::BigInt(i) => Some(*i),
            Value::Integer(i) => Some(*i as i64),
            Value::UInteger(i) => Some(*i as i64),
            _ => None,
        }
    }

    pub fn as_int128(&self) -> Option<i128> {
        match self {
            Value::Int128(i) => Some(*i),
            Value::BigInt(i) => Some(*i as i128),
            Value::Integer(i) => Some(*i as i128),
            Value::UInteger(i) => Some(*i as i128),
            Value::UBigInt(i) => i128::try_from(*i).ok(),
            _ => None,
        }
    }

    pub fn as_uint(&self) -> Option<u32> {
        match self {
            Value::UInteger(i) => Some(*i),
            Value::Integer(i) if *i >= 0 => Some(*i as u32),
            _ => None,
        }
    }

    pub fn as_uint64(&self) -> Option<u64> {
        match self {
            Value::UBigInt(i) => Some(*i),
            Value::UInteger(i) => Some(*i as u64),
            Value::Integer(i) if *i >= 0 => Some(*i as u64),
            Value::BigInt(i) if *i >= 0 => Some(*i as u64),
            _ => None,
        }
    }

    pub fn as_uint128(&self) -> Option<u128> {
        match self {
            Value::UInt128(i) => Some(*i),
            Value::UBigInt(i) => Some(*i as u128),
            Value::UInteger(i) => Some(*i as u128),
            Value::Integer(i) if *i >= 0 => Some(*i as u128),
            Value::BigInt(i) if *i >= 0 => Some(*i as u128),
            Value::Int128(i) if *i >= 0 => Some(*i as u128),
            _ => None,
        }
    }

    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            Value::Blob(bytes) => Some(bytes),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    fn is_integral_value(&self) -> bool {
        matches!(
            self,
            Value::Integer(_)
                | Value::BigInt(_)
                | Value::Int128(_)
                | Value::UInteger(_)
                | Value::UBigInt(_)
                | Value::UInt128(_)
        )
    }

    pub fn is_float_like(&self) -> bool {
        matches!(
            self,
            Value::Float32(_) | Value::Float(_) | Value::Float128(_)
        )
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::BigInt(a), Value::BigInt(b)) => a.partial_cmp(b),
            (Value::Int128(a), Value::Int128(b)) => a.partial_cmp(b),
            (Value::UInteger(a), Value::UInteger(b)) => a.partial_cmp(b),
            (Value::UBigInt(a), Value::UBigInt(b)) => a.partial_cmp(b),
            (Value::UInt128(a), Value::UInt128(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::BigInt(b)) => (*a as i64).partial_cmp(b),
            (Value::BigInt(a), Value::Integer(b)) => a.partial_cmp(&(*b as i64)),
            (Value::Integer(a), Value::Int128(b)) => (*a as i128).partial_cmp(b),
            (Value::Int128(a), Value::Integer(b)) => a.partial_cmp(&(*b as i128)),
            (Value::BigInt(a), Value::Int128(b)) => (*a as i128).partial_cmp(b),
            (Value::Int128(a), Value::BigInt(b)) => a.partial_cmp(&(*b as i128)),
            (left, right) if left.is_integral_value() && right.is_integral_value() => {
                compare_integral_values(left, right)
            }
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Enum(a), Value::Enum(b)) => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Float32(a), Value::Float32(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Float128(a), Value::Float128(b)) => a.partial_cmp(b),
            (Value::Float32(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(b), Value::Float32(a)) => b.partial_cmp(&(*a as f64)),
            (Value::Float32(a), Value::Float128(b)) => {
                Float128Value::from_f64(*a as f64).ok()?.partial_cmp(b)
            }
            (Value::Float128(a), Value::Float32(b)) => {
                a.partial_cmp(&Float128Value::from_f64(*b as f64).ok()?)
            }
            (Value::Float(a), Value::Float128(b)) => {
                Float128Value::from_f64(*a).ok()?.partial_cmp(b)
            }
            (Value::Float128(a), Value::Float(b)) => {
                a.partial_cmp(&Float128Value::from_f64(*b).ok()?)
            }
            (Value::Decimal(a), Value::Decimal(b)) => a.partial_cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.partial_cmp(b),
            (Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
            (Value::Time(a), Value::Time(b)) => a.partial_cmp(b),
            (Value::DateTime(a), Value::DateTime(b)) => a.partial_cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),
            (Value::TimeWithTimeZone(a), Value::TimeWithTimeZone(b)) => a.partial_cmp(b),
            (Value::IntervalYearMonth(a), Value::IntervalYearMonth(b)) => a.partial_cmp(b),
            (Value::IntervalDaySecond(a), Value::IntervalDaySecond(b)) => a.partial_cmp(b),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),
            _ => None,
        }
    }
}

fn compare_integral_values(left: &Value, right: &Value) -> Option<Ordering> {
    #[derive(Clone, Copy)]
    enum Integral {
        Signed(i128),
        Unsigned(u128),
    }

    fn integral(value: &Value) -> Option<Integral> {
        match value {
            Value::Integer(value) => Some(Integral::Signed((*value).into())),
            Value::BigInt(value) => Some(Integral::Signed((*value).into())),
            Value::Int128(value) => Some(Integral::Signed(*value)),
            Value::UInteger(value) => Some(Integral::Unsigned((*value).into())),
            Value::UBigInt(value) => Some(Integral::Unsigned((*value).into())),
            Value::UInt128(value) => Some(Integral::Unsigned(*value)),
            _ => None,
        }
    }

    match (integral(left)?, integral(right)?) {
        (Integral::Signed(left), Integral::Signed(right)) => left.partial_cmp(&right),
        (Integral::Unsigned(left), Integral::Unsigned(right)) => left.partial_cmp(&right),
        (Integral::Signed(left), Integral::Unsigned(right)) => {
            if left < 0 {
                Some(Ordering::Less)
            } else {
                (left as u128).partial_cmp(&right)
            }
        }
        (Integral::Unsigned(left), Integral::Signed(right)) => {
            if right < 0 {
                Some(Ordering::Greater)
            } else {
                left.partial_cmp(&(right as u128))
            }
        }
    }
}

fn parse_time_components(input: &str, type_name: &str) -> Result<(u32, u32, u32)> {
    let parts = input.split(':').collect::<Vec<_>>();
    if parts.len() != 3
        || parts.iter().any(|part| part.len() != 2)
        || !parts
            .iter()
            .all(|part| part.chars().all(|ch| ch.is_ascii_digit()))
    {
        return Err(HematiteError::ParseError(format!(
            "Invalid {} value '{}'",
            type_name, input
        )));
    }
    let hour = parts[0].parse::<u32>().map_err(|_| {
        HematiteError::ParseError(format!("Invalid {} value '{}'", type_name, input))
    })?;
    let minute = parts[1].parse::<u32>().map_err(|_| {
        HematiteError::ParseError(format!("Invalid {} value '{}'", type_name, input))
    })?;
    let second = parts[2].parse::<u32>().map_err(|_| {
        HematiteError::ParseError(format!("Invalid {} value '{}'", type_name, input))
    })?;
    if hour > 23 || minute > 59 || second > 59 {
        return Err(HematiteError::ParseError(format!(
            "Invalid {} value '{}'",
            type_name, input
        )));
    }
    Ok((hour, minute, second))
}

fn parse_timezone_offset(offset: &str, input: &str) -> Result<i16> {
    if offset.len() != 6
        || !matches!(offset.as_bytes()[0], b'+' | b'-')
        || offset.as_bytes()[3] != b':'
        || !offset[1..3].chars().all(|ch| ch.is_ascii_digit())
        || !offset[4..6].chars().all(|ch| ch.is_ascii_digit())
    {
        return Err(HematiteError::ParseError(format!(
            "Invalid TIME WITH TIME ZONE value '{}'",
            input
        )));
    }

    let sign = if offset.as_bytes()[0] == b'-' { -1 } else { 1 };
    let hours = offset[1..3].parse::<i16>().map_err(|_| {
        HematiteError::ParseError(format!("Invalid TIME WITH TIME ZONE value '{}'", input))
    })?;
    let minutes = offset[4..6].parse::<i16>().map_err(|_| {
        HematiteError::ParseError(format!("Invalid TIME WITH TIME ZONE value '{}'", input))
    })?;
    if hours > 23 || minutes > 59 {
        return Err(HematiteError::ParseError(format!(
            "Invalid TIME WITH TIME ZONE value '{}'",
            input
        )));
    }

    Ok(sign * (hours * 60 + minutes))
}

fn validate_date_components(year: i32, month: u32, day: u32, input: &str) -> Result<()> {
    if !(1..=12).contains(&month) {
        return Err(HematiteError::ParseError(format!(
            "Invalid DATE value '{}'",
            input
        )));
    }
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => unreachable!(),
    };
    if day == 0 || day > max_day {
        return Err(HematiteError::ParseError(format!(
            "Invalid DATE value '{}'",
            input
        )));
    }
    Ok(())
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i32 {
    let year = year - if month <= 2 { 1 } else { 0 };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month as i32;
    let day = day as i32;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_from_days(days: i32) -> (i32, u32, u32) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = year + if month <= 2 { 1 } else { 0 };
    (year, month as u32, day as u32)
}
