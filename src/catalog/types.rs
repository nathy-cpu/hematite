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
    UInt8,
    UInt16,
    UInt,
    UInt64,
    Text,
    VarChar(u32),
    Boolean,
    Float32,
    Float,
    Decimal {
        precision: Option<u32>,
        scale: Option<u32>,
    },
    Blob,
}

impl DataType {
    pub fn size(&self) -> usize {
        match self {
            DataType::Int8 => 1,
            DataType::Int16 => 2,
            DataType::Int => 4,
            DataType::Int64 => 8,
            DataType::UInt8 => 1,
            DataType::UInt16 => 2,
            DataType::UInt => 4,
            DataType::UInt64 => 8,
            DataType::Text => 255,
            DataType::VarChar(length) => *length as usize,
            DataType::Boolean => 1,
            DataType::Float32 => 4,
            DataType::Float => 8,
            DataType::Decimal { precision, .. } => precision.unwrap_or(32) as usize,
            DataType::Blob => 4096,
        }
    }

    pub fn name(&self) -> String {
        match self {
            DataType::Int8 => "INT8".to_string(),
            DataType::Int16 => "INT16".to_string(),
            DataType::Int => "INT".to_string(),
            DataType::Int64 => "INT64".to_string(),
            DataType::UInt8 => "UINT8".to_string(),
            DataType::UInt16 => "UINT16".to_string(),
            DataType::UInt => "UINT".to_string(),
            DataType::UInt64 => "UINT64".to_string(),
            DataType::Text => "TEXT".to_string(),
            DataType::VarChar(length) => format!("VARCHAR({length})"),
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Float32 => "FLOAT32".to_string(),
            DataType::Float => "FLOAT".to_string(),
            DataType::Decimal { precision, scale } => {
                format_numeric_type("DECIMAL", *precision, *scale)
            }
            DataType::Blob => "BLOB".to_string(),
        }
    }

    pub fn base_name(&self) -> &'static str {
        match self {
            DataType::Int8 => "INT8",
            DataType::Int16 => "INT16",
            DataType::Int => "INT",
            DataType::Int64 => "INT64",
            DataType::UInt8 => "UINT8",
            DataType::UInt16 => "UINT16",
            DataType::UInt => "UINT",
            DataType::UInt64 => "UINT64",
            DataType::Text => "TEXT",
            DataType::VarChar(_) => "VARCHAR",
            DataType::Boolean => "BOOLEAN",
            DataType::Float32 => "FLOAT32",
            DataType::Float => "FLOAT",
            DataType::Decimal { .. } => "DECIMAL",
            DataType::Blob => "BLOB",
        }
    }

    pub fn decimal_constraints(&self) -> Option<(Option<u32>, Option<u32>)> {
        match self {
            DataType::Decimal { precision, scale } => Some((*precision, *scale)),
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

    pub fn from_u32(value: u32) -> Self {
        Self::parse(&value.to_string()).expect("u32 string is always a valid decimal")
    }

    pub fn from_u64(value: u64) -> Self {
        Self::parse(&value.to_string()).expect("u64 string is always a valid decimal")
    }

    pub fn from_f64(value: f64) -> Result<Self> {
        if !value.is_finite() {
            return Err(HematiteError::ParseError(
                "Decimal value must be finite".to_string(),
            ));
        }
        Self::parse(&value.to_string())
    }

    pub fn is_integral(&self) -> bool {
        self.scale == 0
    }

    pub fn add(&self, other: &Self) -> Self {
        let target_scale = self.scale.max(other.scale);
        let left = scale_decimal_digits(&self.digits, self.scale, target_scale);
        let right = scale_decimal_digits(&other.digits, other.scale, target_scale);

        if self.negative == other.negative {
            normalize_decimal_parts(
                self.negative,
                add_digit_vectors(&left, &right),
                target_scale,
            )
        } else {
            match compare_digit_vectors(&left, &right) {
                Ordering::Greater => normalize_decimal_parts(
                    self.negative,
                    subtract_digit_vectors(&left, &right),
                    target_scale,
                ),
                Ordering::Less => normalize_decimal_parts(
                    other.negative,
                    subtract_digit_vectors(&right, &left),
                    target_scale,
                ),
                Ordering::Equal => Self::zero(),
            }
        }
    }

    pub fn subtract(&self, other: &Self) -> Self {
        if other.is_zero() {
            return self.clone();
        }

        let mut negated = other.clone();
        negated.negative = !negated.negative;
        self.add(&negated)
    }

    pub fn multiply(&self, other: &Self) -> Self {
        normalize_decimal_parts(
            self.negative ^ other.negative,
            multiply_digit_vectors(&self.digits, &other.digits),
            self.scale + other.scale,
        )
    }

    pub fn divide(&self, other: &Self) -> Result<Self> {
        if other.is_zero() {
            return Err(HematiteError::ParseError("Division by zero".to_string()));
        }

        const DECIMAL_DIVISION_SCALE: u32 = 18;

        let mut numerator = self.digits.clone();
        numerator.resize(
            numerator.len() + other.scale as usize + DECIMAL_DIVISION_SCALE as usize,
            0,
        );
        let mut denominator = other.digits.clone();
        denominator.resize(denominator.len() + self.scale as usize, 0);

        let (mut quotient, remainder) = divide_digit_vectors(&numerator, &denominator);
        if !is_zero_digit_vector(&remainder) {
            let doubled_remainder = add_digit_vectors(&remainder, &remainder);
            if compare_digit_vectors(&doubled_remainder, &denominator) != Ordering::Less {
                quotient = increment_digit_vector(&quotient);
            }
        }

        Ok(normalize_decimal_parts(
            self.negative ^ other.negative,
            quotient,
            DECIMAL_DIVISION_SCALE,
        ))
    }

    pub fn remainder(&self, other: &Self) -> Result<Self> {
        if other.is_zero() {
            return Err(HematiteError::ParseError("Division by zero".to_string()));
        }

        let target_scale = self.scale.max(other.scale);
        let left = scale_decimal_digits(&self.digits, self.scale, target_scale);
        let right = scale_decimal_digits(&other.digits, other.scale, target_scale);
        let (_, remainder) = divide_digit_vectors(&left, &right);
        Ok(normalize_decimal_parts(
            self.negative,
            remainder,
            target_scale,
        ))
    }

    pub fn negate(&self) -> Self {
        if self.is_zero() {
            Self::zero()
        } else {
            let mut negated = self.clone();
            negated.negative = !negated.negative;
            negated
        }
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

    pub fn to_f64(&self) -> Option<f64> {
        self.to_string().parse::<f64>().ok()
    }

    pub fn to_integral_u128(&self) -> Option<u128> {
        if !self.is_integral() || self.negative {
            return None;
        }

        let mut value = 0u128;
        for digit in &self.digits {
            value = value.checked_mul(10)?.checked_add(*digit as u128)?;
        }
        Some(value)
    }

    pub fn to_integral_i128(&self) -> Option<i128> {
        if !self.is_integral() {
            return None;
        }

        let magnitude = self.to_integral_u128_abs()?;
        if self.negative {
            if magnitude == (i128::MAX as u128) + 1 {
                Some(i128::MIN)
            } else {
                i128::try_from(magnitude).ok().map(|value| -value)
            }
        } else {
            i128::try_from(magnitude).ok()
        }
    }

    fn to_integral_u128_abs(&self) -> Option<u128> {
        if !self.is_integral() {
            return None;
        }

        let mut value = 0u128;
        for digit in &self.digits {
            value = value.checked_mul(10)?.checked_add(*digit as u128)?;
        }
        Some(value)
    }
}

fn normalize_decimal_parts(negative: bool, mut digits: Vec<u8>, mut scale: u32) -> DecimalValue {
    trim_leading_digit_zeros(&mut digits);
    while scale > 0 && digits.len() > 1 && digits.last() == Some(&0) {
        digits.pop();
        scale -= 1;
    }
    trim_leading_digit_zeros(&mut digits);
    if is_zero_digit_vector(&digits) {
        return DecimalValue::zero();
    }

    DecimalValue {
        negative,
        digits,
        scale,
    }
}

fn scale_decimal_digits(digits: &[u8], scale: u32, target_scale: u32) -> Vec<u8> {
    let mut scaled = digits.to_vec();
    scaled.resize(
        scaled.len() + target_scale.saturating_sub(scale) as usize,
        0,
    );
    scaled
}

fn compare_digit_vectors(left: &[u8], right: &[u8]) -> Ordering {
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

fn add_digit_vectors(left: &[u8], right: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(left.len().max(right.len()) + 1);
    let mut carry = 0u8;
    let mut left_index = left.len();
    let mut right_index = right.len();

    while left_index > 0 || right_index > 0 || carry > 0 {
        let left_digit = if left_index > 0 {
            left_index -= 1;
            left[left_index]
        } else {
            0
        };
        let right_digit = if right_index > 0 {
            right_index -= 1;
            right[right_index]
        } else {
            0
        };
        let total = left_digit + right_digit + carry;
        result.push(total % 10);
        carry = total / 10;
    }

    result.reverse();
    result
}

fn subtract_digit_vectors(left: &[u8], right: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(left.len());
    let mut borrow = 0i16;
    let mut left_index = left.len();
    let mut right_index = right.len();

    while left_index > 0 {
        left_index -= 1;
        let left_digit = left[left_index] as i16 - borrow;
        let right_digit = if right_index > 0 {
            right_index -= 1;
            right[right_index] as i16
        } else {
            0
        };
        if left_digit < right_digit {
            result.push((left_digit + 10 - right_digit) as u8);
            borrow = 1;
        } else {
            result.push((left_digit - right_digit) as u8);
            borrow = 0;
        }
    }

    result.reverse();
    trim_leading_digit_zeros(&mut result);
    result
}

fn multiply_digit_vectors(left: &[u8], right: &[u8]) -> Vec<u8> {
    if is_zero_digit_vector(left) || is_zero_digit_vector(right) {
        return vec![0];
    }

    let mut result = vec![0u32; left.len() + right.len()];
    for (left_index, left_digit) in left.iter().enumerate().rev() {
        for (right_index, right_digit) in right.iter().enumerate().rev() {
            let slot = left_index + right_index + 1;
            result[slot] += (*left_digit as u32) * (*right_digit as u32);
        }
    }

    for index in (1..result.len()).rev() {
        let carry = result[index] / 10;
        result[index] %= 10;
        result[index - 1] += carry;
    }

    let mut digits = result
        .into_iter()
        .map(|digit| digit as u8)
        .collect::<Vec<_>>();
    trim_leading_digit_zeros(&mut digits);
    digits
}

fn divide_digit_vectors(numerator: &[u8], denominator: &[u8]) -> (Vec<u8>, Vec<u8>) {
    debug_assert!(!is_zero_digit_vector(denominator));

    let mut quotient = Vec::with_capacity(numerator.len().max(1));
    let mut remainder = vec![0];

    for digit in numerator {
        if is_zero_digit_vector(&remainder) {
            remainder[0] = *digit;
        } else {
            remainder.push(*digit);
        }
        trim_leading_digit_zeros(&mut remainder);

        let mut quotient_digit = 0u8;
        while compare_digit_vectors(&remainder, denominator) != Ordering::Less {
            remainder = subtract_digit_vectors(&remainder, denominator);
            quotient_digit += 1;
        }
        quotient.push(quotient_digit);
    }

    trim_leading_digit_zeros(&mut quotient);
    trim_leading_digit_zeros(&mut remainder);
    (quotient, remainder)
}

fn increment_digit_vector(digits: &[u8]) -> Vec<u8> {
    add_digit_vectors(digits, &[1])
}

fn trim_leading_digit_zeros(digits: &mut Vec<u8>) {
    while digits.len() > 1 && digits.first() == Some(&0) {
        digits.remove(0);
    }
    if digits.is_empty() {
        digits.push(0);
    }
}

fn is_zero_digit_vector(digits: &[u8]) -> bool {
    digits.len() == 1 && digits[0] == 0
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

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(i32),
    BigInt(i64),
    UInteger(u32),
    UBigInt(u64),
    Text(String),
    Boolean(bool),
    Float32(f32),
    Float(f64),
    Decimal(DecimalValue),
    Blob(Vec<u8>),
    Null,
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Integer(_) => DataType::Int,
            Value::BigInt(_) => DataType::Int64,
            Value::UInteger(_) => DataType::UInt,
            Value::UBigInt(_) => DataType::UInt64,
            Value::Text(_) => DataType::Text,
            Value::Boolean(_) => DataType::Boolean,
            Value::Float32(_) => DataType::Float32,
            Value::Float(_) => DataType::Float,
            Value::Decimal(_) => DataType::Decimal {
                precision: None,
                scale: None,
            },
            Value::Blob(_) => DataType::Blob,
            Value::Null => DataType::Text,
        }
    }

    pub fn is_compatible_with(&self, data_type: DataType) -> bool {
        match (self, data_type) {
            (Value::Integer(_), DataType::Int8)
            | (Value::Integer(_), DataType::Int16)
            | (Value::Integer(_), DataType::Int) => true,
            (Value::BigInt(_), DataType::Int64) => true,
            (Value::UInteger(_), DataType::UInt8)
            | (Value::UInteger(_), DataType::UInt16)
            | (Value::UInteger(_), DataType::UInt) => true,
            (Value::UBigInt(_), DataType::UInt64) => true,
            (Value::Text(_), DataType::Text) | (Value::Text(_), DataType::VarChar(_)) => true,
            (Value::Boolean(_), DataType::Boolean) => true,
            (Value::Float32(_), DataType::Float32) => true,
            (Value::Float(_), DataType::Float) => true,
            (Value::Decimal(value), DataType::Decimal { precision, scale }) => {
                value.fits_precision_scale(precision, scale)
            }
            (Value::Blob(_), DataType::Blob) => true,
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
            Value::Decimal(s) => Some(s.to_string()),
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
            Value::Integer(_) | Value::BigInt(_) | Value::UInteger(_) | Value::UBigInt(_)
        )
    }

    pub fn is_float_like(&self) -> bool {
        matches!(self, Value::Float32(_) | Value::Float(_))
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::BigInt(a), Value::BigInt(b)) => a.partial_cmp(b),
            (Value::UInteger(a), Value::UInteger(b)) => a.partial_cmp(b),
            (Value::UBigInt(a), Value::UBigInt(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::BigInt(b)) => (*a as i64).partial_cmp(b),
            (Value::BigInt(a), Value::Integer(b)) => a.partial_cmp(&(*b as i64)),
            (left, right) if left.is_integral_value() && right.is_integral_value() => {
                compare_integral_values(left, right)
            }
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Float32(a), Value::Float32(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Float32(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(b), Value::Float32(a)) => b.partial_cmp(&(*a as f64)),
            (Value::Decimal(a), Value::Decimal(b)) => a.partial_cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.partial_cmp(b),
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
            Value::UInteger(value) => Some(Integral::Unsigned((*value).into())),
            Value::UBigInt(value) => Some(Integral::Unsigned((*value).into())),
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
