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
    Decimal {
        precision: Option<u32>,
        scale: Option<u32>,
    },
    Blob,
    Date,
    Time,
    DateTime,
    TimeWithTimeZone,
    IntervalYearMonth,
    IntervalDaySecond,
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
            DataType::Decimal { precision, .. } => precision.unwrap_or(32) as usize,
            DataType::Blob => 4096,
            DataType::Date => 4,
            DataType::Time => 4,
            DataType::DateTime => 8,
            DataType::TimeWithTimeZone => 6,
            DataType::IntervalYearMonth => 4,
            DataType::IntervalDaySecond => 8,
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
            DataType::Decimal { precision, scale } => {
                format_numeric_type("DECIMAL", *precision, *scale)
            }
            DataType::Blob => "BLOB".to_string(),
            DataType::Date => "DATE".to_string(),
            DataType::Time => "TIME".to_string(),
            DataType::DateTime => "DATETIME".to_string(),
            DataType::TimeWithTimeZone => "TIME WITH TIME ZONE".to_string(),
            DataType::IntervalYearMonth => "INTERVAL YEAR TO MONTH".to_string(),
            DataType::IntervalDaySecond => "INTERVAL DAY TO SECOND".to_string(),
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
            DataType::Decimal { .. } => "DECIMAL",
            DataType::Blob => "BLOB",
            DataType::Date => "DATE",
            DataType::Time => "TIME",
            DataType::DateTime => "DATETIME",
            DataType::TimeWithTimeZone => "TIME WITH TIME ZONE",
            DataType::IntervalYearMonth => "INTERVAL YEAR TO MONTH",
            DataType::IntervalDaySecond => "INTERVAL DAY TO SECOND",
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
    Decimal(DecimalValue),
    Blob(Vec<u8>),
    Date(DateValue),
    Time(TimeValue),
    DateTime(DateTimeValue),
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
            Value::Decimal(_) => DataType::Decimal {
                precision: None,
                scale: None,
            },
            Value::Blob(_) => DataType::Blob,
            Value::Date(_) => DataType::Date,
            Value::Time(_) => DataType::Time,
            Value::DateTime(_) => DataType::DateTime,
            Value::TimeWithTimeZone(_) => DataType::TimeWithTimeZone,
            Value::IntervalYearMonth(_) => DataType::IntervalYearMonth,
            Value::IntervalDaySecond(_) => DataType::IntervalDaySecond,
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
            (Value::Decimal(value), DataType::Decimal { precision, scale }) => {
                value.fits_precision_scale(precision, scale)
            }
            (Value::Blob(_), DataType::Blob) => true,
            (Value::Date(_), DataType::Date) => true,
            (Value::Time(_), DataType::Time) => true,
            (Value::DateTime(_), DataType::DateTime) => true,
            (Value::TimeWithTimeZone(_), DataType::TimeWithTimeZone) => true,
            (Value::IntervalYearMonth(_), DataType::IntervalYearMonth) => true,
            (Value::IntervalDaySecond(_), DataType::IntervalDaySecond) => true,
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
        matches!(self, Value::Float32(_) | Value::Float(_))
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
            (Value::Float32(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(b), Value::Float32(a)) => b.partial_cmp(&(*a as f64)),
            (Value::Decimal(a), Value::Decimal(b)) => a.partial_cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.partial_cmp(b),
            (Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
            (Value::Time(a), Value::Time(b)) => a.partial_cmp(b),
            (Value::DateTime(a), Value::DateTime(b)) => a.partial_cmp(b),
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
