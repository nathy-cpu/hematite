//! Data types and runtime values for the relational layer.

use std::cmp::Ordering;
use std::fmt;

use crate::error::{HematiteError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Text,
    Char(u32),
    VarChar(u32),
    Boolean,
    Float,
    Real,
    Double,
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
    DateTime,
}

impl DataType {
    pub fn size(&self) -> usize {
        match self {
            DataType::TinyInt => 1,
            DataType::SmallInt => 2,
            DataType::Integer => 4,
            DataType::BigInt => 8,
            DataType::Text => 255,
            DataType::Char(length) | DataType::VarChar(length) => *length as usize,
            DataType::Boolean => 1,
            DataType::Float => 8,
            DataType::Real => 4,
            DataType::Double => 8,
            DataType::Decimal { precision, .. } | DataType::Numeric { precision, .. } => {
                precision.unwrap_or(32) as usize
            }
            DataType::Blob => 255,
            DataType::Date => 4,
            DataType::DateTime => 8,
        }
    }

    pub fn name(&self) -> String {
        match self {
            DataType::TinyInt => "TINYINT".to_string(),
            DataType::SmallInt => "SMALLINT".to_string(),
            DataType::Integer => "INTEGER".to_string(),
            DataType::BigInt => "BIGINT".to_string(),
            DataType::Text => "TEXT".to_string(),
            DataType::Char(length) => format!("CHAR({length})"),
            DataType::VarChar(length) => format!("VARCHAR({length})"),
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Float => "FLOAT".to_string(),
            DataType::Real => "REAL".to_string(),
            DataType::Double => "DOUBLE".to_string(),
            DataType::Decimal { precision, scale } => {
                format_numeric_type("DECIMAL", *precision, *scale)
            }
            DataType::Numeric { precision, scale } => {
                format_numeric_type("NUMERIC", *precision, *scale)
            }
            DataType::Blob => "BLOB".to_string(),
            DataType::Date => "DATE".to_string(),
            DataType::DateTime => "DATETIME".to_string(),
        }
    }

    pub fn base_name(&self) -> &'static str {
        match self {
            DataType::TinyInt => "TINYINT",
            DataType::SmallInt => "SMALLINT",
            DataType::Integer => "INTEGER",
            DataType::BigInt => "BIGINT",
            DataType::Text => "TEXT",
            DataType::Char(_) => "CHAR",
            DataType::VarChar(_) => "VARCHAR",
            DataType::Boolean => "BOOLEAN",
            DataType::Float => "FLOAT",
            DataType::Real => "REAL",
            DataType::Double => "DOUBLE",
            DataType::Decimal { .. } => "DECIMAL",
            DataType::Numeric { .. } => "NUMERIC",
            DataType::Blob => "BLOB",
            DataType::Date => "DATE",
            DataType::DateTime => "DATETIME",
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DateValue {
    days_since_epoch: i32,
}

impl DateValue {
    pub fn epoch() -> Self {
        Self { days_since_epoch: 0 }
    }

    pub fn parse(input: &str) -> Result<Self> {
        let value = input.trim();
        let parts = value.split('-').collect::<Vec<_>>();
        if parts.len() != 3
            || parts[0].len() != 4
            || parts[1].len() != 2
            || parts[2].len() != 2
            || !parts.iter().all(|part| part.chars().all(|ch| ch.is_ascii_digit()))
        {
            return Err(HematiteError::ParseError(format!(
                "Invalid DATE value '{}'",
                input
            )));
        }

        let year = parts[0].parse::<i32>().map_err(|_| {
            HematiteError::ParseError(format!("Invalid DATE value '{}'", input))
        })?;
        let month = parts[1].parse::<u32>().map_err(|_| {
            HematiteError::ParseError(format!("Invalid DATE value '{}'", input))
        })?;
        let day = parts[2].parse::<u32>().map_err(|_| {
            HematiteError::ParseError(format!("Invalid DATE value '{}'", input))
        })?;
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
}

impl fmt::Display for DateValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (year, month, day) = civil_from_days(self.days_since_epoch);
        write!(f, "{year:04}-{month:02}-{day:02}")
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
        let time_parts = time.split(':').collect::<Vec<_>>();
        if time_parts.len() != 3
            || time_parts.iter().any(|part| part.len() != 2)
            || !time_parts
                .iter()
                .all(|part| part.chars().all(|ch| ch.is_ascii_digit()))
        {
            return Err(HematiteError::ParseError(format!(
                "Invalid DATETIME value '{}'",
                input
            )));
        }
        let hour = time_parts[0].parse::<u32>().map_err(|_| {
            HematiteError::ParseError(format!("Invalid DATETIME value '{}'", input))
        })?;
        let minute = time_parts[1].parse::<u32>().map_err(|_| {
            HematiteError::ParseError(format!("Invalid DATETIME value '{}'", input))
        })?;
        let second = time_parts[2].parse::<u32>().map_err(|_| {
            HematiteError::ParseError(format!("Invalid DATETIME value '{}'", input))
        })?;
        if hour > 23 || minute > 59 || second > 59 {
            return Err(HematiteError::ParseError(format!(
                "Invalid DATETIME value '{}'",
                input
            )));
        }

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
}

impl fmt::Display for DateTimeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let days = self.seconds_since_epoch.div_euclid(86_400);
        let seconds = self.seconds_since_epoch.rem_euclid(86_400);
        let (year, month, day) = civil_from_days(days as i32);
        let hour = seconds / 3_600;
        let minute = (seconds % 3_600) / 60;
        let second = seconds % 60;
        write!(
            f,
            "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}"
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(i32),
    BigInt(i64),
    Text(String),
    Boolean(bool),
    Float(f64),
    Decimal(DecimalValue),
    Blob(Vec<u8>),
    Date(DateValue),
    DateTime(DateTimeValue),
    Null,
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Integer(_) => DataType::Integer,
            Value::BigInt(_) => DataType::BigInt,
            Value::Text(_) => DataType::Text,
            Value::Boolean(_) => DataType::Boolean,
            Value::Float(_) => DataType::Float,
            Value::Decimal(_) => DataType::Decimal {
                precision: None,
                scale: None,
            },
            Value::Blob(_) => DataType::Blob,
            Value::Date(_) => DataType::Date,
            Value::DateTime(_) => DataType::DateTime,
            Value::Null => DataType::Text,
        }
    }

    pub fn is_compatible_with(&self, data_type: DataType) -> bool {
        match (self, data_type) {
            (Value::Integer(_), DataType::TinyInt)
            | (Value::Integer(_), DataType::SmallInt)
            | (Value::Integer(_), DataType::Integer) => true,
            (Value::BigInt(_), DataType::BigInt) => true,
            (Value::Text(_), DataType::Text)
            | (Value::Text(_), DataType::Char(_))
            | (Value::Text(_), DataType::VarChar(_)) => true,
            (Value::Boolean(_), DataType::Boolean) => true,
            (Value::Float(_), DataType::Float)
            | (Value::Float(_), DataType::Real)
            | (Value::Float(_), DataType::Double) => true,
            (Value::Decimal(value), DataType::Decimal { precision, scale })
            | (Value::Decimal(value), DataType::Numeric { precision, scale }) => {
                value.fits_precision_scale(precision, scale)
            }
            (Value::Blob(_), DataType::Blob) => true,
            (Value::Date(_), DataType::Date) => true,
            (Value::DateTime(_), DataType::DateTime) => true,
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
            Value::Date(s) => Some(s.to_string()),
            Value::DateTime(s) => Some(s.to_string()),
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
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_bigint(&self) -> Option<i64> {
        match self {
            Value::BigInt(i) => Some(*i),
            Value::Integer(i) => Some(*i as i64),
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
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::BigInt(a), Value::BigInt(b)) => a.partial_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Decimal(a), Value::Decimal(b)) => a.partial_cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.partial_cmp(b),
            (Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
            (Value::DateTime(a), Value::DateTime(b)) => a.partial_cmp(b),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),
            _ => None,
        }
    }
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
