//! Relational row and index-key encoding.

use crate::catalog::types::Float128Value;
use crate::catalog::{
    DateTimeValue, DateValue, DecimalValue, TimeValue, TimeWithTimeZoneValue, Value,
};
use crate::error::{HematiteError, Result};

use super::record::StoredRow;

pub struct RowCodec;

impl RowCodec {
    pub fn encode_values(values: &[Value]) -> Result<Vec<u8>> {
        Self::encode_stored_row(&StoredRow {
            row_id: 0,
            values: values.to_vec(),
        })
    }

    pub fn encode_stored_row(row: &StoredRow) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&(0u32).to_le_bytes());
        buffer.extend_from_slice(&row.row_id.to_le_bytes());
        buffer.extend_from_slice(&(row.values.len() as u32).to_le_bytes());

        for value in &row.values {
            match value {
                Value::Integer(i) => {
                    buffer.push(1);
                    buffer.extend_from_slice(&i.to_le_bytes());
                }
                Value::BigInt(i) => {
                    buffer.push(6);
                    buffer.extend_from_slice(&i.to_le_bytes());
                }
                Value::Int128(i) => {
                    buffer.push(17);
                    buffer.extend_from_slice(&i.to_le_bytes());
                }
                Value::UInteger(i) => {
                    buffer.push(18);
                    buffer.extend_from_slice(&i.to_le_bytes());
                }
                Value::UBigInt(i) => {
                    buffer.push(19);
                    buffer.extend_from_slice(&i.to_le_bytes());
                }
                Value::UInt128(i) => {
                    buffer.push(20);
                    buffer.extend_from_slice(&i.to_le_bytes());
                }
                Value::Text(s) => {
                    buffer.push(2);
                    write_bytes(&mut buffer, s.as_bytes());
                }
                Value::Enum(s) => {
                    buffer.push(11);
                    write_bytes(&mut buffer, s.as_bytes());
                }
                Value::Boolean(b) => {
                    buffer.push(3);
                    buffer.push(u8::from(*b));
                }
                Value::Float32(f) => {
                    buffer.push(21);
                    buffer.extend_from_slice(&f.to_le_bytes());
                }
                Value::Float(f) => {
                    buffer.push(4);
                    buffer.extend_from_slice(&f.to_le_bytes());
                }
                Value::Decimal(decimal) => {
                    buffer.push(7);
                    write_decimal(&mut buffer, decimal);
                }
                Value::Blob(bytes) => {
                    buffer.push(8);
                    write_bytes(&mut buffer, bytes);
                }
                Value::Date(date) => {
                    buffer.push(9);
                    buffer.extend_from_slice(&date.days_since_epoch().to_le_bytes());
                }
                Value::Time(time) => {
                    buffer.push(12);
                    buffer.extend_from_slice(&time.seconds_since_midnight().to_le_bytes());
                }
                Value::DateTime(datetime) => {
                    buffer.push(10);
                    buffer.extend_from_slice(&datetime.seconds_since_epoch().to_le_bytes());
                }
                Value::TimeWithTimeZone(value) => {
                    buffer.push(14);
                    buffer.extend_from_slice(&value.seconds_since_midnight().to_le_bytes());
                    buffer.extend_from_slice(&value.offset_minutes().to_le_bytes());
                }
                Value::IntervalYearMonth(value) => {
                    buffer.push(15);
                    buffer.extend_from_slice(&value.total_months().to_le_bytes());
                }
                Value::IntervalDaySecond(value) => {
                    buffer.push(16);
                    buffer.extend_from_slice(&value.total_seconds().to_le_bytes());
                }
                Value::Null => buffer.push(5),
            }
        }

        let payload_len = buffer.len() - 4;
        buffer[0..4].copy_from_slice(&(payload_len as u32).to_le_bytes());
        Ok(buffer)
    }

    pub fn decode_values(data: &[u8]) -> Result<Vec<Value>> {
        let encoded = if data.len() >= 4 {
            let payload_len = Self::read_payload_length(&data[0..4])?;
            if payload_len + 4 == data.len() {
                data.to_vec()
            } else {
                let mut encoded = Vec::with_capacity(data.len() + 4);
                encoded.extend_from_slice(&(data.len() as u32).to_le_bytes());
                encoded.extend_from_slice(data);
                encoded
            }
        } else {
            let mut encoded = Vec::with_capacity(data.len() + 4);
            encoded.extend_from_slice(&(data.len() as u32).to_le_bytes());
            encoded.extend_from_slice(data);
            encoded
        };

        Ok(Self::decode_stored_row(&encoded)?.values)
    }

    pub fn decode_stored_row(data: &[u8]) -> Result<StoredRow> {
        if data.len() < 12 {
            return Err(HematiteError::CorruptedData(
                "Stored row header is truncated".to_string(),
            ));
        }

        let mut offset = 0usize;
        let payload_len = Self::read_payload_length(&data[0..4])?;
        offset += 4;

        if payload_len + 4 > data.len() {
            return Err(HematiteError::CorruptedData(
                "Stored row length exceeds available bytes".to_string(),
            ));
        }

        let row_id = u64::from_le_bytes(data[offset..offset + 8].try_into().map_err(|_| {
            HematiteError::CorruptedData("Stored row rowid is truncated".to_string())
        })?);
        offset += 8;

        let value_count = u32::from_le_bytes(data[offset..offset + 4].try_into().map_err(|_| {
            HematiteError::CorruptedData("Stored row value count is truncated".to_string())
        })?) as usize;
        offset += 4;

        let payload_end = payload_len + 4;
        let mut values = Vec::with_capacity(value_count);

        for _ in 0..value_count {
            if offset >= payload_end {
                return Err(HematiteError::CorruptedData(
                    "Stored row ended before all values were decoded".to_string(),
                ));
            }

            let tag = data[offset];
            offset += 1;
            let value = match tag {
                1 => {
                    let bytes = read_exact(data, &mut offset, payload_end, 4, "Integer value")?;
                    Value::Integer(i32::from_le_bytes(bytes.try_into().unwrap()))
                }
                2 => {
                    let bytes = read_bytes(data, &mut offset, payload_end, "Text value")?;
                    let text = String::from_utf8(bytes).map_err(|_| {
                        HematiteError::CorruptedData("Invalid UTF-8 in text value".to_string())
                    })?;
                    Value::Text(text)
                }
                11 => {
                    let bytes = read_bytes(data, &mut offset, payload_end, "Enum value")?;
                    let text = String::from_utf8(bytes).map_err(|_| {
                        HematiteError::CorruptedData("Invalid UTF-8 in enum value".to_string())
                    })?;
                    Value::Enum(text)
                }
                3 => {
                    let bytes = read_exact(data, &mut offset, payload_end, 1, "Boolean value")?;
                    Value::Boolean(bytes[0] != 0)
                }
                4 => {
                    let bytes = read_exact(data, &mut offset, payload_end, 8, "Float value")?;
                    Value::Float(f64::from_le_bytes(bytes.try_into().unwrap()))
                }
                21 => {
                    let bytes = read_exact(data, &mut offset, payload_end, 4, "Float32 value")?;
                    Value::Float32(f32::from_le_bytes(bytes.try_into().unwrap()))
                }
                22 => {
                    let bytes = read_exact(data, &mut offset, payload_end, 16, "Float128 value")?;
                    let bits = u128::from_le_bytes(bytes.try_into().unwrap());
                    Value::Float(Float128Value::from_storage_bits(bits)?.to_f64()?)
                }
                5 => Value::Null,
                6 => {
                    let bytes = read_exact(data, &mut offset, payload_end, 8, "BigInt value")?;
                    Value::BigInt(i64::from_le_bytes(bytes.try_into().unwrap()))
                }
                17 => {
                    let bytes = read_exact(data, &mut offset, payload_end, 16, "Int128 value")?;
                    Value::Int128(i128::from_le_bytes(bytes.try_into().unwrap()))
                }
                18 => {
                    let bytes = read_exact(data, &mut offset, payload_end, 4, "UInt value")?;
                    Value::UInteger(u32::from_le_bytes(bytes.try_into().unwrap()))
                }
                19 => {
                    let bytes = read_exact(data, &mut offset, payload_end, 8, "UInt64 value")?;
                    Value::UBigInt(u64::from_le_bytes(bytes.try_into().unwrap()))
                }
                20 => {
                    let bytes = read_exact(data, &mut offset, payload_end, 16, "UInt128 value")?;
                    Value::UInt128(u128::from_le_bytes(bytes.try_into().unwrap()))
                }
                7 => Value::Decimal(read_decimal(data, &mut offset, payload_end)?),
                8 => Value::Blob(read_bytes(data, &mut offset, payload_end, "Blob value")?),
                9 => {
                    let bytes = read_exact(data, &mut offset, payload_end, 4, "Date value")?;
                    Value::Date(DateValue::from_days_since_epoch(i32::from_le_bytes(
                        bytes.try_into().unwrap(),
                    )))
                }
                12 => {
                    let bytes = read_exact(data, &mut offset, payload_end, 4, "Time value")?;
                    Value::Time(TimeValue::from_seconds_since_midnight(u32::from_le_bytes(
                        bytes.try_into().unwrap(),
                    )))
                }
                10 => {
                    let bytes = read_exact(data, &mut offset, payload_end, 8, "DateTime value")?;
                    Value::DateTime(DateTimeValue::from_seconds_since_epoch(i64::from_le_bytes(
                        bytes.try_into().unwrap(),
                    )))
                }
                13 => {
                    let bytes =
                        read_exact(data, &mut offset, payload_end, 8, "legacy Timestamp value")?;
                    Value::DateTime(DateTimeValue::from_seconds_since_epoch(i64::from_le_bytes(
                        bytes.try_into().unwrap(),
                    )))
                }
                14 => {
                    let seconds = u32::from_le_bytes(
                        read_exact(
                            data,
                            &mut offset,
                            payload_end,
                            4,
                            "Time with time zone seconds",
                        )?
                        .try_into()
                        .unwrap(),
                    );
                    let offset_minutes = i16::from_le_bytes(
                        read_exact(
                            data,
                            &mut offset,
                            payload_end,
                            2,
                            "Time with time zone offset",
                        )?
                        .try_into()
                        .unwrap(),
                    );
                    Value::TimeWithTimeZone(TimeWithTimeZoneValue::from_parts(
                        seconds,
                        offset_minutes,
                    ))
                }
                15 => {
                    let bytes =
                        read_exact(data, &mut offset, payload_end, 4, "Interval year-month")?;
                    Value::IntervalYearMonth(crate::catalog::IntervalYearMonthValue::new(
                        i32::from_le_bytes(bytes.try_into().unwrap()),
                    ))
                }
                16 => {
                    let bytes =
                        read_exact(data, &mut offset, payload_end, 8, "Interval day-second")?;
                    Value::IntervalDaySecond(crate::catalog::IntervalDaySecondValue::new(
                        i64::from_le_bytes(bytes.try_into().unwrap()),
                    ))
                }
                _ => {
                    return Err(HematiteError::CorruptedData(format!(
                        "Unknown value tag {} in stored row",
                        tag
                    )))
                }
            };

            values.push(value);
        }

        Ok(StoredRow { row_id, values })
    }

    pub fn read_payload_length(prefix: &[u8]) -> Result<usize> {
        if prefix.len() != 4 {
            return Err(HematiteError::CorruptedData(
                "Row length prefix must be 4 bytes".to_string(),
            ));
        }

        Ok(u32::from_le_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]) as usize)
    }
}

pub struct IndexKeyCodec;

impl IndexKeyCodec {
    pub fn encode_key(values: &[Value]) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        for value in values {
            encode_key_value(&mut buffer, value);
        }
        Ok(buffer)
    }

    pub fn encode_secondary_key(values: &[Value], row_id: u64) -> Result<Vec<u8>> {
        let mut key = Self::encode_key(values)?;
        key.extend_from_slice(&row_id.to_be_bytes());
        Ok(key)
    }

    pub fn decode_row_id(value: &[u8]) -> Result<u64> {
        if value.len() != 8 {
            return Err(HematiteError::CorruptedData(
                "Index rowid payload must be exactly 8 bytes".to_string(),
            ));
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(value);
        Ok(u64::from_be_bytes(bytes))
    }

    pub fn split_secondary_key(key: &[u8]) -> Result<(Vec<u8>, u64)> {
        if key.len() < 8 {
            return Err(HematiteError::CorruptedData(
                "Index entry is missing rowid bytes".to_string(),
            ));
        }
        let mut row_id_bytes = [0u8; 8];
        row_id_bytes.copy_from_slice(&key[key.len() - 8..]);
        let row_id = u64::from_be_bytes(row_id_bytes);
        Ok((key[..key.len() - 8].to_vec(), row_id))
    }
}

pub struct RowSerializer;

impl RowSerializer {
    pub fn serialize(values: &[Value]) -> Result<Vec<u8>> {
        RowCodec::encode_values(values)
    }

    pub fn serialize_stored_row(row: &StoredRow) -> Result<Vec<u8>> {
        RowCodec::encode_stored_row(row)
    }

    pub fn deserialize(data: &[u8]) -> Result<Vec<Value>> {
        RowCodec::decode_values(data)
    }

    pub fn deserialize_stored_row(data: &[u8]) -> Result<StoredRow> {
        RowCodec::decode_stored_row(data)
    }

    pub fn read_row_length(prefix: &[u8]) -> Result<usize> {
        RowCodec::read_payload_length(prefix)
    }
}

fn encode_key_value(buffer: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => buffer.push(0),
        Value::Boolean(false) => buffer.push(1),
        Value::Boolean(true) => buffer.push(2),
        Value::Integer(value) => {
            buffer.push(3);
            buffer.extend_from_slice(&(i32::to_be_bytes(*value ^ i32::MIN)));
        }
        Value::BigInt(value) => {
            buffer.push(4);
            buffer.extend_from_slice(&(i64::to_be_bytes(*value ^ i64::MIN)));
        }
        Value::Int128(value) => {
            buffer.push(17);
            buffer.extend_from_slice(&(i128::to_be_bytes(*value ^ i128::MIN)));
        }
        Value::UInteger(value) => {
            buffer.push(18);
            buffer.extend_from_slice(&value.to_be_bytes());
        }
        Value::UBigInt(value) => {
            buffer.push(19);
            buffer.extend_from_slice(&value.to_be_bytes());
        }
        Value::UInt128(value) => {
            buffer.push(20);
            buffer.extend_from_slice(&value.to_be_bytes());
        }
        Value::Float32(value) => {
            buffer.push(21);
            buffer.extend_from_slice(&ordered_f32_bytes(*value));
        }
        Value::Float(value) => {
            buffer.push(5);
            buffer.extend_from_slice(&ordered_f64_bytes(*value));
        }
        Value::Decimal(value) => {
            buffer.push(6);
            buffer.push(u8::from(value.negative()));
            buffer.extend_from_slice(&value.scale().to_be_bytes());
            buffer.extend_from_slice(&(value.digit_bytes().len() as u32).to_be_bytes());
            write_packed_digits(buffer, value.digit_bytes());
        }
        Value::Text(value) => {
            buffer.push(7);
            write_bytes(buffer, value.as_bytes());
        }
        Value::Enum(value) => {
            buffer.push(11);
            write_bytes(buffer, value.as_bytes());
        }
        Value::Blob(value) => {
            buffer.push(8);
            write_bytes(buffer, value);
        }
        Value::Date(value) => {
            buffer.push(9);
            buffer.extend_from_slice(&(i32::to_be_bytes(value.days_since_epoch() ^ i32::MIN)));
        }
        Value::Time(value) => {
            buffer.push(12);
            buffer.extend_from_slice(&value.seconds_since_midnight().to_be_bytes());
        }
        Value::DateTime(value) => {
            buffer.push(10);
            buffer.extend_from_slice(&(i64::to_be_bytes(value.seconds_since_epoch() ^ i64::MIN)));
        }
        Value::TimeWithTimeZone(value) => {
            buffer.push(14);
            buffer.extend_from_slice(&value.seconds_since_midnight().to_be_bytes());
            buffer.extend_from_slice(&(i16::to_be_bytes(value.offset_minutes() ^ i16::MIN)));
        }
        Value::IntervalYearMonth(value) => {
            buffer.push(15);
            buffer.extend_from_slice(&(i32::to_be_bytes(value.total_months() ^ i32::MIN)));
        }
        Value::IntervalDaySecond(value) => {
            buffer.push(16);
            buffer.extend_from_slice(&(i64::to_be_bytes(value.total_seconds() ^ i64::MIN)));
        }
    }
}

fn ordered_f64_bytes(value: f64) -> [u8; 8] {
    let bits = value.to_bits();
    let transformed = if (bits >> 63) == 0 {
        bits ^ (1u64 << 63)
    } else {
        !bits
    };
    transformed.to_be_bytes()
}

fn ordered_f32_bytes(value: f32) -> [u8; 4] {
    let bits = value.to_bits();
    let transformed = if (bits >> 31) == 0 {
        bits ^ (1u32 << 31)
    } else {
        !bits
    };
    transformed.to_be_bytes()
}

fn write_bytes(buffer: &mut Vec<u8>, bytes: &[u8]) {
    buffer.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buffer.extend_from_slice(bytes);
}

fn read_bytes(data: &[u8], offset: &mut usize, end: usize, label: &str) -> Result<Vec<u8>> {
    let len_bytes = read_exact(data, offset, end, 4, &format!("{label} length"))?;
    let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
    Ok(read_exact(data, offset, end, len, label)?.to_vec())
}

fn read_exact<'a>(
    data: &'a [u8],
    offset: &mut usize,
    end: usize,
    len: usize,
    label: &str,
) -> Result<&'a [u8]> {
    if *offset + len > end {
        return Err(HematiteError::CorruptedData(format!(
            "{} is truncated",
            label
        )));
    }
    let bytes = &data[*offset..*offset + len];
    *offset += len;
    Ok(bytes)
}

fn write_decimal(buffer: &mut Vec<u8>, value: &DecimalValue) {
    buffer.push(u8::from(value.negative()));
    buffer.extend_from_slice(&value.scale().to_le_bytes());
    buffer.extend_from_slice(&(value.digit_bytes().len() as u32).to_le_bytes());
    write_packed_digits(buffer, value.digit_bytes());
}

fn read_decimal(data: &[u8], offset: &mut usize, end: usize) -> Result<DecimalValue> {
    let sign = read_exact(data, offset, end, 1, "Decimal sign")?[0] != 0;
    let scale = u32::from_le_bytes(
        read_exact(data, offset, end, 4, "Decimal scale")?
            .try_into()
            .unwrap(),
    );
    let digit_count = u32::from_le_bytes(
        read_exact(data, offset, end, 4, "Decimal digit count")?
            .try_into()
            .unwrap(),
    ) as usize;
    let packed_len = digit_count.div_ceil(2);
    let packed = read_exact(data, offset, end, packed_len, "Decimal digits")?;
    let digits = read_packed_digits(packed, digit_count)?;
    let mut decimal = DecimalValue::parse(&format_decimal_digits(sign, &digits, scale as usize))?;
    if decimal.digit_bytes().len() == 1 && decimal.digit_bytes()[0] == 0 {
        decimal = DecimalValue::zero();
    }
    Ok(decimal)
}

fn format_decimal_digits(negative: bool, digits: &[u8], scale: usize) -> String {
    let mut out = String::new();
    if negative && !(digits.len() == 1 && digits[0] == 0) {
        out.push('-');
    }
    let digit_string = digits
        .iter()
        .map(|digit| char::from(b'0' + *digit))
        .collect::<String>();
    if scale == 0 {
        out.push_str(&digit_string);
        return out;
    }
    if digit_string.len() <= scale {
        out.push_str("0.");
        for _ in 0..scale - digit_string.len() {
            out.push('0');
        }
        out.push_str(&digit_string);
        return out;
    }
    let split = digit_string.len() - scale;
    out.push_str(&digit_string[..split]);
    out.push('.');
    out.push_str(&digit_string[split..]);
    out
}

fn write_packed_digits(buffer: &mut Vec<u8>, digits: &[u8]) {
    for chunk in digits.chunks(2) {
        let high = chunk[0] & 0x0F;
        let low = if chunk.len() > 1 {
            chunk[1] & 0x0F
        } else {
            0x0F
        };
        buffer.push((high << 4) | low);
    }
}

fn read_packed_digits(bytes: &[u8], digit_count: usize) -> Result<Vec<u8>> {
    let mut digits = Vec::with_capacity(digit_count);
    for byte in bytes {
        digits.push((byte >> 4) & 0x0F);
        if digits.len() == digit_count {
            break;
        }
        let low = byte & 0x0F;
        if low <= 9 {
            digits.push(low);
        }
        if digits.len() == digit_count {
            break;
        }
    }
    if digits.len() != digit_count || digits.iter().any(|digit| *digit > 9) {
        return Err(HematiteError::CorruptedData(
            "Packed decimal digits are invalid".to_string(),
        ));
    }
    Ok(digits)
}
