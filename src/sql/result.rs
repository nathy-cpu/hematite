//! SQL result set and row interface

use crate::error::{HematiteError, Result};
use crate::query::{DecimalValue, QueryResult, Value};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ResultSet {
    pub columns: Vec<String>,
    /// Every row's values end to end, `columns.len()` of them per row.
    ///
    /// Giving each row its own `Vec<Value>` means one allocation per row returned,
    /// and `malloc` and `free` for it are 24 % of a full scan. A [`Row`] is a view
    /// into this buffer instead.
    values: Vec<Value>,
    column_index: HashMap<String, usize>,
}

impl ResultSet {
    pub fn new(columns: Vec<String>, rows: Vec<Vec<Value>>) -> Self {
        let mut values = Vec::with_capacity(rows.len() * columns.len().max(1));
        for row in rows {
            values.extend(row);
        }
        Self::from_values(columns, values)
    }

    /// Builds a result set from values already laid out row-major.
    pub fn from_values(columns: Vec<String>, values: Vec<Value>) -> Self {
        let mut column_index = HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            column_index.insert(col.clone(), i);
        }
        Self {
            columns,
            values,
            column_index,
        }
    }

    /// The row width, which is how many values belong to each row.
    fn width(&self) -> usize {
        self.columns.len()
    }

    /// The rows, as views into the one buffer.
    pub fn rows(&self) -> impl Iterator<Item = Row<'_>> + '_ {
        let width = self.width();
        let chunks = if width == 0 {
            [].chunks_exact(1)
        } else {
            self.values.chunks_exact(width)
        };
        chunks.map(|values| Row { values })
    }

    pub fn len(&self) -> usize {
        if self.width() == 0 {
            0
        } else {
            self.values.len() / self.width()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get_row(&self, index: usize) -> Option<Row<'_>> {
        let width = self.width();
        if width == 0 {
            return None;
        }
        self.values
            .get(index * width..(index + 1) * width)
            .map(|values| Row { values })
    }

    pub fn iter(&self) -> impl Iterator<Item = Row<'_>> + '_ {
        self.rows()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.column_index.get(column_name).copied()
    }

    pub fn to_structs<T: FromRow>(&self) -> Result<Vec<T>> {
        self.rows().map(|row| T::from_row(&row)).collect()
    }

    pub fn render_ascii_table(&self) -> String {
        if self.columns.is_empty() {
            return format!("{} row(s)", self.len());
        }

        let mut widths = self
            .columns
            .iter()
            .map(|column| display_width(column))
            .collect::<Vec<_>>();

        let rendered_rows = self
            .rows()
            .map(|row| {
                row.values
                    .iter()
                    .map(render_value_for_table)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        for row in &rendered_rows {
            for (index, value) in row.iter().enumerate() {
                if index >= widths.len() {
                    widths.push(display_width(value));
                } else {
                    widths[index] = widths[index].max(display_width(value));
                }
            }
        }

        let border = ascii_border(&widths);
        let mut lines = Vec::new();
        lines.push(border.clone());
        lines.push(ascii_row(&self.columns, &widths));
        lines.push(border.clone());
        for row in &rendered_rows {
            lines.push(ascii_row(row, &widths));
        }
        lines.push(border);
        lines.push(format!("{} row(s)", self.len()));
        lines.join("\n")
    }
}

impl<'a> IntoIterator for &'a ResultSet {
    type Item = Row<'a>;
    type IntoIter = std::iter::Map<std::slice::ChunksExact<'a, Value>, fn(&'a [Value]) -> Row<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        let width = self.width();
        let chunks = if width == 0 {
            [].chunks_exact(1)
        } else {
            self.values.chunks_exact(width)
        };
        chunks.map(|values| Row { values })
    }
}

impl ResultSet {
    /// The rows as owned vectors, for a caller that needs to keep them past the
    /// result set.
    pub fn into_row_vecs(self) -> Vec<Vec<Value>> {
        let width = self.columns.len();
        if width == 0 {
            return Vec::new();
        }
        self.values
            .chunks_exact(width)
            .map(<[Value]>::to_vec)
            .collect()
    }
}

/// One row of a [`ResultSet`], borrowed from its buffer.
///
/// Borrowed rather than owning a `Vec<Value>`: that would be the single allocation
/// a scanned row makes, and 24 % of a full scan's instructions.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Row<'a> {
    pub values: &'a [Value],
}

impl<'a> Row<'a> {
    /// A row borrowing an existing slice of values.
    pub fn new(values: &'a [Value]) -> Self {
        Self { values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    pub fn get_by_name(
        &self,
        column_name: &str,
        column_index: &HashMap<String, usize>,
    ) -> Option<&Value> {
        if let Some(&idx) = column_index.get(column_name) {
            self.get(idx)
        } else {
            None
        }
    }

    pub fn to_struct<T: FromRow>(&self) -> Result<T> {
        T::from_row(self)
    }

    pub fn get_int(&self, index: usize) -> Result<i32> {
        match self.get(index) {
            Some(Value::Integer(i)) => Ok(*i),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected INT, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_string(&self, index: usize) -> Result<String> {
        match self.get(index) {
            Some(Value::Text(s)) => Ok(s.clone()),
            Some(Value::Decimal(value)) => Ok(value.to_string()),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected TEXT, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_bool(&self, index: usize) -> Result<bool> {
        match self.get(index) {
            Some(Value::Boolean(b)) => Ok(*b),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected BOOLEAN, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_float(&self, index: usize) -> Result<f64> {
        match self.get(index) {
            Some(Value::Float32(f)) => Ok(*f as f64),
            Some(Value::Float(f)) => Ok(*f),
            Some(Value::Integer(i)) => Ok(*i as f64), // Allow integer to float conversion
            Some(Value::UInteger(i)) => Ok(*i as f64),
            Some(Value::BigInt(i)) => Ok(*i as f64),
            Some(Value::UBigInt(i)) => Ok(*i as f64),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected FLOAT, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn is_null(&self, index: usize) -> bool {
        matches!(self.get(index), Some(Value::Null))
    }

    pub fn get_bigint(&self, index: usize) -> Result<i64> {
        match self.get(index) {
            Some(Value::BigInt(i)) => Ok(*i),
            Some(Value::Integer(i)) => Ok(*i as i64),
            Some(Value::UInteger(i)) => Ok(*i as i64),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected INT64, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_uint(&self, index: usize) -> Result<u32> {
        match self.get(index) {
            Some(Value::UInteger(i)) => Ok(*i),
            Some(Value::Integer(i)) if *i >= 0 => Ok(*i as u32),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected UINT, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_uint64(&self, index: usize) -> Result<u64> {
        match self.get(index) {
            Some(Value::UBigInt(i)) => Ok(*i),
            Some(Value::UInteger(i)) => Ok(*i as u64),
            Some(Value::Integer(i)) if *i >= 0 => Ok(*i as u64),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected UINT64, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_decimal(&self, index: usize) -> Result<DecimalValue> {
        match self.get(index) {
            Some(Value::Decimal(value)) => Ok(value.clone()),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected DECIMAL, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }

    pub fn get_blob(&self, index: usize) -> Result<Vec<u8>> {
        match self.get(index) {
            Some(Value::Blob(value)) => Ok(value.clone()),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected BLOB, found {:?}",
                value
            ))),
            None => Err(HematiteError::ParseError(
                "Column index out of bounds".to_string(),
            )),
        }
    }
}

pub trait FromRow: Sized {
    fn from_row(row: &Row<'_>) -> Result<Self>;
}

impl<'a> IntoIterator for Row<'a> {
    type Item = &'a Value;
    type IntoIter = std::slice::Iter<'a, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}

#[derive(Debug, Clone)]
pub struct StatementResult {
    pub affected_rows: usize,
    pub last_insert_id: Option<i32>,
    pub message: String,
}

impl StatementResult {
    pub fn new(affected_rows: usize, message: String) -> Self {
        Self {
            affected_rows,
            last_insert_id: None,
            message,
        }
    }

    pub fn with_insert_id(affected_rows: usize, last_insert_id: i32, message: String) -> Self {
        Self {
            affected_rows,
            last_insert_id: Some(last_insert_id),
            message,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ExecutedStatement {
    Query(ResultSet),
    Statement(StatementResult),
}

impl ExecutedStatement {
    pub(crate) fn from_query_result(query_result: QueryResult) -> Self {
        if query_result.columns.is_empty() {
            Self::Statement(StatementResult::new(
                query_result.affected_rows,
                "Ok".to_string(),
            ))
        } else {
            {
                // Straight through: both sides store rows end to end in one buffer,
                // so this is a move. Splitting it into a vector per row and joining
                // it back would put back the allocation the flat layout removes.
                let columns = query_result.columns.clone();
                Self::Query(ResultSet::from_values(columns, query_result.into_values()))
            }
        }
    }

    pub fn as_query(&self) -> Option<&ResultSet> {
        match self {
            Self::Query(result_set) => Some(result_set),
            Self::Statement(_) => None,
        }
    }

    pub fn as_statement(&self) -> Option<&StatementResult> {
        match self {
            Self::Query(_) => None,
            Self::Statement(result) => Some(result),
        }
    }

    pub fn render_ascii(&self) -> String {
        match self {
            Self::Query(result_set) => result_set.render_ascii_table(),
            Self::Statement(result) => format!("{} ({})", result.message, result.affected_rows),
        }
    }
}

fn render_value_for_table(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Text(value) => sanitize_table_cell(value),
        Value::Blob(bytes) => sanitize_table_cell(&format!("0x{}", hex::encode(bytes))),
        Value::Boolean(value) => value.to_string(),
        Value::Integer(value) => value.to_string(),
        Value::BigInt(value) => value.to_string(),
        Value::UInteger(value) => value.to_string(),
        Value::UBigInt(value) => value.to_string(),
        Value::Float32(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Decimal(value) => value.to_string(),
    }
}

fn sanitize_table_cell(value: &str) -> String {
    value.replace('\n', "\\n").replace('\r', "\\r")
}

fn display_width(value: &str) -> usize {
    value.chars().count()
}

fn ascii_border(widths: &[usize]) -> String {
    let mut border = String::new();
    border.push('+');
    for width in widths {
        border.push_str(&"-".repeat(*width + 2));
        border.push('+');
    }
    border
}

fn ascii_row(values: &[String], widths: &[usize]) -> String {
    let mut row = String::new();
    row.push('|');
    for (index, width) in widths.iter().enumerate() {
        let value = values.get(index).map(String::as_str).unwrap_or("");
        row.push(' ');
        row.push_str(value);
        let padding = width.saturating_sub(display_width(value));
        row.push_str(&" ".repeat(padding));
        row.push(' ');
        row.push('|');
    }
    row
}

mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            output.push(nibble_to_hex(byte >> 4));
            output.push(nibble_to_hex(byte & 0x0f));
        }
        output
    }

    fn nibble_to_hex(value: u8) -> char {
        match value {
            0..=9 => (b'0' + value) as char,
            10..=15 => (b'A' + (value - 10)) as char,
            _ => unreachable!("hex nibble out of range"),
        }
    }
}
