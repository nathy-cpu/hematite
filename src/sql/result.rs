//! SQL result set and row interface

use crate::catalog::Value;
use crate::error::{HematiteError, Result};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    column_index: HashMap<String, usize>,
}

impl ResultSet {
    pub fn new(columns: Vec<String>, rows: Vec<Vec<Value>>) -> Self {
        let mut column_index = HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            column_index.insert(col.clone(), i);
        }

        let rows = rows.into_iter().map(Row::new).collect();

        Self {
            columns,
            rows,
            column_index,
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn get_row(&self, index: usize) -> Option<&Row> {
        self.rows.get(index)
    }

    pub fn iter(&'_ self) -> std::slice::Iter<'_, Row> {
        self.rows.iter()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.column_index.get(column_name).copied()
    }
}

impl IntoIterator for ResultSet {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
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

    pub fn get_int(&self, index: usize) -> Result<i32> {
        match self.get(index) {
            Some(Value::Integer(i)) => Ok(*i),
            Some(value) => Err(HematiteError::ParseError(format!(
                "Expected INTEGER, found {:?}",
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
            Some(Value::Float(f)) => Ok(*f),
            Some(Value::Integer(i)) => Ok(*i as f64), // Allow integer to float conversion
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
}

impl IntoIterator for Row {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Value;

    #[test]
    fn test_result_set() -> Result<()> {
        let columns = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            vec![Value::Integer(1), Value::Text("Alice".to_string())],
            vec![Value::Integer(2), Value::Text("Bob".to_string())],
        ];

        let result_set = ResultSet::new(columns, rows);

        assert_eq!(result_set.len(), 2);
        assert_eq!(result_set.column_count(), 2);
        assert_eq!(result_set.get_column_index("id"), Some(0));
        assert_eq!(result_set.get_column_index("name"), Some(1));
        assert_eq!(result_set.get_column_index("invalid"), None);

        let row = result_set.get_row(0).unwrap();
        assert_eq!(row.get_int(0)?, 1);
        assert_eq!(row.get_string(1)?, "Alice");

        Ok(())
    }

    #[test]
    fn test_row() -> Result<()> {
        let values = vec![
            Value::Integer(42),
            Value::Text("test".to_string()),
            Value::Boolean(true),
            Value::Float(3.14),
            Value::Null,
        ];

        let row = Row::new(values);

        assert_eq!(row.get_int(0)?, 42);
        assert_eq!(row.get_string(1)?, "test");
        assert_eq!(row.get_bool(2)?, true);
        assert_eq!(row.get_float(3)?, 3.14);
        assert!(row.is_null(4));

        // Test type conversion errors
        assert!(row.get_string(0).is_err());
        assert!(row.get_bool(0).is_err());

        Ok(())
    }

    #[test]
    fn test_statement_result() -> Result<()> {
        let result = StatementResult::new(1, "Table created".to_string());
        assert_eq!(result.affected_rows, 1);
        assert_eq!(result.message, "Table created");
        assert!(result.last_insert_id.is_none());

        let result = StatementResult::with_insert_id(1, 42, "Row inserted".to_string());
        assert_eq!(result.affected_rows, 1);
        assert_eq!(result.last_insert_id, Some(42));
        assert_eq!(result.message, "Row inserted");

        Ok(())
    }
}
