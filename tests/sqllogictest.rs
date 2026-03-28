use hematite::sql::result::ExecutedStatement;
use hematite::{Hematite, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
struct SltError(String);

impl std::fmt::Display for SltError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SltError {}

impl From<hematite::error::HematiteError> for SltError {
    fn from(value: hematite::error::HematiteError) -> Self {
        Self(value.to_string())
    }
}

struct HematiteSlt {
    db: Hematite,
}

impl HematiteSlt {
    fn new() -> Result<Self, SltError> {
        Ok(Self {
            db: Hematite::new_in_memory().map_err(SltError::from)?,
        })
    }
}

impl sqllogictest::DB for HematiteSlt {
    type Error = SltError;
    type ColumnType = sqllogictest::DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<sqllogictest::DBOutput<Self::ColumnType>, Self::Error> {
        match self.db.execute_result(sql).map_err(SltError::from)? {
            ExecutedStatement::Statement(result) => Ok(sqllogictest::DBOutput::StatementComplete(
                result.affected_rows as u64,
            )),
            ExecutedStatement::Query(result) => Ok(sqllogictest::DBOutput::Rows {
                types: infer_column_types(&result.rows),
                rows: result
                    .rows
                    .into_iter()
                    .map(|row| row.values.into_iter().map(render_value).collect())
                    .collect(),
            }),
        }
    }
}

fn infer_column_types(rows: &[hematite::sql::result::Row]) -> Vec<sqllogictest::DefaultColumnType> {
    let Some(first_row) = rows.first() else {
        return Vec::new();
    };

    first_row
        .values
        .iter()
        .map(|value| match value {
            Value::Integer(_) | Value::BigInt(_) | Value::Boolean(_) => {
                sqllogictest::DefaultColumnType::Integer
            }
            Value::Float(_) | Value::Decimal(_) => sqllogictest::DefaultColumnType::FloatingPoint,
            _ => sqllogictest::DefaultColumnType::Text,
        })
        .collect()
}

fn render_value(value: Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(value) => value.to_string(),
        Value::BigInt(value) => value.to_string(),
        Value::Text(value) => value,
        Value::Enum(value) => value,
        Value::Boolean(value) => {
            if value {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Value::Float(value) => value.to_string(),
        Value::Decimal(value) => value.to_string(),
        Value::Blob(value) => format!("{value:02X?}").replace([',', ' ', '[', ']'], ""),
        Value::Date(value) => value.to_string(),
        Value::Time(value) => value.to_string(),
        Value::DateTime(value) => value.to_string(),
        Value::Timestamp(value) => value.to_string(),
        Value::TimeWithTimeZone(value) => value.to_string(),
        Value::IntervalYearMonth(value) => value.to_string(),
        Value::IntervalDaySecond(value) => value.to_string(),
    }
}

#[test]
fn sqllogictest_basic() {
    let mut runner = sqllogictest::Runner::new(|| async { HematiteSlt::new() });
    runner
        .run_file("tests/sqllogictest/basic.slt")
        .expect("sqllogictest basic suite should pass");
}
