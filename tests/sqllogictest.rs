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
            Value::Integer(_)
            | Value::BigInt(_)
            | Value::Int128(_)
            | Value::UInteger(_)
            | Value::UBigInt(_)
            | Value::UInt128(_)
            | Value::Boolean(_) => {
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
        Value::Int128(value) => value.to_string(),
        Value::UInteger(value) => value.to_string(),
        Value::UBigInt(value) => value.to_string(),
        Value::UInt128(value) => value.to_string(),
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
fn sqllogictest_corpus() {
    let files = collect_slt_files_from_manifest("tests/sqllogictest");
    assert!(!files.is_empty(), "sqllogictest corpus should not be empty");

    for file in files {
        let mut runner = sqllogictest::Runner::new(|| async { HematiteSlt::new() });
        runner
            .run_file(&file)
            .unwrap_or_else(|err| panic!("sqllogictest file {} should pass: {err}", file));
    }
}

fn collect_slt_files_from_manifest(root: &str) -> Vec<String> {
    let root_path = std::path::Path::new(root);
    let manifest_path = root_path.join("manifest.txt");
    let manifest = std::fs::read_to_string(&manifest_path).unwrap_or_else(|err| {
        panic!(
            "sqllogictest manifest {} should be readable: {err}",
            manifest_path.display()
        )
    });

    let mut files = Vec::new();
    for line in manifest.lines() {
        let entry = line.trim();
        if entry.is_empty() || entry.starts_with('#') {
            continue;
        }

        let path = root_path.join(entry);
        collect_slt_files_recursive(&path, &mut files);
    }

    files.sort();
    files.dedup();
    files
}

fn collect_slt_files_recursive(path: &std::path::Path, files: &mut Vec<String>) {
    let entries = std::fs::read_dir(path).expect("sqllogictest directory should exist");
    for entry in entries {
        let entry = entry.expect("sqllogictest entry should be readable");
        let entry_path = entry.path();
        if entry_path.is_dir() {
            collect_slt_files_recursive(&entry_path, files);
        } else if entry_path.extension().and_then(|ext| ext.to_str()) == Some("slt") {
            files.push(
                entry_path
                    .to_str()
                    .expect("sqllogictest path should be valid utf-8")
                    .to_string(),
            );
        }
    }
}
