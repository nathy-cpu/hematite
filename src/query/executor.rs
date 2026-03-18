//! Query execution engine for processing SQL statements

use crate::catalog::{Column, DataType, Schema, Table, Value};
use crate::error::{HematiteError, Result};
use crate::parser::ast::*;
use crate::storage::StorageEngine;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug)]
pub struct ExecutionContext<'a> {
    pub catalog: Schema,
    pub storage: &'a mut StorageEngine,
}

impl<'a> ExecutionContext<'a> {
    pub fn new(catalog: &Schema, storage: &'a mut StorageEngine) -> Self {
        Self {
            catalog: catalog.clone(),
            storage,
        }
    }
}

pub trait QueryExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext<'_>) -> Result<QueryResult>;
}

#[derive(Debug, Clone)]
pub struct SelectExecutor {
    pub statement: SelectStatement,
}

impl SelectExecutor {
    pub fn new(statement: SelectStatement) -> Self {
        Self { statement }
    }

    fn evaluate_expression(
        &self,
        ctx: &ExecutionContext,
        expr: &Expression,
        row: &[Value],
    ) -> Result<Value> {
        match expr {
            Expression::Literal(value) => Ok(value.clone()),
            Expression::Column(name) => {
                // Find column index in the current row (simplified for single table)
                if let Some(table_name) = self.get_table_name() {
                    if let Some(table) = ctx.catalog.get_table_by_name(table_name) {
                        for (i, col) in table.columns.iter().enumerate() {
                            if col.name == *name {
                                if i < row.len() {
                                    return Ok(row[i].clone());
                                }
                            }
                        }
                    }
                }
                Err(HematiteError::ParseError(format!(
                    "Column '{}' not found",
                    name
                )))
            }
        }
    }

    fn evaluate_condition(
        &self,
        ctx: &ExecutionContext,
        condition: &Condition,
        row: &[Value],
    ) -> Result<bool> {
        match condition {
            Condition::Comparison {
                left,
                operator,
                right,
            } => {
                let left_val = self.evaluate_expression(ctx, left, row)?;
                let right_val = self.evaluate_expression(ctx, right, row)?;

                match operator {
                    ComparisonOperator::Equal => Ok(left_val == right_val),
                    ComparisonOperator::NotEqual => Ok(left_val != right_val),
                    ComparisonOperator::LessThan => Ok(left_val < right_val),
                    ComparisonOperator::LessThanOrEqual => Ok(left_val <= right_val),
                    ComparisonOperator::GreaterThan => Ok(left_val > right_val),
                    ComparisonOperator::GreaterThanOrEqual => Ok(left_val >= right_val),
                }
            }
            Condition::Logical {
                left,
                operator,
                right,
            } => {
                let left_result = self.evaluate_condition(ctx, left, row)?;
                let right_result = self.evaluate_condition(ctx, right, row)?;

                match operator {
                    LogicalOperator::And => Ok(left_result && right_result),
                    LogicalOperator::Or => Ok(left_result || right_result),
                }
            }
        }
    }

    fn get_table_name(&self) -> Option<&String> {
        match &self.statement.from {
            TableReference::Table(name) => Some(name),
        }
    }

    fn project_row(&self, ctx: &ExecutionContext, row: &[Value]) -> Result<Vec<Value>> {
        let mut projected = Vec::new();

        for item in &self.statement.columns {
            match item {
                SelectItem::Wildcard => projected.extend(row.iter().cloned()),
                SelectItem::Column(name) => {
                    if let Some(table_name) = self.get_table_name() {
                        if let Some(table) = ctx.catalog.get_table_by_name(table_name) {
                            for (i, col) in table.columns.iter().enumerate() {
                                if col.name == *name && i < row.len() {
                                    projected.push(row[i].clone());
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(projected)
    }

    fn get_column_names(&self, ctx: &ExecutionContext) -> Vec<String> {
        let mut columns = Vec::new();

        for item in &self.statement.columns {
            match item {
                SelectItem::Wildcard => {
                    if let Some(table_name) = self.get_table_name() {
                        if let Some(table) = ctx.catalog.get_table_by_name(table_name) {
                            for col in &table.columns {
                                columns.push(col.name.clone());
                            }
                        }
                    }
                }
                SelectItem::Column(name) => columns.push(name.clone()),
            }
        }

        columns
    }
}

impl QueryExecutor for SelectExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext) -> Result<QueryResult> {
        // Validate statement
        self.statement.validate(&ctx.catalog)?;

        let table_name = match &self.statement.from {
            TableReference::Table(name) => name.clone(),
        };

        let _table = ctx.catalog.get_table_by_name(&table_name).ok_or_else(|| {
            HematiteError::ParseError(format!("Table '{}' not found", table_name))
        })?;

        // Read data from storage
        let all_rows = ctx.storage.read_from_table(&table_name)?;

        // Apply WHERE clause filtering
        let mut filtered_rows = Vec::new();
        for row in &all_rows {
            let include = match &self.statement.where_clause {
                Some(where_clause) => {
                    let mut all_conditions_met = true;
                    for condition in &where_clause.conditions {
                        if !self.evaluate_condition(ctx, condition, row)? {
                            all_conditions_met = false;
                            break;
                        }
                    }
                    all_conditions_met
                }
                None => true,
            };

            if include {
                filtered_rows.push(row.clone());
            }
        }

        // Apply column projection
        let mut projected_rows = Vec::new();
        for row in filtered_rows {
            projected_rows.push(self.project_row(ctx, &row)?);
        }

        Ok(QueryResult {
            columns: self.get_column_names(ctx),
            rows: projected_rows,
        })
    }
}

#[derive(Debug, Clone)]
pub struct InsertExecutor {
    pub statement: InsertStatement,
}

impl InsertExecutor {
    pub fn new(statement: InsertStatement) -> Self {
        Self { statement }
    }

    fn validate_values(&self, table: &Table) -> Result<()> {
        for (_row_idx, value_row) in self.statement.values.iter().enumerate() {
            for (col_idx, value_expr) in value_row.iter().enumerate() {
                if col_idx < self.statement.columns.len() {
                    let col_name = &self.statement.columns[col_idx];
                    if let Some(column) = table.get_column_by_name(col_name) {
                        // Extract the actual value from the expression
                        let value = match value_expr {
                            Expression::Literal(v) => v,
                            _ => {
                                return Err(HematiteError::ParseError(format!(
                                    "Only literal values are supported in INSERT, column '{}'",
                                    col_name
                                )));
                            }
                        };

                        // Simplified validation - just check for basic type compatibility
                        match (&column.data_type, value) {
                            (DataType::Integer, Value::Integer(_)) => {}
                            (DataType::Text, Value::Text(_)) => {}
                            (DataType::Boolean, Value::Boolean(_)) => {}
                            (DataType::Float, Value::Float(_)) => {}
                            (DataType::Float, Value::Integer(_)) => {} // Allow integer to float conversion
                            _ => {
                                return Err(HematiteError::ParseError(format!(
                                    "Type mismatch: column '{}' expects {:?}, got {:?}",
                                    col_name, column.data_type, value
                                )));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl QueryExecutor for InsertExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext) -> Result<QueryResult> {
        // Validate statement
        self.statement.validate(&ctx.catalog)?;

        let table = ctx
            .catalog
            .get_table_by_name(&self.statement.table)
            .ok_or_else(|| {
                HematiteError::ParseError(format!("Table '{}' not found", self.statement.table))
            })?;

        // Validate values against column types
        self.validate_values(table)?;

        // Insert data into storage
        for value_row in &self.statement.values {
            // Convert Expression to Value (simplified - only literals supported)
            let row_values: Vec<Value> = value_row
                .iter()
                .map(|expr| {
                    match expr {
                        Expression::Literal(val) => val.clone(),
                        _ => Value::Null, // Simplified: non-literals become NULL
                    }
                })
                .collect();

            ctx.storage
                .insert_into_table(&self.statement.table, row_values)?;
        }

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        })
    }
}

#[derive(Debug, Clone)]
pub struct CreateExecutor {
    pub statement: CreateStatement,
}

impl CreateExecutor {
    pub fn new(statement: CreateStatement) -> Self {
        Self { statement }
    }

    fn convert_column_definitions(&self) -> Result<Vec<Column>> {
        let mut columns = Vec::new();
        let mut next_id = 1;

        for col_def in &self.statement.columns {
            let mut column = Column::new(
                crate::catalog::ColumnId::new(next_id),
                col_def.name.clone(),
                col_def.data_type.clone(),
            )
            .nullable(col_def.nullable)
            .primary_key(col_def.primary_key);

            if let Some(default_val) = &col_def.default_value {
                column = column.default_value(default_val.clone());
            }

            columns.push(column);
            next_id += 1;
        }

        Ok(columns)
    }
}

impl QueryExecutor for CreateExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext) -> Result<QueryResult> {
        // Validate statement
        self.statement.validate(&ctx.catalog)?;

        let columns = self.convert_column_definitions()?;

        // Create table in catalog
        ctx.catalog
            .create_table(self.statement.table.clone(), columns)?;

        // Create table in storage
        ctx.storage.create_table(&self.statement.table)?;

        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::types::{DataType, Value};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_db(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("{}_{}.db", prefix, nanos)
    }

    #[test]
    fn test_select_executor_debug() -> Result<()> {
        println!("=== Starting Select Executor Debug Test ===");

        let mut catalog = Schema::new();

        // Create test table
        let columns = vec![
            Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                DataType::Integer,
            )
            .primary_key(true),
            Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                DataType::Text,
            ),
        ];
        catalog.create_table("users".to_string(), columns)?;

        let path = tmp_db("_test_select_executor_debug");
        let _ = fs::remove_file(&path);
        let mut storage = StorageEngine::new(path.clone())?;
        // Create table in storage as well
        storage.create_table("users")?;

        // Add some test data
        println!("✓ Inserting row 1");
        storage.insert_into_table(
            "users",
            vec![Value::Integer(1), Value::Text("Alice".to_string())],
        )?;
        println!("✓ Inserting row 2");
        storage.insert_into_table(
            "users",
            vec![Value::Integer(2), Value::Text("Bob".to_string())],
        )?;
        println!("✓ Inserting row 3");
        storage.insert_into_table(
            "users",
            vec![Value::Integer(3), Value::Text("Charlie".to_string())],
        )?;

        // Debug: Read all rows directly from storage
        println!("✓ Reading all rows from storage...");
        let all_rows = storage.read_from_table("users")?;
        println!("✓ Found {} rows in storage", all_rows.len());
        for (i, row) in all_rows.iter().enumerate() {
            println!("✓ Row {}: {:?}", i, row);
        }

        let mut ctx = ExecutionContext::new(&catalog, &mut storage);

        let statement = SelectStatement {
            columns: vec![SelectItem::Column("id".to_string())],
            from: TableReference::Table("users".to_string()),
            where_clause: None,
        };

        let mut executor = SelectExecutor::new(statement);
        let result = executor.execute(&mut ctx)?;

        println!("✓ Query result columns: {:?}", result.columns);
        println!("✓ Query result rows: {}", result.rows.len());
        for (i, row) in result.rows.iter().enumerate() {
            println!("✓ Query row {}: {:?}", i, row);
        }

        assert_eq!(result.columns, vec!["id"]);
        assert_eq!(result.rows.len(), 3); // 3 simulated rows
        println!("✓ SUCCESS: Select executor test passed");
        storage.flush()?;
        let _ = fs::remove_file(&path);
        Ok(())
    }

    #[test]
    fn test_select_executor() -> Result<()> {
        let mut catalog = Schema::new();

        // Create test table
        let columns = vec![
            Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                DataType::Integer,
            )
            .primary_key(true),
            Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                DataType::Text,
            ),
        ];
        catalog.create_table("users".to_string(), columns)?;

        let path = tmp_db("_test_select_executor");
        let _ = fs::remove_file(&path);
        let mut storage = StorageEngine::new(path.clone())?;
        // Create table in storage as well
        storage.create_table("users")?;

        // Add some test data
        storage.insert_into_table(
            "users",
            vec![Value::Integer(1), Value::Text("Alice".to_string())],
        )?;
        storage.insert_into_table(
            "users",
            vec![Value::Integer(2), Value::Text("Bob".to_string())],
        )?;
        storage.insert_into_table(
            "users",
            vec![Value::Integer(3), Value::Text("Charlie".to_string())],
        )?;
        let mut ctx = ExecutionContext::new(&catalog, &mut storage);

        let statement = SelectStatement {
            columns: vec![SelectItem::Column("id".to_string())],
            from: TableReference::Table("users".to_string()),
            where_clause: None,
        };

        let mut executor = SelectExecutor::new(statement);
        let result = executor.execute(&mut ctx)?;

        assert_eq!(result.columns, vec!["id"]);
        assert_eq!(result.rows.len(), 3); // 3 simulated rows
        storage.flush()?;
        let _ = fs::remove_file(&path);
        Ok(())
    }

    #[test]
    fn test_insert_executor() -> Result<()> {
        let mut catalog = Schema::new();

        // Create test table
        let columns = vec![
            Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                DataType::Integer,
            )
            .primary_key(true),
            Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                DataType::Text,
            ),
        ];
        catalog.create_table("users".to_string(), columns)?;

        let path = tmp_db("_test_insert_executor");
        let _ = fs::remove_file(&path);
        let mut storage = StorageEngine::new(path.clone())?;
        // Create table in storage as well
        storage.create_table("users")?;
        let mut ctx = ExecutionContext::new(&catalog, &mut storage);

        let statement = InsertStatement {
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            values: vec![vec![
                Expression::Literal(Value::Integer(4)),
                Expression::Literal(Value::Text("Dave".to_string())),
            ]],
        };

        let mut executor = InsertExecutor::new(statement);
        let result = executor.execute(&mut ctx)?;

        assert!(result.columns.is_empty());
        assert!(result.rows.is_empty());
        storage.flush()?;
        let _ = fs::remove_file(&path);
        Ok(())
    }

    #[test]
    fn test_create_executor() -> Result<()> {
        let catalog = Schema::new();
        let path = tmp_db("_test_create_executor");
        let _ = fs::remove_file(&path);
        let mut storage = StorageEngine::new(path.clone())?;
        let mut ctx = ExecutionContext::new(&catalog, &mut storage);

        let statement = CreateStatement {
            table: "test_table".to_string(),
            columns: vec![ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                default_value: None,
            }],
        };

        let mut executor = CreateExecutor::new(statement);
        let result = executor.execute(&mut ctx)?;

        assert!(result.columns.is_empty());
        assert!(result.rows.is_empty());
        assert!(ctx.catalog.get_table_by_name("test_table").is_some());
        storage.flush()?;
        let _ = fs::remove_file(&path);
        Ok(())
    }
}
