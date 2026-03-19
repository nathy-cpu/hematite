//! Query execution engine for processing SQL statements

use crate::catalog::{Column, DataType, Schema, Table, Value};
use crate::error::{HematiteError, Result};
use crate::parser::ast::*;
use crate::storage::StorageEngine;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub affected_rows: usize,
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

    fn compare_values(
        &self,
        left_val: &Value,
        operator: &ComparisonOperator,
        right_val: &Value,
    ) -> Option<bool> {
        if left_val.is_null() || right_val.is_null() {
            return None;
        }

        match operator {
            ComparisonOperator::Equal => Some(left_val == right_val),
            ComparisonOperator::NotEqual => Some(left_val != right_val),
            ComparisonOperator::LessThan => left_val.partial_cmp(right_val).map(|ord| ord.is_lt()),
            ComparisonOperator::LessThanOrEqual => {
                left_val.partial_cmp(right_val).map(|ord| ord.is_le())
            }
            ComparisonOperator::GreaterThan => {
                left_val.partial_cmp(right_val).map(|ord| ord.is_gt())
            }
            ComparisonOperator::GreaterThanOrEqual => {
                left_val.partial_cmp(right_val).map(|ord| ord.is_ge())
            }
        }
    }

    fn logical_and(&self, left: Option<bool>, right: Option<bool>) -> Option<bool> {
        match (left, right) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        }
    }

    fn logical_or(&self, left: Option<bool>, right: Option<bool>) -> Option<bool> {
        match (left, right) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        }
    }

    pub(crate) fn evaluate_condition(
        &self,
        ctx: &ExecutionContext,
        condition: &Condition,
        row: &[Value],
    ) -> Result<Option<bool>> {
        match condition {
            Condition::Comparison {
                left,
                operator,
                right,
            } => {
                let left_val = self.evaluate_expression(ctx, left, row)?;
                let right_val = self.evaluate_expression(ctx, right, row)?;
                Ok(self.compare_values(&left_val, operator, &right_val))
            }
            Condition::NullCheck { expr, is_not } => {
                let value = self.evaluate_expression(ctx, expr, row)?;
                let is_null = value.is_null();
                Ok(Some(if *is_not { !is_null } else { is_null }))
            }
            Condition::Logical {
                left,
                operator,
                right,
            } => {
                let left_result = self.evaluate_condition(ctx, left, row)?;
                let right_result = self.evaluate_condition(ctx, right, row)?;

                match operator {
                    LogicalOperator::And => Ok(self.logical_and(left_result, right_result)),
                    LogicalOperator::Or => Ok(self.logical_or(left_result, right_result)),
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
                        if self.evaluate_condition(ctx, condition, row)? != Some(true) {
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
            affected_rows: projected_rows.len(),
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

    fn ensure_primary_key_is_unique(
        &self,
        table: &Table,
        existing_rows: &[Vec<Value>],
        candidate_row: &[Value],
    ) -> Result<()> {
        let candidate_pk = table.get_primary_key_values(candidate_row).map_err(|err| {
            HematiteError::ParseError(format!("Failed to extract primary key values: {}", err))
        })?;

        for existing_row in existing_rows {
            let existing_pk = table.get_primary_key_values(existing_row).map_err(|err| {
                HematiteError::ParseError(format!("Failed to extract primary key values: {}", err))
            })?;

            if existing_pk == candidate_pk {
                return Err(HematiteError::ParseError(format!(
                    "Duplicate primary key for table '{}': {:?}",
                    table.name, candidate_pk
                )));
            }
        }

        Ok(())
    }

    fn build_row(&self, table: &Table, value_row: &[Expression]) -> Result<Vec<Value>> {
        let mut row = Vec::with_capacity(table.columns.len());

        for column in &table.columns {
            let value = if let Some(position) = self
                .statement
                .columns
                .iter()
                .position(|name| name == &column.name)
            {
                let expr = value_row.get(position).ok_or_else(|| {
                    HematiteError::ParseError(format!("Missing value for column '{}'", column.name))
                })?;
                let literal = match expr {
                    Expression::Literal(value) => value.clone(),
                    _ => {
                        return Err(HematiteError::ParseError(format!(
                            "Only literal values are supported in INSERT, column '{}'",
                            column.name
                        )));
                    }
                };

                match (&column.data_type, literal) {
                    (DataType::Integer, Value::Integer(i)) => Value::Integer(i),
                    (DataType::Text, Value::Text(s)) => Value::Text(s),
                    (DataType::Boolean, Value::Boolean(b)) => Value::Boolean(b),
                    (DataType::Float, Value::Float(f)) => Value::Float(f),
                    (DataType::Float, Value::Integer(i)) => Value::Float(i as f64),
                    (_, Value::Null) if column.nullable => Value::Null,
                    (_, Value::Null) => {
                        return Err(HematiteError::ParseError(format!(
                            "Column '{}' cannot be NULL",
                            column.name
                        )));
                    }
                    (_, value) => {
                        return Err(HematiteError::ParseError(format!(
                            "Type mismatch: column '{}' expects {:?}, got {:?}",
                            column.name, column.data_type, value
                        )));
                    }
                }
            } else if let Some(default_value) = &column.default_value {
                default_value.clone()
            } else if column.nullable {
                Value::Null
            } else {
                return Err(HematiteError::ParseError(format!(
                    "Missing value for required column '{}'",
                    column.name
                )));
            };

            row.push(value);
        }

        table
            .validate_row(&row)
            .map_err(|err| HematiteError::ParseError(err.to_string()))?;

        Ok(row)
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

        let mut existing_rows = ctx.storage.read_from_table(&self.statement.table)?;

        // Insert data into storage
        for value_row in &self.statement.values {
            let row_values = self.build_row(table, value_row)?;
            self.ensure_primary_key_is_unique(table, &existing_rows, &row_values)?;

            ctx.storage
                .insert_into_table(&self.statement.table, row_values.clone())?;
            existing_rows.push(row_values);
        }

        Ok(QueryResult {
            affected_rows: self.statement.values.len(),
            columns: vec![],
            rows: vec![],
        })
    }
}

#[derive(Debug, Clone)]
pub struct UpdateExecutor {
    pub statement: UpdateStatement,
}

impl UpdateExecutor {
    pub fn new(statement: UpdateStatement) -> Self {
        Self { statement }
    }

    fn coerce_assignment_value(
        &self,
        column: &crate::catalog::Column,
        value: Value,
    ) -> Result<Value> {
        match (&column.data_type, value) {
            (DataType::Integer, Value::Integer(i)) => Ok(Value::Integer(i)),
            (DataType::Text, Value::Text(s)) => Ok(Value::Text(s)),
            (DataType::Boolean, Value::Boolean(b)) => Ok(Value::Boolean(b)),
            (DataType::Float, Value::Float(f)) => Ok(Value::Float(f)),
            (DataType::Float, Value::Integer(i)) => Ok(Value::Float(i as f64)),
            (_, Value::Null) if column.nullable => Ok(Value::Null),
            (_, Value::Null) => Err(HematiteError::ParseError(format!(
                "Column '{}' cannot be NULL",
                column.name
            ))),
            (_, value) => Err(HematiteError::ParseError(format!(
                "Type mismatch: column '{}' expects {:?}, got {:?}",
                column.name, column.data_type, value
            ))),
        }
    }

    fn ensure_primary_keys_unique(&self, table: &Table, rows: &[Vec<Value>]) -> Result<()> {
        for i in 0..rows.len() {
            let left = table.get_primary_key_values(&rows[i]).map_err(|err| {
                HematiteError::ParseError(format!("Failed to extract primary key values: {}", err))
            })?;
            for right_row in rows.iter().skip(i + 1) {
                let right = table.get_primary_key_values(right_row).map_err(|err| {
                    HematiteError::ParseError(format!(
                        "Failed to extract primary key values: {}",
                        err
                    ))
                })?;
                if left == right {
                    return Err(HematiteError::ParseError(format!(
                        "Duplicate primary key for table '{}': {:?}",
                        table.name, left
                    )));
                }
            }
        }

        Ok(())
    }
}

impl QueryExecutor for UpdateExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext<'_>) -> Result<QueryResult> {
        self.statement.validate(&ctx.catalog)?;

        let table = ctx
            .catalog
            .get_table_by_name(&self.statement.table)
            .ok_or_else(|| {
                HematiteError::ParseError(format!("Table '{}' not found", self.statement.table))
            })?;

        let all_rows = ctx.storage.read_from_table(&self.statement.table)?;
        let select_executor = SelectExecutor::new(SelectStatement {
            columns: vec![SelectItem::Wildcard],
            from: TableReference::Table(self.statement.table.clone()),
            where_clause: self.statement.where_clause.clone(),
        });

        let mut rewritten_rows = Vec::with_capacity(all_rows.len());
        let mut updated_rows = 0usize;

        for row in all_rows {
            let should_update = match &self.statement.where_clause {
                Some(where_clause) => {
                    let mut matches_where = true;
                    for condition in &where_clause.conditions {
                        if select_executor.evaluate_condition(ctx, condition, &row)? != Some(true) {
                            matches_where = false;
                            break;
                        }
                    }
                    matches_where
                }
                None => true,
            };

            if should_update {
                let mut updated_row = row.clone();
                for assignment in &self.statement.assignments {
                    let column_index =
                        table.get_column_index(&assignment.column).ok_or_else(|| {
                            HematiteError::ParseError(format!(
                                "Column '{}' does not exist in table '{}'",
                                assignment.column, self.statement.table
                            ))
                        })?;
                    let column = &table.columns[column_index];
                    let value = select_executor.evaluate_expression(
                        ctx,
                        &assignment.value,
                        &updated_row,
                    )?;
                    updated_row[column_index] = self.coerce_assignment_value(column, value)?;
                }

                table
                    .validate_row(&updated_row)
                    .map_err(|err| HematiteError::ParseError(err.to_string()))?;
                rewritten_rows.push(updated_row);
                updated_rows += 1;
            } else {
                rewritten_rows.push(row);
            }
        }

        self.ensure_primary_keys_unique(table, &rewritten_rows)?;
        ctx.storage
            .replace_table_rows(&self.statement.table, rewritten_rows)?;

        Ok(QueryResult {
            affected_rows: updated_rows,
            columns: Vec::new(),
            rows: Vec::new(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct DeleteExecutor {
    pub statement: DeleteStatement,
}

impl DeleteExecutor {
    pub fn new(statement: DeleteStatement) -> Self {
        Self { statement }
    }
}

impl QueryExecutor for DeleteExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext) -> Result<QueryResult> {
        self.statement.validate(&ctx.catalog)?;

        let table = ctx
            .catalog
            .get_table_by_name(&self.statement.table)
            .ok_or_else(|| {
                HematiteError::ParseError(format!("Table '{}' not found", self.statement.table))
            })?;

        let all_rows = ctx.storage.read_from_table(&self.statement.table)?;
        let select_executor = SelectExecutor::new(SelectStatement {
            columns: vec![SelectItem::Wildcard],
            from: TableReference::Table(self.statement.table.clone()),
            where_clause: self.statement.where_clause.clone(),
        });

        let mut survivors = Vec::new();
        let mut deleted_rows = 0usize;
        for row in all_rows {
            let delete_row = match &self.statement.where_clause {
                Some(where_clause) => {
                    let mut matches_where = true;
                    for condition in &where_clause.conditions {
                        if select_executor.evaluate_condition(ctx, condition, &row)? != Some(true) {
                            matches_where = false;
                            break;
                        }
                    }
                    matches_where
                }
                None => true,
            };

            if delete_row {
                deleted_rows += 1;
            } else {
                survivors.push(row);
            }
        }

        let _ = table;
        ctx.storage
            .replace_table_rows(&self.statement.table, survivors)?;

        Ok(QueryResult {
            affected_rows: deleted_rows,
            columns: Vec::new(),
            rows: Vec::new(),
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
        let table_id = ctx
            .catalog
            .create_table(self.statement.table.clone(), columns)?;

        // Create table in storage
        let root_page_id = ctx.storage.create_table(&self.statement.table)?;
        ctx.catalog.set_table_root_page(table_id, root_page_id)?;

        Ok(QueryResult {
            affected_rows: 0,
            columns: Vec::new(),
            rows: Vec::new(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct DropExecutor {
    pub statement: DropStatement,
}

impl DropExecutor {
    pub fn new(statement: DropStatement) -> Self {
        Self { statement }
    }
}

impl QueryExecutor for DropExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext<'_>) -> Result<QueryResult> {
        self.statement.validate(&ctx.catalog)?;

        let table = ctx
            .catalog
            .get_table_by_name(&self.statement.table)
            .ok_or_else(|| {
                HematiteError::ParseError(format!("Table '{}' not found", self.statement.table))
            })?
            .clone();

        ctx.storage.drop_table(&self.statement.table)?;
        ctx.catalog
            .drop_table(table.id)
            .map_err(|err| HematiteError::ParseError(err.to_string()))?;

        Ok(QueryResult {
            affected_rows: 0,
            columns: Vec::new(),
            rows: Vec::new(),
        })
    }
}
