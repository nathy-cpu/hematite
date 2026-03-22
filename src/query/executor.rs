//! Query execution engine for processing SQL statements

use crate::catalog::StoredRow;
use crate::catalog::{Column, DataType, Table, Value};
use crate::error::{HematiteError, Result};
use crate::parser::ast::*;
use crate::query::plan::{ExecutionProgram, QueryPlan, SelectAccessPath};
pub use crate::query::runtime::{ExecutionContext, QueryExecutor, QueryResult};
use std::cmp::Ordering;

impl QueryPlan {
    pub fn into_executor(self) -> Box<dyn QueryExecutor> {
        build_executor(self.program)
    }
}

pub fn build_executor(program: ExecutionProgram) -> Box<dyn QueryExecutor> {
    match program {
        ExecutionProgram::Select {
            statement,
            access_path,
        } => Box::new(SelectExecutor::new(statement, access_path)),
        ExecutionProgram::Insert { statement } => Box::new(InsertExecutor::new(statement)),
        ExecutionProgram::Update {
            statement,
            access_path,
        } => Box::new(UpdateExecutor::new(statement, access_path)),
        ExecutionProgram::Delete {
            statement,
            access_path,
        } => Box::new(DeleteExecutor::new(statement, access_path)),
        ExecutionProgram::Create { statement } => Box::new(CreateExecutor::new(statement)),
        ExecutionProgram::Drop { statement } => Box::new(DropExecutor::new(statement)),
    }
}

#[derive(Debug, Clone)]
pub struct SelectExecutor {
    pub statement: SelectStatement,
    pub access_path: SelectAccessPath,
}

impl SelectExecutor {
    pub fn new(statement: SelectStatement, access_path: SelectAccessPath) -> Self {
        Self {
            statement,
            access_path,
        }
    }

    fn evaluate_expression(
        &self,
        ctx: &ExecutionContext,
        expr: &Expression,
        row: &[Value],
    ) -> Result<Value> {
        match expr {
            Expression::Literal(value) => Ok(value.clone()),
            Expression::Parameter(index) => Err(HematiteError::ParseError(format!(
                "Unbound parameter {} reached execution",
                index + 1
            ))),
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
                SelectItem::CountAll => {}
                SelectItem::Aggregate { .. } => {}
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
                SelectItem::CountAll => columns.push("COUNT(*)".to_string()),
                SelectItem::Aggregate { function, column } => columns.push(format!(
                    "{}({})",
                    match function {
                        AggregateFunction::Sum => "SUM",
                        AggregateFunction::Avg => "AVG",
                        AggregateFunction::Min => "MIN",
                        AggregateFunction::Max => "MAX",
                    },
                    column
                )),
            }
        }

        columns
    }

    fn compare_sort_values(&self, left: &Value, right: &Value) -> Ordering {
        match (left.is_null(), right.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => left.partial_cmp(right).unwrap_or(Ordering::Equal),
        }
    }

    fn evaluate_aggregate(
        &self,
        ctx: &ExecutionContext,
        rows: &[Vec<Value>],
    ) -> Result<Option<Value>> {
        let Some(item) = self.statement.columns.first() else {
            return Ok(None);
        };

        match item {
            SelectItem::CountAll => Ok(Some(Value::Integer(rows.len() as i32))),
            SelectItem::Aggregate { function, column } => {
                let table_name = self
                    .get_table_name()
                    .ok_or_else(|| HematiteError::ParseError("Missing table name".to_string()))?;
                let table = ctx.catalog.get_table_by_name(table_name).ok_or_else(|| {
                    HematiteError::ParseError(format!("Table '{}' not found", table_name))
                })?;
                let index = table.get_column_index(column).ok_or_else(|| {
                    HematiteError::ParseError(format!(
                        "Column '{}' does not exist in table '{}'",
                        column, table_name
                    ))
                })?;

                let values: Vec<&Value> = rows
                    .iter()
                    .map(|row| &row[index])
                    .filter(|value| !value.is_null())
                    .collect();

                if values.is_empty() {
                    return Ok(Some(Value::Null));
                }

                match function {
                    AggregateFunction::Min => {
                        let mut current = values[0].clone();
                        for value in values.into_iter().skip(1) {
                            if value.partial_cmp(&current).is_some_and(|ord| ord.is_lt()) {
                                current = value.clone();
                            }
                        }
                        Ok(Some(current))
                    }
                    AggregateFunction::Max => {
                        let mut current = values[0].clone();
                        for value in values.into_iter().skip(1) {
                            if value.partial_cmp(&current).is_some_and(|ord| ord.is_gt()) {
                                current = value.clone();
                            }
                        }
                        Ok(Some(current))
                    }
                    AggregateFunction::Sum => {
                        let mut int_sum: i64 = 0;
                        let mut float_sum: f64 = 0.0;
                        let mut has_float = false;

                        for value in values {
                            match value {
                                Value::Integer(i) => {
                                    int_sum += *i as i64;
                                    float_sum += *i as f64;
                                }
                                Value::Float(f) => {
                                    has_float = true;
                                    float_sum += *f;
                                }
                                _ => {
                                    return Err(HematiteError::ParseError(format!(
                                        "SUM() requires numeric values, found {:?}",
                                        value
                                    )))
                                }
                            }
                        }

                        if has_float {
                            Ok(Some(Value::Float(float_sum)))
                        } else {
                            Ok(Some(Value::Integer(int_sum as i32)))
                        }
                    }
                    AggregateFunction::Avg => {
                        let mut sum: f64 = 0.0;
                        let count = values.len() as f64;

                        for value in values {
                            match value {
                                Value::Integer(i) => sum += *i as f64,
                                Value::Float(f) => sum += *f,
                                _ => {
                                    return Err(HematiteError::ParseError(format!(
                                        "AVG() requires numeric values, found {:?}",
                                        value
                                    )))
                                }
                            }
                        }

                        Ok(Some(Value::Float(sum / count)))
                    }
                }
            }
            _ => Ok(None),
        }
    }

    fn extract_primary_key_lookup(&self, table: &Table) -> Option<Vec<Value>> {
        if table.primary_key_count() != 1 {
            return None;
        }

        let where_clause = self.statement.where_clause.as_ref()?;
        if where_clause.conditions.len() != 1 {
            return None;
        }

        match &where_clause.conditions[0] {
            Condition::Comparison {
                left,
                operator: ComparisonOperator::Equal,
                right,
            } => match (left, right) {
                (Expression::Column(column_name), Expression::Literal(value))
                    if table
                        .primary_key_columns
                        .first()
                        .and_then(|index| table.columns.get(*index))
                        .is_some_and(|column| column.name == *column_name) =>
                {
                    Some(vec![value.clone()])
                }
                (Expression::Literal(value), Expression::Column(column_name))
                    if table
                        .primary_key_columns
                        .first()
                        .and_then(|index| table.columns.get(*index))
                        .is_some_and(|column| column.name == *column_name) =>
                {
                    Some(vec![value.clone()])
                }
                _ => None,
            },
            _ => None,
        }
    }

    fn extract_secondary_index_lookup(
        &self,
        table: &Table,
        index_name: &str,
    ) -> Option<Vec<Value>> {
        let index = table.get_secondary_index(index_name)?;
        if index.column_indices.len() != 1 {
            return None;
        }

        let where_clause = self.statement.where_clause.as_ref()?;
        if where_clause.conditions.len() != 1 {
            return None;
        }

        let indexed_column = table.columns.get(index.column_indices[0])?;
        match &where_clause.conditions[0] {
            Condition::Comparison {
                left,
                operator: ComparisonOperator::Equal,
                right,
            } => match (left, right) {
                (Expression::Column(column_name), Expression::Literal(value))
                    if indexed_column.name == *column_name =>
                {
                    Some(vec![value.clone()])
                }
                (Expression::Literal(value), Expression::Column(column_name))
                    if indexed_column.name == *column_name =>
                {
                    Some(vec![value.clone()])
                }
                _ => None,
            },
            _ => None,
        }
    }

    fn extract_rowid_lookup(&self) -> Option<u64> {
        let where_clause = self.statement.where_clause.as_ref()?;
        if where_clause.conditions.len() != 1 {
            return None;
        }

        match &where_clause.conditions[0] {
            Condition::Comparison {
                left,
                operator: ComparisonOperator::Equal,
                right,
            } => match (left, right) {
                (Expression::Column(column_name), Expression::Literal(Value::Integer(v)))
                    if column_name.eq_ignore_ascii_case("rowid") && *v >= 0 =>
                {
                    Some(*v as u64)
                }
                (Expression::Literal(Value::Integer(v)), Expression::Column(column_name))
                    if column_name.eq_ignore_ascii_case("rowid") && *v >= 0 =>
                {
                    Some(*v as u64)
                }
                _ => None,
            },
            _ => None,
        }
    }
}

impl QueryExecutor for SelectExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext) -> Result<QueryResult> {
        // Validate statement
        self.statement.validate(&ctx.catalog)?;

        let table_name = match &self.statement.from {
            TableReference::Table(name) => name.clone(),
        };

        let table = ctx.catalog.get_table_by_name(&table_name).ok_or_else(|| {
            HematiteError::ParseError(format!("Table '{}' not found", table_name))
        })?;

        let all_rows = match self.access_path {
            SelectAccessPath::RowIdLookup => {
                let rowid = self.extract_rowid_lookup().ok_or_else(|| {
                    HematiteError::InternalError(
                        "Planner selected rowid lookup without a matching predicate".to_string(),
                    )
                })?;
                ctx.engine
                    .lookup_row_by_rowid(&table_name, rowid)?
                    .map(|row| vec![row.values])
                    .unwrap_or_default()
            }
            SelectAccessPath::PrimaryKeyLookup => {
                let primary_key_values =
                    self.extract_primary_key_lookup(table).ok_or_else(|| {
                        HematiteError::InternalError(
                            "Planner selected primary-key lookup without a matching predicate"
                                .to_string(),
                        )
                    })?;
                let encoded_key = ctx.engine.encode_primary_key(&primary_key_values)?;
                let mut index_cursor = ctx.engine.open_primary_key_cursor(table)?;
                let rowid = index_cursor
                    .seek_key(&encoded_key)
                    .then(|| index_cursor.current().map(|entry| entry.row_id))
                    .flatten();
                match rowid {
                    Some(rowid) => {
                        let mut table_cursor = ctx.engine.open_table_cursor(&table_name)?;
                        table_cursor
                            .seek_rowid(rowid)
                            .then(|| table_cursor.current().map(|row| vec![row.values.clone()]))
                            .flatten()
                            .unwrap_or_default()
                    }
                    None => Vec::new(),
                }
            }
            SelectAccessPath::SecondaryIndexLookup(ref index_name) => {
                let key_values = self
                    .extract_secondary_index_lookup(table, index_name)
                    .ok_or_else(|| {
                        HematiteError::InternalError(format!(
                            "Planner selected secondary index lookup '{}' without a matching predicate",
                            index_name
                        ))
                    })?;
                let encoded_key = ctx.engine.encode_secondary_index_key(&key_values)?;
                let mut index_cursor = ctx.engine.open_secondary_index_cursor(table, index_name)?;
                let mut table_cursor = ctx.engine.open_table_cursor(&table_name)?;
                let mut rows = Vec::new();

                if index_cursor.seek_key(&encoded_key) {
                    loop {
                        let Some(entry) = index_cursor.current() else {
                            break;
                        };
                        if entry.key.as_slice() != encoded_key.as_slice() {
                            break;
                        }
                        if table_cursor.seek_rowid(entry.row_id) {
                            if let Some(row) = table_cursor.current() {
                                rows.push(row.values.clone());
                            }
                        }
                        if !index_cursor.next() {
                            break;
                        }
                    }
                }

                rows
            }
            SelectAccessPath::FullTableScan => ctx.engine.read_from_table(&table_name)?,
        };

        // Apply WHERE clause filtering.
        let mut filtered_rows = Vec::new();
        let skip_filter = matches!(self.access_path, SelectAccessPath::RowIdLookup);
        for row in &all_rows {
            let include = if skip_filter {
                true
            } else {
                match &self.statement.where_clause {
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
                }
            };

            if include {
                filtered_rows.push(row.clone());
            }
        }

        if !self.statement.order_by.is_empty() {
            filtered_rows.sort_by(|left, right| {
                for item in &self.statement.order_by {
                    let Some(index) = table.get_column_index(&item.column) else {
                        continue;
                    };

                    let ordering = self.compare_sort_values(&left[index], &right[index]);
                    if ordering != Ordering::Equal {
                        return match item.direction {
                            SortDirection::Asc => ordering,
                            SortDirection::Desc => ordering.reverse(),
                        };
                    }
                }

                Ordering::Equal
            });
        }

        if let Some(limit) = self.statement.limit {
            filtered_rows.truncate(limit);
        }

        // Aggregate scalar functions after filtering and ordering.
        if self
            .statement
            .columns
            .iter()
            .any(|item| matches!(item, SelectItem::CountAll | SelectItem::Aggregate { .. }))
        {
            let aggregate_value = self
                .evaluate_aggregate(ctx, &filtered_rows)?
                .unwrap_or(Value::Null);
            return Ok(QueryResult {
                affected_rows: 1,
                columns: self.get_column_names(ctx),
                rows: vec![vec![aggregate_value]],
            });
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
        ctx: &mut ExecutionContext,
        table: &Table,
        existing_rows: &[Vec<Value>],
        candidate_row: &[Value],
    ) -> Result<()> {
        let candidate_pk = table.get_primary_key_values(candidate_row).map_err(|err| {
            HematiteError::ParseError(format!("Failed to extract primary key values: {}", err))
        })?;

        if ctx
            .engine
            .lookup_row_by_primary_key(table, &candidate_pk)?
            .is_some()
        {
            return Err(HematiteError::ParseError(format!(
                "Duplicate primary key for table '{}': {:?}",
                table.name, candidate_pk
            )));
        }

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
            })?
            .clone();

        // Insert data into storage
        for value_row in &self.statement.values {
            let row_values = self.build_row(&table, value_row)?;
            self.ensure_primary_key_is_unique(ctx, &table, &[], &row_values)?;

            let row_id = ctx
                .engine
                .insert_into_table(&self.statement.table, row_values.clone())?;
            ctx.engine.register_primary_key_row(
                &table,
                StoredRow {
                    row_id,
                    values: row_values.clone(),
                },
            )?;
            ctx.engine.register_secondary_index_row(
                &table,
                StoredRow {
                    row_id,
                    values: row_values.clone(),
                },
            )?;
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
    pub access_path: SelectAccessPath,
}

impl UpdateExecutor {
    pub fn new(statement: UpdateStatement, access_path: SelectAccessPath) -> Self {
        Self {
            statement,
            access_path,
        }
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

    fn ensure_updated_primary_keys_remain_unique(
        &self,
        ctx: &mut ExecutionContext<'_>,
        table: &Table,
        updated_rows: &[StoredRow],
    ) -> Result<()> {
        self.ensure_primary_keys_unique(
            table,
            &updated_rows
                .iter()
                .map(|row| row.values.clone())
                .collect::<Vec<_>>(),
        )?;

        for row in updated_rows {
            let candidate_pk = table.get_primary_key_values(&row.values).map_err(|err| {
                HematiteError::ParseError(format!("Failed to extract primary key values: {}", err))
            })?;
            if let Some(existing_rowid) =
                ctx.engine.lookup_primary_key_rowid(table, &candidate_pk)?
            {
                if existing_rowid != row.row_id
                    && !updated_rows
                        .iter()
                        .any(|updated_row| updated_row.row_id == existing_rowid)
                {
                    return Err(HematiteError::ParseError(format!(
                        "Duplicate primary key for table '{}': {:?}",
                        table.name, candidate_pk
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
            })?
            .clone();

        let select_executor = SelectExecutor::new(
            SelectStatement {
                columns: vec![SelectItem::Wildcard],
                from: TableReference::Table(self.statement.table.clone()),
                where_clause: self.statement.where_clause.clone(),
                order_by: Vec::new(),
                limit: None,
            },
            self.access_path.clone(),
        );

        let rows_to_update = locate_rows_for_access_path(
            ctx,
            &table,
            &self.statement.table,
            &self.access_path,
            &select_executor,
        )?;
        let mut updated_rows_data = Vec::with_capacity(rows_to_update.len());
        let mut updated_rows = 0usize;

        for stored_row in rows_to_update {
            let mut updated_row = stored_row.values.clone();
            for assignment in &self.statement.assignments {
                let column_index = table.get_column_index(&assignment.column).ok_or_else(|| {
                    HematiteError::ParseError(format!(
                        "Column '{}' does not exist in table '{}'",
                        assignment.column, self.statement.table
                    ))
                })?;
                let column = &table.columns[column_index];
                let value =
                    select_executor.evaluate_expression(ctx, &assignment.value, &updated_row)?;
                updated_row[column_index] = self.coerce_assignment_value(column, value)?;
            }

            table
                .validate_row(&updated_row)
                .map_err(|err| HematiteError::ParseError(err.to_string()))?;
            updated_rows_data.push(StoredRow {
                row_id: stored_row.row_id,
                values: updated_row,
            });
            updated_rows += 1;
        }

        self.ensure_updated_primary_keys_remain_unique(ctx, &table, &updated_rows_data)?;

        for original_row in &updated_rows_data {
            if let Some(existing_row) = ctx
                .engine
                .lookup_row_by_rowid(&self.statement.table, original_row.row_id)?
            {
                ctx.engine
                    .delete_secondary_index_row(&table, &existing_row)?;
                let deleted_pk = ctx.engine.delete_primary_key_row(&table, &existing_row)?;
                if !deleted_pk {
                    return Err(HematiteError::CorruptedData(format!(
                        "Primary-key index entry vanished during update execution for table '{}'",
                        self.statement.table
                    )));
                }
                let deleted = ctx
                    .engine
                    .delete_from_table_by_rowid(&self.statement.table, existing_row.row_id)?;
                if !deleted {
                    return Err(HematiteError::CorruptedData(format!(
                        "Rowid {} vanished during update execution for table '{}'",
                        existing_row.row_id, self.statement.table
                    )));
                }
            }
        }

        for row in updated_rows_data {
            ctx.engine
                .insert_row_with_rowid(&self.statement.table, row.clone())?;
            ctx.engine.register_primary_key_row(&table, row.clone())?;
            ctx.engine.register_secondary_index_row(&table, row)?;
        }

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
    pub access_path: SelectAccessPath,
}

impl DeleteExecutor {
    pub fn new(statement: DeleteStatement, access_path: SelectAccessPath) -> Self {
        Self {
            statement,
            access_path,
        }
    }
}

fn locate_rowids_for_access_path(
    ctx: &mut ExecutionContext<'_>,
    table: &Table,
    table_name: &str,
    access_path: &SelectAccessPath,
    select_executor: &SelectExecutor,
) -> Result<Vec<u64>> {
    match access_path {
        SelectAccessPath::RowIdLookup => {
            Ok(select_executor.extract_rowid_lookup().into_iter().collect())
        }
        SelectAccessPath::PrimaryKeyLookup => {
            let Some(primary_key_values) = select_executor.extract_primary_key_lookup(table) else {
                return Ok(Vec::new());
            };
            let encoded_key = ctx.engine.encode_primary_key(&primary_key_values)?;
            let mut index_cursor = ctx.engine.open_primary_key_cursor(table)?;
            Ok(index_cursor
                .seek_key(&encoded_key)
                .then(|| index_cursor.current().map(|entry| entry.row_id))
                .flatten()
                .into_iter()
                .collect())
        }
        SelectAccessPath::SecondaryIndexLookup(index_name) => {
            let Some(key_values) =
                select_executor.extract_secondary_index_lookup(table, index_name)
            else {
                return Ok(Vec::new());
            };
            let encoded_key = ctx.engine.encode_secondary_index_key(&key_values)?;
            let mut index_cursor = ctx.engine.open_secondary_index_cursor(table, index_name)?;
            let mut rowids = Vec::new();

            if index_cursor.seek_key(&encoded_key) {
                loop {
                    let Some(entry) = index_cursor.current() else {
                        break;
                    };
                    if entry.key.as_slice() != encoded_key.as_slice() {
                        break;
                    }
                    rowids.push(entry.row_id);
                    if !index_cursor.next() {
                        break;
                    }
                }
            }

            Ok(rowids)
        }
        SelectAccessPath::FullTableScan => {
            let mut table_cursor = ctx.engine.open_table_cursor(table_name)?;
            let mut rowids = Vec::new();
            if table_cursor.first() {
                loop {
                    if let Some(row) = table_cursor.current() {
                        rowids.push(row.row_id);
                    }
                    if !table_cursor.next() {
                        break;
                    }
                }
            }
            Ok(rowids)
        }
    }
}

fn locate_rows_for_access_path(
    ctx: &mut ExecutionContext<'_>,
    table: &Table,
    table_name: &str,
    access_path: &SelectAccessPath,
    select_executor: &SelectExecutor,
) -> Result<Vec<StoredRow>> {
    let rowids =
        locate_rowids_for_access_path(ctx, table, table_name, access_path, select_executor)?;
    let mut table_cursor = ctx.engine.open_table_cursor(table_name)?;
    let mut rows = Vec::new();

    for rowid in rowids {
        if table_cursor.seek_rowid(rowid) {
            if let Some(row) = table_cursor.current() {
                let row = row.clone();
                let include = match &select_executor.statement.where_clause {
                    Some(where_clause) => {
                        let mut matches_where = true;
                        for condition in &where_clause.conditions {
                            if select_executor.evaluate_condition(ctx, condition, &row.values)?
                                != Some(true)
                            {
                                matches_where = false;
                                break;
                            }
                        }
                        matches_where
                    }
                    None => true,
                };

                if include {
                    rows.push(row);
                }
            }
        }
    }

    Ok(rows)
}

impl QueryExecutor for DeleteExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext) -> Result<QueryResult> {
        self.statement.validate(&ctx.catalog)?;

        let table = ctx
            .catalog
            .get_table_by_name(&self.statement.table)
            .ok_or_else(|| {
                HematiteError::ParseError(format!("Table '{}' not found", self.statement.table))
            })?
            .clone();

        let select_executor = SelectExecutor::new(
            SelectStatement {
                columns: vec![SelectItem::Wildcard],
                from: TableReference::Table(self.statement.table.clone()),
                where_clause: self.statement.where_clause.clone(),
                order_by: Vec::new(),
                limit: None,
            },
            self.access_path.clone(),
        );

        let rows_to_delete = locate_rows_for_access_path(
            ctx,
            &table,
            &self.statement.table,
            &self.access_path,
            &select_executor,
        )?;

        for row in &rows_to_delete {
            ctx.engine.delete_secondary_index_row(&table, row)?;
            let deleted_pk = ctx.engine.delete_primary_key_row(&table, row)?;
            if !deleted_pk {
                return Err(HematiteError::CorruptedData(format!(
                    "Primary-key index entry vanished during delete execution for table '{}'",
                    self.statement.table
                )));
            }
            let deleted = ctx
                .engine
                .delete_from_table_by_rowid(&self.statement.table, row.row_id)?;
            if !deleted {
                return Err(HematiteError::CorruptedData(format!(
                    "Rowid {} vanished during delete execution for table '{}'",
                    row.row_id, self.statement.table
                )));
            }
        }

        Ok(QueryResult {
            affected_rows: rows_to_delete.len(),
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

        // Create storage structures first so the catalog only persists the final roots once.
        let root_page_id = ctx.engine.create_table(&self.statement.table)?;
        let primary_key_root_page_id = ctx.engine.create_empty_btree()?;
        ctx.catalog.create_table_with_roots(
            self.statement.table.clone(),
            columns,
            root_page_id,
            primary_key_root_page_id,
        )?;

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

        ctx.engine.drop_table_with_indexes(&table)?;
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
