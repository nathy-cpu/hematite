//! Query execution.
//!
//! The executor turns a planned access path into concrete catalog operations.
//!
//! ```text
//! parsed statement
//!      +
//! chosen access path
//!      |
//!      v
//!   executor node
//!      |
//!      +--> table cursor scan
//!      +--> rowid lookup
//!      +--> PK index -> rowid -> table lookup
//!      +--> secondary index -> rowid set -> table lookup
//! ```
//!
//! This layer should stay storage-agnostic at the page level. If a change here requires knowledge
//! of B-tree node layout or pager internals, the boundary below catalog has started to leak.

use crate::catalog::table::{CheckConstraint, ForeignKeyConstraint};
use crate::catalog::StoredRow;
use crate::catalog::{Column, DataType, Table, Value};
use crate::error::{HematiteError, Result};
use crate::parser::ast::*;
use crate::query::plan::{ExecutionProgram, QueryPlan, SelectAccessPath};
pub use crate::query::runtime::{ExecutionContext, QueryExecutor, QueryResult};
use crate::query::QueryPlanner;
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
        ExecutionProgram::CreateIndex { statement } => {
            Box::new(CreateIndexExecutor::new(statement))
        }
        ExecutionProgram::Alter { statement } => Box::new(AlterExecutor::new(statement)),
        ExecutionProgram::Drop { statement } => Box::new(DropExecutor::new(statement)),
        ExecutionProgram::DropIndex { statement } => Box::new(DropIndexExecutor::new(statement)),
    }
}

#[derive(Debug, Clone)]
pub struct SelectExecutor {
    pub statement: SelectStatement,
    pub access_path: SelectAccessPath,
}

#[derive(Debug, Clone)]
struct ResolvedSource {
    name: String,
    columns: Vec<String>,
    alias: Option<String>,
    offset: usize,
}

impl ResolvedSource {
    fn width(&self) -> usize {
        self.columns.len()
    }
}

#[derive(Debug, Clone)]
struct GroupedRow {
    projected: Vec<Value>,
    source_rows: Vec<Vec<Value>>,
}

impl SelectExecutor {
    pub fn new(statement: SelectStatement, access_path: SelectAccessPath) -> Self {
        Self {
            statement,
            access_path,
        }
    }

    fn resolve_sources(&self, ctx: &ExecutionContext) -> Result<Vec<ResolvedSource>> {
        let bindings = SelectStatement::collect_table_bindings(&self.statement.from);
        let mut sources = Vec::with_capacity(bindings.len());
        let mut offset = 0usize;

        for binding in bindings {
            sources.push(self.resolve_named_source(
                ctx,
                &binding.table_name,
                binding.alias,
                offset,
            )?);
            offset += sources.last().map(ResolvedSource::width).unwrap_or(0);
        }

        Ok(sources)
    }

    fn query_output_columns(&self, query: &SelectStatement) -> Vec<String> {
        query
            .columns
            .iter()
            .enumerate()
            .map(|(index, _)| query.output_name(index).unwrap_or_else(|| "*".to_string()))
            .collect()
    }

    fn resolve_column_index(
        &self,
        sources: &[ResolvedSource],
        column_reference: &str,
    ) -> Result<Option<usize>> {
        let (qualifier, column_name) = SelectStatement::split_column_reference(column_reference);
        let mut matches = Vec::new();

        for source in sources {
            if let Some(qualifier) = qualifier {
                if qualifier != source.name
                    && source
                        .alias
                        .as_deref()
                        .is_none_or(|alias| alias != qualifier)
                {
                    continue;
                }
            }

            if let Some(index) = source
                .columns
                .iter()
                .position(|column| column == column_name)
            {
                matches.push(source.offset + index);
            }
        }

        match matches.len() {
            0 => Ok(None),
            1 => Ok(matches.into_iter().next()),
            _ => Err(HematiteError::ParseError(format!(
                "Column reference '{}' is ambiguous",
                column_reference
            ))),
        }
    }

    fn evaluate_expression(
        &self,
        ctx: &ExecutionContext,
        sources: &[ResolvedSource],
        expr: &Expression,
        row: &[Value],
    ) -> Result<Value> {
        match expr {
            Expression::Literal(value) => Ok(value.clone()),
            Expression::Parameter(index) => Err(HematiteError::ParseError(format!(
                "Unbound parameter {} reached execution",
                index + 1
            ))),
            Expression::AggregateCall { .. } => Err(HematiteError::ParseError(
                "Aggregate expressions can only be evaluated in grouped query contexts".to_string(),
            )),
            Expression::Column(name) => self
                .resolve_column_index(sources, name)?
                .and_then(|index| row.get(index).cloned())
                .ok_or_else(|| HematiteError::ParseError(format!("Column '{}' not found", name))),
            Expression::UnaryMinus(expr) => match self
                .evaluate_expression(ctx, sources, expr, row)?
            {
                Value::Integer(value) => value.checked_neg().map(Value::Integer).ok_or_else(|| {
                    HematiteError::ParseError(
                        "Integer overflow while evaluating unary '-'".to_string(),
                    )
                }),
                Value::Float(value) => Ok(Value::Float(-value)),
                Value::Null => Ok(Value::Null),
                value => Err(HematiteError::ParseError(format!(
                    "Unary '-' requires a numeric value, found {:?}",
                    value
                ))),
            },
            Expression::Binary {
                left,
                operator,
                right,
            } => {
                let left = self.evaluate_expression(ctx, sources, left, row)?;
                let right = self.evaluate_expression(ctx, sources, right, row)?;
                self.evaluate_arithmetic(operator, left, right)
            }
        }
    }

    fn evaluate_arithmetic(
        &self,
        operator: &ArithmeticOperator,
        left: Value,
        right: Value,
    ) -> Result<Value> {
        evaluate_arithmetic_values(operator, left, right)
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

    fn like_matches(pattern: &str, text: &str) -> bool {
        fn matches(pattern: &[char], text: &[char]) -> bool {
            if pattern.is_empty() {
                return text.is_empty();
            }

            match pattern[0] {
                '%' => {
                    if matches(&pattern[1..], text) {
                        return true;
                    }
                    for index in 0..text.len() {
                        if matches(&pattern[1..], &text[index + 1..]) {
                            return true;
                        }
                    }
                    false
                }
                '_' => !text.is_empty() && matches(&pattern[1..], &text[1..]),
                ch => !text.is_empty() && text[0] == ch && matches(&pattern[1..], &text[1..]),
            }
        }

        let pattern_chars: Vec<char> = pattern.chars().collect();
        let text_chars: Vec<char> = text.chars().collect();
        matches(&pattern_chars, &text_chars)
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

    fn evaluate_in_candidates(
        &self,
        probe: Value,
        candidates: impl IntoIterator<Item = Value>,
        is_not: bool,
    ) -> Option<bool> {
        if probe.is_null() {
            return None;
        }

        let mut matched = false;
        let mut saw_null = false;
        for candidate in candidates {
            if candidate.is_null() {
                saw_null = true;
                continue;
            }
            if candidate == probe {
                matched = true;
                break;
            }
        }

        if matched {
            Some(!is_not)
        } else if saw_null {
            None
        } else {
            Some(is_not)
        }
    }

    fn execute_subquery(
        &self,
        ctx: &mut ExecutionContext<'_>,
        subquery: &SelectStatement,
    ) -> Result<QueryResult> {
        let planner = QueryPlanner::new(ctx.catalog.clone());
        let plan = planner.plan(Statement::Select(subquery.clone()))?;
        let mut executor = plan.into_executor();
        executor.execute(ctx)
    }

    fn evaluate_condition(
        &self,
        ctx: &mut ExecutionContext<'_>,
        sources: &[ResolvedSource],
        condition: &Condition,
        row: &[Value],
    ) -> Result<Option<bool>> {
        match condition {
            Condition::Comparison {
                left,
                operator,
                right,
            } => {
                let left_val = self.evaluate_expression(ctx, sources, left, row)?;
                let right_val = self.evaluate_expression(ctx, sources, right, row)?;
                Ok(self.compare_values(&left_val, operator, &right_val))
            }
            Condition::InList {
                expr,
                values,
                is_not,
            } => {
                let probe = self.evaluate_expression(ctx, sources, expr, row)?;
                let candidates = values
                    .iter()
                    .map(|value_expr| self.evaluate_expression(ctx, sources, value_expr, row))
                    .collect::<Result<Vec<_>>>()?;
                Ok(self.evaluate_in_candidates(probe, candidates, *is_not))
            }
            Condition::InSubquery {
                expr,
                subquery,
                is_not,
            } => {
                let probe = self.evaluate_expression(ctx, sources, expr, row)?;
                let subquery_result = self.execute_subquery(ctx, subquery)?;
                let candidates = subquery_result
                    .rows
                    .into_iter()
                    .map(|row| row.into_iter().next().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                Ok(self.evaluate_in_candidates(probe, candidates, *is_not))
            }
            Condition::Between {
                expr,
                lower,
                upper,
                is_not,
            } => {
                let value = self.evaluate_expression(ctx, sources, expr, row)?;
                let lower_value = self.evaluate_expression(ctx, sources, lower, row)?;
                let upper_value = self.evaluate_expression(ctx, sources, upper, row)?;

                if value.is_null() || lower_value.is_null() || upper_value.is_null() {
                    return Ok(None);
                }

                let lower_ok = value
                    .partial_cmp(&lower_value)
                    .map(|ordering| !ordering.is_lt());
                let upper_ok = value
                    .partial_cmp(&upper_value)
                    .map(|ordering| !ordering.is_gt());

                match (lower_ok, upper_ok) {
                    (Some(true), Some(true)) => Ok(Some(!is_not)),
                    (Some(_), Some(_)) => Ok(Some(*is_not)),
                    _ => Ok(None),
                }
            }
            Condition::Like {
                expr,
                pattern,
                is_not,
            } => {
                let value = self.evaluate_expression(ctx, sources, expr, row)?;
                let pattern_value = self.evaluate_expression(ctx, sources, pattern, row)?;

                match (value, pattern_value) {
                    (Value::Text(text), Value::Text(pattern)) => {
                        let matched = Self::like_matches(&pattern, &text);
                        Ok(Some(if *is_not { !matched } else { matched }))
                    }
                    (left, right) if left.is_null() || right.is_null() => Ok(None),
                    _ => Ok(None),
                }
            }
            Condition::Exists { subquery, is_not } => {
                let subquery_result = self.execute_subquery(ctx, subquery)?;
                let exists = !subquery_result.rows.is_empty();
                Ok(Some(if *is_not { !exists } else { exists }))
            }
            Condition::NullCheck { expr, is_not } => {
                let value = self.evaluate_expression(ctx, sources, expr, row)?;
                let is_null = value.is_null();
                Ok(Some(if *is_not { !is_null } else { is_null }))
            }
            Condition::Not(condition) => Ok(self
                .evaluate_condition(ctx, sources, condition, row)?
                .map(|value| !value)),
            Condition::Logical {
                left,
                operator,
                right,
            } => {
                let left_result = self.evaluate_condition(ctx, sources, left, row)?;
                let right_result = self.evaluate_condition(ctx, sources, right, row)?;

                match operator {
                    LogicalOperator::And => Ok(self.logical_and(left_result, right_result)),
                    LogicalOperator::Or => Ok(self.logical_or(left_result, right_result)),
                }
            }
        }
    }

    fn project_row(
        &self,
        ctx: &ExecutionContext,
        sources: &[ResolvedSource],
        row: &[Value],
    ) -> Result<Vec<Value>> {
        let mut projected = Vec::new();

        for item in &self.statement.columns {
            match item {
                SelectItem::Wildcard => projected.extend(row.iter().cloned()),
                SelectItem::Column(name) => {
                    if let Some(index) = self.resolve_column_index(sources, name)? {
                        if index < row.len() {
                            projected.push(row[index].clone());
                        }
                    }
                }
                SelectItem::Expression(expr) => {
                    projected.push(self.evaluate_expression(ctx, sources, expr, row)?);
                }
                SelectItem::CountAll => {}
                SelectItem::Aggregate { .. } => {}
            }
        }

        Ok(projected)
    }

    fn get_column_names(&self, sources: &[ResolvedSource]) -> Vec<String> {
        let mut columns = Vec::new();

        for (index, item) in self.statement.columns.iter().enumerate() {
            match item {
                SelectItem::Wildcard => {
                    for source in sources {
                        for column in &source.columns {
                            columns.push(column.clone());
                        }
                    }
                }
                _ => {
                    if let Some(name) = self.statement.output_name(index) {
                        columns.push(name);
                    }
                }
            }
        }

        columns
    }

    fn shifted_sources(
        &self,
        mut sources: Vec<ResolvedSource>,
        offset: usize,
    ) -> Vec<ResolvedSource> {
        for source in &mut sources {
            source.offset += offset;
        }
        sources
    }

    fn total_source_width(&self, sources: &[ResolvedSource]) -> usize {
        sources.iter().map(ResolvedSource::width).sum()
    }

    fn combine_join_rows(&self, left_row: &[Value], right_row: &[Value]) -> Vec<Value> {
        let mut combined = left_row.to_vec();
        combined.extend(right_row.iter().cloned());
        combined
    }

    fn resolve_named_source(
        &self,
        ctx: &ExecutionContext,
        table_name: &str,
        alias: Option<String>,
        offset: usize,
    ) -> Result<ResolvedSource> {
        if let Some(cte) = self.statement.lookup_cte(table_name) {
            Ok(ResolvedSource {
                name: table_name.to_string(),
                columns: self.query_output_columns(&cte.query),
                alias,
                offset,
            })
        } else {
            let table = ctx.catalog.get_table_by_name(table_name).ok_or_else(|| {
                HematiteError::ParseError(format!("Table '{}' not found", table_name))
            })?;
            Ok(ResolvedSource {
                name: table.name.clone(),
                columns: table
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect(),
                alias,
                offset,
            })
        }
    }

    fn materialize_named_source(
        &self,
        ctx: &mut ExecutionContext,
        table_name: &str,
        alias: Option<String>,
    ) -> Result<(ResolvedSource, Vec<Vec<Value>>)> {
        let source = self.resolve_named_source(ctx, table_name, alias, 0)?;
        if let Some(cte) = self.statement.lookup_cte(table_name) {
            let result = self.execute_subquery(ctx, &cte.query)?;
            Ok((source, result.rows))
        } else {
            Ok((source, ctx.engine.read_from_table(table_name)?))
        }
    }

    fn materialize_reference(
        &self,
        ctx: &mut ExecutionContext,
        from: &TableReference,
    ) -> Result<(Vec<ResolvedSource>, Vec<Vec<Value>>)> {
        match from {
            TableReference::Table(table_name, alias) => self
                .materialize_named_source(ctx, table_name, alias.clone())
                .map(|(source, rows)| (vec![source], rows)),
            TableReference::Derived { subquery, alias } => {
                let result = self.execute_subquery(ctx, subquery)?;
                Ok((
                    vec![ResolvedSource {
                        name: alias.clone(),
                        columns: result.columns.clone(),
                        alias: None,
                        offset: 0,
                    }],
                    result.rows,
                ))
            }
            TableReference::CrossJoin(left, right) => {
                let (left_sources, left_rows) = self.materialize_reference(ctx, left)?;
                let left_width = self.total_source_width(&left_sources);
                let (right_sources, right_rows) = self.materialize_reference(ctx, right)?;
                let mut rows = Vec::new();
                for left_row in &left_rows {
                    for right_row in &right_rows {
                        rows.push(self.combine_join_rows(left_row, right_row));
                    }
                }

                let mut sources = left_sources;
                sources.extend(self.shifted_sources(right_sources, left_width));
                Ok((sources, rows))
            }
            TableReference::InnerJoin { left, right, on } => {
                let (left_sources, left_rows) = self.materialize_reference(ctx, left)?;
                let left_width = self.total_source_width(&left_sources);
                let (right_sources, right_rows) = self.materialize_reference(ctx, right)?;
                let shifted_right_sources = self.shifted_sources(right_sources, left_width);
                let mut sources = left_sources;
                sources.extend(shifted_right_sources);

                let mut rows = Vec::new();
                for left_row in &left_rows {
                    for right_row in &right_rows {
                        let combined = self.combine_join_rows(left_row, right_row);
                        if self.evaluate_condition(ctx, &sources, on, &combined)? == Some(true) {
                            rows.push(combined);
                        }
                    }
                }

                Ok((sources, rows))
            }
            TableReference::LeftJoin { left, right, on } => {
                let (left_sources, left_rows) = self.materialize_reference(ctx, left)?;
                let left_width = self.total_source_width(&left_sources);
                let (right_sources, right_rows) = self.materialize_reference(ctx, right)?;
                let right_width = self.total_source_width(&right_sources);
                let shifted_right_sources = self.shifted_sources(right_sources, left_width);
                let mut sources = left_sources;
                sources.extend(shifted_right_sources);

                let mut rows = Vec::new();
                for left_row in &left_rows {
                    let mut matched = false;
                    for right_row in &right_rows {
                        let combined = self.combine_join_rows(left_row, right_row);
                        if self.evaluate_condition(ctx, &sources, on, &combined)? == Some(true) {
                            rows.push(combined);
                            matched = true;
                        }
                    }

                    if !matched {
                        let mut combined = left_row.clone();
                        combined.extend(std::iter::repeat_n(Value::Null, right_width));
                        rows.push(combined);
                    }
                }

                Ok((sources, rows))
            }
        }
    }

    fn compare_sort_values(&self, left: &Value, right: &Value) -> Ordering {
        match (left.is_null(), right.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => left.partial_cmp(right).unwrap_or(Ordering::Equal),
        }
    }

    fn has_aggregate_projection(&self) -> bool {
        self.statement
            .columns
            .iter()
            .any(|item| matches!(item, SelectItem::CountAll | SelectItem::Aggregate { .. }))
    }

    fn apply_select_window(&self, rows: &mut Vec<Vec<Value>>) {
        if let Some(offset) = self.statement.offset {
            if offset >= rows.len() {
                rows.clear();
                return;
            }
            rows.drain(0..offset);
        }

        if let Some(limit) = self.statement.limit {
            rows.truncate(limit);
        }
    }

    fn build_query_result(&self, columns: Vec<String>, rows: Vec<Vec<Value>>) -> QueryResult {
        QueryResult {
            affected_rows: rows.len(),
            columns,
            rows,
        }
    }

    fn evaluate_aggregate_value(
        &self,
        sources: &[ResolvedSource],
        function: AggregateFunction,
        target: &AggregateTarget,
        rows: &[Vec<Value>],
    ) -> Result<Option<Value>> {
        if matches!(target, AggregateTarget::All) {
            return match function {
                AggregateFunction::Count => Ok(Some(Value::Integer(rows.len() as i32))),
                _ => Err(HematiteError::ParseError(format!(
                    "{:?}(*) is not supported",
                    function
                ))),
            };
        }

        let AggregateTarget::Column(column) = target else {
            return Ok(None);
        };

        let index = self
            .resolve_column_index(sources, column)?
            .ok_or_else(|| HematiteError::ParseError(format!("Column '{}' not found", column)))?;

        let values: Vec<&Value> = rows
            .iter()
            .map(|row| &row[index])
            .filter(|value| !value.is_null())
            .collect();

        if values.is_empty() {
            return Ok(Some(match function {
                AggregateFunction::Count => Value::Integer(0),
                _ => Value::Null,
            }));
        }

        match function {
            AggregateFunction::Count => Ok(Some(Value::Integer(values.len() as i32))),
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

    fn evaluate_aggregate_item(
        &self,
        sources: &[ResolvedSource],
        item: &SelectItem,
        rows: &[Vec<Value>],
    ) -> Result<Option<Value>> {
        match item {
            SelectItem::CountAll => self.evaluate_aggregate_value(
                sources,
                AggregateFunction::Count,
                &AggregateTarget::All,
                rows,
            ),
            SelectItem::Aggregate { function, column } => self.evaluate_aggregate_value(
                sources,
                *function,
                &AggregateTarget::Column(column.clone()),
                rows,
            ),
            _ => Ok(None),
        }
    }

    fn result_column_index(
        &self,
        output_columns: &[String],
        order_by_column: &str,
    ) -> Option<usize> {
        let base_name = SelectStatement::column_reference_name(order_by_column);
        output_columns.iter().position(|column| {
            column.eq_ignore_ascii_case(order_by_column) || column.eq_ignore_ascii_case(base_name)
        })
    }

    fn sort_projected_rows(&self, output_columns: &[String], rows: &mut [Vec<Value>]) {
        if self.statement.order_by.is_empty() {
            return;
        }

        rows.sort_by(|left, right| {
            for item in &self.statement.order_by {
                let Some(index) = self.result_column_index(output_columns, &item.column) else {
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

    fn evaluate_projected_expression(
        &self,
        sources: &[ResolvedSource],
        expr: &Expression,
        row: &[Value],
        output_columns: &[String],
        group_rows: &[Vec<Value>],
    ) -> Result<Value> {
        match expr {
            Expression::Literal(value) => Ok(value.clone()),
            Expression::Parameter(index) => Err(HematiteError::ParseError(format!(
                "Unbound parameter {} reached execution",
                index + 1
            ))),
            Expression::AggregateCall { function, target } => self
                .evaluate_aggregate_value(sources, *function, target, group_rows)?
                .ok_or_else(|| {
                    HematiteError::InternalError(
                        "Aggregate expression evaluation produced no value".to_string(),
                    )
                }),
            Expression::Column(name) => {
                let index = self
                    .result_column_index(output_columns, name)
                    .ok_or_else(|| {
                        HematiteError::ParseError(format!(
                            "HAVING column '{}' does not match any grouped output column or alias",
                            name
                        ))
                    })?;
                row.get(index).cloned().ok_or_else(|| {
                    HematiteError::InternalError(format!(
                        "Grouped output row is missing column index {} for '{}'",
                        index, name
                    ))
                })
            }
            Expression::UnaryMinus(expr) => {
                match self.evaluate_projected_expression(
                    sources,
                    expr,
                    row,
                    output_columns,
                    group_rows,
                )? {
                    Value::Integer(value) => {
                        value.checked_neg().map(Value::Integer).ok_or_else(|| {
                            HematiteError::ParseError(
                                "Integer overflow while evaluating unary '-'".to_string(),
                            )
                        })
                    }
                    Value::Float(value) => Ok(Value::Float(-value)),
                    Value::Null => Ok(Value::Null),
                    value => Err(HematiteError::ParseError(format!(
                        "Unary '-' requires a numeric value, found {:?}",
                        value
                    ))),
                }
            }
            Expression::Binary {
                left,
                operator,
                right,
            } => self.evaluate_arithmetic(
                operator,
                self.evaluate_projected_expression(sources, left, row, output_columns, group_rows)?,
                self.evaluate_projected_expression(
                    sources,
                    right,
                    row,
                    output_columns,
                    group_rows,
                )?,
            ),
        }
    }

    fn evaluate_projected_condition(
        &self,
        ctx: &mut ExecutionContext<'_>,
        sources: &[ResolvedSource],
        condition: &Condition,
        row: &[Value],
        output_columns: &[String],
        group_rows: &[Vec<Value>],
    ) -> Result<Option<bool>> {
        match condition {
            Condition::Comparison {
                left,
                operator,
                right,
            } => {
                let left_val = self.evaluate_projected_expression(
                    sources,
                    left,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let right_val = self.evaluate_projected_expression(
                    sources,
                    right,
                    row,
                    output_columns,
                    group_rows,
                )?;
                Ok(self.compare_values(&left_val, operator, &right_val))
            }
            Condition::InList {
                expr,
                values,
                is_not,
            } => {
                let probe = self.evaluate_projected_expression(
                    sources,
                    expr,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let candidates = values
                    .iter()
                    .map(|value_expr| {
                        self.evaluate_projected_expression(
                            sources,
                            value_expr,
                            row,
                            output_columns,
                            group_rows,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(self.evaluate_in_candidates(probe, candidates, *is_not))
            }
            Condition::InSubquery {
                expr,
                subquery,
                is_not,
            } => {
                let probe = self.evaluate_projected_expression(
                    sources,
                    expr,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let subquery_result = self.execute_subquery(ctx, subquery)?;
                let candidates = subquery_result
                    .rows
                    .into_iter()
                    .map(|row| row.into_iter().next().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                Ok(self.evaluate_in_candidates(probe, candidates, *is_not))
            }
            Condition::Between {
                expr,
                lower,
                upper,
                is_not,
            } => {
                let value = self.evaluate_projected_expression(
                    sources,
                    expr,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let lower_value = self.evaluate_projected_expression(
                    sources,
                    lower,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let upper_value = self.evaluate_projected_expression(
                    sources,
                    upper,
                    row,
                    output_columns,
                    group_rows,
                )?;

                if value.is_null() || lower_value.is_null() || upper_value.is_null() {
                    return Ok(None);
                }

                let lower_ok = value
                    .partial_cmp(&lower_value)
                    .map(|ordering| !ordering.is_lt());
                let upper_ok = value
                    .partial_cmp(&upper_value)
                    .map(|ordering| !ordering.is_gt());

                match (lower_ok, upper_ok) {
                    (Some(true), Some(true)) => Ok(Some(!is_not)),
                    (Some(_), Some(_)) => Ok(Some(*is_not)),
                    _ => Ok(None),
                }
            }
            Condition::Like {
                expr,
                pattern,
                is_not,
            } => {
                let value = self.evaluate_projected_expression(
                    sources,
                    expr,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let pattern_value = self.evaluate_projected_expression(
                    sources,
                    pattern,
                    row,
                    output_columns,
                    group_rows,
                )?;

                match (value, pattern_value) {
                    (Value::Text(text), Value::Text(pattern)) => {
                        let matched = Self::like_matches(&pattern, &text);
                        Ok(Some(if *is_not { !matched } else { matched }))
                    }
                    (left, right) if left.is_null() || right.is_null() => Ok(None),
                    _ => Ok(None),
                }
            }
            Condition::Exists { subquery, is_not } => {
                let subquery_result = self.execute_subquery(ctx, subquery)?;
                let exists = !subquery_result.rows.is_empty();
                Ok(Some(if *is_not { !exists } else { exists }))
            }
            Condition::NullCheck { expr, is_not } => {
                let value = self.evaluate_projected_expression(
                    sources,
                    expr,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let is_null = value.is_null();
                Ok(Some(if *is_not { !is_null } else { is_null }))
            }
            Condition::Not(condition) => Ok(self
                .evaluate_projected_condition(
                    ctx,
                    sources,
                    condition,
                    row,
                    output_columns,
                    group_rows,
                )?
                .map(|value| !value)),
            Condition::Logical {
                left,
                operator,
                right,
            } => {
                let left_result = self.evaluate_projected_condition(
                    ctx,
                    sources,
                    left,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let right_result = self.evaluate_projected_condition(
                    ctx,
                    sources,
                    right,
                    row,
                    output_columns,
                    group_rows,
                )?;

                match operator {
                    LogicalOperator::And => Ok(self.logical_and(left_result, right_result)),
                    LogicalOperator::Or => Ok(self.logical_or(left_result, right_result)),
                }
            }
        }
    }

    fn project_grouped_row(
        &self,
        ctx: &ExecutionContext,
        sources: &[ResolvedSource],
        group_rows: &[Vec<Value>],
    ) -> Result<Vec<Value>> {
        let representative = group_rows.first().ok_or_else(|| {
            HematiteError::InternalError("Cannot project an empty aggregate group".to_string())
        })?;
        let mut projected = Vec::new();

        for item in &self.statement.columns {
            match item {
                SelectItem::Wildcard => {}
                SelectItem::Column(name) => {
                    if let Some(index) = self.resolve_column_index(sources, name)? {
                        if index < representative.len() {
                            projected.push(representative[index].clone());
                        }
                    }
                }
                SelectItem::Expression(expr) => {
                    projected.push(self.evaluate_expression(ctx, sources, expr, representative)?);
                }
                SelectItem::CountAll | SelectItem::Aggregate { .. } => {
                    projected.push(
                        self.evaluate_aggregate_item(sources, item, group_rows)?
                            .unwrap_or(Value::Null),
                    );
                }
            }
        }

        Ok(projected)
    }

    fn build_groups(
        &self,
        ctx: &ExecutionContext,
        sources: &[ResolvedSource],
        filtered_rows: &[Vec<Value>],
    ) -> Result<Vec<Vec<Vec<Value>>>> {
        if self.statement.group_by.is_empty() {
            return Ok(vec![filtered_rows.to_vec()]);
        }

        let mut keyed_groups: Vec<(Vec<Value>, Vec<Vec<Value>>)> = Vec::new();
        for row in filtered_rows {
            let key = self
                .statement
                .group_by
                .iter()
                .map(|expr| self.evaluate_expression(ctx, sources, expr, row))
                .collect::<Result<Vec<_>>>()?;

            if let Some((_, rows)) = keyed_groups
                .iter_mut()
                .find(|(existing_key, _)| *existing_key == key)
            {
                rows.push(row.clone());
            } else {
                keyed_groups.push((key, vec![row.clone()]));
            }
        }

        Ok(keyed_groups.into_iter().map(|(_, rows)| rows).collect())
    }

    fn apply_having_clause(
        &self,
        ctx: &mut ExecutionContext<'_>,
        sources: &[ResolvedSource],
        output_columns: &[String],
        grouped_rows: Vec<GroupedRow>,
    ) -> Result<Vec<Vec<Value>>> {
        let Some(having_clause) = &self.statement.having_clause else {
            return Ok(grouped_rows
                .into_iter()
                .map(|group| group.projected)
                .collect::<Vec<_>>());
        };

        let mut filtered_rows = Vec::with_capacity(grouped_rows.len());
        for grouped in grouped_rows {
            let mut include = true;
            for condition in &having_clause.conditions {
                if self.evaluate_projected_condition(
                    ctx,
                    sources,
                    condition,
                    &grouped.projected,
                    output_columns,
                    &grouped.source_rows,
                )? != Some(true)
                {
                    include = false;
                    break;
                }
            }

            if include {
                filtered_rows.push(grouped.projected);
            }
        }

        Ok(filtered_rows)
    }

    fn execute_grouped(
        &self,
        ctx: &mut ExecutionContext,
        sources: &[ResolvedSource],
        filtered_rows: &[Vec<Value>],
    ) -> Result<QueryResult> {
        let groups = self.build_groups(ctx, sources, filtered_rows)?;
        let output_columns = self.get_column_names(sources);
        let mut grouped_rows = Vec::with_capacity(groups.len());
        for rows in groups {
            grouped_rows.push(GroupedRow {
                projected: self.project_grouped_row(ctx, sources, &rows)?,
                source_rows: rows,
            });
        }

        let projected_rows =
            self.apply_having_clause(ctx, sources, &output_columns, grouped_rows)?;
        self.finalize_grouped_rows(output_columns, projected_rows)
    }

    fn finalize_grouped_rows(
        &self,
        output_columns: Vec<String>,
        mut projected_rows: Vec<Vec<Value>>,
    ) -> Result<QueryResult> {
        apply_distinct_if_needed(self.statement.distinct, &mut projected_rows);

        self.sort_projected_rows(&output_columns, &mut projected_rows);
        self.apply_select_window(&mut projected_rows);

        Ok(self.build_query_result(output_columns, projected_rows))
    }

    fn filter_source_rows(
        &self,
        ctx: &mut ExecutionContext<'_>,
        sources: &[ResolvedSource],
        all_rows: Vec<Vec<Value>>,
    ) -> Result<Vec<Vec<Value>>> {
        let skip_filter = matches!(self.access_path, SelectAccessPath::RowIdLookup);
        let mut filtered_rows = Vec::new();

        for row in all_rows {
            let include = if skip_filter {
                true
            } else {
                match &self.statement.where_clause {
                    Some(where_clause) => {
                        let mut all_conditions_met = true;
                        for condition in &where_clause.conditions {
                            if self.evaluate_condition(ctx, sources, condition, &row)? != Some(true)
                            {
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
                filtered_rows.push(row);
            }
        }

        Ok(filtered_rows)
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
                        .is_some_and(|column| {
                            column.name == SelectStatement::column_reference_name(column_name)
                        }) =>
                {
                    Some(vec![value.clone()])
                }
                (Expression::Literal(value), Expression::Column(column_name))
                    if table
                        .primary_key_columns
                        .first()
                        .and_then(|index| table.columns.get(*index))
                        .is_some_and(|column| {
                            column.name == SelectStatement::column_reference_name(column_name)
                        }) =>
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
                    if indexed_column.name
                        == SelectStatement::column_reference_name(column_name) =>
                {
                    Some(vec![value.clone()])
                }
                (Expression::Literal(value), Expression::Column(column_name))
                    if indexed_column.name
                        == SelectStatement::column_reference_name(column_name) =>
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
                    if SelectStatement::column_reference_name(column_name)
                        .eq_ignore_ascii_case("rowid")
                        && *v >= 0 =>
                {
                    Some(*v as u64)
                }
                (Expression::Literal(Value::Integer(v)), Expression::Column(column_name))
                    if SelectStatement::column_reference_name(column_name)
                        .eq_ignore_ascii_case("rowid")
                        && *v >= 0 =>
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
        self.statement.validate(&ctx.catalog)?;

        if let Some(set_operation) = &self.statement.set_operation {
            let mut left_statement = self.statement.clone();
            left_statement.set_operation = None;
            let mut left_executor = SelectExecutor::new(left_statement, self.access_path.clone());
            let mut left_result = left_executor.execute(ctx)?;
            let right_result = self.execute_subquery(ctx, &set_operation.right)?;

            left_result.rows.extend(right_result.rows);
            if set_operation.operator == SetOperator::Union {
                apply_distinct_if_needed(true, &mut left_result.rows);
            }
            left_result.affected_rows = left_result.rows.len();
            return Ok(left_result);
        }

        let direct_table = match &self.statement.from {
            TableReference::Table(table_name, _)
                if self.statement.lookup_cte(table_name).is_none() =>
            {
                ctx.catalog.get_table_by_name(table_name).cloned()
            }
            _ => None,
        };

        let (sources, all_rows) = match (
            &self.access_path,
            &self.statement.from,
            direct_table.as_ref(),
        ) {
            (SelectAccessPath::JoinScan, _, _) => {
                self.materialize_reference(ctx, &self.statement.from)?
            }
            (_, TableReference::Derived { .. }, _) => {
                self.materialize_reference(ctx, &self.statement.from)?
            }
            (_, TableReference::CrossJoin(_, _), _) => {
                self.materialize_reference(ctx, &self.statement.from)?
            }
            (_, TableReference::InnerJoin { .. }, _) | (_, TableReference::LeftJoin { .. }, _) => {
                self.materialize_reference(ctx, &self.statement.from)?
            }
            (_, TableReference::Table(table_name, _), Some(table)) => {
                let sources = self.resolve_sources(ctx)?;
                let rows = match self.access_path {
                    SelectAccessPath::RowIdLookup => {
                        let rowid = self.extract_rowid_lookup().ok_or_else(|| {
                            HematiteError::InternalError(
                                "Planner selected rowid lookup without a matching predicate"
                                    .to_string(),
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
                                    .then(|| {
                                        table_cursor.current().map(|row| vec![row.values.clone()])
                                    })
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
                        let mut index_cursor =
                            ctx.engine.open_secondary_index_cursor(table, index_name)?;
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
                    SelectAccessPath::FullTableScan => ctx.engine.read_from_table(table_name)?,
                    SelectAccessPath::JoinScan => unreachable!(),
                };
                (sources, rows)
            }
            _ => {
                return Err(HematiteError::InternalError(
                    "Planner selected a direct table access path for a non-table source"
                        .to_string(),
                ))
            }
        };

        let mut filtered_rows = self.filter_source_rows(ctx, &sources, all_rows)?;

        if !self.statement.order_by.is_empty() {
            filtered_rows.sort_by(|left, right| {
                for item in &self.statement.order_by {
                    let Ok(Some(index)) = self.resolve_column_index(&sources, &item.column) else {
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

        if !self.statement.group_by.is_empty() || self.has_aggregate_projection() {
            return self.execute_grouped(ctx, &sources, &filtered_rows);
        }

        let mut projected_rows = Vec::new();
        for row in filtered_rows {
            projected_rows.push(self.project_row(ctx, &sources, &row)?);
        }

        apply_distinct_if_needed(self.statement.distinct, &mut projected_rows);

        self.apply_select_window(&mut projected_rows);

        Ok(self.build_query_result(self.get_column_names(&sources), projected_rows))
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

    fn evaluate_value_expression(&self, expr: &Expression) -> Result<Value> {
        match expr {
            Expression::Literal(value) => Ok(value.clone()),
            Expression::Parameter(index) => Err(HematiteError::ParseError(format!(
                "Unbound parameter {} reached execution",
                index + 1
            ))),
            Expression::AggregateCall { .. } => Err(HematiteError::ParseError(
                "INSERT expressions cannot use aggregate functions".to_string(),
            )),
            Expression::UnaryMinus(expr) => match self.evaluate_value_expression(expr)? {
                Value::Integer(value) => value.checked_neg().map(Value::Integer).ok_or_else(|| {
                    HematiteError::ParseError(
                        "Integer overflow while evaluating unary '-'".to_string(),
                    )
                }),
                Value::Float(value) => Ok(Value::Float(-value)),
                Value::Null => Ok(Value::Null),
                value => Err(HematiteError::ParseError(format!(
                    "Unary '-' requires a numeric value, found {:?}",
                    value
                ))),
            },
            Expression::Binary {
                left,
                operator,
                right,
            } => evaluate_arithmetic_values(
                operator,
                self.evaluate_value_expression(left)?,
                self.evaluate_value_expression(right)?,
            ),
            Expression::Column(name) => Err(HematiteError::ParseError(format!(
                "INSERT expressions cannot reference column '{}'",
                name
            ))),
        }
    }

    fn ensure_primary_key_is_unique(
        &self,
        ctx: &mut ExecutionContext,
        table: &Table,
        existing_rows: &[Vec<Value>],
        candidate_row: &[Value],
    ) -> Result<()> {
        let candidate_pk = primary_key_values(table, candidate_row)?;

        if ctx
            .engine
            .lookup_row_by_primary_key(table, &candidate_pk)?
            .is_some()
        {
            return Err(duplicate_primary_key_parse_error(
                &table.name,
                &candidate_pk,
            ));
        }

        for existing_row in existing_rows {
            let existing_pk = primary_key_values(table, existing_row)?;

            if existing_pk == candidate_pk {
                return Err(duplicate_primary_key_parse_error(
                    &table.name,
                    &candidate_pk,
                ));
            }
        }

        Ok(())
    }

    fn ensure_unique_secondary_indexes_are_unique(
        &self,
        ctx: &mut ExecutionContext,
        table: &Table,
        candidate_row: &[Value],
    ) -> Result<()> {
        for index in table.secondary_indexes.iter().filter(|index| index.unique) {
            let key_values = secondary_index_key_values(index, candidate_row);
            if !ctx
                .engine
                .lookup_secondary_index_rowids(table, &index.name, &key_values)?
                .is_empty()
            {
                return Err(unique_index_parse_error(&index.name, &table.name));
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
                let literal = self.evaluate_value_expression(expr)?;
                coerce_column_value(column, literal)?
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
        self.statement.validate(&ctx.catalog)?;

        let table = catalog_table(ctx, &self.statement.table)?;

        for value_row in &self.statement.values {
            let row_values = self.build_row(&table, value_row)?;
            validate_row_constraints(ctx, &table, &row_values)?;
            self.ensure_primary_key_is_unique(ctx, &table, &[], &row_values)?;
            self.ensure_unique_secondary_indexes_are_unique(ctx, &table, &row_values)?;
            write_stored_row(
                ctx,
                &self.statement.table,
                &table,
                StoredRow {
                    row_id: 0,
                    values: row_values,
                },
                false,
            )?;
        }

        Ok(mutation_result(self.statement.values.len()))
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

    fn ensure_primary_keys_unique(&self, table: &Table, rows: &[Vec<Value>]) -> Result<()> {
        for i in 0..rows.len() {
            let left = primary_key_values(table, &rows[i])?;
            for right_row in rows.iter().skip(i + 1) {
                let right = primary_key_values(table, right_row)?;
                if left == right {
                    return Err(duplicate_primary_key_parse_error(&table.name, &left));
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
            let candidate_pk = primary_key_values(table, &row.values)?;
            if let Some(existing_rowid) =
                ctx.engine.lookup_primary_key_rowid(table, &candidate_pk)?
            {
                if existing_rowid != row.row_id
                    && !updated_rows
                        .iter()
                        .any(|updated_row| updated_row.row_id == existing_rowid)
                {
                    return Err(duplicate_primary_key_parse_error(
                        &table.name,
                        &candidate_pk,
                    ));
                }
            }
        }

        Ok(())
    }

    fn ensure_updated_unique_indexes_remain_unique(
        &self,
        ctx: &mut ExecutionContext<'_>,
        table: &Table,
        updated_rows: &[StoredRow],
    ) -> Result<()> {
        let mut encoded_keys = std::collections::HashSet::new();

        for index in table.secondary_indexes.iter().filter(|index| index.unique) {
            encoded_keys.clear();
            for row in updated_rows {
                let key_values = secondary_index_key_values(index, &row.values);
                let encoded_key = ctx.engine.encode_secondary_index_key(&key_values)?;
                if !encoded_keys.insert(encoded_key) {
                    return Err(unique_index_parse_error(&index.name, &table.name));
                }
            }

            for row in updated_rows {
                let key_values = secondary_index_key_values(index, &row.values);
                let existing_rowids =
                    ctx.engine
                        .lookup_secondary_index_rowids(table, &index.name, &key_values)?;
                if existing_rowids.into_iter().any(|existing_rowid| {
                    existing_rowid != row.row_id
                        && !updated_rows
                            .iter()
                            .any(|updated_row| updated_row.row_id == existing_rowid)
                }) {
                    return Err(unique_index_parse_error(&index.name, &table.name));
                }
            }
        }

        Ok(())
    }
}

impl QueryExecutor for UpdateExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext<'_>) -> Result<QueryResult> {
        self.statement.validate(&ctx.catalog)?;

        let table = catalog_table(ctx, &self.statement.table)?;

        let select_executor = SelectExecutor::new(
            locator_select_statement(&self.statement.table, self.statement.where_clause.clone()),
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
        let sources = select_executor.resolve_sources(ctx)?;

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
                let value = select_executor.evaluate_expression(
                    ctx,
                    &sources,
                    &assignment.value,
                    &updated_row,
                )?;
                updated_row[column_index] = coerce_column_value(column, value)?;
            }

            table
                .validate_row(&updated_row)
                .map_err(|err| HematiteError::ParseError(err.to_string()))?;
            validate_row_constraints(ctx, &table, &updated_row)?;
            if referenced_parent_value_changed(ctx, &table, &stored_row.values, &updated_row)? {
                ensure_parent_row_is_unreferenced(ctx, &table, &stored_row.values, "update")?;
            }
            updated_rows_data.push(StoredRow {
                row_id: stored_row.row_id,
                values: updated_row,
            });
            updated_rows += 1;
        }

        self.ensure_updated_primary_keys_remain_unique(ctx, &table, &updated_rows_data)?;
        self.ensure_updated_unique_indexes_remain_unique(ctx, &table, &updated_rows_data)?;

        for original_row in &updated_rows_data {
            remove_stored_row(ctx, &self.statement.table, &table, original_row.row_id)?;
        }

        for row in updated_rows_data {
            write_stored_row(ctx, &self.statement.table, &table, row, true)?;
        }

        Ok(mutation_result(updated_rows))
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
        SelectAccessPath::JoinScan => Err(HematiteError::ParseError(
            "Join scans are not valid for UPDATE or DELETE locators".to_string(),
        )),
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
                        let sources = select_executor.resolve_sources(ctx)?;
                        for condition in &where_clause.conditions {
                            if select_executor.evaluate_condition(
                                ctx,
                                &sources,
                                condition,
                                &row.values,
                            )? != Some(true)
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

        let table = catalog_table(ctx, &self.statement.table)?;

        let select_executor = SelectExecutor::new(
            locator_select_statement(&self.statement.table, self.statement.where_clause.clone()),
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
            ensure_parent_row_is_unreferenced(ctx, &table, &row.values, "delete")?;
            remove_stored_row(ctx, &self.statement.table, &table, row.row_id)?;
        }

        Ok(mutation_result(rows_to_delete.len()))
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

    fn unique_index_specs(&self) -> Vec<(String, Vec<usize>)> {
        self.statement
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| {
                if column.unique && !column.primary_key {
                    Some((
                        auto_unique_index_name(&self.statement.table, &column.name, index),
                        vec![index],
                    ))
                } else {
                    None
                }
            })
            .collect()
    }

    fn constraints(&self, table: &Table) -> Result<CreateConstraints> {
        let check_constraints =
            self.statement
                .columns
                .iter()
                .filter_map(|column| column.check_constraint.as_ref())
                .map(Self::clone_check_constraint)
                .chain(self.statement.constraints.iter().filter_map(
                    |constraint| match constraint {
                        TableConstraint::Check(constraint) => {
                            Some(Self::clone_check_constraint(constraint))
                        }
                        TableConstraint::ForeignKey(_) => None,
                    },
                ))
                .collect();

        let foreign_keys =
            self.statement
                .columns
                .iter()
                .filter_map(|column| column.references.as_ref())
                .chain(self.statement.constraints.iter().filter_map(
                    |constraint| match constraint {
                        TableConstraint::Check(_) => None,
                        TableConstraint::ForeignKey(foreign_key) => Some(foreign_key),
                    },
                ))
                .map(|foreign_key| self.convert_foreign_key(table, foreign_key))
                .collect::<Result<Vec<_>>>()?;

        Ok(CreateConstraints {
            check_constraints,
            foreign_keys,
        })
    }

    fn clone_check_constraint(constraint: &CheckConstraintDefinition) -> CheckConstraint {
        CheckConstraint {
            name: constraint.name.clone(),
            expression_sql: constraint.expression_sql.clone(),
        }
    }

    fn convert_foreign_key(
        &self,
        table: &Table,
        foreign_key: &ForeignKeyDefinition,
    ) -> Result<ForeignKeyConstraint> {
        let column_index = table.get_column_index(&foreign_key.column).ok_or_else(|| {
            HematiteError::ParseError(format!(
                "Foreign key column '{}' does not exist in table '{}'",
                foreign_key.column, table.name
            ))
        })?;
        Ok(ForeignKeyConstraint {
            name: foreign_key.name.clone(),
            column_index,
            referenced_table: foreign_key.referenced_table.clone(),
            referenced_column: foreign_key.referenced_column.clone(),
        })
    }
}

struct CreateConstraints {
    check_constraints: Vec<CheckConstraint>,
    foreign_keys: Vec<ForeignKeyConstraint>,
}

impl QueryExecutor for CreateExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext) -> Result<QueryResult> {
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

        let table = ctx
            .catalog
            .get_table_by_name(&self.statement.table)
            .ok_or_else(|| table_disappeared_internal_error(&self.statement.table, "CREATE TABLE"))?
            .clone();
        let constraints = self.constraints(&table)?;
        for (index_name, column_indices) in self.unique_index_specs() {
            let unique_index_root_page_id = ctx.engine.create_empty_btree()?;
            ctx.catalog.add_secondary_index(
                table.id,
                crate::catalog::SecondaryIndex {
                    name: index_name,
                    column_indices,
                    root_page_id: unique_index_root_page_id,
                    unique: true,
                },
            )?;
        }
        for constraint in constraints.check_constraints {
            ctx.catalog.add_check_constraint(table.id, constraint)?;
        }
        for foreign_key in constraints.foreign_keys {
            ctx.catalog.add_foreign_key(table.id, foreign_key)?;
        }

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

        let table = catalog_table(ctx, &self.statement.table)?;

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

#[derive(Debug, Clone)]
pub struct AlterExecutor {
    pub statement: AlterStatement,
}

impl AlterExecutor {
    pub fn new(statement: AlterStatement) -> Self {
        Self { statement }
    }
}

impl QueryExecutor for AlterExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext<'_>) -> Result<QueryResult> {
        self.statement.validate(&ctx.catalog)?;

        match &self.statement.operation {
            AlterOperation::RenameTo(new_name) => {
                let table = catalog_table(ctx, &self.statement.table)?;
                ctx.catalog.rename_table(table.id, new_name.clone())?;
                ctx.engine
                    .rename_table_runtime_metadata(&self.statement.table, new_name)?;
            }
            AlterOperation::RenameColumn { old_name, new_name } => {
                let table = catalog_table(ctx, &self.statement.table)?;
                ctx.catalog
                    .rename_column(table.id, old_name, new_name.clone())?;
            }
            AlterOperation::AddColumn(column_def) => {
                let table = catalog_table(ctx, &self.statement.table)?;

                let column = Column::new(
                    crate::catalog::ColumnId::new(ctx.catalog.next_column_id()),
                    column_def.name.clone(),
                    column_def.data_type,
                )
                .nullable(column_def.nullable)
                .primary_key(column_def.primary_key);
                let column = if let Some(default_value) = &column_def.default_value {
                    column.default_value(default_value.clone())
                } else {
                    column
                };

                let fill_value = column.get_default_or_null();
                let mut rows = ctx.engine.read_rows_with_ids(&self.statement.table)?;
                for row in &mut rows {
                    row.values.push(fill_value.clone());
                }

                ctx.catalog.add_column(table.id, column)?;
                ctx.engine.replace_table_rows(&self.statement.table, rows)?;
            }
        }

        Ok(QueryResult {
            affected_rows: 0,
            columns: Vec::new(),
            rows: Vec::new(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct CreateIndexExecutor {
    pub statement: CreateIndexStatement,
}

impl CreateIndexExecutor {
    pub fn new(statement: CreateIndexStatement) -> Self {
        Self { statement }
    }
}

impl QueryExecutor for CreateIndexExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext<'_>) -> Result<QueryResult> {
        self.statement.validate(&ctx.catalog)?;

        let table = catalog_table(ctx, &self.statement.table)?;

        let column_indices = self
            .statement
            .columns
            .iter()
            .map(|column_name| {
                table.get_column_index(column_name).ok_or_else(|| {
                    HematiteError::ParseError(format!(
                        "Column '{}' does not exist in table '{}'",
                        column_name, self.statement.table
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let root_page_id = ctx.engine.create_empty_btree()?;
        ctx.catalog.add_secondary_index(
            table.id,
            crate::catalog::SecondaryIndex {
                name: self.statement.index_name.clone(),
                column_indices,
                root_page_id,
                unique: self.statement.unique,
            },
        )?;

        let updated_table = ctx
            .catalog
            .get_table(table.id)
            .ok_or_else(|| {
                table_disappeared_internal_error(
                    &self.statement.table,
                    &format!("creating index '{}'", self.statement.index_name),
                )
            })?
            .clone();
        let rows = ctx.engine.read_rows_with_ids(&self.statement.table)?;
        ctx.engine
            .rebuild_secondary_indexes(&updated_table, &rows)?;

        Ok(QueryResult {
            affected_rows: 0,
            columns: Vec::new(),
            rows: Vec::new(),
        })
    }
}

fn secondary_index_key_values(
    index: &crate::catalog::SecondaryIndex,
    row_values: &[Value],
) -> Vec<Value> {
    index
        .column_indices
        .iter()
        .map(|&column_index| row_values[column_index].clone())
        .collect()
}

fn mutation_result(affected_rows: usize) -> QueryResult {
    QueryResult {
        affected_rows,
        columns: Vec::new(),
        rows: Vec::new(),
    }
}

fn duplicate_primary_key_parse_error(table_name: &str, key_values: &[Value]) -> HematiteError {
    HematiteError::ParseError(format!(
        "Duplicate primary key for table '{}': {:?}",
        table_name, key_values
    ))
}

fn table_not_found_parse_error(table_name: &str) -> HematiteError {
    HematiteError::ParseError(format!("Table '{}' not found", table_name))
}

fn table_disappeared_internal_error(table_name: &str, operation: &str) -> HematiteError {
    HematiteError::InternalError(format!(
        "Table '{}' disappeared during {}",
        table_name, operation
    ))
}

fn catalog_table(ctx: &ExecutionContext<'_>, table_name: &str) -> Result<Table> {
    ctx.catalog
        .get_table_by_name(table_name)
        .cloned()
        .ok_or_else(|| table_not_found_parse_error(table_name))
}

fn primary_key_values(table: &Table, row: &[Value]) -> Result<Vec<Value>> {
    table.get_primary_key_values(row).map_err(|err| {
        HematiteError::ParseError(format!("Failed to extract primary key values: {}", err))
    })
}

fn coerce_column_value(column: &Column, value: Value) -> Result<Value> {
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

fn validate_check_constraints(
    ctx: &mut ExecutionContext<'_>,
    table: &Table,
    row: &[Value],
) -> Result<()> {
    if table.check_constraints.is_empty() {
        return Ok(());
    }

    let constraint_executor = SelectExecutor::new(
        locator_select_statement(&table.name, None),
        SelectAccessPath::FullTableScan,
    );
    let sources = constraint_executor.resolve_sources(ctx)?;

    for constraint in &table.check_constraints {
        let condition =
            crate::parser::parser::parse_condition_fragment(&constraint.expression_sql)?;
        let result = constraint_executor.evaluate_condition(ctx, &sources, &condition, row)?;
        if result == Some(false) {
            let constraint_name = constraint
                .name
                .as_deref()
                .unwrap_or(constraint.expression_sql.as_str());
            return Err(HematiteError::ParseError(format!(
                "CHECK constraint '{}' failed for table '{}'",
                constraint_name, table.name
            )));
        }
    }

    Ok(())
}

fn validate_row_constraints(
    ctx: &mut ExecutionContext<'_>,
    table: &Table,
    row: &[Value],
) -> Result<()> {
    validate_check_constraints(ctx, table, row)?;
    validate_foreign_keys(ctx, table, row)
}

fn validate_foreign_keys(
    ctx: &mut ExecutionContext<'_>,
    table: &Table,
    row: &[Value],
) -> Result<()> {
    for foreign_key in &table.foreign_keys {
        let value = row.get(foreign_key.column_index).cloned().ok_or_else(|| {
            HematiteError::CorruptedData(format!(
                "Foreign key column index {} is invalid for table '{}'",
                foreign_key.column_index, table.name
            ))
        })?;
        if value.is_null() {
            continue;
        }
        if !referenced_value_exists(ctx, foreign_key, &value)? {
            return Err(HematiteError::ParseError(format!(
                "Foreign key constraint '{}' failed on table '{}': '{}.{}' does not contain {:?}",
                foreign_key_constraint_name(foreign_key),
                table.name,
                foreign_key.referenced_table,
                foreign_key.referenced_column,
                value
            )));
        }
    }

    Ok(())
}

fn referenced_value_exists(
    ctx: &mut ExecutionContext<'_>,
    foreign_key: &ForeignKeyConstraint,
    value: &Value,
) -> Result<bool> {
    let referenced_table = ctx
        .catalog
        .get_table_by_name(&foreign_key.referenced_table)
        .ok_or_else(|| {
            HematiteError::ParseError(format!(
                "Referenced table '{}' not found",
                foreign_key.referenced_table
            ))
        })?
        .clone();
    let referenced_column_index = referenced_table
        .get_column_index(&foreign_key.referenced_column)
        .ok_or_else(|| {
            HematiteError::ParseError(format!(
                "Referenced column '{}.{}' not found",
                foreign_key.referenced_table, foreign_key.referenced_column
            ))
        })?;

    if referenced_table.primary_key_columns.len() == 1
        && referenced_table.primary_key_columns[0] == referenced_column_index
    {
        return Ok(ctx
            .engine
            .lookup_row_by_primary_key(&referenced_table, std::slice::from_ref(value))?
            .is_some());
    }

    let unique_index = referenced_table
        .secondary_indexes
        .iter()
        .find(|index| {
            index.unique
                && index.column_indices.len() == 1
                && index.column_indices[0] == referenced_column_index
        })
        .ok_or_else(|| {
            HematiteError::CorruptedData(format!(
                "Referenced column '{}.{}' is no longer backed by a PRIMARY KEY or single-column UNIQUE index",
                foreign_key.referenced_table, foreign_key.referenced_column
            ))
        })?;

    Ok(!ctx
        .engine
        .lookup_secondary_index_rowids(
            &referenced_table,
            &unique_index.name,
            std::slice::from_ref(value),
        )?
        .is_empty())
}

fn referencing_foreign_keys(
    ctx: &mut ExecutionContext<'_>,
    parent_table: &Table,
) -> Result<Vec<ReferencingForeignKey>> {
    let mut references = Vec::new();

    for (_, table_name) in ctx.catalog.list_tables() {
        let child_table = ctx.catalog.get_table_by_name(&table_name).ok_or_else(|| {
            HematiteError::ParseError(format!("Table '{}' not found", table_name))
        })?;
        for foreign_key in &child_table.foreign_keys {
            if foreign_key.referenced_table != parent_table.name {
                continue;
            }
            let referenced_column_index = parent_table
                .get_column_index(&foreign_key.referenced_column)
                .ok_or_else(|| {
                    HematiteError::CorruptedData(format!(
                        "Referenced column '{}.{}' is missing",
                        foreign_key.referenced_table, foreign_key.referenced_column
                    ))
                })?;
            references.push(ReferencingForeignKey {
                child_table: child_table.clone(),
                foreign_key: foreign_key.clone(),
                referenced_column_index,
            });
        }
    }

    Ok(references)
}

fn referenced_parent_value_changed(
    ctx: &mut ExecutionContext<'_>,
    parent_table: &Table,
    original_row: &[Value],
    updated_row: &[Value],
) -> Result<bool> {
    for reference in referencing_foreign_keys(ctx, parent_table)? {
        if original_row.get(reference.referenced_column_index)
            != updated_row.get(reference.referenced_column_index)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn ensure_parent_row_is_unreferenced(
    ctx: &mut ExecutionContext<'_>,
    parent_table: &Table,
    row: &[Value],
    action: &str,
) -> Result<()> {
    for reference in referencing_foreign_keys(ctx, parent_table)? {
        let Some(parent_value) = row.get(reference.referenced_column_index) else {
            return Err(HematiteError::CorruptedData(format!(
                "Referenced column index {} is invalid for table '{}'",
                reference.referenced_column_index, parent_table.name
            )));
        };
        if parent_value.is_null() {
            continue;
        }

        for child_row in ctx.engine.read_rows_with_ids(&reference.child_table.name)? {
            let Some(child_value) = child_row.values.get(reference.foreign_key.column_index) else {
                return Err(HematiteError::CorruptedData(format!(
                    "Foreign key column index {} is invalid for table '{}'",
                    reference.foreign_key.column_index, reference.child_table.name
                )));
            };
            if !child_value.is_null() && child_value == parent_value {
                return Err(HematiteError::ParseError(format!(
                    "Cannot {} row in table '{}' because foreign key '{}' on table '{}' still references it",
                    action,
                    parent_table.name,
                    foreign_key_constraint_name(&reference.foreign_key),
                    reference.child_table.name
                )));
            }
        }
    }

    Ok(())
}

fn foreign_key_constraint_name(foreign_key: &ForeignKeyConstraint) -> &str {
    foreign_key
        .name
        .as_deref()
        .unwrap_or(foreign_key.referenced_column.as_str())
}

struct ReferencingForeignKey {
    child_table: Table,
    foreign_key: ForeignKeyConstraint,
    referenced_column_index: usize,
}

fn remove_stored_row(
    ctx: &mut ExecutionContext<'_>,
    table_name: &str,
    table: &Table,
    row_id: u64,
) -> Result<()> {
    let Some(existing_row) = ctx.engine.lookup_row_by_rowid(table_name, row_id)? else {
        return Ok(());
    };

    ctx.engine
        .delete_secondary_index_row(table, &existing_row)?;
    let deleted_pk = ctx.engine.delete_primary_key_row(table, &existing_row)?;
    if !deleted_pk {
        return Err(HematiteError::CorruptedData(format!(
            "Primary-key index entry vanished during row removal for table '{}'",
            table_name
        )));
    }

    let deleted = ctx
        .engine
        .delete_from_table_by_rowid(table_name, existing_row.row_id)?;
    if !deleted {
        return Err(HematiteError::CorruptedData(format!(
            "Rowid {} vanished during row removal for table '{}'",
            existing_row.row_id, table_name
        )));
    }

    Ok(())
}

fn write_stored_row(
    ctx: &mut ExecutionContext<'_>,
    table_name: &str,
    table: &Table,
    mut row: StoredRow,
    preserve_row_id: bool,
) -> Result<u64> {
    let row_id = if preserve_row_id {
        ctx.engine.insert_row_with_rowid(table_name, row.clone())?;
        row.row_id
    } else {
        let allocated_row_id = ctx
            .engine
            .insert_into_table(table_name, row.values.clone())?;
        row.row_id = allocated_row_id;
        allocated_row_id
    };

    ctx.engine.register_primary_key_row(table, row.clone())?;
    ctx.engine.register_secondary_index_row(table, row)?;
    Ok(row_id)
}

fn apply_distinct_if_needed(distinct: bool, rows: &mut Vec<Vec<Value>>) {
    if !distinct {
        return;
    }

    let mut distinct_rows = Vec::new();
    for row in rows.drain(..) {
        if !distinct_rows.contains(&row) {
            distinct_rows.push(row);
        }
    }
    *rows = distinct_rows;
}

fn locator_select_statement(
    table_name: &str,
    where_clause: Option<WhereClause>,
) -> SelectStatement {
    SelectStatement {
        with_clause: Vec::new(),
        distinct: false,
        columns: vec![SelectItem::Wildcard],
        column_aliases: vec![None],
        from: TableReference::Table(table_name.to_string(), None),
        where_clause,
        group_by: Vec::new(),
        having_clause: None,
        order_by: Vec::new(),
        limit: None,
        offset: None,
        set_operation: None,
    }
}

fn evaluate_arithmetic_values(
    operator: &ArithmeticOperator,
    left: Value,
    right: Value,
) -> Result<Value> {
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }

    match (left, right) {
        (Value::Integer(left), Value::Integer(right)) => match operator {
            ArithmeticOperator::Add => {
                left.checked_add(right).map(Value::Integer).ok_or_else(|| {
                    HematiteError::ParseError("Integer overflow while evaluating '+'".to_string())
                })
            }
            ArithmeticOperator::Subtract => {
                left.checked_sub(right).map(Value::Integer).ok_or_else(|| {
                    HematiteError::ParseError("Integer overflow while evaluating '-'".to_string())
                })
            }
            ArithmeticOperator::Multiply => {
                left.checked_mul(right).map(Value::Integer).ok_or_else(|| {
                    HematiteError::ParseError("Integer overflow while evaluating '*'".to_string())
                })
            }
            ArithmeticOperator::Divide => {
                if right == 0 {
                    Err(HematiteError::ParseError("Division by zero".to_string()))
                } else {
                    Ok(Value::Float(left as f64 / right as f64))
                }
            }
        },
        (Value::Integer(left), Value::Float(right)) => {
            evaluate_float_arithmetic(operator, left as f64, right)
        }
        (Value::Float(left), Value::Integer(right)) => {
            evaluate_float_arithmetic(operator, left, right as f64)
        }
        (Value::Float(left), Value::Float(right)) => {
            evaluate_float_arithmetic(operator, left, right)
        }
        (left, right) => Err(HematiteError::ParseError(format!(
            "Arithmetic requires numeric values, found {:?} and {:?}",
            left, right
        ))),
    }
}

fn evaluate_float_arithmetic(
    operator: &ArithmeticOperator,
    left: f64,
    right: f64,
) -> Result<Value> {
    let value = match operator {
        ArithmeticOperator::Add => left + right,
        ArithmeticOperator::Subtract => left - right,
        ArithmeticOperator::Multiply => left * right,
        ArithmeticOperator::Divide => {
            if right == 0.0 {
                return Err(HematiteError::ParseError("Division by zero".to_string()));
            }
            left / right
        }
    };
    Ok(Value::Float(value))
}

fn unique_index_parse_error(index_name: &str, table_name: &str) -> HematiteError {
    HematiteError::ParseError(format!(
        "Duplicate value for UNIQUE index '{}' on table '{}'",
        index_name, table_name
    ))
}

fn auto_unique_index_name(table_name: &str, column_name: &str, position: usize) -> String {
    format!(
        "uq_{}_{}_{}",
        sanitize_identifier(table_name),
        sanitize_identifier(column_name),
        position
    )
}

fn sanitize_identifier(identifier: &str) -> String {
    let mut sanitized = String::with_capacity(identifier.len());
    for ch in identifier.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }
    sanitized
}

#[derive(Debug, Clone)]
pub struct DropIndexExecutor {
    pub statement: DropIndexStatement,
}

impl DropIndexExecutor {
    pub fn new(statement: DropIndexStatement) -> Self {
        Self { statement }
    }
}

impl QueryExecutor for DropIndexExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext<'_>) -> Result<QueryResult> {
        self.statement.validate(&ctx.catalog)?;

        let table = ctx
            .catalog
            .get_table_by_name(&self.statement.table)
            .ok_or_else(|| {
                HematiteError::ParseError(format!("Table '{}' not found", self.statement.table))
            })?
            .clone();
        let index = table
            .get_secondary_index(&self.statement.index_name)
            .ok_or_else(|| {
                HematiteError::ParseError(format!(
                    "Index '{}' does not exist on table '{}'",
                    self.statement.index_name, self.statement.table
                ))
            })?
            .clone();

        ctx.engine.delete_tree(index.root_page_id)?;
        ctx.catalog
            .drop_secondary_index(table.id, &self.statement.index_name)?;

        Ok(QueryResult {
            affected_rows: 0,
            columns: Vec::new(),
            rows: Vec::new(),
        })
    }
}
