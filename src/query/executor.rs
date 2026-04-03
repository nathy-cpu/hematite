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

use crate::catalog::column::{collation_is_nocase, pad_text_to_char_length};
use crate::catalog::table::{
    CheckConstraint, ForeignKeyAction as CatalogForeignKeyAction, ForeignKeyConstraint,
};
use crate::catalog::StoredRow;
use crate::catalog::{
    Column, DataType, DateTimeValue, DateValue, DecimalValue, Float128Value,
    IntervalDaySecondValue, IntervalYearMonthValue, Table, TimeValue, TimeWithTimeZoneValue, Value,
};
use crate::error::{HematiteError, Result};
use crate::parser::ast::*;
use crate::query::lowering::{lower_literal_value, lower_type_name, raise_literal_value};
use crate::query::plan::{ExecutionProgram, QueryPlan, SelectAccessPath};
use crate::query::predicate::extract_literal_equalities;
pub use crate::query::runtime::{ExecutionContext, MutationEvent, QueryExecutor, QueryResult};
use crate::query::validation::{
    projected_column_names, validate_column_reference, validate_statement,
};
use crate::query::QueryPlanner;
use std::cmp::Ordering;
use std::collections::HashMap;

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
    outer_scopes: Vec<CorrelatedScope>,
    materialized_ctes: HashMap<String, QueryResult>,
}

#[derive(Debug, Clone)]
struct ResolvedSource {
    name: String,
    columns: Vec<String>,
    column_types: Vec<DataType>,
    column_collations: Vec<Option<String>>,
    alias: Option<String>,
    offset: usize,
}

impl ResolvedSource {
    fn width(&self) -> usize {
        self.columns.len()
    }
}

#[derive(Debug, Clone, Copy)]
struct TextComparisonContext {
    trim_trailing_spaces: bool,
    case_insensitive: bool,
}

#[derive(Debug, Clone)]
enum NamedSourceKind {
    BaseTable,
    MaterializedCte(Vec<Vec<Value>>),
    Cte(CommonTableExpression),
}

#[derive(Debug, Clone)]
struct NamedSource {
    source: ResolvedSource,
    kind: NamedSourceKind,
}

#[derive(Debug, Clone)]
struct GroupedRow {
    projected: Vec<Value>,
    source_rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone)]
struct CorrelatedScope {
    sources: Vec<ResolvedSource>,
    row: Vec<Value>,
}

type SubqueryCache = HashMap<usize, QueryResult>;

impl SelectExecutor {
    pub fn new(statement: SelectStatement, access_path: SelectAccessPath) -> Self {
        Self {
            statement,
            access_path,
            outer_scopes: Vec::new(),
            materialized_ctes: HashMap::new(),
        }
    }

    fn with_outer_scope(mut self, sources: &[ResolvedSource], row: &[Value]) -> Self {
        self.outer_scopes.push(CorrelatedScope {
            sources: sources.to_vec(),
            row: row.to_vec(),
        });
        self
    }

    fn cte_key(name: &str) -> String {
        name.to_ascii_lowercase()
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

    fn query_output_columns(
        &self,
        query: &SelectStatement,
        ctx: &ExecutionContext,
    ) -> Result<Vec<String>> {
        projected_column_names(query, &ctx.catalog)
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

    fn text_comparison_context_for_expression(
        &self,
        sources: &[ResolvedSource],
        expr: &Expression,
    ) -> Result<Option<TextComparisonContext>> {
        let Expression::Column(column_reference) = expr else {
            return Ok(None);
        };
        let Some(flat_index) = self.resolve_column_index(sources, column_reference)? else {
            return Ok(None);
        };
        Ok(self
            .source_column_metadata(sources, flat_index)
            .map(|(data_type, collation)| TextComparisonContext {
                trim_trailing_spaces: matches!(data_type, DataType::Char(_)),
                case_insensitive: collation_is_nocase(collation.as_deref()),
            }))
    }

    fn merged_text_comparison_context(
        &self,
        sources: &[ResolvedSource],
        left: &Expression,
        right: &Expression,
    ) -> Result<Option<TextComparisonContext>> {
        let left_context = self.text_comparison_context_for_expression(sources, left)?;
        let right_context = self.text_comparison_context_for_expression(sources, right)?;
        Ok(match (left_context, right_context) {
            (Some(left), Some(right)) => Some(TextComparisonContext {
                trim_trailing_spaces: left.trim_trailing_spaces || right.trim_trailing_spaces,
                case_insensitive: left.case_insensitive || right.case_insensitive,
            }),
            (Some(context), None) | (None, Some(context)) => Some(context),
            (None, None) => None,
        })
    }

    fn source_column_metadata(
        &self,
        sources: &[ResolvedSource],
        flat_index: usize,
    ) -> Option<(DataType, Option<String>)> {
        for source in sources {
            if flat_index < source.offset {
                continue;
            }
            let relative = flat_index - source.offset;
            if relative < source.columns.len() {
                return Some((
                    source.column_types.get(relative)?.clone(),
                    source.column_collations.get(relative)?.clone(),
                ));
            }
        }
        None
    }

    fn resolve_column_value(
        &self,
        sources: &[ResolvedSource],
        column_reference: &str,
        row: &[Value],
    ) -> Result<Value> {
        if let Some(index) = self.resolve_column_index(sources, column_reference)? {
            return row.get(index).cloned().ok_or_else(|| {
                HematiteError::ParseError(format!("Column '{}' not found", column_reference))
            });
        }

        for scope in self.outer_scopes.iter().rev() {
            if let Some(index) = self.resolve_column_index(&scope.sources, column_reference)? {
                return scope.row.get(index).cloned().ok_or_else(|| {
                    HematiteError::ParseError(format!("Column '{}' not found", column_reference))
                });
            }
        }

        Err(HematiteError::ParseError(format!(
            "Column '{}' not found",
            column_reference
        )))
    }

    fn evaluate_expression(
        &self,
        ctx: &mut ExecutionContext<'_>,
        cache: &mut SubqueryCache,
        sources: &[ResolvedSource],
        expr: &Expression,
        row: &[Value],
    ) -> Result<Value> {
        match expr {
            Expression::Literal(value) => Ok(lower_literal_value(value)),
            Expression::IntervalLiteral { value, qualifier } => match qualifier {
                IntervalQualifier::YearToMonth => Ok(Value::IntervalYearMonth(
                    IntervalYearMonthValue::parse(value)?,
                )),
                IntervalQualifier::DayToSecond => Ok(Value::IntervalDaySecond(
                    IntervalDaySecondValue::parse(value)?,
                )),
            },
            Expression::Parameter(index) => Err(HematiteError::ParseError(format!(
                "Unbound parameter {} reached execution",
                index + 1
            ))),
            Expression::Cast { expr, target_type } => cast_value_to_type(
                self.evaluate_expression(ctx, cache, sources, expr, row)?,
                lower_type_name(target_type.clone()),
            ),
            Expression::Case {
                branches,
                else_expr,
            } => {
                for branch in branches {
                    match self.evaluate_boolean_expression(
                        ctx,
                        cache,
                        sources,
                        &branch.condition,
                        row,
                    )? {
                        Some(true) => {
                            return self.evaluate_expression(
                                ctx,
                                cache,
                                sources,
                                &branch.result,
                                row,
                            )
                        }
                        Some(false) | None => {}
                    }
                }

                match else_expr {
                    Some(else_expr) => {
                        self.evaluate_expression(ctx, cache, sources, else_expr, row)
                    }
                    None => Ok(Value::Null),
                }
            }
            Expression::AggregateCall { .. } => Err(HematiteError::ParseError(
                "Aggregate expressions can only be evaluated in grouped query contexts".to_string(),
            )),
            Expression::ScalarFunctionCall { function, args } => {
                let mut values = Vec::with_capacity(args.len());
                for arg in args {
                    values.push(self.evaluate_expression(ctx, cache, sources, arg, row)?);
                }
                evaluate_scalar_function(*function, values)
            }
            Expression::ScalarSubquery(subquery) => {
                self.execute_scalar_subquery_cached(ctx, cache, subquery, Some(sources), Some(row))
            }
            Expression::Column(name) => self.resolve_column_value(sources, name, row),
            Expression::UnaryMinus(expr) => {
                negate_numeric_value(self.evaluate_expression(ctx, cache, sources, expr, row)?)
            }
            Expression::UnaryNot(_)
            | Expression::Comparison { .. }
            | Expression::InList { .. }
            | Expression::InSubquery { .. }
            | Expression::Between { .. }
            | Expression::Like { .. }
            | Expression::Exists { .. }
            | Expression::NullCheck { .. }
            | Expression::Logical { .. } => Ok(nullable_bool_to_value(
                self.evaluate_boolean_expression(ctx, cache, sources, expr, row)?,
            )),
            Expression::Binary {
                left,
                operator,
                right,
            } => {
                let left = self.evaluate_expression(ctx, cache, sources, left, row)?;
                let right = self.evaluate_expression(ctx, cache, sources, right, row)?;
                self.evaluate_arithmetic(operator, left, right)
            }
        }
    }

    fn evaluate_boolean_expression(
        &self,
        ctx: &mut ExecutionContext<'_>,
        cache: &mut SubqueryCache,
        sources: &[ResolvedSource],
        expr: &Expression,
        row: &[Value],
    ) -> Result<Option<bool>> {
        match expr {
            Expression::Comparison {
                left,
                operator,
                right,
            } => {
                let left_val = self.evaluate_expression(ctx, cache, sources, left, row)?;
                let right_val = self.evaluate_expression(ctx, cache, sources, right, row)?;
                let text_context =
                    self.merged_text_comparison_context(sources, left, right)?;
                Ok(self.compare_values(&left_val, operator, &right_val, text_context))
            }
            Expression::InList {
                expr,
                values,
                is_not,
            } => {
                let probe = self.evaluate_expression(ctx, cache, sources, expr, row)?;
                let candidates = values
                    .iter()
                    .map(|value_expr| {
                        self.evaluate_expression(ctx, cache, sources, value_expr, row)
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(self.evaluate_in_candidates(probe, candidates, *is_not, None))
            }
            Expression::InSubquery {
                expr,
                subquery,
                is_not,
            } => {
                let probe = self.evaluate_expression(ctx, cache, sources, expr, row)?;
                let subquery_result =
                    self.execute_subquery_cached(ctx, cache, subquery, Some(sources), Some(row))?;
                let candidates = subquery_result
                    .rows
                    .into_iter()
                    .map(|row| row.into_iter().next().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                Ok(self.evaluate_in_candidates(probe, candidates, *is_not, None))
            }
            Expression::Between {
                expr,
                lower,
                upper,
                is_not,
            } => {
                let value = self.evaluate_expression(ctx, cache, sources, expr, row)?;
                let lower_value = self.evaluate_expression(ctx, cache, sources, lower, row)?;
                let upper_value = self.evaluate_expression(ctx, cache, sources, upper, row)?;
                let text_context =
                    self.text_comparison_context_for_expression(sources, expr)?;
                Ok(evaluate_between_values(
                    value,
                    lower_value,
                    upper_value,
                    *is_not,
                    text_context,
                ))
            }
            Expression::Like {
                expr,
                pattern,
                is_not,
            } => {
                let value = self.evaluate_expression(ctx, cache, sources, expr, row)?;
                let pattern_value = self.evaluate_expression(ctx, cache, sources, pattern, row)?;
                let text_context =
                    self.text_comparison_context_for_expression(sources, expr)?;
                Ok(evaluate_like_values(value, pattern_value, *is_not, text_context))
            }
            Expression::Exists { subquery, is_not } => {
                let subquery_result =
                    self.execute_subquery_cached(ctx, cache, subquery, Some(sources), Some(row))?;
                let exists = !subquery_result.rows.is_empty();
                Ok(Some(if *is_not { !exists } else { exists }))
            }
            Expression::NullCheck { expr, is_not } => {
                let value = self.evaluate_expression(ctx, cache, sources, expr, row)?;
                let is_null = value.is_null();
                Ok(Some(if *is_not { !is_null } else { is_null }))
            }
            Expression::UnaryNot(expr) => Ok(self
                .evaluate_boolean_expression(ctx, cache, sources, expr, row)?
                .map(|value| !value)),
            Expression::Logical {
                left,
                operator,
                right,
            } => {
                let left_result =
                    self.evaluate_boolean_expression(ctx, cache, sources, left, row)?;
                let right_result =
                    self.evaluate_boolean_expression(ctx, cache, sources, right, row)?;
                match operator {
                    LogicalOperator::And => Ok(self.logical_and(left_result, right_result)),
                    LogicalOperator::Or => Ok(self.logical_or(left_result, right_result)),
                }
            }
            _ => coerce_value_to_nullable_bool(
                self.evaluate_expression(ctx, cache, sources, expr, row)?,
                "Boolean expression",
            ),
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
        text_context: Option<TextComparisonContext>,
    ) -> Option<bool> {
        compare_condition_values(left_val, operator, right_val, text_context)
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
        logical_and_values(left, right)
    }

    fn logical_or(&self, left: Option<bool>, right: Option<bool>) -> Option<bool> {
        logical_or_values(left, right)
    }

    fn evaluate_in_candidates(
        &self,
        probe: Value,
        candidates: impl IntoIterator<Item = Value>,
        is_not: bool,
        text_context: Option<TextComparisonContext>,
    ) -> Option<bool> {
        evaluate_in_candidates(probe, candidates, is_not, text_context)
    }

    fn execute_subquery(
        &self,
        ctx: &mut ExecutionContext<'_>,
        subquery: &SelectStatement,
        current_sources: Option<&[ResolvedSource]>,
        current_row: Option<&[Value]>,
    ) -> Result<QueryResult> {
        let effective_subquery = if let (Some(sources), Some(row)) = (current_sources, current_row)
        {
            self.bind_correlated_subquery(ctx, subquery, sources, row)?
        } else {
            subquery.clone()
        };
        let planner = QueryPlanner::new(ctx.catalog.clone())
            .with_table_row_counts(current_table_row_counts(ctx.engine));
        let plan = planner.plan(Statement::Select(effective_subquery))?;
        match plan.program {
            ExecutionProgram::Select {
                statement,
                access_path,
            } => {
                let mut executor = SelectExecutor::new(statement, access_path);
                executor.outer_scopes = self.outer_scopes.clone();
                executor.materialized_ctes = self.materialized_ctes.clone();
                if let (Some(sources), Some(row)) = (current_sources, current_row) {
                    executor = executor.with_outer_scope(sources, row);
                }
                executor.execute(ctx)
            }
            _ => Err(HematiteError::InternalError(
                "Expected SELECT execution program for subquery".to_string(),
            )),
        }
    }

    fn bind_correlated_subquery(
        &self,
        ctx: &ExecutionContext<'_>,
        subquery: &SelectStatement,
        current_sources: &[ResolvedSource],
        current_row: &[Value],
    ) -> Result<SelectStatement> {
        let mut bound = subquery.clone();
        let mut scopes = self.outer_scopes.clone();
        scopes.push(CorrelatedScope {
            sources: current_sources.to_vec(),
            row: current_row.to_vec(),
        });
        self.bind_select_outer_references(ctx, &mut bound, &scopes)?;
        Ok(bound)
    }

    fn bind_select_outer_references(
        &self,
        ctx: &ExecutionContext<'_>,
        statement: &mut SelectStatement,
        scopes: &[CorrelatedScope],
    ) -> Result<()> {
        let local_from = statement.from.clone();
        for item in &mut statement.columns {
            match item {
                SelectItem::Expression(expr) => {
                    self.bind_expression_outer_references(ctx, &local_from, expr, scopes)?
                }
                SelectItem::Window { window, .. } => {
                    for expr in &mut window.partition_by {
                        self.bind_expression_outer_references(ctx, &local_from, expr, scopes)?;
                    }
                }
                SelectItem::Wildcard
                | SelectItem::Column(_)
                | SelectItem::CountAll
                | SelectItem::Aggregate { .. } => {}
            }
        }

        if let Some(where_clause) = &mut statement.where_clause {
            for condition in &mut where_clause.conditions {
                self.bind_condition_outer_references(ctx, &local_from, condition, scopes)?;
            }
        }

        for expr in &mut statement.group_by {
            self.bind_expression_outer_references(ctx, &local_from, expr, scopes)?;
        }

        if let Some(having_clause) = &mut statement.having_clause {
            for condition in &mut having_clause.conditions {
                self.bind_condition_outer_references(ctx, &local_from, condition, scopes)?;
            }
        }

        for cte in &mut statement.with_clause {
            self.bind_select_outer_references(ctx, &mut cte.query, scopes)?;
        }

        if let Some(set_operation) = &mut statement.set_operation {
            self.bind_select_outer_references(ctx, &mut set_operation.right, scopes)?;
        }

        Ok(())
    }

    fn bind_condition_outer_references(
        &self,
        ctx: &ExecutionContext<'_>,
        from: &TableReference,
        condition: &mut Condition,
        scopes: &[CorrelatedScope],
    ) -> Result<()> {
        match condition {
            Condition::Comparison { left, right, .. } => {
                self.bind_expression_outer_references(ctx, from, left, scopes)?;
                self.bind_expression_outer_references(ctx, from, right, scopes)?;
            }
            Condition::InList { expr, values, .. } => {
                self.bind_expression_outer_references(ctx, from, expr, scopes)?;
                for value in values {
                    self.bind_expression_outer_references(ctx, from, value, scopes)?;
                }
            }
            Condition::InSubquery { expr, subquery, .. } => {
                self.bind_expression_outer_references(ctx, from, expr, scopes)?;
                self.bind_select_outer_references(ctx, subquery, scopes)?;
            }
            Condition::Between {
                expr, lower, upper, ..
            } => {
                self.bind_expression_outer_references(ctx, from, expr, scopes)?;
                self.bind_expression_outer_references(ctx, from, lower, scopes)?;
                self.bind_expression_outer_references(ctx, from, upper, scopes)?;
            }
            Condition::Like { expr, pattern, .. } => {
                self.bind_expression_outer_references(ctx, from, expr, scopes)?;
                self.bind_expression_outer_references(ctx, from, pattern, scopes)?;
            }
            Condition::Exists { subquery, .. } => {
                self.bind_select_outer_references(ctx, subquery, scopes)?;
            }
            Condition::NullCheck { expr, .. } => {
                self.bind_expression_outer_references(ctx, from, expr, scopes)?;
            }
            Condition::Not(inner) => {
                self.bind_condition_outer_references(ctx, from, inner, scopes)?;
            }
            Condition::Logical { left, right, .. } => {
                self.bind_condition_outer_references(ctx, from, left, scopes)?;
                self.bind_condition_outer_references(ctx, from, right, scopes)?;
            }
        }

        Ok(())
    }

    fn bind_expression_outer_references(
        &self,
        ctx: &ExecutionContext<'_>,
        from: &TableReference,
        expr: &mut Expression,
        scopes: &[CorrelatedScope],
    ) -> Result<()> {
        match expr {
            Expression::Column(name) => {
                let local_scope = SelectStatement {
                    with_clause: Vec::new(),
                    distinct: false,
                    columns: Vec::new(),
                    column_aliases: Vec::new(),
                    from: from.clone(),
                    where_clause: None,
                    group_by: Vec::new(),
                    having_clause: None,
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                    set_operation: None,
                };
                if validate_column_reference(&local_scope, name, &ctx.catalog, from).is_ok() {
                    return Ok(());
                }
                if let Some(value) = self.lookup_outer_scope_value(scopes, name)? {
                    *expr = Expression::Literal(raise_literal_value(&value));
                }
            }
            Expression::Case {
                branches,
                else_expr,
            } => {
                for branch in branches {
                    self.bind_expression_outer_references(
                        ctx,
                        from,
                        &mut branch.condition,
                        scopes,
                    )?;
                    self.bind_expression_outer_references(ctx, from, &mut branch.result, scopes)?;
                }
                if let Some(else_expr) = else_expr {
                    self.bind_expression_outer_references(ctx, from, else_expr, scopes)?;
                }
            }
            Expression::ScalarSubquery(subquery) => {
                self.bind_select_outer_references(ctx, subquery, scopes)?;
            }
            Expression::ScalarFunctionCall { args, .. } => {
                for arg in args {
                    self.bind_expression_outer_references(ctx, from, arg, scopes)?;
                }
            }
            Expression::Cast { expr, .. } => {
                self.bind_expression_outer_references(ctx, from, expr, scopes)?;
            }
            Expression::UnaryMinus(inner) => {
                self.bind_expression_outer_references(ctx, from, inner, scopes)?;
            }
            Expression::UnaryNot(inner) => {
                self.bind_expression_outer_references(ctx, from, inner, scopes)?;
            }
            Expression::Binary { left, right, .. } => {
                self.bind_expression_outer_references(ctx, from, left, scopes)?;
                self.bind_expression_outer_references(ctx, from, right, scopes)?;
            }
            Expression::Comparison { left, right, .. } => {
                self.bind_expression_outer_references(ctx, from, left, scopes)?;
                self.bind_expression_outer_references(ctx, from, right, scopes)?;
            }
            Expression::InList { expr, values, .. } => {
                self.bind_expression_outer_references(ctx, from, expr, scopes)?;
                for value in values {
                    self.bind_expression_outer_references(ctx, from, value, scopes)?;
                }
            }
            Expression::InSubquery { expr, subquery, .. } => {
                self.bind_expression_outer_references(ctx, from, expr, scopes)?;
                self.bind_select_outer_references(ctx, subquery, scopes)?;
            }
            Expression::Between {
                expr, lower, upper, ..
            } => {
                self.bind_expression_outer_references(ctx, from, expr, scopes)?;
                self.bind_expression_outer_references(ctx, from, lower, scopes)?;
                self.bind_expression_outer_references(ctx, from, upper, scopes)?;
            }
            Expression::Like { expr, pattern, .. } => {
                self.bind_expression_outer_references(ctx, from, expr, scopes)?;
                self.bind_expression_outer_references(ctx, from, pattern, scopes)?;
            }
            Expression::Exists { subquery, .. } => {
                self.bind_select_outer_references(ctx, subquery, scopes)?;
            }
            Expression::NullCheck { expr, .. } => {
                self.bind_expression_outer_references(ctx, from, expr, scopes)?;
            }
            Expression::Logical { left, right, .. } => {
                self.bind_expression_outer_references(ctx, from, left, scopes)?;
                self.bind_expression_outer_references(ctx, from, right, scopes)?;
            }
            Expression::AggregateCall { .. }
            | Expression::Literal(_)
            | Expression::IntervalLiteral { .. }
            | Expression::Parameter(_) => {}
        }

        Ok(())
    }

    fn lookup_outer_scope_value(
        &self,
        scopes: &[CorrelatedScope],
        column_reference: &str,
    ) -> Result<Option<Value>> {
        for scope in scopes.iter().rev() {
            if let Some(index) = self.resolve_column_index(&scope.sources, column_reference)? {
                return Ok(scope.row.get(index).cloned());
            }
        }

        Ok(None)
    }

    fn execute_subquery_cached(
        &self,
        ctx: &mut ExecutionContext<'_>,
        cache: &mut SubqueryCache,
        subquery: &SelectStatement,
        current_sources: Option<&[ResolvedSource]>,
        current_row: Option<&[Value]>,
    ) -> Result<QueryResult> {
        if current_sources.is_some() && current_row.is_some() {
            return self.execute_subquery(ctx, subquery, current_sources, current_row);
        }

        let key = subquery as *const SelectStatement as usize;
        if let Some(result) = cache.get(&key) {
            return Ok(result.clone());
        }

        let result = self.execute_subquery(ctx, subquery, None, None)?;
        cache.insert(key, result.clone());
        Ok(result)
    }

    fn execute_scalar_subquery_cached(
        &self,
        ctx: &mut ExecutionContext<'_>,
        cache: &mut SubqueryCache,
        subquery: &SelectStatement,
        current_sources: Option<&[ResolvedSource]>,
        current_row: Option<&[Value]>,
    ) -> Result<Value> {
        let result =
            self.execute_subquery_cached(ctx, cache, subquery, current_sources, current_row)?;
        if result.rows.len() > 1 {
            return Err(HematiteError::ParseError(
                "Scalar subquery returned more than one row".to_string(),
            ));
        }
        Ok(result
            .rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next())
            .unwrap_or(Value::Null))
    }

    fn evaluate_condition(
        &self,
        ctx: &mut ExecutionContext<'_>,
        cache: &mut SubqueryCache,
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
                let left_val = self.evaluate_expression(ctx, cache, sources, left, row)?;
                let right_val = self.evaluate_expression(ctx, cache, sources, right, row)?;
                let text_context =
                    self.merged_text_comparison_context(sources, left, right)?;
                Ok(self.compare_values(&left_val, operator, &right_val, text_context))
            }
            Condition::InList {
                expr,
                values,
                is_not,
            } => {
                let probe = self.evaluate_expression(ctx, cache, sources, expr, row)?;
                let candidates = values
                    .iter()
                    .map(|value_expr| {
                        self.evaluate_expression(ctx, cache, sources, value_expr, row)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let text_context =
                    self.text_comparison_context_for_expression(sources, expr)?;
                Ok(self.evaluate_in_candidates(probe, candidates, *is_not, text_context))
            }
            Condition::InSubquery {
                expr,
                subquery,
                is_not,
            } => {
                let probe = self.evaluate_expression(ctx, cache, sources, expr, row)?;
                let subquery_result =
                    self.execute_subquery_cached(ctx, cache, subquery, Some(sources), Some(row))?;
                let candidates = subquery_result
                    .rows
                    .into_iter()
                    .map(|row| row.into_iter().next().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                let text_context =
                    self.text_comparison_context_for_expression(sources, expr)?;
                Ok(self.evaluate_in_candidates(probe, candidates, *is_not, text_context))
            }
            Condition::Between {
                expr,
                lower,
                upper,
                is_not,
            } => {
                let value = self.evaluate_expression(ctx, cache, sources, expr, row)?;
                let lower_value = self.evaluate_expression(ctx, cache, sources, lower, row)?;
                let upper_value = self.evaluate_expression(ctx, cache, sources, upper, row)?;

                if value.is_null() || lower_value.is_null() || upper_value.is_null() {
                    return Ok(None);
                }

                let text_context =
                    self.text_comparison_context_for_expression(sources, expr)?;
                let lower_ok = sql_partial_cmp(&value, &lower_value, text_context)
                    .map(|ordering| !ordering.is_lt());
                let upper_ok = sql_partial_cmp(&value, &upper_value, text_context)
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
                let value = self.evaluate_expression(ctx, cache, sources, expr, row)?;
                let pattern_value = self.evaluate_expression(ctx, cache, sources, pattern, row)?;
                let text_context =
                    self.text_comparison_context_for_expression(sources, expr)?;

                match (value, pattern_value) {
                    (Value::Text(text), Value::Text(pattern)) => {
                        let matched = like_matches_with_context(&pattern, &text, text_context);
                        Ok(Some(if *is_not { !matched } else { matched }))
                    }
                    (left, right) if left.is_null() || right.is_null() => Ok(None),
                    _ => Ok(None),
                }
            }
            Condition::Exists { subquery, is_not } => {
                let subquery_result =
                    self.execute_subquery_cached(ctx, cache, subquery, Some(sources), Some(row))?;
                let exists = !subquery_result.rows.is_empty();
                Ok(Some(if *is_not { !exists } else { exists }))
            }
            Condition::NullCheck { expr, is_not } => {
                let value = self.evaluate_expression(ctx, cache, sources, expr, row)?;
                let is_null = value.is_null();
                Ok(Some(if *is_not { !is_null } else { is_null }))
            }
            Condition::Not(condition) => Ok(self
                .evaluate_condition(ctx, cache, sources, condition, row)?
                .map(|value| !value)),
            Condition::Logical {
                left,
                operator,
                right,
            } => {
                let left_result = self.evaluate_condition(ctx, cache, sources, left, row)?;
                let right_result = self.evaluate_condition(ctx, cache, sources, right, row)?;

                match operator {
                    LogicalOperator::And => Ok(self.logical_and(left_result, right_result)),
                    LogicalOperator::Or => Ok(self.logical_or(left_result, right_result)),
                }
            }
        }
    }

    fn project_row(
        &self,
        ctx: &mut ExecutionContext<'_>,
        cache: &mut SubqueryCache,
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
                    projected.push(self.evaluate_expression(ctx, cache, sources, expr, row)?);
                }
                SelectItem::CountAll => {}
                SelectItem::Aggregate { .. } => {}
                SelectItem::Window { .. } => {}
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

    fn combine_left_row_with_nulls(&self, left_row: &[Value], right_width: usize) -> Vec<Value> {
        let mut combined = left_row.to_vec();
        combined.extend(std::iter::repeat_n(Value::Null, right_width));
        combined
    }

    fn combine_nulls_with_right_row(&self, left_width: usize, right_row: &[Value]) -> Vec<Value> {
        let mut combined = Vec::with_capacity(left_width + right_row.len());
        combined.extend(std::iter::repeat_n(Value::Null, left_width));
        combined.extend(right_row.iter().cloned());
        combined
    }

    fn join_outer_is_left(&self, left_rows: &[Vec<Value>], right_rows: &[Vec<Value>]) -> bool {
        left_rows.len() <= right_rows.len()
    }

    fn materialize_join_sources(
        &mut self,
        ctx: &mut ExecutionContext,
        left: &TableReference,
        right: &TableReference,
    ) -> Result<(
        Vec<ResolvedSource>,
        Vec<Vec<Value>>,
        usize,
        Vec<Vec<Value>>,
        usize,
    )> {
        let (left_sources, left_rows) = self.materialize_reference(ctx, left)?;
        let left_width = self.total_source_width(&left_sources);
        let (right_sources, right_rows) = self.materialize_reference(ctx, right)?;
        let right_width = self.total_source_width(&right_sources);
        let mut sources = left_sources;
        sources.extend(self.shifted_sources(right_sources, left_width));
        Ok((sources, left_rows, left_width, right_rows, right_width))
    }

    fn push_matching_join_rows(
        &self,
        ctx: &mut ExecutionContext,
        sources: &[ResolvedSource],
        left_rows: &[Vec<Value>],
        right_rows: &[Vec<Value>],
        on: Option<&Condition>,
        rows: &mut Vec<Vec<Value>>,
    ) -> Result<()> {
        let push_matches = |outer_rows: &[Vec<Value>],
                            inner_rows: &[Vec<Value>],
                            outer_is_left: bool,
                            rows: &mut Vec<Vec<Value>>| {
            for outer_row in outer_rows {
                for inner_row in inner_rows {
                    rows.push(if outer_is_left {
                        self.combine_join_rows(outer_row, inner_row)
                    } else {
                        self.combine_join_rows(inner_row, outer_row)
                    });
                }
            }
        };

        if on.is_none() {
            if self.join_outer_is_left(left_rows, right_rows) {
                push_matches(left_rows, right_rows, true, rows);
            } else {
                push_matches(right_rows, left_rows, false, rows);
            }
            return Ok(());
        }

        let predicate = on.expect("checked above");
        if self.join_outer_is_left(left_rows, right_rows) {
            self.push_join_condition_matches(
                ctx, sources, left_rows, right_rows, true, predicate, rows,
            )
        } else {
            self.push_join_condition_matches(
                ctx, sources, right_rows, left_rows, false, predicate, rows,
            )
        }
    }

    fn push_join_condition_matches(
        &self,
        ctx: &mut ExecutionContext,
        sources: &[ResolvedSource],
        outer_rows: &[Vec<Value>],
        inner_rows: &[Vec<Value>],
        outer_is_left: bool,
        predicate: &Condition,
        rows: &mut Vec<Vec<Value>>,
    ) -> Result<()> {
        let mut subquery_cache = SubqueryCache::new();
        for outer_row in outer_rows {
            for inner_row in inner_rows {
                let combined = if outer_is_left {
                    self.combine_join_rows(outer_row, inner_row)
                } else {
                    self.combine_join_rows(inner_row, outer_row)
                };
                if self.evaluate_condition(
                    ctx,
                    &mut subquery_cache,
                    sources,
                    predicate,
                    &combined,
                )? == Some(true)
                {
                    rows.push(combined);
                }
            }
        }
        Ok(())
    }

    fn resolve_named_source(
        &self,
        ctx: &ExecutionContext,
        table_name: &str,
        alias: Option<String>,
        offset: usize,
    ) -> Result<ResolvedSource> {
        Ok(self.named_source(ctx, table_name, alias, offset)?.source)
    }

    fn named_source(
        &self,
        ctx: &ExecutionContext,
        table_name: &str,
        alias: Option<String>,
        offset: usize,
    ) -> Result<NamedSource> {
        if let Some(result) = self.materialized_ctes.get(&Self::cte_key(table_name)) {
            return Ok(NamedSource {
                source: ResolvedSource {
                    name: table_name.to_string(),
                    columns: result.columns.clone(),
                    column_types: vec![DataType::Text; result.columns.len()],
                    column_collations: vec![None; result.columns.len()],
                    alias,
                    offset,
                },
                kind: NamedSourceKind::MaterializedCte(result.rows.clone()),
            });
        }

        if let Some(cte) = self.statement.lookup_cte(table_name) {
            let columns = self.query_output_columns(&cte.query, ctx)?;
            return Ok(NamedSource {
                source: ResolvedSource {
                    name: table_name.to_string(),
                    column_types: vec![DataType::Text; columns.len()],
                    column_collations: vec![None; columns.len()],
                    columns,
                    alias,
                    offset,
                },
                kind: NamedSourceKind::Cte(cte.clone()),
            });
        }

        let table = ctx
            .catalog
            .get_table_by_name(table_name)
            .ok_or_else(|| table_not_found_parse_error(table_name))?;
        Ok(NamedSource {
            source: ResolvedSource {
                name: table.name.clone(),
                columns: table
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect(),
                column_types: table
                    .columns
                    .iter()
                    .map(|column| column.data_type.clone())
                    .collect(),
                column_collations: table
                    .columns
                    .iter()
                    .map(|column| column.collation.clone())
                    .collect(),
                alias,
                offset,
            },
            kind: NamedSourceKind::BaseTable,
        })
    }

    fn materialize_named_source(
        &mut self,
        ctx: &mut ExecutionContext,
        table_name: &str,
        alias: Option<String>,
    ) -> Result<(ResolvedSource, Vec<Vec<Value>>)> {
        let named_source = self.named_source(ctx, table_name, alias, 0)?;
        let rows = match named_source.kind {
            NamedSourceKind::BaseTable => ctx.engine.read_from_table(table_name)?,
            NamedSourceKind::MaterializedCte(rows) => rows,
            NamedSourceKind::Cte(cte) => {
                let key = Self::cte_key(table_name);
                if let Some(result) = self.materialized_ctes.get(&key) {
                    result.rows.clone()
                } else {
                    self.materialize_cte(ctx, &cte)?.rows
                }
            }
        };
        Ok((named_source.source, rows))
    }

    fn materialize_cte(
        &mut self,
        ctx: &mut ExecutionContext<'_>,
        cte: &CommonTableExpression,
    ) -> Result<QueryResult> {
        let key = Self::cte_key(&cte.name);
        if let Some(result) = self.materialized_ctes.get(&key) {
            return Ok(result.clone());
        }

        let result = if cte.recursive {
            self.execute_recursive_cte(ctx, cte)?
        } else {
            self.execute_subquery(ctx, &cte.query, None, None)?
        };
        self.materialized_ctes.insert(key, result.clone());
        Ok(result)
    }

    fn execute_recursive_cte(
        &mut self,
        ctx: &mut ExecutionContext<'_>,
        cte: &CommonTableExpression,
    ) -> Result<QueryResult> {
        const MAX_RECURSIVE_CTE_ITERATIONS: usize = 1024;

        let mut anchor = (*cte.query).clone();
        let set_operation = anchor.set_operation.take().ok_or_else(|| {
            HematiteError::ParseError(format!(
                "Recursive CTE '{}' requires UNION or UNION ALL",
                cte.name
            ))
        })?;

        let operator = set_operation.operator;
        let mut recursive_term = *set_operation.right;
        recursive_term.with_clause.push(CommonTableExpression {
            name: cte.name.clone(),
            recursive: false,
            query: Box::new(anchor.clone()),
        });
        let anchor_result = self.execute_subquery(ctx, &anchor, None, None)?;
        let columns = anchor_result.columns.clone();
        let mut rows = match operator {
            SetOperator::Union => deduplicate_rows(anchor_result.rows),
            SetOperator::UnionAll => anchor_result.rows,
            _ => {
                return Err(HematiteError::ParseError(format!(
                    "Recursive CTE '{}' requires UNION or UNION ALL",
                    cte.name
                )))
            }
        };
        let mut delta = rows.clone();

        let key = Self::cte_key(&cte.name);
        let mut converged = false;
        for _ in 0..MAX_RECURSIVE_CTE_ITERATIONS {
            self.materialized_ctes.insert(
                key.clone(),
                QueryResult {
                    affected_rows: delta.len(),
                    columns: columns.clone(),
                    rows: delta.clone(),
                },
            );

            let mut recursive_executor =
                SelectExecutor::new(recursive_term.clone(), SelectAccessPath::JoinScan);
            recursive_executor.outer_scopes = self.outer_scopes.clone();
            recursive_executor.materialized_ctes = self.materialized_ctes.clone();
            let next_rows = recursive_executor.execute_body(ctx)?.rows;
            if next_rows.is_empty() {
                converged = true;
                break;
            }

            delta = match operator {
                SetOperator::Union => {
                    let mut unique_rows = Vec::new();
                    for row in next_rows {
                        if !rows.contains(&row) && !unique_rows.contains(&row) {
                            unique_rows.push(row);
                        }
                    }
                    unique_rows
                }
                SetOperator::UnionAll => next_rows,
                _ => Vec::new(),
            };

            if delta.is_empty() {
                converged = true;
                break;
            }
            rows.extend(delta.clone());
        }

        self.materialized_ctes.insert(
            key,
            QueryResult {
                affected_rows: rows.len(),
                columns: columns.clone(),
                rows: rows.clone(),
            },
        );

        if !converged {
            return Err(HematiteError::ParseError(format!(
                "Recursive CTE '{}' exceeded the maximum recursion depth of {}",
                cte.name, MAX_RECURSIVE_CTE_ITERATIONS
            )));
        }

        Ok(QueryResult {
            affected_rows: rows.len(),
            columns,
            rows,
        })
    }

    fn materialize_reference(
        &mut self,
        ctx: &mut ExecutionContext,
        from: &TableReference,
    ) -> Result<(Vec<ResolvedSource>, Vec<Vec<Value>>)> {
        match from {
            TableReference::Table(table_name, alias) => self
                .materialize_named_source(ctx, table_name, alias.clone())
                .map(|(source, rows)| (vec![source], rows)),
            TableReference::Derived { subquery, alias } => {
                let result = self.execute_subquery(ctx, subquery, None, None)?;
                Ok((
                    vec![ResolvedSource {
                        name: alias.clone(),
                        columns: result.columns.clone(),
                        column_types: vec![DataType::Text; result.columns.len()],
                        column_collations: vec![None; result.columns.len()],
                        alias: None,
                        offset: 0,
                    }],
                    result.rows,
                ))
            }
            TableReference::CrossJoin(left, right) => {
                let (sources, left_rows, _, right_rows, _) =
                    self.materialize_join_sources(ctx, left, right)?;
                let mut rows = Vec::new();
                self.push_matching_join_rows(
                    ctx,
                    &sources,
                    &left_rows,
                    &right_rows,
                    None,
                    &mut rows,
                )?;
                Ok((sources, rows))
            }
            TableReference::InnerJoin { left, right, on } => {
                let (sources, left_rows, _, right_rows, _) =
                    self.materialize_join_sources(ctx, left, right)?;
                let mut rows = Vec::new();
                self.push_matching_join_rows(
                    ctx,
                    &sources,
                    &left_rows,
                    &right_rows,
                    Some(on),
                    &mut rows,
                )?;
                Ok((sources, rows))
            }
            TableReference::LeftJoin { left, right, on } => {
                let (sources, left_rows, _, right_rows, right_width) =
                    self.materialize_join_sources(ctx, left, right)?;

                let mut rows = Vec::new();
                let mut subquery_cache = SubqueryCache::new();
                for left_row in &left_rows {
                    let mut matched = false;
                    for right_row in &right_rows {
                        let combined = self.combine_join_rows(left_row, right_row);
                        if self.evaluate_condition(
                            ctx,
                            &mut subquery_cache,
                            &sources,
                            on,
                            &combined,
                        )? == Some(true)
                        {
                            rows.push(combined);
                            matched = true;
                        }
                    }

                    if !matched {
                        rows.push(self.combine_left_row_with_nulls(left_row, right_width));
                    }
                }

                Ok((sources, rows))
            }
            TableReference::RightJoin { left, right, on } => {
                let (sources, left_rows, left_width, right_rows, _) =
                    self.materialize_join_sources(ctx, left, right)?;

                let mut rows = Vec::new();
                let mut subquery_cache = SubqueryCache::new();
                for right_row in &right_rows {
                    let mut matched = false;
                    for left_row in &left_rows {
                        let combined = self.combine_join_rows(left_row, right_row);
                        if self.evaluate_condition(
                            ctx,
                            &mut subquery_cache,
                            &sources,
                            on,
                            &combined,
                        )? == Some(true)
                        {
                            rows.push(combined);
                            matched = true;
                        }
                    }

                    if !matched {
                        rows.push(self.combine_nulls_with_right_row(left_width, right_row));
                    }
                }

                Ok((sources, rows))
            }
            TableReference::FullOuterJoin { left, right, on } => {
                let (sources, left_rows, left_width, right_rows, right_width) =
                    self.materialize_join_sources(ctx, left, right)?;

                let mut rows = Vec::new();
                let mut matched_right = vec![false; right_rows.len()];
                let mut subquery_cache = SubqueryCache::new();

                for left_row in &left_rows {
                    let mut matched = false;
                    for (index, right_row) in right_rows.iter().enumerate() {
                        let combined = self.combine_join_rows(left_row, right_row);
                        if self.evaluate_condition(
                            ctx,
                            &mut subquery_cache,
                            &sources,
                            on,
                            &combined,
                        )? == Some(true)
                        {
                            rows.push(combined);
                            matched = true;
                            matched_right[index] = true;
                        }
                    }

                    if !matched {
                        rows.push(self.combine_left_row_with_nulls(left_row, right_width));
                    }
                }

                for (index, right_row) in right_rows.iter().enumerate() {
                    if !matched_right[index] {
                        rows.push(self.combine_nulls_with_right_row(left_width, right_row));
                    }
                }

                Ok((sources, rows))
            }
        }
    }

    fn execute_body(&mut self, ctx: &mut ExecutionContext) -> Result<QueryResult> {
        if let Some(set_operation) = &self.statement.set_operation {
            let mut subquery_cache = SubqueryCache::new();
            let mut left_statement = self.statement.clone();
            left_statement.set_operation = None;
            let mut left_executor = SelectExecutor::new(left_statement, self.access_path.clone());
            left_executor.outer_scopes = self.outer_scopes.clone();
            left_executor.materialized_ctes = self.materialized_ctes.clone();
            let mut left_result = left_executor.execute_body(ctx)?;
            let right_result = self.execute_subquery_cached(
                ctx,
                &mut subquery_cache,
                &set_operation.right,
                None,
                None,
            )?;

            left_result.rows =
                apply_set_operation(set_operation.operator, left_result.rows, right_result.rows);
            left_result.affected_rows = left_result.rows.len();
            return Ok(left_result);
        }

        let (sources, mut filtered_rows) = self.materialize_filtered_rows(ctx)?;
        let mut subquery_cache = SubqueryCache::new();

        if !self.statement.order_by.is_empty() {
            filtered_rows.sort_by(|left, right| {
                for item in &self.statement.order_by {
                    let Ok(Some(index)) = self.resolve_column_index(&sources, &item.column) else {
                        continue;
                    };

                    let text_context = self
                        .text_comparison_context_for_expression(
                            &sources,
                            &Expression::Column(item.column.clone()),
                        )
                        .ok()
                        .flatten();
                    let ordering =
                        self.compare_sort_values(&left[index], &right[index], text_context);
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
            return self.execute_grouped(ctx, &mut subquery_cache, &sources, &filtered_rows);
        }

        if self.has_window_projection() {
            let mut projected_rows =
                self.project_rows_with_windows(ctx, &mut subquery_cache, &sources, &filtered_rows)?;
            apply_distinct_if_needed(self.statement.distinct, &mut projected_rows);
            self.apply_select_window(&mut projected_rows);
            return Ok(self.build_query_result(self.get_column_names(&sources), projected_rows));
        }

        let mut projected_rows = Vec::new();
        for row in filtered_rows {
            projected_rows.push(self.project_row(ctx, &mut subquery_cache, &sources, &row)?);
        }

        apply_distinct_if_needed(self.statement.distinct, &mut projected_rows);
        self.apply_select_window(&mut projected_rows);
        Ok(self.build_query_result(self.get_column_names(&sources), projected_rows))
    }

    fn materialize_filtered_rows(
        &mut self,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<(Vec<ResolvedSource>, Vec<Vec<Value>>)> {
        let direct_table = match &self.statement.from {
            TableReference::Table(table_name, _)
                if self.statement.lookup_cte(table_name).is_none() =>
            {
                ctx.catalog.get_table_by_name(table_name).cloned()
            }
            _ => None,
        };

        let from = self.statement.from.clone();
        let (sources, all_rows) = if self.uses_materialized_reference() {
            self.materialize_reference(ctx, &from)?
        } else if let (TableReference::Table(table_name, _), Some(table)) =
            (&from, direct_table.as_ref())
        {
            let sources = self.resolve_sources(ctx)?;
            let rows = self.materialize_table_access_rows(ctx, table_name, table)?;
            (sources, rows)
        } else {
            return Err(HematiteError::InternalError(
                "Planner selected a direct table access path for a non-table source".to_string(),
            ));
        };

        let mut subquery_cache = SubqueryCache::new();
        let filtered_rows =
            self.filter_source_rows(ctx, &mut subquery_cache, &sources, all_rows)?;
        Ok((sources, filtered_rows))
    }

    fn compare_sort_values(
        &self,
        left: &Value,
        right: &Value,
        text_context: Option<TextComparisonContext>,
    ) -> Ordering {
        match (left.is_null(), right.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => sql_partial_cmp(left, right, text_context).unwrap_or(Ordering::Equal),
        }
    }

    fn has_aggregate_projection(&self) -> bool {
        self.statement
            .columns
            .iter()
            .any(|item| matches!(item, SelectItem::CountAll | SelectItem::Aggregate { .. }))
    }

    fn has_window_projection(&self) -> bool {
        self.statement
            .columns
            .iter()
            .any(|item| matches!(item, SelectItem::Window { .. }))
    }

    fn project_rows_with_windows(
        &self,
        ctx: &mut ExecutionContext<'_>,
        cache: &mut SubqueryCache,
        sources: &[ResolvedSource],
        filtered_rows: &[Vec<Value>],
    ) -> Result<Vec<Vec<Value>>> {
        let mut projected_rows = Vec::with_capacity(filtered_rows.len());

        for (row_index, row) in filtered_rows.iter().enumerate() {
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
                        projected.push(self.evaluate_expression(ctx, cache, sources, expr, row)?);
                    }
                    SelectItem::Window { function, window } => {
                        projected.push(self.evaluate_window_item(
                            ctx,
                            cache,
                            sources,
                            filtered_rows,
                            row_index,
                            function,
                            window,
                        )?)
                    }
                    SelectItem::CountAll | SelectItem::Aggregate { .. } => {}
                }
            }

            projected_rows.push(projected);
        }

        Ok(projected_rows)
    }

    fn evaluate_window_item(
        &self,
        ctx: &mut ExecutionContext<'_>,
        cache: &mut SubqueryCache,
        sources: &[ResolvedSource],
        filtered_rows: &[Vec<Value>],
        row_index: usize,
        function: &WindowFunction,
        window: &WindowSpec,
    ) -> Result<Value> {
        let partition_key = window
            .partition_by
            .iter()
            .map(|expr| {
                self.evaluate_expression(ctx, cache, sources, expr, &filtered_rows[row_index])
            })
            .collect::<Result<Vec<_>>>()?;

        let mut partition_indexes = Vec::new();
        for (index, row) in filtered_rows.iter().enumerate() {
            let row_key = window
                .partition_by
                .iter()
                .map(|expr| self.evaluate_expression(ctx, cache, sources, expr, row))
                .collect::<Result<Vec<_>>>()?;
            if row_key == partition_key {
                partition_indexes.push(index);
            }
        }

        if !window.order_by.is_empty() {
            partition_indexes.sort_by(|left_index, right_index| {
                let left = &filtered_rows[*left_index];
                let right = &filtered_rows[*right_index];

                for item in &window.order_by {
                    let Ok(Some(column_index)) = self.resolve_column_index(sources, &item.column)
                    else {
                        continue;
                    };

                    let ordering =
                        self.compare_sort_values(
                            &left[column_index],
                            &right[column_index],
                            self.text_comparison_context_for_expression(
                                sources,
                                &Expression::Column(item.column.clone()),
                            )
                            .ok()
                            .flatten(),
                        );
                    if ordering != Ordering::Equal {
                        return match item.direction {
                            SortDirection::Asc => ordering,
                            SortDirection::Desc => ordering.reverse(),
                        };
                    }
                }

                left_index.cmp(right_index)
            });
        }

        let position = partition_indexes
            .iter()
            .position(|index| *index == row_index)
            .ok_or_else(|| {
                HematiteError::InternalError(
                    "Current row not found in window partition".to_string(),
                )
            })?;

        match function {
            WindowFunction::RowNumber => Ok(Value::Integer((position + 1) as i32)),
            WindowFunction::Rank => {
                let mut rank = 1usize;
                for current in 1..=position {
                    if self.window_sort_key_changed(
                        sources,
                        window,
                        &filtered_rows[partition_indexes[current - 1]],
                        &filtered_rows[partition_indexes[current]],
                    )? {
                        rank = current + 1;
                    }
                }
                Ok(Value::Integer(rank as i32))
            }
            WindowFunction::DenseRank => {
                let mut rank = 1usize;
                for current in 1..=position {
                    if self.window_sort_key_changed(
                        sources,
                        window,
                        &filtered_rows[partition_indexes[current - 1]],
                        &filtered_rows[partition_indexes[current]],
                    )? {
                        rank += 1;
                    }
                }
                Ok(Value::Integer(rank as i32))
            }
            WindowFunction::Aggregate { function, target } => {
                let partition_rows = partition_indexes
                    .iter()
                    .map(|index| filtered_rows[*index].clone())
                    .collect::<Vec<_>>();
                Ok(self
                    .evaluate_aggregate_value(sources, *function, target, &partition_rows)?
                    .unwrap_or(Value::Null))
            }
        }
    }

    fn window_sort_key_changed(
        &self,
        sources: &[ResolvedSource],
        window: &WindowSpec,
        left: &[Value],
        right: &[Value],
    ) -> Result<bool> {
        if window.order_by.is_empty() {
            return Ok(false);
        }

        for item in &window.order_by {
            let index = self
                .resolve_column_index(sources, &item.column)?
                .ok_or_else(|| {
                    HematiteError::ParseError(format!("Column '{}' not found", item.column))
                })?;

            if self.compare_sort_values(
                &left[index],
                &right[index],
                self.text_comparison_context_for_expression(
                    sources,
                    &Expression::Column(item.column.clone()),
                )
                .ok()
                .flatten(),
            ) != Ordering::Equal
            {
                return Ok(true);
            }
        }

        Ok(false)
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
                let mut float128_sum: Option<Float128Value> = None;
                let mut has_float = false;
                let mut has_float128 = false;

                for value in &values {
                    match value {
                        Value::Integer(i) => {
                            int_sum += *i as i64;
                            float_sum += *i as f64;
                            if let Some(sum) = &mut float128_sum {
                                *sum = sum.add(&Float128Value::from_integer((*i).into()))?;
                            }
                        }
                        Value::Float32(f) => {
                            has_float = true;
                            float_sum += *f as f64;
                            if let Some(sum) = &mut float128_sum {
                                *sum = sum.add(
                                    &Float128Value::from_f64(*f as f64)
                                        .expect("finite FLOAT32 converts to FLOAT128"),
                                )?;
                            }
                        }
                        Value::Float(f) => {
                            has_float = true;
                            float_sum += *f;
                            if let Some(sum) = &mut float128_sum {
                                *sum = sum.add(
                                    &Float128Value::from_f64(*f)
                                        .expect("finite FLOAT converts to FLOAT128"),
                                )?;
                            }
                        }
                        Value::Float128(f) => {
                            has_float128 = true;
                            float128_sum = Some(match float128_sum.take() {
                                Some(sum) => sum.add(f)?,
                                None => f.clone(),
                            });
                        }
                        _ => {
                            return Err(HematiteError::ParseError(format!(
                                "SUM() requires numeric values, found {:?}",
                                value
                            )))
                        }
                    }
                }

                if has_float128 {
                    Ok(float128_sum.map(Value::Float128))
                } else if has_float {
                    Ok(Some(Value::Float(float_sum)))
                } else {
                    Ok(Some(Value::Integer(int_sum as i32)))
                }
            }
            AggregateFunction::Avg => {
                let mut sum: f64 = 0.0;
                let mut float128_sum: Option<Float128Value> = None;
                let mut has_float128 = false;
                let count = values.len() as f64;

                for value in &values {
                    match value {
                        Value::Integer(i) => {
                            sum += *i as f64;
                            if let Some(sum128) = &mut float128_sum {
                                *sum128 = sum128.add(&Float128Value::from_integer((*i).into()))?;
                            }
                        }
                        Value::Float32(f) => {
                            sum += *f as f64;
                            if let Some(sum128) = &mut float128_sum {
                                *sum128 = sum128.add(
                                    &Float128Value::from_f64(*f as f64)
                                        .expect("finite FLOAT32 converts to FLOAT128"),
                                )?;
                            }
                        }
                        Value::Float(f) => {
                            sum += *f;
                            if let Some(sum128) = &mut float128_sum {
                                *sum128 = sum128.add(
                                    &Float128Value::from_f64(*f)
                                        .expect("finite FLOAT converts to FLOAT128"),
                                )?;
                            }
                        }
                        Value::Float128(f) => {
                            has_float128 = true;
                            float128_sum = Some(match float128_sum.take() {
                                Some(sum128) => sum128.add(f)?,
                                None => f.clone(),
                            });
                        }
                        _ => {
                            return Err(HematiteError::ParseError(format!(
                                "AVG() requires numeric values, found {:?}",
                                value
                            )))
                        }
                    }
                }

                if has_float128 {
                    let divisor = Float128Value::from_integer(values.len() as i128);
                    Ok(Some(Value::Float128(
                        float128_sum
                            .expect("float128 aggregate sum present")
                            .divide(&divisor)?,
                    )))
                } else {
                    Ok(Some(Value::Float(sum / count)))
                }
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

                let ordering = self.compare_sort_values(&left[index], &right[index], None);
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
        ctx: &mut ExecutionContext<'_>,
        cache: &mut SubqueryCache,
        sources: &[ResolvedSource],
        expr: &Expression,
        row: &[Value],
        output_columns: &[String],
        group_rows: &[Vec<Value>],
    ) -> Result<Value> {
        match expr {
            Expression::Literal(value) => Ok(lower_literal_value(value)),
            Expression::IntervalLiteral { value, qualifier } => match qualifier {
                IntervalQualifier::YearToMonth => Ok(Value::IntervalYearMonth(
                    IntervalYearMonthValue::parse(value)?,
                )),
                IntervalQualifier::DayToSecond => Ok(Value::IntervalDaySecond(
                    IntervalDaySecondValue::parse(value)?,
                )),
            },
            Expression::Parameter(index) => Err(HematiteError::ParseError(format!(
                "Unbound parameter {} reached execution",
                index + 1
            ))),
            Expression::Cast { expr, target_type } => cast_value_to_type(
                self.evaluate_projected_expression(
                    ctx,
                    cache,
                    sources,
                    expr,
                    row,
                    output_columns,
                    group_rows,
                )?,
                lower_type_name(target_type.clone()),
            ),
            Expression::Case {
                branches,
                else_expr,
            } => {
                for branch in branches {
                    match self.evaluate_projected_boolean_expression(
                        ctx,
                        cache,
                        sources,
                        &branch.condition,
                        row,
                        output_columns,
                        group_rows,
                    )? {
                        Some(true) => {
                            return self.evaluate_projected_expression(
                                ctx,
                                cache,
                                sources,
                                &branch.result,
                                row,
                                output_columns,
                                group_rows,
                            )
                        }
                        Some(false) | None => {}
                    }
                }

                match else_expr {
                    Some(else_expr) => self.evaluate_projected_expression(
                        ctx,
                        cache,
                        sources,
                        else_expr,
                        row,
                        output_columns,
                        group_rows,
                    ),
                    None => Ok(Value::Null),
                }
            }
            Expression::ScalarSubquery(subquery) => {
                self.execute_scalar_subquery_cached(ctx, cache, subquery, Some(sources), Some(row))
            }
            Expression::AggregateCall { function, target } => self
                .evaluate_aggregate_value(sources, *function, target, group_rows)?
                .ok_or_else(|| {
                    HematiteError::InternalError(
                        "Aggregate expression evaluation produced no value".to_string(),
                    )
                }),
            Expression::ScalarFunctionCall { function, args } => {
                let mut values = Vec::with_capacity(args.len());
                for arg in args {
                    values.push(self.evaluate_projected_expression(
                        ctx,
                        cache,
                        sources,
                        arg,
                        row,
                        output_columns,
                        group_rows,
                    )?);
                }
                evaluate_scalar_function(*function, values)
            }
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
                negate_numeric_value(self.evaluate_projected_expression(
                    ctx,
                    cache,
                    sources,
                    expr,
                    row,
                    output_columns,
                    group_rows,
                )?)
            }
            Expression::UnaryNot(_)
            | Expression::Comparison { .. }
            | Expression::InList { .. }
            | Expression::InSubquery { .. }
            | Expression::Between { .. }
            | Expression::Like { .. }
            | Expression::Exists { .. }
            | Expression::NullCheck { .. }
            | Expression::Logical { .. } => Ok(nullable_bool_to_value(
                self.evaluate_projected_boolean_expression(
                    ctx,
                    cache,
                    sources,
                    expr,
                    row,
                    output_columns,
                    group_rows,
                )?,
            )),
            Expression::Binary {
                left,
                operator,
                right,
            } => self.evaluate_arithmetic(
                operator,
                self.evaluate_projected_expression(
                    ctx,
                    cache,
                    sources,
                    left,
                    row,
                    output_columns,
                    group_rows,
                )?,
                self.evaluate_projected_expression(
                    ctx,
                    cache,
                    sources,
                    right,
                    row,
                    output_columns,
                    group_rows,
                )?,
            ),
        }
    }

    fn evaluate_projected_boolean_expression(
        &self,
        ctx: &mut ExecutionContext<'_>,
        cache: &mut SubqueryCache,
        sources: &[ResolvedSource],
        expr: &Expression,
        row: &[Value],
        output_columns: &[String],
        group_rows: &[Vec<Value>],
    ) -> Result<Option<bool>> {
        match expr {
            Expression::Comparison {
                left,
                operator,
                right,
            } => {
                let left_val = self.evaluate_projected_expression(
                    ctx,
                    cache,
                    sources,
                    left,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let right_val = self.evaluate_projected_expression(
                    ctx,
                    cache,
                    sources,
                    right,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let text_context =
                    self.merged_text_comparison_context(sources, left, right)?;
                Ok(compare_condition_values(
                    &left_val,
                    operator,
                    &right_val,
                    text_context,
                ))
            }
            Expression::InList {
                expr,
                values,
                is_not,
            } => {
                let probe = self.evaluate_projected_expression(
                    ctx,
                    cache,
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
                            ctx,
                            cache,
                            sources,
                            value_expr,
                            row,
                            output_columns,
                            group_rows,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                let text_context =
                    self.text_comparison_context_for_expression(sources, expr)?;
                Ok(evaluate_in_candidates(
                    probe,
                    candidates,
                    *is_not,
                    text_context,
                ))
            }
            Expression::InSubquery {
                expr,
                subquery,
                is_not,
            } => {
                let probe = self.evaluate_projected_expression(
                    ctx,
                    cache,
                    sources,
                    expr,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let subquery_result =
                    self.execute_subquery_cached(ctx, cache, subquery, Some(sources), Some(row))?;
                let candidates = subquery_result
                    .rows
                    .into_iter()
                    .map(|row| row.into_iter().next().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                let text_context =
                    self.text_comparison_context_for_expression(sources, expr)?;
                Ok(evaluate_in_candidates(
                    probe,
                    candidates,
                    *is_not,
                    text_context,
                ))
            }
            Expression::Between {
                expr,
                lower,
                upper,
                is_not,
            } => {
                let value = self.evaluate_projected_expression(
                    ctx,
                    cache,
                    sources,
                    expr,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let lower_value = self.evaluate_projected_expression(
                    ctx,
                    cache,
                    sources,
                    lower,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let upper_value = self.evaluate_projected_expression(
                    ctx,
                    cache,
                    sources,
                    upper,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let text_context =
                    self.text_comparison_context_for_expression(sources, expr)?;
                Ok(evaluate_between_values(
                    value,
                    lower_value,
                    upper_value,
                    *is_not,
                    text_context,
                ))
            }
            Expression::Like {
                expr,
                pattern,
                is_not,
            } => {
                let value = self.evaluate_projected_expression(
                    ctx,
                    cache,
                    sources,
                    expr,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let pattern_value = self.evaluate_projected_expression(
                    ctx,
                    cache,
                    sources,
                    pattern,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let text_context =
                    self.text_comparison_context_for_expression(sources, expr)?;
                Ok(evaluate_like_values(value, pattern_value, *is_not, text_context))
            }
            Expression::Exists { subquery, is_not } => {
                let subquery_result =
                    self.execute_subquery_cached(ctx, cache, subquery, Some(sources), Some(row))?;
                let exists = !subquery_result.rows.is_empty();
                Ok(Some(if *is_not { !exists } else { exists }))
            }
            Expression::NullCheck { expr, is_not } => {
                let value = self.evaluate_projected_expression(
                    ctx,
                    cache,
                    sources,
                    expr,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let is_null = value.is_null();
                Ok(Some(if *is_not { !is_null } else { is_null }))
            }
            Expression::UnaryNot(expr) => Ok(self
                .evaluate_projected_boolean_expression(
                    ctx,
                    cache,
                    sources,
                    expr,
                    row,
                    output_columns,
                    group_rows,
                )?
                .map(|value| !value)),
            Expression::Logical {
                left,
                operator,
                right,
            } => {
                let left_result = self.evaluate_projected_boolean_expression(
                    ctx,
                    cache,
                    sources,
                    left,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let right_result = self.evaluate_projected_boolean_expression(
                    ctx,
                    cache,
                    sources,
                    right,
                    row,
                    output_columns,
                    group_rows,
                )?;
                Ok(match operator {
                    LogicalOperator::And => logical_and_values(left_result, right_result),
                    LogicalOperator::Or => logical_or_values(left_result, right_result),
                })
            }
            _ => coerce_value_to_nullable_bool(
                self.evaluate_projected_expression(
                    ctx,
                    cache,
                    sources,
                    expr,
                    row,
                    output_columns,
                    group_rows,
                )?,
                "Boolean expression",
            ),
        }
    }

    fn evaluate_projected_condition(
        &self,
        ctx: &mut ExecutionContext<'_>,
        cache: &mut SubqueryCache,
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
                    ctx,
                    cache,
                    sources,
                    left,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let right_val = self.evaluate_projected_expression(
                    ctx,
                    cache,
                    sources,
                    right,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let text_context =
                    self.merged_text_comparison_context(sources, left, right)?;
                Ok(self.compare_values(&left_val, operator, &right_val, text_context))
            }
            Condition::InList {
                expr,
                values,
                is_not,
            } => {
                let probe = self.evaluate_projected_expression(
                    ctx,
                    cache,
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
                            ctx,
                            cache,
                            sources,
                            value_expr,
                            row,
                            output_columns,
                            group_rows,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                let text_context =
                    self.text_comparison_context_for_expression(sources, expr)?;
                Ok(self.evaluate_in_candidates(probe, candidates, *is_not, text_context))
            }
            Condition::InSubquery {
                expr,
                subquery,
                is_not,
            } => {
                let probe = self.evaluate_projected_expression(
                    ctx,
                    cache,
                    sources,
                    expr,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let subquery_result =
                    self.execute_subquery_cached(ctx, cache, subquery, Some(sources), Some(row))?;
                let candidates = subquery_result
                    .rows
                    .into_iter()
                    .map(|row| row.into_iter().next().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                let text_context =
                    self.text_comparison_context_for_expression(sources, expr)?;
                Ok(self.evaluate_in_candidates(probe, candidates, *is_not, text_context))
            }
            Condition::Between {
                expr,
                lower,
                upper,
                is_not,
            } => {
                let value = self.evaluate_projected_expression(
                    ctx,
                    cache,
                    sources,
                    expr,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let lower_value = self.evaluate_projected_expression(
                    ctx,
                    cache,
                    sources,
                    lower,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let upper_value = self.evaluate_projected_expression(
                    ctx,
                    cache,
                    sources,
                    upper,
                    row,
                    output_columns,
                    group_rows,
                )?;

                if value.is_null() || lower_value.is_null() || upper_value.is_null() {
                    return Ok(None);
                }

                let text_context =
                    self.text_comparison_context_for_expression(sources, expr)?;
                let lower_ok = sql_partial_cmp(&value, &lower_value, text_context)
                    .map(|ordering| !ordering.is_lt());
                let upper_ok = sql_partial_cmp(&value, &upper_value, text_context)
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
                    ctx,
                    cache,
                    sources,
                    expr,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let pattern_value = self.evaluate_projected_expression(
                    ctx,
                    cache,
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
                let subquery_result =
                    self.execute_subquery_cached(ctx, cache, subquery, Some(sources), Some(row))?;
                let exists = !subquery_result.rows.is_empty();
                Ok(Some(if *is_not { !exists } else { exists }))
            }
            Condition::NullCheck { expr, is_not } => {
                let value = self.evaluate_projected_expression(
                    ctx,
                    cache,
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
                    cache,
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
                    cache,
                    sources,
                    left,
                    row,
                    output_columns,
                    group_rows,
                )?;
                let right_result = self.evaluate_projected_condition(
                    ctx,
                    cache,
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
        ctx: &mut ExecutionContext<'_>,
        cache: &mut SubqueryCache,
        sources: &[ResolvedSource],
        group_rows: &[Vec<Value>],
    ) -> Result<Vec<Value>> {
        let representative = group_rows.first().map(Vec::as_slice).unwrap_or(&[]);
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
                    projected.push(self.evaluate_expression(
                        ctx,
                        cache,
                        sources,
                        expr,
                        representative,
                    )?);
                }
                SelectItem::CountAll | SelectItem::Aggregate { .. } => {
                    projected.push(
                        self.evaluate_aggregate_item(sources, item, group_rows)?
                            .unwrap_or(Value::Null),
                    );
                }
                SelectItem::Window { .. } => {
                    return Err(HematiteError::InternalError(
                        "Window projections are not supported in grouped execution".to_string(),
                    ))
                }
            }
        }

        Ok(projected)
    }

    fn build_groups(
        &self,
        ctx: &mut ExecutionContext<'_>,
        cache: &mut SubqueryCache,
        sources: &[ResolvedSource],
        filtered_rows: &[Vec<Value>],
    ) -> Result<Vec<Vec<Vec<Value>>>> {
        if self.statement.group_by.is_empty() {
            if filtered_rows.is_empty() && self.has_aggregate_projection() {
                return Ok(vec![Vec::new()]);
            }
            return Ok(vec![filtered_rows.to_vec()]);
        }

        let mut keyed_groups: Vec<(Vec<Value>, Vec<Vec<Value>>)> = Vec::new();
        for row in filtered_rows {
            let key = self
                .statement
                .group_by
                .iter()
                .map(|expr| self.evaluate_expression(ctx, cache, sources, expr, row))
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
        cache: &mut SubqueryCache,
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
            if self.projected_conditions_match(
                ctx,
                cache,
                sources,
                &having_clause.conditions,
                &grouped.projected,
                output_columns,
                &grouped.source_rows,
            )? {
                filtered_rows.push(grouped.projected);
            }
        }

        Ok(filtered_rows)
    }

    fn execute_grouped(
        &self,
        ctx: &mut ExecutionContext,
        cache: &mut SubqueryCache,
        sources: &[ResolvedSource],
        filtered_rows: &[Vec<Value>],
    ) -> Result<QueryResult> {
        let groups = self.build_groups(ctx, cache, sources, filtered_rows)?;
        let output_columns = self.get_column_names(sources);
        let mut grouped_rows = Vec::with_capacity(groups.len());
        for rows in groups {
            grouped_rows.push(GroupedRow {
                projected: self.project_grouped_row(ctx, cache, sources, &rows)?,
                source_rows: rows,
            });
        }

        let projected_rows =
            self.apply_having_clause(ctx, cache, sources, &output_columns, grouped_rows)?;
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
        cache: &mut SubqueryCache,
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
                        self.conditions_match(ctx, cache, sources, &where_clause.conditions, &row)?
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

    fn conditions_match(
        &self,
        ctx: &mut ExecutionContext<'_>,
        cache: &mut SubqueryCache,
        sources: &[ResolvedSource],
        conditions: &[Condition],
        row: &[Value],
    ) -> Result<bool> {
        for condition in conditions {
            if self.evaluate_condition(ctx, cache, sources, condition, row)? != Some(true) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn projected_conditions_match(
        &self,
        ctx: &mut ExecutionContext<'_>,
        cache: &mut SubqueryCache,
        sources: &[ResolvedSource],
        conditions: &[Condition],
        row: &[Value],
        output_columns: &[String],
        group_rows: &[Vec<Value>],
    ) -> Result<bool> {
        for condition in conditions {
            if self.evaluate_projected_condition(
                ctx,
                cache,
                sources,
                condition,
                row,
                output_columns,
                group_rows,
            )? != Some(true)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn extract_primary_key_lookup(&self, table: &Table) -> Option<Vec<Value>> {
        let equalities = extract_literal_equalities(self.statement.where_clause.as_ref()?)?;
        table
            .primary_key_columns
            .iter()
            .map(|&index| table.columns.get(index))
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .map(|column| equalities.get(column.name.as_str()).cloned())
            .collect()
    }

    fn extract_secondary_index_lookup(
        &self,
        table: &Table,
        index_name: &str,
    ) -> Option<Vec<Value>> {
        let index = table.get_secondary_index(index_name)?;
        let equalities = extract_literal_equalities(self.statement.where_clause.as_ref()?)?;
        index
            .column_indices
            .iter()
            .map(|&column_index| table.columns.get(column_index))
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .map(|column| equalities.get(column.name.as_str()).cloned())
            .collect()
    }

    fn extract_rowid_lookup(&self) -> Option<u64> {
        let equalities = extract_literal_equalities(self.statement.where_clause.as_ref()?)?;
        match equalities.get("rowid") {
            Some(Value::Integer(v)) if v >= &0 => Some(*v as u64),
            _ => None,
        }
    }

    fn uses_materialized_reference(&self) -> bool {
        matches!(
            (&self.access_path, &self.statement.from),
            (SelectAccessPath::JoinScan, _)
                | (_, TableReference::Derived { .. })
                | (_, TableReference::CrossJoin(_, _))
                | (_, TableReference::InnerJoin { .. })
                | (_, TableReference::LeftJoin { .. })
                | (_, TableReference::RightJoin { .. })
                | (_, TableReference::FullOuterJoin { .. })
        )
    }

    fn materialize_table_access_rows(
        &self,
        ctx: &mut ExecutionContext,
        table_name: &str,
        table: &Table,
    ) -> Result<Vec<Vec<Value>>> {
        match self.access_path {
            SelectAccessPath::RowIdLookup => {
                let rowid = self.extract_rowid_lookup().ok_or_else(|| {
                    HematiteError::InternalError(
                        "Planner selected rowid lookup without a matching predicate".to_string(),
                    )
                })?;
                Ok(ctx
                    .engine
                    .lookup_row_by_rowid(table_name, rowid)?
                    .map(|row| vec![row.values])
                    .unwrap_or_default())
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
                        let mut table_cursor = ctx.engine.open_table_cursor(table_name)?;
                        Ok(table_cursor
                            .seek_rowid(rowid)
                            .then(|| table_cursor.current().map(|row| vec![row.values.clone()]))
                            .flatten()
                            .unwrap_or_default())
                    }
                    None => Ok(Vec::new()),
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
                let mut table_cursor = ctx.engine.open_table_cursor(table_name)?;
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

                Ok(rows)
            }
            SelectAccessPath::FullTableScan => ctx.engine.read_from_table(table_name),
            SelectAccessPath::JoinScan => Err(HematiteError::InternalError(
                "Planner selected join scan for direct table access".to_string(),
            )),
        }
    }
}

impl QueryExecutor for SelectExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext) -> Result<QueryResult> {
        validate_statement(&Statement::Select(self.statement.clone()), &ctx.catalog)?;
        self.execute_body(ctx)
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
            Expression::Literal(value) => Ok(lower_literal_value(value)),
            Expression::IntervalLiteral { value, qualifier } => match qualifier {
                IntervalQualifier::YearToMonth => Ok(Value::IntervalYearMonth(
                    IntervalYearMonthValue::parse(value)?,
                )),
                IntervalQualifier::DayToSecond => Ok(Value::IntervalDaySecond(
                    IntervalDaySecondValue::parse(value)?,
                )),
            },
            Expression::Parameter(index) => Err(HematiteError::ParseError(format!(
                "Unbound parameter {} reached execution",
                index + 1
            ))),
            Expression::Cast { expr, target_type } => cast_value_to_type(
                self.evaluate_value_expression(expr)?,
                lower_type_name(target_type.clone()),
            ),
            Expression::Case {
                branches,
                else_expr,
            } => {
                for branch in branches {
                    match self.evaluate_boolean_value_expression(&branch.condition)? {
                        Some(true) => return self.evaluate_value_expression(&branch.result),
                        Some(false) | None => {}
                    }
                }

                match else_expr {
                    Some(else_expr) => self.evaluate_value_expression(else_expr),
                    None => Ok(Value::Null),
                }
            }
            Expression::ScalarSubquery(_) => Err(HematiteError::ParseError(
                "INSERT expressions cannot use scalar subqueries".to_string(),
            )),
            Expression::AggregateCall { .. } => Err(HematiteError::ParseError(
                "INSERT expressions cannot use aggregate functions".to_string(),
            )),
            Expression::ScalarFunctionCall { function, args } => {
                let mut values = Vec::with_capacity(args.len());
                for arg in args {
                    values.push(self.evaluate_value_expression(arg)?);
                }
                evaluate_scalar_function(*function, values)
            }
            Expression::UnaryMinus(expr) => {
                negate_numeric_value(self.evaluate_value_expression(expr)?)
            }
            Expression::UnaryNot(_)
            | Expression::Comparison { .. }
            | Expression::InList { .. }
            | Expression::InSubquery { .. }
            | Expression::Between { .. }
            | Expression::Like { .. }
            | Expression::Exists { .. }
            | Expression::NullCheck { .. }
            | Expression::Logical { .. } => Ok(nullable_bool_to_value(
                self.evaluate_boolean_value_expression(expr)?,
            )),
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

    fn evaluate_boolean_value_expression(&self, expr: &Expression) -> Result<Option<bool>> {
        match expr {
            Expression::Comparison {
                left,
                operator,
                right,
            } => {
                let left_val = self.evaluate_value_expression(left)?;
                let right_val = self.evaluate_value_expression(right)?;
                Ok(compare_condition_values(&left_val, operator, &right_val, None))
            }
            Expression::InList {
                expr,
                values,
                is_not,
            } => {
                let probe = self.evaluate_value_expression(expr)?;
                let candidates = values
                    .iter()
                    .map(|value_expr| self.evaluate_value_expression(value_expr))
                    .collect::<Result<Vec<_>>>()?;
                Ok(evaluate_in_candidates(probe, candidates, *is_not, None))
            }
            Expression::InSubquery { .. } => Err(HematiteError::ParseError(
                "INSERT expressions cannot use subqueries in boolean expressions".to_string(),
            )),
            Expression::Between {
                expr,
                lower,
                upper,
                is_not,
            } => {
                let value = self.evaluate_value_expression(expr)?;
                let lower_value = self.evaluate_value_expression(lower)?;
                let upper_value = self.evaluate_value_expression(upper)?;
                Ok(evaluate_between_values(
                    value,
                    lower_value,
                    upper_value,
                    *is_not,
                    None,
                ))
            }
            Expression::Like {
                expr,
                pattern,
                is_not,
            } => {
                let value = self.evaluate_value_expression(expr)?;
                let pattern_value = self.evaluate_value_expression(pattern)?;
                Ok(evaluate_like_values(value, pattern_value, *is_not, None))
            }
            Expression::Exists { .. } => Err(HematiteError::ParseError(
                "INSERT expressions cannot use EXISTS in boolean expressions".to_string(),
            )),
            Expression::NullCheck { expr, is_not } => {
                let value = self.evaluate_value_expression(expr)?;
                let is_null = value.is_null();
                Ok(Some(if *is_not { !is_null } else { is_null }))
            }
            Expression::UnaryNot(expr) => Ok(self
                .evaluate_boolean_value_expression(expr)?
                .map(|value| !value)),
            Expression::Logical {
                left,
                operator,
                right,
            } => {
                let left_result = self.evaluate_boolean_value_expression(left)?;
                let right_result = self.evaluate_boolean_value_expression(right)?;
                Ok(match operator {
                    LogicalOperator::And => logical_and_values(left_result, right_result),
                    LogicalOperator::Or => logical_or_values(left_result, right_result),
                })
            }
            _ => coerce_value_to_nullable_bool(
                self.evaluate_value_expression(expr)?,
                "Boolean expression",
            ),
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

    fn find_conflicting_row(
        &self,
        ctx: &mut ExecutionContext<'_>,
        table: &Table,
        candidate_row: &[Value],
    ) -> Result<Option<StoredRow>> {
        let mut conflict_row: Option<StoredRow> = ctx
            .engine
            .lookup_row_by_primary_key(table, &primary_key_values(table, candidate_row)?)?;

        for index in table.secondary_indexes.iter().filter(|index| index.unique) {
            let key_values = secondary_index_key_values(index, candidate_row);
            for row_id in
                ctx.engine
                    .lookup_secondary_index_rowids(table, &index.name, &key_values)?
            {
                let row = ctx
                    .engine
                    .lookup_row_by_rowid(&table.name, row_id)?
                    .ok_or_else(|| {
                        HematiteError::CorruptedData(format!(
                            "Unique index '{}' points at missing rowid {} in table '{}'",
                            index.name, row_id, table.name
                        ))
                    })?;
                if let Some(existing) = &conflict_row {
                    if existing.row_id != row.row_id {
                        return Err(HematiteError::ParseError(format!(
                            "INSERT ON DUPLICATE KEY UPDATE matched multiple rows in table '{}'",
                            table.name
                        )));
                    }
                } else {
                    conflict_row = Some(row);
                }
            }
        }

        Ok(conflict_row)
    }

    fn apply_on_duplicate_assignments(
        &self,
        ctx: &mut ExecutionContext<'_>,
        table: &Table,
        mut row: StoredRow,
        assignments: &[UpdateAssignment],
    ) -> Result<()> {
        if assignments.is_empty() {
            return Ok(());
        }

        let evaluator = SelectExecutor::new(
            SelectStatement::single_table_scope(&table.name),
            SelectAccessPath::FullTableScan,
        );
        let sources = evaluator.resolve_sources(ctx)?;
        let mut subquery_cache = SubqueryCache::new();
        let original_values = row.values.clone();

        for assignment in assignments {
            let column_index = table.get_column_index(&assignment.column).ok_or_else(|| {
                HematiteError::ParseError(format!(
                    "Column '{}' does not exist in table '{}'",
                    assignment.column, table.name
                ))
            })?;
            let column = &table.columns[column_index];
            let value = evaluator.evaluate_expression(
                ctx,
                &mut subquery_cache,
                &sources,
                &assignment.value,
                &row.values,
            )?;
            row.values[column_index] = coerce_column_value(column, value)?;
        }

        table
            .validate_row(&row.values)
            .map_err(|err| HematiteError::ParseError(err.to_string()))?;
        validate_row_constraints(ctx, table, &row.values)?;
        if parent_reference_key_changed(ctx, table, &original_values, &row.values)? {
            apply_parent_update_foreign_key_actions(ctx, table, &original_values, &row.values)?;
        }
        ensure_stored_row_uniqueness(ctx, table, &row)?;
        remove_stored_row(ctx, &table.name, table, row.row_id)?;
        write_stored_row(ctx, &table.name, table, row, true)?;
        Ok(())
    }

    fn build_row_with_metadata(
        &self,
        ctx: &ExecutionContext<'_>,
        table: &Table,
        value_row: &[Value],
    ) -> Result<Vec<Value>> {
        let mut row = Vec::with_capacity(table.columns.len());
        let next_row_id = ctx
            .engine
            .get_table_metadata()
            .get(&self.statement.table)
            .map(|metadata| metadata.next_row_id)
            .ok_or_else(|| {
                HematiteError::InternalError(format!(
                    "Table metadata for '{}' disappeared during INSERT",
                    self.statement.table
                ))
            })?;

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
                if column.auto_increment && expr.is_null() {
                    auto_increment_value(column, next_row_id)?
                } else {
                    coerce_column_value(column, expr.clone())?
                }
            } else if column.auto_increment {
                auto_increment_value(column, next_row_id)?
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

    fn evaluate_value_row(&self, row: &[Expression]) -> Result<Vec<Value>> {
        row.iter()
            .map(|expr| self.evaluate_value_expression(expr))
            .collect()
    }
}

impl QueryExecutor for InsertExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext) -> Result<QueryResult> {
        validate_statement(&Statement::Insert(self.statement.clone()), &ctx.catalog)?;

        let table = catalog_table(ctx, &self.statement.table)?;

        let input_rows = match &self.statement.source {
            InsertSource::Values(rows) => rows
                .iter()
                .map(|row| self.evaluate_value_row(row))
                .collect::<Result<Vec<_>>>()?,
            InsertSource::Select(select) => {
                let planner = QueryPlanner::new(ctx.catalog.clone())
                    .with_table_row_counts(current_table_row_counts(ctx.engine));
                let plan = planner.plan(Statement::Select((**select).clone()))?;
                match plan.program {
                    ExecutionProgram::Select {
                        statement,
                        access_path,
                    } => {
                        SelectExecutor::new(statement, access_path)
                            .execute(ctx)?
                            .rows
                    }
                    _ => {
                        return Err(HematiteError::InternalError(
                            "Expected SELECT execution program for INSERT source".to_string(),
                        ))
                    }
                }
            }
        };

        for value_row in &input_rows {
            let row_values = self.build_row_with_metadata(ctx, &table, value_row)?;
            if let Some(assignments) = &self.statement.on_duplicate {
                if let Some(conflicting_row) =
                    self.find_conflicting_row(ctx, &table, &row_values)?
                {
                    self.apply_on_duplicate_assignments(ctx, &table, conflicting_row, assignments)?;
                    continue;
                }
            }

            validate_row_constraints(ctx, &table, &row_values)?;
            self.ensure_primary_key_is_unique(ctx, &table, &[], &row_values)?;
            self.ensure_unique_secondary_indexes_are_unique(ctx, &table, &row_values)?;
            let inserted_row = StoredRow {
                row_id: 0,
                values: row_values,
            };
            write_stored_row(
                ctx,
                &self.statement.table,
                &table,
                inserted_row.clone(),
                false,
            )?;
            if let Some(new_row) = ctx.engine.lookup_row_by_primary_key(
                &table,
                &primary_key_values(&table, &inserted_row.values)?,
            )? {
                ctx.mutation_events.push(MutationEvent::Insert {
                    table_name: self.statement.table.clone(),
                    new_row,
                });
            }
        }

        Ok(mutation_result(input_rows.len()))
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
        validate_statement(&Statement::Update(self.statement.clone()), &ctx.catalog)?;

        let table = catalog_table(ctx, &self.statement.table)?;

        let locator_statement =
            locator_select_statement(self.statement.source(), self.statement.where_clause.clone());
        let mut select_executor = SelectExecutor::new(locator_statement, self.access_path.clone());
        let uses_join_source = matches!(
            self.statement.source.as_ref(),
            Some(source) if !matches!(source, TableReference::Table(_, _))
        );

        let (sources, original_rows, joined_rows) = if uses_join_source {
            let (sources, rows) = locate_rows_for_join_source(
                ctx,
                &table,
                self.statement.target_binding_name(),
                &mut select_executor,
            )?;
            let (stored_rows, joined_rows): (Vec<_>, Vec<_>) = rows.into_iter().unzip();
            (sources, stored_rows, Some(joined_rows))
        } else {
            let rows = locate_rows_for_access_path(
                ctx,
                &table,
                &self.statement.table,
                &self.access_path,
                &select_executor,
            )?;
            (select_executor.resolve_sources(ctx)?, rows, None)
        };
        let original_rows_snapshot = original_rows.clone();
        let mut updated_rows_data = Vec::with_capacity(original_rows.len());
        let mut updated_rows = 0usize;
        let mut subquery_cache = SubqueryCache::new();
        let row_contexts = joined_rows.as_deref();

        for (index, stored_row) in original_rows.into_iter().enumerate() {
            let mut updated_row = stored_row.values.clone();
            for assignment in &self.statement.assignments {
                let column_index = table.get_column_index(&assignment.column).ok_or_else(|| {
                    HematiteError::ParseError(format!(
                        "Column '{}' does not exist in table '{}'",
                        assignment.column, self.statement.table
                    ))
                })?;
                let column = &table.columns[column_index];
                let value = {
                    let evaluation_row = row_contexts
                        .and_then(|rows| rows.get(index).map(Vec::as_slice))
                        .unwrap_or(updated_row.as_slice());
                    select_executor.evaluate_expression(
                        ctx,
                        &mut subquery_cache,
                        &sources,
                        &assignment.value,
                        evaluation_row,
                    )?
                };
                updated_row[column_index] = coerce_column_value(column, value)?;
            }

            table
                .validate_row(&updated_row)
                .map_err(|err| HematiteError::ParseError(err.to_string()))?;
            validate_row_constraints(ctx, &table, &updated_row)?;
            if parent_reference_key_changed(ctx, &table, &stored_row.values, &updated_row)? {
                apply_parent_update_foreign_key_actions(
                    ctx,
                    &table,
                    &stored_row.values,
                    &updated_row,
                )?;
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

        for row in &updated_rows_data {
            write_stored_row(ctx, &self.statement.table, &table, row.clone(), true)?;
        }

        for (old_row, new_row) in original_rows_snapshot
            .into_iter()
            .zip(updated_rows_data.into_iter())
        {
            ctx.mutation_events.push(MutationEvent::Update {
                table_name: self.statement.table.clone(),
                old_row,
                new_row,
            });
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
    let mut subquery_cache = SubqueryCache::new();
    let sources = select_executor.resolve_sources(ctx)?;

    for rowid in rowids {
        if table_cursor.seek_rowid(rowid) {
            if let Some(row) = table_cursor.current() {
                let row = row.clone();
                let include = match &select_executor.statement.where_clause {
                    Some(where_clause) => select_executor.conditions_match(
                        ctx,
                        &mut subquery_cache,
                        &sources,
                        &where_clause.conditions,
                        &row.values,
                    )?,
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

fn locate_rows_for_join_source(
    ctx: &mut ExecutionContext<'_>,
    table: &Table,
    target_binding: &str,
    select_executor: &mut SelectExecutor,
) -> Result<(Vec<ResolvedSource>, Vec<(StoredRow, Vec<Value>)>)> {
    let (sources, joined_rows) = select_executor.materialize_filtered_rows(ctx)?;
    let target_source = sources
        .iter()
        .find(|source| {
            source.name.eq_ignore_ascii_case(&table.name)
                && source
                    .alias
                    .as_deref()
                    .unwrap_or(&source.name)
                    .eq_ignore_ascii_case(target_binding)
        })
        .ok_or_else(|| {
            HematiteError::ParseError(format!(
                "Mutation target '{}' does not resolve to table '{}'",
                target_binding, table.name
            ))
        })?;
    let mut seen_rowids = std::collections::HashSet::new();
    let mut rows = Vec::new();

    for joined_row in joined_rows {
        let Some(candidate_rowid) =
            target_rowid_from_join_row(ctx, table, target_source, &joined_row)?
        else {
            continue;
        };

        if !seen_rowids.insert(candidate_rowid) {
            continue;
        }

        if let Some(stored_row) = ctx
            .engine
            .lookup_row_by_rowid(&table.name, candidate_rowid)?
        {
            rows.push((stored_row, joined_row));
        }
    }

    Ok((sources, rows))
}

fn target_rowid_from_join_row(
    ctx: &mut ExecutionContext<'_>,
    table: &Table,
    target_source: &ResolvedSource,
    joined_row: &[Value],
) -> Result<Option<u64>> {
    let mut primary_key = Vec::with_capacity(table.primary_key_columns.len());
    for &column_index in &table.primary_key_columns {
        let value = joined_row
            .get(target_source.offset + column_index)
            .cloned()
            .unwrap_or(Value::Null);
        if value.is_null() {
            return Ok(None);
        }
        primary_key.push(value);
    }

    ctx.engine.lookup_primary_key_rowid(table, &primary_key)
}

impl QueryExecutor for DeleteExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext) -> Result<QueryResult> {
        validate_statement(&Statement::Delete(self.statement.clone()), &ctx.catalog)?;

        let table = catalog_table(ctx, &self.statement.table)?;

        let locator_statement =
            locator_select_statement(self.statement.source(), self.statement.where_clause.clone());
        let mut select_executor = SelectExecutor::new(locator_statement, self.access_path.clone());
        let uses_join_source = matches!(
            self.statement.source.as_ref(),
            Some(source) if !matches!(source, TableReference::Table(_, _))
        );

        let rows_to_delete = if uses_join_source {
            let (_, rows) = locate_rows_for_join_source(
                ctx,
                &table,
                self.statement.target_binding_name(),
                &mut select_executor,
            )?;
            rows.into_iter().map(|(row, _)| row).collect()
        } else {
            locate_rows_for_access_path(
                ctx,
                &table,
                &self.statement.table,
                &self.access_path,
                &select_executor,
            )?
        };

        for row in &rows_to_delete {
            apply_parent_delete_foreign_key_actions(ctx, &table, &row.values)?;
            ctx.mutation_events.push(MutationEvent::Delete {
                table_name: self.statement.table.clone(),
                old_row: row.clone(),
            });
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
                lower_type_name(col_def.data_type.clone()),
            )
            .character_set(col_def.character_set.clone())
            .collation(col_def.collation.clone())
            .nullable(col_def.nullable)
            .primary_key(col_def.primary_key)
            .auto_increment(col_def.auto_increment);

            if let Some(default_val) = &col_def.default_value {
                let coerced_default =
                    coerce_column_value(&column, lower_literal_value(default_val))?;
                column = column.default_value(coerced_default);
            }

            columns.push(column);
            next_id += 1;
        }

        Ok(columns)
    }

    fn unique_index_specs(&self) -> Result<Vec<(String, Vec<usize>)>> {
        let mut unique_indexes = self
            .statement
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
            .collect::<Vec<_>>();

        for (position, unique) in
            self.statement
                .constraints
                .iter()
                .enumerate()
                .filter_map(|(position, constraint)| match constraint {
                    TableConstraint::Unique(unique) => Some((position, unique)),
                    TableConstraint::Check(_) | TableConstraint::ForeignKey(_) => None,
                })
        {
            let column_indices = unique
                .columns
                .iter()
                .map(|column_name| {
                    self.statement
                        .columns
                        .iter()
                        .position(|column| column.name == *column_name)
                        .ok_or_else(|| {
                            HematiteError::ParseError(format!(
                                "UNIQUE constraint column '{}' does not exist in table '{}'",
                                column_name, self.statement.table
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            unique_indexes.push((
                unique_constraint_index_name(&self.statement.table, unique, position),
                column_indices,
            ));
        }

        Ok(unique_indexes)
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
                        TableConstraint::Unique(_) | TableConstraint::ForeignKey(_) => None,
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
                        TableConstraint::Check(_) | TableConstraint::Unique(_) => None,
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
        let column_indices = foreign_key
            .columns
            .iter()
            .map(|column_name| {
                table.get_column_index(column_name).ok_or_else(|| {
                    HematiteError::ParseError(format!(
                        "Foreign key column '{}' does not exist in table '{}'",
                        column_name, table.name
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ForeignKeyConstraint {
            name: foreign_key.name.clone(),
            column_indices,
            referenced_table: foreign_key.referenced_table.clone(),
            referenced_columns: foreign_key.referenced_columns.clone(),
            on_delete: convert_foreign_key_action(foreign_key.on_delete),
            on_update: convert_foreign_key_action(foreign_key.on_update),
        })
    }
}

struct CreateConstraints {
    check_constraints: Vec<CheckConstraint>,
    foreign_keys: Vec<ForeignKeyConstraint>,
}

impl QueryExecutor for CreateExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext) -> Result<QueryResult> {
        validate_statement(&Statement::Create(self.statement.clone()), &ctx.catalog)?;
        if self.statement.if_not_exists
            && ctx
                .catalog
                .get_table_by_name(&self.statement.table)
                .is_some()
        {
            return Ok(mutation_result(0));
        }

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
        for (index_name, column_indices) in self.unique_index_specs()? {
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
        validate_statement(&Statement::Drop(self.statement.clone()), &ctx.catalog)?;
        if self.statement.if_exists
            && ctx
                .catalog
                .get_table_by_name(&self.statement.table)
                .is_none()
        {
            return Ok(mutation_result(0));
        }

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
        validate_statement(&Statement::Alter(self.statement.clone()), &ctx.catalog)?;

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
                    lower_type_name(column_def.data_type.clone()),
                )
                .character_set(column_def.character_set.clone())
                .collation(column_def.collation.clone())
                .nullable(column_def.nullable)
                .primary_key(column_def.primary_key);
                let column = if let Some(default_value) = &column_def.default_value {
                    let coerced_default =
                        coerce_column_value(&column, lower_literal_value(default_value))?;
                    column.default_value(coerced_default)
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
            AlterOperation::AddConstraint(constraint) => {
                let table = catalog_table(ctx, &self.statement.table)?;
                match constraint {
                    TableConstraint::Check(check) => {
                        ctx.catalog.add_check_constraint(
                            table.id,
                            CheckConstraint {
                                name: check.name.clone(),
                                expression_sql: check.expression_sql.clone(),
                            },
                        )?;
                    }
                    TableConstraint::Unique(unique) => {
                        let root_page_id = ctx.engine.create_empty_btree()?;
                        let column_indices = unique
                            .columns
                            .iter()
                            .map(|column_name| {
                                table.get_column_index(column_name).ok_or_else(|| {
                                    HematiteError::ParseError(format!(
                                        "UNIQUE constraint column '{}' does not exist in table '{}'",
                                        column_name, self.statement.table
                                    ))
                                })
                            })
                            .collect::<Result<Vec<_>>>()?;
                        ctx.catalog.add_secondary_index(
                            table.id,
                            crate::catalog::SecondaryIndex {
                                name: unique.name.clone().ok_or_else(|| {
                                    HematiteError::InternalError(
                                        "validated UNIQUE constraint lost its name".to_string(),
                                    )
                                })?,
                                column_indices,
                                root_page_id,
                                unique: true,
                            },
                        )?;
                        let updated_table = ctx
                            .catalog
                            .get_table(table.id)
                            .ok_or_else(|| {
                                table_disappeared_internal_error(
                                    &self.statement.table,
                                    "adding unique constraint",
                                )
                            })?
                            .clone();
                        let rows = ctx.engine.read_rows_with_ids(&self.statement.table)?;
                        ctx.engine
                            .rebuild_secondary_indexes(&updated_table, &rows)?;
                    }
                    TableConstraint::ForeignKey(foreign_key) => {
                        let column_indices = foreign_key
                            .columns
                            .iter()
                            .map(|column_name| {
                                table.get_column_index(column_name).ok_or_else(|| {
                                    HematiteError::ParseError(format!(
                                        "Foreign key column '{}' does not exist in table '{}'",
                                        column_name, self.statement.table
                                    ))
                                })
                            })
                            .collect::<Result<Vec<_>>>()?;
                        ctx.catalog.add_foreign_key(
                            table.id,
                            ForeignKeyConstraint {
                                name: foreign_key.name.clone(),
                                column_indices,
                                referenced_table: foreign_key.referenced_table.clone(),
                                referenced_columns: foreign_key.referenced_columns.clone(),
                                on_delete: convert_foreign_key_action(foreign_key.on_delete),
                                on_update: convert_foreign_key_action(foreign_key.on_update),
                            },
                        )?;
                    }
                }
            }
            AlterOperation::DropColumn(column_name) => {
                let table = catalog_table(ctx, &self.statement.table)?;
                let column_index = table.get_column_index(column_name).ok_or_else(|| {
                    HematiteError::InternalError(format!(
                        "Column '{}' disappeared during ALTER TABLE DROP COLUMN",
                        column_name
                    ))
                })?;
                let mut rows = ctx.engine.read_rows_with_ids(&self.statement.table)?;
                for row in &mut rows {
                    row.values.remove(column_index);
                }

                ctx.catalog.drop_column(table.id, column_name)?;
                ctx.engine.replace_table_rows(&self.statement.table, rows)?;
            }
            AlterOperation::DropConstraint(constraint_name) => {
                let table = catalog_table(ctx, &self.statement.table)?;
                if let Some(index) = table.get_secondary_index(constraint_name) {
                    if index.unique {
                        ctx.engine.delete_tree(index.root_page_id)?;
                        ctx.catalog
                            .drop_secondary_index(table.id, constraint_name)?;
                    } else {
                        return Err(HematiteError::ParseError(format!(
                            "Constraint '{}' is not a droppable UNIQUE constraint",
                            constraint_name
                        )));
                    }
                } else {
                    ctx.catalog
                        .drop_named_constraint(table.id, constraint_name)?;
                }
            }
            AlterOperation::AlterColumnSetDefault {
                column_name,
                default_value,
            } => {
                let table = catalog_table(ctx, &self.statement.table)?;
                let column = table.get_column_by_name(column_name).ok_or_else(|| {
                    HematiteError::ParseError(format!(
                        "Column '{}' does not exist in table '{}'",
                        column_name, self.statement.table
                    ))
                })?;
                ctx.catalog.set_column_default(
                    table.id,
                    column_name,
                    Some(coerce_column_value(
                        column,
                        lower_literal_value(default_value),
                    )?),
                )?;
            }
            AlterOperation::AlterColumnDropDefault { column_name } => {
                let table = catalog_table(ctx, &self.statement.table)?;
                ctx.catalog
                    .set_column_default(table.id, column_name, None)?;
            }
            AlterOperation::AlterColumnSetNotNull { column_name } => {
                let table = catalog_table(ctx, &self.statement.table)?;
                let column_index = table.get_column_index(column_name).ok_or_else(|| {
                    HematiteError::InternalError(format!(
                        "Column '{}' disappeared during ALTER COLUMN SET NOT NULL",
                        column_name
                    ))
                })?;
                let rows = ctx.engine.read_from_table(&self.statement.table)?;
                if rows
                    .iter()
                    .any(|row| row.get(column_index).is_some_and(Value::is_null))
                {
                    return Err(HematiteError::ParseError(format!(
                        "Cannot set column '{}' to NOT NULL because existing rows contain NULL",
                        column_name
                    )));
                }
                ctx.catalog
                    .set_column_nullable(table.id, column_name, false)?;
            }
            AlterOperation::AlterColumnDropNotNull { column_name } => {
                let table = catalog_table(ctx, &self.statement.table)?;
                ctx.catalog
                    .set_column_nullable(table.id, column_name, true)?;
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
        validate_statement(
            &Statement::CreateIndex(self.statement.clone()),
            &ctx.catalog,
        )?;
        if self.statement.if_not_exists {
            if let Some(table) = ctx.catalog.get_table_by_name(&self.statement.table) {
                if table
                    .get_secondary_index(&self.statement.index_name)
                    .is_some()
                {
                    return Ok(mutation_result(0));
                }
            }
        }

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

fn current_table_row_counts(engine: &crate::catalog::CatalogEngine) -> HashMap<String, usize> {
    engine
        .get_table_metadata()
        .iter()
        .map(|(name, metadata)| (name.clone(), metadata.row_count as usize))
        .collect()
}

fn apply_set_operation(
    operator: SetOperator,
    mut left_rows: Vec<Vec<Value>>,
    right_rows: Vec<Vec<Value>>,
) -> Vec<Vec<Value>> {
    match operator {
        SetOperator::UnionAll => {
            left_rows.extend(right_rows);
            left_rows
        }
        SetOperator::Union => {
            left_rows.extend(right_rows);
            apply_distinct_if_needed(true, &mut left_rows);
            left_rows
        }
        SetOperator::Intersect => {
            apply_distinct_if_needed(true, &mut left_rows);
            let mut distinct_right = right_rows;
            apply_distinct_if_needed(true, &mut distinct_right);
            left_rows
                .into_iter()
                .filter(|row| distinct_right.contains(row))
                .collect()
        }
        SetOperator::Except => {
            apply_distinct_if_needed(true, &mut left_rows);
            let mut distinct_right = right_rows;
            apply_distinct_if_needed(true, &mut distinct_right);
            left_rows
                .into_iter()
                .filter(|row| !distinct_right.contains(row))
                .collect()
        }
    }
}

fn primary_key_values(table: &Table, row: &[Value]) -> Result<Vec<Value>> {
    table.get_primary_key_values(row).map_err(|err| {
        HematiteError::ParseError(format!("Failed to extract primary key values: {}", err))
    })
}

fn out_of_range_error(column: &Column, type_name: &str) -> HematiteError {
    HematiteError::ParseError(format!(
        "Type mismatch: column '{}' expects {}, got out-of-range value",
        column.name, type_name
    ))
}

fn coerce_varchar_value(value: String, max_chars: u32, label: &str) -> Result<Value> {
    if value.chars().count() > max_chars as usize {
        return Err(HematiteError::ParseError(format!(
            "{} exceeds declared character length {}",
            label, max_chars
        )));
    }
    Ok(Value::Text(value))
}

fn coerce_char_value(value: String, length: u32, label: &str) -> Result<Value> {
    if value.chars().count() > length as usize {
        return Err(HematiteError::ParseError(format!(
            "{} exceeds declared character length {}",
            label, length
        )));
    }
    Ok(Value::Text(pad_text_to_char_length(&value, length)))
}

fn cast_value_to_text_string(value: Value) -> Result<String> {
    match value {
        Value::Integer(value) => Ok(value.to_string()),
        Value::BigInt(value) => Ok(value.to_string()),
        Value::Int128(value) => Ok(value.to_string()),
        Value::UInteger(value) => Ok(value.to_string()),
        Value::UBigInt(value) => Ok(value.to_string()),
        Value::UInt128(value) => Ok(value.to_string()),
        Value::Float32(value) => Ok(value.to_string()),
        Value::Float(value) => Ok(value.to_string()),
        Value::Float128(value) => Ok(value.to_string()),
        Value::Enum(value) | Value::Text(value) => Ok(value),
        Value::Boolean(true) => Ok("TRUE".to_string()),
        Value::Boolean(false) => Ok("FALSE".to_string()),
        Value::Decimal(value) => Ok(value.to_string()),
        Value::Date(value) => Ok(value.to_string()),
        Value::Time(value) => Ok(value.to_string()),
        Value::DateTime(value) => Ok(value.to_string()),
        Value::TimeWithTimeZone(value) => Ok(value.to_string()),
        Value::Blob(value) => Ok(String::from_utf8_lossy(&value).into_owned()),
        Value::Null => Err(HematiteError::ParseError(
            "Cannot CAST NULL to text without preserving NULL".to_string(),
        )),
        Value::IntervalYearMonth(value) => Ok(value.to_string()),
        Value::IntervalDaySecond(value) => Ok(value.to_string()),
    }
}

fn coerce_binary_value(value: Value, max_len: u32, label: &str, fixed: bool) -> Result<Value> {
    let mut bytes = match value {
        Value::Blob(bytes) => bytes,
        Value::Text(value) => value.into_bytes(),
        Value::Enum(value) => value.into_bytes(),
        value => {
            return Err(HematiteError::ParseError(format!(
                "Expected binary-compatible value for {}, found {:?}",
                label, value
            )))
        }
    };

    if bytes.len() > max_len as usize {
        return Err(HematiteError::ParseError(format!(
            "{} exceeds declared byte length {}",
            label, max_len
        )));
    }
    if fixed {
        bytes.resize(max_len as usize, 0);
    }
    Ok(Value::Blob(bytes))
}

fn coerce_decimal_value(value: Value) -> Result<DecimalValue> {
    match value {
        Value::Decimal(value) => Ok(value),
        Value::Integer(value) => Ok(DecimalValue::from_i32(value)),
        Value::BigInt(value) => Ok(DecimalValue::from_i64(value)),
        Value::Int128(value) => Ok(DecimalValue::from_i128(value)),
        Value::UInteger(value) => Ok(DecimalValue::from_u32(value)),
        Value::UBigInt(value) => Ok(DecimalValue::from_u64(value)),
        Value::UInt128(value) => Ok(DecimalValue::from_u128(value)),
        Value::Float32(value) => DecimalValue::from_f64(value as f64),
        Value::Float(value) => DecimalValue::from_f64(value),
        Value::Float128(value) => value.to_decimal(),
        Value::Text(value) => DecimalValue::parse(&value),
        value => Err(HematiteError::ParseError(format!(
            "Expected DECIMAL-compatible value, found {:?}",
            value
        ))),
    }
}

fn numeric_value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Integer(value) => Some(*value as f64),
        Value::BigInt(value) => Some(*value as f64),
        Value::Int128(value) => Some(*value as f64),
        Value::UInteger(value) => Some(*value as f64),
        Value::UBigInt(value) => Some(*value as f64),
        Value::UInt128(value) => Some(*value as f64),
        Value::Float32(value) => Some(*value as f64),
        Value::Float(value) => Some(*value),
        Value::Float128(value) => value.to_f64().ok(),
        _ => None,
    }
}

fn numeric_value_as_float128(value: &Value) -> Option<Float128Value> {
    match value {
        Value::Integer(value) => Some(Float128Value::from_integer((*value).into())),
        Value::BigInt(value) => Some(Float128Value::from_integer((*value).into())),
        Value::Int128(value) => Some(Float128Value::from_integer(*value)),
        Value::UInteger(value) => Some(Float128Value::from_unsigned((*value).into())),
        Value::UBigInt(value) => Some(Float128Value::from_unsigned((*value).into())),
        Value::UInt128(value) => Some(Float128Value::from_unsigned(*value)),
        Value::Float32(value) => Float128Value::from_f64(*value as f64).ok(),
        Value::Float(value) => Float128Value::from_f64(*value).ok(),
        Value::Float128(value) => Some(value.clone()),
        _ => None,
    }
}

fn make_float_value(data_type: &DataType, value: f64) -> Value {
    match data_type {
        DataType::Float32 => Value::Float32(value as f32),
        DataType::Float => Value::Float(value),
        DataType::Float128 => Value::Float128(
            Float128Value::from_f64(value).expect("finite FLOAT value should convert to FLOAT128"),
        ),
        _ => unreachable!("non-float type used for float value construction"),
    }
}

fn float_type_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Float32 => "FLOAT32",
        DataType::Float => "FLOAT",
        DataType::Float128 => "FLOAT128",
        _ => unreachable!("non-float type used for float naming"),
    }
}

fn coerce_enum_value(value: Value, variants: &[String], label: &str) -> Result<Value> {
    let value = match value {
        Value::Enum(value) | Value::Text(value) => value,
        value => {
            return Err(HematiteError::ParseError(format!(
                "Expected ENUM-compatible value for {}, found {:?}",
                label, value
            )))
        }
    };

    if !variants.contains(&value) {
        return Err(HematiteError::ParseError(format!(
            "{} is not a valid ENUM variant",
            value
        )));
    }

    Ok(Value::Enum(value))
}

fn coerce_column_value(column: &Column, value: Value) -> Result<Value> {
    match (&column.data_type, value) {
        (DataType::Int8, Value::Integer(i)) => i8::try_from(i)
            .map(|_| Value::Integer(i))
            .map_err(|_| out_of_range_error(column, "INT8")),
        (DataType::Int8, Value::BigInt(i)) => i8::try_from(i)
            .map(|value| Value::Integer(value as i32))
            .map_err(|_| out_of_range_error(column, "INT8")),
        (DataType::Int8, Value::Int128(i)) => i8::try_from(i)
            .map(|value| Value::Integer(value as i32))
            .map_err(|_| out_of_range_error(column, "INT8")),
        (DataType::Int8, Value::UInteger(i)) => i8::try_from(i)
            .map(|value| Value::Integer(value as i32))
            .map_err(|_| out_of_range_error(column, "INT8")),
        (DataType::Int8, Value::UBigInt(i)) => i8::try_from(i)
            .map(|value| Value::Integer(value as i32))
            .map_err(|_| out_of_range_error(column, "INT8")),
        (DataType::Int8, Value::UInt128(i)) => i8::try_from(i)
            .map(|value| Value::Integer(value as i32))
            .map_err(|_| out_of_range_error(column, "INT8")),
        (DataType::Int16, Value::Integer(i)) => i16::try_from(i)
            .map(|_| Value::Integer(i))
            .map_err(|_| out_of_range_error(column, "INT16")),
        (DataType::Int16, Value::BigInt(i)) => i16::try_from(i)
            .map(|value| Value::Integer(value as i32))
            .map_err(|_| out_of_range_error(column, "INT16")),
        (DataType::Int16, Value::Int128(i)) => i16::try_from(i)
            .map(|value| Value::Integer(value as i32))
            .map_err(|_| out_of_range_error(column, "INT16")),
        (DataType::Int16, Value::UInteger(i)) => i16::try_from(i)
            .map(|value| Value::Integer(value as i32))
            .map_err(|_| out_of_range_error(column, "INT16")),
        (DataType::Int16, Value::UBigInt(i)) => i16::try_from(i)
            .map(|value| Value::Integer(value as i32))
            .map_err(|_| out_of_range_error(column, "INT16")),
        (DataType::Int16, Value::UInt128(i)) => i16::try_from(i)
            .map(|value| Value::Integer(value as i32))
            .map_err(|_| out_of_range_error(column, "INT16")),
        (DataType::Int, Value::Integer(i)) => Ok(Value::Integer(i)),
        (DataType::Int, Value::BigInt(i)) => i32::try_from(i)
            .map(Value::Integer)
            .map_err(|_| out_of_range_error(column, "INT")),
        (DataType::Int, Value::Int128(i)) => i32::try_from(i)
            .map(Value::Integer)
            .map_err(|_| out_of_range_error(column, "INT")),
        (DataType::Int, Value::UInteger(i)) => i32::try_from(i)
            .map(Value::Integer)
            .map_err(|_| out_of_range_error(column, "INT")),
        (DataType::Int, Value::UBigInt(i)) => i32::try_from(i)
            .map(Value::Integer)
            .map_err(|_| out_of_range_error(column, "INT")),
        (DataType::Int, Value::UInt128(i)) => i32::try_from(i)
            .map(Value::Integer)
            .map_err(|_| out_of_range_error(column, "INT")),
        (DataType::Int64, Value::Integer(i)) => Ok(Value::BigInt(i as i64)),
        (DataType::Int64, Value::BigInt(i)) => Ok(Value::BigInt(i)),
        (DataType::Int64, Value::Int128(i)) => i64::try_from(i)
            .map(Value::BigInt)
            .map_err(|_| out_of_range_error(column, "INT64")),
        (DataType::Int64, Value::UInteger(i)) => Ok(Value::BigInt(i as i64)),
        (DataType::Int64, Value::UBigInt(i)) => i64::try_from(i)
            .map(Value::BigInt)
            .map_err(|_| out_of_range_error(column, "INT64")),
        (DataType::Int64, Value::UInt128(i)) => i64::try_from(i)
            .map(Value::BigInt)
            .map_err(|_| out_of_range_error(column, "INT64")),
        (DataType::Int128, Value::Integer(i)) => Ok(Value::Int128(i as i128)),
        (DataType::Int128, Value::BigInt(i)) => Ok(Value::Int128(i as i128)),
        (DataType::Int128, Value::Int128(i)) => Ok(Value::Int128(i)),
        (DataType::Int128, Value::UInteger(i)) => Ok(Value::Int128(i as i128)),
        (DataType::Int128, Value::UBigInt(i)) => Ok(Value::Int128(i as i128)),
        (DataType::Int128, Value::UInt128(i)) => i128::try_from(i)
            .map(Value::Int128)
            .map_err(|_| out_of_range_error(column, "INT128")),
        (DataType::UInt8, Value::Integer(i)) if i >= 0 => u8::try_from(i)
            .map(|value| Value::UInteger(value as u32))
            .map_err(|_| out_of_range_error(column, "UINT8")),
        (DataType::UInt8, Value::BigInt(i)) if i >= 0 => u8::try_from(i)
            .map(|value| Value::UInteger(value as u32))
            .map_err(|_| out_of_range_error(column, "UINT8")),
        (DataType::UInt8, Value::Int128(i)) if i >= 0 => u8::try_from(i)
            .map(|value| Value::UInteger(value as u32))
            .map_err(|_| out_of_range_error(column, "UINT8")),
        (DataType::UInt8, Value::UInteger(i)) => u8::try_from(i)
            .map(|value| Value::UInteger(value as u32))
            .map_err(|_| out_of_range_error(column, "UINT8")),
        (DataType::UInt8, Value::UBigInt(i)) => u8::try_from(i)
            .map(|value| Value::UInteger(value as u32))
            .map_err(|_| out_of_range_error(column, "UINT8")),
        (DataType::UInt8, Value::UInt128(i)) => u8::try_from(i)
            .map(|value| Value::UInteger(value as u32))
            .map_err(|_| out_of_range_error(column, "UINT8")),
        (DataType::UInt16, Value::Integer(i)) if i >= 0 => u16::try_from(i)
            .map(|value| Value::UInteger(value as u32))
            .map_err(|_| out_of_range_error(column, "UINT16")),
        (DataType::UInt16, Value::BigInt(i)) if i >= 0 => u16::try_from(i)
            .map(|value| Value::UInteger(value as u32))
            .map_err(|_| out_of_range_error(column, "UINT16")),
        (DataType::UInt16, Value::Int128(i)) if i >= 0 => u16::try_from(i)
            .map(|value| Value::UInteger(value as u32))
            .map_err(|_| out_of_range_error(column, "UINT16")),
        (DataType::UInt16, Value::UInteger(i)) => u16::try_from(i)
            .map(|value| Value::UInteger(value as u32))
            .map_err(|_| out_of_range_error(column, "UINT16")),
        (DataType::UInt16, Value::UBigInt(i)) => u16::try_from(i)
            .map(|value| Value::UInteger(value as u32))
            .map_err(|_| out_of_range_error(column, "UINT16")),
        (DataType::UInt16, Value::UInt128(i)) => u16::try_from(i)
            .map(|value| Value::UInteger(value as u32))
            .map_err(|_| out_of_range_error(column, "UINT16")),
        (DataType::UInt, Value::Integer(i)) if i >= 0 => Ok(Value::UInteger(i as u32)),
        (DataType::UInt, Value::BigInt(i)) if i >= 0 => u32::try_from(i)
            .map(Value::UInteger)
            .map_err(|_| out_of_range_error(column, "UINT")),
        (DataType::UInt, Value::Int128(i)) if i >= 0 => u32::try_from(i)
            .map(Value::UInteger)
            .map_err(|_| out_of_range_error(column, "UINT")),
        (DataType::UInt, Value::UInteger(i)) => Ok(Value::UInteger(i)),
        (DataType::UInt, Value::UBigInt(i)) => u32::try_from(i)
            .map(Value::UInteger)
            .map_err(|_| out_of_range_error(column, "UINT")),
        (DataType::UInt, Value::UInt128(i)) => u32::try_from(i)
            .map(Value::UInteger)
            .map_err(|_| out_of_range_error(column, "UINT")),
        (DataType::UInt64, Value::Integer(i)) if i >= 0 => Ok(Value::UBigInt(i as u64)),
        (DataType::UInt64, Value::BigInt(i)) if i >= 0 => Ok(Value::UBigInt(i as u64)),
        (DataType::UInt64, Value::Int128(i)) if i >= 0 => u64::try_from(i)
            .map(Value::UBigInt)
            .map_err(|_| out_of_range_error(column, "UINT64")),
        (DataType::UInt64, Value::UInteger(i)) => Ok(Value::UBigInt(i as u64)),
        (DataType::UInt64, Value::UBigInt(i)) => Ok(Value::UBigInt(i)),
        (DataType::UInt64, Value::UInt128(i)) => u64::try_from(i)
            .map(Value::UBigInt)
            .map_err(|_| out_of_range_error(column, "UINT64")),
        (DataType::UInt128, Value::Integer(i)) if i >= 0 => Ok(Value::UInt128(i as u128)),
        (DataType::UInt128, Value::BigInt(i)) if i >= 0 => Ok(Value::UInt128(i as u128)),
        (DataType::UInt128, Value::Int128(i)) if i >= 0 => Ok(Value::UInt128(i as u128)),
        (DataType::UInt128, Value::UInteger(i)) => Ok(Value::UInt128(i as u128)),
        (DataType::UInt128, Value::UBigInt(i)) => Ok(Value::UInt128(i as u128)),
        (DataType::UInt128, Value::UInt128(i)) => Ok(Value::UInt128(i)),
        (_, Value::Null) if column.nullable => Ok(Value::Null),
        (_, Value::Null) => Err(HematiteError::ParseError(format!(
            "Column '{}' cannot be NULL",
            column.name
        ))),
        (DataType::Text, Value::Text(s)) => Ok(Value::Text(s)),
        (DataType::Char(length), Value::Text(s)) => coerce_char_value(s, *length, &column.name),
        (DataType::VarChar(length), Value::Text(s)) => {
            coerce_varchar_value(s, *length, &column.name)
        }
        (DataType::Binary(length), value) => {
            coerce_binary_value(value, *length, &column.name, true)
        }
        (DataType::VarBinary(length), value) => {
            coerce_binary_value(value, *length, &column.name, false)
        }
        (DataType::Enum(values), value) => coerce_enum_value(value, values, &column.name),
        (DataType::Boolean, Value::Boolean(b)) => Ok(Value::Boolean(b)),
        (DataType::Float128, value) => {
            let Some(number) = numeric_value_as_float128(&value) else {
                return Err(HematiteError::ParseError(format!(
                    "Type mismatch: column '{}' expects {:?}, got {:?}",
                    column.name, column.data_type, value
                )));
            };
            Ok(Value::Float128(number))
        }
        (data_type @ (DataType::Float32 | DataType::Float), value) => {
            let Some(number) = numeric_value_as_f64(&value) else {
                return Err(HematiteError::ParseError(format!(
                    "Type mismatch: column '{}' expects {:?}, got {:?}",
                    column.name, column.data_type, value
                )));
            };
            Ok(make_float_value(data_type, number))
        }
        (DataType::Decimal { precision, scale }, value) => {
            let decimal = coerce_decimal_value(value)?;
            if !decimal.fits_precision_scale(*precision, *scale) {
                return Err(HematiteError::ParseError(format!(
                    "Type mismatch: column '{}' exceeds {} precision/scale",
                    column.name,
                    column.data_type.base_name()
                )));
            }
            Ok(Value::Decimal(decimal))
        }
        (DataType::Blob, Value::Blob(bytes)) => Ok(Value::Blob(bytes)),
        (DataType::Blob, Value::Text(s)) => Ok(Value::Blob(s.into_bytes())),
        (DataType::Blob, Value::UInteger(i)) => Ok(Value::Blob(i.to_le_bytes().to_vec())),
        (DataType::Blob, Value::UBigInt(i)) => Ok(Value::Blob(i.to_le_bytes().to_vec())),
        (DataType::Blob, Value::Int128(i)) => Ok(Value::Blob(i.to_le_bytes().to_vec())),
        (DataType::Blob, Value::UInt128(i)) => Ok(Value::Blob(i.to_le_bytes().to_vec())),
        (DataType::Date, Value::Date(s)) => Ok(Value::Date(s)),
        (DataType::Date, Value::Text(s)) => Ok(Value::Date(validate_date_string(&s)?)),
        (DataType::Time, Value::Time(s)) => Ok(Value::Time(s)),
        (DataType::Time, Value::Text(s)) => Ok(Value::Time(validate_time_string(&s)?)),
        (DataType::DateTime, Value::DateTime(s)) => Ok(Value::DateTime(s)),
        (DataType::DateTime, Value::Text(s)) => Ok(Value::DateTime(validate_datetime_string(&s)?)),
        (DataType::TimeWithTimeZone, Value::TimeWithTimeZone(s)) => Ok(Value::TimeWithTimeZone(s)),
        (DataType::TimeWithTimeZone, Value::Text(s)) => Ok(Value::TimeWithTimeZone(
            validate_time_with_time_zone_string(&s)?,
        )),
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
        locator_select_statement(TableReference::Table(table.name.clone(), None), None),
        SelectAccessPath::FullTableScan,
    );
    let sources = constraint_executor.resolve_sources(ctx)?;
    let mut subquery_cache = SubqueryCache::new();

    for constraint in &table.check_constraints {
        let condition =
            crate::parser::parser::parse_condition_fragment(&constraint.expression_sql)?;
        let result = constraint_executor.evaluate_condition(
            ctx,
            &mut subquery_cache,
            &sources,
            &condition,
            row,
        )?;
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
    validate_foreign_keys(ctx, table, row, None)
}

fn validate_foreign_keys(
    ctx: &mut ExecutionContext<'_>,
    table: &Table,
    row: &[Value],
    skip_constraint: Option<&ForeignKeyConstraint>,
) -> Result<()> {
    for foreign_key in &table.foreign_keys {
        if skip_constraint == Some(foreign_key) {
            continue;
        }
        let key_values = foreign_key_values_for_row(table, foreign_key, row)?;
        if key_values.iter().any(Value::is_null) {
            continue;
        }
        let referenced_target = resolve_foreign_key_target(ctx, foreign_key)?;
        if !referenced_key_exists(ctx, &referenced_target, &key_values)? {
            return Err(HematiteError::ParseError(format!(
                "Foreign key constraint '{}' failed on table '{}': '{}.{:?}' does not contain {:?}",
                foreign_key_constraint_name(foreign_key),
                table.name,
                foreign_key.referenced_table,
                foreign_key.referenced_columns,
                key_values
            )));
        }
    }

    Ok(())
}

struct ResolvedForeignKeyTarget {
    table: Table,
    unique_index_name: Option<String>,
}

fn resolve_foreign_key_target(
    ctx: &ExecutionContext<'_>,
    foreign_key: &ForeignKeyConstraint,
) -> Result<ResolvedForeignKeyTarget> {
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
    let referenced_column_indices = foreign_key
        .referenced_columns
        .iter()
        .map(|column| {
            referenced_table.get_column_index(column).ok_or_else(|| {
                HematiteError::ParseError(format!(
                    "Referenced column '{}.{}' not found",
                    foreign_key.referenced_table, column
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let unique_index_name = if referenced_table.primary_key_columns == referenced_column_indices {
        None
    } else {
        Some(
            referenced_table
                .secondary_indexes
                .iter()
                .find(|index| index.unique && index.column_indices == referenced_column_indices)
                .ok_or_else(|| {
                    HematiteError::CorruptedData(format!(
                        "Referenced columns '{}.{:?}' are no longer backed by a PRIMARY KEY or UNIQUE index",
                        foreign_key.referenced_table, foreign_key.referenced_columns
                    ))
                })?
                .name
                .clone(),
        )
    };

    Ok(ResolvedForeignKeyTarget {
        table: referenced_table,
        unique_index_name,
    })
}

fn referenced_key_exists(
    ctx: &mut ExecutionContext<'_>,
    target: &ResolvedForeignKeyTarget,
    key_values: &[Value],
) -> Result<bool> {
    if target.unique_index_name.is_none() {
        return Ok(ctx
            .engine
            .lookup_row_by_primary_key(&target.table, key_values)?
            .is_some());
    }

    Ok(!ctx
        .engine
        .lookup_secondary_index_rowids(
            &target.table,
            target
                .unique_index_name
                .as_deref()
                .expect("non-primary target must carry a unique index name"),
            key_values,
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
            let referenced_column_indices = foreign_key
                .referenced_columns
                .iter()
                .map(|column| {
                    parent_table.get_column_index(column).ok_or_else(|| {
                        HematiteError::CorruptedData(format!(
                            "Referenced column '{}.{}' is missing",
                            foreign_key.referenced_table, column
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            references.push(ReferencingForeignKey {
                child_table: child_table.clone(),
                foreign_key: foreign_key.clone(),
                referenced_column_indices,
            });
        }
    }

    Ok(references)
}

fn parent_reference_key_changed(
    ctx: &mut ExecutionContext<'_>,
    parent_table: &Table,
    original_row: &[Value],
    updated_row: &[Value],
) -> Result<bool> {
    for reference in referencing_foreign_keys(ctx, parent_table)? {
        if parent_key_for_reference(parent_table, &reference, original_row)?
            != parent_key_for_reference(parent_table, &reference, updated_row)?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn apply_parent_delete_foreign_key_actions(
    ctx: &mut ExecutionContext<'_>,
    parent_table: &Table,
    row: &[Value],
) -> Result<()> {
    for reference in referencing_foreign_keys(ctx, parent_table)? {
        let parent_key = parent_key_for_reference(parent_table, &reference, row)?;
        if parent_key.iter().any(Value::is_null) {
            continue;
        }
        let child_rows = child_rows_referencing_parent_key(
            ctx,
            &reference.child_table,
            &reference,
            &parent_key,
        )?;
        execute_parent_foreign_key_action(
            ctx,
            parent_table,
            &reference,
            child_rows,
            reference.foreign_key.on_delete,
            "delete",
            None,
        )?;
    }

    Ok(())
}

fn apply_parent_update_foreign_key_actions(
    ctx: &mut ExecutionContext<'_>,
    parent_table: &Table,
    original_row: &[Value],
    updated_row: &[Value],
) -> Result<()> {
    for reference in referencing_foreign_keys(ctx, parent_table)? {
        let old_parent_key = parent_key_for_reference(parent_table, &reference, original_row)?;
        let new_parent_key = parent_key_for_reference(parent_table, &reference, updated_row)?;
        if old_parent_key == new_parent_key || old_parent_key.iter().any(Value::is_null) {
            continue;
        }

        let child_rows = child_rows_referencing_parent_key(
            ctx,
            &reference.child_table,
            &reference,
            &old_parent_key,
        )?;
        execute_parent_foreign_key_action(
            ctx,
            parent_table,
            &reference,
            child_rows,
            reference.foreign_key.on_update,
            "update",
            Some(&new_parent_key),
        )?;
    }
    Ok(())
}

fn foreign_key_constraint_name(foreign_key: &ForeignKeyConstraint) -> &str {
    foreign_key
        .name
        .as_deref()
        .unwrap_or(foreign_key.referenced_table.as_str())
}

struct ReferencingForeignKey {
    child_table: Table,
    foreign_key: ForeignKeyConstraint,
    referenced_column_indices: Vec<usize>,
}

enum ChildKeyRewrite<'a> {
    Replace(&'a [Value]),
    SetNull,
}

fn foreign_key_values_for_row(
    table: &Table,
    foreign_key: &ForeignKeyConstraint,
    row: &[Value],
) -> Result<Vec<Value>> {
    row_values_for_indices(row, &foreign_key.column_indices, &table.name)
}

fn parent_key_for_reference(
    parent_table: &Table,
    reference: &ReferencingForeignKey,
    row: &[Value],
) -> Result<Vec<Value>> {
    row_values_for_indices(
        row,
        &reference.referenced_column_indices,
        &parent_table.name,
    )
}

fn execute_parent_foreign_key_action(
    ctx: &mut ExecutionContext<'_>,
    parent_table: &Table,
    reference: &ReferencingForeignKey,
    child_rows: Vec<StoredRow>,
    action: CatalogForeignKeyAction,
    operation: &str,
    replacement_key: Option<&[Value]>,
) -> Result<()> {
    match action {
        CatalogForeignKeyAction::Restrict => {
            if !child_rows.is_empty() {
                return Err(HematiteError::ParseError(format!(
                    "Cannot {} row in table '{}' because foreign key '{}' on table '{}' still references it",
                    operation,
                    parent_table.name,
                    foreign_key_constraint_name(&reference.foreign_key),
                    reference.child_table.name
                )));
            }
        }
        CatalogForeignKeyAction::Cascade => {
            if let Some(replacement_key) = replacement_key {
                rewrite_child_foreign_key_rows(
                    ctx,
                    &reference.child_table,
                    &reference.foreign_key,
                    child_rows,
                    ChildKeyRewrite::Replace(replacement_key),
                )?;
            } else {
                for child_row in child_rows {
                    remove_stored_row(
                        ctx,
                        &reference.child_table.name,
                        &reference.child_table,
                        child_row.row_id,
                    )?;
                }
            }
        }
        CatalogForeignKeyAction::SetNull => {
            rewrite_child_foreign_key_rows(
                ctx,
                &reference.child_table,
                &reference.foreign_key,
                child_rows,
                ChildKeyRewrite::SetNull,
            )?;
        }
    }

    Ok(())
}

fn row_values_for_indices(
    row: &[Value],
    indices: &[usize],
    table_name: &str,
) -> Result<Vec<Value>> {
    indices
        .iter()
        .map(|&index| {
            row.get(index).cloned().ok_or_else(|| {
                HematiteError::CorruptedData(format!(
                    "Column index {} is invalid for table '{}'",
                    index, table_name
                ))
            })
        })
        .collect()
}

fn child_rows_referencing_parent_key(
    ctx: &mut ExecutionContext<'_>,
    child_table: &Table,
    reference: &ReferencingForeignKey,
    parent_key: &[Value],
) -> Result<Vec<StoredRow>> {
    let mut matches = Vec::new();
    for child_row in ctx.engine.read_rows_with_ids(&child_table.name)? {
        let child_key =
            foreign_key_values_for_row(child_table, &reference.foreign_key, &child_row.values)?;
        if child_key.iter().any(Value::is_null) {
            continue;
        }
        if child_key == parent_key {
            matches.push(child_row);
        }
    }
    Ok(matches)
}

fn rewrite_child_foreign_key_rows(
    ctx: &mut ExecutionContext<'_>,
    table: &Table,
    foreign_key: &ForeignKeyConstraint,
    child_rows: Vec<StoredRow>,
    rewrite: ChildKeyRewrite<'_>,
) -> Result<()> {
    for mut child_row in child_rows {
        match rewrite {
            ChildKeyRewrite::Replace(replacement_key) => {
                for (position, &column_index) in foreign_key.column_indices.iter().enumerate() {
                    child_row.values[column_index] = replacement_key[position].clone();
                }
            }
            ChildKeyRewrite::SetNull => {
                for &column_index in &foreign_key.column_indices {
                    child_row.values[column_index] = Value::Null;
                }
            }
        }
        persist_foreign_key_child_update(ctx, table, foreign_key, child_row)?;
    }
    Ok(())
}

fn persist_foreign_key_child_update(
    ctx: &mut ExecutionContext<'_>,
    table: &Table,
    skipped_foreign_key: &ForeignKeyConstraint,
    row: StoredRow,
) -> Result<()> {
    table
        .validate_row(&row.values)
        .map_err(|err| HematiteError::ParseError(err.to_string()))?;
    validate_check_constraints(ctx, table, &row.values)?;
    validate_foreign_keys(ctx, table, &row.values, Some(skipped_foreign_key))?;
    ensure_stored_row_uniqueness(ctx, table, &row)?;
    remove_stored_row(ctx, &table.name, table, row.row_id)?;
    write_stored_row(ctx, &table.name, table, row, true).map(|_| ())
}

fn ensure_stored_row_uniqueness(
    ctx: &mut ExecutionContext<'_>,
    table: &Table,
    row: &StoredRow,
) -> Result<()> {
    let candidate_pk = primary_key_values(table, &row.values)?;
    if let Some(existing_rowid) = ctx.engine.lookup_primary_key_rowid(table, &candidate_pk)? {
        if existing_rowid != row.row_id {
            return Err(duplicate_primary_key_parse_error(
                &table.name,
                &candidate_pk,
            ));
        }
    }

    for index in table.secondary_indexes.iter().filter(|index| index.unique) {
        let key_values = secondary_index_key_values(index, &row.values);
        if ctx
            .engine
            .lookup_secondary_index_rowids(table, &index.name, &key_values)?
            .into_iter()
            .any(|existing_rowid| existing_rowid != row.row_id)
        {
            return Err(unique_index_parse_error(&index.name, &table.name));
        }
    }

    Ok(())
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

fn deduplicate_rows(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    apply_distinct_if_needed(true, &mut rows);
    rows
}

fn locator_select_statement(
    from: TableReference,
    where_clause: Option<WhereClause>,
) -> SelectStatement {
    SelectStatement {
        with_clause: Vec::new(),
        distinct: false,
        columns: vec![SelectItem::Wildcard],
        column_aliases: vec![None],
        from,
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

    if let Some(value) = evaluate_temporal_arithmetic(operator, &left, &right)? {
        return Ok(value);
    }

    if matches!(left, Value::Float128(_)) || matches!(right, Value::Float128(_)) {
        if let (Some(left), Some(right)) = (
            numeric_value_as_float128(&left),
            numeric_value_as_float128(&right),
        ) {
            return evaluate_float128_arithmetic(operator, left, right);
        }
    }

    if left.is_float_like() || right.is_float_like() {
        if let (Some(left), Some(right)) =
            (numeric_value_as_f64(&left), numeric_value_as_f64(&right))
        {
            return evaluate_float_arithmetic(operator, left, right);
        }
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
            ArithmeticOperator::Modulo => {
                if right == 0 {
                    Err(HematiteError::ParseError("Division by zero".to_string()))
                } else {
                    Ok(Value::Integer(left % right))
                }
            }
        },
        (left, right) => Err(HematiteError::ParseError(format!(
            "Arithmetic requires numeric values, found {:?} and {:?}",
            left, right
        ))),
    }
}

fn evaluate_temporal_arithmetic(
    operator: &ArithmeticOperator,
    left: &Value,
    right: &Value,
) -> Result<Option<Value>> {
    match (left, right) {
        (Value::IntervalYearMonth(left), Value::IntervalYearMonth(right)) => {
            let total_months = match operator {
                ArithmeticOperator::Add => left.total_months().checked_add(right.total_months()),
                ArithmeticOperator::Subtract => {
                    left.total_months().checked_sub(right.total_months())
                }
                _ => None,
            };
            Ok(total_months
                .map(|value| Value::IntervalYearMonth(IntervalYearMonthValue::new(value))))
        }
        (Value::IntervalDaySecond(left), Value::IntervalDaySecond(right)) => {
            let total_seconds = match operator {
                ArithmeticOperator::Add => left.total_seconds().checked_add(right.total_seconds()),
                ArithmeticOperator::Subtract => {
                    left.total_seconds().checked_sub(right.total_seconds())
                }
                _ => None,
            };
            Ok(total_seconds
                .map(|value| Value::IntervalDaySecond(IntervalDaySecondValue::new(value))))
        }
        (Value::Date(left), Value::Date(right))
            if matches!(operator, ArithmeticOperator::Subtract) =>
        {
            Ok(Some(Value::BigInt(
                left.days_since_epoch() as i64 - right.days_since_epoch() as i64,
            )))
        }
        (Value::Date(left), Value::IntervalYearMonth(interval)) => {
            let months = signed_interval_months(operator, *interval)?;
            Ok(Some(Value::Date(add_months_to_date(*left, months)?)))
        }
        (Value::Date(left), Value::IntervalDaySecond(interval)) => {
            let days = whole_days_from_interval(operator, *interval)?;
            let result = left.days_since_epoch() as i64 + days;
            let result = i32::try_from(result).map_err(|_| {
                HematiteError::ParseError("DATE arithmetic overflowed supported range".to_string())
            })?;
            Ok(Some(Value::Date(DateValue::from_days_since_epoch(result))))
        }
        (Value::Date(left), right) => {
            let Some(days) = integral_rhs(right) else {
                return Ok(None);
            };
            let delta = match operator {
                ArithmeticOperator::Add => days,
                ArithmeticOperator::Subtract => -days,
                _ => return Ok(None),
            };
            let result = left.days_since_epoch() as i64 + delta;
            let result = i32::try_from(result).map_err(|_| {
                HematiteError::ParseError("DATE arithmetic overflowed supported range".to_string())
            })?;
            Ok(Some(Value::Date(DateValue::from_days_since_epoch(result))))
        }

        (Value::DateTime(left), Value::DateTime(right))
            if matches!(operator, ArithmeticOperator::Subtract) =>
        {
            Ok(Some(Value::BigInt(
                left.seconds_since_epoch() - right.seconds_since_epoch(),
            )))
        }
        (Value::DateTime(left), Value::IntervalYearMonth(interval)) => Ok(Some(Value::DateTime(
            add_months_to_datetime(*left, signed_interval_months(operator, *interval)?)?,
        ))),
        (Value::DateTime(left), Value::IntervalDaySecond(interval)) => {
            let seconds = signed_interval_seconds(operator, *interval)?;
            Ok(Some(Value::DateTime(
                DateTimeValue::from_seconds_since_epoch(left.seconds_since_epoch() + seconds),
            )))
        }
        (Value::DateTime(left), right) => {
            let Some(seconds) = integral_rhs(right) else {
                return Ok(None);
            };
            let delta = match operator {
                ArithmeticOperator::Add => seconds,
                ArithmeticOperator::Subtract => -seconds,
                _ => return Ok(None),
            };
            Ok(Some(Value::DateTime(
                DateTimeValue::from_seconds_since_epoch(left.seconds_since_epoch() + delta),
            )))
        }

        (Value::Time(left), Value::Time(right))
            if matches!(operator, ArithmeticOperator::Subtract) =>
        {
            Ok(Some(Value::Integer(
                left.seconds_since_midnight() as i32 - right.seconds_since_midnight() as i32,
            )))
        }
        (Value::Time(left), Value::IntervalDaySecond(interval)) => {
            let seconds = signed_interval_seconds(operator, *interval)?;
            Ok(Some(Value::Time(TimeValue::from_seconds_since_midnight(
                add_wrapped_seconds(left.seconds_since_midnight(), seconds),
            ))))
        }
        (Value::Time(left), right) => {
            let Some(seconds) = integral_rhs(right) else {
                return Ok(None);
            };
            let delta = match operator {
                ArithmeticOperator::Add => seconds,
                ArithmeticOperator::Subtract => -seconds,
                _ => return Ok(None),
            };
            Ok(Some(Value::Time(TimeValue::from_seconds_since_midnight(
                add_wrapped_seconds(left.seconds_since_midnight(), delta),
            ))))
        }
        (Value::TimeWithTimeZone(left), Value::IntervalDaySecond(interval)) => {
            let seconds = signed_interval_seconds(operator, *interval)?;
            Ok(Some(Value::TimeWithTimeZone(
                TimeWithTimeZoneValue::from_parts(
                    add_wrapped_seconds(left.seconds_since_midnight(), seconds),
                    left.offset_minutes(),
                ),
            )))
        }
        (Value::TimeWithTimeZone(left), right) => {
            let Some(seconds) = integral_rhs(right) else {
                return Ok(None);
            };
            let delta = match operator {
                ArithmeticOperator::Add => seconds,
                ArithmeticOperator::Subtract => -seconds,
                _ => return Ok(None),
            };
            Ok(Some(Value::TimeWithTimeZone(
                TimeWithTimeZoneValue::from_parts(
                    add_wrapped_seconds(left.seconds_since_midnight(), delta),
                    left.offset_minutes(),
                ),
            )))
        }
        _ => Ok(None),
    }
}

fn add_wrapped_seconds(seconds_since_midnight: u32, delta: i64) -> u32 {
    (seconds_since_midnight as i64 + delta).rem_euclid(86_400) as u32
}

fn signed_interval_months(
    operator: &ArithmeticOperator,
    interval: IntervalYearMonthValue,
) -> Result<i32> {
    match operator {
        ArithmeticOperator::Add => Ok(interval.total_months()),
        ArithmeticOperator::Subtract => interval.total_months().checked_neg().ok_or_else(|| {
            HematiteError::ParseError(
                "INTERVAL YEAR TO MONTH overflowed supported range".to_string(),
            )
        }),
        _ => Err(HematiteError::ParseError(
            "INTERVAL YEAR TO MONTH only supports addition and subtraction".to_string(),
        )),
    }
}

fn signed_interval_seconds(
    operator: &ArithmeticOperator,
    interval: IntervalDaySecondValue,
) -> Result<i64> {
    match operator {
        ArithmeticOperator::Add => Ok(interval.total_seconds()),
        ArithmeticOperator::Subtract => interval.total_seconds().checked_neg().ok_or_else(|| {
            HematiteError::ParseError(
                "INTERVAL DAY TO SECOND overflowed supported range".to_string(),
            )
        }),
        _ => Err(HematiteError::ParseError(
            "INTERVAL DAY TO SECOND only supports addition and subtraction".to_string(),
        )),
    }
}

fn whole_days_from_interval(
    operator: &ArithmeticOperator,
    interval: IntervalDaySecondValue,
) -> Result<i64> {
    let seconds = signed_interval_seconds(operator, interval)?;
    if seconds % 86_400 != 0 {
        return Err(HematiteError::ParseError(
            "DATE arithmetic requires INTERVAL DAY TO SECOND values aligned to whole days"
                .to_string(),
        ));
    }
    Ok(seconds / 86_400)
}

fn add_months_to_date(value: DateValue, delta_months: i32) -> Result<DateValue> {
    let (year, month, day) = value.components();
    let total_months = year
        .checked_mul(12)
        .and_then(|total| total.checked_add(month as i32 - 1))
        .and_then(|total| total.checked_add(delta_months))
        .ok_or_else(|| {
            HematiteError::ParseError(
                "Temporal month arithmetic overflowed supported range".to_string(),
            )
        })?;
    let new_year = total_months.div_euclid(12);
    let new_month = total_months.rem_euclid(12) as u32 + 1;
    let clamped_day = day.min(executor_days_in_month(new_year, new_month));
    Ok(DateValue::from_days_since_epoch(executor_days_from_civil(
        new_year,
        new_month,
        clamped_day,
    )))
}

fn add_months_to_datetime(value: DateTimeValue, delta_months: i32) -> Result<DateTimeValue> {
    let (date, time) = value.components();
    let shifted_date = add_months_to_date(date, delta_months)?;
    Ok(DateTimeValue::from_seconds_since_epoch(
        shifted_date.days_since_epoch() as i64 * 86_400 + time.seconds_since_midnight() as i64,
    ))
}

fn executor_days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if executor_is_leap_year(year) => 29,
        2 => 28,
        _ => unreachable!(),
    }
}

fn executor_is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn executor_days_from_civil(year: i32, month: u32, day: u32) -> i32 {
    let year = year - if month <= 2 { 1 } else { 0 };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month as i32;
    let day = day as i32;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn integral_rhs(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(value) => Some(*value as i64),
        Value::BigInt(value) => Some(*value),
        Value::UInteger(value) => Some(*value as i64),
        Value::UBigInt(value) => i64::try_from(*value).ok(),
        _ => None,
    }
}

fn auto_increment_value(column: &Column, next_row_id: u64) -> Result<Value> {
    match column.data_type {
        DataType::Int => i32::try_from(next_row_id)
            .map(Value::Integer)
            .map_err(|_| out_of_range_error(column, "INT")),
        DataType::UInt => u32::try_from(next_row_id)
            .map(Value::UInteger)
            .map_err(|_| out_of_range_error(column, "UINT")),
        _ => Err(HematiteError::ParseError(format!(
            "AUTO_INCREMENT column '{}' must use INT or UINT",
            column.name
        ))),
    }
}

fn negate_numeric_value(value: Value) -> Result<Value> {
    match value {
        Value::Integer(value) => value.checked_neg().map(Value::Integer).ok_or_else(|| {
            HematiteError::ParseError("Integer overflow while evaluating unary '-'".to_string())
        }),
        Value::BigInt(value) => value.checked_neg().map(Value::BigInt).ok_or_else(|| {
            HematiteError::ParseError("INT64 overflow while evaluating unary '-'".to_string())
        }),
        Value::Int128(value) => value.checked_neg().map(Value::Int128).ok_or_else(|| {
            HematiteError::ParseError("INT128 overflow while evaluating unary '-'".to_string())
        }),
        Value::UInteger(value) => Ok(Value::BigInt(-(value as i64))),
        Value::UBigInt(value) => Ok(Value::Int128(-(value as i128))),
        Value::UInt128(value) => {
            if value > i128::MAX as u128 {
                return Err(HematiteError::ParseError(
                    "UINT128 overflow while evaluating unary '-'".to_string(),
                ));
            }
            Ok(Value::Int128(-(value as i128)))
        }
        Value::Float32(value) => Ok(Value::Float32(-value)),
        Value::Float(value) => Ok(Value::Float(-value)),
        Value::Float128(value) => Ok(Value::Float128(value.negated())),
        Value::Null => Ok(Value::Null),
        value => Err(HematiteError::ParseError(format!(
            "Unary '-' requires a numeric value, found {:?}",
            value
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
        ArithmeticOperator::Modulo => {
            if right == 0.0 {
                return Err(HematiteError::ParseError("Division by zero".to_string()));
            }
            left % right
        }
    };
    Ok(Value::Float(value))
}

fn evaluate_float128_arithmetic(
    operator: &ArithmeticOperator,
    left: Float128Value,
    right: Float128Value,
) -> Result<Value> {
    let value = match operator {
        ArithmeticOperator::Add => left.add(&right)?,
        ArithmeticOperator::Subtract => left.subtract(&right)?,
        ArithmeticOperator::Multiply => left.multiply(&right)?,
        ArithmeticOperator::Divide => left.divide(&right)?,
        ArithmeticOperator::Modulo => {
            return Err(HematiteError::ParseError(
                "Modulo is not supported for FLOAT128 values".to_string(),
            ))
        }
    };
    Ok(Value::Float128(value))
}

fn cast_value_to_type(value: Value, data_type: DataType) -> Result<Value> {
    match (data_type.clone(), value) {
        (_, Value::Null) => Ok(Value::Null),
        (DataType::Int8, Value::Integer(value)) => i8::try_from(value)
            .map(|_| Value::Integer(value))
            .map_err(|_| {
                HematiteError::ParseError("Cannot CAST out-of-range INT AS INT8".to_string())
            }),
        (DataType::Int16, Value::Integer(value)) => i16::try_from(value)
            .map(|_| Value::Integer(value))
            .map_err(|_| {
                HematiteError::ParseError("Cannot CAST out-of-range INT AS INT16".to_string())
            }),
        (DataType::Int, Value::Integer(value)) => Ok(Value::Integer(value)),
        (DataType::Int, Value::BigInt(value)) => {
            i32::try_from(value).map(Value::Integer).map_err(|_| {
                HematiteError::ParseError("Cannot CAST out-of-range INT64 AS INT".to_string())
            })
        }
        (DataType::Int, Value::Int128(value)) => {
            i32::try_from(value).map(Value::Integer).map_err(|_| {
                HematiteError::ParseError("Cannot CAST out-of-range INT128 AS INT".to_string())
            })
        }
        (DataType::Int, Value::Float32(value)) => Ok(Value::Integer(value as i32)),
        (DataType::Int, Value::Float(value)) => Ok(Value::Integer(value as i32)),
        (DataType::Int, Value::Float128(value)) => Ok(Value::Integer(value.to_i32()?)),
        (DataType::Int, Value::Boolean(true)) => Ok(Value::Integer(1)),
        (DataType::Int, Value::Boolean(false)) => Ok(Value::Integer(0)),
        (DataType::Int, Value::Text(value)) => value
            .parse::<i32>()
            .map(Value::Integer)
            .map_err(|_| HematiteError::ParseError(format!("Cannot CAST '{}' AS INT", value))),
        (DataType::Int64, Value::Integer(value)) => Ok(Value::BigInt(value as i64)),
        (DataType::Int64, Value::BigInt(value)) => Ok(Value::BigInt(value)),
        (DataType::Int64, Value::Int128(value)) => {
            i64::try_from(value).map(Value::BigInt).map_err(|_| {
                HematiteError::ParseError("Cannot CAST out-of-range INT128 AS INT64".to_string())
            })
        }
        (DataType::Int64, Value::Float32(value)) => Ok(Value::BigInt(value as i64)),
        (DataType::Int64, Value::Float(value)) => Ok(Value::BigInt(value as i64)),
        (DataType::Int64, Value::Float128(value)) => Ok(Value::BigInt(value.to_i64()?)),
        (DataType::Int64, Value::Boolean(true)) => Ok(Value::BigInt(1)),
        (DataType::Int64, Value::Boolean(false)) => Ok(Value::BigInt(0)),
        (DataType::Int64, Value::Text(value)) => value
            .parse::<i64>()
            .map(Value::BigInt)
            .map_err(|_| HematiteError::ParseError(format!("Cannot CAST '{}' AS INT64", value))),
        (DataType::Int128, Value::Integer(value)) => Ok(Value::Int128(value as i128)),
        (DataType::Int128, Value::BigInt(value)) => Ok(Value::Int128(value as i128)),
        (DataType::Int128, Value::Int128(value)) => Ok(Value::Int128(value)),
        (DataType::Int128, Value::Float32(value)) => Ok(Value::Int128(value as i128)),
        (DataType::Int128, Value::Float(value)) => Ok(Value::Int128(value as i128)),
        (DataType::Int128, Value::Float128(value)) => Ok(Value::Int128(value.to_i128()?)),
        (DataType::Int128, Value::Boolean(true)) => Ok(Value::Int128(1)),
        (DataType::Int128, Value::Boolean(false)) => Ok(Value::Int128(0)),
        (DataType::Int128, Value::Text(value)) => value
            .parse::<i128>()
            .map(Value::Int128)
            .map_err(|_| HematiteError::ParseError(format!("Cannot CAST '{}' AS INT128", value))),
        (DataType::UInt8, Value::Integer(value)) if value >= 0 => u8::try_from(value)
            .map(|value| Value::UInteger(value as u32))
            .map_err(|_| {
                HematiteError::ParseError("Cannot CAST out-of-range INT AS UINT8".to_string())
            }),
        (DataType::UInt16, Value::Integer(value)) if value >= 0 => u16::try_from(value)
            .map(|value| Value::UInteger(value as u32))
            .map_err(|_| {
                HematiteError::ParseError("Cannot CAST out-of-range INT AS UINT16".to_string())
            }),
        (DataType::UInt, Value::Integer(value)) if value >= 0 => Ok(Value::UInteger(value as u32)),
        (DataType::UInt, Value::BigInt(value)) if value >= 0 => {
            u32::try_from(value).map(Value::UInteger).map_err(|_| {
                HematiteError::ParseError("Cannot CAST out-of-range INT64 AS UINT".to_string())
            })
        }
        (DataType::UInt, Value::Int128(value)) if value >= 0 => {
            u32::try_from(value).map(Value::UInteger).map_err(|_| {
                HematiteError::ParseError("Cannot CAST out-of-range INT128 AS UINT".to_string())
            })
        }
        (DataType::UInt, Value::UInteger(value)) => Ok(Value::UInteger(value)),
        (DataType::UInt, Value::UBigInt(value)) => {
            u32::try_from(value).map(Value::UInteger).map_err(|_| {
                HematiteError::ParseError("Cannot CAST out-of-range UINT64 AS UINT".to_string())
            })
        }
        (DataType::UInt, Value::UInt128(value)) => {
            u32::try_from(value).map(Value::UInteger).map_err(|_| {
                HematiteError::ParseError("Cannot CAST out-of-range UINT128 AS UINT".to_string())
            })
        }
        (DataType::UInt, Value::Float32(value)) if value >= 0.0 => {
            Ok(Value::UInteger(value as u32))
        }
        (DataType::UInt, Value::Float(value)) if value >= 0.0 => Ok(Value::UInteger(value as u32)),
        (DataType::UInt, Value::Float128(value)) => Ok(Value::UInteger(value.to_u32()?)),
        (DataType::UInt, Value::Boolean(true)) => Ok(Value::UInteger(1)),
        (DataType::UInt, Value::Boolean(false)) => Ok(Value::UInteger(0)),
        (DataType::UInt, Value::Text(value)) => value
            .parse::<u32>()
            .map(Value::UInteger)
            .map_err(|_| HematiteError::ParseError(format!("Cannot CAST '{}' AS UINT", value))),
        (DataType::UInt64, Value::Integer(value)) if value >= 0 => Ok(Value::UBigInt(value as u64)),
        (DataType::UInt64, Value::BigInt(value)) if value >= 0 => Ok(Value::UBigInt(value as u64)),
        (DataType::UInt64, Value::Int128(value)) if value >= 0 => {
            u64::try_from(value).map(Value::UBigInt).map_err(|_| {
                HematiteError::ParseError("Cannot CAST out-of-range INT128 AS UINT64".to_string())
            })
        }
        (DataType::UInt64, Value::UInteger(value)) => Ok(Value::UBigInt(value as u64)),
        (DataType::UInt64, Value::UBigInt(value)) => Ok(Value::UBigInt(value)),
        (DataType::UInt64, Value::UInt128(value)) => {
            u64::try_from(value).map(Value::UBigInt).map_err(|_| {
                HematiteError::ParseError("Cannot CAST out-of-range UINT128 AS UINT64".to_string())
            })
        }
        (DataType::UInt64, Value::Float32(value)) if value >= 0.0 => {
            Ok(Value::UBigInt(value as u64))
        }
        (DataType::UInt64, Value::Float(value)) if value >= 0.0 => Ok(Value::UBigInt(value as u64)),
        (DataType::UInt64, Value::Float128(value)) => Ok(Value::UBigInt(value.to_u64()?)),
        (DataType::UInt64, Value::Boolean(true)) => Ok(Value::UBigInt(1)),
        (DataType::UInt64, Value::Boolean(false)) => Ok(Value::UBigInt(0)),
        (DataType::UInt64, Value::Text(value)) => value
            .parse::<u64>()
            .map(Value::UBigInt)
            .map_err(|_| HematiteError::ParseError(format!("Cannot CAST '{}' AS UINT64", value))),
        (DataType::UInt128, Value::Integer(value)) if value >= 0 => {
            Ok(Value::UInt128(value as u128))
        }
        (DataType::UInt128, Value::BigInt(value)) if value >= 0 => {
            Ok(Value::UInt128(value as u128))
        }
        (DataType::UInt128, Value::Int128(value)) if value >= 0 => {
            Ok(Value::UInt128(value as u128))
        }
        (DataType::UInt128, Value::UInteger(value)) => Ok(Value::UInt128(value as u128)),
        (DataType::UInt128, Value::UBigInt(value)) => Ok(Value::UInt128(value as u128)),
        (DataType::UInt128, Value::UInt128(value)) => Ok(Value::UInt128(value)),
        (DataType::UInt128, Value::Float32(value)) if value >= 0.0 => {
            Ok(Value::UInt128(value as u128))
        }
        (DataType::UInt128, Value::Float(value)) if value >= 0.0 => {
            Ok(Value::UInt128(value as u128))
        }
        (DataType::UInt128, Value::Float128(value)) => Ok(Value::UInt128(value.to_u128()?)),
        (DataType::UInt128, Value::Boolean(true)) => Ok(Value::UInt128(1)),
        (DataType::UInt128, Value::Boolean(false)) => Ok(Value::UInt128(0)),
        (DataType::UInt128, Value::Text(value)) => value
            .parse::<u128>()
            .map(Value::UInt128)
            .map_err(|_| HematiteError::ParseError(format!("Cannot CAST '{}' AS UINT128", value))),
        (DataType::Text, value) => cast_value_to_text_string(value).map(Value::Text),
        (DataType::Char(length), value) => {
            coerce_char_value(cast_value_to_text_string(value)?, length, "CAST")
        }
        (DataType::VarChar(length), value) => {
            coerce_varchar_value(cast_value_to_text_string(value)?, length, "CAST")
        }
        (DataType::Binary(length), value) => coerce_binary_value(value, length, "CAST", true),
        (DataType::VarBinary(length), value) => coerce_binary_value(value, length, "CAST", false),
        (DataType::Enum(values), value) => coerce_enum_value(value, &values, "CAST"),
        (DataType::Boolean, Value::Boolean(value)) => Ok(Value::Boolean(value)),
        (DataType::Boolean, Value::Integer(value)) => Ok(Value::Boolean(value != 0)),
        (DataType::Boolean, Value::BigInt(value)) => Ok(Value::Boolean(value != 0)),
        (DataType::Boolean, Value::Int128(value)) => Ok(Value::Boolean(value != 0)),
        (DataType::Boolean, Value::UInteger(value)) => Ok(Value::Boolean(value != 0)),
        (DataType::Boolean, Value::UBigInt(value)) => Ok(Value::Boolean(value != 0)),
        (DataType::Boolean, Value::UInt128(value)) => Ok(Value::Boolean(value != 0)),
        (DataType::Boolean, Value::Float32(value)) => Ok(Value::Boolean(value != 0.0)),
        (DataType::Boolean, Value::Float(value)) => Ok(Value::Boolean(value != 0.0)),
        (DataType::Boolean, Value::Float128(value)) => Ok(Value::Boolean(!value.is_zero())),
        (DataType::Boolean, Value::Text(value)) => match value.to_ascii_uppercase().as_str() {
            "TRUE" | "1" => Ok(Value::Boolean(true)),
            "FALSE" | "0" => Ok(Value::Boolean(false)),
            _ => Err(HematiteError::ParseError(format!(
                "Cannot CAST '{}' AS BOOLEAN",
                value
            ))),
        },
        (DataType::Float128, Value::Text(value)) => Float128Value::parse(&value)
            .map(Value::Float128)
            .map_err(|_| HematiteError::ParseError(format!("Cannot CAST '{}' AS FLOAT128", value))),
        (DataType::Float128, Value::Boolean(true)) => {
            Ok(Value::Float128(Float128Value::from_integer(1)))
        }
        (DataType::Float128, Value::Boolean(false)) => Ok(Value::Float128(Float128Value::zero())),
        (DataType::Float128, value) => {
            if let Some(number) = numeric_value_as_float128(&value) {
                Ok(Value::Float128(number))
            } else {
                Err(HematiteError::ParseError(format!(
                    "Cannot CAST '{:?}' AS FLOAT128",
                    value
                )))
            }
        }
        (data_type @ (DataType::Float32 | DataType::Float), Value::Text(value)) => value
            .parse::<f64>()
            .map(|value| make_float_value(&data_type, value))
            .map_err(|_| {
                HematiteError::ParseError(format!(
                    "Cannot CAST '{}' AS {}",
                    value,
                    float_type_name(&data_type)
                ))
            }),
        (data_type @ (DataType::Float32 | DataType::Float), Value::Boolean(true)) => {
            Ok(make_float_value(&data_type, 1.0))
        }
        (data_type @ (DataType::Float32 | DataType::Float), Value::Boolean(false)) => {
            Ok(make_float_value(&data_type, 0.0))
        }
        (data_type @ (DataType::Float32 | DataType::Float), value) => {
            if let Some(number) = numeric_value_as_f64(&value) {
                Ok(make_float_value(&data_type, number))
            } else {
                Err(HematiteError::ParseError(format!(
                    "Cannot CAST '{:?}' AS {}",
                    value,
                    float_type_name(&data_type)
                )))
            }
        }
        (DataType::Decimal { precision, scale }, value) => {
            let decimal = coerce_decimal_value(value)?;
            if !decimal.fits_precision_scale(precision, scale) {
                return Err(HematiteError::ParseError(format!(
                    "Cannot CAST decimal outside declared precision/scale AS {}",
                    data_type.base_name()
                )));
            }
            Ok(Value::Decimal(decimal))
        }
        (DataType::Blob, Value::Blob(value)) => Ok(Value::Blob(value)),
        (DataType::Blob, Value::Text(value)) => Ok(Value::Blob(value.into_bytes())),
        (DataType::Blob, Value::Integer(value)) => Ok(Value::Blob(value.to_le_bytes().to_vec())),
        (DataType::Blob, Value::BigInt(value)) => Ok(Value::Blob(value.to_le_bytes().to_vec())),
        (DataType::Blob, Value::Int128(value)) => Ok(Value::Blob(value.to_le_bytes().to_vec())),
        (DataType::Blob, Value::UInteger(value)) => Ok(Value::Blob(value.to_le_bytes().to_vec())),
        (DataType::Blob, Value::UBigInt(value)) => Ok(Value::Blob(value.to_le_bytes().to_vec())),
        (DataType::Blob, Value::UInt128(value)) => Ok(Value::Blob(value.to_le_bytes().to_vec())),
        (DataType::Date, Value::Date(value)) => Ok(Value::Date(value)),
        (DataType::Date, Value::Text(value)) => Ok(Value::Date(validate_date_string(&value)?)),
        (DataType::Time, Value::Time(value)) => Ok(Value::Time(value)),
        (DataType::Time, Value::Text(value)) => Ok(Value::Time(validate_time_string(&value)?)),
        (DataType::DateTime, Value::DateTime(value)) => Ok(Value::DateTime(value)),
        (DataType::DateTime, Value::Text(value)) => {
            Ok(Value::DateTime(validate_datetime_string(&value)?))
        }
        (DataType::TimeWithTimeZone, Value::TimeWithTimeZone(value)) => {
            Ok(Value::TimeWithTimeZone(value))
        }
        (DataType::TimeWithTimeZone, Value::Text(value)) => Ok(Value::TimeWithTimeZone(
            validate_time_with_time_zone_string(&value)?,
        )),
        (data_type, value) => Err(HematiteError::ParseError(format!(
            "Cannot CAST {:?} AS {}",
            value,
            data_type.name()
        ))),
    }
}

fn evaluate_scalar_function(function: ScalarFunction, args: Vec<Value>) -> Result<Value> {
    match function {
        ScalarFunction::Coalesce => evaluate_coalesce(args),
        ScalarFunction::IfNull => evaluate_ifnull(args),
        ScalarFunction::NullIf => evaluate_nullif(args),
        ScalarFunction::DateFn => evaluate_date_fn(args),
        ScalarFunction::TimeFn => evaluate_time_fn(args),
        ScalarFunction::Year => evaluate_year(args),
        ScalarFunction::Month => evaluate_month(args),
        ScalarFunction::Day => evaluate_day(args),
        ScalarFunction::Hour => evaluate_hour(args),
        ScalarFunction::Minute => evaluate_minute(args),
        ScalarFunction::Second => evaluate_second(args),
        ScalarFunction::TimeToSec => evaluate_time_to_sec(args),
        ScalarFunction::SecToTime => evaluate_sec_to_time(args),
        ScalarFunction::UnixTimestamp => evaluate_unix_timestamp(args),
        ScalarFunction::Lower => evaluate_lower(args),
        ScalarFunction::Upper => evaluate_upper(args),
        ScalarFunction::Length => evaluate_length(args),
        ScalarFunction::Trim => evaluate_trim(args),
        ScalarFunction::Abs => evaluate_abs(args),
        ScalarFunction::Round => evaluate_round(args),
        ScalarFunction::Concat => evaluate_concat(args),
        ScalarFunction::ConcatWs => evaluate_concat_ws(args),
        ScalarFunction::Substring => evaluate_substring(args),
        ScalarFunction::LeftFn => evaluate_left(args),
        ScalarFunction::RightFn => evaluate_right(args),
        ScalarFunction::Greatest => evaluate_extremum("GREATEST", args, true),
        ScalarFunction::Least => evaluate_extremum("LEAST", args, false),
        ScalarFunction::Replace => evaluate_replace(args),
        ScalarFunction::Repeat => evaluate_repeat(args),
        ScalarFunction::Reverse => evaluate_reverse(args),
        ScalarFunction::Locate => evaluate_locate(args),
        ScalarFunction::Ceil => evaluate_ceil(args),
        ScalarFunction::Floor => evaluate_floor(args),
        ScalarFunction::Power => evaluate_power(args),
    }
}

fn evaluate_coalesce(args: Vec<Value>) -> Result<Value> {
    if args.is_empty() {
        return Err(HematiteError::ParseError(
            "COALESCE requires at least one argument".to_string(),
        ));
    }

    for arg in args {
        if !arg.is_null() {
            return Ok(arg);
        }
    }

    Ok(Value::Null)
}

fn evaluate_date_fn(args: Vec<Value>) -> Result<Value> {
    expect_unary_temporal_function("DATE", args, |value| {
        Ok(Value::Date(extract_date_component("DATE", value)?))
    })
}

fn evaluate_time_fn(args: Vec<Value>) -> Result<Value> {
    expect_unary_temporal_function("TIME", args, |value| {
        Ok(Value::Time(extract_time_component("TIME", value)?))
    })
}

fn evaluate_year(args: Vec<Value>) -> Result<Value> {
    expect_unary_temporal_function("YEAR", args, |value| {
        let (year, _, _) = extract_date_component("YEAR", value)?.components();
        Ok(Value::Integer(year))
    })
}

fn evaluate_month(args: Vec<Value>) -> Result<Value> {
    expect_unary_temporal_function("MONTH", args, |value| {
        let (_, month, _) = extract_date_component("MONTH", value)?.components();
        Ok(Value::Integer(month as i32))
    })
}

fn evaluate_day(args: Vec<Value>) -> Result<Value> {
    expect_unary_temporal_function("DAY", args, |value| {
        let (_, _, day) = extract_date_component("DAY", value)?.components();
        Ok(Value::Integer(day as i32))
    })
}

fn evaluate_hour(args: Vec<Value>) -> Result<Value> {
    expect_unary_temporal_function("HOUR", args, |value| {
        let (hour, _, _) = extract_time_component("HOUR", value)?.components();
        Ok(Value::Integer(hour as i32))
    })
}

fn evaluate_minute(args: Vec<Value>) -> Result<Value> {
    expect_unary_temporal_function("MINUTE", args, |value| {
        let (_, minute, _) = extract_time_component("MINUTE", value)?.components();
        Ok(Value::Integer(minute as i32))
    })
}

fn evaluate_second(args: Vec<Value>) -> Result<Value> {
    expect_unary_temporal_function("SECOND", args, |value| {
        let (_, _, second) = extract_time_component("SECOND", value)?.components();
        Ok(Value::Integer(second as i32))
    })
}

fn evaluate_time_to_sec(args: Vec<Value>) -> Result<Value> {
    expect_unary_temporal_function("TIME_TO_SEC", args, |value| {
        Ok(Value::BigInt(
            extract_time_component("TIME_TO_SEC", value)?.seconds_since_midnight() as i64,
        ))
    })
}

fn evaluate_sec_to_time(args: Vec<Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(HematiteError::ParseError(
            "SEC_TO_TIME requires exactly one argument".to_string(),
        ));
    }

    let value = args.into_iter().next().expect("validated arity");
    match value {
        Value::Null => Ok(Value::Null),
        Value::Integer(value) => Ok(Value::Time(TimeValue::from_seconds_since_midnight(
            add_wrapped_seconds(0, value as i64),
        ))),
        Value::BigInt(value) => Ok(Value::Time(TimeValue::from_seconds_since_midnight(
            add_wrapped_seconds(0, value),
        ))),
        value => Err(HematiteError::ParseError(format!(
            "SEC_TO_TIME requires an integer value, found {:?}",
            value
        ))),
    }
}

fn evaluate_unix_timestamp(args: Vec<Value>) -> Result<Value> {
    expect_unary_temporal_function("UNIX_TIMESTAMP", args, |value| {
        let timestamp = extract_timestamp_component("UNIX_TIMESTAMP", value)?;
        Ok(Value::BigInt(timestamp.seconds_since_epoch()))
    })
}

fn evaluate_ifnull(args: Vec<Value>) -> Result<Value> {
    if args.len() != 2 {
        return Err(HematiteError::ParseError(
            "IFNULL requires exactly two arguments".to_string(),
        ));
    }

    let mut args = args.into_iter();
    let first = args.next().expect("ifnull validated arity");
    let second = args.next().expect("ifnull validated arity");
    if first.is_null() {
        Ok(second)
    } else {
        Ok(first)
    }
}

fn evaluate_nullif(args: Vec<Value>) -> Result<Value> {
    if args.len() != 2 {
        return Err(HematiteError::ParseError(
            "NULLIF requires exactly two arguments".to_string(),
        ));
    }

    let mut args = args.into_iter();
    let left = args.next().expect("nullif validated arity");
    let right = args.next().expect("nullif validated arity");
    if left.is_null() {
        return Ok(Value::Null);
    }
    if right.is_null() {
        return Ok(left);
    }
    if sql_values_equal(&left, &right, None) {
        Ok(Value::Null)
    } else {
        Ok(left)
    }
}

fn evaluate_lower(args: Vec<Value>) -> Result<Value> {
    expect_unary_text_function("LOWER", args, |text| Ok(Value::Text(text.to_lowercase())))
}

fn evaluate_upper(args: Vec<Value>) -> Result<Value> {
    expect_unary_text_function("UPPER", args, |text| Ok(Value::Text(text.to_uppercase())))
}

fn evaluate_length(args: Vec<Value>) -> Result<Value> {
    expect_unary_text_function("LENGTH", args, |text| {
        let len = i32::try_from(text.chars().count())
            .map_err(|_| HematiteError::ParseError("LENGTH result overflowed INT".to_string()))?;
        Ok(Value::Integer(len))
    })
}

fn evaluate_trim(args: Vec<Value>) -> Result<Value> {
    expect_unary_text_function("TRIM", args, |text| {
        Ok(Value::Text(text.trim().to_string()))
    })
}

fn expect_unary_text_function<F>(name: &str, args: Vec<Value>, f: F) -> Result<Value>
where
    F: FnOnce(&str) -> Result<Value>,
{
    if args.len() != 1 {
        return Err(HematiteError::ParseError(format!(
            "{} requires exactly one argument",
            name
        )));
    }

    let value = args.into_iter().next().expect("validated unary arity");
    match value {
        Value::Null => Ok(Value::Null),
        Value::Text(text) => f(&text),
        value => Err(HematiteError::ParseError(format!(
            "{} requires a text value, found {:?}",
            name, value
        ))),
    }
}

fn expect_unary_temporal_function<F>(name: &str, args: Vec<Value>, f: F) -> Result<Value>
where
    F: FnOnce(Value) -> Result<Value>,
{
    if args.len() != 1 {
        return Err(HematiteError::ParseError(format!(
            "{} requires exactly one argument",
            name
        )));
    }

    let value = args.into_iter().next().expect("validated unary arity");
    match value {
        Value::Null => Ok(Value::Null),
        value => f(value),
    }
}

fn evaluate_abs(args: Vec<Value>) -> Result<Value> {
    expect_unary_numeric_function("ABS", args, |value| match value {
        Value::Integer(value) => {
            if value == i32::MIN {
                return Err(HematiteError::ParseError("ABS overflowed INT".to_string()));
            }
            Ok(Value::Integer(value.abs()))
        }
        Value::BigInt(value) => {
            if value == i64::MIN {
                return Err(HematiteError::ParseError(
                    "ABS overflowed INT64".to_string(),
                ));
            }
            Ok(Value::BigInt(value.abs()))
        }
        Value::Int128(value) => value
            .checked_abs()
            .map(Value::Int128)
            .ok_or_else(|| HematiteError::ParseError("ABS overflowed INT128".to_string())),
        Value::UInteger(value) => Ok(Value::UInteger(value)),
        Value::UBigInt(value) => Ok(Value::UBigInt(value)),
        Value::UInt128(value) => Ok(Value::UInt128(value)),
        Value::Float32(value) => Ok(Value::Float32(value.abs())),
        Value::Float(value) => Ok(Value::Float(value.abs())),
        Value::Float128(value) => Ok(Value::Float128(value.abs())),
        _ => unreachable!("validated numeric input"),
    })
}

fn evaluate_round(args: Vec<Value>) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(HematiteError::ParseError(
            "ROUND requires one or two arguments".to_string(),
        ));
    }

    let mut args = args.into_iter();
    let value = args.next().expect("validated round arity");
    let precision = match args.next() {
        Some(Value::Null) => return Ok(Value::Null),
        Some(Value::Integer(value)) => value,
        Some(value) => {
            return Err(HematiteError::ParseError(format!(
                "ROUND precision requires an integer value, found {:?}",
                value
            )))
        }
        None => 0,
    };

    match value {
        Value::Null => Ok(Value::Null),
        Value::Integer(value) => round_integer(value, precision),
        Value::BigInt(value) => round_bigint(value, precision),
        Value::Int128(value) => round_int128(value, precision),
        Value::UInteger(value) => round_uinteger(value, precision),
        Value::UBigInt(value) => round_ubigint(value, precision),
        Value::UInt128(value) => round_uint128(value, precision),
        Value::Float32(value) => Ok(Value::Float32(round_float(value as f64, precision) as f32)),
        Value::Float(value) => Ok(Value::Float(round_float(value, precision))),
        Value::Float128(value) => Ok(Value::Float128(value.round(precision)?)),
        value => Err(HematiteError::ParseError(format!(
            "ROUND requires a numeric value, found {:?}",
            value
        ))),
    }
}

fn evaluate_concat(args: Vec<Value>) -> Result<Value> {
    if args.is_empty() {
        return Err(HematiteError::ParseError(
            "CONCAT requires at least one argument".to_string(),
        ));
    }

    let mut out = String::new();
    for arg in args {
        if arg.is_null() {
            return Ok(Value::Null);
        }
        out.push_str(&coerce_value_to_string("CONCAT", arg)?);
    }
    Ok(Value::Text(out))
}

fn evaluate_concat_ws(args: Vec<Value>) -> Result<Value> {
    if args.len() < 2 {
        return Err(HematiteError::ParseError(
            "CONCAT_WS requires at least two arguments".to_string(),
        ));
    }

    let mut args = args.into_iter();
    let separator = args.next().expect("concat_ws validated arity");
    if separator.is_null() {
        return Ok(Value::Null);
    }
    let separator = coerce_value_to_string("CONCAT_WS", separator)?;

    let mut parts = Vec::new();
    for arg in args {
        if arg.is_null() {
            continue;
        }
        parts.push(coerce_value_to_string("CONCAT_WS", arg)?);
    }

    Ok(Value::Text(parts.join(&separator)))
}

fn evaluate_substring(args: Vec<Value>) -> Result<Value> {
    if args.len() != 2 && args.len() != 3 {
        return Err(HematiteError::ParseError(
            "SUBSTRING requires two or three arguments".to_string(),
        ));
    }

    let mut args = args.into_iter();
    let text = args.next().expect("validated substring arity");
    let start = args.next().expect("validated substring arity");
    let len = args.next();

    if text.is_null() || start.is_null() || len.as_ref().is_some_and(Value::is_null) {
        return Ok(Value::Null);
    }

    let text = expect_text_argument("SUBSTRING", text)?;
    let start = expect_integer_argument("SUBSTRING", start, "start position")?;
    let len = len
        .map(|value| expect_integer_argument("SUBSTRING", value, "length"))
        .transpose()?;

    substring_chars(&text, start, len)
}

fn evaluate_left(args: Vec<Value>) -> Result<Value> {
    if args.len() != 2 {
        return Err(HematiteError::ParseError(
            "LEFT requires exactly two arguments".to_string(),
        ));
    }

    let mut args = args.into_iter();
    let text = args.next().expect("validated left arity");
    let len = args.next().expect("validated left arity");
    if text.is_null() || len.is_null() {
        return Ok(Value::Null);
    }

    let text = expect_text_argument("LEFT", text)?;
    let len = expect_integer_argument("LEFT", len, "length")?;
    if len < 0 {
        return Ok(Value::Text(String::new()));
    }

    let out = text.chars().take(len as usize).collect::<String>();
    Ok(Value::Text(out))
}

fn evaluate_right(args: Vec<Value>) -> Result<Value> {
    if args.len() != 2 {
        return Err(HematiteError::ParseError(
            "RIGHT requires exactly two arguments".to_string(),
        ));
    }

    let mut args = args.into_iter();
    let text = args.next().expect("validated right arity");
    let len = args.next().expect("validated right arity");
    if text.is_null() || len.is_null() {
        return Ok(Value::Null);
    }

    let text = expect_text_argument("RIGHT", text)?;
    let len = expect_integer_argument("RIGHT", len, "length")?;
    if len < 0 {
        return Ok(Value::Text(String::new()));
    }

    let chars = text.chars().collect::<Vec<_>>();
    let take = len as usize;
    let start = chars.len().saturating_sub(take);
    let out = chars[start..].iter().collect::<String>();
    Ok(Value::Text(out))
}

fn expect_unary_numeric_function<F>(name: &str, args: Vec<Value>, f: F) -> Result<Value>
where
    F: FnOnce(Value) -> Result<Value>,
{
    if args.len() != 1 {
        return Err(HematiteError::ParseError(format!(
            "{} requires exactly one argument",
            name
        )));
    }

    let value = args.into_iter().next().expect("validated unary arity");
    match value {
        Value::Null => Ok(Value::Null),
        Value::Integer(_)
        | Value::BigInt(_)
        | Value::Int128(_)
        | Value::UInteger(_)
        | Value::UBigInt(_)
        | Value::UInt128(_)
        | Value::Float32(_)
        | Value::Float(_)
        | Value::Float128(_) => f(value),
        value => Err(HematiteError::ParseError(format!(
            "{} requires a numeric value, found {:?}",
            name, value
        ))),
    }
}

fn expect_numeric_argument(function_name: &str, value: Value) -> Result<f64> {
    match value {
        Value::Decimal(value) => value.to_string().parse::<f64>().map_err(|_| {
            HematiteError::ParseError(format!(
                "{} requires a numeric value, found {:?}",
                function_name,
                Value::Decimal(value.clone())
            ))
        }),
        value if numeric_value_as_f64(&value).is_some() => {
            Ok(numeric_value_as_f64(&value).unwrap())
        }
        value => Err(HematiteError::ParseError(format!(
            "{} requires a numeric value, found {:?}",
            function_name, value
        ))),
    }
}

fn coerce_value_to_string(function_name: &str, value: Value) -> Result<String> {
    match value {
        Value::Text(text) => Ok(text),
        Value::Enum(text) => Ok(text),
        Value::Decimal(text) => Ok(text.to_string()),
        Value::Date(text) => Ok(text.to_string()),
        Value::Time(text) => Ok(text.to_string()),
        Value::DateTime(text) => Ok(text.to_string()),
        Value::TimeWithTimeZone(text) => Ok(text.to_string()),
        Value::IntervalYearMonth(text) => Ok(text.to_string()),
        Value::IntervalDaySecond(text) => Ok(text.to_string()),
        Value::Integer(value) => Ok(value.to_string()),
        Value::BigInt(value) => Ok(value.to_string()),
        Value::Int128(value) => Ok(value.to_string()),
        Value::UInteger(value) => Ok(value.to_string()),
        Value::UBigInt(value) => Ok(value.to_string()),
        Value::UInt128(value) => Ok(value.to_string()),
        Value::Float32(value) => Ok(value.to_string()),
        Value::Float(value) => Ok(value.to_string()),
        Value::Float128(value) => Ok(value.to_string()),
        Value::Boolean(true) => Ok("TRUE".to_string()),
        Value::Boolean(false) => Ok("FALSE".to_string()),
        Value::Blob(value) => Ok(String::from_utf8_lossy(&value).into_owned()),
        Value::Null => Err(HematiteError::ParseError(format!(
            "{} cannot stringify NULL directly",
            function_name
        ))),
    }
}

fn expect_text_argument(function_name: &str, value: Value) -> Result<String> {
    match value {
        Value::Text(text) => Ok(text),
        Value::Enum(text) => Ok(text),
        Value::Decimal(text) => Ok(text.to_string()),
        Value::Date(text) => Ok(text.to_string()),
        Value::Time(text) => Ok(text.to_string()),
        Value::DateTime(text) => Ok(text.to_string()),
        Value::TimeWithTimeZone(text) => Ok(text.to_string()),
        Value::Integer(value) => Ok(value.to_string()),
        Value::BigInt(value) => Ok(value.to_string()),
        Value::Int128(value) => Ok(value.to_string()),
        Value::UInteger(value) => Ok(value.to_string()),
        Value::UBigInt(value) => Ok(value.to_string()),
        Value::UInt128(value) => Ok(value.to_string()),
        Value::Float32(value) => Ok(value.to_string()),
        Value::Float(value) => Ok(value.to_string()),
        Value::Float128(value) => Ok(value.to_string()),
        value => Err(HematiteError::ParseError(format!(
            "{} requires a text value, found {:?}",
            function_name, value
        ))),
    }
}

fn expect_integer_argument(function_name: &str, value: Value, label: &str) -> Result<i32> {
    match value {
        Value::Null => Err(HematiteError::ParseError(format!(
            "{} {} cannot be NULL",
            function_name, label
        ))),
        Value::Integer(value) => Ok(value),
        Value::BigInt(value) => i32::try_from(value).map_err(|_| {
            HematiteError::ParseError(format!(
                "{} {} requires a 32-bit integer value, found {:?}",
                function_name,
                label,
                Value::BigInt(value)
            ))
        }),
        Value::Int128(value) => i32::try_from(value).map_err(|_| {
            HematiteError::ParseError(format!(
                "{} {} requires a 32-bit integer value, found {:?}",
                function_name,
                label,
                Value::Int128(value)
            ))
        }),
        Value::UInteger(value) => i32::try_from(value).map_err(|_| {
            HematiteError::ParseError(format!(
                "{} {} requires a 32-bit integer value, found {:?}",
                function_name,
                label,
                Value::UInteger(value)
            ))
        }),
        Value::UBigInt(value) => i32::try_from(value).map_err(|_| {
            HematiteError::ParseError(format!(
                "{} {} requires a 32-bit integer value, found {:?}",
                function_name,
                label,
                Value::UBigInt(value)
            ))
        }),
        Value::UInt128(value) => i32::try_from(value).map_err(|_| {
            HematiteError::ParseError(format!(
                "{} {} requires a 32-bit integer value, found {:?}",
                function_name,
                label,
                Value::UInt128(value)
            ))
        }),
        Value::Float32(value) => Ok(value as i32),
        Value::Float(value) => Ok(value as i32),
        Value::Float128(value) => value.to_i32(),
        value => Err(HematiteError::ParseError(format!(
            "{} {} requires an integer value, found {:?}",
            function_name, label, value
        ))),
    }
}

fn extract_date_component(function_name: &str, value: Value) -> Result<DateValue> {
    match value {
        Value::Date(value) => Ok(value),
        Value::DateTime(value) => Ok(value.components().0),
        Value::Text(value) | Value::Enum(value) => DateValue::parse(&value)
            .or_else(|_| DateTimeValue::parse(&value).map(|value| value.components().0))
            .map_err(|_| {
                HematiteError::ParseError(format!(
                    "{} requires a DATE-like value, found '{}'",
                    function_name, value
                ))
            }),
        value => Err(HematiteError::ParseError(format!(
            "{} requires a DATE-like value, found {:?}",
            function_name, value
        ))),
    }
}

fn extract_time_component(function_name: &str, value: Value) -> Result<TimeValue> {
    match value {
        Value::Time(value) => Ok(value),
        Value::TimeWithTimeZone(value) => Ok(value.time()),
        Value::DateTime(value) => Ok(value.components().1),
        Value::Text(value) | Value::Enum(value) => TimeValue::parse(&value)
            .or_else(|_| TimeWithTimeZoneValue::parse(&value).map(|value| value.time()))
            .or_else(|_| DateTimeValue::parse(&value).map(|value| value.components().1))
            .map_err(|_| {
                HematiteError::ParseError(format!(
                    "{} requires a TIME-like value, found '{}'",
                    function_name, value
                ))
            }),
        value => Err(HematiteError::ParseError(format!(
            "{} requires a TIME-like value, found {:?}",
            function_name, value
        ))),
    }
}

fn extract_timestamp_component(function_name: &str, value: Value) -> Result<DateTimeValue> {
    match value {
        Value::DateTime(value) => Ok(value),
        Value::Date(value) => Ok(DateTimeValue::from_seconds_since_epoch(
            value.days_since_epoch() as i64 * 86_400,
        )),
        Value::Text(value) | Value::Enum(value) => DateTimeValue::parse(&value)
            .or_else(|_| {
                DateValue::parse(&value).map(|value| {
                    DateTimeValue::from_seconds_since_epoch(
                        value.days_since_epoch() as i64 * 86_400,
                    )
                })
            })
            .map_err(|_| {
                HematiteError::ParseError(format!(
                    "{} requires a DATETIME-like value, found '{}'",
                    function_name, value
                ))
            }),
        value => Err(HematiteError::ParseError(format!(
            "{} requires a DATETIME-like value, found {:?}",
            function_name, value
        ))),
    }
}

fn substring_chars(text: &str, start: i32, len: Option<i32>) -> Result<Value> {
    let chars = text.chars().collect::<Vec<_>>();
    let start_index = if start > 0 {
        start.saturating_sub(1) as usize
    } else if start < 0 {
        chars.len().saturating_sub((-start) as usize)
    } else {
        0
    };

    if let Some(len) = len {
        if len <= 0 {
            return Ok(Value::Text(String::new()));
        }
        let end = start_index.saturating_add(len as usize).min(chars.len());
        return Ok(Value::Text(
            chars[start_index.min(chars.len())..end].iter().collect(),
        ));
    }

    Ok(Value::Text(
        chars[start_index.min(chars.len())..].iter().collect(),
    ))
}

fn evaluate_extremum(function_name: &str, args: Vec<Value>, pick_greater: bool) -> Result<Value> {
    if args.len() < 2 {
        return Err(HematiteError::ParseError(format!(
            "{} requires at least two arguments",
            function_name
        )));
    }

    if args.iter().any(Value::is_null) {
        return Ok(Value::Null);
    }

    let mut values = args.into_iter();
    let mut best = values.next().expect("validated extremum arity");
    for value in values {
        let ordering = sql_partial_cmp(&value, &best, None).ok_or_else(|| {
            HematiteError::ParseError(format!(
                "{} requires mutually comparable arguments",
                function_name
            ))
        })?;
        let should_replace = if pick_greater {
            ordering.is_gt()
        } else {
            ordering.is_lt()
        };
        if should_replace {
            best = value;
        }
    }

    Ok(best)
}

fn evaluate_replace(args: Vec<Value>) -> Result<Value> {
    if args.len() != 3 {
        return Err(HematiteError::ParseError(
            "REPLACE requires exactly three arguments".to_string(),
        ));
    }

    let mut args = args.into_iter();
    let text = args.next().expect("validated replace arity");
    let from = args.next().expect("validated replace arity");
    let to = args.next().expect("validated replace arity");
    if text.is_null() || from.is_null() || to.is_null() {
        return Ok(Value::Null);
    }

    let text = expect_text_argument("REPLACE", text)?;
    let from = expect_text_argument("REPLACE", from)?;
    let to = expect_text_argument("REPLACE", to)?;
    Ok(Value::Text(text.replace(&from, &to)))
}

fn evaluate_repeat(args: Vec<Value>) -> Result<Value> {
    if args.len() != 2 {
        return Err(HematiteError::ParseError(
            "REPEAT requires exactly two arguments".to_string(),
        ));
    }

    let mut args = args.into_iter();
    let text = args.next().expect("validated repeat arity");
    let count = args.next().expect("validated repeat arity");
    if text.is_null() || count.is_null() {
        return Ok(Value::Null);
    }

    let text = expect_text_argument("REPEAT", text)?;
    let count = expect_integer_argument("REPEAT", count, "count")?;
    if count <= 0 {
        return Ok(Value::Text(String::new()));
    }

    let count = usize::try_from(count)
        .map_err(|_| HematiteError::ParseError("REPEAT count overflowed usize".to_string()))?;
    Ok(Value::Text(text.repeat(count)))
}

fn evaluate_reverse(args: Vec<Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(HematiteError::ParseError(
            "REVERSE requires exactly one argument".to_string(),
        ));
    }

    let value = args.into_iter().next().expect("validated reverse arity");
    if value.is_null() {
        return Ok(Value::Null);
    }

    let text = expect_text_argument("REVERSE", value)?;
    Ok(Value::Text(text.chars().rev().collect()))
}

fn evaluate_locate(args: Vec<Value>) -> Result<Value> {
    if args.len() != 2 && args.len() != 3 {
        return Err(HematiteError::ParseError(
            "LOCATE requires two or three arguments".to_string(),
        ));
    }

    let mut args = args.into_iter();
    let needle = args.next().expect("validated locate arity");
    let haystack = args.next().expect("validated locate arity");
    let start = args.next();
    if needle.is_null() || haystack.is_null() || start.as_ref().is_some_and(Value::is_null) {
        return Ok(Value::Null);
    }

    let needle = expect_text_argument("LOCATE", needle)?;
    let haystack = expect_text_argument("LOCATE", haystack)?;
    let start = start
        .map(|value| expect_integer_argument("LOCATE", value, "start position"))
        .transpose()?
        .unwrap_or(1);

    let haystack_chars = haystack.chars().collect::<Vec<_>>();
    let needle_chars = needle.chars().collect::<Vec<_>>();
    let start_index = start.saturating_sub(1).max(0) as usize;
    if needle_chars.is_empty() {
        let position = start_index.min(haystack_chars.len()) + 1;
        return Ok(Value::Integer(position as i32));
    }
    if start_index >= haystack_chars.len() || needle_chars.len() > haystack_chars.len() {
        return Ok(Value::Integer(0));
    }

    for index in start_index..=haystack_chars.len() - needle_chars.len() {
        if haystack_chars[index..index + needle_chars.len()] == needle_chars[..] {
            return Ok(Value::Integer((index + 1) as i32));
        }
    }

    Ok(Value::Integer(0))
}

fn evaluate_ceil(args: Vec<Value>) -> Result<Value> {
    expect_unary_numeric_function("CEIL", args, |value| match value {
        Value::Integer(value) => Ok(Value::Integer(value)),
        Value::BigInt(value) => Ok(Value::BigInt(value)),
        Value::Int128(value) => Ok(Value::Int128(value)),
        Value::UInteger(value) => Ok(Value::UInteger(value)),
        Value::UBigInt(value) => Ok(Value::UBigInt(value)),
        Value::UInt128(value) => Ok(Value::UInt128(value)),
        Value::Float32(value) => Ok(Value::Float32(value.ceil())),
        Value::Float(value) => Ok(Value::Float(value.ceil())),
        Value::Float128(value) => Ok(Value::Float128(value.ceil()?)),
        _ => unreachable!("validated numeric input"),
    })
}

fn evaluate_floor(args: Vec<Value>) -> Result<Value> {
    expect_unary_numeric_function("FLOOR", args, |value| match value {
        Value::Integer(value) => Ok(Value::Integer(value)),
        Value::BigInt(value) => Ok(Value::BigInt(value)),
        Value::Int128(value) => Ok(Value::Int128(value)),
        Value::UInteger(value) => Ok(Value::UInteger(value)),
        Value::UBigInt(value) => Ok(Value::UBigInt(value)),
        Value::UInt128(value) => Ok(Value::UInt128(value)),
        Value::Float32(value) => Ok(Value::Float32(value.floor())),
        Value::Float(value) => Ok(Value::Float(value.floor())),
        Value::Float128(value) => Ok(Value::Float128(value.floor()?)),
        _ => unreachable!("validated numeric input"),
    })
}

fn evaluate_power(args: Vec<Value>) -> Result<Value> {
    if args.len() != 2 {
        return Err(HematiteError::ParseError(
            "POWER requires exactly two arguments".to_string(),
        ));
    }

    let mut args = args.into_iter();
    let base = args.next().expect("validated power arity");
    let exponent = args.next().expect("validated power arity");
    if base.is_null() || exponent.is_null() {
        return Ok(Value::Null);
    }

    if matches!(base, Value::Float128(_)) || matches!(exponent, Value::Float128(_)) {
        let base = numeric_value_as_float128(&base).ok_or_else(|| {
            HematiteError::ParseError(format!("POWER requires a numeric value, found {:?}", base))
        })?;
        let exponent = expect_exact_integer_exponent(exponent)?;
        return Ok(Value::Float128(base.powi(exponent)?));
    }

    let base = expect_numeric_argument("POWER", base)?;
    let exponent = expect_numeric_argument("POWER", exponent)?;
    let value = base.powf(exponent);
    if !value.is_finite() {
        return Err(HematiteError::ParseError(
            "POWER produced a non-finite result".to_string(),
        ));
    }

    Ok(Value::Float(value))
}

fn expect_exact_integer_exponent(value: Value) -> Result<i32> {
    match value {
        Value::Integer(value) => Ok(value),
        Value::BigInt(value) => i32::try_from(value)
            .map_err(|_| HematiteError::ParseError("POWER exponent overflowed INT".to_string())),
        Value::Int128(value) => i32::try_from(value)
            .map_err(|_| HematiteError::ParseError("POWER exponent overflowed INT".to_string())),
        Value::UInteger(value) => i32::try_from(value)
            .map_err(|_| HematiteError::ParseError("POWER exponent overflowed INT".to_string())),
        Value::UBigInt(value) => i32::try_from(value)
            .map_err(|_| HematiteError::ParseError("POWER exponent overflowed INT".to_string())),
        Value::UInt128(value) => i32::try_from(value)
            .map_err(|_| HematiteError::ParseError("POWER exponent overflowed INT".to_string())),
        Value::Float32(value) if value.fract() == 0.0 => Ok(value as i32),
        Value::Float(value) if value.fract() == 0.0 => Ok(value as i32),
        Value::Float128(value) if value.exponent() >= 0 => value.to_i32(),
        value => Err(HematiteError::ParseError(format!(
            "POWER with FLOAT128 requires an exact integer exponent, found {:?}",
            value
        ))),
    }
}

fn round_integer(value: i32, precision: i32) -> Result<Value> {
    if precision >= 0 {
        return Ok(Value::Integer(value));
    }

    let rounded = round_float(value as f64, precision);
    let rounded = i32::try_from(rounded as i64)
        .map_err(|_| HematiteError::ParseError("ROUND overflowed INT".to_string()))?;
    Ok(Value::Integer(rounded))
}

fn round_bigint(value: i64, precision: i32) -> Result<Value> {
    if precision >= 0 {
        return Ok(Value::BigInt(value));
    }

    let rounded = round_float(value as f64, precision);
    let rounded = i64::try_from(rounded as i128)
        .map_err(|_| HematiteError::ParseError("ROUND overflowed INT64".to_string()))?;
    Ok(Value::BigInt(rounded))
}

fn round_int128(value: i128, precision: i32) -> Result<Value> {
    if precision >= 0 {
        return Ok(Value::Int128(value));
    }

    let rounded = round_float(value as f64, precision);
    if !rounded.is_finite() || rounded < i128::MIN as f64 || rounded > i128::MAX as f64 {
        return Err(HematiteError::ParseError(
            "ROUND overflowed INT128".to_string(),
        ));
    }
    Ok(Value::Int128(rounded as i128))
}

fn round_uinteger(value: u32, precision: i32) -> Result<Value> {
    if precision >= 0 {
        return Ok(Value::UInteger(value));
    }

    let rounded = round_float(value as f64, precision);
    if rounded < 0.0 || rounded > u32::MAX as f64 {
        return Err(HematiteError::ParseError(
            "ROUND overflowed UINT".to_string(),
        ));
    }
    Ok(Value::UInteger(rounded as u32))
}

fn round_ubigint(value: u64, precision: i32) -> Result<Value> {
    if precision >= 0 {
        return Ok(Value::UBigInt(value));
    }

    let rounded = round_float(value as f64, precision);
    if rounded < 0.0 || rounded > u64::MAX as f64 {
        return Err(HematiteError::ParseError(
            "ROUND overflowed UINT64".to_string(),
        ));
    }
    Ok(Value::UBigInt(rounded as u64))
}

fn round_uint128(value: u128, precision: i32) -> Result<Value> {
    if precision >= 0 {
        return Ok(Value::UInt128(value));
    }

    let rounded = round_float(value as f64, precision);
    if !rounded.is_finite() || rounded < 0.0 || rounded > u128::MAX as f64 {
        return Err(HematiteError::ParseError(
            "ROUND overflowed UINT128".to_string(),
        ));
    }
    Ok(Value::UInt128(rounded as u128))
}

fn round_float(value: f64, precision: i32) -> f64 {
    if precision >= 0 {
        let factor = 10f64.powi(precision);
        (value * factor).round() / factor
    } else {
        let factor = 10f64.powi(-precision);
        (value / factor).round() * factor
    }
}

fn apply_text_comparison_context(value: &str, text_context: Option<TextComparisonContext>) -> String {
    let mut normalized = if text_context.is_some_and(|context| context.trim_trailing_spaces) {
        value.trim_end_matches(' ').to_string()
    } else {
        value.to_string()
    };

    if text_context.is_some_and(|context| context.case_insensitive) {
        normalized = normalized.to_lowercase();
    }

    normalized
}

fn like_matches_with_context(
    pattern: &str,
    text: &str,
    text_context: Option<TextComparisonContext>,
) -> bool {
    let pattern = apply_text_comparison_context(pattern, text_context);
    let text = apply_text_comparison_context(text, text_context);
    SelectExecutor::like_matches(&pattern, &text)
}

fn sql_values_equal(
    left: &Value,
    right: &Value,
    text_context: Option<TextComparisonContext>,
) -> bool {
    if let Some(ordering) = sql_decimal_cmp(left, right) {
        return ordering == Ordering::Equal;
    }
    if let Some((left, right)) = sql_numeric_pair(left, right) {
        return left == right;
    }
    if let (Value::Text(left), Value::Text(right)) = (left, right) {
        return apply_text_comparison_context(left, text_context)
            == apply_text_comparison_context(right, text_context);
    }

    left == right
}

fn sql_partial_cmp(
    left: &Value,
    right: &Value,
    text_context: Option<TextComparisonContext>,
) -> Option<Ordering> {
    if let Some(ordering) = sql_decimal_cmp(left, right) {
        return Some(ordering);
    }
    if let Some((left, right)) = sql_numeric_pair(left, right) {
        return left.partial_cmp(&right);
    }
    if let (Value::Text(left), Value::Text(right)) = (left, right) {
        return Some(
            apply_text_comparison_context(left, text_context)
                .cmp(&apply_text_comparison_context(right, text_context)),
        );
    }

    left.partial_cmp(right)
}

fn sql_numeric_pair(left: &Value, right: &Value) -> Option<(f64, f64)> {
    Some((numeric_value_as_f64(left)?, numeric_value_as_f64(right)?))
}

fn sql_decimal_cmp(left: &Value, right: &Value) -> Option<Ordering> {
    let left = match left {
        Value::Decimal(value) => value.clone(),
        Value::Integer(value) => DecimalValue::from_i32(*value),
        Value::BigInt(value) => DecimalValue::from_i64(*value),
        Value::Int128(value) => DecimalValue::from_i128(*value),
        Value::UInteger(value) => DecimalValue::from_u32(*value),
        Value::UBigInt(value) => DecimalValue::from_u64(*value),
        Value::UInt128(value) => DecimalValue::from_u128(*value),
        Value::Float32(value) => DecimalValue::from_f64(*value as f64).ok()?,
        Value::Float(value) => DecimalValue::from_f64(*value).ok()?,
        Value::Float128(value) => value.to_decimal().ok()?,
        _ => return None,
    };
    let right = match right {
        Value::Decimal(value) => value.clone(),
        Value::Integer(value) => DecimalValue::from_i32(*value),
        Value::BigInt(value) => DecimalValue::from_i64(*value),
        Value::Int128(value) => DecimalValue::from_i128(*value),
        Value::UInteger(value) => DecimalValue::from_u32(*value),
        Value::UBigInt(value) => DecimalValue::from_u64(*value),
        Value::UInt128(value) => DecimalValue::from_u128(*value),
        Value::Float32(value) => DecimalValue::from_f64(*value as f64).ok()?,
        Value::Float(value) => DecimalValue::from_f64(*value).ok()?,
        Value::Float128(value) => value.to_decimal().ok()?,
        _ => return None,
    };
    Some(left.cmp(&right))
}

fn validate_date_string(input: &str) -> Result<DateValue> {
    DateValue::parse(input)
}

fn validate_time_string(input: &str) -> Result<TimeValue> {
    TimeValue::parse(input)
}

fn validate_datetime_string(input: &str) -> Result<DateTimeValue> {
    DateTimeValue::parse(input)
}

fn validate_time_with_time_zone_string(input: &str) -> Result<TimeWithTimeZoneValue> {
    TimeWithTimeZoneValue::parse(input)
}

fn compare_condition_values(
    left: &Value,
    operator: &ComparisonOperator,
    right: &Value,
    text_context: Option<TextComparisonContext>,
) -> Option<bool> {
    if left.is_null() || right.is_null() {
        return None;
    }

    match operator {
        ComparisonOperator::Equal => Some(sql_values_equal(left, right, text_context)),
        ComparisonOperator::NotEqual => Some(!sql_values_equal(left, right, text_context)),
        ComparisonOperator::LessThan => {
            sql_partial_cmp(left, right, text_context).map(|ord| ord.is_lt())
        }
        ComparisonOperator::LessThanOrEqual => {
            sql_partial_cmp(left, right, text_context).map(|ord| ord.is_le())
        }
        ComparisonOperator::GreaterThan => {
            sql_partial_cmp(left, right, text_context).map(|ord| ord.is_gt())
        }
        ComparisonOperator::GreaterThanOrEqual => {
            sql_partial_cmp(left, right, text_context).map(|ord| ord.is_ge())
        }
    }
}

fn logical_and_values(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

fn logical_or_values(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

fn evaluate_in_candidates(
    probe: Value,
    candidates: impl IntoIterator<Item = Value>,
    is_not: bool,
    text_context: Option<TextComparisonContext>,
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
        if sql_values_equal(&candidate, &probe, text_context) {
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

fn evaluate_between_values(
    value: Value,
    lower: Value,
    upper: Value,
    is_not: bool,
    text_context: Option<TextComparisonContext>,
) -> Option<bool> {
    if value.is_null() || lower.is_null() || upper.is_null() {
        return None;
    }

    let lower_ok = sql_partial_cmp(&value, &lower, text_context).map(|ordering| !ordering.is_lt());
    let upper_ok = sql_partial_cmp(&value, &upper, text_context).map(|ordering| !ordering.is_gt());

    match (lower_ok, upper_ok) {
        (Some(true), Some(true)) => Some(!is_not),
        (Some(_), Some(_)) => Some(is_not),
        _ => None,
    }
}

fn evaluate_like_values(
    value: Value,
    pattern: Value,
    is_not: bool,
    text_context: Option<TextComparisonContext>,
) -> Option<bool> {
    match (value, pattern) {
        (Value::Text(text), Value::Text(pattern)) => {
            let matched = like_matches_with_context(&pattern, &text, text_context);
            Some(if is_not { !matched } else { matched })
        }
        (left, right) if left.is_null() || right.is_null() => None,
        _ => None,
    }
}

fn nullable_bool_to_value(value: Option<bool>) -> Value {
    match value {
        Some(value) => Value::Boolean(value),
        None => Value::Null,
    }
}

fn coerce_value_to_nullable_bool(value: Value, context: &str) -> Result<Option<bool>> {
    match value {
        Value::Boolean(value) => Ok(Some(value)),
        Value::Null => Ok(None),
        value => Err(HematiteError::ParseError(format!(
            "{} requires a boolean value, found {:?}",
            context, value
        ))),
    }
}

fn unique_index_parse_error(index_name: &str, table_name: &str) -> HematiteError {
    HematiteError::ParseError(format!(
        "Duplicate value for UNIQUE index '{}' on table '{}'",
        index_name, table_name
    ))
}

fn convert_foreign_key_action(action: ForeignKeyAction) -> CatalogForeignKeyAction {
    match action {
        ForeignKeyAction::Restrict => CatalogForeignKeyAction::Restrict,
        ForeignKeyAction::Cascade => CatalogForeignKeyAction::Cascade,
        ForeignKeyAction::SetNull => CatalogForeignKeyAction::SetNull,
    }
}

fn auto_unique_index_name(table_name: &str, column_name: &str, position: usize) -> String {
    format!(
        "uq_{}_{}_{}",
        sanitize_identifier(table_name),
        sanitize_identifier(column_name),
        position
    )
}

fn unique_constraint_index_name(
    table_name: &str,
    unique: &UniqueConstraintDefinition,
    position: usize,
) -> String {
    if let Some(name) = &unique.name {
        return sanitize_identifier(name);
    }

    let column_suffix = unique
        .columns
        .iter()
        .map(|column| sanitize_identifier(column))
        .collect::<Vec<_>>()
        .join("_");
    format!(
        "uq_{}_{}_{}",
        sanitize_identifier(table_name),
        column_suffix,
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
        validate_statement(&Statement::DropIndex(self.statement.clone()), &ctx.catalog)?;
        if self.statement.if_exists {
            let Some(table) = ctx.catalog.get_table_by_name(&self.statement.table) else {
                return Ok(mutation_result(0));
            };
            if table
                .get_secondary_index(&self.statement.index_name)
                .is_none()
            {
                return Ok(mutation_result(0));
            }
        }

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
