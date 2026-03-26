//! Query planning and optimization

use crate::catalog::{Schema, Table, Value};
use crate::error::Result;
use crate::parser::ast::*;
use crate::query::optimizer::QueryOptimizer;
pub use crate::query::plan::*;
use crate::query::predicate::extract_literal_equalities;
use crate::HematiteError;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct QueryPlanner {
    catalog: Schema,
    table_row_counts: HashMap<String, usize>,
}

impl QueryPlanner {
    pub fn new(catalog: Schema) -> Self {
        Self {
            catalog,
            table_row_counts: HashMap::new(),
        }
    }

    pub fn with_table_row_counts(mut self, table_row_counts: HashMap<String, usize>) -> Self {
        self.table_row_counts = table_row_counts;
        self
    }

    pub fn plan(&self, statement: Statement) -> Result<QueryPlan> {
        // Validate statement against catalog
        statement.validate(&self.catalog)?;

        let plan = match statement {
            Statement::Begin | Statement::Commit | Statement::Rollback => {
                return Err(HematiteError::ParseError(
                    "Transaction control statements are handled at the SQL connection boundary"
                        .to_string(),
                ))
            }
            Statement::Select(select) => self.plan_select(select),
            Statement::Update(update) => self.plan_update(update),
            Statement::Insert(insert) => self.plan_insert(insert),
            Statement::Delete(delete) => self.plan_delete(delete),
            Statement::Create(create) => self.plan_create(create),
            Statement::CreateIndex(create_index) => self.plan_create_index(create_index),
            Statement::Alter(alter) => self.plan_alter(alter),
            Statement::Drop(drop) => self.plan_drop(drop),
            Statement::DropIndex(drop_index) => self.plan_drop_index(drop_index),
        }?;

        let optimizer = QueryOptimizer::new(self.catalog.clone());
        optimizer.optimize(plan)
    }

    fn plan_select(&self, statement: SelectStatement) -> Result<QueryPlan> {
        // Analyze the query to determine optimal execution strategy
        let analysis = self.analyze_select(&statement)?;
        let node = self.build_select_plan_node(&statement, &analysis);
        let access_path = node.access_path.clone();

        // Estimate cost (simplified cost model)
        let estimated_cost = self.estimate_select_cost(&analysis);

        Ok(QueryPlan {
            node: PlanNode::Select(node),
            program: ExecutionProgram::Select {
                statement,
                access_path,
            },
            estimated_cost,
            select_analysis: Some(analysis),
            optimizations: None,
        })
    }

    fn plan_insert(&self, statement: InsertStatement) -> Result<QueryPlan> {
        // For INSERT, the planning is straightforward
        let estimated_cost = statement.values.len() as f64;
        let node = PlanNode::Insert(InsertPlanNode {
            table_name: statement.table.clone(),
            row_count: statement.values.len(),
        });

        Ok(QueryPlan {
            node,
            program: ExecutionProgram::Insert { statement },
            estimated_cost,
            select_analysis: None,
            optimizations: None,
        })
    }

    fn plan_create(&self, statement: CreateStatement) -> Result<QueryPlan> {
        let node = PlanNode::Create(CreatePlanNode {
            table_name: statement.table.clone(),
            column_count: statement.columns.len(),
        });
        Ok(self.simple_plan(node, ExecutionProgram::Create { statement }))
    }

    fn plan_update(&self, statement: UpdateStatement) -> Result<QueryPlan> {
        let analysis = self.analyze_table_access(&statement.table, &statement.where_clause)?;
        let access_path = self.choose_access_path(&analysis);
        let assignment_count = statement.assignments.len();
        let node = PlanNode::Update(UpdatePlanNode {
            table_name: statement.table.clone(),
            assignment_count,
            has_filter: statement.where_clause.is_some(),
            access_path: access_path.clone(),
        });
        let estimated_cost = self.estimate_update_cost(&analysis, &access_path, assignment_count);

        Ok(QueryPlan {
            node,
            program: ExecutionProgram::Update {
                statement,
                access_path,
            },
            estimated_cost,
            select_analysis: None,
            optimizations: None,
        })
    }

    fn plan_delete(&self, statement: DeleteStatement) -> Result<QueryPlan> {
        let analysis = self.analyze_table_access(&statement.table, &statement.where_clause)?;
        let access_path = self.choose_access_path(&analysis);
        let node = PlanNode::Delete(DeletePlanNode {
            table_name: statement.table.clone(),
            has_filter: statement.where_clause.is_some(),
            access_path: access_path.clone(),
        });
        let estimated_cost = self.estimate_delete_cost(&analysis, &access_path);

        Ok(QueryPlan {
            node,
            program: ExecutionProgram::Delete {
                statement,
                access_path,
            },
            estimated_cost,
            select_analysis: None,
            optimizations: None,
        })
    }

    fn plan_drop(&self, statement: DropStatement) -> Result<QueryPlan> {
        let node = PlanNode::Drop(DropPlanNode {
            table_name: statement.table.clone(),
        });
        Ok(self.simple_plan(node, ExecutionProgram::Drop { statement }))
    }

    fn plan_alter(&self, statement: AlterStatement) -> Result<QueryPlan> {
        let node = PlanNode::Alter(AlterPlanNode {
            table_name: statement.table.clone(),
        });
        Ok(self.simple_plan(node, ExecutionProgram::Alter { statement }))
    }

    fn plan_create_index(&self, statement: CreateIndexStatement) -> Result<QueryPlan> {
        let node = PlanNode::CreateIndex(CreateIndexPlanNode {
            table_name: statement.table.clone(),
            index_name: statement.index_name.clone(),
            column_count: statement.columns.len(),
        });
        Ok(self.simple_plan(node, ExecutionProgram::CreateIndex { statement }))
    }

    fn plan_drop_index(&self, statement: DropIndexStatement) -> Result<QueryPlan> {
        let node = PlanNode::DropIndex(DropIndexPlanNode {
            table_name: statement.table.clone(),
            index_name: statement.index_name.clone(),
        });
        Ok(self.simple_plan(node, ExecutionProgram::DropIndex { statement }))
    }

    fn simple_plan(&self, node: PlanNode, program: ExecutionProgram) -> QueryPlan {
        QueryPlan {
            node,
            program,
            estimated_cost: 1.0,
            select_analysis: None,
            optimizations: None,
        }
    }

    fn build_select_plan_node(
        &self,
        statement: &SelectStatement,
        analysis: &SelectAnalysis,
    ) -> SelectPlanNode {
        let access_path = self.choose_access_path(analysis);

        let projection = if statement
            .columns
            .iter()
            .any(|item| matches!(item, SelectItem::Wildcard))
        {
            SelectProjection::Wildcard
        } else if let Some(item) = statement.columns.first() {
            match item {
                SelectItem::CountAll => SelectProjection::CountAll,
                SelectItem::Aggregate { function, column } => SelectProjection::Aggregate {
                    function: *function,
                    column: column.clone(),
                },
                SelectItem::Expression(_) => SelectProjection::Expressions(statement.columns.len()),
                _ => SelectProjection::Columns(
                    statement
                        .columns
                        .iter()
                        .filter_map(|item| match item {
                            SelectItem::Column(name) => {
                                Some(SelectStatement::column_reference_name(name).to_string())
                            }
                            _ => None,
                        })
                        .collect(),
                ),
            }
        } else {
            SelectProjection::Columns(Vec::new())
        };

        SelectPlanNode {
            table_name: analysis.table_name.clone(),
            source_count: analysis.source_count,
            access_path,
            projection,
            distinct: statement.distinct,
            has_filter: statement.where_clause.is_some(),
            order_by_columns: statement
                .order_by
                .iter()
                .map(|item| item.column.clone())
                .collect(),
            limit: statement.limit,
            offset: statement.offset,
        }
    }

    fn extract_rowid_lookup(&self, statement: &SelectStatement) -> Option<u64> {
        let equalities = extract_literal_equalities(statement.where_clause.as_ref()?)?;
        match equalities.get("rowid") {
            Some(Value::Integer(v)) if *v >= 0 => Some(*v as u64),
            _ => None,
        }
    }

    fn analyze_select(&self, statement: &SelectStatement) -> Result<SelectAnalysis> {
        let bindings = SelectStatement::collect_table_bindings(&statement.from);
        let primary = bindings.first().ok_or_else(|| {
            HematiteError::ParseError("SELECT requires at least one table source".to_string())
        })?;

        if bindings.len() == 1 && !statement.has_non_table_source() {
            return self.analyze_table_access(&primary.table_name, &statement.where_clause);
        }

        let estimated_rows = if bindings.len() > 1 || statement.has_non_table_source() {
            self.estimate_complex_source_rows(statement, &statement.from)
        } else {
            bindings
                .iter()
                .try_fold(1usize, |product, binding| -> Result<usize> {
                    let table = self
                        .catalog
                        .get_table_by_name(&binding.table_name)
                        .ok_or_else(|| {
                            HematiteError::ParseError(format!(
                                "Table '{}' not found",
                                binding.table_name
                            ))
                        })?;
                    Ok(product.saturating_mul(self.estimate_table_rows(table).max(1)))
                })?
        };

        Ok(SelectAnalysis {
            table_name: primary.table_name.clone(),
            source_count: bindings.len(),
            has_complex_source: statement.has_non_table_source(),
            table_id: self
                .catalog
                .get_table_by_name(&primary.table_name)
                .map(|table| table.id)
                .unwrap_or_else(|| crate::catalog::TableId::new(0)),
            rowid_lookup: None,
            estimated_rows,
            usable_indexes: Vec::new(),
            accessed_columns: Vec::new(),
        })
    }

    fn analyze_table_access(
        &self,
        table_name: &str,
        where_clause: &Option<WhereClause>,
    ) -> Result<SelectAnalysis> {
        let table_name = table_name.to_string();
        let table = self.catalog.get_table_by_name(&table_name).ok_or_else(|| {
            HematiteError::ParseError(format!("Table '{}' not found", table_name))
        })?;

        let synthetic_select = synthetic_table_select(&table_name, where_clause.clone());
        let rowid_lookup = self.extract_rowid_lookup(&synthetic_select);

        // Analyze WHERE clause for index usage opportunities
        let usable_indexes = self.analyze_where_clause(where_clause, table)?;

        // Analyze column access patterns
        let accessed_columns = self.analyze_column_access(&synthetic_select.columns, table)?;

        Ok(SelectAnalysis {
            table_name,
            source_count: 1,
            has_complex_source: false,
            table_id: table.id,
            rowid_lookup,
            estimated_rows: self.estimate_table_rows(table),
            usable_indexes,
            accessed_columns,
        })
    }

    fn choose_access_path(&self, analysis: &SelectAnalysis) -> SelectAccessPath {
        if analysis.has_complex_source || analysis.source_count > 1 {
            return SelectAccessPath::JoinScan;
        }

        self.access_path_candidates(analysis)
            .into_iter()
            .min_by(|left, right| {
                self.estimate_total_access_cost(analysis, left)
                    .partial_cmp(&self.estimate_total_access_cost(analysis, right))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .unwrap_or(SelectAccessPath::FullTableScan)
    }

    fn access_path_candidates(&self, analysis: &SelectAnalysis) -> Vec<SelectAccessPath> {
        let mut candidates = vec![SelectAccessPath::FullTableScan];

        if analysis.rowid_lookup.is_some() {
            candidates.push(SelectAccessPath::RowIdLookup);
        }

        if analysis
            .usable_indexes
            .iter()
            .any(|usage| matches!(usage.index_type, IndexType::PrimaryKey))
        {
            candidates.push(SelectAccessPath::PrimaryKeyLookup);
        }

        candidates.extend(
            analysis
                .usable_indexes
                .iter()
                .filter(|usage| matches!(usage.index_type, IndexType::Secondary))
                .map(|usage| {
                    SelectAccessPath::SecondaryIndexLookup(
                        usage
                            .index_name
                            .clone()
                            .unwrap_or_else(|| "unnamed_secondary_index".to_string()),
                    )
                }),
        );

        candidates
    }

    fn analyze_where_clause(
        &self,
        where_clause: &Option<WhereClause>,
        table: &Table,
    ) -> Result<Vec<IndexUsage>> {
        let mut usable_indexes = Vec::new();
        let Some(where_clause) = where_clause.as_ref() else {
            return Ok(usable_indexes);
        };
        let Some(equalities) = extract_literal_equalities(where_clause) else {
            return Ok(usable_indexes);
        };

        if table
            .primary_key_columns
            .iter()
            .all(|&index| equalities.contains_key(table.columns[index].name.as_str()))
        {
            let first_pk = table
                .primary_key_columns
                .first()
                .and_then(|&index| table.columns.get(index))
                .ok_or_else(|| {
                    HematiteError::InternalError(format!(
                        "Table '{}' lost its primary key metadata during planning",
                        table.name
                    ))
                })?;
            usable_indexes.push(IndexUsage {
                column_id: first_pk.id,
                index_type: IndexType::PrimaryKey,
                index_name: None,
                selectivity: (1.0 / self.estimate_table_rows(table).max(1) as f64).max(0.0001),
            });
        }

        for index in &table.secondary_indexes {
            if index.column_indices.iter().all(|&column_index| {
                equalities.contains_key(table.columns[column_index].name.as_str())
            }) {
                let column = table.columns.get(index.column_indices[0]).ok_or_else(|| {
                    HematiteError::InternalError(format!(
                        "Index '{}' references an invalid column on table '{}'",
                        index.name, table.name
                    ))
                })?;
                usable_indexes.push(IndexUsage {
                    column_id: column.id,
                    index_type: IndexType::Secondary,
                    index_name: Some(index.name.clone()),
                    selectivity: if index.unique {
                        (1.0 / self.estimate_table_rows(table).max(1) as f64).max(0.0001)
                    } else if index.column_indices.len() > 1 {
                        0.02
                    } else {
                        0.1
                    },
                });
            }
        }

        Ok(usable_indexes)
    }

    fn analyze_column_access(
        &self,
        select_items: &[SelectItem],
        table: &Table,
    ) -> Result<Vec<ColumnAccess>> {
        let mut accessed_columns = Vec::new();

        for item in select_items {
            match item {
                SelectItem::Wildcard => {
                    // All columns are accessed
                    for column in &table.columns {
                        accessed_columns.push(ColumnAccess {
                            column_id: column.id,
                            access_type: ColumnAccessType::Read,
                        });
                    }
                }
                SelectItem::Column(name) => {
                    if let Some(column) =
                        table.get_column_by_name(SelectStatement::column_reference_name(name))
                    {
                        accessed_columns.push(ColumnAccess {
                            column_id: column.id,
                            access_type: ColumnAccessType::Read,
                        });
                    }
                }
                SelectItem::Expression(expr) => {
                    self.collect_expression_columns(expr, table, &mut accessed_columns);
                }
                SelectItem::CountAll => {}
                SelectItem::Aggregate { .. } => {}
            }
        }

        Ok(accessed_columns)
    }

    fn collect_expression_columns(
        &self,
        expr: &Expression,
        table: &Table,
        accessed_columns: &mut Vec<ColumnAccess>,
    ) {
        match expr {
            Expression::Column(name) => {
                if let Some(column) =
                    table.get_column_by_name(SelectStatement::column_reference_name(name))
                {
                    accessed_columns.push(ColumnAccess {
                        column_id: column.id,
                        access_type: ColumnAccessType::Read,
                    });
                }
            }
            Expression::AggregateCall { target, .. } => {
                if let AggregateTarget::Column(name) = target {
                    if let Some(column) =
                        table.get_column_by_name(SelectStatement::column_reference_name(name))
                    {
                        accessed_columns.push(ColumnAccess {
                            column_id: column.id,
                            access_type: ColumnAccessType::Read,
                        });
                    }
                }
            }
            Expression::UnaryMinus(expr) => {
                self.collect_expression_columns(expr, table, accessed_columns);
            }
            Expression::Binary { left, right, .. } => {
                self.collect_expression_columns(left, table, accessed_columns);
                self.collect_expression_columns(right, table, accessed_columns);
            }
            Expression::Literal(_) | Expression::Parameter(_) => {}
        }
    }

    fn estimate_table_rows(&self, table: &Table) -> usize {
        self.table_row_counts
            .get(&table.name)
            .copied()
            .unwrap_or(1000)
    }

    fn estimate_complex_source_rows(
        &self,
        statement: &SelectStatement,
        from: &TableReference,
    ) -> usize {
        match from {
            TableReference::Table(table_name, _) => {
                if statement.references_cte(table_name) {
                    1000
                } else {
                    self.catalog
                        .get_table_by_name(table_name)
                        .map(|table| self.estimate_table_rows(table))
                        .unwrap_or(1000)
                }
            }
            TableReference::Derived { .. } => 1000,
            TableReference::CrossJoin(left, right) => self
                .estimate_complex_source_rows(statement, left)
                .saturating_mul(self.estimate_complex_source_rows(statement, right).max(1)),
            TableReference::InnerJoin { left, right, on } => {
                self.estimate_join_rows(statement, left, right, Some(on), false)
            }
            TableReference::LeftJoin { left, right, on } => {
                self.estimate_join_rows(statement, left, right, Some(on), true)
            }
        }
    }

    fn estimate_join_rows(
        &self,
        statement: &SelectStatement,
        left: &TableReference,
        right: &TableReference,
        on: Option<&Condition>,
        preserve_left_rows: bool,
    ) -> usize {
        let left_rows = self.estimate_complex_source_rows(statement, left).max(1);
        let right_rows = self.estimate_complex_source_rows(statement, right).max(1);

        let join_rows = if on.is_some_and(is_equality_join_condition) {
            left_rows.max(right_rows)
        } else {
            left_rows.saturating_mul(right_rows)
        };

        if preserve_left_rows {
            join_rows.max(left_rows)
        } else {
            join_rows
        }
    }

    fn estimate_select_cost(&self, analysis: &SelectAnalysis) -> f64 {
        let access_path = self.choose_access_path(analysis);
        let mut cost = self.estimate_total_access_cost(analysis, &access_path);
        cost += analysis.accessed_columns.len() as f64 * 0.1;
        cost.max(1.0)
    }

    fn estimate_update_cost(
        &self,
        analysis: &SelectAnalysis,
        access_path: &SelectAccessPath,
        assignment_count: usize,
    ) -> f64 {
        let rows_touched = self.estimate_rows_touched(analysis, access_path);
        (self.estimate_locator_cost(analysis, access_path)
            + rows_touched * 3.0
            + assignment_count as f64 * 0.2)
            .max(1.0)
    }

    fn estimate_delete_cost(
        &self,
        analysis: &SelectAnalysis,
        access_path: &SelectAccessPath,
    ) -> f64 {
        let rows_touched = self.estimate_rows_touched(analysis, access_path);
        (self.estimate_locator_cost(analysis, access_path) + rows_touched * 2.0).max(1.0)
    }

    fn estimate_rows_touched(
        &self,
        analysis: &SelectAnalysis,
        access_path: &SelectAccessPath,
    ) -> f64 {
        match access_path {
            SelectAccessPath::JoinScan => analysis.estimated_rows as f64,
            SelectAccessPath::RowIdLookup | SelectAccessPath::PrimaryKeyLookup => 1.0,
            SelectAccessPath::SecondaryIndexLookup(index_name) => self
                .secondary_index_selectivity(analysis, index_name)
                .map(|selectivity| (analysis.estimated_rows as f64 * selectivity).max(1.0))
                .unwrap_or((analysis.estimated_rows as f64 * 0.1).max(1.0)),
            SelectAccessPath::FullTableScan => analysis.estimated_rows as f64,
        }
    }

    fn estimate_locator_cost(
        &self,
        analysis: &SelectAnalysis,
        access_path: &SelectAccessPath,
    ) -> f64 {
        match access_path {
            SelectAccessPath::JoinScan => analysis.estimated_rows as f64 * 1.5,
            SelectAccessPath::RowIdLookup => 1.0,
            SelectAccessPath::PrimaryKeyLookup => 2.0,
            SelectAccessPath::SecondaryIndexLookup(index_name) => {
                2.5 + self.estimate_rows_touched(analysis, access_path)
                    + self
                        .secondary_index_selectivity(analysis, index_name)
                        .map(|selectivity| selectivity * 5.0)
                        .unwrap_or(0.5)
            }
            SelectAccessPath::FullTableScan => analysis.estimated_rows as f64,
        }
    }

    fn secondary_index_selectivity(
        &self,
        analysis: &SelectAnalysis,
        index_name: &str,
    ) -> Option<f64> {
        analysis
            .usable_indexes
            .iter()
            .find(|usage| {
                matches!(usage.index_type, IndexType::Secondary)
                    && usage.index_name.as_deref() == Some(index_name)
            })
            .map(|usage| usage.selectivity)
    }

    fn estimate_total_access_cost(
        &self,
        analysis: &SelectAnalysis,
        access_path: &SelectAccessPath,
    ) -> f64 {
        self.estimate_locator_cost(analysis, access_path)
            + self.estimate_rows_touched(analysis, access_path) * 0.5
    }
}

fn is_equality_join_condition(condition: &Condition) -> bool {
    match condition {
        Condition::Comparison {
            left: Expression::Column(_),
            operator: ComparisonOperator::Equal,
            right: Expression::Column(_),
        } => true,
        Condition::Logical {
            left,
            operator: LogicalOperator::And,
            right,
        } => is_equality_join_condition(left) && is_equality_join_condition(right),
        _ => false,
    }
}

fn synthetic_table_select(table_name: &str, where_clause: Option<WhereClause>) -> SelectStatement {
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
