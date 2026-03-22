//! Query planning and optimization

use crate::catalog::{Schema, Table, Value};
use crate::error::Result;
use crate::parser::ast::*;
use crate::query::optimizer::QueryOptimizer;
pub use crate::query::plan::*;
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
            Statement::Select(select) => self.plan_select(select),
            Statement::Update(update) => self.plan_update(update),
            Statement::Insert(insert) => self.plan_insert(insert),
            Statement::Delete(delete) => self.plan_delete(delete),
            Statement::Create(create) => self.plan_create(create),
            Statement::Drop(drop) => self.plan_drop(drop),
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
        // For CREATE, the planning is straightforward
        let node = PlanNode::Create(CreatePlanNode {
            table_name: statement.table.clone(),
            column_count: statement.columns.len(),
        });

        // Cost estimation for CREATE is fixed
        let estimated_cost = 1.0;

        Ok(QueryPlan {
            node,
            program: ExecutionProgram::Create { statement },
            estimated_cost,
            select_analysis: None,
            optimizations: None,
        })
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
        let estimated_cost = 1.0;

        Ok(QueryPlan {
            node,
            program: ExecutionProgram::Drop { statement },
            estimated_cost,
            select_analysis: None,
            optimizations: None,
        })
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
                _ => SelectProjection::Columns(
                    statement
                        .columns
                        .iter()
                        .filter_map(|item| match item {
                            SelectItem::Column(name) => Some(name.clone()),
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
            access_path,
            projection,
            has_filter: statement.where_clause.is_some(),
            order_by_columns: statement
                .order_by
                .iter()
                .map(|item| item.column.clone())
                .collect(),
            limit: statement.limit,
        }
    }

    fn extract_rowid_lookup(&self, statement: &SelectStatement) -> Option<u64> {
        let where_clause = statement.where_clause.as_ref()?;
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

    fn analyze_select(&self, statement: &SelectStatement) -> Result<SelectAnalysis> {
        let table_name = match &statement.from {
            TableReference::Table(name) => name.clone(),
        };

        self.analyze_table_access(&table_name, &statement.where_clause)
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

        let synthetic_select = SelectStatement {
            columns: vec![SelectItem::Wildcard],
            from: TableReference::Table(table_name.clone()),
            where_clause: where_clause.clone(),
            order_by: Vec::new(),
            limit: None,
        };
        let rowid_lookup = self.extract_rowid_lookup(&synthetic_select);

        // Analyze WHERE clause for index usage opportunities
        let usable_indexes = self.analyze_where_clause(where_clause, table)?;

        // Analyze column access patterns
        let accessed_columns = self.analyze_column_access(&synthetic_select.columns, table)?;

        Ok(SelectAnalysis {
            table_name,
            table_id: table.id,
            rowid_lookup,
            estimated_rows: self.estimate_table_rows(table),
            usable_indexes,
            accessed_columns,
        })
    }

    fn choose_access_path(&self, analysis: &SelectAnalysis) -> SelectAccessPath {
        if analysis.rowid_lookup.is_some() {
            SelectAccessPath::RowIdLookup
        } else if analysis
            .usable_indexes
            .iter()
            .any(|usage| matches!(usage.index_type, IndexType::PrimaryKey))
        {
            SelectAccessPath::PrimaryKeyLookup
        } else if let Some(index_usage) = analysis
            .usable_indexes
            .iter()
            .find(|usage| matches!(usage.index_type, IndexType::Secondary))
        {
            SelectAccessPath::SecondaryIndexLookup(
                index_usage
                    .index_name
                    .clone()
                    .unwrap_or_else(|| "unnamed_secondary_index".to_string()),
            )
        } else {
            SelectAccessPath::FullTableScan
        }
    }

    fn analyze_where_clause(
        &self,
        where_clause: &Option<WhereClause>,
        table: &Table,
    ) -> Result<Vec<IndexUsage>> {
        let mut usable_indexes = Vec::new();

        if let Some(where_clause) = where_clause {
            for condition in &where_clause.conditions {
                if let Condition::Comparison {
                    left,
                    operator,
                    right,
                } = condition
                {
                    // Check if this is a simple equality on an indexed column
                    if let (Expression::Column(col_name), Expression::Literal(_)) = (left, right) {
                        if let Some(column) = table.get_column_by_name(col_name) {
                            if column.primary_key && matches!(operator, ComparisonOperator::Equal) {
                                usable_indexes.push(IndexUsage {
                                    column_id: column.id,
                                    index_type: IndexType::PrimaryKey,
                                    index_name: None,
                                    selectivity: 1.0, // Primary key equality is highly selective
                                });
                            } else if matches!(operator, ComparisonOperator::Equal) {
                                let Some(column_index) = table.get_column_index(col_name) else {
                                    continue;
                                };
                                for index in &table.secondary_indexes {
                                    if index.column_indices.len() == 1
                                        && index.column_indices[0] == column_index
                                    {
                                        usable_indexes.push(IndexUsage {
                                            column_id: column.id,
                                            index_type: IndexType::Secondary,
                                            index_name: Some(index.name.clone()),
                                            selectivity: 0.1,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
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
                    if let Some(column) = table.get_column_by_name(name) {
                        accessed_columns.push(ColumnAccess {
                            column_id: column.id,
                            access_type: ColumnAccessType::Read,
                        });
                    }
                }
                SelectItem::CountAll => {}
                SelectItem::Aggregate { .. } => {}
            }
        }

        Ok(accessed_columns)
    }

    fn estimate_table_rows(&self, table: &Table) -> usize {
        self.table_row_counts
            .get(&table.name)
            .copied()
            .unwrap_or(1000)
    }

    fn estimate_select_cost(&self, analysis: &SelectAnalysis) -> f64 {
        let access_path = self.choose_access_path(analysis);
        let mut cost = self.estimate_locator_cost(analysis, &access_path)
            + self.estimate_rows_touched(analysis, &access_path) * 0.5;
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
}
