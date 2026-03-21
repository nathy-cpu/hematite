//! Query planning and optimization

use crate::catalog::{Schema, Table, Value};
use crate::error::Result;
use crate::parser::ast::*;
use crate::query::executor::{
    CreateExecutor, DeleteExecutor, DropExecutor, InsertExecutor, QueryExecutor, SelectExecutor,
    UpdateExecutor,
};
use crate::query::optimizer::{QueryOptimizer, SelectOptimizations};
use crate::HematiteError;
use std::collections::HashMap;

pub struct QueryPlan {
    pub node: PlanNode,
    pub executor: Box<dyn QueryExecutor>,
    pub estimated_cost: f64,
    pub select_analysis: Option<SelectAnalysis>,
    pub optimizations: Option<SelectOptimizations>,
}

impl std::fmt::Debug for QueryPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryPlan")
            .field("node", &self.node)
            .field("estimated_cost", &self.estimated_cost)
            .field("select_analysis", &self.select_analysis)
            .field("optimizations", &self.optimizations)
            .field("executor", &"<QueryExecutor>")
            .finish()
    }
}

#[derive(Debug, Clone)]
pub enum PlanNode {
    Select(SelectPlanNode),
    Insert(InsertPlanNode),
    Update(UpdatePlanNode),
    Delete(DeletePlanNode),
    Create(CreatePlanNode),
    Drop(DropPlanNode),
}

#[derive(Debug, Clone)]
pub struct SelectPlanNode {
    pub table_name: String,
    pub access_path: SelectAccessPath,
    pub projection: SelectProjection,
    pub has_filter: bool,
    pub order_by_columns: Vec<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectAccessPath {
    FullTableScan,
    RowIdLookup,
    PrimaryKeyLookup,
    SecondaryIndexLookup(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectProjection {
    Wildcard,
    Columns(Vec<String>),
    CountAll,
    Aggregate {
        function: AggregateFunction,
        column: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertPlanNode {
    pub table_name: String,
    pub row_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePlanNode {
    pub table_name: String,
    pub assignment_count: usize,
    pub has_filter: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletePlanNode {
    pub table_name: String,
    pub has_filter: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePlanNode {
    pub table_name: String,
    pub column_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropPlanNode {
    pub table_name: String,
}

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

        // Create executor based on analysis
        let executor = Box::new(SelectExecutor::new(statement, node.access_path.clone()));

        // Estimate cost (simplified cost model)
        let estimated_cost = self.estimate_select_cost(&analysis);

        Ok(QueryPlan {
            node: PlanNode::Select(node),
            executor,
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
        let executor = Box::new(InsertExecutor::new(statement));

        Ok(QueryPlan {
            node,
            executor,
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
        let executor = Box::new(CreateExecutor::new(statement));

        // Cost estimation for CREATE is fixed
        let estimated_cost = 1.0;

        Ok(QueryPlan {
            node,
            executor,
            estimated_cost,
            select_analysis: None,
            optimizations: None,
        })
    }

    fn plan_update(&self, statement: UpdateStatement) -> Result<QueryPlan> {
        let node = PlanNode::Update(UpdatePlanNode {
            table_name: statement.table.clone(),
            assignment_count: statement.assignments.len(),
            has_filter: statement.where_clause.is_some(),
        });
        let executor = Box::new(UpdateExecutor::new(statement));
        let estimated_cost = 1000.0;

        Ok(QueryPlan {
            node,
            executor,
            estimated_cost,
            select_analysis: None,
            optimizations: None,
        })
    }

    fn plan_delete(&self, statement: DeleteStatement) -> Result<QueryPlan> {
        let node = PlanNode::Delete(DeletePlanNode {
            table_name: statement.table.clone(),
            has_filter: statement.where_clause.is_some(),
        });
        let executor = Box::new(DeleteExecutor::new(statement));
        let estimated_cost = 1000.0;

        Ok(QueryPlan {
            node,
            executor,
            estimated_cost,
            select_analysis: None,
            optimizations: None,
        })
    }

    fn plan_drop(&self, statement: DropStatement) -> Result<QueryPlan> {
        let node = PlanNode::Drop(DropPlanNode {
            table_name: statement.table.clone(),
        });
        let executor = Box::new(DropExecutor::new(statement));
        let estimated_cost = 1.0;

        Ok(QueryPlan {
            node,
            executor,
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
        let access_path = if self.extract_rowid_lookup(statement).is_some() {
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
        };

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

        let table = self.catalog.get_table_by_name(&table_name).ok_or_else(|| {
            HematiteError::ParseError(format!("Table '{}' not found", table_name))
        })?;

        // Analyze WHERE clause for index usage opportunities
        let usable_indexes = self.analyze_where_clause(&statement.where_clause, table)?;

        // Analyze column access patterns
        let accessed_columns = self.analyze_column_access(&statement.columns, table)?;

        Ok(SelectAnalysis {
            table_name,
            table_id: table.id,
            estimated_rows: self.estimate_table_rows(table),
            usable_indexes,
            accessed_columns,
        })
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
        // Simplified cost model
        let mut cost = analysis.estimated_rows as f64;

        // Apply index selectivity benefits
        for index_usage in &analysis.usable_indexes {
            cost *= index_usage.selectivity;
        }

        // Add projection cost based on number of accessed columns
        cost += analysis.accessed_columns.len() as f64 * 0.1;

        cost
    }
}

#[derive(Debug, Clone)]
pub struct SelectAnalysis {
    pub table_name: String,
    pub table_id: crate::catalog::TableId,
    pub estimated_rows: usize,
    pub usable_indexes: Vec<IndexUsage>,
    pub accessed_columns: Vec<ColumnAccess>,
}

#[derive(Debug, Clone)]
pub struct IndexUsage {
    pub column_id: crate::catalog::ColumnId,
    pub index_type: IndexType,
    pub index_name: Option<String>,
    pub selectivity: f64,
}

#[derive(Debug, Clone)]
pub enum IndexType {
    PrimaryKey,
    Secondary,
}

#[derive(Debug, Clone)]
pub struct ColumnAccess {
    pub column_id: crate::catalog::ColumnId,
    pub access_type: ColumnAccessType,
}

#[derive(Debug, Clone)]
pub enum ColumnAccessType {
    Read,
    Write,
}
