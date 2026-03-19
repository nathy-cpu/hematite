//! Query planning and optimization

use crate::catalog::{Schema, Table};
use crate::error::Result;
use crate::parser::ast::*;
use crate::query::executor::{
    CreateExecutor, DeleteExecutor, InsertExecutor, QueryExecutor, SelectExecutor,
};
use crate::HematiteError;

pub struct QueryPlan {
    pub executor: Box<dyn QueryExecutor>,
    pub estimated_cost: f64,
}

impl std::fmt::Debug for QueryPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryPlan")
            .field("estimated_cost", &self.estimated_cost)
            .field("executor", &"<QueryExecutor>")
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct QueryPlanner {
    catalog: Schema,
}

impl QueryPlanner {
    pub fn new(catalog: Schema) -> Self {
        Self { catalog }
    }

    pub fn plan(&self, statement: Statement) -> Result<QueryPlan> {
        // Validate statement against catalog
        statement.validate(&self.catalog)?;

        match statement {
            Statement::Select(select) => self.plan_select(select),
            Statement::Insert(insert) => self.plan_insert(insert),
            Statement::Delete(delete) => self.plan_delete(delete),
            Statement::Create(create) => self.plan_create(create),
        }
    }

    fn plan_select(&self, statement: SelectStatement) -> Result<QueryPlan> {
        // Analyze the query to determine optimal execution strategy
        let analysis = self.analyze_select(&statement)?;

        // Create executor based on analysis
        let executor = Box::new(SelectExecutor::new(statement));

        // Estimate cost (simplified cost model)
        let estimated_cost = self.estimate_select_cost(&analysis);

        Ok(QueryPlan {
            executor,
            estimated_cost,
        })
    }

    fn plan_insert(&self, statement: InsertStatement) -> Result<QueryPlan> {
        // For INSERT, the planning is straightforward
        let estimated_cost = statement.values.len() as f64;
        let executor = Box::new(InsertExecutor::new(statement));

        Ok(QueryPlan {
            executor,
            estimated_cost,
        })
    }

    fn plan_create(&self, statement: CreateStatement) -> Result<QueryPlan> {
        // For CREATE, the planning is straightforward
        let executor = Box::new(CreateExecutor::new(statement));

        // Cost estimation for CREATE is fixed
        let estimated_cost = 1.0;

        Ok(QueryPlan {
            executor,
            estimated_cost,
        })
    }

    fn plan_delete(&self, statement: DeleteStatement) -> Result<QueryPlan> {
        let executor = Box::new(DeleteExecutor::new(statement));
        let estimated_cost = 1000.0;

        Ok(QueryPlan {
            executor,
            estimated_cost,
        })
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
                                    selectivity: 1.0, // Primary key equality is highly selective
                                });
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
            }
        }

        Ok(accessed_columns)
    }

    fn estimate_table_rows(&self, _table: &Table) -> usize {
        // For now, return a fixed estimate (in a real implementation, this would use statistics)
        1000
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
