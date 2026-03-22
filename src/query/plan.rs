//! Query plan structures and access-path descriptions.

use crate::parser::ast::AggregateFunction;

use super::optimizer::SelectOptimizations;
use super::runtime::QueryExecutor;

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
    pub access_path: SelectAccessPath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletePlanNode {
    pub table_name: String,
    pub has_filter: bool,
    pub access_path: SelectAccessPath,
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
pub struct SelectAnalysis {
    pub table_name: String,
    pub table_id: crate::catalog::TableId,
    pub rowid_lookup: Option<u64>,
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
