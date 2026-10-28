//! Query plan structures and access-path descriptions.

use crate::parser::ast::{AggregateFunction, InsertSource, SelectItem};
use crate::parser::ast::{
    AlterStatement, CreateIndexStatement, CreateStatement, DeleteStatement, DropIndexStatement,
    DropStatement, InsertStatement, SelectStatement, UpdateStatement,
};
use crate::query::logest::LogEst;

pub struct QueryPlan {
    pub program: ExecutionProgram,
    pub estimated_cost: LogEst,
    pub select_analysis: Option<SelectAnalysis>,
}

impl QueryPlan {
    /// The plan as `EXPLAIN` describes it.
    ///
    /// Derived on demand rather than stored. A `PlanNode` is a description — owned
    /// table and column names, a `Vec<String>` of ORDER BY columns — and the only
    /// things that read one are `EXPLAIN` and `Debug`. Storing one would cost three
    /// allocations on every point lookup, and one description per outer row for a
    /// correlated subquery, which is re-planned per row. Everything it needs is
    /// already in `program` and `select_analysis`.
    pub fn node(&self) -> PlanNode {
        match &self.program {
            ExecutionProgram::Select {
                statement,
                access_path,
            } => PlanNode::Select(select_plan_node(
                statement,
                access_path,
                self.select_analysis.as_ref(),
            )),
            ExecutionProgram::Insert { statement } => PlanNode::Insert(InsertPlanNode {
                table_name: statement.table.clone(),
                row_count: match &statement.source {
                    InsertSource::Values(rows) => rows.len(),
                    InsertSource::Select(_) => 1,
                },
            }),
            ExecutionProgram::Update {
                statement,
                access_path,
            } => PlanNode::Update(UpdatePlanNode {
                table_name: statement.table.clone(),
                assignment_count: statement.assignments.len(),
                has_filter: statement.where_clause.is_some(),
                access_path: access_path.clone(),
            }),
            ExecutionProgram::Delete {
                statement,
                access_path,
            } => PlanNode::Delete(DeletePlanNode {
                table_name: statement.table.clone(),
                has_filter: statement.where_clause.is_some(),
                access_path: access_path.clone(),
            }),
            ExecutionProgram::Create { statement } => PlanNode::Create(CreatePlanNode {
                table_name: statement.table.clone(),
                column_count: statement.columns.len(),
            }),
            ExecutionProgram::CreateIndex { statement } => {
                PlanNode::CreateIndex(CreateIndexPlanNode {
                    table_name: statement.table.clone(),
                    index_name: statement.index_name.clone(),
                    column_count: statement.columns.len(),
                })
            }
            ExecutionProgram::Alter { statement } => PlanNode::Alter(AlterPlanNode {
                table_name: statement.table.clone(),
            }),
            ExecutionProgram::Drop { statement } => PlanNode::Drop(DropPlanNode {
                table_name: statement.table.clone(),
            }),
            ExecutionProgram::DropIndex { statement } => PlanNode::DropIndex(DropIndexPlanNode {
                table_name: statement.table.clone(),
                index_name: statement.index_name.clone(),
            }),
        }
    }
}

/// Describes a planned SELECT.
///
/// `analysis` is absent only for a `QueryPlan` assembled by hand — `plan_select` always
/// records one. The description then names no table, which is all such a plan knows.
fn select_plan_node(
    statement: &SelectStatement,
    access_path: &SelectAccessPath,
    analysis: Option<&SelectAnalysis>,
) -> SelectPlanNode {
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
        table_name: analysis
            .map(|analysis| analysis.table_name.clone())
            .unwrap_or_default(),
        source_count: analysis.map_or(0, |analysis| analysis.source_count),
        access_path: access_path.clone(),
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

impl std::fmt::Debug for QueryPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryPlan")
            .field("node", &self.node())
            .field("estimated_cost", &self.estimated_cost)
            .field("select_analysis", &self.select_analysis)
            .field("program", &self.program)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub enum ExecutionProgram {
    Select {
        statement: SelectStatement,
        access_path: SelectAccessPath,
    },
    Insert {
        statement: InsertStatement,
    },
    Update {
        statement: UpdateStatement,
        access_path: SelectAccessPath,
    },
    Delete {
        statement: DeleteStatement,
        access_path: SelectAccessPath,
    },
    Create {
        statement: CreateStatement,
    },
    CreateIndex {
        statement: CreateIndexStatement,
    },
    Alter {
        statement: AlterStatement,
    },
    Drop {
        statement: DropStatement,
    },
    DropIndex {
        statement: DropIndexStatement,
    },
}

#[derive(Debug, Clone)]
pub enum PlanNode {
    Select(SelectPlanNode),
    Insert(InsertPlanNode),
    Update(UpdatePlanNode),
    Delete(DeletePlanNode),
    Create(CreatePlanNode),
    CreateIndex(CreateIndexPlanNode),
    Alter(AlterPlanNode),
    Drop(DropPlanNode),
    DropIndex(DropIndexPlanNode),
}

#[derive(Debug, Clone)]
pub struct SelectPlanNode {
    pub table_name: String,
    pub source_count: usize,
    pub access_path: SelectAccessPath,
    pub projection: SelectProjection,
    pub distinct: bool,
    pub has_filter: bool,
    pub order_by_columns: Vec<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectAccessPath {
    FullTableScan,
    JoinScan,
    RowIdLookup,
    PrimaryKeyLookup,
    SecondaryIndexLookup(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectProjection {
    Wildcard,
    Columns(Vec<String>),
    Expressions(usize),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPlanNode {
    pub table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndexPlanNode {
    pub table_name: String,
    pub index_name: String,
    pub column_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropIndexPlanNode {
    pub table_name: String,
    pub index_name: String,
}

#[derive(Debug, Clone)]
pub struct SelectAnalysis {
    pub table_name: String,
    pub source_count: usize,
    pub has_complex_source: bool,
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
    pub selectivity: LogEst,
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
