//! The owned statement tree the query layer reads.
//!
//! Every node owns its children — `Box`, `Vec`, `String` — so a statement outlives
//! its SQL text and can be cloned and rewritten in place: prepared-statement
//! binding, `query::rewrite` and the optimizer all mutate it. The parser does not
//! build this form directly; it builds into [`arena::Ast`](crate::parser::arena::Ast)
//! and `bridge.rs` materializes this tree from it after every parse.

use crate::error::{HematiteError, Result};
use crate::parser::types::{LiteralValue, SqlTypeName};

/// Render `items` into a single `separator`-joined `String`, writing each item
/// directly into one buffer instead of collecting an intermediate `Vec<String>`.
fn join_rendered<I, F>(items: I, separator: &str, mut render: F) -> String
where
    I: IntoIterator,
    F: FnMut(I::Item) -> String,
{
    let mut out = String::new();
    for (index, item) in items.into_iter().enumerate() {
        if index > 0 {
            out.push_str(separator);
        }
        out.push_str(&render(item));
    }
    out
}

#[derive(Debug, Clone)]
pub enum Statement {
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    RollbackToSavepoint(String),
    ReleaseSavepoint(String),
    Explain(ExplainStatement),
    Describe(DescribeStatement),
    ShowTables,
    ShowIndexes(Option<String>),
    ShowCreateTable(String),
    Select(SelectStatement),
    SelectInto(SelectIntoStatement),
    Update(UpdateStatement),
    Insert(InsertStatement),
    Delete(DeleteStatement),
    Create(CreateStatement),
    CreateIndex(CreateIndexStatement),
    Alter(AlterStatement),
    Drop(DropStatement),
    DropIndex(DropIndexStatement),
}

#[derive(Debug, Clone)]
pub struct SelectIntoStatement {
    pub table: String,
    pub query: SelectStatement,
}

#[derive(Debug, Clone)]
pub struct ExplainStatement {
    pub statement: Box<Statement>,
}

#[derive(Debug, Clone)]
pub struct DescribeStatement {
    pub table: String,
}

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub with_clause: Vec<CommonTableExpression>,
    pub distinct: bool,
    pub columns: Vec<SelectItem>,
    pub column_aliases: Vec<Option<String>>,
    pub from: TableReference,
    pub where_clause: Option<WhereClause>,
    pub group_by: Vec<Expression>,
    pub having_clause: Option<WhereClause>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub set_operation: Option<SetOperation>,
}

#[derive(Debug, Clone)]
pub struct CommonTableExpression {
    pub name: String,
    pub query: Box<SelectStatement>,
}

#[derive(Debug, Clone)]
pub struct SetOperation {
    pub operator: SetOperator,
    pub right: Box<SelectStatement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOperator {
    Union,
    UnionAll,
    Intersect,
    Except,
}

#[derive(Debug, Clone)]
pub enum SelectItem {
    Wildcard,
    Column(String),
    Expression(Expression),
    CountAll,
    Aggregate {
        function: AggregateFunction,
        column: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone)]
pub enum TableReference {
    Table(String, Option<String>),
    Derived {
        subquery: Box<SelectStatement>,
        alias: String,
    },
    CrossJoin(Box<TableReference>, Box<TableReference>),
    InnerJoin {
        left: Box<TableReference>,
        right: Box<TableReference>,
        on: Expression,
    },
    LeftJoin {
        left: Box<TableReference>,
        right: Box<TableReference>,
        on: Expression,
    },
    RightJoin {
        left: Box<TableReference>,
        right: Box<TableReference>,
        on: Expression,
    },
    FullOuterJoin {
        left: Box<TableReference>,
        right: Box<TableReference>,
        on: Expression,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableBinding {
    pub table_name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WhereClause {
    pub conditions: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub column: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub enum Expression {
    Column(String),
    Literal(LiteralValue),
    Parameter(usize),
    ScalarSubquery(Box<SelectStatement>),
    Cast {
        expr: Box<Expression>,
        target_type: SqlTypeName,
    },
    Case {
        branches: Vec<CaseWhenClause>,
        else_expr: Option<Box<Expression>>,
    },
    ScalarFunctionCall {
        function: ScalarFunction,
        args: Vec<Expression>,
    },
    AggregateCall {
        function: AggregateFunction,
        target: AggregateTarget,
    },
    UnaryMinus(Box<Expression>),
    UnaryNot(Box<Expression>),
    Binary {
        left: Box<Expression>,
        operator: ArithmeticOperator,
        right: Box<Expression>,
    },
    Comparison {
        left: Box<Expression>,
        operator: ComparisonOperator,
        right: Box<Expression>,
    },
    InList {
        expr: Box<Expression>,
        values: Vec<Expression>,
        is_not: bool,
    },
    InSubquery {
        expr: Box<Expression>,
        subquery: Box<SelectStatement>,
        is_not: bool,
    },
    Between {
        expr: Box<Expression>,
        lower: Box<Expression>,
        upper: Box<Expression>,
        is_not: bool,
    },
    Like {
        expr: Box<Expression>,
        pattern: Box<Expression>,
        is_not: bool,
    },
    Exists {
        subquery: Box<SelectStatement>,
        is_not: bool,
    },
    NullCheck {
        expr: Box<Expression>,
        is_not: bool,
    },
    Logical {
        left: Box<Expression>,
        operator: LogicalOperator,
        right: Box<Expression>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunction {
    Coalesce,
    IfNull,
    NullIf,
    Lower,
    Upper,
    Length,
    OctetLength,
    BitLength,
    Trim,
    Abs,
    Round,
    Concat,
    ConcatWs,
    Substring,
    LeftFn,
    RightFn,
    Greatest,
    Least,
    Replace,
    Repeat,
    Reverse,
    Locate,
    Hex,
    Unhex,
    Ceil,
    Floor,
    Power,
}

#[derive(Debug, Clone)]
pub struct CaseWhenClause {
    pub condition: Expression,
    pub result: Expression,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateTarget {
    All,
    Column(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalOperator {
    And,
    Or,
}

#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Vec<String>,
    pub source: InsertSource,
    pub on_duplicate: Option<Vec<UpdateAssignment>>,
}

#[derive(Debug, Clone)]
pub enum InsertSource {
    Values(Vec<Vec<Expression>>),
    Select(Box<SelectStatement>),
}

#[derive(Debug, Clone)]
pub struct UpdateAssignment {
    pub column: String,
    pub value: Expression,
}

#[derive(Debug, Clone)]
pub struct UpdateStatement {
    pub table: String,
    pub target_binding: Option<String>,
    pub source: Option<TableReference>,
    pub assignments: Vec<UpdateAssignment>,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, Clone)]
pub struct DeleteStatement {
    pub table: String,
    pub target_binding: Option<String>,
    pub source: Option<TableReference>,
    pub where_clause: Option<WhereClause>,
}

impl UpdateStatement {
    pub(crate) fn source(&self) -> TableReference {
        self.source
            .clone()
            .unwrap_or_else(|| TableReference::Table(self.table.clone(), None))
    }

    pub(crate) fn target_binding_name(&self) -> &str {
        self.target_binding.as_deref().unwrap_or(&self.table)
    }
}

impl DeleteStatement {
    pub(crate) fn source(&self) -> TableReference {
        self.source
            .clone()
            .unwrap_or_else(|| TableReference::Table(self.table.clone(), None))
    }

    pub(crate) fn target_binding_name(&self) -> &str {
        self.target_binding.as_deref().unwrap_or(&self.table)
    }
}

#[derive(Debug, Clone)]
pub struct CreateStatement {
    pub table: String,
    pub columns: Vec<ColumnDefinition>,
    pub constraints: Vec<TableConstraint>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DropStatement {
    pub table: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DropIndexStatement {
    pub index_name: String,
    pub table: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct AlterStatement {
    pub table: String,
    pub operation: AlterOperation,
}

#[derive(Debug, Clone)]
pub enum AlterOperation {
    RenameTo(String),
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    AddColumn(ColumnDefinition),
    AddConstraint(TableConstraint),
    DropColumn(String),
    DropConstraint(String),
    AlterColumnSetDefault {
        column_name: String,
        default_value: LiteralValue,
    },
    AlterColumnDropDefault {
        column_name: String,
    },
    AlterColumnSetNotNull {
        column_name: String,
    },
    AlterColumnDropNotNull {
        column_name: String,
    },
}

#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: SqlTypeName,
    pub character_set: Option<String>,
    pub collation: Option<String>,
    pub nullable: bool,
    pub primary_key: bool,
    pub auto_increment: bool,
    pub unique: bool,
    pub default_value: Option<LiteralValue>,
    pub check_constraint: Option<CheckConstraintDefinition>,
    pub references: Option<ForeignKeyDefinition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckConstraintDefinition {
    pub name: Option<String>,
    pub expression_sql: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyDefinition {
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniqueConstraintDefinition {
    pub name: Option<String>,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableConstraint {
    Check(CheckConstraintDefinition),
    Unique(UniqueConstraintDefinition),
    ForeignKey(ForeignKeyDefinition),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignKeyAction {
    Restrict,
    Cascade,
    SetNull,
}

impl Statement {
    pub fn is_read_only(&self) -> bool {
        matches!(
            self,
            Statement::Explain(_)
                | Statement::Describe(_)
                | Statement::ShowTables
                | Statement::ShowIndexes(_)
                | Statement::ShowCreateTable(_)
                | Statement::Select(_)
        )
    }

    pub fn mutates_schema(&self) -> bool {
        matches!(
            self,
            Statement::Create(_)
                | Statement::SelectInto(_)
                | Statement::CreateIndex(_)
                | Statement::Alter(_)
                | Statement::Drop(_)
                | Statement::DropIndex(_)
        )
    }

    pub fn parameter_count(&self) -> usize {
        let mut max_index: Option<usize> = None;
        self.visit_parameters(&mut |index| {
            max_index = Some(max_index.map_or(index, |current| current.max(index)));
        });
        max_index.map_or(0, |index| index + 1)
    }

    pub fn bind_parameters(&self, parameters: &[LiteralValue]) -> Result<Statement> {
        // Clone the (reusable) statement once, then substitute parameter
        // placeholders in place. This avoids rebuilding the entire AST field by
        // field and re-cloning every parameterless subtree on each execution.
        let mut bound = self.clone();
        bound.substitute_parameters(parameters)?;
        Ok(bound)
    }

    fn visit_parameters<F>(&self, f: &mut F)
    where
        F: FnMut(usize),
    {
        match self {
            Statement::Begin
            | Statement::Commit
            | Statement::Rollback
            | Statement::Savepoint(_)
            | Statement::RollbackToSavepoint(_)
            | Statement::ReleaseSavepoint(_)
            | Statement::Describe(_)
            | Statement::ShowTables
            | Statement::ShowIndexes(_)
            | Statement::ShowCreateTable(_) => {}
            Statement::Explain(explain) => explain.statement.visit_parameters(f),
            Statement::Select(select) => {
                select.visit_parameters(f);
            }
            Statement::SelectInto(select_into) => {
                select_into.query.visit_parameters(f);
            }
            Statement::Update(update) => {
                for assignment in &update.assignments {
                    assignment.value.visit_parameters(f);
                }
                if let Some(where_clause) = &update.where_clause {
                    where_clause.visit_parameters(f);
                }
            }
            Statement::Insert(insert) => {
                match &insert.source {
                    InsertSource::Values(rows) => {
                        for row in rows {
                            for expr in row {
                                expr.visit_parameters(f);
                            }
                        }
                    }
                    InsertSource::Select(select) => {
                        select.visit_parameters(f);
                    }
                }
                if let Some(assignments) = &insert.on_duplicate {
                    for assignment in assignments {
                        assignment.value.visit_parameters(f);
                    }
                }
            }
            Statement::Delete(delete) => {
                if let Some(where_clause) = &delete.where_clause {
                    where_clause.visit_parameters(f);
                }
            }
            Statement::Create(_)
            | Statement::CreateIndex(_)
            | Statement::Alter(_)
            | Statement::Drop(_)
            | Statement::DropIndex(_) => {}
        }
    }

    fn substitute_parameters(&mut self, parameters: &[LiteralValue]) -> Result<()> {
        match self {
            Statement::Explain(explain) => explain.statement.substitute_parameters(parameters)?,
            Statement::Select(select) => select.substitute_parameters(parameters)?,
            Statement::SelectInto(select_into) => {
                select_into.query.substitute_parameters(parameters)?
            }
            Statement::Update(update) => {
                for assignment in &mut update.assignments {
                    assignment.value.substitute_parameters(parameters)?;
                }
                if let Some(where_clause) = &mut update.where_clause {
                    where_clause.substitute_parameters(parameters)?;
                }
            }
            Statement::Insert(insert) => {
                match &mut insert.source {
                    InsertSource::Values(rows) => {
                        for row in rows {
                            for expr in row {
                                expr.substitute_parameters(parameters)?;
                            }
                        }
                    }
                    InsertSource::Select(select) => select.substitute_parameters(parameters)?,
                }
                if let Some(assignments) = &mut insert.on_duplicate {
                    for assignment in assignments {
                        assignment.value.substitute_parameters(parameters)?;
                    }
                }
            }
            Statement::Delete(delete) => {
                if let Some(where_clause) = &mut delete.where_clause {
                    where_clause.substitute_parameters(parameters)?;
                }
            }
            Statement::Begin
            | Statement::Commit
            | Statement::Rollback
            | Statement::Savepoint(_)
            | Statement::RollbackToSavepoint(_)
            | Statement::ReleaseSavepoint(_)
            | Statement::Describe(_)
            | Statement::ShowTables
            | Statement::ShowIndexes(_)
            | Statement::ShowCreateTable(_)
            | Statement::Create(_)
            | Statement::CreateIndex(_)
            | Statement::Alter(_)
            | Statement::Drop(_)
            | Statement::DropIndex(_) => {}
        }
        Ok(())
    }
}

impl WhereClause {
    fn visit_parameters<F>(&self, f: &mut F)
    where
        F: FnMut(usize),
    {
        for condition in &self.conditions {
            condition.visit_parameters(f);
        }
    }

    fn substitute_parameters(&mut self, parameters: &[LiteralValue]) -> Result<()> {
        for condition in &mut self.conditions {
            condition.substitute_parameters(parameters)?;
        }
        Ok(())
    }
}
impl SelectItem {
    fn visit_parameters<F>(&self, f: &mut F)
    where
        F: FnMut(usize),
    {
        match self {
            SelectItem::Expression(expr) => expr.visit_parameters(f),
            SelectItem::Wildcard
            | SelectItem::Column(_)
            | SelectItem::CountAll
            | SelectItem::Aggregate { .. } => {}
        }
    }

    fn substitute_parameters(&mut self, parameters: &[LiteralValue]) -> Result<()> {
        match self {
            SelectItem::Expression(expr) => expr.substitute_parameters(parameters)?,
            SelectItem::Wildcard
            | SelectItem::Column(_)
            | SelectItem::CountAll
            | SelectItem::Aggregate { .. } => {}
        }
        Ok(())
    }
}

impl Expression {
    fn visit_parameters<F>(&self, f: &mut F)
    where
        F: FnMut(usize),
    {
        match self {
            Expression::Parameter(index) => f(*index),
            Expression::ScalarSubquery(subquery) => subquery.visit_parameters(f),
            Expression::Cast { expr, .. } => expr.visit_parameters(f),
            Expression::Case {
                branches,
                else_expr,
            } => {
                for branch in branches {
                    branch.condition.visit_parameters(f);
                    branch.result.visit_parameters(f);
                }
                if let Some(else_expr) = else_expr {
                    else_expr.visit_parameters(f);
                }
            }
            Expression::ScalarFunctionCall { args, .. } => {
                for arg in args {
                    arg.visit_parameters(f);
                }
            }
            Expression::AggregateCall { .. } => {}
            Expression::UnaryMinus(expr) => expr.visit_parameters(f),
            Expression::UnaryNot(expr) => expr.visit_parameters(f),
            Expression::Binary { left, right, .. } => {
                left.visit_parameters(f);
                right.visit_parameters(f);
            }
            Expression::Comparison { left, right, .. } => {
                left.visit_parameters(f);
                right.visit_parameters(f);
            }
            Expression::InList { expr, values, .. } => {
                expr.visit_parameters(f);
                for value in values {
                    value.visit_parameters(f);
                }
            }
            Expression::InSubquery { expr, subquery, .. } => {
                expr.visit_parameters(f);
                subquery.visit_parameters(f);
            }
            Expression::Between {
                expr, lower, upper, ..
            } => {
                expr.visit_parameters(f);
                lower.visit_parameters(f);
                upper.visit_parameters(f);
            }
            Expression::Like { expr, pattern, .. } => {
                expr.visit_parameters(f);
                pattern.visit_parameters(f);
            }
            Expression::Exists { subquery, .. } => subquery.visit_parameters(f),
            Expression::NullCheck { expr, .. } => expr.visit_parameters(f),
            Expression::Logical { left, right, .. } => {
                left.visit_parameters(f);
                right.visit_parameters(f);
            }
            Expression::Column(_) | Expression::Literal(_) => {}
        }
    }

    fn substitute_parameters(&mut self, parameters: &[LiteralValue]) -> Result<()> {
        match self {
            Expression::Parameter(index) => {
                let value = parameters.get(*index).cloned().ok_or_else(|| {
                    HematiteError::ParseError(format!(
                        "Missing bound value for parameter {}",
                        *index + 1
                    ))
                })?;
                *self = Expression::Literal(value);
            }
            Expression::ScalarSubquery(subquery) => subquery.substitute_parameters(parameters)?,
            Expression::Cast { expr, .. } => expr.substitute_parameters(parameters)?,
            Expression::Case {
                branches,
                else_expr,
            } => {
                for branch in branches {
                    branch.condition.substitute_parameters(parameters)?;
                    branch.result.substitute_parameters(parameters)?;
                }
                if let Some(else_expr) = else_expr {
                    else_expr.substitute_parameters(parameters)?;
                }
            }
            Expression::ScalarFunctionCall { args, .. } => {
                for arg in args {
                    arg.substitute_parameters(parameters)?;
                }
            }
            Expression::AggregateCall { .. } => {}
            Expression::UnaryMinus(expr) => expr.substitute_parameters(parameters)?,
            Expression::UnaryNot(expr) => expr.substitute_parameters(parameters)?,
            Expression::Binary { left, right, .. } => {
                left.substitute_parameters(parameters)?;
                right.substitute_parameters(parameters)?;
            }
            Expression::Comparison { left, right, .. } => {
                left.substitute_parameters(parameters)?;
                right.substitute_parameters(parameters)?;
            }
            Expression::InList { expr, values, .. } => {
                expr.substitute_parameters(parameters)?;
                for value in values {
                    value.substitute_parameters(parameters)?;
                }
            }
            Expression::InSubquery { expr, subquery, .. } => {
                expr.substitute_parameters(parameters)?;
                subquery.substitute_parameters(parameters)?;
            }
            Expression::Between {
                expr, lower, upper, ..
            } => {
                expr.substitute_parameters(parameters)?;
                lower.substitute_parameters(parameters)?;
                upper.substitute_parameters(parameters)?;
            }
            Expression::Like { expr, pattern, .. } => {
                expr.substitute_parameters(parameters)?;
                pattern.substitute_parameters(parameters)?;
            }
            Expression::Exists { subquery, .. } => subquery.substitute_parameters(parameters)?,
            Expression::NullCheck { expr, .. } => expr.substitute_parameters(parameters)?,
            Expression::Logical { left, right, .. } => {
                left.substitute_parameters(parameters)?;
                right.substitute_parameters(parameters)?;
            }
            Expression::Column(_) | Expression::Literal(_) => {}
        }
        Ok(())
    }

    /// Whether any column reference outside a subquery satisfies `matches`.
    ///
    /// Subqueries are skipped because a column inside one resolves against the
    /// subquery's own sources first, so it is not this expression's to answer for.
    pub(crate) fn mentions_column<F>(&self, matches: &F) -> bool
    where
        F: Fn(&str) -> bool,
    {
        match self {
            Expression::Column(name) => matches(name),
            Expression::Literal(_)
            | Expression::Parameter(_)
            | Expression::ScalarSubquery(_)
            | Expression::AggregateCall { .. }
            | Expression::Exists { .. } => false,
            Expression::Cast { expr, .. }
            | Expression::UnaryMinus(expr)
            | Expression::UnaryNot(expr)
            | Expression::NullCheck { expr, .. }
            | Expression::InSubquery { expr, .. } => expr.mentions_column(matches),
            Expression::Case {
                branches,
                else_expr,
            } => {
                branches.iter().any(|branch| {
                    branch.condition.mentions_column(matches)
                        || branch.result.mentions_column(matches)
                }) || else_expr
                    .as_ref()
                    .is_some_and(|expr| expr.mentions_column(matches))
            }
            Expression::ScalarFunctionCall { args, .. } => {
                args.iter().any(|arg| arg.mentions_column(matches))
            }
            Expression::Binary { left, right, .. }
            | Expression::Comparison { left, right, .. }
            | Expression::Logical { left, right, .. } => {
                left.mentions_column(matches) || right.mentions_column(matches)
            }
            Expression::InList { expr, values, .. } => {
                expr.mentions_column(matches)
                    || values.iter().any(|value| value.mentions_column(matches))
            }
            Expression::Between {
                expr, lower, upper, ..
            } => {
                expr.mentions_column(matches)
                    || lower.mentions_column(matches)
                    || upper.mentions_column(matches)
            }
            Expression::Like { expr, pattern, .. } => {
                expr.mentions_column(matches) || pattern.mentions_column(matches)
            }
        }
    }

    /// Replaces every column reference outside a subquery that `matches` accepts with
    /// `value`, skipping subqueries for the reason [`Expression::mentions_column`]
    /// gives.
    pub(crate) fn bind_columns<F>(&mut self, matches: &F, value: &LiteralValue)
    where
        F: Fn(&str) -> bool,
    {
        match self {
            Expression::Column(name) => {
                if matches(name) {
                    *self = Expression::Literal(value.clone());
                }
            }
            Expression::Literal(_)
            | Expression::Parameter(_)
            | Expression::ScalarSubquery(_)
            | Expression::AggregateCall { .. }
            | Expression::Exists { .. } => {}
            Expression::Cast { expr, .. }
            | Expression::UnaryMinus(expr)
            | Expression::UnaryNot(expr)
            | Expression::NullCheck { expr, .. }
            | Expression::InSubquery { expr, .. } => expr.bind_columns(matches, value),
            Expression::Case {
                branches,
                else_expr,
            } => {
                for branch in branches {
                    branch.condition.bind_columns(matches, value);
                    branch.result.bind_columns(matches, value);
                }
                if let Some(else_expr) = else_expr {
                    else_expr.bind_columns(matches, value);
                }
            }
            Expression::ScalarFunctionCall { args, .. } => {
                for arg in args {
                    arg.bind_columns(matches, value);
                }
            }
            Expression::Binary { left, right, .. }
            | Expression::Comparison { left, right, .. }
            | Expression::Logical { left, right, .. } => {
                left.bind_columns(matches, value);
                right.bind_columns(matches, value);
            }
            Expression::InList { expr, values, .. } => {
                expr.bind_columns(matches, value);
                for candidate in values {
                    candidate.bind_columns(matches, value);
                }
            }
            Expression::Between {
                expr, lower, upper, ..
            } => {
                expr.bind_columns(matches, value);
                lower.bind_columns(matches, value);
                upper.bind_columns(matches, value);
            }
            Expression::Like { expr, pattern, .. } => {
                expr.bind_columns(matches, value);
                pattern.bind_columns(matches, value);
            }
        }
    }
}

impl SelectStatement {
    pub(crate) fn single_table_scope(table_name: &str) -> Self {
        Self {
            with_clause: Vec::new(),
            distinct: false,
            columns: Vec::new(),
            column_aliases: Vec::new(),
            from: TableReference::Table(table_name.to_string(), None),
            where_clause: None,
            group_by: Vec::new(),
            having_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operation: None,
        }
    }

    fn visit_parameters<F>(&self, f: &mut F)
    where
        F: FnMut(usize),
    {
        for item in &self.columns {
            item.visit_parameters(f);
        }
        for cte in &self.with_clause {
            cte.query.visit_parameters(f);
        }
        if let Some(where_clause) = &self.where_clause {
            where_clause.visit_parameters(f);
        }
        for expr in &self.group_by {
            expr.visit_parameters(f);
        }
        if let Some(having_clause) = &self.having_clause {
            having_clause.visit_parameters(f);
        }
        if let Some(set_operation) = &self.set_operation {
            set_operation.right.visit_parameters(f);
        }
    }

    fn substitute_parameters(&mut self, parameters: &[LiteralValue]) -> Result<()> {
        for item in &mut self.columns {
            item.substitute_parameters(parameters)?;
        }
        for cte in &mut self.with_clause {
            cte.query.substitute_parameters(parameters)?;
        }
        if let Some(where_clause) = &mut self.where_clause {
            where_clause.substitute_parameters(parameters)?;
        }
        for expr in &mut self.group_by {
            expr.substitute_parameters(parameters)?;
        }
        if let Some(having_clause) = &mut self.having_clause {
            having_clause.substitute_parameters(parameters)?;
        }
        if let Some(set_operation) = &mut self.set_operation {
            set_operation.right.substitute_parameters(parameters)?;
        }
        Ok(())
    }

    pub(crate) fn is_hidden_rowid(name: &str) -> bool {
        name.eq_ignore_ascii_case("rowid")
    }

    pub(crate) fn lookup_cte<'a>(&'a self, name: &str) -> Option<&'a CommonTableExpression> {
        self.with_clause
            .iter()
            .find(|cte| cte.name.eq_ignore_ascii_case(name))
    }

    pub(crate) fn references_cte(&self, name: &str) -> bool {
        self.lookup_cte(name).is_some()
    }

    pub(crate) fn has_non_table_source(&self) -> bool {
        self.from.has_non_table_source(self)
    }

    /// Splits `table.column` into its qualifier and column name.
    ///
    /// Scans bytes rather than going through `split_once('.')`, whose char-pattern
    /// searcher is UTF-8 aware and showed up as 3 % of a full scan's instructions.
    /// `.` is ASCII, so a byte position is always a char boundary and the slices
    /// below cannot split a multi-byte character.
    pub(crate) fn split_column_reference(name: &str) -> (Option<&str>, &str) {
        match name.as_bytes().iter().position(|&byte| byte == b'.') {
            Some(dot) => (Some(&name[..dot]), &name[dot + 1..]),
            None => (None, name),
        }
    }

    pub(crate) fn column_reference_name(name: &str) -> &str {
        Self::split_column_reference(name).1
    }

    pub(crate) fn default_output_name(item: &SelectItem, index: usize) -> Option<String> {
        match item {
            SelectItem::Wildcard => None,
            SelectItem::Column(name) => Some(Self::column_reference_name(name).to_string()),
            SelectItem::Expression(_) => Some(format!("expr{}", index + 1)),
            SelectItem::CountAll => Some("COUNT(*)".to_string()),
            SelectItem::Aggregate { function, column } => Some(format!(
                "{}({})",
                match function {
                    AggregateFunction::Count => "COUNT",
                    AggregateFunction::Sum => "SUM",
                    AggregateFunction::Avg => "AVG",
                    AggregateFunction::Min => "MIN",
                    AggregateFunction::Max => "MAX",
                },
                column
            )),
        }
    }

    pub(crate) fn output_name(&self, index: usize) -> Option<String> {
        self.column_aliases
            .get(index)
            .and_then(|alias| alias.clone())
            .or_else(|| {
                self.columns
                    .get(index)
                    .and_then(|item| Self::default_output_name(item, index))
            })
    }

    pub(crate) fn collect_table_bindings(from: &TableReference) -> Vec<TableBinding> {
        let mut bindings = Vec::new();
        Self::collect_table_bindings_into(from, &mut bindings);
        bindings
    }

    fn collect_table_bindings_into(from: &TableReference, bindings: &mut Vec<TableBinding>) {
        match from {
            TableReference::Table(table_name, alias) => bindings.push(TableBinding {
                table_name: table_name.clone(),
                alias: alias.clone(),
            }),
            TableReference::Derived { alias, .. } => bindings.push(TableBinding {
                table_name: alias.clone(),
                alias: None,
            }),
            TableReference::CrossJoin(left, right) => {
                Self::collect_table_bindings_into(left, bindings);
                Self::collect_table_bindings_into(right, bindings);
            }
            TableReference::InnerJoin { left, right, .. }
            | TableReference::LeftJoin { left, right, .. }
            | TableReference::RightJoin { left, right, .. }
            | TableReference::FullOuterJoin { left, right, .. } => {
                Self::collect_table_bindings_into(left, bindings);
                Self::collect_table_bindings_into(right, bindings);
            }
        }
    }
}

impl TableReference {
    pub(crate) fn has_non_table_source(&self, statement: &SelectStatement) -> bool {
        match self {
            TableReference::Table(table_name, _) => statement.references_cte(table_name),
            TableReference::Derived { .. } => true,
            TableReference::CrossJoin(left, right)
            | TableReference::InnerJoin { left, right, .. }
            | TableReference::LeftJoin { left, right, .. }
            | TableReference::RightJoin { left, right, .. }
            | TableReference::FullOuterJoin { left, right, .. } => {
                left.has_non_table_source(statement) || right.has_non_table_source(statement)
            }
        }
    }
}

impl Expression {
    pub(crate) fn references_column(&self, column_name: &str, table_name: Option<&str>) -> bool {
        match self {
            Expression::Column(name) => column_name_matches(name, column_name, table_name),
            Expression::ScalarSubquery(_) => false,
            Expression::Case {
                branches,
                else_expr,
            } => {
                branches.iter().any(|branch| {
                    branch.condition.references_column(column_name, table_name)
                        || branch.result.references_column(column_name, table_name)
                }) || else_expr
                    .as_ref()
                    .is_some_and(|expr| expr.references_column(column_name, table_name))
            }
            Expression::ScalarFunctionCall { args, .. } => args
                .iter()
                .any(|arg| arg.references_column(column_name, table_name)),
            Expression::AggregateCall { target, .. } => match target {
                AggregateTarget::All => false,
                AggregateTarget::Column(name) => column_name_matches(name, column_name, table_name),
            },
            Expression::Cast { expr, .. } => expr.references_column(column_name, table_name),
            Expression::UnaryMinus(expr) => expr.references_column(column_name, table_name),
            Expression::UnaryNot(expr) => expr.references_column(column_name, table_name),
            Expression::Binary { left, right, .. } => {
                left.references_column(column_name, table_name)
                    || right.references_column(column_name, table_name)
            }
            Expression::Comparison { left, right, .. } => {
                left.references_column(column_name, table_name)
                    || right.references_column(column_name, table_name)
            }
            Expression::InList { expr, values, .. } => {
                expr.references_column(column_name, table_name)
                    || values
                        .iter()
                        .any(|value| value.references_column(column_name, table_name))
            }
            Expression::InSubquery { expr, .. } => expr.references_column(column_name, table_name),
            Expression::Between {
                expr, lower, upper, ..
            } => {
                expr.references_column(column_name, table_name)
                    || lower.references_column(column_name, table_name)
                    || upper.references_column(column_name, table_name)
            }
            Expression::Like { expr, pattern, .. } => {
                expr.references_column(column_name, table_name)
                    || pattern.references_column(column_name, table_name)
            }
            Expression::Exists { .. } => false,
            Expression::NullCheck { expr, .. } => expr.references_column(column_name, table_name),
            Expression::Logical { left, right, .. } => {
                left.references_column(column_name, table_name)
                    || right.references_column(column_name, table_name)
            }
            Expression::Literal(_) | Expression::Parameter(_) => false,
        }
    }

    pub(crate) fn rename_column_references(
        &mut self,
        old_name: &str,
        new_name: &str,
        table_name: Option<&str>,
    ) {
        match self {
            Expression::Column(name) => {
                rename_column_name(name, old_name, new_name, table_name);
            }
            Expression::ScalarSubquery(_) => {}
            Expression::Case {
                branches,
                else_expr,
            } => {
                for branch in branches {
                    branch
                        .condition
                        .rename_column_references(old_name, new_name, table_name);
                    branch
                        .result
                        .rename_column_references(old_name, new_name, table_name);
                }
                if let Some(else_expr) = else_expr {
                    else_expr.rename_column_references(old_name, new_name, table_name);
                }
            }
            Expression::ScalarFunctionCall { args, .. } => {
                for arg in args {
                    arg.rename_column_references(old_name, new_name, table_name);
                }
            }
            Expression::AggregateCall { target, .. } => {
                if let AggregateTarget::Column(name) = target {
                    rename_column_name(name, old_name, new_name, table_name);
                }
            }
            Expression::Cast { expr, .. } => {
                expr.rename_column_references(old_name, new_name, table_name);
            }
            Expression::UnaryMinus(expr) => {
                expr.rename_column_references(old_name, new_name, table_name);
            }
            Expression::UnaryNot(expr) => {
                expr.rename_column_references(old_name, new_name, table_name);
            }
            Expression::Binary { left, right, .. } => {
                left.rename_column_references(old_name, new_name, table_name);
                right.rename_column_references(old_name, new_name, table_name);
            }
            Expression::Comparison { left, right, .. } => {
                left.rename_column_references(old_name, new_name, table_name);
                right.rename_column_references(old_name, new_name, table_name);
            }
            Expression::InList { expr, values, .. } => {
                expr.rename_column_references(old_name, new_name, table_name);
                for value in values {
                    value.rename_column_references(old_name, new_name, table_name);
                }
            }
            Expression::InSubquery { expr, .. } => {
                expr.rename_column_references(old_name, new_name, table_name);
            }
            Expression::Between {
                expr, lower, upper, ..
            } => {
                expr.rename_column_references(old_name, new_name, table_name);
                lower.rename_column_references(old_name, new_name, table_name);
                upper.rename_column_references(old_name, new_name, table_name);
            }
            Expression::Like { expr, pattern, .. } => {
                expr.rename_column_references(old_name, new_name, table_name);
                pattern.rename_column_references(old_name, new_name, table_name);
            }
            Expression::Exists { .. } => {}
            Expression::NullCheck { expr, .. } => {
                expr.rename_column_references(old_name, new_name, table_name);
            }
            Expression::Logical { left, right, .. } => {
                left.rename_column_references(old_name, new_name, table_name);
                right.rename_column_references(old_name, new_name, table_name);
            }
            Expression::Literal(_) | Expression::Parameter(_) => {}
        }
    }

    pub fn to_sql(&self) -> String {
        match self {
            Expression::Column(name) => name.clone(),
            Expression::Literal(value) => match value {
                LiteralValue::Integer(i) => i.to_string(),
                LiteralValue::Text(s) => format!("'{}'", s.replace('\'', "''")),
                LiteralValue::Blob(bytes) => {
                    format!(
                        "X'{}'",
                        bytes
                            .iter()
                            .map(|byte| format!("{byte:02X}"))
                            .collect::<String>()
                    )
                }
                LiteralValue::Boolean(true) => "TRUE".to_string(),
                LiteralValue::Boolean(false) => "FALSE".to_string(),
                LiteralValue::Float(f) => f.to_string(),
                LiteralValue::Null => "NULL".to_string(),
            },
            Expression::Parameter(index) => format!("?{}", index + 1),
            Expression::ScalarSubquery(_) => "(<subquery>)".to_string(),
            Expression::Case {
                branches,
                else_expr,
            } => {
                let mut parts = vec!["CASE".to_string()];
                for branch in branches {
                    parts.push(format!(
                        "WHEN {} THEN {}",
                        branch.condition.to_sql(),
                        branch.result.to_sql()
                    ));
                }
                if let Some(else_expr) = else_expr {
                    parts.push(format!("ELSE {}", else_expr.to_sql()));
                }
                parts.push("END".to_string());
                parts.join(" ")
            }
            Expression::ScalarFunctionCall { function, args } => format!(
                "{}({})",
                function.to_sql(),
                join_rendered(args.iter(), ", ", Expression::to_sql)
            ),
            Expression::AggregateCall { function, target } => {
                format!("{}({})", function.to_sql(), target.to_sql())
            }
            Expression::Cast { expr, target_type } => {
                format!("CAST({} AS {target_type})", expr.to_sql())
            }
            Expression::UnaryMinus(expr) => format!("-{}", expr.to_sql()),
            Expression::UnaryNot(expr) => format!("NOT {}", expr.to_sql()),
            Expression::Binary {
                left,
                operator,
                right,
            } => format!(
                "({} {} {})",
                left.to_sql(),
                operator.to_sql(),
                right.to_sql()
            ),
            Expression::Comparison {
                left,
                operator,
                right,
            } => format!("{} {} {}", left.to_sql(), operator.to_sql(), right.to_sql()),
            Expression::InList {
                expr,
                values,
                is_not,
            } => format!(
                "{} {}IN ({})",
                expr.to_sql(),
                if *is_not { "NOT " } else { "" },
                join_rendered(values.iter(), ", ", Expression::to_sql)
            ),
            Expression::InSubquery { expr, is_not, .. } => format!(
                "{} {}IN (<subquery>)",
                expr.to_sql(),
                if *is_not { "NOT " } else { "" }
            ),
            Expression::Between {
                expr,
                lower,
                upper,
                is_not,
            } => format!(
                "{} {}BETWEEN {} AND {}",
                expr.to_sql(),
                if *is_not { "NOT " } else { "" },
                lower.to_sql(),
                upper.to_sql()
            ),
            Expression::Like {
                expr,
                pattern,
                is_not,
            } => format!(
                "{} {}LIKE {}",
                expr.to_sql(),
                if *is_not { "NOT " } else { "" },
                pattern.to_sql()
            ),
            Expression::Exists { is_not, .. } => {
                format!("{}EXISTS (<subquery>)", if *is_not { "NOT " } else { "" })
            }
            Expression::NullCheck { expr, is_not } => format!(
                "{} IS {}NULL",
                expr.to_sql(),
                if *is_not { "NOT " } else { "" }
            ),
            Expression::Logical {
                left,
                operator,
                right,
            } => format!(
                "({}) {} ({})",
                left.to_sql(),
                operator.to_sql(),
                right.to_sql()
            ),
        }
    }
}

impl AggregateFunction {
    fn to_sql(self) -> &'static str {
        match self {
            AggregateFunction::Count => "COUNT",
            AggregateFunction::Sum => "SUM",
            AggregateFunction::Avg => "AVG",
            AggregateFunction::Min => "MIN",
            AggregateFunction::Max => "MAX",
        }
    }
}

impl ScalarFunction {
    pub fn from_identifier(name: &str) -> Option<Self> {
        match () {
            _ if name.eq_ignore_ascii_case("COALESCE") => Some(Self::Coalesce),
            _ if name.eq_ignore_ascii_case("IFNULL") => Some(Self::IfNull),
            _ if name.eq_ignore_ascii_case("NULLIF") => Some(Self::NullIf),
            _ if name.eq_ignore_ascii_case("LOWER") => Some(Self::Lower),
            _ if name.eq_ignore_ascii_case("UPPER") => Some(Self::Upper),
            _ if name.eq_ignore_ascii_case("LENGTH") => Some(Self::Length),
            _ if name.eq_ignore_ascii_case("OCTET_LENGTH") => Some(Self::OctetLength),
            _ if name.eq_ignore_ascii_case("BIT_LENGTH") => Some(Self::BitLength),
            _ if name.eq_ignore_ascii_case("TRIM") => Some(Self::Trim),
            _ if name.eq_ignore_ascii_case("ABS") => Some(Self::Abs),
            _ if name.eq_ignore_ascii_case("ROUND") => Some(Self::Round),
            _ if name.eq_ignore_ascii_case("CONCAT") => Some(Self::Concat),
            _ if name.eq_ignore_ascii_case("CONCAT_WS") => Some(Self::ConcatWs),
            _ if name.eq_ignore_ascii_case("SUBSTRING") || name.eq_ignore_ascii_case("SUBSTR") => {
                Some(Self::Substring)
            }
            _ if name.eq_ignore_ascii_case("LEFT") => Some(Self::LeftFn),
            _ if name.eq_ignore_ascii_case("RIGHT") => Some(Self::RightFn),
            _ if name.eq_ignore_ascii_case("GREATEST") => Some(Self::Greatest),
            _ if name.eq_ignore_ascii_case("LEAST") => Some(Self::Least),
            _ if name.eq_ignore_ascii_case("REPLACE") => Some(Self::Replace),
            _ if name.eq_ignore_ascii_case("REPEAT") => Some(Self::Repeat),
            _ if name.eq_ignore_ascii_case("REVERSE") => Some(Self::Reverse),
            _ if name.eq_ignore_ascii_case("LOCATE") => Some(Self::Locate),
            _ if name.eq_ignore_ascii_case("HEX") => Some(Self::Hex),
            _ if name.eq_ignore_ascii_case("UNHEX") => Some(Self::Unhex),
            _ if name.eq_ignore_ascii_case("CEIL") || name.eq_ignore_ascii_case("CEILING") => {
                Some(Self::Ceil)
            }
            _ if name.eq_ignore_ascii_case("FLOOR") => Some(Self::Floor),
            _ if name.eq_ignore_ascii_case("POWER") || name.eq_ignore_ascii_case("POW") => {
                Some(Self::Power)
            }
            _ => None,
        }
    }

    pub(crate) fn to_sql(self) -> &'static str {
        match self {
            ScalarFunction::Coalesce => "COALESCE",
            ScalarFunction::IfNull => "IFNULL",
            ScalarFunction::NullIf => "NULLIF",
            ScalarFunction::Lower => "LOWER",
            ScalarFunction::Upper => "UPPER",
            ScalarFunction::Length => "LENGTH",
            ScalarFunction::OctetLength => "OCTET_LENGTH",
            ScalarFunction::BitLength => "BIT_LENGTH",
            ScalarFunction::Trim => "TRIM",
            ScalarFunction::Abs => "ABS",
            ScalarFunction::Round => "ROUND",
            ScalarFunction::Concat => "CONCAT",
            ScalarFunction::ConcatWs => "CONCAT_WS",
            ScalarFunction::Substring => "SUBSTRING",
            ScalarFunction::LeftFn => "LEFT",
            ScalarFunction::RightFn => "RIGHT",
            ScalarFunction::Greatest => "GREATEST",
            ScalarFunction::Least => "LEAST",
            ScalarFunction::Replace => "REPLACE",
            ScalarFunction::Repeat => "REPEAT",
            ScalarFunction::Reverse => "REVERSE",
            ScalarFunction::Locate => "LOCATE",
            ScalarFunction::Hex => "HEX",
            ScalarFunction::Unhex => "UNHEX",
            ScalarFunction::Ceil => "CEIL",
            ScalarFunction::Floor => "FLOOR",
            ScalarFunction::Power => "POWER",
        }
    }
}

impl AggregateTarget {
    fn to_sql(&self) -> String {
        match self {
            AggregateTarget::All => "*".to_string(),
            AggregateTarget::Column(column) => column.clone(),
        }
    }
}

impl ComparisonOperator {
    fn to_sql(self) -> &'static str {
        match self {
            ComparisonOperator::Equal => "=",
            ComparisonOperator::NotEqual => "!=",
            ComparisonOperator::LessThan => "<",
            ComparisonOperator::LessThanOrEqual => "<=",
            ComparisonOperator::GreaterThan => ">",
            ComparisonOperator::GreaterThanOrEqual => ">=",
        }
    }
}

impl LogicalOperator {
    fn to_sql(self) -> &'static str {
        match self {
            LogicalOperator::And => "AND",
            LogicalOperator::Or => "OR",
        }
    }
}

impl ArithmeticOperator {
    fn to_sql(self) -> &'static str {
        match self {
            ArithmeticOperator::Add => "+",
            ArithmeticOperator::Subtract => "-",
            ArithmeticOperator::Multiply => "*",
            ArithmeticOperator::Divide => "/",
            ArithmeticOperator::Modulo => "%",
        }
    }
}

fn rename_column_name(name: &mut String, old_name: &str, new_name: &str, table_name: Option<&str>) {
    if name == old_name {
        *name = new_name.to_string();
    } else if let Some(table_name) = table_name {
        let qualified = format!("{}.{}", table_name, old_name);
        if name == &qualified {
            *name = format!("{}.{}", table_name, new_name);
        }
    }
}

fn column_name_matches(name: &str, column_name: &str, table_name: Option<&str>) -> bool {
    let (qualifier, bare_name) = SelectStatement::split_column_reference(name);
    if let Some(qualifier) = qualifier {
        qualifier == table_name.unwrap_or_default() && bare_name == column_name
    } else {
        name == column_name
    }
}
