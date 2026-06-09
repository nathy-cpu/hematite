//! Abstract syntax tree for SQL statements

use crate::error::{HematiteError, Result};
use crate::parser::types::{LiteralValue, SqlTypeName};

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
    ShowViews,
    ShowIndexes(Option<String>),
    ShowTriggers(Option<String>),
    ShowCreateTable(String),
    ShowCreateView(String),
    Select(SelectStatement),
    SelectInto(SelectIntoStatement),
    Update(UpdateStatement),
    Insert(InsertStatement),
    Delete(DeleteStatement),
    Create(CreateStatement),
    CreateView(CreateViewStatement),
    CreateTrigger(CreateTriggerStatement),
    CreateIndex(CreateIndexStatement),
    Alter(AlterStatement),
    Drop(DropStatement),
    DropView(DropViewStatement),
    DropTrigger(DropTriggerStatement),
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
    pub recursive: bool,
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
    Window {
        function: WindowFunction,
        window: WindowSpec,
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
pub struct WindowSpec {
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<OrderByItem>,
}

#[derive(Debug, Clone)]
pub enum WindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    Aggregate {
        function: AggregateFunction,
        target: AggregateTarget,
    },
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
    IntervalLiteral {
        value: String,
        qualifier: IntervalQualifier,
    },
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
pub enum IntervalQualifier {
    YearToMonth,
    DayToSecond,
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
    DateFn,
    TimeFn,
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    TimeToSec,
    SecToTime,
    UnixTimestamp,
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

#[derive(Debug, Clone)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

#[derive(Debug, Clone)]
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
pub struct CreateViewStatement {
    pub view: String,
    pub if_not_exists: bool,
    pub query: SelectStatement,
}

#[derive(Debug, Clone)]
pub struct CreateTriggerStatement {
    pub trigger: String,
    pub table: String,
    pub event: TriggerEvent,
    pub body: Box<Statement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
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
pub struct DropViewStatement {
    pub view: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DropTriggerStatement {
    pub trigger: String,
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
    pub(crate) fn to_sql(&self) -> String {
        match self {
            Statement::Select(select) => select.to_sql(),
            Statement::SelectInto(select_into) => select_into.to_sql(),
            Statement::Insert(insert) => {
                let mut sql = format!(
                    "INSERT INTO {} ({}) ",
                    insert.table,
                    insert.columns.join(", ")
                );
                match &insert.source {
                    InsertSource::Values(rows) => {
                        let rows_sql = rows
                            .iter()
                            .map(|row| {
                                format!(
                                    "({})",
                                    row.iter()
                                        .map(Expression::to_sql)
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        sql.push_str(&format!("VALUES {rows_sql}"));
                    }
                    InsertSource::Select(select) => {
                        sql.push_str(&select.to_sql());
                    }
                }
                if let Some(assignments) = &insert.on_duplicate {
                    sql.push_str(" ON DUPLICATE KEY UPDATE ");
                    sql.push_str(
                        &assignments
                            .iter()
                            .map(|assignment| {
                                format!("{} = {}", assignment.column, assignment.value.to_sql())
                            })
                            .collect::<Vec<_>>()
                            .join(", "),
                    );
                }
                sql
            }
            Statement::Update(update) => {
                let source = update.source();
                let mut sql = format!(
                    "UPDATE {} SET {}",
                    source.to_sql(),
                    update
                        .assignments
                        .iter()
                        .map(|assignment| {
                            format!("{} = {}", assignment.column, assignment.value.to_sql())
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                if let Some(where_clause) = &update.where_clause {
                    sql.push_str(&format!(
                        " WHERE {}",
                        where_clause
                            .conditions
                            .iter()
                            .map(Expression::to_sql)
                            .collect::<Vec<_>>()
                            .join(" AND ")
                    ));
                }
                sql
            }
            Statement::Delete(delete) => {
                let mut sql = match delete.source.as_ref() {
                    Some(source) => format!(
                        "DELETE {} FROM {}",
                        delete.target_binding_name(),
                        source.to_sql()
                    ),
                    None => format!("DELETE FROM {}", delete.table),
                };
                if let Some(where_clause) = &delete.where_clause {
                    sql.push_str(&format!(
                        " WHERE {}",
                        where_clause
                            .conditions
                            .iter()
                            .map(Expression::to_sql)
                            .collect::<Vec<_>>()
                            .join(" AND ")
                    ));
                }
                sql
            }
            Statement::Explain(explain) => format!("EXPLAIN {}", explain.statement.to_sql()),
            Statement::Describe(describe) => format!("DESCRIBE {}", describe.table),
            Statement::ShowTables => "SHOW TABLES".to_string(),
            Statement::ShowViews => "SHOW VIEWS".to_string(),
            Statement::ShowIndexes(table) => match table {
                Some(table) => format!("SHOW INDEXES FROM {table}"),
                None => "SHOW INDEXES".to_string(),
            },
            Statement::ShowTriggers(table) => match table {
                Some(table) => format!("SHOW TRIGGERS FROM {table}"),
                None => "SHOW TRIGGERS".to_string(),
            },
            Statement::ShowCreateTable(table) => format!("SHOW CREATE TABLE {table}"),
            Statement::ShowCreateView(view) => format!("SHOW CREATE VIEW {view}"),
            Statement::Begin => "BEGIN".to_string(),
            Statement::Commit => "COMMIT".to_string(),
            Statement::Rollback => "ROLLBACK".to_string(),
            Statement::Savepoint(name) => format!("SAVEPOINT {name}"),
            Statement::RollbackToSavepoint(name) => format!("ROLLBACK TO SAVEPOINT {name}"),
            Statement::ReleaseSavepoint(name) => format!("RELEASE SAVEPOINT {name}"),
            Statement::Create(_)
            | Statement::CreateView(_)
            | Statement::CreateTrigger(_)
            | Statement::CreateIndex(_)
            | Statement::Alter(_)
            | Statement::Drop(_)
            | Statement::DropView(_)
            | Statement::DropTrigger(_)
            | Statement::DropIndex(_) => format!("{self:?}"),
        }
    }

    fn into_select(self) -> Result<SelectStatement> {
        match self {
            Statement::Select(select) => Ok(select),
            _ => Err(HematiteError::InternalError(
                "expected SELECT statement while binding a subquery".to_string(),
            )),
        }
    }

    pub fn is_read_only(&self) -> bool {
        matches!(
            self,
            Statement::Explain(_)
                | Statement::Describe(_)
                | Statement::ShowTables
                | Statement::ShowViews
                | Statement::ShowIndexes(_)
                | Statement::ShowTriggers(_)
                | Statement::ShowCreateTable(_)
                | Statement::ShowCreateView(_)
                | Statement::Select(_)
        )
    }

    pub fn mutates_schema(&self) -> bool {
        matches!(
            self,
            Statement::Create(_)
                | Statement::SelectInto(_)
                | Statement::CreateView(_)
                | Statement::CreateTrigger(_)
                | Statement::CreateIndex(_)
                | Statement::Alter(_)
                | Statement::Drop(_)
                | Statement::DropView(_)
                | Statement::DropTrigger(_)
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
        self.bind_statement(parameters)
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
            | Statement::ShowViews
            | Statement::ShowIndexes(_)
            | Statement::ShowTriggers(_)
            | Statement::ShowCreateTable(_)
            | Statement::ShowCreateView(_) => {}
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
            | Statement::CreateView(_)
            | Statement::CreateTrigger(_)
            | Statement::CreateIndex(_)
            | Statement::Alter(_)
            | Statement::Drop(_)
            | Statement::DropView(_)
            | Statement::DropTrigger(_)
            | Statement::DropIndex(_) => {}
        }
    }

    fn bind_statement(&self, parameters: &[LiteralValue]) -> Result<Statement> {
        match self {
            Statement::Begin => Ok(Statement::Begin),
            Statement::Commit => Ok(Statement::Commit),
            Statement::Rollback => Ok(Statement::Rollback),
            Statement::Savepoint(name) => Ok(Statement::Savepoint(name.clone())),
            Statement::RollbackToSavepoint(name) => {
                Ok(Statement::RollbackToSavepoint(name.clone()))
            }
            Statement::ReleaseSavepoint(name) => Ok(Statement::ReleaseSavepoint(name.clone())),
            Statement::Explain(explain) => Ok(Statement::Explain(ExplainStatement {
                statement: Box::new(explain.statement.bind_parameters(parameters)?),
            })),
            Statement::Describe(describe) => Ok(Statement::Describe(describe.clone())),
            Statement::ShowTables => Ok(Statement::ShowTables),
            Statement::ShowViews => Ok(Statement::ShowViews),
            Statement::ShowIndexes(table) => Ok(Statement::ShowIndexes(table.clone())),
            Statement::ShowTriggers(table) => Ok(Statement::ShowTriggers(table.clone())),
            Statement::ShowCreateTable(table) => Ok(Statement::ShowCreateTable(table.clone())),
            Statement::ShowCreateView(view) => Ok(Statement::ShowCreateView(view.clone())),
            Statement::Select(select) => Ok(Statement::Select(SelectStatement {
                with_clause: select
                    .with_clause
                    .iter()
                    .map(|cte| {
                        Ok(CommonTableExpression {
                            name: cte.name.clone(),
                            recursive: cte.recursive,
                            query: Box::new(
                                Statement::Select((*cte.query).clone())
                                    .bind_parameters(parameters)?
                                    .into_select()?,
                            ),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                distinct: select.distinct,
                columns: select
                    .columns
                    .iter()
                    .map(|item| item.bind(parameters))
                    .collect::<Result<Vec<_>>>()?,
                column_aliases: select.column_aliases.clone(),
                from: select.from.clone(),
                where_clause: select
                    .where_clause
                    .as_ref()
                    .map(|where_clause| where_clause.bind(parameters))
                    .transpose()?,
                group_by: select
                    .group_by
                    .iter()
                    .map(|expr| expr.bind(parameters))
                    .collect::<Result<Vec<_>>>()?,
                having_clause: select
                    .having_clause
                    .as_ref()
                    .map(|having_clause| having_clause.bind(parameters))
                    .transpose()?,
                order_by: select.order_by.clone(),
                limit: select.limit,
                offset: select.offset,
                set_operation: select
                    .set_operation
                    .as_ref()
                    .map(|set_operation| {
                        Ok::<SetOperation, HematiteError>(SetOperation {
                            operator: set_operation.operator,
                            right: Box::new(
                                Statement::Select((*set_operation.right).clone())
                                    .bind_parameters(parameters)?
                                    .into_select()?,
                            ),
                        })
                    })
                    .transpose()?,
            })),
            Statement::SelectInto(select_into) => Ok(Statement::SelectInto(SelectIntoStatement {
                table: select_into.table.clone(),
                query: Statement::Select(select_into.query.clone())
                    .bind_parameters(parameters)?
                    .into_select()?,
            })),
            Statement::Update(update) => Ok(Statement::Update(UpdateStatement {
                table: update.table.clone(),
                target_binding: update.target_binding.clone(),
                source: update.source.clone(),
                assignments: update
                    .assignments
                    .iter()
                    .map(|assignment| {
                        Ok(UpdateAssignment {
                            column: assignment.column.clone(),
                            value: assignment.value.bind(parameters)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                where_clause: update
                    .where_clause
                    .as_ref()
                    .map(|where_clause| where_clause.bind(parameters))
                    .transpose()?,
            })),
            Statement::Insert(insert) => Ok(Statement::Insert(InsertStatement {
                table: insert.table.clone(),
                columns: insert.columns.clone(),
                source: match &insert.source {
                    InsertSource::Values(rows) => InsertSource::Values(
                        rows.iter()
                            .map(|row| {
                                row.iter()
                                    .map(|expr| expr.bind(parameters))
                                    .collect::<Result<Vec<_>>>()
                            })
                            .collect::<Result<Vec<_>>>()?,
                    ),
                    InsertSource::Select(select) => InsertSource::Select(Box::new(
                        Statement::Select((**select).clone())
                            .bind_parameters(parameters)?
                            .into_select()?,
                    )),
                },
                on_duplicate: insert
                    .on_duplicate
                    .as_ref()
                    .map(|assignments| {
                        assignments
                            .iter()
                            .map(|assignment| {
                                Ok(UpdateAssignment {
                                    column: assignment.column.clone(),
                                    value: assignment.value.bind(parameters)?,
                                })
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()?,
            })),
            Statement::Delete(delete) => Ok(Statement::Delete(DeleteStatement {
                table: delete.table.clone(),
                target_binding: delete.target_binding.clone(),
                source: delete.source.clone(),
                where_clause: delete
                    .where_clause
                    .as_ref()
                    .map(|where_clause| where_clause.bind(parameters))
                    .transpose()?,
            })),
            Statement::Create(create) => Ok(Statement::Create(create.clone())),
            Statement::CreateView(create_view) => Ok(Statement::CreateView(CreateViewStatement {
                view: create_view.view.clone(),
                if_not_exists: create_view.if_not_exists,
                query: Statement::Select(create_view.query.clone())
                    .bind_parameters(parameters)?
                    .into_select()?,
            })),
            Statement::CreateTrigger(create_trigger) => {
                Ok(Statement::CreateTrigger(CreateTriggerStatement {
                    trigger: create_trigger.trigger.clone(),
                    table: create_trigger.table.clone(),
                    event: create_trigger.event,
                    body: Box::new(create_trigger.body.bind_parameters(parameters)?),
                }))
            }
            Statement::CreateIndex(create_index) => {
                Ok(Statement::CreateIndex(create_index.clone()))
            }
            Statement::Alter(alter) => Ok(Statement::Alter(alter.clone())),
            Statement::Drop(drop) => Ok(Statement::Drop(drop.clone())),
            Statement::DropView(drop_view) => Ok(Statement::DropView(drop_view.clone())),
            Statement::DropTrigger(drop_trigger) => {
                Ok(Statement::DropTrigger(drop_trigger.clone()))
            }
            Statement::DropIndex(drop_index) => Ok(Statement::DropIndex(drop_index.clone())),
        }
    }
}

impl SelectIntoStatement {
    fn to_sql(&self) -> String {
        self.query.to_sql_with_into(&self.table)
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

    fn bind(&self, parameters: &[LiteralValue]) -> Result<WhereClause> {
        Ok(WhereClause {
            conditions: self
                .conditions
                .iter()
                .map(|condition| condition.bind(parameters))
                .collect::<Result<Vec<_>>>()?,
        })
    }
}
impl SelectItem {
    fn to_sql(&self) -> String {
        match self {
            SelectItem::Wildcard => "*".to_string(),
            SelectItem::Column(name) => name.clone(),
            SelectItem::Expression(expr) => expr.to_sql(),
            SelectItem::CountAll => "COUNT(*)".to_string(),
            SelectItem::Aggregate { function, column } => {
                format!("{}({})", function.to_sql(), column)
            }
            SelectItem::Window { function, window } => {
                format!("{} OVER ({})", function.to_sql(), window.to_sql())
            }
        }
    }

    fn visit_parameters<F>(&self, f: &mut F)
    where
        F: FnMut(usize),
    {
        match self {
            SelectItem::Expression(expr) => expr.visit_parameters(f),
            SelectItem::Window { window, .. } => {
                for expr in &window.partition_by {
                    expr.visit_parameters(f);
                }
            }
            SelectItem::Wildcard
            | SelectItem::Column(_)
            | SelectItem::CountAll
            | SelectItem::Aggregate { .. } => {}
        }
    }

    fn bind(&self, parameters: &[LiteralValue]) -> Result<SelectItem> {
        match self {
            SelectItem::Wildcard => Ok(SelectItem::Wildcard),
            SelectItem::Column(name) => Ok(SelectItem::Column(name.clone())),
            SelectItem::Expression(expr) => Ok(SelectItem::Expression(expr.bind(parameters)?)),
            SelectItem::CountAll => Ok(SelectItem::CountAll),
            SelectItem::Aggregate { function, column } => Ok(SelectItem::Aggregate {
                function: *function,
                column: column.clone(),
            }),
            SelectItem::Window { function, window } => Ok(SelectItem::Window {
                function: function.clone(),
                window: WindowSpec {
                    partition_by: window
                        .partition_by
                        .iter()
                        .map(|expr| expr.bind(parameters))
                        .collect::<Result<Vec<_>>>()?,
                    order_by: window.order_by.clone(),
                },
            }),
        }
    }
}

impl WindowSpec {
    fn to_sql(&self) -> String {
        let mut parts = Vec::new();
        if !self.partition_by.is_empty() {
            parts.push(format!(
                "PARTITION BY {}",
                self.partition_by
                    .iter()
                    .map(Expression::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if !self.order_by.is_empty() {
            parts.push(format!(
                "ORDER BY {}",
                self.order_by
                    .iter()
                    .map(|item| format!(
                        "{} {}",
                        item.column,
                        match item.direction {
                            SortDirection::Asc => "ASC",
                            SortDirection::Desc => "DESC",
                        }
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        parts.join(" ")
    }
}

impl WindowFunction {
    fn to_sql(&self) -> String {
        match self {
            WindowFunction::RowNumber => "ROW_NUMBER()".to_string(),
            WindowFunction::Rank => "RANK()".to_string(),
            WindowFunction::DenseRank => "DENSE_RANK()".to_string(),
            WindowFunction::Aggregate { function, target } => match target {
                AggregateTarget::All => format!("{}(*)", function.to_sql()),
                AggregateTarget::Column(column) => format!("{}({})", function.to_sql(), column),
            },
        }
    }
}

impl Expression {
    fn collect_dependency_names_into(&self, names: &mut std::collections::BTreeSet<String>) {
        match self {
            Expression::ScalarSubquery(subquery) => subquery.collect_dependency_names_into(names),
            Expression::Case {
                branches,
                else_expr,
            } => {
                for branch in branches {
                    branch.condition.collect_dependency_names_into(names);
                    branch.result.collect_dependency_names_into(names);
                }
                if let Some(else_expr) = else_expr {
                    else_expr.collect_dependency_names_into(names);
                }
            }
            Expression::ScalarFunctionCall { args, .. } => {
                for arg in args {
                    arg.collect_dependency_names_into(names);
                }
            }
            Expression::AggregateCall { .. } => {}
            Expression::Cast { expr, .. }
            | Expression::UnaryMinus(expr)
            | Expression::UnaryNot(expr) => expr.collect_dependency_names_into(names),
            Expression::Binary { left, right, .. }
            | Expression::Comparison { left, right, .. }
            | Expression::Logical { left, right, .. } => {
                left.collect_dependency_names_into(names);
                right.collect_dependency_names_into(names);
            }
            Expression::InList { expr, values, .. } => {
                expr.collect_dependency_names_into(names);
                for value in values {
                    value.collect_dependency_names_into(names);
                }
            }
            Expression::InSubquery { expr, subquery, .. } => {
                expr.collect_dependency_names_into(names);
                subquery.collect_dependency_names_into(names);
            }
            Expression::Between {
                expr, lower, upper, ..
            } => {
                expr.collect_dependency_names_into(names);
                lower.collect_dependency_names_into(names);
                upper.collect_dependency_names_into(names);
            }
            Expression::Like { expr, pattern, .. } => {
                expr.collect_dependency_names_into(names);
                pattern.collect_dependency_names_into(names);
            }
            Expression::Exists { subquery, .. } => subquery.collect_dependency_names_into(names),
            Expression::NullCheck { expr, .. } => expr.collect_dependency_names_into(names),
            Expression::Column(_)
            | Expression::Literal(_)
            | Expression::IntervalLiteral { .. }
            | Expression::Parameter(_) => {}
        }
    }

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
            Expression::Column(_) | Expression::Literal(_) | Expression::IntervalLiteral { .. } => {
            }
        }
    }

    fn bind(&self, parameters: &[LiteralValue]) -> Result<Expression> {
        match self {
            Expression::Column(name) => Ok(Expression::Column(name.clone())),
            Expression::Literal(value) => Ok(Expression::Literal(value.clone())),
            Expression::IntervalLiteral { value, qualifier } => Ok(Expression::IntervalLiteral {
                value: value.clone(),
                qualifier: *qualifier,
            }),
            Expression::Parameter(index) => parameters
                .get(*index)
                .cloned()
                .map(Expression::Literal)
                .ok_or_else(|| {
                    HematiteError::ParseError(format!(
                        "Missing bound value for parameter {}",
                        index + 1
                    ))
                }),
            Expression::ScalarSubquery(subquery) => Ok(Expression::ScalarSubquery(Box::new(
                Statement::Select((**subquery).clone())
                    .bind_parameters(parameters)?
                    .into_select()?,
            ))),
            Expression::Cast { expr, target_type } => Ok(Expression::Cast {
                expr: Box::new(expr.bind(parameters)?),
                target_type: target_type.clone(),
            }),
            Expression::Case {
                branches,
                else_expr,
            } => Ok(Expression::Case {
                branches: branches
                    .iter()
                    .map(|branch| {
                        Ok(CaseWhenClause {
                            condition: branch.condition.bind(parameters)?,
                            result: branch.result.bind(parameters)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                else_expr: else_expr
                    .as_ref()
                    .map(|expr| expr.bind(parameters).map(Box::new))
                    .transpose()?,
            }),
            Expression::ScalarFunctionCall { function, args } => {
                let mut bound_args = Vec::with_capacity(args.len());
                for arg in args {
                    bound_args.push(arg.bind(parameters)?);
                }
                Ok(Expression::ScalarFunctionCall {
                    function: *function,
                    args: bound_args,
                })
            }
            Expression::AggregateCall { function, target } => Ok(Expression::AggregateCall {
                function: *function,
                target: target.clone(),
            }),
            Expression::UnaryMinus(expr) => {
                Ok(Expression::UnaryMinus(Box::new(expr.bind(parameters)?)))
            }
            Expression::UnaryNot(expr) => {
                Ok(Expression::UnaryNot(Box::new(expr.bind(parameters)?)))
            }
            Expression::Binary {
                left,
                operator,
                right,
            } => Ok(Expression::Binary {
                left: Box::new(left.bind(parameters)?),
                operator: *operator,
                right: Box::new(right.bind(parameters)?),
            }),
            Expression::Comparison {
                left,
                operator,
                right,
            } => Ok(Expression::Comparison {
                left: Box::new(left.bind(parameters)?),
                operator: operator.clone(),
                right: Box::new(right.bind(parameters)?),
            }),
            Expression::InList {
                expr,
                values,
                is_not,
            } => Ok(Expression::InList {
                expr: Box::new(expr.bind(parameters)?),
                values: values
                    .iter()
                    .map(|value| value.bind(parameters))
                    .collect::<Result<Vec<_>>>()?,
                is_not: *is_not,
            }),
            Expression::InSubquery {
                expr,
                subquery,
                is_not,
            } => Ok(Expression::InSubquery {
                expr: Box::new(expr.bind(parameters)?),
                subquery: Box::new(
                    Statement::Select((**subquery).clone())
                        .bind_parameters(parameters)?
                        .into_select()?,
                ),
                is_not: *is_not,
            }),
            Expression::Between {
                expr,
                lower,
                upper,
                is_not,
            } => Ok(Expression::Between {
                expr: Box::new(expr.bind(parameters)?),
                lower: Box::new(lower.bind(parameters)?),
                upper: Box::new(upper.bind(parameters)?),
                is_not: *is_not,
            }),
            Expression::Like {
                expr,
                pattern,
                is_not,
            } => Ok(Expression::Like {
                expr: Box::new(expr.bind(parameters)?),
                pattern: Box::new(pattern.bind(parameters)?),
                is_not: *is_not,
            }),
            Expression::Exists { subquery, is_not } => Ok(Expression::Exists {
                subquery: Box::new(
                    Statement::Select((**subquery).clone())
                        .bind_parameters(parameters)?
                        .into_select()?,
                ),
                is_not: *is_not,
            }),
            Expression::NullCheck { expr, is_not } => Ok(Expression::NullCheck {
                expr: Box::new(expr.bind(parameters)?),
                is_not: *is_not,
            }),
            Expression::Logical {
                left,
                operator,
                right,
            } => Ok(Expression::Logical {
                left: Box::new(left.bind(parameters)?),
                operator: operator.clone(),
                right: Box::new(right.bind(parameters)?),
            }),
        }
    }
}

impl IntervalQualifier {
    pub fn to_sql(self) -> &'static str {
        match self {
            IntervalQualifier::YearToMonth => "YEAR TO MONTH",
            IntervalQualifier::DayToSecond => "DAY TO SECOND",
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

    pub(crate) fn references_source_name(&self, name: &str) -> bool {
        self.from.references_source_name(name)
            || self.where_clause.as_ref().is_some_and(|where_clause| {
                where_clause
                    .conditions
                    .iter()
                    .any(|condition| condition.references_source_name(name))
            })
            || self
                .group_by
                .iter()
                .any(|expr| expr.references_source_name(name))
            || self.having_clause.as_ref().is_some_and(|having_clause| {
                having_clause
                    .conditions
                    .iter()
                    .any(|condition| condition.references_source_name(name))
            })
            || self
                .set_operation
                .as_ref()
                .is_some_and(|set_operation| set_operation.right.references_source_name(name))
    }

    pub(crate) fn has_non_table_source(&self) -> bool {
        self.from.has_non_table_source(self)
    }

    pub(crate) fn split_column_reference(name: &str) -> (Option<&str>, &str) {
        match name.split_once('.') {
            Some((qualifier, column_name)) => (Some(qualifier), column_name),
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
            SelectItem::Window { function, .. } => Some(function.to_sql()),
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

    pub(crate) fn dependency_names(&self) -> Vec<String> {
        let mut names = std::collections::BTreeSet::new();
        self.collect_dependency_names_into(&mut names);
        names.into_iter().collect()
    }

    fn collect_dependency_names_into(&self, names: &mut std::collections::BTreeSet<String>) {
        for cte in &self.with_clause {
            cte.query.collect_dependency_names_into(names);
        }
        self.from.collect_dependency_names_into(names);
        if let Some(where_clause) = &self.where_clause {
            for condition in &where_clause.conditions {
                condition.collect_dependency_names_into(names);
            }
        }
        for expr in &self.group_by {
            expr.collect_dependency_names_into(names);
        }
        if let Some(having_clause) = &self.having_clause {
            for condition in &having_clause.conditions {
                condition.collect_dependency_names_into(names);
            }
        }
        if let Some(set_operation) = &self.set_operation {
            set_operation.right.collect_dependency_names_into(names);
        }
    }

    pub(crate) fn to_sql_with_into(&self, table: &str) -> String {
        let mut parts = Vec::new();
        if !self.with_clause.is_empty() {
            let recursive = self.with_clause.iter().any(|cte| cte.recursive);
            let ctes = self
                .with_clause
                .iter()
                .map(|cte| format!("{} AS ({})", cte.name, cte.query.to_sql()))
                .collect::<Vec<_>>()
                .join(", ");
            parts.push(format!(
                "WITH {}{}",
                if recursive { "RECURSIVE " } else { "" },
                ctes
            ));
        }

        let projections = self
            .columns
            .iter()
            .enumerate()
            .map(|(index, item)| {
                let base = item.to_sql();
                match self
                    .column_aliases
                    .get(index)
                    .and_then(|alias| alias.clone())
                {
                    Some(alias) => format!("{base} AS {alias}"),
                    None => base,
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        parts.push(format!(
            "SELECT{} {} INTO {}",
            if self.distinct { " DISTINCT" } else { "" },
            projections,
            table
        ));
        parts.push(format!("FROM {}", self.from.to_sql()));

        if let Some(where_clause) = &self.where_clause {
            parts.push(format!(
                "WHERE {}",
                where_clause
                    .conditions
                    .iter()
                    .map(Expression::to_sql)
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ));
        }
        if !self.group_by.is_empty() {
            parts.push(format!(
                "GROUP BY {}",
                self.group_by
                    .iter()
                    .map(Expression::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if let Some(having_clause) = &self.having_clause {
            parts.push(format!(
                "HAVING {}",
                having_clause
                    .conditions
                    .iter()
                    .map(Expression::to_sql)
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ));
        }
        if !self.order_by.is_empty() {
            parts.push(format!(
                "ORDER BY {}",
                self.order_by
                    .iter()
                    .map(|item| format!("{} {}", item.column, item.direction.to_sql()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if let Some(limit) = self.limit {
            parts.push(format!("LIMIT {}", limit));
        }
        if let Some(offset) = self.offset {
            parts.push(format!("OFFSET {}", offset));
        }
        if let Some(set_operation) = &self.set_operation {
            parts.push(format!(
                "{} {}",
                set_operation.operator.to_sql(),
                set_operation.right.to_sql()
            ));
        }

        parts.join(" ")
    }

    pub(crate) fn to_sql(&self) -> String {
        let mut parts = Vec::new();
        if !self.with_clause.is_empty() {
            let recursive = self.with_clause.iter().any(|cte| cte.recursive);
            let ctes = self
                .with_clause
                .iter()
                .map(|cte| format!("{} AS ({})", cte.name, cte.query.to_sql()))
                .collect::<Vec<_>>()
                .join(", ");
            parts.push(format!(
                "WITH {}{}",
                if recursive { "RECURSIVE " } else { "" },
                ctes
            ));
        }

        let projections = self
            .columns
            .iter()
            .enumerate()
            .map(|(index, item)| {
                let base = item.to_sql();
                match self
                    .column_aliases
                    .get(index)
                    .and_then(|alias| alias.clone())
                {
                    Some(alias) => format!("{base} AS {alias}"),
                    None => base,
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        parts.push(format!(
            "SELECT{} {}",
            if self.distinct { " DISTINCT" } else { "" },
            projections
        ));
        parts.push(format!("FROM {}", self.from.to_sql()));

        if let Some(where_clause) = &self.where_clause {
            parts.push(format!(
                "WHERE {}",
                where_clause
                    .conditions
                    .iter()
                    .map(Expression::to_sql)
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ));
        }
        if !self.group_by.is_empty() {
            parts.push(format!(
                "GROUP BY {}",
                self.group_by
                    .iter()
                    .map(Expression::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if let Some(having_clause) = &self.having_clause {
            parts.push(format!(
                "HAVING {}",
                having_clause
                    .conditions
                    .iter()
                    .map(Expression::to_sql)
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ));
        }
        if !self.order_by.is_empty() {
            parts.push(format!(
                "ORDER BY {}",
                self.order_by
                    .iter()
                    .map(|item| format!("{} {}", item.column, item.direction.to_sql()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if let Some(limit) = self.limit {
            parts.push(format!("LIMIT {}", limit));
        }
        if let Some(offset) = self.offset {
            parts.push(format!("OFFSET {}", offset));
        }
        if let Some(set_operation) = &self.set_operation {
            parts.push(format!(
                "{} {}",
                set_operation.operator.to_sql(),
                set_operation.right.to_sql()
            ));
        }

        parts.join(" ")
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
    fn collect_dependency_names_into(&self, names: &mut std::collections::BTreeSet<String>) {
        match self {
            TableReference::Table(table_name, _) => {
                names.insert(table_name.clone());
            }
            TableReference::Derived { subquery, .. } => {
                subquery.collect_dependency_names_into(names)
            }
            TableReference::CrossJoin(left, right) => {
                left.collect_dependency_names_into(names);
                right.collect_dependency_names_into(names);
            }
            TableReference::InnerJoin { left, right, on }
            | TableReference::LeftJoin { left, right, on }
            | TableReference::RightJoin { left, right, on }
            | TableReference::FullOuterJoin { left, right, on } => {
                left.collect_dependency_names_into(names);
                right.collect_dependency_names_into(names);
                on.collect_dependency_names_into(names);
            }
        }
    }

    fn to_sql(&self) -> String {
        match self {
            TableReference::Table(table_name, Some(alias)) => format!("{table_name} {alias}"),
            TableReference::Table(table_name, None) => table_name.clone(),
            TableReference::Derived { subquery, alias } => {
                format!("({}) {}", subquery.to_sql(), alias)
            }
            TableReference::CrossJoin(left, right) => {
                format!("{}, {}", left.to_sql(), right.to_sql())
            }
            TableReference::InnerJoin { left, right, on } => {
                format!(
                    "{} INNER JOIN {} ON {}",
                    left.to_sql(),
                    right.to_sql(),
                    on.to_sql()
                )
            }
            TableReference::LeftJoin { left, right, on } => {
                format!(
                    "{} LEFT JOIN {} ON {}",
                    left.to_sql(),
                    right.to_sql(),
                    on.to_sql()
                )
            }
            TableReference::RightJoin { left, right, on } => {
                format!(
                    "{} RIGHT JOIN {} ON {}",
                    left.to_sql(),
                    right.to_sql(),
                    on.to_sql()
                )
            }
            TableReference::FullOuterJoin { left, right, on } => format!(
                "{} FULL OUTER JOIN {} ON {}",
                left.to_sql(),
                right.to_sql(),
                on.to_sql()
            ),
        }
    }

    pub(crate) fn references_source_name(&self, name: &str) -> bool {
        match self {
            TableReference::Table(table_name, _) => table_name.eq_ignore_ascii_case(name),
            TableReference::Derived { subquery, .. } => subquery.references_source_name(name),
            TableReference::CrossJoin(left, right) => {
                left.references_source_name(name) || right.references_source_name(name)
            }
            TableReference::InnerJoin { left, right, on }
            | TableReference::LeftJoin { left, right, on }
            | TableReference::RightJoin { left, right, on }
            | TableReference::FullOuterJoin { left, right, on } => {
                left.references_source_name(name)
                    || right.references_source_name(name)
                    || on.references_source_name(name)
            }
        }
    }

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
    pub(crate) fn references_source_name(&self, name: &str) -> bool {
        match self {
            Expression::ScalarSubquery(subquery) => subquery.references_source_name(name),
            Expression::Case {
                branches,
                else_expr,
            } => {
                branches.iter().any(|branch| {
                    branch.condition.references_source_name(name)
                        || branch.result.references_source_name(name)
                }) || else_expr
                    .as_ref()
                    .is_some_and(|expr| expr.references_source_name(name))
            }
            Expression::ScalarFunctionCall { args, .. } => {
                args.iter().any(|arg| arg.references_source_name(name))
            }
            Expression::Cast { expr, .. } => expr.references_source_name(name),
            Expression::UnaryMinus(expr) => expr.references_source_name(name),
            Expression::UnaryNot(expr) => expr.references_source_name(name),
            Expression::Binary { left, right, .. } => {
                left.references_source_name(name) || right.references_source_name(name)
            }
            Expression::Comparison { left, right, .. } => {
                left.references_source_name(name) || right.references_source_name(name)
            }
            Expression::InList { expr, values, .. } => {
                expr.references_source_name(name)
                    || values
                        .iter()
                        .any(|value| value.references_source_name(name))
            }
            Expression::InSubquery { expr, subquery, .. } => {
                expr.references_source_name(name) || subquery.references_source_name(name)
            }
            Expression::Between {
                expr, lower, upper, ..
            } => {
                expr.references_source_name(name)
                    || lower.references_source_name(name)
                    || upper.references_source_name(name)
            }
            Expression::Like { expr, pattern, .. } => {
                expr.references_source_name(name) || pattern.references_source_name(name)
            }
            Expression::Exists { subquery, .. } => subquery.references_source_name(name),
            Expression::NullCheck { expr, .. } => expr.references_source_name(name),
            Expression::Logical { left, right, .. } => {
                left.references_source_name(name) || right.references_source_name(name)
            }
            Expression::Column(_)
            | Expression::Literal(_)
            | Expression::IntervalLiteral { .. }
            | Expression::Parameter(_)
            | Expression::AggregateCall { .. } => false,
        }
    }

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
            Expression::Literal(_)
            | Expression::IntervalLiteral { .. }
            | Expression::Parameter(_) => false,
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
            Expression::Literal(_)
            | Expression::IntervalLiteral { .. }
            | Expression::Parameter(_) => {}
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
            Expression::IntervalLiteral { value, qualifier } => {
                format!(
                    "INTERVAL '{}' {}",
                    value.replace('\'', "''"),
                    qualifier.to_sql()
                )
            }
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
                args.iter()
                    .map(Expression::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Expression::AggregateCall { function, target } => {
                format!("{}({})", function.to_sql(), target.to_sql())
            }
            Expression::Cast { expr, target_type } => {
                format!("CAST({} AS {})", expr.to_sql(), target_type.to_sql())
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
                values
                    .iter()
                    .map(Expression::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ")
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
            _ if name.eq_ignore_ascii_case("DATE") => Some(Self::DateFn),
            _ if name.eq_ignore_ascii_case("TIME") => Some(Self::TimeFn),
            _ if name.eq_ignore_ascii_case("YEAR") => Some(Self::Year),
            _ if name.eq_ignore_ascii_case("MONTH") => Some(Self::Month),
            _ if name.eq_ignore_ascii_case("DAY") => Some(Self::Day),
            _ if name.eq_ignore_ascii_case("HOUR") => Some(Self::Hour),
            _ if name.eq_ignore_ascii_case("MINUTE") => Some(Self::Minute),
            _ if name.eq_ignore_ascii_case("SECOND") => Some(Self::Second),
            _ if name.eq_ignore_ascii_case("TIME_TO_SEC") => Some(Self::TimeToSec),
            _ if name.eq_ignore_ascii_case("SEC_TO_TIME") => Some(Self::SecToTime),
            _ if name.eq_ignore_ascii_case("UNIX_TIMESTAMP") => Some(Self::UnixTimestamp),
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
            ScalarFunction::DateFn => "DATE",
            ScalarFunction::TimeFn => "TIME",
            ScalarFunction::Year => "YEAR",
            ScalarFunction::Month => "MONTH",
            ScalarFunction::Day => "DAY",
            ScalarFunction::Hour => "HOUR",
            ScalarFunction::Minute => "MINUTE",
            ScalarFunction::Second => "SECOND",
            ScalarFunction::TimeToSec => "TIME_TO_SEC",
            ScalarFunction::SecToTime => "SEC_TO_TIME",
            ScalarFunction::UnixTimestamp => "UNIX_TIMESTAMP",
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
    fn to_sql(&self) -> &'static str {
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
    fn to_sql(&self) -> &'static str {
        match self {
            LogicalOperator::And => "AND",
            LogicalOperator::Or => "OR",
        }
    }
}

impl ArithmeticOperator {
    fn to_sql(&self) -> &'static str {
        match self {
            ArithmeticOperator::Add => "+",
            ArithmeticOperator::Subtract => "-",
            ArithmeticOperator::Multiply => "*",
            ArithmeticOperator::Divide => "/",
            ArithmeticOperator::Modulo => "%",
        }
    }
}

impl SortDirection {
    fn to_sql(&self) -> &'static str {
        match self {
            SortDirection::Asc => "ASC",
            SortDirection::Desc => "DESC",
        }
    }
}

impl SetOperator {
    fn to_sql(&self) -> &'static str {
        match self {
            SetOperator::Union => "UNION",
            SetOperator::UnionAll => "UNION ALL",
            SetOperator::Intersect => "INTERSECT",
            SetOperator::Except => "EXCEPT",
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
