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
        on: Condition,
    },
    LeftJoin {
        left: Box<TableReference>,
        right: Box<TableReference>,
        on: Condition,
    },
    RightJoin {
        left: Box<TableReference>,
        right: Box<TableReference>,
        on: Condition,
    },
    FullOuterJoin {
        left: Box<TableReference>,
        right: Box<TableReference>,
        on: Condition,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableBinding {
    pub table_name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
#[cfg(test)]
struct SourceBinding {
    source_name: String,
    alias: Option<String>,
    columns: Vec<String>,
    has_hidden_rowid: bool,
}

#[derive(Debug, Clone)]
pub struct WhereClause {
    pub conditions: Vec<Condition>,
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
pub enum Condition {
    Comparison {
        left: Expression,
        operator: ComparisonOperator,
        right: Expression,
    },
    InList {
        expr: Expression,
        values: Vec<Expression>,
        is_not: bool,
    },
    InSubquery {
        expr: Expression,
        subquery: Box<SelectStatement>,
        is_not: bool,
    },
    Between {
        expr: Expression,
        lower: Expression,
        upper: Expression,
        is_not: bool,
    },
    Like {
        expr: Expression,
        pattern: Expression,
        is_not: bool,
    },
    Exists {
        subquery: Box<SelectStatement>,
        is_not: bool,
    },
    NullCheck {
        expr: Expression,
        is_not: bool,
    },
    Not(Box<Condition>),
    Logical {
        left: Box<Condition>,
        operator: LogicalOperator,
        right: Box<Condition>,
    },
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
                            .map(Condition::to_sql)
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
                            .map(Condition::to_sql)
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

    #[cfg(test)]
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        match self {
            Statement::Begin
            | Statement::Commit
            | Statement::Rollback
            | Statement::Savepoint(_)
            | Statement::RollbackToSavepoint(_)
            | Statement::ReleaseSavepoint(_)
            | Statement::ShowTables
            | Statement::ShowViews
            | Statement::ShowIndexes(_)
            | Statement::ShowTriggers(_)
            | Statement::ShowCreateTable(_)
            | Statement::ShowCreateView(_) => Ok(()),
            Statement::Explain(explain) => explain.statement.validate(catalog),
            Statement::Describe(describe) => {
                if catalog.get_table_by_name(&describe.table).is_none() {
                    Err(HematiteError::ParseError(format!(
                        "Table '{}' does not exist",
                        describe.table
                    )))
                } else {
                    Ok(())
                }
            }
            Statement::Select(select) => select.validate(catalog),
            Statement::SelectInto(select_into) => {
                if catalog.get_table_by_name(&select_into.table).is_some()
                    || catalog.view(&select_into.table).is_some()
                {
                    Err(HematiteError::ParseError(format!(
                        "Table '{}' already exists",
                        select_into.table
                    )))
                } else {
                    select_into.query.validate(catalog)
                }
            }
            Statement::Update(update) => update.validate(catalog),
            Statement::Insert(insert) => insert.validate(catalog),
            Statement::Delete(delete) => delete.validate(catalog),
            Statement::Create(create) => create.validate(catalog),
            Statement::CreateView(_create_view) => Ok(()),
            Statement::CreateTrigger(_create_trigger) => Ok(()),
            Statement::CreateIndex(create_index) => create_index.validate(catalog),
            Statement::Alter(alter) => alter.validate(catalog),
            Statement::Drop(drop) => drop.validate(catalog),
            Statement::DropView(_drop_view) => Ok(()),
            Statement::DropTrigger(_drop_trigger) => Ok(()),
            Statement::DropIndex(drop_index) => drop_index.validate(catalog),
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

impl Condition {
    fn collect_dependency_names_into(&self, names: &mut std::collections::BTreeSet<String>) {
        match self {
            Condition::Comparison { left, right, .. } => {
                left.collect_dependency_names_into(names);
                right.collect_dependency_names_into(names);
            }
            Condition::InList { expr, values, .. } => {
                expr.collect_dependency_names_into(names);
                for value in values {
                    value.collect_dependency_names_into(names);
                }
            }
            Condition::InSubquery { expr, subquery, .. } => {
                expr.collect_dependency_names_into(names);
                subquery.collect_dependency_names_into(names);
            }
            Condition::Between {
                expr, lower, upper, ..
            } => {
                expr.collect_dependency_names_into(names);
                lower.collect_dependency_names_into(names);
                upper.collect_dependency_names_into(names);
            }
            Condition::Like { expr, pattern, .. } => {
                expr.collect_dependency_names_into(names);
                pattern.collect_dependency_names_into(names);
            }
            Condition::Exists { subquery, .. } => subquery.collect_dependency_names_into(names),
            Condition::NullCheck { expr, .. } => expr.collect_dependency_names_into(names),
            Condition::Not(condition) => condition.collect_dependency_names_into(names),
            Condition::Logical { left, right, .. } => {
                left.collect_dependency_names_into(names);
                right.collect_dependency_names_into(names);
            }
        }
    }

    fn visit_parameters<F>(&self, f: &mut F)
    where
        F: FnMut(usize),
    {
        match self {
            Condition::Comparison { left, right, .. } => {
                left.visit_parameters(f);
                right.visit_parameters(f);
            }
            Condition::InList { expr, values, .. } => {
                expr.visit_parameters(f);
                for value in values {
                    value.visit_parameters(f);
                }
            }
            Condition::InSubquery { expr, subquery, .. } => {
                expr.visit_parameters(f);
                subquery.visit_parameters(f);
            }
            Condition::Between {
                expr, lower, upper, ..
            } => {
                expr.visit_parameters(f);
                lower.visit_parameters(f);
                upper.visit_parameters(f);
            }
            Condition::Like { expr, pattern, .. } => {
                expr.visit_parameters(f);
                pattern.visit_parameters(f);
            }
            Condition::Exists { subquery, .. } => subquery.visit_parameters(f),
            Condition::NullCheck { expr, .. } => expr.visit_parameters(f),
            Condition::Not(condition) => condition.visit_parameters(f),
            Condition::Logical { left, right, .. } => {
                left.visit_parameters(f);
                right.visit_parameters(f);
            }
        }
    }

    fn bind(&self, parameters: &[LiteralValue]) -> Result<Condition> {
        match self {
            Condition::Comparison {
                left,
                operator,
                right,
            } => Ok(Condition::Comparison {
                left: left.bind(parameters)?,
                operator: operator.clone(),
                right: right.bind(parameters)?,
            }),
            Condition::InList {
                expr,
                values,
                is_not,
            } => Ok(Condition::InList {
                expr: expr.bind(parameters)?,
                values: values
                    .iter()
                    .map(|value| value.bind(parameters))
                    .collect::<Result<Vec<_>>>()?,
                is_not: *is_not,
            }),
            Condition::InSubquery {
                expr,
                subquery,
                is_not,
            } => Ok(Condition::InSubquery {
                expr: expr.bind(parameters)?,
                subquery: Box::new(
                    Statement::Select((**subquery).clone())
                        .bind_parameters(parameters)?
                        .into_select()?,
                ),
                is_not: *is_not,
            }),
            Condition::Between {
                expr,
                lower,
                upper,
                is_not,
            } => Ok(Condition::Between {
                expr: expr.bind(parameters)?,
                lower: lower.bind(parameters)?,
                upper: upper.bind(parameters)?,
                is_not: *is_not,
            }),
            Condition::Like {
                expr,
                pattern,
                is_not,
            } => Ok(Condition::Like {
                expr: expr.bind(parameters)?,
                pattern: pattern.bind(parameters)?,
                is_not: *is_not,
            }),
            Condition::Exists { subquery, is_not } => Ok(Condition::Exists {
                subquery: Box::new(
                    Statement::Select((**subquery).clone())
                        .bind_parameters(parameters)?
                        .into_select()?,
                ),
                is_not: *is_not,
            }),
            Condition::NullCheck { expr, is_not } => Ok(Condition::NullCheck {
                expr: expr.bind(parameters)?,
                is_not: *is_not,
            }),
            Condition::Not(condition) => Ok(Condition::Not(Box::new(condition.bind(parameters)?))),
            Condition::Logical {
                left,
                operator,
                right,
            } => Ok(Condition::Logical {
                left: Box::new(left.bind(parameters)?),
                operator: operator.clone(),
                right: Box::new(right.bind(parameters)?),
            }),
        }
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
                    .map(Condition::to_sql)
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
                    .map(Condition::to_sql)
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
                    .map(Condition::to_sql)
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
                    .map(Condition::to_sql)
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

    #[cfg(test)]
    fn collect_source_bindings(
        &self,
        catalog: &crate::catalog::Schema,
        from: &TableReference,
    ) -> Result<Vec<SourceBinding>> {
        let mut bindings = Vec::new();
        self.collect_source_bindings_into(catalog, from, &mut bindings)?;
        Ok(bindings)
    }

    #[cfg(test)]
    fn collect_source_bindings_into(
        &self,
        catalog: &crate::catalog::Schema,
        from: &TableReference,
        bindings: &mut Vec<SourceBinding>,
    ) -> Result<()> {
        match from {
            TableReference::Table(table_name, alias) => {
                if let Some(cte) = self.lookup_cte(table_name) {
                    if !cte.recursive {
                        cte.query.validate(catalog)?;
                    }
                    bindings.push(SourceBinding {
                        source_name: table_name.clone(),
                        alias: alias.clone(),
                        columns: if cte.recursive {
                            cte.query
                                .columns
                                .iter()
                                .enumerate()
                                .map(|(index, _)| {
                                    cte.query.output_name(index).ok_or_else(|| {
                                        HematiteError::ParseError(format!(
                                            "Recursive CTE '{}' requires a name for projected column {}",
                                            cte.name,
                                            index + 1
                                        ))
                                    })
                                })
                                .collect::<Result<Vec<_>>>()?
                        } else {
                            cte.query.projected_column_names(catalog)?
                        },
                        has_hidden_rowid: false,
                    });
                    Ok(())
                } else {
                    let table = catalog.get_table_by_name(table_name).ok_or_else(|| {
                        HematiteError::ParseError(format!("Table '{}' does not exist", table_name))
                    })?;
                    bindings.push(SourceBinding {
                        source_name: table_name.clone(),
                        alias: alias.clone(),
                        columns: table
                            .columns
                            .iter()
                            .map(|column| column.name.clone())
                            .collect(),
                        has_hidden_rowid: true,
                    });
                    Ok(())
                }
            }
            TableReference::Derived { subquery, alias } => {
                subquery.validate(catalog)?;
                bindings.push(SourceBinding {
                    source_name: alias.clone(),
                    alias: None,
                    columns: subquery.projected_column_names(catalog)?,
                    has_hidden_rowid: false,
                });
                Ok(())
            }
            TableReference::CrossJoin(left, right) => {
                self.collect_source_bindings_into(catalog, left, bindings)?;
                self.collect_source_bindings_into(catalog, right, bindings)
            }
            TableReference::InnerJoin { left, right, .. }
            | TableReference::LeftJoin { left, right, .. }
            | TableReference::RightJoin { left, right, .. }
            | TableReference::FullOuterJoin { left, right, .. } => {
                self.collect_source_bindings_into(catalog, left, bindings)?;
                self.collect_source_bindings_into(catalog, right, bindings)
            }
        }
    }

    #[cfg(test)]
    fn projected_column_names(&self, catalog: &crate::catalog::Schema) -> Result<Vec<String>> {
        let mut names = Vec::with_capacity(self.columns.len());
        for (index, item) in self.columns.iter().enumerate() {
            if let Some(alias) = self
                .column_aliases
                .get(index)
                .and_then(|alias| alias.clone())
            {
                names.push(alias);
                continue;
            }

            match item {
                SelectItem::Wildcard => {
                    names.extend(
                        self.collect_source_bindings(catalog, &self.from)?
                            .into_iter()
                            .flat_map(|binding| binding.columns),
                    );
                }
                SelectItem::Column(name) => {
                    self.validate_column_reference(name, catalog, &self.from)?;
                    if let Some(name) = Self::default_output_name(item, index) {
                        names.push(name);
                    }
                }
                SelectItem::CountAll | SelectItem::Aggregate { .. } => {
                    if let Some(name) = Self::default_output_name(item, index) {
                        names.push(name);
                    }
                }
                SelectItem::Window { .. } => {
                    if let Some(name) = Self::default_output_name(item, index) {
                        names.push(name);
                    }
                }
                SelectItem::Expression(_) => {
                    return Err(HematiteError::ParseError(
                        "Expression projections in derived tables or CTEs require aliases"
                            .to_string(),
                    ))
                }
            }
        }
        Ok(names)
    }

    #[cfg(test)]
    pub(crate) fn validate_column_reference(
        &self,
        name: &str,
        catalog: &crate::catalog::Schema,
        from: &TableReference,
    ) -> Result<()> {
        self.validate_column_reference_with_outer(name, catalog, from, &[])
    }

    #[cfg(test)]
    fn validate_column_reference_with_outer(
        &self,
        name: &str,
        catalog: &crate::catalog::Schema,
        from: &TableReference,
        outer_bindings: &[SourceBinding],
    ) -> Result<()> {
        let (qualifier, column_name) = Self::split_column_reference(name);
        let local_bindings = self.collect_source_bindings(catalog, from)?;
        let local_matches =
            Self::collect_matching_source_names(qualifier, column_name, &local_bindings)?;
        if !local_matches.is_empty() {
            return match local_matches.len() {
                1 => Ok(()),
                _ => Err(HematiteError::ParseError(format!(
                    "Column reference '{}' is ambiguous",
                    name
                ))),
            };
        }

        let outer_matches =
            Self::collect_matching_source_names(qualifier, column_name, outer_bindings)?;
        match outer_matches.len() {
            0 => {
                if let Some(qualifier) = qualifier {
                    Err(HematiteError::ParseError(format!(
                        "Column '{}' does not exist in table '{}'",
                        column_name, qualifier
                    )))
                } else {
                    Err(HematiteError::ParseError(format!(
                        "Column '{}' does not exist in the query source set",
                        column_name
                    )))
                }
            }
            1 => Ok(()),
            _ => Err(HematiteError::ParseError(format!(
                "Column reference '{}' is ambiguous",
                name
            ))),
        }
    }

    #[cfg(test)]
    fn collect_matching_source_names(
        qualifier: Option<&str>,
        column_name: &str,
        bindings: &[SourceBinding],
    ) -> Result<Vec<String>> {
        let candidate_bindings: Vec<&SourceBinding> = if let Some(qualifier) = qualifier {
            bindings
                .iter()
                .filter(|binding| {
                    binding.source_name == qualifier
                        || binding
                            .alias
                            .as_deref()
                            .is_some_and(|alias| alias == qualifier)
                })
                .collect()
        } else {
            bindings.iter().collect()
        };
        let mut matched_tables = Vec::new();

        for binding in candidate_bindings {
            if binding.columns.iter().any(|column| column == column_name)
                || (binding.has_hidden_rowid && Self::is_hidden_rowid(column_name))
            {
                matched_tables.push(binding.source_name.clone());
            }
        }

        Ok(matched_tables)
    }

    #[cfg(test)]
    fn combined_outer_bindings(
        &self,
        catalog: &crate::catalog::Schema,
        from: &TableReference,
        outer_bindings: &[SourceBinding],
    ) -> Result<Vec<SourceBinding>> {
        let mut bindings = self.collect_source_bindings(catalog, from)?;
        bindings.extend(outer_bindings.iter().cloned());
        Ok(bindings)
    }

    #[cfg(test)]
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        self.validate_with_outer_bindings(catalog, &[])
    }

    #[cfg(test)]
    fn validate_with_outer_bindings(
        &self,
        catalog: &crate::catalog::Schema,
        outer_bindings: &[SourceBinding],
    ) -> Result<()> {
        if let Some(set_operation) = &self.set_operation {
            set_operation
                .right
                .validate_with_outer_bindings(catalog, outer_bindings)?;
            if self.columns.len() != set_operation.right.columns.len() {
                return Err(HematiteError::ParseError(
                    "Set operations require both queries to project the same number of columns"
                        .to_string(),
                ));
            }
        }

        for cte in &self.with_clause {
            if cte.recursive {
                let set_operation = cte.query.set_operation.as_ref().ok_or_else(|| {
                    HematiteError::ParseError(format!(
                        "Recursive CTE '{}' requires UNION or UNION ALL",
                        cte.name
                    ))
                })?;
                if !matches!(
                    set_operation.operator,
                    SetOperator::Union | SetOperator::UnionAll
                ) {
                    return Err(HematiteError::ParseError(format!(
                        "Recursive CTE '{}' requires UNION or UNION ALL",
                        cte.name
                    )));
                }

                let mut anchor = (*cte.query).clone();
                anchor.set_operation = None;
                if anchor.references_source_name(&cte.name) {
                    return Err(HematiteError::ParseError(format!(
                        "Recursive CTE '{}' anchor term cannot reference itself",
                        cte.name
                    )));
                }
                if !set_operation.right.references_source_name(&cte.name) {
                    return Err(HematiteError::ParseError(format!(
                        "Recursive CTE '{}' recursive term must reference itself",
                        cte.name
                    )));
                }
                if anchor.columns.len() != set_operation.right.columns.len() {
                    return Err(HematiteError::ParseError(format!(
                        "Recursive CTE '{}' anchor and recursive terms must project the same number of columns",
                        cte.name
                    )));
                }

                anchor.validate(catalog)?;

                let mut recursive_term = (*set_operation.right).clone();
                recursive_term.with_clause.push(CommonTableExpression {
                    name: cte.name.clone(),
                    recursive: false,
                    query: Box::new(anchor.clone()),
                });
                recursive_term.validate(catalog)?;
            } else {
                cte.query.validate(catalog)?;
            }
        }

        let bindings = self.collect_source_bindings(catalog, &self.from)?;
        if bindings.is_empty() {
            return Err(HematiteError::ParseError(
                "SELECT requires at least one table source".to_string(),
            ));
        }
        self.validate_table_reference(catalog, &self.from, outer_bindings)?;

        let has_aggregate = self.columns.iter().any(|item| match item {
            SelectItem::CountAll | SelectItem::Aggregate { .. } => true,
            SelectItem::Expression(expr) => Self::expression_contains_aggregate(expr),
            SelectItem::Window { .. } | SelectItem::Wildcard | SelectItem::Column(_) => false,
        }) || self.having_clause.as_ref().is_some_and(|having| {
            having
                .conditions
                .iter()
                .any(Self::condition_contains_aggregate)
        });
        if self.distinct && has_aggregate {
            return Err(HematiteError::ParseError(
                "DISTINCT cannot be combined with aggregate select items yet".to_string(),
            ));
        }

        for item in &self.columns {
            match item {
                SelectItem::Column(name) => self.validate_column_reference_with_outer(
                    name,
                    catalog,
                    &self.from,
                    outer_bindings,
                )?,
                SelectItem::Expression(expr) => {
                    self.validate_expression(expr, catalog, &self.from, outer_bindings)?;
                }
                SelectItem::Aggregate { column, .. } => {
                    self.validate_column_reference_with_outer(
                        column,
                        catalog,
                        &self.from,
                        outer_bindings,
                    )?;
                }
                SelectItem::Window { window, .. } => {
                    for expr in &window.partition_by {
                        self.validate_expression(expr, catalog, &self.from, outer_bindings)?;
                    }
                    for item in &window.order_by {
                        self.validate_column_reference_with_outer(
                            &item.column,
                            catalog,
                            &self.from,
                            outer_bindings,
                        )?;
                    }
                }
                SelectItem::Wildcard | SelectItem::CountAll => {} // Always valid
            }
        }

        if let Some(where_clause) = &self.where_clause {
            for condition in &where_clause.conditions {
                self.validate_condition(condition, catalog, &self.from, outer_bindings)?;
            }
        }

        for expr in &self.group_by {
            self.validate_expression(expr, catalog, &self.from, outer_bindings)?;
        }

        if !self.group_by.is_empty() {
            for item in &self.columns {
                match item {
                    SelectItem::Wildcard => {
                        return Err(HematiteError::ParseError(
                            "Wildcard select is not supported with GROUP BY".to_string(),
                        ));
                    }
                    SelectItem::Column(name) => {
                        let grouped = self.group_by.iter().any(|expr| {
                            matches!(expr, Expression::Column(group_name) if group_name == name)
                        });
                        if !grouped {
                            return Err(HematiteError::ParseError(format!(
                                "Selected column '{}' must appear in GROUP BY or be aggregated",
                                name
                            )));
                        }
                    }
                    SelectItem::Expression(_) => {
                        return Err(HematiteError::ParseError(
                            "Expression select items are not supported with GROUP BY yet"
                                .to_string(),
                        ));
                    }
                    SelectItem::Window { .. } => {
                        return Err(HematiteError::ParseError(
                            "Window functions cannot be combined with GROUP BY yet".to_string(),
                        ));
                    }
                    SelectItem::CountAll | SelectItem::Aggregate { .. } => {}
                }
            }
        } else if has_aggregate
            && self
                .columns
                .iter()
                .any(|item| !matches!(item, SelectItem::CountAll | SelectItem::Aggregate { .. }))
        {
            return Err(HematiteError::ParseError(
                "Aggregate select items cannot be combined with non-aggregate select items without GROUP BY"
                    .to_string(),
            ));
        }

        if self.having_clause.is_some() && self.group_by.is_empty() && !has_aggregate {
            return Err(HematiteError::ParseError(
                "HAVING requires GROUP BY or aggregate select items".to_string(),
            ));
        }

        for item in &self.order_by {
            self.validate_column_reference_with_outer(
                &item.column,
                catalog,
                &self.from,
                outer_bindings,
            )?;
        }

        Ok(())
    }

    #[cfg(test)]
    fn validate_table_reference(
        &self,
        catalog: &crate::catalog::Schema,
        from: &TableReference,
        outer_bindings: &[SourceBinding],
    ) -> Result<()> {
        match from {
            TableReference::Table(_, _) => Ok(()),
            TableReference::Derived { subquery, .. } => {
                subquery.validate(catalog)?;
                let _ = subquery.projected_column_names(catalog)?;
                Ok(())
            }
            TableReference::CrossJoin(left, right) => {
                self.validate_table_reference(catalog, left, outer_bindings)?;
                self.validate_table_reference(catalog, right, outer_bindings)
            }
            TableReference::InnerJoin { left, right, on }
            | TableReference::LeftJoin { left, right, on }
            | TableReference::RightJoin { left, right, on }
            | TableReference::FullOuterJoin { left, right, on } => {
                self.validate_table_reference(catalog, left, outer_bindings)?;
                self.validate_table_reference(catalog, right, outer_bindings)?;
                self.validate_condition(on, catalog, from, outer_bindings)
            }
        }
    }

    #[cfg(test)]
    fn validate_condition(
        &self,
        condition: &Condition,
        catalog: &crate::catalog::Schema,
        from: &TableReference,
        outer_bindings: &[SourceBinding],
    ) -> Result<()> {
        match condition {
            Condition::Comparison { left, right, .. } => {
                self.validate_expression(left, catalog, from, outer_bindings)?;
                self.validate_expression(right, catalog, from, outer_bindings)?;
            }
            Condition::InList { expr, values, .. } => {
                self.validate_expression(expr, catalog, from, outer_bindings)?;
                for value in values {
                    self.validate_expression(value, catalog, from, outer_bindings)?;
                }
            }
            Condition::InSubquery { expr, subquery, .. } => {
                self.validate_expression(expr, catalog, from, outer_bindings)?;
                subquery.validate_with_outer_bindings(
                    catalog,
                    &self.combined_outer_bindings(catalog, from, outer_bindings)?,
                )?;
                if subquery.columns.len() != 1 {
                    return Err(HematiteError::ParseError(
                        "Subquery predicates require exactly one selected column".to_string(),
                    ));
                }
            }
            Condition::Between {
                expr, lower, upper, ..
            } => {
                self.validate_expression(expr, catalog, from, outer_bindings)?;
                self.validate_expression(lower, catalog, from, outer_bindings)?;
                self.validate_expression(upper, catalog, from, outer_bindings)?;
            }
            Condition::Like { expr, pattern, .. } => {
                self.validate_expression(expr, catalog, from, outer_bindings)?;
                self.validate_expression(pattern, catalog, from, outer_bindings)?;
            }
            Condition::Exists { subquery, .. } => {
                subquery.validate_with_outer_bindings(
                    catalog,
                    &self.combined_outer_bindings(catalog, from, outer_bindings)?,
                )?;
            }
            Condition::NullCheck { expr, .. } => {
                self.validate_expression(expr, catalog, from, outer_bindings)?;
            }
            Condition::Not(condition) => {
                self.validate_condition(condition, catalog, from, outer_bindings)?;
            }
            Condition::Logical { left, right, .. } => {
                self.validate_condition(left, catalog, from, outer_bindings)?;
                self.validate_condition(right, catalog, from, outer_bindings)?;
            }
        }

        Ok(())
    }

    #[cfg(test)]
    fn validate_expression(
        &self,
        expr: &Expression,
        catalog: &crate::catalog::Schema,
        from: &TableReference,
        outer_bindings: &[SourceBinding],
    ) -> Result<()> {
        match expr {
            Expression::Column(name) => {
                self.validate_column_reference_with_outer(name, catalog, from, outer_bindings)?
            }
            Expression::ScalarSubquery(subquery) => {
                subquery.validate_with_outer_bindings(
                    catalog,
                    &self.combined_outer_bindings(catalog, from, outer_bindings)?,
                )?;
                if subquery.columns.len() != 1 {
                    return Err(HematiteError::ParseError(
                        "Scalar subqueries require exactly one selected column".to_string(),
                    ));
                }
            }
            Expression::Case {
                branches,
                else_expr,
            } => {
                for branch in branches {
                    self.validate_expression(&branch.condition, catalog, from, outer_bindings)?;
                    self.validate_expression(&branch.result, catalog, from, outer_bindings)?;
                }
                if let Some(else_expr) = else_expr {
                    self.validate_expression(else_expr, catalog, from, outer_bindings)?;
                }
            }
            Expression::ScalarFunctionCall { args, .. } => {
                for arg in args {
                    self.validate_expression(arg, catalog, from, outer_bindings)?;
                }
            }
            Expression::AggregateCall { target, .. } => {
                if let AggregateTarget::Column(name) = target {
                    self.validate_column_reference_with_outer(name, catalog, from, outer_bindings)?;
                }
            }
            Expression::UnaryMinus(expr) => {
                self.validate_expression(expr, catalog, from, outer_bindings)?
            }
            Expression::UnaryNot(expr) => {
                self.validate_expression(expr, catalog, from, outer_bindings)?
            }
            Expression::Cast { expr, .. } => {
                self.validate_expression(expr, catalog, from, outer_bindings)?
            }
            Expression::Binary { left, right, .. } => {
                self.validate_expression(left, catalog, from, outer_bindings)?;
                self.validate_expression(right, catalog, from, outer_bindings)?;
            }
            Expression::Comparison { left, right, .. } => {
                self.validate_expression(left, catalog, from, outer_bindings)?;
                self.validate_expression(right, catalog, from, outer_bindings)?;
            }
            Expression::InList { expr, values, .. } => {
                self.validate_expression(expr, catalog, from, outer_bindings)?;
                for value in values {
                    self.validate_expression(value, catalog, from, outer_bindings)?;
                }
            }
            Expression::InSubquery { expr, subquery, .. } => {
                self.validate_expression(expr, catalog, from, outer_bindings)?;
                subquery.validate_with_outer_bindings(
                    catalog,
                    &self.combined_outer_bindings(catalog, from, outer_bindings)?,
                )?;
                if subquery.columns.len() != 1 {
                    return Err(HematiteError::ParseError(
                        "Subquery predicates require exactly one selected column".to_string(),
                    ));
                }
            }
            Expression::Between {
                expr, lower, upper, ..
            } => {
                self.validate_expression(expr, catalog, from, outer_bindings)?;
                self.validate_expression(lower, catalog, from, outer_bindings)?;
                self.validate_expression(upper, catalog, from, outer_bindings)?;
            }
            Expression::Like { expr, pattern, .. } => {
                self.validate_expression(expr, catalog, from, outer_bindings)?;
                self.validate_expression(pattern, catalog, from, outer_bindings)?;
            }
            Expression::Exists { subquery, .. } => {
                subquery.validate_with_outer_bindings(
                    catalog,
                    &self.combined_outer_bindings(catalog, from, outer_bindings)?,
                )?;
            }
            Expression::NullCheck { expr, .. } => {
                self.validate_expression(expr, catalog, from, outer_bindings)?;
            }
            Expression::Logical { left, right, .. } => {
                self.validate_expression(left, catalog, from, outer_bindings)?;
                self.validate_expression(right, catalog, from, outer_bindings)?;
            }
            Expression::Literal(_)
            | Expression::IntervalLiteral { .. }
            | Expression::Parameter(_) => {}
        }

        Ok(())
    }

    #[cfg(test)]
    fn expression_contains_aggregate(expr: &Expression) -> bool {
        match expr {
            Expression::AggregateCall { .. } => true,
            Expression::ScalarSubquery(_) => false,
            Expression::Case {
                branches,
                else_expr,
            } => {
                branches.iter().any(|branch| {
                    Self::expression_contains_aggregate(&branch.condition)
                        || Self::expression_contains_aggregate(&branch.result)
                }) || else_expr
                    .as_ref()
                    .is_some_and(|expr| Self::expression_contains_aggregate(expr))
            }
            Expression::ScalarFunctionCall { args, .. } => {
                args.iter().any(Self::expression_contains_aggregate)
            }
            Expression::Cast { expr, .. } => Self::expression_contains_aggregate(expr),
            Expression::UnaryMinus(expr) => Self::expression_contains_aggregate(expr),
            Expression::UnaryNot(expr) => Self::expression_contains_aggregate(expr),
            Expression::Binary { left, right, .. } => {
                Self::expression_contains_aggregate(left)
                    || Self::expression_contains_aggregate(right)
            }
            Expression::Comparison { left, right, .. } => {
                Self::expression_contains_aggregate(left)
                    || Self::expression_contains_aggregate(right)
            }
            Expression::InList { expr, values, .. } => {
                Self::expression_contains_aggregate(expr)
                    || values.iter().any(Self::expression_contains_aggregate)
            }
            Expression::InSubquery { expr, subquery, .. } => {
                Self::expression_contains_aggregate(expr)
                    || subquery.where_clause.as_ref().is_some_and(|where_clause| {
                        where_clause
                            .conditions
                            .iter()
                            .any(Self::condition_contains_aggregate)
                    })
            }
            Expression::Between {
                expr, lower, upper, ..
            } => {
                Self::expression_contains_aggregate(expr)
                    || Self::expression_contains_aggregate(lower)
                    || Self::expression_contains_aggregate(upper)
            }
            Expression::Like { expr, pattern, .. } => {
                Self::expression_contains_aggregate(expr)
                    || Self::expression_contains_aggregate(pattern)
            }
            Expression::Exists { subquery, .. } => {
                subquery.where_clause.as_ref().is_some_and(|where_clause| {
                    where_clause
                        .conditions
                        .iter()
                        .any(Self::condition_contains_aggregate)
                })
            }
            Expression::NullCheck { expr, .. } => Self::expression_contains_aggregate(expr),
            Expression::Logical { left, right, .. } => {
                Self::expression_contains_aggregate(left)
                    || Self::expression_contains_aggregate(right)
            }
            Expression::Column(_)
            | Expression::Literal(_)
            | Expression::IntervalLiteral { .. }
            | Expression::Parameter(_) => false,
        }
    }

    #[cfg(test)]
    fn condition_contains_aggregate(condition: &Condition) -> bool {
        match condition {
            Condition::Comparison { left, right, .. } => {
                Self::expression_contains_aggregate(left)
                    || Self::expression_contains_aggregate(right)
            }
            Condition::InList { expr, values, .. } => {
                Self::expression_contains_aggregate(expr)
                    || values.iter().any(Self::expression_contains_aggregate)
            }
            Condition::InSubquery { expr, subquery, .. } => {
                Self::expression_contains_aggregate(expr)
                    || subquery.where_clause.as_ref().is_some_and(|where_clause| {
                        where_clause
                            .conditions
                            .iter()
                            .any(Self::condition_contains_aggregate)
                    })
            }
            Condition::Between {
                expr, lower, upper, ..
            } => {
                Self::expression_contains_aggregate(expr)
                    || Self::expression_contains_aggregate(lower)
                    || Self::expression_contains_aggregate(upper)
            }
            Condition::Like { expr, pattern, .. } => {
                Self::expression_contains_aggregate(expr)
                    || Self::expression_contains_aggregate(pattern)
            }
            Condition::Exists { subquery, .. } => {
                subquery.where_clause.as_ref().is_some_and(|where_clause| {
                    where_clause
                        .conditions
                        .iter()
                        .any(Self::condition_contains_aggregate)
                })
            }
            Condition::NullCheck { expr, .. } => Self::expression_contains_aggregate(expr),
            Condition::Not(condition) => Self::condition_contains_aggregate(condition),
            Condition::Logical { left, right, .. } => {
                Self::condition_contains_aggregate(left)
                    || Self::condition_contains_aggregate(right)
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

#[cfg(test)]
impl InsertStatement {
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        let table = catalog.get_table_by_name(&self.table).ok_or_else(|| {
            HematiteError::ParseError(format!("Table '{}' does not exist", self.table))
        })?;

        let mut seen_columns = std::collections::HashSet::new();

        // Validate columns
        for col_name in &self.columns {
            if !seen_columns.insert(col_name) {
                return Err(HematiteError::ParseError(format!(
                    "Duplicate column '{}' in INSERT",
                    col_name
                )));
            }
            if table.get_column_by_name(col_name).is_none() {
                return Err(HematiteError::ParseError(format!(
                    "Column '{}' does not exist in table '{}'",
                    col_name, self.table
                )));
            }
        }

        if self.columns.is_empty() {
            return Err(HematiteError::ParseError(
                "INSERT must specify at least one column".to_string(),
            ));
        }

        match &self.source {
            InsertSource::Values(rows) => {
                for (i, value_row) in rows.iter().enumerate() {
                    if value_row.len() != self.columns.len() {
                        return Err(HematiteError::ParseError(format!(
                            "Value row {} has {} values, expected {}",
                            i,
                            value_row.len(),
                            self.columns.len()
                        )));
                    }

                    for value in value_row {
                        if matches!(value, Expression::Column(_)) {
                            return Err(HematiteError::ParseError(format!(
                                "INSERT value row {} cannot reference columns",
                                i
                            )));
                        }
                    }
                }
            }
            InsertSource::Select(select) => {
                if select.columns.len() != self.columns.len() {
                    return Err(HematiteError::ParseError(format!(
                        "INSERT SELECT returns {} columns, expected {}",
                        select.columns.len(),
                        self.columns.len()
                    )));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
impl UpdateStatement {
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        let table = catalog.get_table_by_name(&self.table).ok_or_else(|| {
            HematiteError::ParseError(format!("Table '{}' does not exist", self.table))
        })?;

        if self.assignments.is_empty() {
            return Err(HematiteError::ParseError(
                "UPDATE must specify at least one assignment".to_string(),
            ));
        }

        let mut seen_columns = std::collections::HashSet::new();
        let scope = SelectStatement {
            with_clause: Vec::new(),
            distinct: false,
            columns: Vec::new(),
            column_aliases: Vec::new(),
            from: self.source(),
            where_clause: None,
            group_by: Vec::new(),
            having_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operation: None,
        };
        for assignment in &self.assignments {
            if !seen_columns.insert(&assignment.column) {
                return Err(HematiteError::ParseError(format!(
                    "Duplicate column '{}' in UPDATE",
                    assignment.column
                )));
            }

            if table.get_column_by_name(&assignment.column).is_none() {
                return Err(HematiteError::ParseError(format!(
                    "Column '{}' does not exist in table '{}'",
                    assignment.column, self.table
                )));
            }

            scope.validate_expression(&assignment.value, catalog, &scope.from, &[])?;
        }

        if let Some(where_clause) = &self.where_clause {
            for condition in &where_clause.conditions {
                scope.validate_condition(condition, catalog, &scope.from, &[])?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
impl CreateStatement {
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        // Validate table doesn't already exist
        if catalog.get_table_by_name(&self.table).is_some() {
            if self.if_not_exists {
                return Ok(());
            }
            return Err(HematiteError::ParseError(format!(
                "Table '{}' already exists",
                self.table
            )));
        }

        // Validate column names are unique
        let mut column_names = std::collections::HashSet::new();
        for col in &self.columns {
            if column_names.contains(&col.name) {
                return Err(HematiteError::ParseError(format!(
                    "Duplicate column name '{}'",
                    col.name
                )));
            }
            column_names.insert(col.name.clone());
        }

        // Validate at least one primary key
        if !self.columns.iter().any(|col| col.primary_key) {
            return Err(HematiteError::ParseError(
                "Table must have at least one primary key column".to_string(),
            ));
        }

        let auto_increment_columns = self
            .columns
            .iter()
            .filter(|column| column.auto_increment)
            .collect::<Vec<_>>();
        if auto_increment_columns.len() > 1 {
            return Err(HematiteError::ParseError(
                "Only one AUTO_INCREMENT column is allowed per table".to_string(),
            ));
        }
        if let Some(column) = auto_increment_columns.first() {
            if column.data_type != SqlTypeName::Integer {
                return Err(HematiteError::ParseError(format!(
                    "AUTO_INCREMENT column '{}' must use an integer type",
                    column.name
                )));
            }
            if !column.primary_key {
                return Err(HematiteError::ParseError(format!(
                    "AUTO_INCREMENT column '{}' must be a PRIMARY KEY",
                    column.name
                )));
            }
            if column.default_value.is_some() {
                return Err(HematiteError::ParseError(format!(
                    "AUTO_INCREMENT column '{}' cannot also declare a DEFAULT value",
                    column.name
                )));
            }
        }

        for unique_constraint in self.unique_constraints() {
            self.validate_unique_constraint(unique_constraint)?;
        }

        for foreign_key in self.foreign_keys() {
            self.validate_foreign_key(catalog, foreign_key)?;
        }

        Ok(())
    }

    fn foreign_keys(&self) -> Vec<&ForeignKeyDefinition> {
        let mut foreign_keys = self
            .columns
            .iter()
            .filter_map(|column| column.references.as_ref())
            .collect::<Vec<_>>();

        foreign_keys.extend(
            self.constraints
                .iter()
                .filter_map(|constraint| match constraint {
                    TableConstraint::Check(_) | TableConstraint::Unique(_) => None,
                    TableConstraint::ForeignKey(foreign_key) => Some(foreign_key),
                }),
        );

        foreign_keys
    }

    fn unique_constraints(&self) -> Vec<&UniqueConstraintDefinition> {
        self.constraints
            .iter()
            .filter_map(|constraint| match constraint {
                TableConstraint::Unique(unique) => Some(unique),
                TableConstraint::Check(_) | TableConstraint::ForeignKey(_) => None,
            })
            .collect()
    }

    fn validate_unique_constraint(
        &self,
        unique_constraint: &UniqueConstraintDefinition,
    ) -> Result<()> {
        if unique_constraint.columns.is_empty() {
            return Err(HematiteError::ParseError(
                "UNIQUE constraint must reference at least one column".to_string(),
            ));
        }

        self.validate_local_constraint_columns(&unique_constraint.columns, "UNIQUE constraint")?;

        Ok(())
    }

    fn validate_local_constraint_columns(
        &self,
        columns: &[String],
        constraint_label: &str,
    ) -> Result<()> {
        validate_named_columns(columns, constraint_label, |column| {
            if self
                .columns
                .iter()
                .any(|candidate| candidate.name == column)
            {
                Ok(())
            } else {
                Err(HematiteError::ParseError(format!(
                    "{} column '{}' does not exist in table '{}'",
                    constraint_label, column, self.table
                )))
            }
        })
    }

    fn validate_foreign_key(
        &self,
        catalog: &crate::catalog::Schema,
        foreign_key: &ForeignKeyDefinition,
    ) -> Result<()> {
        if foreign_key.columns.is_empty() {
            return Err(HematiteError::ParseError(
                "Foreign key must reference at least one local column".to_string(),
            ));
        }
        if foreign_key.columns.len() != foreign_key.referenced_columns.len() {
            return Err(HematiteError::ParseError(format!(
                "Foreign key on table '{}' must reference the same number of local and parent columns",
                self.table
            )));
        }
        self.validate_local_constraint_columns(&foreign_key.columns, "Foreign key")?;

        let referenced_table = catalog
            .get_table_by_name(&foreign_key.referenced_table)
            .ok_or_else(|| {
                HematiteError::ParseError(format!(
                    "Referenced table '{}' does not exist",
                    foreign_key.referenced_table
                ))
            })?;
        let referenced_column_indices =
            self.referenced_column_indices(referenced_table, foreign_key)?;
        let references_primary_key =
            referenced_table.primary_key_columns == referenced_column_indices;
        let references_unique_index = referenced_table
            .secondary_indexes
            .iter()
            .any(|index| index.unique && index.column_indices == referenced_column_indices);

        if !references_primary_key && !references_unique_index {
            return Err(HematiteError::ParseError(format!(
                "Foreign key '{}.{:?}' must reference a PRIMARY KEY or UNIQUE index with the same column list",
                foreign_key.referenced_table, foreign_key.referenced_columns
            )));
        }

        Ok(())
    }

    fn referenced_column_indices(
        &self,
        referenced_table: &crate::catalog::Table,
        foreign_key: &ForeignKeyDefinition,
    ) -> Result<Vec<usize>> {
        foreign_key
            .referenced_columns
            .iter()
            .map(|column| {
                referenced_table.get_column_index(column).ok_or_else(|| {
                    HematiteError::ParseError(format!(
                        "Referenced column '{}.{}' does not exist",
                        foreign_key.referenced_table, column
                    ))
                })
            })
            .collect()
    }
}

#[cfg(test)]
impl DeleteStatement {
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        let _table = require_table(catalog, &self.table)?;
        let scope = SelectStatement {
            with_clause: Vec::new(),
            distinct: false,
            columns: Vec::new(),
            column_aliases: Vec::new(),
            from: self.source(),
            where_clause: None,
            group_by: Vec::new(),
            having_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operation: None,
        };

        if let Some(where_clause) = &self.where_clause {
            for condition in &where_clause.conditions {
                scope.validate_condition(condition, catalog, &scope.from, &[])?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
impl DropStatement {
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        if self.if_exists && catalog.get_table_by_name(&self.table).is_none() {
            return Ok(());
        }
        let _table = require_table(catalog, &self.table)?;
        Ok(())
    }
}

#[cfg(test)]
impl AlterStatement {
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        match &self.operation {
            AlterOperation::RenameTo(new_name) => {
                self.require_table(catalog)?;
                if new_name == &self.table {
                    return Err(HematiteError::ParseError(
                        "ALTER TABLE RENAME TO requires a different table name".to_string(),
                    ));
                }
                if catalog.get_table_by_name(new_name).is_some() {
                    return Err(HematiteError::ParseError(format!(
                        "Table '{}' already exists",
                        new_name
                    )));
                }
            }
            AlterOperation::RenameColumn { old_name, new_name } => {
                self.validate_rename_column(catalog, old_name, new_name)?;
            }
            AlterOperation::AddColumn(column) => {
                let table = self.require_table(catalog)?;
                if table.get_column_by_name(&column.name).is_some() {
                    return Err(HematiteError::ParseError(format!(
                        "Column '{}' already exists in table '{}'",
                        column.name, self.table
                    )));
                }
                if column.primary_key {
                    return Err(HematiteError::ParseError(
                        "ALTER TABLE ADD COLUMN cannot add a PRIMARY KEY column".to_string(),
                    ));
                }
                if column.auto_increment {
                    return Err(HematiteError::ParseError(
                        "ALTER TABLE ADD COLUMN does not support AUTO_INCREMENT columns"
                            .to_string(),
                    ));
                }
                if column.unique {
                    return Err(HematiteError::ParseError(
                        "ALTER TABLE ADD COLUMN does not support UNIQUE columns; add a UNIQUE index separately".to_string(),
                    ));
                }
                if !column.nullable && column.default_value.is_none() {
                    return Err(HematiteError::ParseError(
                        "ALTER TABLE ADD COLUMN requires the new column to be nullable or have a DEFAULT value".to_string(),
                    ));
                }
                if column.check_constraint.is_some() {
                    return Err(HematiteError::ParseError(
                        "ALTER TABLE ADD COLUMN does not support CHECK constraints".to_string(),
                    ));
                }
                if column.references.is_some() {
                    return Err(HematiteError::ParseError(
                        "ALTER TABLE ADD COLUMN does not support FOREIGN KEY constraints"
                            .to_string(),
                    ));
                }
                if let Some(default_value) = &column.default_value {
                    if default_value.is_null() && !column.nullable {
                        return Err(HematiteError::ParseError(format!(
                            "Column '{}' cannot use DEFAULT NULL when declared NOT NULL",
                            column.name
                        )));
                    }
                    if !default_value.is_null()
                        && !default_value.is_compatible_with(column.data_type.clone())
                    {
                        return Err(HematiteError::ParseError(format!(
                            "DEFAULT value for column '{}' is incompatible with {:?}",
                            column.name, column.data_type
                        )));
                    }
                }
            }
            AlterOperation::AddConstraint(constraint) => match constraint {
                TableConstraint::Check(check) => {
                    if check.name.is_none() {
                        return Err(HematiteError::ParseError(
                            "ALTER TABLE ADD CONSTRAINT requires a constraint name".to_string(),
                        ));
                    }
                }
                TableConstraint::Unique(unique) => {
                    if unique.name.is_none() {
                        return Err(HematiteError::ParseError(
                            "ALTER TABLE ADD CONSTRAINT requires a constraint name".to_string(),
                        ));
                    }
                }
                TableConstraint::ForeignKey(foreign_key) => {
                    if foreign_key.name.is_none() {
                        return Err(HematiteError::ParseError(
                            "ALTER TABLE ADD CONSTRAINT requires a constraint name".to_string(),
                        ));
                    }
                }
            },
            AlterOperation::DropColumn(column_name) => {
                self.validate_drop_column(catalog, column_name)?;
            }
            AlterOperation::DropConstraint(constraint_name) => {
                let table = self.require_table(catalog)?;
                if !table
                    .list_named_constraints()
                    .iter()
                    .any(|constraint| constraint.name == *constraint_name)
                {
                    return Err(HematiteError::ParseError(format!(
                        "Constraint '{}' does not exist on table '{}'",
                        constraint_name, self.table
                    )));
                }
            }
            AlterOperation::AlterColumnSetDefault {
                column_name,
                default_value,
            } => {
                self.validate_set_column_default(catalog, column_name, default_value)?;
            }
            AlterOperation::AlterColumnDropDefault { column_name } => {
                self.validate_existing_column(catalog, column_name)?;
            }
            AlterOperation::AlterColumnSetNotNull { column_name } => {
                self.validate_existing_column(catalog, column_name)?;
            }
            AlterOperation::AlterColumnDropNotNull { column_name } => {
                self.validate_drop_not_null(catalog, column_name)?;
            }
        }

        Ok(())
    }

    fn require_table<'a>(
        &self,
        catalog: &'a crate::catalog::Schema,
    ) -> Result<&'a crate::catalog::Table> {
        require_table(catalog, &self.table)
    }

    fn validate_rename_column(
        &self,
        catalog: &crate::catalog::Schema,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        let table = self.require_table(catalog)?;
        if old_name == new_name {
            return Err(HematiteError::ParseError(
                "ALTER TABLE RENAME COLUMN requires a different column name".to_string(),
            ));
        }
        if table.get_column_by_name(old_name).is_none() {
            return Err(HematiteError::ParseError(format!(
                "Column '{}' does not exist in table '{}'",
                old_name, self.table
            )));
        }
        if table.get_column_by_name(new_name).is_some() {
            return Err(HematiteError::ParseError(format!(
                "Column '{}' already exists in table '{}'",
                new_name, self.table
            )));
        }
        Ok(())
    }

    fn validate_existing_column(
        &self,
        catalog: &crate::catalog::Schema,
        column_name: &str,
    ) -> Result<()> {
        let table = self.require_table(catalog)?;
        if table.get_column_by_name(column_name).is_none() {
            return Err(HematiteError::ParseError(format!(
                "Column '{}' does not exist in table '{}'",
                column_name, self.table
            )));
        }
        Ok(())
    }

    fn validate_set_column_default(
        &self,
        catalog: &crate::catalog::Schema,
        column_name: &str,
        default_value: &LiteralValue,
    ) -> Result<()> {
        let table = self.require_table(catalog)?;
        let column = table.get_column_by_name(column_name).ok_or_else(|| {
            HematiteError::ParseError(format!(
                "Column '{}' does not exist in table '{}'",
                column_name, self.table
            ))
        })?;
        if default_value.is_null() && !column.nullable {
            return Err(HematiteError::ParseError(format!(
                "Column '{}' cannot use DEFAULT NULL while declared NOT NULL",
                column_name
            )));
        }
        if !default_value.is_null()
            && !default_value
                .is_compatible_with(sql_type_name_for_catalog_type(column.data_type.clone()))
        {
            return Err(HematiteError::ParseError(format!(
                "DEFAULT value for column '{}' is incompatible with {:?}",
                column_name, column.data_type
            )));
        }
        Ok(())
    }

    fn validate_drop_not_null(
        &self,
        catalog: &crate::catalog::Schema,
        column_name: &str,
    ) -> Result<()> {
        let table = self.require_table(catalog)?;
        let column = table.get_column_by_name(column_name).ok_or_else(|| {
            HematiteError::ParseError(format!(
                "Column '{}' does not exist in table '{}'",
                column_name, self.table
            ))
        })?;
        if column.primary_key {
            return Err(HematiteError::ParseError(format!(
                "Primary-key column '{}' cannot drop NOT NULL",
                column_name
            )));
        }
        if column.auto_increment {
            return Err(HematiteError::ParseError(format!(
                "AUTO_INCREMENT column '{}' cannot drop NOT NULL",
                column_name
            )));
        }
        Ok(())
    }

    fn validate_drop_column(
        &self,
        catalog: &crate::catalog::Schema,
        column_name: &str,
    ) -> Result<()> {
        let table = self.require_table(catalog)?;
        let column_index = table.get_column_index(column_name).ok_or_else(|| {
            HematiteError::ParseError(format!(
                "Column '{}' does not exist in table '{}'",
                column_name, self.table
            ))
        })?;
        if table.columns.len() == 1 {
            return Err(HematiteError::ParseError(
                "ALTER TABLE DROP COLUMN cannot remove the last column".to_string(),
            ));
        }
        if table.primary_key_columns.contains(&column_index) {
            return Err(HematiteError::ParseError(format!(
                "Cannot drop primary-key column '{}'",
                column_name
            )));
        }
        if table
            .secondary_indexes
            .iter()
            .any(|index| index.column_indices.contains(&column_index))
        {
            return Err(HematiteError::ParseError(format!(
                "Cannot drop column '{}' because it is used by an index",
                column_name
            )));
        }
        if table
            .foreign_keys
            .iter()
            .any(|foreign_key| foreign_key.column_indices.contains(&column_index))
        {
            return Err(HematiteError::ParseError(format!(
                "Cannot drop column '{}' because it is used by a foreign key",
                column_name
            )));
        }
        for constraint in &table.check_constraints {
            let condition =
                crate::parser::parser::parse_condition_fragment(&constraint.expression_sql)?;
            if condition.references_column(column_name, Some(&table.name)) {
                return Err(HematiteError::ParseError(format!(
                    "Cannot drop column '{}' because it is used by a CHECK constraint",
                    column_name
                )));
            }
        }
        if catalog.tables().values().any(|other_table| {
            other_table.name != table.name
                && other_table.foreign_keys.iter().any(|foreign_key| {
                    foreign_key.referenced_table == table.name
                        && foreign_key
                            .referenced_columns
                            .iter()
                            .any(|referenced_column| referenced_column == column_name)
                })
        }) {
            return Err(HematiteError::ParseError(format!(
                "Cannot drop column '{}' because it is referenced by a foreign key",
                column_name
            )));
        }
        Ok(())
    }
}

impl Condition {
    pub(crate) fn references_source_name(&self, name: &str) -> bool {
        match self {
            Condition::Comparison { left, right, .. } => {
                left.references_source_name(name) || right.references_source_name(name)
            }
            Condition::InList { expr, values, .. } => {
                expr.references_source_name(name)
                    || values
                        .iter()
                        .any(|value| value.references_source_name(name))
            }
            Condition::InSubquery { expr, subquery, .. } => {
                expr.references_source_name(name) || subquery.references_source_name(name)
            }
            Condition::Between {
                expr, lower, upper, ..
            } => {
                expr.references_source_name(name)
                    || lower.references_source_name(name)
                    || upper.references_source_name(name)
            }
            Condition::Like { expr, pattern, .. } => {
                expr.references_source_name(name) || pattern.references_source_name(name)
            }
            Condition::Exists { subquery, .. } => subquery.references_source_name(name),
            Condition::NullCheck { expr, .. } => expr.references_source_name(name),
            Condition::Not(condition) => condition.references_source_name(name),
            Condition::Logical { left, right, .. } => {
                left.references_source_name(name) || right.references_source_name(name)
            }
        }
    }

    pub(crate) fn references_column(&self, column_name: &str, table_name: Option<&str>) -> bool {
        match self {
            Condition::Comparison { left, right, .. } => {
                left.references_column(column_name, table_name)
                    || right.references_column(column_name, table_name)
            }
            Condition::InList { expr, values, .. } => {
                expr.references_column(column_name, table_name)
                    || values
                        .iter()
                        .any(|value| value.references_column(column_name, table_name))
            }
            Condition::InSubquery { expr, .. } => expr.references_column(column_name, table_name),
            Condition::Between {
                expr, lower, upper, ..
            } => {
                expr.references_column(column_name, table_name)
                    || lower.references_column(column_name, table_name)
                    || upper.references_column(column_name, table_name)
            }
            Condition::Like { expr, pattern, .. } => {
                expr.references_column(column_name, table_name)
                    || pattern.references_column(column_name, table_name)
            }
            Condition::Exists { .. } => false,
            Condition::NullCheck { expr, .. } => expr.references_column(column_name, table_name),
            Condition::Not(condition) => condition.references_column(column_name, table_name),
            Condition::Logical { left, right, .. } => {
                left.references_column(column_name, table_name)
                    || right.references_column(column_name, table_name)
            }
        }
    }

    pub(crate) fn rename_column_references(
        &mut self,
        old_name: &str,
        new_name: &str,
        table_name: Option<&str>,
    ) {
        match self {
            Condition::Comparison { left, right, .. } => {
                left.rename_column_references(old_name, new_name, table_name);
                right.rename_column_references(old_name, new_name, table_name);
            }
            Condition::InList { expr, values, .. } => {
                expr.rename_column_references(old_name, new_name, table_name);
                for value in values {
                    value.rename_column_references(old_name, new_name, table_name);
                }
            }
            Condition::InSubquery { expr, .. } => {
                expr.rename_column_references(old_name, new_name, table_name);
            }
            Condition::Between {
                expr, lower, upper, ..
            } => {
                expr.rename_column_references(old_name, new_name, table_name);
                lower.rename_column_references(old_name, new_name, table_name);
                upper.rename_column_references(old_name, new_name, table_name);
            }
            Condition::Like { expr, pattern, .. } => {
                expr.rename_column_references(old_name, new_name, table_name);
                pattern.rename_column_references(old_name, new_name, table_name);
            }
            Condition::Exists { .. } => {}
            Condition::NullCheck { expr, .. } => {
                expr.rename_column_references(old_name, new_name, table_name);
            }
            Condition::Not(condition) => {
                condition.rename_column_references(old_name, new_name, table_name);
            }
            Condition::Logical { left, right, .. } => {
                left.rename_column_references(old_name, new_name, table_name);
                right.rename_column_references(old_name, new_name, table_name);
            }
        }
    }

    pub fn to_sql(&self) -> String {
        match self {
            Condition::Comparison {
                left,
                operator,
                right,
            } => format!("{} {} {}", left.to_sql(), operator.to_sql(), right.to_sql()),
            Condition::InList {
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
            Condition::InSubquery { expr, is_not, .. } => format!(
                "{} {}IN (<subquery>)",
                expr.to_sql(),
                if *is_not { "NOT " } else { "" }
            ),
            Condition::Between {
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
            Condition::Like {
                expr,
                pattern,
                is_not,
            } => format!(
                "{} {}LIKE {}",
                expr.to_sql(),
                if *is_not { "NOT " } else { "" },
                pattern.to_sql()
            ),
            Condition::Exists { is_not, .. } => {
                format!("{}EXISTS (<subquery>)", if *is_not { "NOT " } else { "" })
            }
            Condition::NullCheck { expr, is_not } => format!(
                "{} IS {}NULL",
                expr.to_sql(),
                if *is_not { "NOT " } else { "" }
            ),
            Condition::Not(inner) => format!("NOT ({})", inner.to_sql()),
            Condition::Logical {
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
        match name.to_ascii_uppercase().as_str() {
            "COALESCE" => Some(Self::Coalesce),
            "IFNULL" => Some(Self::IfNull),
            "NULLIF" => Some(Self::NullIf),
            "DATE" => Some(Self::DateFn),
            "TIME" => Some(Self::TimeFn),
            "YEAR" => Some(Self::Year),
            "MONTH" => Some(Self::Month),
            "DAY" => Some(Self::Day),
            "HOUR" => Some(Self::Hour),
            "MINUTE" => Some(Self::Minute),
            "SECOND" => Some(Self::Second),
            "TIME_TO_SEC" => Some(Self::TimeToSec),
            "SEC_TO_TIME" => Some(Self::SecToTime),
            "UNIX_TIMESTAMP" => Some(Self::UnixTimestamp),
            "LOWER" => Some(Self::Lower),
            "UPPER" => Some(Self::Upper),
            "LENGTH" => Some(Self::Length),
            "TRIM" => Some(Self::Trim),
            "ABS" => Some(Self::Abs),
            "ROUND" => Some(Self::Round),
            "CONCAT" => Some(Self::Concat),
            "CONCAT_WS" => Some(Self::ConcatWs),
            "SUBSTRING" | "SUBSTR" => Some(Self::Substring),
            "LEFT" => Some(Self::LeftFn),
            "RIGHT" => Some(Self::RightFn),
            "GREATEST" => Some(Self::Greatest),
            "LEAST" => Some(Self::Least),
            "REPLACE" => Some(Self::Replace),
            "REPEAT" => Some(Self::Repeat),
            "REVERSE" => Some(Self::Reverse),
            "LOCATE" => Some(Self::Locate),
            "CEIL" | "CEILING" => Some(Self::Ceil),
            "FLOOR" => Some(Self::Floor),
            "POWER" | "POW" => Some(Self::Power),
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

#[cfg(test)]
impl CreateIndexStatement {
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        let table = require_table(catalog, &self.table)?;

        if self.columns.is_empty() {
            return Err(HematiteError::ParseError(
                "CREATE INDEX must specify at least one column".to_string(),
            ));
        }

        validate_named_columns(&self.columns, "CREATE INDEX", |column| {
            if table.get_column_by_name(column).is_some() {
                Ok(())
            } else {
                Err(HematiteError::ParseError(format!(
                    "Column '{}' does not exist in table '{}'",
                    column, self.table
                )))
            }
        })?;

        if table.get_secondary_index(&self.index_name).is_some() {
            if self.if_not_exists {
                return Ok(());
            }
            return Err(HematiteError::ParseError(format!(
                "Index '{}' already exists on table '{}'",
                self.index_name, self.table
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
fn require_table<'a>(
    catalog: &'a crate::catalog::Schema,
    table_name: &str,
) -> Result<&'a crate::catalog::Table> {
    catalog
        .get_table_by_name(table_name)
        .ok_or_else(|| HematiteError::ParseError(format!("Table '{}' does not exist", table_name)))
}

#[cfg(test)]
fn sql_type_name_for_catalog_type(data_type: crate::catalog::DataType) -> SqlTypeName {
    match data_type {
        crate::catalog::DataType::TinyInt => SqlTypeName::TinyInt,
        crate::catalog::DataType::SmallInt => SqlTypeName::SmallInt,
        crate::catalog::DataType::Integer => SqlTypeName::Integer,
        crate::catalog::DataType::BigInt => SqlTypeName::BigInt,
        crate::catalog::DataType::Text => SqlTypeName::Text,
        crate::catalog::DataType::Char(length) => SqlTypeName::Char(length),
        crate::catalog::DataType::VarChar(length) => SqlTypeName::VarChar(length),
        crate::catalog::DataType::Binary(length) => SqlTypeName::Binary(length),
        crate::catalog::DataType::VarBinary(length) => SqlTypeName::VarBinary(length),
        crate::catalog::DataType::Enum(values) => SqlTypeName::Enum(values),
        crate::catalog::DataType::Boolean => SqlTypeName::Boolean,
        crate::catalog::DataType::Float => SqlTypeName::Float,
        crate::catalog::DataType::Real => SqlTypeName::Real,
        crate::catalog::DataType::Double => SqlTypeName::Double,
        crate::catalog::DataType::Decimal { precision, scale } => {
            SqlTypeName::Decimal { precision, scale }
        }
        crate::catalog::DataType::Numeric { precision, scale } => {
            SqlTypeName::Numeric { precision, scale }
        }
        crate::catalog::DataType::Blob => SqlTypeName::Blob,
        crate::catalog::DataType::Date => SqlTypeName::Date,
        crate::catalog::DataType::Time => SqlTypeName::Time,
        crate::catalog::DataType::DateTime => SqlTypeName::DateTime,
        crate::catalog::DataType::Timestamp => SqlTypeName::Timestamp,
        crate::catalog::DataType::TimeWithTimeZone => SqlTypeName::TimeWithTimeZone,
    }
}

#[cfg(test)]
fn validate_named_columns<F>(
    columns: &[String],
    constraint_label: &str,
    mut validate_column: F,
) -> Result<()>
where
    F: FnMut(&str) -> Result<()>,
{
    let mut seen = std::collections::HashSet::new();
    for column in columns {
        if !seen.insert(column) {
            return Err(HematiteError::ParseError(format!(
                "{} repeats column '{}'",
                constraint_label, column
            )));
        }
        validate_column(column)?;
    }
    Ok(())
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

#[cfg(test)]
impl DropIndexStatement {
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        if self.if_exists && catalog.get_table_by_name(&self.table).is_none() {
            return Ok(());
        }
        let table = require_table(catalog, &self.table)?;

        if table.get_secondary_index(&self.index_name).is_none() {
            if self.if_exists {
                return Ok(());
            }
            return Err(HematiteError::ParseError(format!(
                "Index '{}' does not exist on table '{}'",
                self.index_name, self.table
            )));
        }

        Ok(())
    }
}
