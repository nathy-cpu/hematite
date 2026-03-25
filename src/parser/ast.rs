//! Abstract syntax tree for SQL statements

use crate::catalog::types::Value;
use crate::catalog::DataType;
use crate::error::{HematiteError, Result};

#[derive(Debug, Clone)]
pub enum Statement {
    Begin,
    Commit,
    Rollback,
    Select(SelectStatement),
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
        on: Condition,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableBinding {
    pub table_name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
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
    Literal(Value),
    Parameter(usize),
    AggregateCall {
        function: AggregateFunction,
        target: AggregateTarget,
    },
    UnaryMinus(Box<Expression>),
    Binary {
        left: Box<Expression>,
        operator: ArithmeticOperator,
        right: Box<Expression>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
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
    pub values: Vec<Vec<Expression>>,
}

#[derive(Debug, Clone)]
pub struct UpdateAssignment {
    pub column: String,
    pub value: Expression,
}

#[derive(Debug, Clone)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<UpdateAssignment>,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, Clone)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, Clone)]
pub struct CreateStatement {
    pub table: String,
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Debug, Clone)]
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone)]
pub struct DropStatement {
    pub table: String,
}

#[derive(Debug, Clone)]
pub struct DropIndexStatement {
    pub index_name: String,
    pub table: String,
}

#[derive(Debug, Clone)]
pub struct AlterStatement {
    pub table: String,
    pub operation: AlterOperation,
}

#[derive(Debug, Clone)]
pub enum AlterOperation {
    RenameTo(String),
    AddColumn(ColumnDefinition),
}

#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default_value: Option<Value>,
}

impl Statement {
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        match self {
            Statement::Begin | Statement::Commit | Statement::Rollback => Ok(()),
            Statement::Select(select) => select.validate(catalog),
            Statement::Update(update) => update.validate(catalog),
            Statement::Insert(insert) => insert.validate(catalog),
            Statement::Delete(delete) => delete.validate(catalog),
            Statement::Create(create) => create.validate(catalog),
            Statement::CreateIndex(create_index) => create_index.validate(catalog),
            Statement::Alter(alter) => alter.validate(catalog),
            Statement::Drop(drop) => drop.validate(catalog),
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
        matches!(self, Statement::Select(_))
    }

    pub fn mutates_schema(&self) -> bool {
        matches!(
            self,
            Statement::Create(_)
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

    pub fn bind_parameters(&self, parameters: &[Value]) -> Result<Statement> {
        self.bind_statement(parameters)
    }

    fn visit_parameters<F>(&self, f: &mut F)
    where
        F: FnMut(usize),
    {
        match self {
            Statement::Begin | Statement::Commit | Statement::Rollback => {}
            Statement::Select(select) => {
                select.visit_parameters(f);
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
                for row in &insert.values {
                    for expr in row {
                        expr.visit_parameters(f);
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

    fn bind_statement(&self, parameters: &[Value]) -> Result<Statement> {
        match self {
            Statement::Begin => Ok(Statement::Begin),
            Statement::Commit => Ok(Statement::Commit),
            Statement::Rollback => Ok(Statement::Rollback),
            Statement::Select(select) => Ok(Statement::Select(SelectStatement {
                with_clause: select
                    .with_clause
                    .iter()
                    .map(|cte| {
                        Ok(CommonTableExpression {
                            name: cte.name.clone(),
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
            Statement::Update(update) => Ok(Statement::Update(UpdateStatement {
                table: update.table.clone(),
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
                values: insert
                    .values
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|expr| expr.bind(parameters))
                            .collect::<Result<Vec<_>>>()
                    })
                    .collect::<Result<Vec<_>>>()?,
            })),
            Statement::Delete(delete) => Ok(Statement::Delete(DeleteStatement {
                table: delete.table.clone(),
                where_clause: delete
                    .where_clause
                    .as_ref()
                    .map(|where_clause| where_clause.bind(parameters))
                    .transpose()?,
            })),
            Statement::Create(create) => Ok(Statement::Create(create.clone())),
            Statement::CreateIndex(create_index) => {
                Ok(Statement::CreateIndex(create_index.clone()))
            }
            Statement::Alter(alter) => Ok(Statement::Alter(alter.clone())),
            Statement::Drop(drop) => Ok(Statement::Drop(drop.clone())),
            Statement::DropIndex(drop_index) => Ok(Statement::DropIndex(drop_index.clone())),
        }
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

    fn bind(&self, parameters: &[Value]) -> Result<WhereClause> {
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

    fn bind(&self, parameters: &[Value]) -> Result<Condition> {
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

    fn bind(&self, parameters: &[Value]) -> Result<SelectItem> {
        match self {
            SelectItem::Wildcard => Ok(SelectItem::Wildcard),
            SelectItem::Column(name) => Ok(SelectItem::Column(name.clone())),
            SelectItem::Expression(expr) => Ok(SelectItem::Expression(expr.bind(parameters)?)),
            SelectItem::CountAll => Ok(SelectItem::CountAll),
            SelectItem::Aggregate { function, column } => Ok(SelectItem::Aggregate {
                function: *function,
                column: column.clone(),
            }),
        }
    }
}

impl Expression {
    fn visit_parameters<F>(&self, f: &mut F)
    where
        F: FnMut(usize),
    {
        match self {
            Expression::Parameter(index) => f(*index),
            Expression::AggregateCall { .. } => {}
            Expression::UnaryMinus(expr) => expr.visit_parameters(f),
            Expression::Binary { left, right, .. } => {
                left.visit_parameters(f);
                right.visit_parameters(f);
            }
            Expression::Column(_) | Expression::Literal(_) => {}
        }
    }

    fn bind(&self, parameters: &[Value]) -> Result<Expression> {
        match self {
            Expression::Column(name) => Ok(Expression::Column(name.clone())),
            Expression::Literal(value) => Ok(Expression::Literal(value.clone())),
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
            Expression::AggregateCall { function, target } => Ok(Expression::AggregateCall {
                function: *function,
                target: target.clone(),
            }),
            Expression::UnaryMinus(expr) => {
                Ok(Expression::UnaryMinus(Box::new(expr.bind(parameters)?)))
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
        }
    }
}

impl SelectStatement {
    fn single_table_scope(table_name: &str) -> Self {
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

    fn is_hidden_rowid(name: &str) -> bool {
        name.eq_ignore_ascii_case("rowid")
    }

    fn lookup_cte<'a>(&'a self, name: &str) -> Option<&'a CommonTableExpression> {
        self.with_clause
            .iter()
            .find(|cte| cte.name.eq_ignore_ascii_case(name))
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
            TableReference::InnerJoin { left, right, .. } => {
                Self::collect_table_bindings_into(left, bindings);
                Self::collect_table_bindings_into(right, bindings);
            }
        }
    }

    fn collect_source_bindings(
        &self,
        catalog: &crate::catalog::Schema,
        from: &TableReference,
    ) -> Result<Vec<SourceBinding>> {
        let mut bindings = Vec::new();
        self.collect_source_bindings_into(catalog, from, &mut bindings)?;
        Ok(bindings)
    }

    fn collect_source_bindings_into(
        &self,
        catalog: &crate::catalog::Schema,
        from: &TableReference,
        bindings: &mut Vec<SourceBinding>,
    ) -> Result<()> {
        match from {
            TableReference::Table(table_name, alias) => {
                if let Some(cte) = self.lookup_cte(table_name) {
                    cte.query.validate(catalog)?;
                    bindings.push(SourceBinding {
                        source_name: table_name.clone(),
                        alias: alias.clone(),
                        columns: cte.query.projected_column_names(catalog)?,
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
            TableReference::InnerJoin { left, right, .. } => {
                self.collect_source_bindings_into(catalog, left, bindings)?;
                self.collect_source_bindings_into(catalog, right, bindings)
            }
        }
    }

    fn lookup_source_bindings<'a>(
        qualifier: Option<&str>,
        bindings: &'a [SourceBinding],
    ) -> Result<Vec<&'a SourceBinding>> {
        if let Some(qualifier) = qualifier {
            let matches: Vec<&SourceBinding> = bindings
                .iter()
                .filter(|binding| {
                    binding.source_name == qualifier
                        || binding
                            .alias
                            .as_deref()
                            .is_some_and(|alias| alias == qualifier)
                })
                .collect();
            if matches.is_empty() {
                return Err(HematiteError::ParseError(format!(
                    "Unknown table or alias '{}'",
                    qualifier
                )));
            }
            return Ok(matches);
        }

        Ok(bindings.iter().collect())
    }

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
                    return Err(HematiteError::ParseError(
                        "Wildcard projections are not supported in derived tables or CTEs"
                            .to_string(),
                    ))
                }
                SelectItem::Column(name) => {
                    self.validate_column_reference(name, catalog, &self.from)?;
                    names.push(Self::column_reference_name(name).to_string());
                }
                SelectItem::CountAll => names.push("COUNT(*)".to_string()),
                SelectItem::Aggregate { function, column } => {
                    names.push(format!("{:?}({})", function, column))
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

    pub(crate) fn validate_column_reference(
        &self,
        name: &str,
        catalog: &crate::catalog::Schema,
        from: &TableReference,
    ) -> Result<()> {
        let (qualifier, column_name) = Self::split_column_reference(name);
        let bindings = self.collect_source_bindings(catalog, from)?;
        let candidate_bindings = Self::lookup_source_bindings(qualifier, &bindings)?;
        let mut matched_tables = Vec::new();

        for binding in candidate_bindings {
            if binding.columns.iter().any(|column| column == column_name)
                || (binding.has_hidden_rowid && Self::is_hidden_rowid(column_name))
            {
                matched_tables.push(binding.source_name.clone());
            }
        }

        match matched_tables.len() {
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

    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        if let Some(set_operation) = &self.set_operation {
            set_operation.right.validate(catalog)?;
            if self.columns.len() != set_operation.right.columns.len() {
                return Err(HematiteError::ParseError(
                    "Set operations require both queries to project the same number of columns"
                        .to_string(),
                ));
            }
        }

        for cte in &self.with_clause {
            cte.query.validate(catalog)?;
        }

        let bindings = self.collect_source_bindings(catalog, &self.from)?;
        if bindings.is_empty() {
            return Err(HematiteError::ParseError(
                "SELECT requires at least one table source".to_string(),
            ));
        }
        self.validate_table_reference(catalog, &self.from)?;

        let has_aggregate = self.columns.iter().any(|item| match item {
            SelectItem::CountAll | SelectItem::Aggregate { .. } => true,
            SelectItem::Expression(expr) => Self::expression_contains_aggregate(expr),
            SelectItem::Wildcard | SelectItem::Column(_) => false,
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
                SelectItem::Column(name) => {
                    self.validate_column_reference(name, catalog, &self.from)?
                }
                SelectItem::Expression(expr) => {
                    self.validate_expression(expr, catalog, &self.from)?;
                }
                SelectItem::Aggregate { column, .. } => {
                    self.validate_column_reference(column, catalog, &self.from)?;
                }
                SelectItem::Wildcard | SelectItem::CountAll => {} // Always valid
            }
        }

        if let Some(where_clause) = &self.where_clause {
            for condition in &where_clause.conditions {
                self.validate_condition(condition, catalog, &self.from)?;
            }
        }

        for expr in &self.group_by {
            self.validate_expression(expr, catalog, &self.from)?;
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
            self.validate_column_reference(&item.column, catalog, &self.from)?;
        }

        Ok(())
    }

    fn validate_table_reference(
        &self,
        catalog: &crate::catalog::Schema,
        from: &TableReference,
    ) -> Result<()> {
        match from {
            TableReference::Table(_, _) => Ok(()),
            TableReference::Derived { subquery, .. } => {
                subquery.validate(catalog)?;
                let _ = subquery.projected_column_names(catalog)?;
                Ok(())
            }
            TableReference::CrossJoin(left, right) => {
                self.validate_table_reference(catalog, left)?;
                self.validate_table_reference(catalog, right)
            }
            TableReference::InnerJoin { left, right, on } => {
                self.validate_table_reference(catalog, left)?;
                self.validate_table_reference(catalog, right)?;
                self.validate_condition(on, catalog, from)
            }
        }
    }

    fn validate_condition(
        &self,
        condition: &Condition,
        catalog: &crate::catalog::Schema,
        from: &TableReference,
    ) -> Result<()> {
        match condition {
            Condition::Comparison { left, right, .. } => {
                self.validate_expression(left, catalog, from)?;
                self.validate_expression(right, catalog, from)?;
            }
            Condition::InList { expr, values, .. } => {
                self.validate_expression(expr, catalog, from)?;
                for value in values {
                    self.validate_expression(value, catalog, from)?;
                }
            }
            Condition::InSubquery { expr, subquery, .. } => {
                self.validate_expression(expr, catalog, from)?;
                subquery.validate(catalog)?;
                if subquery.columns.len() != 1 {
                    return Err(HematiteError::ParseError(
                        "Subquery predicates require exactly one selected column".to_string(),
                    ));
                }
            }
            Condition::Between {
                expr, lower, upper, ..
            } => {
                self.validate_expression(expr, catalog, from)?;
                self.validate_expression(lower, catalog, from)?;
                self.validate_expression(upper, catalog, from)?;
            }
            Condition::Like { expr, pattern, .. } => {
                self.validate_expression(expr, catalog, from)?;
                self.validate_expression(pattern, catalog, from)?;
            }
            Condition::Exists { subquery, .. } => {
                subquery.validate(catalog)?;
            }
            Condition::NullCheck { expr, .. } => {
                self.validate_expression(expr, catalog, from)?;
            }
            Condition::Not(condition) => {
                self.validate_condition(condition, catalog, from)?;
            }
            Condition::Logical { left, right, .. } => {
                self.validate_condition(left, catalog, from)?;
                self.validate_condition(right, catalog, from)?;
            }
        }

        Ok(())
    }

    fn validate_expression(
        &self,
        expr: &Expression,
        catalog: &crate::catalog::Schema,
        from: &TableReference,
    ) -> Result<()> {
        match expr {
            Expression::Column(name) => self.validate_column_reference(name, catalog, from)?,
            Expression::AggregateCall { target, .. } => {
                if let AggregateTarget::Column(name) = target {
                    self.validate_column_reference(name, catalog, from)?;
                }
            }
            Expression::UnaryMinus(expr) => self.validate_expression(expr, catalog, from)?,
            Expression::Binary { left, right, .. } => {
                self.validate_expression(left, catalog, from)?;
                self.validate_expression(right, catalog, from)?;
            }
            Expression::Literal(_) | Expression::Parameter(_) => {}
        }

        Ok(())
    }

    fn expression_contains_aggregate(expr: &Expression) -> bool {
        match expr {
            Expression::AggregateCall { .. } => true,
            Expression::UnaryMinus(expr) => Self::expression_contains_aggregate(expr),
            Expression::Binary { left, right, .. } => {
                Self::expression_contains_aggregate(left)
                    || Self::expression_contains_aggregate(right)
            }
            Expression::Column(_) | Expression::Literal(_) | Expression::Parameter(_) => false,
        }
    }

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
                    || subquery.columns.iter().any(|item| {
                        matches!(item, SelectItem::CountAll | SelectItem::Aggregate { .. })
                    })
                    || subquery.having_clause.as_ref().is_some_and(|having| {
                        having
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
                subquery
                    .columns
                    .iter()
                    .any(|item| matches!(item, SelectItem::CountAll | SelectItem::Aggregate { .. }))
                    || subquery.having_clause.as_ref().is_some_and(|having| {
                        having
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

        // Validate values count matches columns
        for (i, value_row) in self.values.iter().enumerate() {
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

        Ok(())
    }
}

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
        let scope = SelectStatement::single_table_scope(&self.table);
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

            scope.validate_expression(&assignment.value, catalog, &scope.from)?;
        }

        if let Some(where_clause) = &self.where_clause {
            for condition in &where_clause.conditions {
                scope.validate_condition(condition, catalog, &scope.from)?;
            }
        }

        Ok(())
    }
}

impl CreateStatement {
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        // Validate table doesn't already exist
        if catalog.get_table_by_name(&self.table).is_some() {
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

        Ok(())
    }
}

impl DeleteStatement {
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        let _table = catalog.get_table_by_name(&self.table).ok_or_else(|| {
            HematiteError::ParseError(format!("Table '{}' does not exist", self.table))
        })?;
        let scope = SelectStatement::single_table_scope(&self.table);

        if let Some(where_clause) = &self.where_clause {
            for condition in &where_clause.conditions {
                scope.validate_condition(condition, catalog, &scope.from)?;
            }
        }

        Ok(())
    }
}

impl DropStatement {
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        catalog.get_table_by_name(&self.table).ok_or_else(|| {
            HematiteError::ParseError(format!("Table '{}' does not exist", self.table))
        })?;
        Ok(())
    }
}

impl AlterStatement {
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        catalog.get_table_by_name(&self.table).ok_or_else(|| {
            HematiteError::ParseError(format!("Table '{}' does not exist", self.table))
        })?;

        match &self.operation {
            AlterOperation::RenameTo(new_name) => {
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
            AlterOperation::AddColumn(column) => {
                let table = catalog.get_table_by_name(&self.table).ok_or_else(|| {
                    HematiteError::ParseError(format!("Table '{}' does not exist", self.table))
                })?;
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
                if let Some(default_value) = &column.default_value {
                    if default_value.is_null() && !column.nullable {
                        return Err(HematiteError::ParseError(format!(
                            "Column '{}' cannot use DEFAULT NULL when declared NOT NULL",
                            column.name
                        )));
                    }
                    if !default_value.is_null()
                        && !default_value.is_compatible_with(column.data_type)
                    {
                        return Err(HematiteError::ParseError(format!(
                            "DEFAULT value for column '{}' is incompatible with {:?}",
                            column.name, column.data_type
                        )));
                    }
                }
            }
        }

        Ok(())
    }
}

impl CreateIndexStatement {
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        let table = catalog.get_table_by_name(&self.table).ok_or_else(|| {
            HematiteError::ParseError(format!("Table '{}' does not exist", self.table))
        })?;

        if self.columns.is_empty() {
            return Err(HematiteError::ParseError(
                "CREATE INDEX must specify at least one column".to_string(),
            ));
        }

        let mut seen = std::collections::HashSet::new();
        for column in &self.columns {
            if !seen.insert(column) {
                return Err(HematiteError::ParseError(format!(
                    "Duplicate column '{}' in CREATE INDEX",
                    column
                )));
            }

            if table.get_column_by_name(column).is_none() {
                return Err(HematiteError::ParseError(format!(
                    "Column '{}' does not exist in table '{}'",
                    column, self.table
                )));
            }
        }

        if table.get_secondary_index(&self.index_name).is_some() {
            return Err(HematiteError::ParseError(format!(
                "Index '{}' already exists on table '{}'",
                self.index_name, self.table
            )));
        }

        Ok(())
    }
}

impl DropIndexStatement {
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        let table = catalog.get_table_by_name(&self.table).ok_or_else(|| {
            HematiteError::ParseError(format!("Table '{}' does not exist", self.table))
        })?;

        if table.get_secondary_index(&self.index_name).is_none() {
            return Err(HematiteError::ParseError(format!(
                "Index '{}' does not exist on table '{}'",
                self.index_name, self.table
            )));
        }

        Ok(())
    }
}
