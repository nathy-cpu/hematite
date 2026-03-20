//! Abstract syntax tree for SQL statements

use crate::catalog::types::Value;
use crate::catalog::DataType;
use crate::error::{HematiteError, Result};

#[derive(Debug, Clone)]
pub enum Statement {
    Select(SelectStatement),
    Update(UpdateStatement),
    Insert(InsertStatement),
    Delete(DeleteStatement),
    Create(CreateStatement),
    Drop(DropStatement),
}

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub columns: Vec<SelectItem>,
    pub from: TableReference,
    pub where_clause: Option<WhereClause>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub enum SelectItem {
    Wildcard,
    Column(String),
    CountAll,
    Aggregate {
        function: AggregateFunction,
        column: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone)]
pub enum TableReference {
    Table(String),
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
    NullCheck {
        expr: Expression,
        is_not: bool,
    },
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
pub struct DropStatement {
    pub table: String,
}

#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub default_value: Option<Value>,
}

impl Statement {
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        match self {
            Statement::Select(select) => select.validate(catalog),
            Statement::Update(update) => update.validate(catalog),
            Statement::Insert(insert) => insert.validate(catalog),
            Statement::Delete(delete) => delete.validate(catalog),
            Statement::Create(create) => create.validate(catalog),
            Statement::Drop(drop) => drop.validate(catalog),
        }
    }

    pub fn is_read_only(&self) -> bool {
        matches!(self, Statement::Select(_))
    }

    pub fn mutates_schema(&self) -> bool {
        matches!(self, Statement::Create(_) | Statement::Drop(_))
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
            Statement::Select(select) => {
                if let Some(where_clause) = &select.where_clause {
                    where_clause.visit_parameters(f);
                }
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
            Statement::Create(_) | Statement::Drop(_) => {}
        }
    }

    fn bind_statement(&self, parameters: &[Value]) -> Result<Statement> {
        match self {
            Statement::Select(select) => Ok(Statement::Select(SelectStatement {
                columns: select.columns.clone(),
                from: select.from.clone(),
                where_clause: select
                    .where_clause
                    .as_ref()
                    .map(|where_clause| where_clause.bind(parameters))
                    .transpose()?,
                order_by: select.order_by.clone(),
                limit: select.limit,
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
            Statement::Drop(drop) => Ok(Statement::Drop(drop.clone())),
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
            Condition::NullCheck { expr, .. } => expr.visit_parameters(f),
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
            Condition::NullCheck { expr, is_not } => Ok(Condition::NullCheck {
                expr: expr.bind(parameters)?,
                is_not: *is_not,
            }),
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

impl Expression {
    fn visit_parameters<F>(&self, f: &mut F)
    where
        F: FnMut(usize),
    {
        if let Expression::Parameter(index) = self {
            f(*index);
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
        }
    }
}

impl SelectStatement {
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        let table = match &self.from {
            TableReference::Table(table_name) => {
                catalog.get_table_by_name(table_name).ok_or_else(|| {
                    HematiteError::ParseError(format!("Table '{}' does not exist", table_name))
                })?
            }
        };

        // Validate columns
        let has_aggregate = self
            .columns
            .iter()
            .any(|item| matches!(item, SelectItem::CountAll | SelectItem::Aggregate { .. }));
        if has_aggregate && self.columns.len() > 1 {
            return Err(HematiteError::ParseError(
                "Aggregate select items cannot be combined with other select items yet".to_string(),
            ));
        }

        let has_count_all = self
            .columns
            .iter()
            .any(|item| matches!(item, SelectItem::CountAll));
        if has_count_all && self.columns.len() > 1 {
            return Err(HematiteError::ParseError(
                "COUNT(*) cannot be combined with other select items yet".to_string(),
            ));
        }

        for item in &self.columns {
            match item {
                SelectItem::Column(name) => {
                    if table.get_column_by_name(name).is_none() {
                        let TableReference::Table(table_name) = &self.from;
                        return Err(HematiteError::ParseError(format!(
                            "Column '{}' does not exist in table '{}'",
                            name, table_name
                        )));
                    }
                }
                SelectItem::Aggregate { column, .. } => {
                    if table.get_column_by_name(column).is_none() {
                        let TableReference::Table(table_name) = &self.from;
                        return Err(HematiteError::ParseError(format!(
                            "Column '{}' does not exist in table '{}'",
                            column, table_name
                        )));
                    }
                }
                SelectItem::Wildcard | SelectItem::CountAll => {} // Always valid
            }
        }

        if let Some(where_clause) = &self.where_clause {
            for condition in &where_clause.conditions {
                Self::validate_condition(condition, table, &self.from)?;
            }
        }

        for item in &self.order_by {
            if table.get_column_by_name(&item.column).is_none() {
                let TableReference::Table(table_name) = &self.from;
                return Err(HematiteError::ParseError(format!(
                    "Column '{}' does not exist in table '{}'",
                    item.column, table_name
                )));
            }
        }

        Ok(())
    }

    fn validate_condition(
        condition: &Condition,
        table: &crate::catalog::Table,
        from: &TableReference,
    ) -> Result<()> {
        match condition {
            Condition::Comparison { left, right, .. } => {
                Self::validate_expression(left, table, from)?;
                Self::validate_expression(right, table, from)?;
            }
            Condition::NullCheck { expr, .. } => {
                Self::validate_expression(expr, table, from)?;
            }
            Condition::Logical { left, right, .. } => {
                Self::validate_condition(left, table, from)?;
                Self::validate_condition(right, table, from)?;
            }
        }

        Ok(())
    }

    fn validate_expression(
        expr: &Expression,
        table: &crate::catalog::Table,
        from: &TableReference,
    ) -> Result<()> {
        if let Expression::Column(name) = expr {
            if table.get_column_by_name(name).is_none() {
                let TableReference::Table(table_name) = from;
                return Err(HematiteError::ParseError(format!(
                    "Column '{}' does not exist in table '{}'",
                    name, table_name
                )));
            }
        }

        Ok(())
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

            SelectStatement::validate_expression(
                &assignment.value,
                table,
                &TableReference::Table(self.table.clone()),
            )?;
        }

        if let Some(where_clause) = &self.where_clause {
            for condition in &where_clause.conditions {
                SelectStatement::validate_condition(
                    condition,
                    table,
                    &TableReference::Table(self.table.clone()),
                )?;
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
        let table = catalog.get_table_by_name(&self.table).ok_or_else(|| {
            HematiteError::ParseError(format!("Table '{}' does not exist", self.table))
        })?;

        if let Some(where_clause) = &self.where_clause {
            for condition in &where_clause.conditions {
                SelectStatement::validate_condition(
                    condition,
                    table,
                    &TableReference::Table(self.table.clone()),
                )?;
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
