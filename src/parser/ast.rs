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
}

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub columns: Vec<SelectItem>,
    pub from: TableReference,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, Clone)]
pub enum SelectItem {
    Wildcard,
    Column(String),
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
pub enum Condition {
    Comparison {
        left: Expression,
        operator: ComparisonOperator,
        right: Expression,
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
        }
    }

    pub fn mutates_schema(&self) -> bool {
        matches!(self, Statement::Create(_))
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
                SelectItem::Wildcard => {} // Always valid
            }
        }

        if let Some(where_clause) = &self.where_clause {
            for condition in &where_clause.conditions {
                Self::validate_condition(condition, table, &self.from)?;
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
