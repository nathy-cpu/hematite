//! Abstract syntax tree for SQL statements

use crate::catalog::types::Value;
use crate::catalog::DataType;
use crate::error::{HematiteError, Result};

#[derive(Debug, Clone)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
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
            Statement::Insert(insert) => insert.validate(catalog),
            Statement::Create(create) => create.validate(catalog),
        }
    }
}

impl SelectStatement {
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        // Validate table exists
        match &self.from {
            TableReference::Table(table_name) => {
                if catalog.get_table_by_name(table_name).is_none() {
                    return Err(HematiteError::ParseError(format!(
                        "Table '{}' does not exist",
                        table_name
                    )));
                }
            }
        }

        // Validate columns
        for item in &self.columns {
            match item {
                SelectItem::Column(name) => {
                    let TableReference::Table(table_name) = &self.from;
                    if let Some(table) = catalog.get_table_by_name(table_name) {
                        if table.get_column_by_name(name).is_none() {
                            return Err(HematiteError::ParseError(format!(
                                "Column '{}' does not exist in table '{}'",
                                name, table_name
                            )));
                        }
                    }
                }
                SelectItem::Wildcard => {} // Always valid
            }
        }

        Ok(())
    }
}

impl InsertStatement {
    pub fn validate(&self, catalog: &crate::catalog::Schema) -> Result<()> {
        // Validate table exists
        if catalog.get_table_by_name(&self.table).is_none() {
            return Err(HematiteError::ParseError(format!(
                "Table '{}' does not exist",
                self.table
            )));
        }

        // Validate columns
        if let Some(table) = catalog.get_table_by_name(&self.table) {
            for col_name in &self.columns {
                if table.get_column_by_name(col_name).is_none() {
                    return Err(HematiteError::ParseError(format!(
                        "Column '{}' does not exist in table '{}'",
                        col_name, self.table
                    )));
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::types::DataType;

    #[test]
    fn test_select_statement_validation() -> Result<()> {
        let mut catalog = crate::catalog::Schema::new();

        // Create a test table
        let columns = vec![
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                DataType::Integer,
            )
            .primary_key(true),
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                DataType::Text,
            ),
        ];
        catalog.create_table("users".to_string(), columns)?;

        let select = SelectStatement {
            columns: vec![SelectItem::Column("id".to_string())],
            from: TableReference::Table("users".to_string()),
            where_clause: None,
        };

        assert!(select.validate(&catalog).is_ok());
        Ok(())
    }

    #[test]
    fn test_invalid_column_reference() {
        let mut catalog = crate::catalog::Schema::new();

        let columns = vec![crate::catalog::Column::new(
            crate::catalog::ColumnId::new(1),
            "id".to_string(),
            DataType::Integer,
        )
        .primary_key(true)];
        catalog.create_table("users".to_string(), columns).unwrap();

        let select = SelectStatement {
            columns: vec![SelectItem::Column("invalid".to_string())],
            from: TableReference::Table("users".to_string()),
            where_clause: None,
        };

        assert!(select.validate(&catalog).is_err());
    }
}
