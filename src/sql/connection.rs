//! SQL connection and statement interface

use crate::catalog::Catalog;
use crate::catalog::Value;
use crate::error::{HematiteError, Result};
use crate::parser::{Lexer, Parser};
use crate::query::{ExecutionContext, QueryPlanner, QueryResult};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct Connection {
    catalog: Arc<Mutex<Catalog>>,
}

impl Connection {
    pub fn new(database_path: &str) -> Result<Self> {
        let catalog = Catalog::open_or_create(database_path)?;
        Ok(Self {
            catalog: Arc::new(Mutex::new(catalog)),
        })
    }

    pub fn new_in_memory() -> Result<Self> {
        let catalog = Catalog::open_in_memory()?;
        Ok(Self {
            catalog: Arc::new(Mutex::new(catalog)),
        })
    }

    pub(crate) fn execute_statement(
        &mut self,
        statement: crate::parser::ast::Statement,
    ) -> Result<QueryResult> {
        if statement.is_read_only() {
            return self.execute_read_statement(statement);
        }

        self.execute_mutating_statement(statement)
    }

    fn execute_read_statement(
        &mut self,
        statement: crate::parser::ast::Statement,
    ) -> Result<QueryResult> {
        let (schema, table_row_counts) = {
            let catalog_guard = self.catalog.lock().unwrap();
            let schema = catalog_guard.clone_schema();
            let table_row_counts = catalog_guard
                .with_storage(|storage| Ok(Self::collect_table_row_counts(storage)))?;
            (schema, table_row_counts)
        };

        let planner = QueryPlanner::new(schema.clone()).with_table_row_counts(table_row_counts);
        let plan = planner.plan(statement)?;
        let mut executor = plan.executor;

        let result = {
            let catalog_guard = self.catalog.lock().unwrap();
            catalog_guard.with_storage(|storage| {
                let mut ctx = ExecutionContext::for_read(&schema, storage);
                executor.execute(&mut ctx)
            })?
        };

        Ok(result)
    }

    fn execute_mutating_statement(
        &mut self,
        statement: crate::parser::ast::Statement,
    ) -> Result<QueryResult> {
        let persists_schema = statement.mutates_schema();
        let (schema, table_row_counts) = {
            let catalog_guard = self.catalog.lock().unwrap();
            let schema = catalog_guard.clone_schema();
            let table_row_counts = catalog_guard
                .with_storage(|storage| Ok(Self::collect_table_row_counts(storage)))?;
            (schema, table_row_counts)
        };

        let planner = QueryPlanner::new(schema.clone()).with_table_row_counts(table_row_counts);
        let plan = planner.plan(statement)?;
        let mut executor = plan.executor;

        let (result, updated_schema) = {
            let catalog_guard = self.catalog.lock().unwrap();
            catalog_guard.with_storage(|storage| {
                let mut ctx = ExecutionContext::for_mutation(&schema, storage);
                let result = executor.execute(&mut ctx)?;
                Ok((result, ctx.catalog))
            })?
        };

        if persists_schema {
            let mut catalog_guard = self.catalog.lock().unwrap();
            catalog_guard.replace_schema(updated_schema)?;
        }

        Ok(result)
    }

    fn collect_table_row_counts(storage: &crate::storage::StorageEngine) -> HashMap<String, usize> {
        storage
            .get_table_metadata()
            .iter()
            .map(|(name, metadata)| (name.clone(), metadata.row_count as usize))
            .collect()
    }

    pub fn close(&mut self) -> Result<()> {
        let mut catalog_guard = self.catalog.lock().unwrap();
        catalog_guard.flush()
    }

    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        let mut lexer = Lexer::new(sql.to_string());
        lexer.tokenize()?;

        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        self.execute_statement(statement)
    }

    pub fn execute_query(&mut self, sql: &str) -> Result<QueryResult> {
        self.execute(sql)
    }

    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        let mut lexer = Lexer::new(sql.to_string());
        lexer.tokenize()?;

        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        let parameter_count = statement.parameter_count();

        Ok(PreparedStatement {
            statement,
            parameters: vec![None; parameter_count],
        })
    }

    pub fn begin_transaction(&'_ mut self) -> Result<Transaction<'_>> {
        Err(HematiteError::InternalError(
            "Transactions are not supported yet".to_string(),
        ))
    }

    #[cfg(test)]
    pub(crate) fn schema_snapshot(&self) -> Result<crate::catalog::Schema> {
        let catalog_guard = self.catalog.lock().unwrap();
        Ok(catalog_guard.clone_schema())
    }
}

#[derive(Debug, Clone)]
pub struct PreparedStatement {
    statement: crate::parser::ast::Statement,
    parameters: Vec<Option<Value>>,
}

impl PreparedStatement {
    pub fn bind(&mut self, index: usize, value: Value) -> Result<()> {
        if index == 0 || index > self.parameters.len() {
            return Err(HematiteError::ParseError(format!(
                "Parameter index {} is out of range",
                index
            )));
        }

        self.parameters[index - 1] = Some(value);
        Ok(())
    }

    pub fn bind_all(&mut self, values: Vec<Value>) -> Result<()> {
        if values.len() != self.parameters.len() {
            return Err(HematiteError::ParseError(format!(
                "Expected {} parameters, got {}",
                self.parameters.len(),
                values.len()
            )));
        }

        self.parameters = values.into_iter().map(Some).collect();
        Ok(())
    }

    pub fn clear_bindings(&mut self) {
        self.parameters.fill(None);
    }

    pub fn parameter_count(&self) -> usize {
        self.parameters.len()
    }

    pub fn execute(&mut self, connection: &mut Connection) -> Result<QueryResult> {
        let statement = self.bound_statement()?;
        connection.execute_statement(statement)
    }

    pub fn query(&mut self, connection: &mut Connection) -> Result<QueryResult> {
        self.execute(connection)
    }

    fn bound_statement(&self) -> Result<crate::parser::ast::Statement> {
        let bound_values = self
            .parameters
            .iter()
            .enumerate()
            .map(|(index, value)| {
                value.clone().ok_or_else(|| {
                    HematiteError::ParseError(format!("Parameter {} has not been bound", index + 1))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        self.statement.bind_parameters(&bound_values)
    }
}

#[derive(Debug)]
pub struct Transaction<'a> {
    #[allow(dead_code)]
    connection: &'a mut Connection,
}

impl<'a> Transaction<'a> {
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        Err(HematiteError::InternalError(format!(
            "Transactions are not supported yet; cannot execute '{}'",
            sql
        )))
    }

    pub fn commit(&mut self) -> Result<()> {
        Err(HematiteError::InternalError(
            "Transactions are not supported yet".to_string(),
        ))
    }

    pub fn rollback(&mut self) -> Result<()> {
        Err(HematiteError::InternalError(
            "Transactions are not supported yet".to_string(),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct Database;

impl Database {
    pub fn new() -> Self {
        Self
    }

    pub fn open(database_path: &str) -> Result<Connection> {
        Connection::new(database_path)
    }

    pub fn open_in_memory() -> Result<Connection> {
        Connection::new_in_memory()
    }

    pub fn connect(&mut self, database_path: &str) -> Result<Connection> {
        Connection::new(database_path)
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}
