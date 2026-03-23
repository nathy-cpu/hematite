//! SQL connection and statement interface

use crate::catalog::Catalog;
use crate::catalog::CatalogEngine;
use crate::catalog::JournalMode;
use crate::catalog::Value;
use crate::error::{HematiteError, Result};
use crate::parser::{Lexer, Parser};
use crate::query::{ExecutionContext, QueryPlanner, QueryResult};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
struct ConnectionTransaction {
    snapshot: crate::catalog::catalog::CatalogSnapshot,
}

#[derive(Debug)]
pub struct Connection {
    catalog: Arc<Mutex<Catalog>>,
    transaction: Option<ConnectionTransaction>,
}

impl Connection {
    pub fn new(database_path: &str) -> Result<Self> {
        let catalog = Catalog::open_or_create(database_path)?;
        Ok(Self {
            catalog: Arc::new(Mutex::new(catalog)),
            transaction: None,
        })
    }

    pub fn new_in_memory() -> Result<Self> {
        let catalog = Catalog::open_in_memory()?;
        Ok(Self {
            catalog: Arc::new(Mutex::new(catalog)),
            transaction: None,
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
            let mut catalog_guard = self.catalog.lock().unwrap();
            let schema = catalog_guard.clone_schema();
            let table_row_counts =
                catalog_guard.with_engine(|engine| Ok(Self::collect_table_row_counts(engine)))?;
            (schema, table_row_counts)
        };

        let planner = QueryPlanner::new(schema.clone()).with_table_row_counts(table_row_counts);
        let plan = planner.plan(statement)?;
        let mut executor = plan.into_executor();

        let result = {
            let mut catalog_guard = self.catalog.lock().unwrap();
            catalog_guard.with_read_engine(|engine| {
                let mut ctx = ExecutionContext::for_read(&schema, engine);
                executor.execute(&mut ctx)
            })?
        };

        Ok(result)
    }

    fn execute_mutating_statement(
        &mut self,
        statement: crate::parser::ast::Statement,
    ) -> Result<QueryResult> {
        let explicit_transaction = self.transaction.is_some();
        let persists_schema = statement.mutates_schema();
        let (schema, table_row_counts) = {
            let mut catalog_guard = self.catalog.lock().unwrap();
            let schema = catalog_guard.clone_schema();
            let table_row_counts =
                catalog_guard.with_engine(|engine| Ok(Self::collect_table_row_counts(engine)))?;
            (schema, table_row_counts)
        };

        let planner = QueryPlanner::new(schema.clone()).with_table_row_counts(table_row_counts);
        let plan = planner.plan(statement)?;
        let mut executor = plan.into_executor();

        let implicit_snapshot = if explicit_transaction {
            None
        } else {
            let mut catalog_guard = self.catalog.lock().unwrap();
            let snapshot = catalog_guard.snapshot();
            catalog_guard.begin_transaction()?;
            Some(snapshot)
        };

        let execution_result = {
            let mut catalog_guard = self.catalog.lock().unwrap();
            catalog_guard.with_engine(|engine| {
                let mut ctx = ExecutionContext::for_mutation(&schema, engine);
                let result = executor.execute(&mut ctx)?;
                Ok((result, ctx.catalog))
            })
        };

        match execution_result {
            Ok((result, updated_schema)) => {
                if persists_schema {
                    let mut catalog_guard = self.catalog.lock().unwrap();
                    if let Err(err) = catalog_guard.replace_schema(updated_schema) {
                        if let Some(snapshot) = implicit_snapshot {
                            let _ = catalog_guard.rollback_transaction();
                            catalog_guard.restore_snapshot(snapshot);
                        }
                        return Err(err);
                    }
                }

                if !explicit_transaction {
                    let mut catalog_guard = self.catalog.lock().unwrap();
                    if let Err(err) = catalog_guard.commit_transaction() {
                        if let Some(snapshot) = implicit_snapshot {
                            let _ = catalog_guard.rollback_transaction();
                            catalog_guard.restore_snapshot(snapshot);
                        }
                        return Err(err);
                    }
                }

                Ok(result)
            }
            Err(err) => {
                if let Some(snapshot) = implicit_snapshot {
                    let mut catalog_guard = self.catalog.lock().unwrap();
                    let _ = catalog_guard.rollback_transaction();
                    catalog_guard.restore_snapshot(snapshot);
                }
                Err(err)
            }
        }
    }

    fn collect_table_row_counts(engine: &CatalogEngine) -> HashMap<String, usize> {
        engine
            .get_table_metadata()
            .iter()
            .map(|(name, metadata)| (name.clone(), metadata.row_count as usize))
            .collect()
    }

    pub fn close(&mut self) -> Result<()> {
        if self.transaction.is_some() {
            return Err(HematiteError::InternalError(
                "Cannot close connection with an active transaction".to_string(),
            ));
        }
        let mut catalog_guard = self.catalog.lock().unwrap();
        catalog_guard.flush()
    }

    pub fn journal_mode(&self) -> Result<JournalMode> {
        let catalog_guard = self.catalog.lock().unwrap();
        catalog_guard.journal_mode()
    }

    pub fn set_journal_mode(&mut self, journal_mode: JournalMode) -> Result<()> {
        let mut catalog_guard = self.catalog.lock().unwrap();
        catalog_guard.set_journal_mode(journal_mode)
    }

    pub fn checkpoint_wal(&mut self) -> Result<()> {
        let mut catalog_guard = self.catalog.lock().unwrap();
        catalog_guard.checkpoint_wal()
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
        if self.transaction.is_some() {
            return Err(HematiteError::InternalError(
                "Transaction is already active".to_string(),
            ));
        }

        let mut catalog_guard = self.catalog.lock().unwrap();
        let snapshot = catalog_guard.snapshot();
        catalog_guard.begin_transaction()?;
        drop(catalog_guard);

        self.transaction = Some(ConnectionTransaction { snapshot });
        Ok(Transaction {
            connection: self,
            completed: false,
        })
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
    connection: &'a mut Connection,
    completed: bool,
}

impl<'a> Transaction<'a> {
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        self.connection.execute(sql)
    }

    pub fn commit(&mut self) -> Result<()> {
        if self.completed {
            return Err(HematiteError::InternalError(
                "Transaction is already completed".to_string(),
            ));
        }
        self.connection.commit_active_transaction()?;
        self.completed = true;
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<()> {
        if self.completed {
            return Err(HematiteError::InternalError(
                "Transaction is already completed".to_string(),
            ));
        }
        self.connection.rollback_active_transaction()?;
        self.completed = true;
        Ok(())
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.connection.rollback_active_transaction();
        }
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

impl Connection {
    fn commit_active_transaction(&mut self) -> Result<()> {
        if self.transaction.is_none() {
            return Err(HematiteError::InternalError(
                "No active transaction to commit".to_string(),
            ));
        }

        let mut catalog_guard = self.catalog.lock().unwrap();
        match catalog_guard.commit_transaction() {
            Ok(()) => {
                self.transaction = None;
                Ok(())
            }
            Err(err) => {
                if let Some(state) = self.transaction.take() {
                    let _ = catalog_guard.rollback_transaction();
                    catalog_guard.restore_snapshot(state.snapshot);
                }
                Err(err)
            }
        }
    }

    fn rollback_active_transaction(&mut self) -> Result<()> {
        let state = self.transaction.take().ok_or_else(|| {
            HematiteError::InternalError("No active transaction to roll back".to_string())
        })?;
        let mut catalog_guard = self.catalog.lock().unwrap();
        catalog_guard.rollback_transaction()?;
        catalog_guard.restore_snapshot(state.snapshot);
        Ok(())
    }
}
