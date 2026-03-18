//! SQL connection and statement interface

use crate::catalog::Catalog;
use crate::error::Result;
use crate::parser::{Lexer, Parser};
use crate::query::{ExecutionContext, QueryPlanner, QueryResult};
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

    fn execute_statement(&mut self, statement: crate::parser::ast::Statement) -> Result<QueryResult> {
        let schema = {
            let catalog_guard = self.catalog.lock().unwrap();
            catalog_guard.clone_schema()
        };

        let planner = QueryPlanner::new(schema.clone());
        let plan = planner.plan(statement)?;
        let mut executor = plan.executor;

        let (result, updated_schema) = {
            let catalog_guard = self.catalog.lock().unwrap();
            catalog_guard.with_storage(|storage| {
                let mut ctx = ExecutionContext::new(&schema, storage);
                let result = executor.execute(&mut ctx)?;
                Ok((result, ctx.catalog))
            })?
        };

        {
            let mut catalog_guard = self.catalog.lock().unwrap();
            catalog_guard.replace_schema(updated_schema)?;
        }

        Ok(result)
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

        Ok(PreparedStatement { statement })
    }

    pub fn begin_transaction(&'_ mut self) -> Result<Transaction<'_>> {
        Ok(Transaction::new(self))
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
}

impl PreparedStatement {
    pub fn execute(&mut self, connection: &mut Connection) -> Result<QueryResult> {
        connection.execute_statement(self.statement.clone())
    }

    pub fn query(&mut self, connection: &mut Connection) -> Result<QueryResult> {
        self.execute(connection)
    }
}

#[derive(Debug)]
pub struct Transaction<'a> {
    connection: &'a mut Connection,
    committed: bool,
}

impl<'a> Transaction<'a> {
    fn new(connection: &'a mut Connection) -> Self {
        Self {
            connection,
            committed: false,
        }
    }

    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        self.connection.execute(sql)
    }

    pub fn commit(&mut self) -> Result<()> {
        self.committed = true;
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<()> {
        self.committed = false;
        Ok(())
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.rollback();
        }
    }
}

#[derive(Debug, Clone)]
pub struct Database {
    connections: Arc<Mutex<Vec<Connection>>>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn open(database_path: &str) -> Result<Connection> {
        Connection::new(database_path)
    }

    pub fn open_in_memory() -> Result<Connection> {
        Connection::new(&unique_test_db_path("_test_in_memory"))
    }

    pub fn connect(&mut self, database_path: &str) -> Result<Connection> {
        let connection = Connection::new(database_path)?;
        Ok(connection)
    }
}

fn unique_test_db_path(prefix: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{}_{}.db", prefix, nanos)
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}
