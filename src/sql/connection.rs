//! SQL connection and statement interface

use crate::catalog::{Column, ColumnId, DataType, Schema};
use crate::error::{HematiteError, Result};
use crate::parser::{Lexer, Parser};
use crate::query::{ExecutionContext, QueryPlanner, QueryResult};
use crate::storage::StorageEngine;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct Connection {
    storage: StorageEngine,
    schema: Arc<Mutex<Schema>>,
}

impl Connection {
    pub fn new(database_path: &str) -> Result<Self> {
        let mut storage = StorageEngine::new(database_path.to_string())?;

        // Load existing schema from storage
        let schema = Self::load_schema(&mut storage)?;

        Ok(Self { storage, schema })
    }

    fn load_schema(storage: &mut StorageEngine) -> Result<Arc<Mutex<Schema>>> {
        // Load schema from existing table metadata
        let mut schema = Schema::new();

        // Get table metadata from storage engine
        let table_metadata = storage.get_table_metadata();

        // Reconstruct schema from table metadata
        for (table_name, _metadata) in table_metadata {
            // Create a placeholder table with basic columns
            // In a real implementation, we would persist column definitions
            let columns = vec![
                Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer)
                    .primary_key(true),
                Column::new(ColumnId::new(2), "data".to_string(), DataType::Text),
            ];

            // Create the table in the schema
            if let Err(_) = schema.create_table(table_name.clone(), columns) {
                // If table creation fails, skip it
                continue;
            }
        }

        Ok(Arc::new(Mutex::new(schema)))
    }

    pub fn close(&mut self) -> Result<()> {
        self.storage.flush()?;
        Ok(())
    }

    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        // Parse SQL
        let mut lexer = Lexer::new(sql.to_string());
        lexer.tokenize()?;

        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;

        // Create execution context
        let schema = {
            let schema_guard = self
                .schema
                .lock()
                .map_err(|_| HematiteError::InternalError("Schema lock error".to_string()))?;
            schema_guard.clone()
        };

        let mut ctx = ExecutionContext::new(&schema, &mut self.storage);

        // Plan and execute query
        let planner = QueryPlanner::new(schema.clone());
        let plan = planner.plan(statement)?;

        // Execute the plan
        let mut executor = plan.executor;
        let result = executor.execute(&mut ctx)?;

        // Update schema if it was modified
        {
            let mut schema_guard = self
                .schema
                .lock()
                .map_err(|_| HematiteError::InternalError("Schema lock error".to_string()))?;
            *schema_guard = ctx.catalog;
        }

        Ok(result)
    }

    pub fn execute_query(&mut self, sql: &str) -> Result<QueryResult> {
        self.execute(sql)
    }

    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        // Parse SQL to validate syntax
        let mut lexer = Lexer::new(sql.to_string());
        lexer.tokenize()?;

        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;

        Ok(PreparedStatement {
            sql: sql.to_string(),
            statement,
            connection_schema: self.schema.clone(),
        })
    }

    pub fn begin_transaction(&'_ mut self) -> Result<Transaction<'_>> {
        Ok(Transaction::new(self))
    }
}

#[derive(Debug, Clone)]
pub struct PreparedStatement {
    sql: String,
    statement: crate::parser::ast::Statement,
    connection_schema: Arc<Mutex<Schema>>,
}

impl PreparedStatement {
    pub fn execute(&mut self, connection: &mut Connection) -> Result<QueryResult> {
        // Create execution context
        let schema_guard = connection
            .schema
            .lock()
            .map_err(|_| HematiteError::InternalError("Schema lock error".to_string()))?;
        let mut ctx = ExecutionContext::new(&schema_guard, &mut connection.storage);

        // Plan and execute query
        let planner = QueryPlanner::new(schema_guard.clone());
        let plan = planner.plan(self.statement.clone())?;

        // Execute the plan
        let mut executor = plan.executor;
        let result = executor.execute(&mut ctx)?;

        // Update schema if it was modified
        {
            let mut schema_guard = connection
                .schema
                .lock()
                .map_err(|_| HematiteError::InternalError("Schema lock error".to_string()))?;
            *schema_guard = ctx.catalog;
        }

        Ok(result)
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
        // In a real implementation, this would commit changes to storage
        self.committed = true;
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<()> {
        // In a real implementation, this would rollback changes
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
        Connection::new("_test.db")
    }

    pub fn connect(&mut self, database_path: &str) -> Result<Connection> {
        let connection = Connection::new(database_path)?;

        // Don't store connections for now to avoid Clone issues
        Ok(connection)
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_execute() -> Result<()> {
        let mut conn = Connection::new("_test.db")?;

        // Create table
        let result = conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        assert!(result.columns.is_empty());
        assert!(result.rows.is_empty());

        // Insert data
        let result = conn.execute("INSERT INTO test (id, name) VALUES (1, 'test');")?;
        assert!(result.columns.is_empty());
        assert!(result.rows.is_empty());

        // Query data
        let result = conn.execute("SELECT * FROM test;")?;
        assert_eq!(result.columns, vec!["id", "name"]);
        assert_eq!(result.rows.len(), 1);

        Ok(())
    }

    #[test]
    fn test_prepared_statement() -> Result<()> {
        let mut conn = Connection::new("_test.db")?;

        // Create table
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;

        // Prepare statement
        let mut stmt = conn.prepare("INSERT INTO test (id, name) VALUES (1, 'test');")?;
        let result = stmt.execute(&mut conn)?;
        assert!(result.columns.is_empty());
        assert!(result.rows.is_empty());

        Ok(())
    }

    #[test]
    fn test_transaction() -> Result<()> {
        let mut conn = Connection::new("_test.db")?;

        // Create table
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;

        // Begin transaction and execute within its scope
        {
            let mut tx = conn.begin_transaction()?;

            // Insert data
            tx.execute("INSERT INTO test (id, name) VALUES (1, 'test');")?;

            // Commit transaction
            tx.commit()?;
        } // tx is dropped here, releasing the mutable borrow

        // Verify data - now safe to use conn again
        let result = conn.execute("SELECT * FROM test;")?;
        assert_eq!(result.rows.len(), 1);

        Ok(())
    }

    #[test]
    fn test_database() -> Result<()> {
        let mut db = Database::new();

        // Connect to database
        let mut conn = db.connect("_test.db")?;

        // Create table
        let result = conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY);")?;
        assert!(result.columns.is_empty());

        Ok(())
    }
}
