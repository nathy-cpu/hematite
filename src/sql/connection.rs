//! SQL connection and statement interface

use crate::catalog::Schema;
use crate::error::{HematiteError, Result};
use crate::parser::{Lexer, Parser};
use crate::query::{ExecutionContext, QueryPlanner, QueryResult};
use crate::storage::{Page, PageId, StorageEngine};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct Connection {
    storage: StorageEngine,
    schema: Arc<Mutex<Schema>>,
}

impl Connection {
    const SCHEMA_MAGIC: [u8; 4] = *b"HMSC";
    const SCHEMA_VERSION: u32 = 1;
    const SCHEMA_PAGE_ID: PageId = PageId::new(0);

    pub fn new(database_path: &str) -> Result<Self> {
        let mut storage = StorageEngine::new(database_path.to_string())?;

        // Load the durable schema from the reserved schema page.
        let schema = Self::load_schema(&mut storage)?;

        Ok(Self {
            storage,
            schema,
        })
    }

    fn load_schema(storage: &mut StorageEngine) -> Result<Arc<Mutex<Schema>>> {
        let schema = match storage.read_page(Self::SCHEMA_PAGE_ID) {
            Ok(page) => Self::deserialize_schema_page(&page)?,
            Err(_) => Schema::new(),
        };

        Ok(Arc::new(Mutex::new(schema)))
    }

    fn persist_schema(&mut self, schema: &Schema) -> Result<()> {
        let page = Self::serialize_schema_page(schema)?;
        self.storage.write_page(page)
    }

    fn serialize_schema_page(schema: &Schema) -> Result<Page> {
        let mut payload = Vec::new();
        schema.serialize(&mut payload)?;

        if payload.len() + 12 > crate::storage::PAGE_SIZE {
            return Err(HematiteError::StorageError(
                "Schema too large for reserved schema page".to_string(),
            ));
        }

        let mut page = Page::new(Self::SCHEMA_PAGE_ID);
        page.data[0..4].copy_from_slice(&Self::SCHEMA_MAGIC);
        page.data[4..8].copy_from_slice(&Self::SCHEMA_VERSION.to_le_bytes());
        page.data[8..12].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        page.data[12..12 + payload.len()].copy_from_slice(&payload);
        Ok(page)
    }

    fn deserialize_schema_page(page: &Page) -> Result<Schema> {
        if page.data.iter().all(|&byte| byte == 0) {
            return Ok(Schema::new());
        }

        if page.data[0..4] != Self::SCHEMA_MAGIC {
            return Err(HematiteError::CorruptedData(
                "Invalid schema page magic".to_string(),
            ));
        }

        let version = u32::from_le_bytes([page.data[4], page.data[5], page.data[6], page.data[7]]);
        if version != Self::SCHEMA_VERSION {
            return Err(HematiteError::CorruptedData(format!(
                "Unsupported schema page version {}",
                version
            )));
        }

        let length =
            u32::from_le_bytes([page.data[8], page.data[9], page.data[10], page.data[11]])
                as usize;
        if 12 + length > page.data.len() {
            return Err(HematiteError::CorruptedData(
                "Invalid schema page length".to_string(),
            ));
        }

        Schema::deserialize(&page.data[12..12 + length])
    }

    pub fn close(&mut self) -> Result<()> {
        let schema = {
            let schema_guard = self
                .schema
                .lock()
                .map_err(|_| HematiteError::InternalError("Schema lock error".to_string()))?;
            schema_guard.clone()
        };
        self.persist_schema(&schema)?;
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
        let updated_schema = {
            let mut schema_guard = self
                .schema
                .lock()
                .map_err(|_| HematiteError::InternalError("Schema lock error".to_string()))?;
            *schema_guard = ctx.catalog;
            schema_guard.clone()
        };
        self.persist_schema(&updated_schema)?;

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
        let schema = {
            let schema_guard = connection
                .schema
                .lock()
                .map_err(|_| HematiteError::InternalError("Schema lock error".to_string()))?;
            schema_guard.clone()
        };

        let mut ctx = ExecutionContext::new(&schema, &mut connection.storage);

        // Plan and execute query
        let planner = QueryPlanner::new(schema.clone());
        let plan = planner.plan(self.statement.clone())?;

        // Execute the plan
        let mut executor = plan.executor;
        let result = executor.execute(&mut ctx)?;

        // Update schema if it was modified
        let updated_schema = {
            let mut schema_guard = connection
                .schema
                .lock()
                .map_err(|_| HematiteError::InternalError("Schema lock error".to_string()))?;
            *schema_guard = ctx.catalog;
            schema_guard.clone()
        };
        connection.persist_schema(&updated_schema)?;

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
        // This project doesn't yet support a true in-memory backend; use a unique temp file to
        // avoid test contention when running in parallel.
        Connection::new(&unique_test_db_path("_test_in_memory"))
    }

    pub fn connect(&mut self, database_path: &str) -> Result<Connection> {
        let connection = Connection::new(database_path)?;

        // Don't store connections for now to avoid Clone issues
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::DataType;
    use std::fs;

    fn tmp_db(prefix: &str) -> String {
        unique_test_db_path(prefix)
    }

    #[test]
    fn test_connection_execute() -> Result<()> {
        let path = tmp_db("_test_connection_execute");
        let _ = fs::remove_file(&path);
        let mut conn = Connection::new(&path)?;

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

        conn.close()?;
        let _ = fs::remove_file(&path);
        Ok(())
    }

    #[test]
    fn test_prepared_statement() -> Result<()> {
        let path = tmp_db("_test_prepared_statement");
        let _ = fs::remove_file(&path);
        let mut conn = Connection::new(&path)?;

        // Create table
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;

        // Prepare statement
        let mut stmt = conn.prepare("INSERT INTO test (id, name) VALUES (1, 'test');")?;
        let result = stmt.execute(&mut conn)?;
        assert!(result.columns.is_empty());
        assert!(result.rows.is_empty());
        assert_eq!(result.affected_rows, 1);

        let query = conn.execute("SELECT * FROM test;")?;
        assert_eq!(query.rows.len(), 1);

        conn.close()?;
        let _ = fs::remove_file(&path);
        Ok(())
    }

    #[test]
    fn test_transaction() -> Result<()> {
        let path = tmp_db("_test_transaction");
        let _ = fs::remove_file(&path);
        let mut conn = Connection::new(&path)?;

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

        conn.close()?;
        let _ = fs::remove_file(&path);
        Ok(())
    }

    #[test]
    fn test_insert_reorders_columns_and_applies_defaults() -> Result<()> {
        let path = tmp_db("_test_insert_reorders_columns");
        let _ = fs::remove_file(&path);
        let mut conn = Connection::new(&path)?;

        conn.execute(
            "CREATE TABLE test (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                active BOOLEAN NOT NULL DEFAULT TRUE
            );",
        )?;

        conn.execute("INSERT INTO test (name, id) VALUES ('test', 1);")?;

        let result = conn.execute("SELECT * FROM test;")?;
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0],
            vec![
                crate::catalog::Value::Integer(1),
                crate::catalog::Value::Text("test".to_string()),
                crate::catalog::Value::Boolean(true),
            ]
        );

        conn.close()?;
        let _ = fs::remove_file(&path);
        Ok(())
    }

    #[test]
    fn test_insert_missing_required_column_fails() -> Result<()> {
        let path = tmp_db("_test_insert_missing_required_column");
        let _ = fs::remove_file(&path);
        let mut conn = Connection::new(&path)?;

        conn.execute(
            "CREATE TABLE test (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );",
        )?;

        let result = conn.execute("INSERT INTO test (id) VALUES (1);");
        assert!(result.is_err());

        conn.close()?;
        let _ = fs::remove_file(&path);
        Ok(())
    }

    #[test]
    fn test_reopen_preserves_exact_schema() -> Result<()> {
        let path = tmp_db("_test_reopen_preserves_exact_schema");
        let _ = fs::remove_file(&path);

        {
            let mut conn = Connection::new(&path)?;
            conn.execute(
                "CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    active BOOLEAN DEFAULT TRUE
                );",
            )?;
            conn.close()?;
        }

        {
            let mut conn = Connection::new(&path)?;
            let schema = conn
                .schema
                .lock()
                .map_err(|_| HematiteError::InternalError("Schema lock error".to_string()))?;
            let table = schema.get_table_by_name("users").unwrap();

            assert_eq!(table.columns.len(), 3);
            assert_eq!(table.columns[0].name, "id");
            assert_eq!(table.columns[0].data_type, DataType::Integer);
            assert!(table.columns[0].primary_key);

            assert_eq!(table.columns[1].name, "name");
            assert_eq!(table.columns[1].data_type, DataType::Text);
            assert!(!table.columns[1].nullable);

            assert_eq!(table.columns[2].name, "active");
            assert_eq!(table.columns[2].data_type, DataType::Boolean);
            assert_eq!(
                table.columns[2].default_value,
                Some(crate::catalog::Value::Boolean(true))
            );
            drop(schema);

            let result =
                conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
            assert_eq!(result.affected_rows, 1);

            let result = conn.execute("SELECT * FROM users;")?;
            assert_eq!(result.columns, vec!["id", "name", "active"]);
            assert_eq!(
                result.rows[0],
                vec![
                    crate::catalog::Value::Integer(1),
                    crate::catalog::Value::Text("Alice".to_string()),
                    crate::catalog::Value::Boolean(true),
                ]
            );

            conn.close()?;
        }

        let _ = fs::remove_file(&path);
        Ok(())
    }

    #[test]
    fn test_database() -> Result<()> {
        let mut db = Database::new();

        // Connect to database
        let path = tmp_db("_test_database_connect");
        let _ = fs::remove_file(&path);
        let mut conn = db.connect(&path)?;

        // Create table
        let result = conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY);")?;
        assert!(result.columns.is_empty());

        conn.close()?;
        let _ = fs::remove_file(&path);
        Ok(())
    }
}
