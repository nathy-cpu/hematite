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
    fn schema_snapshot(&self) -> Result<crate::catalog::Schema> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::DataType;
    use crate::test_utils::TestDbFile;

    #[test]
    fn test_connection_execute() -> Result<()> {
        let db = TestDbFile::new("_test_connection_execute");
        let mut conn = Connection::new(db.path())?;

        let result = conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        assert!(result.columns.is_empty());
        assert!(result.rows.is_empty());

        let result = conn.execute("INSERT INTO test (id, name) VALUES (1, 'test');")?;
        assert!(result.columns.is_empty());
        assert!(result.rows.is_empty());

        let result = conn.execute("SELECT * FROM test;")?;
        assert_eq!(result.columns, vec!["id", "name"]);
        assert_eq!(result.rows.len(), 1);

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_prepared_statement() -> Result<()> {
        let db = TestDbFile::new("_test_prepared_statement");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;

        let mut stmt = conn.prepare("INSERT INTO test (id, name) VALUES (1, 'test');")?;
        let result = stmt.execute(&mut conn)?;
        assert!(result.columns.is_empty());
        assert!(result.rows.is_empty());
        assert_eq!(result.affected_rows, 1);

        let query = conn.execute("SELECT * FROM test;")?;
        assert_eq!(query.rows.len(), 1);

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_transaction() -> Result<()> {
        let db = TestDbFile::new("_test_transaction");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;

        {
            let mut tx = conn.begin_transaction()?;
            tx.execute("INSERT INTO test (id, name) VALUES (1, 'test');")?;
            tx.commit()?;
        }

        let result = conn.execute("SELECT * FROM test;")?;
        assert_eq!(result.rows.len(), 1);

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_insert_reorders_columns_and_applies_defaults() -> Result<()> {
        let db = TestDbFile::new("_test_insert_reorders_columns");
        let mut conn = Connection::new(db.path())?;

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
        Ok(())
    }

    #[test]
    fn test_insert_missing_required_column_fails() -> Result<()> {
        let db = TestDbFile::new("_test_insert_missing_required_column");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE test (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );",
        )?;

        let result = conn.execute("INSERT INTO test (id) VALUES (1);");
        assert!(result.is_err());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_reopen_preserves_exact_schema() -> Result<()> {
        let db = TestDbFile::new("_test_reopen_preserves_exact_schema");

        {
            let mut conn = Connection::new(db.path())?;
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
            let mut conn = Connection::new(db.path())?;
            let schema = conn.schema_snapshot()?;
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

            let result = conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
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

        Ok(())
    }

    #[test]
    fn test_reopen_preserves_table_root_page() -> Result<()> {
        let db = TestDbFile::new("_test_reopen_preserves_table_root_page");

        let root_page_before = {
            let mut conn = Connection::new(db.path())?;
            conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
            let schema = conn.schema_snapshot()?;
            let table = schema.get_table_by_name("users").unwrap();
            let root_page = table.root_page_id;
            assert_ne!(root_page.as_u32(), 0);
            conn.close()?;
            root_page
        };

        let conn = Connection::new(db.path())?;
        let schema = conn.schema_snapshot()?;
        let table = schema.get_table_by_name("users").unwrap();
        assert_eq!(table.root_page_id, root_page_before);

        Ok(())
    }

    #[test]
    fn test_database() -> Result<()> {
        let mut db = Database::new();
        let test_db = TestDbFile::new("_test_database_connect");
        let mut conn = db.connect(test_db.path())?;

        let result = conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY);")?;
        assert!(result.columns.is_empty());

        conn.close()?;
        Ok(())
    }
}
