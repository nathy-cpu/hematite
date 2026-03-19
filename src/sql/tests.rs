//! Centralized tests for the sql module

mod connection_tests {
    use crate::catalog::DataType;
    use crate::error::Result;
    use crate::sql::connection::*;
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

        let result = conn.begin_transaction();
        assert!(result.is_err());

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

mod interface_tests {
    use crate::error::Result;
    use crate::sql::interface::*;
    use crate::test_utils::TestDbFile;

    #[test]
    fn test_hematite_basic_operations() -> Result<()> {
        let test_db = TestDbFile::new("_test_in_memory");
        let mut db = Hematite::new(test_db.path())?;

        // Create table
        let result = db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        assert_eq!(result.affected_rows, 0);

        // Insert data
        let result = db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
        assert_eq!(result.affected_rows, 1);

        // Query data
        let result_set = db.query("SELECT * FROM users;")?;
        assert_eq!(result_set.len(), 1);

        let row = result_set.get_row(0).unwrap();
        assert_eq!(row.get_int(0)?, 1);
        assert_eq!(row.get_string(1)?, "Alice");

        Ok(())
    }

    #[test]
    fn test_hematite_query_one() -> Result<()> {
        let test_db = TestDbFile::new("_test_in_memory");
        let mut db = Hematite::new(test_db.path())?;

        // Create table and insert data
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER);")?;
        db.execute("INSERT INTO test (id, value) VALUES (1, 42);")?;

        // Query single value using simple SELECT
        let result_set = db.query("SELECT * FROM test;")?;
        assert_eq!(result_set.len(), 1);

        Ok(())
    }

    #[test]
    fn test_hematite_prepare() -> Result<()> {
        let test_db = TestDbFile::new("_test_in_memory");
        let mut db = Hematite::new(test_db.path())?;

        // Create table
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;

        // Prepare statement with actual values instead of placeholders for this simplified implementation
        let mut stmt = db.prepare("INSERT INTO test (id, name) VALUES (1, 'test');")?;

        // Note: This is a simplified implementation - real prepared statements would support parameters
        let result = stmt.execute(&mut db.connection)?;
        assert_eq!(result.rows.len(), 0);

        Ok(())
    }

    #[test]
    fn test_hematite_transaction() -> Result<()> {
        let test_db = TestDbFile::new("_test_in_memory");
        let mut db = Hematite::new(test_db.path())?;

        // Create table
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY);")?;

        let result = db.transaction();
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_execute_batch_semicolon_handling() -> Result<()> {
        let test_db = TestDbFile::new("_test_in_memory");
        let mut db = Hematite::new(test_db.path())?;

        db.execute_batch(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n\
             INSERT INTO users (id, name) VALUES (1, 'Alice');\n\
             INSERT INTO users (id, name) VALUES (2, 'Bob');",
        )?;

        let rs = db.query("SELECT * FROM users;")?;
        assert_eq!(rs.len(), 2);
        Ok(())
    }

    #[test]
    fn test_sql_debug_simple() -> Result<()> {
        println!("=== Testing SQL Parsing Only ===");

        use crate::parser::{Lexer, Parser};

        println!("✓ Step 1: Creating lexer...");
        let mut lexer =
            Lexer::new("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);".to_string());
        lexer.tokenize()?;
        println!("✓ Lexing completed");

        println!("✓ Step 2: Creating parser...");
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        println!("✓ Parsing completed: {:?}", statement);

        println!("✓ SUCCESS: Parsing test passed");
        Ok(())
    }
}

mod result_tests {
    use crate::catalog::Value;
    use crate::error::Result;
    use crate::sql::result::*;

    #[test]
    fn test_result_set() -> Result<()> {
        let columns = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            vec![Value::Integer(1), Value::Text("Alice".to_string())],
            vec![Value::Integer(2), Value::Text("Bob".to_string())],
        ];

        let result_set = ResultSet::new(columns, rows);

        assert_eq!(result_set.len(), 2);
        assert_eq!(result_set.column_count(), 2);
        assert_eq!(result_set.get_column_index("id"), Some(0));
        assert_eq!(result_set.get_column_index("name"), Some(1));
        assert_eq!(result_set.get_column_index("invalid"), None);

        let row = result_set.get_row(0).unwrap();
        assert_eq!(row.get_int(0)?, 1);
        assert_eq!(row.get_string(1)?, "Alice");

        Ok(())
    }

    #[test]
    fn test_row() -> Result<()> {
        let values = vec![
            Value::Integer(42),
            Value::Text("test".to_string()),
            Value::Boolean(true),
            Value::Float(3.14),
            Value::Null,
        ];

        let row = Row::new(values);

        assert_eq!(row.get_int(0)?, 42);
        assert_eq!(row.get_string(1)?, "test");
        assert_eq!(row.get_bool(2)?, true);
        assert_eq!(row.get_float(3)?, 3.14);
        assert!(row.is_null(4));

        // Test type conversion errors
        assert!(row.get_string(0).is_err());
        assert!(row.get_bool(0).is_err());

        Ok(())
    }

    #[test]
    fn test_statement_result() -> Result<()> {
        let result = StatementResult::new(1, "Table created".to_string());
        assert_eq!(result.affected_rows, 1);
        assert_eq!(result.message, "Table created");
        assert!(result.last_insert_id.is_none());

        let result = StatementResult::with_insert_id(1, 42, "Row inserted".to_string());
        assert_eq!(result.affected_rows, 1);
        assert_eq!(result.last_insert_id, Some(42));
        assert_eq!(result.message, "Row inserted");

        Ok(())
    }
}
