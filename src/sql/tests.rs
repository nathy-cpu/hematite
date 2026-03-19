//! Centralized tests for the sql module

mod connection_tests {
    use crate::catalog::DataType;
    use crate::error::Result;
    use crate::sql::connection::*;
    use crate::test_utils::TestDbFile;
    use std::fs;

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
    fn test_primary_key_is_implicitly_not_null() -> Result<()> {
        let db = TestDbFile::new("_test_primary_key_is_implicitly_not_null");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;

        let schema = conn.schema_snapshot()?;
        let table = schema.get_table_by_name("test").unwrap();
        assert!(!table.columns[0].nullable);

        let result = conn.execute("INSERT INTO test (id, name) VALUES (NULL, 'x');");
        assert!(result.is_err());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_duplicate_primary_key_insert_fails() -> Result<()> {
        let db = TestDbFile::new("_test_duplicate_primary_key_insert_fails");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;

        let result = conn.execute("INSERT INTO test (id, name) VALUES (1, 'Bob');");
        assert!(result.is_err());

        let result = conn.execute("SELECT * FROM test;")?;
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0],
            vec![
                crate::catalog::Value::Integer(1),
                crate::catalog::Value::Text("Alice".to_string()),
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_duplicate_primary_key_in_single_multi_row_insert_fails() -> Result<()> {
        let db = TestDbFile::new("_test_duplicate_primary_key_in_single_multi_row_insert_fails");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;

        let result = conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice'), (1, 'Bob');");
        assert!(result.is_err());

        let result = conn.execute("SELECT * FROM test;")?;
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0],
            vec![
                crate::catalog::Value::Integer(1),
                crate::catalog::Value::Text("Alice".to_string()),
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_where_null_comparisons_filter_out_rows() -> Result<()> {
        let db = TestDbFile::new("_test_where_null_comparisons_filter_out_rows");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, NULL);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (2, 'Alice');")?;

        let eq_null = conn.execute("SELECT * FROM test WHERE name = NULL;")?;
        assert_eq!(eq_null.rows.len(), 0);

        let neq_null = conn.execute("SELECT * FROM test WHERE name != NULL;")?;
        assert_eq!(neq_null.rows.len(), 0);

        let null_eq_null = conn.execute("SELECT * FROM test WHERE NULL = NULL;")?;
        assert_eq!(null_eq_null.rows.len(), 0);

        let eq_text = conn.execute("SELECT * FROM test WHERE name = 'Alice';")?;
        assert_eq!(eq_text.rows.len(), 1);
        assert_eq!(
            eq_text.rows[0],
            vec![
                crate::catalog::Value::Integer(2),
                crate::catalog::Value::Text("Alice".to_string()),
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_where_and_or_precedence() -> Result<()> {
        let db = TestDbFile::new("_test_where_and_or_precedence");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE test (
                id INTEGER PRIMARY KEY,
                active BOOLEAN NOT NULL
            );",
        )?;
        conn.execute("INSERT INTO test (id, active) VALUES (1, FALSE);")?;
        conn.execute("INSERT INTO test (id, active) VALUES (2, TRUE);")?;
        conn.execute("INSERT INTO test (id, active) VALUES (3, FALSE);")?;

        let result =
            conn.execute("SELECT id FROM test WHERE id = 1 OR id = 2 AND active = TRUE;")?;

        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0], vec![crate::catalog::Value::Integer(1)]);
        assert_eq!(result.rows[1], vec![crate::catalog::Value::Integer(2)]);

        let parenthesized =
            conn.execute("SELECT id FROM test WHERE (id = 1 OR id = 2) AND active = TRUE;")?;
        assert_eq!(parenthesized.rows.len(), 1);
        assert_eq!(
            parenthesized.rows[0],
            vec![crate::catalog::Value::Integer(2)]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_is_null_and_is_not_null() -> Result<()> {
        let db = TestDbFile::new("_test_is_null_and_is_not_null");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, NULL);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (2, 'Alice');")?;

        let is_null = conn.execute("SELECT * FROM test WHERE name IS NULL;")?;
        assert_eq!(is_null.rows.len(), 1);
        assert_eq!(is_null.rows[0][0], crate::catalog::Value::Integer(1));

        let is_not_null = conn.execute("SELECT * FROM test WHERE name IS NOT NULL;")?;
        assert_eq!(is_not_null.rows.len(), 1);
        assert_eq!(is_not_null.rows[0][0], crate::catalog::Value::Integer(2));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_order_by_asc_and_desc() -> Result<()> {
        let db = TestDbFile::new("_test_order_by_asc_and_desc");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (3, NULL);")?;

        let asc = conn.execute("SELECT id FROM test ORDER BY name ASC, id ASC;")?;
        assert_eq!(
            asc.rows,
            vec![
                vec![crate::catalog::Value::Integer(3)],
                vec![crate::catalog::Value::Integer(1)],
                vec![crate::catalog::Value::Integer(2)],
            ]
        );

        let desc = conn.execute("SELECT id FROM test ORDER BY name DESC, id DESC;")?;
        assert_eq!(
            desc.rows,
            vec![
                vec![crate::catalog::Value::Integer(2)],
                vec![crate::catalog::Value::Integer(1)],
                vec![crate::catalog::Value::Integer(3)],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_limit_applies_after_order_by() -> Result<()> {
        let db = TestDbFile::new("_test_limit_applies_after_order_by");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (3, 'Cara');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")?;

        let result = conn.execute("SELECT id FROM test ORDER BY id ASC LIMIT 2;")?;
        assert_eq!(
            result.rows,
            vec![
                vec![crate::catalog::Value::Integer(1)],
                vec![crate::catalog::Value::Integer(2)],
            ]
        );

        let zero = conn.execute("SELECT id FROM test ORDER BY id ASC LIMIT 0;")?;
        assert_eq!(zero.rows.len(), 0);

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_count_all_returns_single_row() -> Result<()> {
        let db = TestDbFile::new("_test_count_all_returns_single_row");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (2, NULL);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (3, 'Cara');")?;

        let result = conn.execute("SELECT COUNT(*) FROM test;")?;
        assert_eq!(result.columns, vec!["COUNT(*)"]);
        assert_eq!(result.rows, vec![vec![crate::catalog::Value::Integer(3)]]);

        let filtered = conn.execute("SELECT COUNT(*) FROM test WHERE name IS NOT NULL;")?;
        assert_eq!(filtered.rows, vec![vec![crate::catalog::Value::Integer(2)]]);

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
    fn test_delete_with_where_clause() -> Result<()> {
        let db = TestDbFile::new("_test_delete_with_where_clause");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (3, 'Cara');")?;

        let result = conn.execute("DELETE FROM test WHERE id = 2;")?;
        assert_eq!(result.affected_rows, 1);

        let result = conn.execute("SELECT * FROM test;")?;
        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0],
            vec![
                crate::catalog::Value::Integer(1),
                crate::catalog::Value::Text("Alice".to_string()),
            ]
        );
        assert_eq!(
            result.rows[1],
            vec![
                crate::catalog::Value::Integer(3),
                crate::catalog::Value::Text("Cara".to_string()),
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_delete_without_where_clause() -> Result<()> {
        let db = TestDbFile::new("_test_delete_without_where_clause");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")?;

        let result = conn.execute("DELETE FROM test;")?;
        assert_eq!(result.affected_rows, 2);

        let result = conn.execute("SELECT * FROM test;")?;
        assert_eq!(result.rows.len(), 0);

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_drop_table_removes_schema_and_storage() -> Result<()> {
        let db = TestDbFile::new("_test_drop_table_removes_schema_and_storage");

        {
            let mut conn = Connection::new(db.path())?;
            conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
            conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;
            let result = conn.execute("DROP TABLE test;")?;
            assert_eq!(result.affected_rows, 0);

            let schema = conn.schema_snapshot()?;
            assert!(schema.get_table_by_name("test").is_none());

            let select_result = conn.execute("SELECT * FROM test;");
            assert!(select_result.is_err());

            conn.close()?;
        }

        {
            let conn = Connection::new(db.path())?;
            let schema = conn.schema_snapshot()?;
            assert!(schema.get_table_by_name("test").is_none());
        }

        Ok(())
    }

    #[test]
    fn test_update_with_where_clause() -> Result<()> {
        let db = TestDbFile::new("_test_update_with_where_clause");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE test (
                id INTEGER PRIMARY KEY,
                name TEXT,
                active BOOLEAN NOT NULL
            );",
        )?;
        conn.execute("INSERT INTO test (id, name, active) VALUES (1, 'Alice', FALSE);")?;
        conn.execute("INSERT INTO test (id, name, active) VALUES (2, 'Bob', FALSE);")?;

        let result = conn.execute("UPDATE test SET name = 'Bobby', active = TRUE WHERE id = 2;")?;
        assert_eq!(result.affected_rows, 1);

        let result = conn.execute("SELECT * FROM test WHERE id = 2;")?;
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0],
            vec![
                crate::catalog::Value::Integer(2),
                crate::catalog::Value::Text("Bobby".to_string()),
                crate::catalog::Value::Boolean(true),
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_update_without_where_clause() -> Result<()> {
        let db = TestDbFile::new("_test_update_without_where_clause");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, active BOOLEAN NOT NULL);")?;
        conn.execute("INSERT INTO test (id, active) VALUES (1, FALSE);")?;
        conn.execute("INSERT INTO test (id, active) VALUES (2, FALSE);")?;

        let result = conn.execute("UPDATE test SET active = TRUE;")?;
        assert_eq!(result.affected_rows, 2);

        let result = conn.execute("SELECT * FROM test WHERE active = TRUE;")?;
        assert_eq!(result.rows.len(), 2);

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_update_rejects_duplicate_primary_key() -> Result<()> {
        let db = TestDbFile::new("_test_update_rejects_duplicate_primary_key");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")?;

        let result = conn.execute("UPDATE test SET id = 1 WHERE id = 2;");
        assert!(result.is_err());

        let result = conn.execute("SELECT * FROM test;")?;
        assert_eq!(result.rows.len(), 2);

        conn.close()?;
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
    fn test_select_does_not_grow_catalog_storage() -> Result<()> {
        let db = TestDbFile::new("_test_select_does_not_grow_catalog_storage");

        let size_after_create = {
            let mut conn = Connection::new(db.path())?;
            conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
            conn.close()?;
            fs::metadata(db.path())?.len()
        };

        {
            let mut conn = Connection::new(db.path())?;
            let _ = conn.execute("SELECT * FROM users;")?;
            conn.close()?;
        }

        let size_after_select = fs::metadata(db.path())?.len();
        assert_eq!(size_after_select, size_after_create);

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
    fn test_execute_batch_handles_semicolon_in_string_literal() -> Result<()> {
        let test_db = TestDbFile::new("_test_batch_semicolon_in_string");
        let mut db = Hematite::new(test_db.path())?;

        db.execute_batch(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);\n\
             INSERT INTO notes (id, body) VALUES (1, 'hello;world');",
        )?;

        let rs = db.query("SELECT * FROM notes;")?;
        assert_eq!(rs.len(), 1);
        let row = rs.get_row(0).unwrap();
        assert_eq!(row.get_string(1)?, "hello;world");

        Ok(())
    }

    #[test]
    fn test_execute_batch_allows_select_statements() -> Result<()> {
        let test_db = TestDbFile::new("_test_batch_select_statements");
        let mut db = Hematite::new(test_db.path())?;

        db.execute_batch(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n\
             INSERT INTO users (id, name) VALUES (1, 'Alice');\n\
             SELECT * FROM users;",
        )?;

        let rs = db.query("SELECT * FROM users;")?;
        assert_eq!(rs.len(), 1);

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
