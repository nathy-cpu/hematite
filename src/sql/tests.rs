//! Centralized tests for the sql module

mod connection_tests {
    use crate::catalog::{DataType, JournalMode};
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
    fn test_connection_execute_in_memory() -> Result<()> {
        let mut conn = Connection::new_in_memory()?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, 'test');")?;

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
    fn test_prepared_statement_with_parameters() -> Result<()> {
        let db = TestDbFile::new("_test_prepared_statement_with_parameters");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;

        let mut stmt = conn.prepare("INSERT INTO test (id, name) VALUES (?, ?);")?;
        assert_eq!(stmt.parameter_count(), 2);
        stmt.bind(1, crate::catalog::Value::Integer(1))?;
        stmt.bind(2, crate::catalog::Value::Text("test".to_string()))?;
        stmt.execute(&mut conn)?;

        let query = conn.execute("SELECT * FROM test;")?;
        assert_eq!(query.rows.len(), 1);
        assert_eq!(
            query.rows[0],
            vec![
                crate::catalog::Value::Integer(1),
                crate::catalog::Value::Text("test".to_string()),
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_prepared_statement_requires_all_parameters() -> Result<()> {
        let db = TestDbFile::new("_test_prepared_statement_requires_all_parameters");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;

        let mut stmt = conn.prepare("INSERT INTO test (id, name) VALUES (?, ?);")?;
        stmt.bind(1, crate::catalog::Value::Integer(1))?;

        let result = stmt.execute(&mut conn);
        assert!(result.is_err());

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
            tx.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;
            tx.rollback()?;
        }

        let result = conn.execute("SELECT * FROM test;")?;
        assert!(result.rows.is_empty());

        {
            let mut tx = conn.begin_transaction()?;
            tx.execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")?;
            tx.commit()?;
        }

        let result = conn.execute("SELECT * FROM test;")?;
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0],
            vec![
                crate::catalog::Value::Integer(2),
                crate::catalog::Value::Text("Bob".to_string()),
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_sql_begin_commit_and_rollback() -> Result<()> {
        let db = TestDbFile::new("_test_sql_begin_commit_and_rollback");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;

        conn.execute("BEGIN;")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;
        conn.execute("ROLLBACK;")?;

        let result = conn.execute("SELECT * FROM test;")?;
        assert!(result.rows.is_empty());

        conn.execute("BEGIN;")?;
        conn.execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")?;
        conn.execute("COMMIT;")?;

        let result = conn.execute("SELECT * FROM test;")?;
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0],
            vec![
                crate::catalog::Value::Integer(2),
                crate::catalog::Value::Text("Bob".to_string()),
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_writer_transaction_blocks_second_writer_connection() -> Result<()> {
        let db = TestDbFile::new("_test_writer_transaction_blocks_second_writer");
        let mut conn1 = Connection::new(db.path())?;

        conn1.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        let mut conn2 = Connection::new(db.path())?;

        let mut tx = conn1.begin_transaction()?;
        tx.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;

        let err = conn2
            .execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")
            .unwrap_err();
        assert!(err.to_string().contains("locked"));

        tx.rollback()?;
        drop(tx);

        conn2.execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")?;
        let result = conn2.execute("SELECT * FROM test;")?;
        assert_eq!(result.rows.len(), 1);

        conn1.close()?;
        conn2.close()?;
        Ok(())
    }

    #[test]
    fn test_writer_transaction_blocks_reader_connection() -> Result<()> {
        let db = TestDbFile::new("_test_writer_transaction_blocks_reader");
        let mut conn1 = Connection::new(db.path())?;

        conn1.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        let mut conn2 = Connection::new(db.path())?;

        let mut tx = conn1.begin_transaction()?;
        tx.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;

        let err = conn2.execute("SELECT * FROM test;").unwrap_err();
        assert!(err.to_string().contains("locked"));

        tx.rollback()?;
        drop(tx);

        let result = conn2.execute("SELECT * FROM test;")?;
        assert!(result.rows.is_empty());

        conn1.close()?;
        conn2.close()?;
        Ok(())
    }

    #[test]
    fn test_wal_mode_reader_sees_precommit_snapshot() -> Result<()> {
        let db = TestDbFile::new("_test_wal_mode_reader_sees_precommit_snapshot");
        let mut conn1 = Connection::new(db.path())?;
        conn1.set_journal_mode(JournalMode::Wal)?;

        conn1.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn1.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;

        let mut conn2 = Connection::new(db.path())?;
        assert_eq!(conn2.journal_mode()?, JournalMode::Wal);

        let mut tx = conn1.begin_transaction()?;
        tx.execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")?;

        let result = conn2.execute("SELECT * FROM test ORDER BY id;")?;
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0],
            vec![
                crate::catalog::Value::Integer(1),
                crate::catalog::Value::Text("Alice".to_string()),
            ]
        );

        tx.commit()?;
        drop(tx);

        let result = conn2.execute("SELECT * FROM test ORDER BY id;")?;
        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[1],
            vec![
                crate::catalog::Value::Integer(2),
                crate::catalog::Value::Text("Bob".to_string()),
            ]
        );

        conn1.close()?;
        conn2.close()?;
        Ok(())
    }

    #[test]
    fn test_connection_can_switch_from_wal_back_to_rollback() -> Result<()> {
        let db = TestDbFile::new("_test_connection_can_switch_from_wal_back_to_rollback");
        let mut conn = Connection::new(db.path())?;
        conn.set_journal_mode(JournalMode::Wal)?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;
        assert_eq!(conn.journal_mode()?, JournalMode::Wal);

        conn.set_journal_mode(JournalMode::Rollback)?;
        assert_eq!(conn.journal_mode()?, JournalMode::Rollback);
        conn.close()?;

        let mut reopened = Connection::new(db.path())?;
        assert_eq!(reopened.journal_mode()?, JournalMode::Rollback);
        let result = reopened.execute("SELECT * FROM test;")?;
        assert_eq!(result.rows.len(), 1);
        reopened.close()?;
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
        assert!(result.rows.is_empty());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_create_and_drop_index_sql() -> Result<()> {
        let db = TestDbFile::new("_test_create_and_drop_index_sql");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, name TEXT);")?;
        conn.execute("INSERT INTO users (id, email, name) VALUES (1, 'a@example.com', 'Alice');")?;
        conn.execute("INSERT INTO users (id, email, name) VALUES (2, 'b@example.com', 'Bob');")?;

        conn.execute("CREATE INDEX idx_users_email ON users (email);")?;

        let schema = conn.schema_snapshot()?;
        let table = schema.get_table_by_name("users").unwrap();
        let index = table.get_secondary_index("idx_users_email").unwrap();
        assert_eq!(index.column_indices, vec![1]);

        let result = conn.execute("SELECT id FROM users WHERE email = 'b@example.com';")?;
        assert_eq!(result.rows, vec![vec![crate::catalog::Value::Integer(2)]]);

        conn.execute("DROP INDEX idx_users_email ON users;")?;
        let schema = conn.schema_snapshot()?;
        let table = schema.get_table_by_name("users").unwrap();
        assert!(table.get_secondary_index("idx_users_email").is_none());

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
    fn test_select_with_table_alias_and_projection_alias() -> Result<()> {
        let db = TestDbFile::new("_test_select_with_aliases");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO users (id, name) VALUES (2, 'Bob');")?;

        let result = conn.execute(
            "SELECT u.name AS user_name FROM users AS u WHERE u.id = 1 ORDER BY u.name;",
        )?;

        assert_eq!(result.columns, vec!["user_name"]);
        assert_eq!(
            result.rows,
            vec![vec![crate::catalog::Value::Text("Alice".to_string())]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_select_distinct_deduplicates_rows() -> Result<()> {
        let db = TestDbFile::new("_test_select_distinct_deduplicates_rows");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (2, 'Alice');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (3, 'Bob');")?;

        let result = conn.execute("SELECT DISTINCT name FROM test ORDER BY name ASC;")?;

        assert_eq!(result.columns, vec!["name"]);
        assert_eq!(
            result.rows,
            vec![
                vec![crate::catalog::Value::Text("Alice".to_string())],
                vec![crate::catalog::Value::Text("Bob".to_string())],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_where_in_and_not_in() -> Result<()> {
        let db = TestDbFile::new("_test_where_in_and_not_in");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (3, 'Cara');")?;

        let included = conn.execute("SELECT id FROM test WHERE id IN (1, 3) ORDER BY id ASC;")?;
        assert_eq!(
            included.rows,
            vec![
                vec![crate::catalog::Value::Integer(1)],
                vec![crate::catalog::Value::Integer(3)],
            ]
        );

        let excluded =
            conn.execute("SELECT id FROM test WHERE id NOT IN (1, 3) ORDER BY id ASC;")?;
        assert_eq!(excluded.rows, vec![vec![crate::catalog::Value::Integer(2)]]);

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_where_between() -> Result<()> {
        let db = TestDbFile::new("_test_where_between");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (3, 'Cara');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (4, 'Dina');")?;

        let result =
            conn.execute("SELECT id FROM test WHERE id BETWEEN 2 AND 3 ORDER BY id ASC;")?;
        assert_eq!(
            result.rows,
            vec![
                vec![crate::catalog::Value::Integer(2)],
                vec![crate::catalog::Value::Integer(3)],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_where_like_and_not_like() -> Result<()> {
        let db = TestDbFile::new("_test_where_like_and_not_like");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (2, 'Al');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (3, 'Bob');")?;

        let like = conn.execute("SELECT id FROM test WHERE name LIKE 'Al%' ORDER BY id ASC;")?;
        assert_eq!(
            like.rows,
            vec![
                vec![crate::catalog::Value::Integer(1)],
                vec![crate::catalog::Value::Integer(2)],
            ]
        );

        let not_like =
            conn.execute("SELECT id FROM test WHERE name NOT LIKE 'Al%' ORDER BY id ASC;")?;
        assert_eq!(not_like.rows, vec![vec![crate::catalog::Value::Integer(3)]]);

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_where_not_with_grouping() -> Result<()> {
        let db = TestDbFile::new("_test_where_not_with_grouping");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (3, 'Cara');")?;

        let result =
            conn.execute("SELECT id FROM test WHERE NOT (id = 1 OR id = 2) ORDER BY id ASC;")?;
        assert_eq!(result.rows, vec![vec![crate::catalog::Value::Integer(3)]]);

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_select_expression_projection_and_where_arithmetic() -> Result<()> {
        let db = TestDbFile::new("_test_select_expression_projection_and_where_arithmetic");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, score INTEGER);")?;
        conn.execute("INSERT INTO test (id, score) VALUES (1, 1);")?;
        conn.execute("INSERT INTO test (id, score) VALUES (2, 2);")?;
        conn.execute("INSERT INTO test (id, score) VALUES (3, 3);")?;

        let result = conn.execute(
            "SELECT score + 1 AS next_score FROM test WHERE score * 2 >= 4 ORDER BY id ASC;",
        )?;
        assert_eq!(result.columns, vec!["next_score"]);
        assert_eq!(
            result.rows,
            vec![
                vec![crate::catalog::Value::Integer(3)],
                vec![crate::catalog::Value::Integer(4)],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_insert_and_update_with_arithmetic_expressions() -> Result<()> {
        let db = TestDbFile::new("_test_insert_and_update_with_arithmetic_expressions");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, score INTEGER);")?;
        conn.execute("INSERT INTO test (id, score) VALUES (1 + 1, -3);")?;
        conn.execute("UPDATE test SET score = score + 5 WHERE id = 2;")?;

        let result = conn.execute("SELECT id, score FROM test ORDER BY id ASC;")?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Integer(2),
                crate::catalog::Value::Integer(2)
            ]]
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
    fn test_offset_applies_after_order_by() -> Result<()> {
        let db = TestDbFile::new("_test_offset_applies_after_order_by");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (3, 'Cara');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")?;

        let result = conn.execute("SELECT id FROM test ORDER BY id ASC OFFSET 1;")?;
        assert_eq!(
            result.rows,
            vec![
                vec![crate::catalog::Value::Integer(2)],
                vec![crate::catalog::Value::Integer(3)],
            ]
        );

        let paged = conn.execute("SELECT id FROM test ORDER BY id ASC LIMIT 1 OFFSET 1;")?;
        assert_eq!(paged.rows, vec![vec![crate::catalog::Value::Integer(2)]]);

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
    fn test_simple_aggregates_without_group_by() -> Result<()> {
        let db = TestDbFile::new("_test_simple_aggregates_without_group_by");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, score FLOAT);")?;
        conn.execute("INSERT INTO test (id, score) VALUES (1, 10);")?;
        conn.execute("INSERT INTO test (id, score) VALUES (2, 20);")?;
        conn.execute("INSERT INTO test (id, score) VALUES (3, NULL);")?;

        let sum = conn.execute("SELECT SUM(score) FROM test;")?;
        assert_eq!(sum.rows, vec![vec![crate::catalog::Value::Float(30.0)]]);

        let avg = conn.execute("SELECT AVG(score) FROM test;")?;
        assert_eq!(avg.rows, vec![vec![crate::catalog::Value::Float(15.0)]]);

        let min = conn.execute("SELECT MIN(score) FROM test;")?;
        assert_eq!(min.rows, vec![vec![crate::catalog::Value::Float(10.0)]]);

        let max = conn.execute("SELECT MAX(score) FROM test;")?;
        assert_eq!(max.rows, vec![vec![crate::catalog::Value::Float(20.0)]]);

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_group_by_with_count_and_sum() -> Result<()> {
        let db = TestDbFile::new("_test_group_by_with_count_and_sum");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);")?;
        conn.execute("INSERT INTO test (id, name, score) VALUES (1, 'Alice', 10);")?;
        conn.execute("INSERT INTO test (id, name, score) VALUES (2, 'Alice', NULL);")?;
        conn.execute("INSERT INTO test (id, name, score) VALUES (3, 'Bob', 7);")?;
        conn.execute("INSERT INTO test (id, name, score) VALUES (4, 'Cara', NULL);")?;

        let result = conn.execute(
            "SELECT name, COUNT(score) AS score_count, SUM(score) AS total_score \
             FROM test GROUP BY name ORDER BY name ASC;",
        )?;

        assert_eq!(result.columns, vec!["name", "score_count", "total_score"]);
        assert_eq!(
            result.rows,
            vec![
                vec![
                    crate::catalog::Value::Text("Alice".to_string()),
                    crate::catalog::Value::Integer(1),
                    crate::catalog::Value::Integer(10),
                ],
                vec![
                    crate::catalog::Value::Text("Bob".to_string()),
                    crate::catalog::Value::Integer(1),
                    crate::catalog::Value::Integer(7),
                ],
                vec![
                    crate::catalog::Value::Text("Cara".to_string()),
                    crate::catalog::Value::Integer(0),
                    crate::catalog::Value::Null,
                ],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_multiple_aggregates_without_group_by() -> Result<()> {
        let db = TestDbFile::new("_test_multiple_aggregates_without_group_by");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, score INTEGER);")?;
        conn.execute("INSERT INTO test (id, score) VALUES (1, 10);")?;
        conn.execute("INSERT INTO test (id, score) VALUES (2, NULL);")?;
        conn.execute("INSERT INTO test (id, score) VALUES (3, 5);")?;

        let result = conn
            .execute("SELECT COUNT(score) AS score_count, SUM(score) AS total_score FROM test;")?;

        assert_eq!(result.columns, vec!["score_count", "total_score"]);
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Integer(2),
                crate::catalog::Value::Integer(15),
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_group_by_having_uses_aggregate_alias() -> Result<()> {
        let db = TestDbFile::new("_test_group_by_having_uses_aggregate_alias");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (2, 'Alice');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (3, 'Bob');")?;

        let result = conn.execute(
            "SELECT name, COUNT(*) AS total_count \
             FROM test GROUP BY name HAVING total_count > 1 ORDER BY name ASC;",
        )?;

        assert_eq!(result.columns, vec!["name", "total_count"]);
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Text("Alice".to_string()),
                crate::catalog::Value::Integer(2),
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_having_works_for_implicit_single_group() -> Result<()> {
        let db = TestDbFile::new("_test_having_works_for_implicit_single_group");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, score INTEGER);")?;
        conn.execute("INSERT INTO test (id, score) VALUES (1, 10);")?;
        conn.execute("INSERT INTO test (id, score) VALUES (2, NULL);")?;

        let result =
            conn.execute("SELECT COUNT(score) AS score_count FROM test HAVING score_count = 1;")?;
        assert_eq!(result.rows, vec![vec![crate::catalog::Value::Integer(1)]]);

        let filtered_out =
            conn.execute("SELECT COUNT(score) AS score_count FROM test HAVING score_count > 1;")?;
        assert!(filtered_out.rows.is_empty());

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
    fn test_delete_does_not_rewrite_table_storage() -> Result<()> {
        let db = TestDbFile::new("_test_delete_does_not_rewrite_table_storage");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        for i in 1..=40 {
            conn.execute(&format!(
                "INSERT INTO test (id, name) VALUES ({}, 'name{}');",
                i, i
            ))?;
        }
        conn.close()?;

        let size_before_delete = fs::metadata(db.path())?.len();

        let mut conn = Connection::new(db.path())?;
        conn.execute("DELETE FROM test WHERE id = 20;")?;
        conn.close()?;

        let size_after_delete = fs::metadata(db.path())?.len();
        assert_eq!(size_after_delete, size_before_delete);

        let mut conn = Connection::new(db.path())?;
        let result = conn.execute("SELECT * FROM test ORDER BY id;")?;
        assert_eq!(result.rows.len(), 39);
        assert!(!result
            .rows
            .iter()
            .any(|row| row[0] == crate::catalog::Value::Integer(20)));
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
    fn test_repeated_schema_rewrites_reuse_schema_pages() -> Result<()> {
        let db = TestDbFile::new("_test_repeated_schema_rewrites_reuse_schema_pages");

        {
            let mut conn = Connection::new(db.path())?;
            conn.execute("CREATE TABLE seed (id INTEGER PRIMARY KEY);")?;
            conn.close()?;
        }

        let initial_size = fs::metadata(db.path())?.len();

        {
            let mut conn = Connection::new(db.path())?;
            for cycle in 0..5 {
                conn.execute(&format!(
                    "CREATE TABLE t{} (id INTEGER PRIMARY KEY, name TEXT);",
                    cycle
                ))?;
                conn.execute(&format!("DROP TABLE t{};", cycle))?;
            }
            conn.close()?;
        }

        let after_cycles = fs::metadata(db.path())?.len();

        {
            let mut conn = Connection::new(db.path())?;
            conn.execute("CREATE TABLE final_table (id INTEGER PRIMARY KEY);")?;
            conn.close()?;
        }

        let final_size = fs::metadata(db.path())?.len();

        assert!(after_cycles <= initial_size + (crate::storage::PAGE_SIZE as u64 * 2));
        assert!(final_size <= after_cycles + (crate::storage::PAGE_SIZE as u64 * 2));

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
            assert_ne!(root_page, 0);
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
    fn test_hematite_new_in_memory() -> Result<()> {
        let mut db = Hematite::new_in_memory()?;

        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        db.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;

        let result_set = db.query("SELECT * FROM test;")?;
        assert_eq!(result_set.len(), 1);
        let row = result_set.get_row(0).unwrap();
        assert_eq!(row.get_int(0)?, 1);
        assert_eq!(row.get_string(1)?, "Alice");

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
    fn test_hematite_prepare_with_parameters() -> Result<()> {
        let test_db = TestDbFile::new("_test_prepare_with_parameters");
        let mut db = Hematite::new(test_db.path())?;

        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;

        let mut stmt = db.prepare("INSERT INTO test (id, name) VALUES (?, ?);")?;
        stmt.bind_all(vec![
            crate::catalog::Value::Integer(1),
            crate::catalog::Value::Text("Alice".to_string()),
        ])?;

        let result = stmt.execute(&mut db.connection)?;
        assert_eq!(result.affected_rows, 1);

        let rows = db.query("SELECT * FROM test;")?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows.get_row(0).unwrap().get_string(1)?, "Alice");

        Ok(())
    }

    #[test]
    fn test_hematite_transaction() -> Result<()> {
        let test_db = TestDbFile::new("_test_in_memory");
        let mut db = Hematite::new(test_db.path())?;

        // Create table
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY);")?;

        {
            let mut tx = db.transaction()?;
            tx.execute("INSERT INTO test (id) VALUES (1);")?;
            tx.commit()?;
        }

        let rows = db.query("SELECT * FROM test;")?;
        assert_eq!(rows.len(), 1);

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
