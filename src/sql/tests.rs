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
    fn test_mysql_identifier_quoting_and_type_aliases() -> Result<()> {
        let db = TestDbFile::new("_test_mysql_identifier_quoting_and_type_aliases");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE `user data` (`id` INT PRIMARY KEY, `active` BOOL NOT NULL DEFAULT TRUE, `score` DOUBLE, `name` VARCHAR(32));",
        )?;
        conn.execute(
            "INSERT INTO `user data` (`id`, `active`, `score`, `name`) VALUES (1, TRUE, 2.5, 'alice');",
        )?;

        let result = conn.execute(
            "SELECT `id`, `active`, `score`, `name` FROM `user data` WHERE `name` = 'alice';",
        )?;

        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Integer(1),
                crate::catalog::Value::Boolean(true),
                crate::catalog::Value::Float(2.5),
                crate::catalog::Value::Text("alice".to_string()),
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_additional_mysql_type_aliases() -> Result<()> {
        let db = TestDbFile::new("_test_additional_mysql_type_aliases");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE metrics (id BIGINT UNSIGNED PRIMARY KEY, ratio REAL, amount DECIMAL(10, 2), code CHAR(8), tiny TINYINT, small SMALLINT, exact NUMERIC(6));",
        )?;
        conn.execute(
            "INSERT INTO metrics (id, ratio, amount, code, tiny, small, exact) VALUES (1, 1.5, 2.5, 'AB', 3, 4, 5.5);",
        )?;

        let result =
            conn.execute("SELECT id, ratio, amount, code, tiny, small, exact FROM metrics;")?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Integer(1),
                crate::catalog::Value::Float(1.5),
                crate::catalog::Value::Float(2.5),
                crate::catalog::Value::Text("AB".to_string()),
                crate::catalog::Value::Integer(3),
                crate::catalog::Value::Integer(4),
                crate::catalog::Value::Float(5.5),
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_alter_table_rename_to() -> Result<()> {
        let db = TestDbFile::new("_test_alter_table_rename_to");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'alice');")?;
        conn.execute("ALTER TABLE users RENAME TO members;")?;

        let result = conn.execute("SELECT * FROM members;")?;
        assert_eq!(result.rows.len(), 1);
        assert!(conn.execute("SELECT * FROM users;").is_err());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_alter_table_rename_column() -> Result<()> {
        let db = TestDbFile::new("_test_alter_table_rename_column");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'alice');")?;
        conn.execute("ALTER TABLE users RENAME COLUMN name TO full_name;")?;

        let result = conn.execute("SELECT full_name FROM users;")?;
        assert_eq!(
            result.rows,
            vec![vec![crate::catalog::Value::Text("alice".to_string())]]
        );
        assert!(conn.execute("SELECT name FROM users;").is_err());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_check_constraint_rejects_invalid_insert() -> Result<()> {
        let db = TestDbFile::new("_test_check_constraint_rejects_invalid_insert");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, score INTEGER, CHECK (score >= 0));",
        )?;

        let result = conn.execute("INSERT INTO test (id, score) VALUES (1, -1);");
        assert!(result.is_err());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_check_constraint_rejects_invalid_update() -> Result<()> {
        let db = TestDbFile::new("_test_check_constraint_rejects_invalid_update");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, score INTEGER, CHECK (score >= 0));",
        )?;
        conn.execute("INSERT INTO test (id, score) VALUES (1, 1);")?;

        let result = conn.execute("UPDATE test SET score = -1 WHERE id = 1;");
        assert!(result.is_err());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_foreign_key_rejects_missing_parent_on_insert() -> Result<()> {
        let db = TestDbFile::new("_test_foreign_key_rejects_missing_parent_on_insert");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY);")?;
        conn.execute(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id));",
        )?;

        let result = conn.execute("INSERT INTO children (id, parent_id) VALUES (1, 99);");
        assert!(result.is_err());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_foreign_key_rejects_missing_parent_on_update() -> Result<()> {
        let db = TestDbFile::new("_test_foreign_key_rejects_missing_parent_on_update");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY);")?;
        conn.execute(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id));",
        )?;
        conn.execute("INSERT INTO parents (id) VALUES (1);")?;
        conn.execute("INSERT INTO children (id, parent_id) VALUES (1, 1);")?;

        let result = conn.execute("UPDATE children SET parent_id = 2 WHERE id = 1;");
        assert!(result.is_err());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_foreign_key_restricts_parent_delete() -> Result<()> {
        let db = TestDbFile::new("_test_foreign_key_restricts_parent_delete");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY);")?;
        conn.execute(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id));",
        )?;
        conn.execute("INSERT INTO parents (id) VALUES (1);")?;
        conn.execute("INSERT INTO children (id, parent_id) VALUES (1, 1);")?;

        let result = conn.execute("DELETE FROM parents WHERE id = 1;");
        assert!(result.is_err());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_foreign_key_restricts_parent_key_update() -> Result<()> {
        let db = TestDbFile::new("_test_foreign_key_restricts_parent_key_update");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY);")?;
        conn.execute(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id));",
        )?;
        conn.execute("INSERT INTO parents (id) VALUES (1);")?;
        conn.execute("INSERT INTO children (id, parent_id) VALUES (1, 1);")?;

        let result = conn.execute("UPDATE parents SET id = 2 WHERE id = 1;");
        assert!(result.is_err());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_multi_column_foreign_key_rejects_missing_parent() -> Result<()> {
        let db = TestDbFile::new("_test_multi_column_foreign_key_rejects_missing_parent");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE parents (x INTEGER PRIMARY KEY, y INTEGER PRIMARY KEY);")?;
        conn.execute(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_x INTEGER, parent_y INTEGER, FOREIGN KEY (parent_x, parent_y) REFERENCES parents(x, y));",
        )?;

        let err = conn
            .execute("INSERT INTO children (id, parent_x, parent_y) VALUES (1, 10, 20);")
            .unwrap_err();
        assert!(err.to_string().contains("Foreign key constraint"));

        conn.execute("INSERT INTO parents (x, y) VALUES (10, 20);")?;
        conn.execute("INSERT INTO children (id, parent_x, parent_y) VALUES (1, 10, 20);")?;

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_multi_column_foreign_key_restricts_parent_delete() -> Result<()> {
        let db = TestDbFile::new("_test_multi_column_foreign_key_restricts_parent_delete");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE parents (x INTEGER PRIMARY KEY, y INTEGER PRIMARY KEY);")?;
        conn.execute(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_x INTEGER, parent_y INTEGER, FOREIGN KEY (parent_x, parent_y) REFERENCES parents(x, y));",
        )?;
        conn.execute("INSERT INTO parents (x, y) VALUES (10, 20);")?;
        conn.execute("INSERT INTO children (id, parent_x, parent_y) VALUES (1, 10, 20);")?;

        let err = conn
            .execute("DELETE FROM parents WHERE x = 10 AND y = 20;")
            .unwrap_err();
        assert!(err.to_string().contains("still references"));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_multi_column_foreign_key_cascades_parent_update() -> Result<()> {
        let db = TestDbFile::new("_test_multi_column_foreign_key_cascades_parent_update");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE parents (x INTEGER PRIMARY KEY, y INTEGER PRIMARY KEY);")?;
        conn.execute(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_x INTEGER, parent_y INTEGER, FOREIGN KEY (parent_x, parent_y) REFERENCES parents(x, y) ON UPDATE CASCADE);",
        )?;
        conn.execute("INSERT INTO parents (x, y) VALUES (10, 20);")?;
        conn.execute("INSERT INTO children (id, parent_x, parent_y) VALUES (1, 10, 20);")?;

        conn.execute("UPDATE parents SET x = 11, y = 21 WHERE x = 10 AND y = 20;")?;
        let result = conn.execute("SELECT parent_x, parent_y FROM children WHERE id = 1;")?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Integer(11),
                crate::catalog::Value::Integer(21),
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_multi_column_foreign_key_sets_null_on_parent_delete() -> Result<()> {
        let db = TestDbFile::new("_test_multi_column_foreign_key_sets_null_on_parent_delete");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE parents (x INTEGER PRIMARY KEY, y INTEGER PRIMARY KEY);")?;
        conn.execute(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_x INTEGER, parent_y INTEGER, FOREIGN KEY (parent_x, parent_y) REFERENCES parents(x, y) ON DELETE SET NULL);",
        )?;
        conn.execute("INSERT INTO parents (x, y) VALUES (10, 20);")?;
        conn.execute("INSERT INTO children (id, parent_x, parent_y) VALUES (1, 10, 20);")?;

        conn.execute("DELETE FROM parents WHERE x = 10 AND y = 20;")?;
        let result = conn.execute("SELECT parent_x, parent_y FROM children WHERE id = 1;")?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Null,
                crate::catalog::Value::Null,
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_rename_column_rewrites_check_constraints() -> Result<()> {
        let db = TestDbFile::new("_test_rename_column_rewrites_check_constraints");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, CHECK (name != ''));",
        )?;
        conn.execute("ALTER TABLE users RENAME COLUMN name TO full_name;")?;
        conn.execute("INSERT INTO users (id, full_name) VALUES (1, 'alice');")?;

        let result = conn.execute("INSERT INTO users (id, full_name) VALUES (2, '');");
        assert!(result.is_err());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_rename_column_rewrites_referenced_parent_column() -> Result<()> {
        let db = TestDbFile::new("_test_rename_column_rewrites_referenced_parent_column");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY);")?;
        conn.execute(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id));",
        )?;
        conn.execute("INSERT INTO parents (id) VALUES (1);")?;
        conn.execute("ALTER TABLE parents RENAME COLUMN id TO parent_id;")?;
        conn.execute("INSERT INTO children (id, parent_id) VALUES (1, 1);")?;

        let result = conn.execute("INSERT INTO children (id, parent_id) VALUES (2, 99);");
        assert!(result.is_err());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_alter_table_add_column_rejects_check_constraint() -> Result<()> {
        let db = TestDbFile::new("_test_alter_table_add_column_rejects_check_constraint");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY);")?;

        let result = conn.execute("ALTER TABLE users ADD COLUMN score INTEGER CHECK (score >= 0);");
        assert!(result.is_err());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_alter_table_add_column_preserves_existing_rows() -> Result<()> {
        let db = TestDbFile::new("_test_alter_table_add_column_preserves_existing_rows");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'alice');")?;
        conn.execute("ALTER TABLE users ADD COLUMN active BOOL NOT NULL DEFAULT TRUE;")?;

        let result = conn.execute("SELECT * FROM users;")?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Integer(1),
                crate::catalog::Value::Text("alice".to_string()),
                crate::catalog::Value::Boolean(true),
            ]]
        );

        conn.execute("INSERT INTO users (id, name) VALUES (2, 'bob');")?;
        let result = conn.execute("SELECT active FROM users WHERE id = 2;")?;
        assert_eq!(
            result.rows,
            vec![vec![crate::catalog::Value::Boolean(true)]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_create_table_unique_column_rejects_duplicate_insert_and_update() -> Result<()> {
        let db = TestDbFile::new("_test_create_table_unique_column");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT);")?;
        conn.execute("INSERT INTO users (id, email, name) VALUES (1, 'a@example.com', 'alice');")?;
        conn.execute("INSERT INTO users (id, email, name) VALUES (2, 'b@example.com', 'bob');")?;

        let insert_err = conn
            .execute("INSERT INTO users (id, email, name) VALUES (3, 'a@example.com', 'cara');")
            .unwrap_err();
        assert!(insert_err.to_string().contains("UNIQUE index"));

        let update_err = conn
            .execute("UPDATE users SET email = 'a@example.com' WHERE id = 2;")
            .unwrap_err();
        assert!(update_err.to_string().contains("UNIQUE index"));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_create_unique_index_rejects_duplicate_existing_rows() -> Result<()> {
        let db = TestDbFile::new("_test_create_unique_index_rejects_duplicate_existing_rows");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);")?;
        conn.execute("INSERT INTO users (id, email) VALUES (1, 'a@example.com');")?;
        conn.execute("INSERT INTO users (id, email) VALUES (2, 'a@example.com');")?;

        let err = conn
            .execute("CREATE UNIQUE INDEX idx_users_email ON users (email);")
            .unwrap_err();
        assert!(err.to_string().contains("Duplicate value"));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_create_unique_index_blocks_future_duplicate_insert() -> Result<()> {
        let db = TestDbFile::new("_test_create_unique_index_blocks_future_duplicate_insert");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);")?;
        conn.execute("INSERT INTO users (id, email) VALUES (1, 'a@example.com');")?;
        conn.execute("CREATE UNIQUE INDEX idx_users_email ON users (email);")?;

        let err = conn
            .execute("INSERT INTO users (id, email) VALUES (2, 'a@example.com');")
            .unwrap_err();
        assert!(err.to_string().contains("UNIQUE index"));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_table_level_multi_column_unique_rejects_duplicate_insert_and_update() -> Result<()> {
        let db = TestDbFile::new("_test_table_level_multi_column_unique");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE memberships (id INTEGER PRIMARY KEY, user_id INTEGER, org_id INTEGER, role TEXT, CONSTRAINT uq_membership UNIQUE (user_id, org_id));",
        )?;
        conn.execute(
            "INSERT INTO memberships (id, user_id, org_id, role) VALUES (1, 10, 20, 'owner');",
        )?;
        conn.execute(
            "INSERT INTO memberships (id, user_id, org_id, role) VALUES (2, 10, 21, 'member');",
        )?;

        let insert_err = conn
            .execute(
                "INSERT INTO memberships (id, user_id, org_id, role) VALUES (3, 10, 20, 'viewer');",
            )
            .unwrap_err();
        assert!(insert_err.to_string().contains("UNIQUE index"));

        let update_err = conn
            .execute("UPDATE memberships SET org_id = 20 WHERE id = 2;")
            .unwrap_err();
        assert!(update_err.to_string().contains("UNIQUE index"));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_composite_primary_key_lookup_uses_conjunctive_predicates() -> Result<()> {
        let db = TestDbFile::new("_test_composite_primary_key_lookup");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE edges (src INTEGER PRIMARY KEY, dst INTEGER PRIMARY KEY, weight INTEGER);",
        )?;
        conn.execute("INSERT INTO edges (src, dst, weight) VALUES (1, 2, 7);")?;
        conn.execute("INSERT INTO edges (src, dst, weight) VALUES (1, 3, 9);")?;

        let result = conn.execute("SELECT weight FROM edges WHERE src = 1 AND dst = 2;")?;
        assert_eq!(result.rows, vec![vec![crate::catalog::Value::Integer(7)]]);

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_composite_unique_index_lookup_uses_conjunctive_predicates() -> Result<()> {
        let db = TestDbFile::new("_test_composite_unique_index_lookup");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE memberships (id INTEGER PRIMARY KEY, user_id INTEGER, org_id INTEGER, role TEXT, CONSTRAINT uq_membership UNIQUE (user_id, org_id));",
        )?;
        conn.execute(
            "INSERT INTO memberships (id, user_id, org_id, role) VALUES (1, 10, 20, 'owner');",
        )?;
        conn.execute(
            "INSERT INTO memberships (id, user_id, org_id, role) VALUES (2, 10, 21, 'member');",
        )?;

        let result =
            conn.execute("SELECT role FROM memberships WHERE user_id = 10 AND org_id = 20;")?;
        assert_eq!(
            result.rows,
            vec![vec![crate::catalog::Value::Text("owner".to_string())]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_alter_table_add_column_requires_nullable_or_default() -> Result<()> {
        let db = TestDbFile::new("_test_alter_table_add_column_requires_nullable_or_default");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;

        let err = conn
            .execute("ALTER TABLE users ADD COLUMN active BOOL NOT NULL;")
            .unwrap_err();
        assert!(err.to_string().contains("nullable or have a DEFAULT"));

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
    fn test_if_exists_modifiers_are_noops() -> Result<()> {
        let db = TestDbFile::new("_test_if_exists_modifiers_are_noops");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_users_name ON users (name);")?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_users_name ON users (name);")?;
        conn.execute("DROP INDEX IF EXISTS missing_idx ON users;")?;
        conn.execute("DROP TABLE IF EXISTS missing_table;")?;

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
    fn test_select_with_cross_join() -> Result<()> {
        let db = TestDbFile::new("_test_select_with_cross_join");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);")?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO users (id, name) VALUES (2, 'Bob');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (10, 1, 'First');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (11, 2, 'Second');")?;

        let result = conn.execute(
            "SELECT u.name, p.title FROM users AS u, posts AS p ORDER BY u.name ASC, p.title ASC;",
        )?;

        assert_eq!(result.columns, vec!["name", "title"]);
        assert_eq!(result.rows.len(), 4);
        assert_eq!(
            result.rows[0],
            vec![
                crate::catalog::Value::Text("Alice".to_string()),
                crate::catalog::Value::Text("First".to_string()),
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_select_with_inner_join() -> Result<()> {
        let db = TestDbFile::new("_test_select_with_inner_join");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);")?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO users (id, name) VALUES (2, 'Bob');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (10, 1, 'First');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (11, 1, 'Second');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (12, 3, 'Orphan');")?;

        let result = conn.execute(
            "SELECT u.name, p.title FROM users AS u INNER JOIN posts AS p ON u.id = p.user_id ORDER BY u.name ASC, p.title ASC;",
        )?;

        assert_eq!(result.columns, vec!["name", "title"]);
        assert_eq!(
            result.rows,
            vec![
                vec![
                    crate::catalog::Value::Text("Alice".to_string()),
                    crate::catalog::Value::Text("First".to_string()),
                ],
                vec![
                    crate::catalog::Value::Text("Alice".to_string()),
                    crate::catalog::Value::Text("Second".to_string()),
                ],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_join_rejects_ambiguous_unqualified_columns() -> Result<()> {
        let db = TestDbFile::new("_test_join_rejects_ambiguous_unqualified_columns");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);")?;

        let error = conn
            .execute("SELECT id FROM users AS u INNER JOIN posts AS p ON u.id = p.user_id;")
            .expect_err("ambiguous unqualified join column should be rejected");

        assert!(format!("{}", error).contains("ambiguous"));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_group_by_over_joined_rows() -> Result<()> {
        let db = TestDbFile::new("_test_group_by_over_joined_rows");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);")?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO users (id, name) VALUES (2, 'Bob');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (10, 1, 'First');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (11, 1, 'Second');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (12, 2, 'Third');")?;

        let result = conn.execute(
            "SELECT u.name, COUNT(p.id) AS post_count \
             FROM users AS u INNER JOIN posts AS p ON u.id = p.user_id \
             GROUP BY u.name HAVING COUNT(p.id) >= 1 ORDER BY u.name ASC;",
        )?;

        assert_eq!(result.columns, vec!["name", "post_count"]);
        assert_eq!(
            result.rows,
            vec![
                vec![
                    crate::catalog::Value::Text("Alice".to_string()),
                    crate::catalog::Value::Integer(2),
                ],
                vec![
                    crate::catalog::Value::Text("Bob".to_string()),
                    crate::catalog::Value::Integer(1),
                ],
            ]
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
    fn test_where_in_subquery_and_exists() -> Result<()> {
        let db = TestDbFile::new("_test_where_in_subquery_and_exists");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);")?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO users (id, name) VALUES (2, 'Bob');")?;
        conn.execute("INSERT INTO users (id, name) VALUES (3, 'Cara');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (10, 1, 'First');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (11, 1, 'Second');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (12, 3, 'Third');")?;

        let in_result = conn.execute(
            "SELECT id FROM users WHERE id IN (SELECT user_id FROM posts) ORDER BY id ASC;",
        )?;
        assert_eq!(
            in_result.rows,
            vec![
                vec![crate::catalog::Value::Integer(1)],
                vec![crate::catalog::Value::Integer(3)],
            ]
        );

        let not_in_result = conn.execute(
            "SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM posts) ORDER BY id ASC;",
        )?;
        assert_eq!(
            not_in_result.rows,
            vec![vec![crate::catalog::Value::Integer(2)]]
        );

        let exists_result = conn.execute(
            "SELECT id FROM users WHERE EXISTS (SELECT user_id FROM posts WHERE user_id = 1) ORDER BY id ASC;",
        )?;
        assert_eq!(
            exists_result.rows,
            vec![
                vec![crate::catalog::Value::Integer(1)],
                vec![crate::catalog::Value::Integer(2)],
                vec![crate::catalog::Value::Integer(3)],
            ]
        );

        let not_exists_result = conn.execute(
            "SELECT id FROM users WHERE NOT EXISTS (SELECT user_id FROM posts WHERE user_id = 99) ORDER BY id ASC;",
        )?;
        assert_eq!(
            not_exists_result.rows,
            vec![
                vec![crate::catalog::Value::Integer(1)],
                vec![crate::catalog::Value::Integer(2)],
                vec![crate::catalog::Value::Integer(3)],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_select_union_and_union_all() -> Result<()> {
        let db = TestDbFile::new("_test_select_union_and_union_all");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);")?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO users (id, name) VALUES (2, 'Bob');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (10, 1, 'First');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (11, 2, 'Second');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (12, 2, 'Third');")?;

        let union_result = conn.execute("SELECT id FROM users UNION SELECT user_id FROM posts;")?;
        assert_eq!(union_result.columns, vec!["id"]);
        assert_eq!(
            union_result.rows,
            vec![
                vec![crate::catalog::Value::Integer(1)],
                vec![crate::catalog::Value::Integer(2)],
            ]
        );

        let union_all_result =
            conn.execute("SELECT id FROM users UNION ALL SELECT user_id FROM posts;")?;
        assert_eq!(
            union_all_result.rows,
            vec![
                vec![crate::catalog::Value::Integer(1)],
                vec![crate::catalog::Value::Integer(2)],
                vec![crate::catalog::Value::Integer(1)],
                vec![crate::catalog::Value::Integer(2)],
                vec![crate::catalog::Value::Integer(2)],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_select_from_derived_table() -> Result<()> {
        let db = TestDbFile::new("_test_select_from_derived_table");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (10, 1, 'First');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (11, 2, 'Second');")?;

        let result = conn.execute(
            "SELECT p.user_id FROM (SELECT user_id FROM posts) AS p ORDER BY p.user_id ASC;",
        )?;

        assert_eq!(result.columns, vec!["user_id"]);
        assert_eq!(
            result.rows,
            vec![
                vec![crate::catalog::Value::Integer(1)],
                vec![crate::catalog::Value::Integer(2)],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_select_from_cte() -> Result<()> {
        let db = TestDbFile::new("_test_select_from_cte");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (10, 1, 'First');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (11, 2, 'Second');")?;

        let result = conn.execute(
            "WITH post_users AS (SELECT user_id FROM posts) \
             SELECT post_users.user_id FROM post_users ORDER BY post_users.user_id ASC;",
        )?;

        assert_eq!(result.columns, vec!["user_id"]);
        assert_eq!(
            result.rows,
            vec![
                vec![crate::catalog::Value::Integer(1)],
                vec![crate::catalog::Value::Integer(2)],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_select_with_left_join() -> Result<()> {
        let db = TestDbFile::new("_test_select_with_left_join");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);")?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO users (id, name) VALUES (2, 'Bob');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (10, 1, 'First');")?;

        let result = conn.execute(
            "SELECT u.name, p.title FROM users u LEFT JOIN posts p ON u.id = p.user_id ORDER BY u.name ASC;",
        )?;

        assert_eq!(result.columns, vec!["name", "title"]);
        assert_eq!(
            result.rows,
            vec![
                vec![
                    crate::catalog::Value::Text("Alice".to_string()),
                    crate::catalog::Value::Text("First".to_string()),
                ],
                vec![
                    crate::catalog::Value::Text("Bob".to_string()),
                    crate::catalog::Value::Null,
                ],
            ]
        );

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
    fn test_having_supports_raw_aggregate_calls() -> Result<()> {
        let db = TestDbFile::new("_test_having_supports_raw_aggregate_calls");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);")?;
        conn.execute("INSERT INTO test (id, name, score) VALUES (1, 'Alice', 10);")?;
        conn.execute("INSERT INTO test (id, name, score) VALUES (2, 'Alice', 2);")?;
        conn.execute("INSERT INTO test (id, name, score) VALUES (3, 'Bob', 3);")?;

        let grouped = conn.execute(
            "SELECT name FROM test GROUP BY name HAVING COUNT(*) > 1 ORDER BY name ASC;",
        )?;
        assert_eq!(
            grouped.rows,
            vec![vec![crate::catalog::Value::Text("Alice".to_string())]]
        );

        let implicit =
            conn.execute("SELECT COUNT(*) AS total_rows FROM test HAVING SUM(score) > 10;")?;
        assert_eq!(implicit.rows, vec![vec![crate::catalog::Value::Integer(3)]]);

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
