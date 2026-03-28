//! Centralized tests for the sql module

mod connection_tests {
    use crate::catalog::{DataType, JournalMode, TimeValue, TimeWithTimeZoneValue, TimestampValue};
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
    fn test_additional_temporal_binary_and_enum_types_round_trip() -> Result<()> {
        let db = TestDbFile::new("_test_additional_temporal_binary_and_enum_types_round_trip");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE typed (\
                id INTEGER PRIMARY KEY,\
                at TIME,\
                stamped TIMESTAMP,\
                zone_time TIME WITH TIME ZONE,\
                code BINARY(4),\
                bytes VARBINARY(8),\
                state ENUM('draft', 'live')\
            );",
        )?;
        conn.execute(
            "INSERT INTO typed (id, at, stamped, zone_time, code, bytes, state) \
             VALUES (1, '10:11:12', '2026-03-28 13:14:15', '10:11:12+03:00', 'AB', 'xyz', 'live');",
        )?;

        let result = conn.execute(
            "SELECT at, stamped, zone_time, code, bytes, state FROM typed WHERE id = 1;",
        )?;
        let row = crate::sql::result::Row::new(result.rows[0].clone());
        assert_eq!(row.get_time(0)?, TimeValue::parse("10:11:12")?);
        assert_eq!(
            row.get_timestamp(1)?,
            TimestampValue::parse("2026-03-28 13:14:15")?
        );
        assert_eq!(
            row.get_time_with_time_zone(2)?,
            TimeWithTimeZoneValue::parse("10:11:12+03:00")?
        );
        assert_eq!(row.get_blob(3)?, vec![b'A', b'B', 0, 0]);
        assert_eq!(row.get_blob(4)?, b"xyz".to_vec());
        assert_eq!(row.get_string(5)?, "live");

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_temporal_scalar_functions_and_arithmetic() -> Result<()> {
        let db = TestDbFile::new("_test_temporal_scalar_functions_and_arithmetic");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE typed (\
                id INTEGER PRIMARY KEY,\
                event_date DATE,\
                at TIME,\
                created_at DATETIME,\
                stamped TIMESTAMP,\
                zone_time TIME WITH TIME ZONE\
            );",
        )?;
        conn.execute(
            "INSERT INTO typed (id, event_date, at, created_at, stamped, zone_time) \
             VALUES (1, '2026-03-28', '10:11:12', '2026-03-28 13:14:15', '2026-03-28 13:14:15', '10:11:12+03:00');",
        )?;

        let result = conn.execute(
            "SELECT \
                DATE(created_at), \
                TIME(created_at), \
                YEAR(event_date), \
                MONTH(event_date), \
                DAY(event_date), \
                HOUR(at), \
                MINUTE(at), \
                SECOND(at), \
                TIME_TO_SEC(at), \
                SEC_TO_TIME(3661), \
                UNIX_TIMESTAMP(stamped), \
                event_date + 2, \
                created_at + 45, \
                stamped - 15, \
                at + 120, \
                zone_time + 60, \
                DATE('2026-03-29 01:02:03'), \
                TIME('2026-03-29 01:02:03') \
             FROM typed WHERE id = 1;",
        )?;

        let row = crate::sql::result::Row::new(result.rows[0].clone());
        assert_eq!(row.get_date(0)?.to_string(), "2026-03-28");
        assert_eq!(row.get_time(1)?.to_string(), "13:14:15");
        assert_eq!(row.get_int(2)?, 2026);
        assert_eq!(row.get_int(3)?, 3);
        assert_eq!(row.get_int(4)?, 28);
        assert_eq!(row.get_int(5)?, 10);
        assert_eq!(row.get_int(6)?, 11);
        assert_eq!(row.get_int(7)?, 12);
        assert_eq!(row.get_bigint(8)?, 36_672);
        assert_eq!(row.get_time(9)?.to_string(), "01:01:01");
        assert_eq!(
            row.get_bigint(10)?,
            TimestampValue::parse("2026-03-28 13:14:15")?.seconds_since_epoch()
        );
        assert_eq!(row.get_date(11)?.to_string(), "2026-03-30");
        assert_eq!(row.get_datetime(12)?.to_string(), "2026-03-28 13:15:00");
        assert_eq!(row.get_timestamp(13)?.to_string(), "2026-03-28 13:14:00");
        assert_eq!(row.get_time(14)?.to_string(), "10:13:12");
        assert_eq!(
            row.get_time_with_time_zone(15)?.to_string(),
            "10:12:12+03:00"
        );
        assert_eq!(row.get_date(16)?.to_string(), "2026-03-29");
        assert_eq!(row.get_time(17)?.to_string(), "01:02:03");

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
    fn test_explain_describe_and_show_tables() -> Result<()> {
        let db = TestDbFile::new("_test_explain_describe_show_tables");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE);",
        )?;

        let explain = conn.execute("EXPLAIN SELECT * FROM users WHERE id = 1;")?;
        assert_eq!(explain.columns, vec!["kind", "detail"]);
        assert!(!explain.rows.is_empty());

        let describe = conn.execute("DESCRIBE users;")?;
        assert_eq!(
            describe.columns,
            vec![
                "column",
                "type",
                "nullable",
                "default",
                "primary_key",
                "unique",
                "auto_increment",
            ]
        );
        assert_eq!(describe.rows.len(), 3);

        let show_tables = conn.execute("SHOW TABLES;")?;
        assert_eq!(show_tables.columns, vec!["table_name"]);
        assert_eq!(
            show_tables.rows,
            vec![vec![crate::catalog::Value::Text("users".to_string())]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_create_drop_and_show_views() -> Result<()> {
        let db = TestDbFile::new("_test_create_drop_and_show_views");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE VIEW user_names AS SELECT name FROM users;")?;

        let show_views = conn.execute("SHOW VIEWS;")?;
        assert_eq!(show_views.columns, vec!["view_name"]);
        assert_eq!(
            show_views.rows,
            vec![vec![crate::catalog::Value::Text("user_names".to_string())]]
        );

        conn.execute("DROP VIEW user_names;")?;
        let show_views = conn.execute("SHOW VIEWS;")?;
        assert!(show_views.rows.is_empty());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_create_view_persists_across_reopen_and_blocks_base_drop() -> Result<()> {
        let db = TestDbFile::new("_test_create_view_persists_across_reopen_and_blocks_base_drop");

        {
            let mut conn = Connection::new(db.path())?;
            conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
            conn.execute("CREATE VIEW user_names AS SELECT name FROM users;")?;
            conn.close()?;
        }

        let mut reopened = Connection::new(db.path())?;
        let show_views = reopened.execute("SHOW VIEWS;")?;
        assert_eq!(
            show_views.rows,
            vec![vec![crate::catalog::Value::Text("user_names".to_string())]]
        );

        let drop_result = reopened.execute("DROP TABLE users;");
        assert!(drop_result.is_err());
        assert!(drop_result
            .unwrap_err()
            .to_string()
            .contains("depends on it"));

        reopened.close()?;
        Ok(())
    }

    #[test]
    fn test_create_view_rejects_direct_self_reference() -> Result<()> {
        let db = TestDbFile::new("_test_create_view_rejects_direct_self_reference");
        let mut conn = Connection::new(db.path())?;

        let result = conn.execute("CREATE VIEW user_names AS SELECT * FROM user_names;");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot depend on itself"));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_drop_view_rejects_dependent_view() -> Result<()> {
        let db = TestDbFile::new("_test_drop_view_rejects_dependent_view");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE VIEW user_names AS SELECT id, name FROM users;")?;
        conn.execute("CREATE VIEW user_ids AS SELECT id FROM user_names;")?;

        let result = conn.execute("DROP VIEW user_names;");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("depends on it"));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_select_from_view() -> Result<()> {
        let db = TestDbFile::new("_test_select_from_view");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN);")?;
        conn.execute("INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Bob', FALSE);")?;
        conn.execute(
            "CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = TRUE;",
        )?;

        let result = conn.execute("SELECT id, name FROM active_users ORDER BY id ASC;")?;
        assert_eq!(result.columns, vec!["id", "name"]);
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Integer(1),
                crate::catalog::Value::Text("Ada".to_string()),
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_select_from_nested_view_and_joined_view() -> Result<()> {
        let db = TestDbFile::new("_test_select_from_nested_view_and_joined_view");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);")?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'Ada'), (2, 'Bob');")?;
        conn.execute(
            "INSERT INTO posts (id, user_id, title) VALUES (10, 1, 'Intro'), (11, 1, 'Rust'), (12, 2, 'SQL');",
        )?;
        conn.execute("CREATE VIEW user_names AS SELECT id, name FROM users;")?;
        conn.execute("CREATE VIEW post_counts AS SELECT user_id, COUNT(*) AS total FROM posts GROUP BY user_id;")?;

        let result = conn.execute(
            "SELECT u.name, p.total \
             FROM user_names u INNER JOIN post_counts p ON u.id = p.user_id \
             ORDER BY u.name ASC;",
        )?;
        assert_eq!(result.columns, vec!["name", "total"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], crate::catalog::Value::Text("Ada".to_string()));
        assert_eq!(result.rows[1][0], crate::catalog::Value::Text("Bob".to_string()));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_grouped_query_over_view() -> Result<()> {
        let db = TestDbFile::new("_test_grouped_query_over_view");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);")?;
        conn.execute(
            "INSERT INTO posts (id, user_id, title) VALUES (10, 1, 'Intro'), (11, 1, 'Rust'), (12, 2, 'SQL');",
        )?;
        conn.execute("CREATE VIEW post_counts AS SELECT user_id, COUNT(*) AS total FROM posts GROUP BY user_id;")?;

        let result = conn.execute(
            "SELECT user_id, total FROM post_counts WHERE total >= 1 ORDER BY total DESC, user_id ASC;",
        )?;

        assert_eq!(result.columns, vec!["user_id", "total"]);
        assert_eq!(
            result.rows,
            vec![
                vec![crate::catalog::Value::Integer(1), crate::catalog::Value::Integer(2)],
                vec![crate::catalog::Value::Integer(2), crate::catalog::Value::Integer(1)],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_view_in_cte_and_subquery() -> Result<()> {
        let db = TestDbFile::new("_test_view_in_cte_and_subquery");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN);")?;
        conn.execute(
            "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Bob', FALSE), (3, 'Cara', TRUE);",
        )?;
        conn.execute("CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = TRUE;")?;

        let cte_result = conn.execute(
            "WITH named_users AS (SELECT id, name FROM active_users) \
             SELECT name FROM named_users ORDER BY id ASC;",
        )?;
        assert_eq!(cte_result.columns, vec!["name"]);
        assert_eq!(
            cte_result.rows,
            vec![
                vec![crate::catalog::Value::Text("Ada".to_string())],
                vec![crate::catalog::Value::Text("Cara".to_string())],
            ]
        );

        let subquery_result = conn.execute(
            "SELECT name FROM (SELECT id, name FROM active_users) filtered WHERE id > 1 ORDER BY id ASC;",
        )?;
        assert_eq!(subquery_result.columns, vec!["name"]);
        assert_eq!(
            subquery_result.rows,
            vec![vec![crate::catalog::Value::Text("Cara".to_string())]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_explain_select_from_view() -> Result<()> {
        let db = TestDbFile::new("_test_explain_select_from_view");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN);")?;
        conn.execute("CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = TRUE;")?;

        let explain = conn.execute("EXPLAIN SELECT id, name FROM active_users WHERE id = 1;")?;
        assert_eq!(explain.columns, vec!["kind", "detail"]);
        assert!(!explain.rows.is_empty());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_create_drop_trigger_persists_across_reopen() -> Result<()> {
        let db = TestDbFile::new("_test_create_drop_trigger_persists_across_reopen");

        {
            let mut conn = Connection::new(db.path())?;
            conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
            conn.execute("CREATE TABLE audit_log (id INTEGER PRIMARY KEY, entry TEXT);")?;
            conn.execute(
                "CREATE TRIGGER audit_users AFTER INSERT ON users AS INSERT INTO audit_log (id, entry) VALUES (1, NEW.name);",
            )?;
            assert!(conn.schema_snapshot()?.trigger("audit_users").is_some());
            conn.close()?;
        }

        {
            let mut reopened = Connection::new(db.path())?;
            let trigger = reopened
                .schema_snapshot()?
                .trigger("audit_users")
                .cloned()
                .expect("trigger should persist across reopen");
            assert_eq!(trigger.table_name, "users");
            assert_eq!(trigger.body_sql, "INSERT INTO audit_log (id, entry) VALUES (1, NEW.name)");

            reopened.execute("DROP TRIGGER audit_users;")?;
            assert!(reopened.schema_snapshot()?.trigger("audit_users").is_none());
            reopened.close()?;
        }

        Ok(())
    }

    #[test]
    fn test_show_indexes_triggers_and_create_statements() -> Result<()> {
        let db = TestDbFile::new("_test_show_indexes_triggers_and_create_statements");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, org_id INTEGER);",
        )?;
        conn.execute("CREATE INDEX idx_users_org ON users (org_id);")?;
        conn.execute("CREATE VIEW user_emails AS SELECT id, email FROM users;")?;
        conn.execute("CREATE TABLE audit_log (id INTEGER PRIMARY KEY, entry TEXT);")?;
        conn.execute(
            "CREATE TRIGGER audit_users AFTER INSERT ON users AS INSERT INTO audit_log (id, entry) VALUES (1, NEW.email);",
        )?;

        let indexes = conn.execute("SHOW INDEXES FROM users;")?;
        assert_eq!(
            indexes.columns,
            vec!["table_name", "index_name", "unique", "columns"]
        );
        assert!(indexes.rows.iter().any(|row| {
            row[1] == crate::catalog::Value::Text("idx_users_org".to_string())
        }));

        let triggers = conn.execute("SHOW TRIGGERS FROM users;")?;
        assert_eq!(
            triggers.columns,
            vec!["trigger_name", "table_name", "event"]
        );
        assert_eq!(
            triggers.rows,
            vec![vec![
                crate::catalog::Value::Text("audit_users".to_string()),
                crate::catalog::Value::Text("users".to_string()),
                crate::catalog::Value::Text("INSERT".to_string()),
            ]]
        );

        let show_create_table = conn.execute("SHOW CREATE TABLE users;")?;
        assert_eq!(
            show_create_table.columns,
            vec!["table_name", "create_sql"]
        );
        let table_sql = match &show_create_table.rows[0][1] {
            crate::catalog::Value::Text(sql) => sql,
            other => panic!("expected create sql text, found {other:?}"),
        };
        assert!(table_sql.contains("CREATE TABLE users"));
        assert!(table_sql.contains("UNIQUE"));

        let show_create_view = conn.execute("SHOW CREATE VIEW user_emails;")?;
        assert_eq!(show_create_view.columns, vec!["view_name", "create_sql"]);
        assert_eq!(
            show_create_view.rows,
            vec![vec![
                crate::catalog::Value::Text("user_emails".to_string()),
                crate::catalog::Value::Text(
                    "CREATE VIEW user_emails AS SELECT id, email FROM users".to_string()
                ),
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_trigger_validation_rejects_invalid_old_new_usage() -> Result<()> {
        let db = TestDbFile::new("_test_trigger_validation_rejects_invalid_old_new_usage");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE audit_log (id INTEGER PRIMARY KEY, entry TEXT);")?;

        let insert_result = conn.execute(
            "CREATE TRIGGER bad_insert AFTER INSERT ON users AS INSERT INTO audit_log (id, entry) VALUES (1, OLD.name);",
        );
        assert!(insert_result.is_err());
        assert!(insert_result.unwrap_err().to_string().contains("cannot reference OLD"));

        let delete_result = conn.execute(
            "CREATE TRIGGER bad_delete AFTER DELETE ON users AS INSERT INTO audit_log (id, entry) VALUES (1, NEW.name);",
        );
        assert!(delete_result.is_err());
        assert!(delete_result.unwrap_err().to_string().contains("cannot reference NEW"));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_trigger_validation_rejects_self_targeting_body() -> Result<()> {
        let db = TestDbFile::new("_test_trigger_validation_rejects_self_targeting_body");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;

        let result = conn.execute(
            "CREATE TRIGGER audit_users AFTER INSERT ON users AS INSERT INTO users (id, name) VALUES (2, NEW.name);",
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot target its own table"));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_insert_update_delete_triggers_fire_with_old_and_new_values() -> Result<()> {
        let db = TestDbFile::new("_test_insert_update_delete_triggers_fire_with_old_and_new_values");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE audit_log (id INTEGER PRIMARY KEY, entry TEXT);")?;
        conn.execute(
            "CREATE TRIGGER audit_insert AFTER INSERT ON users AS INSERT INTO audit_log (id, entry) VALUES (NEW.id, NEW.name);",
        )?;
        conn.execute(
            "CREATE TRIGGER audit_update AFTER UPDATE ON users AS INSERT INTO audit_log (id, entry) VALUES (NEW.id + 10, OLD.name);",
        )?;
        conn.execute(
            "CREATE TRIGGER audit_delete AFTER DELETE ON users AS INSERT INTO audit_log (id, entry) VALUES (OLD.id + 20, OLD.name);",
        )?;

        conn.execute("INSERT INTO users (id, name) VALUES (1, 'Ada');")?;
        conn.execute("UPDATE users SET name = 'Ada Lovelace' WHERE id = 1;")?;
        conn.execute("DELETE FROM users WHERE id = 1;")?;

        let audit = conn.execute("SELECT id, entry FROM audit_log ORDER BY id ASC;")?;
        assert_eq!(
            audit.rows,
            vec![
                vec![
                    crate::catalog::Value::Integer(1),
                    crate::catalog::Value::Text("Ada".to_string()),
                ],
                vec![
                    crate::catalog::Value::Integer(11),
                    crate::catalog::Value::Text("Ada".to_string()),
                ],
                vec![
                    crate::catalog::Value::Integer(21),
                    crate::catalog::Value::Text("Ada Lovelace".to_string()),
                ],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_trigger_failure_aborts_outer_statement() -> Result<()> {
        let db = TestDbFile::new("_test_trigger_failure_aborts_outer_statement");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE audit_log (id INTEGER PRIMARY KEY, entry TEXT);")?;
        conn.execute(
            "CREATE TRIGGER audit_insert AFTER INSERT ON users AS INSERT INTO audit_log (id, entry) VALUES (1, NEW.name);",
        )?;

        conn.execute("INSERT INTO users (id, name) VALUES (1, 'Ada');")?;
        let result = conn.execute("INSERT INTO users (id, name) VALUES (2, 'Bob');");
        assert!(result.is_err());

        let users = conn.execute("SELECT id, name FROM users ORDER BY id ASC;")?;
        assert_eq!(
            users.rows,
            vec![vec![
                crate::catalog::Value::Integer(1),
                crate::catalog::Value::Text("Ada".to_string()),
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    #[ignore = "trigger side effects do not yet roll back with savepoint snapshots"]
    fn test_trigger_effects_rollback_with_savepoint() -> Result<()> {
        let db = TestDbFile::new("_test_trigger_effects_rollback_with_savepoint");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE audit_log (id INTEGER PRIMARY KEY, entry TEXT);")?;
        conn.execute(
            "CREATE TRIGGER audit_insert AFTER INSERT ON users AS INSERT INTO audit_log (id, entry) VALUES (NEW.id, NEW.name);",
        )?;

        conn.execute("BEGIN;")?;
        conn.execute("SAVEPOINT before_insert;")?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'Ada');")?;
        conn.execute("ROLLBACK TO SAVEPOINT before_insert;")?;
        conn.execute("COMMIT;")?;

        let users = conn.execute("SELECT id, name FROM users;")?;
        assert!(users.rows.is_empty());
        let audit = conn.execute("SELECT id, entry FROM audit_log;")?;
        assert!(audit.rows.is_empty());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_trigger_recursion_limit_is_enforced() -> Result<()> {
        let db = TestDbFile::new("_test_trigger_recursion_limit_is_enforced");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE audit_log (id INTEGER PRIMARY KEY, entry TEXT);")?;
        conn.execute(
            "CREATE TRIGGER audit_users AFTER INSERT ON users AS INSERT INTO audit_log (id, entry) VALUES (NEW.id, NEW.name);",
        )?;
        conn.execute(
            "CREATE TRIGGER audit_log_loop AFTER INSERT ON audit_log AS INSERT INTO users (id, name) VALUES (NEW.id + 100, NEW.entry);",
        )?;

        let result = conn.execute("INSERT INTO users (id, name) VALUES (1, 'Ada');");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Trigger recursion limit exceeded"));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_savepoint_requires_active_transaction_and_rejects_duplicates() -> Result<()> {
        let db = TestDbFile::new("_test_savepoint_requires_active_transaction_and_rejects_duplicates");
        let mut conn = Connection::new(db.path())?;

        let missing_tx = conn.execute("SAVEPOINT before_users;");
        assert!(missing_tx.is_err());
        assert!(missing_tx
            .unwrap_err()
            .to_string()
            .contains("requires an active transaction"));

        conn.execute("BEGIN;")?;
        conn.execute("SAVEPOINT before_users;")?;
        let duplicate = conn.execute("SAVEPOINT before_users;");
        assert!(duplicate.is_err());
        assert!(duplicate.unwrap_err().to_string().contains("already exists"));
        conn.execute("ROLLBACK;")?;

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_rollback_to_savepoint_restores_state_and_keeps_outer_transaction() -> Result<()> {
        let db = TestDbFile::new("_test_rollback_to_savepoint_restores_state_and_keeps_outer_transaction");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("BEGIN;")?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'Ada');")?;
        conn.execute("SAVEPOINT after_ada;")?;
        conn.execute("INSERT INTO users (id, name) VALUES (2, 'Bob');")?;
        conn.execute("ROLLBACK TO SAVEPOINT after_ada;")?;
        conn.execute("INSERT INTO users (id, name) VALUES (3, 'Cara');")?;
        conn.execute("COMMIT;")?;

        let result = conn.execute("SELECT id, name FROM users ORDER BY id ASC;")?;
        assert_eq!(
            result.rows,
            vec![
                vec![
                    crate::catalog::Value::Integer(1),
                    crate::catalog::Value::Text("Ada".to_string()),
                ],
                vec![
                    crate::catalog::Value::Integer(3),
                    crate::catalog::Value::Text("Cara".to_string()),
                ],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_release_savepoint_and_missing_savepoint_errors() -> Result<()> {
        let db = TestDbFile::new("_test_release_savepoint_and_missing_savepoint_errors");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("BEGIN;")?;
        conn.execute("SAVEPOINT first;")?;
        conn.execute("SAVEPOINT second;")?;
        conn.execute("RELEASE SAVEPOINT first;")?;

        let missing_release = conn.execute("RELEASE SAVEPOINT first;");
        assert!(missing_release.is_err());
        assert!(missing_release
            .unwrap_err()
            .to_string()
            .contains("does not exist"));

        let missing_rollback = conn.execute("ROLLBACK TO SAVEPOINT first;");
        assert!(missing_rollback.is_err());
        assert!(missing_rollback
            .unwrap_err()
            .to_string()
            .contains("does not exist"));

        conn.execute("ROLLBACK;")?;
        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_alter_table_add_and_drop_constraints() -> Result<()> {
        let db = TestDbFile::new("_test_alter_table_add_and_drop_constraints");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE orgs (id INTEGER PRIMARY KEY, code TEXT UNIQUE);")?;
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, org_id INTEGER);")?;

        conn.execute(
            "ALTER TABLE users ADD CONSTRAINT uq_users_email UNIQUE (email);",
        )?;
        conn.execute(
            "ALTER TABLE users ADD CONSTRAINT chk_users_email CHECK (email IS NOT NULL);",
        )?;
        conn.execute(
            "ALTER TABLE users ADD CONSTRAINT fk_users_org FOREIGN KEY (org_id) REFERENCES orgs (id);",
        )?;

        let describe = conn.execute("DESCRIBE users;")?;
        assert!(describe.rows.iter().any(|row| row[0] == crate::catalog::Value::Text("email".to_string())));

        conn.execute(
            "INSERT INTO orgs (id, code) VALUES (1, 'eng');",
        )?;
        conn.execute(
            "INSERT INTO users (id, email, org_id) VALUES (1, 'ada@example.com', 1);",
        )?;

        let dup = conn.execute(
            "INSERT INTO users (id, email, org_id) VALUES (2, 'ada@example.com', 1);",
        );
        assert!(dup.is_err());

        conn.execute("ALTER TABLE users DROP CONSTRAINT uq_users_email;")?;
        conn.execute("ALTER TABLE users DROP CONSTRAINT chk_users_email;")?;
        conn.execute("ALTER TABLE users DROP CONSTRAINT fk_users_org;")?;

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_insert_into_view_is_rejected() -> Result<()> {
        let db = TestDbFile::new("_test_insert_into_view_is_rejected");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE VIEW user_names AS SELECT id, name FROM users;")?;

        let result = conn.execute("INSERT INTO user_names (id, name) VALUES (1, 'Ada');");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("read-only"));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_update_view_is_rejected() -> Result<()> {
        let db = TestDbFile::new("_test_update_view_is_rejected");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE VIEW user_names AS SELECT id, name FROM users;")?;

        let result = conn.execute("UPDATE user_names SET name = 'Ada';");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("read-only"));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_delete_from_view_is_rejected() -> Result<()> {
        let db = TestDbFile::new("_test_delete_from_view_is_rejected");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE VIEW user_names AS SELECT id, name FROM users;")?;

        let result = conn.execute("DELETE FROM user_names;");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("read-only"));

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
                crate::catalog::Value::BigInt(1),
                crate::catalog::Value::Float(1.5),
                crate::catalog::Value::Decimal(crate::catalog::DecimalValue::parse("2.5")?),
                crate::catalog::Value::Text("AB".to_string()),
                crate::catalog::Value::Integer(3),
                crate::catalog::Value::Integer(4),
                crate::catalog::Value::Decimal(crate::catalog::DecimalValue::parse("5.5")?),
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
    fn test_scalar_null_handling_functions() -> Result<()> {
        let db = TestDbFile::new("_test_scalar_null_handling_functions");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, nickname TEXT);")?;
        conn.execute(
            "INSERT INTO users (id, name, nickname) VALUES (1, NULL, 'ally'), (2, 'Bob', NULL);",
        )?;

        let result = conn.execute(
            "SELECT COALESCE(name, nickname, 'unknown') AS display_name, IFNULL(nickname, 'none') AS nick, NULLIF(name, 'Bob') AS maybe_name FROM users ORDER BY id ASC;",
        )?;

        assert_eq!(
            result.rows,
            vec![
                vec![
                    crate::catalog::Value::Text("ally".to_string()),
                    crate::catalog::Value::Text("ally".to_string()),
                    crate::catalog::Value::Null,
                ],
                vec![
                    crate::catalog::Value::Text("Bob".to_string()),
                    crate::catalog::Value::Text("none".to_string()),
                    crate::catalog::Value::Null,
                ],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_scalar_null_handling_functions_in_insert_and_update() -> Result<()> {
        let db = TestDbFile::new("_test_scalar_null_handling_functions_in_insert_and_update");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, nickname TEXT);")?;
        conn.execute(
            "INSERT INTO users (id, name, nickname) VALUES (1, COALESCE(NULL, 'alice'), IFNULL(NULL, 'ally'));",
        )?;
        conn.execute(
            "UPDATE users SET nickname = NULLIF(name, 'alice'), name = COALESCE(name, 'unknown') WHERE id = 1;",
        )?;

        let result = conn.execute("SELECT name, nickname FROM users WHERE id = 1;")?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Text("alice".to_string()),
                crate::catalog::Value::Null,
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_scalar_string_functions() -> Result<()> {
        let db = TestDbFile::new("_test_scalar_string_functions");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, nickname TEXT);")?;
        conn.execute(
            "INSERT INTO users (id, name, nickname) VALUES (1, '  Alice  ', 'ally'), (2, NULL, 'BOB');",
        )?;

        let result = conn.execute(
            "SELECT LOWER(TRIM(name)) AS lowered_name, UPPER(nickname) AS loud_nick, LENGTH(TRIM(name)) AS trimmed_len FROM users ORDER BY id ASC;",
        )?;

        assert_eq!(
            result.rows,
            vec![
                vec![
                    crate::catalog::Value::Text("alice".to_string()),
                    crate::catalog::Value::Text("ALLY".to_string()),
                    crate::catalog::Value::Integer(5),
                ],
                vec![
                    crate::catalog::Value::Null,
                    crate::catalog::Value::Text("BOB".to_string()),
                    crate::catalog::Value::Null,
                ],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_scalar_string_functions_in_filters_and_updates() -> Result<()> {
        let db = TestDbFile::new("_test_scalar_string_functions_in_filters_and_updates");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, nickname TEXT);")?;
        conn.execute("INSERT INTO users (id, name, nickname) VALUES (1, '  Alice  ', 'ally');")?;
        conn.execute(
            "UPDATE users SET nickname = UPPER(TRIM(name)) WHERE LENGTH(TRIM(name)) = 5;",
        )?;

        let result =
            conn.execute("SELECT nickname FROM users WHERE LOWER(TRIM(name)) = 'alice';")?;
        assert_eq!(
            result.rows,
            vec![vec![crate::catalog::Value::Text("ALICE".to_string())]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_scalar_numeric_functions() -> Result<()> {
        let db = TestDbFile::new("_test_scalar_numeric_functions");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE metrics (id INTEGER PRIMARY KEY, amount FLOAT, delta INTEGER);",
        )?;
        conn.execute(
            "INSERT INTO metrics (id, amount, delta) VALUES (1, -12.345, -7), (2, NULL, NULL);",
        )?;

        let result = conn.execute(
            "SELECT ABS(delta), ROUND(amount), ROUND(amount, 2), ROUND(delta, -1) FROM metrics ORDER BY id ASC;",
        )?;

        assert_eq!(
            result.rows,
            vec![
                vec![
                    crate::catalog::Value::Integer(7),
                    crate::catalog::Value::Float(-12.0),
                    crate::catalog::Value::Float(-12.35),
                    crate::catalog::Value::Integer(-10),
                ],
                vec![
                    crate::catalog::Value::Null,
                    crate::catalog::Value::Null,
                    crate::catalog::Value::Null,
                    crate::catalog::Value::Null,
                ],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_scalar_numeric_functions_in_updates_and_filters() -> Result<()> {
        let db = TestDbFile::new("_test_scalar_numeric_functions_in_updates_and_filters");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE metrics (id INTEGER PRIMARY KEY, amount FLOAT, delta INTEGER);",
        )?;
        conn.execute("INSERT INTO metrics (id, amount, delta) VALUES (1, 1.26, -4);")?;
        conn.execute(
            "UPDATE metrics SET amount = ROUND(amount, 1), delta = ABS(delta) WHERE ROUND(amount, 1) = 1.3;",
        )?;

        let result = conn.execute("SELECT amount, delta FROM metrics WHERE ABS(delta) = 4;")?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Float(1.3),
                crate::catalog::Value::Integer(4),
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_scalar_function_numeric_comparisons_coerce_integer_and_float() -> Result<()> {
        let db =
            TestDbFile::new("_test_scalar_function_numeric_comparisons_coerce_integer_and_float");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE metrics (id INTEGER PRIMARY KEY, amount FLOAT);")?;
        conn.execute("INSERT INTO metrics (id, amount) VALUES (1, 1.26), (2, 2.49);")?;

        let result =
            conn.execute("SELECT id FROM metrics WHERE ROUND(amount) IN (1, 2) ORDER BY id ASC;")?;
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
    fn test_scalar_function_argument_errors_are_reported() -> Result<()> {
        let db = TestDbFile::new("_test_scalar_function_argument_errors_are_reported");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE metrics (id INTEGER PRIMARY KEY, name TEXT, amount FLOAT);")?;
        conn.execute("INSERT INTO metrics (id, name, amount) VALUES (1, 'alice', 1.26);")?;

        assert!(conn.execute("SELECT COALESCE() FROM metrics;").is_err());
        assert!(conn.execute("SELECT LOWER(id) FROM metrics;").is_err());
        assert!(conn
            .execute("SELECT ROUND(amount, 1.5) FROM metrics;")
            .is_err());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_case_expression_in_select_and_where() -> Result<()> {
        let db = TestDbFile::new("_test_case_expression_in_select_and_where");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE grades (id INTEGER PRIMARY KEY, score INTEGER, nickname TEXT);",
        )?;
        conn.execute(
            "INSERT INTO grades (id, score, nickname) VALUES (1, 95, 'ace'), (2, 82, NULL), (3, 70, 'steady');",
        )?;

        let result = conn.execute(
            "SELECT CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END AS grade FROM grades WHERE score >= CASE WHEN nickname IS NULL THEN 80 ELSE 90 END ORDER BY id ASC;",
        )?;

        assert_eq!(
            result.rows,
            vec![
                vec![crate::catalog::Value::Text("A".to_string())],
                vec![crate::catalog::Value::Text("B".to_string())],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_case_expression_in_update_assignment() -> Result<()> {
        let db = TestDbFile::new("_test_case_expression_in_update_assignment");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE grades (id INTEGER PRIMARY KEY, score INTEGER, label TEXT);")?;
        conn.execute("INSERT INTO grades (id, score, label) VALUES (1, 95, NULL), (2, 72, NULL);")?;
        conn.execute(
            "UPDATE grades SET label = CASE WHEN score >= 90 THEN 'top' WHEN score >= 80 THEN 'mid' ELSE 'base' END;",
        )?;

        let result = conn.execute("SELECT label FROM grades ORDER BY id ASC;")?;
        assert_eq!(
            result.rows,
            vec![
                vec![crate::catalog::Value::Text("top".to_string())],
                vec![crate::catalog::Value::Text("base".to_string())],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_boolean_expressions_in_projection_and_case() -> Result<()> {
        let db = TestDbFile::new("_test_boolean_expressions_in_projection_and_case");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE flags (id INTEGER PRIMARY KEY, score INTEGER, active BOOLEAN);",
        )?;
        conn.execute(
            "INSERT INTO flags (id, score, active) VALUES (1, 1, FALSE), (2, 2, FALSE), (3, 3, TRUE);",
        )?;

        let result = conn.execute(
            "SELECT (score > 1 AND NOT active) AS flagged, CASE WHEN (score > 1 AND NOT active) OR active THEN 'yes' ELSE 'no' END AS verdict FROM flags ORDER BY id ASC;",
        )?;

        assert_eq!(
            result.rows,
            vec![
                vec![
                    crate::catalog::Value::Boolean(false),
                    crate::catalog::Value::Text("no".to_string()),
                ],
                vec![
                    crate::catalog::Value::Boolean(true),
                    crate::catalog::Value::Text("yes".to_string()),
                ],
                vec![
                    crate::catalog::Value::Boolean(false),
                    crate::catalog::Value::Text("yes".to_string()),
                ],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_insert_and_update_with_boolean_expressions() -> Result<()> {
        let db = TestDbFile::new("_test_insert_and_update_with_boolean_expressions");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE flags (id INTEGER PRIMARY KEY, enabled BOOLEAN, score INTEGER);",
        )?;
        conn.execute(
            "INSERT INTO flags (id, enabled, score) VALUES (1, 1 < 2 AND NOT FALSE, 4), (2, 3 NOT BETWEEN 1 AND 2 OR FALSE, 7);",
        )?;
        conn.execute("UPDATE flags SET enabled = (enabled AND score >= 5) OR id = 1;")?;

        let result = conn.execute("SELECT id, enabled FROM flags ORDER BY id ASC;")?;
        assert_eq!(
            result.rows,
            vec![
                vec![
                    crate::catalog::Value::Integer(1),
                    crate::catalog::Value::Boolean(true),
                ],
                vec![
                    crate::catalog::Value::Integer(2),
                    crate::catalog::Value::Boolean(true),
                ],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_boolean_expression_rejects_non_boolean_operands() -> Result<()> {
        let db = TestDbFile::new("_test_boolean_expression_rejects_non_boolean_operands");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE flags (id INTEGER PRIMARY KEY, active BOOLEAN);")?;
        conn.execute("INSERT INTO flags (id, active) VALUES (1, TRUE);")?;

        assert!(conn.execute("SELECT 1 AND active FROM flags;").is_err());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_concat_and_concat_ws_functions() -> Result<()> {
        let db = TestDbFile::new("_test_concat_and_concat_ws_functions");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, nickname TEXT);")?;
        conn.execute(
            "INSERT INTO users (id, first_name, last_name, nickname) VALUES (1, 'Ada', 'Lovelace', NULL), (2, 'Linus', 'Torvalds', 'LT');",
        )?;

        let result = conn.execute(
            "SELECT CONCAT(first_name, ' ', last_name), CONCAT_WS('-', first_name, nickname, last_name) FROM users ORDER BY id ASC;",
        )?;

        assert_eq!(
            result.rows,
            vec![
                vec![
                    crate::catalog::Value::Text("Ada Lovelace".to_string()),
                    crate::catalog::Value::Text("Ada-Lovelace".to_string()),
                ],
                vec![
                    crate::catalog::Value::Text("Linus Torvalds".to_string()),
                    crate::catalog::Value::Text("Linus-LT-Torvalds".to_string()),
                ],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_concat_function_null_propagation() -> Result<()> {
        let db = TestDbFile::new("_test_concat_function_null_propagation");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, nickname TEXT);")?;
        conn.execute("INSERT INTO users (id, name, nickname) VALUES (1, 'Ada', NULL);")?;

        let result = conn
            .execute("SELECT CONCAT(name, nickname), CONCAT_WS(':', name, nickname) FROM users;")?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Null,
                crate::catalog::Value::Text("Ada".to_string()),
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_substring_left_and_right_functions() -> Result<()> {
        let db = TestDbFile::new("_test_substring_left_and_right_functions");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT);")?;
        conn.execute("INSERT INTO docs (id, title) VALUES (1, 'hematite');")?;

        let result = conn.execute(
            "SELECT SUBSTRING(title, 2, 4), SUBSTR(title, -4), LEFT(title, 3), RIGHT(title, 2) FROM docs;",
        )?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Text("emat".to_string()),
                crate::catalog::Value::Text("tite".to_string()),
                crate::catalog::Value::Text("hem".to_string()),
                crate::catalog::Value::Text("te".to_string()),
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_substring_functions_work_in_filters_and_updates() -> Result<()> {
        let db = TestDbFile::new("_test_substring_functions_work_in_filters_and_updates");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, short_code TEXT);")?;
        conn.execute("INSERT INTO docs (id, title, short_code) VALUES (1, 'hematite', NULL);")?;
        conn.execute(
            "UPDATE docs SET short_code = RIGHT(title, 3) WHERE LEFT(title, 4) = 'hema';",
        )?;

        let result =
            conn.execute("SELECT short_code FROM docs WHERE SUBSTRING(title, 5) = 'tite';")?;
        assert_eq!(
            result.rows,
            vec![vec![crate::catalog::Value::Text("ite".to_string())]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_greatest_and_least_functions() -> Result<()> {
        let db = TestDbFile::new("_test_greatest_and_least_functions");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE metrics (id INTEGER PRIMARY KEY, score FLOAT, floor INTEGER);")?;
        conn.execute("INSERT INTO metrics (id, score, floor) VALUES (1, 7.5, 8), (2, NULL, 3);")?;

        let result = conn.execute(
            "SELECT GREATEST(score, floor, 6), LEAST(score, floor, 6) FROM metrics ORDER BY id ASC;",
        )?;
        assert_eq!(
            result.rows,
            vec![
                vec![
                    crate::catalog::Value::Integer(8),
                    crate::catalog::Value::Integer(6),
                ],
                vec![crate::catalog::Value::Null, crate::catalog::Value::Null,],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_case_without_else_and_extremum_errors() -> Result<()> {
        let db = TestDbFile::new("_test_case_without_else_and_extremum_errors");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE metrics (id INTEGER PRIMARY KEY, score INTEGER, name TEXT);")?;
        conn.execute("INSERT INTO metrics (id, score, name) VALUES (1, 5, 'five');")?;

        let result = conn.execute(
            "SELECT CASE WHEN score > 10 THEN 'high' END, LEAST(score, 7) FROM metrics;",
        )?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Null,
                crate::catalog::Value::Integer(5),
            ]]
        );

        assert!(conn
            .execute("SELECT GREATEST(score) FROM metrics;")
            .is_err());
        assert!(conn
            .execute("SELECT LEAST(score, name) FROM metrics;")
            .is_err());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_replace_repeat_and_reverse_functions() -> Result<()> {
        let db = TestDbFile::new("_test_replace_repeat_and_reverse_functions");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, code TEXT);")?;
        conn.execute("INSERT INTO docs (id, title, code) VALUES (1, 'hematite-db', 'ab');")?;

        let result = conn.execute(
            "SELECT REPLACE(title, '-', ' '), REPEAT(code, 3), REVERSE(code) FROM docs;",
        )?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Text("hematite db".to_string()),
                crate::catalog::Value::Text("ababab".to_string()),
                crate::catalog::Value::Text("ba".to_string()),
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_replace_repeat_and_reverse_in_update_and_null_cases() -> Result<()> {
        let db = TestDbFile::new("_test_replace_repeat_and_reverse_in_update_and_null_cases");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, code TEXT);")?;
        conn.execute("INSERT INTO docs (id, title, code) VALUES (1, 'hematite-db', NULL);")?;
        conn.execute(
            "UPDATE docs SET title = REPLACE(title, '-', '_'), code = REPEAT(REVERSE('ab'), 2) WHERE id = 1;",
        )?;

        let result = conn.execute("SELECT title, code, REVERSE(NULL) FROM docs;")?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Text("hematite_db".to_string()),
                crate::catalog::Value::Text("baba".to_string()),
                crate::catalog::Value::Null,
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_locate_function() -> Result<()> {
        let db = TestDbFile::new("_test_locate_function");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT);")?;
        conn.execute("INSERT INTO docs (id, title) VALUES (1, 'hematite'), (2, 'metadata');")?;

        let result = conn.execute(
            "SELECT LOCATE('ti', title), LOCATE('ta', title, 4) FROM docs ORDER BY id ASC;",
        )?;
        assert_eq!(
            result.rows,
            vec![
                vec![
                    crate::catalog::Value::Integer(5),
                    crate::catalog::Value::Integer(0),
                ],
                vec![
                    crate::catalog::Value::Integer(0),
                    crate::catalog::Value::Integer(7),
                ],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_locate_function_in_filters() -> Result<()> {
        let db = TestDbFile::new("_test_locate_function_in_filters");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT);")?;
        conn.execute("INSERT INTO docs (id, title) VALUES (1, 'hematite'), (2, 'metal');")?;

        let result =
            conn.execute("SELECT id FROM docs WHERE LOCATE('ta', title) > 0 ORDER BY id ASC;")?;
        assert_eq!(result.rows, vec![vec![crate::catalog::Value::Integer(2)]]);

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_ceil_floor_and_power_functions() -> Result<()> {
        let db = TestDbFile::new("_test_ceil_floor_and_power_functions");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE metrics (id INTEGER PRIMARY KEY, score FLOAT, exponent INTEGER);",
        )?;
        conn.execute("INSERT INTO metrics (id, score, exponent) VALUES (1, 2.25, 3);")?;

        let result = conn.execute(
            "SELECT CEIL(score), CEILING(score), FLOOR(score), POWER(score, exponent), POW(2, exponent) FROM metrics;",
        )?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Float(3.0),
                crate::catalog::Value::Float(3.0),
                crate::catalog::Value::Float(2.0),
                crate::catalog::Value::Float(11.390625),
                crate::catalog::Value::Float(8.0),
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_numeric_helper_functions_in_updates_and_filters() -> Result<()> {
        let db = TestDbFile::new("_test_numeric_helper_functions_in_updates_and_filters");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE metrics (id INTEGER PRIMARY KEY, score FLOAT, bucket FLOAT);")?;
        conn.execute("INSERT INTO metrics (id, score, bucket) VALUES (1, 2.25, NULL);")?;
        conn.execute("UPDATE metrics SET bucket = CEIL(score) WHERE FLOOR(score) = 2;")?;

        let result =
            conn.execute("SELECT bucket FROM metrics WHERE POWER(FLOOR(score), 2) = 4;")?;
        assert_eq!(result.rows, vec![vec![crate::catalog::Value::Float(3.0)]]);

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_additional_builtin_function_edge_cases() -> Result<()> {
        let db = TestDbFile::new("_test_additional_builtin_function_edge_cases");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT);")?;
        conn.execute("INSERT INTO docs (id, title) VALUES (1, 'abc');")?;

        let result = conn.execute(
            "SELECT REPEAT(title, -2), LOCATE('', title, 2), CEIL(NULL), POWER(NULL, 2) FROM docs;",
        )?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Text(String::new()),
                crate::catalog::Value::Integer(2),
                crate::catalog::Value::Null,
                crate::catalog::Value::Null,
            ]]
        );

        assert!(conn.execute("SELECT LOCATE('a') FROM docs;").is_err());
        assert!(conn.execute("SELECT POWER(title, 2) FROM docs;").is_err());

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
    fn test_alter_table_drop_column_rewrites_existing_rows() -> Result<()> {
        let db = TestDbFile::new("_test_alter_table_drop_column_rewrites_existing_rows");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN);")?;
        conn.execute("INSERT INTO users (id, name, active) VALUES (1, 'alice', TRUE);")?;
        conn.execute("ALTER TABLE users DROP COLUMN active;")?;

        let result = conn.execute("SELECT * FROM users;")?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Integer(1),
                crate::catalog::Value::Text("alice".to_string()),
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_alter_table_drop_column_rejects_index_dependency() -> Result<()> {
        let db = TestDbFile::new("_test_alter_table_drop_column_rejects_index_dependency");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, name TEXT);")?;
        conn.execute("CREATE INDEX idx_users_email ON users (email);")?;

        let err = conn
            .execute("ALTER TABLE users DROP COLUMN email;")
            .unwrap_err();
        assert!(err.to_string().contains("used by an index"));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_alter_table_drop_column_rejects_check_constraint_dependency() -> Result<()> {
        let db = TestDbFile::new("_test_alter_table_drop_column_rejects_check_dependency");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, score INTEGER, CHECK (score >= 0));",
        )?;

        let err = conn
            .execute("ALTER TABLE users DROP COLUMN score;")
            .unwrap_err();
        assert!(err.to_string().contains("CHECK constraint"));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_alter_table_drop_column_rejects_inbound_foreign_key_dependency() -> Result<()> {
        let db =
            TestDbFile::new("_test_alter_table_drop_column_rejects_inbound_foreign_key_dependency");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY, code TEXT UNIQUE);")?;
        conn.execute(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_code TEXT, FOREIGN KEY (parent_code) REFERENCES parents(code));",
        )?;

        let err = conn
            .execute("ALTER TABLE parents DROP COLUMN code;")
            .unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("referenced by a foreign key") || message.contains("used by an index")
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_alter_table_set_default_affects_future_inserts_only() -> Result<()> {
        let db = TestDbFile::new("_test_alter_table_set_default_affects_future_inserts_only");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, active BOOLEAN);")?;
        conn.execute("INSERT INTO users (id, active) VALUES (1, NULL);")?;
        conn.execute("ALTER TABLE users ALTER COLUMN active SET DEFAULT TRUE;")?;
        conn.execute("INSERT INTO users (id) VALUES (2);")?;

        let result = conn.execute("SELECT id, active FROM users ORDER BY id ASC;")?;
        assert_eq!(
            result.rows,
            vec![
                vec![
                    crate::catalog::Value::Integer(1),
                    crate::catalog::Value::Null,
                ],
                vec![
                    crate::catalog::Value::Integer(2),
                    crate::catalog::Value::Boolean(true),
                ],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_alter_table_set_default_persists_across_reopen() -> Result<()> {
        let db = TestDbFile::new("_test_alter_table_set_default_persists_across_reopen");
        {
            let mut conn = Connection::new(db.path())?;
            conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, active BOOLEAN);")?;
            conn.execute("ALTER TABLE users ALTER COLUMN active SET DEFAULT TRUE;")?;
            conn.close()?;
        }

        let mut conn = Connection::new(db.path())?;
        conn.execute("INSERT INTO users (id) VALUES (1);")?;
        let result = conn.execute("SELECT active FROM users WHERE id = 1;")?;
        assert_eq!(
            result.rows,
            vec![vec![crate::catalog::Value::Boolean(true)]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_alter_table_drop_default_restores_null_insert_behavior() -> Result<()> {
        let db = TestDbFile::new("_test_alter_table_drop_default_restores_null_insert_behavior");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, active BOOLEAN DEFAULT TRUE);")?;
        conn.execute("ALTER TABLE users ALTER COLUMN active DROP DEFAULT;")?;
        conn.execute("INSERT INTO users (id) VALUES (1);")?;

        let result = conn.execute("SELECT active FROM users WHERE id = 1;")?;
        assert_eq!(result.rows, vec![vec![crate::catalog::Value::Null]]);

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_alter_table_set_not_null_rejects_existing_null_rows() -> Result<()> {
        let db = TestDbFile::new("_test_alter_table_set_not_null_rejects_existing_null_rows");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, active BOOLEAN);")?;
        conn.execute("INSERT INTO users (id, active) VALUES (1, NULL);")?;

        let err = conn
            .execute("ALTER TABLE users ALTER COLUMN active SET NOT NULL;")
            .unwrap_err();
        assert!(err.to_string().contains("existing rows contain NULL"));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_alter_table_set_not_null_enforces_future_writes() -> Result<()> {
        let db = TestDbFile::new("_test_alter_table_set_not_null_enforces_future_writes");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, active BOOLEAN);")?;
        conn.execute("INSERT INTO users (id, active) VALUES (1, TRUE);")?;
        conn.execute("ALTER TABLE users ALTER COLUMN active SET NOT NULL;")?;

        let insert_err = conn
            .execute("INSERT INTO users (id, active) VALUES (2, NULL);")
            .unwrap_err();
        assert!(insert_err.to_string().contains("cannot be NULL"));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_alter_table_set_not_null_persists_across_reopen() -> Result<()> {
        let db = TestDbFile::new("_test_alter_table_set_not_null_persists_across_reopen");
        {
            let mut conn = Connection::new(db.path())?;
            conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, active BOOLEAN);")?;
            conn.execute("INSERT INTO users (id, active) VALUES (1, TRUE);")?;
            conn.execute("ALTER TABLE users ALTER COLUMN active SET NOT NULL;")?;
            conn.close()?;
        }

        let mut conn = Connection::new(db.path())?;
        let err = conn
            .execute("INSERT INTO users (id, active) VALUES (2, NULL);")
            .unwrap_err();
        assert!(err.to_string().contains("cannot be NULL"));

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_alter_table_drop_not_null_allows_null_inserts() -> Result<()> {
        let db = TestDbFile::new("_test_alter_table_drop_not_null_allows_null_inserts");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, active BOOLEAN NOT NULL);")?;
        conn.execute("ALTER TABLE users ALTER COLUMN active DROP NOT NULL;")?;
        conn.execute("INSERT INTO users (id, active) VALUES (1, NULL);")?;

        let result = conn.execute("SELECT active FROM users WHERE id = 1;")?;
        assert_eq!(result.rows, vec![vec![crate::catalog::Value::Null]]);

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_alter_table_drop_not_null_rejects_primary_key_column() -> Result<()> {
        let db = TestDbFile::new("_test_alter_table_drop_not_null_rejects_primary_key_column");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, active BOOLEAN);")?;
        let err = conn
            .execute("ALTER TABLE users ALTER COLUMN id DROP NOT NULL;")
            .unwrap_err();
        assert!(err.to_string().contains("Primary-key column"));

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
    fn test_select_intersect_and_except() -> Result<()> {
        let db = TestDbFile::new("_test_select_intersect_and_except");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER);")?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'alice');")?;
        conn.execute("INSERT INTO users (id, name) VALUES (2, 'bob');")?;
        conn.execute("INSERT INTO users (id, name) VALUES (3, 'cara');")?;
        conn.execute("INSERT INTO posts (id, user_id) VALUES (10, 2);")?;
        conn.execute("INSERT INTO posts (id, user_id) VALUES (11, 3);")?;
        conn.execute("INSERT INTO posts (id, user_id) VALUES (12, 3);")?;

        let intersect = conn
            .execute("SELECT id FROM users INTERSECT SELECT user_id FROM posts ORDER BY id ASC;")?;
        assert_eq!(
            intersect.rows,
            vec![
                vec![crate::catalog::Value::Integer(2)],
                vec![crate::catalog::Value::Integer(3)],
            ]
        );

        let except =
            conn.execute("SELECT id FROM users EXCEPT SELECT user_id FROM posts ORDER BY id ASC;")?;
        assert_eq!(except.rows, vec![vec![crate::catalog::Value::Integer(1)]]);

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_select_with_scalar_subquery_expression() -> Result<()> {
        let db = TestDbFile::new("_test_select_with_scalar_subquery_expression");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER);")?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'alice');")?;
        conn.execute("INSERT INTO users (id, name) VALUES (2, 'bob');")?;
        conn.execute("INSERT INTO posts (id, user_id) VALUES (10, 1);")?;
        conn.execute("INSERT INTO posts (id, user_id) VALUES (11, 1);")?;

        let projected = conn.execute(
            "SELECT (SELECT COUNT(*) FROM posts) AS post_count FROM users ORDER BY id ASC LIMIT 1;",
        )?;
        assert_eq!(
            projected.rows,
            vec![vec![crate::catalog::Value::Integer(2)]]
        );

        let filtered =
            conn.execute("SELECT name FROM users WHERE id = (SELECT MIN(user_id) FROM posts);")?;
        assert_eq!(
            filtered.rows,
            vec![vec![crate::catalog::Value::Text("alice".to_string())]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_select_with_correlated_subquery_predicates() -> Result<()> {
        let db = TestDbFile::new("_test_select_with_correlated_subquery_predicates");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER);")?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'alice');")?;
        conn.execute("INSERT INTO users (id, name) VALUES (2, 'bob');")?;
        conn.execute("INSERT INTO users (id, name) VALUES (3, 'cara');")?;
        conn.execute("INSERT INTO posts (id, user_id) VALUES (10, 1);")?;
        conn.execute("INSERT INTO posts (id, user_id) VALUES (11, 1);")?;
        conn.execute("INSERT INTO posts (id, user_id) VALUES (12, 3);")?;

        let exists = conn.execute(
            "SELECT u.id FROM users AS u WHERE EXISTS (SELECT p.user_id FROM posts AS p WHERE p.user_id = u.id) ORDER BY u.id ASC;",
        )?;
        assert_eq!(
            exists.rows,
            vec![
                vec![crate::catalog::Value::Integer(1)],
                vec![crate::catalog::Value::Integer(3)],
            ]
        );

        let in_result = conn.execute(
            "SELECT u.id FROM users AS u WHERE u.id IN (SELECT p.user_id FROM posts AS p WHERE p.user_id = u.id) ORDER BY u.id ASC;",
        )?;
        assert_eq!(
            in_result.rows,
            vec![
                vec![crate::catalog::Value::Integer(1)],
                vec![crate::catalog::Value::Integer(3)],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_select_with_correlated_scalar_subquery_expression() -> Result<()> {
        let db = TestDbFile::new("_test_select_with_correlated_scalar_subquery_expression");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER);")?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'alice');")?;
        conn.execute("INSERT INTO users (id, name) VALUES (2, 'bob');")?;
        conn.execute("INSERT INTO users (id, name) VALUES (3, 'cara');")?;
        conn.execute("INSERT INTO posts (id, user_id) VALUES (10, 1);")?;
        conn.execute("INSERT INTO posts (id, user_id) VALUES (11, 1);")?;
        conn.execute("INSERT INTO posts (id, user_id) VALUES (12, 3);")?;

        let projected = conn.execute(
            "SELECT u.id, (SELECT COUNT(*) FROM posts AS p WHERE p.user_id = u.id) AS post_count FROM users AS u ORDER BY u.id ASC;",
        )?;
        assert_eq!(
            projected.rows,
            vec![
                vec![
                    crate::catalog::Value::Integer(1),
                    crate::catalog::Value::Integer(2),
                ],
                vec![
                    crate::catalog::Value::Integer(2),
                    crate::catalog::Value::Integer(0),
                ],
                vec![
                    crate::catalog::Value::Integer(3),
                    crate::catalog::Value::Integer(1),
                ],
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
    fn test_recursive_cte_requires_union_shape() -> Result<()> {
        let db = TestDbFile::new("_test_recursive_cte_requires_union_shape");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE seeds (n INTEGER PRIMARY KEY);")?;
        conn.execute("INSERT INTO seeds (n) VALUES (1);")?;

        let err = conn
            .execute("WITH RECURSIVE nums AS (SELECT n FROM seeds) SELECT n FROM nums;")
            .expect_err("recursive CTE without UNION should be rejected");
        assert!(
            err.to_string().contains("requires UNION or UNION ALL"),
            "unexpected error: {err}"
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_select_with_recursive_cte_union_all() -> Result<()> {
        let db = TestDbFile::new("_test_select_with_recursive_cte_union_all");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE seeds (n INTEGER PRIMARY KEY);")?;
        conn.execute("INSERT INTO seeds (n) VALUES (1);")?;

        let result = conn.execute(
            "WITH RECURSIVE nums AS (\
             SELECT n FROM seeds \
             UNION ALL \
             SELECT n + 1 AS n FROM nums WHERE n < 3\
             ) \
             SELECT n FROM nums ORDER BY n ASC;",
        )?;
        assert_eq!(
            result.rows,
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
    fn test_select_with_recursive_cte_union_deduplicates() -> Result<()> {
        let db = TestDbFile::new("_test_select_with_recursive_cte_union_deduplicates");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE seeds (n INTEGER PRIMARY KEY);")?;
        conn.execute("INSERT INTO seeds (n) VALUES (1);")?;

        let result = conn.execute(
            "WITH RECURSIVE nums AS (\
             SELECT n FROM seeds \
             UNION \
             SELECT n + 0 AS n FROM nums WHERE n <= 1\
             ) \
             SELECT n FROM nums;",
        )?;
        assert_eq!(result.rows, vec![vec![crate::catalog::Value::Integer(1)]]);

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
    fn test_select_with_right_join() -> Result<()> {
        let db = TestDbFile::new("_test_select_with_right_join");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);")?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (10, 1, 'First');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (11, 9, 'Orphan');")?;

        let result = conn.execute(
            "SELECT u.name, p.title FROM users u RIGHT JOIN posts p ON u.id = p.user_id ORDER BY p.id ASC;",
        )?;

        assert_eq!(
            result.rows,
            vec![
                vec![
                    crate::catalog::Value::Text("Alice".to_string()),
                    crate::catalog::Value::Text("First".to_string()),
                ],
                vec![
                    crate::catalog::Value::Null,
                    crate::catalog::Value::Text("Orphan".to_string()),
                ],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_select_with_full_outer_join() -> Result<()> {
        let db = TestDbFile::new("_test_select_with_full_outer_join");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);")?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO users (id, name) VALUES (2, 'Bob');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (10, 1, 'First');")?;
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (11, 9, 'Orphan');")?;

        let result = conn.execute(
            "SELECT u.name, p.title FROM users u FULL OUTER JOIN posts p ON u.id = p.user_id ORDER BY name ASC, title ASC;",
        )?;

        assert_eq!(
            result.rows,
            vec![
                vec![
                    crate::catalog::Value::Null,
                    crate::catalog::Value::Text("Orphan".to_string()),
                ],
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
    fn test_cast_and_modulo_expressions() -> Result<()> {
        let db = TestDbFile::new("_test_cast_and_modulo_expressions");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, score INTEGER, label TEXT);")?;
        conn.execute("INSERT INTO test (id, score, label) VALUES (1, 5, '7');")?;
        conn.execute("INSERT INTO test (id, score, label) VALUES (2, 8, 'bad');")?;

        let result = conn.execute(
            "SELECT score % 2 AS remainder, CAST(label AS INTEGER) AS parsed FROM test WHERE id = 1;",
        )?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Integer(1),
                crate::catalog::Value::Integer(7),
            ]]
        );

        conn.execute("UPDATE test SET score = CAST(label AS INTEGER) % 3 WHERE id = 1;")?;
        let updated = conn.execute("SELECT score FROM test WHERE id = 1;")?;
        assert_eq!(updated.rows, vec![vec![crate::catalog::Value::Integer(1)]]);

        let bad_cast = conn.execute("SELECT CAST(label AS INTEGER) FROM test WHERE id = 2;");
        assert!(bad_cast.is_err());

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_practical_core_runtime_types_round_trip() -> Result<()> {
        let db = TestDbFile::new("_test_practical_core_runtime_types_round_trip");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE typed (\
                id BIGINT PRIMARY KEY,\
                amount DECIMAL(10, 2),\
                payload BLOB,\
                event_date DATE,\
                created_at DATETIME\
            );",
        )?;
        conn.execute(
            "INSERT INTO typed (id, amount, payload, event_date, created_at) VALUES (\
                CAST('5000000000' AS BIGINT),\
                CAST('12.3400' AS DECIMAL),\
                CAST('abc' AS BLOB),\
                '2026-03-27',\
                '2026-03-27 10:11:12'\
            );",
        )?;

        let result =
            conn.execute("SELECT id, amount, payload, event_date, created_at FROM typed;")?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::BigInt(5_000_000_000),
                crate::catalog::Value::Decimal(crate::catalog::DecimalValue::parse("12.34")?),
                crate::catalog::Value::Blob(b"abc".to_vec()),
                crate::catalog::Value::Date(crate::catalog::DateValue::parse("2026-03-27")?),
                crate::catalog::Value::DateTime(crate::catalog::DateTimeValue::parse(
                    "2026-03-27 10:11:12",
                )?),
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
    fn test_mysql_limit_offset_count_syntax() -> Result<()> {
        let db = TestDbFile::new("_test_mysql_limit_offset_count_syntax");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO test (id, name) VALUES (3, 'Cara');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")?;

        let result = conn.execute("SELECT id FROM test ORDER BY id ASC LIMIT 1, 1;")?;
        assert_eq!(result.rows, vec![vec![crate::catalog::Value::Integer(2)]]);

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_insert_set_syntax() -> Result<()> {
        let db = TestDbFile::new("_test_insert_set_syntax");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN);")?;
        conn.execute("INSERT INTO test SET id = 1, name = 'Alice', active = TRUE;")?;

        let result =
            conn.execute("SELECT id, name, active FROM test WHERE id = 1 ORDER BY id ASC;")?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Integer(1),
                crate::catalog::Value::Text("Alice".to_string()),
                crate::catalog::Value::Boolean(true),
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_insert_select_syntax() -> Result<()> {
        let db = TestDbFile::new("_test_insert_select_syntax");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE source (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("CREATE TABLE target (id INTEGER PRIMARY KEY, name TEXT);")?;
        conn.execute("INSERT INTO source (id, name) VALUES (1, 'Alice'), (2, 'Bob');")?;

        conn.execute("INSERT INTO target (id, name) SELECT id, name FROM source WHERE id >= 2;")?;

        let result = conn.execute("SELECT id, name FROM target ORDER BY id ASC;")?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Integer(2),
                crate::catalog::Value::Text("Bob".to_string()),
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_insert_on_duplicate_key_update() -> Result<()> {
        let db = TestDbFile::new("_test_insert_on_duplicate_key_update");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT UNIQUE, visits INTEGER);",
        )?;
        conn.execute("INSERT INTO test (id, name, visits) VALUES (1, 'Alice', 1);")?;

        conn.execute(
            "INSERT INTO test (id, name, visits) VALUES (1, 'Alice', 5) ON DUPLICATE KEY UPDATE visits = visits + 1;",
        )?;

        let result = conn.execute("SELECT id, name, visits FROM test ORDER BY id ASC;")?;
        assert_eq!(
            result.rows,
            vec![vec![
                crate::catalog::Value::Integer(1),
                crate::catalog::Value::Text("Alice".to_string()),
                crate::catalog::Value::Integer(2),
            ]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_auto_increment_assigns_integer_primary_keys() -> Result<()> {
        let db = TestDbFile::new("_test_auto_increment_assigns_integer_primary_keys");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE test (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT);")?;
        conn.execute("INSERT INTO test (name) VALUES ('Alice');")?;
        conn.execute("INSERT INTO test (id, name) VALUES (NULL, 'Bob');")?;

        let result = conn.execute("SELECT id, name FROM test ORDER BY id ASC;")?;
        assert_eq!(
            result.rows,
            vec![
                vec![
                    crate::catalog::Value::Integer(1),
                    crate::catalog::Value::Text("Alice".to_string()),
                ],
                vec![
                    crate::catalog::Value::Integer(2),
                    crate::catalog::Value::Text("Bob".to_string()),
                ],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_auto_increment_persists_across_reopen() -> Result<()> {
        let db = TestDbFile::new("_test_auto_increment_persists_across_reopen");
        {
            let mut conn = Connection::new(db.path())?;
            conn.execute("CREATE TABLE test (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT);")?;
            conn.execute("INSERT INTO test (name) VALUES ('Alice');")?;
            conn.close()?;
        }

        let mut reopened = Connection::new(db.path())?;
        reopened.execute("INSERT INTO test (name) VALUES ('Bob');")?;
        let result = reopened.execute("SELECT id, name FROM test ORDER BY id ASC;")?;
        assert_eq!(
            result.rows,
            vec![
                vec![
                    crate::catalog::Value::Integer(1),
                    crate::catalog::Value::Text("Alice".to_string()),
                ],
                vec![
                    crate::catalog::Value::Integer(2),
                    crate::catalog::Value::Text("Bob".to_string()),
                ],
            ]
        );
        reopened.close()?;
        Ok(())
    }

    #[test]
    fn test_mysql_create_table_options_are_accepted() -> Result<()> {
        let db = TestDbFile::new("_test_mysql_create_table_options_are_accepted");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;",
        )?;
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
        let result = conn.execute("SELECT name FROM users WHERE id = 1;")?;
        assert_eq!(
            result.rows,
            vec![vec![crate::catalog::Value::Text("Alice".to_string())]]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_mysql_create_key_syntax_is_accepted() -> Result<()> {
        let db = TestDbFile::new("_test_mysql_create_key_syntax_is_accepted");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INT PRIMARY KEY, email TEXT);")?;
        conn.execute("CREATE UNIQUE KEY idx_users_email USING BTREE ON users (email);")?;
        conn.execute("INSERT INTO users (id, email) VALUES (1, 'a@example.com');")?;
        let duplicate = conn.execute("INSERT INTO users (id, email) VALUES (2, 'a@example.com');");
        assert!(duplicate.is_err());

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
    fn test_distinct_alias_types_enforce_range_and_length() -> Result<()> {
        let db = TestDbFile::new("_test_distinct_alias_types_enforce_range_and_length");
        let mut conn = Connection::new(db.path())?;

        conn.execute(
            "CREATE TABLE typed (
                id INTEGER PRIMARY KEY,
                tiny TINYINT,
                small SMALLINT,
                code CHAR(4),
                nick VARCHAR(6),
                amount DECIMAL(5, 2)
            );",
        )?;

        conn.execute(
            "INSERT INTO typed (id, tiny, small, code, nick, amount)
             VALUES (1, 120, 32000, 'ABCD', 'alice', CAST('12.34' AS DECIMAL(5, 2)));",
        )?;

        assert!(conn
            .execute(
                "INSERT INTO typed (id, tiny, small, code, nick, amount)
                 VALUES (2, 200, 0, 'ABCD', 'bob', CAST('1.00' AS DECIMAL(5, 2)));",
            )
            .is_err());
        assert!(conn
            .execute(
                "INSERT INTO typed (id, tiny, small, code, nick, amount)
                 VALUES (3, 1, 40000, 'ABCD', 'bob', CAST('1.00' AS DECIMAL(5, 2)));",
            )
            .is_err());
        assert!(conn
            .execute(
                "INSERT INTO typed (id, tiny, small, code, nick, amount)
                 VALUES (4, 1, 2, 'TOOLONG', 'bob', CAST('1.00' AS DECIMAL(5, 2)));",
            )
            .is_err());
        assert!(conn
            .execute(
                "INSERT INTO typed (id, tiny, small, code, nick, amount)
                 VALUES (5, 1, 2, 'ABCD', 'toolong', CAST('1.00' AS DECIMAL(5, 2)));",
            )
            .is_err());
        assert!(conn
            .execute(
                "INSERT INTO typed (id, tiny, small, code, nick, amount)
                 VALUES (6, 1, 2, 'ABCD', 'bob', CAST('1234.56' AS DECIMAL(5, 2)));",
            )
            .is_err());

        conn.close()?;
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
    fn test_joined_update_uses_join_context_for_target_rows() -> Result<()> {
        let db = TestDbFile::new("_test_joined_update_uses_join_context_for_target_rows");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, team_id INTEGER, name TEXT);")?;
        conn.execute(
            "CREATE TABLE teams (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN NOT NULL);",
        )?;
        conn.execute(
            "INSERT INTO teams (id, name, active) VALUES
                (1, 'Core', TRUE),
                (2, 'Ops', FALSE);",
        )?;
        conn.execute(
            "INSERT INTO users (id, team_id, name) VALUES
                (1, 1, 'Alice'),
                (2, 2, 'Bob');",
        )?;

        let result = conn.execute(
            "UPDATE users u
             JOIN teams t ON u.team_id = t.id
             SET name = t.name
             WHERE t.active = TRUE;",
        )?;
        assert_eq!(result.affected_rows, 1);

        let rows = conn.execute("SELECT id, name FROM users ORDER BY id;")?;
        assert_eq!(
            rows.rows,
            vec![
                vec![
                    crate::catalog::Value::Integer(1),
                    crate::catalog::Value::Text("Core".to_string())
                ],
                vec![
                    crate::catalog::Value::Integer(2),
                    crate::catalog::Value::Text("Bob".to_string())
                ],
            ]
        );

        conn.close()?;
        Ok(())
    }

    #[test]
    fn test_joined_delete_removes_only_target_matches() -> Result<()> {
        let db = TestDbFile::new("_test_joined_delete_removes_only_target_matches");
        let mut conn = Connection::new(db.path())?;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, team_id INTEGER, name TEXT);")?;
        conn.execute("CREATE TABLE teams (id INTEGER PRIMARY KEY, active BOOLEAN NOT NULL);")?;
        conn.execute(
            "INSERT INTO teams (id, active) VALUES
                (1, TRUE),
                (2, FALSE);",
        )?;
        conn.execute(
            "INSERT INTO users (id, team_id, name) VALUES
                (1, 1, 'Alice'),
                (2, 2, 'Bob'),
                (3, 2, 'Cara');",
        )?;

        let result = conn.execute(
            "DELETE u
             FROM users u
             JOIN teams t ON u.team_id = t.id
             WHERE t.id = 2;",
        )?;
        assert_eq!(result.affected_rows, 2);

        let rows = conn.execute("SELECT id, name FROM users ORDER BY id;")?;
        assert_eq!(
            rows.rows,
            vec![vec![
                crate::catalog::Value::Integer(1),
                crate::catalog::Value::Text("Alice".to_string())
            ]]
        );

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
    use crate::sql::ExecutedStatement;
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

    #[test]
    fn test_iter_script_steps_through_mixed_statements() -> Result<()> {
        let test_db = TestDbFile::new("_test_iter_script_steps");
        let mut db = Hematite::new(test_db.path())?;

        let mut steps = db.iter_script(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\
             INSERT INTO users (id, name) VALUES (1, 'Alice');\
             SELECT name FROM users;",
        )?;

        match steps.next().transpose()?.unwrap() {
            ExecutedStatement::Statement(result) => assert_eq!(result.affected_rows, 0),
            ExecutedStatement::Query(_) => panic!("expected statement result"),
        }

        match steps.next().transpose()?.unwrap() {
            ExecutedStatement::Statement(result) => assert_eq!(result.affected_rows, 1),
            ExecutedStatement::Query(_) => panic!("expected statement result"),
        }

        match steps.next().transpose()?.unwrap() {
            ExecutedStatement::Query(result_set) => {
                assert_eq!(result_set.len(), 1);
                assert_eq!(result_set.get_row(0).unwrap().get_string(0)?, "Alice");
            }
            ExecutedStatement::Statement(_) => panic!("expected query result"),
        }

        assert!(steps.next().is_none());
        Ok(())
    }

    #[test]
    fn test_iter_script_stops_at_first_error() -> Result<()> {
        let test_db = TestDbFile::new("_test_iter_script_error_stop");
        let mut db = Hematite::new(test_db.path())?;

        let mut steps = db.iter_script(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\
             INSERT INTO users (id, name) VALUES (1, 'Alice');\
             INSERT INTO users (id, name) VALUES (1, 'Bob');\
             INSERT INTO users (id, name) VALUES (2, 'Cara');",
        )?;

        assert!(steps.next().transpose()?.is_some());
        assert!(steps.next().transpose()?.is_some());
        assert!(steps.next().transpose().is_err());
        drop(steps);

        let rs = db.query("SELECT name FROM users ORDER BY id;")?;
        assert_eq!(rs.len(), 1);
        assert_eq!(rs.get_row(0).unwrap().get_string(0)?, "Alice");
        Ok(())
    }
}

mod result_tests {
    use crate::catalog::{
        DateTimeValue, DateValue, DecimalValue, TimeValue, TimeWithTimeZoneValue, TimestampValue,
    };
    use crate::error::Result;
    use crate::query::{QueryResult, Value};
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
    fn test_row_rich_type_accessors() -> Result<()> {
        let values = vec![
            Value::BigInt(5_000_000_000),
            Value::Decimal(DecimalValue::parse("12.34")?),
            Value::Blob(b"abc".to_vec()),
            Value::Date(DateValue::parse("2026-03-27")?),
            Value::DateTime(DateTimeValue::parse("2026-03-27 10:11:12")?),
            Value::Time(TimeValue::parse("10:11:12")?),
            Value::Timestamp(TimestampValue::parse("2026-03-27 10:11:12")?),
            Value::TimeWithTimeZone(TimeWithTimeZoneValue::parse("10:11:12+03:00")?),
            Value::Enum("live".to_string()),
        ];

        let row = Row::new(values);

        assert_eq!(row.get_bigint(0)?, 5_000_000_000);
        assert_eq!(row.get_decimal(1)?.to_string(), "12.34");
        assert_eq!(row.get_blob(2)?, b"abc".to_vec());
        assert_eq!(row.get_date(3)?.to_string(), "2026-03-27");
        assert_eq!(row.get_datetime(4)?.to_string(), "2026-03-27 10:11:12");
        assert_eq!(row.get_time(5)?.to_string(), "10:11:12");
        assert_eq!(row.get_timestamp(6)?.to_string(), "2026-03-27 10:11:12");
        assert_eq!(
            row.get_time_with_time_zone(7)?.to_string(),
            "10:11:12+03:00"
        );
        assert_eq!(row.get_string(8)?, "live");

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

    #[test]
    fn test_executed_statement_helpers() -> Result<()> {
        let query = ExecutedStatement::from_query_result(QueryResult {
            affected_rows: 0,
            columns: vec!["value".to_string()],
            rows: vec![vec![Value::Integer(7)]],
        });
        assert!(matches!(query, ExecutedStatement::Query(_)));
        assert_eq!(query.as_query().unwrap().get_row(0).unwrap().get_int(0)?, 7);
        assert!(query.as_statement().is_none());

        let statement = ExecutedStatement::from_query_result(QueryResult {
            affected_rows: 2,
            columns: Vec::new(),
            rows: Vec::new(),
        });
        assert!(matches!(statement, ExecutedStatement::Statement(_)));
        assert_eq!(statement.as_statement().unwrap().affected_rows, 2);
        assert!(statement.as_query().is_none());
        Ok(())
    }
}
