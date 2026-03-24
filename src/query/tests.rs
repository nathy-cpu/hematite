//! Centralized tests for the query module

mod executor_tests {
    use crate::catalog::types::{DataType, Value};
    use crate::catalog::CatalogEngine;
    use crate::catalog::{Column, Schema};
    use crate::error::Result;
    use crate::parser::ast::*;
    use crate::query::executor::*;
    use crate::test_utils::TestDbFile;

    #[test]
    fn test_select_executor() -> Result<()> {
        let mut catalog = Schema::new();

        // Create test table
        let columns = vec![
            Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                DataType::Integer,
            )
            .primary_key(true),
            Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                DataType::Text,
            ),
        ];
        catalog.create_table("users".to_string(), columns)?;

        let db = TestDbFile::new("_test_select_executor");
        let mut storage = CatalogEngine::new(db.path())?;
        // Create table in storage as well
        let _ = storage.create_table("users")?;

        // Add some test data
        let _ = storage.insert_into_table(
            "users",
            vec![Value::Integer(1), Value::Text("Alice".to_string())],
        )?;
        let _ = storage.insert_into_table(
            "users",
            vec![Value::Integer(2), Value::Text("Bob".to_string())],
        )?;
        let _ = storage.insert_into_table(
            "users",
            vec![Value::Integer(3), Value::Text("Charlie".to_string())],
        )?;
        let mut ctx = ExecutionContext::for_mutation(&catalog, &mut storage);

        let statement = SelectStatement {
            distinct: false,
            columns: vec![SelectItem::Column("id".to_string())],
            column_aliases: vec![None],
            from: TableReference::Table("users".to_string(), None),
            where_clause: None,
            group_by: Vec::new(),
            having_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operation: None,
        };

        let mut executor = SelectExecutor::new(
            statement,
            crate::query::planner::SelectAccessPath::FullTableScan,
        );
        let result = executor.execute(&mut ctx)?;

        assert_eq!(result.columns, vec!["id"]);
        assert_eq!(result.rows.len(), 3); // 3 simulated rows
        storage.flush()?;
        Ok(())
    }

    #[test]
    fn test_select_executor_secondary_index_lookup() -> Result<()> {
        let mut catalog = Schema::new();

        let mut table = crate::catalog::Table::new(
            crate::catalog::TableId::new(1),
            "users".to_string(),
            vec![
                Column::new(
                    crate::catalog::ColumnId::new(1),
                    "id".to_string(),
                    DataType::Integer,
                )
                .primary_key(true),
                Column::new(
                    crate::catalog::ColumnId::new(2),
                    "email".to_string(),
                    DataType::Text,
                ),
            ],
            0u32,
        )?;

        let db = TestDbFile::new("_test_select_executor_secondary_index_lookup");
        let mut storage = CatalogEngine::new(db.path())?;
        let root_page_id = storage.create_table("users")?;
        let primary_key_root_page_id = storage.create_empty_btree()?;
        let secondary_index_root_page_id = storage.create_empty_btree()?;
        table.root_page_id = root_page_id.into();
        table.primary_key_index_root_page_id = primary_key_root_page_id.into();
        table.add_secondary_index(crate::catalog::SecondaryIndex {
            name: "idx_users_email".to_string(),
            column_indices: vec![1],
            root_page_id: secondary_index_root_page_id.into(),
            unique: false,
        })?;
        catalog.insert_table(table)?;

        let row_id_1 = storage.insert_into_table(
            "users",
            vec![Value::Integer(1), Value::Text("a@example.com".to_string())],
        )?;
        let row_id_2 = storage.insert_into_table(
            "users",
            vec![Value::Integer(2), Value::Text("b@example.com".to_string())],
        )?;

        let table = catalog.get_table_by_name("users").unwrap();
        storage.register_secondary_index_row(
            table,
            crate::catalog::StoredRow {
                row_id: row_id_1,
                values: vec![Value::Integer(1), Value::Text("a@example.com".to_string())],
            },
        )?;
        storage.register_primary_key_row(
            table,
            crate::catalog::StoredRow {
                row_id: row_id_1,
                values: vec![Value::Integer(1), Value::Text("a@example.com".to_string())],
            },
        )?;
        storage.register_secondary_index_row(
            table,
            crate::catalog::StoredRow {
                row_id: row_id_2,
                values: vec![Value::Integer(2), Value::Text("b@example.com".to_string())],
            },
        )?;
        storage.register_primary_key_row(
            table,
            crate::catalog::StoredRow {
                row_id: row_id_2,
                values: vec![Value::Integer(2), Value::Text("b@example.com".to_string())],
            },
        )?;

        let mut ctx = ExecutionContext::for_read(&catalog, &mut storage);
        let statement = SelectStatement {
            distinct: false,
            columns: vec![SelectItem::Column("id".to_string())],
            column_aliases: vec![None],
            from: TableReference::Table("users".to_string(), None),
            where_clause: Some(WhereClause {
                conditions: vec![Condition::Comparison {
                    left: Expression::Column("email".to_string()),
                    operator: ComparisonOperator::Equal,
                    right: Expression::Literal(Value::Text("b@example.com".to_string())),
                }],
            }),
            group_by: Vec::new(),
            having_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operation: None,
        };

        let mut executor = SelectExecutor::new(
            statement,
            crate::query::planner::SelectAccessPath::SecondaryIndexLookup(
                "idx_users_email".to_string(),
            ),
        );
        let result = executor.execute(&mut ctx)?;

        assert_eq!(result.rows, vec![vec![Value::Integer(2)]]);
        storage.flush()?;
        Ok(())
    }

    #[test]
    fn test_select_executor_primary_key_lookup() -> Result<()> {
        let mut catalog = Schema::new();

        let mut table = crate::catalog::Table::new(
            crate::catalog::TableId::new(1),
            "users".to_string(),
            vec![
                Column::new(
                    crate::catalog::ColumnId::new(1),
                    "id".to_string(),
                    DataType::Integer,
                )
                .primary_key(true),
                Column::new(
                    crate::catalog::ColumnId::new(2),
                    "name".to_string(),
                    DataType::Text,
                ),
            ],
            0u32,
        )?;

        let db = TestDbFile::new("_test_select_executor_primary_key_lookup");
        let mut storage = CatalogEngine::new(db.path())?;
        let root_page_id = storage.create_table("users")?;
        let primary_key_root_page_id = storage.create_empty_btree()?;
        table.root_page_id = root_page_id.into();
        table.primary_key_index_root_page_id = primary_key_root_page_id.into();
        catalog.insert_table(table)?;

        let row_id_1 = storage.insert_into_table(
            "users",
            vec![Value::Integer(1), Value::Text("Alice".to_string())],
        )?;
        let row_id_2 = storage.insert_into_table(
            "users",
            vec![Value::Integer(2), Value::Text("Bob".to_string())],
        )?;

        let table = catalog.get_table_by_name("users").unwrap();
        storage.register_primary_key_row(
            table,
            crate::catalog::StoredRow {
                row_id: row_id_1,
                values: vec![Value::Integer(1), Value::Text("Alice".to_string())],
            },
        )?;
        storage.register_primary_key_row(
            table,
            crate::catalog::StoredRow {
                row_id: row_id_2,
                values: vec![Value::Integer(2), Value::Text("Bob".to_string())],
            },
        )?;

        let mut ctx = ExecutionContext::for_read(&catalog, &mut storage);
        let statement = SelectStatement {
            distinct: false,
            columns: vec![SelectItem::Column("name".to_string())],
            column_aliases: vec![None],
            from: TableReference::Table("users".to_string(), None),
            where_clause: Some(WhereClause {
                conditions: vec![Condition::Comparison {
                    left: Expression::Column("id".to_string()),
                    operator: ComparisonOperator::Equal,
                    right: Expression::Literal(Value::Integer(2)),
                }],
            }),
            group_by: Vec::new(),
            having_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operation: None,
        };

        let mut executor = SelectExecutor::new(
            statement,
            crate::query::planner::SelectAccessPath::PrimaryKeyLookup,
        );
        let result = executor.execute(&mut ctx)?;

        assert_eq!(result.rows, vec![vec![Value::Text("Bob".to_string())]]);
        storage.flush()?;
        Ok(())
    }

    #[test]
    fn test_select_executor_rowid_lookup() -> Result<()> {
        let mut catalog = Schema::new();
        let columns = vec![
            Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                DataType::Integer,
            )
            .primary_key(true),
            Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                DataType::Text,
            ),
        ];
        catalog.create_table("users".to_string(), columns)?;

        let db = TestDbFile::new("_test_select_executor_rowid_lookup");
        let mut storage = CatalogEngine::new(db.path())?;
        let _ = storage.create_table("users")?;

        let rowid_1 = storage.insert_into_table(
            "users",
            vec![Value::Integer(1), Value::Text("Alice".to_string())],
        )?;
        let _rowid_2 = storage.insert_into_table(
            "users",
            vec![Value::Integer(2), Value::Text("Bob".to_string())],
        )?;

        let mut ctx = ExecutionContext::for_read(&catalog, &mut storage);
        let statement = SelectStatement {
            distinct: false,
            columns: vec![SelectItem::Column("id".to_string())],
            column_aliases: vec![None],
            from: TableReference::Table("users".to_string(), None),
            where_clause: Some(WhereClause {
                conditions: vec![Condition::Comparison {
                    left: Expression::Column("rowid".to_string()),
                    operator: ComparisonOperator::Equal,
                    right: Expression::Literal(Value::Integer(rowid_1 as i32)),
                }],
            }),
            group_by: Vec::new(),
            having_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operation: None,
        };

        let mut executor = SelectExecutor::new(
            statement,
            crate::query::planner::SelectAccessPath::RowIdLookup,
        );
        let result = executor.execute(&mut ctx)?;
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
        Ok(())
    }

    #[test]
    fn test_insert_executor() -> Result<()> {
        let mut catalog = Schema::new();

        // Create test table
        let columns = vec![
            Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                DataType::Integer,
            )
            .primary_key(true),
            Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                DataType::Text,
            ),
        ];
        let table_id = catalog.create_table("users".to_string(), columns)?;

        let db = TestDbFile::new("_test_insert_executor");
        let mut storage = CatalogEngine::new(db.path())?;
        // Create table in storage as well
        let root_page_id = storage.create_table("users")?;
        let primary_key_root_page_id = storage.create_empty_btree()?;
        catalog.set_table_root_page(table_id, root_page_id)?;
        catalog.set_table_primary_key_root_page(table_id, primary_key_root_page_id)?;
        let mut ctx = ExecutionContext::for_mutation(&catalog, &mut storage);

        let statement = InsertStatement {
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            values: vec![vec![
                Expression::Literal(Value::Integer(4)),
                Expression::Literal(Value::Text("Dave".to_string())),
            ]],
        };

        let mut executor = InsertExecutor::new(statement);
        let result = executor.execute(&mut ctx)?;

        assert!(result.columns.is_empty());
        assert!(result.rows.is_empty());
        storage.flush()?;
        Ok(())
    }

    #[test]
    fn test_delete_executor_primary_key_lookup() -> Result<()> {
        let mut catalog = Schema::new();

        let columns = vec![
            Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                DataType::Integer,
            )
            .primary_key(true),
            Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                DataType::Text,
            ),
        ];
        let table_id = catalog.create_table("users".to_string(), columns)?;

        let db = TestDbFile::new("_test_delete_executor_primary_key_lookup");
        let mut storage = CatalogEngine::new(db.path())?;
        let root_page_id = storage.create_table("users")?;
        let primary_key_root_page_id = storage.create_empty_btree()?;
        catalog.set_table_root_page(table_id, root_page_id)?;
        catalog.set_table_primary_key_root_page(table_id, primary_key_root_page_id)?;

        let table = catalog.get_table_by_name("users").unwrap().clone();
        let row_id_1 = storage.insert_into_table(
            "users",
            vec![Value::Integer(1), Value::Text("Alice".to_string())],
        )?;
        let row_id_2 = storage.insert_into_table(
            "users",
            vec![Value::Integer(2), Value::Text("Bob".to_string())],
        )?;
        storage.register_primary_key_row(
            &table,
            crate::catalog::StoredRow {
                row_id: row_id_1,
                values: vec![Value::Integer(1), Value::Text("Alice".to_string())],
            },
        )?;
        storage.register_primary_key_row(
            &table,
            crate::catalog::StoredRow {
                row_id: row_id_2,
                values: vec![Value::Integer(2), Value::Text("Bob".to_string())],
            },
        )?;

        let mut ctx = ExecutionContext::for_mutation(&catalog, &mut storage);
        let statement = DeleteStatement {
            table: "users".to_string(),
            where_clause: Some(WhereClause {
                conditions: vec![Condition::Comparison {
                    left: Expression::Column("id".to_string()),
                    operator: ComparisonOperator::Equal,
                    right: Expression::Literal(Value::Integer(2)),
                }],
            }),
        };

        let mut executor = DeleteExecutor::new(
            statement,
            crate::query::planner::SelectAccessPath::PrimaryKeyLookup,
        );
        let result = executor.execute(&mut ctx)?;

        assert_eq!(result.affected_rows, 1);
        assert_eq!(ctx.engine.read_from_table("users")?.len(), 1);
        assert!(ctx
            .engine
            .lookup_row_by_primary_key(&table, &[Value::Integer(2)])?
            .is_none());
        storage.flush()?;
        Ok(())
    }

    #[test]
    fn test_update_executor_primary_key_lookup() -> Result<()> {
        let mut catalog = Schema::new();

        let columns = vec![
            Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                DataType::Integer,
            )
            .primary_key(true),
            Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                DataType::Text,
            ),
        ];
        let table_id = catalog.create_table("users".to_string(), columns)?;

        let db = TestDbFile::new("_test_update_executor_primary_key_lookup");
        let mut storage = CatalogEngine::new(db.path())?;
        let root_page_id = storage.create_table("users")?;
        let primary_key_root_page_id = storage.create_empty_btree()?;
        catalog.set_table_root_page(table_id, root_page_id)?;
        catalog.set_table_primary_key_root_page(table_id, primary_key_root_page_id)?;

        let table = catalog.get_table_by_name("users").unwrap().clone();
        let row_id_1 = storage.insert_into_table(
            "users",
            vec![Value::Integer(1), Value::Text("Alice".to_string())],
        )?;
        let row_id_2 = storage.insert_into_table(
            "users",
            vec![Value::Integer(2), Value::Text("Bob".to_string())],
        )?;
        storage.register_primary_key_row(
            &table,
            crate::catalog::StoredRow {
                row_id: row_id_1,
                values: vec![Value::Integer(1), Value::Text("Alice".to_string())],
            },
        )?;
        storage.register_primary_key_row(
            &table,
            crate::catalog::StoredRow {
                row_id: row_id_2,
                values: vec![Value::Integer(2), Value::Text("Bob".to_string())],
            },
        )?;

        let mut ctx = ExecutionContext::for_mutation(&catalog, &mut storage);
        let statement = UpdateStatement {
            table: "users".to_string(),
            assignments: vec![UpdateAssignment {
                column: "name".to_string(),
                value: Expression::Literal(Value::Text("Bobby".to_string())),
            }],
            where_clause: Some(WhereClause {
                conditions: vec![Condition::Comparison {
                    left: Expression::Column("id".to_string()),
                    operator: ComparisonOperator::Equal,
                    right: Expression::Literal(Value::Integer(2)),
                }],
            }),
        };

        let mut executor = UpdateExecutor::new(
            statement,
            crate::query::planner::SelectAccessPath::PrimaryKeyLookup,
        );
        let result = executor.execute(&mut ctx)?;

        assert_eq!(result.affected_rows, 1);
        let row = ctx
            .engine
            .lookup_row_by_primary_key(&table, &[Value::Integer(2)])?
            .expect("updated row should exist");
        assert_eq!(
            row.values,
            vec![Value::Integer(2), Value::Text("Bobby".to_string())]
        );
        storage.flush()?;
        Ok(())
    }

    #[test]
    fn test_create_executor() -> Result<()> {
        let catalog = Schema::new();
        let db = TestDbFile::new("_test_create_executor");
        let mut storage = CatalogEngine::new(db.path())?;
        let mut ctx = ExecutionContext::for_mutation(&catalog, &mut storage);

        let statement = CreateStatement {
            table: "test_table".to_string(),
            columns: vec![ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                unique: false,
                default_value: None,
            }],
        };

        let mut executor = CreateExecutor::new(statement);
        let result = executor.execute(&mut ctx)?;

        assert!(result.columns.is_empty());
        assert!(result.rows.is_empty());
        assert!(ctx.catalog.get_table_by_name("test_table").is_some());
        storage.flush()?;
        Ok(())
    }
}

mod optimizer_tests {
    use crate::catalog::types::DataType;
    use crate::catalog::Schema;
    use crate::error::Result;
    use crate::query::optimizer::*;
    use crate::query::planner::SelectAnalysis;

    #[test]
    fn test_query_optimizer() -> Result<()> {
        let mut catalog = Schema::new();

        // Create test table
        let columns = vec![
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                DataType::Integer,
            )
            .primary_key(true),
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                DataType::Text,
            ),
        ];
        catalog.create_table("users".to_string(), columns)?;

        let optimizer = QueryOptimizer::new(catalog);

        let analysis = SelectAnalysis {
            table_name: "users".to_string(),
            source_count: 1,
            table_id: crate::catalog::TableId::new(1),
            rowid_lookup: None,
            estimated_rows: 1000,
            usable_indexes: vec![],
            accessed_columns: vec![],
        };

        let optimizations = optimizer.optimize_select(&analysis)?;

        assert_eq!(optimizations.recommended_index_scans.len(), 0);
        // Allow either true or false for covering index recommendation
        Ok(())
    }
}

mod planner_tests {
    use crate::catalog::types::{DataType, Value};
    use crate::catalog::Schema;
    use crate::error::Result;
    use crate::parser::ast::*;
    use crate::query::planner::*;
    use std::collections::HashMap;

    #[test]
    fn test_query_planner_select() -> Result<()> {
        let mut catalog = Schema::new();

        // Create test table
        let columns = vec![
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                DataType::Integer,
            )
            .primary_key(true),
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                DataType::Text,
            ),
        ];
        catalog.create_table("users".to_string(), columns)?;

        let planner = QueryPlanner::new(catalog);

        let statement = SelectStatement {
            distinct: false,
            columns: vec![SelectItem::Column("id".to_string())],
            column_aliases: vec![None],
            from: TableReference::Table("users".to_string(), None),
            where_clause: Some(WhereClause {
                conditions: vec![Condition::Comparison {
                    left: Expression::Column("id".to_string()),
                    operator: ComparisonOperator::Equal,
                    right: Expression::Literal(Value::Integer(1)),
                }],
            }),
            group_by: Vec::new(),
            having_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operation: None,
        };

        let plan = planner.plan(Statement::Select(statement))?;

        assert!(plan.estimated_cost > 0.0);
        match &plan.node {
            PlanNode::Select(node) => {
                assert_eq!(node.table_name, "users");
                assert_eq!(node.access_path, SelectAccessPath::PrimaryKeyLookup);
                assert_eq!(
                    node.projection,
                    SelectProjection::Columns(vec!["id".to_string()])
                );
                assert!(node.has_filter);
            }
            other => panic!("expected select plan node, got {:?}", other),
        }
        assert!(plan.select_analysis.is_some());
        let optimizations = plan
            .optimizations
            .expect("select plans should be optimized");
        assert_eq!(optimizations.recommended_index_scans.len(), 1);
        Ok(())
    }

    #[test]
    fn test_query_planner_select_uses_secondary_index() -> Result<()> {
        let mut catalog = Schema::new();

        let mut table = crate::catalog::Table::new(
            crate::catalog::TableId::new(1),
            "users".to_string(),
            vec![
                crate::catalog::Column::new(
                    crate::catalog::ColumnId::new(1),
                    "id".to_string(),
                    DataType::Integer,
                )
                .primary_key(true),
                crate::catalog::Column::new(
                    crate::catalog::ColumnId::new(2),
                    "email".to_string(),
                    DataType::Text,
                ),
            ],
            10u32,
        )?;
        table.add_secondary_index(crate::catalog::SecondaryIndex {
            name: "idx_users_email".to_string(),
            column_indices: vec![1],
            root_page_id: 11u32.into(),
            unique: false,
        })?;
        catalog.insert_table(table)?;

        let planner = QueryPlanner::new(catalog);

        let statement = SelectStatement {
            distinct: false,
            columns: vec![SelectItem::Column("id".to_string())],
            column_aliases: vec![None],
            from: TableReference::Table("users".to_string(), None),
            where_clause: Some(WhereClause {
                conditions: vec![Condition::Comparison {
                    left: Expression::Column("email".to_string()),
                    operator: ComparisonOperator::Equal,
                    right: Expression::Literal(Value::Text("a@example.com".to_string())),
                }],
            }),
            group_by: Vec::new(),
            having_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operation: None,
        };

        let plan = planner.plan(Statement::Select(statement))?;

        match &plan.node {
            PlanNode::Select(node) => {
                assert_eq!(
                    node.access_path,
                    SelectAccessPath::SecondaryIndexLookup("idx_users_email".to_string())
                );
            }
            other => panic!("expected select plan node, got {:?}", other),
        }

        Ok(())
    }

    #[test]
    fn test_query_planner_select_uses_join_scan_for_multi_table_sources() -> Result<()> {
        let mut catalog = Schema::new();

        catalog.create_table(
            "users".to_string(),
            vec![
                crate::catalog::Column::new(
                    crate::catalog::ColumnId::new(1),
                    "id".to_string(),
                    DataType::Integer,
                )
                .primary_key(true),
                crate::catalog::Column::new(
                    crate::catalog::ColumnId::new(2),
                    "name".to_string(),
                    DataType::Text,
                ),
            ],
        )?;
        catalog.create_table(
            "posts".to_string(),
            vec![
                crate::catalog::Column::new(
                    crate::catalog::ColumnId::new(3),
                    "id".to_string(),
                    DataType::Integer,
                )
                .primary_key(true),
                crate::catalog::Column::new(
                    crate::catalog::ColumnId::new(4),
                    "user_id".to_string(),
                    DataType::Integer,
                ),
            ],
        )?;

        let planner = QueryPlanner::new(catalog);
        let statement = SelectStatement {
            distinct: false,
            columns: vec![SelectItem::Column("u.name".to_string())],
            column_aliases: vec![None],
            from: TableReference::InnerJoin {
                left: Box::new(TableReference::Table(
                    "users".to_string(),
                    Some("u".to_string()),
                )),
                right: Box::new(TableReference::Table(
                    "posts".to_string(),
                    Some("p".to_string()),
                )),
                on: Condition::Comparison {
                    left: Expression::Column("u.id".to_string()),
                    operator: ComparisonOperator::Equal,
                    right: Expression::Column("p.user_id".to_string()),
                },
            },
            where_clause: None,
            group_by: Vec::new(),
            having_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operation: None,
        };

        let plan = planner.plan(Statement::Select(statement))?;

        match &plan.node {
            PlanNode::Select(node) => {
                assert_eq!(node.access_path, SelectAccessPath::JoinScan);
                assert_eq!(node.source_count, 2);
            }
            other => panic!("expected select plan node, got {:?}", other),
        }

        Ok(())
    }

    #[test]
    fn test_query_planner_delete_uses_primary_key_lookup() -> Result<()> {
        let mut catalog = Schema::new();

        let columns = vec![
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                DataType::Integer,
            )
            .primary_key(true),
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                DataType::Text,
            ),
        ];
        catalog.create_table("users".to_string(), columns)?;

        let planner = QueryPlanner::new(catalog);
        let statement = DeleteStatement {
            table: "users".to_string(),
            where_clause: Some(WhereClause {
                conditions: vec![Condition::Comparison {
                    left: Expression::Column("id".to_string()),
                    operator: ComparisonOperator::Equal,
                    right: Expression::Literal(Value::Integer(1)),
                }],
            }),
        };

        let plan = planner.plan(Statement::Delete(statement))?;

        match &plan.node {
            PlanNode::Delete(node) => {
                assert!(node.has_filter);
                assert_eq!(node.access_path, SelectAccessPath::PrimaryKeyLookup);
            }
            other => panic!("expected delete plan node, got {:?}", other),
        }

        Ok(())
    }

    #[test]
    fn test_query_planner_update_uses_primary_key_lookup() -> Result<()> {
        let mut catalog = Schema::new();

        let columns = vec![
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                DataType::Integer,
            )
            .primary_key(true),
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                DataType::Text,
            ),
        ];
        catalog.create_table("users".to_string(), columns)?;

        let planner = QueryPlanner::new(catalog);
        let statement = UpdateStatement {
            table: "users".to_string(),
            assignments: vec![UpdateAssignment {
                column: "name".to_string(),
                value: Expression::Literal(Value::Text("Updated".to_string())),
            }],
            where_clause: Some(WhereClause {
                conditions: vec![Condition::Comparison {
                    left: Expression::Column("id".to_string()),
                    operator: ComparisonOperator::Equal,
                    right: Expression::Literal(Value::Integer(1)),
                }],
            }),
        };

        let plan = planner.plan(Statement::Update(statement))?;

        match &plan.node {
            PlanNode::Update(node) => {
                assert!(node.has_filter);
                assert_eq!(node.access_path, SelectAccessPath::PrimaryKeyLookup);
            }
            other => panic!("expected update plan node, got {:?}", other),
        }

        Ok(())
    }

    #[test]
    fn test_query_planner_costs_favor_locator_access_paths() -> Result<()> {
        let mut catalog = Schema::new();

        let columns = vec![
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                DataType::Integer,
            )
            .primary_key(true),
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                DataType::Text,
            ),
        ];
        catalog.create_table("users".to_string(), columns)?;

        let mut row_counts = HashMap::new();
        row_counts.insert("users".to_string(), 10_000usize);
        let planner = QueryPlanner::new(catalog).with_table_row_counts(row_counts);

        let full_scan = planner.plan(Statement::Select(SelectStatement {
            distinct: false,
            columns: vec![SelectItem::Column("name".to_string())],
            column_aliases: vec![None],
            from: TableReference::Table("users".to_string(), None),
            where_clause: None,
            group_by: Vec::new(),
            having_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operation: None,
        }))?;
        let rowid_lookup = planner.plan(Statement::Select(SelectStatement {
            distinct: false,
            columns: vec![SelectItem::Column("name".to_string())],
            column_aliases: vec![None],
            from: TableReference::Table("users".to_string(), None),
            where_clause: Some(WhereClause {
                conditions: vec![Condition::Comparison {
                    left: Expression::Column("rowid".to_string()),
                    operator: ComparisonOperator::Equal,
                    right: Expression::Literal(Value::Integer(7)),
                }],
            }),
            group_by: Vec::new(),
            having_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operation: None,
        }))?;
        let delete_full_scan = planner.plan(Statement::Delete(DeleteStatement {
            table: "users".to_string(),
            where_clause: None,
        }))?;
        let delete_pk_lookup = planner.plan(Statement::Delete(DeleteStatement {
            table: "users".to_string(),
            where_clause: Some(WhereClause {
                conditions: vec![Condition::Comparison {
                    left: Expression::Column("id".to_string()),
                    operator: ComparisonOperator::Equal,
                    right: Expression::Literal(Value::Integer(1)),
                }],
            }),
        }))?;

        assert!(rowid_lookup.estimated_cost < full_scan.estimated_cost);
        assert!(delete_pk_lookup.estimated_cost < delete_full_scan.estimated_cost);

        Ok(())
    }

    #[test]
    fn test_query_planner_select_uses_rowid_lookup() -> Result<()> {
        let mut catalog = Schema::new();

        let columns = vec![
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                DataType::Integer,
            )
            .primary_key(true),
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                DataType::Text,
            ),
        ];
        catalog.create_table("users".to_string(), columns)?;

        let planner = QueryPlanner::new(catalog);
        let statement = SelectStatement {
            distinct: false,
            columns: vec![SelectItem::Column("id".to_string())],
            column_aliases: vec![None],
            from: TableReference::Table("users".to_string(), None),
            where_clause: Some(WhereClause {
                conditions: vec![Condition::Comparison {
                    left: Expression::Column("rowid".to_string()),
                    operator: ComparisonOperator::Equal,
                    right: Expression::Literal(Value::Integer(7)),
                }],
            }),
            group_by: Vec::new(),
            having_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operation: None,
        };

        let plan = planner.plan(Statement::Select(statement))?;
        match &plan.node {
            PlanNode::Select(node) => {
                assert_eq!(node.access_path, SelectAccessPath::RowIdLookup);
            }
            other => panic!("expected select plan node, got {:?}", other),
        }
        assert_eq!(
            plan.select_analysis.as_ref().and_then(|a| a.rowid_lookup),
            Some(7)
        );
        Ok(())
    }

    #[test]
    fn test_query_planner_uses_metadata_backed_row_estimate() -> Result<()> {
        let mut catalog = Schema::new();

        let columns = vec![
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                DataType::Integer,
            )
            .primary_key(true),
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                DataType::Text,
            ),
        ];
        catalog.create_table("users".to_string(), columns)?;

        let planner = QueryPlanner::new(catalog)
            .with_table_row_counts(std::collections::HashMap::from([("users".to_string(), 12)]));

        let statement = SelectStatement {
            distinct: false,
            columns: vec![SelectItem::Column("id".to_string())],
            column_aliases: vec![None],
            from: TableReference::Table("users".to_string(), None),
            where_clause: None,
            group_by: Vec::new(),
            having_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operation: None,
        };

        let plan = planner.plan(Statement::Select(statement))?;

        assert_eq!(plan.select_analysis.unwrap().estimated_rows, 12);
        Ok(())
    }

    #[test]
    fn test_query_planner_insert() -> Result<()> {
        let mut catalog = Schema::new();

        // Create test table first
        let columns = vec![
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                DataType::Integer,
            )
            .primary_key(true),
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                DataType::Text,
            ),
        ];
        catalog.create_table("users".to_string(), columns)?;

        let planner = QueryPlanner::new(catalog);

        let statement = InsertStatement {
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            values: vec![vec![
                Expression::Literal(Value::Integer(1)),
                Expression::Literal(Value::Text("Alice".to_string())),
            ]],
        };

        let plan = planner.plan(Statement::Insert(statement))?;

        match &plan.node {
            PlanNode::Insert(node) => {
                assert_eq!(node.table_name, "users");
                assert_eq!(node.row_count, 1);
            }
            other => panic!("expected insert plan node, got {:?}", other),
        }
        assert_eq!(plan.estimated_cost, 1.0); // One row
        Ok(())
    }

    #[test]
    fn test_query_planner_create() -> Result<()> {
        let catalog = Schema::new();
        let planner = QueryPlanner::new(catalog);

        let statement = CreateStatement {
            table: "test_table".to_string(),
            columns: vec![ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                unique: false,
                default_value: None,
            }],
        };

        let plan = planner.plan(Statement::Create(statement))?;

        match &plan.node {
            PlanNode::Create(node) => {
                assert_eq!(node.table_name, "test_table");
                assert_eq!(node.column_count, 1);
            }
            other => panic!("expected create plan node, got {:?}", other),
        }
        assert_eq!(plan.estimated_cost, 1.0); // Fixed cost for CREATE
        Ok(())
    }
}
