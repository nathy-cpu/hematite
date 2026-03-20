//! Centralized tests for the query module

mod executor_tests {
    use crate::catalog::types::{DataType, Value};
    use crate::catalog::{Column, Schema};
    use crate::error::Result;
    use crate::parser::ast::*;
    use crate::query::executor::*;
    use crate::storage::StorageEngine;
    use crate::test_utils::TestDbFile;

    #[test]
    fn test_select_executor_debug() -> Result<()> {
        println!("=== Starting Select Executor Debug Test ===");

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

        let db = TestDbFile::new("_test_select_executor_debug");
        let mut storage = StorageEngine::new(db.path())?;
        // Create table in storage as well
        let _ = storage.create_table("users")?;

        // Add some test data
        println!("✓ Inserting row 1");
        storage.insert_into_table(
            "users",
            vec![Value::Integer(1), Value::Text("Alice".to_string())],
        )?;
        println!("✓ Inserting row 2");
        storage.insert_into_table(
            "users",
            vec![Value::Integer(2), Value::Text("Bob".to_string())],
        )?;
        println!("✓ Inserting row 3");
        storage.insert_into_table(
            "users",
            vec![Value::Integer(3), Value::Text("Charlie".to_string())],
        )?;

        // Debug: Read all rows directly from storage
        println!("✓ Reading all rows from storage...");
        let all_rows = storage.read_from_table("users")?;
        println!("✓ Found {} rows in storage", all_rows.len());
        for (i, row) in all_rows.iter().enumerate() {
            println!("✓ Row {}: {:?}", i, row);
        }

        let mut ctx = ExecutionContext::for_read(&catalog, &mut storage);

        let statement = SelectStatement {
            columns: vec![SelectItem::Column("id".to_string())],
            from: TableReference::Table("users".to_string()),
            where_clause: None,
            order_by: Vec::new(),
            limit: None,
        };

        let mut executor = SelectExecutor::new(statement);
        let result = executor.execute(&mut ctx)?;

        println!("✓ Query result columns: {:?}", result.columns);
        println!("✓ Query result rows: {}", result.rows.len());
        for (i, row) in result.rows.iter().enumerate() {
            println!("✓ Query row {}: {:?}", i, row);
        }

        assert_eq!(result.columns, vec!["id"]);
        assert_eq!(result.rows.len(), 3); // 3 simulated rows
        println!("✓ SUCCESS: Select executor test passed");
        storage.flush()?;
        Ok(())
    }

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
        let mut storage = StorageEngine::new(db.path())?;
        // Create table in storage as well
        let _ = storage.create_table("users")?;

        // Add some test data
        storage.insert_into_table(
            "users",
            vec![Value::Integer(1), Value::Text("Alice".to_string())],
        )?;
        storage.insert_into_table(
            "users",
            vec![Value::Integer(2), Value::Text("Bob".to_string())],
        )?;
        storage.insert_into_table(
            "users",
            vec![Value::Integer(3), Value::Text("Charlie".to_string())],
        )?;
        let mut ctx = ExecutionContext::for_mutation(&catalog, &mut storage);

        let statement = SelectStatement {
            columns: vec![SelectItem::Column("id".to_string())],
            from: TableReference::Table("users".to_string()),
            where_clause: None,
            order_by: Vec::new(),
            limit: None,
        };

        let mut executor = SelectExecutor::new(statement);
        let result = executor.execute(&mut ctx)?;

        assert_eq!(result.columns, vec!["id"]);
        assert_eq!(result.rows.len(), 3); // 3 simulated rows
        storage.flush()?;
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
        catalog.create_table("users".to_string(), columns)?;

        let db = TestDbFile::new("_test_insert_executor");
        let mut storage = StorageEngine::new(db.path())?;
        // Create table in storage as well
        let _ = storage.create_table("users")?;
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
    fn test_create_executor() -> Result<()> {
        let catalog = Schema::new();
        let db = TestDbFile::new("_test_create_executor");
        let mut storage = StorageEngine::new(db.path())?;
        let mut ctx = ExecutionContext::for_mutation(&catalog, &mut storage);

        let statement = CreateStatement {
            table: "test_table".to_string(),
            columns: vec![ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
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
            table_id: crate::catalog::TableId::new(1),
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
            columns: vec![SelectItem::Column("id".to_string())],
            from: TableReference::Table("users".to_string()),
            where_clause: None,
            order_by: Vec::new(),
            limit: None,
        };

        let plan = planner.plan(Statement::Select(statement))?;

        assert!(plan.estimated_cost > 0.0);
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
                default_value: None,
            }],
        };

        let plan = planner.plan(Statement::Create(statement))?;

        assert_eq!(plan.estimated_cost, 1.0); // Fixed cost for CREATE
        Ok(())
    }
}
