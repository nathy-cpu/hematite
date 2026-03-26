//! Centralized tests for the parser module

mod ast_tests {
    use crate::catalog::types::DataType;
    use crate::catalog::Value;
    use crate::error::Result;
    use crate::parser::ast::*;

    #[test]
    fn test_select_statement_validation() -> Result<()> {
        let mut catalog = crate::catalog::Schema::new();

        // Create a test table
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

        let select = SelectStatement {
            with_clause: Vec::new(),
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

        assert!(select.validate(&catalog).is_ok());
        Ok(())
    }

    #[test]
    fn test_invalid_column_reference() {
        let mut catalog = crate::catalog::Schema::new();

        let columns = vec![crate::catalog::Column::new(
            crate::catalog::ColumnId::new(1),
            "id".to_string(),
            DataType::Integer,
        )
        .primary_key(true)];
        catalog.create_table("users".to_string(), columns).unwrap();

        let select = SelectStatement {
            with_clause: Vec::new(),
            distinct: false,
            columns: vec![SelectItem::Column("invalid".to_string())],
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

        assert!(select.validate(&catalog).is_err());
    }

    #[test]
    fn test_invalid_where_column_reference() {
        let mut catalog = crate::catalog::Schema::new();

        let columns = vec![crate::catalog::Column::new(
            crate::catalog::ColumnId::new(1),
            "id".to_string(),
            DataType::Integer,
        )
        .primary_key(true)];
        catalog.create_table("users".to_string(), columns).unwrap();

        let select = SelectStatement {
            with_clause: Vec::new(),
            distinct: false,
            columns: vec![SelectItem::Wildcard],
            column_aliases: vec![None],
            from: TableReference::Table("users".to_string(), None),
            where_clause: Some(WhereClause {
                conditions: vec![Condition::Comparison {
                    left: Expression::Column("missing".to_string()),
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

        assert!(select.validate(&catalog).is_err());
    }

    #[test]
    fn test_group_by_rejects_non_grouped_column_projection() {
        let mut catalog = crate::catalog::Schema::new();

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
        catalog.create_table("users".to_string(), columns).unwrap();

        let select = SelectStatement {
            with_clause: Vec::new(),
            distinct: false,
            columns: vec![
                SelectItem::Column("id".to_string()),
                SelectItem::Aggregate {
                    function: AggregateFunction::Count,
                    column: "name".to_string(),
                },
            ],
            column_aliases: vec![None, Some("name_count".to_string())],
            from: TableReference::Table("users".to_string(), None),
            where_clause: None,
            group_by: vec![Expression::Column("name".to_string())],
            having_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operation: None,
        };

        assert!(select.validate(&catalog).is_err());
    }

    #[test]
    fn test_multi_table_column_resolution_requires_qualification_when_ambiguous() -> Result<()> {
        let mut catalog = crate::catalog::Schema::new();
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

        let ambiguous = SelectStatement {
            with_clause: Vec::new(),
            distinct: false,
            columns: vec![SelectItem::Column("id".to_string())],
            column_aliases: vec![None],
            from: TableReference::CrossJoin(
                Box::new(TableReference::Table(
                    "users".to_string(),
                    Some("u".to_string()),
                )),
                Box::new(TableReference::Table(
                    "posts".to_string(),
                    Some("p".to_string()),
                )),
            ),
            where_clause: None,
            group_by: Vec::new(),
            having_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operation: None,
        };

        assert!(ambiguous.validate(&catalog).is_err());

        let qualified = SelectStatement {
            columns: vec![SelectItem::Column("u.id".to_string())],
            ..ambiguous.clone()
        };

        assert!(qualified.validate(&catalog).is_ok());
        Ok(())
    }
}

mod lexer_tests {
    use crate::error::Result;
    use crate::parser::lexer::*;

    #[test]
    fn test_simple_select() -> Result<()> {
        let mut lexer = Lexer::new("SELECT * FROM users".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Select,
            Token::Asterisk,
            Token::From,
            Token::Identifier("users".to_string()),
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_distinct_statement() -> Result<()> {
        let mut lexer = Lexer::new("SELECT DISTINCT name FROM users;".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Select,
            Token::Distinct,
            Token::Identifier("name".to_string()),
            Token::From,
            Token::Identifier("users".to_string()),
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_backtick_identifier_and_type_alias_tokens() -> Result<()> {
        let mut lexer = Lexer::new(
            "CREATE TABLE `user data` (`id` INT PRIMARY KEY, `active` BOOL, `score` DOUBLE, `name` VARCHAR(32));"
                .to_string(),
        );
        lexer.tokenize()?;

        let expected = vec![
            Token::Create,
            Token::Table,
            Token::Identifier("user data".to_string()),
            Token::LeftParen,
            Token::Identifier("id".to_string()),
            Token::Int,
            Token::Primary,
            Token::Key,
            Token::Comma,
            Token::Identifier("active".to_string()),
            Token::Bool,
            Token::Comma,
            Token::Identifier("score".to_string()),
            Token::Double,
            Token::Comma,
            Token::Identifier("name".to_string()),
            Token::Varchar,
            Token::LeftParen,
            Token::NumberLiteral(32.0),
            Token::RightParen,
            Token::RightParen,
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_in_statement() -> Result<()> {
        let mut lexer = Lexer::new("SELECT id FROM users WHERE id IN (1, 2, 3);".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Select,
            Token::Identifier("id".to_string()),
            Token::From,
            Token::Identifier("users".to_string()),
            Token::Where,
            Token::Identifier("id".to_string()),
            Token::In,
            Token::LeftParen,
            Token::NumberLiteral(1.0),
            Token::Comma,
            Token::NumberLiteral(2.0),
            Token::Comma,
            Token::NumberLiteral(3.0),
            Token::RightParen,
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_between_statement() -> Result<()> {
        let mut lexer = Lexer::new("SELECT id FROM users WHERE id BETWEEN 1 AND 3;".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Select,
            Token::Identifier("id".to_string()),
            Token::From,
            Token::Identifier("users".to_string()),
            Token::Where,
            Token::Identifier("id".to_string()),
            Token::Between,
            Token::NumberLiteral(1.0),
            Token::And,
            Token::NumberLiteral(3.0),
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_transaction_tokens() -> Result<()> {
        let mut lexer = Lexer::new("BEGIN; COMMIT; ROLLBACK;".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Begin,
            Token::Semicolon,
            Token::Commit,
            Token::Semicolon,
            Token::Rollback,
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_select_with_where_and_and() -> Result<()> {
        let mut lexer = Lexer::new("SELECT id FROM users WHERE id = 1 AND id != 2".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Select,
            Token::Identifier("id".to_string()),
            Token::From,
            Token::Identifier("users".to_string()),
            Token::Where,
            Token::Identifier("id".to_string()),
            Token::Equal,
            Token::NumberLiteral(1.0),
            Token::And,
            Token::Identifier("id".to_string()),
            Token::NotEqual,
            Token::NumberLiteral(2.0),
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_limit_statement() -> Result<()> {
        let mut lexer = Lexer::new("SELECT id FROM users ORDER BY name DESC LIMIT 5;".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Select,
            Token::Identifier("id".to_string()),
            Token::From,
            Token::Identifier("users".to_string()),
            Token::Order,
            Token::By,
            Token::Identifier("name".to_string()),
            Token::Desc,
            Token::Limit,
            Token::NumberLiteral(5.0),
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_offset_statement() -> Result<()> {
        let mut lexer =
            Lexer::new("SELECT id FROM users ORDER BY name DESC LIMIT 5 OFFSET 2;".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Select,
            Token::Identifier("id".to_string()),
            Token::From,
            Token::Identifier("users".to_string()),
            Token::Order,
            Token::By,
            Token::Identifier("name".to_string()),
            Token::Desc,
            Token::Limit,
            Token::NumberLiteral(5.0),
            Token::Offset,
            Token::NumberLiteral(2.0),
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_count_statement() -> Result<()> {
        let mut lexer = Lexer::new("SELECT COUNT(*) FROM users;".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Select,
            Token::Count,
            Token::LeftParen,
            Token::Asterisk,
            Token::RightParen,
            Token::From,
            Token::Identifier("users".to_string()),
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_aggregate_statement() -> Result<()> {
        let mut lexer = Lexer::new("SELECT SUM(score) FROM users;".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Select,
            Token::Sum,
            Token::LeftParen,
            Token::Identifier("score".to_string()),
            Token::RightParen,
            Token::From,
            Token::Identifier("users".to_string()),
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_group_by_having_statement() -> Result<()> {
        let mut lexer = Lexer::new(
            "SELECT name, COUNT(id) FROM users GROUP BY name HAVING name = 'Alice';".to_string(),
        );
        lexer.tokenize()?;

        let expected = vec![
            Token::Select,
            Token::Identifier("name".to_string()),
            Token::Comma,
            Token::Count,
            Token::LeftParen,
            Token::Identifier("id".to_string()),
            Token::RightParen,
            Token::From,
            Token::Identifier("users".to_string()),
            Token::Group,
            Token::By,
            Token::Identifier("name".to_string()),
            Token::Having,
            Token::Identifier("name".to_string()),
            Token::Equal,
            Token::StringLiteral("Alice".to_string()),
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_placeholder_statement() -> Result<()> {
        let mut lexer = Lexer::new("SELECT * FROM users WHERE id = ?;".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Select,
            Token::Asterisk,
            Token::From,
            Token::Identifier("users".to_string()),
            Token::Where,
            Token::Identifier("id".to_string()),
            Token::Equal,
            Token::Placeholder,
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_order_by_statement() -> Result<()> {
        let mut lexer = Lexer::new("SELECT id FROM users ORDER BY name DESC, id ASC;".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Select,
            Token::Identifier("id".to_string()),
            Token::From,
            Token::Identifier("users".to_string()),
            Token::Order,
            Token::By,
            Token::Identifier("name".to_string()),
            Token::Desc,
            Token::Comma,
            Token::Identifier("id".to_string()),
            Token::Asc,
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_insert_statement() -> Result<()> {
        let mut lexer = Lexer::new("INSERT INTO users (id, name) VALUES (1, 'John')".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Insert,
            Token::Into,
            Token::Identifier("users".to_string()),
            Token::LeftParen,
            Token::Identifier("id".to_string()),
            Token::Comma,
            Token::Identifier("name".to_string()),
            Token::RightParen,
            Token::Values,
            Token::LeftParen,
            Token::NumberLiteral(1.0),
            Token::Comma,
            Token::StringLiteral("John".to_string()),
            Token::RightParen,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_create_and_drop_index_tokens() -> Result<()> {
        let mut lexer = Lexer::new(
            "CREATE INDEX idx_users_name ON users (name); DROP INDEX idx_users_name ON users;"
                .to_string(),
        );
        lexer.tokenize()?;

        let expected = vec![
            Token::Create,
            Token::Index,
            Token::Identifier("idx_users_name".to_string()),
            Token::On,
            Token::Identifier("users".to_string()),
            Token::LeftParen,
            Token::Identifier("name".to_string()),
            Token::RightParen,
            Token::Semicolon,
            Token::Drop,
            Token::Index,
            Token::Identifier("idx_users_name".to_string()),
            Token::On,
            Token::Identifier("users".to_string()),
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_delete_statement() -> Result<()> {
        let mut lexer = Lexer::new("DELETE FROM users WHERE id = 1;".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Delete,
            Token::From,
            Token::Identifier("users".to_string()),
            Token::Where,
            Token::Identifier("id".to_string()),
            Token::Equal,
            Token::NumberLiteral(1.0),
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_update_statement() -> Result<()> {
        let mut lexer =
            Lexer::new("UPDATE users SET name = 'John', active = TRUE WHERE id = 1;".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Update,
            Token::Identifier("users".to_string()),
            Token::Set,
            Token::Identifier("name".to_string()),
            Token::Equal,
            Token::StringLiteral("John".to_string()),
            Token::Comma,
            Token::Identifier("active".to_string()),
            Token::Equal,
            Token::BooleanLiteral(true),
            Token::Where,
            Token::Identifier("id".to_string()),
            Token::Equal,
            Token::NumberLiteral(1.0),
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_is_null_statement() -> Result<()> {
        let mut lexer = Lexer::new("SELECT * FROM users WHERE name IS NOT NULL;".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Select,
            Token::Asterisk,
            Token::From,
            Token::Identifier("users".to_string()),
            Token::Where,
            Token::Identifier("name".to_string()),
            Token::Is,
            Token::Not,
            Token::Null,
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_drop_table_statement() -> Result<()> {
        let mut lexer = Lexer::new("DROP TABLE users;".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Drop,
            Token::Table,
            Token::Identifier("users".to_string()),
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_create_table() -> Result<()> {
        let mut lexer =
            Lexer::new("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Create,
            Token::Table,
            Token::Identifier("users".to_string()),
            Token::LeftParen,
            Token::Identifier("id".to_string()),
            Token::Integer,
            Token::Primary,
            Token::Key,
            Token::Comma,
            Token::Identifier("name".to_string()),
            Token::Text,
            Token::RightParen,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_unicode_identifier_and_string_literal() -> Result<()> {
        let mut lexer = Lexer::new("SELECT navn FROM brukere WHERE navn = 'Alíce';".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Select,
            Token::Identifier("navn".to_string()),
            Token::From,
            Token::Identifier("brukere".to_string()),
            Token::Where,
            Token::Identifier("navn".to_string()),
            Token::Equal,
            Token::StringLiteral("Alíce".to_string()),
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_string_literal_escaped_quotes() -> Result<()> {
        let mut lexer =
            Lexer::new("INSERT INTO users (name) VALUES ('O\\'Brien'), ('D''Angelo');".to_string());
        lexer.tokenize()?;

        assert_eq!(
            lexer.get_tokens(),
            &[
                Token::Insert,
                Token::Into,
                Token::Identifier("users".to_string()),
                Token::LeftParen,
                Token::Identifier("name".to_string()),
                Token::RightParen,
                Token::Values,
                Token::LeftParen,
                Token::StringLiteral("O'Brien".to_string()),
                Token::RightParen,
                Token::Comma,
                Token::LeftParen,
                Token::StringLiteral("D'Angelo".to_string()),
                Token::RightParen,
                Token::Semicolon,
            ]
        );
        Ok(())
    }
}

mod parser_tests {
    use crate::catalog::types::DataType;
    use crate::error::Result;
    use crate::parser::ast::*;
    use crate::parser::lexer::*;
    use crate::parser::parser::*;

    fn parse_statement(sql: &str) -> Result<Statement> {
        let mut lexer = Lexer::new(sql.to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        parser.parse()
    }

    fn parse_select(sql: &str) -> Result<SelectStatement> {
        match parse_statement(sql)? {
            Statement::Select(select) => Ok(select),
            other => Err(crate::error::HematiteError::InternalError(format!(
                "Expected SELECT statement, found {:?}",
                other
            ))),
        }
    }

    fn parse_insert(sql: &str) -> Result<InsertStatement> {
        match parse_statement(sql)? {
            Statement::Insert(insert) => Ok(insert),
            other => Err(crate::error::HematiteError::InternalError(format!(
                "Expected INSERT statement, found {:?}",
                other
            ))),
        }
    }

    fn parse_update(sql: &str) -> Result<UpdateStatement> {
        match parse_statement(sql)? {
            Statement::Update(update) => Ok(update),
            other => Err(crate::error::HematiteError::InternalError(format!(
                "Expected UPDATE statement, found {:?}",
                other
            ))),
        }
    }

    fn parse_delete(sql: &str) -> Result<DeleteStatement> {
        match parse_statement(sql)? {
            Statement::Delete(delete) => Ok(delete),
            other => Err(crate::error::HematiteError::InternalError(format!(
                "Expected DELETE statement, found {:?}",
                other
            ))),
        }
    }

    fn parse_create(sql: &str) -> Result<CreateStatement> {
        match parse_statement(sql)? {
            Statement::Create(create) => Ok(create),
            other => Err(crate::error::HematiteError::InternalError(format!(
                "Expected CREATE statement, found {:?}",
                other
            ))),
        }
    }

    fn parse_alter(sql: &str) -> Result<AlterStatement> {
        match parse_statement(sql)? {
            Statement::Alter(alter) => Ok(alter),
            other => Err(crate::error::HematiteError::InternalError(format!(
                "Expected ALTER statement, found {:?}",
                other
            ))),
        }
    }

    fn parse_create_index(sql: &str) -> Result<CreateIndexStatement> {
        match parse_statement(sql)? {
            Statement::CreateIndex(create_index) => Ok(create_index),
            other => Err(crate::error::HematiteError::InternalError(format!(
                "Expected CREATE INDEX statement, found {:?}",
                other
            ))),
        }
    }

    fn parse_drop_index(sql: &str) -> Result<DropIndexStatement> {
        match parse_statement(sql)? {
            Statement::DropIndex(drop_index) => Ok(drop_index),
            other => Err(crate::error::HematiteError::InternalError(format!(
                "Expected DROP INDEX statement, found {:?}",
                other
            ))),
        }
    }

    #[test]
    fn test_parse_simple_select() -> Result<()> {
        let select = parse_select("SELECT * FROM users;")?;
        assert_eq!(select.columns.len(), 1);
        assert!(matches!(select.columns[0], SelectItem::Wildcard));
        assert!(matches!(select.from, TableReference::Table(name, None) if name == "users"));
        assert!(select.where_clause.is_none());
        assert!(select.order_by.is_empty());
        assert!(select.limit.is_none());
        Ok(())
    }

    #[test]
    fn test_parse_select_with_where() -> Result<()> {
        let select = parse_select("SELECT id FROM users WHERE id = 1;")?;
        assert!(select.where_clause.is_some());
        assert!(select.order_by.is_empty());
        assert!(select.limit.is_none());
        assert_eq!(
            select.where_clause.as_ref().map(|w| w.conditions.len()),
            Some(1)
        );
        Ok(())
    }

    #[test]
    fn test_parse_distinct() -> Result<()> {
        let select = parse_select("SELECT DISTINCT name FROM users;")?;
        assert!(select.distinct);
        assert_eq!(select.columns.len(), 1);
        assert!(matches!(&select.columns[0], SelectItem::Column(name) if name == "name"));
        Ok(())
    }

    #[test]
    fn test_parse_in_condition() -> Result<()> {
        let select = parse_select("SELECT id FROM users WHERE id IN (1, 2);")?;
        let where_clause = select.where_clause.expect("missing WHERE clause");
        assert_eq!(where_clause.conditions.len(), 1);
        assert!(matches!(
            &where_clause.conditions[0],
            Condition::InList { is_not: false, values, .. } if values.len() == 2
        ));
        Ok(())
    }

    #[test]
    fn test_parse_between_condition() -> Result<()> {
        let select = parse_select("SELECT id FROM users WHERE id BETWEEN 1 AND 3;")?;
        let where_clause = select.where_clause.expect("missing WHERE clause");
        assert_eq!(where_clause.conditions.len(), 1);
        assert!(matches!(
            &where_clause.conditions[0],
            Condition::Between { is_not: false, .. }
        ));
        Ok(())
    }

    #[test]
    fn test_parse_like_condition() -> Result<()> {
        let select = parse_select("SELECT name FROM users WHERE name LIKE 'A%';")?;
        let where_clause = select.where_clause.expect("missing WHERE clause");
        assert_eq!(where_clause.conditions.len(), 1);
        assert!(matches!(
            &where_clause.conditions[0],
            Condition::Like { is_not: false, .. }
        ));
        Ok(())
    }

    #[test]
    fn test_parse_not_condition() -> Result<()> {
        let select = parse_select("SELECT id FROM users WHERE NOT (id = 1 OR id = 2);")?;
        let where_clause = select.where_clause.expect("missing WHERE clause");
        assert_eq!(where_clause.conditions.len(), 1);
        assert!(matches!(&where_clause.conditions[0], Condition::Not(_)));
        Ok(())
    }

    #[test]
    fn test_parse_arithmetic_expression() -> Result<()> {
        let select = parse_select("SELECT id + 1 AS next_id FROM users WHERE -id < 0;")?;
        assert!(matches!(
            &select.columns[0],
            SelectItem::Expression(Expression::Binary {
                operator: ArithmeticOperator::Add,
                ..
            })
        ));
        assert_eq!(select.column_aliases[0].as_deref(), Some("next_id"));
        let where_clause = select.where_clause.expect("missing WHERE clause");
        assert!(matches!(
            &where_clause.conditions[0],
            Condition::Comparison {
                left: Expression::UnaryMinus(_),
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn test_parse_order_by() -> Result<()> {
        let select = parse_select("SELECT id FROM users ORDER BY name DESC, id ASC;")?;
        assert_eq!(select.order_by.len(), 2);
        assert_eq!(select.order_by[0].column, "name");
        assert_eq!(select.order_by[0].direction, SortDirection::Desc);
        assert_eq!(select.order_by[1].column, "id");
        assert_eq!(select.order_by[1].direction, SortDirection::Asc);
        Ok(())
    }

    #[test]
    fn test_parse_limit() -> Result<()> {
        let select = parse_select("SELECT id FROM users ORDER BY name DESC LIMIT 5;")?;
        assert_eq!(select.order_by.len(), 1);
        assert_eq!(select.limit, Some(5));
        Ok(())
    }

    #[test]
    fn test_parse_offset() -> Result<()> {
        let select = parse_select("SELECT id FROM users ORDER BY name DESC LIMIT 5 OFFSET 2;")?;
        assert_eq!(select.order_by.len(), 1);
        assert_eq!(select.limit, Some(5));
        assert_eq!(select.offset, Some(2));
        Ok(())
    }

    #[test]
    fn test_parse_count() -> Result<()> {
        let select = parse_select("SELECT COUNT(*) FROM users;")?;
        assert_eq!(select.columns.len(), 1);
        assert!(matches!(select.columns[0], SelectItem::CountAll));
        Ok(())
    }

    #[test]
    fn test_parse_aggregate() -> Result<()> {
        let select = parse_select("SELECT MAX(score) FROM users;")?;
        assert_eq!(select.columns.len(), 1);
        assert!(matches!(
            select.columns[0],
            SelectItem::Aggregate {
                function: AggregateFunction::Max,
                ref column,
            } if column == "score"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_count_column() -> Result<()> {
        let select = parse_select("SELECT COUNT(score) FROM users;")?;
        assert!(matches!(
            select.columns[0],
            SelectItem::Aggregate {
                function: AggregateFunction::Count,
                ref column,
            } if column == "score"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_group_by_having() -> Result<()> {
        let select = parse_select(
            "SELECT name, COUNT(id) AS total_count FROM users GROUP BY name HAVING total_count > 1;",
        )?;
        assert_eq!(select.group_by.len(), 1);
        assert!(matches!(
            &select.group_by[0],
            Expression::Column(name) if name == "name"
        ));
        assert!(select.having_clause.is_some());
        assert_eq!(select.column_aliases[1].as_deref(), Some("total_count"));
        Ok(())
    }

    #[test]
    fn test_parse_where_operator_precedence() -> Result<()> {
        let select =
            parse_select("SELECT id FROM users WHERE id = 1 OR id = 2 AND active = TRUE;")?;
        let where_clause = select.where_clause.expect("missing WHERE clause");
        assert_eq!(where_clause.conditions.len(), 1);

        match &where_clause.conditions[0] {
            Condition::Logical {
                left,
                operator: LogicalOperator::Or,
                right,
            } => {
                assert!(matches!(**left, Condition::Comparison { .. }));
                assert!(matches!(
                    **right,
                    Condition::Logical {
                        operator: LogicalOperator::And,
                        ..
                    }
                ));
            }
            other => panic!("Expected OR condition at the root, found {:?}", other),
        }

        Ok(())
    }

    #[test]
    fn test_parse_parameterized_insert() -> Result<()> {
        let insert = parse_insert("INSERT INTO users (id, name) VALUES (?, ?);")?;
        assert!(matches!(insert.values[0][0], Expression::Parameter(0)));
        assert!(matches!(insert.values[0][1], Expression::Parameter(1)));

        Ok(())
    }

    #[test]
    fn test_parse_where_parentheses() -> Result<()> {
        let select =
            parse_select("SELECT id FROM users WHERE (id = 1 OR id = 2) AND active = TRUE;")?;
        let where_clause = select.where_clause.expect("missing WHERE clause");
        assert_eq!(where_clause.conditions.len(), 1);

        match &where_clause.conditions[0] {
            Condition::Logical {
                left,
                operator: LogicalOperator::And,
                right,
            } => {
                assert!(matches!(
                    **left,
                    Condition::Logical {
                        operator: LogicalOperator::Or,
                        ..
                    }
                ));
                assert!(matches!(**right, Condition::Comparison { .. }));
            }
            other => panic!("Expected AND condition at the root, found {:?}", other),
        }

        Ok(())
    }

    #[test]
    fn test_parse_insert() -> Result<()> {
        let insert = parse_insert("INSERT INTO users (id, name) VALUES (1, 'John');")?;
        assert_eq!(insert.table, "users");
        assert_eq!(insert.columns, vec!["id", "name"]);
        assert_eq!(insert.values.len(), 1);
        assert_eq!(insert.values[0].len(), 2);
        Ok(())
    }

    #[test]
    fn test_parse_delete() -> Result<()> {
        let delete = parse_delete("DELETE FROM users WHERE id = 1;")?;
        assert_eq!(delete.table, "users");
        assert!(delete.where_clause.is_some());
        Ok(())
    }

    #[test]
    fn test_parse_update() -> Result<()> {
        let update = parse_update("UPDATE users SET name = 'John', active = TRUE WHERE id = 1;")?;
        assert_eq!(update.table, "users");
        assert_eq!(update.assignments.len(), 2);
        assert!(update.where_clause.is_some());
        Ok(())
    }

    #[test]
    fn test_parse_is_null() -> Result<()> {
        let select = parse_select("SELECT * FROM users WHERE name IS NULL;")?;
        let where_clause = select.where_clause.expect("missing WHERE clause");
        assert_eq!(where_clause.conditions.len(), 1);
        assert!(matches!(
            where_clause.conditions[0],
            Condition::NullCheck { is_not: false, .. }
        ));
        Ok(())
    }

    #[test]
    fn test_parse_drop() -> Result<()> {
        let statement = parse_statement("DROP TABLE users;")?;
        let Statement::Drop(drop) = statement else {
            panic!("Expected DROP statement");
        };
        assert_eq!(drop.table, "users");
        Ok(())
    }

    #[test]
    fn test_parse_create_with_default_literal() -> Result<()> {
        let create =
            parse_create("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT DEFAULT 'x');")?;
        assert_eq!(create.table, "t");
        assert_eq!(create.columns.len(), 2);
        assert_eq!(create.columns[1].name, "name");
        assert_eq!(
            create.columns[1].default_value,
            Some(crate::catalog::types::Value::Text("x".to_string()))
        );

        Ok(())
    }

    #[test]
    fn test_parse_create_and_drop_index() -> Result<()> {
        let create_index =
            parse_create_index("CREATE UNIQUE INDEX idx_users_name ON users (name);")?;
        assert_eq!(create_index.index_name, "idx_users_name");
        assert_eq!(create_index.table, "users");
        assert_eq!(create_index.columns, vec!["name"]);
        assert!(create_index.unique);

        let drop_index = parse_drop_index("DROP INDEX idx_users_name ON users;")?;
        assert_eq!(drop_index.index_name, "idx_users_name");
        assert_eq!(drop_index.table, "users");

        Ok(())
    }

    #[test]
    fn test_parse_if_exists_modifiers() -> Result<()> {
        let create = parse_create("CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY);")?;
        assert!(create.if_not_exists);

        let create_index =
            parse_create_index("CREATE INDEX IF NOT EXISTS idx_users_id ON users (id);")?;
        assert!(create_index.if_not_exists);

        let drop_table = parse_statement("DROP TABLE IF EXISTS users;")?;
        assert!(matches!(
            drop_table,
            Statement::Drop(DropStatement {
                if_exists: true,
                ..
            })
        ));

        let drop_index = parse_drop_index("DROP INDEX IF EXISTS idx_users_id ON users;")?;
        assert!(drop_index.if_exists);
        Ok(())
    }

    #[test]
    fn test_parse_create_with_backticks_and_type_aliases() -> Result<()> {
        let create = parse_create(
            "CREATE TABLE `user data` (`id` INT PRIMARY KEY UNIQUE, `active` BOOL NOT NULL, `score` DOUBLE DEFAULT 1.5, `name` VARCHAR(32) DEFAULT 'x');",
        )?;
        assert_eq!(create.table, "user data");
        assert_eq!(create.columns.len(), 4);
        assert_eq!(create.columns[0].name, "id");
        assert_eq!(create.columns[0].data_type, DataType::Integer);
        assert!(create.columns[0].unique);
        assert_eq!(create.columns[1].data_type, DataType::Boolean);
        assert!(!create.columns[1].nullable);
        assert_eq!(create.columns[2].data_type, DataType::Float);
        assert_eq!(
            create.columns[2].default_value,
            Some(crate::catalog::Value::Float(1.5))
        );
        assert_eq!(create.columns[3].data_type, DataType::Text);
        assert_eq!(
            create.columns[3].default_value,
            Some(crate::catalog::Value::Text("x".to_string()))
        );
        assert!(create.constraints.is_empty());

        Ok(())
    }

    #[test]
    fn test_parse_create_with_check_and_foreign_key_constraints() -> Result<()> {
        let create = parse_create(
            "CREATE TABLE posts (id INT PRIMARY KEY, user_id INT REFERENCES users(id), title TEXT CHECK (title != ''), CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id), CHECK (id > 0));",
        )?;
        assert_eq!(create.columns.len(), 3);
        assert_eq!(create.constraints.len(), 2);
        assert_eq!(
            create.columns[1].references,
            Some(ForeignKeyDefinition {
                name: None,
                column: "user_id".to_string(),
                referenced_table: "users".to_string(),
                referenced_column: "id".to_string(),
            })
        );
        assert_eq!(
            create.columns[2].check_constraint,
            Some(CheckConstraintDefinition {
                name: None,
                expression_sql: "title != ''".to_string(),
            })
        );
        assert!(matches!(
            &create.constraints[0],
            TableConstraint::ForeignKey(ForeignKeyDefinition { name: Some(name), column, referenced_table, referenced_column })
                if name == "fk_user" && column == "user_id" && referenced_table == "users" && referenced_column == "id"
        ));
        assert!(matches!(
            &create.constraints[1],
            TableConstraint::Check(CheckConstraintDefinition { expression_sql, .. }) if expression_sql == "id > 0"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_alter_table_rename_to() -> Result<()> {
        let alter = parse_alter("ALTER TABLE users RENAME TO members;")?;
        assert_eq!(alter.table, "users");
        assert!(matches!(
            alter.operation,
            AlterOperation::RenameTo(ref new_name) if new_name == "members"
        ));

        Ok(())
    }

    #[test]
    fn test_parse_alter_table_rename_column() -> Result<()> {
        let alter = parse_alter("ALTER TABLE users RENAME COLUMN name TO full_name;")?;
        assert_eq!(alter.table, "users");
        assert!(matches!(
            alter.operation,
            AlterOperation::RenameColumn { ref old_name, ref new_name }
                if old_name == "name" && new_name == "full_name"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_alter_table_add_column() -> Result<()> {
        let alter = parse_alter("ALTER TABLE users ADD COLUMN active BOOL NOT NULL DEFAULT TRUE;")?;
        assert_eq!(alter.table, "users");
        assert!(matches!(
            alter.operation,
            AlterOperation::AddColumn(ColumnDefinition {
                name,
                data_type: DataType::Boolean,
                nullable: false,
                primary_key: false,
                unique: false,
                default_value: Some(crate::catalog::Value::Boolean(true)),
                check_constraint: None,
                references: None,
            }) if name == "active"
        ));

        Ok(())
    }

    #[test]
    fn test_parse_begin_commit_rollback() -> Result<()> {
        for (sql, expected) in [
            ("BEGIN;", "begin"),
            ("COMMIT;", "commit"),
            ("ROLLBACK;", "rollback"),
        ] {
            let statement = parse_statement(sql)?;

            match (expected, statement) {
                ("begin", Statement::Begin)
                | ("commit", Statement::Commit)
                | ("rollback", Statement::Rollback) => {}
                _ => panic!("unexpected transaction statement parse result"),
            }
        }

        Ok(())
    }

    #[test]
    fn test_parse_select_with_table_and_column_aliases() -> Result<()> {
        let select = parse_select("SELECT u.name AS user_name FROM users AS u WHERE u.id = 1;")?;
        assert_eq!(select.columns.len(), 1);
        assert!(matches!(
            &select.columns[0],
            SelectItem::Column(name) if name == "u.name"
        ));
        assert_eq!(select.column_aliases, vec![Some("user_name".to_string())]);
        assert!(matches!(
            select.from,
            TableReference::Table(name, Some(alias))
                if name == "users" && alias == "u"
        ));

        Ok(())
    }

    #[test]
    fn test_parse_select_with_cross_join_sources() -> Result<()> {
        let select = parse_select("SELECT u.id, p.user_id FROM users u, posts p;")?;
        match select.from {
            TableReference::CrossJoin(left, right) => {
                assert!(matches!(
                    *left,
                    TableReference::Table(name, Some(alias)) if name == "users" && alias == "u"
                ));
                assert!(matches!(
                    *right,
                    TableReference::Table(name, Some(alias)) if name == "posts" && alias == "p"
                ));
            }
            other => panic!("Expected cross join source tree, found {:?}", other),
        }

        Ok(())
    }

    #[test]
    fn test_parse_select_with_inner_join() -> Result<()> {
        let select = parse_select(
            "SELECT u.name, p.title FROM users u INNER JOIN posts p ON u.id = p.user_id;",
        )?;
        match select.from {
            TableReference::InnerJoin { left, right, on } => {
                assert!(matches!(
                    *left,
                    TableReference::Table(name, Some(alias)) if name == "users" && alias == "u"
                ));
                assert!(matches!(
                    *right,
                    TableReference::Table(name, Some(alias)) if name == "posts" && alias == "p"
                ));
                assert!(matches!(
                    on,
                    Condition::Comparison {
                        left: Expression::Column(left),
                        operator: ComparisonOperator::Equal,
                        right: Expression::Column(right),
                    } if left == "u.id" && right == "p.user_id"
                ));
            }
            other => panic!("Expected inner join source tree, found {:?}", other),
        }

        Ok(())
    }

    #[test]
    fn test_parse_select_with_subquery_predicates() -> Result<()> {
        let select = parse_select(
            "SELECT id FROM users WHERE id IN (SELECT user_id FROM posts) OR EXISTS (SELECT id FROM posts);",
        )?;
        assert_eq!(
            select.where_clause.as_ref().map(|w| w.conditions.len()),
            Some(1)
        );
        assert!(matches!(
            &select.where_clause.as_ref().expect("missing WHERE clause").conditions[0],
            Condition::Logical { left, operator: LogicalOperator::Or, right }
            if matches!(
                &**left,
                Condition::InSubquery { is_not: false, .. }
            ) && matches!(
                &**right,
                Condition::Exists { is_not: false, .. }
            )
        ));

        Ok(())
    }

    #[test]
    fn test_parse_select_with_union() -> Result<()> {
        let select = parse_select("SELECT id FROM users UNION ALL SELECT user_id FROM posts;")?;
        let set_operation = select
            .set_operation
            .expect("expected set operation on parsed union");
        assert_eq!(set_operation.operator, SetOperator::UnionAll);
        assert!(matches!(*set_operation.right, SelectStatement { .. }));

        Ok(())
    }

    #[test]
    fn test_parse_select_with_derived_table() -> Result<()> {
        let select = parse_select("SELECT p.user_id FROM (SELECT user_id FROM posts) AS p;")?;
        match select.from {
            TableReference::Derived { subquery, alias } => {
                assert_eq!(alias, "p");
                assert_eq!(subquery.columns.len(), 1);
                assert!(matches!(
                    &subquery.columns[0],
                    SelectItem::Column(name) if name == "user_id"
                ));
            }
            other => panic!("Expected derived table source, found {:?}", other),
        }

        Ok(())
    }

    #[test]
    fn test_parse_select_with_cte() -> Result<()> {
        let select = parse_select(
            "WITH recent_posts AS (SELECT user_id FROM posts) SELECT recent_posts.user_id FROM recent_posts;",
        )?;
        assert_eq!(select.with_clause.len(), 1);
        assert_eq!(select.with_clause[0].name, "recent_posts");
        assert!(matches!(
            select.from,
            TableReference::Table(name, None) if name == "recent_posts"
        ));

        Ok(())
    }

    #[test]
    fn test_parse_select_with_left_join() -> Result<()> {
        let select = parse_select(
            "SELECT u.name, p.title FROM users u LEFT JOIN posts p ON u.id = p.user_id;",
        )?;
        match select.from {
            TableReference::LeftJoin { left, right, on } => {
                assert!(matches!(
                    *left,
                    TableReference::Table(name, Some(alias)) if name == "users" && alias == "u"
                ));
                assert!(matches!(
                    *right,
                    TableReference::Table(name, Some(alias)) if name == "posts" && alias == "p"
                ));
                assert!(matches!(
                    on,
                    Condition::Comparison {
                        left: Expression::Column(left),
                        operator: ComparisonOperator::Equal,
                        right: Expression::Column(right),
                    } if left == "u.id" && right == "p.user_id"
                ));
            }
            other => panic!("Expected left join source tree, found {:?}", other),
        }

        Ok(())
    }
}
