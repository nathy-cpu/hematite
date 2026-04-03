//! Centralized tests for the parser module

mod ast_tests {
    use crate::catalog::types::DataType as CatalogDataType;
    use crate::error::Result;
    use crate::parser::ast::*;
    use crate::parser::LiteralValue;
    use crate::query::validation::validate_statement;

    #[test]
    fn test_select_statement_validation() -> Result<()> {
        let mut catalog = crate::catalog::Schema::new();

        // Create a test table
        let columns = vec![
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                CatalogDataType::Int,
            )
            .primary_key(true),
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                CatalogDataType::Text,
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

        assert!(validate_statement(&Statement::Select(select.clone()), &catalog).is_ok());
        Ok(())
    }

    #[test]
    fn test_invalid_column_reference() {
        let mut catalog = crate::catalog::Schema::new();

        let columns = vec![crate::catalog::Column::new(
            crate::catalog::ColumnId::new(1),
            "id".to_string(),
            CatalogDataType::Int,
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

        assert!(validate_statement(&Statement::Select(select.clone()), &catalog).is_err());
    }

    #[test]
    fn test_invalid_where_column_reference() {
        let mut catalog = crate::catalog::Schema::new();

        let columns = vec![crate::catalog::Column::new(
            crate::catalog::ColumnId::new(1),
            "id".to_string(),
            CatalogDataType::Int,
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
                    right: Expression::Literal(LiteralValue::Integer(1)),
                }],
            }),
            group_by: Vec::new(),
            having_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operation: None,
        };

        assert!(validate_statement(&Statement::Select(select.clone()), &catalog).is_err());
    }

    #[test]
    fn test_group_by_rejects_non_grouped_column_projection() {
        let mut catalog = crate::catalog::Schema::new();

        let columns = vec![
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                CatalogDataType::Int,
            )
            .primary_key(true),
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                CatalogDataType::Text,
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

        assert!(validate_statement(&Statement::Select(select.clone()), &catalog).is_err());
    }

    #[test]
    fn test_window_projection_rejects_group_by_mix() {
        let mut catalog = crate::catalog::Schema::new();

        let columns = vec![
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(1),
                "id".to_string(),
                CatalogDataType::Int,
            )
            .primary_key(true),
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(2),
                "name".to_string(),
                CatalogDataType::Text,
            ),
            crate::catalog::Column::new(
                crate::catalog::ColumnId::new(3),
                "team".to_string(),
                CatalogDataType::Text,
            ),
        ];
        catalog.create_table("users".to_string(), columns).unwrap();

        let select = SelectStatement {
            with_clause: Vec::new(),
            distinct: false,
            columns: vec![
                SelectItem::Column("team".to_string()),
                SelectItem::Window {
                    function: WindowFunction::RowNumber,
                    window: WindowSpec {
                        partition_by: vec![Expression::Column("team".to_string())],
                        order_by: vec![OrderByItem {
                            column: "name".to_string(),
                            direction: SortDirection::Asc,
                        }],
                    },
                },
            ],
            column_aliases: vec![None, Some("row_num".to_string())],
            from: TableReference::Table("users".to_string(), None),
            where_clause: None,
            group_by: vec![Expression::Column("team".to_string())],
            having_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operation: None,
        };

        assert!(validate_statement(&Statement::Select(select), &catalog).is_err());
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
                    CatalogDataType::Int,
                )
                .primary_key(true),
                crate::catalog::Column::new(
                    crate::catalog::ColumnId::new(2),
                    "name".to_string(),
                    CatalogDataType::Text,
                ),
            ],
        )?;
        catalog.create_table(
            "posts".to_string(),
            vec![
                crate::catalog::Column::new(
                    crate::catalog::ColumnId::new(3),
                    "id".to_string(),
                    CatalogDataType::Int,
                )
                .primary_key(true),
                crate::catalog::Column::new(
                    crate::catalog::ColumnId::new(4),
                    "user_id".to_string(),
                    CatalogDataType::Int,
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

        assert!(validate_statement(&Statement::Select(ambiguous.clone()), &catalog).is_err());

        let qualified = SelectStatement {
            columns: vec![SelectItem::Column("u.id".to_string())],
            ..ambiguous.clone()
        };

        assert!(validate_statement(&Statement::Select(qualified.clone()), &catalog).is_ok());
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
            "CREATE TABLE `user data` (`id` INT PRIMARY KEY, `active` BOOL, `score` FLOAT, `name` VARCHAR(32));"
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
            Token::Float,
            Token::Comma,
            Token::Identifier("name".to_string()),
            Token::Varchar,
            Token::LeftParen,
            Token::NumberLiteral("32".to_string()),
            Token::RightParen,
            Token::RightParen,
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }

    #[test]
    fn test_hex_blob_literal_tokens() -> Result<()> {
        let mut lexer = Lexer::new("SELECT X'48656C6C6F' FROM files;".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Select,
            Token::BlobLiteral(b"Hello".to_vec()),
            Token::From,
            Token::Identifier("files".to_string()),
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
            Token::NumberLiteral("1".to_string()),
            Token::Comma,
            Token::NumberLiteral("2".to_string()),
            Token::Comma,
            Token::NumberLiteral("3".to_string()),
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
            Token::NumberLiteral("1".to_string()),
            Token::And,
            Token::NumberLiteral("3".to_string()),
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
            Token::NumberLiteral("1".to_string()),
            Token::And,
            Token::Identifier("id".to_string()),
            Token::NotEqual,
            Token::NumberLiteral("2".to_string()),
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
            Token::NumberLiteral("5".to_string()),
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
            Token::NumberLiteral("5".to_string()),
            Token::Offset,
            Token::NumberLiteral("2".to_string()),
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
            Token::NumberLiteral("1".to_string()),
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
            Token::NumberLiteral("1".to_string()),
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
            Token::NumberLiteral("1".to_string()),
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
            Lexer::new("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Create,
            Token::Table,
            Token::Identifier("users".to_string()),
            Token::LeftParen,
            Token::Identifier("id".to_string()),
            Token::Int,
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

    #[test]
    fn test_lowercase_keywords_are_not_tokenized_as_keywords() -> Result<()> {
        let mut lexer = Lexer::new("select * from users;".to_string());
        lexer.tokenize()?;

        let expected = vec![
            Token::Identifier("select".to_string()),
            Token::Asterisk,
            Token::Identifier("from".to_string()),
            Token::Identifier("users".to_string()),
            Token::Semicolon,
        ];

        assert_eq!(lexer.get_tokens(), &expected);
        Ok(())
    }
}

mod parser_tests {
    use crate::error::Result;
    use crate::parser::ast::*;
    use crate::parser::lexer::*;
    use crate::parser::parser::*;
    use crate::parser::{LiteralValue, SqlTypeName};

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

    fn parse_create_view(sql: &str) -> Result<CreateViewStatement> {
        match parse_statement(sql)? {
            Statement::CreateView(create_view) => Ok(create_view),
            other => Err(crate::error::HematiteError::InternalError(format!(
                "Expected CREATE VIEW statement, found {:?}",
                other
            ))),
        }
    }

    fn parse_create_trigger(sql: &str) -> Result<CreateTriggerStatement> {
        match parse_statement(sql)? {
            Statement::CreateTrigger(create_trigger) => Ok(create_trigger),
            other => Err(crate::error::HematiteError::InternalError(format!(
                "Expected CREATE TRIGGER statement, found {:?}",
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
    fn test_parse_rejects_lowercase_keywords() {
        let err = parse_statement("select * from users;").unwrap_err();
        assert!(matches!(
            err,
            crate::error::HematiteError::ParseError(message)
                if message.contains("Keyword 'select' must be capitalized as 'SELECT'")
        ));
    }

    #[test]
    fn test_parse_rejects_lowercase_identifier_keywords() {
        let using_err =
            parse_create_index("CREATE UNIQUE KEY idx_users_name using btree ON users (name);")
                .unwrap_err();
        assert!(matches!(
            using_err,
            crate::error::HematiteError::ParseError(message)
                if message.contains("Keyword 'using' must be capitalized as 'USING'")
        ));

        let charset_err = parse_create(
            "CREATE TABLE users (id INT PRIMARY KEY) ENGINE=InnoDB DEFAULT charset=utf8mb4;",
        )
        .unwrap_err();
        assert!(matches!(
            charset_err,
            crate::error::HematiteError::ParseError(message)
                if message.contains("Keyword 'charset' must be capitalized as 'CHARSET'")
        ));
    }

    #[test]
    fn test_parse_rejects_lowercase_data_type_with_capitalization_hint() {
        let err = parse_create("CREATE TABLE users (id int PRIMARY KEY);").unwrap_err();
        assert!(matches!(
            err,
            crate::error::HematiteError::ParseError(message)
                if message.contains("Keyword 'int' must be capitalized as 'INT'")
        ));
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
    fn test_parse_scalar_function_expression() -> Result<()> {
        let select = parse_select("SELECT COALESCE(name, 'unknown') AS display_name FROM users;")?;
        assert!(matches!(
            &select.columns[0],
            SelectItem::Expression(Expression::ScalarFunctionCall {
                function: ScalarFunction::Coalesce,
                args,
            }) if args.len() == 2
        ));
        assert_eq!(select.column_aliases[0].as_deref(), Some("display_name"));
        Ok(())
    }

    #[test]
    fn test_parse_hex_blob_literal_expression() -> Result<()> {
        let select = parse_select("SELECT X'48656C6C6F' AS payload FROM files;")?;
        assert!(matches!(
            &select.columns[0],
            SelectItem::Expression(Expression::Literal(LiteralValue::Blob(bytes)))
                if bytes == b"Hello"
        ));
        assert_eq!(select.column_aliases[0].as_deref(), Some("payload"));
        Ok(())
    }

    #[test]
    fn test_parse_nested_scalar_functions() -> Result<()> {
        let select = parse_select("SELECT ROUND(ABS(score), 1) FROM users;")?;
        assert!(matches!(
            &select.columns[0],
            SelectItem::Expression(Expression::ScalarFunctionCall {
                function: ScalarFunction::Round,
                args,
            }) if args.len() == 2
        ));
        Ok(())
    }

    #[test]
    fn test_parse_temporal_scalar_functions() -> Result<()> {
        let select = parse_select(
            "SELECT DATE(created_at), TIME(created_at), YEAR(created_at), TIME_TO_SEC(at), UNIX_TIMESTAMP(stamped) FROM typed;",
        )?;
        assert!(matches!(
            &select.columns[0],
            SelectItem::Expression(Expression::ScalarFunctionCall {
                function: ScalarFunction::DateFn,
                ..
            })
        ));
        assert!(matches!(
            &select.columns[1],
            SelectItem::Expression(Expression::ScalarFunctionCall {
                function: ScalarFunction::TimeFn,
                ..
            })
        ));
        assert!(matches!(
            &select.columns[2],
            SelectItem::Expression(Expression::ScalarFunctionCall {
                function: ScalarFunction::Year,
                ..
            })
        ));
        assert!(matches!(
            &select.columns[3],
            SelectItem::Expression(Expression::ScalarFunctionCall {
                function: ScalarFunction::TimeToSec,
                ..
            })
        ));
        assert!(matches!(
            &select.columns[4],
            SelectItem::Expression(Expression::ScalarFunctionCall {
                function: ScalarFunction::UnixTimestamp,
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn test_parse_case_expression() -> Result<()> {
        let select = parse_select(
            "SELECT CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END AS grade FROM users;",
        )?;
        assert!(matches!(
            &select.columns[0],
            SelectItem::Expression(Expression::Case { branches, else_expr })
                if branches.len() == 2 && else_expr.is_some()
        ));
        assert_eq!(select.column_aliases[0].as_deref(), Some("grade"));
        Ok(())
    }

    #[test]
    fn test_parse_boolean_expression_projection() -> Result<()> {
        let select = parse_select("SELECT (score > 1 AND NOT active) AS keep_row FROM users;")?;
        assert!(matches!(
            &select.columns[0],
            SelectItem::Expression(Expression::Logical {
                operator: LogicalOperator::And,
                ..
            })
        ));
        assert_eq!(select.column_aliases[0].as_deref(), Some("keep_row"));
        Ok(())
    }

    #[test]
    fn test_parse_not_equal_angle_brackets() -> Result<()> {
        let select = parse_select("SELECT id FROM users WHERE id <> 1;")?;
        let where_clause = select.where_clause.expect("missing WHERE clause");
        assert!(matches!(
            &where_clause.conditions[0],
            Condition::Comparison {
                operator: ComparisonOperator::NotEqual,
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn test_parse_case_expression_with_boolean_condition_expression() -> Result<()> {
        let select = parse_select(
            "SELECT CASE WHEN score > 10 AND NOT active THEN 'high' ELSE 'low' END FROM users;",
        )?;
        assert!(matches!(
            &select.columns[0],
            SelectItem::Expression(Expression::Case { branches, .. })
                if matches!(
                    &branches[0].condition,
                    Expression::Logical {
                        operator: LogicalOperator::And,
                        ..
                    }
                )
        ));
        Ok(())
    }

    #[test]
    fn test_parse_not_between_expression() -> Result<()> {
        let select = parse_select("SELECT score NOT BETWEEN 1 AND 3 AS outside_range FROM users;")?;
        assert!(matches!(
            &select.columns[0],
            SelectItem::Expression(Expression::Between { is_not: true, .. })
        ));
        assert_eq!(select.column_aliases[0].as_deref(), Some("outside_range"));
        Ok(())
    }

    #[test]
    fn test_parse_cast_and_modulo_expression() -> Result<()> {
        let select = parse_select("SELECT CAST(score % 2 AS INT) AS bucket FROM users;")?;
        assert!(matches!(
            &select.columns[0],
            SelectItem::Expression(Expression::Cast { expr, target_type })
                if matches!(
                    expr.as_ref(),
                    Expression::Binary {
                        operator: ArithmeticOperator::Modulo,
                        ..
                    }
                ) && *target_type == SqlTypeName::Int
        ));
        assert_eq!(select.column_aliases[0].as_deref(), Some("bucket"));
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
    fn test_parse_mysql_limit_offset_count() -> Result<()> {
        let select = parse_select("SELECT id FROM users ORDER BY name DESC LIMIT 2, 5;")?;
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
    fn test_parse_window_functions() -> Result<()> {
        let select = parse_select(
            "SELECT name, ROW_NUMBER() OVER (PARTITION BY team ORDER BY score DESC), \
             COUNT(*) OVER (PARTITION BY team) \
             FROM users;",
        )?;

        assert_eq!(select.columns.len(), 3);
        assert!(matches!(select.columns[1], SelectItem::Window { .. }));
        assert!(matches!(select.columns[2], SelectItem::Window { .. }));

        match &select.columns[1] {
            SelectItem::Window { function, window } => {
                assert!(matches!(function, WindowFunction::RowNumber));
                assert_eq!(window.partition_by.len(), 1);
                assert_eq!(window.order_by.len(), 1);
            }
            _ => unreachable!("expected window function"),
        }

        Ok(())
    }

    #[test]
    fn test_parse_interval_literals() -> Result<()> {
        let select = parse_select(
            "SELECT DATE('2026-03-28') + INTERVAL '1-02' YEAR TO MONTH, \
             CAST('2026-03-28 10:00:00' AS DATETIME) - INTERVAL '2 03:04:05' DAY TO SECOND, \
             CAST('1-02' AS INTERVAL YEAR TO MONTH), \
             CAST('2 03:04:05' AS INTERVAL DAY TO SECOND) \
             FROM users;",
        )?;

        assert_eq!(select.columns.len(), 4);
        assert!(matches!(select.columns[0], SelectItem::Expression(_)));
        assert!(matches!(select.columns[1], SelectItem::Expression(_)));
        assert!(matches!(select.columns[2], SelectItem::Expression(_)));
        assert!(matches!(select.columns[3], SelectItem::Expression(_)));
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
        match &insert.source {
            InsertSource::Values(rows) => {
                assert!(matches!(rows[0][0], Expression::Parameter(0)));
                assert!(matches!(rows[0][1], Expression::Parameter(1)));
            }
            InsertSource::Select(_) => panic!("expected VALUES source"),
        }

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
        match &insert.source {
            InsertSource::Values(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].len(), 2);
            }
            InsertSource::Select(_) => panic!("expected VALUES source"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_insert_set() -> Result<()> {
        let insert = parse_insert("INSERT INTO users SET id = 1, name = 'John';")?;
        assert_eq!(insert.table, "users");
        assert_eq!(insert.columns, vec!["id", "name"]);
        match &insert.source {
            InsertSource::Values(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].len(), 2);
            }
            InsertSource::Select(_) => panic!("expected VALUES source"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_insert_select() -> Result<()> {
        let insert = parse_insert("INSERT INTO users (id, name) SELECT id, name FROM source;")?;
        assert_eq!(insert.table, "users");
        assert_eq!(insert.columns, vec!["id", "name"]);
        assert!(matches!(insert.source, InsertSource::Select(_)));
        Ok(())
    }

    #[test]
    fn test_parse_insert_on_duplicate_key_update() -> Result<()> {
        let insert = parse_insert(
            "INSERT INTO users (id, name) VALUES (1, 'Alice') ON DUPLICATE KEY UPDATE name = 'Bob';",
        )?;
        assert!(matches!(insert.source, InsertSource::Values(_)));
        assert!(matches!(insert.on_duplicate, Some(assignments) if assignments.len() == 1));
        Ok(())
    }

    #[test]
    fn test_parse_delete() -> Result<()> {
        let delete = parse_delete("DELETE FROM users WHERE id = 1;")?;
        assert_eq!(delete.table, "users");
        assert!(delete.target_binding.is_none());
        assert!(delete.source.is_none());
        assert!(delete.where_clause.is_some());
        Ok(())
    }

    #[test]
    fn test_parse_update() -> Result<()> {
        let update = parse_update("UPDATE users SET name = 'John', active = TRUE WHERE id = 1;")?;
        assert_eq!(update.table, "users");
        assert!(update.target_binding.is_none());
        assert!(update.source.is_none());
        assert_eq!(update.assignments.len(), 2);
        assert!(update.where_clause.is_some());
        Ok(())
    }

    #[test]
    fn test_parse_joined_update_and_delete() -> Result<()> {
        let update = parse_update(
            "UPDATE users u JOIN teams t ON u.team_id = t.id SET name = t.name WHERE t.active = TRUE;",
        )?;
        assert_eq!(update.table, "users");
        assert_eq!(update.target_binding.as_deref(), Some("u"));
        assert!(matches!(
            update.source,
            Some(TableReference::InnerJoin { .. })
        ));

        let delete = parse_delete(
            "DELETE u FROM users u JOIN teams t ON u.team_id = t.id WHERE t.active = FALSE;",
        )?;
        assert_eq!(delete.table, "users");
        assert_eq!(delete.target_binding.as_deref(), Some("u"));
        assert!(matches!(
            delete.source,
            Some(TableReference::InnerJoin { .. })
        ));
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
        let create = parse_create("CREATE TABLE t (id INT PRIMARY KEY, name TEXT DEFAULT 'x');")?;
        assert_eq!(create.table, "t");
        assert_eq!(create.columns.len(), 2);
        assert_eq!(create.columns[1].name, "name");
        assert_eq!(
            create.columns[1].default_value,
            Some(LiteralValue::Text("x".to_string()))
        );

        Ok(())
    }

    #[test]
    fn test_parse_create_with_auto_increment() -> Result<()> {
        let create =
            parse_create("CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT);")?;
        assert!(create.columns[0].auto_increment);
        assert!(create.columns[0].primary_key);
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
    fn test_parse_create_key_with_using_clause() -> Result<()> {
        let create_index =
            parse_create_index("CREATE UNIQUE KEY idx_users_name USING BTREE ON users (name);")?;
        assert_eq!(create_index.index_name, "idx_users_name");
        assert_eq!(create_index.table, "users");
        assert_eq!(create_index.columns, vec!["name"]);
        assert!(create_index.unique);
        Ok(())
    }

    #[test]
    fn test_parse_create_table_with_ignored_mysql_options() -> Result<()> {
        let create = parse_create(
            "CREATE TABLE users (id INT PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=10;",
        )?;
        assert_eq!(create.table, "users");
        assert_eq!(create.columns.len(), 1);
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
            "CREATE TABLE `user data` (`id` INT PRIMARY KEY UNIQUE, `active` BOOL NOT NULL, `score` FLOAT DEFAULT 1.5, `name` VARCHAR(32) DEFAULT 'x');",
        )?;
        assert_eq!(create.table, "user data");
        assert_eq!(create.columns.len(), 4);
        assert_eq!(create.columns[0].name, "id");
        assert_eq!(create.columns[0].data_type, SqlTypeName::Int);
        assert!(create.columns[0].unique);
        assert_eq!(create.columns[1].data_type, SqlTypeName::Boolean);
        assert!(!create.columns[1].nullable);
        assert_eq!(create.columns[2].data_type, SqlTypeName::Float);
        assert_eq!(
            create.columns[2].default_value,
            Some(LiteralValue::Float("1.5".to_string()))
        );
        assert_eq!(create.columns[3].data_type, SqlTypeName::VarChar(32));
        assert_eq!(
            create.columns[3].default_value,
            Some(LiteralValue::Text("x".to_string()))
        );
        assert!(create.constraints.is_empty());

        Ok(())
    }

    #[test]
    fn test_parse_current_numeric_type_names() -> Result<()> {
        let create = parse_create(
            "CREATE TABLE metrics (id UINT64 PRIMARY KEY, ratio FLOAT32, amount DECIMAL(10, 2), code CHAR(8), tiny INT8, small INT16, exact DECIMAL(6));",
        )?;
        assert_eq!(create.columns[0].data_type, SqlTypeName::UInt64);
        assert_eq!(create.columns[1].data_type, SqlTypeName::Float32);
        assert_eq!(
            create.columns[2].data_type,
            SqlTypeName::Decimal {
                precision: Some(10),
                scale: Some(2)
            }
        );
        assert_eq!(create.columns[3].data_type, SqlTypeName::Char(8));
        assert_eq!(create.columns[4].data_type, SqlTypeName::Int8);
        assert_eq!(create.columns[5].data_type, SqlTypeName::Int16);
        assert_eq!(
            create.columns[6].data_type,
            SqlTypeName::Decimal {
                precision: Some(6),
                scale: None
            }
        );
        Ok(())
    }

    #[test]
    fn test_parse_practical_core_type_names() -> Result<()> {
        let create = parse_create(
            "CREATE TABLE events (id INT64 PRIMARY KEY, amount DECIMAL(12, 4), payload BLOB, start_date DATE, created_at DATETIME);",
        )?;
        assert_eq!(create.columns[0].data_type, SqlTypeName::Int64);
        assert_eq!(
            create.columns[1].data_type,
            SqlTypeName::Decimal {
                precision: Some(12),
                scale: Some(4)
            }
        );
        assert_eq!(create.columns[2].data_type, SqlTypeName::Blob);
        assert_eq!(create.columns[3].data_type, SqlTypeName::Date);
        assert_eq!(create.columns[4].data_type, SqlTypeName::DateTime);
        Ok(())
    }

    #[test]
    fn test_parse_column_character_set_and_collation() -> Result<()> {
        let create = parse_create(
            "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(12) CHARACTER SET utf8mb4 COLLATE NOCASE, note TEXT COLLATE utf8mb4_bin);",
        )?;
        assert_eq!(create.columns[1].character_set.as_deref(), Some("utf8mb4"));
        assert_eq!(create.columns[1].collation.as_deref(), Some("NOCASE"));
        assert_eq!(create.columns[2].character_set, None);
        assert_eq!(create.columns[2].collation.as_deref(), Some("utf8mb4_bin"));
        Ok(())
    }

    #[test]
    fn test_parse_new_integer_type_names() -> Result<()> {
        let create = parse_create(
            "CREATE TABLE ints (id INT PRIMARY KEY, tiny INT8, small INT16, normal INT32, large INT64, massive INT128);",
        )?;
        assert_eq!(create.columns[0].data_type, SqlTypeName::Int);
        assert_eq!(create.columns[1].data_type, SqlTypeName::Int8);
        assert_eq!(create.columns[2].data_type, SqlTypeName::Int16);
        assert_eq!(create.columns[3].data_type, SqlTypeName::Int);
        assert_eq!(create.columns[4].data_type, SqlTypeName::Int64);
        assert_eq!(create.columns[5].data_type, SqlTypeName::Int128);
        Ok(())
    }

    #[test]
    fn test_parse_unsigned_integer_type_names() -> Result<()> {
        let create = parse_create(
            "CREATE TABLE uints (id UINT PRIMARY KEY, tiny UINT8, small UINT16, normal UINT32, large UINT64, massive UINT128, canonical UINT64);",
        )?;
        assert_eq!(create.columns[0].data_type, SqlTypeName::UInt);
        assert_eq!(create.columns[1].data_type, SqlTypeName::UInt8);
        assert_eq!(create.columns[2].data_type, SqlTypeName::UInt16);
        assert_eq!(create.columns[3].data_type, SqlTypeName::UInt);
        assert_eq!(create.columns[4].data_type, SqlTypeName::UInt64);
        assert_eq!(create.columns[5].data_type, SqlTypeName::UInt128);
        assert_eq!(create.columns[6].data_type, SqlTypeName::UInt64);
        Ok(())
    }

    #[test]
    fn test_parse_additional_temporal_binary_and_enum_types() -> Result<()> {
        let create = parse_create(
            "CREATE TABLE typed (id INT PRIMARY KEY, at TIME, stamped DATETIME, zone_time TIME WITH TIME ZONE, code BINARY(4), bytes VARBINARY(16), state ENUM('draft', 'live'));",
        )?;
        assert_eq!(create.columns[1].data_type, SqlTypeName::Time);
        assert_eq!(create.columns[2].data_type, SqlTypeName::DateTime);
        assert_eq!(create.columns[3].data_type, SqlTypeName::TimeWithTimeZone);
        assert_eq!(create.columns[4].data_type, SqlTypeName::Binary(4));
        assert_eq!(create.columns[5].data_type, SqlTypeName::VarBinary(16));
        assert_eq!(
            create.columns[6].data_type,
            SqlTypeName::Enum(vec!["draft".to_string(), "live".to_string()])
        );
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
                columns: vec!["user_id".to_string()],
                referenced_table: "users".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: ForeignKeyAction::Restrict,
                on_update: ForeignKeyAction::Restrict,
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
            TableConstraint::ForeignKey(ForeignKeyDefinition { name: Some(name), columns, referenced_table, referenced_columns, on_delete: ForeignKeyAction::Restrict, on_update: ForeignKeyAction::Restrict })
                if name == "fk_user"
                    && columns == &vec!["user_id".to_string()]
                    && referenced_table == "users"
                    && referenced_columns == &vec!["id".to_string()]
        ));
        assert!(matches!(
            &create.constraints[1],
            TableConstraint::Check(CheckConstraintDefinition { expression_sql, .. }) if expression_sql == "id > 0"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_multi_column_foreign_key_with_actions() -> Result<()> {
        let create = parse_create(
            "CREATE TABLE child (id INT PRIMARY KEY, a INT, b INT, CONSTRAINT fk_parent FOREIGN KEY (a, b) REFERENCES parent (x, y) ON DELETE CASCADE ON UPDATE SET NULL);",
        )?;
        assert!(matches!(
            &create.constraints[0],
            TableConstraint::ForeignKey(ForeignKeyDefinition {
                name: Some(name),
                columns,
                referenced_table,
                referenced_columns,
                on_delete: ForeignKeyAction::Cascade,
                on_update: ForeignKeyAction::SetNull,
            }) if name == "fk_parent"
                && columns == &vec!["a".to_string(), "b".to_string()]
                && referenced_table == "parent"
                && referenced_columns == &vec!["x".to_string(), "y".to_string()]
        ));
        Ok(())
    }

    #[test]
    fn test_parse_create_with_table_unique_constraint() -> Result<()> {
        let create = parse_create(
            "CREATE TABLE memberships (id INT PRIMARY KEY, user_id INT, org_id INT, CONSTRAINT uq_membership UNIQUE (user_id, org_id));",
        )?;
        assert!(matches!(
            &create.constraints[0],
            TableConstraint::Unique(UniqueConstraintDefinition { name: Some(name), columns })
                if name == "uq_membership"
                    && columns == &vec!["user_id".to_string(), "org_id".to_string()]
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
                data_type: SqlTypeName::Boolean,
                character_set: None,
                collation: None,
                nullable: false,
                primary_key: false,
                auto_increment: false,
                unique: false,
                default_value: Some(LiteralValue::Boolean(true)),
                check_constraint: None,
                references: None,
            }) if name == "active"
        ));

        Ok(())
    }

    #[test]
    fn test_parse_alter_table_drop_column() -> Result<()> {
        let alter = parse_alter("ALTER TABLE users DROP COLUMN active;")?;
        assert_eq!(alter.table, "users");
        assert!(matches!(
            alter.operation,
            AlterOperation::DropColumn(ref column_name) if column_name == "active"
        ));

        Ok(())
    }

    #[test]
    fn test_parse_alter_table_set_default() -> Result<()> {
        let alter = parse_alter("ALTER TABLE users ALTER COLUMN active SET DEFAULT TRUE;")?;
        assert_eq!(alter.table, "users");
        assert!(matches!(
            alter.operation,
            AlterOperation::AlterColumnSetDefault {
                ref column_name,
                default_value: LiteralValue::Boolean(true),
            } if column_name == "active"
        ));

        Ok(())
    }

    #[test]
    fn test_parse_alter_table_drop_default() -> Result<()> {
        let alter = parse_alter("ALTER TABLE users ALTER COLUMN active DROP DEFAULT;")?;
        assert_eq!(alter.table, "users");
        assert!(matches!(
            alter.operation,
            AlterOperation::AlterColumnDropDefault { ref column_name } if column_name == "active"
        ));

        Ok(())
    }

    #[test]
    fn test_parse_alter_table_set_not_null() -> Result<()> {
        let alter = parse_alter("ALTER TABLE users ALTER COLUMN active SET NOT NULL;")?;
        assert_eq!(alter.table, "users");
        assert!(matches!(
            alter.operation,
            AlterOperation::AlterColumnSetNotNull { ref column_name } if column_name == "active"
        ));

        Ok(())
    }

    #[test]
    fn test_parse_alter_table_drop_not_null() -> Result<()> {
        let alter = parse_alter("ALTER TABLE users ALTER COLUMN active DROP NOT NULL;")?;
        assert_eq!(alter.table, "users");
        assert!(matches!(
            alter.operation,
            AlterOperation::AlterColumnDropNotNull { ref column_name } if column_name == "active"
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
    fn test_parse_select_with_intersect_and_except() -> Result<()> {
        let intersect = parse_select("SELECT id FROM users INTERSECT SELECT user_id FROM posts;")?;
        assert_eq!(
            intersect
                .set_operation
                .as_ref()
                .expect("expected intersect")
                .operator,
            SetOperator::Intersect
        );

        let except = parse_select("SELECT id FROM users EXCEPT SELECT user_id FROM posts;")?;
        assert_eq!(
            except
                .set_operation
                .as_ref()
                .expect("expected except")
                .operator,
            SetOperator::Except
        );

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
    fn test_parse_scalar_subquery_expression() -> Result<()> {
        let select = parse_select("SELECT (SELECT COUNT(*) FROM posts) AS post_count FROM users;")?;
        assert!(matches!(
            &select.columns[0],
            SelectItem::Expression(Expression::ScalarSubquery(_))
        ));
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
    fn test_parse_select_with_recursive_cte() -> Result<()> {
        let select = parse_select(
            "WITH RECURSIVE nums AS (SELECT n FROM seeds UNION ALL SELECT n + 1 AS n FROM nums WHERE n < 3) SELECT n FROM nums;",
        )?;
        assert_eq!(select.with_clause.len(), 1);
        assert!(select.with_clause[0].recursive);
        assert_eq!(select.with_clause[0].name, "nums");

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

    #[test]
    fn test_parse_select_with_right_and_full_join() -> Result<()> {
        let right = parse_select(
            "SELECT u.name, p.title FROM users u RIGHT JOIN posts p ON u.id = p.user_id;",
        )?;
        assert!(matches!(right.from, TableReference::RightJoin { .. }));

        let full = parse_select(
            "SELECT u.name, p.title FROM users u FULL OUTER JOIN posts p ON u.id = p.user_id;",
        )?;
        assert!(matches!(full.from, TableReference::FullOuterJoin { .. }));

        Ok(())
    }

    #[test]
    fn test_parse_explain_describe_and_show_tables() -> Result<()> {
        let mut lexer = Lexer::new("EXPLAIN SELECT * FROM users;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        assert!(matches!(parser.parse()?, Statement::Explain(_)));

        let mut lexer = Lexer::new("DESCRIBE users;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        assert!(matches!(parser.parse()?, Statement::Describe(_)));

        let mut lexer = Lexer::new("SHOW TABLES;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        assert!(matches!(parser.parse()?, Statement::ShowTables));

        let mut lexer = Lexer::new("SHOW VIEWS;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        assert!(matches!(parser.parse()?, Statement::ShowViews));
        Ok(())
    }

    #[test]
    fn test_parse_view_trigger_and_savepoint_statements() -> Result<()> {
        assert!(matches!(
            parse_statement("SELECT name INTO copied_users FROM users;")?,
            Statement::SelectInto(SelectIntoStatement { ref table, ref query })
                if table == "copied_users"
                    && matches!(query.from, TableReference::Table(ref name, None) if name == "users")
        ));

        let create_view = parse_create_view("CREATE VIEW user_names AS SELECT name FROM users;")?;
        assert_eq!(create_view.view, "user_names");
        assert!(matches!(
            create_view.query.from,
            TableReference::Table(name, None) if name == "users"
        ));

        let create_trigger = parse_create_trigger(
            "CREATE TRIGGER audit_users AFTER INSERT ON users AS INSERT INTO audit_log (entry) VALUES (NEW.name);",
        )?;
        assert_eq!(create_trigger.trigger, "audit_users");
        assert_eq!(create_trigger.table, "users");
        assert_eq!(create_trigger.event, TriggerEvent::Insert);
        assert!(matches!(*create_trigger.body, Statement::Insert(_)));

        assert!(matches!(
            parse_statement("DROP VIEW IF EXISTS user_names;")?,
            Statement::DropView(DropViewStatement { ref view, if_exists })
                if view == "user_names" && if_exists
        ));
        assert!(matches!(
            parse_statement("DROP TRIGGER IF EXISTS audit_users;")?,
            Statement::DropTrigger(DropTriggerStatement { ref trigger, if_exists })
                if trigger == "audit_users" && if_exists
        ));
        assert!(matches!(
            parse_statement("SAVEPOINT before_users;")?,
            Statement::Savepoint(ref name) if name == "before_users"
        ));
        assert!(matches!(
            parse_statement("ROLLBACK TO SAVEPOINT before_users;")?,
            Statement::RollbackToSavepoint(ref name) if name == "before_users"
        ));
        assert!(matches!(
            parse_statement("RELEASE SAVEPOINT before_users;")?,
            Statement::ReleaseSavepoint(ref name) if name == "before_users"
        ));
        assert!(matches!(
            parse_statement("SHOW INDEXES FROM users;")?,
            Statement::ShowIndexes(Some(ref table)) if table == "users"
        ));
        assert!(matches!(
            parse_statement("SHOW TRIGGERS;")?,
            Statement::ShowTriggers(None)
        ));
        assert!(matches!(
            parse_statement("SHOW CREATE TABLE users;")?,
            Statement::ShowCreateTable(ref table) if table == "users"
        ));
        assert!(matches!(
            parse_statement("SHOW CREATE VIEW user_names;")?,
            Statement::ShowCreateView(ref view) if view == "user_names"
        ));
        assert!(matches!(
            parse_statement("ALTER TABLE users ADD CONSTRAINT uq_users_email UNIQUE (email);")?,
            Statement::Alter(AlterStatement {
                table,
                operation: AlterOperation::AddConstraint(TableConstraint::Unique(_))
            }) if table == "users"
        ));
        assert!(matches!(
            parse_statement("ALTER TABLE users DROP CONSTRAINT uq_users_email;")?,
            Statement::Alter(AlterStatement {
                table,
                operation: AlterOperation::DropConstraint(ref name)
            }) if table == "users" && name == "uq_users_email"
        ));
        Ok(())
    }
}
