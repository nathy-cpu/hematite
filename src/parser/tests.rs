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
        };

        assert!(select.validate(&catalog).is_err());
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
    use crate::error::Result;
    use crate::parser::ast::*;
    use crate::parser::lexer::*;
    use crate::parser::parser::*;

    #[test]
    fn test_parse_simple_select() -> Result<()> {
        let mut lexer = Lexer::new("SELECT * FROM users;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        match statement {
            Statement::Select(select) => {
                assert_eq!(select.columns.len(), 1);
                assert!(matches!(select.columns[0], SelectItem::Wildcard));
                assert!(
                    matches!(select.from, TableReference::Table(name, None) if name == "users")
                );
                assert!(select.where_clause.is_none());
                assert!(select.order_by.is_empty());
                assert!(select.limit.is_none());
            }
            _ => panic!("Expected SELECT statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_select_with_where() -> Result<()> {
        let mut lexer = Lexer::new("SELECT id FROM users WHERE id = 1;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        match statement {
            Statement::Select(select) => {
                assert!(select.where_clause.is_some());
                assert!(select.order_by.is_empty());
                assert!(select.limit.is_none());
                if let Some(where_clause) = select.where_clause {
                    assert_eq!(where_clause.conditions.len(), 1);
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_distinct() -> Result<()> {
        let mut lexer = Lexer::new("SELECT DISTINCT name FROM users;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        match statement {
            Statement::Select(select) => {
                assert!(select.distinct);
                assert_eq!(select.columns.len(), 1);
                assert!(matches!(&select.columns[0], SelectItem::Column(name) if name == "name"));
            }
            _ => panic!("Expected SELECT statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_in_condition() -> Result<()> {
        let mut lexer = Lexer::new("SELECT id FROM users WHERE id IN (1, 2);".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;

        match statement {
            Statement::Select(select) => {
                let where_clause = select.where_clause.expect("missing WHERE clause");
                assert_eq!(where_clause.conditions.len(), 1);
                assert!(matches!(
                    &where_clause.conditions[0],
                    Condition::InList { is_not: false, values, .. } if values.len() == 2
                ));
            }
            _ => panic!("Expected SELECT statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_between_condition() -> Result<()> {
        let mut lexer = Lexer::new("SELECT id FROM users WHERE id BETWEEN 1 AND 3;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;

        match statement {
            Statement::Select(select) => {
                let where_clause = select.where_clause.expect("missing WHERE clause");
                assert_eq!(where_clause.conditions.len(), 1);
                assert!(matches!(
                    &where_clause.conditions[0],
                    Condition::Between { is_not: false, .. }
                ));
            }
            _ => panic!("Expected SELECT statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_like_condition() -> Result<()> {
        let mut lexer = Lexer::new("SELECT name FROM users WHERE name LIKE 'A%';".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;

        match statement {
            Statement::Select(select) => {
                let where_clause = select.where_clause.expect("missing WHERE clause");
                assert_eq!(where_clause.conditions.len(), 1);
                assert!(matches!(
                    &where_clause.conditions[0],
                    Condition::Like { is_not: false, .. }
                ));
            }
            _ => panic!("Expected SELECT statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_not_condition() -> Result<()> {
        let mut lexer =
            Lexer::new("SELECT id FROM users WHERE NOT (id = 1 OR id = 2);".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;

        match statement {
            Statement::Select(select) => {
                let where_clause = select.where_clause.expect("missing WHERE clause");
                assert_eq!(where_clause.conditions.len(), 1);
                assert!(matches!(&where_clause.conditions[0], Condition::Not(_)));
            }
            _ => panic!("Expected SELECT statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_arithmetic_expression() -> Result<()> {
        let mut lexer =
            Lexer::new("SELECT id + 1 AS next_id FROM users WHERE -id < 0;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;

        match statement {
            Statement::Select(select) => {
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
            }
            _ => panic!("Expected SELECT statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_order_by() -> Result<()> {
        let mut lexer = Lexer::new("SELECT id FROM users ORDER BY name DESC, id ASC;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        match statement {
            Statement::Select(select) => {
                assert_eq!(select.order_by.len(), 2);
                assert_eq!(select.order_by[0].column, "name");
                assert_eq!(select.order_by[0].direction, SortDirection::Desc);
                assert_eq!(select.order_by[1].column, "id");
                assert_eq!(select.order_by[1].direction, SortDirection::Asc);
            }
            _ => panic!("Expected SELECT statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_limit() -> Result<()> {
        let mut lexer = Lexer::new("SELECT id FROM users ORDER BY name DESC LIMIT 5;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        match statement {
            Statement::Select(select) => {
                assert_eq!(select.order_by.len(), 1);
                assert_eq!(select.limit, Some(5));
            }
            _ => panic!("Expected SELECT statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_offset() -> Result<()> {
        let mut lexer =
            Lexer::new("SELECT id FROM users ORDER BY name DESC LIMIT 5 OFFSET 2;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        match statement {
            Statement::Select(select) => {
                assert_eq!(select.order_by.len(), 1);
                assert_eq!(select.limit, Some(5));
                assert_eq!(select.offset, Some(2));
            }
            _ => panic!("Expected SELECT statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_count() -> Result<()> {
        let mut lexer = Lexer::new("SELECT COUNT(*) FROM users;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        match statement {
            Statement::Select(select) => {
                assert_eq!(select.columns.len(), 1);
                assert!(matches!(select.columns[0], SelectItem::CountAll));
            }
            _ => panic!("Expected SELECT statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_aggregate() -> Result<()> {
        let mut lexer = Lexer::new("SELECT MAX(score) FROM users;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        match statement {
            Statement::Select(select) => {
                assert_eq!(select.columns.len(), 1);
                assert!(matches!(
                    select.columns[0],
                    SelectItem::Aggregate {
                        function: AggregateFunction::Max,
                        ref column,
                    } if column == "score"
                ));
            }
            _ => panic!("Expected SELECT statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_count_column() -> Result<()> {
        let mut lexer = Lexer::new("SELECT COUNT(score) FROM users;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        match statement {
            Statement::Select(select) => {
                assert!(matches!(
                    select.columns[0],
                    SelectItem::Aggregate {
                        function: AggregateFunction::Count,
                        ref column,
                    } if column == "score"
                ));
            }
            _ => panic!("Expected SELECT statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_group_by_having() -> Result<()> {
        let mut lexer = Lexer::new(
            "SELECT name, COUNT(id) AS total_count FROM users GROUP BY name HAVING total_count > 1;"
                .to_string(),
        );
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        match statement {
            Statement::Select(select) => {
                assert_eq!(select.group_by.len(), 1);
                assert!(matches!(
                    &select.group_by[0],
                    Expression::Column(name) if name == "name"
                ));
                assert!(select.having_clause.is_some());
                assert_eq!(select.column_aliases[1].as_deref(), Some("total_count"));
            }
            _ => panic!("Expected SELECT statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_where_operator_precedence() -> Result<()> {
        let mut lexer = Lexer::new(
            "SELECT id FROM users WHERE id = 1 OR id = 2 AND active = TRUE;".to_string(),
        );
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;

        match statement {
            Statement::Select(select) => {
                let where_clause = select.where_clause.unwrap();
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
                    _ => panic!("Expected OR condition at the root"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }

        Ok(())
    }

    #[test]
    fn test_parse_parameterized_insert() -> Result<()> {
        let mut lexer = Lexer::new("INSERT INTO users (id, name) VALUES (?, ?);".to_string());
        lexer.tokenize()?;

        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;

        match statement {
            Statement::Insert(insert) => {
                assert!(matches!(insert.values[0][0], Expression::Parameter(0)));
                assert!(matches!(insert.values[0][1], Expression::Parameter(1)));
            }
            _ => panic!("Expected INSERT statement"),
        }

        Ok(())
    }

    #[test]
    fn test_parse_where_parentheses() -> Result<()> {
        let mut lexer = Lexer::new(
            "SELECT id FROM users WHERE (id = 1 OR id = 2) AND active = TRUE;".to_string(),
        );
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;

        match statement {
            Statement::Select(select) => {
                let where_clause = select.where_clause.unwrap();
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
                    _ => panic!("Expected AND condition at the root"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }

        Ok(())
    }

    #[test]
    fn test_parse_insert() -> Result<()> {
        let mut lexer = Lexer::new("INSERT INTO users (id, name) VALUES (1, 'John');".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        match statement {
            Statement::Insert(insert) => {
                assert_eq!(insert.table, "users");
                assert_eq!(insert.columns, vec!["id", "name"]);
                assert_eq!(insert.values.len(), 1);
                assert_eq!(insert.values[0].len(), 2);
            }
            _ => panic!("Expected INSERT statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_delete() -> Result<()> {
        let mut lexer = Lexer::new("DELETE FROM users WHERE id = 1;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        match statement {
            Statement::Delete(delete) => {
                assert_eq!(delete.table, "users");
                assert!(delete.where_clause.is_some());
            }
            _ => panic!("Expected DELETE statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_update() -> Result<()> {
        let mut lexer =
            Lexer::new("UPDATE users SET name = 'John', active = TRUE WHERE id = 1;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        match statement {
            Statement::Update(update) => {
                assert_eq!(update.table, "users");
                assert_eq!(update.assignments.len(), 2);
                assert!(update.where_clause.is_some());
            }
            _ => panic!("Expected UPDATE statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_is_null() -> Result<()> {
        let mut lexer = Lexer::new("SELECT * FROM users WHERE name IS NULL;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        match statement {
            Statement::Select(select) => {
                let where_clause = select.where_clause.unwrap();
                assert_eq!(where_clause.conditions.len(), 1);
                assert!(matches!(
                    where_clause.conditions[0],
                    Condition::NullCheck { is_not: false, .. }
                ));
            }
            _ => panic!("Expected SELECT statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_drop() -> Result<()> {
        let mut lexer = Lexer::new("DROP TABLE users;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        match statement {
            Statement::Drop(drop) => {
                assert_eq!(drop.table, "users");
            }
            _ => panic!("Expected DROP statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_create_with_default_literal() -> Result<()> {
        let mut lexer = Lexer::new(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT DEFAULT 'x');".to_string(),
        );
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;

        match statement {
            Statement::Create(create) => {
                assert_eq!(create.table, "t");
                assert_eq!(create.columns.len(), 2);
                assert_eq!(create.columns[1].name, "name");
                assert_eq!(
                    create.columns[1].default_value,
                    Some(crate::catalog::types::Value::Text("x".to_string()))
                );
            }
            _ => panic!("Expected CREATE statement"),
        }

        Ok(())
    }

    #[test]
    fn test_parse_create_and_drop_index() -> Result<()> {
        let mut lexer = Lexer::new("CREATE INDEX idx_users_name ON users (name);".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        match statement {
            Statement::CreateIndex(create_index) => {
                assert_eq!(create_index.index_name, "idx_users_name");
                assert_eq!(create_index.table, "users");
                assert_eq!(create_index.columns, vec!["name"]);
            }
            _ => panic!("Expected CREATE INDEX statement"),
        }

        let mut lexer = Lexer::new("DROP INDEX idx_users_name ON users;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;
        match statement {
            Statement::DropIndex(drop_index) => {
                assert_eq!(drop_index.index_name, "idx_users_name");
                assert_eq!(drop_index.table, "users");
            }
            _ => panic!("Expected DROP INDEX statement"),
        }

        Ok(())
    }

    #[test]
    fn test_parse_begin_commit_rollback() -> Result<()> {
        for (sql, expected) in [
            ("BEGIN;", "begin"),
            ("COMMIT;", "commit"),
            ("ROLLBACK;", "rollback"),
        ] {
            let mut lexer = Lexer::new(sql.to_string());
            lexer.tokenize()?;
            let mut parser = Parser::new(lexer.get_tokens().to_vec());
            let statement = parser.parse()?;

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
        let mut lexer =
            Lexer::new("SELECT u.name AS user_name FROM users AS u WHERE u.id = 1;".to_string());
        lexer.tokenize()?;
        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        let statement = parser.parse()?;

        match statement {
            Statement::Select(select) => {
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
            }
            _ => panic!("Expected SELECT statement"),
        }

        Ok(())
    }
}
