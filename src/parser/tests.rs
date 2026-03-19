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
            columns: vec![SelectItem::Column("id".to_string())],
            from: TableReference::Table("users".to_string()),
            where_clause: None,
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
            columns: vec![SelectItem::Column("invalid".to_string())],
            from: TableReference::Table("users".to_string()),
            where_clause: None,
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
            columns: vec![SelectItem::Wildcard],
            from: TableReference::Table("users".to_string()),
            where_clause: Some(WhereClause {
                conditions: vec![Condition::Comparison {
                    left: Expression::Column("missing".to_string()),
                    operator: ComparisonOperator::Equal,
                    right: Expression::Literal(Value::Integer(1)),
                }],
            }),
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
                assert!(matches!(select.from, TableReference::Table(name) if name == "users"));
                assert!(select.where_clause.is_none());
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
                if let Some(where_clause) = select.where_clause {
                    assert_eq!(where_clause.conditions.len(), 1);
                }
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
}
