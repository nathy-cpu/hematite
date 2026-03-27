//! Token-aware SQL script splitting and stepped execution.

use std::collections::VecDeque;

use crate::error::Result;
use crate::parser::lexer::Token;
use crate::parser::{Lexer, Parser};

use super::connection::Connection;
use super::result::ExecutedStatement;

pub(crate) fn split_script_tokens(sql: &str) -> Result<Vec<Vec<Token>>> {
    let mut lexer = Lexer::new(sql.to_string());
    lexer.tokenize()?;

    let mut statements = Vec::new();
    let mut current_tokens = Vec::new();

    for token in lexer.get_tokens().iter().cloned() {
        let is_semicolon = matches!(token, Token::Semicolon);
        current_tokens.push(token);

        if is_semicolon {
            if contains_statement_tokens(&current_tokens) {
                statements.push(current_tokens);
            }
            current_tokens = Vec::new();
        }
    }

    if contains_statement_tokens(&current_tokens) {
        current_tokens.push(Token::Semicolon);
        statements.push(current_tokens);
    }

    Ok(statements)
}

fn contains_statement_tokens(tokens: &[Token]) -> bool {
    tokens
        .iter()
        .any(|token| !matches!(token, Token::Semicolon))
}

pub struct ScriptIter<'a> {
    connection: &'a mut Connection,
    statements: VecDeque<Vec<Token>>,
}

impl<'a> ScriptIter<'a> {
    pub(crate) fn new(connection: &'a mut Connection, statements: Vec<Vec<Token>>) -> Self {
        Self {
            connection,
            statements: statements.into(),
        }
    }
}

impl Iterator for ScriptIter<'_> {
    type Item = Result<ExecutedStatement>;

    fn next(&mut self) -> Option<Self::Item> {
        let tokens = self.statements.pop_front()?;
        let mut parser = Parser::new(tokens);
        Some(
            parser
                .parse()
                .and_then(|statement| self.connection.execute_statement_result(statement)),
        )
    }
}
