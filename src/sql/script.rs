//! Token-aware SQL script splitting and stepped execution.

use std::collections::VecDeque;

use crate::error::Result;
use crate::parser::lexer::Token;
use crate::parser::{tokenize_sql, Parser};

use super::connection::Connection;
use super::result::ExecutedStatement;

pub(crate) fn split_script_tokens(sql: &str) -> Result<Vec<Vec<Token>>> {
    Ok(split_script_state(tokenize_sql(sql)?, true).0)
}

pub fn script_is_complete(sql: &str) -> Result<bool> {
    let tokens = tokenize_sql(sql)?;
    let mut has_complete_statement = false;
    let mut has_current_statement_tokens = false;

    for token in &tokens {
        if matches!(token, Token::Semicolon) {
            if has_current_statement_tokens {
                has_complete_statement = true;
                has_current_statement_tokens = false;
            }
        } else {
            has_current_statement_tokens = true;
        }
    }

    Ok(has_complete_statement && !has_current_statement_tokens)
}

fn split_script_state(
    tokens: Vec<Token>,
    append_trailing_statement: bool,
) -> (Vec<Vec<Token>>, bool) {
    let mut statements = Vec::new();
    let mut current_tokens = Vec::new();
    let mut has_current_statement_tokens = false;

    for token in tokens {
        let is_semicolon = matches!(token, Token::Semicolon);
        has_current_statement_tokens |= !is_semicolon;
        current_tokens.push(token);

        if is_semicolon {
            if has_current_statement_tokens {
                statements.push(current_tokens);
                current_tokens = Vec::new();
                has_current_statement_tokens = false;
            } else {
                current_tokens.clear();
            }
        }
    }

    if has_current_statement_tokens {
        if append_trailing_statement {
            current_tokens.push(Token::Semicolon);
            statements.push(current_tokens);
            return (statements, false);
        }
        return (statements, true);
    }

    (statements, false)
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
