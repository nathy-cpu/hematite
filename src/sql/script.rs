//! Splitting a SQL script into statements, running them one at a time, and telling
//! the REPL whether its buffer ends on a statement boundary.

use std::collections::VecDeque;
use std::ops::Range;

use crate::error::Result;
use crate::parser::lexer::{Token, TokenKind};
use crate::parser::{tokenize_sql, Parser};

use super::connection::Connection;
use super::result::ExecutedStatement;

/// A script lexed once, plus where each statement starts and ends in it.
///
/// The statements are ranges over one token buffer rather than a `Vec<Token>` each:
/// a vector per statement would make a 360-statement DDL script cut 360 vectors out
/// of the buffer it had just finished filling. `ScriptIter` copies a statement's
/// tokens out only when it is about to parse it, because `Parser` owns its tokens.
pub(crate) struct ScriptTokens<'a> {
    sql: &'a str,
    tokens: Vec<Token>,
    statements: Vec<Range<usize>>,
}

impl ScriptTokens<'_> {
    /// How many statements the script holds. Used by the parser's perf baselines.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn len(&self) -> usize {
        self.statements.len()
    }
}

pub(crate) fn split_script_tokens(sql: &str) -> Result<ScriptTokens<'_>> {
    let tokens = tokenize_sql(sql)?;
    let statements = split_statement_ranges(&tokens, true).0;
    Ok(ScriptTokens {
        sql,
        tokens,
        statements,
    })
}

/// Whether `sql` ends on a statement boundary.
///
/// Scans bytes rather than lexing. The REPL calls this after every line of a
/// statement it is still collecting, so lexing the whole buffer each time would
/// make pasting a long script quadratic — and a token stream is far more than this
/// question needs, which is only "is this semicolon inside a literal".
///
/// It never returns `Err`: an unterminated literal is a statement that is not
/// finished yet, not an error. `main` propagates the `Result`, so an error here
/// would end the REPL session on `SELECT 'oops`.
pub fn script_is_complete(sql: &str) -> Result<bool> {
    let bytes = sql.as_bytes();
    let mut index = 0;
    let mut has_complete_statement = false;
    let mut has_current_statement_content = false;

    while index < bytes.len() {
        match bytes[index] {
            // A quoted run. The lexer has no double-quoted strings and no comments,
            // so these two quotes are the only places a `;` can hide.
            // `''` and `` `` `` are escapes for their own delimiter, and
            // inside a single-quoted literal `\'` and `\\` are too — all three keep
            // the run open, which is the only thing that matters here. `X'..'` needs
            // no case of its own: it starts with an ordinary identifier byte and its
            // hex digits end at the same closing quote.
            quote @ (b'\'' | b'`') => {
                has_current_statement_content = true;
                index += 1;
                loop {
                    let Some(&byte) = bytes.get(index) else {
                        // Ran off the end still inside the literal.
                        return Ok(false);
                    };
                    if byte == b'\\'
                        && quote == b'\''
                        && matches!(bytes.get(index + 1), Some(b'\'' | b'\\'))
                    {
                        index += 2;
                        continue;
                    }
                    if byte == quote {
                        if bytes.get(index + 1) == Some(&quote) {
                            index += 2;
                            continue;
                        }
                        index += 1;
                        break;
                    }
                    index += 1;
                }
            }
            b';' => {
                if has_current_statement_content {
                    has_complete_statement = true;
                    has_current_statement_content = false;
                }
                index += 1;
            }
            byte => {
                if !byte.is_ascii_whitespace() {
                    has_current_statement_content = true;
                }
                index += 1;
            }
        }
    }

    Ok(has_complete_statement && !has_current_statement_content)
}

/// Splits a token stream at semicolons.
///
/// A trailing statement with no semicolon is either kept as a range of its own (its
/// semicolon is supplied by `ScriptIter` when it runs) or left out, depending on
/// `terminate_trailing_statement`; the returned flag says whether an unterminated
/// statement was left out.
fn split_statement_ranges(
    tokens: &[Token],
    terminate_trailing_statement: bool,
) -> (Vec<Range<usize>>, bool) {
    let mut statements = Vec::new();
    let mut start = 0usize;
    let mut has_current_statement_tokens = false;

    for (index, token) in tokens.iter().enumerate() {
        if token.kind != TokenKind::Semicolon {
            has_current_statement_tokens = true;
            continue;
        }

        if has_current_statement_tokens {
            statements.push(start..index + 1);
            has_current_statement_tokens = false;
        }
        // Either way the next statement begins after this semicolon; a run of empty
        // statements collapses because `start` keeps moving and nothing is pushed.
        start = index + 1;
    }

    if has_current_statement_tokens {
        if terminate_trailing_statement {
            statements.push(start..tokens.len());
            return (statements, false);
        }
        return (statements, true);
    }

    (statements, false)
}

/// Runs a script one statement per `next`.
///
/// The whole script is lexed up front, so a lexer error fails `iter_script` before
/// anything runs. Parsing is per statement: a syntax error in statement N surfaces
/// from the N-th `next`, after every statement before it has already executed
/// (and, outside an explicit transaction, committed).
pub struct ScriptIter<'a> {
    connection: &'a mut Connection,
    sql: &'a str,
    tokens: Vec<Token>,
    statements: VecDeque<Range<usize>>,
}

impl<'a> ScriptIter<'a> {
    pub(crate) fn new(connection: &'a mut Connection, script: ScriptTokens<'a>) -> Self {
        Self {
            connection,
            sql: script.sql,
            tokens: script.tokens,
            statements: script.statements.into(),
        }
    }
}

impl Iterator for ScriptIter<'_> {
    type Item = Result<ExecutedStatement>;

    fn next(&mut self) -> Option<Self::Item> {
        let range = self.statements.pop_front()?;
        // A statement that ran off the end of the script has no semicolon of its own.
        // The parser requires one, so supply it rather than make every statement rule
        // treat end-of-input as a terminator.
        let mut tokens = self.tokens[range.clone()].to_vec();
        if tokens.last().map(|token| token.kind) != Some(TokenKind::Semicolon) {
            tokens.push(Token::new(
                TokenKind::Semicolon,
                self.sql.len(),
                self.sql.len(),
            ));
        }
        let mut parser = Parser::new(self.sql, tokens);
        // The parser builds into an arena `Ast`; execution takes the owned tree.
        let statement = parser.parse().map(|root| {
            let mut ast = parser.into_ast();
            ast.set_root(root);
            ast.to_owned_statement()
        });
        Some(statement.and_then(|statement| self.connection.execute_statement_result(statement)))
    }
}
