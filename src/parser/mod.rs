//! SQL text to a parsed statement.
//!
//! [`lexer`] turns SQL into `Copy` tokens that are spans of the input. [`parser`]
//! builds one statement from them into the flat arena in [`arena`] ([`arena::Ast`]:
//! `u32` ids, names as spans of the SQL). The query layer still reads the owned tree
//! in [`ast`], so [`parse_sql_statement`] parses into a fresh arena and then
//! materializes that tree through [`bridge`]. [`types`] holds `SqlTypeName`, which
//! both forms use, and the owned tree's `LiteralValue`.

pub mod arena;
pub mod ast;
pub mod bridge;
pub mod lexer;
pub mod parser;
pub mod types;

pub use ast::*;
pub use lexer::Lexer;
pub use parser::{parse_sql_statement, tokenize_sql, Parser};
pub use types::*;

#[cfg(test)]
mod tests;
