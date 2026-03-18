//! SQL query lexer for tokenizing SQL statements

use crate::error::{HematiteError, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    From,
    Insert,
    Into,
    Values,
    Create,
    Table,
    Where,
    Integer,
    Text,
    Boolean,
    Float,
    Primary,
    Key,
    Not,
    Null,
    Default,
    And,
    Or,

    // Operators
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    // Note: logical operators are tokenized as keywords (AND/OR) and as symbols (&&/||)

    // Punctuation
    Comma,
    Semicolon,
    LeftParen,
    RightParen,
    Asterisk,

    // Literals
    Identifier(String),
    StringLiteral(String),
    NumberLiteral(f64),
    BooleanLiteral(bool),
    NullLiteral,
}

#[derive(Debug, Clone)]
pub struct Lexer {
    input: String,
    position: usize,
    tokens: Vec<Token>,
}

impl Lexer {
    pub fn new(input: String) -> Self {
        Self {
            input,
            position: 0,
            tokens: Vec::new(),
        }
    }

    pub fn tokenize(&mut self) -> Result<()> {
        while self.position < self.input.len() {
            self.skip_whitespace();

            if self.position >= self.input.len() {
                break;
            }

            let ch = self.current_char();

            // Handle identifiers and keywords
            if ch.is_alphabetic() || ch == '_' {
                self.read_identifier()?;
            }
            // Handle string literals
            else if ch == '\'' {
                self.read_string_literal()?;
            }
            // Handle numbers
            else if ch.is_ascii_digit() {
                self.read_number()?;
            }
            // Handle operators and punctuation
            else {
                self.read_operator_or_punctuation()?;
            }
        }

        Ok(())
    }

    pub fn get_tokens(&self) -> &[Token] {
        &self.tokens
    }

    fn skip_whitespace(&mut self) {
        while self.position < self.input.len() {
            let ch = self.current_char();
            if !ch.is_whitespace() {
                break;
            }
            self.position += 1;
        }
    }

    fn current_char(&self) -> char {
        self.input.chars().nth(self.position).unwrap_or('\0')
    }

    fn peek_char(&self) -> Option<char> {
        self.input.chars().nth(self.position + 1)
    }

    fn read_identifier(&mut self) -> Result<()> {
        let start = self.position;

        while self.position < self.input.len() {
            let ch = self.current_char();
            if ch.is_alphanumeric() || ch == '_' {
                self.position += 1;
            } else {
                break;
            }
        }

        let identifier = &self.input[start..self.position];
        let token = match identifier.to_uppercase().as_str() {
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "INSERT" => Token::Insert,
            "INTO" => Token::Into,
            "VALUES" => Token::Values,
            "CREATE" => Token::Create,
            "TABLE" => Token::Table,
            "WHERE" => Token::Where,
            "INTEGER" => Token::Integer,
            "TEXT" => Token::Text,
            "BOOLEAN" => Token::Boolean,
            "FLOAT" => Token::Float,
            "PRIMARY" => Token::Primary,
            "KEY" => Token::Key,
            "NOT" => Token::Not,
            "NULL" => Token::Null,
            "DEFAULT" => Token::Default,
            "AND" => Token::And,
            "OR" => Token::Or,
            "TRUE" => Token::BooleanLiteral(true),
            "FALSE" => Token::BooleanLiteral(false),
            _ => Token::Identifier(identifier.to_string()),
        };

        self.tokens.push(token);
        Ok(())
    }

    fn read_string_literal(&mut self) -> Result<()> {
        self.position += 1; // Skip opening quote
        let start = self.position;

        while self.position < self.input.len() {
            let ch = self.current_char();
            if ch == '\'' {
                let literal = &self.input[start..self.position];
                self.tokens.push(Token::StringLiteral(literal.to_string()));
                self.position += 1; // Skip closing quote
                return Ok(());
            }
            // Handle escaped quotes
            if ch == '\\' && self.peek_char() == Some('\'') {
                self.position += 2; // Skip backslash and quote
            } else {
                self.position += 1;
            }
        }

        Err(HematiteError::ParseError(
            "Unterminated string literal".to_string(),
        ))
    }

    fn read_number(&mut self) -> Result<()> {
        let start = self.position;
        let mut has_decimal = false;

        while self.position < self.input.len() {
            let ch = self.current_char();
            if ch == '.' {
                if has_decimal {
                    return Err(HematiteError::ParseError(
                        "Invalid number format".to_string(),
                    ));
                }
                has_decimal = true;
                self.position += 1;
            } else if ch.is_ascii_digit() {
                self.position += 1;
            } else {
                break;
            }
        }

        let number_str = &self.input[start..self.position];
        let number = number_str
            .parse::<f64>()
            .map_err(|_| HematiteError::ParseError("Invalid number".to_string()))?;

        if has_decimal {
            self.tokens.push(Token::NumberLiteral(number));
        } else {
            self.tokens.push(Token::NumberLiteral(number as f64));
        }

        Ok(())
    }

    fn read_operator_or_punctuation(&mut self) -> Result<()> {
        let ch = self.current_char();
        let token = match ch {
            '=' => Token::Equal,
            '!' => {
                if self.peek_char() == Some('=') {
                    self.position += 1;
                    Token::NotEqual
                } else {
                    Token::Not
                }
            }
            '<' => {
                if self.peek_char() == Some('=') {
                    self.position += 1;
                    Token::LessThanOrEqual
                } else {
                    Token::LessThan
                }
            }
            '>' => {
                if self.peek_char() == Some('=') {
                    self.position += 1;
                    Token::GreaterThanOrEqual
                } else {
                    Token::GreaterThan
                }
            }
            '&' => {
                if self.peek_char() == Some('&') {
                    self.position += 1;
                    Token::And
                } else {
                    return Err(HematiteError::ParseError("Invalid operator".to_string()));
                }
            }
            '|' => {
                if self.peek_char() == Some('|') {
                    self.position += 1;
                    Token::Or
                } else {
                    return Err(HematiteError::ParseError("Invalid operator".to_string()));
                }
            }
            ',' => Token::Comma,
            ';' => Token::Semicolon,
            '(' => Token::LeftParen,
            ')' => Token::RightParen,
            '*' => Token::Asterisk,
            _ => {
                return Err(HematiteError::ParseError(format!(
                    "Unexpected character: {}",
                    ch
                )))
            }
        };

        self.position += 1;
        self.tokens.push(token);
        Ok(())
    }
}

