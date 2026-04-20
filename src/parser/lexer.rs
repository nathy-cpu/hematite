//! SQL query lexer for tokenizing SQL statements

use crate::error::{HematiteError, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Begin,
    Commit,
    Rollback,
    Savepoint,
    Release,
    Select,
    Update,
    From,
    Insert,
    Delete,
    Drop,
    Explain,
    Describe,
    Show,
    Tables,
    Views,
    Indexes,
    Triggers,
    Alter,
    Add,
    If,
    Into,
    Set,
    Values,
    Create,
    View,
    Trigger,
    Index,
    Exists,
    Union,
    Intersect,
    Except,
    All,
    With,
    Recursive,
    Left,
    Right,
    Full,
    Outer,
    Inner,
    Join,
    On,
    As,
    Distinct,
    Cast,
    Table,
    Column,
    Where,
    Group,
    Having,
    Order,
    By,
    Asc,
    Desc,
    Over,
    Partition,
    Interval,
    Limit,
    Offset,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Int32,
    Text,
    Boolean,
    Float,
    Int8,
    Int16,
    Int64,
    Int128,
    Int,
    UInt8,
    UInt16,
    UInt64,
    UInt128,
    UInt32,
    UInt,
    Bool,
    Float32,
    Float64,
    Decimal,
    Blob,
    Date,
    Time,
    DateTime,
    Zone,
    Char,
    Varchar,
    BinaryType,
    VarBinary,
    Enum,
    AutoIncrement,
    Unique,
    Primary,
    Key,
    Duplicate,
    Constraint,
    Check,
    Foreign,
    References,
    Cascade,
    Restrict,
    Rename,
    To,
    After,
    Not,
    Is,
    Null,
    Default,
    In,
    Between,
    Like,
    Case,
    When,
    Then,
    Else,
    End,
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
    Dot,
    Semicolon,
    LeftParen,
    RightParen,
    Plus,
    Minus,
    Asterisk,
    Slash,
    Percent,
    Placeholder,

    // Literals
    Identifier(String),
    StringLiteral(String),
    BlobLiteral(Vec<u8>),
    NumberLiteral(String),
    BooleanLiteral(bool),
    NullLiteral,
}

#[derive(Debug, Clone)]
pub struct Lexer<'a> {
    input: &'a str,
    position: usize,
    tokens: Vec<Token>,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
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

            let byte = self.current_byte();

            // Handle identifiers and keywords
            if byte == b'X' && self.peek_byte() == Some(b'\'') {
                self.read_blob_literal()?;
            } else if is_identifier_start_byte(byte)
                || (!byte.is_ascii() && self.current_char().is_alphabetic())
            {
                self.read_identifier()?;
            } else if byte == b'`' {
                self.read_quoted_identifier()?;
            }
            // Handle string literals
            else if byte == b'\'' {
                self.read_string_literal()?;
            }
            // Handle numbers
            else if byte.is_ascii_digit() {
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

    pub fn into_tokens(self) -> Vec<Token> {
        self.tokens
    }

    fn skip_whitespace(&mut self) {
        while self.position < self.input.len() {
            let byte = self.current_byte();
            if byte.is_ascii_whitespace() {
                self.position += 1;
                continue;
            }
            if byte.is_ascii() || !self.current_char().is_whitespace() {
                break;
            }
            self.advance_char();
        }
    }

    fn current_byte(&self) -> u8 {
        self.input.as_bytes()[self.position]
    }

    fn peek_byte(&self) -> Option<u8> {
        self.input.as_bytes().get(self.position + 1).copied()
    }

    fn current_char(&self) -> char {
        self.input[self.position..].chars().next().unwrap_or('\0')
    }

    fn peek_char(&self) -> Option<char> {
        let mut chars = self.input[self.position..].chars();
        chars.next()?;
        chars.next()
    }

    fn advance_char(&mut self) {
        if self.position < self.input.len() {
            self.position += self.current_char().len_utf8();
        }
    }

    fn read_identifier(&mut self) -> Result<()> {
        let start = self.position;

        while self.position < self.input.len() {
            let byte = self.current_byte();
            if is_identifier_continue_byte(byte) {
                self.position += 1;
            } else if !byte.is_ascii() {
                let ch = self.current_char();
                if ch.is_alphanumeric() || ch == '_' {
                    self.advance_char();
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        let identifier = &self.input[start..self.position];
        let token = match identifier {
            "BEGIN" => Token::Begin,
            "COMMIT" => Token::Commit,
            "ROLLBACK" => Token::Rollback,
            "SAVEPOINT" => Token::Savepoint,
            "RELEASE" => Token::Release,
            "SELECT" => Token::Select,
            "UPDATE" => Token::Update,
            "FROM" => Token::From,
            "INSERT" => Token::Insert,
            "DELETE" => Token::Delete,
            "DROP" => Token::Drop,
            "EXPLAIN" => Token::Explain,
            "DESCRIBE" => Token::Describe,
            "SHOW" => Token::Show,
            "TABLES" => Token::Tables,
            "VIEWS" => Token::Views,
            "INDEXES" => Token::Indexes,
            "TRIGGERS" => Token::Triggers,
            "ALTER" => Token::Alter,
            "ADD" => Token::Add,
            "IF" => Token::If,
            "INTO" => Token::Into,
            "SET" => Token::Set,
            "VALUES" => Token::Values,
            "CREATE" => Token::Create,
            "VIEW" => Token::View,
            "TRIGGER" => Token::Trigger,
            "INDEX" => Token::Index,
            "EXISTS" => Token::Exists,
            "UNION" => Token::Union,
            "INTERSECT" => Token::Intersect,
            "EXCEPT" => Token::Except,
            "ALL" => Token::All,
            "WITH" => Token::With,
            "RECURSIVE" => Token::Recursive,
            "LEFT" => Token::Left,
            "RIGHT" => Token::Right,
            "FULL" => Token::Full,
            "OUTER" => Token::Outer,
            "INNER" => Token::Inner,
            "JOIN" => Token::Join,
            "ON" => Token::On,
            "AS" => Token::As,
            "DISTINCT" => Token::Distinct,
            "CAST" => Token::Cast,
            "TABLE" => Token::Table,
            "COLUMN" => Token::Column,
            "WHERE" => Token::Where,
            "GROUP" => Token::Group,
            "HAVING" => Token::Having,
            "ORDER" => Token::Order,
            "BY" => Token::By,
            "ASC" => Token::Asc,
            "DESC" => Token::Desc,
            "OVER" => Token::Over,
            "PARTITION" => Token::Partition,
            "INTERVAL" => Token::Interval,
            "LIMIT" => Token::Limit,
            "OFFSET" => Token::Offset,
            "COUNT" => Token::Count,
            "SUM" => Token::Sum,
            "AVG" => Token::Avg,
            "MIN" => Token::Min,
            "MAX" => Token::Max,
            "INT8" => Token::Int8,
            "INT16" => Token::Int16,
            "INT64" => Token::Int64,
            "INT128" => Token::Int128,
            "INT32" => Token::Int32,
            "INT" => Token::Int,
            "UINT8" => Token::UInt8,
            "UINT16" => Token::UInt16,
            "UINT64" => Token::UInt64,
            "UINT128" => Token::UInt128,
            "UINT32" => Token::UInt32,
            "UINT" => Token::UInt,
            "TEXT" => Token::Text,
            "BOOLEAN" => Token::Boolean,
            "BOOL" => Token::Bool,
            "FLOAT" => Token::Float,
            "FLOAT32" => Token::Float32,
            "FLOAT64" => Token::Float64,
            "DECIMAL" => Token::Decimal,
            "BLOB" => Token::Blob,
            "DATE" => Token::Date,
            "TIME" => Token::Time,
            "DATETIME" => Token::DateTime,
            "ZONE" => Token::Zone,
            "CHAR" => Token::Char,
            "VARCHAR" => Token::Varchar,
            "BINARY" => Token::BinaryType,
            "VARBINARY" => Token::VarBinary,
            "ENUM" => Token::Enum,
            "AUTO_INCREMENT" => Token::AutoIncrement,
            "UNIQUE" => Token::Unique,
            "PRIMARY" => Token::Primary,
            "KEY" => Token::Key,
            "DUPLICATE" => Token::Duplicate,
            "CONSTRAINT" => Token::Constraint,
            "CHECK" => Token::Check,
            "FOREIGN" => Token::Foreign,
            "REFERENCES" => Token::References,
            "CASCADE" => Token::Cascade,
            "RESTRICT" => Token::Restrict,
            "RENAME" => Token::Rename,
            "TO" => Token::To,
            "AFTER" => Token::After,
            "NOT" => Token::Not,
            "IS" => Token::Is,
            "NULL" => Token::Null,
            "DEFAULT" => Token::Default,
            "IN" => Token::In,
            "BETWEEN" => Token::Between,
            "LIKE" => Token::Like,
            "CASE" => Token::Case,
            "WHEN" => Token::When,
            "THEN" => Token::Then,
            "ELSE" => Token::Else,
            "END" => Token::End,
            "AND" => Token::And,
            "OR" => Token::Or,
            "TRUE" => Token::BooleanLiteral(true),
            "FALSE" => Token::BooleanLiteral(false),
            _ => Token::Identifier(identifier.to_string()),
        };

        self.tokens.push(token);
        Ok(())
    }

    fn read_quoted_identifier(&mut self) -> Result<()> {
        self.advance_char();
        let mut identifier = String::new();

        while self.position < self.input.len() {
            let ch = self.current_char();
            if ch == '`' {
                if self.peek_char() == Some('`') {
                    identifier.push('`');
                    self.advance_char();
                    self.advance_char();
                    continue;
                }

                self.advance_char();
                self.tokens.push(Token::Identifier(identifier));
                return Ok(());
            }

            identifier.push(ch);
            self.advance_char();
        }

        Err(HematiteError::ParseError(
            "Unterminated quoted identifier".to_string(),
        ))
    }

    fn read_string_literal(&mut self) -> Result<()> {
        self.advance_char(); // Skip opening quote
        let mut literal = String::new();

        while self.position < self.input.len() {
            let ch = self.current_char();
            if ch == '\'' {
                if self.peek_char() == Some('\'') {
                    literal.push('\'');
                    self.advance_char();
                    self.advance_char();
                    continue;
                }

                self.tokens.push(Token::StringLiteral(literal));
                self.advance_char(); // Skip closing quote
                return Ok(());
            }

            if ch == '\\' {
                if let Some(next) = self.peek_char() {
                    if next == '\'' || next == '\\' {
                        literal.push(next);
                        self.advance_char();
                        self.advance_char();
                        continue;
                    }
                }
            }

            literal.push(ch);
            self.advance_char();
        }

        Err(HematiteError::ParseError(
            "Unterminated string literal".to_string(),
        ))
    }

    fn read_blob_literal(&mut self) -> Result<()> {
        self.position += 2; // Skip X'
        let mut bytes = Vec::new();
        let mut high_nibble = None;

        while self.position < self.input.len() {
            let byte = self.current_byte();
            if byte == b'\'' {
                if high_nibble.is_some() {
                    return Err(HematiteError::ParseError(
                        "Hex blob literal must contain an even number of digits".to_string(),
                    ));
                }

                self.tokens.push(Token::BlobLiteral(bytes));
                self.position += 1; // Skip closing quote
                return Ok(());
            }

            let nibble = hex_nibble(byte).ok_or_else(|| {
                HematiteError::ParseError(
                    "Hex blob literal may only contain hexadecimal digits".to_string(),
                )
            })?;
            if let Some(high) = high_nibble.take() {
                bytes.push((high << 4) | nibble);
            } else {
                high_nibble = Some(nibble);
            }
            self.position += 1;
        }

        Err(HematiteError::ParseError(
            "Unterminated hex blob literal".to_string(),
        ))
    }

    fn read_number(&mut self) -> Result<()> {
        let start = self.position;
        let mut has_decimal = false;

        while self.position < self.input.len() {
            let byte = self.current_byte();
            if byte == b'.' {
                if has_decimal {
                    return Err(HematiteError::ParseError(
                        "Invalid number format".to_string(),
                    ));
                }
                has_decimal = true;
                self.position += 1;
            } else if byte.is_ascii_digit() {
                self.position += 1;
            } else {
                break;
            }
        }

        let number_str = &self.input[start..self.position];
        self.tokens
            .push(Token::NumberLiteral(number_str.to_string()));

        Ok(())
    }

    fn read_operator_or_punctuation(&mut self) -> Result<()> {
        let byte = self.current_byte();
        let token = match byte {
            b'=' => Token::Equal,
            b'!' => {
                if self.peek_byte() == Some(b'=') {
                    self.position += 1;
                    Token::NotEqual
                } else {
                    Token::Not
                }
            }
            b'<' => {
                if self.peek_byte() == Some(b'=') {
                    self.position += 1;
                    Token::LessThanOrEqual
                } else if self.peek_byte() == Some(b'>') {
                    self.position += 1;
                    Token::NotEqual
                } else {
                    Token::LessThan
                }
            }
            b'>' => {
                if self.peek_byte() == Some(b'=') {
                    self.position += 1;
                    Token::GreaterThanOrEqual
                } else {
                    Token::GreaterThan
                }
            }
            b'&' => {
                if self.peek_byte() == Some(b'&') {
                    self.position += 1;
                    Token::And
                } else {
                    return Err(HematiteError::ParseError("Invalid operator".to_string()));
                }
            }
            b'|' => {
                if self.peek_byte() == Some(b'|') {
                    self.position += 1;
                    Token::Or
                } else {
                    return Err(HematiteError::ParseError("Invalid operator".to_string()));
                }
            }
            b',' => Token::Comma,
            b'.' => Token::Dot,
            b';' => Token::Semicolon,
            b'(' => Token::LeftParen,
            b')' => Token::RightParen,
            b'+' => Token::Plus,
            b'-' => Token::Minus,
            b'*' => Token::Asterisk,
            b'/' => Token::Slash,
            b'%' => Token::Percent,
            b'?' => Token::Placeholder,
            _ => {
                return Err(HematiteError::ParseError(format!(
                    "Unexpected character: {}",
                    self.current_char()
                )))
            }
        };

        self.position += 1;
        self.tokens.push(token);
        Ok(())
    }
}

fn is_identifier_start_byte(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_identifier_continue_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
