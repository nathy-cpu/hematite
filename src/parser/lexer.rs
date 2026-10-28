//! SQL query lexer for tokenizing SQL statements
//!
//! A token is a kind plus a byte range into the input, never owned text. Three
//! things follow from that, and they are the reason it is shaped this way:
//!
//! 1. `Token` is `Copy` and 12 bytes, against 32 for a kind plus an owned `String`,
//!    so the token buffer is a third the size and can be reused across statements —
//!    it borrows nothing.
//! 2. Comparing tokens is a `u8` compare on the kind, not a comparison that might
//!    walk a string or a byte vector.
//! 3. Nothing in the lexer allocates beyond the token buffer. Text that needs
//!    unescaping (`''`, `\\'`, doubled backticks) is reported as a distinct kind
//!    carrying the *raw* span, and unescaped by the parser only when it builds the
//!    node that needs it, into the arena's text buffer.

use std::borrow::Cow;

use crate::error::{HematiteError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TokenKind {
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
    Int,
    UInt8,
    UInt16,
    UInt64,
    UInt32,
    UInt,
    Bool,
    Float32,
    Float64,
    Decimal,
    Blob,
    Varchar,
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
    //
    // Each carries its text as the token's span. The `Escaped` kinds mark the slow
    // paths — a literal or identifier whose source text is not its value — so the
    // parser knows to unescape rather than slice.
    Identifier,
    EscapedIdentifier,
    StringLiteral,
    EscapedStringLiteral,
    /// Span covers the hex digits only, without the `X'` and `'`. Validated by the
    /// lexer, decoded by the parser.
    BlobLiteral,
    NumberLiteral,
    /// `TRUE` or `FALSE`; the span says which.
    BooleanLiteral,
    NullLiteral,
}

/// A kind and the input bytes it came from.
///
/// `start`/`len` are `u32`: a statement long enough to overflow them would exhaust
/// memory long before it reached the lexer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Token {
    pub kind: TokenKind,
    pub start: u32,
    pub len: u32,
}

impl Token {
    #[inline]
    pub fn new(kind: TokenKind, start: usize, end: usize) -> Self {
        Self {
            kind,
            start: start as u32,
            len: (end - start) as u32,
        }
    }

    /// The kind this token dispatches as.
    ///
    /// An escaped identifier is still an identifier and an escaped string literal is
    /// still a string literal; the distinction exists only so the routine that reads
    /// the *text* knows it has to unescape. Every routine that decides what a token
    /// *is* should ask this rather than read `kind` directly.
    #[inline]
    pub fn dispatch_kind(&self) -> TokenKind {
        match self.kind {
            TokenKind::EscapedIdentifier => TokenKind::Identifier,
            TokenKind::EscapedStringLiteral => TokenKind::StringLiteral,
            other => other,
        }
    }

    /// The token's own text, sliced out of the input it was lexed from.
    ///
    /// Passing the wrong input yields the wrong text rather than a panic, so callers
    /// hold the two together: [`Parser`](crate::parser::Parser) owns both.
    #[inline]
    pub fn text<'a>(&self, input: &'a str) -> &'a str {
        &input[self.start as usize..(self.start + self.len) as usize]
    }
}

#[derive(Debug, Clone)]
pub struct Lexer<'a> {
    input: &'a str,
    position: usize,
    tokens: Vec<Token>,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        // One token per ~8 bytes of SQL is a slight over-estimate for real
        // statements, which is what a capacity hint should be. A fixed capacity
        // over-allocates for a four-token point lookup and still grows for anything
        // larger.
        Self::with_capacity(input, input.len() / 8 + 8)
    }

    /// Create a lexer with a pre-sized token buffer to avoid repeated reallocations.
    pub fn with_capacity(input: &'a str, token_capacity: usize) -> Self {
        Self {
            input,
            position: 0,
            tokens: Vec::with_capacity(token_capacity),
        }
    }

    /// Lex `input` into a buffer the caller already owns, so a caller that lexes
    /// repeatedly pays for the buffer once.
    pub fn tokenize_into(input: &'a str, tokens: &mut Vec<Token>) -> Result<()> {
        tokens.clear();
        let mut lexer = Lexer {
            input,
            position: 0,
            tokens: std::mem::take(tokens),
        };
        let result = lexer.tokenize();
        *tokens = lexer.tokens;
        result
    }

    pub fn input(&self) -> &'a str {
        self.input
    }

    pub fn tokenize(&mut self) -> Result<()> {
        while self.position < self.input.len() {
            self.skip_whitespace();

            if self.position >= self.input.len() {
                break;
            }

            let byte = self.current_byte();

            if byte == b'X' && self.peek_byte() == Some(b'\'') {
                self.read_blob_literal()?;
            } else if is_identifier_start_byte(byte)
                || (!byte.is_ascii() && self.current_char().is_alphabetic())
            {
                self.read_identifier();
            } else if byte == b'`' {
                self.read_quoted_identifier()?;
            } else if byte == b'\'' {
                self.read_string_literal()?;
            } else if byte.is_ascii_digit() {
                self.read_number()?;
            } else {
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

    #[inline]
    fn push(&mut self, kind: TokenKind, start: usize, end: usize) {
        self.tokens.push(Token::new(kind, start, end));
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

    fn read_identifier(&mut self) {
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

        let end = self.position;
        self.push(self.classify_identifier(start, end), start, end);
    }

    /// Keywords are matched **case-sensitively**, and that is load-bearing: a
    /// lowercase keyword must fall through to `Identifier` so the parser can report
    /// "must be capitalized". Matching case-insensitively here would silently widen
    /// the dialect and make those diagnostics unreachable.
    ///
    /// The first-byte test skips the whole match for lowercase identifiers, which is
    /// what user table and column names normally look like.
    fn classify_identifier(&self, start: usize, end: usize) -> TokenKind {
        let identifier = &self.input[start..end];
        if !identifier.as_bytes()[0].is_ascii_uppercase() {
            return TokenKind::Identifier;
        }
        match identifier {
            "BEGIN" => TokenKind::Begin,
            "COMMIT" => TokenKind::Commit,
            "ROLLBACK" => TokenKind::Rollback,
            "SAVEPOINT" => TokenKind::Savepoint,
            "RELEASE" => TokenKind::Release,
            "SELECT" => TokenKind::Select,
            "UPDATE" => TokenKind::Update,
            "FROM" => TokenKind::From,
            "INSERT" => TokenKind::Insert,
            "DELETE" => TokenKind::Delete,
            "DROP" => TokenKind::Drop,
            "EXPLAIN" => TokenKind::Explain,
            "DESCRIBE" => TokenKind::Describe,
            "SHOW" => TokenKind::Show,
            "TABLES" => TokenKind::Tables,
            "VIEWS" => TokenKind::Views,
            "INDEXES" => TokenKind::Indexes,
            "TRIGGERS" => TokenKind::Triggers,
            "ALTER" => TokenKind::Alter,
            "ADD" => TokenKind::Add,
            "IF" => TokenKind::If,
            "INTO" => TokenKind::Into,
            "SET" => TokenKind::Set,
            "VALUES" => TokenKind::Values,
            "CREATE" => TokenKind::Create,
            "VIEW" => TokenKind::View,
            "TRIGGER" => TokenKind::Trigger,
            "INDEX" => TokenKind::Index,
            "EXISTS" => TokenKind::Exists,
            "UNION" => TokenKind::Union,
            "INTERSECT" => TokenKind::Intersect,
            "EXCEPT" => TokenKind::Except,
            "ALL" => TokenKind::All,
            "WITH" => TokenKind::With,
            "LEFT" => TokenKind::Left,
            "RIGHT" => TokenKind::Right,
            "FULL" => TokenKind::Full,
            "OUTER" => TokenKind::Outer,
            "INNER" => TokenKind::Inner,
            "JOIN" => TokenKind::Join,
            "ON" => TokenKind::On,
            "AS" => TokenKind::As,
            "DISTINCT" => TokenKind::Distinct,
            "CAST" => TokenKind::Cast,
            "TABLE" => TokenKind::Table,
            "COLUMN" => TokenKind::Column,
            "WHERE" => TokenKind::Where,
            "GROUP" => TokenKind::Group,
            "HAVING" => TokenKind::Having,
            "ORDER" => TokenKind::Order,
            "BY" => TokenKind::By,
            "ASC" => TokenKind::Asc,
            "DESC" => TokenKind::Desc,
            "LIMIT" => TokenKind::Limit,
            "OFFSET" => TokenKind::Offset,
            "COUNT" => TokenKind::Count,
            "SUM" => TokenKind::Sum,
            "AVG" => TokenKind::Avg,
            "MIN" => TokenKind::Min,
            "MAX" => TokenKind::Max,
            "INT8" => TokenKind::Int8,
            "INT16" => TokenKind::Int16,
            "INT64" => TokenKind::Int64,
            "INT32" => TokenKind::Int32,
            "INT" => TokenKind::Int,
            "UINT8" => TokenKind::UInt8,
            "UINT16" => TokenKind::UInt16,
            "UINT64" => TokenKind::UInt64,
            "UINT32" => TokenKind::UInt32,
            "UINT" => TokenKind::UInt,
            "TEXT" => TokenKind::Text,
            "BOOLEAN" => TokenKind::Boolean,
            "BOOL" => TokenKind::Bool,
            "FLOAT" => TokenKind::Float,
            "FLOAT32" => TokenKind::Float32,
            "FLOAT64" => TokenKind::Float64,
            "DECIMAL" => TokenKind::Decimal,
            "BLOB" => TokenKind::Blob,
            "VARCHAR" => TokenKind::Varchar,
            "AUTO_INCREMENT" => TokenKind::AutoIncrement,
            "UNIQUE" => TokenKind::Unique,
            "PRIMARY" => TokenKind::Primary,
            "KEY" => TokenKind::Key,
            "DUPLICATE" => TokenKind::Duplicate,
            "CONSTRAINT" => TokenKind::Constraint,
            "CHECK" => TokenKind::Check,
            "FOREIGN" => TokenKind::Foreign,
            "REFERENCES" => TokenKind::References,
            "CASCADE" => TokenKind::Cascade,
            "RESTRICT" => TokenKind::Restrict,
            "RENAME" => TokenKind::Rename,
            "TO" => TokenKind::To,
            "AFTER" => TokenKind::After,
            "NOT" => TokenKind::Not,
            "IS" => TokenKind::Is,
            "NULL" => TokenKind::Null,
            "DEFAULT" => TokenKind::Default,
            "IN" => TokenKind::In,
            "BETWEEN" => TokenKind::Between,
            "LIKE" => TokenKind::Like,
            "CASE" => TokenKind::Case,
            "WHEN" => TokenKind::When,
            "THEN" => TokenKind::Then,
            "ELSE" => TokenKind::Else,
            "END" => TokenKind::End,
            "AND" => TokenKind::And,
            "OR" => TokenKind::Or,
            "TRUE" => TokenKind::BooleanLiteral,
            "FALSE" => TokenKind::BooleanLiteral,
            _ => TokenKind::Identifier,
        }
    }

    fn read_quoted_identifier(&mut self) -> Result<()> {
        self.advance_char();
        let content_start = self.position;

        while self.position < self.input.len() {
            let ch = self.current_char();
            if ch == '`' {
                // A doubled backtick means the text is not its own value, so the
                // parser has to unescape it. Report the raw span and say so.
                if self.peek_char() == Some('`') {
                    return self.read_escaped_quoted_identifier(content_start);
                }

                let end = self.position;
                self.advance_char();
                self.push(TokenKind::Identifier, content_start, end);
                return Ok(());
            }

            self.advance_char();
        }

        Err(HematiteError::ParseError(
            "Unterminated quoted identifier".to_string(),
        ))
    }

    fn read_escaped_quoted_identifier(&mut self, content_start: usize) -> Result<()> {
        while self.position < self.input.len() {
            let ch = self.current_char();
            if ch == '`' {
                if self.peek_char() == Some('`') {
                    self.advance_char();
                    self.advance_char();
                    continue;
                }

                let end = self.position;
                self.advance_char();
                self.push(TokenKind::EscapedIdentifier, content_start, end);
                return Ok(());
            }

            self.advance_char();
        }

        Err(HematiteError::ParseError(
            "Unterminated quoted identifier".to_string(),
        ))
    }

    fn read_string_literal(&mut self) -> Result<()> {
        self.advance_char(); // Skip opening quote
        let content_start = self.position;

        while self.position < self.input.len() {
            let ch = self.current_char();
            if ch == '\'' {
                if self.peek_char() == Some('\'') {
                    return self.read_escaped_string_literal(content_start);
                }

                let end = self.position;
                self.push(TokenKind::StringLiteral, content_start, end);
                self.advance_char(); // Skip closing quote
                return Ok(());
            }

            if ch == '\\' {
                if let Some(next) = self.peek_char() {
                    if next == '\'' || next == '\\' {
                        return self.read_escaped_string_literal(content_start);
                    }
                }
            }

            self.advance_char();
        }

        Err(HematiteError::ParseError(
            "Unterminated string literal".to_string(),
        ))
    }

    fn read_escaped_string_literal(&mut self, content_start: usize) -> Result<()> {
        while self.position < self.input.len() {
            let ch = self.current_char();
            if ch == '\'' {
                if self.peek_char() == Some('\'') {
                    self.advance_char();
                    self.advance_char();
                    continue;
                }

                let end = self.position;
                self.push(TokenKind::EscapedStringLiteral, content_start, end);
                self.advance_char(); // Skip closing quote
                return Ok(());
            }

            if ch == '\\' {
                if let Some(next) = self.peek_char() {
                    if next == '\'' || next == '\\' {
                        self.advance_char();
                        self.advance_char();
                        continue;
                    }
                }
            }

            self.advance_char();
        }

        Err(HematiteError::ParseError(
            "Unterminated string literal".to_string(),
        ))
    }

    /// Validates the hex run but does not decode it. Decoding needs somewhere to put
    /// the bytes, and the parser has the arena; the lexer would have to allocate.
    /// The two error cases stay here because they are lexical.
    fn read_blob_literal(&mut self) -> Result<()> {
        self.position += 2; // Skip X'
        let content_start = self.position;
        let mut digits = 0usize;

        while self.position < self.input.len() {
            let byte = self.current_byte();
            if byte == b'\'' {
                if !digits.is_multiple_of(2) {
                    return Err(HematiteError::ParseError(
                        "Hex blob literal must contain an even number of digits".to_string(),
                    ));
                }

                let end = self.position;
                self.push(TokenKind::BlobLiteral, content_start, end);
                self.position += 1; // Skip closing quote
                return Ok(());
            }

            if hex_nibble(byte).is_none() {
                return Err(HematiteError::ParseError(
                    "Hex blob literal may only contain hexadecimal digits".to_string(),
                ));
            }
            digits += 1;
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

        self.push(TokenKind::NumberLiteral, start, self.position);
        Ok(())
    }

    fn read_operator_or_punctuation(&mut self) -> Result<()> {
        let start = self.position;
        let byte = self.current_byte();
        let kind = match byte {
            b'=' => TokenKind::Equal,
            b'!' => {
                if self.peek_byte() == Some(b'=') {
                    self.position += 1;
                    TokenKind::NotEqual
                } else {
                    TokenKind::Not
                }
            }
            b'<' => {
                if self.peek_byte() == Some(b'=') {
                    self.position += 1;
                    TokenKind::LessThanOrEqual
                } else if self.peek_byte() == Some(b'>') {
                    self.position += 1;
                    TokenKind::NotEqual
                } else {
                    TokenKind::LessThan
                }
            }
            b'>' => {
                if self.peek_byte() == Some(b'=') {
                    self.position += 1;
                    TokenKind::GreaterThanOrEqual
                } else {
                    TokenKind::GreaterThan
                }
            }
            b'&' => {
                if self.peek_byte() == Some(b'&') {
                    self.position += 1;
                    TokenKind::And
                } else {
                    return Err(HematiteError::ParseError("Invalid operator".to_string()));
                }
            }
            b'|' => {
                if self.peek_byte() == Some(b'|') {
                    self.position += 1;
                    TokenKind::Or
                } else {
                    return Err(HematiteError::ParseError("Invalid operator".to_string()));
                }
            }
            b',' => TokenKind::Comma,
            b'.' => TokenKind::Dot,
            b';' => TokenKind::Semicolon,
            b'(' => TokenKind::LeftParen,
            b')' => TokenKind::RightParen,
            b'+' => TokenKind::Plus,
            b'-' => TokenKind::Minus,
            b'*' => TokenKind::Asterisk,
            b'/' => TokenKind::Slash,
            b'%' => TokenKind::Percent,
            b'?' => TokenKind::Placeholder,
            _ => {
                return Err(HematiteError::ParseError(format!(
                    "Unexpected character: {}",
                    self.current_char()
                )))
            }
        };

        self.position += 1;
        self.push(kind, start, self.position);
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

// ── Turning a span back into a value ─────────────────────────────────────────
//
// The lexer reports where a literal is, not what it says. These do the second half,
// and live here rather than in the parser because what an escape means is lexical.
// Each borrows unless the text genuinely differs from the value.

/// An identifier's value. Only a backtick-quoted identifier containing doubled
/// backticks has text that is not its own value.
pub(crate) fn unescape_identifier(kind: TokenKind, text: &str) -> Cow<'_, str> {
    match kind {
        TokenKind::EscapedIdentifier => Cow::Owned(text.replace("``", "`")),
        _ => Cow::Borrowed(text),
    }
}

/// A string literal's value, with doubled quotes and backslash escapes resolved.
pub(crate) fn unescape_string_literal(kind: TokenKind, text: &str) -> Cow<'_, str> {
    if kind != TokenKind::EscapedStringLiteral {
        return Cow::Borrowed(text);
    }

    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        match ch {
            // The lexer ends a literal at a lone quote, so a quote inside one is
            // always the first of a doubled pair.
            '\'' => {
                chars.next();
                out.push('\'');
            }
            '\\' => match chars.next() {
                Some(next @ ('\'' | '\\')) => out.push(next),
                Some(next) => {
                    out.push('\\');
                    out.push(next);
                }
                None => out.push('\\'),
            },
            other => out.push(other),
        }
    }
    Cow::Owned(out)
}

/// Decodes the hex digits of a blob literal.
///
/// The lexer has already rejected an odd digit count and any non-hex digit, so this
/// cannot fail on a span it produced.
pub(crate) fn decode_hex_blob(digits: &str) -> Vec<u8> {
    let digits = digits.as_bytes();
    let mut bytes = Vec::with_capacity(digits.len() / 2);
    for [high, low] in digits.as_chunks::<2>().0 {
        bytes.push((hex_nibble(*high).unwrap_or(0) << 4) | hex_nibble(*low).unwrap_or(0));
    }
    bytes
}
