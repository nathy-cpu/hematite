//! Hand-rolled recursive-descent SQL parser.
//!
//! Consumes the span tokens [`Lexer`] produces and builds one statement into an
//! arena [`Ast`]. [`Parser::parse`] returns the root `arena::Statement` rather
//! than storing it; [`parse_sql_reusing`] records it with [`Ast::set_root`]. The
//! query layer still reads the owned tree in `ast.rs`, so [`parse_sql_statement`]
//! follows the parse with [`Ast::to_owned_statement`] (`bridge.rs`).
//!
//! ## Shape
//!
//! Each grammar rule is a method — `parse_select_statement`, `parse_table_reference`,
//! `parse_column_definition` — and the methods call each other recursively down the
//! grammar. A hand-written parser costs no generator dependency, and every error
//! message is written where the parser knows what it expected.
//!
//! Decisions look at the next token's kind ([`Token::dispatch_kind`]), and at most
//! the one after it: `next_token_is(LeftParen)` is what tells a function call
//! (`lower(x)`, `LEFT(s, 2)`) from a column reference or a `LEFT JOIN`. Nothing
//! backtracks.
//!
//! ```text
//!   "SELECT * FROM t WHERE x = 1;"
//!                 |
//!                 |  tokenize_sql -> Lexer::tokenize
//!                 v
//!   Vec<Token>    [Select, Asterisk, From, Identifier, Where, ...]   (kind + span)
//!                 |
//!                 |  Parser::parse: dispatch on the first token
//!                 |    Select | With -> parse_select,  Insert -> parse_insert, ...
//!                 v
//!   Ast           Statement::Select(SelectId) -> Select { where_clause, .. }
//!                 Expression::Comparison { left: ExprId, right: ExprId, .. }
//!                 |
//!                 |  Ast::to_owned_statement   (bridge.rs)
//!                 v
//!   ast::Statement
//! ```
//!
//! ## Expressions
//!
//! Precedence is encoded as one method per level, lowest first; each calls the next
//! for its operands, so a tighter operator is always parsed deeper in the call stack
//! and ends up lower in the tree:
//!
//! ```text
//!   parse_or_expression             OR, ||                     left-assoc loop
//!   parse_and_expression            AND, &&                    left-assoc loop
//!   parse_not_expression            NOT, ! (prefix)
//!   parse_predicate_expression      = != < <= > >=, [NOT] IN / LIKE / BETWEEN,
//!                                   IS [NOT] NULL, EXISTS      at most one, no chaining
//!   parse_additive_expression       + -                        left-assoc loop
//!   parse_multiplicative_expression * / %                      left-assoc loop
//!   parse_unary_expression          - (prefix)
//!   parse_primary_expression        literal, column, ?, call, CASE, CAST, ( … )
//! ```
//!
//! `a + b * c`: additive parses `a` through multiplicative, sees `+`, and asks
//! multiplicative for the right operand; that call parses `b`, sees `*`, takes `c`,
//! and returns `b * c` whole. Additive builds `a + (b * c)`.
//!
//! ## Invariants
//!
//! 1. **Keywords are uppercase.** The lexer matches them case-sensitively, so
//!    `select` arrives as an `Identifier`; `parse` and `consume_token` recognise it
//!    and report "Keyword 'select' must be capitalized as 'SELECT'" rather than an
//!    unexpected identifier. Words that are keywords only in context (`ENGINE`,
//!    `USING`, `CHARSET`) are not lexer keywords; they are matched by text, uppercase
//!    only, through `peek_identifier_keyword` / `consume_identifier_keyword`.
//! 2. **Every statement ends in `;`.** Each statement rule consumes it; a script's
//!    last statement without one has it supplied by `sql::script`.
//! 3. **`position` only moves forward.** Every branch either consumes a token or
//!    returns an error, so no rule can loop without progress.
//! 4. **Every [`Ast::list_open`] is closed or aborted, on the error path too.** One
//!    scratch stack stages every list in the statement, so a list left open is
//!    swept into whichever list closes next; [`Ast::reset`] debug-asserts the stack
//!    is empty to catch it. `parse_comma_separated` does both for the common case.

use crate::error::{HematiteError, Result};
use crate::parser::arena::{
    AggregateTarget as AAggregateTarget, Alter as AAlter, AlterOperation as AAlterOperation,
    ArenaId, AssignId, Assignment as AAssignment, Ast, BranchId, CaseBranch,
    ColumnDef as AColumnDef, ColumnDefId, ConstraintId, Create as ACreate,
    CreateIndex as ACreateIndex, Cte as ACte, CteId, Delete as ADelete, Drop as ADrop,
    DropIndex as ADropIndex, ExprId, Expression as AExpr, FloatLit, Insert as AInsert,
    InsertSource as AInsertSource, ItemId, JoinKind, ListId, LitId, Literal as ALiteral, NameId,
    OrderByItem as AOrderByItem, OrderId, RowId, Select as ASelect, SelectId,
    SelectItem as ASelectItem, SelectItemKind, SetOp as ASetOp, SetOpId, Span, Statement as AStmt,
    TableConstraint as ATableConstraint, TableRef as ATableRef, TableRefId, Update as AUpdate,
};
use crate::parser::ast::*;
use crate::parser::lexer::{
    decode_hex_blob, unescape_identifier, unescape_string_literal, Lexer, Token, TokenKind,
};
use crate::parser::types::SqlTypeName;

/// A token stream and the text its spans point into.
///
/// The two travel together because a `Token` carries only a byte range: reading a
/// token's text means slicing `input`, so neither is meaningful without the other.
pub struct Parser<'a> {
    input: &'a str,
    tokens: Vec<Token>,
    position: usize,
    /// Where the statement is built. `input` is the same bytes as `ast.text()`, kept
    /// separately so reading a token's text does not borrow the arena being written.
    ast: Ast,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str, tokens: Vec<Token>) -> Self {
        Self::with_arena(input, tokens, Ast::default())
    }

    /// Parses into an arena the caller already owns, so a caller that parses
    /// repeatedly pays for its buffers once.
    pub fn with_arena(input: &'a str, tokens: Vec<Token>, mut ast: Ast) -> Self {
        ast.reset(input);
        Self {
            input,
            tokens,
            position: 0,
            ast,
        }
    }

    /// The arena the statement was built in. Its root is not set: `parse` returns
    /// the root, and the caller records it with [`Ast::set_root`].
    pub fn into_ast(self) -> Ast {
        self.ast
    }

    /// Records a token's span as a name without copying its text.
    #[inline]
    fn name_of(&mut self, token: Token) -> NameId {
        if token.kind == TokenKind::EscapedIdentifier {
            // The only identifier whose text is not its value.
            let value = unescape_identifier(token.kind, token.text(self.input)).into_owned();
            return self.ast.push_name(&value);
        }
        self.ast.push_name_span(Span {
            start: token.start,
            len: token.len,
        })
    }

    pub fn parse(&mut self) -> Result<AStmt> {
        if self.tokens.is_empty() {
            return Err(HematiteError::ParseError("Empty input".to_string()));
        }

        let token = self.peek_token()?;

        if token.dispatch_kind() == TokenKind::Identifier {
            let name = self.text(token);
            if let Some(keyword) = uppercase_keyword_match(name, TOP_LEVEL_KEYWORDS) {
                return Err(HematiteError::ParseError(format!(
                    "Keyword '{}' must be capitalized as '{}'",
                    name, keyword
                )));
            }
        }

        match token.dispatch_kind() {
            TokenKind::Begin => self.parse_begin(),
            TokenKind::Commit => self.parse_commit(),
            TokenKind::Rollback => self.parse_rollback(),
            TokenKind::Savepoint => self.parse_savepoint(),
            TokenKind::Release => self.parse_release_savepoint(),
            TokenKind::Explain => self.parse_explain(),
            TokenKind::Describe => self.parse_describe(),
            TokenKind::Show => self.parse_show(),
            TokenKind::Select | TokenKind::With => self.parse_select(),
            TokenKind::Update => self.parse_update(),
            TokenKind::Insert => self.parse_insert(),
            TokenKind::Delete => self.parse_delete(),
            TokenKind::Create => self.parse_create(),
            TokenKind::Alter => self.parse_alter(),
            TokenKind::Drop => self.parse_drop(),
            _ => Err(HematiteError::ParseError(format!(
                "Expected BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, EXPLAIN, DESCRIBE, SHOW, SELECT, UPDATE, INSERT, DELETE, CREATE, ALTER, or DROP, found: {}",
                self.describe_next()
            ))),
        }
    }

    fn parse_begin(&mut self) -> Result<AStmt> {
        self.consume_token(TokenKind::Begin)?;
        self.consume_token(TokenKind::Semicolon)?;
        Ok(AStmt::Begin)
    }

    fn parse_commit(&mut self) -> Result<AStmt> {
        self.consume_token(TokenKind::Commit)?;
        self.consume_token(TokenKind::Semicolon)?;
        Ok(AStmt::Commit)
    }

    fn parse_rollback(&mut self) -> Result<AStmt> {
        self.consume_token(TokenKind::Rollback)?;
        if matches!(self.peek_kind(), Ok(TokenKind::To)) {
            self.consume_token(TokenKind::To)?;
            if matches!(self.peek_kind(), Ok(TokenKind::Savepoint)) {
                self.consume_token(TokenKind::Savepoint)?;
            }
            let name = self.parse_name()?;
            self.consume_token(TokenKind::Semicolon)?;
            return Ok(AStmt::RollbackToSavepoint(name));
        }
        self.consume_token(TokenKind::Semicolon)?;
        Ok(AStmt::Rollback)
    }

    fn parse_savepoint(&mut self) -> Result<AStmt> {
        self.consume_token(TokenKind::Savepoint)?;
        let name = self.parse_name()?;
        self.consume_token(TokenKind::Semicolon)?;
        Ok(AStmt::Savepoint(name))
    }

    fn parse_release_savepoint(&mut self) -> Result<AStmt> {
        self.consume_token(TokenKind::Release)?;
        if matches!(self.peek_kind(), Ok(TokenKind::Savepoint)) {
            self.consume_token(TokenKind::Savepoint)?;
        }
        let name = self.parse_name()?;
        self.consume_token(TokenKind::Semicolon)?;
        Ok(AStmt::ReleaseSavepoint(name))
    }

    fn parse_select(&mut self) -> Result<AStmt> {
        let (query, into_table) = self.parse_query_statement(true, true)?;
        if into_table.is_none() {
            return Ok(AStmt::Select(query));
        }
        Ok(AStmt::SelectInto {
            table: into_table,
            query,
        })
    }

    fn parse_explain(&mut self) -> Result<AStmt> {
        self.consume_token(TokenKind::Explain)?;
        let inner = self.parse()?;
        Ok(AStmt::Explain(self.ast.push_stmt(inner)))
    }

    fn parse_describe(&mut self) -> Result<AStmt> {
        self.consume_token(TokenKind::Describe)?;
        let table = self.parse_name()?;
        self.consume_token(TokenKind::Semicolon)?;
        Ok(AStmt::Describe(table))
    }

    fn parse_show(&mut self) -> Result<AStmt> {
        self.consume_token(TokenKind::Show)?;
        match self.peek_kind()? {
            TokenKind::Tables => {
                self.consume_token(TokenKind::Tables)?;
                self.consume_token(TokenKind::Semicolon)?;
                Ok(AStmt::ShowTables)
            }
            TokenKind::Views => {
                self.consume_token(TokenKind::Views)?;
                self.consume_token(TokenKind::Semicolon)?;
                Err(HematiteError::ParseError(
                    "SHOW VIEWS is not supported".to_string(),
                ))
            }
            TokenKind::Indexes => {
                self.consume_token(TokenKind::Indexes)?;
                let table = if matches!(self.peek_kind(), Ok(TokenKind::From)) {
                    self.consume_token(TokenKind::From)?;
                    Some(self.parse_name()?)
                } else {
                    None
                };
                self.consume_token(TokenKind::Semicolon)?;
                Ok(AStmt::ShowIndexes(table.unwrap_or(NameId::NONE)))
            }
            TokenKind::Triggers => Err(HematiteError::ParseError(
                "SHOW TRIGGERS is not supported".to_string(),
            )),
            TokenKind::Create => {
                self.consume_token(TokenKind::Create)?;
                match self.peek_kind()? {
                    TokenKind::Table => {
                        self.consume_token(TokenKind::Table)?;
                        let table = self.parse_name()?;
                        self.consume_token(TokenKind::Semicolon)?;
                        Ok(AStmt::ShowCreateTable(table))
                    }
                    _ => Err(HematiteError::ParseError(format!(
                        "Expected TABLE after SHOW CREATE, found: {}",
                        self.describe_next()
                    ))),
                }
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected TABLES, VIEWS, INDEXES, TRIGGERS, or CREATE after SHOW, found: {}",
                self.describe_next()
            ))),
        }
    }

    fn parse_query_statement(
        &mut self,
        expect_semicolon: bool,
        allow_into: bool,
    ) -> Result<(SelectId, NameId)> {
        let with_clause = if matches!(self.peek_kind(), Ok(TokenKind::With)) {
            self.parse_with_clause()?
        } else {
            ListId::EMPTY
        };

        let (id, into_table) = self.parse_select_statement(expect_semicolon, allow_into)?;
        let mut select = self.ast.select(id);
        select.with_clause = with_clause;
        self.ast.set_select(id, select);
        Ok((id, into_table))
    }

    fn parse_select_statement(
        &mut self,
        expect_semicolon: bool,
        allow_into: bool,
    ) -> Result<(SelectId, NameId)> {
        self.consume_token(TokenKind::Select)?;
        let distinct = if matches!(self.peek_kind(), Ok(TokenKind::Distinct)) {
            self.consume_token(TokenKind::Distinct)?;
            true
        } else {
            false
        };

        let columns = self.parse_select_columns()?;

        let into_table = if allow_into && matches!(self.peek_kind(), Ok(TokenKind::Into)) {
            self.consume_token(TokenKind::Into)?;
            let token = self.parse_identifier_token()?;
            self.name_of(token)
        } else {
            NameId::NONE
        };

        self.consume_token(TokenKind::From)?;

        let from = self.parse_from_clause()?;

        let where_clause = if self.peek_kind()? == TokenKind::Where {
            self.parse_where_clause()?
        } else {
            ListId::EMPTY
        };

        let group_by = if matches!(self.peek_kind(), Ok(TokenKind::Group)) {
            self.parse_group_by_clause()?
        } else {
            ListId::EMPTY
        };

        let having_clause = if matches!(self.peek_kind(), Ok(TokenKind::Having)) {
            self.parse_having_clause()?
        } else {
            ListId::EMPTY
        };

        let order_by = if matches!(self.peek_kind(), Ok(TokenKind::Order)) {
            self.parse_order_by_clause()?
        } else {
            ListId::EMPTY
        };

        let (limit, limit_offset) = if matches!(self.peek_kind(), Ok(TokenKind::Limit)) {
            self.parse_limit_clause()?
        } else {
            (None, None)
        };

        let offset = if limit_offset.is_some() {
            limit_offset
        } else if matches!(self.peek_kind(), Ok(TokenKind::Offset)) {
            Some(self.parse_offset_clause()?)
        } else {
            None
        };

        let set_operation = if matches!(
            self.peek_kind(),
            Ok(TokenKind::Union | TokenKind::Intersect | TokenKind::Except)
        ) {
            let operator = match self.peek_kind()? {
                TokenKind::Union => {
                    self.consume_token(TokenKind::Union)?;
                    if matches!(self.peek_kind(), Ok(TokenKind::All)) {
                        self.consume_token(TokenKind::All)?;
                        SetOperator::UnionAll
                    } else {
                        SetOperator::Union
                    }
                }
                TokenKind::Intersect => {
                    self.consume_token(TokenKind::Intersect)?;
                    SetOperator::Intersect
                }
                TokenKind::Except => {
                    self.consume_token(TokenKind::Except)?;
                    SetOperator::Except
                }
                _ => {
                    return Err(HematiteError::ParseError(format!(
                        "Expected set operation, found: {}",
                        self.describe_next()
                    )))
                }
            };

            let right = self.parse_select_statement(false, false)?.0;
            self.ast.push_set_op(ASetOp { operator, right })
        } else {
            SetOpId::NONE
        };

        if expect_semicolon {
            self.consume_token(TokenKind::Semicolon)?;
        }

        let select = self.ast.push_select(ASelect {
            with_clause: ListId::EMPTY,
            distinct,
            columns,
            from,
            where_clause,
            group_by,
            having_clause,
            order_by,
            limit,
            offset,
            set_operation,
        });
        Ok((select, into_table))
    }

    fn parse_with_clause(&mut self) -> Result<ListId<CteId>> {
        self.consume_token(TokenKind::With)?;
        let mark = self.ast.list_open();

        loop {
            let name_token = self.parse_identifier_token()?;
            let name = self.name_of(name_token);
            self.consume_token(TokenKind::As)?;
            self.consume_token(TokenKind::LeftParen)?;
            let query = self.parse_query_statement(false, false)?.0;
            self.consume_token(TokenKind::RightParen)?;
            let cte = self.ast.push_cte(ACte { name, query });
            self.ast.list_push(cte);

            if matches!(self.peek_kind(), Ok(TokenKind::Comma)) {
                self.consume_token(TokenKind::Comma)?;
                continue;
            }

            break;
        }

        Ok(self.ast.list_close(mark))
    }

    /// The select list.
    ///
    /// An item carries its own alias, rather than the aliases living in a parallel
    /// vector every reader must index in step (as `ast::SelectStatement::column_aliases`
    /// does).
    fn parse_select_columns(&mut self) -> Result<ListId<ItemId>> {
        let mark = self.ast.list_open();
        let token = self.peek_token()?;

        if token.dispatch_kind() == TokenKind::Asterisk {
            self.consume_token(TokenKind::Asterisk)?;
            let item = self.ast.push_item(ASelectItem {
                kind: SelectItemKind::Wildcard,
                alias: NameId::NONE,
            });
            self.ast.list_push(item);
        } else {
            loop {
                match self.parse_aliased_select_item() {
                    Ok(item) => self.ast.list_push(item),
                    Err(err) => {
                        self.ast.list_abort(mark);
                        return Err(err);
                    }
                }

                if self.peek_kind()? == TokenKind::Comma {
                    self.consume_token(TokenKind::Comma)?;
                    continue;
                } else {
                    break;
                }
            }
        }

        Ok(self.ast.list_close(mark))
    }

    fn parse_aliased_select_item(&mut self) -> Result<ItemId> {
        let kind = self.parse_select_item_kind()?;
        let alias = self.parse_optional_alias()?;
        Ok(self.ast.push_item(ASelectItem { kind, alias }))
    }

    fn parse_select_item_kind(&mut self) -> Result<SelectItemKind> {
        let token = self.peek_token()?;
        match token.dispatch_kind() {
            TokenKind::Count
            | TokenKind::Sum
            | TokenKind::Avg
            | TokenKind::Min
            | TokenKind::Max => {
                let expr = self.parse_aggregate_expression()?;
                self.aggregate_expression_to_select_item(expr)
            }
            TokenKind::Identifier
            | TokenKind::StringLiteral
            | TokenKind::BlobLiteral
            | TokenKind::NumberLiteral
            | TokenKind::BooleanLiteral
            | TokenKind::Null
            | TokenKind::NullLiteral
            | TokenKind::Placeholder
            | TokenKind::LeftParen
            | TokenKind::Case
            | TokenKind::Cast
            | TokenKind::Left
            | TokenKind::Right
            | TokenKind::Minus => {
                let expr = self.parse_expression()?;
                // A bare column reference is kept as its own kind: the planner and
                // the projection resolver both ask whether an item is one.
                Ok(match self.ast.expr(expr) {
                    AExpr::Column { .. } => SelectItemKind::Column(expr),
                    _ => SelectItemKind::Expression(expr),
                })
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected select item or aggregate, found: {}",
                self.describe_next()
            ))),
        }
    }

    fn parse_aggregate_expression(&mut self) -> Result<ExprId> {
        let function = match self.peek_kind()? {
            TokenKind::Count => {
                self.consume_token(TokenKind::Count)?;
                AggregateFunction::Count
            }
            TokenKind::Sum => {
                self.consume_token(TokenKind::Sum)?;
                AggregateFunction::Sum
            }
            TokenKind::Avg => {
                self.consume_token(TokenKind::Avg)?;
                AggregateFunction::Avg
            }
            TokenKind::Min => {
                self.consume_token(TokenKind::Min)?;
                AggregateFunction::Min
            }
            TokenKind::Max => {
                self.consume_token(TokenKind::Max)?;
                AggregateFunction::Max
            }
            _ => {
                return Err(HematiteError::ParseError(format!(
                    "Expected aggregate function, found: {}",
                    self.describe_next()
                )))
            }
        };

        self.consume_token(TokenKind::LeftParen)?;
        if function == AggregateFunction::Count
            && matches!(self.peek_kind(), Ok(TokenKind::Asterisk))
        {
            self.consume_token(TokenKind::Asterisk)?;
            self.consume_token(TokenKind::RightParen)?;
            return Ok(self.ast.push_expr(AExpr::AggregateCall {
                function,
                // `COUNT(*)` has no column; the absent name is what says so.
                target: AAggregateTarget {
                    qualifier: NameId::NONE,
                    name: NameId::NONE,
                },
            }));
        }

        let (qualifier, name) = self.parse_column_reference()?;
        self.consume_token(TokenKind::RightParen)?;

        Ok(self.ast.push_expr(AExpr::AggregateCall {
            function,
            target: AAggregateTarget { qualifier, name },
        }))
    }

    fn aggregate_expression_to_select_item(&self, expr: ExprId) -> Result<SelectItemKind> {
        match self.ast.expr(expr) {
            AExpr::AggregateCall {
                function: AggregateFunction::Count,
                target,
            } if target.name.is_none() => Ok(SelectItemKind::CountAll),
            AExpr::AggregateCall { function, target } => {
                Ok(SelectItemKind::Aggregate { function, target })
            }
            _ => Err(HematiteError::InternalError(
                "aggregate expression parser returned a non-aggregate expression".to_string(),
            )),
        }
    }

    fn parse_from_clause(&mut self) -> Result<TableRefId> {
        let from = self.parse_table_reference()?;
        self.parse_join_chain(from)
    }

    fn parse_table_reference(&mut self) -> Result<TableRefId> {
        match self.peek_kind()? {
            TokenKind::Identifier => {
                let token = self.parse_identifier_token()?;
                let name = self.name_of(token);
                let alias = self.parse_optional_alias()?;
                Ok(self.ast.push_table(ATableRef::Table { name, alias }))
            }
            TokenKind::LeftParen => {
                self.consume_token(TokenKind::LeftParen)?;
                let subquery = self.parse_query_statement(false, false)?.0;
                self.consume_token(TokenKind::RightParen)?;
                let alias = self.parse_required_alias("derived table")?;
                Ok(self.ast.push_table(ATableRef::Derived { subquery, alias }))
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected table name, found: {}",
                self.describe_next()
            ))),
        }
    }

    fn parse_join_chain(&mut self, mut from: TableRefId) -> Result<TableRefId> {
        loop {
            match self.peek_kind()? {
                TokenKind::Comma => {
                    self.consume_token(TokenKind::Comma)?;
                    let right = self.parse_table_reference()?;
                    from = self
                        .ast
                        .push_table(ATableRef::CrossJoin { left: from, right });
                }
                TokenKind::Join => {
                    self.consume_token(TokenKind::Join)?;
                    let right = self.parse_table_reference()?;
                    self.consume_token(TokenKind::On)?;
                    let on = self.parse_or_expression()?;
                    from = self.ast.push_table(ATableRef::Join {
                        kind: JoinKind::Inner,
                        left: from,
                        right,
                        on,
                    });
                }
                TokenKind::Inner => {
                    self.consume_token(TokenKind::Inner)?;
                    self.consume_token(TokenKind::Join)?;
                    let right = self.parse_table_reference()?;
                    self.consume_token(TokenKind::On)?;
                    let on = self.parse_or_expression()?;
                    from = self.ast.push_table(ATableRef::Join {
                        kind: JoinKind::Inner,
                        left: from,
                        right,
                        on,
                    });
                }
                TokenKind::Left => {
                    self.consume_token(TokenKind::Left)?;
                    if matches!(self.peek_kind(), Ok(TokenKind::Outer)) {
                        self.consume_token(TokenKind::Outer)?;
                    }
                    self.consume_token(TokenKind::Join)?;
                    let right = self.parse_table_reference()?;
                    self.consume_token(TokenKind::On)?;
                    let on = self.parse_or_expression()?;
                    from = self.ast.push_table(ATableRef::Join {
                        kind: JoinKind::Left,
                        left: from,
                        right,
                        on,
                    });
                }
                TokenKind::Right => {
                    self.consume_token(TokenKind::Right)?;
                    if matches!(self.peek_kind(), Ok(TokenKind::Outer)) {
                        self.consume_token(TokenKind::Outer)?;
                    }
                    self.consume_token(TokenKind::Join)?;
                    let right = self.parse_table_reference()?;
                    self.consume_token(TokenKind::On)?;
                    let on = self.parse_or_expression()?;
                    from = self.ast.push_table(ATableRef::Join {
                        kind: JoinKind::Right,
                        left: from,
                        right,
                        on,
                    });
                }
                TokenKind::Full => {
                    self.consume_token(TokenKind::Full)?;
                    if matches!(self.peek_kind(), Ok(TokenKind::Outer)) {
                        self.consume_token(TokenKind::Outer)?;
                    }
                    self.consume_token(TokenKind::Join)?;
                    let right = self.parse_table_reference()?;
                    self.consume_token(TokenKind::On)?;
                    let on = self.parse_or_expression()?;
                    from = self.ast.push_table(ATableRef::Join {
                        kind: JoinKind::FullOuter,
                        left: from,
                        right,
                        on,
                    });
                }
                _ => break,
            }
        }

        Ok(from)
    }

    /// A `WHERE` clause, as a list of conditions.
    ///
    /// The grammar only ever produces one; the list is what the rewrite passes need,
    /// since promoting a `HAVING` term into a `WHERE` appends to it. An empty list is
    /// what "no WHERE clause" is, so that state has one spelling rather than the two
    /// an `Option` around a `Vec` would allow.
    fn parse_where_clause(&mut self) -> Result<ListId<ExprId>> {
        self.consume_token(TokenKind::Where)?;
        let condition = self.parse_or_expression()?;
        Ok(self.ast.list_from([condition]))
    }

    fn parse_order_by_clause(&mut self) -> Result<ListId<OrderId>> {
        self.consume_token(TokenKind::Order)?;
        self.consume_token(TokenKind::By)?;

        let mark = self.ast.list_open();
        loop {
            let (qualifier, name) = match self.parse_column_reference() {
                Ok(reference) => reference,
                Err(err) => {
                    self.ast.list_abort(mark);
                    return Err(err);
                }
            };
            let direction = match self.peek_kind() {
                Ok(TokenKind::Asc) => {
                    self.consume_token(TokenKind::Asc)?;
                    SortDirection::Asc
                }
                Ok(TokenKind::Desc) => {
                    self.consume_token(TokenKind::Desc)?;
                    SortDirection::Desc
                }
                _ => SortDirection::Asc,
            };

            let item = self.ast.push_order(AOrderByItem {
                qualifier,
                name,
                direction,
            });
            self.ast.list_push(item);

            if matches!(self.peek_kind(), Ok(TokenKind::Comma)) {
                self.consume_token(TokenKind::Comma)?;
                continue;
            }

            break;
        }

        Ok(self.ast.list_close(mark))
    }

    fn parse_group_by_clause(&mut self) -> Result<ListId<ExprId>> {
        self.consume_token(TokenKind::Group)?;
        self.consume_token(TokenKind::By)?;

        let mark = self.ast.list_open();
        loop {
            match self.parse_expression() {
                Ok(expr) => self.ast.list_push(expr),
                Err(err) => {
                    self.ast.list_abort(mark);
                    return Err(err);
                }
            }
            if matches!(self.peek_kind(), Ok(TokenKind::Comma)) {
                self.consume_token(TokenKind::Comma)?;
                continue;
            }
            break;
        }
        Ok(self.ast.list_close(mark))
    }

    fn parse_having_clause(&mut self) -> Result<ListId<ExprId>> {
        self.consume_token(TokenKind::Having)?;
        let condition = self.parse_or_expression()?;
        Ok(self.ast.list_from([condition]))
    }

    fn parse_limit_clause(&mut self) -> Result<(Option<usize>, Option<usize>)> {
        self.consume_token(TokenKind::Limit)?;
        let first = self.parse_non_negative_integer_clause("LIMIT")?;
        if matches!(self.peek_kind(), Ok(TokenKind::Comma)) {
            self.consume_token(TokenKind::Comma)?;
            let second = self.parse_non_negative_integer_clause("LIMIT")?;
            Ok((Some(second), Some(first)))
        } else {
            Ok((Some(first), None))
        }
    }

    fn parse_offset_clause(&mut self) -> Result<usize> {
        self.consume_token(TokenKind::Offset)?;
        self.parse_non_negative_integer_clause("OFFSET")
    }

    fn parse_non_negative_integer_clause(&mut self, clause_name: &str) -> Result<usize> {
        match self.peek_kind()? {
            TokenKind::NumberLiteral => {
                let value = self.text(self.peek_token()?);
                let parsed = parse_non_negative_integer_literal(value, clause_name)?;
                self.advance();
                Ok(parsed)
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected non-negative integer after {}, found: {}",
                clause_name,
                self.describe_next()
            ))),
        }
    }

    fn parse_expression(&mut self) -> Result<ExprId> {
        self.parse_or_expression()
    }

    fn parse_or_expression(&mut self) -> Result<ExprId> {
        let mut expr = self.parse_and_expression()?;

        while matches!(self.peek_kind(), Ok(TokenKind::Or)) {
            self.consume_token(TokenKind::Or)?;
            let right = self.parse_and_expression()?;
            expr = self.ast.push_expr(AExpr::Logical {
                left: expr,
                operator: LogicalOperator::Or,
                right,
            });
        }

        Ok(expr)
    }

    fn parse_and_expression(&mut self) -> Result<ExprId> {
        let mut expr = self.parse_not_expression()?;

        while matches!(self.peek_kind(), Ok(TokenKind::And)) {
            self.consume_token(TokenKind::And)?;
            let right = self.parse_not_expression()?;
            expr = self.ast.push_expr(AExpr::Logical {
                left: expr,
                operator: LogicalOperator::And,
                right,
            });
        }

        Ok(expr)
    }

    fn parse_not_expression(&mut self) -> Result<ExprId> {
        if matches!(self.peek_kind(), Ok(TokenKind::Not)) {
            self.consume_token(TokenKind::Not)?;
            let inner = self.parse_not_expression()?;
            return Ok(self.ast.push_expr(AExpr::UnaryNot(inner)));
        }

        self.parse_predicate_expression()
    }

    fn parse_predicate_expression(&mut self) -> Result<ExprId> {
        if matches!(self.peek_kind(), Ok(TokenKind::Exists)) {
            self.consume_token(TokenKind::Exists)?;
            return self.parse_exists_expression(false);
        }

        let left = self.parse_value_expression()?;

        if matches!(self.peek_kind(), Ok(TokenKind::Not)) {
            self.consume_token(TokenKind::Not)?;
            if matches!(self.peek_kind(), Ok(TokenKind::In)) {
                self.consume_token(TokenKind::In)?;
                return self.parse_in_list_expression(left, true);
            }
            if matches!(self.peek_kind(), Ok(TokenKind::Like)) {
                self.consume_token(TokenKind::Like)?;
                return self.parse_like_expression(left, true);
            }
            if matches!(self.peek_kind(), Ok(TokenKind::Between)) {
                self.consume_token(TokenKind::Between)?;
                return self.parse_between_expression(left, true);
            }
            return Err(HematiteError::ParseError(
                "Expected IN, LIKE, or BETWEEN after NOT in expression".to_string(),
            ));
        }

        if matches!(self.peek_kind(), Ok(TokenKind::In)) {
            self.consume_token(TokenKind::In)?;
            return self.parse_in_list_expression(left, false);
        }

        if matches!(self.peek_kind(), Ok(TokenKind::Between)) {
            self.consume_token(TokenKind::Between)?;
            return self.parse_between_expression(left, false);
        }

        if matches!(self.peek_kind(), Ok(TokenKind::Like)) {
            self.consume_token(TokenKind::Like)?;
            return self.parse_like_expression(left, false);
        }

        if matches!(self.peek_kind(), Ok(TokenKind::Is)) {
            self.consume_token(TokenKind::Is)?;
            let is_not = if matches!(self.peek_kind(), Ok(TokenKind::Not)) {
                self.consume_token(TokenKind::Not)?;
                true
            } else {
                false
            };
            self.consume_token(TokenKind::Null)?;
            return Ok(self.ast.push_expr(AExpr::NullCheck { expr: left, is_not }));
        }

        if self.peek_token_starts_comparison() {
            let operator = self.parse_comparison_operator()?;
            let right = self.parse_value_expression()?;
            return Ok(self.ast.push_expr(AExpr::Comparison {
                left,
                operator,
                right,
            }));
        }

        Ok(left)
    }

    fn parse_value_expression(&mut self) -> Result<ExprId> {
        self.parse_additive_expression()
    }

    fn parse_in_list_expression(&mut self, expr: ExprId, is_not: bool) -> Result<ExprId> {
        self.consume_token(TokenKind::LeftParen)?;
        if matches!(self.peek_kind(), Ok(TokenKind::Select | TokenKind::With)) {
            let subquery = self.parse_query_statement(false, false)?.0;
            self.consume_token(TokenKind::RightParen)?;
            return Ok(self.ast.push_expr(AExpr::InSubquery {
                expr,
                subquery,
                is_not,
            }));
        }

        let mark = self.ast.list_open();
        let values = loop {
            match self.parse_expression() {
                Ok(value) => self.ast.list_push(value),
                Err(err) => {
                    self.ast.list_abort(mark);
                    return Err(err);
                }
            }
            if matches!(self.peek_kind(), Ok(TokenKind::Comma)) {
                self.consume_token(TokenKind::Comma)?;
                continue;
            }
            break self.ast.list_close(mark);
        };

        if values.is_empty() {
            return Err(HematiteError::ParseError(
                "IN list must contain at least one expression".to_string(),
            ));
        }

        self.consume_token(TokenKind::RightParen)?;
        Ok(self.ast.push_expr(AExpr::InList {
            expr,
            values,
            is_not,
        }))
    }

    fn parse_exists_expression(&mut self, is_not: bool) -> Result<ExprId> {
        self.consume_token(TokenKind::LeftParen)?;
        let subquery = self.parse_query_statement(false, false)?.0;
        self.consume_token(TokenKind::RightParen)?;
        Ok(self.ast.push_expr(AExpr::Exists { subquery, is_not }))
    }

    fn parse_between_expression(&mut self, expr: ExprId, is_not: bool) -> Result<ExprId> {
        let lower = self.parse_value_expression()?;
        self.consume_token(TokenKind::And)?;
        let upper = self.parse_value_expression()?;
        Ok(self.ast.push_expr(AExpr::Between {
            expr,
            lower,
            upper,
            is_not,
        }))
    }

    fn parse_like_expression(&mut self, expr: ExprId, is_not: bool) -> Result<ExprId> {
        let pattern = self.parse_value_expression()?;
        Ok(self.ast.push_expr(AExpr::Like {
            expr,
            pattern,
            is_not,
        }))
    }

    fn parse_additive_expression(&mut self) -> Result<ExprId> {
        let mut expr = self.parse_multiplicative_expression()?;

        loop {
            let operator = match self.peek_kind() {
                Ok(TokenKind::Plus) => ArithmeticOperator::Add,
                Ok(TokenKind::Minus) => ArithmeticOperator::Subtract,
                _ => break,
            };

            match operator {
                ArithmeticOperator::Add => self.consume_token(TokenKind::Plus)?,
                ArithmeticOperator::Subtract => self.consume_token(TokenKind::Minus)?,
                ArithmeticOperator::Multiply
                | ArithmeticOperator::Divide
                | ArithmeticOperator::Modulo => unreachable!(),
            }

            let right = self.parse_multiplicative_expression()?;
            expr = self.ast.push_expr(AExpr::Binary {
                left: expr,
                operator,
                right,
            });
        }

        Ok(expr)
    }

    fn parse_multiplicative_expression(&mut self) -> Result<ExprId> {
        let mut expr = self.parse_unary_expression()?;

        loop {
            let operator = match self.peek_kind() {
                Ok(TokenKind::Asterisk) => ArithmeticOperator::Multiply,
                Ok(TokenKind::Slash) => ArithmeticOperator::Divide,
                Ok(TokenKind::Percent) => ArithmeticOperator::Modulo,
                _ => break,
            };

            match operator {
                ArithmeticOperator::Multiply => self.consume_token(TokenKind::Asterisk)?,
                ArithmeticOperator::Divide => self.consume_token(TokenKind::Slash)?,
                ArithmeticOperator::Modulo => self.consume_token(TokenKind::Percent)?,
                ArithmeticOperator::Add | ArithmeticOperator::Subtract => unreachable!(),
            }

            let right = self.parse_unary_expression()?;
            expr = self.ast.push_expr(AExpr::Binary {
                left: expr,
                operator,
                right,
            });
        }

        Ok(expr)
    }

    fn parse_unary_expression(&mut self) -> Result<ExprId> {
        if matches!(self.peek_kind(), Ok(TokenKind::Minus)) {
            self.consume_token(TokenKind::Minus)?;
            let inner = self.parse_unary_expression()?;
            return Ok(self.ast.push_expr(AExpr::UnaryMinus(inner)));
        }

        self.parse_primary_expression()
    }

    fn parse_primary_expression(&mut self) -> Result<ExprId> {
        let token = self.peek_token()?;
        match token.dispatch_kind() {
            TokenKind::Cast => self.parse_cast_expression(),
            TokenKind::Case => self.parse_case_expression(),
            TokenKind::Left | TokenKind::Right if self.next_token_is(TokenKind::LeftParen) => {
                self.parse_scalar_function_expression()
            }
            TokenKind::Count
            | TokenKind::Sum
            | TokenKind::Avg
            | TokenKind::Min
            | TokenKind::Max => self.parse_aggregate_expression(),
            TokenKind::Identifier if self.next_token_is(TokenKind::LeftParen) => {
                self.parse_scalar_function_expression()
            }
            TokenKind::Identifier => {
                let (qualifier, name) = self.parse_column_reference()?;
                Ok(self.ast.push_expr(AExpr::Column { qualifier, name }))
            }
            TokenKind::StringLiteral => {
                let lit = self.push_text_literal(token);
                self.advance();
                Ok(self.ast.push_expr(AExpr::Literal(lit)))
            }
            TokenKind::BlobLiteral => {
                let bytes = self.blob_value(token);
                let span = self.ast.push_bytes(&bytes);
                let lit = self.ast.push_lit(ALiteral::Blob(span));
                self.advance();
                Ok(self.ast.push_expr(AExpr::Literal(lit)))
            }
            TokenKind::NumberLiteral => {
                let lit = self.push_number_literal(token)?;
                self.advance();
                Ok(self.ast.push_expr(AExpr::Literal(lit)))
            }
            TokenKind::BooleanLiteral => {
                let value = self.boolean_value(token);
                let lit = self.ast.push_lit(ALiteral::Boolean(value));
                self.advance();
                Ok(self.ast.push_expr(AExpr::Literal(lit)))
            }
            TokenKind::NullLiteral | TokenKind::Null => {
                // `NULL` appears both as a constraint keyword and as an expression literal.
                if token.dispatch_kind() == TokenKind::NullLiteral {
                    self.consume_token(TokenKind::NullLiteral)?;
                } else {
                    self.consume_token(TokenKind::Null)?;
                }
                let lit = self.ast.push_lit(ALiteral::Null);
                Ok(self.ast.push_expr(AExpr::Literal(lit)))
            }
            TokenKind::Placeholder => {
                self.consume_token(TokenKind::Placeholder)?;
                Ok(self.ast.push_parameter())
            }
            TokenKind::LeftParen => {
                self.consume_token(TokenKind::LeftParen)?;
                if matches!(self.peek_kind(), Ok(TokenKind::Select | TokenKind::With)) {
                    let subquery = self.parse_query_statement(false, false)?.0;
                    self.consume_token(TokenKind::RightParen)?;
                    return Ok(self.ast.push_expr(AExpr::ScalarSubquery(subquery)));
                }
                let expr = self.parse_expression()?;
                self.consume_token(TokenKind::RightParen)?;
                Ok(expr)
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected expression, found: {}",
                self.describe_next()
            ))),
        }
    }

    fn parse_case_expression(&mut self) -> Result<ExprId> {
        self.consume_token(TokenKind::Case)?;
        let mark = self.ast.list_open();

        while matches!(self.peek_kind(), Ok(TokenKind::When)) {
            self.consume_token(TokenKind::When)?;
            match self.parse_case_branch() {
                Ok(branch) => self.ast.list_push(branch),
                Err(err) => {
                    self.ast.list_abort(mark);
                    return Err(err);
                }
            }
        }
        let branches: ListId<BranchId> = self.ast.list_close(mark);

        if branches.is_empty() {
            return Err(HematiteError::ParseError(
                "CASE expression requires at least one WHEN ... THEN branch".to_string(),
            ));
        }

        let else_expr = if matches!(self.peek_kind(), Ok(TokenKind::Else)) {
            self.consume_token(TokenKind::Else)?;
            self.parse_expression()?
        } else {
            ExprId::NONE
        };

        self.consume_token(TokenKind::End)?;
        Ok(self.ast.push_expr(AExpr::Case {
            branches,
            else_expr,
        }))
    }

    fn parse_cast_expression(&mut self) -> Result<ExprId> {
        self.consume_token(TokenKind::Cast)?;
        self.consume_token(TokenKind::LeftParen)?;
        let expr = self.parse_expression()?;
        self.consume_token(TokenKind::As)?;
        let target_type = self.parse_data_type()?;
        self.consume_token(TokenKind::RightParen)?;
        let target_type = self.ast.push_type(target_type);
        Ok(self.ast.push_expr(AExpr::Cast { expr, target_type }))
    }

    fn parse_scalar_function_expression(&mut self) -> Result<ExprId> {
        let function_name = self.parse_scalar_function_name()?;
        let function = ScalarFunction::from_identifier(function_name).ok_or_else(|| {
            HematiteError::ParseError(format!("Unsupported scalar function '{}'", function_name))
        })?;
        self.consume_token(TokenKind::LeftParen)?;

        let mark = self.ast.list_open();
        if !matches!(self.peek_kind(), Ok(TokenKind::RightParen)) {
            loop {
                match self.parse_expression() {
                    Ok(arg) => self.ast.list_push(arg),
                    Err(err) => {
                        self.ast.list_abort(mark);
                        return Err(err);
                    }
                }
                if matches!(self.peek_kind(), Ok(TokenKind::Comma)) {
                    self.consume_token(TokenKind::Comma)?;
                    continue;
                }
                break;
            }
        }
        let args = self.ast.list_close(mark);

        self.consume_token(TokenKind::RightParen)?;
        Ok(self
            .ast
            .push_expr(AExpr::ScalarFunctionCall { function, args }))
    }

    /// A scalar function's name, borrowed from the input.
    ///
    /// `LEFT` and `RIGHT` are lexer keywords because they also introduce joins, so
    /// they arrive as kinds rather than identifiers and name themselves.
    fn parse_scalar_function_name(&mut self) -> Result<&'a str> {
        match self.peek_kind()? {
            TokenKind::Identifier => {
                let token = self.parse_identifier_token()?;
                Ok(self.text(token))
            }
            TokenKind::Left => {
                self.consume_token(TokenKind::Left)?;
                Ok("LEFT")
            }
            TokenKind::Right => {
                self.consume_token(TokenKind::Right)?;
                Ok("RIGHT")
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected scalar function name, found: {}",
                self.describe_next()
            ))),
        }
    }

    fn parse_comparison_operator(&mut self) -> Result<ComparisonOperator> {
        match self.peek_kind()? {
            TokenKind::Equal => {
                self.consume_token(TokenKind::Equal)?;
                Ok(ComparisonOperator::Equal)
            }
            TokenKind::NotEqual => {
                self.consume_token(TokenKind::NotEqual)?;
                Ok(ComparisonOperator::NotEqual)
            }
            TokenKind::LessThan => {
                self.consume_token(TokenKind::LessThan)?;
                Ok(ComparisonOperator::LessThan)
            }
            TokenKind::LessThanOrEqual => {
                self.consume_token(TokenKind::LessThanOrEqual)?;
                Ok(ComparisonOperator::LessThanOrEqual)
            }
            TokenKind::GreaterThan => {
                self.consume_token(TokenKind::GreaterThan)?;
                Ok(ComparisonOperator::GreaterThan)
            }
            TokenKind::GreaterThanOrEqual => {
                self.consume_token(TokenKind::GreaterThanOrEqual)?;
                Ok(ComparisonOperator::GreaterThanOrEqual)
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected comparison operator, found: {}",
                self.describe_next()
            ))),
        }
    }

    fn parse_insert(&mut self) -> Result<AStmt> {
        self.consume_token(TokenKind::Insert)?;
        self.consume_token(TokenKind::Into)?;

        let table = self.parse_name()?;

        let (columns, source) = match self.peek_kind()? {
            TokenKind::LeftParen => {
                self.consume_token(TokenKind::LeftParen)?;
                let columns = self.parse_column_list()?;
                self.consume_token(TokenKind::RightParen)?;
                let source = match self.peek_kind()? {
                    TokenKind::Values => {
                        self.consume_token(TokenKind::Values)?;
                        AInsertSource::Values(self.parse_value_lists()?)
                    }
                    TokenKind::Select | TokenKind::With => {
                        AInsertSource::Select(self.parse_query_statement(false, false)?.0)
                    }
                    _ => {
                        return Err(HematiteError::ParseError(format!(
                            "Expected VALUES or SELECT after INSERT column list, found: {}",
                            self.describe_next()
                        )))
                    }
                };
                (columns, source)
            }
            // `INSERT … SET a = 1, b = 2` is the same statement as
            // `INSERT … (a, b) VALUES (1, 2)`, so it is rewritten into that here
            // rather than carried as a second shape through the whole engine.
            TokenKind::Set => {
                self.consume_token(TokenKind::Set)?;
                let assignments = self.parse_update_assignments()?;
                let assigns: Vec<AssignId> = self.ast.list(assignments).collect();

                let mark = self.ast.list_open();
                for &assign in &assigns {
                    let column = self.ast.assign(assign).column;
                    self.ast.list_push(column);
                }
                let columns = self.ast.list_close(mark);

                let mark = self.ast.list_open();
                for &assign in &assigns {
                    let value = self.ast.assign(assign).value;
                    self.ast.list_push(value);
                }
                let values = self.ast.list_close(mark);

                let row = self.ast.push_row(values);
                let rows = self.ast.list_from([row]);
                (columns, AInsertSource::Values(rows))
            }
            _ => {
                return Err(HematiteError::ParseError(format!(
                    "Expected column list or SET after INSERT INTO table, found: {}",
                    self.describe_next()
                )))
            }
        };

        // Presence is recorded apart from the list because the owned tree's
        // `on_duplicate` is an `Option<Vec<_>>`. The grammar requires at least one
        // assignment, so the list is never empty when the flag is set.
        let (on_duplicate, has_on_duplicate) = if matches!(self.peek_kind(), Ok(TokenKind::On)) {
            self.consume_token(TokenKind::On)?;
            self.consume_token(TokenKind::Duplicate)?;
            self.consume_token(TokenKind::Key)?;
            self.consume_token(TokenKind::Update)?;
            (self.parse_update_assignments()?, true)
        } else {
            (ListId::EMPTY, false)
        };

        self.consume_token(TokenKind::Semicolon)?;

        let insert = self.ast.push_insert(AInsert {
            table,
            columns,
            source,
            on_duplicate,
            has_on_duplicate,
        });
        Ok(AStmt::Insert(insert))
    }

    fn parse_update(&mut self) -> Result<AStmt> {
        self.consume_token(TokenKind::Update)?;
        let table = self.parse_name()?;
        let alias = self.parse_optional_alias()?;
        let from = self.ast.push_table(ATableRef::Table { name: table, alias });
        let from = self.parse_join_chain(from)?;
        // Only record a source when the statement named more than the target table:
        // `UPDATE t SET …` and `UPDATE t JOIN u … SET …` are different shapes.
        let has_explicit_source = !matches!(
            self.ast.table(from),
            ATableRef::Table { name, alias } if name == table && alias.is_none()
        );
        self.consume_token(TokenKind::Set)?;
        let assignments = self.parse_update_assignments()?;

        let where_clause = if matches!(self.peek_kind(), Ok(TokenKind::Where)) {
            self.parse_where_clause()?
        } else {
            ListId::EMPTY
        };

        self.consume_token(TokenKind::Semicolon)?;

        let (target_binding, source) = if has_explicit_source {
            let binding = if alias.is_none() { table } else { alias };
            (binding, from)
        } else {
            (NameId::NONE, TableRefId::NONE)
        };

        let update = self.ast.push_update(AUpdate {
            table,
            target_binding,
            source,
            assignments,
            where_clause,
        });
        Ok(AStmt::Update(update))
    }

    fn parse_delete(&mut self) -> Result<AStmt> {
        self.consume_token(TokenKind::Delete)?;
        let (table, target_binding, source) = match self.peek_kind()? {
            TokenKind::From => {
                self.consume_token(TokenKind::From)?;
                let table = self.parse_name()?;
                (table, NameId::NONE, TableRefId::NONE)
            }
            TokenKind::Identifier => {
                let target_binding = self.parse_name()?;
                self.consume_token(TokenKind::From)?;
                let source = self.parse_from_clause()?;
                let table = self.resolve_delete_target_table(target_binding, source)?;
                (table, target_binding, source)
            }
            _ => {
                return Err(HematiteError::ParseError(format!(
                    "Expected FROM or target table alias after DELETE, found: {}",
                    self.describe_next()
                )))
            }
        };

        let where_clause = if matches!(self.peek_kind(), Ok(TokenKind::Where)) {
            self.parse_where_clause()?
        } else {
            ListId::EMPTY
        };

        self.consume_token(TokenKind::Semicolon)?;

        let delete = self.ast.push_delete(ADelete {
            table,
            target_binding,
            source,
            where_clause,
        });
        Ok(AStmt::Delete(delete))
    }

    /// Every `(table, alias)` a FROM clause binds, in source order.
    fn collect_table_bindings(&self, source: TableRefId, out: &mut Vec<(NameId, NameId)>) {
        match self.ast.table(source) {
            ATableRef::Table { name, alias } => out.push((name, alias)),
            // A derived table introduces only its own alias, not the tables inside it.
            ATableRef::Derived { .. } => {}
            ATableRef::CrossJoin { left, right } => {
                self.collect_table_bindings(left, out);
                self.collect_table_bindings(right, out);
            }
            ATableRef::Join { left, right, .. } => {
                self.collect_table_bindings(left, out);
                self.collect_table_bindings(right, out);
            }
        }
    }

    /// Which table a `DELETE t FROM …` names, given the bindings its FROM introduces.
    fn resolve_delete_target_table(
        &self,
        target_binding: NameId,
        source: TableRefId,
    ) -> Result<NameId> {
        let mut matches = Vec::new();
        self.collect_table_bindings(source, &mut matches);
        let mut matches: Vec<NameId> = matches
            .into_iter()
            .filter(|&(name, alias)| {
                let binding = if alias.is_none() { name } else { alias };
                self.ast.name_eq(binding, target_binding) || self.ast.name_eq(name, target_binding)
            })
            .map(|(name, _)| name)
            .collect();

        match matches.len() {
            1 => Ok(matches.remove(0)),
            0 => Err(HematiteError::ParseError(format!(
                "DELETE target '{}' does not match any table in the FROM clause",
                self.ast.name(target_binding)
            ))),
            _ => Err(HematiteError::ParseError(format!(
                "DELETE target '{}' is ambiguous in the FROM clause",
                self.ast.name(target_binding)
            ))),
        }
    }

    fn parse_create(&mut self) -> Result<AStmt> {
        self.consume_token(TokenKind::Create)?;
        let unique = if matches!(self.peek_kind(), Ok(TokenKind::Unique)) {
            self.consume_token(TokenKind::Unique)?;
            true
        } else {
            false
        };
        match self.peek_kind()? {
            TokenKind::Table => {
                if unique {
                    return Err(HematiteError::ParseError(
                        "CREATE UNIQUE TABLE is not supported".to_string(),
                    ));
                }
                self.consume_token(TokenKind::Table)?;
                let if_not_exists = self.parse_if_not_exists_clause()?;

                let table = self.parse_name()?;

                self.consume_token(TokenKind::LeftParen)?;

                let (columns, constraints) = self.parse_table_definition_items()?;

                self.consume_token(TokenKind::RightParen)?;
                self.consume_ignored_create_table_options()?;
                self.consume_token(TokenKind::Semicolon)?;

                let create = self.ast.push_create(ACreate {
                    table,
                    columns,
                    constraints,
                    if_not_exists,
                });
                Ok(AStmt::Create(create))
            }
            TokenKind::View => Err(HematiteError::ParseError(
                "CREATE VIEW is not supported".to_string(),
            )),
            TokenKind::Trigger => Err(HematiteError::ParseError(
                "CREATE TRIGGER is not supported".to_string(),
            )),
            TokenKind::Index | TokenKind::Key => {
                if matches!(self.peek_kind(), Ok(TokenKind::Index)) {
                    self.consume_token(TokenKind::Index)?;
                } else {
                    self.consume_token(TokenKind::Key)?;
                }
                let if_not_exists = self.parse_if_not_exists_clause()?;
                let index_name = self.parse_name()?;
                self.consume_optional_index_type_clause()?;
                self.consume_token(TokenKind::On)?;
                let table = self.parse_name()?;
                self.consume_token(TokenKind::LeftParen)?;
                let columns = self.parse_column_list()?;
                self.consume_token(TokenKind::RightParen)?;
                self.consume_optional_index_type_clause()?;
                self.consume_token(TokenKind::Semicolon)?;

                let create_index = self.ast.push_create_index(ACreateIndex {
                    index_name,
                    table,
                    columns,
                    unique,
                    if_not_exists,
                });
                Ok(AStmt::CreateIndex(create_index))
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected TABLE, VIEW, TRIGGER, INDEX, or KEY after CREATE, found: {}",
                self.describe_next()
            ))),
        }
    }

    fn parse_if_not_exists_clause(&mut self) -> Result<bool> {
        if matches!(self.peek_kind(), Ok(TokenKind::If)) {
            self.consume_token(TokenKind::If)?;
            self.consume_token(TokenKind::Not)?;
            self.consume_token(TokenKind::Exists)?;
            return Ok(true);
        }
        Ok(false)
    }

    fn parse_if_exists_clause(&mut self) -> Result<bool> {
        if matches!(self.peek_kind(), Ok(TokenKind::If)) {
            self.consume_token(TokenKind::If)?;
            self.consume_token(TokenKind::Exists)?;
            return Ok(true);
        }
        Ok(false)
    }

    fn peek_token_starts_comparison(&self) -> bool {
        matches!(
            self.peek_kind(),
            Ok(TokenKind::Equal
                | TokenKind::NotEqual
                | TokenKind::LessThan
                | TokenKind::LessThanOrEqual
                | TokenKind::GreaterThan
                | TokenKind::GreaterThanOrEqual)
        )
    }

    fn parse_drop(&mut self) -> Result<AStmt> {
        self.consume_token(TokenKind::Drop)?;
        match self.peek_kind()? {
            TokenKind::Table => {
                self.consume_token(TokenKind::Table)?;
                let if_exists = self.parse_if_exists_clause()?;
                let table = self.parse_name()?;
                self.consume_token(TokenKind::Semicolon)?;
                let drop = self.ast.push_drop(ADrop { table, if_exists });
                Ok(AStmt::Drop(drop))
            }
            TokenKind::View => Err(HematiteError::ParseError(
                "DROP VIEW is not supported".to_string(),
            )),
            TokenKind::Trigger => Err(HematiteError::ParseError(
                "DROP TRIGGER is not supported".to_string(),
            )),
            TokenKind::Index => {
                self.consume_token(TokenKind::Index)?;
                let if_exists = self.parse_if_exists_clause()?;
                let index_name = self.parse_name()?;
                self.consume_token(TokenKind::On)?;
                let table = self.parse_name()?;
                self.consume_token(TokenKind::Semicolon)?;
                let drop_index = self.ast.push_drop_index(ADropIndex {
                    index_name,
                    table,
                    if_exists,
                });
                Ok(AStmt::DropIndex(drop_index))
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected TABLE, VIEW, TRIGGER, or INDEX after DROP, found: {}",
                self.describe_next()
            ))),
        }
    }

    fn parse_alter(&mut self) -> Result<AStmt> {
        self.consume_token(TokenKind::Alter)?;
        self.consume_token(TokenKind::Table)?;
        let table = self.parse_name()?;

        match self.peek_kind()? {
            TokenKind::Rename => {
                self.consume_token(TokenKind::Rename)?;
                let operation = if matches!(self.peek_kind(), Ok(TokenKind::Column)) {
                    self.consume_token(TokenKind::Column)?;
                    let old_name = self.parse_name()?;
                    self.consume_token(TokenKind::To)?;
                    let new_name = self.parse_name()?;
                    AAlterOperation::RenameColumn { old_name, new_name }
                } else {
                    self.consume_token(TokenKind::To)?;
                    let new_name = self.parse_name()?;
                    AAlterOperation::RenameTo(new_name)
                };
                self.consume_token(TokenKind::Semicolon)?;
                let alter = self.ast.push_alter(AAlter { table, operation });
                Ok(AStmt::Alter(alter))
            }
            TokenKind::Add => {
                self.consume_token(TokenKind::Add)?;
                let operation = match self.peek_kind()? {
                    TokenKind::Column => {
                        self.consume_token(TokenKind::Column)?;
                        AAlterOperation::AddColumn(self.parse_column_definition()?)
                    }
                    TokenKind::Constraint
                    | TokenKind::Check
                    | TokenKind::Unique
                    | TokenKind::Foreign => {
                        AAlterOperation::AddConstraint(self.parse_table_constraint()?)
                    }
                    TokenKind::Identifier => {
                        AAlterOperation::AddColumn(self.parse_column_definition()?)
                    }
                    _ => {
                        return Err(HematiteError::ParseError(format!(
                            "Expected COLUMN or constraint after ADD, found: {}",
                            self.describe_next()
                        )))
                    }
                };
                self.consume_token(TokenKind::Semicolon)?;
                let alter = self.ast.push_alter(AAlter { table, operation });
                Ok(AStmt::Alter(alter))
            }
            TokenKind::Drop => {
                self.consume_token(TokenKind::Drop)?;
                let operation = match self.peek_kind()? {
                    TokenKind::Column => {
                        self.consume_token(TokenKind::Column)?;
                        AAlterOperation::DropColumn(self.parse_name()?)
                    }
                    TokenKind::Constraint => {
                        self.consume_token(TokenKind::Constraint)?;
                        AAlterOperation::DropConstraint(self.parse_name()?)
                    }
                    TokenKind::Identifier => AAlterOperation::DropColumn(self.parse_name()?),
                    _ => {
                        return Err(HematiteError::ParseError(format!(
                            "Expected COLUMN or CONSTRAINT after DROP, found: {}",
                            self.describe_next()
                        )))
                    }
                };
                self.consume_token(TokenKind::Semicolon)?;
                let alter = self.ast.push_alter(AAlter { table, operation });
                Ok(AStmt::Alter(alter))
            }
            TokenKind::Alter => {
                self.consume_token(TokenKind::Alter)?;
                if matches!(self.peek_kind(), Ok(TokenKind::Column)) {
                    self.consume_token(TokenKind::Column)?;
                }
                let column_name = self.parse_name()?;
                let operation = match self.peek_kind()? {
                    TokenKind::Set => {
                        self.consume_token(TokenKind::Set)?;
                        match self.peek_kind()? {
                            TokenKind::Default => {
                                self.consume_token(TokenKind::Default)?;
                                let default_value = self.parse_default_value()?;
                                AAlterOperation::AlterColumnSetDefault {
                                    column_name,
                                    value: default_value,
                                }
                            }
                            TokenKind::Not => {
                                self.consume_token(TokenKind::Not)?;
                                self.consume_token(TokenKind::Null)?;
                                AAlterOperation::AlterColumnSetNotNull { column_name }
                            }
                            _ => {
                                return Err(HematiteError::ParseError(format!(
                                    "Expected DEFAULT or NOT NULL after SET, found: {}",
                                    self.describe_next()
                                )))
                            }
                        }
                    }
                    TokenKind::Drop => {
                        self.consume_token(TokenKind::Drop)?;
                        match self.peek_kind()? {
                            TokenKind::Default => {
                                self.consume_token(TokenKind::Default)?;
                                AAlterOperation::AlterColumnDropDefault { column_name }
                            }
                            TokenKind::Not => {
                                self.consume_token(TokenKind::Not)?;
                                self.consume_token(TokenKind::Null)?;
                                AAlterOperation::AlterColumnDropNotNull { column_name }
                            }
                            _ => {
                                return Err(HematiteError::ParseError(format!(
                                    "Expected DEFAULT or NOT NULL after DROP, found: {}",
                                    self.describe_next()
                                )))
                            }
                        }
                    }
                    _ => {
                        return Err(HematiteError::ParseError(format!(
                            "Expected SET or DROP after ALTER COLUMN, found: {}",
                            self.describe_next()
                        )))
                    }
                };
                self.consume_token(TokenKind::Semicolon)?;
                let alter = self.ast.push_alter(AAlter { table, operation });
                Ok(AStmt::Alter(alter))
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected supported ALTER TABLE operation, found: {}",
                self.describe_next()
            ))),
        }
    }

    /// An alias, or the absent name when there is none.
    fn parse_optional_alias(&mut self) -> Result<NameId> {
        match self.peek_kind() {
            Ok(TokenKind::As) => {
                self.consume_token(TokenKind::As)?;
                let token = self.parse_identifier_token()?;
                Ok(self.name_of(token))
            }
            Ok(TokenKind::Identifier) => {
                let token = self.parse_identifier_token()?;
                Ok(self.name_of(token))
            }
            _ => Ok(NameId::NONE),
        }
    }

    fn parse_required_alias(&mut self, subject: &str) -> Result<NameId> {
        let alias = self.parse_optional_alias()?;
        if alias.is_none() {
            return Err(HematiteError::ParseError(format!(
                "{} must have an alias",
                subject
            )));
        }
        Ok(alias)
    }

    fn peek_identifier_keyword(&self, keyword: &str) -> bool {
        self.peek_token()
            .is_ok_and(|token| token.kind == TokenKind::Identifier && self.text(token) == keyword)
    }

    fn consume_identifier_keyword(&mut self, keyword: &str) -> Result<()> {
        let token = self.peek_token()?;
        if token.kind == TokenKind::Identifier {
            let name = self.text(token);
            if name == keyword {
                self.advance();
                return Ok(());
            }
            // Not a lexer keyword, so the "must be capitalized" check that
            // `consume_token` does cannot fire for it; make it here instead.
            if name.eq_ignore_ascii_case(keyword) {
                return Err(HematiteError::ParseError(format!(
                    "Keyword '{}' must be capitalized as '{}'",
                    name, keyword
                )));
            }
        }
        Err(HematiteError::ParseError(format!(
            "Expected {}, found: {}",
            keyword,
            self.describe_next()
        )))
    }

    fn consume_optional_equals(&mut self) -> Result<()> {
        if matches!(self.peek_kind(), Ok(TokenKind::Equal)) {
            self.consume_token(TokenKind::Equal)?;
        }
        Ok(())
    }

    fn consume_optional_index_type_clause(&mut self) -> Result<()> {
        if !self.peek_identifier_keyword("USING") {
            return Ok(());
        }
        self.consume_identifier_keyword("USING")?;
        let token = self.peek_token()?;
        if token.kind == TokenKind::Identifier && matches!(self.text(token), "BTREE" | "HASH") {
            self.advance();
            return Ok(());
        }
        Err(HematiteError::ParseError(format!(
            "Expected BTREE or HASH after USING, found: {}",
            self.describe_next()
        )))
    }

    fn consume_ignored_create_table_options(&mut self) -> Result<()> {
        loop {
            if self.peek_identifier_keyword("ENGINE") {
                self.consume_identifier_keyword("ENGINE")?;
                self.consume_optional_equals()?;
                self.parse_name()?;
                continue;
            }

            if matches!(self.peek_kind(), Ok(TokenKind::AutoIncrement)) {
                self.consume_token(TokenKind::AutoIncrement)?;
                self.consume_optional_equals()?;
                self.consume_positive_integer_literal("AUTO_INCREMENT")?;
                continue;
            }

            if matches!(self.peek_kind(), Ok(TokenKind::Default)) {
                self.consume_token(TokenKind::Default)?;
                if self.peek_identifier_keyword("CHARSET") {
                    self.consume_identifier_keyword("CHARSET")?;
                    self.consume_optional_equals()?;
                    self.parse_name()?;
                    continue;
                }
                if self.peek_identifier_keyword("CHARACTER") {
                    self.consume_identifier_keyword("CHARACTER")?;
                    self.consume_token(TokenKind::Set)?;
                    self.consume_optional_equals()?;
                    self.parse_name()?;
                    continue;
                }
                let token = self.peek_token()?;
                if let Some(message) =
                    capitalization_hint_for_token(token.dispatch_kind(), self.text(token))
                {
                    return Err(HematiteError::ParseError(message));
                }
                return Err(HematiteError::ParseError(
                    "Unsupported DEFAULT table option".to_string(),
                ));
            }

            if self.peek_identifier_keyword("CHARACTER") {
                self.consume_identifier_keyword("CHARACTER")?;
                self.consume_token(TokenKind::Set)?;
                self.consume_optional_equals()?;
                self.parse_name()?;
                continue;
            }

            if self.peek_identifier_keyword("CHARSET") {
                self.consume_identifier_keyword("CHARSET")?;
                self.consume_optional_equals()?;
                self.parse_name()?;
                continue;
            }

            if self.peek_identifier_keyword("COLLATE") {
                self.consume_identifier_keyword("COLLATE")?;
                self.consume_optional_equals()?;
                self.parse_name()?;
                continue;
            }

            break;
        }

        Ok(())
    }

    fn parse_column_list(&mut self) -> Result<ListId<NameId>> {
        self.parse_comma_separated(|parser| {
            let token = parser.peek_token()?;
            match token.dispatch_kind() {
                TokenKind::Identifier => {
                    parser.advance();
                    Ok(parser.name_of(token))
                }
                _ => Err(HematiteError::ParseError(format!(
                    "Expected column name, found: {}",
                    parser.describe_next()
                ))),
            }
        })
    }

    fn parse_update_assignments(&mut self) -> Result<ListId<AssignId>> {
        self.parse_comma_separated(|parser| {
            let column = parser.parse_name()?;
            parser.consume_token(TokenKind::Equal)?;
            let value = parser.parse_expression()?;
            Ok(parser.ast.push_assign(AAssignment { column, value }))
        })
    }

    fn parse_value_lists(&mut self) -> Result<ListId<RowId>> {
        self.parse_comma_separated(|parser| {
            parser.consume_token(TokenKind::LeftParen)?;
            let values = parser.parse_comma_separated(|parser| parser.parse_expression())?;
            parser.consume_token(TokenKind::RightParen)?;
            Ok(parser.ast.push_row(values))
        })
    }

    /// The body of a `CREATE TABLE`: column definitions and table constraints, which
    /// interleave in the source but are collected into two runs.
    fn parse_table_definition_items(
        &mut self,
    ) -> Result<(ListId<ColumnDefId>, ListId<ConstraintId>)> {
        let mut columns = Vec::new();
        let mut constraints = Vec::new();

        loop {
            let token = self.peek_token()?;
            match token.dispatch_kind() {
                TokenKind::Constraint | TokenKind::Check | TokenKind::Foreign => {
                    constraints.push(self.parse_table_constraint()?);
                }
                TokenKind::Identifier => columns.push(self.parse_column_definition()?),
                _ => {
                    return Err(HematiteError::ParseError(format!(
                        "Expected column definition or table constraint, found: {}",
                        self.describe_next()
                    )))
                }
            }

            if self.peek_kind()? == TokenKind::Comma {
                self.consume_token(TokenKind::Comma)?;
                continue;
            }
            break;
        }

        let columns = self.ast.list_from(columns);
        let constraints = self.ast.list_from(constraints);
        Ok((columns, constraints))
    }

    fn parse_column_definition(&mut self) -> Result<ColumnDefId> {
        let name = self.parse_name()?;

        let data_type = self.parse_data_type()?;
        let data_type = self.ast.push_type(data_type);

        let mut nullable = true;
        let mut primary_key = false;
        let mut auto_increment = false;
        let mut unique = false;
        let mut default_value = LitId::NONE;
        let mut character_set = NameId::NONE;
        let mut collation = NameId::NONE;
        let mut check_constraint = ConstraintId::NONE;
        let mut references = ConstraintId::NONE;

        while let Ok(token) = self.peek_token() {
            match token.dispatch_kind() {
                TokenKind::Identifier if self.peek_identifier_keyword("CHARSET") => {
                    self.consume_identifier_keyword("CHARSET")?;
                    self.consume_optional_equals()?;
                    character_set = self.parse_name()?;
                }
                TokenKind::Identifier if self.peek_identifier_keyword("CHARACTER") => {
                    self.consume_identifier_keyword("CHARACTER")?;
                    self.consume_token(TokenKind::Set)?;
                    self.consume_optional_equals()?;
                    character_set = self.parse_name()?;
                }
                TokenKind::Identifier if self.peek_identifier_keyword("COLLATE") => {
                    self.consume_identifier_keyword("COLLATE")?;
                    self.consume_optional_equals()?;
                    collation = self.parse_name()?;
                }
                TokenKind::Not => {
                    self.consume_token(TokenKind::Not)?;
                    if self.peek_kind()? == TokenKind::Null {
                        self.consume_token(TokenKind::Null)?;
                        nullable = false;
                    }
                }
                TokenKind::Primary => {
                    self.consume_token(TokenKind::Primary)?;
                    self.consume_token(TokenKind::Key)?;
                    primary_key = true;
                    nullable = false;
                }
                TokenKind::Unique => {
                    self.consume_token(TokenKind::Unique)?;
                    unique = true;
                }
                TokenKind::AutoIncrement => {
                    self.consume_token(TokenKind::AutoIncrement)?;
                    auto_increment = true;
                }
                TokenKind::Default => {
                    self.consume_token(TokenKind::Default)?;
                    default_value = self.parse_default_value()?;
                }
                TokenKind::Constraint | TokenKind::Check => {
                    let constraint_name = self.parse_optional_constraint_name()?;
                    check_constraint = self.parse_check_constraint(constraint_name)?;
                }
                TokenKind::References => {
                    references = self.parse_column_foreign_key(NameId::NONE, name)?;
                }
                _ => break,
            }
        }

        Ok(self.ast.push_coldef(AColumnDef {
            name,
            data_type,
            character_set,
            collation,
            nullable,
            primary_key,
            auto_increment,
            unique,
            default_value,
            check_constraint,
            references,
        }))
    }

    fn parse_table_constraint(&mut self) -> Result<ConstraintId> {
        let constraint_name = self.parse_optional_constraint_name()?;
        match self.peek_kind()? {
            TokenKind::Check => self.parse_check_constraint(constraint_name),
            TokenKind::Unique => self.parse_unique_constraint(constraint_name),
            TokenKind::Foreign => self.parse_table_foreign_key(constraint_name),
            _ => Err(HematiteError::ParseError(format!(
                "Expected CHECK, UNIQUE, or FOREIGN KEY constraint, found: {}",
                self.describe_next()
            ))),
        }
    }

    fn parse_optional_constraint_name(&mut self) -> Result<NameId> {
        if matches!(self.peek_kind(), Ok(TokenKind::Constraint)) {
            self.consume_token(TokenKind::Constraint)?;
            return self.parse_name();
        }
        Ok(NameId::NONE)
    }

    fn parse_parenthesized_condition(&mut self) -> Result<ExprId> {
        self.consume_token(TokenKind::LeftParen)?;
        let condition = self.parse_or_expression()?;
        self.consume_token(TokenKind::RightParen)?;
        Ok(condition)
    }

    /// A `CHECK (…)` constraint.
    ///
    /// The condition is kept as a parsed expression; the catalog persists it as SQL
    /// text, and rendering it here would build a `String` for every check while
    /// parsing. While the query layer reads the owned tree, `bridge.rs` renders it
    /// when it materializes the statement.
    fn parse_check_constraint(&mut self, name: NameId) -> Result<ConstraintId> {
        self.consume_token(TokenKind::Check)?;
        let expression = self.parse_parenthesized_condition()?;
        Ok(self
            .ast
            .push_constraint(ATableConstraint::Check { name, expression }))
    }

    fn parse_unique_constraint(&mut self, name: NameId) -> Result<ConstraintId> {
        self.consume_token(TokenKind::Unique)?;
        let columns = self.parse_column_reference_list()?;
        Ok(self
            .ast
            .push_constraint(ATableConstraint::Unique { name, columns }))
    }

    fn parse_table_foreign_key(&mut self, name: NameId) -> Result<ConstraintId> {
        self.consume_token(TokenKind::Foreign)?;
        self.consume_token(TokenKind::Key)?;
        let columns = self.parse_column_reference_list()?;
        self.parse_foreign_key_reference(name, columns)
    }

    fn parse_column_foreign_key(&mut self, name: NameId, column: NameId) -> Result<ConstraintId> {
        let columns = self.ast.list_from([column]);
        self.parse_foreign_key_reference(name, columns)
    }

    fn parse_column_reference_list(&mut self) -> Result<ListId<NameId>> {
        self.consume_token(TokenKind::LeftParen)?;
        let columns = self.parse_comma_separated(|parser| parser.parse_name())?;
        self.consume_token(TokenKind::RightParen)?;
        Ok(columns)
    }

    fn parse_foreign_key_reference(
        &mut self,
        name: NameId,
        columns: ListId<NameId>,
    ) -> Result<ConstraintId> {
        self.consume_token(TokenKind::References)?;
        let referenced_table = self.parse_name()?;
        let referenced_columns = self.parse_column_reference_list()?;
        let mut on_delete = crate::parser::ast::ForeignKeyAction::Restrict;
        let mut on_update = crate::parser::ast::ForeignKeyAction::Restrict;
        while matches!(self.peek_kind(), Ok(TokenKind::On)) {
            self.consume_token(TokenKind::On)?;
            let target_is_delete = match self.peek_kind()? {
                TokenKind::Delete => {
                    self.consume_token(TokenKind::Delete)?;
                    true
                }
                TokenKind::Update => {
                    self.consume_token(TokenKind::Update)?;
                    false
                }
                _ => {
                    return Err(HematiteError::ParseError(format!(
                        "Expected DELETE or UPDATE after ON, found: {}",
                        self.describe_next()
                    )))
                }
            };
            let action = self.parse_foreign_key_action()?;
            if target_is_delete {
                on_delete = action;
            } else {
                on_update = action;
            }
        }
        Ok(self.ast.push_constraint(ATableConstraint::ForeignKey {
            name,
            columns,
            referenced_table,
            referenced_columns,
            on_delete,
            on_update,
        }))
    }

    fn parse_foreign_key_action(&mut self) -> Result<crate::parser::ast::ForeignKeyAction> {
        match self.peek_kind()? {
            TokenKind::Restrict => {
                self.consume_token(TokenKind::Restrict)?;
                Ok(crate::parser::ast::ForeignKeyAction::Restrict)
            }
            TokenKind::Cascade => {
                self.consume_token(TokenKind::Cascade)?;
                Ok(crate::parser::ast::ForeignKeyAction::Cascade)
            }
            TokenKind::Set => {
                self.consume_token(TokenKind::Set)?;
                self.consume_token(TokenKind::Null)?;
                Ok(crate::parser::ast::ForeignKeyAction::SetNull)
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected foreign key action, found: {}",
                self.describe_next()
            ))),
        }
    }

    fn parse_data_type(&mut self) -> Result<SqlTypeName> {
        let kind = self.peek_kind()?;
        let data_type = match kind {
            TokenKind::Int8 => return self.parse_integer_type(TokenKind::Int8, SqlTypeName::Int8),
            TokenKind::Int16 => {
                return self.parse_integer_type(TokenKind::Int16, SqlTypeName::Int16)
            }
            TokenKind::Int32 | TokenKind::Int => {
                return self.parse_integer_type(kind, SqlTypeName::Int)
            }
            TokenKind::Int64 => {
                return self.parse_integer_type(TokenKind::Int64, SqlTypeName::Int64)
            }
            TokenKind::UInt8 => SqlTypeName::UInt8,
            TokenKind::UInt16 => SqlTypeName::UInt16,
            TokenKind::UInt32 | TokenKind::UInt => SqlTypeName::UInt,
            TokenKind::UInt64 => SqlTypeName::UInt64,
            TokenKind::Text => SqlTypeName::Text,
            TokenKind::Boolean | TokenKind::Bool => SqlTypeName::Boolean,
            TokenKind::Float32 => SqlTypeName::Float32,
            TokenKind::Float | TokenKind::Float64 => SqlTypeName::Float,
            TokenKind::Decimal => {
                self.consume_token(kind)?;
                let (precision, scale) = self.parse_optional_numeric_precision()?;
                return Ok(SqlTypeName::Decimal { precision, scale });
            }
            TokenKind::Blob => SqlTypeName::Blob,
            TokenKind::Varchar => {
                self.consume_token(kind)?;
                return Ok(SqlTypeName::VarChar(self.parse_type_length()?));
            }
            TokenKind::Identifier => {
                let name = self.text(self.peek_token()?);
                if let Some(keyword) = uppercase_keyword_match(name, DATA_TYPE_KEYWORDS) {
                    return Err(HematiteError::ParseError(format!(
                        "Keyword '{}' must be capitalized as '{}'",
                        name, keyword
                    )));
                }
                return Err(HematiteError::ParseError(format!(
                    "Expected data type, found: {}",
                    self.describe_next()
                )));
            }
            _ => {
                return Err(HematiteError::ParseError(format!(
                    "Expected data type, found: {}",
                    self.describe_next()
                )))
            }
        };

        self.consume_token(kind)?;
        Ok(data_type)
    }

    fn parse_integer_type(
        &mut self,
        kind: TokenKind,
        signed_type: SqlTypeName,
    ) -> Result<SqlTypeName> {
        self.consume_token(kind)?;
        Ok(signed_type)
    }

    fn parse_type_length(&mut self) -> Result<u32> {
        self.consume_token(TokenKind::LeftParen)?;
        let length = match self.peek_kind()? {
            TokenKind::NumberLiteral => {
                let parsed = parse_positive_u32_literal(self.text(self.peek_token()?), "length")?;
                self.advance();
                parsed
            }
            _ => {
                return Err(HematiteError::ParseError(format!(
                    "Expected positive integer length, found: {}",
                    self.describe_next()
                )))
            }
        };
        self.consume_token(TokenKind::RightParen)?;
        Ok(length)
    }

    fn parse_optional_numeric_precision(&mut self) -> Result<(Option<u32>, Option<u32>)> {
        if !matches!(self.peek_kind(), Ok(TokenKind::LeftParen)) {
            return Ok((None, None));
        }

        self.consume_token(TokenKind::LeftParen)?;
        let precision = self.consume_positive_integer_literal("precision")?;
        let mut scale = None;
        if matches!(self.peek_kind(), Ok(TokenKind::Comma)) {
            self.consume_token(TokenKind::Comma)?;
            scale = Some(self.consume_positive_integer_literal("scale")?);
        }
        self.consume_token(TokenKind::RightParen)?;
        Ok((Some(precision), scale))
    }

    fn consume_positive_integer_literal(&mut self, label: &str) -> Result<u32> {
        match self.peek_kind()? {
            TokenKind::NumberLiteral => {
                let parsed = parse_non_negative_u32_literal(self.text(self.peek_token()?), label)?;
                self.advance();
                Ok(parsed)
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected non-negative integer {} value, found: {}",
                label,
                self.describe_next()
            ))),
        }
    }

    fn parse_default_value(&mut self) -> Result<LitId> {
        let token = self.peek_token()?;
        match token.dispatch_kind() {
            TokenKind::StringLiteral => {
                let lit = self.push_text_literal(token);
                self.advance();
                Ok(lit)
            }
            TokenKind::BlobLiteral => {
                let bytes = self.blob_value(token);
                let span = self.ast.push_bytes(&bytes);
                self.advance();
                Ok(self.ast.push_lit(ALiteral::Blob(span)))
            }
            TokenKind::NumberLiteral => {
                let lit = self.push_number_literal(token)?;
                self.advance();
                Ok(lit)
            }
            TokenKind::BooleanLiteral => {
                let value = self.boolean_value(token);
                self.advance();
                Ok(self.ast.push_lit(ALiteral::Boolean(value)))
            }
            TokenKind::NullLiteral | TokenKind::Null => {
                if token.dispatch_kind() == TokenKind::NullLiteral {
                    self.consume_token(TokenKind::NullLiteral)?;
                } else {
                    self.consume_token(TokenKind::Null)?;
                }
                Ok(self.ast.push_lit(ALiteral::Null))
            }
            _ => Err(HematiteError::ParseError(format!(
                "Expected DEFAULT literal (NULL, number, string, boolean), found: {}",
                self.describe_next()
            ))),
        }
    }

    fn peek_token(&self) -> Result<Token> {
        self.tokens
            .get(self.position)
            .copied()
            .ok_or_else(|| HematiteError::ParseError("Unexpected end of input".to_string()))
    }

    /// The next token's kind. Most decisions need only this, and it copies a byte
    /// rather than a span.
    fn peek_kind(&self) -> Result<TokenKind> {
        self.peek_token().map(|token| token.dispatch_kind())
    }

    /// Decodes a hex blob literal.
    fn blob_value(&self, token: Token) -> Vec<u8> {
        decode_hex_blob(self.text(token))
    }

    /// `TRUE` or `FALSE`, told apart by their first byte.
    fn boolean_value(&self, token: Token) -> bool {
        self.text(token).as_bytes()[0] == b'T'
    }

    /// A text literal, stored as a span rather than a `String`.
    ///
    /// The escape-free literal — nearly all of them — is a slice of the statement's
    /// own text and costs no bytes of its own. Only an escaped one appends.
    fn push_text_literal(&mut self, token: Token) -> LitId {
        let span = if token.kind == TokenKind::EscapedStringLiteral {
            let value = unescape_string_literal(token.kind, token.text(self.input)).into_owned();
            self.ast.push_text(&value)
        } else {
            Span {
                start: token.start,
                len: token.len,
            }
        };
        self.ast.push_lit(ALiteral::Text(span))
    }

    /// An integer or float literal.
    ///
    /// A float keeps its decimal digits, because ordering on float literals is exact
    /// decimal ordering rather than `f64` ordering and the catalog persists `DEFAULT`
    /// values through it. Normalizing — dropping leading zeros from the integer part
    /// and trailing zeros from the fraction — moves two span boundaries instead of
    /// building the two or three `String`s `types::normalize_float_literal` does on
    /// owned text.
    fn push_number_literal(&mut self, token: Token) -> Result<LitId> {
        let text = token.text(self.input);
        if !text.contains('.') {
            let parsed = text.parse::<i128>().map_err(|_| {
                HematiteError::ParseError(format!("Invalid integer literal '{}'", text))
            })?;
            if parsed.unsigned_abs() > u64::MAX as u128 {
                return Err(HematiteError::ParseError(format!(
                    "Integer literal '{}' is out of 64-bit range",
                    text
                )));
            }
            return Ok(self.ast.push_lit(ALiteral::Integer(parsed)));
        }

        let value = text
            .parse::<f64>()
            .map_err(|_| HematiteError::ParseError(format!("Invalid float literal '{}'", text)))?;

        // The lexer guarantees one '.', digits either side (possibly none).
        let dot = text.find('.').expect("checked above");
        let integer_text = &text[..dot];
        let fraction_text = &text[dot + 1..];
        let integer_offset = integer_text.len() - integer_text.trim_start_matches('0').len();
        let fraction_len = fraction_text.trim_end_matches('0').len();

        let float = FloatLit {
            value,
            negative: false,
            integer: Span {
                start: token.start + integer_offset as u32,
                len: (integer_text.len() - integer_offset) as u32,
            },
            fraction: Span {
                start: token.start + dot as u32 + 1,
                len: fraction_len as u32,
            },
        };
        let float = self.ast.push_float(float);
        Ok(self.ast.push_lit(ALiteral::Float(float)))
    }

    /// A `table.column` or bare `column` reference, kept as its two halves.
    ///
    /// Joining them into one `"t.col"` string, as the owned tree does, costs a
    /// `format!` here and a byte scan to take it apart on every row that reads it.
    fn parse_column_reference(&mut self) -> Result<(NameId, NameId)> {
        let first = self.parse_identifier_token()?;
        if matches!(self.peek_kind(), Ok(TokenKind::Dot)) {
            self.consume_token(TokenKind::Dot)?;
            let second = self.parse_identifier_token()?;
            let qualifier = self.name_of(first);
            let name = self.name_of(second);
            return Ok((qualifier, name));
        }
        let name = self.name_of(first);
        Ok((NameId::NONE, name))
    }

    /// An identifier, recorded in the arena as a name.
    fn parse_name(&mut self) -> Result<NameId> {
        let token = self.parse_identifier_token()?;
        Ok(self.name_of(token))
    }

    /// The next token, required to be an identifier, without interpreting it.
    fn parse_identifier_token(&mut self) -> Result<Token> {
        let token = self.peek_token()?;
        if token.dispatch_kind() != TokenKind::Identifier {
            return Err(HematiteError::ParseError(format!(
                "Expected identifier, found: {}",
                self.describe_next()
            )));
        }
        self.advance();
        Ok(token)
    }

    /// One `WHEN condition THEN result` of a `CASE`.
    fn parse_case_branch(&mut self) -> Result<BranchId> {
        let condition = self.parse_expression()?;
        self.consume_token(TokenKind::Then)?;
        let result = self.parse_expression()?;
        Ok(self.ast.push_branch(CaseBranch { condition, result }))
    }

    /// Renders the next token for an error message.
    ///
    /// A span-based token's `Debug` is a kind and two offsets, which tells a reader
    /// nothing. This prints the kind and, for the kinds whose text is the point, the
    /// text: `Identifier("users")`.
    fn describe_next(&self) -> String {
        let Ok(token) = self.peek_token() else {
            return "end of input".to_string();
        };
        match token.dispatch_kind() {
            TokenKind::Identifier
            | TokenKind::StringLiteral
            | TokenKind::NumberLiteral
            | TokenKind::BlobLiteral
            | TokenKind::BooleanLiteral => {
                format!("{:?}({:?})", token.dispatch_kind(), self.text(token))
            }
            kind => format!("{kind:?}"),
        }
    }

    /// The text a token spans, out of the input this parser was built over.
    #[inline]
    fn text(&self, token: Token) -> &'a str {
        token.text(self.input)
    }

    fn advance(&mut self) {
        self.position += 1;
    }

    /// A comma-separated list, staged on the scratch stack and closed into the arena.
    ///
    /// The stack is what lets an item's own parse open lists of its own — a `VALUES`
    /// row inside a row list, an argument list inside an argument.
    fn parse_comma_separated<T, F>(&mut self, mut parse_item: F) -> Result<ListId<T>>
    where
        T: ArenaId,
        F: FnMut(&mut Self) -> Result<T>,
    {
        let mark = self.ast.list_open();
        loop {
            match parse_item(self) {
                Ok(item) => self.ast.list_push(item),
                Err(err) => {
                    self.ast.list_abort(mark);
                    return Err(err);
                }
            }
            if !matches!(self.peek_kind(), Ok(TokenKind::Comma)) {
                break;
            }
            if let Err(err) = self.consume_token(TokenKind::Comma) {
                self.ast.list_abort(mark);
                return Err(err);
            }
        }
        Ok(self.ast.list_close(mark))
    }

    fn next_token_is(&self, expected: TokenKind) -> bool {
        self.tokens
            .get(self.position + 1)
            .is_some_and(|token| token.dispatch_kind() == expected)
    }

    fn consume_token(&mut self, expected: TokenKind) -> Result<()> {
        let token = self.peek_token()?;
        if token.dispatch_kind() == expected {
            self.position += 1;
            return Ok(());
        }

        // Keywords are lexed case-sensitively, so a lowercase one arrives here as an
        // identifier. Saying which capitalization was meant is more use than saying
        // an identifier was unexpected.
        if token.dispatch_kind() == TokenKind::Identifier {
            let name = self.text(token);
            if let Some(keyword) = uppercase_keyword_match(name, ALL_UPPERCASE_KEYWORDS) {
                return Err(HematiteError::ParseError(format!(
                    "Keyword '{}' must be capitalized as '{}'",
                    name, keyword
                )));
            }
            if let Some(keyword) = token_keyword_name(expected) {
                if name.eq_ignore_ascii_case(keyword) {
                    return Err(HematiteError::ParseError(format!(
                        "Keyword '{}' must be capitalized as '{}'",
                        name, keyword
                    )));
                }
            }
        }

        Err(HematiteError::ParseError(format!(
            "Expected {:?}, found: {}",
            expected,
            self.describe_next()
        )))
    }
}

pub fn tokenize_sql(sql: &str) -> Result<Vec<Token>> {
    let mut lexer = Lexer::new(sql);
    lexer.tokenize()?;
    Ok(lexer.into_tokens())
}

/// Parses one statement into a fresh arena.
pub fn parse_sql_into_arena(sql: &str) -> Result<Ast> {
    parse_sql_reusing(sql, Ast::default())
}

/// Parses one statement into an arena the caller already owns.
///
/// This is the entry point that keeps the arena's buffers across statements: hand
/// back the same `Ast` each time and they are reused rather than reacquired. The
/// token buffer is not — `tokenize_sql` builds a fresh one per call.
pub fn parse_sql_reusing(sql: &str, ast: Ast) -> Result<Ast> {
    let mut parser = Parser::with_arena(sql, tokenize_sql(sql)?, ast);
    let root = parser.parse()?;
    let mut ast = parser.into_ast();
    ast.set_root(root);
    Ok(ast)
}

/// Parses one statement and materializes it as the owned tree in `ast.rs`, which is
/// what the query layer reads. The arena is built fresh and dropped afterwards.
pub fn parse_sql_statement(sql: &str) -> Result<Statement> {
    let ast = parse_sql_into_arena(sql)?;
    Ok(ast.to_owned_statement())
}

/// Parses a bare expression — a stored `CHECK` condition, re-read from the catalog.
pub fn parse_expression_fragment(sql: &str) -> Result<Expression> {
    let ast = parse_expression_into_arena(sql)?;
    let root = ast.fragment_root();
    Ok(ast.to_owned_expression(root))
}

/// Parses a bare expression into an arena, keeping it in arena form.
pub fn parse_expression_into_arena(sql: &str) -> Result<Ast> {
    let mut parser = Parser::new(sql, tokenize_sql(sql)?);
    let condition = parser.parse_or_expression()?;
    if parser.position != parser.tokens.len() {
        return Err(HematiteError::ParseError(
            "Unexpected trailing tokens in CHECK constraint".to_string(),
        ));
    }
    let mut ast = parser.into_ast();
    ast.set_fragment_root(condition);
    Ok(ast)
}

fn parse_non_negative_integer_literal(value: &str, label: &str) -> Result<usize> {
    value.parse::<usize>().map_err(|_| {
        HematiteError::ParseError(format!(
            "Expected non-negative integer after {}, found: {}",
            label, value
        ))
    })
}

fn parse_non_negative_u32_literal(value: &str, label: &str) -> Result<u32> {
    value.parse::<u32>().map_err(|_| {
        HematiteError::ParseError(format!(
            "Expected non-negative integer {} value, found: {}",
            label, value
        ))
    })
}

fn parse_positive_u32_literal(value: &str, label: &str) -> Result<u32> {
    let parsed = value.parse::<u32>().map_err(|_| {
        HematiteError::ParseError(format!(
            "Expected positive integer {}, found: {}",
            label, value
        ))
    })?;
    if parsed == 0 {
        return Err(HematiteError::ParseError(format!(
            "Expected positive integer {}, found: {}",
            label, value
        )));
    }
    Ok(parsed)
}

const TOP_LEVEL_KEYWORDS: &[&str] = &[
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
    "RELEASE",
    "EXPLAIN",
    "DESCRIBE",
    "SHOW",
    "SELECT",
    "UPDATE",
    "INSERT",
    "DELETE",
    "CREATE",
    "ALTER",
    "DROP",
    "WITH",
];

const ALL_UPPERCASE_KEYWORDS: &[&str] = &[
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
    "RELEASE",
    "SELECT",
    "UPDATE",
    "FROM",
    "INSERT",
    "DELETE",
    "DROP",
    "EXPLAIN",
    "DESCRIBE",
    "SHOW",
    "TABLES",
    "VIEWS",
    "INDEXES",
    "TRIGGERS",
    "ALTER",
    "ADD",
    "IF",
    "INTO",
    "SET",
    "VALUES",
    "CREATE",
    "VIEW",
    "TRIGGER",
    "INDEX",
    "EXISTS",
    "UNION",
    "INTERSECT",
    "EXCEPT",
    "ALL",
    "WITH",
    "LEFT",
    "RIGHT",
    "FULL",
    "OUTER",
    "INNER",
    "JOIN",
    "ON",
    "AS",
    "DISTINCT",
    "CAST",
    "TABLE",
    "COLUMN",
    "WHERE",
    "GROUP",
    "HAVING",
    "ORDER",
    "BY",
    "ASC",
    "DESC",
    "LIMIT",
    "OFFSET",
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "INT8",
    "INT16",
    "INT",
    "INT32",
    "INT64",
    "UINT8",
    "UINT16",
    "UINT",
    "UINT32",
    "UINT64",
    "TEXT",
    "BOOLEAN",
    "FLOAT",
    "FLOAT32",
    "FLOAT64",
    "BOOL",
    "DECIMAL",
    "BLOB",
    "VARCHAR",
    "AUTO_INCREMENT",
    "UNIQUE",
    "PRIMARY",
    "KEY",
    "DUPLICATE",
    "CONSTRAINT",
    "CHECK",
    "FOREIGN",
    "REFERENCES",
    "CASCADE",
    "RESTRICT",
    "RENAME",
    "TO",
    "AFTER",
    "NOT",
    "IS",
    "NULL",
    "DEFAULT",
    "IN",
    "BETWEEN",
    "LIKE",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "AND",
    "OR",
    "USING",
    "ENGINE",
    "CHARSET",
    "CHARACTER",
    "COLLATE",
    "BTREE",
    "HASH",
];

const DATA_TYPE_KEYWORDS: &[&str] = &[
    "INT8", "INT16", "INT", "INT32", "INT64", "UINT8", "UINT16", "UINT", "UINT32", "UINT64",
    "TEXT", "BOOLEAN", "BOOL", "FLOAT", "FLOAT32", "FLOAT64", "DECIMAL", "BLOB", "VARCHAR",
];

fn uppercase_keyword_match<'a>(name: &str, keywords: &'a [&'a str]) -> Option<&'a str> {
    keywords
        .iter()
        .copied()
        .find(|keyword| name.eq_ignore_ascii_case(keyword) && name != *keyword)
}

fn capitalization_hint_for_token(kind: TokenKind, text: &str) -> Option<String> {
    match kind {
        TokenKind::Identifier => uppercase_keyword_match(text, ALL_UPPERCASE_KEYWORDS)
            .map(|keyword| format!("Keyword '{}' must be capitalized as '{}'", text, keyword)),
        _ => None,
    }
}

fn token_keyword_name(kind: TokenKind) -> Option<&'static str> {
    match kind {
        TokenKind::Begin => Some("BEGIN"),
        TokenKind::Commit => Some("COMMIT"),
        TokenKind::Rollback => Some("ROLLBACK"),
        TokenKind::Savepoint => Some("SAVEPOINT"),
        TokenKind::Release => Some("RELEASE"),
        TokenKind::Select => Some("SELECT"),
        TokenKind::Update => Some("UPDATE"),
        TokenKind::From => Some("FROM"),
        TokenKind::Insert => Some("INSERT"),
        TokenKind::Delete => Some("DELETE"),
        TokenKind::Drop => Some("DROP"),
        TokenKind::Explain => Some("EXPLAIN"),
        TokenKind::Describe => Some("DESCRIBE"),
        TokenKind::Show => Some("SHOW"),
        TokenKind::Tables => Some("TABLES"),
        TokenKind::Views => Some("VIEWS"),
        TokenKind::Indexes => Some("INDEXES"),
        TokenKind::Triggers => Some("TRIGGERS"),
        TokenKind::Alter => Some("ALTER"),
        TokenKind::Add => Some("ADD"),
        TokenKind::If => Some("IF"),
        TokenKind::Into => Some("INTO"),
        TokenKind::Set => Some("SET"),
        TokenKind::Values => Some("VALUES"),
        TokenKind::Create => Some("CREATE"),
        TokenKind::View => Some("VIEW"),
        TokenKind::Trigger => Some("TRIGGER"),
        TokenKind::Index => Some("INDEX"),
        TokenKind::Exists => Some("EXISTS"),
        TokenKind::Union => Some("UNION"),
        TokenKind::Intersect => Some("INTERSECT"),
        TokenKind::Except => Some("EXCEPT"),
        TokenKind::All => Some("ALL"),
        TokenKind::With => Some("WITH"),
        TokenKind::Left => Some("LEFT"),
        TokenKind::Right => Some("RIGHT"),
        TokenKind::Full => Some("FULL"),
        TokenKind::Outer => Some("OUTER"),
        TokenKind::Inner => Some("INNER"),
        TokenKind::Join => Some("JOIN"),
        TokenKind::On => Some("ON"),
        TokenKind::As => Some("AS"),
        TokenKind::Distinct => Some("DISTINCT"),
        TokenKind::Cast => Some("CAST"),
        TokenKind::Table => Some("TABLE"),
        TokenKind::Column => Some("COLUMN"),
        TokenKind::Where => Some("WHERE"),
        TokenKind::Group => Some("GROUP"),
        TokenKind::Having => Some("HAVING"),
        TokenKind::Order => Some("ORDER"),
        TokenKind::By => Some("BY"),
        TokenKind::Asc => Some("ASC"),
        TokenKind::Desc => Some("DESC"),
        TokenKind::Limit => Some("LIMIT"),
        TokenKind::Offset => Some("OFFSET"),
        TokenKind::Count => Some("COUNT"),
        TokenKind::Sum => Some("SUM"),
        TokenKind::Avg => Some("AVG"),
        TokenKind::Min => Some("MIN"),
        TokenKind::Max => Some("MAX"),
        TokenKind::Int32 => Some("INT32"),
        TokenKind::Text => Some("TEXT"),
        TokenKind::Boolean => Some("BOOLEAN"),
        TokenKind::Float => Some("FLOAT"),
        TokenKind::Float32 => Some("FLOAT32"),
        TokenKind::Float64 => Some("FLOAT64"),
        TokenKind::Int8 => Some("INT8"),
        TokenKind::Int16 => Some("INT16"),
        TokenKind::Int64 => Some("INT64"),
        TokenKind::Int => Some("INT"),
        TokenKind::UInt8 => Some("UINT8"),
        TokenKind::UInt16 => Some("UINT16"),
        TokenKind::UInt64 => Some("UINT64"),
        TokenKind::UInt32 => Some("UINT32"),
        TokenKind::UInt => Some("UINT"),
        TokenKind::Bool => Some("BOOL"),
        TokenKind::Decimal => Some("DECIMAL"),
        TokenKind::Blob => Some("BLOB"),
        TokenKind::Varchar => Some("VARCHAR"),
        TokenKind::AutoIncrement => Some("AUTO_INCREMENT"),
        TokenKind::Unique => Some("UNIQUE"),
        TokenKind::Primary => Some("PRIMARY"),
        TokenKind::Key => Some("KEY"),
        TokenKind::Duplicate => Some("DUPLICATE"),
        TokenKind::Constraint => Some("CONSTRAINT"),
        TokenKind::Check => Some("CHECK"),
        TokenKind::Foreign => Some("FOREIGN"),
        TokenKind::References => Some("REFERENCES"),
        TokenKind::Cascade => Some("CASCADE"),
        TokenKind::Restrict => Some("RESTRICT"),
        TokenKind::Rename => Some("RENAME"),
        TokenKind::To => Some("TO"),
        TokenKind::After => Some("AFTER"),
        TokenKind::Not => Some("NOT"),
        TokenKind::Is => Some("IS"),
        TokenKind::Null => Some("NULL"),
        TokenKind::Default => Some("DEFAULT"),
        TokenKind::In => Some("IN"),
        TokenKind::Between => Some("BETWEEN"),
        TokenKind::Like => Some("LIKE"),
        TokenKind::Case => Some("CASE"),
        TokenKind::When => Some("WHEN"),
        TokenKind::Then => Some("THEN"),
        TokenKind::Else => Some("ELSE"),
        TokenKind::End => Some("END"),
        TokenKind::And => Some("AND"),
        TokenKind::Or => Some("OR"),
        _ => None,
    }
}
