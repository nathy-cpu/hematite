//! The parsed form of a statement, and the storage it lives in.
//!
//! # Why a statement is not a tree of boxes
//!
//! The obvious representation of an AST in Rust — `Box` for each child, `Vec` for
//! each list, `String` for each name — costs one allocation per node and one per
//! name. `docs/performance.md` measures what that comes to: a 400-row
//! `INSERT … VALUES` allocated 817 times, one `Vec` per row and one `String` per text
//! literal, and a four-join `SELECT` allocated 82 times for a tree with 82 nodes in
//! it. None of that memory outlives the statement, and every statement re-acquires
//! all of it.
//!
//! So a statement is stored the way a page is: a few flat buffers, addressed by
//! index. `Box<Expression>` becomes [`ExprId`], a `u32`. `Vec<Expression>` becomes a
//! run in one shared list arena. A name becomes a byte range into the statement's own
//! SQL. Three things follow:
//!
//! 1. **Building into a warm [`Ast`] allocates nothing.** [`Ast::reset`] truncates
//!    without releasing, so the buffers are already the right size by the second
//!    statement. (The token buffer is the lexer's, and an escaped literal or a blob
//!    is decoded through a temporary before it is appended.)
//! 2. **Nodes are `Copy` and small.** An `Expression` is 16 bytes and is read out of
//!    the arena by value, so a pass that rewrites the tree reads a node, changes it,
//!    and writes it back — no `&mut` walk, and no borrow conflicts with the arena.
//! 3. **Copying a whole statement is a memcpy per buffer**, not a traversal. That is
//!    what binding a prepared statement's parameters, and binding a correlated
//!    subquery once per row of its outer query, need to be cheap — once the query
//!    layer binds against the arena rather than the owned tree in `ast.rs`.
//!
//! # Invariants
//!
//! - Arena buffers are **append-only, with in-place overwrite**. Never `remove`,
//!   `swap_remove`, `retain`, `sort`, `truncate` or `drain` one: ids handed out
//!   earlier would silently start pointing at other nodes. Only `push`, index-assign,
//!   and [`Ast::reset`].
//! - Consequently a rewrite that shortens a list emits a *new* run and abandons the
//!   old one. The abandoned run is garbage until `reset`, which costs nothing —
//!   garbage is bounded by the size of the statement.
//! - Every id is a `u32` newtype with `u32::MAX` as its niche-free `None`. That is
//!   what lets one `Vec<u32>` hold runs of *any* id type.

use std::marker::PhantomData;

use crate::parser::ast::{
    AggregateFunction, ArithmeticOperator, ComparisonOperator, ForeignKeyAction, LogicalOperator,
    ScalarFunction, SetOperator, SortDirection,
};
use crate::parser::types::SqlTypeName;

/// A byte range into [`Ast::text`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Span {
    pub start: u32,
    pub len: u32,
}

impl Span {
    #[inline]
    pub fn is_empty(self) -> bool {
        self.len == 0
    }
}

/// A handle into one of the arena's buffers.
///
/// Implemented only by the `u32` newtypes [`arena_id!`] generates. The trait exists
/// so [`Ast::lists`] can store a run of any of them: erasing to `u32` is sound
/// because every implementor is `#[repr(transparent)]` over one.
pub trait ArenaId: Copy + Eq {
    const NONE: Self;

    fn from_raw(raw: u32) -> Self;
    fn raw(self) -> u32;

    /// Whether this is the absent handle rather than a real node.
    #[inline]
    fn is_none(self) -> bool {
        self.raw() == u32::MAX
    }

    /// The handle as an `Option`, for code that spells absence that way — such as
    /// `bridge.rs` filling the owned tree's `Option<Box<…>>` fields.
    #[inline]
    fn get(self) -> Option<Self> {
        if self.is_none() {
            None
        } else {
            Some(self)
        }
    }
}

macro_rules! arena_id {
    ($($(#[$meta:meta])* $name:ident),* $(,)?) => {$(
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[repr(transparent)]
        pub struct $name(u32);

        impl ArenaId for $name {
            const NONE: Self = $name(u32::MAX);

            #[inline]
            fn from_raw(raw: u32) -> Self {
                $name(raw)
            }

            #[inline]
            fn raw(self) -> u32 {
                self.0
            }
        }

        impl Default for $name {
            #[inline]
            fn default() -> Self {
                Self::NONE
            }
        }
    )*};
}

arena_id!(
    /// An expression node.
    ExprId,
    /// A `SELECT`, including one nested as a subquery or a CTE body.
    SelectId,
    /// A table reference: a named table, a derived table, or a join.
    TableRefId,
    /// One item of a select list.
    ItemId,
    /// One `ORDER BY` term.
    OrderId,
    /// One `WHEN … THEN` branch of a `CASE`.
    BranchId,
    /// One common table expression.
    CteId,
    /// One `SET column = value` assignment.
    AssignId,
    /// One column definition in a `CREATE TABLE`.
    ColumnDefId,
    /// One table-level constraint.
    ConstraintId,
    /// One `VALUES (…)` row; the node is the run of its expressions.
    RowId,
    /// A literal's value.
    LitId,
    /// A float literal's normalized decimal form.
    FloatId,
    /// A type name, which may carry a precision.
    TypeId,
    /// An identifier, as a range of the statement's text.
    NameId,
    /// A set operation joining two selects.
    SetOpId,
);

/// A run of `len` consecutive ids in [`Ast::lists`].
///
/// Carries its own length so `len()` and `is_empty()` are free — the consumers ask
/// constantly, and a length-prefixed run would make both a memory access.
#[derive(Debug)]
pub struct ListId<T> {
    start: u32,
    len: u32,
    /// `fn() -> T` rather than `T` so `ListId<T>` stays `Copy`, `Send` and `Sync`
    /// whatever `T` is, and imposes no drop or variance of its own.
    marker: PhantomData<fn() -> T>,
}

impl<T> Clone for ListId<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for ListId<T> {}

impl<T> PartialEq for ListId<T> {
    fn eq(&self, other: &Self) -> bool {
        self.start == other.start && self.len == other.len
    }
}

impl<T> Eq for ListId<T> {}

impl<T> Default for ListId<T> {
    fn default() -> Self {
        Self::EMPTY
    }
}

impl<T> ListId<T> {
    /// The empty run. Also what an absent optional list is: nothing distinguishes
    /// "no `GROUP BY`" from "a `GROUP BY` of nothing", and the grammar cannot produce
    /// the latter.
    pub const EMPTY: Self = ListId {
        start: 0,
        len: 0,
        marker: PhantomData,
    };

    #[inline]
    pub fn len(self) -> usize {
        self.len as usize
    }

    #[inline]
    pub fn is_empty(self) -> bool {
        self.len == 0
    }
}

/// A saved height of the scratch stack, returned by [`Ast::list_open`].
#[derive(Debug, Clone, Copy)]
pub struct Mark(u32);

/// Where a list under construction is staged.
///
/// A recursive-descent parser fills lists *nested*: parsing `COALESCE(a, LOWER(b), c)`
/// begins `COALESCE`'s argument list, then begins and finishes `LOWER`'s inside it.
/// Appending ids straight to the arena would interleave the two runs.
///
/// Lists nevertheless open and close in strict LIFO order — that is what recursive
/// descent means — so one stack is enough. Each list records the stack height it
/// started at, pushes its element ids as it parses them, and on close moves
/// everything above that mark into the arena, where it is contiguous *because* every
/// nested list closed first. The nested run lands earlier in the arena than its
/// parent, which nothing cares about.
impl Ast {
    /// Begins a list. Every `list_open` must be matched by a `list_close` or a
    /// [`list_abort`](Self::list_abort), including on the error path.
    #[inline]
    pub fn list_open(&mut self) -> Mark {
        Mark(self.scratch.len() as u32)
    }

    #[inline]
    pub fn list_push<T: ArenaId>(&mut self, id: T) {
        self.scratch.push(id.raw());
    }

    /// Ends a list, moving its ids into the arena as one contiguous run.
    #[inline]
    pub fn list_close<T: ArenaId>(&mut self, mark: Mark) -> ListId<T> {
        let from = mark.0 as usize;
        let start = self.lists.len() as u32;
        self.lists.extend_from_slice(&self.scratch[from..]);
        let len = self.lists.len() as u32 - start;
        self.scratch.truncate(from);
        ListId {
            start,
            len,
            marker: PhantomData,
        }
    }

    /// Abandons a list under construction, so a parse that fails part-way through one
    /// leaves the stack balanced for the next statement.
    #[inline]
    pub fn list_abort(&mut self, mark: Mark) {
        self.scratch.truncate(mark.0 as usize);
    }

    /// The ids in a run.
    #[inline]
    pub fn list<T: ArenaId + 'static>(&self, id: ListId<T>) -> impl Iterator<Item = T> + '_ {
        let start = id.start as usize;
        self.lists[start..start + id.len as usize]
            .iter()
            .copied()
            .map(T::from_raw)
    }

    /// The `index`-th id in a run, or `None` past the end.
    #[inline]
    pub fn list_at<T: ArenaId>(&self, id: ListId<T>, index: usize) -> Option<T> {
        (index < id.len()).then(|| T::from_raw(self.lists[id.start as usize + index]))
    }

    /// Builds a run from ids the caller already has in hand.
    pub fn list_from<T: ArenaId>(&mut self, ids: impl IntoIterator<Item = T>) -> ListId<T> {
        let mark = self.list_open();
        for id in ids {
            self.list_push(id);
        }
        self.list_close(mark)
    }
}

// ── Nodes ────────────────────────────────────────────────────────────────────
//
// Field and variant names match `crate::parser::ast` exactly. Only the *types*
// differ — `Box<Expression>` is an `ExprId`, `Vec<Expression>` a `ListId<ExprId>`,
// `String` a `NameId` — so a consumer moving from the owned tree to the arena
// changes how it reaches a child, never which child it means.

/// A statement. 12 bytes: everything it owns lives in the arena.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Statement {
    /// Also what an arena holds before anything has been parsed into it. `BEGIN` is
    /// the one statement with no parts, so it needs nothing from the arena to be a
    /// valid placeholder.
    #[default]
    Begin,
    Commit,
    Rollback,
    Savepoint(NameId),
    RollbackToSavepoint(NameId),
    ReleaseSavepoint(NameId),
    Explain(StmtId),
    Describe(NameId),
    ShowTables,
    /// `NONE` when no table was named.
    ShowIndexes(NameId),
    ShowCreateTable(NameId),
    Select(SelectId),
    SelectInto {
        table: NameId,
        query: SelectId,
    },
    Update(UpdateId),
    Insert(InsertId),
    Delete(DeleteId),
    Create(CreateId),
    CreateIndex(CreateIndexId),
    Alter(AlterId),
    Drop(DropId),
    DropIndex(DropIndexId),
}

arena_id!(
    /// A statement node, used where one statement nests inside another (`EXPLAIN`).
    StmtId,
    /// An `UPDATE`'s parts.
    UpdateId,
    /// An `INSERT`'s parts.
    InsertId,
    /// A `DELETE`'s parts.
    DeleteId,
    /// A `CREATE TABLE`'s parts.
    CreateId,
    /// A `CREATE INDEX`'s parts.
    CreateIndexId,
    /// An `ALTER TABLE`'s parts.
    AlterId,
    /// A `DROP TABLE`'s parts.
    DropId,
    /// A `DROP INDEX`'s parts.
    DropIndexId,
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Select {
    pub with_clause: ListId<CteId>,
    pub distinct: bool,
    pub columns: ListId<ItemId>,
    pub from: TableRefId,
    /// Empty is absent; `bridge.rs` turns it into the owned tree's
    /// `Option<WhereClause>`. Nothing needs "no WHERE" told apart from "a WHERE of no
    /// conditions" — the grammar cannot produce the second.
    pub where_clause: ListId<ExprId>,
    pub group_by: ListId<ExprId>,
    pub having_clause: ListId<ExprId>,
    pub order_by: ListId<OrderId>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub set_operation: SetOpId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetOp {
    pub operator: SetOperator,
    pub right: SelectId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cte {
    pub name: NameId,
    pub query: SelectId,
}

/// One item of a select list.
///
/// The alias lives here rather than in a `Vec<Option<String>>` parallel to the item
/// list — the owned tree's `column_aliases` — which every reader must index in step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectItem {
    pub kind: SelectItemKind,
    /// `NONE` when the item was not aliased.
    pub alias: NameId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectItemKind {
    Wildcard,
    Column(ExprId),
    Expression(ExprId),
    CountAll,
    Aggregate {
        function: AggregateFunction,
        target: AggregateTarget,
    },
}

/// What an aggregate is applied to. `name` is `NONE` for `COUNT(*)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregateTarget {
    pub qualifier: NameId,
    pub name: NameId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableRef {
    Table {
        name: NameId,
        alias: NameId,
    },
    Derived {
        subquery: SelectId,
        alias: NameId,
    },
    CrossJoin {
        left: TableRefId,
        right: TableRefId,
    },
    Join {
        kind: JoinKind,
        left: TableRefId,
        right: TableRefId,
        on: ExprId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    FullOuter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderByItem {
    pub qualifier: NameId,
    pub name: NameId,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CaseBranch {
    pub condition: ExprId,
    pub result: ExprId,
}

/// An expression node.
///
/// 16 bytes, `Copy`, no allocation of its own. The widest variants carry three ids
/// and a flag; `Literal` carries a `LitId` rather than a value so an `i128` cannot
/// set the size of every node in the arena.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expression {
    /// `qualifier` is `NONE` for an unqualified reference. The two halves are stored
    /// apart: the owned tree joins them into one `"t.col"` string, which costs a
    /// `format!` to build and a byte scan to take apart again on every row.
    Column {
        qualifier: NameId,
        name: NameId,
    },
    Literal(LitId),
    Parameter(u32),
    ScalarSubquery(SelectId),
    Cast {
        expr: ExprId,
        target_type: TypeId,
    },
    Case {
        branches: ListId<BranchId>,
        /// `NONE` when there is no `ELSE`.
        else_expr: ExprId,
    },
    ScalarFunctionCall {
        function: ScalarFunction,
        args: ListId<ExprId>,
    },
    AggregateCall {
        function: AggregateFunction,
        target: AggregateTarget,
    },
    UnaryMinus(ExprId),
    UnaryNot(ExprId),
    Binary {
        left: ExprId,
        operator: ArithmeticOperator,
        right: ExprId,
    },
    Comparison {
        left: ExprId,
        operator: ComparisonOperator,
        right: ExprId,
    },
    InList {
        expr: ExprId,
        values: ListId<ExprId>,
        is_not: bool,
    },
    InSubquery {
        expr: ExprId,
        subquery: SelectId,
        is_not: bool,
    },
    Between {
        expr: ExprId,
        lower: ExprId,
        upper: ExprId,
        is_not: bool,
    },
    Like {
        expr: ExprId,
        pattern: ExprId,
        is_not: bool,
    },
    Exists {
        subquery: SelectId,
        is_not: bool,
    },
    NullCheck {
        expr: ExprId,
        is_not: bool,
    },
    Logical {
        left: ExprId,
        operator: LogicalOperator,
        right: ExprId,
    },
}

// ── Statements with parts ────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Insert {
    pub table: NameId,
    pub columns: ListId<NameId>,
    pub source: InsertSource,
    /// Empty when there is no `ON DUPLICATE KEY UPDATE`; `has_on_duplicate` says
    /// which, mirroring the owned tree's `Option<Vec<_>>`.
    pub on_duplicate: ListId<AssignId>,
    pub has_on_duplicate: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertSource {
    Values(ListId<RowId>),
    Select(SelectId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Assignment {
    pub column: NameId,
    pub value: ExprId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Update {
    pub table: NameId,
    pub target_binding: NameId,
    /// `NONE` when the target table is the only source.
    pub source: TableRefId,
    pub assignments: ListId<AssignId>,
    pub where_clause: ListId<ExprId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Delete {
    pub table: NameId,
    pub target_binding: NameId,
    pub source: TableRefId,
    pub where_clause: ListId<ExprId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Create {
    pub table: NameId,
    pub columns: ListId<ColumnDefId>,
    pub constraints: ListId<ConstraintId>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreateIndex {
    pub index_name: NameId,
    pub table: NameId,
    pub columns: ListId<NameId>,
    pub unique: bool,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Drop {
    pub table: NameId,
    pub if_exists: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DropIndex {
    pub index_name: NameId,
    pub table: NameId,
    pub if_exists: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Alter {
    pub table: NameId,
    pub operation: AlterOperation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterOperation {
    RenameTo(NameId),
    RenameColumn { old_name: NameId, new_name: NameId },
    AddColumn(ColumnDefId),
    AddConstraint(ConstraintId),
    DropColumn(NameId),
    DropConstraint(NameId),
    AlterColumnSetDefault { column_name: NameId, value: LitId },
    AlterColumnDropDefault { column_name: NameId },
    AlterColumnSetNotNull { column_name: NameId },
    AlterColumnDropNotNull { column_name: NameId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: NameId,
    pub data_type: TypeId,
    pub character_set: NameId,
    pub collation: NameId,
    pub nullable: bool,
    pub primary_key: bool,
    pub auto_increment: bool,
    pub unique: bool,
    /// `NONE` when the column has no `DEFAULT`.
    pub default_value: LitId,
    pub check_constraint: ConstraintId,
    pub references: ConstraintId,
}

/// A table-level constraint, or a column-level one hoisted to the same form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableConstraint {
    PrimaryKey {
        name: NameId,
        columns: ListId<NameId>,
    },
    Unique {
        name: NameId,
        columns: ListId<NameId>,
    },
    /// The condition is kept as a parsed expression. The catalog persists it as SQL
    /// text; the arena never renders it, and while the query layer reads the owned
    /// tree, `bridge.rs` does so when it materializes the statement.
    Check { name: NameId, expression: ExprId },
    ForeignKey {
        name: NameId,
        columns: ListId<NameId>,
        referenced_table: NameId,
        referenced_columns: ListId<NameId>,
        on_delete: ForeignKeyAction,
        on_update: ForeignKeyAction,
    },
}

// ── Literals ─────────────────────────────────────────────────────────────────

/// A literal's value. `Copy`, because its text lives in the arena.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Literal {
    /// `i128` only so it can span both `i64` and `u64`; the parser rejects anything
    /// wider. It lives behind a `LitId` so it does not set the size of every
    /// expression node.
    Integer(i128),
    Text(Span),
    Blob(Span),
    Boolean(bool),
    Float(FloatId),
    Null,
}

/// A float literal in normalized decimal form.
///
/// Ordering on float literals is exact decimal ordering, not `f64` ordering, and the
/// catalog persists `DEFAULT` values through it — so the digits are kept. They are
/// kept as *spans of the original token*: normalizing means stripping leading zeros
/// from the integer part and trailing zeros from the fraction, which is moving two
/// span boundaries rather than building a string.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FloatLit {
    /// Parsed once here: `push_number_literal` has to parse it to validate anyway.
    pub value: f64,
    pub negative: bool,
    /// Integer digits, leading zeros stripped. Empty means zero.
    pub integer: Span,
    /// Fraction digits, trailing zeros stripped. Empty means integral.
    pub fraction: Span,
}

impl Eq for FloatLit {}

// ── The arena ────────────────────────────────────────────────────────────────

/// A parsed statement and the storage its nodes live in.
///
/// Built to be reused for statement after statement: [`reset`](Self::reset) empties
/// every buffer without releasing it, so a warm `Ast` is built into without reaching
/// the allocator. `parse_sql_reusing` is the entry point that does this.
#[derive(Debug, Clone, Default)]
pub struct Ast {
    /// The statement's SQL, followed by any text whose value is not in it — an
    /// unescaped literal or identifier. Names and text literals are spans into this,
    /// so the ordinary case stores no bytes of its own beyond the copy of the
    /// statement.
    text: String,
    /// `NameId` → span. The indirection buys 4-byte name handles, which is what lets
    /// a list of names share the one list arena with every other kind of list.
    spans: Vec<Span>,
    /// Decoded `X'…'` bytes, which have no representation in the source text.
    blobs: Vec<u8>,

    exprs: Vec<Expression>,
    selects: Vec<Select>,
    tables: Vec<TableRef>,
    items: Vec<SelectItem>,
    orders: Vec<OrderByItem>,
    branches: Vec<CaseBranch>,
    ctes: Vec<Cte>,
    set_ops: Vec<SetOp>,
    assigns: Vec<Assignment>,
    coldefs: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    rows: Vec<ListId<ExprId>>,
    lits: Vec<Literal>,
    floats: Vec<FloatLit>,
    types: Vec<SqlTypeName>,
    stmts: Vec<Statement>,
    inserts: Vec<Insert>,
    updates: Vec<Update>,
    deletes: Vec<Delete>,
    creates: Vec<Create>,
    create_indexes: Vec<CreateIndex>,
    alters: Vec<Alter>,
    drops: Vec<Drop>,
    drop_indexes: Vec<DropIndex>,

    /// Every list, of every element type, as runs of erased ids.
    lists: Vec<u32>,
    /// Lists under construction. Empty between statements.
    scratch: Vec<u32>,

    root: Statement,
    fragment_root: ExprId,
    /// `parameters[i]` is the node for `?` number `i`, so binding a prepared
    /// statement against the arena patches N slots instead of walking the whole tree.
    parameters: Vec<ExprId>,
}

impl Ast {
    /// Empties the arena and loads `sql`, keeping every buffer's capacity.
    pub fn reset(&mut self, sql: &str) {
        self.text.clear();
        self.text.push_str(sql);

        self.spans.clear();
        self.blobs.clear();
        self.exprs.clear();
        self.selects.clear();
        self.tables.clear();
        self.items.clear();
        self.orders.clear();
        self.branches.clear();
        self.ctes.clear();
        self.set_ops.clear();
        self.assigns.clear();
        self.coldefs.clear();
        self.constraints.clear();
        self.rows.clear();
        self.lits.clear();
        self.floats.clear();
        self.types.clear();
        self.stmts.clear();
        self.inserts.clear();
        self.updates.clear();
        self.deletes.clear();
        self.creates.clear();
        self.create_indexes.clear();
        self.alters.clear();
        self.drops.clear();
        self.drop_indexes.clear();

        self.lists.clear();
        // A list left open means a parse returned without closing or aborting one.
        // The arena recovers either way — this is here to catch the bug, not the leak.
        debug_assert!(
            self.scratch.is_empty(),
            "a list was opened and never closed"
        );
        self.scratch.clear();

        self.root = Statement::Begin;
        self.fragment_root = ExprId::NONE;
        self.parameters.clear();
    }

    /// The SQL this statement was parsed from, plus any synthesized text after it.
    #[inline]
    pub fn text(&self) -> &str {
        &self.text
    }

    #[inline]
    pub fn root(&self) -> Statement {
        self.root
    }

    #[inline]
    pub fn set_root(&mut self, root: Statement) {
        self.root = root;
    }

    /// The expression a bare-fragment parse produced.
    ///
    /// A `CHECK` condition read back from the catalog is an expression, not a
    /// statement, so it has no `Statement` root to hang from.
    #[inline]
    pub fn fragment_root(&self) -> ExprId {
        self.fragment_root
    }

    #[inline]
    pub fn set_fragment_root(&mut self, root: ExprId) {
        self.fragment_root = root;
    }

    #[inline]
    pub fn parameters(&self) -> &[ExprId] {
        &self.parameters
    }

    /// How many `?` placeholders the statement has.
    #[inline]
    pub fn parameter_count(&self) -> usize {
        self.parameters.len()
    }

    // ── Reading ──────────────────────────────────────────────────────────────

    /// The text a span covers.
    #[inline]
    pub fn str(&self, span: Span) -> &str {
        &self.text[span.start as usize..(span.start + span.len) as usize]
    }

    /// An identifier's text. The absent name reads as `""`, so a caller comparing a
    /// name against a string needs no separate `None` case.
    #[inline]
    pub fn name(&self, id: NameId) -> &str {
        if id.is_none() {
            return "";
        }
        self.str(self.spans[id.raw() as usize])
    }

    /// An identifier's text, or `None` when it is absent.
    #[inline]
    pub fn opt_name(&self, id: NameId) -> Option<&str> {
        id.get().map(|id| self.name(id))
    }

    /// Whether two names are the same identifier. SQL compares identifiers without
    /// regard to case; the id comparison is the fast path for the common case where
    /// both came from the same spelling.
    #[inline]
    pub fn name_eq(&self, a: NameId, b: NameId) -> bool {
        a == b || self.name(a).eq_ignore_ascii_case(self.name(b))
    }

    #[inline]
    pub fn bytes(&self, span: Span) -> &[u8] {
        &self.blobs[span.start as usize..(span.start + span.len) as usize]
    }

    // ── Writing ──────────────────────────────────────────────────────────────

    /// Records a name that is already part of the statement's text.
    #[inline]
    pub fn push_name_span(&mut self, span: Span) -> NameId {
        let id = NameId::from_raw(self.spans.len() as u32);
        self.spans.push(span);
        id
    }

    /// Records a name whose text is not in the statement — an unescaped identifier —
    /// by appending it to the text buffer first.
    pub fn push_name(&mut self, name: &str) -> NameId {
        let span = self.push_text(name);
        self.push_name_span(span)
    }

    /// Appends text to the buffer and returns its span.
    pub fn push_text(&mut self, text: &str) -> Span {
        let start = self.text.len() as u32;
        self.text.push_str(text);
        Span {
            start,
            len: text.len() as u32,
        }
    }

    /// Appends blob bytes and returns their span.
    pub fn push_bytes(&mut self, bytes: &[u8]) -> Span {
        let start = self.blobs.len() as u32;
        self.blobs.extend_from_slice(bytes);
        Span {
            start,
            len: bytes.len() as u32,
        }
    }

    /// Records a `?` placeholder and returns the expression node standing for it.
    pub fn push_parameter(&mut self) -> ExprId {
        let index = self.parameters.len() as u32;
        let id = self.push_expr(Expression::Parameter(index));
        self.parameters.push(id);
        id
    }
}

/// Generates the push/read pair for one buffer, plus its `Index` impl.
///
/// One macro rather than 24 hand-written triples: they must agree on which buffer an
/// id addresses, and a copy-paste slip between two of them would be a node silently
/// read out of the wrong arena.
macro_rules! arena_buffer {
    ($($id:ident => $field:ident : $node:ty, $push:ident, $get:ident, $set:ident;)*) => {
        impl Ast {$(
            /// Appends a node and returns its id.
            #[inline]
            pub fn $push(&mut self, node: $node) -> $id {
                let id = <$id as ArenaId>::from_raw(self.$field.len() as u32);
                self.$field.push(node);
                id
            }

            /// Reads a node by id.
            #[inline]
            pub fn $get(&self, id: $id) -> $node {
                self.$field[id.raw() as usize]
            }

            /// Overwrites a node in place.
            ///
            /// This is how a rewrite pass works: read the node, change it, write it
            /// back. Nodes it orphans stay in the arena as garbage until `reset`,
            /// which is bounded by the size of the statement and costs nothing.
            #[inline]
            pub fn $set(&mut self, id: $id, node: $node) {
                self.$field[id.raw() as usize] = node;
            }
        )*}

        $(impl core::ops::Index<$id> for Ast {
            type Output = $node;

            #[inline]
            fn index(&self, id: $id) -> &$node {
                &self.$field[id.raw() as usize]
            }
        })*
    };
}

arena_buffer! {
    ExprId => exprs: Expression, push_expr, expr, set_expr;
    SelectId => selects: Select, push_select, select, set_select;
    TableRefId => tables: TableRef, push_table, table, set_table;
    ItemId => items: SelectItem, push_item, item, set_item;
    OrderId => orders: OrderByItem, push_order, order, set_order;
    BranchId => branches: CaseBranch, push_branch, branch, set_branch;
    CteId => ctes: Cte, push_cte, cte, set_cte;
    SetOpId => set_ops: SetOp, push_set_op, set_op, set_set_op;
    AssignId => assigns: Assignment, push_assign, assign, set_assign;
    ColumnDefId => coldefs: ColumnDef, push_coldef, coldef, set_coldef;
    ConstraintId => constraints: TableConstraint, push_constraint, constraint, set_constraint;
    RowId => rows: ListId<ExprId>, push_row, row, set_row;
    LitId => lits: Literal, push_lit, lit, set_lit;
    FloatId => floats: FloatLit, push_float, float, set_float;
    TypeId => types: SqlTypeName, push_type, ty, set_type;
    StmtId => stmts: Statement, push_stmt, stmt, set_stmt;
    InsertId => inserts: Insert, push_insert, insert, set_insert;
    UpdateId => updates: Update, push_update, update, set_update;
    DeleteId => deletes: Delete, push_delete, delete, set_delete;
    CreateId => creates: Create, push_create, create, set_create;
    CreateIndexId => create_indexes: CreateIndex, push_create_index, create_index, set_create_index;
    AlterId => alters: Alter, push_alter, alter, set_alter;
    DropId => drops: Drop, push_drop, drop_stmt, set_drop;
    DropIndexId => drop_indexes: DropIndex, push_drop_index, drop_index, set_drop_index;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Node size is the whole point of the id representation, so it is pinned.
    ///
    /// The owned `ast::Expression` is 48 bytes *and* one `Box` per child; the owned
    /// `ast::Statement` is 336, with ten `Vec`s under it. `Statement` here is 12
    /// rather than 8 because `SelectInto` carries two ids — the one statement that
    /// names a table and a query at the same level.
    #[test]
    fn nodes_are_small_and_copy() {
        assert!(
            std::mem::size_of::<Expression>() <= 16,
            "Expression grew to {} bytes",
            std::mem::size_of::<Expression>()
        );
        assert!(
            std::mem::size_of::<Statement>() <= 12,
            "Statement grew to {} bytes",
            std::mem::size_of::<Statement>()
        );
        assert_eq!(std::mem::size_of::<ExprId>(), 4);
        assert_eq!(std::mem::size_of::<ListId<ExprId>>(), 8);
    }

    /// The case the scratch stack exists for: a list that opens while another is
    /// still open. Both runs must come back contiguous.
    #[test]
    fn nested_lists_do_not_interleave() {
        let mut ast = Ast::default();
        ast.reset("");

        let a = ast.push_expr(Expression::Parameter(0));
        let b = ast.push_expr(Expression::Parameter(1));
        let c = ast.push_expr(Expression::Parameter(2));

        let outer = ast.list_open();
        ast.list_push(a);
        let inner = ast.list_open();
        ast.list_push(b);
        let inner: ListId<ExprId> = ast.list_close(inner);
        ast.list_push(c);
        let outer: ListId<ExprId> = ast.list_close(outer);

        assert_eq!(ast.list(inner).collect::<Vec<_>>(), vec![b]);
        assert_eq!(ast.list(outer).collect::<Vec<_>>(), vec![a, c]);
    }

    /// Three levels, to check the stack unwinds rather than just balancing once.
    #[test]
    fn deeply_nested_lists_stay_separate() {
        let mut ast = Ast::default();
        ast.reset("");
        let ids: Vec<ExprId> = (0..6)
            .map(|i| ast.push_expr(Expression::Parameter(i)))
            .collect();

        let l0 = ast.list_open();
        ast.list_push(ids[0]);
        let l1 = ast.list_open();
        ast.list_push(ids[1]);
        let l2 = ast.list_open();
        ast.list_push(ids[2]);
        ast.list_push(ids[3]);
        let l2: ListId<ExprId> = ast.list_close(l2);
        ast.list_push(ids[4]);
        let l1: ListId<ExprId> = ast.list_close(l1);
        ast.list_push(ids[5]);
        let l0: ListId<ExprId> = ast.list_close(l0);

        assert_eq!(ast.list(l2).collect::<Vec<_>>(), vec![ids[2], ids[3]]);
        assert_eq!(ast.list(l1).collect::<Vec<_>>(), vec![ids[1], ids[4]]);
        assert_eq!(ast.list(l0).collect::<Vec<_>>(), vec![ids[0], ids[5]]);
    }

    #[test]
    fn aborting_a_list_leaves_the_stack_balanced() {
        let mut ast = Ast::default();
        ast.reset("");
        let a = ast.push_expr(Expression::Parameter(0));

        let outer = ast.list_open();
        ast.list_push(a);
        let abandoned = ast.list_open();
        ast.list_push(a);
        ast.list_abort(abandoned);
        let outer: ListId<ExprId> = ast.list_close(outer);

        assert_eq!(ast.list(outer).collect::<Vec<_>>(), vec![a]);
        // `reset` debug-asserts on an unbalanced stack; reaching it proves balance.
        ast.reset("");
    }

    /// Names are spans, so `reset` has to copy the statement's text into the arena
    /// for them to read back out of.
    #[test]
    fn names_read_back_out_of_the_statement_text() {
        let mut ast = Ast::default();
        ast.reset("SELECT alpha FROM beta");
        let alpha = ast.push_name_span(Span { start: 7, len: 5 });
        let beta = ast.push_name_span(Span { start: 18, len: 4 });
        assert_eq!(ast.name(alpha), "alpha");
        assert_eq!(ast.name(beta), "beta");

        // A name that is not in the source appends to the same buffer.
        let synth = ast.push_name("gamma");
        assert_eq!(ast.name(synth), "gamma");
        assert_eq!(
            ast.name(alpha),
            "alpha",
            "appending must not move earlier spans"
        );

        assert_eq!(ast.name(NameId::NONE), "");
        assert_eq!(ast.opt_name(NameId::NONE), None);
    }

    #[test]
    fn names_compare_without_regard_to_case() {
        let mut ast = Ast::default();
        ast.reset("SELECT Users FROM users");
        let upper = ast.push_name_span(Span { start: 7, len: 5 });
        let lower = ast.push_name_span(Span { start: 18, len: 5 });
        assert_ne!(upper, lower, "distinct spellings get distinct ids");
        assert!(ast.name_eq(upper, lower));
        assert!(ast.name_eq(upper, upper));
    }

    /// The reuse claim: after `reset` the buffers are empty but still allocated, so a
    /// second statement of the same shape reaches the allocator for nothing.
    #[test]
    fn reset_keeps_capacity() {
        let mut ast = Ast::default();
        ast.reset("SELECT a FROM t");
        for i in 0..64 {
            let id = ast.push_expr(Expression::Parameter(i));
            ast.list_from([id]);
        }
        let exprs = ast.exprs.capacity();
        let lists = ast.lists.capacity();
        let text = ast.text.capacity();
        assert!(exprs > 0 && lists > 0 && text > 0);

        ast.reset("SELECT b FROM u");
        assert_eq!(ast.exprs.len(), 0);
        assert_eq!(ast.exprs.capacity(), exprs);
        assert_eq!(ast.lists.capacity(), lists);
        assert_eq!(ast.text.capacity(), text);
    }

    /// A rewrite pass overwrites a node in place; ids handed out before it stay valid.
    #[test]
    fn overwriting_a_node_keeps_other_ids_valid() {
        let mut ast = Ast::default();
        ast.reset("");
        let a = ast.push_expr(Expression::Parameter(0));
        let b = ast.push_expr(Expression::Parameter(1));
        let lit = ast.push_lit(Literal::Integer(7));

        ast.set_expr(a, Expression::Literal(lit));
        assert_eq!(ast.expr(a), Expression::Literal(lit));
        assert_eq!(ast.expr(b), Expression::Parameter(1));
    }

    #[test]
    fn parameters_are_recorded_in_order() {
        let mut ast = Ast::default();
        ast.reset("");
        let first = ast.push_parameter();
        let second = ast.push_parameter();
        assert_eq!(ast.parameter_count(), 2);
        assert_eq!(ast.parameters(), &[first, second]);
        assert_eq!(ast.expr(first), Expression::Parameter(0));
        assert_eq!(ast.expr(second), Expression::Parameter(1));
    }
}
