//! # SQL Session Connection and Transaction Coordinator (`connection`)
//!
//! The Connection module acts as the entry-point facade for client sessions. It coordinates statement
//! parsing, query planning/execution, autocommit boundaries, explicit transaction states, and savepoint
//! stacks.
//!
//! ---
//!
//! ## 1. Core Database Connection Concepts
//!
//! ### The Connection Session
//! A connection encapsulates the state of a single database client session. It holds a reference to the
//! `Catalog` registry, tracks active transactional states, manages savepoint lists, and serializes query
//! executions, preventing multi-threaded race conditions within a single connection session.
//!
//! ### Autocommit vs. Explicit Transactions
//! By default, database connections run in **Autocommit Mode**. Every SQL DML or DDL statement (e.g. `INSERT`
//! or `CREATE TABLE`) is wrapped in an implicit transaction:
//! * If the statement executes successfully, the transaction is committed to disk immediately.
//! * If the statement fails (due to constraint violations, type mismatch, or syntax error), the transaction
//!   is automatically rolled back.
//!
//! When the user executes `BEGIN` or `START TRANSACTION`:
//! 1. Autocommit mode is disabled.
//! 2. An explicit transaction is opened.
//! 3. Subsequent statements execute within this transaction's scope.
//! 4. The changes become durable only when `COMMIT` is called. If `ROLLBACK` is called or a session error occurs,
//!    all changes are discarded.
//!
//! ### Savepoint Rollback Stack
//! A **Savepoint** is a sub-transaction checkpoint. It allows a client to roll back a portion of a transaction
//! to a specific named state without aborting the entire transaction. The connection coordinates savepoints by
//! maintaining a stack of named catalog and storage snapshots. Executing `ROLLBACK TO SAVEPOINT name` pops the
//! transaction stack back to that saved state, reverting B-Tree mutations and schema updates made since the
//! savepoint was established.
//!
//! ---
//!
//! ## 2. SQL Statement Processing Pipeline
//!
//! ```text
//!                      Raw SQL Query Text
//!                              |
//!                       Connection::execute()
//!                              v
//!                      +-------+-------+
//!                      |   SQL Parser  | <---+ Builds the logical AST
//!                      +-------+-------+
//!                              | AST
//!                              v
//!                      +-------+-------+
//!                      |   Optimizer   | <---+ Constant folding & logical folds
//!                      +-------+-------+
//!                              | Optimized AST
//!                              v
//!                      +-------+-------+
//!                      |    Planner    | <---+ Resolves bindings against Schema
//!                      +-------+-------+
//!                              | Physical Plan
//!                              v
//!                      +-------+-------+
//!                      |   Executor    | <---+ Volcano-style streaming evaluation
//!                      +-------+-------+
//!                              | Results
//!                              v
//!                      QueryResult / Rowset
//! ```
//!
//! ---
//!
//! ## 3. Transaction State Transitions
//!
//! ```text
//!                      +-----------------------+
//!                      |    1. Autocommit      |
//!                      |  (Implicit Tx per Stmt) |
//!                      +----------+------------+
//!                                 |
//!                         BEGIN / START TX
//!                                 |
//!                                 v
//!                      +----------+------------+
//!                      |  2. Explicit Active   | <---+ SAVEPOINT name (adds to stack)
//!                      +----+------------+-----+
//!                           |            |
//!                         COMMIT      ROLLBACK or ROLLBACK TO SAVEPOINT
//!                           |            |
//!                           v            v
//!                       Commit Tx    Rollback Tx / Revert Stack
//!                           \            /
//!                            \          /
//!                             v        v
//!                         Back to Autocommit
//! ```
//!
//! ---
//!
//! ## 4. Connection Invariants
//!
//! 1. **Autocommit Isolation**: If an error occurs during an autocommit statement, the connection must ensure
//!    the pager transaction is aborted immediately, preventing partial writes from leaking.
//! 2. **Savepoint Stack LIFO**: Savepoint stacks must follow Last-In, First-Out (LIFO) behavior. Releasing or
//!    rolling back to a savepoint implicitly releases all savepoints created after it.
//! 3. **Concurrency Thread Safety**: The connection session coordinates transaction states sequentially.
//!    Multiple threads must not mutate connection transaction boundaries concurrently without proper external locking.
//!

use crate::error::{HematiteError, Result};
use crate::parser::ast::{
    ColumnDefinition, CreateStatement, Expression, InsertSource, InsertStatement,
    SelectIntoStatement, SelectStatement, Statement, TableReference,
};
use crate::parser::{parse_sql_statement, SqlTypeName};
use crate::query::lowering::raise_literal_value;
use crate::query::metadata as query_metadata;
use crate::query::validation::{source_column_names, validate_statement};
use crate::query::{
    Catalog, CatalogEngine, ExecutionContext, JournalMode, QueryCatalogSnapshot, QueryExecutor,
    QueryPlanner, QueryResult, Schema, Value,
};
use crate::sql::result::ExecutedStatement;
use crate::sql::script::{split_script_tokens, ScriptIter};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug, Clone)]
struct ConnectionTransaction {
    snapshot: QueryCatalogSnapshot,
    savepoints: Vec<SavepointState>,
}

#[derive(Debug, Clone)]
struct SavepointState {
    name: String,
    snapshot: QueryCatalogSnapshot,
}

#[derive(Debug)]
struct ImplicitMutation {
    snapshot: Option<QueryCatalogSnapshot>,
}

impl ImplicitMutation {
    fn begin(connection: &mut Connection) -> Result<Self> {
        if connection.transaction.is_some() {
            return Ok(Self { snapshot: None });
        }

        let mut catalog_guard = connection.lock_catalog()?;
        catalog_guard.begin_transaction()?;
        if let Err(err) = catalog_guard.refresh_from_storage() {
            let _ = catalog_guard.rollback_transaction();
            return Err(err);
        }
        let snapshot = match catalog_guard.transaction_entry_snapshot() {
            Ok(snapshot) => snapshot,
            Err(err) => {
                let _ = catalog_guard.rollback_transaction();
                return Err(err);
            }
        };
        Ok(Self {
            snapshot: Some(snapshot),
        })
    }

    fn rollback(mut self, connection: &mut Connection) -> Result<()> {
        if let Some(snapshot) = self.snapshot.take() {
            let mut catalog_guard = connection.lock_catalog()?;
            let _ = catalog_guard.rollback_transaction();
            catalog_guard.restore_snapshot(snapshot)?;
        }
        Ok(())
    }

    fn commit(mut self, connection: &mut Connection) -> Result<()> {
        let Some(snapshot) = self.snapshot.take() else {
            return Ok(());
        };

        let mut catalog_guard = connection.lock_catalog()?;
        match catalog_guard.commit_transaction() {
            Ok(()) => Ok(()),
            Err(err) => {
                let _ = catalog_guard.rollback_transaction();
                catalog_guard.restore_snapshot(snapshot)?;
                Err(err)
            }
        }
    }
}

#[derive(Debug)]
pub struct Connection {
    catalog: Arc<Mutex<Catalog>>,
    transaction: Option<ConnectionTransaction>,
}

impl Connection {
    const SELECT_INTO_ROWID_COLUMN: &'static str = "__hematite_select_into_rowid";

    fn empty_result() -> QueryResult {
        QueryResult {
            affected_rows: 0,
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    fn mutation_result(affected_rows: usize) -> QueryResult {
        QueryResult {
            affected_rows,
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    fn select_into_synthetic_pk_name(column_names: &[String]) -> String {
        let mut candidate = Self::SELECT_INTO_ROWID_COLUMN.to_string();
        let used = column_names
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect::<HashSet<_>>();
        let mut suffix = 2usize;
        while used.contains(&candidate.to_ascii_lowercase()) {
            candidate = format!("{}_{}", Self::SELECT_INTO_ROWID_COLUMN, suffix);
            suffix += 1;
        }
        candidate
    }

    fn select_into_column_names(result: &QueryResult) -> Vec<String> {
        let mut used = HashSet::new();
        let mut names = Vec::with_capacity(result.columns.len());
        for (index, name) in result.columns.iter().enumerate() {
            let mut candidate = if name.trim().is_empty() {
                format!("column{}", index + 1)
            } else {
                name.clone()
            };
            let base = candidate.clone();
            let mut suffix = 2usize;
            while used.contains(&candidate.to_ascii_lowercase())
                || candidate.eq_ignore_ascii_case(Self::SELECT_INTO_ROWID_COLUMN)
            {
                candidate = format!("{base}_{suffix}");
                suffix += 1;
            }
            used.insert(candidate.to_ascii_lowercase());
            names.push(candidate);
        }
        names
    }

    fn infer_select_into_type(
        column_name: &str,
        values: &[Vec<Value>],
        index: usize,
    ) -> Result<SqlTypeName> {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum NumericKind {
            Int,
            Int64,
            UInt,
            UInt64,
            Float32,
            Float,
            Decimal,
        }

        #[derive(Debug, Clone)]
        enum InferredKind {
            Numeric(NumericKind),
            String,
            Boolean,
            Blob,
        }

        impl InferredKind {
            fn absorb(self, value: &Value, column_name: &str) -> Result<Self> {
                use InferredKind::*;
                use NumericKind::*;
                match (self, value) {
                    (kind, Value::Null) => Ok(kind),
                    (Numeric(Int), Value::Integer(_)) => Ok(Numeric(Int)),
                    (Numeric(Int), Value::BigInt(_))
                    | (Numeric(Int64), Value::Integer(_))
                    | (Numeric(Int64), Value::BigInt(_)) => Ok(Numeric(Int64)),
                    (Numeric(UInt), Value::UInteger(_)) => Ok(Numeric(UInt)),
                    (Numeric(UInt), Value::UBigInt(_))
                    | (Numeric(UInt64), Value::UInteger(_))
                    | (Numeric(UInt64), Value::UBigInt(_)) => Ok(Numeric(UInt64)),
                    (Numeric(Int), Value::UInteger(_))
                    | (Numeric(Int64), Value::UInteger(_))
                    | (Numeric(UInt), Value::Integer(_))
                    | (Numeric(UInt), Value::BigInt(_))
                    | (Numeric(UInt64), Value::Integer(_))
                    | (Numeric(UInt64), Value::BigInt(_))
                    | (Numeric(Int64), Value::UBigInt(_))
                    | (Numeric(Int), Value::UBigInt(_))
                    => Ok(Numeric(Decimal)),
                    (Numeric(Int), Value::Float32(_))
                    | (Numeric(Int64), Value::Float32(_))
                    | (Numeric(UInt), Value::Float32(_))
                    | (Numeric(UInt64), Value::Float32(_))
                    | (Numeric(Float32), Value::Integer(_))
                    | (Numeric(Float32), Value::BigInt(_))
                    | (Numeric(Float32), Value::UInteger(_))
                    | (Numeric(Float32), Value::UBigInt(_))
                    | (Numeric(Float32), Value::Float32(_)) => Ok(Numeric(Float32)),
                    (Numeric(Int), Value::Float(_))
                    | (Numeric(Int64), Value::Float(_))
                    | (Numeric(UInt), Value::Float(_))
                    | (Numeric(UInt64), Value::Float(_))
                    | (Numeric(Float32), Value::Float(_))
                    | (Numeric(Float), Value::Integer(_))
                    | (Numeric(Float), Value::BigInt(_))
                    | (Numeric(Float), Value::UInteger(_))
                    | (Numeric(Float), Value::UBigInt(_))
                    | (Numeric(Float), Value::Float32(_))
                    | (Numeric(Float), Value::Float(_)) => Ok(Numeric(Float)),
                    (Numeric(Int), Value::Decimal(_))
                    | (Numeric(Int64), Value::Decimal(_))
                    | (Numeric(UInt), Value::Decimal(_))
                    | (Numeric(UInt64), Value::Decimal(_))
                    | (Numeric(Float32), Value::Decimal(_))
                    | (Numeric(Float), Value::Decimal(_))
                    | (Numeric(Decimal), Value::Integer(_))
                    | (Numeric(Decimal), Value::BigInt(_))
                    | (Numeric(Decimal), Value::UInteger(_))
                    | (Numeric(Decimal), Value::UBigInt(_))
                    | (Numeric(Decimal), Value::Float32(_))
                    | (Numeric(Decimal), Value::Float(_))
                    | (Numeric(Decimal), Value::Decimal(_)) => Ok(Numeric(Decimal)),
                    (String, Value::Text(_)) => Ok(String),
                    (Blob, Value::Blob(_)) => Ok(Blob),
                    (Blob, Value::Text(_)) => Ok(Blob),
                    (Boolean, Value::Boolean(_)) => Ok(Boolean),
                    (left, right) => Err(HematiteError::ParseError(format!(
                        "SELECT INTO cannot infer a stable column type for '{}': {:?} cannot be combined with {:?}",
                        column_name, left, right
                    ))),
                }
            }

            fn from_value(value: &Value, _column_name: &str) -> Result<Option<Self>> {
                use InferredKind::*;
                use NumericKind::*;
                let inferred = match value {
                    Value::Null => return Ok(None),
                    Value::Integer(_) => Numeric(Int),
                    Value::BigInt(_) => Numeric(Int64),
                    Value::UInteger(_) => Numeric(UInt),
                    Value::UBigInt(_) => Numeric(UInt64),
                    Value::Float32(_) => Numeric(Float32),
                    Value::Float(_) => Numeric(Float),
                    Value::Decimal(_) => Numeric(Decimal),
                    Value::Text(_) => String,
                    Value::Boolean(_) => Boolean,
                    Value::Blob(_) => Blob,
                };
                Ok(Some(inferred))
            }

            fn into_sql_type(self) -> SqlTypeName {
                match self {
                    InferredKind::Numeric(NumericKind::Int) => SqlTypeName::Int,
                    InferredKind::Numeric(NumericKind::Int64) => SqlTypeName::Int64,
                    InferredKind::Numeric(NumericKind::UInt) => SqlTypeName::UInt,
                    InferredKind::Numeric(NumericKind::UInt64) => SqlTypeName::UInt64,
                    InferredKind::Numeric(NumericKind::Float32) => SqlTypeName::Float32,
                    InferredKind::Numeric(NumericKind::Float) => SqlTypeName::Float,
                    InferredKind::Numeric(NumericKind::Decimal) => SqlTypeName::Decimal {
                        precision: None,
                        scale: None,
                    },
                    InferredKind::String => SqlTypeName::Text,
                    InferredKind::Boolean => SqlTypeName::Boolean,
                    InferredKind::Blob => SqlTypeName::Blob,
                }
            }
        }

        let mut inferred = None;
        for row in values {
            let Some(value) = row.get(index) else {
                return Err(HematiteError::InternalError(format!(
                    "SELECT INTO result row is missing projected column {}",
                    index
                )));
            };

            inferred = match (inferred, InferredKind::from_value(value, column_name)?) {
                (None, None) => None,
                (None, Some(kind)) => Some(kind),
                (Some(kind), None) => Some(kind),
                (Some(kind), Some(_)) => Some(kind.absorb(value, column_name)?),
            };
        }

        Ok(inferred
            .map(InferredKind::into_sql_type)
            .unwrap_or(SqlTypeName::Text))
    }

    fn infer_select_into_columns(result: &QueryResult) -> Result<Vec<ColumnDefinition>> {
        let column_names = Self::select_into_column_names(result);
        column_names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                Ok(ColumnDefinition {
                    name: name.clone(),
                    data_type: Self::infer_select_into_type(name, &result.rows, index)?,
                    character_set: None,
                    collation: None,
                    nullable: true,
                    primary_key: false,
                    auto_increment: false,
                    unique: false,
                    default_value: None,
                    check_constraint: None,
                    references: None,
                })
            })
            .collect()
    }

    fn lock_catalog(&self) -> Result<MutexGuard<'_, Catalog>> {
        self.catalog.lock().map_err(|_| {
            HematiteError::InternalError("SQL connection catalog mutex is poisoned".to_string())
        })
    }

    fn refresh_catalog_view_if_needed(&mut self) -> Result<()> {
        if self.transaction.is_some() {
            return Ok(());
        }

        let mut catalog_guard = self.lock_catalog()?;
        catalog_guard.refresh_from_storage()
    }

    pub fn new(database_path: &str) -> Result<Self> {
        let catalog = Catalog::open_or_create(database_path)?;
        Ok(Self {
            catalog: Arc::new(Mutex::new(catalog)),
            transaction: None,
        })
    }

    /// Open a file-based connection with an explicit page-cache capacity (in pages).
    pub fn with_cache_capacity(database_path: &str, cache_capacity: usize) -> Result<Self> {
        let catalog = Catalog::open_or_create_with_cache_capacity(database_path, cache_capacity)?;
        Ok(Self {
            catalog: Arc::new(Mutex::new(catalog)),
            transaction: None,
        })
    }

    pub fn new_in_memory() -> Result<Self> {
        let catalog = Catalog::open_in_memory()?;
        Ok(Self {
            catalog: Arc::new(Mutex::new(catalog)),
            transaction: None,
        })
    }

    fn parse_statement(sql: &str) -> Result<crate::parser::ast::Statement> {
        parse_sql_statement(sql)
    }

    fn normalize_statement(mut statement: Statement, schema: &Schema) -> Result<Statement> {
        Self::rewrite_select_aliases_in_statement(&mut statement, schema)?;
        Ok(statement)
    }

    fn rewrite_select_aliases_in_statement(
        statement: &mut Statement,
        schema: &Schema,
    ) -> Result<()> {
        match statement {
            Statement::Explain(explain) => {
                Self::rewrite_select_aliases_in_statement(&mut explain.statement, schema)
            }
            Statement::Select(select) => Self::rewrite_select_aliases_in_select(select, schema),
            Statement::Insert(insert) => {
                if let InsertSource::Select(select) = &mut insert.source {
                    Self::rewrite_select_aliases_in_select(select, schema)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn rewrite_select_aliases_in_select(
        select: &mut SelectStatement,
        schema: &Schema,
    ) -> Result<()> {
        for cte in &mut select.with_clause {
            Self::rewrite_select_aliases_in_select(&mut cte.query, schema)?;
        }

        Self::rewrite_select_aliases_in_table_reference(&mut select.from, schema)?;

        for item in &mut select.columns {
            match item {
                crate::parser::ast::SelectItem::Expression(expr) => {
                    Self::rewrite_nested_select_aliases_in_expression(expr, schema)?;
                }
                crate::parser::ast::SelectItem::Wildcard
                | crate::parser::ast::SelectItem::Column(_)
                | crate::parser::ast::SelectItem::CountAll
                | crate::parser::ast::SelectItem::Aggregate { .. } => {}
            }
        }

        let alias_map = Self::where_alias_map(select);
        let source_columns = source_column_names(select, schema)?
            .into_iter()
            .collect::<HashSet<_>>();

        if let Some(where_clause) = &mut select.where_clause {
            for condition in &mut where_clause.conditions {
                Self::rewrite_where_aliases_in_expression(
                    condition,
                    &alias_map,
                    &source_columns,
                    &mut HashSet::new(),
                )?;
            }
        }

        for expr in &mut select.group_by {
            Self::rewrite_nested_select_aliases_in_expression(expr, schema)?;
        }

        if let Some(having_clause) = &mut select.having_clause {
            for condition in &mut having_clause.conditions {
                Self::rewrite_nested_select_aliases_in_expression(condition, schema)?;
            }
        }

        if let Some(set_operation) = &mut select.set_operation {
            Self::rewrite_select_aliases_in_select(&mut set_operation.right, schema)?;
        }

        Ok(())
    }

    fn rewrite_select_aliases_in_table_reference(
        from: &mut TableReference,
        schema: &Schema,
    ) -> Result<()> {
        match from {
            TableReference::Derived { subquery, .. } => {
                Self::rewrite_select_aliases_in_select(subquery, schema)
            }
            TableReference::CrossJoin(left, right) => {
                Self::rewrite_select_aliases_in_table_reference(left, schema)?;
                Self::rewrite_select_aliases_in_table_reference(right, schema)
            }
            TableReference::InnerJoin { left, right, on }
            | TableReference::LeftJoin { left, right, on }
            | TableReference::RightJoin { left, right, on }
            | TableReference::FullOuterJoin { left, right, on } => {
                Self::rewrite_select_aliases_in_table_reference(left, schema)?;
                Self::rewrite_select_aliases_in_table_reference(right, schema)?;
                Self::rewrite_nested_select_aliases_in_expression(on, schema)
            }
            TableReference::Table(_, _) => Ok(()),
        }
    }

    fn rewrite_nested_select_aliases_in_expression(
        expr: &mut Expression,
        schema: &Schema,
    ) -> Result<()> {
        let mut rewrite = |subquery: &mut SelectStatement| {
            Self::rewrite_select_aliases_in_select(subquery, schema)
        };
        Self::rewrite_nested_subqueries_in_expression(expr, &mut rewrite)
    }

    fn rewrite_nested_subqueries_in_expression<F>(
        expr: &mut Expression,
        on_subquery: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&mut SelectStatement) -> Result<()>,
    {
        match expr {
            Expression::ScalarSubquery(subquery) => on_subquery(subquery),
            Expression::Cast { expr, .. }
            | Expression::UnaryMinus(expr)
            | Expression::UnaryNot(expr)
            | Expression::NullCheck { expr, .. } => {
                Self::rewrite_nested_subqueries_in_expression(expr, on_subquery)
            }
            Expression::Case {
                branches,
                else_expr,
            } => {
                for branch in branches {
                    Self::rewrite_nested_subqueries_in_expression(
                        &mut branch.condition,
                        on_subquery,
                    )?;
                    Self::rewrite_nested_subqueries_in_expression(&mut branch.result, on_subquery)?;
                }
                if let Some(else_expr) = else_expr {
                    Self::rewrite_nested_subqueries_in_expression(else_expr, on_subquery)?;
                }
                Ok(())
            }
            Expression::ScalarFunctionCall { args, .. } => {
                for arg in args {
                    Self::rewrite_nested_subqueries_in_expression(arg, on_subquery)?;
                }
                Ok(())
            }
            Expression::Binary { left, right, .. }
            | Expression::Comparison { left, right, .. }
            | Expression::Logical { left, right, .. } => {
                Self::rewrite_nested_subqueries_in_expression(left, on_subquery)?;
                Self::rewrite_nested_subqueries_in_expression(right, on_subquery)
            }
            Expression::InList { expr, values, .. } => {
                Self::rewrite_nested_subqueries_in_expression(expr, on_subquery)?;
                for value in values {
                    Self::rewrite_nested_subqueries_in_expression(value, on_subquery)?;
                }
                Ok(())
            }
            Expression::InSubquery { expr, subquery, .. } => {
                Self::rewrite_nested_subqueries_in_expression(expr, on_subquery)?;
                on_subquery(subquery)
            }
            Expression::Between {
                expr, lower, upper, ..
            } => {
                Self::rewrite_nested_subqueries_in_expression(expr, on_subquery)?;
                Self::rewrite_nested_subqueries_in_expression(lower, on_subquery)?;
                Self::rewrite_nested_subqueries_in_expression(upper, on_subquery)
            }
            Expression::Like { expr, pattern, .. } => {
                Self::rewrite_nested_subqueries_in_expression(expr, on_subquery)?;
                Self::rewrite_nested_subqueries_in_expression(pattern, on_subquery)
            }
            Expression::Exists { subquery, .. } => on_subquery(subquery),
            Expression::AggregateCall { .. }
            | Expression::Column(_)
            | Expression::Literal(_)
            | Expression::Parameter(_) => Ok(()),
        }
    }

    fn where_alias_map(select: &SelectStatement) -> HashMap<String, Expression> {
        let mut aliases = HashMap::new();
        for (index, alias) in select.column_aliases.iter().enumerate() {
            let Some(alias) = alias.as_ref() else {
                continue;
            };
            let Some(item) = select.columns.get(index) else {
                continue;
            };

            let replacement = match item {
                crate::parser::ast::SelectItem::Column(name) => Expression::Column(name.clone()),
                crate::parser::ast::SelectItem::Expression(expr) => expr.clone(),
                _ => continue,
            };
            aliases.insert(alias.clone(), replacement);
        }
        aliases
    }

    fn rewrite_where_aliases_in_expression(
        expr: &mut Expression,
        aliases: &HashMap<String, Expression>,
        source_columns: &HashSet<String>,
        active_aliases: &mut HashSet<String>,
    ) -> Result<()> {
        match expr {
            Expression::Column(name) => {
                if SelectStatement::split_column_reference(name).0.is_some()
                    || source_columns.contains(name)
                {
                    return Ok(());
                }

                let Some(replacement) = aliases.get(name).cloned() else {
                    return Ok(());
                };

                if !active_aliases.insert(name.clone()) {
                    return Err(HematiteError::ParseError(format!(
                        "Select alias '{}' is recursively defined",
                        name
                    )));
                }

                let mut replacement = replacement;
                Self::rewrite_where_aliases_in_expression(
                    &mut replacement,
                    aliases,
                    source_columns,
                    active_aliases,
                )?;
                active_aliases.remove(name);
                *expr = replacement;
            }
            Expression::Cast { expr, .. }
            | Expression::UnaryMinus(expr)
            | Expression::UnaryNot(expr)
            | Expression::NullCheck { expr, .. } => {
                Self::rewrite_where_aliases_in_expression(
                    expr,
                    aliases,
                    source_columns,
                    active_aliases,
                )?;
            }
            Expression::Case {
                branches,
                else_expr,
            } => {
                for branch in branches {
                    Self::rewrite_where_aliases_in_expression(
                        &mut branch.condition,
                        aliases,
                        source_columns,
                        active_aliases,
                    )?;
                    Self::rewrite_where_aliases_in_expression(
                        &mut branch.result,
                        aliases,
                        source_columns,
                        active_aliases,
                    )?;
                }
                if let Some(else_expr) = else_expr {
                    Self::rewrite_where_aliases_in_expression(
                        else_expr,
                        aliases,
                        source_columns,
                        active_aliases,
                    )?;
                }
            }
            Expression::ScalarFunctionCall { args, .. } => {
                for arg in args {
                    Self::rewrite_where_aliases_in_expression(
                        arg,
                        aliases,
                        source_columns,
                        active_aliases,
                    )?;
                }
            }
            Expression::Binary { left, right, .. }
            | Expression::Comparison { left, right, .. }
            | Expression::Logical { left, right, .. } => {
                Self::rewrite_where_aliases_in_expression(
                    left,
                    aliases,
                    source_columns,
                    active_aliases,
                )?;
                Self::rewrite_where_aliases_in_expression(
                    right,
                    aliases,
                    source_columns,
                    active_aliases,
                )?;
            }
            Expression::InList { expr, values, .. } => {
                Self::rewrite_where_aliases_in_expression(
                    expr,
                    aliases,
                    source_columns,
                    active_aliases,
                )?;
                for value in values {
                    Self::rewrite_where_aliases_in_expression(
                        value,
                        aliases,
                        source_columns,
                        active_aliases,
                    )?;
                }
            }
            Expression::Between {
                expr, lower, upper, ..
            } => {
                Self::rewrite_where_aliases_in_expression(
                    expr,
                    aliases,
                    source_columns,
                    active_aliases,
                )?;
                Self::rewrite_where_aliases_in_expression(
                    lower,
                    aliases,
                    source_columns,
                    active_aliases,
                )?;
                Self::rewrite_where_aliases_in_expression(
                    upper,
                    aliases,
                    source_columns,
                    active_aliases,
                )?;
            }
            Expression::Like { expr, pattern, .. } => {
                Self::rewrite_where_aliases_in_expression(
                    expr,
                    aliases,
                    source_columns,
                    active_aliases,
                )?;
                Self::rewrite_where_aliases_in_expression(
                    pattern,
                    aliases,
                    source_columns,
                    active_aliases,
                )?;
            }
            Expression::AggregateCall { .. }
            | Expression::ScalarSubquery(_)
            | Expression::InSubquery { .. }
            | Expression::Exists { .. }
            | Expression::Literal(_)
            | Expression::Parameter(_) => {}
        }
        Ok(())
    }

    pub(crate) fn execute_statement(
        &mut self,
        statement: crate::parser::ast::Statement,
    ) -> Result<QueryResult> {
        match statement {
            crate::parser::ast::Statement::Begin => {
                self.begin_active_transaction()?;
                return Ok(Self::empty_result());
            }
            crate::parser::ast::Statement::Commit => {
                self.commit_active_transaction()?;
                return Ok(Self::empty_result());
            }
            crate::parser::ast::Statement::Rollback => {
                self.rollback_active_transaction()?;
                return Ok(Self::empty_result());
            }
            crate::parser::ast::Statement::Savepoint(name) => {
                self.create_savepoint(&name)?;
                return Ok(Self::empty_result());
            }
            crate::parser::ast::Statement::RollbackToSavepoint(name) => {
                self.rollback_to_savepoint(&name)?;
                return Ok(Self::empty_result());
            }
            crate::parser::ast::Statement::ReleaseSavepoint(name) => {
                self.release_savepoint(&name)?;
                return Ok(Self::empty_result());
            }
            crate::parser::ast::Statement::Explain(explain) => {
                return self.execute_explain_statement(*explain.statement);
            }
            crate::parser::ast::Statement::Describe(describe) => {
                return self.execute_describe_statement(&describe.table);
            }
            crate::parser::ast::Statement::ShowTables => {
                return self.execute_show_tables_statement();
            }
            crate::parser::ast::Statement::ShowIndexes(table_name) => {
                return self.execute_show_indexes_statement(table_name.as_deref());
            }
            crate::parser::ast::Statement::ShowCreateTable(table_name) => {
                return self.execute_show_create_table_statement(&table_name);
            }
            crate::parser::ast::Statement::SelectInto(select_into) => {
                return self.execute_select_into_statement(select_into);
            }
            _ => {}
        }

        self.refresh_catalog_view_if_needed()?;

        if statement.is_read_only() {
            return self.execute_read_statement(statement);
        }

        self.execute_mutating_statement(statement)
    }

    fn execute_explain_statement(
        &mut self,
        statement: crate::parser::ast::Statement,
    ) -> Result<QueryResult> {
        let statement = match statement {
            Statement::SelectInto(select_into) => Statement::Select(select_into.query),
            other => other,
        };
        let (schema, table_row_counts) = self.read_planning_state()?;
        let planner = QueryPlanner::new(schema).with_table_row_counts(table_row_counts);
        let plan = planner.plan(statement)?;
        Ok(QueryResult {
            affected_rows: 0,
            columns: vec!["kind".to_string(), "detail".to_string()],
            rows: vec![
                vec![
                    Value::Text("node".to_string()),
                    Value::Text(format!("{:?}", plan.node)),
                ],
                vec![
                    Value::Text("estimated_cost".to_string()),
                    Value::Text(format!("{}", plan.estimated_cost)),
                ],
            ],
        })
    }

    fn execute_describe_statement(&mut self, table_name: &str) -> Result<QueryResult> {
        let catalog_guard = self.lock_catalog()?;
        query_metadata::describe_table(&catalog_guard, table_name)
    }

    fn execute_show_tables_statement(&mut self) -> Result<QueryResult> {
        let catalog_guard = self.lock_catalog()?;
        query_metadata::show_tables(&catalog_guard)
    }

    fn execute_show_indexes_statement(&mut self, table_name: Option<&str>) -> Result<QueryResult> {
        let catalog_guard = self.lock_catalog()?;
        query_metadata::show_indexes(&catalog_guard, table_name)
    }

    fn execute_show_create_table_statement(&mut self, table_name: &str) -> Result<QueryResult> {
        let catalog_guard = self.lock_catalog()?;
        query_metadata::show_create_table(&catalog_guard, table_name)
    }

    fn execute_select_into_statement(
        &mut self,
        statement: SelectIntoStatement,
    ) -> Result<QueryResult> {
        let (schema, _) = self.read_planning_state()?;
        if schema.get_table_by_name(&statement.table).is_some() {
            return Err(HematiteError::ParseError(format!(
                "Table '{}' already exists",
                statement.table
            )));
        }

        let normalized_query =
            match Self::normalize_statement(Statement::Select(statement.query.clone()), &schema)? {
                Statement::Select(select) => select,
                _ => unreachable!("normalized SELECT INTO query should remain a select"),
            };
        validate_statement(&Statement::Select(normalized_query), &schema)?;

        let query_result =
            self.execute_read_statement(Statement::Select(statement.query.clone()))?;
        let projected_columns = Self::infer_select_into_columns(&query_result)?;
        let insert_columns = projected_columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let synthetic_pk = Self::select_into_synthetic_pk_name(&insert_columns);

        let mut create_columns = Vec::with_capacity(projected_columns.len() + 1);
        create_columns.push(ColumnDefinition {
            name: synthetic_pk,
            data_type: SqlTypeName::Int,
            character_set: None,
            collation: None,
            nullable: false,
            primary_key: true,
            auto_increment: true,
            unique: false,
            default_value: None,
            check_constraint: None,
            references: None,
        });
        create_columns.extend(projected_columns);

        let mut implicit_mutation = Some(ImplicitMutation::begin(self)?);
        let result: Result<QueryResult> = (|| {
            self.execute_mutating_statement_in_scope(
                Statement::Create(CreateStatement {
                    table: statement.table.clone(),
                    columns: create_columns,
                    constraints: Vec::new(),
                    if_not_exists: false,
                }),
                false,
            )?;

            let insert_result = self.execute_mutating_statement_in_scope(
                Statement::Insert(InsertStatement {
                    table: statement.table.clone(),
                    columns: insert_columns,
                    source: InsertSource::Select(Box::new(statement.query)),
                    on_duplicate: None,
                }),
                false,
            )?;

            Ok(Self::mutation_result(insert_result.affected_rows))
        })();

        match result {
            Ok(result) => {
                implicit_mutation
                    .take()
                    .expect("implicit mutation should be present")
                    .commit(self)?;
                Ok(result)
            }
            Err(err) => {
                implicit_mutation
                    .take()
                    .expect("implicit mutation should be present")
                    .rollback(self)?;
                Err(err)
            }
        }
    }

    pub(crate) fn execute_statement_result(
        &mut self,
        statement: crate::parser::ast::Statement,
    ) -> Result<ExecutedStatement> {
        self.execute_statement(statement)
            .map(ExecutedStatement::from_query_result)
    }

    fn execute_read_statement(
        &mut self,
        statement: crate::parser::ast::Statement,
    ) -> Result<QueryResult> {
        let (schema, mut executor) = self.plan_executor(statement)?;

        let result = {
            let mut catalog_guard = self.lock_catalog()?;
            catalog_guard.with_read_engine(|engine| {
                let mut ctx = ExecutionContext::for_read(&schema, engine);
                executor.execute(&mut ctx)
            })?
        };

        Ok(result)
    }

    fn execute_mutating_statement(
        &mut self,
        statement: crate::parser::ast::Statement,
    ) -> Result<QueryResult> {
        self.execute_mutating_statement_in_scope(statement, true)
    }

    fn execute_mutating_statement_in_scope(
        &mut self,
        statement: crate::parser::ast::Statement,
        use_implicit_mutation: bool,
    ) -> Result<QueryResult> {
        let persists_schema = statement.mutates_schema();
        let (schema, mut executor) = self.plan_executor(statement)?;
        let mut implicit_mutation = if use_implicit_mutation {
            Some(ImplicitMutation::begin(self)?)
        } else {
            None
        };

        let execution_result = {
            let mut catalog_guard = self.lock_catalog()?;
            catalog_guard.with_engine(|engine| {
                let mut ctx = ExecutionContext::for_mutation(&schema, engine);
                let result = executor.execute(&mut ctx)?;
                Ok((result, ctx.catalog))
            })
        };

        match execution_result {
            Ok((result, updated_schema)) => {
                if persists_schema {
                    let mut catalog_guard = self.lock_catalog()?;
                    if let Err(err) = catalog_guard.replace_schema(updated_schema) {
                        drop(catalog_guard);
                        if let Some(implicit_mutation) = implicit_mutation.take() {
                            implicit_mutation.rollback(self)?;
                        }
                        return Err(err);
                    }
                }

                if let Some(implicit_mutation) = implicit_mutation.take() {
                    implicit_mutation.commit(self)?;
                }

                Ok(result)
            }
            Err(err) => {
                if let Some(implicit_mutation) = implicit_mutation.take() {
                    implicit_mutation.rollback(self)?;
                }
                Err(err)
            }
        }
    }

    fn plan_executor(
        &self,
        statement: crate::parser::ast::Statement,
    ) -> Result<(Schema, Box<dyn QueryExecutor>)> {
        let (schema, table_row_counts) = self.read_planning_state()?;
        let statement = Self::normalize_statement(statement, &schema)?;
        let planner = QueryPlanner::new(schema.clone()).with_table_row_counts(table_row_counts);
        let plan = planner.plan(statement)?;
        Ok((schema, plan.into_executor()))
    }

    fn read_planning_state(&self) -> Result<(Schema, HashMap<String, usize>)> {
        let mut catalog_guard = self.lock_catalog()?;
        let schema = catalog_guard.clone_schema();
        let table_row_counts =
            catalog_guard.with_engine(|engine| Ok(Self::collect_table_row_counts(engine)))?;
        Ok((schema, table_row_counts))
    }

    fn collect_table_row_counts(engine: &CatalogEngine) -> HashMap<String, usize> {
        engine
            .get_table_metadata()
            .iter()
            .map(|(name, metadata)| (name.to_string(), metadata.row_count as usize))
            .collect()
    }

    pub fn close(&mut self) -> Result<()> {
        if self.transaction.is_some() {
            return Err(HematiteError::InternalError(
                "Cannot close connection with an active transaction".to_string(),
            ));
        }
        let mut catalog_guard = self.lock_catalog()?;
        if catalog_guard.has_pending_changes()? {
            catalog_guard.flush()?;
        }
        Ok(())
    }

    pub fn journal_mode(&self) -> Result<JournalMode> {
        let catalog_guard = self.lock_catalog()?;
        catalog_guard.journal_mode()
    }

    pub fn storage_counters(&self) -> Result<crate::storage::StatsSnapshot> {
        let catalog_guard = self.lock_catalog()?;
        catalog_guard.storage_counters()
    }

    pub fn set_journal_mode(&mut self, journal_mode: JournalMode) -> Result<()> {
        let mut catalog_guard = self.lock_catalog()?;
        catalog_guard.set_journal_mode(journal_mode)
    }

    pub fn checkpoint_wal(&mut self) -> Result<()> {
        let mut catalog_guard = self.lock_catalog()?;
        catalog_guard.checkpoint_wal()
    }

    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        self.execute_statement(Self::parse_statement(sql)?)
    }

    pub fn execute_result(&mut self, sql: &str) -> Result<ExecutedStatement> {
        self.execute(sql).map(ExecutedStatement::from_query_result)
    }

    pub fn iter_script<'a>(&'a mut self, sql: &'a str) -> Result<ScriptIter<'a>> {
        Ok(ScriptIter::new(self, split_script_tokens(sql)?))
    }

    pub fn execute_batch(&mut self, sql: &str) -> Result<()> {
        for result in self.iter_script(sql)? {
            result?;
        }
        Ok(())
    }

    pub fn execute_query(&mut self, sql: &str) -> Result<QueryResult> {
        self.execute(sql)
    }

    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        let statement = Self::parse_statement(sql)?;
        let parameter_count = statement.parameter_count();

        Ok(PreparedStatement {
            statement,
            parameters: vec![None; parameter_count],
        })
    }

    pub fn begin_transaction(&'_ mut self) -> Result<Transaction<'_>> {
        self.begin_active_transaction()?;
        Ok(Transaction {
            connection: self,
            completed: false,
        })
    }

    fn begin_active_transaction(&mut self) -> Result<()> {
        if self.transaction.is_some() {
            return Err(HematiteError::InternalError(
                "Transaction is already active".to_string(),
            ));
        }

        let mut catalog_guard = self.lock_catalog()?;
        catalog_guard.begin_transaction()?;
        if let Err(err) = catalog_guard.refresh_from_storage() {
            let _ = catalog_guard.rollback_transaction();
            return Err(err);
        }
        let snapshot = match catalog_guard.transaction_entry_snapshot() {
            Ok(snapshot) => snapshot,
            Err(err) => {
                let _ = catalog_guard.rollback_transaction();
                return Err(err);
            }
        };
        drop(catalog_guard);
        self.transaction = Some(ConnectionTransaction {
            snapshot,
            savepoints: Vec::new(),
        });
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn schema_snapshot(&self) -> Result<Schema> {
        let catalog_guard = self.lock_catalog()?;
        Ok(catalog_guard.clone_schema())
    }

    fn active_transaction_mut(&mut self, action: &str) -> Result<&mut ConnectionTransaction> {
        self.transaction.as_mut().ok_or_else(|| {
            HematiteError::ParseError(format!("{} requires an active transaction", action))
        })
    }

    fn create_savepoint(&mut self, name: &str) -> Result<()> {
        {
            let transaction = self.active_transaction_mut("SAVEPOINT")?;
            if transaction
                .savepoints
                .iter()
                .any(|savepoint| savepoint.name.eq_ignore_ascii_case(name))
            {
                return Err(HematiteError::ParseError(format!(
                    "Savepoint '{}' already exists",
                    name
                )));
            }
        }

        let snapshot = {
            let catalog_guard = self.lock_catalog()?;
            catalog_guard.snapshot()
        }?;

        let transaction = self.active_transaction_mut("SAVEPOINT")?;
        transaction.savepoints.push(SavepointState {
            name: name.to_string(),
            snapshot,
        });
        Ok(())
    }

    fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        let position = {
            let transaction = self.active_transaction_mut("ROLLBACK TO SAVEPOINT")?;
            transaction
                .savepoints
                .iter()
                .position(|savepoint| savepoint.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| {
                    HematiteError::ParseError(format!("Savepoint '{}' does not exist", name))
                })?
        };

        let snapshot = {
            let transaction = self.active_transaction_mut("ROLLBACK TO SAVEPOINT")?;
            transaction.savepoints[position].snapshot.clone()
        };

        {
            let mut catalog_guard = self.lock_catalog()?;
            catalog_guard.restore_snapshot(snapshot)?;
        }

        let transaction = self.active_transaction_mut("ROLLBACK TO SAVEPOINT")?;
        transaction.savepoints.truncate(position + 1);
        Ok(())
    }

    fn release_savepoint(&mut self, name: &str) -> Result<()> {
        let transaction = self.active_transaction_mut("RELEASE SAVEPOINT")?;
        let position = transaction
            .savepoints
            .iter()
            .position(|savepoint| savepoint.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| {
                HematiteError::ParseError(format!("Savepoint '{}' does not exist", name))
            })?;
        transaction.savepoints.remove(position);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PreparedStatement {
    statement: crate::parser::ast::Statement,
    parameters: Vec<Option<Value>>,
}

impl PreparedStatement {
    pub fn bind(&mut self, index: usize, value: Value) -> Result<()> {
        if index == 0 || index > self.parameters.len() {
            return Err(HematiteError::ParseError(format!(
                "Parameter index {} is out of range",
                index
            )));
        }

        self.parameters[index - 1] = Some(value);
        Ok(())
    }

    pub fn bind_all(&mut self, values: Vec<Value>) -> Result<()> {
        if values.len() != self.parameters.len() {
            return Err(HematiteError::ParseError(format!(
                "Expected {} parameters, got {}",
                self.parameters.len(),
                values.len()
            )));
        }

        self.parameters = values.into_iter().map(Some).collect();
        Ok(())
    }

    pub fn clear_bindings(&mut self) {
        self.parameters.fill(None);
    }

    pub fn parameter_count(&self) -> usize {
        self.parameters.len()
    }

    pub fn execute(&mut self, connection: &mut Connection) -> Result<QueryResult> {
        let statement = self.bound_statement()?;
        connection.execute_statement(statement)
    }

    pub fn query(&mut self, connection: &mut Connection) -> Result<QueryResult> {
        self.execute(connection)
    }

    fn bound_statement(&self) -> Result<crate::parser::ast::Statement> {
        let bound_values = self
            .parameters
            .iter()
            .enumerate()
            .map(|(index, value)| {
                value.clone().ok_or_else(|| {
                    HematiteError::ParseError(format!("Parameter {} has not been bound", index + 1))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let bound_literals = bound_values
            .iter()
            .map(raise_literal_value)
            .collect::<Vec<_>>();

        self.statement.bind_parameters(&bound_literals)
    }
}

#[derive(Debug)]
pub struct Transaction<'a> {
    connection: &'a mut Connection,
    completed: bool,
}

impl<'a> Transaction<'a> {
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        self.connection.execute(sql)
    }

    pub fn commit(&mut self) -> Result<()> {
        if self.completed {
            return Err(HematiteError::InternalError(
                "Transaction is already completed".to_string(),
            ));
        }
        self.connection.commit_active_transaction()?;
        self.completed = true;
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<()> {
        if self.completed {
            return Err(HematiteError::InternalError(
                "Transaction is already completed".to_string(),
            ));
        }
        self.connection.rollback_active_transaction()?;
        self.completed = true;
        Ok(())
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.connection.rollback_active_transaction();
        }
    }
}

#[derive(Debug, Clone)]
pub struct Database;

impl Database {
    pub fn new() -> Self {
        Self
    }

    pub fn open(database_path: &str) -> Result<Connection> {
        Connection::new(database_path)
    }

    pub fn open_in_memory() -> Result<Connection> {
        Connection::new_in_memory()
    }

    pub fn connect(&mut self, database_path: &str) -> Result<Connection> {
        Connection::new(database_path)
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

impl Connection {
    fn take_active_transaction(&mut self, action: &str) -> Result<ConnectionTransaction> {
        self.transaction.take().ok_or_else(|| {
            HematiteError::InternalError(format!("No active transaction to {}", action))
        })
    }

    fn commit_active_transaction(&mut self) -> Result<()> {
        let state = self.take_active_transaction("commit")?;
        let mut catalog_guard = self.lock_catalog()?;
        match catalog_guard.commit_transaction() {
            Ok(()) => Ok(()),
            Err(err) => {
                let _ = catalog_guard.rollback_transaction();
                catalog_guard.restore_snapshot(state.snapshot)?;
                Err(err)
            }
        }
    }

    fn rollback_active_transaction(&mut self) -> Result<()> {
        let state = self.take_active_transaction("roll back")?;
        let mut catalog_guard = self.lock_catalog()?;
        catalog_guard.rollback_transaction()?;
        catalog_guard.restore_snapshot(state.snapshot)?;
        Ok(())
    }
}
