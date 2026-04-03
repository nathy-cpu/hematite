//! SQL connection boundary.
//!
//! A connection owns a catalog instance plus statement-level transaction behavior.
//!
//! ```text
//! SQL text / prepared statement
//!            |
//!            v
//!         parser
//!            |
//!            v
//!    planner + executor
//!            |
//!            v
//!         catalog
//!            |
//!            v
//!      btree + pager
//! ```
//!
//! This is where autocommit, explicit transactions, journal mode changes, and user-facing SQL
//! errors are coordinated. The connection should not need to understand row encoding or page
//! structure; it only sequences higher-level components.

use crate::error::{HematiteError, Result};
use crate::parser::ast::{
    ColumnDefinition, Condition, CreateStatement, CreateViewStatement, Expression, InsertSource,
    InsertStatement, SelectIntoStatement, SelectStatement, Statement, TableReference, TriggerEvent,
    WhereClause,
};
use crate::parser::{Lexer, Parser, SqlTypeName};
use crate::query::lowering::raise_literal_value;
use crate::query::metadata as query_metadata;
use crate::query::validation::{projected_column_names, source_column_names, validate_statement};
use crate::query::{
    Catalog, CatalogEngine, ExecutionContext, JournalMode, MutationEvent, QueryCatalogSnapshot,
    QueryExecutor, QueryPlanner, QueryResult, Schema, Value,
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
        let snapshot = catalog_guard.snapshot()?;
        catalog_guard.begin_transaction()?;
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
    trigger_depth: usize,
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
            Int128,
            UInt,
            UInt64,
            UInt128,
            Float32,
            Float,
            Float128,
            Decimal,
        }

        #[derive(Debug, Clone)]
        enum InferredKind {
            Numeric(NumericKind),
            String { saw_enum: bool, values: Vec<String> },
            Boolean,
            Blob,
            Date,
            Time,
            DateTime,
            TimeWithTimeZone,
        }

        impl InferredKind {
            fn absorb(self, value: &Value, column_name: &str) -> Result<Self> {
                use InferredKind::*;
                use NumericKind::*;
                match (self, value) {
                    (kind, Value::Null) => Ok(kind),
                    (_, Value::IntervalYearMonth(_)) | (_, Value::IntervalDaySecond(_)) => {
                        Err(HematiteError::ParseError(format!(
                            "SELECT INTO cannot infer a stored column type for interval-valued column '{}'",
                            column_name
                        )))
                    }
                    (Numeric(Int), Value::Integer(_)) => Ok(Numeric(Int)),
                    (Numeric(Int), Value::BigInt(_))
                    | (Numeric(Int64), Value::Integer(_))
                    | (Numeric(Int64), Value::BigInt(_)) => Ok(Numeric(Int64)),
                    (Numeric(Int), Value::Int128(_))
                    | (Numeric(Int64), Value::Int128(_))
                    | (Numeric(Int128), Value::Integer(_))
                    | (Numeric(Int128), Value::BigInt(_))
                    | (Numeric(Int128), Value::Int128(_)) => Ok(Numeric(Int128)),
                    (Numeric(UInt), Value::UInteger(_)) => Ok(Numeric(UInt)),
                    (Numeric(UInt), Value::UBigInt(_))
                    | (Numeric(UInt64), Value::UInteger(_))
                    | (Numeric(UInt64), Value::UBigInt(_)) => Ok(Numeric(UInt64)),
                    (Numeric(UInt), Value::UInt128(_))
                    | (Numeric(UInt64), Value::UInt128(_))
                    | (Numeric(UInt128), Value::UInteger(_))
                    | (Numeric(UInt128), Value::UBigInt(_))
                    | (Numeric(UInt128), Value::UInt128(_)) => Ok(Numeric(UInt128)),
                    (Numeric(Int), Value::UInteger(_))
                    | (Numeric(Int64), Value::UInteger(_))
                    | (Numeric(Int128), Value::UInteger(_))
                    | (Numeric(UInt), Value::Integer(_))
                    | (Numeric(UInt), Value::BigInt(_))
                    | (Numeric(UInt), Value::Int128(_))
                    | (Numeric(UInt64), Value::Integer(_))
                    | (Numeric(UInt64), Value::BigInt(_))
                    | (Numeric(UInt64), Value::Int128(_))
                    | (Numeric(UInt128), Value::Integer(_))
                    | (Numeric(UInt128), Value::BigInt(_))
                    | (Numeric(UInt128), Value::Int128(_))
                    | (Numeric(Int64), Value::UBigInt(_))
                    | (Numeric(Int128), Value::UBigInt(_))
                    | (Numeric(Int128), Value::UInt128(_))
                    => Ok(Numeric(Decimal)),
                    (Numeric(Int), Value::Float32(_))
                    | (Numeric(Int64), Value::Float32(_))
                    | (Numeric(Int128), Value::Float32(_))
                    | (Numeric(UInt), Value::Float32(_))
                    | (Numeric(UInt64), Value::Float32(_))
                    | (Numeric(UInt128), Value::Float32(_))
                    | (Numeric(Float32), Value::Integer(_))
                    | (Numeric(Float32), Value::BigInt(_))
                    | (Numeric(Float32), Value::Int128(_))
                    | (Numeric(Float32), Value::UInteger(_))
                    | (Numeric(Float32), Value::UBigInt(_))
                    | (Numeric(Float32), Value::UInt128(_))
                    | (Numeric(Float32), Value::Float32(_)) => Ok(Numeric(Float32)),
                    (Numeric(Int), Value::Float(_))
                    | (Numeric(Int64), Value::Float(_))
                    | (Numeric(Int128), Value::Float(_))
                    | (Numeric(UInt), Value::Float(_))
                    | (Numeric(UInt64), Value::Float(_))
                    | (Numeric(UInt128), Value::Float(_))
                    | (Numeric(Float32), Value::Float(_))
                    | (Numeric(Float), Value::Integer(_))
                    | (Numeric(Float), Value::BigInt(_))
                    | (Numeric(Float), Value::Int128(_))
                    | (Numeric(Float), Value::UInteger(_))
                    | (Numeric(Float), Value::UBigInt(_))
                    | (Numeric(Float), Value::UInt128(_))
                    | (Numeric(Float), Value::Float32(_))
                    | (Numeric(Float), Value::Float(_)) => Ok(Numeric(Float)),
                    (Numeric(Int), Value::Float128(_))
                    | (Numeric(Int64), Value::Float128(_))
                    | (Numeric(Int128), Value::Float128(_))
                    | (Numeric(UInt), Value::Float128(_))
                    | (Numeric(UInt64), Value::Float128(_))
                    | (Numeric(UInt128), Value::Float128(_))
                    | (Numeric(Float32), Value::Float128(_))
                    | (Numeric(Float), Value::Float128(_))
                    | (Numeric(Float128), Value::Integer(_))
                    | (Numeric(Float128), Value::BigInt(_))
                    | (Numeric(Float128), Value::Int128(_))
                    | (Numeric(Float128), Value::UInteger(_))
                    | (Numeric(Float128), Value::UBigInt(_))
                    | (Numeric(Float128), Value::UInt128(_))
                    | (Numeric(Float128), Value::Float32(_))
                    | (Numeric(Float128), Value::Float(_))
                    | (Numeric(Float128), Value::Float128(_)) => Ok(Numeric(Float128)),
                    (Numeric(Int), Value::Decimal(_))
                    | (Numeric(Int64), Value::Decimal(_))
                    | (Numeric(Int128), Value::Decimal(_))
                    | (Numeric(UInt), Value::Decimal(_))
                    | (Numeric(UInt64), Value::Decimal(_))
                    | (Numeric(UInt128), Value::Decimal(_))
                    | (Numeric(Float32), Value::Decimal(_))
                    | (Numeric(Float), Value::Decimal(_))
                    | (Numeric(Float128), Value::Decimal(_))
                    | (Numeric(Decimal), Value::Integer(_))
                    | (Numeric(Decimal), Value::BigInt(_))
                    | (Numeric(Decimal), Value::Int128(_))
                    | (Numeric(Decimal), Value::UInteger(_))
                    | (Numeric(Decimal), Value::UBigInt(_))
                    | (Numeric(Decimal), Value::UInt128(_))
                    | (Numeric(Decimal), Value::Float32(_))
                    | (Numeric(Decimal), Value::Float(_))
                    | (Numeric(Decimal), Value::Float128(_))
                    | (Numeric(Decimal), Value::Decimal(_)) => Ok(Numeric(Decimal)),
                    (
                        String {
                            saw_enum,
                            mut values,
                        },
                        Value::Text(text),
                    ) => {
                        if !values.iter().any(|candidate| candidate == text) {
                            values.push(text.clone());
                        }
                        Ok(String { saw_enum, values })
                    }
                    (
                        String {
                            saw_enum: _,
                            mut values,
                        },
                        Value::Enum(text),
                    ) => {
                        if !values.iter().any(|candidate| candidate == text) {
                            values.push(text.clone());
                        }
                        Ok(String {
                            saw_enum: true,
                            values,
                        })
                    }
                    (Blob, Value::Blob(_)) => Ok(Blob),
                    (Blob, Value::Text(_)) => Ok(Blob),
                    (Date, Value::Date(_)) => Ok(Date),
                    (Time, Value::Time(_)) => Ok(Time),
                    (DateTime, Value::DateTime(_)) => Ok(DateTime),
                    (TimeWithTimeZone, Value::TimeWithTimeZone(_)) => Ok(TimeWithTimeZone),
                    (Boolean, Value::Boolean(_)) => Ok(Boolean),
                    (left, right) => Err(HematiteError::ParseError(format!(
                        "SELECT INTO cannot infer a stable column type for '{}': {:?} cannot be combined with {:?}",
                        column_name, left, right
                    ))),
                }
            }

            fn from_value(value: &Value, column_name: &str) -> Result<Option<Self>> {
                use InferredKind::*;
                use NumericKind::*;
                let inferred = match value {
                    Value::Null => return Ok(None),
                    Value::Integer(_) => Numeric(Int),
                    Value::BigInt(_) => Numeric(Int64),
                    Value::Int128(_) => Numeric(Int128),
                    Value::UInteger(_) => Numeric(UInt),
                    Value::UBigInt(_) => Numeric(UInt64),
                    Value::UInt128(_) => Numeric(UInt128),
                    Value::Float32(_) => Numeric(Float32),
                    Value::Float(_) => Numeric(Float),
                    Value::Float128(_) => Numeric(Float128),
                    Value::Decimal(_) => Numeric(Decimal),
                    Value::Text(text) => String {
                        saw_enum: false,
                        values: vec![text.clone()],
                    },
                    Value::Enum(text) => String {
                        saw_enum: true,
                        values: vec![text.clone()],
                    },
                    Value::Boolean(_) => Boolean,
                    Value::Blob(_) => Blob,
                    Value::Date(_) => Date,
                    Value::Time(_) => Time,
                    Value::DateTime(_) => DateTime,
                    Value::TimeWithTimeZone(_) => TimeWithTimeZone,
                    Value::IntervalYearMonth(_) | Value::IntervalDaySecond(_) => {
                        return Err(HematiteError::ParseError(format!(
                            "SELECT INTO cannot infer a stored column type for interval-valued column '{}'",
                            column_name
                        )))
                    }
                };
                Ok(Some(inferred))
            }

            fn into_sql_type(self) -> SqlTypeName {
                match self {
                    InferredKind::Numeric(NumericKind::Int) => SqlTypeName::Int,
                    InferredKind::Numeric(NumericKind::Int64) => SqlTypeName::Int64,
                    InferredKind::Numeric(NumericKind::Int128) => SqlTypeName::Int128,
                    InferredKind::Numeric(NumericKind::UInt) => SqlTypeName::UInt,
                    InferredKind::Numeric(NumericKind::UInt64) => SqlTypeName::UInt64,
                    InferredKind::Numeric(NumericKind::UInt128) => SqlTypeName::UInt128,
                    InferredKind::Numeric(NumericKind::Float32) => SqlTypeName::Float32,
                    InferredKind::Numeric(NumericKind::Float) => SqlTypeName::Float,
                    InferredKind::Numeric(NumericKind::Float128) => SqlTypeName::Float128,
                    InferredKind::Numeric(NumericKind::Decimal) => SqlTypeName::Decimal {
                        precision: None,
                        scale: None,
                    },
                    InferredKind::String {
                        saw_enum: true,
                        values,
                    } => SqlTypeName::Enum(values),
                    InferredKind::String { .. } => SqlTypeName::Text,
                    InferredKind::Boolean => SqlTypeName::Boolean,
                    InferredKind::Blob => SqlTypeName::Blob,
                    InferredKind::Date => SqlTypeName::Date,
                    InferredKind::Time => SqlTypeName::Time,
                    InferredKind::DateTime => SqlTypeName::DateTime,
                    InferredKind::TimeWithTimeZone => SqlTypeName::TimeWithTimeZone,
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

    pub fn new(database_path: &str) -> Result<Self> {
        let catalog = Catalog::open_or_create(database_path)?;
        Ok(Self {
            catalog: Arc::new(Mutex::new(catalog)),
            transaction: None,
            trigger_depth: 0,
        })
    }

    pub fn new_in_memory() -> Result<Self> {
        let catalog = Catalog::open_in_memory()?;
        Ok(Self {
            catalog: Arc::new(Mutex::new(catalog)),
            transaction: None,
            trigger_depth: 0,
        })
    }

    fn parse_statement(sql: &str) -> Result<crate::parser::ast::Statement> {
        let mut lexer = Lexer::new(sql.to_string());
        lexer.tokenize()?;

        let mut parser = Parser::new(lexer.get_tokens().to_vec());
        parser.parse()
    }

    fn parse_select_sql(sql: &str) -> Result<SelectStatement> {
        match Self::parse_statement(&format!("{sql};"))? {
            Statement::Select(select) => Ok(select),
            other => Err(HematiteError::ParseError(format!(
                "Expected stored view query to be SELECT, found {:?}",
                other
            ))),
        }
    }

    fn expand_views_in_statement(statement: Statement, schema: &Schema) -> Result<Statement> {
        match statement {
            Statement::Explain(explain) => {
                Ok(Statement::Explain(crate::parser::ast::ExplainStatement {
                    statement: Box::new(Self::expand_views_in_statement(
                        *explain.statement,
                        schema,
                    )?),
                }))
            }
            Statement::Select(select) => Ok(Statement::Select(Self::expand_views_in_select(
                select, schema,
            )?)),
            Statement::Insert(mut insert) => {
                if let InsertSource::Select(select) = insert.source {
                    insert.source = InsertSource::Select(Box::new(Self::expand_views_in_select(
                        *select, schema,
                    )?));
                }
                Ok(Statement::Insert(insert))
            }
            Statement::CreateView(mut create_view) => {
                create_view.query = Self::expand_views_in_select(create_view.query, schema)?;
                Ok(Statement::CreateView(create_view))
            }
            other => Ok(other),
        }
    }

    fn expand_views_in_select(
        mut select: SelectStatement,
        schema: &Schema,
    ) -> Result<SelectStatement> {
        for cte in &mut select.with_clause {
            cte.query = Box::new(Self::expand_views_in_select((*cte.query).clone(), schema)?);
        }
        let original_from = select.from.clone();
        let select_context = select.clone();
        select.from =
            Self::expand_views_in_table_reference(original_from, &select_context, schema)?;
        if let Some(where_clause) = &mut select.where_clause {
            Self::expand_views_in_where_clause(where_clause, schema)?;
        }
        for expr in &mut select.group_by {
            Self::expand_views_in_expression(expr, schema)?;
        }
        if let Some(having_clause) = &mut select.having_clause {
            Self::expand_views_in_where_clause(having_clause, schema)?;
        }
        if let Some(set_operation) = &mut select.set_operation {
            set_operation.right = Box::new(Self::expand_views_in_select(
                (*set_operation.right).clone(),
                schema,
            )?);
        }
        for item in &mut select.columns {
            if let crate::parser::ast::SelectItem::Expression(expr) = item {
                Self::expand_views_in_expression(expr, schema)?;
            }
        }
        Ok(select)
    }

    fn expand_views_in_table_reference(
        from: TableReference,
        select: &SelectStatement,
        schema: &Schema,
    ) -> Result<TableReference> {
        match from {
            TableReference::Table(table_name, alias) => {
                if select.lookup_cte(&table_name).is_some()
                    || schema.get_table_by_name(&table_name).is_some()
                {
                    Ok(TableReference::Table(table_name, alias))
                } else if let Some(view) = schema.view(&table_name) {
                    let subquery = Self::expand_views_in_select(
                        Self::parse_select_sql(&view.query_sql)?,
                        schema,
                    )?;
                    Ok(TableReference::Derived {
                        subquery: Box::new(subquery),
                        alias: alias.unwrap_or(table_name),
                    })
                } else {
                    Ok(TableReference::Table(table_name, alias))
                }
            }
            TableReference::Derived { subquery, alias } => Ok(TableReference::Derived {
                subquery: Box::new(Self::expand_views_in_select(*subquery, schema)?),
                alias,
            }),
            TableReference::CrossJoin(left, right) => Ok(TableReference::CrossJoin(
                Box::new(Self::expand_views_in_table_reference(
                    *left, select, schema,
                )?),
                Box::new(Self::expand_views_in_table_reference(
                    *right, select, schema,
                )?),
            )),
            TableReference::InnerJoin {
                left,
                right,
                mut on,
            } => {
                Self::expand_views_in_condition(&mut on, schema)?;
                Ok(TableReference::InnerJoin {
                    left: Box::new(Self::expand_views_in_table_reference(
                        *left, select, schema,
                    )?),
                    right: Box::new(Self::expand_views_in_table_reference(
                        *right, select, schema,
                    )?),
                    on,
                })
            }
            TableReference::LeftJoin {
                left,
                right,
                mut on,
            } => {
                Self::expand_views_in_condition(&mut on, schema)?;
                Ok(TableReference::LeftJoin {
                    left: Box::new(Self::expand_views_in_table_reference(
                        *left, select, schema,
                    )?),
                    right: Box::new(Self::expand_views_in_table_reference(
                        *right, select, schema,
                    )?),
                    on,
                })
            }
            TableReference::RightJoin {
                left,
                right,
                mut on,
            } => {
                Self::expand_views_in_condition(&mut on, schema)?;
                Ok(TableReference::RightJoin {
                    left: Box::new(Self::expand_views_in_table_reference(
                        *left, select, schema,
                    )?),
                    right: Box::new(Self::expand_views_in_table_reference(
                        *right, select, schema,
                    )?),
                    on,
                })
            }
            TableReference::FullOuterJoin {
                left,
                right,
                mut on,
            } => {
                Self::expand_views_in_condition(&mut on, schema)?;
                Ok(TableReference::FullOuterJoin {
                    left: Box::new(Self::expand_views_in_table_reference(
                        *left, select, schema,
                    )?),
                    right: Box::new(Self::expand_views_in_table_reference(
                        *right, select, schema,
                    )?),
                    on,
                })
            }
        }
    }

    fn expand_views_in_where_clause(where_clause: &mut WhereClause, schema: &Schema) -> Result<()> {
        for condition in &mut where_clause.conditions {
            Self::expand_views_in_condition(condition, schema)?;
        }
        Ok(())
    }

    fn expand_views_in_condition(condition: &mut Condition, schema: &Schema) -> Result<()> {
        match condition {
            Condition::Comparison { left, right, .. } => {
                Self::expand_views_in_expression(left, schema)?;
                Self::expand_views_in_expression(right, schema)?;
            }
            Condition::InList { expr, values, .. } => {
                Self::expand_views_in_expression(expr, schema)?;
                for value in values {
                    Self::expand_views_in_expression(value, schema)?;
                }
            }
            Condition::InSubquery { expr, subquery, .. } => {
                Self::expand_views_in_expression(expr, schema)?;
                *subquery = Box::new(Self::expand_views_in_select((**subquery).clone(), schema)?);
            }
            Condition::Between {
                expr, lower, upper, ..
            } => {
                Self::expand_views_in_expression(expr, schema)?;
                Self::expand_views_in_expression(lower, schema)?;
                Self::expand_views_in_expression(upper, schema)?;
            }
            Condition::Like { expr, pattern, .. } => {
                Self::expand_views_in_expression(expr, schema)?;
                Self::expand_views_in_expression(pattern, schema)?;
            }
            Condition::Exists { subquery, .. } => {
                *subquery = Box::new(Self::expand_views_in_select((**subquery).clone(), schema)?);
            }
            Condition::NullCheck { expr, .. } => {
                Self::expand_views_in_expression(expr, schema)?;
            }
            Condition::Not(inner) => Self::expand_views_in_condition(inner, schema)?,
            Condition::Logical { left, right, .. } => {
                Self::expand_views_in_condition(left, schema)?;
                Self::expand_views_in_condition(right, schema)?;
            }
        }
        Ok(())
    }

    fn expand_views_in_expression(expr: &mut Expression, schema: &Schema) -> Result<()> {
        match expr {
            Expression::ScalarSubquery(subquery) => {
                *subquery = Box::new(Self::expand_views_in_select((**subquery).clone(), schema)?);
            }
            Expression::Cast { expr, .. }
            | Expression::UnaryMinus(expr)
            | Expression::UnaryNot(expr)
            | Expression::NullCheck { expr, .. } => Self::expand_views_in_expression(expr, schema)?,
            Expression::Case {
                branches,
                else_expr,
            } => {
                for branch in branches {
                    Self::expand_views_in_expression(&mut branch.condition, schema)?;
                    Self::expand_views_in_expression(&mut branch.result, schema)?;
                }
                if let Some(else_expr) = else_expr {
                    Self::expand_views_in_expression(else_expr, schema)?;
                }
            }
            Expression::ScalarFunctionCall { args, .. } => {
                for arg in args {
                    Self::expand_views_in_expression(arg, schema)?;
                }
            }
            Expression::Binary { left, right, .. }
            | Expression::Comparison { left, right, .. }
            | Expression::Logical { left, right, .. } => {
                Self::expand_views_in_expression(left, schema)?;
                Self::expand_views_in_expression(right, schema)?;
            }
            Expression::InList { expr, values, .. } => {
                Self::expand_views_in_expression(expr, schema)?;
                for value in values {
                    Self::expand_views_in_expression(value, schema)?;
                }
            }
            Expression::InSubquery { expr, subquery, .. } => {
                Self::expand_views_in_expression(expr, schema)?;
                *subquery = Box::new(Self::expand_views_in_select((**subquery).clone(), schema)?);
            }
            Expression::Between {
                expr, lower, upper, ..
            } => {
                Self::expand_views_in_expression(expr, schema)?;
                Self::expand_views_in_expression(lower, schema)?;
                Self::expand_views_in_expression(upper, schema)?;
            }
            Expression::Like { expr, pattern, .. } => {
                Self::expand_views_in_expression(expr, schema)?;
                Self::expand_views_in_expression(pattern, schema)?;
            }
            Expression::Exists { subquery, .. } => {
                *subquery = Box::new(Self::expand_views_in_select((**subquery).clone(), schema)?);
            }
            Expression::AggregateCall { .. }
            | Expression::Column(_)
            | Expression::Literal(_)
            | Expression::IntervalLiteral { .. }
            | Expression::Parameter(_) => {}
        }
        Ok(())
    }

    fn normalize_statement(statement: Statement, schema: &Schema) -> Result<Statement> {
        let mut statement = Self::expand_views_in_statement(statement, schema)?;
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
            Statement::CreateView(create_view) => {
                Self::rewrite_select_aliases_in_select(&mut create_view.query, schema)
            }
            _ => Ok(()),
        }
    }

    fn rewrite_select_aliases_in_select(
        select: &mut SelectStatement,
        schema: &Schema,
    ) -> Result<()> {
        for cte in &mut select.with_clause {
            if !cte.recursive {
                Self::rewrite_select_aliases_in_select(&mut cte.query, schema)?;
            }
        }

        Self::rewrite_select_aliases_in_table_reference(&mut select.from, schema)?;

        for item in &mut select.columns {
            match item {
                crate::parser::ast::SelectItem::Expression(expr) => {
                    Self::rewrite_nested_select_aliases_in_expression(expr, schema)?;
                }
                crate::parser::ast::SelectItem::Window { window, .. } => {
                    for expr in &mut window.partition_by {
                        Self::rewrite_nested_select_aliases_in_expression(expr, schema)?;
                    }
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
                Self::rewrite_where_aliases_in_condition(
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
                Self::rewrite_nested_select_aliases_in_condition(condition, schema)?;
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
                Self::rewrite_nested_select_aliases_in_condition(on, schema)
            }
            TableReference::Table(_, _) => Ok(()),
        }
    }

    fn rewrite_nested_select_aliases_in_condition(
        condition: &mut Condition,
        schema: &Schema,
    ) -> Result<()> {
        match condition {
            Condition::Comparison { left, right, .. } => {
                Self::rewrite_nested_select_aliases_in_expression(left, schema)?;
                Self::rewrite_nested_select_aliases_in_expression(right, schema)?;
            }
            Condition::InList { expr, values, .. } => {
                Self::rewrite_nested_select_aliases_in_expression(expr, schema)?;
                for value in values {
                    Self::rewrite_nested_select_aliases_in_expression(value, schema)?;
                }
            }
            Condition::InSubquery { expr, subquery, .. } => {
                Self::rewrite_nested_select_aliases_in_expression(expr, schema)?;
                Self::rewrite_select_aliases_in_select(subquery, schema)?;
            }
            Condition::Between {
                expr, lower, upper, ..
            } => {
                Self::rewrite_nested_select_aliases_in_expression(expr, schema)?;
                Self::rewrite_nested_select_aliases_in_expression(lower, schema)?;
                Self::rewrite_nested_select_aliases_in_expression(upper, schema)?;
            }
            Condition::Like { expr, pattern, .. } => {
                Self::rewrite_nested_select_aliases_in_expression(expr, schema)?;
                Self::rewrite_nested_select_aliases_in_expression(pattern, schema)?;
            }
            Condition::Exists { subquery, .. } => {
                Self::rewrite_select_aliases_in_select(subquery, schema)?;
            }
            Condition::NullCheck { expr, .. } => {
                Self::rewrite_nested_select_aliases_in_expression(expr, schema)?;
            }
            Condition::Not(inner) => {
                Self::rewrite_nested_select_aliases_in_condition(inner, schema)?
            }
            Condition::Logical { left, right, .. } => {
                Self::rewrite_nested_select_aliases_in_condition(left, schema)?;
                Self::rewrite_nested_select_aliases_in_condition(right, schema)?;
            }
        }
        Ok(())
    }

    fn rewrite_nested_select_aliases_in_expression(
        expr: &mut Expression,
        schema: &Schema,
    ) -> Result<()> {
        match expr {
            Expression::ScalarSubquery(subquery) => {
                Self::rewrite_select_aliases_in_select(subquery, schema)
            }
            Expression::Cast { expr, .. }
            | Expression::UnaryMinus(expr)
            | Expression::UnaryNot(expr)
            | Expression::NullCheck { expr, .. } => {
                Self::rewrite_nested_select_aliases_in_expression(expr, schema)
            }
            Expression::Case {
                branches,
                else_expr,
            } => {
                for branch in branches {
                    Self::rewrite_nested_select_aliases_in_expression(
                        &mut branch.condition,
                        schema,
                    )?;
                    Self::rewrite_nested_select_aliases_in_expression(&mut branch.result, schema)?;
                }
                if let Some(else_expr) = else_expr {
                    Self::rewrite_nested_select_aliases_in_expression(else_expr, schema)?;
                }
                Ok(())
            }
            Expression::ScalarFunctionCall { args, .. } => {
                for arg in args {
                    Self::rewrite_nested_select_aliases_in_expression(arg, schema)?;
                }
                Ok(())
            }
            Expression::Binary { left, right, .. }
            | Expression::Comparison { left, right, .. }
            | Expression::Logical { left, right, .. } => {
                Self::rewrite_nested_select_aliases_in_expression(left, schema)?;
                Self::rewrite_nested_select_aliases_in_expression(right, schema)
            }
            Expression::InList { expr, values, .. } => {
                Self::rewrite_nested_select_aliases_in_expression(expr, schema)?;
                for value in values {
                    Self::rewrite_nested_select_aliases_in_expression(value, schema)?;
                }
                Ok(())
            }
            Expression::InSubquery { expr, subquery, .. } => {
                Self::rewrite_nested_select_aliases_in_expression(expr, schema)?;
                Self::rewrite_select_aliases_in_select(subquery, schema)
            }
            Expression::Between {
                expr, lower, upper, ..
            } => {
                Self::rewrite_nested_select_aliases_in_expression(expr, schema)?;
                Self::rewrite_nested_select_aliases_in_expression(lower, schema)?;
                Self::rewrite_nested_select_aliases_in_expression(upper, schema)
            }
            Expression::Like { expr, pattern, .. } => {
                Self::rewrite_nested_select_aliases_in_expression(expr, schema)?;
                Self::rewrite_nested_select_aliases_in_expression(pattern, schema)
            }
            Expression::Exists { subquery, .. } => {
                Self::rewrite_select_aliases_in_select(subquery, schema)
            }
            Expression::AggregateCall { .. }
            | Expression::Column(_)
            | Expression::Literal(_)
            | Expression::IntervalLiteral { .. }
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

    fn rewrite_where_aliases_in_condition(
        condition: &mut Condition,
        aliases: &HashMap<String, Expression>,
        source_columns: &HashSet<String>,
        active_aliases: &mut HashSet<String>,
    ) -> Result<()> {
        match condition {
            Condition::Comparison { left, right, .. } => {
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
            Condition::InList { expr, values, .. } => {
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
            Condition::InSubquery { expr, .. } => {
                Self::rewrite_where_aliases_in_expression(
                    expr,
                    aliases,
                    source_columns,
                    active_aliases,
                )?;
            }
            Condition::Between {
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
            Condition::Like { expr, pattern, .. } => {
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
            Condition::Exists { .. } => {}
            Condition::NullCheck { expr, .. } => {
                Self::rewrite_where_aliases_in_expression(
                    expr,
                    aliases,
                    source_columns,
                    active_aliases,
                )?;
            }
            Condition::Not(inner) => {
                Self::rewrite_where_aliases_in_condition(
                    inner,
                    aliases,
                    source_columns,
                    active_aliases,
                )?;
            }
            Condition::Logical { left, right, .. } => {
                Self::rewrite_where_aliases_in_condition(
                    left,
                    aliases,
                    source_columns,
                    active_aliases,
                )?;
                Self::rewrite_where_aliases_in_condition(
                    right,
                    aliases,
                    source_columns,
                    active_aliases,
                )?;
            }
        }
        Ok(())
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
            | Expression::IntervalLiteral { .. }
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
            crate::parser::ast::Statement::ShowViews => {
                return self.execute_show_views_statement();
            }
            crate::parser::ast::Statement::ShowIndexes(table_name) => {
                return self.execute_show_indexes_statement(table_name.as_deref());
            }
            crate::parser::ast::Statement::ShowTriggers(table_name) => {
                return self.execute_show_triggers_statement(table_name.as_deref());
            }
            crate::parser::ast::Statement::ShowCreateTable(table_name) => {
                return self.execute_show_create_table_statement(&table_name);
            }
            crate::parser::ast::Statement::ShowCreateView(view_name) => {
                return self.execute_show_create_view_statement(&view_name);
            }
            crate::parser::ast::Statement::SelectInto(select_into) => {
                return self.execute_select_into_statement(select_into);
            }
            crate::parser::ast::Statement::CreateView(create_view) => {
                return self.execute_create_view_statement(create_view);
            }
            crate::parser::ast::Statement::DropView(drop_view) => {
                return self.execute_drop_view_statement(&drop_view.view, drop_view.if_exists);
            }
            crate::parser::ast::Statement::CreateTrigger(create_trigger) => {
                return self.execute_create_trigger_statement(create_trigger);
            }
            crate::parser::ast::Statement::DropTrigger(drop_trigger) => {
                return self
                    .execute_drop_trigger_statement(&drop_trigger.trigger, drop_trigger.if_exists);
            }
            _ => {}
        }

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
        let statement = Self::expand_views_in_statement(statement, &schema)?;
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
                    Value::Text(format!("{:.2}", plan.estimated_cost)),
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

    fn execute_show_views_statement(&mut self) -> Result<QueryResult> {
        let catalog_guard = self.lock_catalog()?;
        query_metadata::show_views(&catalog_guard)
    }

    fn execute_show_indexes_statement(&mut self, table_name: Option<&str>) -> Result<QueryResult> {
        let catalog_guard = self.lock_catalog()?;
        query_metadata::show_indexes(&catalog_guard, table_name)
    }

    fn execute_show_triggers_statement(&mut self, table_name: Option<&str>) -> Result<QueryResult> {
        let catalog_guard = self.lock_catalog()?;
        query_metadata::show_triggers(&catalog_guard, table_name)
    }

    fn execute_show_create_table_statement(&mut self, table_name: &str) -> Result<QueryResult> {
        let catalog_guard = self.lock_catalog()?;
        query_metadata::show_create_table(&catalog_guard, table_name)
    }

    fn execute_show_create_view_statement(&mut self, view_name: &str) -> Result<QueryResult> {
        let catalog_guard = self.lock_catalog()?;
        query_metadata::show_create_view(&catalog_guard, view_name)
    }

    fn execute_select_into_statement(
        &mut self,
        statement: SelectIntoStatement,
    ) -> Result<QueryResult> {
        let (schema, _) = self.read_planning_state()?;
        if schema.get_table_by_name(&statement.table).is_some()
            || schema.view(&statement.table).is_some()
        {
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

    fn execute_create_view_statement(
        &mut self,
        statement: crate::parser::ast::CreateViewStatement,
    ) -> Result<QueryResult> {
        let mut implicit_mutation = Some(ImplicitMutation::begin(self)?);
        let result: Result<QueryResult> = (|| {
            let mut catalog_guard = self.lock_catalog()?;
            let schema = catalog_guard.clone_schema();
            let dependencies = statement.query.dependency_names();
            if dependencies
                .iter()
                .any(|dependency| dependency.eq_ignore_ascii_case(&statement.view))
            {
                return Err(HematiteError::ParseError(format!(
                    "View '{}' cannot depend on itself",
                    statement.view
                )));
            }
            let normalized_query = match Self::normalize_statement(
                Statement::Select(statement.query.clone()),
                &schema,
            )? {
                Statement::Select(select) => select,
                _ => unreachable!("normalized create view query should remain a select"),
            };
            validate_statement(
                &crate::parser::ast::Statement::CreateView(CreateViewStatement {
                    view: statement.view.clone(),
                    if_not_exists: statement.if_not_exists,
                    query: normalized_query.clone(),
                }),
                &schema,
            )?;

            if statement.if_not_exists && catalog_guard.get_view(&statement.view)?.is_some() {
                Ok(Self::mutation_result(0))
            } else {
                let column_names = projected_column_names(&normalized_query, &schema)?;

                catalog_guard.create_view(crate::catalog::View {
                    name: statement.view.clone(),
                    query_sql: statement.query.to_sql(),
                    column_names,
                    dependencies,
                })?;
                Ok(Self::mutation_result(0))
            }
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

    fn execute_drop_view_statement(
        &mut self,
        view_name: &str,
        if_exists: bool,
    ) -> Result<QueryResult> {
        let mut implicit_mutation = Some(ImplicitMutation::begin(self)?);
        let result: Result<QueryResult> = (|| {
            let mut catalog_guard = self.lock_catalog()?;
            if if_exists && catalog_guard.get_view(view_name)?.is_none() {
                Ok(Self::mutation_result(0))
            } else {
                catalog_guard.drop_view(view_name)?;
                Ok(Self::mutation_result(0))
            }
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

    fn execute_create_trigger_statement(
        &mut self,
        statement: crate::parser::ast::CreateTriggerStatement,
    ) -> Result<QueryResult> {
        let mut implicit_mutation = Some(ImplicitMutation::begin(self)?);
        let result: Result<QueryResult> = (|| {
            let mut catalog_guard = self.lock_catalog()?;
            let schema = catalog_guard.clone_schema();
            validate_statement(
                &crate::parser::ast::Statement::CreateTrigger(statement.clone()),
                &schema,
            )?;

            catalog_guard.create_trigger(crate::catalog::Trigger {
                name: statement.trigger.clone(),
                table_name: statement.table.clone(),
                event: match statement.event {
                    TriggerEvent::Insert => crate::catalog::TriggerEvent::Insert,
                    TriggerEvent::Update => crate::catalog::TriggerEvent::Update,
                    TriggerEvent::Delete => crate::catalog::TriggerEvent::Delete,
                },
                body_sql: statement.body.to_sql(),
                old_alias: match statement.event {
                    TriggerEvent::Insert => None,
                    TriggerEvent::Update | TriggerEvent::Delete => Some("OLD".to_string()),
                },
                new_alias: match statement.event {
                    TriggerEvent::Delete => None,
                    TriggerEvent::Insert | TriggerEvent::Update => Some("NEW".to_string()),
                },
            })?;
            Ok(Self::mutation_result(0))
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

    fn execute_drop_trigger_statement(
        &mut self,
        trigger_name: &str,
        if_exists: bool,
    ) -> Result<QueryResult> {
        let mut implicit_mutation = Some(ImplicitMutation::begin(self)?);
        let result: Result<QueryResult> = (|| {
            let mut catalog_guard = self.lock_catalog()?;
            if if_exists && catalog_guard.get_trigger(trigger_name)?.is_none() {
                Ok(Self::mutation_result(0))
            } else {
                catalog_guard.drop_trigger(trigger_name)?;
                Ok(Self::mutation_result(0))
            }
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
                Ok((result, ctx.catalog, ctx.mutation_events))
            })
        };

        match execution_result {
            Ok((result, updated_schema, mutation_events)) => {
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

                if let Err(err) = self.fire_triggers(mutation_events) {
                    if let Some(implicit_mutation) = implicit_mutation.take() {
                        implicit_mutation.rollback(self)?;
                    }
                    return Err(err);
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
            .map(|(name, metadata)| (name.clone(), metadata.row_count as usize))
            .collect()
    }

    fn fire_triggers(&mut self, mutation_events: Vec<MutationEvent>) -> Result<()> {
        if mutation_events.is_empty() {
            return Ok(());
        }

        if self.trigger_depth >= 32 {
            return Err(HematiteError::ParseError(
                "Trigger recursion limit exceeded".to_string(),
            ));
        }

        self.trigger_depth += 1;
        let result = (|| {
            for event in mutation_events {
                let (table_name, event_kind, old_row, new_row) = match event {
                    MutationEvent::Insert {
                        table_name,
                        new_row,
                    } => (
                        table_name,
                        crate::catalog::TriggerEvent::Insert,
                        None,
                        Some(new_row),
                    ),
                    MutationEvent::Update {
                        table_name,
                        old_row,
                        new_row,
                    } => (
                        table_name,
                        crate::catalog::TriggerEvent::Update,
                        Some(old_row),
                        Some(new_row),
                    ),
                    MutationEvent::Delete {
                        table_name,
                        old_row,
                    } => (
                        table_name,
                        crate::catalog::TriggerEvent::Delete,
                        Some(old_row),
                        None,
                    ),
                };

                let (table, triggers) = {
                    let catalog_guard = self.lock_catalog()?;
                    let table = catalog_guard
                        .get_table_by_name(&table_name)?
                        .ok_or_else(|| {
                            HematiteError::InternalError(format!(
                                "Table '{}' disappeared while firing triggers",
                                table_name
                            ))
                        })?;
                    let mut triggers = catalog_guard
                        .list_triggers()?
                        .into_iter()
                        .filter_map(|name| catalog_guard.get_trigger(&name).ok().flatten())
                        .filter(|trigger| {
                            trigger.table_name == table_name && trigger.event == event_kind
                        })
                        .collect::<Vec<_>>();
                    triggers.sort_by(|left, right| left.name.cmp(&right.name));
                    (table, triggers)
                };

                for trigger in triggers {
                    let trigger_statement =
                        Self::parse_statement(&format!("{};", trigger.body_sql))?;
                    let trigger_statement = substitute_trigger_statement(
                        trigger_statement,
                        &table,
                        old_row.as_ref(),
                        new_row.as_ref(),
                    );
                    if trigger_statement.is_read_only() {
                        let _ = self.execute_read_statement(trigger_statement)?;
                    } else {
                        let _ =
                            self.execute_mutating_statement_in_scope(trigger_statement, false)?;
                    }
                }
            }
            Ok(())
        })();
        self.trigger_depth -= 1;
        result
    }

    pub fn close(&mut self) -> Result<()> {
        if self.transaction.is_some() {
            return Err(HematiteError::InternalError(
                "Cannot close connection with an active transaction".to_string(),
            ));
        }
        let mut catalog_guard = self.lock_catalog()?;
        catalog_guard.flush()
    }

    pub fn journal_mode(&self) -> Result<JournalMode> {
        let catalog_guard = self.lock_catalog()?;
        catalog_guard.journal_mode()
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

    pub fn iter_script<'a>(&'a mut self, sql: &str) -> Result<ScriptIter<'a>> {
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
        let snapshot = catalog_guard.snapshot()?;
        catalog_guard.begin_transaction()?;
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

fn substitute_trigger_statement(
    statement: Statement,
    table: &crate::catalog::Table,
    old_row: Option<&crate::catalog::StoredRow>,
    new_row: Option<&crate::catalog::StoredRow>,
) -> Statement {
    let mut bindings = HashMap::new();
    if let Some(old_row) = old_row {
        for (column, value) in table.columns.iter().zip(old_row.values.iter()) {
            bindings.insert(format!("OLD.{}", column.name), raise_literal_value(value));
        }
    }
    if let Some(new_row) = new_row {
        for (column, value) in table.columns.iter().zip(new_row.values.iter()) {
            bindings.insert(format!("NEW.{}", column.name), raise_literal_value(value));
        }
    }

    substitute_statement_bindings(statement, &bindings)
}

fn substitute_statement_bindings(
    statement: Statement,
    bindings: &HashMap<String, crate::parser::types::LiteralValue>,
) -> Statement {
    match statement {
        Statement::Select(select) => {
            Statement::Select(substitute_select_bindings(select, bindings))
        }
        Statement::Insert(insert) => Statement::Insert(crate::parser::ast::InsertStatement {
            table: insert.table,
            columns: insert.columns,
            source: match insert.source {
                InsertSource::Values(rows) => InsertSource::Values(
                    rows.into_iter()
                        .map(|row| {
                            row.into_iter()
                                .map(|expr| substitute_expression_bindings(expr, bindings))
                                .collect()
                        })
                        .collect(),
                ),
                InsertSource::Select(select) => {
                    InsertSource::Select(Box::new(substitute_select_bindings(*select, bindings)))
                }
            },
            on_duplicate: insert.on_duplicate.map(|assignments| {
                assignments
                    .into_iter()
                    .map(|assignment| crate::parser::ast::UpdateAssignment {
                        column: assignment.column,
                        value: substitute_expression_bindings(assignment.value, bindings),
                    })
                    .collect()
            }),
        }),
        Statement::Update(update) => Statement::Update(crate::parser::ast::UpdateStatement {
            table: update.table,
            target_binding: update.target_binding,
            source: update.source,
            assignments: update
                .assignments
                .into_iter()
                .map(|assignment| crate::parser::ast::UpdateAssignment {
                    column: assignment.column,
                    value: substitute_expression_bindings(assignment.value, bindings),
                })
                .collect(),
            where_clause: update
                .where_clause
                .map(|where_clause| substitute_where_clause_bindings(where_clause, bindings)),
        }),
        Statement::Delete(delete) => Statement::Delete(crate::parser::ast::DeleteStatement {
            table: delete.table,
            target_binding: delete.target_binding,
            source: delete.source,
            where_clause: delete
                .where_clause
                .map(|where_clause| substitute_where_clause_bindings(where_clause, bindings)),
        }),
        other => other,
    }
}

fn substitute_select_bindings(
    select: SelectStatement,
    bindings: &HashMap<String, crate::parser::types::LiteralValue>,
) -> SelectStatement {
    SelectStatement {
        with_clause: select
            .with_clause
            .into_iter()
            .map(|cte| crate::parser::ast::CommonTableExpression {
                name: cte.name,
                recursive: cte.recursive,
                query: Box::new(substitute_select_bindings(*cte.query, bindings)),
            })
            .collect(),
        distinct: select.distinct,
        columns: select
            .columns
            .into_iter()
            .map(|item| match item {
                crate::parser::ast::SelectItem::Expression(expr) => {
                    crate::parser::ast::SelectItem::Expression(substitute_expression_bindings(
                        expr, bindings,
                    ))
                }
                crate::parser::ast::SelectItem::Column(name) => bindings
                    .get(&name)
                    .cloned()
                    .map(crate::parser::ast::Expression::Literal)
                    .map(crate::parser::ast::SelectItem::Expression)
                    .unwrap_or(crate::parser::ast::SelectItem::Column(name)),
                other => other,
            })
            .collect(),
        column_aliases: select.column_aliases,
        from: substitute_table_reference_bindings(select.from, bindings),
        where_clause: select
            .where_clause
            .map(|where_clause| substitute_where_clause_bindings(where_clause, bindings)),
        group_by: select
            .group_by
            .into_iter()
            .map(|expr| substitute_expression_bindings(expr, bindings))
            .collect(),
        having_clause: select
            .having_clause
            .map(|where_clause| substitute_where_clause_bindings(where_clause, bindings)),
        order_by: select.order_by,
        limit: select.limit,
        offset: select.offset,
        set_operation: select
            .set_operation
            .map(|set_operation| crate::parser::ast::SetOperation {
                operator: set_operation.operator,
                right: Box::new(substitute_select_bindings(*set_operation.right, bindings)),
            }),
    }
}

fn substitute_table_reference_bindings(
    table_reference: TableReference,
    bindings: &HashMap<String, crate::parser::types::LiteralValue>,
) -> TableReference {
    match table_reference {
        TableReference::Table(name, alias) => TableReference::Table(name, alias),
        TableReference::Derived { subquery, alias } => TableReference::Derived {
            subquery: Box::new(substitute_select_bindings(*subquery, bindings)),
            alias,
        },
        TableReference::CrossJoin(left, right) => TableReference::CrossJoin(
            Box::new(substitute_table_reference_bindings(*left, bindings)),
            Box::new(substitute_table_reference_bindings(*right, bindings)),
        ),
        TableReference::InnerJoin { left, right, on } => TableReference::InnerJoin {
            left: Box::new(substitute_table_reference_bindings(*left, bindings)),
            right: Box::new(substitute_table_reference_bindings(*right, bindings)),
            on: substitute_condition_bindings(on, bindings),
        },
        TableReference::LeftJoin { left, right, on } => TableReference::LeftJoin {
            left: Box::new(substitute_table_reference_bindings(*left, bindings)),
            right: Box::new(substitute_table_reference_bindings(*right, bindings)),
            on: substitute_condition_bindings(on, bindings),
        },
        TableReference::RightJoin { left, right, on } => TableReference::RightJoin {
            left: Box::new(substitute_table_reference_bindings(*left, bindings)),
            right: Box::new(substitute_table_reference_bindings(*right, bindings)),
            on: substitute_condition_bindings(on, bindings),
        },
        TableReference::FullOuterJoin { left, right, on } => TableReference::FullOuterJoin {
            left: Box::new(substitute_table_reference_bindings(*left, bindings)),
            right: Box::new(substitute_table_reference_bindings(*right, bindings)),
            on: substitute_condition_bindings(on, bindings),
        },
    }
}

fn substitute_where_clause_bindings(
    where_clause: WhereClause,
    bindings: &HashMap<String, crate::parser::types::LiteralValue>,
) -> WhereClause {
    WhereClause {
        conditions: where_clause
            .conditions
            .into_iter()
            .map(|condition| substitute_condition_bindings(condition, bindings))
            .collect(),
    }
}

fn substitute_condition_bindings(
    condition: Condition,
    bindings: &HashMap<String, crate::parser::types::LiteralValue>,
) -> Condition {
    match condition {
        Condition::Comparison {
            left,
            operator,
            right,
        } => Condition::Comparison {
            left: substitute_expression_bindings(left, bindings),
            operator,
            right: substitute_expression_bindings(right, bindings),
        },
        Condition::InList {
            expr,
            values,
            is_not,
        } => Condition::InList {
            expr: substitute_expression_bindings(expr, bindings),
            values: values
                .into_iter()
                .map(|expr| substitute_expression_bindings(expr, bindings))
                .collect(),
            is_not,
        },
        Condition::InSubquery {
            expr,
            subquery,
            is_not,
        } => Condition::InSubquery {
            expr: substitute_expression_bindings(expr, bindings),
            subquery: Box::new(substitute_select_bindings(*subquery, bindings)),
            is_not,
        },
        Condition::Between {
            expr,
            lower,
            upper,
            is_not,
        } => Condition::Between {
            expr: substitute_expression_bindings(expr, bindings),
            lower: substitute_expression_bindings(lower, bindings),
            upper: substitute_expression_bindings(upper, bindings),
            is_not,
        },
        Condition::Like {
            expr,
            pattern,
            is_not,
        } => Condition::Like {
            expr: substitute_expression_bindings(expr, bindings),
            pattern: substitute_expression_bindings(pattern, bindings),
            is_not,
        },
        Condition::Exists { subquery, is_not } => Condition::Exists {
            subquery: Box::new(substitute_select_bindings(*subquery, bindings)),
            is_not,
        },
        Condition::NullCheck { expr, is_not } => Condition::NullCheck {
            expr: substitute_expression_bindings(expr, bindings),
            is_not,
        },
        Condition::Not(condition) => Condition::Not(Box::new(substitute_condition_bindings(
            *condition, bindings,
        ))),
        Condition::Logical {
            left,
            operator,
            right,
        } => Condition::Logical {
            left: Box::new(substitute_condition_bindings(*left, bindings)),
            operator,
            right: Box::new(substitute_condition_bindings(*right, bindings)),
        },
    }
}

fn substitute_expression_bindings(
    expression: Expression,
    bindings: &HashMap<String, crate::parser::types::LiteralValue>,
) -> Expression {
    match expression {
        Expression::Column(name) => bindings
            .get(&name)
            .cloned()
            .map(Expression::Literal)
            .unwrap_or(Expression::Column(name)),
        Expression::Literal(_) | Expression::IntervalLiteral { .. } | Expression::Parameter(_) => {
            expression
        }
        Expression::ScalarSubquery(subquery) => {
            Expression::ScalarSubquery(Box::new(substitute_select_bindings(*subquery, bindings)))
        }
        Expression::Cast { expr, target_type } => Expression::Cast {
            expr: Box::new(substitute_expression_bindings(*expr, bindings)),
            target_type,
        },
        Expression::Case {
            branches,
            else_expr,
        } => Expression::Case {
            branches: branches
                .into_iter()
                .map(|branch| crate::parser::ast::CaseWhenClause {
                    condition: substitute_expression_bindings(branch.condition, bindings),
                    result: substitute_expression_bindings(branch.result, bindings),
                })
                .collect(),
            else_expr: else_expr
                .map(|expr| Box::new(substitute_expression_bindings(*expr, bindings))),
        },
        Expression::ScalarFunctionCall { function, args } => Expression::ScalarFunctionCall {
            function,
            args: args
                .into_iter()
                .map(|expr| substitute_expression_bindings(expr, bindings))
                .collect(),
        },
        Expression::AggregateCall { function, target } => {
            Expression::AggregateCall { function, target }
        }
        Expression::UnaryMinus(expr) => {
            Expression::UnaryMinus(Box::new(substitute_expression_bindings(*expr, bindings)))
        }
        Expression::UnaryNot(expr) => {
            Expression::UnaryNot(Box::new(substitute_expression_bindings(*expr, bindings)))
        }
        Expression::Binary {
            left,
            operator,
            right,
        } => Expression::Binary {
            left: Box::new(substitute_expression_bindings(*left, bindings)),
            operator,
            right: Box::new(substitute_expression_bindings(*right, bindings)),
        },
        Expression::Comparison {
            left,
            operator,
            right,
        } => Expression::Comparison {
            left: Box::new(substitute_expression_bindings(*left, bindings)),
            operator,
            right: Box::new(substitute_expression_bindings(*right, bindings)),
        },
        Expression::InList {
            expr,
            values,
            is_not,
        } => Expression::InList {
            expr: Box::new(substitute_expression_bindings(*expr, bindings)),
            values: values
                .into_iter()
                .map(|expr| substitute_expression_bindings(expr, bindings))
                .collect(),
            is_not,
        },
        Expression::InSubquery {
            expr,
            subquery,
            is_not,
        } => Expression::InSubquery {
            expr: Box::new(substitute_expression_bindings(*expr, bindings)),
            subquery: Box::new(substitute_select_bindings(*subquery, bindings)),
            is_not,
        },
        Expression::Between {
            expr,
            lower,
            upper,
            is_not,
        } => Expression::Between {
            expr: Box::new(substitute_expression_bindings(*expr, bindings)),
            lower: Box::new(substitute_expression_bindings(*lower, bindings)),
            upper: Box::new(substitute_expression_bindings(*upper, bindings)),
            is_not,
        },
        Expression::Like {
            expr,
            pattern,
            is_not,
        } => Expression::Like {
            expr: Box::new(substitute_expression_bindings(*expr, bindings)),
            pattern: Box::new(substitute_expression_bindings(*pattern, bindings)),
            is_not,
        },
        Expression::Exists { subquery, is_not } => Expression::Exists {
            subquery: Box::new(substitute_select_bindings(*subquery, bindings)),
            is_not,
        },
        Expression::NullCheck { expr, is_not } => Expression::NullCheck {
            expr: Box::new(substitute_expression_bindings(*expr, bindings)),
            is_not,
        },
        Expression::Logical {
            left,
            operator,
            right,
        } => Expression::Logical {
            left: Box::new(substitute_expression_bindings(*left, bindings)),
            operator,
            right: Box::new(substitute_expression_bindings(*right, bindings)),
        },
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
