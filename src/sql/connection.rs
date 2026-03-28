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
    Condition, CreateViewStatement, Expression, InsertSource, SelectStatement, Statement,
    TableReference, WhereClause,
};
use crate::parser::{Lexer, Parser};
use crate::query::lowering::raise_literal_value;
use crate::query::validation::validate_statement;
use crate::query::{
    Catalog, CatalogEngine, ExecutionContext, JournalMode, QueryCatalogSnapshot, QueryExecutor,
    QueryPlanner, QueryResult, Schema, Value,
};
use crate::sql::result::ExecutedStatement;
use crate::sql::script::{split_script_tokens, ScriptIter};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug, Clone)]
struct ConnectionTransaction {
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
        let snapshot = catalog_guard.snapshot();
        catalog_guard.begin_transaction()?;
        Ok(Self {
            snapshot: Some(snapshot),
        })
    }

    fn rollback(mut self, connection: &mut Connection) -> Result<()> {
        if let Some(snapshot) = self.snapshot.take() {
            let mut catalog_guard = connection.lock_catalog()?;
            let _ = catalog_guard.rollback_transaction();
            catalog_guard.restore_snapshot(snapshot);
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
                catalog_guard.restore_snapshot(snapshot);
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
            Statement::Explain(explain) => Ok(Statement::Explain(crate::parser::ast::ExplainStatement {
                statement: Box::new(Self::expand_views_in_statement(*explain.statement, schema)?),
            })),
            Statement::Select(select) => Ok(Statement::Select(Self::expand_views_in_select(
                select, schema,
            )?)),
            Statement::Insert(mut insert) => {
                if let InsertSource::Select(select) = insert.source {
                    insert.source =
                        InsertSource::Select(Box::new(Self::expand_views_in_select(*select, schema)?));
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

    fn expand_views_in_select(mut select: SelectStatement, schema: &Schema) -> Result<SelectStatement> {
        for cte in &mut select.with_clause {
            cte.query = Box::new(Self::expand_views_in_select((*cte.query).clone(), schema)?);
        }
        let original_from = select.from.clone();
        let select_context = select.clone();
        select.from = Self::expand_views_in_table_reference(original_from, &select_context, schema)?;
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
                if select.lookup_cte(&table_name).is_some() || schema.get_table_by_name(&table_name).is_some() {
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
                Box::new(Self::expand_views_in_table_reference(*left, select, schema)?),
                Box::new(Self::expand_views_in_table_reference(*right, select, schema)?),
            )),
            TableReference::InnerJoin { left, right, mut on } => {
                Self::expand_views_in_condition(&mut on, schema)?;
                Ok(TableReference::InnerJoin {
                    left: Box::new(Self::expand_views_in_table_reference(*left, select, schema)?),
                    right: Box::new(Self::expand_views_in_table_reference(*right, select, schema)?),
                    on,
                })
            }
            TableReference::LeftJoin { left, right, mut on } => {
                Self::expand_views_in_condition(&mut on, schema)?;
                Ok(TableReference::LeftJoin {
                    left: Box::new(Self::expand_views_in_table_reference(*left, select, schema)?),
                    right: Box::new(Self::expand_views_in_table_reference(*right, select, schema)?),
                    on,
                })
            }
            TableReference::RightJoin { left, right, mut on } => {
                Self::expand_views_in_condition(&mut on, schema)?;
                Ok(TableReference::RightJoin {
                    left: Box::new(Self::expand_views_in_table_reference(*left, select, schema)?),
                    right: Box::new(Self::expand_views_in_table_reference(*right, select, schema)?),
                    on,
                })
            }
            TableReference::FullOuterJoin { left, right, mut on } => {
                Self::expand_views_in_condition(&mut on, schema)?;
                Ok(TableReference::FullOuterJoin {
                    left: Box::new(Self::expand_views_in_table_reference(*left, select, schema)?),
                    right: Box::new(Self::expand_views_in_table_reference(*right, select, schema)?),
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
            Condition::Between { expr, lower, upper, .. } => {
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
            Expression::Case { branches, else_expr } => {
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
            Expression::Between { expr, lower, upper, .. } => {
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
            crate::parser::ast::Statement::Savepoint(_)
            | crate::parser::ast::Statement::RollbackToSavepoint(_)
            | crate::parser::ast::Statement::ReleaseSavepoint(_) => {
                return Err(HematiteError::ParseError(
                    "SAVEPOINT statements are not implemented yet".to_string(),
                ));
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
            crate::parser::ast::Statement::CreateView(create_view) => {
                return self.execute_create_view_statement(create_view);
            }
            crate::parser::ast::Statement::DropView(drop_view) => {
                return self.execute_drop_view_statement(&drop_view.view, drop_view.if_exists);
            }
            crate::parser::ast::Statement::CreateTrigger(_)
            | crate::parser::ast::Statement::DropTrigger(_) => {
                return Err(HematiteError::ParseError(
                    "View and trigger statements are not implemented yet".to_string(),
                ));
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
        let table = catalog_guard
            .get_table_by_name(table_name)?
            .ok_or_else(|| {
                HematiteError::ParseError(format!("Table '{}' does not exist", table_name))
            })?;
        drop(catalog_guard);

        let rows = table
            .columns
            .iter()
            .map(|column| {
                let is_unique = table.secondary_indexes.iter().any(|index| {
                    index.unique && index.column_indices == vec![column.id.as_u32() as usize]
                });
                vec![
                    Value::Text(column.name.clone()),
                    Value::Text(column.data_type.name().to_string()),
                    Value::Boolean(column.nullable),
                    match &column.default_value {
                        Some(default) => Value::Text(format!("{default:?}")),
                        None => Value::Null,
                    },
                    Value::Boolean(column.primary_key),
                    Value::Boolean(is_unique),
                    Value::Boolean(column.auto_increment),
                ]
            })
            .collect();

        Ok(QueryResult {
            affected_rows: 0,
            columns: vec![
                "column".to_string(),
                "type".to_string(),
                "nullable".to_string(),
                "default".to_string(),
                "primary_key".to_string(),
                "unique".to_string(),
                "auto_increment".to_string(),
            ],
            rows,
        })
    }

    fn execute_show_tables_statement(&mut self) -> Result<QueryResult> {
        let catalog_guard = self.lock_catalog()?;
        let mut tables = catalog_guard.list_tables()?;
        drop(catalog_guard);
        tables.sort_by(|left, right| left.1.cmp(&right.1));

        Ok(QueryResult {
            affected_rows: 0,
            columns: vec!["table_name".to_string()],
            rows: tables
                .into_iter()
                .map(|(_, name)| vec![Value::Text(name)])
                .collect(),
        })
    }

    fn execute_show_views_statement(&mut self) -> Result<QueryResult> {
        let catalog_guard = self.lock_catalog()?;
        let mut views = catalog_guard.list_views()?;
        drop(catalog_guard);
        views.sort();

        Ok(QueryResult {
            affected_rows: 0,
            columns: vec!["view_name".to_string()],
            rows: views.into_iter().map(|name| vec![Value::Text(name)]).collect(),
        })
    }

    fn execute_create_view_statement(
        &mut self,
        statement: crate::parser::ast::CreateViewStatement,
    ) -> Result<QueryResult> {
        let mut implicit_mutation = Some(ImplicitMutation::begin(self)?);
        let result = {
            let mut catalog_guard = self.lock_catalog()?;
            let schema = catalog_guard.clone_schema();
            let expanded_query = Self::expand_views_in_select(statement.query.clone(), &schema)?;
            validate_statement(
                &crate::parser::ast::Statement::CreateView(CreateViewStatement {
                    view: statement.view.clone(),
                    if_not_exists: statement.if_not_exists,
                    query: expanded_query,
                }),
                &schema,
            )?;

            if statement.if_not_exists && catalog_guard.get_view(&statement.view)?.is_some() {
                Ok(Self::mutation_result(0))
            } else {
                let column_names = statement
                    .query
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(index, _)| {
                        statement.query.output_name(index).ok_or_else(|| {
                            HematiteError::ParseError(format!(
                                "View '{}' requires a name for projected column {}",
                                statement.view,
                                index + 1
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                catalog_guard.create_view(crate::catalog::View {
                    name: statement.view.clone(),
                    query_sql: statement.query.to_sql(),
                    column_names,
                    dependencies: statement.query.dependency_names(),
                })?;
                Ok(Self::mutation_result(0))
            }
        };

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
        let result = {
            let mut catalog_guard = self.lock_catalog()?;
            if if_exists && catalog_guard.get_view(view_name)?.is_none() {
                Ok(Self::mutation_result(0))
            } else {
                catalog_guard.drop_view(view_name)?;
                Ok(Self::mutation_result(0))
            }
        };

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
        let persists_schema = statement.mutates_schema();
        let (schema, mut executor) = self.plan_executor(statement)?;
        let mut implicit_mutation = Some(ImplicitMutation::begin(self)?);

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
                        implicit_mutation
                            .take()
                            .expect("implicit mutation should be present")
                            .rollback(self)?;
                        return Err(err);
                    }
                }

                if let Err(err) = implicit_mutation
                    .take()
                    .expect("implicit mutation should be present")
                    .commit(self)
                {
                    return Err(err);
                }

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

    fn plan_executor(
        &self,
        statement: crate::parser::ast::Statement,
    ) -> Result<(Schema, Box<dyn QueryExecutor>)> {
        let (schema, table_row_counts) = self.read_planning_state()?;
        let statement = Self::expand_views_in_statement(statement, &schema)?;
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
        let snapshot = catalog_guard.snapshot();
        catalog_guard.begin_transaction()?;
        drop(catalog_guard);
        self.transaction = Some(ConnectionTransaction { snapshot });
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn schema_snapshot(&self) -> Result<Schema> {
        let catalog_guard = self.lock_catalog()?;
        Ok(catalog_guard.clone_schema())
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
                catalog_guard.restore_snapshot(state.snapshot);
                Err(err)
            }
        }
    }

    fn rollback_active_transaction(&mut self) -> Result<()> {
        let state = self.take_active_transaction("roll back")?;
        let mut catalog_guard = self.lock_catalog()?;
        catalog_guard.rollback_transaction()?;
        catalog_guard.restore_snapshot(state.snapshot);
        Ok(())
    }
}
