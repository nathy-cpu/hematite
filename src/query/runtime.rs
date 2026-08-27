//! Query execution runtime context and executor trait.

use crate::catalog::{CatalogEngine, Schema, Value};
use crate::error::Result;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub affected_rows: usize,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug)]
pub struct ExecutionContext<'a> {
    /// Shared with the connection that built this context, not copied from it.
    ///
    /// A `Schema` owns a `Table` per table and a `Column` per column, and this
    /// used to deep clone one per statement — 28 allocations on a one-table point
    /// lookup, on top of the copy the planner already took.
    pub catalog: Arc<Schema>,
    pub engine: &'a mut CatalogEngine,
}

impl<'a> ExecutionContext<'a> {
    pub fn for_read(catalog: Arc<Schema>, engine: &'a mut CatalogEngine) -> Self {
        Self { catalog, engine }
    }

    pub fn for_mutation(catalog: Arc<Schema>, engine: &'a mut CatalogEngine) -> Self {
        Self { catalog, engine }
    }
}

pub trait QueryExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext<'_>) -> Result<QueryResult>;
}
