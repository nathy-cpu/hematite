//! Query execution runtime context and executor trait.

use crate::catalog::{CatalogEngine, Schema, Value};
use crate::error::Result;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub affected_rows: usize,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug)]
pub struct ExecutionContext<'a> {
    pub catalog: Schema,
    pub engine: &'a mut CatalogEngine,
}

impl<'a> ExecutionContext<'a> {
    pub fn for_read(catalog: &Schema, engine: &'a mut CatalogEngine) -> Self {
        Self {
            catalog: catalog.clone(),
            engine,
        }
    }

    pub fn for_mutation(catalog: &Schema, engine: &'a mut CatalogEngine) -> Self {
        Self {
            catalog: catalog.clone(),
            engine,
        }
    }
}

pub trait QueryExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext<'_>) -> Result<QueryResult>;
}
