//! Query execution runtime context and executor trait.

use crate::catalog::{CatalogEngine, Schema, StoredRow, Value};
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
    pub mutation_events: Vec<MutationEvent>,
}

#[derive(Debug, Clone)]
pub enum MutationEvent {
    Insert {
        table_name: String,
        new_row: StoredRow,
    },
    Update {
        table_name: String,
        old_row: StoredRow,
        new_row: StoredRow,
    },
    Delete {
        table_name: String,
        old_row: StoredRow,
    },
}

impl<'a> ExecutionContext<'a> {
    pub fn for_read(catalog: &Schema, engine: &'a mut CatalogEngine) -> Self {
        Self {
            catalog: catalog.clone(),
            engine,
            mutation_events: Vec::new(),
        }
    }

    pub fn for_mutation(catalog: &Schema, engine: &'a mut CatalogEngine) -> Self {
        Self {
            catalog: catalog.clone(),
            engine,
            mutation_events: Vec::new(),
        }
    }
}

pub trait QueryExecutor {
    fn execute(&mut self, ctx: &mut ExecutionContext<'_>) -> Result<QueryResult>;
}
