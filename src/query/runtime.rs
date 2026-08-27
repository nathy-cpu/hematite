//! Query execution runtime context and executor trait.

use crate::catalog::{CatalogEngine, Schema, Value};
use crate::error::Result;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub affected_rows: usize,
    pub columns: Vec<String>,
    /// Every row's values end to end, `columns.len()` of them per row.
    ///
    /// A row used to be its own `Vec<Value>`, which meant one allocation per row
    /// returned — 24 % of a full scan's instructions went to `malloc` and `free`
    /// for it, and a scan allocated exactly 1.02 blocks per row. Rows are slices
    /// of one buffer now; see [`QueryResult::rows`].
    values: Vec<Value>,
}

impl QueryResult {
    /// Builds a result from rows that are already separate vectors.
    ///
    /// The general path, for executors that assemble rows one at a time — sorting,
    /// grouping and joining all want an owned row to move around. Flattening here
    /// costs one pass and the per-row vectors are dropped straight after.
    pub fn new(affected_rows: usize, columns: Vec<String>, rows: Vec<Vec<Value>>) -> Self {
        let mut values = Vec::with_capacity(rows.len() * columns.len().max(1));
        for row in rows {
            values.extend(row);
        }
        Self {
            affected_rows,
            columns,
            values,
        }
    }

    /// Builds a result from values already laid out row-major.
    ///
    /// The scan path, which decodes straight into one buffer and never builds a
    /// row vector at all.
    pub fn from_values(affected_rows: usize, columns: Vec<String>, values: Vec<Value>) -> Self {
        debug_assert!(
            columns.is_empty() || values.len().is_multiple_of(columns.len()),
            "{} values do not divide into rows of {}",
            values.len(),
            columns.len()
        );
        Self {
            affected_rows,
            columns,
            values,
        }
    }

    /// The rows, as slices into the one buffer.
    pub fn rows(&self) -> Rows<'_> {
        Rows {
            values: &self.values,
            width: self.columns.len(),
        }
    }

    /// The rows as separate vectors, for a caller that needs to own them.
    pub fn into_row_vecs(self) -> Vec<Vec<Value>> {
        let width = self.columns.len();
        if width == 0 {
            return Vec::new();
        }
        self.values
            .chunks_exact(width)
            .map(<[Value]>::to_vec)
            .collect()
    }

    /// The flat buffer, for a caller that keeps it flat.
    pub fn into_values(self) -> Vec<Value> {
        self.values
    }
}

/// A view over a [`QueryResult`]'s rows.
///
/// Indexing and iteration hand back `&[Value]` slices of the result's own buffer.
/// It compares equal to a `Vec<Vec<Value>>` so that an assertion written against
/// the old shape still says what it said.
#[derive(Clone, Copy)]
pub struct Rows<'a> {
    values: &'a [Value],
    width: usize,
}

impl<'a> Rows<'a> {
    pub fn len(&self) -> usize {
        self.values.len().checked_div(self.width).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, index: usize) -> Option<&'a [Value]> {
        if self.width == 0 {
            return None;
        }
        self.values.get(index * self.width..(index + 1) * self.width)
    }

    pub fn first(&self) -> Option<&'a [Value]> {
        self.get(0)
    }

    pub fn iter(&self) -> impl Iterator<Item = &'a [Value]> + 'a {
        if self.width == 0 {
            [].chunks_exact(1)
        } else {
            self.values.chunks_exact(self.width)
        }
    }

    pub fn to_vec(&self) -> Vec<Vec<Value>> {
        self.iter().map(<[Value]>::to_vec).collect()
    }
}

impl<'a> IntoIterator for &Rows<'a> {
    type Item = &'a [Value];
    type IntoIter = std::slice::ChunksExact<'a, Value>;

    fn into_iter(self) -> Self::IntoIter {
        (*self).into_iter()
    }
}

impl<'a> IntoIterator for Rows<'a> {
    type Item = &'a [Value];
    type IntoIter = std::slice::ChunksExact<'a, Value>;

    fn into_iter(self) -> Self::IntoIter {
        if self.width == 0 {
            [].chunks_exact(1)
        } else {
            self.values.chunks_exact(self.width)
        }
    }
}

impl std::ops::Index<usize> for Rows<'_> {
    type Output = [Value];

    fn index(&self, index: usize) -> &[Value] {
        self.get(index).expect("row index out of bounds")
    }
}

impl std::fmt::Debug for Rows<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl PartialEq<Vec<Vec<Value>>> for Rows<'_> {
    fn eq(&self, other: &Vec<Vec<Value>>) -> bool {
        self.len() == other.len()
            && self.iter().zip(other).all(|(left, right)| left == right.as_slice())
    }
}

impl PartialEq<Rows<'_>> for Vec<Vec<Value>> {
    fn eq(&self, other: &Rows<'_>) -> bool {
        other == self
    }
}

impl PartialEq for Rows<'_> {
    fn eq(&self, other: &Rows<'_>) -> bool {
        self.width == other.width && self.values == other.values
    }
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
