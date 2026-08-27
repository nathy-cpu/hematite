//! Query processing module

pub mod executor;
pub mod logest;
pub(crate) mod lowering;
pub(crate) mod metadata;
pub mod optimizer;
pub mod plan;
pub mod planner;
pub(crate) mod predicate;
pub(crate) mod rewrite;
pub mod runtime;
pub(crate) mod validation;

pub(crate) use crate::catalog::catalog::CatalogSnapshot as QueryCatalogSnapshot;
pub use crate::catalog::{Catalog, CatalogEngine, DecimalValue, JournalMode, Schema, Value};
pub use executor::*;
pub use optimizer::*;
pub use planner::*;
pub use runtime::*;

#[cfg(test)]
mod tests;
