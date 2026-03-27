//! Query processing module

pub mod executor;
pub(crate) mod lowering;
pub mod optimizer;
pub mod plan;
pub mod planner;
pub(crate) mod predicate;
pub mod runtime;
pub(crate) mod validation;

pub(crate) use crate::catalog::catalog::CatalogSnapshot as QueryCatalogSnapshot;
pub use crate::catalog::{
    Catalog, CatalogEngine, DateTimeValue, DateValue, DecimalValue, JournalMode, Schema, TimeValue,
    TimeWithTimeZoneValue, TimestampValue, Value,
};
pub use executor::*;
pub use optimizer::*;
pub use plan::*;
pub use planner::*;
pub use runtime::*;

#[cfg(test)]
mod tests;
