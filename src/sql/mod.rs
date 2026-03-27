//! SQL interface module

pub mod connection;
pub mod interface;
pub mod result;
pub(crate) mod script;

pub use connection::*;
pub use interface::*;
pub use result::*;
pub use script::{script_is_complete, ScriptIter};

#[cfg(test)]
mod tests;
