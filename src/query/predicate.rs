//! The `column = literal` equalities a `WHERE` clause guarantees.
//!
//! Access-path selection (the planner) and the lookups that follow it (the executor)
//! both ask the same question — which columns does every matching row pin to one
//! value? — so they share this one answer. Only equalities reachable through `AND`
//! count; `OR`, `NOT` and other comparisons contribute nothing but do not disqualify
//! the rest. Two different literals for one column make the clause unsatisfiable,
//! which `extract_literal_equalities` reports as `None`.

use crate::catalog::{Table, Value};
use crate::parser::ast::{
    ComparisonOperator, Expression, LogicalOperator, SelectStatement, WhereClause,
};
use crate::query::lowering::lower_literal_value;
use std::collections::HashMap;

pub(crate) fn extract_literal_equalities(
    where_clause: &WhereClause,
) -> Option<HashMap<String, Value>> {
    let mut equalities = HashMap::new();
    for condition in &where_clause.conditions {
        if !collect_literal_equalities(condition, &mut equalities) {
            return None;
        }
    }
    Some(equalities)
}

fn collect_literal_equalities(
    condition: &Expression,
    equalities: &mut HashMap<String, Value>,
) -> bool {
    match condition {
        Expression::Comparison {
            left,
            operator: ComparisonOperator::Equal,
            right,
        } => {
            let (column_name, value) = match (left.as_ref(), right.as_ref()) {
                (Expression::Column(column_name), Expression::Literal(value)) => (
                    SelectStatement::column_reference_name(column_name),
                    lower_literal_value(value),
                ),
                (Expression::Literal(value), Expression::Column(column_name)) => (
                    SelectStatement::column_reference_name(column_name),
                    lower_literal_value(value),
                ),
                _ => return true,
            };

            match equalities.get(column_name) {
                Some(existing) if existing != &value => false,
                _ => {
                    equalities.insert(column_name.to_string(), value);
                    true
                }
            }
        }
        Expression::Logical {
            left,
            operator: LogicalOperator::And,
            right,
        } => {
            collect_literal_equalities(left, equalities)
                && collect_literal_equalities(right, equalities)
        }
        // OR/NOT and non-equality predicates are not contradictions; they simply do
        // not contribute guaranteed equality constraints for access-path selection.
        _ => true,
    }
}

/// Whether `column_reference` names `table`'s hidden row id.
///
/// `rowid` means the row id only while the table has no column of that name — a real
/// column called `rowid` is just a column — and only when any qualifier names this
/// table, by its name or by `alias`.
pub(crate) fn names_hidden_rowid(
    table: &Table,
    alias: Option<&str>,
    column_reference: &str,
) -> bool {
    let (qualifier, column_name) = SelectStatement::split_column_reference(column_reference);
    SelectStatement::is_hidden_rowid(column_name)
        && table.get_column_index(column_name).is_none()
        && qualifier.is_none_or(|qualifier| qualifier == table.name || Some(qualifier) == alias)
}

/// The row id a `WHERE` clause pins `table` to, if it pins it to exactly one.
///
/// The planner chooses a row-id lookup from this and the executor performs it, so they
/// must agree: both call here.
pub(crate) fn rowid_lookup(where_clause: &WhereClause, table: &Table) -> Option<u64> {
    let equalities = extract_literal_equalities(where_clause)?;
    // The equalities are keyed by unqualified name, so a qualifier is checked by the
    // filter the executor applies afterwards, not here.
    equalities
        .iter()
        .find(|(name, _)| names_hidden_rowid(table, None, name))
        .and_then(|(_, value)| match value {
            Value::Integer(value) => u64::try_from(*value).ok(),
            Value::BigInt(value) => u64::try_from(*value).ok(),
            Value::UInteger(value) => Some(u64::from(*value)),
            Value::UBigInt(value) => Some(*value),
            _ => None,
        })
}
