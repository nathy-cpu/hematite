use crate::catalog::Value;
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

