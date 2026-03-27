use crate::catalog::Value;
use crate::parser::ast::{
    ComparisonOperator, Condition, Expression, LogicalOperator, SelectStatement, WhereClause,
};
use crate::query::lowering::lower_literal_value;
use std::collections::HashMap;

pub(crate) fn extract_literal_equalities(
    where_clause: &WhereClause,
) -> Option<HashMap<String, Value>> {
    let mut equalities = HashMap::new();
    for condition in &where_clause.conditions {
        collect_literal_equalities(condition, &mut equalities)?;
    }
    Some(equalities)
}

fn collect_literal_equalities(
    condition: &Condition,
    equalities: &mut HashMap<String, Value>,
) -> Option<()> {
    match condition {
        Condition::Comparison {
            left,
            operator: ComparisonOperator::Equal,
            right,
        } => {
            let (column_name, value) = match (left, right) {
                (Expression::Column(column_name), Expression::Literal(value)) => (
                    SelectStatement::column_reference_name(column_name),
                    lower_literal_value(value),
                ),
                (Expression::Literal(value), Expression::Column(column_name)) => (
                    SelectStatement::column_reference_name(column_name),
                    lower_literal_value(value),
                ),
                _ => return None,
            };

            match equalities.get(column_name) {
                Some(existing) if existing != &value => None,
                _ => {
                    equalities.insert(column_name.to_string(), value);
                    Some(())
                }
            }
        }
        Condition::Logical {
            left,
            operator: LogicalOperator::And,
            right,
        } => {
            collect_literal_equalities(left, equalities)?;
            collect_literal_equalities(right, equalities)
        }
        _ => None,
    }
}
