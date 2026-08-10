//! AST-level query rewrites inspired by SQLite's select.c / whereexpr.c.
//!
//! These run *before* the planner sees the tree. Each pass is a narrow,
//! semantics-preserving transformation that makes later planning cheaper
//! or enables better access paths.

use crate::parser::ast::*;

/// Applies all safe AST rewrites to a SELECT statement.
pub(crate) fn rewrite_select(select: &mut SelectStatement) {
    // Order matters: some passes enable others.
    for cte in &mut select.with_clause {
        rewrite_select(&mut cte.query);
    }

    rewrite_table_reference(&mut select.from);

    remove_subquery_order_by(select);
    promote_having_to_where(select);

    if let Some(set_op) = &mut select.set_operation {
        rewrite_select(&mut set_op.right);
    }
}

fn rewrite_table_reference(from: &mut TableReference) {
    match from {
        TableReference::Derived { subquery, .. } => rewrite_select(subquery),
        TableReference::CrossJoin(l, r) => {
            rewrite_table_reference(l);
            rewrite_table_reference(r);
        }
        TableReference::InnerJoin { left, right, .. }
        | TableReference::LeftJoin { left, right, .. }
        | TableReference::RightJoin { left, right, .. }
        | TableReference::FullOuterJoin { left, right, .. } => {
            rewrite_table_reference(left);
            rewrite_table_reference(right);
        }
        TableReference::Table(_, _) => {}
    }
}

// ── 1. Subquery ORDER BY removal ─────────────────────────────────────────────
//
// If a FROM-clause subquery has ORDER BY but the outer query doesn't depend on
// it (no LIMIT in the subquery), strip it.

fn remove_subquery_order_by(select: &mut SelectStatement) {
    remove_order_by_from_table_ref(&mut select.from);
}

fn remove_order_by_from_table_ref(from: &mut TableReference) {
    match from {
        TableReference::Derived { subquery, .. } => {
            if can_remove_subquery_order(subquery) {
                subquery.order_by.clear();
            }
            // Recurse into the subquery's own FROM
            remove_order_by_from_table_ref(&mut subquery.from);
        }
        TableReference::CrossJoin(l, r) => {
            remove_order_by_from_table_ref(l);
            remove_order_by_from_table_ref(r);
        }
        TableReference::InnerJoin { left, right, .. }
        | TableReference::LeftJoin { left, right, .. }
        | TableReference::RightJoin { left, right, .. }
        | TableReference::FullOuterJoin { left, right, .. } => {
            remove_order_by_from_table_ref(left);
            remove_order_by_from_table_ref(right);
        }
        TableReference::Table(_, _) => {}
    }
}

fn can_remove_subquery_order(sub: &SelectStatement) -> bool {
    if sub.order_by.is_empty() {
        return false;
    }
    // Cannot remove if LIMIT is present (ordering affects which rows survive).
    if sub.limit.is_some() {
        return false;
    }
    // Cannot remove from compound queries (UNION etc).
    if sub.set_operation.is_some() {
        return false;
    }
    true
}

// ── 2. HAVING → WHERE promotion ──────────────────────────────────────────────
//
// A HAVING term that depends only on GROUP BY expressions or constants can be
// evaluated earlier in the WHERE phase, reducing rows entering aggregation.

fn promote_having_to_where(select: &mut SelectStatement) {
    if select.group_by.is_empty() {
        return;
    }
    let Some(having) = select.having_clause.as_mut() else {
        return;
    };

    let group_columns: Vec<String> = select
        .group_by
        .iter()
        .filter_map(|expr| match expr {
            Expression::Column(name) => Some(name.clone()),
            _ => None,
        })
        .collect();

    if group_columns.is_empty() {
        return;
    }

    let mut promote = Vec::new();
    let mut keep = Vec::new();

    for condition in std::mem::take(&mut having.conditions) {
        if condition_depends_only_on(&condition, &group_columns) {
            promote.push(condition);
        } else {
            keep.push(condition);
        }
    }

    if promote.is_empty() {
        having.conditions = keep;
        return;
    }

    having.conditions = keep;
    if having.conditions.is_empty() {
        select.having_clause = None;
    }

    match &mut select.where_clause {
        Some(wc) => wc.conditions.extend(promote),
        None => {
            select.where_clause = Some(WhereClause {
                conditions: promote,
            });
        }
    }
}

fn condition_depends_only_on(cond: &Expression, allowed_columns: &[String]) -> bool {
    match cond {
        Expression::Comparison { left, right, .. } => {
            expr_depends_only_on(left, allowed_columns)
                && expr_depends_only_on(right, allowed_columns)
        }
        Expression::NullCheck { expr, .. } => expr_depends_only_on(expr, allowed_columns),
        Expression::InList { expr, values, .. } => {
            expr_depends_only_on(expr, allowed_columns)
                && values
                    .iter()
                    .all(|v| expr_depends_only_on(v, allowed_columns))
        }
        Expression::Between {
            expr, lower, upper, ..
        } => {
            expr_depends_only_on(expr, allowed_columns)
                && expr_depends_only_on(lower, allowed_columns)
                && expr_depends_only_on(upper, allowed_columns)
        }
        Expression::Like { expr, pattern, .. } => {
            expr_depends_only_on(expr, allowed_columns)
                && expr_depends_only_on(pattern, allowed_columns)
        }
        Expression::UnaryNot(inner) => condition_depends_only_on(inner, allowed_columns),
        Expression::Logical { left, right, .. } => {
            condition_depends_only_on(left, allowed_columns)
                && condition_depends_only_on(right, allowed_columns)
        }
        // Subqueries are never safe to promote.
        Expression::Exists { .. } | Expression::InSubquery { .. } => false,
        other => expr_depends_only_on(other, allowed_columns),
    }
}

fn expr_depends_only_on(expr: &Expression, allowed_columns: &[String]) -> bool {
    match expr {
        Expression::Literal(_) | Expression::Parameter(_) => true,
        Expression::Column(name) => {
            let col = SelectStatement::column_reference_name(name);
            allowed_columns
                .iter()
                .any(|c| SelectStatement::column_reference_name(c) == col)
        }
        Expression::Binary { left, right, .. }
        | Expression::Comparison { left, right, .. }
        | Expression::Logical { left, right, .. } => {
            expr_depends_only_on(left, allowed_columns)
                && expr_depends_only_on(right, allowed_columns)
        }
        Expression::UnaryMinus(inner)
        | Expression::UnaryNot(inner)
        | Expression::Cast { expr: inner, .. } => expr_depends_only_on(inner, allowed_columns),
        Expression::ScalarFunctionCall { args, .. } => args
            .iter()
            .all(|a| expr_depends_only_on(a, allowed_columns)),
        Expression::NullCheck { expr, .. } => expr_depends_only_on(expr, allowed_columns),
        Expression::Case {
            branches,
            else_expr,
        } => {
            branches.iter().all(|b| {
                expr_depends_only_on(&b.condition, allowed_columns)
                    && expr_depends_only_on(&b.result, allowed_columns)
            }) && else_expr
                .as_ref()
                .map_or(true, |e| expr_depends_only_on(e, allowed_columns))
        }
        Expression::InList { expr, values, .. } => {
            expr_depends_only_on(expr, allowed_columns)
                && values
                    .iter()
                    .all(|v| expr_depends_only_on(v, allowed_columns))
        }
        Expression::Between {
            expr, lower, upper, ..
        } => {
            expr_depends_only_on(expr, allowed_columns)
                && expr_depends_only_on(lower, allowed_columns)
                && expr_depends_only_on(upper, allowed_columns)
        }
        Expression::Like { expr, pattern, .. } => {
            expr_depends_only_on(expr, allowed_columns)
                && expr_depends_only_on(pattern, allowed_columns)
        }
        // Subqueries / aggregates / EXISTS are not safe.
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::LiteralValue;

    fn col(name: &str) -> Expression {
        Expression::Column(name.to_string())
    }

    fn make_select(where_clause: Option<WhereClause>) -> SelectStatement {
        SelectStatement {
            with_clause: Vec::new(),
            distinct: false,
            columns: vec![SelectItem::Wildcard],
            column_aliases: vec![],
            from: TableReference::Table("t".to_string(), None),
            where_clause,
            group_by: Vec::new(),
            having_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operation: None,
        }
    }

    #[test]
    fn test_having_to_where_promotion() {
        let mut select = make_select(None);
        select.group_by = vec![col("category")];
        select.having_clause = Some(WhereClause {
            conditions: vec![Expression::Comparison {
                left: Box::new(col("category")),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expression::Literal(LiteralValue::Text("A".to_string()))),
            }],
        });
        promote_having_to_where(&mut select);
        // HAVING should be gone, WHERE should have the promoted condition
        assert!(select.having_clause.is_none());
        assert_eq!(select.where_clause.as_ref().unwrap().conditions.len(), 1);
    }

    #[test]
    fn test_subquery_order_by_removal() {
        let sub = SelectStatement {
            order_by: vec![OrderByItem {
                column: "id".to_string(),
                direction: SortDirection::Asc,
            }],
            ..make_select(None)
        };
        let mut outer = make_select(None);
        outer.from = TableReference::Derived {
            subquery: Box::new(sub),
            alias: "s".to_string(),
        };
        rewrite_select(&mut outer);
        match &outer.from {
            TableReference::Derived { subquery, .. } => {
                assert!(subquery.order_by.is_empty());
            }
            _ => panic!("Expected Derived"),
        }
    }

    #[test]
    fn test_subquery_order_by_kept_with_limit() {
        let sub = SelectStatement {
            order_by: vec![OrderByItem {
                column: "id".to_string(),
                direction: SortDirection::Asc,
            }],
            limit: Some(10),
            ..make_select(None)
        };
        let mut outer = make_select(None);
        outer.from = TableReference::Derived {
            subquery: Box::new(sub),
            alias: "s".to_string(),
        };
        rewrite_select(&mut outer);
        match &outer.from {
            TableReference::Derived { subquery, .. } => {
                assert_eq!(subquery.order_by.len(), 1);
            }
            _ => panic!("Expected Derived"),
        }
    }
}
