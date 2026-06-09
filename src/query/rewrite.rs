//! AST-level query rewrites inspired by SQLite's select.c / whereexpr.c.
//!
//! These run *before* the planner sees the tree. Each pass is a narrow,
//! semantics-preserving transformation that makes later planning cheaper
//! or enables better access paths.

#![allow(dead_code)]

use crate::parser::ast::*;
use crate::parser::LiteralValue;

/// Applies all safe AST rewrites to a SELECT statement.
pub(crate) fn rewrite_select(select: &mut SelectStatement) {
    // Order matters: some passes enable others.
    for cte in &mut select.with_clause {
        rewrite_select(&mut cte.query);
    }

    rewrite_table_reference(&mut select.from);

    remove_subquery_order_by(select);
    promote_having_to_where(select);
    // NOTE: Constant propagation is disabled pending a fix for self-substitution
    // (it turns `id = 1` into `1 = 1`, breaking the WHERE clause).
    // propagate_constants_in_where(&mut select.where_clause);
    decompose_between_in_where(&mut select.where_clause);
    convert_or_to_in_in_where(&mut select.where_clause);

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
// it (no LIMIT, no window functions in the subquery), strip it.

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
    // Cannot remove if window functions are used (ordering affects window output).
    if sub
        .columns
        .iter()
        .any(|c| matches!(c, SelectItem::Window { .. }))
    {
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
        Expression::Literal(_) | Expression::Parameter(_) | Expression::IntervalLiteral { .. } => {
            true
        }
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

// ── 3. Constant propagation ──────────────────────────────────────────────────
//
// When `a = 5` appears in a top-level AND, substitute `a` references in
// sibling terms with the constant value.

pub(crate) fn propagate_constants_in_where(where_clause: &mut Option<WhereClause>) {
    let Some(wc) = where_clause.as_mut() else {
        return;
    };

    // Collect column=literal equalities from top-level conditions.
    let mut substitutions: Vec<(String, LiteralValue)> = Vec::new();
    for cond in &wc.conditions {
        if let Some((col, lit)) = extract_column_literal_eq(cond) {
            substitutions.push((col, lit));
        }
    }

    if substitutions.is_empty() {
        return;
    }

    // Apply substitutions to all conditions (the original equality stays too).
    let mut changed = true;
    let mut iterations = 0;
    while changed && iterations < 4 {
        changed = false;
        iterations += 1;
        for cond in &mut wc.conditions {
            changed |= substitute_in_condition(cond, &substitutions);
        }
    }
}

fn extract_column_literal_eq(cond: &Expression) -> Option<(String, LiteralValue)> {
    match cond {
        Expression::Comparison {
            left,
            operator: ComparisonOperator::Equal,
            right,
        } => match (left.as_ref(), right.as_ref()) {
            (Expression::Column(col), Expression::Literal(lit)) => Some((col.clone(), lit.clone())),
            (Expression::Literal(lit), Expression::Column(col)) => Some((col.clone(), lit.clone())),
            _ => None,
        },
        _ => None,
    }
}

fn substitute_in_condition(cond: &mut Expression, subs: &[(String, LiteralValue)]) -> bool {
    match cond {
        Expression::Comparison { left, right, .. } => {
            let a = substitute_in_expr(left, subs);
            let b = substitute_in_expr(right, subs);
            a || b
        }
        Expression::Between {
            expr, lower, upper, ..
        } => {
            substitute_in_expr(expr, subs)
                | substitute_in_expr(lower, subs)
                | substitute_in_expr(upper, subs)
        }
        Expression::InList { expr, values, .. } => {
            let mut c = substitute_in_expr(expr, subs);
            for v in values {
                c |= substitute_in_expr(v, subs);
            }
            c
        }
        Expression::Like { expr, pattern, .. } => {
            substitute_in_expr(expr, subs) | substitute_in_expr(pattern, subs)
        }
        Expression::NullCheck { expr, .. } => substitute_in_expr(expr, subs),
        Expression::UnaryNot(inner) => substitute_in_condition(inner, subs),
        Expression::Logical { left, right, .. } => {
            substitute_in_condition(left, subs) | substitute_in_condition(right, subs)
        }
        Expression::InSubquery { expr, .. } => substitute_in_expr(expr, subs),
        Expression::Exists { .. } => false,
        other => substitute_in_expr(other, subs),
    }
}

fn substitute_in_expr(expr: &mut Expression, subs: &[(String, LiteralValue)]) -> bool {
    match expr {
        Expression::Column(col) => {
            let col_name = SelectStatement::column_reference_name(col);
            for (sub_col, sub_lit) in subs {
                let sub_name = SelectStatement::column_reference_name(sub_col);
                if col_name == sub_name {
                    *expr = Expression::Literal(sub_lit.clone());
                    return true;
                }
            }
            false
        }
        Expression::Binary { left, right, .. }
        | Expression::Comparison { left, right, .. }
        | Expression::Logical { left, right, .. } => {
            substitute_in_expr(left, subs) | substitute_in_expr(right, subs)
        }
        Expression::UnaryMinus(inner)
        | Expression::UnaryNot(inner)
        | Expression::Cast { expr: inner, .. } => substitute_in_expr(inner, subs),
        Expression::ScalarFunctionCall { args, .. } => {
            let mut c = false;
            for a in args {
                c |= substitute_in_expr(a, subs);
            }
            c
        }
        Expression::Case {
            branches,
            else_expr,
        } => {
            let mut c = false;
            for b in branches {
                c |= substitute_in_expr(&mut b.condition, subs);
                c |= substitute_in_expr(&mut b.result, subs);
            }
            if let Some(e) = else_expr {
                c |= substitute_in_expr(e, subs);
            }
            c
        }
        Expression::NullCheck { expr: inner, .. } => substitute_in_expr(inner, subs),
        Expression::InList {
            expr: inner,
            values,
            ..
        } => {
            let mut c = substitute_in_expr(inner, subs);
            for v in values {
                c |= substitute_in_expr(v, subs);
            }
            c
        }
        Expression::Between {
            expr: inner,
            lower,
            upper,
            ..
        } => {
            substitute_in_expr(inner, subs)
                | substitute_in_expr(lower, subs)
                | substitute_in_expr(upper, subs)
        }
        Expression::Like {
            expr: inner,
            pattern,
            ..
        } => substitute_in_expr(inner, subs) | substitute_in_expr(pattern, subs),
        _ => false,
    }
}

// ── 4. BETWEEN decomposition ─────────────────────────────────────────────────
//
// `x BETWEEN a AND b` → adds virtual terms `x >= a` AND `x <= b` so the
// planner can recognise range scans.

fn decompose_between_in_where(where_clause: &mut Option<WhereClause>) {
    let Some(wc) = where_clause.as_mut() else {
        return;
    };

    let mut extra = Vec::new();
    for cond in &wc.conditions {
        if let Some((ge, le)) = decompose_between(cond) {
            extra.push(ge);
            extra.push(le);
        }
    }
    // We keep the original BETWEEN (it's the semantic anchor) and add the
    // decomposed range terms as additional conditions.
    wc.conditions.extend(extra);
}

fn decompose_between(cond: &Expression) -> Option<(Expression, Expression)> {
    match cond {
        Expression::Between {
            expr,
            lower,
            upper,
            is_not: false,
        } => Some((
            Expression::Comparison {
                left: expr.clone(),
                operator: ComparisonOperator::GreaterThanOrEqual,
                right: lower.clone(),
            },
            Expression::Comparison {
                left: expr.clone(),
                operator: ComparisonOperator::LessThanOrEqual,
                right: upper.clone(),
            },
        )),
        _ => None,
    }
}

// ── 5. OR-to-IN conversion ──────────────────────────────────────────────────
//
// `x = 1 OR x = 2 OR x = 3` → `x IN (1, 2, 3)`

fn convert_or_to_in_in_where(where_clause: &mut Option<WhereClause>) {
    let Some(wc) = where_clause.as_mut() else {
        return;
    };

    for cond in &mut wc.conditions {
        try_convert_or_to_in(cond);
    }
}

fn try_convert_or_to_in(cond: &mut Expression) {
    // Recurse into nested Logical nodes first.
    if let Expression::Logical {
        left,
        right,
        operator: LogicalOperator::And,
    } = cond
    {
        try_convert_or_to_in(left);
        try_convert_or_to_in(right);
        return;
    }

    if !matches!(
        cond,
        Expression::Logical {
            operator: LogicalOperator::Or,
            ..
        }
    ) {
        return;
    }

    // Collect all OR branches.
    let mut branches = Vec::new();
    collect_or_branches(cond, &mut branches);

    if branches.len() < 2 {
        return;
    }

    // Check: all branches are `col = literal` on the same column.
    let first_col = match &branches[0] {
        Expression::Comparison {
            left,
            operator: ComparisonOperator::Equal,
            right,
        } => match (left.as_ref(), right.as_ref()) {
            (Expression::Column(col), Expression::Literal(_)) => col.clone(),
            _ => return,
        },
        _ => return,
    };

    let mut values = Vec::new();
    for branch in &branches {
        match branch {
            Expression::Comparison {
                left,
                operator: ComparisonOperator::Equal,
                right,
            } => match (left.as_ref(), right.as_ref()) {
                (Expression::Column(col), Expression::Literal(lit))
                    if SelectStatement::column_reference_name(col)
                        == SelectStatement::column_reference_name(&first_col) =>
                {
                    values.push(Expression::Literal(lit.clone()));
                }
                _ => return,
            },
            _ => return,
        }
    }

    *cond = Expression::InList {
        expr: Box::new(Expression::Column(first_col)),
        values,
        is_not: false,
    };
}

fn collect_or_branches<'a>(cond: &'a Expression, out: &mut Vec<&'a Expression>) {
    match cond {
        Expression::Logical {
            left,
            operator: LogicalOperator::Or,
            right,
        } => {
            collect_or_branches(left, out);
            collect_or_branches(right, out);
        }
        other => out.push(other),
    }
}

// ── 6. Simple count(*) detection ─────────────────────────────────────────────
//
// Pattern: `SELECT count(*) FROM table` with no WHERE, GROUP BY, HAVING,
// single table source, no set operation. Tags the select so the planner can
// use a btree node count instead of a full scan.

/// Returns `true` if the statement is a simple `count(*)` that can be answered
/// without scanning rows.
pub(crate) fn is_simple_count(select: &SelectStatement) -> bool {
    // Must be: single count(*) column, no WHERE, no GROUP BY, no HAVING,
    // no DISTINCT, no LIMIT/OFFSET, no set operation, single table source.
    if select.columns.len() != 1 {
        return false;
    }
    if !matches!(select.columns[0], SelectItem::CountAll) {
        return false;
    }
    if select.where_clause.is_some()
        || !select.group_by.is_empty()
        || select.having_clause.is_some()
        || select.distinct
        || select.limit.is_some()
        || select.offset.is_some()
        || select.set_operation.is_some()
    {
        return false;
    }
    matches!(select.from, TableReference::Table(_, _))
}

// ── 7. Min/Max → ordered lookup detection ────────────────────────────────────
//
// Pattern: `SELECT min(col)` or `SELECT max(col)` as the sole aggregate,
// no GROUP BY, no HAVING, single table. The planner can use the first/last
// key of an index on `col` instead of scanning all rows.

/// If the statement is a simple `min(col)` or `max(col)`, returns
/// `Some((column_name, is_max))`.
pub(crate) fn detect_min_max_aggregate(select: &SelectStatement) -> Option<(String, bool)> {
    if select.columns.len() != 1 {
        return None;
    }
    if !select.group_by.is_empty()
        || select.having_clause.is_some()
        || select.distinct
        || select.set_operation.is_some()
    {
        return None;
    }
    if !matches!(select.from, TableReference::Table(_, _)) {
        return None;
    }
    match &select.columns[0] {
        SelectItem::Aggregate {
            function: AggregateFunction::Min,
            column,
        } => Some((column.clone(), false)),
        SelectItem::Aggregate {
            function: AggregateFunction::Max,
            column,
        } => Some((column.clone(), true)),
        _ => None,
    }
}

// ── 8. EXISTS-to-join rewrite ────────────────────────────────────────────────
//
// Converts safe `WHERE EXISTS (SELECT ... FROM t2 WHERE t2.fk = t1.pk)`
// predicates into INNER JOIN equivalents. The join is marked so the executor
// breaks after the first match (preserving existential semantics).
//
// Safety requirements (SQLite §3):
//   - Subquery has exactly one FROM item (simple table)
//   - No aggregate, no LIMIT, no compound, no nested FROM-subquery
//   - No DISTINCT

/// Result of an EXISTS-to-join conversion attempt.
pub(crate) struct ExistsToJoinResult {
    /// The table to append to the outer FROM.
    pub join_table: String,
    /// The alias for the joined table.
    pub join_alias: Option<String>,
    /// The subquery's WHERE conditions to merge into the outer WHERE.
    pub join_conditions: Vec<Expression>,
}

/// Try to convert an EXISTS in the WHERE clause to a join. Returns the
/// conversion result if successful. The caller is responsible for splicing
/// the result into the outer query.
pub(crate) fn try_exists_to_join(cond: &Expression) -> Option<ExistsToJoinResult> {
    let (subquery, is_not) = match cond {
        Expression::Exists { subquery, is_not } => (subquery, *is_not),
        _ => return None,
    };

    // NOT EXISTS cannot be converted to a simple join.
    if is_not {
        return None;
    }

    // Must be a simple subquery: single FROM table, no aggregate, no LIMIT,
    // no compound, no nested subquery source.
    let table_name = match &subquery.from {
        TableReference::Table(name, _) => name.clone(),
        _ => return None,
    };

    if subquery.limit.is_some()
        || subquery.offset.is_some()
        || subquery.distinct
        || subquery.set_operation.is_some()
        || !subquery.group_by.is_empty()
        || subquery.having_clause.is_some()
    {
        return None;
    }

    // Must have aggregate-free columns.
    for col in &subquery.columns {
        match col {
            SelectItem::CountAll | SelectItem::Aggregate { .. } | SelectItem::Window { .. } => {
                return None;
            }
            _ => {}
        }
    }

    // Must have a WHERE clause with at least one condition.
    let sub_where = subquery.where_clause.as_ref()?;
    if sub_where.conditions.is_empty() {
        return None;
    }

    let alias = match &subquery.from {
        TableReference::Table(_, alias) => alias.clone(),
        _ => None,
    };

    Some(ExistsToJoinResult {
        join_table: table_name,
        join_alias: alias,
        join_conditions: sub_where.conditions.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str) -> Expression {
        Expression::Column(name.to_string())
    }

    fn int(v: i128) -> Expression {
        Expression::Literal(LiteralValue::Integer(v))
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
    fn test_constant_propagation() {
        let mut wc = Some(WhereClause {
            conditions: vec![
                Expression::Comparison {
                    left: Box::new(col("a")),
                    operator: ComparisonOperator::Equal,
                    right: Box::new(int(5)),
                },
                Expression::Comparison {
                    left: Box::new(col("b")),
                    operator: ComparisonOperator::Equal,
                    right: Box::new(col("a")),
                },
            ],
        });
        propagate_constants_in_where(&mut wc);
        let conditions = &wc.unwrap().conditions;
        // Second condition should now be b = 5
        match &conditions[1] {
            Expression::Comparison { right, .. } => match right.as_ref() {
                Expression::Literal(LiteralValue::Integer(5)) => {}
                other => panic!("Expected b = 5, got {other:?}"),
            },
            other => panic!("Expected Comparison, got {other:?}"),
        }
    }

    #[test]
    fn test_between_decomposition() {
        let mut wc = Some(WhereClause {
            conditions: vec![Expression::Between {
                expr: Box::new(col("x")),
                lower: Box::new(int(10)),
                upper: Box::new(int(20)),
                is_not: false,
            }],
        });
        decompose_between_in_where(&mut wc);
        // Should have original + 2 range conditions = 3 total
        assert_eq!(wc.as_ref().unwrap().conditions.len(), 3);
    }

    #[test]
    fn test_or_to_in() {
        let mut cond = Expression::Logical {
            left: Box::new(Expression::Comparison {
                left: Box::new(col("x")),
                operator: ComparisonOperator::Equal,
                right: Box::new(int(1)),
            }),
            operator: LogicalOperator::Or,
            right: Box::new(Expression::Logical {
                left: Box::new(Expression::Comparison {
                    left: Box::new(col("x")),
                    operator: ComparisonOperator::Equal,
                    right: Box::new(int(2)),
                }),
                operator: LogicalOperator::Or,
                right: Box::new(Expression::Comparison {
                    left: Box::new(col("x")),
                    operator: ComparisonOperator::Equal,
                    right: Box::new(int(3)),
                }),
            }),
        };
        try_convert_or_to_in(&mut cond);
        match &cond {
            Expression::InList { values, .. } => assert_eq!(values.len(), 3),
            other => panic!("Expected InList, got {other:?}"),
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
