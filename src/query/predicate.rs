#![allow(dead_code)]
use crate::catalog::Value;
use crate::parser::ast::{
    ComparisonOperator, Expression, LogicalOperator, SelectStatement, WhereClause,
};
use crate::parser::LiteralValue;
use crate::query::logest::LogEst;
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

// ── WhereTerm-style analysis (SQLite whereexpr.c) ────────────────────────────

/// Classified operator for a WHERE term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TermOperator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    IsNull,
    IsNotNull,
    LikePrefix,
    Between,
    Like,
    Other,
}

/// A single analysed term from the WHERE clause.
#[derive(Debug, Clone)]
pub(crate) struct WhereTerm {
    /// The original (or synthesized) condition.
    pub condition: Expression,
    /// Classified operator.
    pub op: TermOperator,
    /// The column this term constrains (if single-column).
    pub column: Option<String>,
    /// The table this column belongs to (if qualified).
    pub table: Option<String>,
    /// `true` if this term was manufactured by the analyser, not user SQL.
    pub is_virtual: bool,
    /// Estimated selectivity of this term.
    pub selectivity: LogEst,
}

/// Analyse a WHERE clause into structured terms, generating virtual terms
/// where beneficial.
pub(crate) fn analyse_where(where_clause: &WhereClause) -> Vec<WhereTerm> {
    let mut terms = Vec::new();

    // Phase 1: Flatten the top-level AND tree into individual terms.
    for cond in &where_clause.conditions {
        flatten_and_tree(cond, &mut terms);
    }

    // Phase 2: Manufacture virtual terms.
    let mut virtual_terms = Vec::new();

    // Commuted comparisons: `5 = a` → virtual `a = 5`
    for term in &terms {
        if let Some(commuted) = try_commute(&term.condition) {
            let commuted_op = classify_condition(&commuted);
            let (col, tbl) = extract_column_info(&commuted);
            virtual_terms.push(WhereTerm {
                condition: commuted,
                op: commuted_op,
                column: col,
                table: tbl,
                is_virtual: true,
                selectivity: term.selectivity,
            });
        }
    }

    // LIKE prefix ranges: `x LIKE 'abc%'` → virtual `x >= 'abc'` + `x < 'abd'`
    for term in &terms {
        virtual_terms.extend(generate_like_prefix_ranges(&term.condition));
    }

    // Transitive equalities: if `a = b` and `b = 5`, generate virtual `a = 5`.
    let transitive = generate_transitive_equalities(&terms);
    virtual_terms.extend(transitive);

    terms.extend(virtual_terms);
    terms
}

fn flatten_and_tree(cond: &Expression, out: &mut Vec<WhereTerm>) {
    match cond {
        Expression::Logical {
            left,
            operator: LogicalOperator::And,
            right,
        } => {
            flatten_and_tree(left, out);
            flatten_and_tree(right, out);
        }
        other => {
            let op = classify_condition(other);
            let (col, tbl) = extract_condition_column_info(other);
            let selectivity = estimate_selectivity(op);
            out.push(WhereTerm {
                condition: other.clone(),
                op,
                column: col,
                table: tbl,
                is_virtual: false,
                selectivity,
            });
        }
    }
}

fn classify_condition(cond: &Expression) -> TermOperator {
    match cond {
        Expression::Comparison { operator, .. } => match operator {
            ComparisonOperator::Equal => TermOperator::Eq,
            ComparisonOperator::NotEqual => TermOperator::Ne,
            ComparisonOperator::LessThan => TermOperator::Lt,
            ComparisonOperator::LessThanOrEqual => TermOperator::Le,
            ComparisonOperator::GreaterThan => TermOperator::Gt,
            ComparisonOperator::GreaterThanOrEqual => TermOperator::Ge,
        },
        Expression::InList { .. } | Expression::InSubquery { .. } => TermOperator::In,
        Expression::NullCheck { is_not: false, .. } => TermOperator::IsNull,
        Expression::NullCheck { is_not: true, .. } => TermOperator::IsNotNull,
        Expression::Between { .. } => TermOperator::Between,
        Expression::Like { .. } => TermOperator::Like,
        _ => TermOperator::Other,
    }
}

fn extract_condition_column_info(cond: &Expression) -> (Option<String>, Option<String>) {
    match cond {
        Expression::Comparison { left, .. } => extract_column_info_from_expr(left),
        Expression::InList { expr, .. }
        | Expression::Between { expr, .. }
        | Expression::Like { expr, .. }
        | Expression::NullCheck { expr, .. } => extract_column_info_from_expr(expr),
        _ => (None, None),
    }
}

fn extract_column_info_from_expr(expr: &Expression) -> (Option<String>, Option<String>) {
    match expr {
        Expression::Column(ref_name) => {
            let (qualifier, name) = SelectStatement::split_column_reference(ref_name);
            (Some(name.to_string()), qualifier.map(str::to_string))
        }
        _ => (None, None),
    }
}

fn extract_column_info(cond: &Expression) -> (Option<String>, Option<String>) {
    extract_condition_column_info(cond)
}

/// Heuristic selectivity estimates (in LogEst).
fn estimate_selectivity(op: TermOperator) -> LogEst {
    match op {
        TermOperator::Eq => LogEst(0),
        TermOperator::In => LogEst(10),
        TermOperator::Lt | TermOperator::Le | TermOperator::Gt | TermOperator::Ge => LogEst(33),
        TermOperator::Between => LogEst(23),
        TermOperator::LikePrefix => LogEst(30),
        TermOperator::Like => LogEst(33),
        TermOperator::IsNull => LogEst(10),
        TermOperator::IsNotNull => LogEst(33),
        TermOperator::Ne => LogEst(33),
        TermOperator::Other => LogEst(33),
    }
}

/// Commute `literal op column` → `column op' column` where applicable.
fn try_commute(cond: &Expression) -> Option<Expression> {
    match cond {
        Expression::Comparison {
            left,
            operator,
            right,
        } => match (left.as_ref(), right.as_ref()) {
            (left_inner @ Expression::Literal(_), right_inner @ Expression::Column(_)) => {
                let commuted_op = match operator {
                    ComparisonOperator::Equal => ComparisonOperator::Equal,
                    ComparisonOperator::NotEqual => ComparisonOperator::NotEqual,
                    ComparisonOperator::LessThan => ComparisonOperator::GreaterThan,
                    ComparisonOperator::LessThanOrEqual => ComparisonOperator::GreaterThanOrEqual,
                    ComparisonOperator::GreaterThan => ComparisonOperator::LessThan,
                    ComparisonOperator::GreaterThanOrEqual => ComparisonOperator::LessThanOrEqual,
                };
                Some(Expression::Comparison {
                    left: Box::new(right_inner.clone()),
                    operator: commuted_op,
                    right: Box::new(left_inner.clone()),
                })
            }
            _ => None,
        },
        _ => None,
    }
}

/// Generate range terms from LIKE prefix patterns.
fn generate_like_prefix_ranges(cond: &Expression) -> Vec<WhereTerm> {
    let (expr, pattern, is_not) = match cond {
        Expression::Like {
            expr,
            pattern,
            is_not,
        } => (expr, pattern, *is_not),
        _ => return Vec::new(),
    };

    if is_not {
        return Vec::new();
    }

    let prefix = match pattern.as_ref() {
        Expression::Literal(LiteralValue::Text(s)) => extract_like_prefix(s),
        _ => return Vec::new(),
    };

    let Some(prefix) = prefix else {
        return Vec::new();
    };

    if prefix.is_empty() {
        return Vec::new();
    }

    let mut result = Vec::new();
    let (col, tbl) = extract_column_info_from_expr(expr.as_ref());

    // Lower bound: x >= prefix
    result.push(WhereTerm {
        condition: Expression::Comparison {
            left: expr.clone(),
            operator: ComparisonOperator::GreaterThanOrEqual,
            right: Box::new(Expression::Literal(LiteralValue::Text(prefix.clone()))),
        },
        op: TermOperator::Ge,
        column: col.clone(),
        table: tbl.clone(),
        is_virtual: true,
        selectivity: LogEst(30),
    });

    // Upper bound: x < prefix_next
    if let Some(upper_str) = increment_string(&prefix) {
        result.push(WhereTerm {
            condition: Expression::Comparison {
                left: expr.clone(),
                operator: ComparisonOperator::LessThan,
                right: Box::new(Expression::Literal(LiteralValue::Text(upper_str))),
            },
            op: TermOperator::Lt,
            column: col,
            table: tbl,
            is_virtual: true,
            selectivity: LogEst(30),
        });
    }

    result
}

/// Extract the fixed prefix before any wildcard in a LIKE pattern.
fn extract_like_prefix(pattern: &str) -> Option<String> {
    let mut prefix = String::new();
    for ch in pattern.chars() {
        match ch {
            '%' | '_' => break,
            other => prefix.push(other),
        }
    }
    if prefix.is_empty() || prefix.len() == pattern.len() {
        None
    } else {
        Some(prefix)
    }
}

/// Increment the last character of a string to form the exclusive upper bound.
fn increment_string(s: &str) -> Option<String> {
    let mut chars: Vec<char> = s.chars().collect();
    if let Some(last) = chars.last_mut() {
        *last = char::from_u32(*last as u32 + 1)?;
        Some(chars.into_iter().collect())
    } else {
        None
    }
}

/// Generate transitive equalities: `a = b` + `b = 5` → virtual `a = 5`.
fn generate_transitive_equalities(terms: &[WhereTerm]) -> Vec<WhereTerm> {
    let mut col_literals: Vec<(String, LiteralValue)> = Vec::new();
    let mut col_col_eqs: Vec<(String, String)> = Vec::new();

    for term in terms {
        if term.op != TermOperator::Eq || term.is_virtual {
            continue;
        }
        match &term.condition {
            Expression::Comparison {
                left,
                operator: ComparisonOperator::Equal,
                right,
            } => match (left.as_ref(), right.as_ref()) {
                (Expression::Column(a), Expression::Literal(lit)) => {
                    col_literals.push((
                        SelectStatement::column_reference_name(a).to_string(),
                        lit.clone(),
                    ));
                }
                (Expression::Literal(lit), Expression::Column(a)) => {
                    col_literals.push((
                        SelectStatement::column_reference_name(a).to_string(),
                        lit.clone(),
                    ));
                }
                (Expression::Column(a), Expression::Column(b)) => {
                    col_col_eqs.push((
                        SelectStatement::column_reference_name(a).to_string(),
                        SelectStatement::column_reference_name(b).to_string(),
                    ));
                }
                _ => {}
            },
            _ => {}
        }
    }

    let mut result = Vec::new();

    for (a, b) in &col_col_eqs {
        for (col, lit) in &col_literals {
            let target = if col == b {
                Some(a)
            } else if col == a {
                Some(b)
            } else {
                None
            };
            if let Some(target_col) = target {
                let already_exists = col_literals.iter().any(|(c, _)| c == target_col);
                if !already_exists {
                    result.push(WhereTerm {
                        condition: Expression::Comparison {
                            left: Box::new(Expression::Column(target_col.clone())),
                            operator: ComparisonOperator::Equal,
                            right: Box::new(Expression::Literal(lit.clone())),
                        },
                        op: TermOperator::Eq,
                        column: Some(target_col.clone()),
                        table: None,
                        is_virtual: true,
                        selectivity: LogEst(0),
                    });
                }
            }
        }
    }

    result
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

    #[test]
    fn test_where_term_analysis_basic() {
        let wc = WhereClause {
            conditions: vec![
                Expression::Comparison {
                    left: Box::new(col("id")),
                    operator: ComparisonOperator::Equal,
                    right: Box::new(int(5)),
                },
                Expression::Comparison {
                    left: Box::new(col("age")),
                    operator: ComparisonOperator::GreaterThan,
                    right: Box::new(int(18)),
                },
            ],
        };
        let terms = analyse_where(&wc);
        assert!(terms.len() >= 2);
        let eq_terms: Vec<_> = terms.iter().filter(|t| t.op == TermOperator::Eq).collect();
        assert!(!eq_terms.is_empty());
        assert_eq!(eq_terms[0].column.as_deref(), Some("id"));
    }

    #[test]
    fn test_commuted_comparison() {
        let wc = WhereClause {
            conditions: vec![Expression::Comparison {
                left: Box::new(int(5)),
                operator: ComparisonOperator::LessThan,
                right: Box::new(col("x")),
            }],
        };
        let terms = analyse_where(&wc);
        let virtual_terms: Vec<_> = terms.iter().filter(|t| t.is_virtual).collect();
        assert_eq!(virtual_terms.len(), 1);
        assert_eq!(virtual_terms[0].op, TermOperator::Gt);
    }

    #[test]
    fn test_like_prefix_ranges() {
        let wc = WhereClause {
            conditions: vec![Expression::Like {
                expr: Box::new(col("name")),
                pattern: Box::new(Expression::Literal(LiteralValue::Text("abc%".to_string()))),
                is_not: false,
            }],
        };
        let terms = analyse_where(&wc);
        let virtual_terms: Vec<_> = terms.iter().filter(|t| t.is_virtual).collect();
        assert_eq!(virtual_terms.len(), 2);
        assert!(virtual_terms.iter().any(|t| t.op == TermOperator::Ge));
        assert!(virtual_terms.iter().any(|t| t.op == TermOperator::Lt));
    }

    #[test]
    fn test_transitive_equality() {
        let wc = WhereClause {
            conditions: vec![
                Expression::Comparison {
                    left: Box::new(col("a")),
                    operator: ComparisonOperator::Equal,
                    right: Box::new(col("b")),
                },
                Expression::Comparison {
                    left: Box::new(col("b")),
                    operator: ComparisonOperator::Equal,
                    right: Box::new(int(5)),
                },
            ],
        };
        let terms = analyse_where(&wc);
        let virtual_eq: Vec<_> = terms
            .iter()
            .filter(|t| t.is_virtual && t.op == TermOperator::Eq)
            .collect();
        assert_eq!(virtual_eq.len(), 1);
        assert_eq!(virtual_eq[0].column.as_deref(), Some("a"));
    }

    #[test]
    fn test_extract_like_prefix() {
        assert_eq!(extract_like_prefix("abc%"), Some("abc".to_string()));
        assert_eq!(extract_like_prefix("a_bc%"), Some("a".to_string()));
        assert_eq!(extract_like_prefix("%abc"), None);
        assert_eq!(extract_like_prefix("abc"), None);
    }

    #[test]
    fn test_increment_string() {
        assert_eq!(increment_string("abc"), Some("abd".to_string()));
        assert_eq!(increment_string("az"), Some("a{".to_string()));
    }
}
