//! Query optimizer for improving logical query execution plans.

use crate::catalog::Schema;
use crate::error::Result;
use crate::parser::ast::*;
use crate::parser::LiteralValue;
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub struct QueryOptimizer {
    _catalog: Schema,
}

impl QueryOptimizer {
    pub fn new(catalog: Schema) -> Self {
        Self { _catalog: catalog }
    }

    pub fn optimize_statement(&self, statement: Statement) -> Result<Statement> {
        match statement {
            Statement::Select(mut select) => {
                self.optimize_select(&mut select)?;
                Ok(Statement::Select(select))
            }
            Statement::Insert(mut insert) => {
                self.optimize_insert(&mut insert)?;
                Ok(Statement::Insert(insert))
            }
            Statement::Update(mut update) => {
                self.optimize_update(&mut update)?;
                Ok(Statement::Update(update))
            }
            Statement::Delete(mut delete) => {
                self.optimize_delete(&mut delete)?;
                Ok(Statement::Delete(delete))
            }
            other => Ok(other),
        }
    }

    fn optimize_insert(&self, insert: &mut InsertStatement) -> Result<()> {
        match &mut insert.source {
            InsertSource::Values(rows) => {
                for row in rows {
                    for expr in row {
                        self.optimize_expression(expr)?;
                    }
                }
            }
            InsertSource::Select(select) => self.optimize_select(select)?,
        }

        if let Some(assignments) = &mut insert.on_duplicate {
            for assignment in assignments {
                self.optimize_expression(&mut assignment.value)?;
            }
        }

        Ok(())
    }

    fn optimize_update(&self, update: &mut UpdateStatement) -> Result<()> {
        if let Some(source) = &mut update.source {
            self.optimize_table_reference(source)?;
        }

        for assignment in &mut update.assignments {
            self.optimize_expression(&mut assignment.value)?;
        }

        if let Some(where_clause) = &mut update.where_clause {
            self.optimize_conditions(&mut where_clause.conditions)?;
        }

        Ok(())
    }

    fn optimize_delete(&self, delete: &mut DeleteStatement) -> Result<()> {
        if let Some(source) = &mut delete.source {
            self.optimize_table_reference(source)?;
        }

        if let Some(where_clause) = &mut delete.where_clause {
            self.optimize_conditions(&mut where_clause.conditions)?;
        }

        Ok(())
    }

    fn optimize_select(&self, select: &mut SelectStatement) -> Result<()> {
        for cte in &mut select.with_clause {
            self.optimize_select(&mut cte.query)?;
        }

        self.optimize_table_reference(&mut select.from)?;

        if let Some(where_clause) = &mut select.where_clause {
            self.optimize_conditions(&mut where_clause.conditions)?;
        }

        for item in &mut select.columns {
            match item {
                SelectItem::Expression(expr) => self.optimize_expression(expr)?,
                SelectItem::Window { window, .. } => {
                    for expr in &mut window.partition_by {
                        self.optimize_expression(expr)?;
                    }
                }
                SelectItem::Wildcard | SelectItem::Column(_) | SelectItem::CountAll => {}
                SelectItem::Aggregate { .. } => {}
            }
        }

        for expr in &mut select.group_by {
            self.optimize_expression(expr)?;
        }

        if let Some(having_clause) = &mut select.having_clause {
            self.optimize_conditions(&mut having_clause.conditions)?;
        }

        if let Some(set_operation) = &mut select.set_operation {
            self.optimize_select(&mut set_operation.right)?;
        }

        Ok(())
    }

    fn optimize_table_reference(&self, from: &mut TableReference) -> Result<()> {
        match from {
            TableReference::Table(_, _) => {}
            TableReference::Derived { subquery, .. } => {
                self.optimize_select(subquery)?;
            }
            TableReference::CrossJoin(left, right) => {
                self.optimize_table_reference(left)?;
                self.optimize_table_reference(right)?;
            }
            TableReference::InnerJoin { left, right, on }
            | TableReference::LeftJoin { left, right, on }
            | TableReference::RightJoin { left, right, on }
            | TableReference::FullOuterJoin { left, right, on } => {
                self.optimize_table_reference(left)?;
                self.optimize_table_reference(right)?;
                self.optimize_condition(on)?;
            }
        }

        Ok(())
    }

    fn optimize_conditions(&self, conditions: &mut Vec<Condition>) -> Result<()> {
        for condition in conditions {
            self.optimize_condition(condition)?;
        }

        Ok(())
    }

    fn optimize_condition(&self, condition: &mut Condition) -> Result<()> {
        match condition {
            Condition::Comparison { left, right, .. } => {
                self.optimize_expression(left)?;
                self.optimize_expression(right)?;
            }
            Condition::Between {
                expr, lower, upper, ..
            } => {
                self.optimize_expression(expr)?;
                self.optimize_expression(lower)?;
                self.optimize_expression(upper)?;
            }
            Condition::InList { expr, values, .. } => {
                self.optimize_expression(expr)?;
                for value in values {
                    self.optimize_expression(value)?;
                }
            }
            Condition::InSubquery { expr, subquery, .. } => {
                self.optimize_expression(expr)?;
                self.optimize_select(subquery)?;
            }
            Condition::Exists { subquery, .. } => {
                self.optimize_select(subquery)?;
            }
            Condition::Like { expr, pattern, .. } => {
                self.optimize_expression(expr)?;
                self.optimize_expression(pattern)?;
            }
            Condition::NullCheck { expr, .. } => {
                self.optimize_expression(expr)?;
            }
            Condition::Not(inner) => {
                self.optimize_condition(inner)?;
                if let Condition::Not(double_inner) = inner.as_ref() {
                    *condition = *double_inner.clone();
                }
            }
            Condition::Logical { left, right, .. } => {
                self.optimize_condition(left)?;
                self.optimize_condition(right)?;
            }
        }

        Ok(())
    }

    fn optimize_expression(&self, expr: &mut Expression) -> Result<()> {
        match expr {
            Expression::Binary {
                left,
                operator,
                right,
            } => {
                self.optimize_expression(left)?;
                self.optimize_expression(right)?;

                if let (Expression::Literal(left), Expression::Literal(right)) =
                    (left.as_ref(), right.as_ref())
                {
                    if let Some(folded) = fold_arithmetic_literals(*operator, left, right) {
                        *expr = Expression::Literal(folded);
                        return Ok(());
                    }
                }

                if let Some(simplified) =
                    simplify_arithmetic_identity(*operator, left.as_ref(), right.as_ref())
                {
                    *expr = simplified;
                }
            }
            Expression::Comparison {
                left,
                operator,
                right,
            } => {
                self.optimize_expression(left)?;
                self.optimize_expression(right)?;

                if let (Expression::Literal(left), Expression::Literal(right)) =
                    (left.as_ref(), right.as_ref())
                {
                    if let Some(folded) = fold_comparison_literals(operator, left, right) {
                        *expr = Expression::Literal(folded);
                    }
                }
            }
            Expression::Logical {
                left,
                operator,
                right,
            } => {
                self.optimize_expression(left)?;
                self.optimize_expression(right)?;

                if let (Expression::Literal(left), Expression::Literal(right)) =
                    (left.as_ref(), right.as_ref())
                {
                    if let Some(folded) = fold_logical_literals(operator, left, right) {
                        *expr = Expression::Literal(folded);
                        return Ok(());
                    }
                }

                if let Some(simplified) =
                    simplify_logical_identity(operator, left.as_ref(), right.as_ref())
                {
                    *expr = simplified;
                }
            }
            Expression::UnaryMinus(inner) => {
                self.optimize_expression(inner)?;
                if let Expression::Literal(LiteralValue::Integer(value)) = inner.as_ref() {
                    if let Some(folded) = value.checked_neg() {
                        *expr = Expression::Literal(LiteralValue::Integer(folded));
                    }
                } else if let Expression::Literal(LiteralValue::Float(value)) = inner.as_ref() {
                    let folded = if let Some(stripped) = value.strip_prefix('-') {
                        stripped.to_string()
                    } else {
                        format!("-{value}")
                    };
                    *expr = Expression::Literal(LiteralValue::Float(folded));
                }
            }
            Expression::UnaryNot(inner) => {
                self.optimize_expression(inner)?;
                if let Some(value) = literal_as_nullable_bool(inner.as_ref()) {
                    *expr = literal_expression_from_nullable_bool(value);
                }
            }
            Expression::ScalarFunctionCall { args, .. } => {
                for arg in args {
                    self.optimize_expression(arg)?;
                }
            }
            Expression::Case {
                branches,
                else_expr,
            } => {
                for branch in branches.iter_mut() {
                    self.optimize_expression(&mut branch.condition)?;
                    self.optimize_expression(&mut branch.result)?;
                }
                if let Some(else_expr) = else_expr {
                    self.optimize_expression(else_expr)?;
                }

                let mut remaining = Vec::with_capacity(branches.len());
                for branch in std::mem::take(branches) {
                    match literal_as_nullable_bool(&branch.condition) {
                        Some(Some(true)) => {
                            *expr = branch.result;
                            return Ok(());
                        }
                        Some(Some(false)) | Some(None) => {}
                        None => remaining.push(branch),
                    }
                }
                *branches = remaining;

                if branches.is_empty() {
                    *expr = else_expr
                        .take()
                        .map(|expr| *expr)
                        .unwrap_or(Expression::Literal(LiteralValue::Null));
                }
            }
            Expression::NullCheck {
                expr: inner,
                is_not,
            } => {
                self.optimize_expression(inner)?;
                if let Expression::Literal(value) = inner.as_ref() {
                    let is_null = matches!(value, LiteralValue::Null);
                    *expr = Expression::Literal(LiteralValue::Boolean(if *is_not {
                        !is_null
                    } else {
                        is_null
                    }));
                }
            }
            Expression::Cast { expr: inner, .. } => {
                self.optimize_expression(inner)?;
            }
            Expression::ScalarSubquery(subquery) => {
                self.optimize_select(subquery)?;
            }
            Expression::InSubquery {
                expr: inner,
                subquery,
                ..
            } => {
                self.optimize_expression(inner)?;
                self.optimize_select(subquery)?;
            }
            Expression::InList {
                expr: inner,
                values,
                is_not,
            } => {
                self.optimize_expression(inner)?;
                for value in values.iter_mut() {
                    self.optimize_expression(value)?;
                }

                if let Expression::Literal(probe) = inner.as_ref() {
                    if values
                        .iter()
                        .all(|value| matches!(value, Expression::Literal(_)))
                    {
                        let literals = values
                            .iter()
                            .filter_map(|value| match value {
                                Expression::Literal(literal) => Some(literal),
                                _ => None,
                            })
                            .collect::<Vec<_>>();
                        if let Some(folded) = fold_in_list_literals(probe, &literals, *is_not) {
                            *expr = Expression::Literal(folded);
                        }
                    }
                }
            }
            Expression::Between {
                expr: inner,
                lower,
                upper,
                is_not,
            } => {
                self.optimize_expression(inner)?;
                self.optimize_expression(lower)?;
                self.optimize_expression(upper)?;

                if let (
                    Expression::Literal(probe),
                    Expression::Literal(lower),
                    Expression::Literal(upper),
                ) = (inner.as_ref(), lower.as_ref(), upper.as_ref())
                {
                    if let Some(folded) = fold_between_literals(probe, lower, upper, *is_not) {
                        *expr = Expression::Literal(folded);
                    }
                }
            }
            Expression::Like {
                expr: inner,
                pattern,
                is_not,
            } => {
                self.optimize_expression(inner)?;
                self.optimize_expression(pattern)?;

                if let (
                    Expression::Literal(LiteralValue::Text(text)),
                    Expression::Literal(LiteralValue::Text(pattern)),
                ) = (inner.as_ref(), pattern.as_ref())
                {
                    let matched = like_matches(pattern, text);
                    *expr = Expression::Literal(LiteralValue::Boolean(if *is_not {
                        !matched
                    } else {
                        matched
                    }));
                }
            }
            Expression::AggregateCall { .. } => {}
            Expression::Exists { subquery, .. } => {
                self.optimize_select(subquery)?;
            }
            Expression::Column(_)
            | Expression::Literal(_)
            | Expression::IntervalLiteral { .. }
            | Expression::Parameter(_) => {}
        }

        Ok(())
    }
}

fn literal_as_f64(value: &LiteralValue) -> Option<f64> {
    match value {
        LiteralValue::Integer(value) => Some(*value as f64),
        LiteralValue::Float(value) => value.parse::<f64>().ok(),
        _ => None,
    }
}

fn is_literal_integer_zero(expr: &Expression) -> bool {
    matches!(expr, Expression::Literal(LiteralValue::Integer(0)))
}

fn is_literal_integer_one(expr: &Expression) -> bool {
    matches!(expr, Expression::Literal(LiteralValue::Integer(1)))
}

fn fold_arithmetic_literals(
    operator: ArithmeticOperator,
    left: &LiteralValue,
    right: &LiteralValue,
) -> Option<LiteralValue> {
    match (left, right) {
        (LiteralValue::Integer(left), LiteralValue::Integer(right)) => {
            let value = match operator {
                ArithmeticOperator::Add => left.checked_add(*right),
                ArithmeticOperator::Subtract => left.checked_sub(*right),
                ArithmeticOperator::Multiply => left.checked_mul(*right),
                ArithmeticOperator::Divide => {
                    if *right == 0 {
                        None
                    } else {
                        left.checked_div(*right)
                    }
                }
                ArithmeticOperator::Modulo => {
                    if *right == 0 {
                        None
                    } else {
                        left.checked_rem(*right)
                    }
                }
            }?;
            Some(LiteralValue::Integer(value))
        }
        _ => {
            let left = literal_as_f64(left)?;
            let right = literal_as_f64(right)?;
            let value = match operator {
                ArithmeticOperator::Add => left + right,
                ArithmeticOperator::Subtract => left - right,
                ArithmeticOperator::Multiply => left * right,
                ArithmeticOperator::Divide => {
                    if right == 0.0 {
                        return None;
                    }
                    left / right
                }
                ArithmeticOperator::Modulo => {
                    if right == 0.0 {
                        return None;
                    }
                    left % right
                }
            };
            Some(LiteralValue::Float(value.to_string()))
        }
    }
}

fn simplify_arithmetic_identity(
    operator: ArithmeticOperator,
    left: &Expression,
    right: &Expression,
) -> Option<Expression> {
    match operator {
        ArithmeticOperator::Add => {
            if is_literal_integer_zero(right) {
                Some(left.clone())
            } else if is_literal_integer_zero(left) {
                Some(right.clone())
            } else {
                None
            }
        }
        ArithmeticOperator::Subtract => {
            if is_literal_integer_zero(right) {
                Some(left.clone())
            } else {
                None
            }
        }
        ArithmeticOperator::Multiply => {
            if is_literal_integer_one(right) {
                Some(left.clone())
            } else if is_literal_integer_one(left) {
                Some(right.clone())
            } else {
                None
            }
        }
        ArithmeticOperator::Divide => {
            if is_literal_integer_one(right) {
                Some(left.clone())
            } else {
                None
            }
        }
        ArithmeticOperator::Modulo => None,
    }
}

fn literal_partial_cmp(left: &LiteralValue, right: &LiteralValue) -> Option<Ordering> {
    match (left, right) {
        (LiteralValue::Integer(left), LiteralValue::Integer(right)) => Some(left.cmp(right)),
        (LiteralValue::Text(left), LiteralValue::Text(right)) => Some(left.cmp(right)),
        (LiteralValue::Boolean(left), LiteralValue::Boolean(right)) => Some(left.cmp(right)),
        (LiteralValue::Float(left), LiteralValue::Float(right)) => {
            let left = left.parse::<f64>().ok()?;
            let right = right.parse::<f64>().ok()?;
            left.partial_cmp(&right)
        }
        (LiteralValue::Integer(left), LiteralValue::Float(right)) => {
            let right = right.parse::<f64>().ok()?;
            (*left as f64).partial_cmp(&right)
        }
        (LiteralValue::Float(left), LiteralValue::Integer(right)) => {
            let left = left.parse::<f64>().ok()?;
            left.partial_cmp(&(*right as f64))
        }
        _ => None,
    }
}

fn fold_comparison_literals(
    operator: &ComparisonOperator,
    left: &LiteralValue,
    right: &LiteralValue,
) -> Option<LiteralValue> {
    if matches!(left, LiteralValue::Null) || matches!(right, LiteralValue::Null) {
        return Some(LiteralValue::Null);
    }

    let ordering = literal_partial_cmp(left, right)?;
    let result = match operator {
        ComparisonOperator::Equal => ordering == Ordering::Equal,
        ComparisonOperator::NotEqual => ordering != Ordering::Equal,
        ComparisonOperator::LessThan => ordering == Ordering::Less,
        ComparisonOperator::LessThanOrEqual => ordering != Ordering::Greater,
        ComparisonOperator::GreaterThan => ordering == Ordering::Greater,
        ComparisonOperator::GreaterThanOrEqual => ordering != Ordering::Less,
    };
    Some(LiteralValue::Boolean(result))
}

fn logical_and(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        (Some(true), None) | (None, Some(true)) | (None, None) => None,
    }
}

fn logical_or(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        (Some(false), None) | (None, Some(false)) | (None, None) => None,
    }
}

fn literal_as_nullable_bool(expr: &Expression) -> Option<Option<bool>> {
    match expr {
        Expression::Literal(LiteralValue::Boolean(value)) => Some(Some(*value)),
        Expression::Literal(LiteralValue::Null) => Some(None),
        _ => None,
    }
}

fn literal_expression_from_nullable_bool(value: Option<bool>) -> Expression {
    match value {
        Some(value) => Expression::Literal(LiteralValue::Boolean(value)),
        None => Expression::Literal(LiteralValue::Null),
    }
}

fn fold_logical_literals(
    operator: &LogicalOperator,
    left: &LiteralValue,
    right: &LiteralValue,
) -> Option<LiteralValue> {
    let left = match left {
        LiteralValue::Boolean(value) => Some(*value),
        LiteralValue::Null => None,
        _ => return None,
    };
    let right = match right {
        LiteralValue::Boolean(value) => Some(*value),
        LiteralValue::Null => None,
        _ => return None,
    };

    Some(match operator {
        LogicalOperator::And => match logical_and(left, right) {
            Some(value) => LiteralValue::Boolean(value),
            None => LiteralValue::Null,
        },
        LogicalOperator::Or => match logical_or(left, right) {
            Some(value) => LiteralValue::Boolean(value),
            None => LiteralValue::Null,
        },
    })
}

fn simplify_logical_identity(
    operator: &LogicalOperator,
    left: &Expression,
    right: &Expression,
) -> Option<Expression> {
    let left_literal = literal_as_nullable_bool(left);
    let right_literal = literal_as_nullable_bool(right);

    match operator {
        LogicalOperator::And => match (left_literal, right_literal) {
            (Some(Some(true)), _) => Some(right.clone()),
            (_, Some(Some(true))) => Some(left.clone()),
            (Some(Some(false)), _) | (_, Some(Some(false))) => {
                Some(Expression::Literal(LiteralValue::Boolean(false)))
            }
            _ => None,
        },
        LogicalOperator::Or => match (left_literal, right_literal) {
            (Some(Some(false)), _) => Some(right.clone()),
            (_, Some(Some(false))) => Some(left.clone()),
            (Some(Some(true)), _) | (_, Some(Some(true))) => {
                Some(Expression::Literal(LiteralValue::Boolean(true)))
            }
            _ => None,
        },
    }
}

fn fold_in_list_literals(
    probe: &LiteralValue,
    candidates: &[&LiteralValue],
    is_not: bool,
) -> Option<LiteralValue> {
    if matches!(probe, LiteralValue::Null) {
        return Some(LiteralValue::Null);
    }

    let mut has_null = false;
    for candidate in candidates {
        if matches!(candidate, LiteralValue::Null) {
            has_null = true;
            continue;
        }

        if literal_partial_cmp(probe, candidate).is_some_and(|ordering| ordering == Ordering::Equal)
        {
            return Some(LiteralValue::Boolean(!is_not));
        }
    }

    if has_null {
        Some(LiteralValue::Null)
    } else {
        Some(LiteralValue::Boolean(is_not))
    }
}

fn fold_between_literals(
    probe: &LiteralValue,
    lower: &LiteralValue,
    upper: &LiteralValue,
    is_not: bool,
) -> Option<LiteralValue> {
    if matches!(probe, LiteralValue::Null)
        || matches!(lower, LiteralValue::Null)
        || matches!(upper, LiteralValue::Null)
    {
        return Some(LiteralValue::Null);
    }

    let lower_ok = literal_partial_cmp(probe, lower).map(|ordering| ordering != Ordering::Less)?;
    let upper_ok =
        literal_partial_cmp(probe, upper).map(|ordering| ordering != Ordering::Greater)?;
    let matched = lower_ok && upper_ok;
    Some(LiteralValue::Boolean(if is_not {
        !matched
    } else {
        matched
    }))
}

fn like_matches(pattern: &str, text: &str) -> bool {
    let pchars: Vec<char> = pattern.chars().collect();
    let tchars: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    let mut star_pi: Option<usize> = None;
    let mut star_ti: usize = 0;

    while ti < tchars.len() {
        if pi < pchars.len() && pchars[pi] == '%' {
            star_pi = Some(pi);
            pi += 1;
            star_ti = ti;
            continue;
        }

        if pi < pchars.len() && (pchars[pi] == '_' || pchars[pi] == tchars[ti]) {
            pi += 1;
            ti += 1;
            continue;
        }

        if let Some(sp) = star_pi {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
            continue;
        }

        return false;
    }

    while pi < pchars.len() && pchars[pi] == '%' {
        pi += 1;
    }

    pi == pchars.len()
}
