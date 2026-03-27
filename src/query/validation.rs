use crate::catalog::{Schema, Table};
use crate::error::{HematiteError, Result};
use crate::parser::ast::*;
use crate::parser::{LiteralValue, SqlTypeName};

#[derive(Debug, Clone)]
struct SourceBinding {
    source_name: String,
    alias: Option<String>,
    columns: Vec<String>,
    has_hidden_rowid: bool,
}

pub(crate) fn validate_statement(statement: &Statement, catalog: &Schema) -> Result<()> {
    match statement {
        Statement::Begin | Statement::Commit | Statement::Rollback => Ok(()),
        Statement::Select(select) => validate_select(select, catalog),
        Statement::Update(update) => validate_update(update, catalog),
        Statement::Insert(insert) => validate_insert(insert, catalog),
        Statement::Delete(delete) => validate_delete(delete, catalog),
        Statement::Create(create) => validate_create(create, catalog),
        Statement::CreateIndex(create_index) => validate_create_index(create_index, catalog),
        Statement::Alter(alter) => validate_alter(alter, catalog),
        Statement::Drop(drop) => validate_drop(drop, catalog),
        Statement::DropIndex(drop_index) => validate_drop_index(drop_index, catalog),
    }
}

pub(crate) fn validate_column_reference(
    select: &SelectStatement,
    name: &str,
    catalog: &Schema,
    from: &TableReference,
) -> Result<()> {
    validate_column_reference_with_outer(select, name, catalog, from, &[])
}

fn validate_select(select: &SelectStatement, catalog: &Schema) -> Result<()> {
    validate_select_with_outer_bindings(select, catalog, &[])
}

fn validate_select_with_outer_bindings(
    select: &SelectStatement,
    catalog: &Schema,
    outer_bindings: &[SourceBinding],
) -> Result<()> {
    if let Some(set_operation) = &select.set_operation {
        validate_select_with_outer_bindings(&set_operation.right, catalog, outer_bindings)?;
        if select.columns.len() != set_operation.right.columns.len() {
            return Err(HematiteError::ParseError(
                "Set operations require both queries to project the same number of columns"
                    .to_string(),
            ));
        }
    }

    for cte in &select.with_clause {
        if cte.recursive {
            let set_operation = cte.query.set_operation.as_ref().ok_or_else(|| {
                HematiteError::ParseError(format!(
                    "Recursive CTE '{}' requires UNION or UNION ALL",
                    cte.name
                ))
            })?;
            if !matches!(
                set_operation.operator,
                SetOperator::Union | SetOperator::UnionAll
            ) {
                return Err(HematiteError::ParseError(format!(
                    "Recursive CTE '{}' requires UNION or UNION ALL",
                    cte.name
                )));
            }

            let mut anchor = (*cte.query).clone();
            anchor.set_operation = None;
            if anchor.references_source_name(&cte.name) {
                return Err(HematiteError::ParseError(format!(
                    "Recursive CTE '{}' anchor term cannot reference itself",
                    cte.name
                )));
            }
            if !set_operation.right.references_source_name(&cte.name) {
                return Err(HematiteError::ParseError(format!(
                    "Recursive CTE '{}' recursive term must reference itself",
                    cte.name
                )));
            }
            if anchor.columns.len() != set_operation.right.columns.len() {
                return Err(HematiteError::ParseError(format!(
                    "Recursive CTE '{}' anchor and recursive terms must project the same number of columns",
                    cte.name
                )));
            }

            validate_select(&anchor, catalog)?;

            let mut recursive_term = (*set_operation.right).clone();
            recursive_term.with_clause.push(CommonTableExpression {
                name: cte.name.clone(),
                recursive: false,
                query: Box::new(anchor.clone()),
            });
            validate_select(&recursive_term, catalog)?;
        } else {
            validate_select(&cte.query, catalog)?;
        }
    }

    let bindings = collect_source_bindings(select, catalog, &select.from)?;
    if bindings.is_empty() {
        return Err(HematiteError::ParseError(
            "SELECT requires at least one table source".to_string(),
        ));
    }
    validate_table_reference(select, catalog, &select.from, outer_bindings)?;

    let has_aggregate = select.columns.iter().any(|item| match item {
        SelectItem::CountAll | SelectItem::Aggregate { .. } => true,
        SelectItem::Expression(expr) => expression_contains_aggregate(expr),
        SelectItem::Wildcard | SelectItem::Column(_) => false,
    }) || select
        .having_clause
        .as_ref()
        .is_some_and(|having| having.conditions.iter().any(condition_contains_aggregate));
    if select.distinct && has_aggregate {
        return Err(HematiteError::ParseError(
            "DISTINCT cannot be combined with aggregate select items yet".to_string(),
        ));
    }

    for item in &select.columns {
        match item {
            SelectItem::Column(name) => {
                validate_column_reference_with_outer(
                    select,
                    name,
                    catalog,
                    &select.from,
                    outer_bindings,
                )?;
            }
            SelectItem::Expression(expr) => {
                validate_expression(select, expr, catalog, &select.from, outer_bindings)?;
            }
            SelectItem::Aggregate { column, .. } => {
                validate_column_reference_with_outer(
                    select,
                    column,
                    catalog,
                    &select.from,
                    outer_bindings,
                )?;
            }
            SelectItem::Wildcard | SelectItem::CountAll => {}
        }
    }

    if let Some(where_clause) = &select.where_clause {
        for condition in &where_clause.conditions {
            validate_condition(select, condition, catalog, &select.from, outer_bindings)?;
        }
    }

    for expr in &select.group_by {
        validate_expression(select, expr, catalog, &select.from, outer_bindings)?;
    }

    if !select.group_by.is_empty() {
        for item in &select.columns {
            match item {
                SelectItem::Wildcard => {
                    return Err(HematiteError::ParseError(
                        "Wildcard select is not supported with GROUP BY".to_string(),
                    ))
                }
                SelectItem::Column(name) => {
                    let grouped = select.group_by.iter().any(
                        |expr| matches!(expr, Expression::Column(group_name) if group_name == name),
                    );
                    if !grouped {
                        return Err(HematiteError::ParseError(format!(
                            "Selected column '{}' must appear in GROUP BY or be aggregated",
                            name
                        )));
                    }
                }
                SelectItem::Expression(_) => {
                    return Err(HematiteError::ParseError(
                        "Expression select items are not supported with GROUP BY yet".to_string(),
                    ))
                }
                SelectItem::CountAll | SelectItem::Aggregate { .. } => {}
            }
        }
    } else if has_aggregate
        && select
            .columns
            .iter()
            .any(|item| !matches!(item, SelectItem::CountAll | SelectItem::Aggregate { .. }))
    {
        return Err(HematiteError::ParseError(
            "Aggregate select items cannot be combined with non-aggregate select items without GROUP BY"
                .to_string(),
        ));
    }

    if select.having_clause.is_some() && select.group_by.is_empty() && !has_aggregate {
        return Err(HematiteError::ParseError(
            "HAVING requires GROUP BY or aggregate select items".to_string(),
        ));
    }

    for item in &select.order_by {
        validate_column_reference_with_outer(
            select,
            &item.column,
            catalog,
            &select.from,
            outer_bindings,
        )?;
    }

    Ok(())
}

fn validate_insert(insert: &InsertStatement, catalog: &Schema) -> Result<()> {
    let table = catalog.get_table_by_name(&insert.table).ok_or_else(|| {
        HematiteError::ParseError(format!("Table '{}' does not exist", insert.table))
    })?;

    let mut seen_columns = std::collections::HashSet::new();
    for col_name in &insert.columns {
        if !seen_columns.insert(col_name) {
            return Err(HematiteError::ParseError(format!(
                "Duplicate column '{}' in INSERT",
                col_name
            )));
        }
        if table.get_column_by_name(col_name).is_none() {
            return Err(HematiteError::ParseError(format!(
                "Column '{}' does not exist in table '{}'",
                col_name, insert.table
            )));
        }
    }

    if insert.columns.is_empty() {
        return Err(HematiteError::ParseError(
            "INSERT must specify at least one column".to_string(),
        ));
    }

    for (i, value_row) in insert.values.iter().enumerate() {
        if value_row.len() != insert.columns.len() {
            return Err(HematiteError::ParseError(format!(
                "Value row {} has {} values, expected {}",
                i,
                value_row.len(),
                insert.columns.len()
            )));
        }

        for value in value_row {
            if matches!(value, Expression::Column(_)) {
                return Err(HematiteError::ParseError(format!(
                    "INSERT value row {} cannot reference columns",
                    i
                )));
            }
        }
    }

    Ok(())
}

fn validate_update(update: &UpdateStatement, catalog: &Schema) -> Result<()> {
    let table = catalog.get_table_by_name(&update.table).ok_or_else(|| {
        HematiteError::ParseError(format!("Table '{}' does not exist", update.table))
    })?;

    if update.assignments.is_empty() {
        return Err(HematiteError::ParseError(
            "UPDATE must specify at least one assignment".to_string(),
        ));
    }

    let mut seen_columns = std::collections::HashSet::new();
    let scope = SelectStatement::single_table_scope(&update.table);
    for assignment in &update.assignments {
        if !seen_columns.insert(&assignment.column) {
            return Err(HematiteError::ParseError(format!(
                "Duplicate column '{}' in UPDATE",
                assignment.column
            )));
        }
        if table.get_column_by_name(&assignment.column).is_none() {
            return Err(HematiteError::ParseError(format!(
                "Column '{}' does not exist in table '{}'",
                assignment.column, update.table
            )));
        }

        validate_expression(&scope, &assignment.value, catalog, &scope.from, &[])?;
    }

    if let Some(where_clause) = &update.where_clause {
        for condition in &where_clause.conditions {
            validate_condition(&scope, condition, catalog, &scope.from, &[])?;
        }
    }

    Ok(())
}

fn validate_create(create: &CreateStatement, catalog: &Schema) -> Result<()> {
    if catalog.get_table_by_name(&create.table).is_some() {
        if create.if_not_exists {
            return Ok(());
        }
        return Err(HematiteError::ParseError(format!(
            "Table '{}' already exists",
            create.table
        )));
    }

    let mut column_names = std::collections::HashSet::new();
    for column in &create.columns {
        if !column_names.insert(column.name.clone()) {
            return Err(HematiteError::ParseError(format!(
                "Duplicate column name '{}'",
                column.name
            )));
        }
    }

    if !create.columns.iter().any(|column| column.primary_key) {
        return Err(HematiteError::ParseError(
            "Table must have at least one primary key column".to_string(),
        ));
    }

    let auto_increment_columns = create
        .columns
        .iter()
        .filter(|column| column.auto_increment)
        .collect::<Vec<_>>();
    if auto_increment_columns.len() > 1 {
        return Err(HematiteError::ParseError(
            "Only one AUTO_INCREMENT column is allowed per table".to_string(),
        ));
    }
    if let Some(column) = auto_increment_columns.first() {
        if column.data_type != SqlTypeName::Integer {
            return Err(HematiteError::ParseError(format!(
                "AUTO_INCREMENT column '{}' must use an integer type",
                column.name
            )));
        }
        if !column.primary_key {
            return Err(HematiteError::ParseError(format!(
                "AUTO_INCREMENT column '{}' must be a PRIMARY KEY",
                column.name
            )));
        }
        if column.default_value.is_some() {
            return Err(HematiteError::ParseError(format!(
                "AUTO_INCREMENT column '{}' cannot also declare a DEFAULT value",
                column.name
            )));
        }
    }

    for unique_constraint in create
        .constraints
        .iter()
        .filter_map(|constraint| match constraint {
            TableConstraint::Unique(unique) => Some(unique),
            TableConstraint::Check(_) | TableConstraint::ForeignKey(_) => None,
        })
    {
        validate_unique_constraint(create, unique_constraint)?;
    }

    for foreign_key in foreign_keys(create) {
        validate_foreign_key(create, catalog, foreign_key)?;
    }

    Ok(())
}

fn validate_delete(delete: &DeleteStatement, catalog: &Schema) -> Result<()> {
    let _table = require_table(catalog, &delete.table)?;
    let scope = SelectStatement::single_table_scope(&delete.table);

    if let Some(where_clause) = &delete.where_clause {
        for condition in &where_clause.conditions {
            validate_condition(&scope, condition, catalog, &scope.from, &[])?;
        }
    }

    Ok(())
}

fn validate_drop(drop: &DropStatement, catalog: &Schema) -> Result<()> {
    if drop.if_exists && catalog.get_table_by_name(&drop.table).is_none() {
        return Ok(());
    }
    let _table = require_table(catalog, &drop.table)?;
    Ok(())
}

fn validate_alter(alter: &AlterStatement, catalog: &Schema) -> Result<()> {
    match &alter.operation {
        AlterOperation::RenameTo(new_name) => {
            require_table(catalog, &alter.table)?;
            if new_name == &alter.table {
                return Err(HematiteError::ParseError(
                    "ALTER TABLE RENAME TO requires a different table name".to_string(),
                ));
            }
            if catalog.get_table_by_name(new_name).is_some() {
                return Err(HematiteError::ParseError(format!(
                    "Table '{}' already exists",
                    new_name
                )));
            }
        }
        AlterOperation::RenameColumn { old_name, new_name } => {
            validate_rename_column(alter, catalog, old_name, new_name)?;
        }
        AlterOperation::AddColumn(column) => {
            let table = require_table(catalog, &alter.table)?;
            if table.get_column_by_name(&column.name).is_some() {
                return Err(HematiteError::ParseError(format!(
                    "Column '{}' already exists in table '{}'",
                    column.name, alter.table
                )));
            }
            if column.primary_key {
                return Err(HematiteError::ParseError(
                    "ALTER TABLE ADD COLUMN cannot add a PRIMARY KEY column".to_string(),
                ));
            }
            if column.auto_increment {
                return Err(HematiteError::ParseError(
                    "ALTER TABLE ADD COLUMN does not support AUTO_INCREMENT columns".to_string(),
                ));
            }
            if column.unique {
                return Err(HematiteError::ParseError(
                    "ALTER TABLE ADD COLUMN does not support UNIQUE columns; add a UNIQUE index separately".to_string(),
                ));
            }
            if !column.nullable && column.default_value.is_none() {
                return Err(HematiteError::ParseError(
                    "ALTER TABLE ADD COLUMN requires the new column to be nullable or have a DEFAULT value".to_string(),
                ));
            }
            if column.check_constraint.is_some() {
                return Err(HematiteError::ParseError(
                    "ALTER TABLE ADD COLUMN does not support CHECK constraints".to_string(),
                ));
            }
            if column.references.is_some() {
                return Err(HematiteError::ParseError(
                    "ALTER TABLE ADD COLUMN does not support FOREIGN KEY constraints".to_string(),
                ));
            }
            if let Some(default_value) = &column.default_value {
                if default_value.is_null() && !column.nullable {
                    return Err(HematiteError::ParseError(format!(
                        "Column '{}' cannot use DEFAULT NULL when declared NOT NULL",
                        column.name
                    )));
                }
                if !default_value.is_null() && !default_value.is_compatible_with(column.data_type) {
                    return Err(HematiteError::ParseError(format!(
                        "DEFAULT value for column '{}' is incompatible with {:?}",
                        column.name, column.data_type
                    )));
                }
            }
        }
        AlterOperation::DropColumn(column_name) => {
            validate_drop_column(alter, catalog, column_name)?;
        }
        AlterOperation::AlterColumnSetDefault {
            column_name,
            default_value,
        } => {
            validate_set_column_default(alter, catalog, column_name, default_value)?;
        }
        AlterOperation::AlterColumnDropDefault { column_name } => {
            validate_existing_column(alter, catalog, column_name)?;
        }
        AlterOperation::AlterColumnSetNotNull { column_name } => {
            validate_existing_column(alter, catalog, column_name)?;
        }
        AlterOperation::AlterColumnDropNotNull { column_name } => {
            validate_drop_not_null(alter, catalog, column_name)?;
        }
    }

    Ok(())
}

fn validate_create_index(create_index: &CreateIndexStatement, catalog: &Schema) -> Result<()> {
    let table = require_table(catalog, &create_index.table)?;

    if create_index.columns.is_empty() {
        return Err(HematiteError::ParseError(
            "CREATE INDEX must specify at least one column".to_string(),
        ));
    }

    validate_named_columns(&create_index.columns, "CREATE INDEX", |column| {
        if table.get_column_by_name(column).is_some() {
            Ok(())
        } else {
            Err(HematiteError::ParseError(format!(
                "Column '{}' does not exist in table '{}'",
                column, create_index.table
            )))
        }
    })?;

    if table
        .get_secondary_index(&create_index.index_name)
        .is_some()
    {
        if create_index.if_not_exists {
            return Ok(());
        }
        return Err(HematiteError::ParseError(format!(
            "Index '{}' already exists on table '{}'",
            create_index.index_name, create_index.table
        )));
    }

    Ok(())
}

fn validate_drop_index(drop_index: &DropIndexStatement, catalog: &Schema) -> Result<()> {
    if drop_index.if_exists && catalog.get_table_by_name(&drop_index.table).is_none() {
        return Ok(());
    }
    let table = require_table(catalog, &drop_index.table)?;

    if table.get_secondary_index(&drop_index.index_name).is_none() {
        if drop_index.if_exists {
            return Ok(());
        }
        return Err(HematiteError::ParseError(format!(
            "Index '{}' does not exist on table '{}'",
            drop_index.index_name, drop_index.table
        )));
    }

    Ok(())
}

fn validate_table_reference(
    select: &SelectStatement,
    catalog: &Schema,
    from: &TableReference,
    outer_bindings: &[SourceBinding],
) -> Result<()> {
    match from {
        TableReference::Table(_, _) => Ok(()),
        TableReference::Derived { subquery, .. } => {
            validate_select(subquery, catalog)?;
            let _ = projected_column_names(subquery, catalog)?;
            Ok(())
        }
        TableReference::CrossJoin(left, right) => {
            validate_table_reference(select, catalog, left, outer_bindings)?;
            validate_table_reference(select, catalog, right, outer_bindings)
        }
        TableReference::InnerJoin { left, right, on }
        | TableReference::LeftJoin { left, right, on }
        | TableReference::RightJoin { left, right, on }
        | TableReference::FullOuterJoin { left, right, on } => {
            validate_table_reference(select, catalog, left, outer_bindings)?;
            validate_table_reference(select, catalog, right, outer_bindings)?;
            validate_condition(select, on, catalog, from, outer_bindings)
        }
    }
}

fn validate_condition(
    select: &SelectStatement,
    condition: &Condition,
    catalog: &Schema,
    from: &TableReference,
    outer_bindings: &[SourceBinding],
) -> Result<()> {
    match condition {
        Condition::Comparison { left, right, .. } => {
            validate_expression(select, left, catalog, from, outer_bindings)?;
            validate_expression(select, right, catalog, from, outer_bindings)?;
        }
        Condition::InList { expr, values, .. } => {
            validate_expression(select, expr, catalog, from, outer_bindings)?;
            for value in values {
                validate_expression(select, value, catalog, from, outer_bindings)?;
            }
        }
        Condition::InSubquery { expr, subquery, .. } => {
            validate_expression(select, expr, catalog, from, outer_bindings)?;
            validate_select_with_outer_bindings(
                subquery,
                catalog,
                &combined_outer_bindings(select, catalog, from, outer_bindings)?,
            )?;
            if subquery.columns.len() != 1 {
                return Err(HematiteError::ParseError(
                    "Subquery predicates require exactly one selected column".to_string(),
                ));
            }
        }
        Condition::Between {
            expr, lower, upper, ..
        } => {
            validate_expression(select, expr, catalog, from, outer_bindings)?;
            validate_expression(select, lower, catalog, from, outer_bindings)?;
            validate_expression(select, upper, catalog, from, outer_bindings)?;
        }
        Condition::Like { expr, pattern, .. } => {
            validate_expression(select, expr, catalog, from, outer_bindings)?;
            validate_expression(select, pattern, catalog, from, outer_bindings)?;
        }
        Condition::Exists { subquery, .. } => {
            validate_select_with_outer_bindings(
                subquery,
                catalog,
                &combined_outer_bindings(select, catalog, from, outer_bindings)?,
            )?;
        }
        Condition::NullCheck { expr, .. } => {
            validate_expression(select, expr, catalog, from, outer_bindings)?;
        }
        Condition::Not(condition) => {
            validate_condition(select, condition, catalog, from, outer_bindings)?;
        }
        Condition::Logical { left, right, .. } => {
            validate_condition(select, left, catalog, from, outer_bindings)?;
            validate_condition(select, right, catalog, from, outer_bindings)?;
        }
    }

    Ok(())
}

fn validate_expression(
    select: &SelectStatement,
    expr: &Expression,
    catalog: &Schema,
    from: &TableReference,
    outer_bindings: &[SourceBinding],
) -> Result<()> {
    match expr {
        Expression::Column(name) => {
            validate_column_reference_with_outer(select, name, catalog, from, outer_bindings)?
        }
        Expression::ScalarSubquery(subquery) => {
            validate_select_with_outer_bindings(
                subquery,
                catalog,
                &combined_outer_bindings(select, catalog, from, outer_bindings)?,
            )?;
            if subquery.columns.len() != 1 {
                return Err(HematiteError::ParseError(
                    "Scalar subqueries require exactly one selected column".to_string(),
                ));
            }
        }
        Expression::Case {
            branches,
            else_expr,
        } => {
            for branch in branches {
                validate_expression(select, &branch.condition, catalog, from, outer_bindings)?;
                validate_expression(select, &branch.result, catalog, from, outer_bindings)?;
            }
            if let Some(else_expr) = else_expr {
                validate_expression(select, else_expr, catalog, from, outer_bindings)?;
            }
        }
        Expression::ScalarFunctionCall { args, .. } => {
            for arg in args {
                validate_expression(select, arg, catalog, from, outer_bindings)?;
            }
        }
        Expression::AggregateCall { target, .. } => {
            if let AggregateTarget::Column(name) = target {
                validate_column_reference_with_outer(select, name, catalog, from, outer_bindings)?;
            }
        }
        Expression::UnaryMinus(expr) | Expression::UnaryNot(expr) => {
            validate_expression(select, expr, catalog, from, outer_bindings)?;
        }
        Expression::Binary { left, right, .. }
        | Expression::Comparison { left, right, .. }
        | Expression::Logical { left, right, .. } => {
            validate_expression(select, left, catalog, from, outer_bindings)?;
            validate_expression(select, right, catalog, from, outer_bindings)?;
        }
        Expression::InList { expr, values, .. } => {
            validate_expression(select, expr, catalog, from, outer_bindings)?;
            for value in values {
                validate_expression(select, value, catalog, from, outer_bindings)?;
            }
        }
        Expression::InSubquery { expr, subquery, .. } => {
            validate_expression(select, expr, catalog, from, outer_bindings)?;
            validate_select_with_outer_bindings(
                subquery,
                catalog,
                &combined_outer_bindings(select, catalog, from, outer_bindings)?,
            )?;
            if subquery.columns.len() != 1 {
                return Err(HematiteError::ParseError(
                    "Subquery predicates require exactly one selected column".to_string(),
                ));
            }
        }
        Expression::Between {
            expr, lower, upper, ..
        } => {
            validate_expression(select, expr, catalog, from, outer_bindings)?;
            validate_expression(select, lower, catalog, from, outer_bindings)?;
            validate_expression(select, upper, catalog, from, outer_bindings)?;
        }
        Expression::Like { expr, pattern, .. } => {
            validate_expression(select, expr, catalog, from, outer_bindings)?;
            validate_expression(select, pattern, catalog, from, outer_bindings)?;
        }
        Expression::Exists { subquery, .. } => {
            validate_select_with_outer_bindings(
                subquery,
                catalog,
                &combined_outer_bindings(select, catalog, from, outer_bindings)?,
            )?;
        }
        Expression::NullCheck { expr, .. } => {
            validate_expression(select, expr, catalog, from, outer_bindings)?;
        }
        Expression::Literal(_) | Expression::Parameter(_) => {}
    }

    Ok(())
}

fn collect_source_bindings(
    select: &SelectStatement,
    catalog: &Schema,
    from: &TableReference,
) -> Result<Vec<SourceBinding>> {
    let mut bindings = Vec::new();
    collect_source_bindings_into(select, catalog, from, &mut bindings)?;
    Ok(bindings)
}

fn collect_source_bindings_into(
    select: &SelectStatement,
    catalog: &Schema,
    from: &TableReference,
    bindings: &mut Vec<SourceBinding>,
) -> Result<()> {
    match from {
        TableReference::Table(table_name, alias) => {
            if let Some(cte) = select.lookup_cte(table_name) {
                if !cte.recursive {
                    validate_select(&cte.query, catalog)?;
                }
                bindings.push(SourceBinding {
                    source_name: table_name.clone(),
                    alias: alias.clone(),
                    columns: projected_column_names(&cte.query, catalog)?,
                    has_hidden_rowid: false,
                });
                Ok(())
            } else {
                let table = catalog.get_table_by_name(table_name).ok_or_else(|| {
                    HematiteError::ParseError(format!("Table '{}' does not exist", table_name))
                })?;
                bindings.push(SourceBinding {
                    source_name: table_name.clone(),
                    alias: alias.clone(),
                    columns: table
                        .columns
                        .iter()
                        .map(|column| column.name.clone())
                        .collect(),
                    has_hidden_rowid: true,
                });
                Ok(())
            }
        }
        TableReference::Derived { subquery, alias } => {
            validate_select(subquery, catalog)?;
            bindings.push(SourceBinding {
                source_name: alias.clone(),
                alias: None,
                columns: projected_column_names(subquery, catalog)?,
                has_hidden_rowid: false,
            });
            Ok(())
        }
        TableReference::CrossJoin(left, right) => {
            collect_source_bindings_into(select, catalog, left, bindings)?;
            collect_source_bindings_into(select, catalog, right, bindings)
        }
        TableReference::InnerJoin { left, right, .. }
        | TableReference::LeftJoin { left, right, .. }
        | TableReference::RightJoin { left, right, .. }
        | TableReference::FullOuterJoin { left, right, .. } => {
            collect_source_bindings_into(select, catalog, left, bindings)?;
            collect_source_bindings_into(select, catalog, right, bindings)
        }
    }
}

fn projected_column_names(select: &SelectStatement, catalog: &Schema) -> Result<Vec<String>> {
    let mut names = Vec::with_capacity(select.columns.len());
    for (index, item) in select.columns.iter().enumerate() {
        if let Some(alias) = select
            .column_aliases
            .get(index)
            .and_then(|alias| alias.clone())
        {
            names.push(alias);
            continue;
        }

        match item {
            SelectItem::Wildcard => {
                return Err(HematiteError::ParseError(
                    "Wildcard projections are not supported in derived tables or CTEs".to_string(),
                ))
            }
            SelectItem::Column(name) => {
                validate_column_reference(select, name, catalog, &select.from)?;
                if let Some(name) = SelectStatement::default_output_name(item, index) {
                    names.push(name);
                }
            }
            SelectItem::CountAll | SelectItem::Aggregate { .. } => {
                if let Some(name) = SelectStatement::default_output_name(item, index) {
                    names.push(name);
                }
            }
            SelectItem::Expression(_) => {
                return Err(HematiteError::ParseError(
                    "Expression projections in derived tables or CTEs require aliases".to_string(),
                ))
            }
        }
    }
    Ok(names)
}

fn validate_column_reference_with_outer(
    select: &SelectStatement,
    name: &str,
    catalog: &Schema,
    from: &TableReference,
    outer_bindings: &[SourceBinding],
) -> Result<()> {
    let (qualifier, column_name) = SelectStatement::split_column_reference(name);
    let local_bindings = collect_source_bindings(select, catalog, from)?;
    let local_matches = collect_matching_source_names(qualifier, column_name, &local_bindings)?;
    if !local_matches.is_empty() {
        return match local_matches.len() {
            1 => Ok(()),
            _ => Err(HematiteError::ParseError(format!(
                "Column reference '{}' is ambiguous",
                name
            ))),
        };
    }

    let outer_matches = collect_matching_source_names(qualifier, column_name, outer_bindings)?;
    match outer_matches.len() {
        0 => {
            if let Some(qualifier) = qualifier {
                Err(HematiteError::ParseError(format!(
                    "Column '{}' does not exist in table '{}'",
                    column_name, qualifier
                )))
            } else {
                Err(HematiteError::ParseError(format!(
                    "Column '{}' does not exist in the query source set",
                    column_name
                )))
            }
        }
        1 => Ok(()),
        _ => Err(HematiteError::ParseError(format!(
            "Column reference '{}' is ambiguous",
            name
        ))),
    }
}

fn collect_matching_source_names(
    qualifier: Option<&str>,
    column_name: &str,
    bindings: &[SourceBinding],
) -> Result<Vec<String>> {
    let candidate_bindings: Vec<&SourceBinding> = if let Some(qualifier) = qualifier {
        bindings
            .iter()
            .filter(|binding| {
                binding.source_name == qualifier
                    || binding
                        .alias
                        .as_deref()
                        .is_some_and(|alias| alias == qualifier)
            })
            .collect()
    } else {
        bindings.iter().collect()
    };
    let mut matched_tables = Vec::new();

    for binding in candidate_bindings {
        if binding.columns.iter().any(|column| column == column_name)
            || (binding.has_hidden_rowid && SelectStatement::is_hidden_rowid(column_name))
        {
            matched_tables.push(binding.source_name.clone());
        }
    }

    Ok(matched_tables)
}

fn combined_outer_bindings(
    select: &SelectStatement,
    catalog: &Schema,
    from: &TableReference,
    outer_bindings: &[SourceBinding],
) -> Result<Vec<SourceBinding>> {
    let mut bindings = collect_source_bindings(select, catalog, from)?;
    bindings.extend(outer_bindings.iter().cloned());
    Ok(bindings)
}

fn foreign_keys(create: &CreateStatement) -> Vec<&ForeignKeyDefinition> {
    let mut foreign_keys = create
        .columns
        .iter()
        .filter_map(|column| column.references.as_ref())
        .collect::<Vec<_>>();

    foreign_keys.extend(
        create
            .constraints
            .iter()
            .filter_map(|constraint| match constraint {
                TableConstraint::Check(_) | TableConstraint::Unique(_) => None,
                TableConstraint::ForeignKey(foreign_key) => Some(foreign_key),
            }),
    );

    foreign_keys
}

fn validate_unique_constraint(
    create: &CreateStatement,
    unique_constraint: &UniqueConstraintDefinition,
) -> Result<()> {
    if unique_constraint.columns.is_empty() {
        return Err(HematiteError::ParseError(
            "UNIQUE constraint must reference at least one column".to_string(),
        ));
    }

    validate_local_constraint_columns(create, &unique_constraint.columns, "UNIQUE constraint")?;
    Ok(())
}

fn validate_local_constraint_columns(
    create: &CreateStatement,
    columns: &[String],
    constraint_label: &str,
) -> Result<()> {
    validate_named_columns(columns, constraint_label, |column| {
        if create
            .columns
            .iter()
            .any(|candidate| candidate.name == column)
        {
            Ok(())
        } else {
            Err(HematiteError::ParseError(format!(
                "{} column '{}' does not exist in table '{}'",
                constraint_label, column, create.table
            )))
        }
    })
}

fn validate_foreign_key(
    create: &CreateStatement,
    catalog: &Schema,
    foreign_key: &ForeignKeyDefinition,
) -> Result<()> {
    if foreign_key.columns.is_empty() {
        return Err(HematiteError::ParseError(
            "Foreign key must reference at least one local column".to_string(),
        ));
    }
    if foreign_key.columns.len() != foreign_key.referenced_columns.len() {
        return Err(HematiteError::ParseError(format!(
            "Foreign key on table '{}' must reference the same number of local and parent columns",
            create.table
        )));
    }
    validate_local_constraint_columns(create, &foreign_key.columns, "Foreign key")?;

    let referenced_table = catalog
        .get_table_by_name(&foreign_key.referenced_table)
        .ok_or_else(|| {
            HematiteError::ParseError(format!(
                "Referenced table '{}' does not exist",
                foreign_key.referenced_table
            ))
        })?;
    let referenced_column_indices = referenced_column_indices(referenced_table, foreign_key)?;
    let references_primary_key = referenced_table.primary_key_columns == referenced_column_indices;
    let references_unique_index = referenced_table
        .secondary_indexes
        .iter()
        .any(|index| index.unique && index.column_indices == referenced_column_indices);

    if !references_primary_key && !references_unique_index {
        return Err(HematiteError::ParseError(format!(
            "Foreign key '{}.{:?}' must reference a PRIMARY KEY or UNIQUE index with the same column list",
            foreign_key.referenced_table, foreign_key.referenced_columns
        )));
    }

    Ok(())
}

fn referenced_column_indices(
    referenced_table: &Table,
    foreign_key: &ForeignKeyDefinition,
) -> Result<Vec<usize>> {
    foreign_key
        .referenced_columns
        .iter()
        .map(|column| {
            referenced_table.get_column_index(column).ok_or_else(|| {
                HematiteError::ParseError(format!(
                    "Referenced column '{}.{}' does not exist",
                    foreign_key.referenced_table, column
                ))
            })
        })
        .collect()
}

fn validate_rename_column(
    alter: &AlterStatement,
    catalog: &Schema,
    old_name: &str,
    new_name: &str,
) -> Result<()> {
    let table = require_table(catalog, &alter.table)?;
    if old_name == new_name {
        return Err(HematiteError::ParseError(
            "ALTER TABLE RENAME COLUMN requires a different column name".to_string(),
        ));
    }
    if table.get_column_by_name(old_name).is_none() {
        return Err(HematiteError::ParseError(format!(
            "Column '{}' does not exist in table '{}'",
            old_name, alter.table
        )));
    }
    if table.get_column_by_name(new_name).is_some() {
        return Err(HematiteError::ParseError(format!(
            "Column '{}' already exists in table '{}'",
            new_name, alter.table
        )));
    }
    Ok(())
}

fn validate_existing_column(
    alter: &AlterStatement,
    catalog: &Schema,
    column_name: &str,
) -> Result<()> {
    let table = require_table(catalog, &alter.table)?;
    if table.get_column_by_name(column_name).is_none() {
        return Err(HematiteError::ParseError(format!(
            "Column '{}' does not exist in table '{}'",
            column_name, alter.table
        )));
    }
    Ok(())
}

fn validate_set_column_default(
    alter: &AlterStatement,
    catalog: &Schema,
    column_name: &str,
    default_value: &LiteralValue,
) -> Result<()> {
    let table = require_table(catalog, &alter.table)?;
    let column = table.get_column_by_name(column_name).ok_or_else(|| {
        HematiteError::ParseError(format!(
            "Column '{}' does not exist in table '{}'",
            column_name, alter.table
        ))
    })?;
    if default_value.is_null() && !column.nullable {
        return Err(HematiteError::ParseError(format!(
            "Column '{}' cannot use DEFAULT NULL while declared NOT NULL",
            column_name
        )));
    }
    if !default_value.is_null()
        && !default_value.is_compatible_with(sql_type_name_for_catalog_type(column.data_type))
    {
        return Err(HematiteError::ParseError(format!(
            "DEFAULT value for column '{}' is incompatible with {:?}",
            column_name, column.data_type
        )));
    }
    Ok(())
}

fn validate_drop_not_null(
    alter: &AlterStatement,
    catalog: &Schema,
    column_name: &str,
) -> Result<()> {
    let table = require_table(catalog, &alter.table)?;
    let column = table.get_column_by_name(column_name).ok_or_else(|| {
        HematiteError::ParseError(format!(
            "Column '{}' does not exist in table '{}'",
            column_name, alter.table
        ))
    })?;
    if column.primary_key {
        return Err(HematiteError::ParseError(format!(
            "Primary-key column '{}' cannot drop NOT NULL",
            column_name
        )));
    }
    if column.auto_increment {
        return Err(HematiteError::ParseError(format!(
            "AUTO_INCREMENT column '{}' cannot drop NOT NULL",
            column_name
        )));
    }
    Ok(())
}

fn validate_drop_column(alter: &AlterStatement, catalog: &Schema, column_name: &str) -> Result<()> {
    let table = require_table(catalog, &alter.table)?;
    let column_index = table.get_column_index(column_name).ok_or_else(|| {
        HematiteError::ParseError(format!(
            "Column '{}' does not exist in table '{}'",
            column_name, alter.table
        ))
    })?;
    if table.columns.len() == 1 {
        return Err(HematiteError::ParseError(
            "ALTER TABLE DROP COLUMN cannot remove the last column".to_string(),
        ));
    }
    if table.primary_key_columns.contains(&column_index) {
        return Err(HematiteError::ParseError(format!(
            "Cannot drop primary-key column '{}'",
            column_name
        )));
    }
    if table
        .secondary_indexes
        .iter()
        .any(|index| index.column_indices.contains(&column_index))
    {
        return Err(HematiteError::ParseError(format!(
            "Cannot drop column '{}' because it is used by an index",
            column_name
        )));
    }
    if table
        .foreign_keys
        .iter()
        .any(|foreign_key| foreign_key.column_indices.contains(&column_index))
    {
        return Err(HematiteError::ParseError(format!(
            "Cannot drop column '{}' because it is used by a foreign key",
            column_name
        )));
    }
    for constraint in &table.check_constraints {
        let condition =
            crate::parser::parser::parse_condition_fragment(&constraint.expression_sql)?;
        if condition.references_column(column_name, Some(&table.name)) {
            return Err(HematiteError::ParseError(format!(
                "Cannot drop column '{}' because it is used by a CHECK constraint",
                column_name
            )));
        }
    }
    if catalog.tables().values().any(|other_table| {
        other_table.name != table.name
            && other_table.foreign_keys.iter().any(|foreign_key| {
                foreign_key.referenced_table == table.name
                    && foreign_key
                        .referenced_columns
                        .iter()
                        .any(|referenced_column| referenced_column == column_name)
            })
    }) {
        return Err(HematiteError::ParseError(format!(
            "Cannot drop column '{}' because it is referenced by a foreign key",
            column_name
        )));
    }
    Ok(())
}

fn expression_contains_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::AggregateCall { .. } => true,
        Expression::ScalarSubquery(_) => false,
        Expression::Case {
            branches,
            else_expr,
        } => {
            branches.iter().any(|branch| {
                expression_contains_aggregate(&branch.condition)
                    || expression_contains_aggregate(&branch.result)
            }) || else_expr
                .as_ref()
                .is_some_and(|expr| expression_contains_aggregate(expr))
        }
        Expression::ScalarFunctionCall { args, .. } => {
            args.iter().any(expression_contains_aggregate)
        }
        Expression::UnaryMinus(expr)
        | Expression::UnaryNot(expr)
        | Expression::NullCheck { expr, .. } => expression_contains_aggregate(expr),
        Expression::Binary { left, right, .. }
        | Expression::Comparison { left, right, .. }
        | Expression::Logical { left, right, .. } => {
            expression_contains_aggregate(left) || expression_contains_aggregate(right)
        }
        Expression::InList { expr, values, .. } => {
            expression_contains_aggregate(expr) || values.iter().any(expression_contains_aggregate)
        }
        Expression::InSubquery { expr, subquery, .. } => {
            expression_contains_aggregate(expr)
                || subquery.where_clause.as_ref().is_some_and(|where_clause| {
                    where_clause
                        .conditions
                        .iter()
                        .any(condition_contains_aggregate)
                })
        }
        Expression::Between {
            expr, lower, upper, ..
        } => {
            expression_contains_aggregate(expr)
                || expression_contains_aggregate(lower)
                || expression_contains_aggregate(upper)
        }
        Expression::Like { expr, pattern, .. } => {
            expression_contains_aggregate(expr) || expression_contains_aggregate(pattern)
        }
        Expression::Exists { subquery, .. } => {
            subquery.where_clause.as_ref().is_some_and(|where_clause| {
                where_clause
                    .conditions
                    .iter()
                    .any(condition_contains_aggregate)
            })
        }
        Expression::Column(_) | Expression::Literal(_) | Expression::Parameter(_) => false,
    }
}

fn condition_contains_aggregate(condition: &Condition) -> bool {
    match condition {
        Condition::Comparison { left, right, .. } => {
            expression_contains_aggregate(left) || expression_contains_aggregate(right)
        }
        Condition::InList { expr, values, .. } => {
            expression_contains_aggregate(expr) || values.iter().any(expression_contains_aggregate)
        }
        Condition::InSubquery { expr, subquery, .. } => {
            expression_contains_aggregate(expr)
                || subquery.where_clause.as_ref().is_some_and(|where_clause| {
                    where_clause
                        .conditions
                        .iter()
                        .any(condition_contains_aggregate)
                })
        }
        Condition::Between {
            expr, lower, upper, ..
        } => {
            expression_contains_aggregate(expr)
                || expression_contains_aggregate(lower)
                || expression_contains_aggregate(upper)
        }
        Condition::Like { expr, pattern, .. } => {
            expression_contains_aggregate(expr) || expression_contains_aggregate(pattern)
        }
        Condition::Exists { subquery, .. } => {
            subquery.where_clause.as_ref().is_some_and(|where_clause| {
                where_clause
                    .conditions
                    .iter()
                    .any(condition_contains_aggregate)
            })
        }
        Condition::NullCheck { expr, .. } => expression_contains_aggregate(expr),
        Condition::Not(condition) => condition_contains_aggregate(condition),
        Condition::Logical { left, right, .. } => {
            condition_contains_aggregate(left) || condition_contains_aggregate(right)
        }
    }
}

fn require_table<'a>(catalog: &'a Schema, table_name: &str) -> Result<&'a Table> {
    catalog
        .get_table_by_name(table_name)
        .ok_or_else(|| HematiteError::ParseError(format!("Table '{}' does not exist", table_name)))
}

fn sql_type_name_for_catalog_type(data_type: crate::catalog::DataType) -> SqlTypeName {
    match data_type {
        crate::catalog::DataType::Integer => SqlTypeName::Integer,
        crate::catalog::DataType::Text => SqlTypeName::Text,
        crate::catalog::DataType::Boolean => SqlTypeName::Boolean,
        crate::catalog::DataType::Float => SqlTypeName::Float,
    }
}

fn validate_named_columns<F>(
    columns: &[String],
    constraint_label: &str,
    mut validate_column: F,
) -> Result<()>
where
    F: FnMut(&str) -> Result<()>,
{
    let mut seen = std::collections::HashSet::new();
    for column in columns {
        if !seen.insert(column) {
            return Err(HematiteError::ParseError(format!(
                "{} repeats column '{}'",
                constraint_label, column
            )));
        }
        validate_column(column)?;
    }
    Ok(())
}
