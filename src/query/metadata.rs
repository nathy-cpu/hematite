//! Metadata result shaping for SQL introspection commands.

use crate::catalog::{
    table::ForeignKeyAction, Catalog, SecondaryIndex, Table, TriggerEvent, Value,
};
use crate::error::{HematiteError, Result};

use super::runtime::QueryResult;

pub(crate) fn describe_table(catalog: &Catalog, table_name: &str) -> Result<QueryResult> {
    let table = catalog.get_table_by_name(table_name)?.ok_or_else(|| {
        HematiteError::ParseError(format!("Table '{}' does not exist", table_name))
    })?;
    let column_metadata = build_table_column_metadata(&table);

    let rows = table
        .columns
        .iter()
        .enumerate()
        .map(|(column_index, column)| {
            let metadata = &column_metadata[column_index];
            vec![
                Value::Text(column.name.clone()),
                Value::Text(column.data_type.name().to_string()),
                Value::Boolean(column.nullable),
                match &column.default_value {
                    Some(default) => Value::Text(format!("{default:?}")),
                    None => Value::Null,
                },
                Value::Boolean(column.primary_key),
                Value::Boolean(metadata.is_unique),
                Value::Boolean(column.auto_increment),
                text_or_null(metadata.constraints.clone()),
                text_or_null(metadata.indexes.clone()),
            ]
        })
        .collect();

    Ok(QueryResult {
        affected_rows: 0,
        columns: vec![
            "column".to_string(),
            "type".to_string(),
            "nullable".to_string(),
            "default".to_string(),
            "primary_key".to_string(),
            "unique".to_string(),
            "auto_increment".to_string(),
            "constraints".to_string(),
            "indexes".to_string(),
        ],
        rows,
    })
}

pub(crate) fn show_tables(catalog: &Catalog) -> Result<QueryResult> {
    let mut tables = catalog.list_tables()?;
    tables.sort_by(|left, right| left.1.cmp(&right.1));

    Ok(QueryResult {
        affected_rows: 0,
        columns: vec!["table_name".to_string()],
        rows: tables
            .into_iter()
            .map(|(_, name)| vec![Value::Text(name)])
            .collect(),
    })
}

pub(crate) fn show_views(catalog: &Catalog) -> Result<QueryResult> {
    let mut views = catalog.list_views()?;
    views.sort();

    Ok(QueryResult {
        affected_rows: 0,
        columns: vec!["view_name".to_string()],
        rows: views
            .into_iter()
            .map(|name| vec![Value::Text(name)])
            .collect(),
    })
}

pub(crate) fn show_indexes(catalog: &Catalog, table_name: Option<&str>) -> Result<QueryResult> {
    let mut rows = Vec::new();
    let mut tables = catalog.list_tables()?;
    tables.sort_by(|left, right| left.1.cmp(&right.1));

    for (table_id, name) in tables {
        if table_name.is_some_and(|filter| filter != name) {
            continue;
        }
        let Some(table) = catalog.get_table(table_id)? else {
            continue;
        };
        for index in &table.secondary_indexes {
            let columns = index
                .column_indices
                .iter()
                .map(|&column_index| table.columns[column_index].name.clone())
                .collect::<Vec<_>>()
                .join(", ");
            rows.push(vec![
                Value::Text(table.name.clone()),
                Value::Text(index.name.clone()),
                Value::Boolean(index.unique),
                Value::Text(columns),
            ]);
        }
    }

    Ok(QueryResult {
        affected_rows: 0,
        columns: vec![
            "table_name".to_string(),
            "index_name".to_string(),
            "unique".to_string(),
            "columns".to_string(),
        ],
        rows,
    })
}

pub(crate) fn show_triggers(catalog: &Catalog, table_name: Option<&str>) -> Result<QueryResult> {
    let mut trigger_names = catalog.list_triggers()?;
    trigger_names.sort();
    let mut rows = Vec::new();
    for trigger_name in trigger_names {
        let Some(trigger) = catalog.get_trigger(&trigger_name)? else {
            continue;
        };
        if table_name.is_some_and(|filter| filter != trigger.table_name) {
            continue;
        }
        rows.push(vec![
            Value::Text(trigger.name.clone()),
            Value::Text(trigger.table_name.clone()),
            Value::Text(match trigger.event {
                TriggerEvent::Insert => "INSERT".to_string(),
                TriggerEvent::Update => "UPDATE".to_string(),
                TriggerEvent::Delete => "DELETE".to_string(),
            }),
        ]);
    }

    Ok(QueryResult {
        affected_rows: 0,
        columns: vec![
            "trigger_name".to_string(),
            "table_name".to_string(),
            "event".to_string(),
        ],
        rows,
    })
}

pub(crate) fn show_create_table(catalog: &Catalog, table_name: &str) -> Result<QueryResult> {
    let table = catalog.get_table_by_name(table_name)?.ok_or_else(|| {
        HematiteError::ParseError(format!("Table '{}' does not exist", table_name))
    })?;

    Ok(QueryResult {
        affected_rows: 0,
        columns: vec!["table_name".to_string(), "create_sql".to_string()],
        rows: vec![vec![
            Value::Text(table.name.clone()),
            Value::Text(render_create_table_sql(&table)),
        ]],
    })
}

pub(crate) fn show_create_view(catalog: &Catalog, view_name: &str) -> Result<QueryResult> {
    let view = catalog
        .get_view(view_name)?
        .ok_or_else(|| HematiteError::ParseError(format!("View '{}' does not exist", view_name)))?;

    Ok(QueryResult {
        affected_rows: 0,
        columns: vec!["view_name".to_string(), "create_sql".to_string()],
        rows: vec![vec![
            Value::Text(view.name.clone()),
            Value::Text(format!("CREATE VIEW {} AS {}", view.name, view.query_sql)),
        ]],
    })
}

struct TableColumnMetadata {
    is_unique: bool,
    constraints: Option<String>,
    indexes: Option<String>,
}

fn build_table_column_metadata(table: &Table) -> Vec<TableColumnMetadata> {
    let mut constraints = vec![Vec::new(); table.columns.len()];
    let mut indexes = vec![Vec::new(); table.columns.len()];
    let mut is_unique = vec![false; table.columns.len()];

    for &column_index in &table.primary_key_columns {
        if let Some(column_constraints) = constraints.get_mut(column_index) {
            column_constraints.push("PRIMARY KEY".to_string());
        }
    }

    for index in &table.secondary_indexes {
        if index.unique && index.column_indices.len() == 1 {
            if let Some(&column_index) = index.column_indices.first() {
                if let Some(is_unique_column) = is_unique.get_mut(column_index) {
                    *is_unique_column = true;
                }
            }
        }

        for &column_index in &index.column_indices {
            if let Some(column_indexes) = indexes.get_mut(column_index) {
                column_indexes.push(index.name.clone());
            }
        }
    }

    for constraint in table.list_named_constraints() {
        match constraint.kind {
            crate::catalog::NamedConstraintKind::Check => {
                if let Some(check) = table
                    .check_constraints
                    .iter()
                    .find(|check| check.name.as_deref() == Some(constraint.name.as_str()))
                {
                    for (column_index, column) in table.columns.iter().enumerate() {
                        if check.expression_sql.contains(&column.name) {
                            constraints[column_index].push(format!("CHECK {}", constraint.name));
                        }
                    }
                }
            }
            crate::catalog::NamedConstraintKind::ForeignKey => {
                if let Some(foreign_key) = table.foreign_keys.iter().find(|foreign_key| {
                    foreign_key.name.as_deref() == Some(constraint.name.as_str())
                }) {
                    for &column_index in &foreign_key.column_indices {
                        constraints[column_index].push(format!("FOREIGN KEY {}", constraint.name));
                    }
                }
            }
            crate::catalog::NamedConstraintKind::Unique => {
                if let Some(index) = table.secondary_indexes.iter().find(|index| {
                    index.name == constraint.name && index.unique
                }) {
                    for &column_index in &index.column_indices {
                        constraints[column_index].push(format!("UNIQUE {}", constraint.name));
                    }
                }
            }
        }
    }

    constraints
        .into_iter()
        .zip(indexes)
        .zip(is_unique)
        .map(|((constraints, indexes), is_unique)| TableColumnMetadata {
            is_unique,
            constraints: (!constraints.is_empty()).then(|| constraints.join(", ")),
            indexes: (!indexes.is_empty()).then(|| indexes.join(", ")),
        })
        .collect()
}

fn text_or_null(value: Option<String>) -> Value {
    value.map(Value::Text).unwrap_or(Value::Null)
}

fn render_create_table_sql(table: &Table) -> String {
    let mut definitions = Vec::new();

    for (index, column) in table.columns.iter().enumerate() {
        let mut parts = vec![format!("{} {}", column.name, column.data_type.name())];
        if let Some(character_set) = &column.character_set {
            parts.push(format!("CHARACTER SET {}", character_set));
        }
        if let Some(collation) = &column.collation {
            parts.push(format!("COLLATE {}", collation));
        }
        if !column.nullable {
            parts.push("NOT NULL".to_string());
        }
        if column.primary_key
            && table.primary_key_columns.len() == 1
            && table.primary_key_columns[0] == index
        {
            parts.push("PRIMARY KEY".to_string());
        }
        if column.auto_increment {
            parts.push("AUTO_INCREMENT".to_string());
        }
        if let Some(default_value) = &column.default_value {
            parts.push(format!("DEFAULT {:?}", default_value));
        }
        definitions.push(parts.join(" "));
    }

    if table.primary_key_columns.len() > 1 {
        definitions.push(format!(
            "PRIMARY KEY ({})",
            table
                .primary_key_columns
                .iter()
                .map(|&index| table.columns[index].name.clone())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    for index in table.secondary_indexes.iter().filter(|index| index.unique) {
        definitions.push(format!(
            "CONSTRAINT {} UNIQUE ({})",
            index.name,
            render_index_columns(table, index)
        ));
    }

    for constraint in &table.check_constraints {
        definitions.push(match &constraint.name {
            Some(name) => format!("CONSTRAINT {} CHECK ({})", name, constraint.expression_sql),
            None => format!("CHECK ({})", constraint.expression_sql),
        });
    }

    for foreign_key in &table.foreign_keys {
        let local_columns = foreign_key
            .column_indices
            .iter()
            .map(|&index| table.columns[index].name.clone())
            .collect::<Vec<_>>()
            .join(", ");
        let mut rendered = String::new();
        if let Some(name) = &foreign_key.name {
            rendered.push_str(&format!("CONSTRAINT {} ", name));
        }
        rendered.push_str(&format!(
            "FOREIGN KEY ({}) REFERENCES {} ({})",
            local_columns,
            foreign_key.referenced_table,
            foreign_key.referenced_columns.join(", ")
        ));
        rendered.push_str(&format!(
            " ON DELETE {} ON UPDATE {}",
            render_foreign_key_action(foreign_key.on_delete),
            render_foreign_key_action(foreign_key.on_update)
        ));
        definitions.push(rendered);
    }

    format!("CREATE TABLE {} ({})", table.name, definitions.join(", "))
}

fn render_index_columns(table: &Table, index: &SecondaryIndex) -> String {
    index
        .column_indices
        .iter()
        .map(|&column_index| table.columns[column_index].name.clone())
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_foreign_key_action(action: ForeignKeyAction) -> &'static str {
    match action {
        ForeignKeyAction::Restrict => "RESTRICT",
        ForeignKeyAction::Cascade => "CASCADE",
        ForeignKeyAction::SetNull => "SET NULL",
    }
}
