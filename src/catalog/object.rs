//! Schema-level relational objects beyond base tables.

use crate::error::{HematiteError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct View {
    pub name: String,
    pub query_sql: String,
    pub column_names: Vec<String>,
    pub dependencies: Vec<String>,
}

impl View {
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(HematiteError::StorageError(
                "View name cannot be empty".to_string(),
            ));
        }
        if self.query_sql.trim().is_empty() {
            return Err(HematiteError::StorageError(format!(
                "View '{}' must store a query",
                self.name
            )));
        }
        if self.column_names.is_empty() {
            return Err(HematiteError::StorageError(format!(
                "View '{}' must expose at least one column",
                self.name
            )));
        }
        Ok(())
    }

    pub fn serialize(&self, buffer: &mut Vec<u8>) {
        write_string(buffer, &self.name);
        write_string(buffer, &self.query_sql);
        buffer.extend_from_slice(&(self.column_names.len() as u32).to_le_bytes());
        for column_name in &self.column_names {
            write_string(buffer, column_name);
        }
        buffer.extend_from_slice(&(self.dependencies.len() as u32).to_le_bytes());
        for dependency in &self.dependencies {
            write_string(buffer, dependency);
        }
    }

    pub fn deserialize(buffer: &[u8], offset: &mut usize) -> Result<Self> {
        let name = read_string_with_len(buffer, offset, "view name")?;
        let query_sql = read_string_with_len(buffer, offset, "view query")?;
        let column_count = read_len(buffer, offset, "view column count")?;
        let mut column_names = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            column_names.push(read_string_with_len(buffer, offset, "view column name")?);
        }
        let dependency_count = read_len(buffer, offset, "view dependency count")?;
        let mut dependencies = Vec::with_capacity(dependency_count);
        for _ in 0..dependency_count {
            dependencies.push(read_string_with_len(buffer, offset, "view dependency")?);
        }
        let view = Self {
            name,
            query_sql,
            column_names,
            dependencies,
        };
        view.validate()?;
        Ok(view)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trigger {
    pub name: String,
    pub table_name: String,
    pub event: TriggerEvent,
    pub body_sql: String,
    pub old_alias: Option<String>,
    pub new_alias: Option<String>,
}

impl Trigger {
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(HematiteError::StorageError(
                "Trigger name cannot be empty".to_string(),
            ));
        }
        if self.table_name.is_empty() {
            return Err(HematiteError::StorageError(format!(
                "Trigger '{}' must reference a table",
                self.name
            )));
        }
        if self.body_sql.trim().is_empty() {
            return Err(HematiteError::StorageError(format!(
                "Trigger '{}' must store a body statement",
                self.name
            )));
        }
        Ok(())
    }

    pub fn serialize(&self, buffer: &mut Vec<u8>) {
        write_string(buffer, &self.name);
        write_string(buffer, &self.table_name);
        buffer.push(trigger_event_to_byte(self.event));
        write_string(buffer, &self.body_sql);
        write_optional_string(buffer, self.old_alias.as_deref());
        write_optional_string(buffer, self.new_alias.as_deref());
    }

    pub fn deserialize(buffer: &[u8], offset: &mut usize) -> Result<Self> {
        let name = read_string_with_len(buffer, offset, "trigger name")?;
        let table_name = read_string_with_len(buffer, offset, "trigger table")?;
        let event = read_trigger_event(buffer, offset)?;
        let body_sql = read_string_with_len(buffer, offset, "trigger body")?;
        let old_alias = read_optional_string(buffer, offset, "trigger OLD alias")?;
        let new_alias = read_optional_string(buffer, offset, "trigger NEW alias")?;
        let trigger = Self {
            name,
            table_name,
            event,
            body_sql,
            old_alias,
            new_alias,
        };
        trigger.validate()?;
        Ok(trigger)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamedConstraintKind {
    Check,
    ForeignKey,
    Unique,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedConstraint {
    pub table_name: String,
    pub name: String,
    pub kind: NamedConstraintKind,
}

fn write_string(buffer: &mut Vec<u8>, value: &str) {
    buffer.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buffer.extend_from_slice(value.as_bytes());
}

fn write_optional_string(buffer: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            buffer.push(1);
            write_string(buffer, value);
        }
        None => buffer.push(0),
    }
}

fn read_len(buffer: &[u8], offset: &mut usize, label: &str) -> Result<usize> {
    if *offset + 4 > buffer.len() {
        return Err(HematiteError::CorruptedData(format!("Invalid {}", label)));
    }
    let len = u32::from_le_bytes(
        buffer[*offset..*offset + 4]
            .try_into()
            .map_err(|_| HematiteError::CorruptedData(format!("Invalid {}", label)))?,
    ) as usize;
    *offset += 4;
    Ok(len)
}

fn read_string_with_len(buffer: &[u8], offset: &mut usize, label: &str) -> Result<String> {
    let len = read_len(buffer, offset, &format!("{} length", label))?;
    if *offset + len > buffer.len() {
        return Err(HematiteError::CorruptedData(format!("Invalid {}", label)));
    }
    let value = String::from_utf8(buffer[*offset..*offset + len].to_vec())
        .map_err(|_| HematiteError::CorruptedData(format!("Invalid UTF-8 in {}", label)))?;
    *offset += len;
    Ok(value)
}

fn read_optional_string(buffer: &[u8], offset: &mut usize, label: &str) -> Result<Option<String>> {
    if *offset >= buffer.len() {
        return Err(HematiteError::CorruptedData(format!("Invalid {}", label)));
    }
    let flag = buffer[*offset];
    *offset += 1;
    match flag {
        0 => Ok(None),
        1 => Ok(Some(read_string_with_len(buffer, offset, label)?)),
        _ => Err(HematiteError::CorruptedData(format!("Invalid {}", label))),
    }
}

fn trigger_event_to_byte(event: TriggerEvent) -> u8 {
    match event {
        TriggerEvent::Insert => 0,
        TriggerEvent::Update => 1,
        TriggerEvent::Delete => 2,
    }
}

fn read_trigger_event(buffer: &[u8], offset: &mut usize) -> Result<TriggerEvent> {
    if *offset >= buffer.len() {
        return Err(HematiteError::CorruptedData(
            "Invalid trigger event".to_string(),
        ));
    }
    let event = match buffer[*offset] {
        0 => TriggerEvent::Insert,
        1 => TriggerEvent::Update,
        2 => TriggerEvent::Delete,
        _ => {
            return Err(HematiteError::CorruptedData(
                "Invalid trigger event".to_string(),
            ))
        }
    };
    *offset += 1;
    Ok(event)
}
