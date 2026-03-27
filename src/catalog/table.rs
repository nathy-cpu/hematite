//! Table definitions for database tables

use super::column::Column;
use super::ids::TableId;
use super::types::Value;
use crate::catalog::object::{NamedConstraint, NamedConstraintKind};
use crate::HematiteError;
use crate::Result;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Table {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<Column>,
    pub column_indices: HashMap<String, usize>,
    pub primary_key_columns: Vec<usize>,
    pub secondary_indexes: Vec<SecondaryIndex>,
    pub check_constraints: Vec<CheckConstraint>,
    pub foreign_keys: Vec<ForeignKeyConstraint>,
    pub root_page_id: u32,
    pub primary_key_index_root_page_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecondaryIndex {
    pub name: String,
    pub column_indices: Vec<usize>,
    pub root_page_id: u32,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckConstraint {
    pub name: Option<String>,
    pub expression_sql: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyConstraint {
    pub name: Option<String>,
    pub column_indices: Vec<usize>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignKeyAction {
    Restrict,
    Cascade,
    SetNull,
}

impl ForeignKeyConstraint {
    fn validate_for_table(&self, table_name: &str, column_count: usize) -> Result<()> {
        if self.column_indices.is_empty() {
            return Err(HematiteError::StorageError(
                "Foreign key must reference at least one local column".to_string(),
            ));
        }
        for &column_index in &self.column_indices {
            if column_index >= column_count {
                return Err(HematiteError::StorageError(format!(
                    "Foreign key references invalid column index {}",
                    column_index
                )));
            }
        }
        if self.column_indices.len() != self.referenced_columns.len() {
            return Err(HematiteError::StorageError(format!(
                "Foreign key has {} local columns but {} referenced columns",
                self.column_indices.len(),
                self.referenced_columns.len()
            )));
        }
        if self.referenced_table.is_empty() {
            return Err(HematiteError::StorageError(format!(
                "Foreign key on table '{}' must reference a table name",
                table_name
            )));
        }
        Ok(())
    }

    fn serialize(&self, buffer: &mut Vec<u8>) {
        write_optional_string(buffer, self.name.as_deref());
        buffer.extend_from_slice(&(self.column_indices.len() as u32).to_le_bytes());
        for &column_index in &self.column_indices {
            buffer.extend_from_slice(&(column_index as u32).to_le_bytes());
        }
        write_string(buffer, &self.referenced_table);
        buffer.extend_from_slice(&(self.referenced_columns.len() as u32).to_le_bytes());
        for referenced_column in &self.referenced_columns {
            write_string(buffer, referenced_column);
        }
        buffer.push(foreign_key_action_to_byte(self.on_delete));
        buffer.push(foreign_key_action_to_byte(self.on_update));
    }

    fn deserialize(buffer: &[u8], offset: &mut usize) -> Result<Self> {
        let name = read_optional_string(buffer, offset, "foreign key name")?;
        let local_column_count = read_len(buffer, offset, "foreign key local column count")?;
        let mut column_indices = Vec::with_capacity(local_column_count);
        for _ in 0..local_column_count {
            column_indices.push(read_len(buffer, offset, "foreign key column index")?);
        }
        let table_len = read_len(buffer, offset, "foreign key table length")?;
        let referenced_table = read_string(buffer, offset, table_len, "foreign key table")?;
        let referenced_column_count =
            read_len(buffer, offset, "foreign key referenced column count")?;
        let mut referenced_columns = Vec::with_capacity(referenced_column_count);
        for _ in 0..referenced_column_count {
            let column_len = read_len(buffer, offset, "foreign key column length")?;
            referenced_columns.push(read_string(
                buffer,
                offset,
                column_len,
                "foreign key column",
            )?);
        }
        let on_delete = read_foreign_key_action(buffer, offset, "foreign key ON DELETE action")?;
        let on_update = read_foreign_key_action(buffer, offset, "foreign key ON UPDATE action")?;
        Ok(Self {
            name,
            column_indices,
            referenced_table,
            referenced_columns,
            on_delete,
            on_update,
        })
    }
}

impl Table {
    pub fn new(
        id: TableId,
        name: String,
        mut columns: Vec<Column>,
        root_page_id: u32,
    ) -> Result<Self> {
        let mut column_indices = HashMap::new();
        let mut primary_key_columns = Vec::new();

        for column in &mut columns {
            if column.primary_key {
                column.nullable = false;
            }
        }

        for (index, column) in columns.iter().enumerate() {
            column_indices.insert(column.name.clone(), index);
            if column.primary_key {
                primary_key_columns.push(index);
            }
        }

        // Validate that at least one column exists
        if columns.is_empty() {
            return Err(HematiteError::StorageError(
                "Table must have at least one column".to_string(),
            ));
        }

        // Validate primary key
        if primary_key_columns.is_empty() {
            return Err(HematiteError::StorageError(
                "Table must have at least one primary key column".to_string(),
            ));
        }

        Ok(Self {
            id,
            name,
            columns,
            column_indices,
            primary_key_columns,
            secondary_indexes: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            root_page_id,
            primary_key_index_root_page_id: 0,
        })
    }

    pub fn get_column_by_name(&self, name: &str) -> Option<&Column> {
        self.column_indices
            .get(name)
            .map(|&index| &self.columns[index])
    }

    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.column_indices.get(name).copied()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn primary_key_count(&self) -> usize {
        self.primary_key_columns.len()
    }

    pub fn validate_row(&self, values: &[Value]) -> Result<()> {
        if values.len() != self.columns.len() {
            return Err(HematiteError::StorageError(format!(
                "Expected {} values, got {}",
                self.columns.len(),
                values.len()
            )));
        }

        for (column, value) in self.columns.iter().zip(values.iter()) {
            if !column.validate_value(value) {
                return Err(HematiteError::StorageError(format!(
                    "Invalid value for column '{}': {:?}",
                    column.name, value
                )));
            }
        }

        Ok(())
    }

    pub fn get_primary_key_values(&self, values: &[Value]) -> Result<Vec<Value>> {
        self.primary_key_columns
            .iter()
            .map(|&index| {
                if index < values.len() {
                    Ok(values[index].clone())
                } else {
                    Err(HematiteError::StorageError(
                        "Primary key value not found".to_string(),
                    ))
                }
            })
            .collect()
    }

    pub fn row_size(&self) -> usize {
        self.columns.iter().map(|col| col.size()).sum()
    }

    pub fn get_secondary_index(&self, name: &str) -> Option<&SecondaryIndex> {
        self.secondary_indexes
            .iter()
            .find(|index| index.name == name)
    }

    pub fn add_secondary_index(&mut self, index: SecondaryIndex) -> Result<()> {
        if self.get_secondary_index(&index.name).is_some() {
            return Err(HematiteError::StorageError(format!(
                "Secondary index '{}' already exists on table '{}'",
                index.name, self.name
            )));
        }

        if index.column_indices.is_empty() {
            return Err(HematiteError::StorageError(
                "Secondary index must reference at least one column".to_string(),
            ));
        }

        for &column_index in &index.column_indices {
            if column_index >= self.columns.len() {
                return Err(HematiteError::StorageError(format!(
                    "Secondary index '{}' references invalid column index {}",
                    index.name, column_index
                )));
            }
        }

        self.secondary_indexes.push(index);
        Ok(())
    }

    pub fn list_named_constraints(&self) -> Vec<NamedConstraint> {
        let mut constraints = Vec::new();
        constraints.extend(self.check_constraints.iter().filter_map(|constraint| {
            constraint.name.as_ref().map(|name| NamedConstraint {
                table_name: self.name.clone(),
                name: name.clone(),
                kind: NamedConstraintKind::Check,
            })
        }));
        constraints.extend(self.foreign_keys.iter().filter_map(|constraint| {
            constraint.name.as_ref().map(|name| NamedConstraint {
                table_name: self.name.clone(),
                name: name.clone(),
                kind: NamedConstraintKind::ForeignKey,
            })
        }));
        constraints.extend(
            self.secondary_indexes
                .iter()
                .filter(|index| index.unique)
                .map(|index| NamedConstraint {
                    table_name: self.name.clone(),
                    name: index.name.clone(),
                    kind: NamedConstraintKind::Unique,
                }),
        );
        constraints
    }

    pub fn named_constraint(&self, name: &str) -> Option<NamedConstraint> {
        self.list_named_constraints()
            .into_iter()
            .find(|constraint| constraint.name == name)
    }

    pub fn add_column(&mut self, column: Column) -> Result<()> {
        if self.column_indices.contains_key(&column.name) {
            return Err(HematiteError::StorageError(format!(
                "Column '{}' already exists in table '{}'",
                column.name, self.name
            )));
        }
        if column.primary_key {
            return Err(HematiteError::StorageError(
                "Cannot add a primary-key column to an existing table".to_string(),
            ));
        }

        let index = self.columns.len();
        self.column_indices.insert(column.name.clone(), index);
        self.columns.push(column);
        Ok(())
    }

    pub fn rename_column(&mut self, old_name: &str, new_name: String) -> Result<()> {
        if self.column_indices.contains_key(&new_name) {
            return Err(HematiteError::StorageError(format!(
                "Column '{}' already exists in table '{}'",
                new_name, self.name
            )));
        }

        let index = self.get_column_index(old_name).ok_or_else(|| {
            HematiteError::StorageError(format!(
                "Column '{}' does not exist in table '{}'",
                old_name, self.name
            ))
        })?;

        self.columns[index].name = new_name.clone();
        self.column_indices.remove(old_name);
        self.column_indices.insert(new_name.clone(), index);
        self.rewrite_check_constraints(old_name, &new_name)?;
        Ok(())
    }

    pub fn drop_column(&mut self, name: &str) -> Result<usize> {
        let index = self.get_column_index(name).ok_or_else(|| {
            HematiteError::StorageError(format!(
                "Column '{}' does not exist in table '{}'",
                name, self.name
            ))
        })?;

        if self.columns.len() == 1 {
            return Err(HematiteError::StorageError(
                "Cannot drop the last column from a table".to_string(),
            ));
        }
        if self.primary_key_columns.contains(&index) {
            return Err(HematiteError::StorageError(format!(
                "Cannot drop primary-key column '{}'",
                name
            )));
        }
        if self
            .secondary_indexes
            .iter()
            .any(|secondary_index| secondary_index.column_indices.contains(&index))
        {
            return Err(HematiteError::StorageError(format!(
                "Cannot drop column '{}' because it is used by an index",
                name
            )));
        }
        if self
            .foreign_keys
            .iter()
            .any(|foreign_key| foreign_key.column_indices.contains(&index))
        {
            return Err(HematiteError::StorageError(format!(
                "Cannot drop column '{}' because it is used by a foreign key",
                name
            )));
        }

        self.columns.remove(index);
        self.rebuild_column_indices();
        self.primary_key_columns = self
            .primary_key_columns
            .iter()
            .filter_map(|&primary_key_index| {
                if primary_key_index == index {
                    None
                } else if primary_key_index > index {
                    Some(primary_key_index - 1)
                } else {
                    Some(primary_key_index)
                }
            })
            .collect();
        for foreign_key in &mut self.foreign_keys {
            for column_index in &mut foreign_key.column_indices {
                if *column_index > index {
                    *column_index -= 1;
                }
            }
        }

        Ok(index)
    }

    pub fn set_column_default(&mut self, name: &str, default_value: Option<Value>) -> Result<()> {
        let index = self.get_column_index(name).ok_or_else(|| {
            HematiteError::StorageError(format!(
                "Column '{}' does not exist in table '{}'",
                name, self.name
            ))
        })?;
        self.columns[index].default_value = default_value;
        Ok(())
    }

    pub fn set_column_nullable(&mut self, name: &str, nullable: bool) -> Result<()> {
        let index = self.get_column_index(name).ok_or_else(|| {
            HematiteError::StorageError(format!(
                "Column '{}' does not exist in table '{}'",
                name, self.name
            ))
        })?;
        if self.columns[index].primary_key && nullable {
            return Err(HematiteError::StorageError(format!(
                "Primary-key column '{}' cannot be nullable",
                name
            )));
        }
        if self.columns[index].auto_increment && nullable {
            return Err(HematiteError::StorageError(format!(
                "AUTO_INCREMENT column '{}' cannot be nullable",
                name
            )));
        }
        self.columns[index].nullable = nullable;
        Ok(())
    }

    pub fn rewrite_inbound_referenced_column(
        &mut self,
        referenced_table: &str,
        old_name: &str,
        new_name: &str,
    ) {
        for foreign_key in &mut self.foreign_keys {
            if foreign_key.referenced_table == referenced_table {
                for referenced_column in &mut foreign_key.referenced_columns {
                    if referenced_column == old_name {
                        *referenced_column = new_name.to_string();
                    }
                }
            }
        }
    }

    fn rewrite_check_constraints(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        for constraint in &mut self.check_constraints {
            let mut condition =
                crate::parser::parser::parse_condition_fragment(&constraint.expression_sql)?;
            condition.rename_column_references(old_name, new_name, Some(&self.name));
            constraint.expression_sql = condition.to_sql();
        }
        Ok(())
    }

    fn rebuild_column_indices(&mut self) {
        self.column_indices = self
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| (column.name.clone(), index))
            .collect();
    }

    pub fn add_check_constraint(&mut self, constraint: CheckConstraint) -> Result<()> {
        if let Some(name) = &constraint.name {
            if self.named_constraint(name).is_some() {
                return Err(HematiteError::StorageError(format!(
                    "Constraint '{}' already exists on table '{}'",
                    name, self.name
                )));
            }
        }
        self.check_constraints.push(constraint);
        Ok(())
    }

    pub fn add_foreign_key(&mut self, constraint: ForeignKeyConstraint) -> Result<()> {
        constraint.validate_for_table(&self.name, self.columns.len())?;
        if let Some(name) = &constraint.name {
            if self.named_constraint(name).is_some() {
                return Err(HematiteError::StorageError(format!(
                    "Constraint '{}' already exists on table '{}'",
                    name, self.name
                )));
            }
        }
        self.foreign_keys.push(constraint);
        Ok(())
    }

    pub fn drop_secondary_index(&mut self, name: &str) -> Result<SecondaryIndex> {
        let index = self
            .secondary_indexes
            .iter()
            .position(|index| index.name == name)
            .ok_or_else(|| {
                HematiteError::StorageError(format!(
                    "Secondary index '{}' does not exist on table '{}'",
                    name, self.name
                ))
            })?;

        Ok(self.secondary_indexes.remove(index))
    }

    pub fn drop_named_constraint(&mut self, name: &str) -> Result<NamedConstraintKind> {
        if let Some(index) = self
            .check_constraints
            .iter()
            .position(|constraint| constraint.name.as_deref() == Some(name))
        {
            self.check_constraints.remove(index);
            return Ok(NamedConstraintKind::Check);
        }

        if let Some(index) = self
            .foreign_keys
            .iter()
            .position(|constraint| constraint.name.as_deref() == Some(name))
        {
            self.foreign_keys.remove(index);
            return Ok(NamedConstraintKind::ForeignKey);
        }

        if let Some(index) = self
            .secondary_indexes
            .iter()
            .position(|constraint| constraint.unique && constraint.name == name)
        {
            self.secondary_indexes.remove(index);
            return Ok(NamedConstraintKind::Unique);
        }

        Err(HematiteError::StorageError(format!(
            "Constraint '{}' does not exist on table '{}'",
            name, self.name
        )))
    }

    pub fn serialize(&self, buffer: &mut Vec<u8>) -> Result<()> {
        // Table ID (4 bytes)
        buffer.extend_from_slice(&self.id.as_u32().to_le_bytes());

        // Name length (4 bytes) + name
        let name_bytes = self.name.as_bytes();
        buffer.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        buffer.extend_from_slice(name_bytes);

        // Root page ID (4 bytes)
        buffer.extend_from_slice(&self.root_page_id.to_le_bytes());
        buffer.extend_from_slice(&self.primary_key_index_root_page_id.to_le_bytes());

        // Column count (4 bytes)
        buffer.extend_from_slice(&(self.columns.len() as u32).to_le_bytes());

        // Columns
        for column in &self.columns {
            column.serialize(buffer)?;
        }

        // Primary key column count (4 bytes)
        buffer.extend_from_slice(&(self.primary_key_columns.len() as u32).to_le_bytes());

        // Primary key column indices
        for &index in &self.primary_key_columns {
            buffer.extend_from_slice(&(index as u32).to_le_bytes());
        }

        // Secondary indexes
        buffer.extend_from_slice(&(self.secondary_indexes.len() as u32).to_le_bytes());
        for index in &self.secondary_indexes {
            let name_bytes = index.name.as_bytes();
            buffer.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buffer.extend_from_slice(name_bytes);
            buffer.extend_from_slice(&index.root_page_id.to_le_bytes());
            buffer.push(index.unique as u8);
            buffer.extend_from_slice(&(index.column_indices.len() as u32).to_le_bytes());
            for &column_index in &index.column_indices {
                buffer.extend_from_slice(&(column_index as u32).to_le_bytes());
            }
        }

        buffer.extend_from_slice(&(self.check_constraints.len() as u32).to_le_bytes());
        for constraint in &self.check_constraints {
            match &constraint.name {
                Some(name) => {
                    buffer.push(1);
                    let bytes = name.as_bytes();
                    buffer.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    buffer.extend_from_slice(bytes);
                }
                None => buffer.push(0),
            }
            let expression_bytes = constraint.expression_sql.as_bytes();
            buffer.extend_from_slice(&(expression_bytes.len() as u32).to_le_bytes());
            buffer.extend_from_slice(expression_bytes);
        }

        buffer.extend_from_slice(&(self.foreign_keys.len() as u32).to_le_bytes());
        for constraint in &self.foreign_keys {
            constraint.serialize(buffer);
        }

        Ok(())
    }

    pub fn deserialize(buffer: &[u8], offset: &mut usize) -> Result<Self> {
        if *offset + 16 > buffer.len() {
            return Err(HematiteError::CorruptedData(
                "Invalid table header".to_string(),
            ));
        }

        // Table ID
        let id = TableId::new(u32::from_le_bytes([
            buffer[*offset],
            buffer[*offset + 1],
            buffer[*offset + 2],
            buffer[*offset + 3],
        ]));
        *offset += 4;

        // Name
        let name_len = u32::from_le_bytes([
            buffer[*offset],
            buffer[*offset + 1],
            buffer[*offset + 2],
            buffer[*offset + 3],
        ]) as usize;
        *offset += 4;

        if *offset + name_len > buffer.len() {
            return Err(HematiteError::CorruptedData(
                "Invalid table name".to_string(),
            ));
        }
        let name = String::from_utf8(buffer[*offset..*offset + name_len].to_vec())
            .map_err(|_| HematiteError::CorruptedData("Invalid UTF-8 in table name".to_string()))?;
        *offset += name_len;

        // Root page ID
        let root_page_id = u32::from_le_bytes([
            buffer[*offset],
            buffer[*offset + 1],
            buffer[*offset + 2],
            buffer[*offset + 3],
        ]);
        *offset += 4;

        let primary_key_index_root_page_id = u32::from_le_bytes([
            buffer[*offset],
            buffer[*offset + 1],
            buffer[*offset + 2],
            buffer[*offset + 3],
        ]);
        *offset += 4;

        // Column count
        let column_count = u32::from_le_bytes([
            buffer[*offset],
            buffer[*offset + 1],
            buffer[*offset + 2],
            buffer[*offset + 3],
        ]) as usize;
        *offset += 4;

        // Columns
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            columns.push(Column::deserialize(buffer, offset)?);
        }

        // Primary key column count
        if *offset + 4 > buffer.len() {
            return Err(HematiteError::CorruptedData(
                "Invalid primary key count".to_string(),
            ));
        }
        let pk_count = u32::from_le_bytes([
            buffer[*offset],
            buffer[*offset + 1],
            buffer[*offset + 2],
            buffer[*offset + 3],
        ]) as usize;
        *offset += 4;

        // Primary key column indices
        let mut primary_key_columns = Vec::with_capacity(pk_count);
        for _ in 0..pk_count {
            if *offset + 4 > buffer.len() {
                return Err(HematiteError::CorruptedData(
                    "Invalid primary key index".to_string(),
                ));
            }
            let index = u32::from_le_bytes([
                buffer[*offset],
                buffer[*offset + 1],
                buffer[*offset + 2],
                buffer[*offset + 3],
            ]) as usize;
            *offset += 4;
            primary_key_columns.push(index);
        }

        let mut table = Self::new(id, name, columns, root_page_id)?;
        table.primary_key_index_root_page_id = primary_key_index_root_page_id;

        if *offset == buffer.len() {
            return Ok(table);
        }

        if *offset + 4 > buffer.len() {
            return Err(HematiteError::CorruptedData(
                "Invalid secondary index count".to_string(),
            ));
        }
        let secondary_index_count = u32::from_le_bytes([
            buffer[*offset],
            buffer[*offset + 1],
            buffer[*offset + 2],
            buffer[*offset + 3],
        ]) as usize;
        *offset += 4;

        for _ in 0..secondary_index_count {
            if *offset + 4 > buffer.len() {
                return Err(HematiteError::CorruptedData(
                    "Invalid secondary index name length".to_string(),
                ));
            }
            let name_len = u32::from_le_bytes([
                buffer[*offset],
                buffer[*offset + 1],
                buffer[*offset + 2],
                buffer[*offset + 3],
            ]) as usize;
            *offset += 4;

            if *offset + name_len > buffer.len() {
                return Err(HematiteError::CorruptedData(
                    "Invalid secondary index name".to_string(),
                ));
            }
            let name =
                String::from_utf8(buffer[*offset..*offset + name_len].to_vec()).map_err(|_| {
                    HematiteError::CorruptedData(
                        "Invalid UTF-8 in secondary index name".to_string(),
                    )
                })?;
            *offset += name_len;

            if *offset + 9 > buffer.len() {
                return Err(HematiteError::CorruptedData(
                    "Invalid secondary index metadata".to_string(),
                ));
            }
            let index_root_page_id = u32::from_le_bytes([
                buffer[*offset],
                buffer[*offset + 1],
                buffer[*offset + 2],
                buffer[*offset + 3],
            ]);
            *offset += 4;

            let unique = match buffer[*offset] {
                0 => false,
                1 => true,
                _ => {
                    return Err(HematiteError::CorruptedData(
                        "Invalid secondary index uniqueness flag".to_string(),
                    ))
                }
            };
            *offset += 1;

            let column_count = u32::from_le_bytes([
                buffer[*offset],
                buffer[*offset + 1],
                buffer[*offset + 2],
                buffer[*offset + 3],
            ]) as usize;
            *offset += 4;

            let mut column_indices = Vec::with_capacity(column_count);
            for _ in 0..column_count {
                if *offset + 4 > buffer.len() {
                    return Err(HematiteError::CorruptedData(
                        "Invalid secondary index column index".to_string(),
                    ));
                }
                let column_index = u32::from_le_bytes([
                    buffer[*offset],
                    buffer[*offset + 1],
                    buffer[*offset + 2],
                    buffer[*offset + 3],
                ]) as usize;
                *offset += 4;
                column_indices.push(column_index);
            }

            table.add_secondary_index(SecondaryIndex {
                name,
                column_indices,
                root_page_id: index_root_page_id,
                unique,
            })?;
        }

        if *offset == buffer.len() {
            return Ok(table);
        }

        if *offset + 4 > buffer.len() {
            return Err(HematiteError::CorruptedData(
                "Invalid check constraint count".to_string(),
            ));
        }
        let check_count = u32::from_le_bytes([
            buffer[*offset],
            buffer[*offset + 1],
            buffer[*offset + 2],
            buffer[*offset + 3],
        ]) as usize;
        *offset += 4;

        for _ in 0..check_count {
            if *offset >= buffer.len() {
                return Err(HematiteError::CorruptedData(
                    "Invalid check constraint metadata".to_string(),
                ));
            }
            let name = if buffer[*offset] == 1 {
                *offset += 1;
                let len = read_len(buffer, offset, "check constraint name length")?;
                let value = read_string(buffer, offset, len, "check constraint name")?;
                Some(value)
            } else {
                *offset += 1;
                None
            };
            let len = read_len(buffer, offset, "check constraint expression length")?;
            let expression_sql = read_string(buffer, offset, len, "check constraint expression")?;
            table.add_check_constraint(CheckConstraint {
                name,
                expression_sql,
            })?;
        }

        if *offset == buffer.len() {
            return Ok(table);
        }

        let foreign_key_count = read_len(buffer, offset, "foreign key count")?;
        for _ in 0..foreign_key_count {
            table.add_foreign_key(ForeignKeyConstraint::deserialize(buffer, offset)?)?;
        }

        Ok(table)
    }

    /// Convert table to bytes for storage in schema B-tree
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        self.serialize(&mut buffer)?;
        Ok(buffer)
    }

    /// Create table from bytes stored in schema B-tree
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut offset = 0;
        Self::deserialize(bytes, &mut offset)
    }
}

fn read_len(buffer: &[u8], offset: &mut usize, field: &str) -> Result<usize> {
    if *offset + 4 > buffer.len() {
        return Err(HematiteError::CorruptedData(format!("Invalid {}", field)));
    }
    let value = u32::from_le_bytes([
        buffer[*offset],
        buffer[*offset + 1],
        buffer[*offset + 2],
        buffer[*offset + 3],
    ]) as usize;
    *offset += 4;
    Ok(value)
}

fn write_string(buffer: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    buffer.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buffer.extend_from_slice(bytes);
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

fn read_string(buffer: &[u8], offset: &mut usize, len: usize, field: &str) -> Result<String> {
    if *offset + len > buffer.len() {
        return Err(HematiteError::CorruptedData(format!("Invalid {}", field)));
    }
    let value = String::from_utf8(buffer[*offset..*offset + len].to_vec())
        .map_err(|_| HematiteError::CorruptedData(format!("Invalid UTF-8 in {}", field)))?;
    *offset += len;
    Ok(value)
}

fn read_optional_string(buffer: &[u8], offset: &mut usize, field: &str) -> Result<Option<String>> {
    if *offset >= buffer.len() {
        return Err(HematiteError::CorruptedData(format!("Invalid {}", field)));
    }
    let present = buffer[*offset];
    *offset += 1;
    match present {
        0 => Ok(None),
        1 => {
            let len = read_len(buffer, offset, &format!("{} length", field))?;
            read_string(buffer, offset, len, field).map(Some)
        }
        _ => Err(HematiteError::CorruptedData(format!(
            "Invalid {} marker",
            field
        ))),
    }
}

fn foreign_key_action_to_byte(action: ForeignKeyAction) -> u8 {
    match action {
        ForeignKeyAction::Restrict => 0,
        ForeignKeyAction::Cascade => 1,
        ForeignKeyAction::SetNull => 2,
    }
}

fn read_foreign_key_action(
    buffer: &[u8],
    offset: &mut usize,
    field: &str,
) -> Result<ForeignKeyAction> {
    if *offset >= buffer.len() {
        return Err(HematiteError::CorruptedData(format!("Invalid {}", field)));
    }
    let action = match buffer[*offset] {
        0 => ForeignKeyAction::Restrict,
        1 => ForeignKeyAction::Cascade,
        2 => ForeignKeyAction::SetNull,
        _ => {
            return Err(HematiteError::CorruptedData(format!(
                "Invalid {} value",
                field
            )))
        }
    };
    *offset += 1;
    Ok(action)
}
