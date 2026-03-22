use crate::catalog::{Table, Value};
use crate::error::Result;
use crate::storage::StoredRow;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct TransientIndexStore {
    primary_key_indexes: HashMap<String, HashMap<Vec<u8>, StoredRow>>,
    secondary_indexes: HashMap<String, HashMap<String, HashMap<Vec<u8>, Vec<StoredRow>>>>,
}

impl TransientIndexStore {
    pub fn remove_table(&mut self, table_name: &str) {
        self.primary_key_indexes.remove(table_name);
        self.secondary_indexes.remove(table_name);
    }

    pub fn has_primary_key_index(&self, table_name: &str) -> bool {
        self.primary_key_indexes.contains_key(table_name)
    }

    pub fn has_secondary_indexes(&self, table_name: &str) -> bool {
        self.secondary_indexes.contains_key(table_name)
    }

    pub fn lookup_primary_key(&self, table_name: &str, key: &[u8]) -> Option<StoredRow> {
        self.primary_key_indexes
            .get(table_name)
            .and_then(|index| index.get(key).cloned())
    }

    pub fn register_primary_key_row(
        &mut self,
        table: &Table,
        row: StoredRow,
        encode_primary_key: impl Fn(&[Value]) -> Result<Vec<u8>>,
    ) -> Result<()> {
        let key = encode_primary_key(&table.get_primary_key_values(&row.values)?)?;
        self.primary_key_indexes
            .entry(table.name.clone())
            .or_default()
            .insert(key, row);
        Ok(())
    }

    pub fn rebuild_primary_key_index(
        &mut self,
        table: &Table,
        rows: &[StoredRow],
        encode_primary_key: impl Fn(&[Value]) -> Result<Vec<u8>>,
    ) -> Result<()> {
        let mut index = HashMap::new();
        for row in rows {
            let key = encode_primary_key(&table.get_primary_key_values(&row.values)?)?;
            index.insert(key, row.clone());
        }
        self.primary_key_indexes.insert(table.name.clone(), index);
        Ok(())
    }

    pub fn lookup_secondary_index(
        &self,
        table_name: &str,
        index_name: &str,
        key: &[u8],
    ) -> Vec<StoredRow> {
        self.secondary_indexes
            .get(table_name)
            .and_then(|table_indexes| table_indexes.get(index_name))
            .and_then(|index| index.get(key))
            .cloned()
            .unwrap_or_default()
    }

    pub fn register_secondary_index_row(
        &mut self,
        table: &Table,
        row: StoredRow,
        encode_index_key: impl Fn(&[Value]) -> Result<Vec<u8>>,
    ) -> Result<()> {
        if table.secondary_indexes.is_empty() {
            return Ok(());
        }

        let table_indexes = self
            .secondary_indexes
            .entry(table.name.clone())
            .or_default();
        for index in &table.secondary_indexes {
            let key_values = index
                .column_indices
                .iter()
                .map(|&column_index| row.values[column_index].clone())
                .collect::<Vec<_>>();
            let key = encode_index_key(&key_values)?;
            table_indexes
                .entry(index.name.clone())
                .or_default()
                .entry(key)
                .or_default()
                .push(row.clone());
        }

        Ok(())
    }

    pub fn rebuild_secondary_indexes(
        &mut self,
        table: &Table,
        rows: &[StoredRow],
        encode_index_key: impl Fn(&[Value]) -> Result<Vec<u8>>,
    ) -> Result<()> {
        let mut table_indexes: HashMap<String, HashMap<Vec<u8>, Vec<StoredRow>>> = HashMap::new();

        for index in &table.secondary_indexes {
            let mut entries: HashMap<Vec<u8>, Vec<StoredRow>> = HashMap::new();
            for row in rows {
                let key_values = index
                    .column_indices
                    .iter()
                    .map(|&column_index| row.values[column_index].clone())
                    .collect::<Vec<_>>();
                let key = encode_index_key(&key_values)?;
                entries.entry(key).or_default().push(row.clone());
            }
            table_indexes.insert(index.name.clone(), entries);
        }

        self.secondary_indexes
            .insert(table.name.clone(), table_indexes);
        Ok(())
    }
}
