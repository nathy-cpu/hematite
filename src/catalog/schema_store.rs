//! Durable schema persistence over the generic B-tree layer.

use crate::btree::KeyValueCodec;
use crate::error::{HematiteError, Result};

use super::engine::CatalogEngine;
use super::{Schema, Table};

#[derive(Debug, Clone, Copy, Default)]
struct CatalogSchemaCodec;

impl KeyValueCodec for CatalogSchemaCodec {
    type Key = String;
    type Value = Table;

    fn encode_key(key: &Self::Key) -> Result<Vec<u8>> {
        Ok(key.as_bytes().to_vec())
    }

    fn decode_key(bytes: &[u8]) -> Result<Self::Key> {
        String::from_utf8(bytes.to_vec())
            .map_err(|e| HematiteError::StorageError(format!("Invalid table name: {}", e)))
    }

    fn encode_value(value: &Self::Value) -> Result<Vec<u8>> {
        value.to_bytes()
    }

    fn decode_value(bytes: &[u8]) -> Result<Self::Value> {
        Table::from_bytes(bytes)
    }
}

pub(crate) fn load_schema(engine: &CatalogEngine, schema_root: u32) -> Result<Schema> {
    let mut schema = Schema::new();
    engine.visit_tree_entries(schema_root, |key, value| {
        let table_name = CatalogSchemaCodec::decode_key(key)?;
        let mut table = CatalogSchemaCodec::decode_value(value)?;
        table.name = table_name;
        schema.insert_table(table)?;
        Ok(())
    })?;
    Ok(schema)
}

pub(crate) fn save_schema(
    engine: &mut CatalogEngine,
    schema: &Schema,
    current_root: u32,
) -> Result<u32> {
    let table_entries = schema
        .list_tables()
        .into_iter()
        .filter_map(|(table_id, _)| schema.get_table(table_id).cloned())
        .collect::<Vec<_>>();

    engine.delete_tree(current_root)?;
    let mut schema_root = engine.create_tree()?;

    for table in table_entries {
        let key = CatalogSchemaCodec::encode_key(&table.name)?;
        let value = CatalogSchemaCodec::encode_value(&table)?;
        schema_root = engine.insert_tree_entry(schema_root, &key, &value)?;
    }

    Ok(schema_root)
}
