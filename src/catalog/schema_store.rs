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
    let tree_store = engine.typed_tree_store::<CatalogSchemaCodec>();
    let tree = tree_store.open_tree(schema_root)?;
    for (table_name, mut table) in tree.entries()? {
        table.name = table_name;
        schema.insert_table(table)?;
    }
    Ok(schema)
}

pub(crate) fn save_schema(
    engine: &mut CatalogEngine,
    schema: &Schema,
    current_root: u32,
) -> Result<u32> {
    let tree_store = engine.typed_tree_store::<CatalogSchemaCodec>();
    let table_entries = schema
        .list_tables()
        .into_iter()
        .filter_map(|(table_id, _)| schema.get_table(table_id).cloned())
        .collect::<Vec<_>>();

    tree_store.delete_tree(current_root)?;
    let schema_root = tree_store.create_tree()?;
    let mut tree = tree_store.open_tree(schema_root)?;

    for table in table_entries {
        tree.insert(&table.name, &table)?;
    }

    Ok(schema_root)
}
