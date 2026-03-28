//! Durable schema persistence over the generic B-tree layer.

use crate::btree::KeyValueCodec;
use crate::error::{HematiteError, Result};

use super::engine::CatalogEngine;
use super::{Schema, Table};

const SCHEMA_BLOB_KEY: &str = "__schema__";

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

#[derive(Debug, Clone, Copy, Default)]
struct CatalogSchemaBlobCodec;

impl KeyValueCodec for CatalogSchemaBlobCodec {
    type Key = String;
    type Value = Vec<u8>;

    fn encode_key(key: &Self::Key) -> Result<Vec<u8>> {
        Ok(key.as_bytes().to_vec())
    }

    fn decode_key(bytes: &[u8]) -> Result<Self::Key> {
        String::from_utf8(bytes.to_vec())
            .map_err(|e| HematiteError::StorageError(format!("Invalid schema key: {}", e)))
    }

    fn encode_value(value: &Self::Value) -> Result<Vec<u8>> {
        Ok(value.clone())
    }

    fn decode_value(bytes: &[u8]) -> Result<Self::Value> {
        Ok(bytes.to_vec())
    }
}

pub(crate) fn load_schema(engine: &CatalogEngine, schema_root: u32) -> Result<Schema> {
    let blob_store = engine.typed_tree_store::<CatalogSchemaBlobCodec>();
    let mut blob_tree = blob_store.open_tree(schema_root)?;
    if let Some(blob) = blob_tree.get(&SCHEMA_BLOB_KEY.to_string())? {
        return Schema::deserialize(&blob);
    }

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
    let tree_store = engine.typed_tree_store::<CatalogSchemaBlobCodec>();
    let mut buffer = Vec::new();
    schema.serialize(&mut buffer)?;

    tree_store.delete_tree(current_root)?;
    let schema_root = tree_store.create_tree()?;
    let mut tree = tree_store.open_tree(schema_root)?;
    tree.insert(&SCHEMA_BLOB_KEY.to_string(), &buffer)?;

    Ok(schema_root)
}
