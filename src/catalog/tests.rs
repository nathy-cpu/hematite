#[cfg(test)]
mod tests {
    use crate::catalog::types::{DataType, Value};
    use crate::catalog::{Column, ColumnId, DatabaseHeader, Schema, Table, TableId};
    use crate::error::Result;
    use crate::storage::Page;

    #[test]
    fn test_database_header_creation() {
        let schema_root = 42u32;
        let header = DatabaseHeader::new(schema_root);

        assert_eq!(header.magic, DatabaseHeader::MAGIC);
        assert_eq!(header.version, DatabaseHeader::CURRENT_VERSION);
        assert_eq!(header.schema_root_page, schema_root.into());
        assert_eq!(header.next_table_id, 1);
        assert!(header.verify_checksum());
    }

    #[test]
    fn test_database_header_serialization_roundtrip() -> Result<()> {
        let original = DatabaseHeader::new(123u32);

        let mut page = Page::new(crate::storage::DB_HEADER_PAGE_ID);
        original.serialize(&mut page.data)?;

        let deserialized = DatabaseHeader::deserialize(&page.data)?;

        assert_eq!(original.magic, deserialized.magic);
        assert_eq!(original.version, deserialized.version);
        assert_eq!(original.schema_root_page, deserialized.schema_root_page);
        assert_eq!(original.next_table_id, deserialized.next_table_id);
        assert_eq!(original.checksum, deserialized.checksum);

        Ok(())
    }

    #[test]
    fn test_table_to_bytes_roundtrip() -> Result<()> {
        let columns = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(2), "name".to_string(), DataType::Text),
            Column::new(ColumnId::new(3), "active".to_string(), DataType::Boolean),
        ];

        let original = Table::new(TableId::new(42), "test_table".to_string(), columns, 123)?;

        // Convert to bytes and back
        let bytes = original.to_bytes()?;
        let deserialized = Table::from_bytes(&bytes)?;

        // Verify all fields match
        assert_eq!(original.id, deserialized.id);
        assert_eq!(original.name, deserialized.name);
        assert_eq!(original.root_page_id, deserialized.root_page_id);
        assert_eq!(original.columns.len(), deserialized.columns.len());

        // Verify column details
        for (orig_col, deser_col) in original.columns.iter().zip(deserialized.columns.iter()) {
            assert_eq!(orig_col.id, deser_col.id);
            assert_eq!(orig_col.name, deser_col.name);
            assert_eq!(orig_col.data_type, deser_col.data_type);
            assert_eq!(orig_col.nullable, deser_col.nullable);
            assert_eq!(orig_col.primary_key, deser_col.primary_key);
            assert_eq!(orig_col.auto_increment, deser_col.auto_increment);
        }

        Ok(())
    }

    #[test]
    fn test_table_from_bytes_empty() {
        let bytes = vec![]; // Empty buffer
        let result = Table::from_bytes(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_table_from_bytes_corrupt() {
        let bytes = vec![1, 2, 3]; // Too short for table header
        let result = Table::from_bytes(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_table_id() {
        let table_id = TableId::new(42);
        assert_eq!(table_id.as_u32(), 42);
    }

    #[test]
    fn test_column_id() {
        let column_id = ColumnId::new(123);
        assert_eq!(column_id.as_u32(), 123);
    }

    #[test]
    fn test_schema_creation() {
        let schema = Schema::new();
        assert_eq!(schema.get_table_count(), 0);
    }

    #[test]
    fn test_create_table() -> Result<()> {
        let mut schema = Schema::new();

        let columns = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(2), "name".to_string(), DataType::Text),
        ];

        let table_id = schema.create_table("users".to_string(), columns)?;
        assert_eq!(schema.get_table_count(), 1);
        assert!(schema.get_table(table_id).is_some());
        assert!(schema.get_table_by_name("users").is_some());

        Ok(())
    }

    #[test]
    fn test_duplicate_table_name() -> Result<()> {
        let mut schema = Schema::new();

        let columns = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
        ];

        schema
            .create_table("users".to_string(), columns.clone())
            .unwrap();

        let result = schema.create_table("users".to_string(), columns);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_column_creation() {
        let column = Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer);

        assert_eq!(column.id.as_u32(), 1);
        assert_eq!(column.name, "id");
        assert_eq!(column.data_type, DataType::Integer);
        assert!(column.nullable);
        assert!(!column.primary_key);
        assert!(!column.auto_increment);
        assert!(column.default_value.is_none());
    }

    #[test]
    fn test_column_auto_increment_roundtrip() -> Result<()> {
        let original = Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer)
            .primary_key(true)
            .auto_increment(true);

        let mut buffer = Vec::new();
        original.serialize(&mut buffer)?;
        let mut offset = 0;
        let deserialized = Column::deserialize(&buffer, &mut offset)?;

        assert!(deserialized.primary_key);
        assert!(deserialized.auto_increment);
        assert!(!deserialized.nullable);
        Ok(())
    }

    #[test]
    fn test_column_builder() {
        let column = Column::new(ColumnId::new(1), "name".to_string(), DataType::Text)
            .nullable(false)
            .primary_key(true)
            .default_value(Value::Text("default".to_string()));

        assert!(!column.nullable);
        assert!(column.primary_key);
        assert_eq!(
            column.default_value,
            Some(Value::Text("default".to_string()))
        );
    }

    #[test]
    fn test_column_validation() {
        // Test valid values
        let int_column = Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer);
        assert!(int_column.validate_value(&Value::Integer(42)));
        assert!(int_column.validate_value(&Value::Null)); // NULL is allowed by default

        let non_null_int_column =
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).nullable(false);
        assert!(non_null_int_column.validate_value(&Value::Integer(42)));
        assert!(!non_null_int_column.validate_value(&Value::Null)); // NULL not allowed

        // Test type compatibility
        assert!(!int_column.validate_value(&Value::Text("not an integer".to_string())));
        assert!(!int_column.validate_value(&Value::Boolean(true)));
        assert!(!int_column.validate_value(&Value::Float(3.14)));

        let text_column = Column::new(ColumnId::new(2), "name".to_string(), DataType::Text);
        assert!(text_column.validate_value(&Value::Text("hello".to_string())));
        assert!(!text_column.validate_value(&Value::Integer(42)));
    }

    #[test]
    fn test_data_type_size() {
        assert_eq!(DataType::Integer.size(), 4);
        assert_eq!(DataType::Text.size(), 255);
        assert_eq!(DataType::Boolean.size(), 1);
        assert_eq!(DataType::Float.size(), 8);
    }

    #[test]
    fn test_data_type_name() {
        assert_eq!(DataType::Integer.name(), "INTEGER");
        assert_eq!(DataType::Text.name(), "TEXT");
        assert_eq!(DataType::Boolean.name(), "BOOLEAN");
        assert_eq!(DataType::Float.name(), "FLOAT");
    }

    #[test]
    fn test_value_data_type() {
        assert_eq!(Value::Integer(42).data_type(), DataType::Integer);
        assert_eq!(Value::Text("hello".to_string()).data_type(), DataType::Text);
        assert_eq!(Value::Boolean(true).data_type(), DataType::Boolean);
        assert_eq!(Value::Float(3.14).data_type(), DataType::Float);
        assert_eq!(Value::Null.data_type(), DataType::Text); // NULL maps to Text
    }

    #[test]
    fn test_value_compatibility() {
        // Compatible values
        assert!(Value::Integer(42).is_compatible_with(DataType::Integer));
        assert!(Value::Text("hello".to_string()).is_compatible_with(DataType::Text));
        assert!(Value::Boolean(true).is_compatible_with(DataType::Boolean));
        assert!(Value::Float(3.14).is_compatible_with(DataType::Float));
        assert!(Value::Null.is_compatible_with(DataType::Integer));
        assert!(Value::Null.is_compatible_with(DataType::Text));
        assert!(Value::Null.is_compatible_with(DataType::Boolean));
        assert!(Value::Null.is_compatible_with(DataType::Float));

        // Incompatible values
        assert!(!Value::Integer(42).is_compatible_with(DataType::Text));
        assert!(!Value::Text("hello".to_string()).is_compatible_with(DataType::Integer));
        assert!(!Value::Boolean(true).is_compatible_with(DataType::Float));
        assert!(!Value::Float(3.14).is_compatible_with(DataType::Boolean));
    }

    #[test]
    fn test_value_equality() {
        // Same type equality
        assert_eq!(Value::Integer(42), Value::Integer(42));
        assert_eq!(
            Value::Text("hello".to_string()),
            Value::Text("hello".to_string())
        );
        assert_eq!(Value::Boolean(true), Value::Boolean(true));
        assert_eq!(Value::Float(3.14), Value::Float(3.14));
        assert_eq!(Value::Null, Value::Null);

        // Different types
        assert_ne!(Value::Integer(42), Value::Text("42".to_string()));
        assert_ne!(Value::Boolean(true), Value::Integer(1));
        assert_ne!(Value::Null, Value::Integer(0));

        // Different values
        assert_ne!(Value::Integer(42), Value::Integer(43));
        assert_ne!(
            Value::Text("hello".to_string()),
            Value::Text("world".to_string())
        );
        assert_ne!(Value::Boolean(true), Value::Boolean(false));
        assert_ne!(Value::Float(3.14), Value::Float(2.71));
    }

    #[test]
    fn test_column_default_values() {
        // Column with explicit default
        let column_with_default =
            Column::new(ColumnId::new(1), "status".to_string(), DataType::Text)
                .default_value(Value::Text("active".to_string()));
        assert_eq!(
            column_with_default.get_default_or_null(),
            Value::Text("active".to_string())
        );

        // Nullable column without default
        let nullable_column =
            Column::new(ColumnId::new(2), "description".to_string(), DataType::Text).nullable(true);
        assert_eq!(nullable_column.get_default_or_null(), Value::Null);

        // Non-nullable column without default (should get type default)
        let non_null_int_column =
            Column::new(ColumnId::new(3), "count".to_string(), DataType::Integer).nullable(false);
        assert_eq!(non_null_int_column.get_default_or_null(), Value::Integer(0));

        let non_null_text_column =
            Column::new(ColumnId::new(4), "name".to_string(), DataType::Text).nullable(false);
        assert_eq!(
            non_null_text_column.get_default_or_null(),
            Value::Text(String::new())
        );

        let non_null_bool_column =
            Column::new(ColumnId::new(5), "active".to_string(), DataType::Boolean).nullable(false);
        assert_eq!(
            non_null_bool_column.get_default_or_null(),
            Value::Boolean(false)
        );

        let non_null_float_column =
            Column::new(ColumnId::new(6), "price".to_string(), DataType::Float).nullable(false);
        assert_eq!(
            non_null_float_column.get_default_or_null(),
            Value::Float(0.0)
        );
    }

    #[test]
    fn test_column_size() {
        let int_column = Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer);
        assert_eq!(int_column.size(), 4);

        let text_column = Column::new(ColumnId::new(2), "name".to_string(), DataType::Text);
        assert_eq!(text_column.size(), 255);

        let bool_column = Column::new(ColumnId::new(3), "active".to_string(), DataType::Boolean);
        assert_eq!(bool_column.size(), 1);

        let float_column = Column::new(ColumnId::new(4), "price".to_string(), DataType::Float);
        assert_eq!(float_column.size(), 8);
    }

    #[test]
    fn test_column_serialization_roundtrip() -> Result<()> {
        let original = Column::new(
            ColumnId::new(42),
            "test_column".to_string(),
            DataType::Integer,
        )
        .nullable(false)
        .primary_key(true)
        .default_value(Value::Integer(123));

        let mut buffer = Vec::new();
        original.serialize(&mut buffer)?;

        let mut offset = 0;
        let deserialized = Column::deserialize(&buffer, &mut offset)?;

        assert_eq!(original.id, deserialized.id);
        assert_eq!(original.name, deserialized.name);
        assert_eq!(original.data_type, deserialized.data_type);
        assert_eq!(original.nullable, deserialized.nullable);
        assert_eq!(original.primary_key, deserialized.primary_key);
        assert_eq!(original.default_value, deserialized.default_value);

        Ok(())
    }

    #[test]
    fn test_column_serialization_no_default() -> Result<()> {
        let original = Column::new(ColumnId::new(1), "simple".to_string(), DataType::Boolean);

        let mut buffer = Vec::new();
        original.serialize(&mut buffer)?;

        let mut offset = 0;
        let deserialized = Column::deserialize(&buffer, &mut offset)?;

        assert_eq!(original.default_value, deserialized.default_value);
        assert!(deserialized.default_value.is_none());

        Ok(())
    }

    #[test]
    fn test_column_serialization_text_default() -> Result<()> {
        let original = Column::new(ColumnId::new(1), "message".to_string(), DataType::Text)
            .default_value(Value::Text("hello world".to_string()));

        let mut buffer = Vec::new();
        original.serialize(&mut buffer)?;

        let mut offset = 0;
        let deserialized = Column::deserialize(&buffer, &mut offset)?;

        assert_eq!(
            deserialized.default_value,
            Some(Value::Text("hello world".to_string()))
        );

        Ok(())
    }

    #[test]
    fn test_column_serialization_null_default() -> Result<()> {
        let original = Column::new(ColumnId::new(1), "optional".to_string(), DataType::Integer)
            .default_value(Value::Null);

        let mut buffer = Vec::new();
        original.serialize(&mut buffer)?;

        let mut offset = 0;
        let deserialized = Column::deserialize(&buffer, &mut offset)?;

        assert_eq!(deserialized.default_value, Some(Value::Null));

        Ok(())
    }

    #[test]
    fn test_column_deserialization_errors() {
        let buffer = vec![]; // Empty buffer
        let mut offset = 0;
        assert!(Column::deserialize(&buffer, &mut offset).is_err());

        let buffer = vec![1, 2, 3]; // Too short for column ID
        let mut offset = 0;
        assert!(Column::deserialize(&buffer, &mut offset).is_err());
    }

    #[test]
    fn test_column_clone() {
        let original = Column::new(ColumnId::new(1), "test".to_string(), DataType::Text)
            .nullable(false)
            .primary_key(true)
            .default_value(Value::Text("default".to_string()));

        let cloned = original.clone();
        assert_eq!(original.id, cloned.id);
        assert_eq!(original.name, cloned.name);
        assert_eq!(original.data_type, cloned.data_type);
        assert_eq!(original.nullable, cloned.nullable);
        assert_eq!(original.primary_key, cloned.primary_key);
        assert_eq!(original.default_value, cloned.default_value);
    }

    #[test]
    fn test_column_debug() {
        let column = Column::new(ColumnId::new(1), "test".to_string(), DataType::Integer);
        let debug_str = format!("{:?}", column);
        assert!(debug_str.contains("Column"));
        assert!(debug_str.contains("test"));
    }

    #[test]
    fn test_database_header_invalid_magic() -> Result<()> {
        let mut page = Page::new(crate::storage::DB_HEADER_PAGE_ID);

        // Write invalid magic bytes
        page.data[0..4].copy_from_slice(b"BAD!");

        let result = DatabaseHeader::deserialize(&page.data);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("wrong magic bytes"));

        Ok(())
    }

    #[test]
    fn test_database_header_checksum_verification() -> Result<()> {
        let mut header = DatabaseHeader::new(42u32);

        // Corrupt the checksum
        header.checksum = 999;

        let mut page = Page::new(crate::storage::DB_HEADER_PAGE_ID);
        header.serialize(&mut page.data)?;

        // Corrupt checksum in page data
        page.data[16..20].copy_from_slice(&999u32.to_le_bytes());

        let result = DatabaseHeader::deserialize(&page.data);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("checksum verification failed"));

        Ok(())
    }

    #[test]
    fn test_database_header_rejects_unsupported_version() -> Result<()> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut page = Page::new(crate::storage::DB_HEADER_PAGE_ID);
        page.data[0..4].copy_from_slice(&DatabaseHeader::MAGIC);

        // Write an older on-disk version on purpose.
        let unsupported_version = DatabaseHeader::CURRENT_VERSION - 1;
        page.data[4..8].copy_from_slice(&unsupported_version.to_le_bytes());
        page.data[8..12].copy_from_slice(&42u32.to_le_bytes()); // schema root page id
        page.data[12..16].copy_from_slice(&1u32.to_le_bytes()); // next table id

        // Checksum must match the bytes above so version mismatch is what fails.
        let mut hasher = DefaultHasher::new();
        DatabaseHeader::MAGIC.hash(&mut hasher);
        unsupported_version.hash(&mut hasher);
        42.hash(&mut hasher);
        1u32.hash(&mut hasher);
        let checksum = hasher.finish() as u32;
        page.data[16..20].copy_from_slice(&checksum.to_le_bytes());

        let result = DatabaseHeader::deserialize(&page.data);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported database header version"));
        Ok(())
    }

    #[test]
    fn test_database_header_increment_table_id() {
        let mut header = DatabaseHeader::new(42u32);

        let table_id1 = header.increment_table_id();
        assert_eq!(table_id1.as_u32(), 1);
        assert_eq!(header.next_table_id, 2);

        let table_id2 = header.increment_table_id();
        assert_eq!(table_id2.as_u32(), 2);
        assert_eq!(header.next_table_id, 3);

        assert!(header.verify_checksum());
    }

    #[test]
    fn test_database_header_debug() {
        let header = DatabaseHeader::new(42u32);
        let debug_str = format!("{:?}", header);
        assert!(debug_str.contains("DatabaseHeader"));
        assert!(debug_str.contains("42")); // Check for page ID instead
    }

    #[test]
    fn test_table_id_hash() {
        use std::collections::HashSet;

        let id1 = TableId::new(1);
        let id2 = TableId::new(1);
        let id3 = TableId::new(2);

        let mut set = HashSet::new();
        set.insert(id1);
        set.insert(id2); // Same value, shouldn't increase size
        set.insert(id3);

        assert_eq!(set.len(), 2);
        assert!(set.contains(&TableId::new(1)));
        assert!(set.contains(&TableId::new(2)));
    }

    #[test]
    fn test_column_id_hash() {
        use std::collections::HashSet;

        let id1 = ColumnId::new(1);
        let id2 = ColumnId::new(1);
        let id3 = ColumnId::new(2);

        let mut set = HashSet::new();
        set.insert(id1);
        set.insert(id2); // Same value, shouldn't increase size
        set.insert(id3);

        assert_eq!(set.len(), 2);
        assert!(set.contains(&ColumnId::new(1)));
        assert!(set.contains(&ColumnId::new(2)));
    }

    #[test]
    fn test_table_id_equality() {
        let id1 = TableId::new(42);
        let id2 = TableId::new(42);
        let id3 = TableId::new(43);

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_column_id_equality() {
        let id1 = ColumnId::new(42);
        let id2 = ColumnId::new(42);
        let id3 = ColumnId::new(43);

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_table_id_debug() {
        let id = TableId::new(42);
        let debug_str = format!("{:?}", id);
        assert!(debug_str.contains("42"));
    }

    #[test]
    fn test_column_id_debug() {
        let id = ColumnId::new(42);
        let debug_str = format!("{:?}", id);
        assert!(debug_str.contains("42"));
    }

    fn create_test_columns() -> Vec<Column> {
        vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(2), "name".to_string(), DataType::Text),
            Column::new(ColumnId::new(3), "active".to_string(), DataType::Boolean),
        ]
    }

    #[test]
    fn test_duplicate_column_names() -> Result<()> {
        let mut schema = Schema::new();

        let columns = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(2), "id".to_string(), DataType::Text), // Duplicate name
        ];

        let result = schema.create_table("users".to_string(), columns);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Duplicate column name"));

        Ok(())
    }

    #[test]
    fn test_drop_table() -> Result<()> {
        let mut schema = Schema::new();

        let columns = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
        ];

        let table_id = schema.create_table("users".to_string(), columns)?;
        assert_eq!(schema.get_table_count(), 1);

        schema.drop_table(table_id)?;
        assert_eq!(schema.get_table_count(), 0);
        assert!(schema.get_table(table_id).is_none());
        assert!(schema.get_table_by_name("users").is_none());

        Ok(())
    }

    #[test]
    fn test_drop_nonexistent_table() {
        let mut schema = Schema::new();
        let table_id = TableId::new(999);
        let result = schema.drop_table(table_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_tables() -> Result<()> {
        let mut schema = Schema::new();

        let columns1 = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
        ];
        let columns2 = vec![
            Column::new(ColumnId::new(2), "id".to_string(), DataType::Integer).primary_key(true),
        ];

        let table1_id = schema.create_table("users".to_string(), columns1)?;
        let table2_id = schema.create_table("products".to_string(), columns2)?;

        let tables = schema.list_tables();
        assert_eq!(tables.len(), 2);

        // Check that both tables are listed
        let table_ids: Vec<TableId> = tables.iter().map(|(id, _)| *id).collect();
        assert!(table_ids.contains(&table1_id));
        assert!(table_ids.contains(&table2_id));

        // Check table names
        let table_names: Vec<String> = tables.iter().map(|(_, name)| name.clone()).collect();
        assert!(table_names.contains(&"users".to_string()));
        assert!(table_names.contains(&"products".to_string()));

        Ok(())
    }

    #[test]
    fn test_get_table_by_name() -> Result<()> {
        let mut schema = Schema::new();

        let columns = create_test_columns();
        let table_id = schema.create_table("users".to_string(), columns)?;

        let table = schema.get_table_by_name("users");
        assert!(table.is_some());
        assert_eq!(table.unwrap().id, table_id);

        let nonexistent = schema.get_table_by_name("nonexistent");
        assert!(nonexistent.is_none());

        Ok(())
    }

    #[test]
    fn test_table_id_assignment() -> Result<()> {
        let mut schema = Schema::new();

        let columns1 = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
        ];
        let columns2 = vec![
            Column::new(ColumnId::new(2), "id".to_string(), DataType::Integer).primary_key(true),
        ];

        let table1_id = schema.create_table("table1".to_string(), columns1)?;
        let table2_id = schema.create_table("table2".to_string(), columns2)?;

        assert_eq!(table1_id.as_u32(), 1);
        assert_eq!(table2_id.as_u32(), 2);

        Ok(())
    }

    #[test]
    fn test_get_total_column_count() -> Result<()> {
        let mut schema = Schema::new();

        assert_eq!(schema.get_total_column_count(), 0);

        let columns1 = create_test_columns(); // 3 columns
        schema.create_table("users".to_string(), columns1)?;

        assert_eq!(schema.get_total_column_count(), 3);

        let columns2 = vec![
            Column::new(ColumnId::new(4), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(5), "name".to_string(), DataType::Text),
        ]; // 2 columns
        schema.create_table("products".to_string(), columns2)?;

        assert_eq!(schema.get_total_column_count(), 5);

        Ok(())
    }

    #[test]
    fn test_schema_validation() -> Result<()> {
        let mut schema = Schema::new();

        // Valid schema should pass validation
        let columns = create_test_columns();
        schema.create_table("users".to_string(), columns)?;
        assert!(schema.validate().is_ok());

        Ok(())
    }

    #[test]
    fn test_schema_serialization_roundtrip() -> Result<()> {
        let mut original_schema = Schema::new();

        // Add some tables
        let columns1 = create_test_columns();
        let table1_id = original_schema.create_table("users".to_string(), columns1)?;

        let columns2 = vec![
            Column::new(ColumnId::new(4), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(5), "name".to_string(), DataType::Text),
        ];
        let table2_id = original_schema.create_table("products".to_string(), columns2)?;

        // Serialize
        let mut buffer = Vec::new();
        original_schema.serialize(&mut buffer)?;

        // Deserialize
        let deserialized_schema = Schema::deserialize(&buffer)?;

        // Verify structure
        assert_eq!(deserialized_schema.get_table_count(), 2);

        // Verify tables
        assert!(deserialized_schema.get_table(table1_id).is_some());
        assert!(deserialized_schema.get_table(table2_id).is_some());
        assert!(deserialized_schema.get_table_by_name("users").is_some());
        assert!(deserialized_schema.get_table_by_name("products").is_some());

        Ok(())
    }

    #[test]
    fn test_schema_serialization_empty() -> Result<()> {
        let schema = Schema::new();

        let mut buffer = Vec::new();
        schema.serialize(&mut buffer)?;

        let deserialized = Schema::deserialize(&buffer)?;
        assert_eq!(deserialized.get_table_count(), 0);

        Ok(())
    }

    #[test]
    fn test_schema_deserialization_errors() {
        let buffer = vec![]; // Empty buffer
        let result = Schema::deserialize(&buffer);
        assert!(result.is_err());

        let buffer = vec![1, 2, 3]; // Too short for header
        let result = Schema::deserialize(&buffer);
        assert!(result.is_err());
    }

    #[test]
    fn test_set_table_root_page() -> Result<()> {
        let mut schema = Schema::new();

        let columns = create_test_columns();
        let table_id = schema.create_table("users".to_string(), columns)?;

        let new_root_page = 42u32;
        schema.set_table_root_page(table_id, new_root_page)?;

        let table = schema.get_table(table_id).unwrap();
        assert_eq!(table.root_page_id, new_root_page.into());

        Ok(())
    }

    #[test]
    fn test_set_table_root_page_nonexistent() {
        let mut schema = Schema::new();
        let table_id = TableId::new(999);
        let root_page = 42u32;

        let result = schema.set_table_root_page(table_id, root_page);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_clone() -> Result<()> {
        let mut original = Schema::new();

        let columns = create_test_columns();
        original.create_table("users".to_string(), columns)?;

        let cloned = original.clone();
        assert_eq!(cloned.get_table_count(), original.get_table_count());

        // Verify tables are cloned
        assert!(cloned.get_table_by_name("users").is_some());
        assert_eq!(
            cloned.get_total_column_count(),
            original.get_total_column_count()
        );

        Ok(())
    }

    #[test]
    fn test_schema_debug() {
        let schema = Schema::new();
        let debug_str = format!("{:?}", schema);
        assert!(debug_str.contains("Schema"));
    }

    #[test]
    fn test_table_creation() -> Result<()> {
        let columns = create_test_columns();
        let table = Table::new(TableId::new(1), "users".to_string(), columns, 42u32)?;

        assert_eq!(table.id.as_u32(), 1);
        assert_eq!(table.name, "users");
        assert_eq!(table.column_count(), 3);
        assert_eq!(table.primary_key_count(), 1);
        assert_eq!(table.root_page_id, 42);

        Ok(())
    }

    #[test]
    fn test_table_validation_no_columns() {
        let result = Table::new(TableId::new(1), "empty".to_string(), vec![], 1u32);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("at least one column"));
    }

    #[test]
    fn test_table_validation_no_primary_key() {
        let columns = vec![
            Column::new(ColumnId::new(1), "name".to_string(), DataType::Text),
            Column::new(ColumnId::new(2), "age".to_string(), DataType::Integer),
        ];

        let result = Table::new(TableId::new(1), "no_pk".to_string(), columns, 1u32);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("primary key"));
    }

    #[test]
    fn test_table_get_column_by_name() -> Result<()> {
        let columns = create_test_columns();
        let table = Table::new(TableId::new(1), "users".to_string(), columns, 42u32)?;

        let id_column = table.get_column_by_name("id");
        assert!(id_column.is_some());
        assert_eq!(id_column.unwrap().name, "id");

        let name_column = table.get_column_by_name("name");
        assert!(name_column.is_some());
        assert_eq!(name_column.unwrap().data_type, DataType::Text);

        let nonexistent = table.get_column_by_name("nonexistent");
        assert!(nonexistent.is_none());

        Ok(())
    }

    #[test]
    fn test_table_get_column_index() -> Result<()> {
        let columns = create_test_columns();
        let table = Table::new(TableId::new(1), "users".to_string(), columns, 42u32)?;

        assert_eq!(table.get_column_index("id"), Some(0));
        assert_eq!(table.get_column_index("name"), Some(1));
        assert_eq!(table.get_column_index("active"), Some(2));
        assert_eq!(table.get_column_index("nonexistent"), None);

        Ok(())
    }

    #[test]
    fn test_table_validate_row() -> Result<()> {
        let columns = create_test_columns();
        let table = Table::new(TableId::new(1), "users".to_string(), columns, 42u32)?;

        // Valid row
        let valid_row = vec![
            Value::Integer(1),
            Value::Text("Alice".to_string()),
            Value::Boolean(true),
        ];
        assert!(table.validate_row(&valid_row).is_ok());

        // Invalid row length
        let short_row = vec![Value::Integer(1), Value::Text("Alice".to_string())];
        assert!(table.validate_row(&short_row).is_err());

        let long_row = vec![
            Value::Integer(1),
            Value::Text("Alice".to_string()),
            Value::Boolean(true),
            Value::Float(3.14),
        ];
        assert!(table.validate_row(&long_row).is_err());

        // Invalid value types
        let invalid_types = vec![
            Value::Text("not an integer".to_string()),
            Value::Text("Alice".to_string()),
            Value::Boolean(true),
        ];
        assert!(table.validate_row(&invalid_types).is_err());

        Ok(())
    }

    #[test]
    fn test_table_get_primary_key_values() -> Result<()> {
        let columns = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(2), "name".to_string(), DataType::Text),
            Column::new(
                ColumnId::new(3),
                "created_at".to_string(),
                DataType::Integer,
            )
            .primary_key(true),
        ];
        let table = Table::new(TableId::new(1), "logs".to_string(), columns, 42u32)?;

        let row = vec![
            Value::Integer(123),
            Value::Text("log entry".to_string()),
            Value::Integer(456),
        ];

        let pk_values = table.get_primary_key_values(&row)?;
        assert_eq!(pk_values.len(), 2);
        assert_eq!(pk_values[0], Value::Integer(123));
        assert_eq!(pk_values[1], Value::Integer(456));

        Ok(())
    }

    #[test]
    fn test_table_get_primary_key_values_invalid() -> Result<()> {
        let columns = create_test_columns();
        let table = Table::new(TableId::new(1), "users".to_string(), columns, 42u32)?;

        // Row too short for primary key extraction
        let short_row = vec![];
        let result = table.get_primary_key_values(&short_row);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_table_row_size() -> Result<()> {
        let columns = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(2), "name".to_string(), DataType::Text),
            Column::new(ColumnId::new(3), "active".to_string(), DataType::Boolean),
            Column::new(ColumnId::new(4), "price".to_string(), DataType::Float),
        ];
        let table = Table::new(TableId::new(1), "products".to_string(), columns, 42u32)?;

        // Integer (4) + Text (255) + Boolean (1) + Float (8) = 268
        assert_eq!(table.row_size(), 268);

        Ok(())
    }

    #[test]
    fn test_table_serialization_roundtrip() -> Result<()> {
        let columns = create_test_columns();
        let original = Table::new(TableId::new(42), "test_table".to_string(), columns, 123u32)?;

        let mut buffer = Vec::new();
        original.serialize(&mut buffer)?;

        let mut offset = 0;
        let deserialized = Table::deserialize(&buffer, &mut offset)?;

        assert_eq!(original.id, deserialized.id);
        assert_eq!(original.name, deserialized.name);
        assert_eq!(original.root_page_id, deserialized.root_page_id);
        assert_eq!(original.column_count(), deserialized.column_count());
        assert_eq!(
            original.primary_key_count(),
            deserialized.primary_key_count()
        );

        // Check columns
        assert_eq!(deserialized.column_count(), 3);
        assert!(deserialized.get_column_by_name("id").is_some());
        assert!(deserialized.get_column_by_name("name").is_some());
        assert!(deserialized.get_column_by_name("active").is_some());

        // Check primary key columns
        assert_eq!(deserialized.primary_key_columns.len(), 1);
        assert_eq!(deserialized.primary_key_columns[0], 0); // First column is primary key

        Ok(())
    }

    #[test]
    fn test_table_serialization_multiple_primary_keys() -> Result<()> {
        let columns = vec![
            Column::new(ColumnId::new(1), "user_id".to_string(), DataType::Integer)
                .primary_key(true),
            Column::new(ColumnId::new(2), "post_id".to_string(), DataType::Integer)
                .primary_key(true),
            Column::new(ColumnId::new(3), "content".to_string(), DataType::Text),
        ];
        let original = Table::new(TableId::new(1), "user_posts".to_string(), columns, 42u32)?;

        let mut buffer = Vec::new();
        original.serialize(&mut buffer)?;

        let mut offset = 0;
        let deserialized = Table::deserialize(&buffer, &mut offset)?;

        assert_eq!(deserialized.primary_key_columns.len(), 2);
        assert_eq!(deserialized.primary_key_columns[0], 0); // First column
        assert_eq!(deserialized.primary_key_columns[1], 1); // Second column

        Ok(())
    }

    #[test]
    fn test_table_serialization_roundtrip_with_constraints() -> Result<()> {
        let columns = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(2), "user_id".to_string(), DataType::Integer),
            Column::new(ColumnId::new(3), "name".to_string(), DataType::Text),
        ];
        let mut original = Table::new(TableId::new(7), "posts".to_string(), columns, 42u32)?;
        original.add_check_constraint(crate::catalog::table::CheckConstraint {
            name: Some("ck_name".to_string()),
            expression_sql: "name != ''".to_string(),
        })?;
        original.add_foreign_key(crate::catalog::table::ForeignKeyConstraint {
            name: Some("fk_posts_user".to_string()),
            column_indices: vec![1],
            referenced_table: "users".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: crate::catalog::table::ForeignKeyAction::Restrict,
            on_update: crate::catalog::table::ForeignKeyAction::Restrict,
        })?;

        let mut buffer = Vec::new();
        original.serialize(&mut buffer)?;

        let mut offset = 0;
        let deserialized = Table::deserialize(&buffer, &mut offset)?;

        assert_eq!(deserialized.check_constraints, original.check_constraints);
        assert_eq!(deserialized.foreign_keys, original.foreign_keys);
        Ok(())
    }

    #[test]
    fn test_table_deserialization_errors() {
        let buffer = vec![]; // Empty buffer
        let mut offset = 0;
        assert!(Table::deserialize(&buffer, &mut offset).is_err());

        let buffer = vec![1, 2, 3]; // Too short for table header
        let mut offset = 0;
        assert!(Table::deserialize(&buffer, &mut offset).is_err());
    }

    #[test]
    fn test_table_clone() -> Result<()> {
        let columns = create_test_columns();
        let original = Table::new(TableId::new(1), "users".to_string(), columns, 42u32)?;

        let cloned = original.clone();
        assert_eq!(original.id, cloned.id);
        assert_eq!(original.name, cloned.name);
        assert_eq!(original.column_count(), cloned.column_count());
        assert_eq!(original.primary_key_count(), cloned.primary_key_count());

        // Verify column indices are preserved
        assert_eq!(
            original.get_column_index("id"),
            cloned.get_column_index("id")
        );
        assert_eq!(
            original.get_column_index("name"),
            cloned.get_column_index("name")
        );

        Ok(())
    }

    #[test]
    fn test_table_debug() -> Result<()> {
        let columns = create_test_columns();
        let table = Table::new(TableId::new(1), "users".to_string(), columns, 42u32)?;

        let debug_str = format!("{:?}", table);
        assert!(debug_str.contains("Table"));
        assert!(debug_str.contains("users"));

        Ok(())
    }

    #[test]
    fn test_value_type_conversions() {
        let int_val = Value::Integer(42);
        assert_eq!(int_val.as_integer(), Some(42));
        assert_eq!(int_val.as_text(), None);
        assert_eq!(int_val.as_boolean(), None);
        assert_eq!(int_val.as_float(), None);

        let text_val = Value::Text("hello".to_string());
        assert_eq!(text_val.as_integer(), None);
        assert_eq!(text_val.as_text(), Some("hello".to_string()));
        assert_eq!(text_val.as_boolean(), None);
        assert_eq!(text_val.as_float(), None);

        let bool_val = Value::Boolean(true);
        assert_eq!(bool_val.as_integer(), None);
        assert_eq!(bool_val.as_text(), None);
        assert_eq!(bool_val.as_boolean(), Some(true));
        assert_eq!(bool_val.as_float(), None);

        let float_val = Value::Float(3.14);
        assert_eq!(float_val.as_integer(), None);
        assert_eq!(float_val.as_text(), None);
        assert_eq!(float_val.as_boolean(), None);
        assert_eq!(float_val.as_float(), Some(3.14));

        let null_val = Value::Null;
        assert_eq!(null_val.as_integer(), None);
        assert_eq!(null_val.as_text(), None);
        assert_eq!(null_val.as_boolean(), None);
        assert_eq!(null_val.as_float(), None);
        assert!(null_val.is_null());
    }

    #[test]
    fn test_value_ordering() {
        // Integer ordering
        assert!(Value::Integer(1) < Value::Integer(2));
        assert!(Value::Integer(2) > Value::Integer(1));

        // Text ordering
        assert!(Value::Text("a".to_string()) < Value::Text("b".to_string()));
        assert!(Value::Text("b".to_string()) > Value::Text("a".to_string()));

        // Boolean ordering
        assert!(Value::Boolean(false) < Value::Boolean(true));
        assert!(Value::Boolean(true) > Value::Boolean(false));

        // Float ordering
        assert!(Value::Float(1.0) < Value::Float(2.0));
        assert!(Value::Float(2.0) > Value::Float(1.0));

        // NULL ordering (NULL is always less)
        assert!(Value::Null < Value::Integer(0));
        assert!(Value::Null < Value::Text("".to_string()));
        assert!(Value::Null < Value::Boolean(false));
        assert!(Value::Null < Value::Float(0.0));
        assert!(Value::Integer(0) > Value::Null);
        assert!(Value::Text("".to_string()) > Value::Null);
        assert!(Value::Boolean(false) > Value::Null);
        assert!(Value::Float(0.0) > Value::Null);

        // Different types (should not be comparable)
        assert_eq!(
            Value::Integer(1).partial_cmp(&Value::Text("1".to_string())),
            None
        );
        assert_eq!(Value::Boolean(true).partial_cmp(&Value::Integer(1)), None);
    }

    #[test]
    fn test_value_clone() {
        let original = Value::Text("hello".to_string());
        let cloned = original.clone();
        assert_eq!(original, cloned);
        assert_eq!(original.as_text(), Some("hello".to_string()));
        assert_eq!(cloned.as_text(), Some("hello".to_string()));
    }
}

// Tests for the new SQLite-style catalog implementation
#[cfg(test)]
mod catalog_new_tests {
    use crate::catalog::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::catalog::ids::{ColumnId, TableId};
    use crate::catalog::serialization::IndexKeyCodec;
    use crate::catalog::types::DataType;
    use crate::catalog::Value;
    use crate::error::Result;
    use crate::test_utils::TestDbFile;

    #[test]
    fn test_catalog_new_database() -> Result<()> {
        let test_db = TestDbFile::new("_test_new_catalog");

        {
            let mut catalog = Catalog::open_or_create(test_db.path())?;

            // Should start with empty schema
            assert_eq!(catalog.list_tables()?.len(), 0);

            // Create a table
            let columns = vec![
                Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer)
                    .primary_key(true),
                Column::new(ColumnId::new(2), "name".to_string(), DataType::Text),
            ];

            let table_id = catalog.create_table("users", columns)?;
            assert_eq!(catalog.list_tables()?.len(), 1);

            let table = catalog.get_table(table_id)?.unwrap();
            assert_eq!(table.name, "users");
        } // catalog is dropped here

        // Reopen and verify persistence
        {
            let catalog = Catalog::open_or_create(test_db.path())?;
            assert_eq!(catalog.list_tables()?.len(), 1);
        }

        Ok(())
    }

    #[test]
    fn test_catalog_table_operations() -> Result<()> {
        let test_db = TestDbFile::new("_test_table_ops");

        let mut catalog = Catalog::open_or_create(test_db.path())?;

        // Create multiple tables
        let columns1 = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(2), "email".to_string(), DataType::Text),
        ];

        let columns2 = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(2), "title".to_string(), DataType::Text),
        ];

        let users_id = catalog.create_table("users", columns1)?;
        let posts_id = catalog.create_table("posts", columns2)?;

        // Test retrieval
        let users = catalog.get_table(users_id)?.unwrap();
        assert_eq!(users.name, "users");
        assert_eq!(users.column_count(), 2);

        let posts = catalog.get_table_by_name("posts")?.unwrap();
        assert_eq!(posts.id, posts_id);
        assert_eq!(posts.column_count(), 2);

        // Test listing
        let tables = catalog.list_tables()?;
        assert_eq!(tables.len(), 2);

        // Test dropping
        catalog.drop_table(users_id)?;
        assert_eq!(catalog.list_tables()?.len(), 1);
        assert!(catalog.get_table(users_id)?.is_none());

        Ok(())
    }

    #[test]
    fn test_catalog_duplicate_table() -> Result<()> {
        let test_db = TestDbFile::new("_test_duplicate");

        let mut catalog = Catalog::open_or_create(test_db.path())?;

        let columns = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
        ];

        // Create first table
        catalog.create_table("users", columns.clone())?;

        // Try to create duplicate - should fail
        let result = catalog.create_table("users", columns);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));

        Ok(())
    }

    #[test]
    fn test_catalog_new_methods() -> Result<()> {
        let test_db = TestDbFile::new("_test_new_methods");

        let mut catalog = Catalog::open_or_create(test_db.path())?;

        // Create a table
        let columns = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
            Column::new(ColumnId::new(2), "name".to_string(), DataType::Text),
        ];

        let table_id = catalog.create_table("users", columns)?;

        // Test table existence methods
        assert!(catalog.table_exists("users"));
        assert!(catalog.table_exists_by_id(table_id));
        assert!(!catalog.table_exists("nonexistent"));
        assert!(!catalog.table_exists_by_id(TableId::new(999)));

        // Test root page management
        let root_page = 42u32;
        catalog.set_table_root_page(table_id, root_page)?;

        let retrieved_page = catalog.get_table_root_page(table_id)?.unwrap();
        assert_eq!(retrieved_page, root_page);

        // Test table statistics
        let stats = catalog.get_table_stats(table_id)?.unwrap();
        assert_eq!(stats.id, table_id);
        assert_eq!(stats.name, "users");
        assert_eq!(stats.column_count, 2);
        assert_eq!(stats.primary_key_count, 1);
        assert_eq!(stats.root_page_id, root_page.into());

        // Test all table statistics
        let all_stats = catalog.get_all_table_stats()?;
        assert_eq!(all_stats.len(), 1);
        assert_eq!(all_stats[0].name, "users");

        // Test column methods
        let columns = catalog.get_table_columns(table_id)?.unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[1].name, "name");

        let columns_by_name = catalog.get_table_columns_by_name("users")?.unwrap();
        assert_eq!(columns_by_name.len(), 2);

        let pk_columns = catalog.get_primary_key_columns(table_id)?.unwrap();
        assert_eq!(pk_columns.len(), 1);
        assert_eq!(pk_columns[0].name, "id");
        assert!(pk_columns[0].primary_key);

        // Test schema validation
        assert!(catalog.validate_schema().is_ok());

        // Test column count
        assert_eq!(catalog.get_total_column_count(), 2);

        // Test peek next table ID
        let next_id = catalog.peek_next_table_id()?;
        assert_eq!(next_id.as_u32(), 2); // First table was ID 1

        Ok(())
    }

    #[test]
    fn test_catalog_create_table_with_root() -> Result<()> {
        let test_db = TestDbFile::new("_test_create_with_root");

        let root_page = 100u32;

        {
            let mut catalog = Catalog::open_or_create(test_db.path())?;
            let columns = vec![
                Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer)
                    .primary_key(true),
            ];

            let table_id = catalog.create_table_with_root("products", columns, root_page)?;

            let table = catalog.get_table(table_id)?.unwrap();
            assert_eq!(table.name, "products");
            assert_eq!(table.root_page_id, root_page.into());
        }

        let reopened = Catalog::open_or_create(test_db.path())?;
        let table = reopened.get_table_by_name("products")?.unwrap();
        assert_eq!(table.root_page_id, root_page.into());

        Ok(())
    }

    #[test]
    fn test_catalog_root_page_update_persists_across_reopen() -> Result<()> {
        let test_db = TestDbFile::new("_test_root_page_persistence");
        let updated_root = 77u32;

        {
            let mut catalog = Catalog::open_or_create(test_db.path())?;
            let columns = vec![
                Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer)
                    .primary_key(true),
            ];

            let table_id = catalog.create_table("users", columns)?;
            catalog.set_table_root_page(table_id, updated_root)?;
            assert_eq!(catalog.get_table_root_page(table_id)?, Some(updated_root));
        }

        let reopened = Catalog::open_or_create(test_db.path())?;
        let table = reopened.get_table_by_name("users")?.unwrap();
        assert_eq!(table.root_page_id, updated_root.into());

        Ok(())
    }

    #[test]
    fn test_catalog_validation_logic() -> Result<()> {
        let test_db = TestDbFile::new("_test_validation");

        let mut catalog = Catalog::open_or_create(test_db.path())?;

        // Create a table
        let columns = vec![
            Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer).primary_key(true),
        ];

        let table_id = catalog.create_table("users", columns)?;

        // Test validation of newly created table (should pass - root page 0 is OK)
        assert!(catalog.validate_schema().is_ok());

        // Test setting invalid root page (page 0 should be rejected)
        let result = catalog.set_table_root_page(table_id, 0u32);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("reserved for database header"));

        // Test setting valid root page
        let valid_page = 42u32;
        assert!(catalog.set_table_root_page(table_id, valid_page).is_ok());

        // Test getting root page
        let retrieved_page = catalog.get_table_root_page(table_id)?.unwrap();
        assert_eq!(retrieved_page, valid_page);

        // Test validation with valid root page
        assert!(catalog.validate_schema().is_ok());

        // Test setting root page for non-existent table
        let fake_id = TableId::new(999);
        let result = catalog.set_table_root_page(fake_id, 100u32);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));

        Ok(())
    }

    #[test]
    fn test_table_secondary_index_metadata_roundtrip() -> Result<()> {
        let mut table = crate::catalog::Table::new(
            TableId::new(1),
            "users".to_string(),
            vec![
                Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer)
                    .primary_key(true),
                Column::new(ColumnId::new(2), "email".to_string(), DataType::Text),
            ],
            10u32,
        )?;

        table.add_secondary_index(crate::catalog::SecondaryIndex {
            name: "idx_users_email".to_string(),
            column_indices: vec![1],
            root_page_id: 42u32.into(),
            unique: false,
        })?;

        let bytes = table.to_bytes()?;
        let decoded = crate::catalog::Table::from_bytes(&bytes)?;

        assert_eq!(decoded.secondary_indexes.len(), 1);
        let index = decoded.get_secondary_index("idx_users_email").unwrap();
        assert_eq!(index.column_indices, vec![1]);
        assert_eq!(index.root_page_id, 42u32.into());

        Ok(())
    }

    #[test]
    fn test_catalog_secondary_index_metadata_persists_across_reopen() -> Result<()> {
        let test_db = TestDbFile::new("_test_secondary_index_metadata_persistence");

        {
            let mut catalog = Catalog::open_or_create(test_db.path())?;
            let table_id = catalog.create_table(
                "users",
                vec![
                    Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer)
                        .primary_key(true),
                    Column::new(ColumnId::new(2), "email".to_string(), DataType::Text),
                ],
            )?;
            catalog.add_secondary_index(
                table_id,
                crate::catalog::SecondaryIndex {
                    name: "idx_users_email".to_string(),
                    column_indices: vec![1],
                    root_page_id: 55u32.into(),
                    unique: false,
                },
            )?;
            catalog.flush()?;
        }

        let reopened = Catalog::open_or_create(test_db.path())?;
        let table = reopened.get_table_by_name("users")?.unwrap();
        assert_eq!(table.secondary_indexes.len(), 1);
        let index = table.get_secondary_index("idx_users_email").unwrap();
        assert_eq!(index.column_indices, vec![1]);
        assert_eq!(index.root_page_id, 55u32.into());

        Ok(())
    }

    #[test]
    fn test_catalog_validate_integrity_detects_storage_schema_mismatch() -> Result<()> {
        let test_db = TestDbFile::new("_test_catalog_validate_integrity_mismatch");

        let mut catalog = Catalog::open_or_create(test_db.path())?;
        let table_id = catalog.create_table(
            "users",
            vec![
                Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer)
                    .primary_key(true)
                    .nullable(false),
            ],
        )?;
        let root_page_id = catalog.with_engine(|storage| storage.create_table("users"))?;
        catalog.set_table_root_page(table_id, root_page_id)?;
        assert!(catalog.validate_integrity().is_ok());

        catalog.with_engine(|storage| storage.drop_table("users"))?;

        let err = catalog.validate_integrity().unwrap_err();
        assert!(err.to_string().contains("missing from storage metadata"));

        Ok(())
    }

    #[test]
    fn test_catalog_validate_integrity_detects_index_table_overlap() -> Result<()> {
        let test_db = TestDbFile::new("_test_catalog_validate_integrity_index_overlap");

        let mut catalog = Catalog::open_or_create(test_db.path())?;
        let table_id = catalog.create_table(
            "users",
            vec![
                Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer)
                    .primary_key(true)
                    .nullable(false),
                Column::new(ColumnId::new(2), "email".to_string(), DataType::Text),
            ],
        )?;
        let table_root_id = catalog.with_engine(|storage| storage.create_table("users"))?;
        let primary_key_root_id = catalog.with_engine(|storage| storage.create_empty_btree())?;
        catalog.set_table_root_page(table_id, table_root_id)?;
        catalog.set_table_primary_key_root_page(table_id, primary_key_root_id)?;
        catalog.add_secondary_index(
            table_id,
            crate::catalog::SecondaryIndex {
                name: "idx_users_email".to_string(),
                column_indices: vec![1],
                root_page_id: table_root_id,
                unique: false,
            },
        )?;

        let err = catalog.validate_integrity().unwrap_err();
        assert!(err.to_string().contains("overlaps table storage"));

        Ok(())
    }

    #[test]
    fn test_secondary_index_cursor_exposes_logical_key_only() -> Result<()> {
        let test_db = TestDbFile::new("_test_secondary_index_cursor_logical_key");
        let mut catalog = Catalog::open_or_create(test_db.path())?;
        let table_id = catalog.create_table(
            "users",
            vec![
                Column::new(ColumnId::new(1), "id".to_string(), DataType::Integer)
                    .primary_key(true)
                    .nullable(false),
                Column::new(ColumnId::new(2), "email".to_string(), DataType::Text),
            ],
        )?;
        let table_root_id = catalog.with_engine(|engine| engine.create_table("users"))?;
        let primary_key_root_id = catalog.with_engine(|engine| engine.create_empty_btree())?;
        let secondary_index_root_id = catalog.with_engine(|engine| engine.create_empty_btree())?;
        catalog.set_table_root_page(table_id, table_root_id)?;
        catalog.set_table_primary_key_root_page(table_id, primary_key_root_id)?;
        catalog.add_secondary_index(
            table_id,
            crate::catalog::SecondaryIndex {
                name: "idx_users_email".to_string(),
                column_indices: vec![1],
                root_page_id: secondary_index_root_id,
                unique: false,
            },
        )?;

        let row_id = catalog.with_engine(|engine| {
            engine.insert_into_table(
                "users",
                vec![
                    Value::Integer(1),
                    Value::Text("alice@example.com".to_string()),
                ],
            )
        })?;
        let table = catalog
            .get_table_by_name("users")?
            .expect("users table should exist");
        catalog.with_engine(|engine| {
            let row = crate::catalog::StoredRow {
                row_id,
                values: vec![
                    Value::Integer(1),
                    Value::Text("alice@example.com".to_string()),
                ],
            };
            engine.register_primary_key_row(&table, row.clone())?;
            engine.register_secondary_index_row(&table, row)?;
            let mut cursor = engine.open_secondary_index_cursor(&table, "idx_users_email")?;
            assert!(cursor.first());
            let entry = cursor
                .current()
                .expect("secondary index entry should exist");
            assert_eq!(
                entry.key,
                IndexKeyCodec::encode_key(&[Value::Text("alice@example.com".to_string())])?
            );
            assert_eq!(entry.row_id, row_id);
            Ok(())
        })?;

        Ok(())
    }

    #[test]
    fn test_index_key_codec_rejects_truncated_rowid_bytes() {
        let err = IndexKeyCodec::decode_row_id(&[1, 2, 3]).unwrap_err();
        assert!(err
            .to_string()
            .contains("Index rowid payload must be exactly 8 bytes"));

        let err = IndexKeyCodec::split_secondary_key(&[1, 2, 3, 4, 5, 6, 7]).unwrap_err();
        assert!(err
            .to_string()
            .contains("Index entry is missing rowid bytes"));
    }
}
