//! Data types and values for the database

use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Integer,
    Text,
    Boolean,
    Float,
}

impl DataType {
    pub fn size(&self) -> usize {
        match self {
            DataType::Integer => 4,
            DataType::Text => 255, // Maximum length
            DataType::Boolean => 1,
            DataType::Float => 8,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            DataType::Integer => "INTEGER",
            DataType::Text => "TEXT",
            DataType::Boolean => "BOOLEAN",
            DataType::Float => "FLOAT",
        }
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Integer(i32),
    Text(String),
    Boolean(bool),
    Float(f64),
    Null,
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Integer(_) => DataType::Integer,
            Value::Text(_) => DataType::Text,
            Value::Boolean(_) => DataType::Boolean,
            Value::Float(_) => DataType::Float,
            Value::Null => DataType::Text, // NULL can be any type
        }
    }

    pub fn is_compatible_with(&self, data_type: DataType) -> bool {
        match (self, data_type) {
            (Value::Integer(_), DataType::Integer) => true,
            (Value::Text(_), DataType::Text) => true,
            (Value::Boolean(_), DataType::Boolean) => true,
            (Value::Float(_), DataType::Float) => true,
            (Value::Null, _) => true, // NULL is compatible with any type
            _ => false,
        }
    }

    pub fn as_integer(&self) -> Option<i32> {
        match self {
            Value::Integer(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Null, _) => Some(Ordering::Less), // NULL is always less than any value
            (_, Value::Null) => Some(Ordering::Greater),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_value_type_conversions() {
        let int_val = Value::Integer(42);
        assert_eq!(int_val.as_integer(), Some(42));
        assert_eq!(int_val.as_text(), None);
        assert_eq!(int_val.as_boolean(), None);
        assert_eq!(int_val.as_float(), None);

        let text_val = Value::Text("hello".to_string());
        assert_eq!(text_val.as_integer(), None);
        assert_eq!(text_val.as_text(), Some("hello"));
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
        assert_eq!(original.as_text(), Some("hello"));
        assert_eq!(cloned.as_text(), Some("hello"));
    }
}
