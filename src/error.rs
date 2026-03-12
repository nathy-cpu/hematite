//! Error handling for Hematite database

use std::fmt;

#[derive(Debug)]
pub enum HematiteError {
    IoError(std::io::Error),
    CorruptedData(String),
    InvalidPage(u32),
    PageNotFound(u32),
    InvalidSchema(String),
    ParseError(String),
    StorageError(String),
}

impl fmt::Display for HematiteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HematiteError::IoError(e) => write!(f, "IO Error: {}", e),
            HematiteError::CorruptedData(msg) => write!(f, "Corrupted data: {}", msg),
            HematiteError::InvalidPage(page) => write!(f, "Invalid page: {}", page),
            HematiteError::PageNotFound(page) => write!(f, "Page not found: {}", page),
            HematiteError::InvalidSchema(msg) => write!(f, "Invalid schema: {}", msg),
            HematiteError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            HematiteError::StorageError(msg) => write!(f, "Storage error: {}", msg),
        }
    }
}

impl std::error::Error for HematiteError {}

impl From<std::io::Error> for HematiteError {
    fn from(error: std::io::Error) -> Self {
        HematiteError::IoError(error)
    }
}

pub type Result<T> = std::result::Result<T, HematiteError>;
