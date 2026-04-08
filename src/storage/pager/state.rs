use crate::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalMode {
    Rollback,
    Wal,
}

impl JournalMode {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value {
            "rollback" => Ok(Self::Rollback),
            "wal" => Ok(Self::Wal),
            _ => Err(crate::error::HematiteError::StorageError(format!(
                "Unsupported pager journal mode '{}'",
                value
            ))),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Rollback => "rollback",
            Self::Wal => "wal",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PagerState {
    Open,
    Reader,
    WriterLocked,
    WriterCacheMod,
    WriterDbMod,
    WriterFinished,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PagerLockMode {
    None,
    Shared { depth: usize },
    Write,
}
