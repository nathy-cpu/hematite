use super::Pager;
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

impl PagerState {
    fn allows_transition_to(self, next: PagerState) -> bool {
        use PagerState as S;
        match (self, next) {
            (current, next) if current == next => true,
            (_, S::Error) => true,
            (S::Open, S::Reader | S::WriterLocked) => true,
            (S::Reader, S::Open) => true,
            (S::WriterLocked, S::WriterCacheMod | S::WriterFinished | S::Open) => true,
            (S::WriterCacheMod, S::WriterDbMod | S::WriterFinished | S::Open) => true,
            (S::WriterDbMod, S::WriterFinished | S::Open) => true,
            (S::WriterFinished, S::Open) => true,
            (S::Error, S::Open) => true,
            _ => false,
        }
    }

    fn is_compatible_with_lock(self, lock_mode: PagerLockMode) -> bool {
        match self {
            PagerState::Open => matches!(lock_mode, PagerLockMode::None),
            PagerState::Reader => matches!(lock_mode, PagerLockMode::Shared { .. }),
            PagerState::WriterLocked
            | PagerState::WriterCacheMod
            | PagerState::WriterDbMod
            | PagerState::WriterFinished => matches!(lock_mode, PagerLockMode::Write),
            PagerState::Error => true,
        }
    }
}

impl Pager {
    pub(super) fn transition_state(&mut self, next: PagerState) -> Result<()> {
        if !self.state.allows_transition_to(next) {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Illegal pager state transition from {:?} to {:?}",
                self.state, next
            )));
        }
        if !next.is_compatible_with_lock(self.lock_mode) {
            return Err(crate::error::HematiteError::StorageError(format!(
                "Pager state {:?} is incompatible with lock mode {:?}",
                next, self.lock_mode
            )));
        }
        self.state = next;
        Ok(())
    }

    pub(super) fn enter_error_state(&mut self) {
        self.state = PagerState::Error;
    }
}
