use super::{JournalMode, Pager, PagerState};
use crate::error::Result;
use crate::storage::journal::JournalState;

impl Pager {
    pub(super) fn begin_rollback_transaction(&mut self) -> Result<()> {
        if self.journal_mode == JournalMode::Rollback {
            self.persist_journal(JournalState::Active)?;
        }
        Ok(())
    }

    pub(super) fn commit_rollback_transaction(&mut self) -> Result<()> {
        self.flush()?;
        self.persist_journal(JournalState::Committed)?;
        self.state = PagerState::WriterFinished;
        Ok(())
    }

    pub(super) fn rollback_rollback_transaction(&mut self) -> Result<()> {
        self.rollback_from_active_transaction()?;
        self.remove_journal_file()
    }
}
