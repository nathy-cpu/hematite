use super::{JournalMode, Pager, PagerState};
use crate::error::Result;

impl Pager {
    pub(super) fn begin_rollback_transaction(&mut self) -> Result<()> {
        if self.journal_mode == JournalMode::Rollback {
            self.initialize_rollback_journal()?;
        }
        Ok(())
    }

    pub(super) fn commit_rollback_transaction(&mut self) -> Result<()> {
        self.flush()?;
        self.mark_rollback_journal_committed()?;
        self.transition_state(PagerState::WriterFinished)?;
        Ok(())
    }

    pub(super) fn rollback_rollback_transaction(&mut self) -> Result<()> {
        self.rollback_from_active_transaction()?;
        self.remove_journal_file()
    }
}
