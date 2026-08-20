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
        // No journal sync here. `flush` stages the persisted-state page — which
        // journals it — and only then syncs, so syncing first meant syncing twice:
        // once for the records written during the transaction, and again for the
        // record staging added. One sync covers both, and it still lands before any
        // database page is written, which is the write-ahead rule this relies on.
        //
        // The sync inside `flush` always fires: `initialize_rollback_journal` sets
        // `journal_needs_sync` at BEGIN, so even a transaction that journals no page
        // still gets its header made durable before the database file is touched.
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
