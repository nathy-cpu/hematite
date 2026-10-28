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
        // journals it — and only then syncs, so syncing first would mean syncing twice:
        // once for the records written during the transaction, and again for the
        // record staging added. One sync covers both, and it still lands before any
        // database page is written, which is the write-ahead rule this relies on.
        //
        // The sync inside `flush` always fires: `initialize_rollback_journal` sets
        // `journal_needs_sync` at BEGIN, so even a transaction that journals no page
        // still gets its header made durable before the database file is touched.
        self.flush_for_commit()?;
        // No commit-marker fsync. The journal carries a commit intent — every page
        // this transaction wrote, with the checksum it must hold — synced before any
        // database page was overwritten. Recovery reads the database and compares,
        // so it needs no durable "committed" flag to tell a finished commit from an
        // abandoned one, and the commit stays at two fsyncs: journal, then database.
        self.transition_state(PagerState::WriterFinished)?;
        Ok(())
    }

    pub(super) fn rollback_rollback_transaction(&mut self) -> Result<()> {
        self.rollback_from_active_transaction()?;
        self.retire_journal_file()
    }
}
