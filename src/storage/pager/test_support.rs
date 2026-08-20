use super::{Pager, PagerState};
use crate::error::Result;

#[cfg(test)]
impl Pager {
    pub(crate) fn dirty_page_count(&self) -> usize {
        self.cache_read()
            .expect("pager cache lock should not be poisoned in tests")
            .dirty_count()
    }

    pub(crate) fn cached_page_count(&self) -> usize {
        self.cache_read()
            .expect("pager cache lock should not be poisoned in tests")
            .entry_count()
    }

    pub(crate) fn wal_snapshot_sequence(&self) -> Option<u64> {
        self.wal_read_snapshot
            .as_ref()
            .map(|snapshot| snapshot.visible_sequence)
    }

    pub(crate) fn wal_visible_state_reload_count(&self) -> usize {
        self.wal_visible_state_reload_count
    }

    /// Throws away every journal write since the last `sync_all`, as a power loss
    /// would.
    ///
    /// Lets a test distinguish a synced journal write from an unsynced one, which no
    /// process-level test can do on its own: dropping the pager is a process exit,
    /// and the kernel hands unsynced bytes straight back on reopen.
    pub fn simulate_journal_power_loss(&mut self) -> Result<()> {
        if let Some(file) = &mut self.journal_file {
            file.lose_unsynced_writes()?;
        }
        Ok(())
    }

    /// Throws away every database-file write since the last fsync, as a power loss
    /// would.
    pub fn simulate_database_power_loss(&mut self) -> Result<()> {
        self.file_manager.lose_unsynced_writes()
    }

    /// Truncates the WAL to `bytes`, modelling an append that was interrupted.
    ///
    /// A torn tail is the WAL's characteristic failure: frames are appended and only
    /// then synced, so a power loss leaves a partial frame at the end. Recovery is
    /// supposed to ignore everything past the last complete commit.
    pub fn simulate_torn_wal_append(&self, bytes: u64) -> Result<()> {
        let Some(path) = &self.wal_path else {
            return Ok(());
        };
        let file = std::fs::OpenOptions::new().write(true).open(path)?;
        file.set_len(bytes)?;
        file.sync_all()?;
        Ok(())
    }

    /// Current size of the WAL, so a test can tear it at a chosen fraction.
    pub fn wal_len(&self) -> Result<u64> {
        let Some(path) = &self.wal_path else {
            return Ok(0);
        };
        Ok(std::fs::metadata(path).map(|m| m.len()).unwrap_or(0))
    }

    /// Leaves the journal in place after a commit, standing in for a crash in the
    /// window between the database fsync and the journal's removal.
    ///
    /// That window is where the commit intent has to do its work: the transaction is
    /// durable, but a journal full of pre-images is still sitting on disk, and
    /// recovery must not replay it.
    pub fn keep_journal_after_commit(&mut self) {
        self.keep_journal_after_commit = true;
    }

    pub fn inject_io_failure(&mut self) {
        self.file_manager.inject_write_failure();
    }

    pub fn inject_io_failure_after(&mut self, writes_before_failure: usize) {
        self.file_manager
            .inject_write_failure_after(writes_before_failure);
    }

    pub fn state(&self) -> PagerState {
        self.state
    }
}
