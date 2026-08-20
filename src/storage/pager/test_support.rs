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
