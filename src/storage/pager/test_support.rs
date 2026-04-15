use super::{Pager, PagerState};

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
