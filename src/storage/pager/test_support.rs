use super::{Pager, PagerState};

#[cfg(test)]
impl Pager {
    pub(crate) fn dirty_page_count(&self) -> usize {
        self.cache.dirty_count()
    }

    pub(crate) fn wal_snapshot_sequence(&self) -> Option<u64> {
        self.wal_read_snapshot
            .as_ref()
            .map(|snapshot| snapshot.visible_sequence)
    }

    pub fn inject_io_failure(&mut self) {
        self.file_manager.inject_write_failure();
    }

    pub fn state(&self) -> PagerState {
        self.state
    }
}
