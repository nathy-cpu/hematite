use super::{JournalMode, Pager, PagerLockMode, PagerState};
use crate::error::Result;

impl Pager {
    pub fn begin_read(&mut self) -> Result<()> {
        self.check_error_state()?;
        let previous_lock_mode = self.lock_mode;
        self.acquire_shared_lock()?;
        if matches!(previous_lock_mode, PagerLockMode::Shared { .. }) {
            return Ok(());
        }
        if !matches!(previous_lock_mode, PagerLockMode::Write) {
            if let Err(err) = self.refresh_persisted_view() {
                let _ = self.release_shared_lock();
                return Err(err);
            }
        }
        if self.journal_mode == JournalMode::Wal {
            if matches!(previous_lock_mode, PagerLockMode::Write) {
                return Ok(());
            }
            let snapshot = self.snapshot_wal_visible_state()?;
            self.register_wal_reader_sequence(snapshot.visible_sequence)?;
            self.wal_read_snapshot = Some(snapshot);
        }
        if !matches!(self.lock_mode, PagerLockMode::Write) {
            self.state = PagerState::Reader;
        }
        Ok(())
    }

    pub fn end_read(&mut self) -> Result<()> {
        if matches!(self.lock_mode, PagerLockMode::Shared { depth: 1 }) {
            if let Some(snapshot) = &self.wal_read_snapshot {
                self.unregister_wal_reader_sequence(snapshot.visible_sequence)?;
            }
        }
        self.wal_read_snapshot = None;
        self.release_shared_lock()?;
        if matches!(self.lock_mode, PagerLockMode::None) {
            self.state = PagerState::Open;
        }
        Ok(())
    }
}
