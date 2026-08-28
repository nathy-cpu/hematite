use super::{JournalMode, Pager, PagerLockMode};
use crate::error::Result;
use crate::storage::{Page, PageId};
use std::sync::Arc;

/// Frames the WAL may reach before a commit tries to check it back in.
///
/// At 4 KiB pages this is roughly 4 MiB. The number is a balance: too low and
/// commits pay for checkpoints they did not need, too high and readers pay to
/// re-parse a large WAL. SQLite uses 1000 frames as its default for the same reasons.
const WAL_AUTOCHECKPOINT_FRAMES: u64 = 1000;

impl Pager {
    fn checkpoint_wal_guarded(&mut self) -> Result<()> {
        if let Err(err) = self.checkpoint_wal_unlocked() {
            self.enter_error_state();
            return Err(err);
        }
        Ok(())
    }

    pub fn set_journal_mode(&mut self, journal_mode: JournalMode) -> Result<()> {
        if self.transaction.is_some() {
            return Err(crate::error::HematiteError::StorageError(
                "Cannot change pager journal mode during an active transaction".to_string(),
            ));
        }
        if self.journal_mode == journal_mode {
            return Ok(());
        }
        if self.journal_mode == JournalMode::Wal && journal_mode == JournalMode::Rollback {
            if !self.can_checkpoint_wal()? {
                return Err(crate::error::HematiteError::StorageError(
                    "Cannot switch from WAL while readers are active".to_string(),
                ));
            }
            self.checkpoint_wal_guarded()?;
        }
        if journal_mode == JournalMode::Rollback {
            self.remove_wal_file()?;
            self.latest_wal_state = None;
            self.latest_wal_file_stamp = None;
            self.wal_read_snapshot = None;
        } else {
            self.remove_journal_file()?;
        }
        self.journal_mode = journal_mode;
        if journal_mode == JournalMode::Wal {
            self.load_latest_wal_state()?;
        }
        self.persist_checksums()
    }

    pub fn checkpoint_wal(&mut self) -> Result<()> {
        self.check_error_state()?;
        if self.journal_mode != JournalMode::Wal {
            return Ok(());
        }
        if self.transaction.is_some() {
            return Err(crate::error::HematiteError::StorageError(
                "Cannot checkpoint WAL during an active transaction".to_string(),
            ));
        }
        if !self.can_checkpoint_wal()? {
            return Err(crate::error::HematiteError::StorageError(
                "Cannot checkpoint WAL while readers are active".to_string(),
            ));
        }
        self.checkpoint_wal_guarded()
    }

    /// Checkpoints once the WAL has grown past [`WAL_AUTOCHECKPOINT_FRAMES`].
    ///
    /// Without this the WAL never shrinks. Nothing called `checkpoint_wal` unless the
    /// embedding application did, so a long-running writer grew the file without
    /// bound — and every reader whose snapshot went stale re-read and re-decoded the
    /// whole thing to rebuild its page index.
    ///
    /// The commit is already durable by the time this runs, so its result is not the
    /// commit's to report — returning an error here would fail a transaction that
    /// succeeded. Two different things are therefore swallowed, and they are not
    /// equally harmless:
    ///
    /// - *Preconditions.* A reader still on an old snapshot, or another active
    ///   writer, means this is simply not the moment. Nothing happens and the next
    ///   commit past the threshold tries again.
    /// - *A checkpoint that started and failed.* `checkpoint_wal_guarded` puts the
    ///   pager into its error state, and that is left in place deliberately: a
    ///   half-backfilled database with the WAL still present leaves the in-memory
    ///   state untrustworthy even though a reopen would recover. The commit still
    ///   reports success, because it succeeded, and the next operation fails loudly.
    ///   Only an I/O failure gets here, at which point the database is in trouble
    ///   regardless.
    pub(super) fn autocheckpoint_wal_if_needed(&mut self) {
        if self.journal_mode != JournalMode::Wal || self.transaction.is_some() {
            return;
        }

        let frames = match self.wal_frame_count {
            Some(frames) => frames,
            None => {
                // First commit since the WAL's size became unknown: ask the
                // filesystem once and remember, rather than every commit.
                let frames = self
                    .wal_path
                    .as_ref()
                    .and_then(|path| std::fs::metadata(path).ok())
                    .map(|metadata| {
                        metadata.len()
                            / (crate::storage::wal::WAL_FRAME_PREFIX_SIZE
                                + crate::storage::PAGE_SIZE) as u64
                    })
                    .unwrap_or(0);
                self.wal_frame_count = Some(frames);
                frames
            }
        };
        if frames < WAL_AUTOCHECKPOINT_FRAMES {
            return;
        }

        if !matches!(self.can_checkpoint_wal(), Ok(true)) {
            return;
        }
        let _ = self.checkpoint_wal_guarded();
    }

    pub(super) fn can_checkpoint_wal(&self) -> Result<bool> {
        if self.database_identity.is_none() {
            return Ok(true);
        }

        if self.lock_mode != PagerLockMode::Write && self.wal_writer_active()? {
            return Ok(false);
        }

        let active_sequences = self.active_wal_reader_sequences()?;
        if active_sequences.is_empty() {
            return Ok(true);
        }

        let latest_sequence = self
            .latest_wal_state
            .as_ref()
            .map(|state| state.visible_sequence)
            .unwrap_or(0);
        Ok(active_sequences
            .into_iter()
            .all(|sequence| sequence == latest_sequence))
    }

    pub(super) fn checkpoint_wal_unlocked(&mut self) -> Result<()> {
        let Some(state) = self.latest_wal_state.clone() else {
            self.remove_wal_file()?;
            return Ok(());
        };

        self.file_manager.restore_file_len(state.file_len)?;
        self.file_manager.set_free_pages(state.free_pages.clone());
        self.file_manager.compact_free_pages()?;

        // Collect overridden page IDs, then read each page's data.
        // We get mutable access through Arc::make_mut for LRU cache updates.
        let overridden_pages: Vec<PageId> = state.overridden_page_ids().collect();
        let state_mut = Arc::make_mut(self.latest_wal_state.as_mut().unwrap());
        for page_id in overridden_pages {
            if let Some(data) = state_mut.read_page_data(page_id)? {
                let page = Page::from_bytes(page_id, data)?;
                self.file_manager.write_page(&page)?;
            }
        }

        self.file_manager.flush()?;
        self.latest_wal_state = None;
        self.latest_wal_file_stamp = None;
        self.wal_read_snapshot = None;
        // The WAL file is about to be removed, so it holds no frames.
        self.wal_frame_count = Some(0);
        self.cache_mut()?.reset();
        self.remove_wal_file()?;
        self.persist_checksums()
    }
}
