use super::{JournalMode, Pager, PagerTransaction, WalFileStamp};
use crate::error::Result;
use crate::storage::journal::{
    append_journal_record, initialize_journal_file, write_commit_intents,
    write_journal_record_count, JournalRecord, JournalState, RollbackJournal,
};
use crate::storage::pager_metadata::PersistedPagerState;
use crate::storage::wal::{
    append_committed_frames_to_path, load_visible_state_from_path_with_base, VisibleWalState,
    WalFrame,
};
use crate::storage::{
    file_len_for_next_page_id, metadata_page, Page, PageId, PAGE_SIZE, STORAGE_METADATA_PAGE_ID,
};
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::storage::USABLE_PAGE_SIZE;

impl Pager {
    pub(super) fn journal_path(db_path: &Path) -> PathBuf {
        let mut file_name = db_path
            .file_name()
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("hematite.db"));
        file_name.push(".journal");
        match db_path.parent() {
            Some(parent) => parent.join(file_name),
            None => PathBuf::from(file_name),
        }
    }

    pub(super) fn wal_path(db_path: &Path) -> PathBuf {
        let mut file_name = db_path
            .file_name()
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("hematite.db"));
        file_name.push(".wal");
        match db_path.parent() {
            Some(parent) => parent.join(file_name),
            None => PathBuf::from(file_name),
        }
    }

    fn read_persisted_state_page(&mut self) -> Result<Option<Page>> {
        self.file_manager.sync_with_disk()?;
        if STORAGE_METADATA_PAGE_ID >= self.file_manager.next_page_id() {
            return Ok(None);
        }
        Ok(Some(self.file_manager.read_page(STORAGE_METADATA_PAGE_ID)?))
    }

    /// Whether the persisted state page on disk still matches the copy in the cache.
    ///
    /// Answering this is the entire job of the read at the start of a read scope,
    /// and the answer is almost always yes: a commit writes that page through the
    /// cache, so bytes on disk that differ from the cached copy are bytes this
    /// process did not write.
    ///
    /// Reading it without verifying or parsing it first is what makes that cheap.
    /// A page that compares equal to one this pager itself wrote needs neither: it
    /// is not being trusted, it is being recognised. Only a page that differs goes
    /// on to be checked and applied, and that is the path where another writer's
    /// commit has to be handled anyway.
    ///
    /// Verifying every one of these reads cost an xxHash64 over 4 KiB — 11 % of a
    /// point-lookup workload, on a page that had not changed.
    fn persisted_state_page_matches_cache(&mut self) -> Result<bool> {
        self.file_manager.sync_with_disk()?;
        if STORAGE_METADATA_PAGE_ID >= self.file_manager.next_page_id() {
            return Ok(false);
        }
        let Some(cached) = self.cache_read()?.peek_shared(STORAGE_METADATA_PAGE_ID) else {
            return Ok(false);
        };
        let mut scratch = self.metadata_scratch.take().unwrap_or_else(|| {
            crate::storage::types::page_pool::take_for_overwrite().into_boxed_slice()
        });
        let matches = self
            .file_manager
            .read_page_content_into(STORAGE_METADATA_PAGE_ID, &mut scratch)
            .map(|()| scratch[..USABLE_PAGE_SIZE] == cached.data[..USABLE_PAGE_SIZE])
            .unwrap_or(false);
        self.metadata_scratch = Some(scratch);
        Ok(matches)
    }

    fn apply_persisted_state_page(&mut self, page: Option<&Page>) -> Result<()> {
        if let Some(page) = page {
            if let Some(bytes) = metadata_page::read_pager_metadata(&page.data)? {
                return self.apply_persisted_state(&bytes);
            }
        }

        self.journal_mode = JournalMode::Rollback;
        self.file_manager.set_free_pages(Vec::new());
        Ok(())
    }

    pub(super) fn load_persisted_state(&mut self) -> Result<()> {
        let page = self.read_persisted_state_page()?;
        self.apply_persisted_state_page(page.as_ref())
    }

    fn apply_persisted_state(&mut self, contents: &[u8]) -> Result<()> {
        let persisted =
            PersistedPagerState::decode_bytes(contents, Self::CHECKSUM_METADATA_VERSION)?;
        self.journal_mode = persisted.journal_mode;
        self.file_manager.set_free_pages(persisted.free_pages);
        Ok(())
    }

    pub(super) fn refresh_persisted_view(&mut self) -> Result<()> {
        if self.transaction.is_some() || self.cache_mut()?.dirty_count() != 0 {
            return Ok(());
        }

        if self.journal_mode == JournalMode::Wal
            && self.latest_wal_state.is_some()
            && self.latest_wal_file_stamp == self.current_wal_file_stamp()?
        {
            // The WAL has not moved, so nothing has been committed and the state
            // page cannot have changed either. Asked first because it is the
            // cheaper question: this used to read and verify that 4 KiB page and
            // *then* check the stamp, which is the same page-per-read-scope cost
            // rollback mode was paying.
            return Ok(());
        }

        if self.journal_mode != JournalMode::Wal && self.persisted_state_page_matches_cache()? {
            // Recognised: this is the page we last wrote, so nothing has been
            // committed by anyone since, and there is nothing to verify, parse or
            // apply. The cache stays, and so does everything derived from it.
            self.latest_wal_state = None;
            self.latest_wal_file_stamp = None;
            return Ok(());
        }

        let metadata_page = self.read_persisted_state_page()?;
        self.apply_persisted_state_page(metadata_page.as_ref())?;
        if self.journal_mode != JournalMode::Wal {
            // In rollback mode there is no WAL to version reads, so a fresh read must
            // observe anything another writer committed, and the only safe answer to
            // "did someone else change the file" is to drop every cached page.
            //
            // The question is how to tell someone else's commit from our own. This
            // used to keep a separate fingerprint of the metadata page as last read
            // from disk — but that page is shared with the catalog, whose section
            // carries row counts, so *our* commits changed it too. The fingerprint
            // never matched and the cache was reset at the start of every single
            // transaction: 205 resets over 205 one-row transactions, and 7 of the 9
            // page reads each of those cost.
            //
            // The cached copy of the page is the better witness, and it needs no
            // separate bookkeeping: a commit writes it through the cache, so it is
            // exactly what we last put on disk. Bytes still on disk that differ from
            // it are bytes we did not write. Content only -- the trailer holds a
            // checksum the pager stamps on the way out and never mirrors in memory.
            let unchanged = {
                let cache = self.cache_read()?;
                match (metadata_page.as_ref(), cache.peek(STORAGE_METADATA_PAGE_ID)) {
                    (Some(on_disk), Some(cached)) => {
                        on_disk.data[..USABLE_PAGE_SIZE] == cached.data[..USABLE_PAGE_SIZE]
                    }
                    _ => false,
                }
            };
            if !unchanged {
                // Someone else committed. Drop the cached pages, and tell the
                // layers above that what they derived from this file is stale.
                self.cache_mut()?.reset();
                self.bump_durable_state_token();
            }
            self.latest_wal_state = None;
            self.latest_wal_file_stamp = None;
            return Ok(());
        }
        self.load_latest_wal_state_if_changed(
            metadata_page.as_ref().map(|page| page.data.as_slice()),
        )
    }

    pub(super) fn snapshot_wal_visible_state(&mut self) -> Result<Arc<VisibleWalState>> {
        if let Some(state) = &self.latest_wal_state {
            return Ok(state.clone());
        }

        Ok(Arc::new(VisibleWalState::from_database_state(
            self.logical_file_len()?,
            self.logical_free_pages().to_vec(),
        )))
    }

    pub(super) fn load_latest_wal_state(&mut self) -> Result<()> {
        self.load_latest_wal_state_with_base_metadata(None)
    }

    fn load_latest_wal_state_if_changed(
        &mut self,
        baseline_metadata_page: Option<&[u8]>,
    ) -> Result<()> {
        if self.journal_mode != JournalMode::Wal {
            self.latest_wal_state = None;
            self.latest_wal_file_stamp = None;
            return Ok(());
        }

        let stamp = self.current_wal_file_stamp()?;
        if self.latest_wal_file_stamp == stamp {
            return Ok(());
        }

        self.load_latest_wal_state_with_base_metadata(baseline_metadata_page)
    }

    fn load_latest_wal_state_with_base_metadata(
        &mut self,
        baseline_metadata_page: Option<&[u8]>,
    ) -> Result<()> {
        #[cfg(test)]
        {
            self.wal_visible_state_reload_count =
                self.wal_visible_state_reload_count.saturating_add(1);
        }
        if self.journal_mode != JournalMode::Wal {
            self.latest_wal_state = None;
            self.latest_wal_file_stamp = None;
            return Ok(());
        }

        if self.wal_path.is_none() {
            self.latest_wal_state = None;
            self.latest_wal_file_stamp = None;
            return Ok(());
        }
        // Reloading means the WAL is not what we last saw, so anything derived from
        // the database's contents may be stale. Over-reporting is safe here;
        // under-reporting would hand a caller a stale schema.
        self.bump_durable_state_token();
        let Some(path) = &self.wal_path else {
            unreachable!("checked just above");
        };

        let wal_file_stamp = self.current_wal_file_stamp()?;
        let owned_metadata_page;
        let metadata_page = if let Some(page) = baseline_metadata_page {
            page
        } else {
            owned_metadata_page = self.file_manager.read_page(STORAGE_METADATA_PAGE_ID)?;
            owned_metadata_page.data.as_slice()
        };
        self.latest_wal_state = load_visible_state_from_path_with_base(
            path,
            self.file_manager.file_len()?,
            self.file_manager.free_pages().to_vec(),
            metadata_page,
        )?
        .map(Arc::new);
        self.latest_wal_file_stamp = wal_file_stamp;
        Ok(())
    }

    fn current_wal_file_stamp(&self) -> Result<Option<WalFileStamp>> {
        let Some(path) = &self.wal_path else {
            return Ok(None);
        };

        match fs::metadata(path) {
            Ok(metadata) => Ok(Some(WalFileStamp {
                len: metadata.len(),
                modified: metadata.modified().ok(),
            })),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn refresh_wal_file_stamp_best_effort(&mut self) {
        self.latest_wal_file_stamp = self.current_wal_file_stamp().ok().flatten();
    }

    fn encoded_persisted_state_page(&mut self) -> Result<Page> {
        let free_pages = self.logical_free_pages().to_vec();

        let existing_page = self
            .cache_mut()?
            .peek(STORAGE_METADATA_PAGE_ID)
            .cloned()
            .unwrap_or_else(|| {
                self.file_manager
                    .read_page(STORAGE_METADATA_PAGE_ID)
                    .unwrap_or_else(|_| Page::new(STORAGE_METADATA_PAGE_ID))
            });

        let full_state = PersistedPagerState {
            journal_mode: self.journal_mode,
            free_pages: free_pages.clone(),
        };

        let encoded = full_state.encode(Self::CHECKSUM_METADATA_VERSION);
        let metadata_page_bytes =
            match metadata_page::write_pager_metadata(&existing_page.data, &encoded) {
                Ok(bytes) => bytes,
                Err(err) => return Err(err),
            };

        Page::from_bytes(STORAGE_METADATA_PAGE_ID, metadata_page_bytes)
    }

    pub(super) fn stage_persisted_state_page(&mut self) -> Result<()> {
        let page = self.encoded_persisted_state_page()?;
        self.snapshot_original_page(STORAGE_METADATA_PAGE_ID)?;
        {
            let cache = self.cache_mut()?;
            cache.put(page);
            cache.mark_dirty(STORAGE_METADATA_PAGE_ID);
        }
        if self.active_rollback_transaction().is_some() {
            self.cache_mut()?.mark_need_sync(STORAGE_METADATA_PAGE_ID);
        }
        Ok(())
    }

    pub(super) fn persist_checksums(&mut self) -> Result<()> {
        let page = self.encoded_persisted_state_page()?;
        self.file_manager.write_page(&page)?;
        self.file_manager.flush()?;
        let cache = self.cache_mut()?;
        cache.put(page);
        cache.clear_dirty(STORAGE_METADATA_PAGE_ID);
        Ok(())
    }

    pub(super) fn snapshot_original_page(&mut self, page_id: PageId) -> Result<()> {
        // Two independent questions, and they used to be conflated. The rollback
        // journal needs one pre-image per page per *transaction*; a savepoint needs
        // one per page per *savepoint*. The `journaled_pages` check answered only the
        // first and returned early, so a page written before a savepoint and again
        // after it never reached the capture loop below — which is why savepoints
        // also had to eagerly copy every dirty page when they were created.
        //
        // Only the journal half is rollback-specific. This used to return immediately
        // in WAL mode, which left WAL with no undo log and forced its snapshots to
        // copy every dirty page per row instead.
        let needs_journal_record = self.journal_mode != JournalMode::Wal
            && self
                .active_rollback_transaction()
                .is_some_and(|transaction| !transaction.journaled_pages.contains(&page_id));

        let Some(savepoints) = self.active_savepoints() else {
            return Ok(());
        };
        let needs_savepoint_capture = savepoints.iter().any(|savepoint| {
            page_is_live_at_savepoint(page_id, savepoint)
                && !savepoint.captured_page_ids.contains(&page_id)
        });

        if !needs_journal_record && !needs_savepoint_capture {
            return Ok(());
        }

        // A page past the length the file had when the transaction began has no
        // pre-image on disk. That bound is rollback's; in WAL mode a page's previous
        // version may live in the WAL rather than the file, so the equivalent question
        // is answered per savepoint by `page_is_live_at_savepoint` instead.
        let page_end = (page_id as u64 * PAGE_SIZE as u64).saturating_add(PAGE_SIZE as u64);
        let beyond_original_file = match &self.transaction {
            Some(PagerTransaction::Rollback(transaction)) => {
                page_end > transaction.original_file_len
            }
            Some(PagerTransaction::Wal(_)) => false,
            None => return Ok(()),
        };

        let is_live_somewhere = self.active_savepoints().is_some_and(|savepoints| {
            savepoints
                .iter()
                .any(|savepoint| page_is_live_at_savepoint(page_id, savepoint))
        });

        if beyond_original_file || !(is_live_somewhere || needs_journal_record) {
            // Nothing to preserve, and nothing to record: a page that did not exist
            // at a savepoint is recognised as such at restore time, by the same
            // `page_is_live_at_savepoint` test used here.
            return Ok(());
        }

        // The pre-image: the page as it stands *before* this write, which is why this
        // runs ahead of `cache.put`. In WAL mode the newest version may be a WAL frame
        // rather than the file, so go through the WAL-aware path when the cache misses.
        // Shared, not copied. The copies that this function genuinely owes are the
        // ones below -- one per savepoint that still needs a pre-image, plus one for
        // the journal -- and cloning the cached page here added a fourth 4 KiB memcpy
        // of bytes nobody was going to mutate. A savepoint is taken per row, so that
        // clone alone was 79 MB of memcpy over a 20 000-row insert.
        let cached = self.cache_read()?.peek_shared(page_id);
        let page: Arc<Page> = match cached {
            Some(page) => page,
            None if self.journal_mode == JournalMode::Wal => {
                match self.read_wal_page_data(page_id)? {
                    Some(data) => Arc::new(Page::from_bytes(page_id, data)?),
                    None => Arc::new(self.file_manager.read_page(page_id)?),
                }
            }
            None => Arc::new(self.file_manager.read_page(page_id)?),
        };

        if let Some(savepoints) = self.active_savepoints_mut() {
            for savepoint in savepoints.iter_mut() {
                if !page_is_live_at_savepoint(page_id, savepoint) {
                    continue;
                }
                if savepoint.captured_page_ids.insert(page_id) {
                    savepoint.page_images.push(Arc::clone(&page));
                }
            }
        }

        if !needs_journal_record || self.active_rollback_transaction().is_none() {
            return Ok(());
        }

        crate::storage::stats::PagerStats::bump(&self.stats.undo_page_copies);
        let record = JournalRecord {
            page_id,
            data: page.data.to_vec(),
        };
        {
            let cache = self.cache_mut()?;
            cache.mark_journaled(page_id);
            cache.mark_need_sync(page_id);
        }
        self.journal_needs_sync = true;
        self.append_rollback_journal_record(&record)?;

        // Appended first, then handed over. The record used to be cloned so that the
        // in-memory list could take one copy and the journal writer another -- a
        // second 4 KiB copy of a page that was already copied once above.
        //
        // This also puts the two in a safer order. The file is sized from this
        // vector's length by `sync_rollback_journal_from_transaction`, so a failed
        // append now leaves the vector short and the partial record gets truncated
        // away; pushing first would have claimed a record the file does not hold.
        let Some(transaction) = self.active_rollback_transaction_mut() else {
            return Ok(());
        };
        transaction.journaled_pages.insert(page_id);
        transaction.page_records.push(record);
        Ok(())
    }

    /// Write the journal header and metadata section once at transaction begin.
    /// Keeps the file handle open for incremental record appending.
    pub(super) fn initialize_rollback_journal(&mut self) -> Result<()> {
        let Some(path) = self.journal_path.clone() else {
            return Ok(());
        };
        let Some((original_file_len, original_free_pages)) = self
            .active_rollback_transaction()
            .map(|transaction| {
                (
                    transaction.original_file_len,
                    transaction.original_free_pages.clone(),
                )
            })
        else {
            return Ok(());
        };

        let reuse = self.journal_file.take();
        let (file, header_len) = initialize_journal_file(
            &path,
            original_file_len,
            &original_free_pages,
            &[],
            Arc::clone(&self.stats),
            reuse,
        )?;

        self.journal_file = Some(file);
        self.journal_record_count = 0;
        self.journal_header_len = header_len;
        self.journal_needs_sync = true;

        Ok(())
    }

    /// Append a single page record to the already-open journal file.
    pub(super) fn append_rollback_journal_record(&mut self, record: &JournalRecord) -> Result<()> {
        let Some(file) = &mut self.journal_file else {
            return Ok(());
        };

        append_journal_record(file, record)?;
        self.journal_record_count += 1;

        // Written per record, not deferred to the sync: the decoder requires the
        // header's count to describe the file exactly, and rejects a journal with
        // records beyond it as truncated. Recovery and the tests read the journal
        // mid-transaction, so it has to be decodable at every point. It is one
        // positional write now rather than a seek, four bytes, and a seek back.
        write_journal_record_count(file, self.journal_record_count)?;
        Ok(())
    }

    pub(super) fn sync_rollback_journal(&mut self) -> Result<()> {
        if !self.journal_needs_sync {
            return Ok(());
        }
        let Some(file) = &mut self.journal_file else {
            self.journal_needs_sync = false;
            return Ok(());
        };
        file.sync_all()?;
        crate::storage::stats::PagerStats::bump(&self.stats.fsyncs);
        // BUG-09 fix: only clear need_sync on pages that are already journaled.
        // Pages dirtied after this journal sync started have not had their
        // original images captured yet; clearing need_sync on them would allow
        // them to spill to the main file without journal coverage.
        let dirty_ids = self.cache_mut()?.dirty_page_ids();
        for page_id in dirty_ids {
            let is_journaled = self
                .cache_read()?
                .meta(page_id)
                .map(|m| m.journaled)
                .unwrap_or(false);
            if is_journaled {
                self.cache_mut()?.clear_need_sync(page_id);
            }
        }
        self.journal_needs_sync = false;
        Ok(())
    }

    /// Records what every page this commit touches must contain once it has landed.
    ///
    /// Checksums are computed from the cached pages rather than read out of
    /// `page_checksums`. That map deliberately has no entry for the storage metadata
    /// page, and skipping that page here would be a durability hole: it is written
    /// last and carries the free list, so a commit whose data pages landed but whose
    /// metadata page did not would be judged complete, pairing new data with stale
    /// bookkeeping. Recomputing costs one hash per dirty page against a commit that
    /// is already writing every one of them.
    pub(super) fn declare_rollback_commit_intent(&mut self) -> Result<()> {
        let dirty_ids = self.cache_read()?.dirty_page_ids();
        let mut intents = Vec::with_capacity(dirty_ids.len());
        for page_id in dirty_ids {
            let Some(page) = self.cache_read()?.peek_shared(page_id) else {
                // A dirty page that is not resident cannot be described, so leave the
                // intent empty and let recovery fall back to rolling back.
                return Ok(());
            };
            intents.push((page_id, Self::calculate_page_checksum(&page)));
        }

        let Some(file) = &mut self.journal_file else {
            return Ok(());
        };
        write_commit_intents(file, &intents)?;
        self.journal_needs_sync = true;
        Ok(())
    }

    /// After a savepoint restore, truncate the journal to match the reduced
    /// page-record set and update the header's record count.
    pub(super) fn sync_rollback_journal_from_transaction(&mut self) -> Result<()> {
        if self.journal_mode != JournalMode::Rollback {
            return Ok(());
        }

        if self.active_rollback_transaction().is_some() {
            let new_record_count = self
                .active_rollback_transaction()
                .map(|t| t.page_records.len() as u32)
                .unwrap_or(0);

            if let Some(file) = &mut self.journal_file {
                let record_size = (8 + PAGE_SIZE) as u64;
                let new_file_len = self.journal_header_len + new_record_count as u64 * record_size;
                file.set_len(new_file_len)?;

                write_journal_record_count(file, new_record_count)?;
                file.sync_all()?;
                crate::storage::stats::PagerStats::bump(&self.stats.fsyncs);

                self.journal_record_count = new_record_count;
                self.journal_needs_sync = false;
            }
            Ok(())
        } else {
            self.remove_journal_file()
        }
    }

    /// Retires the journal without giving up the file or the handle.
    ///
    /// The end of a transaction, which is the hot path: one `ftruncate` where
    /// deleting the file cost an `unlink` here and an `openat` plus a truncating
    /// `open` at the next `BEGIN`.
    pub(super) fn retire_journal_file(&mut self) -> Result<()> {
        self.journal_record_count = 0;
        self.journal_header_len = 0;
        self.journal_needs_sync = false;
        match &mut self.journal_file {
            Some(file) => file.retire(),
            // Nothing open: fall back to removing whatever is on disk, since
            // something may have been left there by an earlier process.
            None => self.remove_journal_file(),
        }
    }

    /// Close the journal file handle and remove the file from disk.
    pub(super) fn remove_journal_file(&mut self) -> Result<()> {
        // Close the handle first so the file can be removed cleanly.
        self.journal_file = None;
        self.journal_record_count = 0;
        self.journal_header_len = 0;
        self.journal_needs_sync = false;

        let Some(path) = &self.journal_path else {
            return Ok(());
        };
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    pub(super) fn remove_wal_file(&self) -> Result<()> {
        let Some(path) = &self.wal_path else {
            return Ok(());
        };
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    pub(super) fn recover_if_needed(&mut self) -> Result<()> {
        let Some(path) = &self.journal_path else {
            return Ok(());
        };
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        // An empty file is a retired journal: the transaction that owned it
        // finished, and emptying it is what says so. Distinct from a file with
        // unrecognisable contents, which is corruption and still an error.
        if bytes.is_empty() {
            return Ok(());
        }

        let journal = RollbackJournal::decode(&bytes)?;
        match journal.state {
            JournalState::Active => {
                if self.has_live_writer()? {
                    return Ok(());
                }
                if self.commit_intent_was_fulfilled(&journal)? {
                    // Every page the commit meant to write already holds the contents
                    // it meant to write. The commit reached the disk; replaying the
                    // pre-images here would undo a transaction that completed.
                    self.remove_journal_file()?;
                } else {
                    self.restore_from_journal(&journal)?;
                    self.remove_journal_file()?;
                }
            }
            JournalState::Committed => {
                self.remove_journal_file()?;
            }
        }
        Ok(())
    }

    /// Whether the database already holds everything the commit intended to write.
    ///
    /// This is the oracle that replaced the durable commit marker. A journal with no
    /// intent never got as far as declaring one, so it is by definition unfinished.
    ///
    /// Any page that cannot be read — beyond the file's end, say, because the commit
    /// died before extending it — counts as unfulfilled, which is the safe answer: an
    /// unfinished commit gets rolled back.
    fn commit_intent_was_fulfilled(&self, journal: &RollbackJournal) -> Result<bool> {
        if journal.commit_intents.is_empty() {
            return Ok(false);
        }

        for (page_id, expected_checksum) in &journal.commit_intents {
            let Ok(page) = self.file_manager.read_page(*page_id) else {
                return Ok(false);
            };
            if Self::calculate_page_checksum(&page) != *expected_checksum {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn rollback_from_active_transaction(&mut self) -> Result<()> {
        if self.journal_path.is_some() {
            let Some(path) = &self.journal_path else {
                unreachable!();
            };
            let journal = RollbackJournal::decode(&fs::read(path)?)?;
            return self.restore_from_journal(&journal);
        }

        // Taken, not cloned. Cloning the transaction to read three fields off it
        // copied every 4 KiB pre-image in the undo log, plus the savepoints and page
        // sets, to build a journal that is used once and dropped. The transaction is
        // being rolled back, so it has no use for them either.
        let (original_file_len, original_free_pages, page_records) = {
            let transaction = self.active_rollback_transaction_mut().ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Pager transaction is not active".to_string(),
                )
            })?;
            (
                transaction.original_file_len,
                std::mem::take(&mut transaction.original_free_pages),
                std::mem::take(&mut transaction.page_records),
            )
        };
        let journal = RollbackJournal {
            state: JournalState::Active,
            original_file_len,
            original_free_pages,
            page_records,
            // An in-memory rollback of a live transaction; nothing has been declared
            // committed, so there is nothing for recovery to weigh up.
            commit_intents: Vec::new(),
        };
        self.restore_from_journal(&journal)
    }

    pub(super) fn restore_from_journal(&mut self, journal: &RollbackJournal) -> Result<()> {
        self.cache_mut()?.reset();
        self.file_manager
            .restore_file_len(journal.original_file_len)?;
        self.file_manager
            .set_free_pages(journal.original_free_pages.clone());

        for record in &journal.page_records {
            self.file_manager
                .write_page_data(record.page_id, &record.data)?;
        }
        self.file_manager.flush()?;

        Ok(())
    }

    pub(super) fn rollback_wal_transaction(&mut self) -> Result<()> {
        let _transaction = self.active_wal_transaction().cloned().ok_or_else(|| {
            crate::error::HematiteError::StorageError("Pager transaction is not active".to_string())
        })?;
        self.cache_mut()?.reset();
        Ok(())
    }

    pub(super) fn commit_wal_transaction(&mut self) -> Result<()> {
        let (wal_next_page_id, wal_free_pages) = {
            let transaction = self.active_wal_transaction().ok_or_else(|| {
                crate::error::HematiteError::StorageError(
                    "Pager transaction is not active".to_string(),
                )
            })?;
            (
                transaction.wal_next_page_id,
                transaction.wal_free_pages.clone(),
            )
        };
        let next_sequence = self
            .latest_wal_state
            .as_ref()
            .map(|state| state.visible_sequence + 1)
            .unwrap_or(1);

        let mut page_ids = self.cache_read()?.dirty_page_ids();
        page_ids.sort_unstable();

        let mut frames = Vec::with_capacity(page_ids.len());
        for page_id in page_ids {
            let mut page = self.cache_read()?.peek(page_id).cloned().ok_or_else(|| {
                crate::error::HematiteError::StorageError(format!(
                    "Dirty page {} missing from page cache",
                    page_id
                ))
            })?;
            // The frame carries the image a reader will verify, so the trailer has to
            // be in it. `write_page` stamps as it writes and leaves memory alone; here
            // there is no later write to stamp at until a checkpoint.
            crate::storage::file_manager::stamp_page_trailer(&mut page);
            frames.push(WalFrame::new(page_id, page.take_data()));
        }
        let metadata_page = self
            .cache_read()?
            .peek(STORAGE_METADATA_PAGE_ID)
            .cloned()
            .unwrap_or(self.file_manager.read_page(STORAGE_METADATA_PAGE_ID)?);
        let frame_offsets = if let Some(path) = &self.wal_path {
            append_committed_frames_to_path(
                path,
                next_sequence,
                wal_next_page_id,
                &wal_free_pages,
                &metadata_page.data,
                &frames,
            )
            .inspect(|offsets| {
                // One fsync per WAL commit, plus the frames just appended.
                crate::storage::stats::PagerStats::bump(&self.stats.fsyncs);
                crate::storage::stats::PagerStats::add(&self.stats.wal_frames, frames.len() as u64);
                // `offsets` covers the whole batch, including the synthesized
                // metadata frame, so it is the count the file actually grew by.
                if let Some(count) = self.wal_frame_count.as_mut() {
                    *count += offsets.len() as u64;
                }
            })?
        } else {
            std::collections::HashMap::new()
        };
        let mut updated_visible_state = if let Some(state) = &self.latest_wal_state {
            state.as_ref().clone()
        } else {
            VisibleWalState::from_database_state(
                self.file_manager.file_len()?,
                self.file_manager.free_pages().to_vec(),
            )
        };
        updated_visible_state.apply_committed_delta(
            next_sequence,
            file_len_for_next_page_id(wal_next_page_id),
            wal_free_pages.clone(),
            frames,
            &frame_offsets,
        )?;
        // Ensure the WAL path is set so on-demand page reads can access the file
        updated_visible_state.wal_path = self.wal_path.clone();
        self.latest_wal_state = Some(Arc::new(updated_visible_state));
        self.refresh_wal_file_stamp_best_effort();
        let committed_view_token = self.cache_view_token();
        let dirty_ids = self.cache_mut()?.dirty_page_ids();
        for page_id in dirty_ids {
            let cache = self.cache_mut()?;
            cache.clear_dirty(page_id);
            cache.set_view_token(page_id, committed_view_token);
        }
        Ok(())
    }
}

/// Whether `page_id` existed and was allocated when `savepoint` was taken.
///
/// A page that did not is one the transaction created afterwards: it has no
/// pre-image, and rolling back to the savepoint should drop it rather than restore
/// anything.
pub(super) fn page_is_live_at_savepoint_for_restore(
    page_id: PageId,
    savepoint: &super::PagerSavepoint,
) -> bool {
    page_is_live_at_savepoint(page_id, savepoint)
}

fn page_is_live_at_savepoint(page_id: PageId, savepoint: &super::PagerSavepoint) -> bool {
    page_id < savepoint.next_page_id && !savepoint.free_page_set.contains(&page_id)
}
