//! Storage-layer micro-benchmarks.
//!
//! These measure the primitives the rest of the engine is built on: a page-cache
//! hit, a page write, allocation, and transaction boundaries. They run against the
//! public `Pager` surface so they keep compiling across the storage rewrite, which
//! is what makes a before/after comparison meaningful.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use hematite::storage::{Page, Pager, FIRST_ALLOCATABLE_PAGE_ID, PAGE_SIZE};

mod support;
use support::TempDb;

/// Page cache large enough that nothing in these benches ever evicts.
const CACHE_PAGES: usize = 4096;

/// Build a pager over a file pre-populated with `pages` allocated pages.
fn seeded_pager(db: &TempDb, pages: u32) -> Pager {
    let mut pager = Pager::new(db.path(), CACHE_PAGES).expect("open pager");
    pager.begin_transaction().expect("begin");
    for _ in 0..pages {
        let id = pager.allocate_page().expect("allocate");
        let mut page = Page::new(id);
        page.data[0] = 0xAB;
        pager.write_page(page).expect("write");
    }
    pager.commit_transaction().expect("commit");
    pager
}

fn page_read_hit(c: &mut Criterion) {
    let db = TempDb::new("micro_read_hit");
    let pager = seeded_pager(&db, 64);
    let id = FIRST_ALLOCATABLE_PAGE_ID;

    c.bench_function("page_read_hit", |b| {
        b.iter(|| black_box(pager.read_page(black_box(id)).expect("read")))
    });
}

fn page_read_miss(c: &mut Criterion) {
    let db = TempDb::new("micro_read_miss");
    let pages = 512u32;
    {
        let _seed = seeded_pager(&db, pages);
    }
    // Capacity 1 forces every read to evict and re-fault from the file.
    let pager = Pager::new(db.path(), 1).expect("open pager");
    let mut next = FIRST_ALLOCATABLE_PAGE_ID;

    c.bench_function("page_read_miss", |b| {
        b.iter(|| {
            let page = pager.read_page(black_box(next)).expect("read");
            next += 1;
            if next >= FIRST_ALLOCATABLE_PAGE_ID + pages {
                next = FIRST_ALLOCATABLE_PAGE_ID;
            }
            black_box(page)
        })
    });
}

fn page_write(c: &mut Criterion) {
    let db = TempDb::new("micro_write");
    let mut pager = seeded_pager(&db, 64);
    let id = FIRST_ALLOCATABLE_PAGE_ID;

    c.bench_function("page_write_dirty", |b| {
        b.iter(|| {
            let mut page = Page::new(black_box(id));
            page.data[0] = 0x5A;
            pager.write_page(page).expect("write");
        })
    });
}

fn page_alloc_free(c: &mut Criterion) {
    let db = TempDb::new("micro_alloc");
    let mut pager = seeded_pager(&db, 8);

    c.bench_function("page_alloc_free", |b| {
        b.iter(|| {
            let id = pager.allocate_page().expect("allocate");
            pager.deallocate_page(black_box(id)).expect("deallocate");
        })
    });
}

/// One dirty page per transaction, measured across database sizes.
///
/// The transaction does the same amount of user work at every size, so any growth
/// across sizes is pure per-database-size overhead: today `BEGIN` writes the entire
/// free list and checksum map into the journal, and `COMMIT` re-sorts and
/// re-serialises both.
///
/// A genuinely empty transaction would isolate that overhead better, but the pager
/// rejects `begin_transaction` immediately followed by `commit_transaction` with
/// "Illegal pager state transition from WriterLocked to WriterDbMod" — the state
/// machine has no path for a writer that never dirtied a page. It is not reachable
/// from SQL, because the connection layer skips the pager commit when nothing is
/// pending, so this is an API sharp edge rather than a user-visible bug. The guard
/// types in the rewrite remove the state machine and with it this case.
fn single_page_txn(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_page_txn");
    for pages in [64u32, 1024, 8192] {
        let db = TempDb::new(&format!("micro_1page_{pages}"));
        let mut pager = seeded_pager(&db, pages);
        let id = FIRST_ALLOCATABLE_PAGE_ID;
        group.bench_function(format!("{pages}_pages"), |b| {
            b.iter(|| {
                pager.begin_transaction().expect("begin");
                let mut page = Page::new(id);
                page.data[1] = 0x11;
                pager.write_page(page).expect("write");
                pager.commit_transaction().expect("commit");
            })
        });
    }
    group.finish();
}

/// A bulk transaction: 1000 dirty pages committed once. Isolates per-page write
/// amplification from per-commit fixed cost.
fn bulk_txn(c: &mut Criterion) {
    let db = TempDb::new("micro_bulk");
    let mut pager = seeded_pager(&db, 2048);

    c.bench_function("bulk_txn_1000_pages", |b| {
        b.iter(|| {
            pager.begin_transaction().expect("begin");
            for i in 0..1000u32 {
                let mut page = Page::new(FIRST_ALLOCATABLE_PAGE_ID + i);
                page.data[2] = (i % 251) as u8;
                pager.write_page(page).expect("write");
            }
            pager.commit_transaction().expect("commit");
        })
    });
}

/// Whole-page checksum cost, with the algorithm that was replaced kept alongside
/// the one that replaced it. A 4 KiB page write pays this at least twice in
/// rollback mode (page checksum plus journal record checksum), so it is a direct
/// tax on every write.
///
/// `fnv1a_bytewise` is the original: one multiply per byte, in four separate copies
/// of the same loop. It is retained here purely so the swap has a number attached.
fn checksum_4k(c: &mut Criterion) {
    let page = vec![0x5Au8; PAGE_SIZE];

    let mut group = c.benchmark_group("checksum_4k");
    group.throughput(criterion::Throughput::Bytes(PAGE_SIZE as u64));
    group.bench_function("fnv1a_bytewise", |b| {
        b.iter(|| {
            let mut hash: u32 = 0x811C_9DC5;
            for byte in black_box(&page) {
                hash ^= *byte as u32;
                hash = hash.wrapping_mul(0x0100_0193);
            }
            black_box(hash)
        })
    });
    group.bench_function("xxhash64", |b| {
        b.iter(|| black_box(hematite::storage::checksum::hash32(0, black_box(&page))))
    });
    group.finish();
}

criterion_group!(
    micro,
    page_read_hit,
    page_read_miss,
    page_write,
    page_alloc_free,
    single_page_txn,
    bulk_txn,
    checksum_4k,
);
criterion_main!(micro);
