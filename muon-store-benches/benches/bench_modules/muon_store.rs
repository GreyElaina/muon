use criterion::{black_box, Criterion};
use muon::PathSegment;
use muon_reactivity::{Field, ReactiveStore};
use muon_store::{track, Store, StorePath};
use muon_sync::{SyncChannel, TransactionQueue};
use std::mem::offset_of;
use std::sync::{Arc, Mutex};

use crate::types::*;

// Helper: write via track! + commit.
fn write_small(store: &Store<Small>, v: i32) {
    track!(store, |s| s.value = v).commit();
}
fn write_nested(store: &Store<NestedRoot>, name: &str) {
    track!(store, |s| s.profile.first_name = name.to_owned()).commit();
}
// Helper: write through the sync pipeline (tracked write → queue → publish).
fn synced_write_small(channel: &SyncChannel, store: &Store<Small>, v: i32) {
    channel
        .sync_write(track!(store, |s| s.value = v))
        .expect("sync write must succeed");
}

pub fn bench_field(c: &mut Criterion) {
    let mut g = c.benchmark_group("muon-store/field");

    // Untracked read
    g.bench_function("untracked_read", |b| {
        let store = ReactiveStore::new(Small { value: 42 });
        let f = store.value();
        b.iter(|| black_box(f.get_untracked()))
    });

    // Tracked read
    g.bench_function("tracked_read", |b| {
        let store = ReactiveStore::new(Small { value: 42 });
        let f = store.value();
        b.iter(|| black_box(f.get()))
    });

    // Write
    g.bench_function("write", |b| {
        let s = Store::new(Small { value: 42 });
        b.iter(|| write_small(&s, 99));
    });

    // Sync write: full pipeline cost of the sync capture path (materialize
    // mutation tree + serialize base + build transactions + enqueue), all
    // under the store write lock. Each iteration writes a fresh value so a
    // real mutation is produced; the queue is drained every 1024 writes to
    // bound memory.
    g.bench_function("write_synced", |b| {
        let store = Store::new(Small { value: 42 });
        let queue = Arc::new(Mutex::new(TransactionQueue::new(1)));
        let channel = SyncChannel::new(queue.clone(), "small");
        let mut next = 0i32;
        b.iter(|| {
            next = next.wrapping_add(1);
            synced_write_small(&channel, &store, next);
            if next & 1023 == 0 {
                queue.lock().unwrap().collect();
            }
        });
    });

    // Nested read (manual Field construction)
    g.bench_function("nested_read", |b| {
        let store = ReactiveStore::new(NestedRoot::default());
        let path: StorePath = [
            PathSegment::String("profile".into()),
            PathSegment::String("first_name".into()),
        ]
        .into_iter()
        .collect();
        let offset = offset_of!(NestedRoot, profile) + offset_of!(Profile, first_name);
        // SAFETY: `offset` is the sum of two `offset_of!` results, so it is
        // the exact byte offset of `first_name` inside `NestedRoot`, and
        // `NestedRoot` is not `#[repr(packed)]`.
        let f: Field<NestedRoot, String> =
            unsafe { Field::new(path, offset, store.core().clone(), store.triggers().clone()) };
        b.iter(|| black_box(f.get()))
    });

    // Nested write
    g.bench_function("nested_write", |b| {
        let s = Store::new(NestedRoot::default());
        b.iter(|| write_nested(&s, "Ben"));
    });

    g.finish();
}
