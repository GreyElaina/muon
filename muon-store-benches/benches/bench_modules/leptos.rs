//! Leptos reactive baselines.
//!
//! Measures hand-split `RwSignal` as a lower-bound comparison against
//! muon-store's whole-state approach.
//!
//! The hand-split approach represents what you'd write if you manually
//! decomposed state into individual signals — the minimum overhead achievable
//! without a store layer.
//!
//! **Note**: Subscribers are registered via `Track::track()` rather than
//! through effects, because effect reruns are async and not in the hot path
//! for synchronous signal operations.

use criterion::{black_box, measurement::WallTime, BenchmarkGroup, Criterion};
use reactive_graph::signal::RwSignal;
use reactive_graph::traits::{Get, GetUntracked, Set, Track};

use crate::types::*;

// ── Hand-split RwSignal field benchmarks ───────────────────────────────

pub fn bench_leptos(c: &mut Criterion) {
    let mut group = c.benchmark_group("leptos");

    rw_untracked_read(&mut group);
    rw_tracked_read(&mut group);
    rw_write_no_subscribers(&mut group);
    rw_write_with_subscribers(&mut group);
    rw_nested_hand_split(&mut group);
    bench_leptos_local(&mut group);

    group.finish();
}

fn rw_untracked_read(group: &mut BenchmarkGroup<WallTime>) {
    let signal = RwSignal::new(42_i32);

    group.bench_function("untracked_read", |b| {
        b.iter(|| black_box(signal.get_untracked()))
    });
}

fn rw_tracked_read(group: &mut BenchmarkGroup<WallTime>) {
    let signal = RwSignal::new(42_i32);

    group.bench_function("tracked_read", |b| b.iter(|| black_box(signal.get())));
}

fn rw_write_no_subscribers(group: &mut BenchmarkGroup<WallTime>) {
    let signal = RwSignal::new(0_i32);

    group.bench_function("write_no_subscribers", |b| {
        b.iter(|| signal.set(black_box(99)))
    });
}

fn rw_write_with_subscribers(group: &mut BenchmarkGroup<WallTime>) {
    for &n_subs in SUBSCRIBER_COUNTS {
        if n_subs == 0 {
            continue;
        }

        let signal = RwSignal::new(0_i32);

        // Register N subscribers by tracking the signal
        let _subscribers: Vec<_> = (0..n_subs)
            .map(|_| {
                let s = signal;
                s.track();
                s
            })
            .collect();

        group.bench_function(format!("write_with_subscribers/{n_subs}"), |b| {
            b.iter(|| signal.set(black_box(99)))
        });
    }
}

fn rw_nested_hand_split(group: &mut BenchmarkGroup<WallTime>) {
    // Hand-split nested state: each field gets its own RwSignal
    let first_name = RwSignal::new(String::from("Alice"));
    let _last_name = RwSignal::new(String::from("Smith"));
    let count = RwSignal::new(0_i32);

    // Read benchmark: access the nested field
    group.bench_function("hand_split_nested_read", |b| {
        b.iter(|| black_box(first_name.get()))
    });

    // Write benchmark: write the nested field
    group.bench_function("hand_split_nested_write", |b| {
        b.iter(|| first_name.set(black_box("Ben".into())))
    });

    // Sibling isolation: write first_name while count has a subscriber
    {
        let _sub = {
            count.track();
            count
        };

        group.bench_function("hand_split_sibling_isolation", |b| {
            b.iter(|| first_name.set(black_box("Charlie".into())))
        });
    }
}

// ── RwSignal with Rc<RefCell> equivalent (LocalStorage comparison) ─────

pub fn bench_leptos_local(group: &mut BenchmarkGroup<WallTime>) {
    // RwSignal::new_local — uses Rc<RefCell> internally
    let signal = RwSignal::new_local(42_i32);
    group.bench_function("leptos-local/tracked_read", |b| {
        b.iter(|| black_box(signal.get()))
    });
    group.bench_function("leptos-local/write_no_subscribers", |b| {
        b.iter(|| signal.set(black_box(99)))
    });
}
