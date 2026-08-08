//! Sycamore reactive baselines.
//!
//! Measures `Signal` and hand-split reactive state as a comparison
//! against muon-store's whole-state approach.
//!
//! Sycamore effects are synchronous (run immediately on trigger),
//! unlike Leptos which needs async ticks.

use criterion::{black_box, measurement::WallTime, BenchmarkGroup, Criterion};
use sycamore_reactive::{create_effect, create_root, create_signal, Signal};

use crate::types::*;

// ── Signal field benchmarks ────────────────────────────────────────────

pub fn bench_sycamore(c: &mut Criterion) {
    let mut group = c.benchmark_group("sycamore");

    read_untracked(&mut group);
    read_tracked(&mut group);
    write_no_subscribers(&mut group);
    write_with_subscribers(&mut group);
    nested_hand_split(&mut group);

    group.finish();
}

// NOTE: Sycamore's `signal.get()` only establishes a reactive
// dependency when called inside an effect/memo context.  Outside
// one, `track()` is a no-op and the raw get cost (~3 ns) does NOT
// represent the true reactive-read cost.
//
// The honest number is hidden inside `write_with_subscribers/1`:
// Sycamore reruns effects synchronously, so every `signal.set()`
// includes one tracked read via the effect rerun.  53 ns total
// minus ~17 ns for the raw write ≈ 36 ns for the tracked read.

fn read_untracked(group: &mut BenchmarkGroup<WallTime>) {
    let _root = create_root(|| {
        let signal: Signal<i32> = create_signal(42);
        group.bench_function("read_untracked", |b| b.iter(|| black_box(signal.get())));
    });
}

fn read_tracked(group: &mut BenchmarkGroup<WallTime>) {
    // Same measurement as untracked — outside effect, no tracking.
    // Kept for symmetry but the number is NOT comparable to
    // muon-store's or Leptos's tracked_read.
    let _root = create_root(|| {
        let signal: Signal<i32> = create_signal(42);
        group.bench_function("read_tracked", |b| b.iter(|| black_box(signal.get())));
    });
}

fn write_no_subscribers(group: &mut BenchmarkGroup<WallTime>) {
    let _root = create_root(|| {
        let signal: Signal<i32> = create_signal(0);

        group.bench_function("write_no_subscribers", |b| {
            b.iter(|| {
                signal.set(black_box(99));
            })
        });
    });
}

fn write_with_subscribers(group: &mut BenchmarkGroup<WallTime>) {
    for &n_subs in SUBSCRIBER_COUNTS {
        if n_subs == 0 {
            continue;
        }
        if n_subs > 100 {
            continue;
        }

        group.bench_function(format!("write_with_subscribers/{n_subs}"), |b| {
            let _root = create_root(|| {
                let signal: Signal<i32> = create_signal(0);
                let _effects: Vec<_> = (0..n_subs)
                    .map(|_| {
                        let s = signal;
                        create_effect(move || {
                            black_box(s.get());
                        })
                    })
                    .collect();
                b.iter(|| {
                    signal.set(black_box(99));
                })
            });
        });
    }
}

fn nested_hand_split(group: &mut BenchmarkGroup<WallTime>) {
    let _root = create_root(|| {
        let first_name: Signal<String> = create_signal(String::from("Alice"));
        let _last_name: Signal<String> = create_signal(String::from("Smith"));
        let count: Signal<i32> = create_signal(0);

        // Read benchmark
        group.bench_function("hand_split_nested_read", |b| {
            b.iter(|| black_box(first_name.get_clone()))
        });

        // Write benchmark
        group.bench_function("hand_split_nested_write", |b| {
            b.iter(|| {
                first_name.set(black_box("Ben".into()));
            })
        });

        // Sibling isolation
        {
            create_effect(move || {
                black_box(count.get());
            });

            group.bench_function("hand_split_sibling_isolation", |b| {
                b.iter(|| {
                    first_name.set(black_box("Charlie".into()));
                })
            });
        }
    });
}
