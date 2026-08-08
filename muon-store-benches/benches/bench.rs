//! muon-store benchmark suite entry point.
//!
//! Runs all benchmark groups: muon-store, Leptos baseline, Sycamore baseline.

use criterion::{criterion_group, criterion_main, Criterion};

#[path = "bench_modules/leptos.rs"]
mod leptos;
#[path = "bench_modules/muon_store.rs"]
mod muon_store;
#[path = "bench_modules/muon_sync.rs"]
mod muon_sync;
#[path = "bench_modules/sycamore.rs"]
mod sycamore;
#[path = "bench_modules/types.rs"]
mod types;

fn all_benchmarks(c: &mut Criterion) {
    muon_store::bench_field(c);
    muon_sync::bench_sync(c);
    muon_sync::bench_seq_engine(c);
    muon_sync::bench_seq_sync(c);
    leptos::bench_leptos(c);
    sycamore::bench_sycamore(c);
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(100)
        .warm_up_time(std::time::Duration::from_secs(2))
        .measurement_time(std::time::Duration::from_secs(5))
        .significance_level(0.01)
        .noise_threshold(0.05);
    targets = all_benchmarks
}

criterion_main!(benches);
