use core::convert::Infallible;
use core::sync::atomic::{AtomicU64, Ordering};
use std::cell::RefCell;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use kernel::{Change, Here, Path, QuasiObserver, Query, Replace};

#[derive(Default)]
struct Sink {
    events: usize,
}

impl<T: ?Sized, Before: ?Sized> Replace<T, Before> for Sink {
    type Error = Infallible;

    fn replace(
        &mut self,
        path: &Path<'_>,
        before: Option<&Before>,
        after: &T,
    ) -> Result<(), Self::Error> {
        black_box(path);
        black_box(before);
        black_box(after);
        self.events += 1;
        Ok(())
    }
}

impl<'a, T: ?Sized, Before: ?Sized> Query<Change<'a, T, Before>, Here> for Sink {
    type Output = Self;

    fn query(&mut self) -> &mut Self::Output {
        self
    }
}

#[derive(kernel::Observe)]
struct Wide {
    a: u64,
    b: u64,
    c: u64,
    d: u64,
    e: u64,
    f: u64,
    g: u64,
    h: u64,
}

impl Wide {
    const fn new() -> Self {
        Self {
            a: 0,
            b: 0,
            c: 0,
            d: 0,
            e: 0,
            f: 0,
            g: 0,
            h: 0,
        }
    }

    fn increment_all(&mut self) {
        self.a += 1;
        self.b += 1;
        self.c += 1;
        self.d += 1;
        self.e += 1;
        self.f += 1;
        self.g += 1;
        self.h += 1;
    }
}

fn scalar(c: &mut Criterion) {
    let mut group = c.benchmark_group("scalar_increment");

    let mut direct = 0_u64;
    group.bench_function("direct", |bencher| {
        bencher.iter(|| {
            let value = black_box(&mut direct);
            *value = value.wrapping_add(1);
            black_box(*value)
        });
    });

    let mut observed_once = 0_u64;
    let mut once_sink = Sink::default();
    group.bench_function("collect", |bencher| {
        bencher.iter(|| {
            let result: Result<(), Infallible> = kernel::collect(
                black_box(&mut observed_once),
                kernel::tracked!(|value| value = value.wrapping_add(1)),
                black_box(&mut once_sink),
            );
            result.unwrap();
            black_box((observed_once, once_sink.events))
        });
    });

    let mut retained = kernel::Observed::new(0_u64);
    let mut retained_sink = Sink::default();
    group.bench_function("observed_collect_with", |bencher| {
        bencher.iter(|| {
            let result: Result<(), kernel::ObserverError<Infallible>> = black_box(&mut retained)
                .collect_with(
                    kernel::tracked!(|value| value = value.wrapping_add(1)),
                    black_box(&mut retained_sink),
                );
            result.unwrap();
            black_box(retained_sink.events)
        });
    });

    group.finish();
}

fn wide(c: &mut Criterion) {
    let mut one = c.benchmark_group("wide_one_field");

    let mut direct = Wide::new();
    one.bench_function("direct", |bencher| {
        bencher.iter(|| {
            let value = black_box(&mut direct);
            value.a = value.a.wrapping_add(1);
            black_box(value.a)
        });
    });

    let mut observed_once = Wide::new();
    let mut once_sink = Sink::default();
    one.bench_function("collect", |bencher| {
        bencher.iter(|| {
            let result: Result<(), Infallible> = kernel::collect(
                black_box(&mut observed_once),
                kernel::tracked!(|value| value.a = value.a.wrapping_add(1)),
                black_box(&mut once_sink),
            );
            result.unwrap();
            black_box((observed_once.a, once_sink.events))
        });
    });

    let mut retained = kernel::Observed::new(Wide::new());
    let mut retained_sink = Sink::default();
    one.bench_function("observed_collect_with", |bencher| {
        bencher.iter(|| {
            let result: Result<(), kernel::ObserverError<Infallible>> = black_box(&mut retained)
                .collect_with(
                    kernel::tracked!(|value| value.a = value.a.wrapping_add(1)),
                    black_box(&mut retained_sink),
                );
            result.unwrap();
            black_box(retained_sink.events)
        });
    });
    one.finish();

    let mut all = c.benchmark_group("wide_all_fields");

    let mut direct = Wide::new();
    all.bench_function("direct", |bencher| {
        bencher.iter(|| {
            let direct = black_box(&mut direct);
            direct.increment_all();
            black_box((
                direct.a, direct.b, direct.c, direct.d, direct.e, direct.f, direct.g, direct.h,
            ))
        });
    });

    let mut observed_once = Wide::new();
    let mut once_sink = Sink::default();
    all.bench_function("collect", |bencher| {
        bencher.iter(|| {
            let result: Result<(), Infallible> = kernel::collect(
                black_box(&mut observed_once),
                kernel::tracked!(|value| {
                    value.a += 1;
                    value.b += 1;
                    value.c += 1;
                    value.d += 1;
                    value.e += 1;
                    value.f += 1;
                    value.g += 1;
                    value.h += 1;
                }),
                black_box(&mut once_sink),
            );
            result.unwrap();
            black_box(once_sink.events)
        });
    });

    let mut retained = kernel::Observed::new(Wide::new());
    let mut retained_sink = Sink::default();
    all.bench_function("observed_collect_with", |bencher| {
        bencher.iter(|| {
            let result: Result<(), kernel::ObserverError<Infallible>> = black_box(&mut retained)
                .collect_with(
                    kernel::tracked!(|value| {
                        value.a += 1;
                        value.b += 1;
                        value.c += 1;
                        value.d += 1;
                        value.e += 1;
                        value.f += 1;
                        value.g += 1;
                        value.h += 1;
                    }),
                    black_box(&mut retained_sink),
                );
            result.unwrap();
            black_box(retained_sink.events)
        });
    });

    all.finish();
}

fn interior_mutability(c: &mut Criterion) {
    let mut group = c.benchmark_group("ref_cell_increment");

    let direct = RefCell::new(0_u64);
    group.bench_function("direct", |bencher| {
        bencher.iter(|| {
            let mut value = black_box(&direct).borrow_mut();
            *value = value.wrapping_add(1);
            black_box(*value)
        });
    });

    let mut observed_once = RefCell::new(0_u64);
    let mut once_sink = Sink::default();
    group.bench_function("collect", |bencher| {
        bencher.iter(|| {
            let result: Result<(), Infallible> = kernel::collect(
                black_box(&mut observed_once),
                |observer| {
                    let mut value = observer.borrow_mut();
                    *value.tracked_mut() = value.untracked_ref().wrapping_add(1);
                },
                black_box(&mut once_sink),
            );
            result.unwrap();
            black_box(once_sink.events)
        });
    });

    let mut retained = kernel::Observed::new(RefCell::new(0_u64));
    let mut retained_sink = Sink::default();
    group.bench_function("observed_collect_with", |bencher| {
        bencher.iter(|| {
            let result: Result<(), kernel::ObserverError<Infallible>> = black_box(&mut retained)
                .collect_with(
                    |observer| {
                        let mut value = observer.borrow_mut();
                        *value.tracked_mut() = value.untracked_ref().wrapping_add(1);
                    },
                    black_box(&mut retained_sink),
                );
            result.unwrap();
            black_box(retained_sink.events)
        });
    });

    group.finish();
}

fn atomic(c: &mut Criterion) {
    let mut group = c.benchmark_group("atomic_increment");

    let direct = AtomicU64::new(0);
    group.bench_function("direct", |bencher| {
        bencher.iter(|| black_box(black_box(&direct).fetch_add(1, Ordering::Relaxed)));
    });

    let mut observed_once = AtomicU64::new(0);
    let mut once_sink = Sink::default();
    group.bench_function("collect", |bencher| {
        bencher.iter(|| {
            let result: Result<(), Infallible> = kernel::collect(
                black_box(&mut observed_once),
                |value| {
                    black_box(value.fetch_add(1, Ordering::Relaxed));
                },
                black_box(&mut once_sink),
            );
            result.unwrap();
            black_box(once_sink.events)
        });
    });

    let mut retained = kernel::Observed::new(AtomicU64::new(0));
    let mut retained_sink = Sink::default();
    group.bench_function("observed_collect_with", |bencher| {
        bencher.iter(|| {
            let result: Result<(), kernel::ObserverError<Infallible>> = black_box(&mut retained)
                .collect_with(
                    |value| {
                        black_box(value.fetch_add(1, Ordering::Relaxed));
                    },
                    black_box(&mut retained_sink),
                );
            result.unwrap();
            black_box(retained_sink.events)
        });
    });

    group.finish();
}

criterion_group!(benches, scalar, wide, interior_mutability, atomic);
criterion_main!(benches);
