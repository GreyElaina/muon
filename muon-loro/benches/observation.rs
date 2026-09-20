use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use loro::{Container, LoroDoc, LoroText, ValueOrContainer};
use muon_loro::{Context, List, Map, Text};

const SESSIONS: usize = 32;
const CHILDREN: usize = 32;

fn text(c: &mut Criterion) {
    let mut group = c.benchmark_group("text_32_sessions");

    group.bench_function("direct", |bencher| {
        bencher.iter_batched(
            || {
                let doc = LoroDoc::new();
                let text = doc.get_text("model");
                text.insert(0, "seed").unwrap();
                doc.commit();
                (doc, text)
            },
            |(doc, text)| {
                for _ in 0..SESSIONS {
                    black_box(&text).insert(text.len_unicode(), "x").unwrap();
                    black_box(&doc).commit();
                }
                black_box(text.len_unicode())
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("collect", |bencher| {
        bencher.iter_batched(
            || {
                let doc = LoroDoc::new();
                let root = doc.get_text("model");
                let context = Context::new(root).unwrap();
                let model = Text::from("seed");
                context.initialize(&model).unwrap();
                doc.commit();
                (doc, context, model)
            },
            |(doc, mut context, mut model)| {
                for _ in 0..SESSIONS {
                    context
                        .collect(
                            black_box(&mut model),
                            kernel::tracked!(|text| text.push_str("x")),
                        )
                        .unwrap();
                    black_box(&doc).commit();
                }
                black_box(model.len())
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("observed", |bencher| {
        bencher.iter_batched(
            || {
                let doc = LoroDoc::new();
                let root = doc.get_text("model");
                let context = Context::new(root).unwrap();
                let model = Text::from("seed");
                context.initialize(&model).unwrap();
                doc.commit();
                (doc, context, kernel::Observed::new(model))
            },
            |(doc, mut context, mut model)| {
                for _ in 0..SESSIONS {
                    let result: Result<_, kernel::ObserverError<muon_loro::Error>> = model
                        .collect_with(
                            kernel::tracked!(|text| text.push_str("x")),
                            black_box(&mut context),
                        );
                    result.unwrap();
                    black_box(&doc).commit();
                }
                black_box(model.into_inner().len())
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn list(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_32_sessions");

    group.bench_function("direct", |bencher| {
        bencher.iter_batched(
            || {
                let doc = LoroDoc::new();
                let list = doc.get_list("model");
                doc.commit();
                (doc, list)
            },
            |(doc, list)| {
                for value in 0..SESSIONS {
                    black_box(&list).push(value as i64).unwrap();
                    black_box(&doc).commit();
                }
                black_box(list.len())
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("collect", |bencher| {
        bencher.iter_batched(
            || {
                let doc = LoroDoc::new();
                let root = doc.get_list("model");
                let context = Context::new(root).unwrap();
                let model = List::<u64>::new();
                context.initialize(&model).unwrap();
                doc.commit();
                (doc, context, model)
            },
            |(doc, mut context, mut model)| {
                for value in 0..SESSIONS {
                    context
                        .collect(
                            black_box(&mut model),
                            kernel::tracked!(|list| list.push(value as u64)),
                        )
                        .unwrap();
                    black_box(&doc).commit();
                }
                black_box(model.len())
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("observed", |bencher| {
        bencher.iter_batched(
            || {
                let doc = LoroDoc::new();
                let root = doc.get_list("model");
                let context = Context::new(root).unwrap();
                let model = List::<u64>::new();
                context.initialize(&model).unwrap();
                doc.commit();
                (doc, context, kernel::Observed::new(model))
            },
            |(doc, mut context, mut model)| {
                for value in 0..SESSIONS {
                    let result: Result<_, kernel::ObserverError<muon_loro::Error>> = model
                        .collect_with(
                            kernel::tracked!(|list| list.push(value as u64)),
                            black_box(&mut context),
                        );
                    result.unwrap();
                    black_box(&doc).commit();
                }
                black_box(model.into_inner().len())
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn map(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_32_sessions");

    group.bench_function("direct", |bencher| {
        bencher.iter_batched(
            || {
                let doc = LoroDoc::new();
                let map = doc.get_map("model");
                let keys: Vec<_> = (0..SESSIONS).map(|i| format!("key-{i}")).collect();
                doc.commit();
                (doc, map, keys)
            },
            |(doc, map, keys)| {
                for (value, key) in keys.iter().enumerate() {
                    black_box(&map).insert(key, value as i64).unwrap();
                    black_box(&doc).commit();
                }
                black_box(map.len())
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("collect", |bencher| {
        bencher.iter_batched(
            || {
                let doc = LoroDoc::new();
                let root = doc.get_map("model");
                let context = Context::new(root).unwrap();
                let model = Map::<u64>::new();
                let keys: Vec<_> = (0..SESSIONS).map(|i| format!("key-{i}")).collect();
                context.initialize(&model).unwrap();
                doc.commit();
                (doc, context, model, keys)
            },
            |(doc, mut context, mut model, keys)| {
                for (value, key) in keys.into_iter().enumerate() {
                    context
                        .collect(
                            black_box(&mut model),
                            kernel::tracked!(|map| map.insert(key, value as u64)),
                        )
                        .unwrap();
                    black_box(&doc).commit();
                }
                black_box(model.len())
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("observed", |bencher| {
        bencher.iter_batched(
            || {
                let doc = LoroDoc::new();
                let root = doc.get_map("model");
                let context = Context::new(root).unwrap();
                let model = Map::<u64>::new();
                let keys: Vec<_> = (0..SESSIONS).map(|i| format!("key-{i}")).collect();
                context.initialize(&model).unwrap();
                doc.commit();
                (doc, context, kernel::Observed::new(model), keys)
            },
            |(doc, mut context, mut model, keys)| {
                for (value, key) in keys.into_iter().enumerate() {
                    let result: Result<_, kernel::ObserverError<muon_loro::Error>> = model
                        .collect_with(
                            kernel::tracked!(|map| map.insert(key, value as u64)),
                            black_box(&mut context),
                        );
                    result.unwrap();
                    black_box(&doc).commit();
                }
                black_box(model.into_inner().len())
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn nested_text(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_text_32_sessions");

    group.bench_function("direct", |bencher| {
        bencher.iter_batched(
            || {
                let doc = LoroDoc::new();
                let list = doc.get_list("model");
                for _ in 0..CHILDREN {
                    let text = list.push_container(LoroText::new()).unwrap();
                    text.insert(0, "seed").unwrap();
                }
                doc.commit();
                (doc, list)
            },
            |(doc, list)| {
                for index in 0..SESSIONS {
                    let Some(ValueOrContainer::Container(Container::Text(text))) =
                        black_box(&list).get(index % CHILDREN)
                    else {
                        unreachable!()
                    };
                    text.insert(text.len_unicode(), "x").unwrap();
                    black_box(&doc).commit();
                }
                black_box(list.len())
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("collect", |bencher| {
        bencher.iter_batched(
            || {
                let doc = LoroDoc::new();
                let root = doc.get_list("model");
                let context = Context::new(root).unwrap();
                let model = List::from(vec![Text::from("seed"); CHILDREN]);
                context.initialize(&model).unwrap();
                doc.commit();
                (doc, context, model)
            },
            |(doc, mut context, mut model)| {
                for index in 0..SESSIONS {
                    context
                        .collect(
                            black_box(&mut model),
                            kernel::tracked!(|list| {
                                list.get_mut(index % CHILDREN).unwrap().push_str("x")
                            }),
                        )
                        .unwrap();
                    black_box(&doc).commit();
                }
                black_box(model.len())
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("observed", |bencher| {
        bencher.iter_batched(
            || {
                let doc = LoroDoc::new();
                let root = doc.get_list("model");
                let context = Context::new(root).unwrap();
                let model = List::from(vec![Text::from("seed"); CHILDREN]);
                context.initialize(&model).unwrap();
                doc.commit();
                (doc, context, kernel::Observed::new(model))
            },
            |(doc, mut context, mut model)| {
                for index in 0..SESSIONS {
                    let result: Result<_, kernel::ObserverError<muon_loro::Error>> = model
                        .collect_with(
                            kernel::tracked!(|list| {
                                list.get_mut(index % CHILDREN).unwrap().push_str("x")
                            }),
                            black_box(&mut context),
                        );
                    result.unwrap();
                    black_box(&doc).commit();
                }
                black_box(model.into_inner().len())
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn nested_hot_text(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_text_hot_child_32_sessions");

    group.bench_function("direct", |bencher| {
        bencher.iter_batched(
            || {
                let doc = LoroDoc::new();
                let list = doc.get_list("model");
                for _ in 0..CHILDREN {
                    let text = list.push_container(LoroText::new()).unwrap();
                    text.insert(0, "seed").unwrap();
                }
                doc.commit();
                (doc, list)
            },
            |(doc, list)| {
                for _ in 0..SESSIONS {
                    let Some(ValueOrContainer::Container(Container::Text(text))) =
                        black_box(&list).get(0)
                    else {
                        unreachable!()
                    };
                    text.insert(text.len_unicode(), "x").unwrap();
                    black_box(&doc).commit();
                }
                black_box(list.len())
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("collect", |bencher| {
        bencher.iter_batched(
            || {
                let doc = LoroDoc::new();
                let root = doc.get_list("model");
                let context = Context::new(root).unwrap();
                let model = List::from(vec![Text::from("seed"); CHILDREN]);
                context.initialize(&model).unwrap();
                doc.commit();
                (doc, context, model)
            },
            |(doc, mut context, mut model)| {
                for _ in 0..SESSIONS {
                    context
                        .collect(
                            black_box(&mut model),
                            kernel::tracked!(|list| list.get_mut(0).unwrap().push_str("x")),
                        )
                        .unwrap();
                    black_box(&doc).commit();
                }
                black_box(model.len())
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("observed", |bencher| {
        bencher.iter_batched(
            || {
                let doc = LoroDoc::new();
                let root = doc.get_list("model");
                let context = Context::new(root).unwrap();
                let model = List::from(vec![Text::from("seed"); CHILDREN]);
                context.initialize(&model).unwrap();
                doc.commit();
                (doc, context, kernel::Observed::new(model))
            },
            |(doc, mut context, mut model)| {
                for _ in 0..SESSIONS {
                    let result: Result<_, kernel::ObserverError<muon_loro::Error>> = model
                        .collect_with(
                            kernel::tracked!(|list| list.get_mut(0).unwrap().push_str("x")),
                            black_box(&mut context),
                        );
                    result.unwrap();
                    black_box(&doc).commit();
                }
                black_box(model.into_inner().len())
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn nested_deferred_collect(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_text_32_edits_one_collect");

    group.bench_function("direct", |bencher| {
        bencher.iter_batched(
            || {
                let doc = LoroDoc::new();
                let list = doc.get_list("model");
                let text = list.push_container(LoroText::new()).unwrap();
                text.insert(0, "seed").unwrap();
                doc.commit();
                (doc, list, text)
            },
            |(doc, list, text)| {
                for _ in 0..SESSIONS {
                    black_box(&text).insert(text.len_unicode(), "x").unwrap();
                }
                black_box(&doc).commit();
                black_box(list.len())
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("collect", |bencher| {
        bencher.iter_batched(
            || {
                let doc = LoroDoc::new();
                let root = doc.get_list("model");
                let context = Context::new(root).unwrap();
                let model = List::from(vec![Text::from("seed")]);
                context.initialize(&model).unwrap();
                doc.commit();
                (doc, context, model)
            },
            |(doc, mut context, mut model)| {
                context
                    .collect(
                        black_box(&mut model),
                        kernel::tracked!(|list| {
                            for _ in 0..SESSIONS {
                                list.get_mut(0).unwrap().push_str("x");
                            }
                        }),
                    )
                    .unwrap();
                black_box(&doc).commit();
                black_box(model.len())
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("observed_edit_then_collect", |bencher| {
        bencher.iter_batched(
            || {
                let doc = LoroDoc::new();
                let root = doc.get_list("model");
                let context = Context::new(root).unwrap();
                let model = List::from(vec![Text::from("seed")]);
                context.initialize(&model).unwrap();
                doc.commit();
                (doc, context, kernel::Observed::new(model))
            },
            |(doc, mut context, mut model)| {
                for _ in 0..SESSIONS {
                    model
                        .edit(kernel::tracked!(|list| {
                            list.get_mut(0).unwrap().push_str("x")
                        }))
                        .unwrap();
                }
                let result: Result<_, kernel::ObserverError<muon_loro::Error>> =
                    model.collect(black_box(&mut context));
                result.unwrap();
                black_box(&doc).commit();
                black_box(model.into_inner().len())
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(
    benches,
    text,
    list,
    map,
    nested_text,
    nested_hot_text,
    nested_deferred_collect
);
criterion_main!(benches);
