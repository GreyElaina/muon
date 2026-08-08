use muon::Observe;
use muon_reactivity::*;
use muon_store::{Track, track};
use reactive_graph::effect::{Effect, ImmediateEffect};
use reactive_graph::owner::Owner;
use serde::Serialize;
use std::mem::offset_of;
use std::sync::{
    Arc as StdArc,
    atomic::{AtomicUsize, Ordering},
};

/// Serializes the reactivity tests: `reactive_graph`'s batching is a
/// process-global (`BATCH`), so a `batch` window in one test would
/// swallow another test's effect notifications. Take this lock at the
/// top of every test. A tokio mutex is used so the guard is `Send` —
/// the async tests hold it across await points.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    TEST_LOCK.blocking_lock()
}

/// The async form of [`test_guard`] for `#[tokio::test]` bodies.
async fn test_guard_async() -> tokio::sync::MutexGuard<'static, ()> {
    TEST_LOCK.lock().await
}

#[derive(Debug, Clone, Serialize, Observe, Track, Reactivity)]
struct TestData {
    name: String,
    count: i32,
}

#[derive(Debug, Clone, Serialize, Observe, Track, Reactivity)]
struct Profile {
    first: String,
    age: i32,
}

#[derive(Debug, Clone, Serialize, Observe, Track, Reactivity)]
struct NestedData {
    profile: Profile,
    count: i32,
}

/// A generic model for the generic-derive regression test. The field type
/// must be readable (Clone) and observable (Observe) — the accessor
/// `Field<GenericData<T>, T>` requires both.
#[derive(Debug, Clone, Serialize, Observe, Track, Reactivity)]
struct GenericData<T: Clone + Serialize + Observe + 'static> {
    value: T,
}

// ── Serde-named model: trigger paths must match muon's mutation paths ──

#[derive(Debug, Clone, Serialize, Observe, Track, Reactivity)]
#[serde(rename_all = "snake_case")]
struct RenamedData {
    user_name: String,
    #[serde(rename = "the_count")]
    count: i32,
}

async fn tick() {
    tokio::time::sleep(std::time::Duration::from_micros(50)).await;
}

// ── Field ─────────────────────────────────────────────────────────────

#[test]
fn field_get() {
    let _test_guard = test_guard();
    let store = ReactiveStore::new(TestData {
        name: "Alice".into(),
        count: 42,
    });
    assert_eq!(store.name().get(), "Alice");
    assert_eq!(store.count().get(), 42);
}

#[test]
fn field_get_untracked() {
    let _test_guard = test_guard();
    let store = ReactiveStore::new(TestData {
        name: "Alice".into(),
        count: 42,
    });
    assert_eq!(store.name().get_untracked(), "Alice");
}

#[test]
fn field_get_reflects_writes() {
    let _test_guard = test_guard();
    let store = ReactiveStore::new(TestData {
        name: "Alice".into(),
        count: 0,
    });
    track!(store.core(), |s| s.name = "Bob".into())
        .commit()
        .notify(&store);
    assert_eq!(store.name().get(), "Bob");
}

// ── Reactivity (flat) ─────────────────────────────────────────────────

#[tokio::test]
async fn write_notifies_effect() {
    let _test_guard = test_guard_async().await;
    let _ = any_spawner::Executor::init_tokio();
    let owner = Owner::new();
    owner.set();
    let store = StdArc::new(ReactiveStore::new(TestData {
        name: "Alice".into(),
        count: 0,
    }));
    let calls = StdArc::new(AtomicUsize::new(0));
    let c = StdArc::clone(&calls);
    let s = StdArc::clone(&store);

    tokio::task::LocalSet::new()
        .run_until(async move {
            Effect::new(move |_: Option<()>| {
                s.name().get();
                c.fetch_add(1, Ordering::Relaxed);
            });
            tick().await;
            assert_eq!(calls.load(Ordering::Relaxed), 1);
            track!(store.core(), |s| s.name = "Bob".into())
                .commit()
                .notify(&store);
            tick().await;
            assert_eq!(calls.load(Ordering::Relaxed), 2);
        })
        .await;
}

#[tokio::test]
async fn field_isolation() {
    let _test_guard = test_guard_async().await;
    let _ = any_spawner::Executor::init_tokio();
    let owner = Owner::new();
    owner.set();
    let store = StdArc::new(ReactiveStore::new(TestData {
        name: "Alice".into(),
        count: 0,
    }));
    let nc = StdArc::new(AtomicUsize::new(0));
    let cc = StdArc::new(AtomicUsize::new(0));
    let sn = StdArc::clone(&store);
    let sc = StdArc::clone(&store);
    let nc1 = StdArc::clone(&nc);
    let cc1 = StdArc::clone(&cc);

    tokio::task::LocalSet::new()
        .run_until(async move {
            Effect::new(move |_: Option<()>| {
                sn.name().get();
                nc1.fetch_add(1, Ordering::Relaxed);
            });
            Effect::new(move |_: Option<()>| {
                sc.count().get();
                cc1.fetch_add(1, Ordering::Relaxed);
            });
            tick().await;
            assert_eq!(nc.load(Ordering::Relaxed), 1);
            assert_eq!(cc.load(Ordering::Relaxed), 1);
            track!(store.core(), |s| s.name = "Bob".into())
                .commit()
                .notify(&store);
            tick().await;
            assert_eq!(nc.load(Ordering::Relaxed), 2);
            assert_eq!(cc.load(Ordering::Relaxed), 1);
            track!(store.core(), |s| s.count += 1)
                .commit()
                .notify(&store);
            tick().await;
            assert_eq!(nc.load(Ordering::Relaxed), 2);
            assert_eq!(cc.load(Ordering::Relaxed), 2);
        })
        .await;
}

// ── Reactivity (nested field, manual Field::new) ───────────────────────

#[tokio::test]
async fn nested_field_isolation() {
    let _test_guard = test_guard_async().await;
    let _ = any_spawner::Executor::init_tokio();
    let owner = Owner::new();
    owner.set();
    let store = StdArc::new(ReactiveStore::new(NestedData {
        profile: Profile {
            first: "Alice".into(),
            age: 30,
        },
        count: 0,
    }));

    // Manual Field construction for the nested field.
    let path: muon_store::StorePath = [
        muon::PathSegment::String("profile".into()),
        muon::PathSegment::String("first".into()),
    ]
    .into_iter()
    .collect();
    let offset = offset_of!(NestedData, profile) + offset_of!(Profile, first);
    // SAFETY: `offset` is the sum of two `offset_of!` results, so it is
    // the exact byte offset of `first` inside `NestedData`, and
    // `NestedData` is not `#[repr(packed)]`.
    let first_field = unsafe {
        Field::<NestedData, String>::new(
            path,
            offset,
            store.core().clone(),
            store.triggers().clone(),
        )
    };
    let count_field = store.count();

    let fc = StdArc::new(AtomicUsize::new(0));
    let cc = StdArc::new(AtomicUsize::new(0));
    let ff = first_field.clone();
    let cf = count_field.clone();
    let s1 = StdArc::clone(&store);
    let s2 = StdArc::clone(&store);

    tokio::task::LocalSet::new()
        .run_until(async move {
            let f1 = StdArc::clone(&fc);
            Effect::new(move |_: Option<()>| {
                ff.get();
                f1.fetch_add(1, Ordering::Relaxed);
            });
            let c1 = StdArc::clone(&cc);
            Effect::new(move |_: Option<()>| {
                cf.get();
                c1.fetch_add(1, Ordering::Relaxed);
            });
            tick().await;
            assert_eq!(fc.load(Ordering::Relaxed), 1, "first initial");
            assert_eq!(cc.load(Ordering::Relaxed), 1, "count initial");
            // Write to nested field → only first effect reruns
            track!(s1.core(), |s| s.profile.first = "Bob".into())
                .commit()
                .notify(&s1);
            tick().await;
            assert_eq!(fc.load(Ordering::Relaxed), 2, "first after profile change");
            assert_eq!(
                cc.load(Ordering::Relaxed),
                1,
                "count unchanged after profile change"
            );
            // Write to sibling field → only count effect reruns
            track!(s2.core(), |s| s.count += 1).commit().notify(&s2);
            tick().await;
            assert_eq!(
                fc.load(Ordering::Relaxed),
                2,
                "first unchanged after count change"
            );
            assert_eq!(cc.load(Ordering::Relaxed), 2, "count after count change");
        })
        .await;
}

/// A renamed field's subscriber must receive its mutation notifications:
/// the accessor's trigger path uses the same serde naming as muon's
/// `Observe` derive (field `rename` wins, else container `rename_all`).
#[test]
fn serde_rename_notifies_field_subscriber() {
    let _test_guard = test_guard();
    let owner = Owner::new();
    owner.set();
    let store = ReactiveStore::new(RenamedData {
        user_name: "Alice".into(),
        count: 42,
    });
    let calls = StdArc::new(AtomicUsize::new(0));
    let c = StdArc::clone(&calls);
    let s = store.clone();

    let _effect = ImmediateEffect::new(move || {
        s.user_name().get(); // trigger path ["user_name"] via snake_case
        s.count().get(); // trigger path ["the_count"] via field rename
        c.fetch_add(1, Ordering::Relaxed);
    });
    assert_eq!(calls.load(Ordering::Relaxed), 1, "runs at construction");

    track!(store.core(), |s| s.user_name = "Bob".into())
        .commit()
        .notify(&store);

    assert_eq!(
        calls.load(Ordering::Relaxed),
        2,
        "renamed field write reached the subscriber",
    );
}

/// A generic model's accessors and triggers compile and notify.
#[test]
fn generic_model_works() {
    let _test_guard = test_guard();
    let owner = Owner::new();
    owner.set();
    let store = ReactiveStore::new(GenericData { value: 1i32 });
    let calls = StdArc::new(AtomicUsize::new(0));
    let c = StdArc::clone(&calls);
    let s = store.clone();

    let _effect = ImmediateEffect::new(move || {
        s.value().get();
        c.fetch_add(1, Ordering::Relaxed);
    });
    assert_eq!(calls.load(Ordering::Relaxed), 1, "runs at construction");

    track!(store.core(), |s| s.value = 2)
        .commit()
        .notify(&store);

    assert_eq!(
        calls.load(Ordering::Relaxed),
        2,
        "generic model field write reached the subscriber",
    );
}

// ── No-op semantics ─────────────────────────────────────────────────────

/// A tracked no-op write produces no paths, so it does not notify (there
/// is nothing stale to re-read). The raw write entry point, which cannot
/// observe fields, conservatively reports the root and broadcasts.
#[test]
fn no_op_semantics() {
    let _test_guard = test_guard();
    let owner = Owner::new();
    owner.set();
    let store = ReactiveStore::new(TestData {
        name: "Alice".into(),
        count: 0,
    });
    let calls = StdArc::new(AtomicUsize::new(0));
    let c = StdArc::clone(&calls);
    let s = store.clone();

    let _effect = ImmediateEffect::new(move || {
        s.count().get();
        c.fetch_add(1, Ordering::Relaxed);
    });
    assert_eq!(calls.load(Ordering::Relaxed), 1, "runs at construction");

    // Tracked no-op: `s.count = 0` on a current value of 0 produces no
    // mutation, hence no paths and no notification.
    track!(store.core(), |s| s.count = 0)
        .commit()
        .notify(&store);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "tracked no-op does not notify"
    );

    // Raw write no-op: the raw entry point cannot observe fields, so it
    // reports the root and broadcasts — subscribers rerun.
    store.core().write(|_arc| {}).notify(&store);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        2,
        "raw write broadcasts even as a no-op"
    );
}

// ── Notification merging ───────────────────────────────────────────────

/// Two synchronous commits in one synchronous stack: the async effect's
/// notification channel is set-bit style, so no executor poll happens
/// between them — the reruns coalesce into one.
#[tokio::test]
async fn consecutive_commits_coalesce_async_effect() {
    let _test_guard = test_guard_async().await;
    let _ = any_spawner::Executor::init_tokio();
    let owner = Owner::new();
    owner.set();
    let store = StdArc::new(ReactiveStore::new(TestData {
        name: "Alice".into(),
        count: 0,
    }));
    let calls = StdArc::new(AtomicUsize::new(0));
    let c = StdArc::clone(&calls);
    let s = StdArc::clone(&store);

    tokio::task::LocalSet::new()
        .run_until(async move {
            Effect::new(move |_: Option<()>| {
                s.name().get();
                s.count().get();
                c.fetch_add(1, Ordering::Relaxed);
            });
            tick().await;
            assert_eq!(calls.load(Ordering::Relaxed), 1, "initial run");

            track!(store.core(), |s| s.name = "B".into())
                .commit()
                .notify(&store);
            track!(store.core(), |s| s.count = 5)
                .commit()
                .notify(&store);
            tick().await;
            assert_eq!(calls.load(Ordering::Relaxed), 2, "coalesced into one rerun");
        })
        .await;
}

/// A single multi-field commit notifies each path in turn. An
/// ImmediateEffect subscribed to both fields reruns once per notified
/// path: path-level notification is not subscriber-level dedup.
#[test]
fn multi_field_commit_notifies_immediate_effect_per_path() {
    let _test_guard = test_guard();
    let owner = Owner::new();
    owner.set();
    let store = ReactiveStore::new(TestData {
        name: "Alice".into(),
        count: 0,
    });
    let calls = StdArc::new(AtomicUsize::new(0));
    let c = StdArc::clone(&calls);
    let s = store.clone();

    let _effect = ImmediateEffect::new(move || {
        s.name().get();
        s.count().get();
        c.fetch_add(1, Ordering::Relaxed);
    });
    assert_eq!(calls.load(Ordering::Relaxed), 1, "runs at construction");

    track!(store.core(), |s| {
        s.name.push_str("B"); // Append — keeps both leaf paths
        s.count = 5; // Replace
    })
    .commit()
    .notify(&store);

    assert_eq!(
        calls.load(Ordering::Relaxed),
        3,
        "one rerun per notified path (name, then count)",
    );
}

/// A commit that replaces every field produces per-field `Replace`
/// diffs (muon's flush contract — no root-level collapse). Each
/// field's event reruns its subscribers.
#[test]
fn full_field_replace_notifies_field_subscribers() {
    let _test_guard = test_guard();
    let owner = Owner::new();
    owner.set();
    let store = ReactiveStore::new(TestData {
        name: "Alice".into(),
        count: 0,
    });
    let calls = StdArc::new(AtomicUsize::new(0));
    let c = StdArc::clone(&calls);
    let s = store.clone();

    let _effect = ImmediateEffect::new(move || {
        s.name().get();
        s.count().get();
        c.fetch_add(1, Ordering::Relaxed);
    });
    assert_eq!(calls.load(Ordering::Relaxed), 1, "runs at construction");

    track!(store.core(), |s| {
        s.name = "B".into(); // per-field Replace
        s.count = 5; // per-field Replace
    })
    .commit()
    .notify(&store);

    assert_eq!(
        calls.load(Ordering::Relaxed),
        3,
        "each replaced field reruns the field subscribers",
    );
}

/// Raw external replacement (the writeback path in muon-sync) changes
/// the store without a local muon observation: the raw write's root
/// event must still rerun field subscribers.
#[test]
fn raw_write_event_notifies_field_subscribers() {
    let _test_guard = test_guard();
    let owner = Owner::new();
    owner.set();
    let store = ReactiveStore::new(TestData {
        name: "Alice".into(),
        count: 0,
    });
    let calls = StdArc::new(AtomicUsize::new(0));
    let c = StdArc::clone(&calls);
    let s = store.clone();

    let _effect = ImmediateEffect::new(move || {
        s.name().get();
        s.count().get();
        c.fetch_add(1, Ordering::Relaxed);
    });
    assert_eq!(calls.load(Ordering::Relaxed), 1, "runs at construction");

    let ev = store
        .core()
        .write(|arc| {
            let mut new = (**arc).clone();
            new.name = "B".into();
            *arc = StdArc::new(new);
        })
        .event();
    store.notify(&ev);

    assert_eq!(
        calls.load(Ordering::Relaxed),
        2,
        "external root notification reruns field subscribers",
    );
}

/// `reactive_graph::effect::batch` merges multiple commits for an
/// ImmediateEffect: it collects `AnySubscriber`, so the effect reruns once.
#[test]
fn graph_batch_merges_immediate_effect() {
    let _test_guard = test_guard();
    let owner = Owner::new();
    owner.set();
    let store = ReactiveStore::new(TestData {
        name: "Alice".into(),
        count: 0,
    });
    let calls = StdArc::new(AtomicUsize::new(0));
    let c = StdArc::clone(&calls);
    let s = store.clone();

    let _effect = ImmediateEffect::new(move || {
        s.name().get();
        s.count().get();
        c.fetch_add(1, Ordering::Relaxed);
    });
    assert_eq!(calls.load(Ordering::Relaxed), 1, "runs at construction");

    reactive_graph::effect::batch(|| {
        track!(store.core(), |s| s.name = "B".into())
            .commit()
            .notify(&store);
        track!(store.core(), |s| s.count = 5)
            .commit()
            .notify(&store);
    });

    assert_eq!(
        calls.load(Ordering::Relaxed),
        2,
        "batch merges both commits into one rerun",
    );
}
