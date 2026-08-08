use crate::*;
use muon::Observe;

#[derive(Debug, Clone, serde::Serialize, Observe, Track)]
struct TestData {
    name: String,
    count: i32,
}

#[derive(Debug, Clone, serde::Serialize, Observe, Track)]
struct Profile {
    first: String,
    age: i32,
}

#[derive(Debug, Clone, serde::Serialize, Observe, Track)]
struct NestedData {
    profile: Profile,
    count: i32,
}

/// A generic model: the `Track` derive must merge into the user's own
/// `where` clause instead of emitting a duplicate one.
#[derive(Debug, Clone, serde::Serialize, Observe, Track)]
struct GenericData<T: Clone + serde::Serialize + muon::Observe + 'static> {
    value: T,
}

// ── Store basics ──────────────────────────────────────────────────────

#[test]
fn store_new_and_snapshot() {
    let store = Store::new(TestData {
        name: "Alice".into(),
        count: 0,
    });
    assert_eq!(store.snapshot().name, "Alice");
    assert_eq!(store.snapshot().count, 0);
}

#[test]
fn store_write_modifies_value() {
    let store = Store::new(TestData {
        name: "Alice".into(),
        count: 0,
    });
    track!(store, |s| s.name = "Bob".into()).commit();
    assert_eq!(store.snapshot().name, "Bob");
}

#[test]
fn store_write_returns_value() {
    let store = Store::new(TestData {
        name: "Alice".into(),
        count: 0,
    });
    let r = track!(store, |s| {
        s.count += 1;
        42
    })
    .commit()
    .result();
    assert_eq!(r, 42);
    assert_eq!(store.snapshot().count, 1);
}

// ── Snapshot nested ───────────────────────────────────────────────────

#[test]
fn nested_read() {
    let store = Store::new(NestedData {
        profile: Profile {
            first: "Alice".into(),
            age: 30,
        },
        count: 0,
    });
    assert_eq!(store.snapshot().profile.first, "Alice");
}

#[test]
fn nested_write() {
    let store = Store::new(NestedData {
        profile: Profile {
            first: "Alice".into(),
            age: 30,
        },
        count: 0,
    });
    track!(store, |s| s.profile.first = "Bob".into()).commit();
    assert_eq!(store.snapshot().profile.first, "Bob");
}

/// Generic models work through the tracked write path.
#[test]
fn generic_model_write() {
    let store = Store::new(GenericData { value: 1i32 });
    track!(store, |s| s.value = 2).commit();
    assert_eq!(store.snapshot().value, 2);
}

// ── Write intent semantics (lazy / move / drop) ────────────────────────

/// `track!` builds a lazy [`Write`]: nothing happens until it is consumed.
#[test]
fn write_is_lazy() {
    let store = Store::new(TestData {
        name: "Alice".into(),
        count: 0,
    });
    let w = track!(store, |s| s.name = "Bob".into());
    assert_eq!(store.snapshot().name, "Alice", "nothing executed yet");
    w.commit();
    assert_eq!(store.snapshot().name, "Bob", "commit runs the write");
}

/// `move` capture survives the lazy `Write` and is used at commit time.
#[test]
fn move_capture_survives_lazy_write() {
    let store = Store::new(TestData {
        name: "Alice".into(),
        count: 0,
    });
    let suffix = String::from("!");
    let w = track!(store, move |s| s.name.push_str(&suffix));
    w.commit();
    assert_eq!(store.snapshot().name, "Alice!");
}

/// Dropping an `ObservedWrite` without flushing aborts the write (the
/// body never ran).
#[test]
fn snapshot_drop_aborts() {
    let store = Store::new(TestData {
        name: "Alice".into(),
        count: 0,
    });
    {
        let _snap = track!(store, |s| s.name = "Bob".into()).observe();
    }
    assert_eq!(
        store.snapshot().name,
        "Alice",
        "dropping the snapshot aborts the write"
    );
}

/// Nested comparisons and assignments are fully rewritten (the observer
/// visitor recurses into subexpressions, matching muon's own macro).
#[test]
fn nested_expression_rewrite() {
    let store = Store::new(TestData {
        name: "Alice".into(),
        count: 1,
    });
    let matched = track!(store, |s| {
        let cond = s.count == 1;
        if cond {
            s.name = "Bob".into();
        }
        cond
    })
    .commit()
    .result();
    assert!(matched, "comparison result returned through the chain");
    assert_eq!(store.snapshot().name, "Bob", "assignment inside if ran");
}

// ── Enum models ────────────────────────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize, Observe, Track)]
enum State {
    Idle,
    Working { progress: i32, note: String },
    Done(String),
}

/// Enum models work through the tracked write path: assignment rewrites
/// the whole value (a wholesale replace with unknown before).
#[test]
fn enum_write() {
    let store = Store::new(State::Idle);
    track!(store, |s| *s = State::Done("ok".into())).commit();
    assert!(matches!(&*store.snapshot(), State::Done(v) if v == "ok"));
}

/// Variant fields are observed per field.
#[test]
fn enum_field_write() {
    let store = Store::new(State::Working {
        progress: 0,
        note: String::new(),
    });
    track!(store, |s| {
        if let State::Working { progress, .. } = &mut ***s {
            *progress = 42;
        }
    })
    .commit();
    assert!(matches!(
        &*store.snapshot(),
        State::Working { progress: 42, .. }
    ));
}
