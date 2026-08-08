//! Regression: generic `deref` fields with a general observer
//! (`shallow`), plus single-field generic `deref`.
//!
//! The derive must (1) carry the general-observer bounds
//! (`T: SerializeSnapshot + Serialize`) on the input `Observe` impl so
//! the projection stays usable, and (2) place `'ob` on the observer
//! impls whenever a generic field is skipped or deref'd — including
//! single-field structs, where the observer struct itself has no
//! `'ob` parameter.

use muon::general::{SerializeSnapshot, Snapshot};
use muon::{Observe, observe};
use serde::Serialize;
use serde_json::json;

/// A type with snapshot machinery but no `Observe` impl: the target
/// type for a `shallow` observer.
#[derive(Serialize)]
struct Tracked(Vec<i32>);

impl Snapshot for Tracked {
    type Snapshot = Vec<i32>;

    fn to_snapshot(&self) -> Vec<i32> {
        self.0.clone()
    }
}

impl SerializeSnapshot for Tracked {
    fn flush<S: muon::observe::Sink + ?Sized>(&self, snapshot: Vec<i32>, sink: &mut S) {
        if self.0 != snapshot {
            sink.replace(Some(&snapshot), Some(&self.0));
        }
    }
}

#[derive(Serialize, Observe)]
struct Wrapper<T> {
    #[muon(deref, shallow)]
    inner: T,
    other: i32,
}

impl<T> std::ops::Deref for Wrapper<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> std::ops::DerefMut for Wrapper<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[test]
fn generic_deref_shallow_reports_replace() {
    let mut w = Wrapper {
        inner: Tracked(vec![1]),
        other: 0,
    };
    let mutation = observe!(w => {
        // Autoderef chain: WrapperObserver -> ShallowObserver ->
        // Pointer -> Wrapper -> Tracked -> Vec. The mutable borrow
        // marks the shallow state.
        w.0.push(2);
    });
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["inner"], "before": [1], "after": [1, 2]}]),
    );
}

#[test]
fn generic_deref_shallow_no_mutation() {
    let mut w = Wrapper {
        inner: Tracked(vec![1]),
        other: 0,
    };
    let mutation = observe!(w => {});
    assert!(mutation.is_empty(), "no mutation, no change");
}

#[derive(Serialize, Observe)]
struct Single<T>(#[muon(deref, shallow)] T);

impl<T> std::ops::Deref for Single<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for Single<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[test]
fn single_field_generic_deref_shallow_reports_replace() {
    let mut s = Single(Tracked(vec![1]));
    let mutation = observe!(s => {
        // `s.0` is the field observer; `s.0.0` is the tracked value
        // (through the deref chain), `s.0.0.0` its Vec.
        s.0.0.0.push(2);
    });
    assert_eq!(
        mutation.into_json(),
        json!([{"path": [0], "before": [1], "after": [1, 2]}]),
    );
}
