use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use muon::{Observe, observe};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Serialize, Observe)]
struct VecWrapper(#[muon(deref)] Vec<i32>);

impl Deref for VecWrapper {
    type Target = Vec<i32>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for VecWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[test]
fn deref_delegates() {
    let mut w = VecWrapper(vec![1, 2, 3]);
    let mutation = observe!(w => {
        w.push(4);
    });
    // Vec push produces an index-level replace through the deref
    // observer.
    assert_eq!(
        mutation.into_json(),
        json!([{"path": [0, -1], "before": null, "after": 4}]),
    );
}

#[test]
fn deref_no_mutation() {
    let mut w = VecWrapper(vec![1, 2, 3]);
    let mutation = observe!(w => {});
    assert!(mutation.is_empty());
}

#[test]
fn deref_vec_replace() {
    let mut w = VecWrapper(vec![1, 2, 3]);
    let mutation = observe!(w => {
        w.clear();
    });
    // `clear` goes through the raw deref-mut: a whole-field replace.
    assert_eq!(
        mutation.into_json(),
        json!([{"path": [0], "before": [1, 2, 3], "after": []}]),
    );
}

#[test]
fn deref_flush_resets() {
    let mut w = VecWrapper(vec![1, 2, 3]);
    let mutation1 = observe!(w => {
        w.push(4);
    });
    assert!(!mutation1.is_empty());

    let mutation2 = observe!(w => {});
    assert!(mutation2.is_empty());
}

#[derive(Serialize, Observe)]
struct Inner {
    c: i32,
}

#[derive(Serialize, Observe)]
struct Outer {
    a: i32,
    b: i32,
    #[serde(flatten)]
    #[muon(deref)]
    inner: Inner,
}

impl Deref for Outer {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Outer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[test]
fn deref_replace_outer() {
    let mut o = Outer {
        a: 1,
        b: 1,
        inner: Inner { c: 2 },
    };
    let mutation = observe!(o => {
        o.a = 10;
        o = Outer { a: 100, b: 100, inner: Inner { c: 200 } };
    });
    // The wholesale assignment subsumes the field edit: one
    // whole-struct replace against the observed state.
    // The wholesale assignment diffs at field level: every field
    // of the new value differs from the observed state.
    assert_eq!(
        mutation.into_json(),
        json!([
            {"path": ["a"], "before": 1, "after": 100},
            {"path": ["b"], "before": 1, "after": 100},
            {"path": ["inner", "c"], "before": 2, "after": 200},
        ]),
    );
}

#[test]
fn deref_replace_inner() {
    let mut o = Outer {
        a: 1,
        b: 1,
        inner: Inner { c: 2 },
    };
    let mutation = observe!(o => {
        o.a = 10;
        *o = Inner { c: 200 };
    });
    // The deref assignment replaces the inner fields; both edits
    // stay granular.
    assert_eq!(
        mutation.into_json(),
        json!([
            {"path": ["a"], "before": 1, "after": 10},
            {"path": ["inner", "c"], "before": 2, "after": 200},
        ]),
    );
}

#[derive(Serialize, Observe)]
struct FlatMap {
    #[serde(flatten)]
    map: HashMap<String, i32>,
    b: u32,
}

fn sorted_changes(changes: muon::Changes<()>) -> Vec<Value> {
    let mut batch: Vec<Value> = changes.into_json().as_array().expect("flat array").clone();
    batch.sort_by_key(|c| c["path"].to_string());
    batch
}

#[test]
fn flat_map_no_change() {
    let mut f = FlatMap {
        map: HashMap::from([("x".into(), 1)]),
        b: 10,
    };
    let mutation = observe!(f => {});
    assert!(mutation.is_empty());
}

#[test]
fn flat_map_granular_insert() {
    let mut f = FlatMap {
        map: HashMap::from([("x".into(), 1)]),
        b: 10,
    };
    let mutation = observe!(f => {
        f.map.insert("y".into(), 2);
    });
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["map", "y"], "before": null, "after": 2}]),
    );
}

#[test]
fn flat_map_granular_remove() {
    let mut f = FlatMap {
        map: HashMap::from([("x".into(), 1), ("y".into(), 2)]),
        b: 10,
    };
    let mutation = observe!(f => {
        f.map.remove("y");
    });
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["map", "y"], "before": 2, "after": null}]),
    );
}

#[test]
fn flat_map_b_only() {
    let mut f = FlatMap {
        map: HashMap::from([("x".into(), 1)]),
        b: 10,
    };
    let mutation = observe!(f => {
        f.b = 20;
    });
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["b"], "before": 10, "after": 20}]),
    );
}

#[test]
fn flat_map_map_and_b_no_collapse() {
    let mut f = FlatMap {
        map: HashMap::from([("x".into(), 1)]),
        b: 10,
    };
    let mutation = observe!(f => {
        f.map.insert("x".into(), 99);
        f.b = 20;
    });
    // Map insert is granular, so no collapse despite b also changing.
    let batch = Value::Array(sorted_changes(mutation));
    assert_eq!(
        batch,
        json!([
            {"path": ["b"], "before": 10, "after": 20},
            {"path": ["map", "x"], "before": 1, "after": 99},
        ]),
    );
}

#[test]
fn flat_map_map_replace_b_unchanged() {
    let mut f = FlatMap {
        map: HashMap::from([("x".into(), 1)]),
        b: 10,
    };
    let mutation = observe!(f => {
        f.map.insert("x".into(), 99);
    });
    // Only map reports Replace, b unchanged → per-field mutation.
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["map", "x"], "before": 1, "after": 99}]),
    );
}

#[test]
fn flat_map_deref_mut_full_replace() {
    let mut f = FlatMap {
        map: HashMap::from([("x".into(), 1)]),
        b: 10,
    };
    let mutation = observe!(f => {
        f = FlatMap { map: HashMap::from([("y".into(), 2)]), b: 20 };
    });
    // Full outer replace → whole-struct Replace.
    assert_eq!(
        mutation.into_json(),
        json!([
            {"path": ["map"], "before": {"x": 1}, "after": {"y": 2}},
            {"path": ["b"], "before": 10, "after": 20},
        ]),
    );
}

#[test]
fn flat_map_map_deref_mut_with_new_keys() {
    let mut f = FlatMap {
        map: HashMap::from([("x".into(), 1)]),
        b: 10,
    };
    let mutation = observe!(f => {
        *f.map = HashMap::from([("y".into(), 2)]);
    });
    // The map replaced wholesale: one whole-map replace (the
    // container is a plain `HashMap`, not a tracked container), with
    // b unchanged.
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["map"], "before": {"x": 1}, "after": {"y": 2}}]),
    );
}

#[test]
fn flat_map_map_deref_mut_and_b() {
    let mut f = FlatMap {
        map: HashMap::from([("x".into(), 1)]),
        b: 10,
    };
    let mutation = observe!(f => {
        *f.map = HashMap::from([("x".into(), 99)]);
        f.b = 20;
    });
    // The map replaced wholesale: one whole-map replace.
    let batch = Value::Array(sorted_changes(mutation));
    assert_eq!(
        batch,
        json!([
            {"path": ["b"], "before": 10, "after": 20},
            {"path": ["map"], "before": {"x": 1}, "after": {"x": 99}},
        ]),
    );
}
