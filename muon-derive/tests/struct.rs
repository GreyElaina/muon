use muon::{Observe, observe};
use serde::Serialize;
use serde_json::json;

#[derive(Serialize, Observe)]
struct Simple {
    x: i32,
    y: String,
}

#[test]
fn no_mutation_returns_empty() {
    let mut s = Simple {
        x: 1,
        y: "hello".into(),
    };
    let mutation = observe!(s => {});
    assert!(mutation.is_empty());
}

#[test]
fn single_field_mutation() {
    let mut s = Simple {
        x: 10,
        y: "hello".into(),
    };
    let mutation = observe!(s => {
        s.x = 20;
    });
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["x"], "before": 10, "after": 20}]),
    );
}

#[test]
fn multiple_field_mutations_batch() {
    let mut s = Simple {
        x: 1,
        y: "a".into(),
    };
    let mutation = observe!(s => {
        s.x = 2;
        s.y.push_str("b");
    });
    // Field-level diffs: the assignment and the string append each
    // produce their own replace.
    assert_eq!(
        mutation.into_json(),
        json!([
            {"path": ["x"], "before": 1, "after": 2},
            {"path": ["y"], "before": "a", "after": "ab"},
        ]),
    );
}

#[test]
fn full_replace_via_deref_mut() {
    let mut s = Simple {
        x: 1,
        y: "a".into(),
    };
    let mutation = observe!(s => {
        *s = Simple { x: 99, y: "z".into() };
    });
    // The wholesale assignment diffs at field level.
    assert_eq!(
        mutation.into_json(),
        json!([
            {"path": ["x"], "before": 1, "after": 99},
            {"path": ["y"], "before": "a", "after": "z"},
        ]),
    );
}

#[derive(Serialize, Observe)]
struct WithRename {
    #[serde(rename = "alpha")]
    a: i32,
    b: i32,
}

#[test]
fn serde_rename_path_segments() {
    let mut w = WithRename { a: 1, b: 2 };
    let mutation = observe!(w => {
        w.a = 10;
    });
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["alpha"], "before": 1, "after": 10}]),
    );
}

#[derive(Serialize, Observe)]
#[serde(rename_all = "camelCase")]
struct WithRenameAll {
    foo_bar: i32,
    baz_qux: i32,
}

#[test]
fn serde_rename_all_path_segments() {
    let mut w = WithRenameAll {
        foo_bar: 1,
        baz_qux: 2,
    };
    let mutation = observe!(w => {
        w.foo_bar = 10;
    });
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["fooBar"], "before": 1, "after": 10}]),
    );
}

#[derive(Serialize, Observe)]
struct Inner {
    c: i32,
    d: i32,
}

#[derive(Serialize, Observe)]
struct WithFlatten {
    a: i32,
    #[serde(flatten)]
    inner: Inner,
}

#[test]
fn serde_flatten_extends() {
    let mut w = WithFlatten {
        a: 1,
        inner: Inner { c: 3, d: 4 },
    };
    let mutation = observe!(w => {
        w.inner.c = 30;
    });
    // The flattened field's path keeps the field name.
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["inner", "c"], "before": 3, "after": 30}]),
    );
}

#[derive(Serialize, Observe)]
struct WithSkipIf {
    #[serde(skip_serializing_if = "Option::is_none")]
    val: Option<i32>,
    other: i32,
}

#[test]
fn serde_skip_serializing_if_delete() {
    let mut w = WithSkipIf {
        val: Some(42),
        other: 10,
    };
    let mutation = observe!(w => {
        w.val = None;
    });
    // `None` is not serialized: the diff deletes the key.
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["val"], "before": 42, "after": null}]),
    );
}

#[test]
fn serde_skip_serializing_if_replace() {
    let mut w = WithSkipIf {
        val: None,
        other: 10,
    };
    let mutation = observe!(w => {
        let _ = w.val.insert(42);
    });
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["val"], "before": null, "after": 42}]),
    );
}

#[derive(Serialize, Observe)]
struct WithSkip {
    a: i32,
    #[muon(skip)]
    b: i32,
}

#[test]
fn muon_skip_not_tracked() {
    let mut w = WithSkip { a: 1, b: 2 };
    let mutation = observe!(w => {
        w.b = 99;
    });
    assert!(mutation.is_empty());
}

#[test]
fn muon_skip_with_tracked() {
    let mut w = WithSkip { a: 1, b: 2 };
    let mutation = observe!(w => {
        w.a = 10;
        w.b = 99;
    });
    // Only the tracked field reports.
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["a"], "before": 1, "after": 10}]),
    );
}

#[derive(Serialize, Observe)]
struct Outer {
    inner: Inner,
    x: i32,
}

#[test]
fn nested_struct_observation() {
    let mut o = Outer {
        inner: Inner { c: 1, d: 2 },
        x: 10,
    };
    let mutation = observe!(o => {
        o.inner.c = 100;
        o.x = 20;
    });
    assert_eq!(
        mutation.into_json(),
        json!([
            {"path": ["inner", "c"], "before": 1, "after": 100},
            {"path": ["x"], "before": 10, "after": 20},
        ]),
    );
}

#[derive(Serialize, Observe)]
struct SingleTuple(String);

#[test]
fn tuple_struct_single_field() {
    let mut t = SingleTuple("hello".into());
    let mutation = observe!(t => {
        t.0.push_str(" world");
    });
    // Single unnamed field keeps its index segment.
    assert_eq!(
        mutation.into_json(),
        json!([{"path": [0], "before": "hello", "after": "hello world"}]),
    );
}

#[derive(Serialize, Observe)]
struct MultiTuple(i32, String);

#[test]
fn tuple_struct_multi_field() {
    let mut t = MultiTuple(1, "hello".into());
    let mutation = observe!(t => {
        t.0 = 42;
        t.1.push_str("!");
    });
    assert_eq!(
        mutation.into_json(),
        json!([
            {"path": [0], "before": 1, "after": 42},
            {"path": [1], "before": "hello", "after": "hello!"},
        ]),
    );
}

#[test]
fn flush_resets_state() {
    let mut s = Simple {
        x: 1,
        y: "a".into(),
    };
    let mutation1 = observe!(s => {
        s.x = 2;
    });
    assert!(!mutation1.is_empty());

    // Second observe with no changes returns empty.
    let mutation2 = observe!(s => {});
    assert!(mutation2.is_empty());
}

#[derive(Serialize, Observe)]
struct AllSkipped {
    #[muon(skip)]
    a: i32,
    #[muon(skip)]
    b: String,
}

#[test]
fn all_fields_skipped_noop() {
    let mut a = AllSkipped {
        a: 1,
        b: "hello".into(),
    };
    let mutation = observe!(a => {
        a.a = 999;
        a.b = "changed".into();
    });
    assert!(mutation.is_empty());
}

#[test]
fn serde_flatten_with_normal_field() {
    let mut w = WithFlatten {
        a: 1,
        inner: Inner { c: 3, d: 4 },
    };
    let mutation = observe!(w => {
        w.a = 10;
        w.inner.c = 30;
    });
    assert_eq!(
        mutation.into_json(),
        json!([
            {"path": ["a"], "before": 1, "after": 10},
            {"path": ["inner", "c"], "before": 3, "after": 30},
        ]),
    );
}

#[derive(Serialize, Observe)]
struct WithVec {
    items: Vec<i32>,
}

#[test]
fn vec_field_append() {
    let mut w = WithVec { items: vec![1, 2] };
    let mutation = observe!(w => {
        w.items.push(3);
    });
    // Vec push is an index-level replace.
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["items", -1], "before": null, "after": 3}]),
    );
}
