use muon::{Observe, observe};
use serde::Serialize;
use serde_json::json;

#[derive(Serialize, Observe)]
struct Point {
    x: i32,
    y: i32,
}

#[derive(Serialize, Observe)]
struct Nested {
    pos: Point,
    label: String,
}

#[test]
fn arm_form_basic() {
    let mut p = Point { x: 1, y: 2 };
    let mutation = observe!(p => {
        p.x = 10;
    });
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["x"], "before": 1, "after": 10}]),
    );
}

#[test]
fn closure_form() {
    let cb = observe!(|p: &mut Point| {
        p.x = 10;
    });
    let mut p = Point { x: 1, y: 2 };
    let mutation = cb(&mut p);
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["x"], "before": 1, "after": 10}]),
    );
}

#[test]
fn assignment_tracks_mutation() {
    let mut p = Point { x: 0, y: 0 };
    let mutation = observe!(p => {
        p.x = 42;
        p.y = 99;
    });
    // Field-level diffs: each assignment is its own replace.
    assert_eq!(
        mutation.into_json(),
        json!([
            {"path": ["x"], "before": 0, "after": 42},
            {"path": ["y"], "before": 0, "after": 99},
        ]),
    );
}

#[test]
fn comparison_works() {
    let mut p = Point { x: 5, y: 10 };
    let mutation = observe!(p => {
        if p.x == 5 {
            p.y = 20;
        }
    });
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["y"], "before": 10, "after": 20}]),
    );
}

#[test]
fn comparison_no_mutation() {
    let mut p = Point { x: 5, y: 10 };
    let mutation = observe!(p => {
        if p.x == 999 {
            p.y = 20;
        }
    });
    assert!(mutation.is_empty());
}

#[test]
fn wildcard_pattern() {
    let mutation: muon::Changes<()> = observe!(_ => {
        let _ = 1 + 1;
    });
    assert!(mutation.is_empty());
}

#[test]
fn nested_field_access() {
    let mut n = Nested {
        pos: Point { x: 0, y: 0 },
        label: "start".into(),
    };
    let mutation = observe!(n => {
        n.pos.x = 100;
        n.label.push_str("!");
    });
    // The inner field diff carries the combined path.
    assert_eq!(
        mutation.into_json(),
        json!([
            {"path": ["pos", "x"], "before": 0, "after": 100},
            {"path": ["label"], "before": "start", "after": "start!"},
        ]),
    );
}

#[test]
fn no_mutation_returns_empty() {
    let mut p = Point { x: 1, y: 2 };
    let mutation = observe!(p => {});
    assert!(mutation.is_empty());
}

#[test]
fn closure_no_mutation() {
    let cb = observe!(|p: &mut Point| {});
    let mut p = Point { x: 1, y: 2 };
    let mutation = cb(&mut p);
    assert!(mutation.is_empty());
}

#[test]
fn closure_multiple_calls() {
    let cb = observe!(|p: &mut Point| {
        p.x += 1;
    });

    let mut p = Point { x: 0, y: 0 };

    let mutation1 = cb(&mut p);
    assert_eq!(
        mutation1.into_json(),
        json!([{"path": ["x"], "before": 0, "after": 1}]),
    );

    let mutation2 = cb(&mut p);
    assert_eq!(
        mutation2.into_json(),
        json!([{"path": ["x"], "before": 1, "after": 2}]),
    );
}

#[test]
fn compound_assignment() {
    let mut p = Point { x: 10, y: 20 };
    let mutation = observe!(p => {
        p.x += 5;
        p.y -= 3;
    });
    assert_eq!(
        mutation.into_json(),
        json!([
            {"path": ["x"], "before": 10, "after": 15},
            {"path": ["y"], "before": 20, "after": 17},
        ]),
    );
}
