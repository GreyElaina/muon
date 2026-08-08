use muon::helper::QuasiObserver;
use muon::observe::ObserveExt;
use muon::{Observe, observe};
use muon_test_utils::*;
use serde::Serialize;
use serde_json::json;

#[derive(Serialize, Debug, PartialEq, Observe)]
enum Shape {
    Circle { radius: f64 },
    Rectangle { width: f64, height: f64 },
    Point,
    Origin,
}

#[test]
fn unit_variant_no_change() {
    let mut s = Shape::Point;
    let mutation = observe!(s => {});
    assert!(mutation.is_empty());
}

#[test]
fn unit_variant_change_to_unit() {
    let mut s = Shape::Point;
    let mutation = observe!(s => {
        *s = Shape::Origin;
    });
    assert_eq!(
        mutation.into_json(),
        json!([{"path": [], "before": null, "after": "Origin"}]),
    );
}

#[test]
fn unit_to_field_variant() {
    let mut s = Shape::Point;
    let mutation = observe!(s => {
        *s = Shape::Circle { radius: 5.0 };
    });
    assert_eq!(
        mutation.into_json(),
        json!([{"path": [], "before": null, "after": {"Circle": {"radius": 5.0}}}]),
    );
}

#[test]
fn field_to_unit_variant() {
    let mut s = Shape::Circle { radius: 3.0 };
    let mutation = observe!(s => {
        *s = Shape::Point;
    });
    assert_eq!(
        mutation.into_json(),
        json!([{"path": [], "before": null, "after": "Point"}]),
    );
}

#[test]
fn field_variant_inner_mutation() {
    let mut s = Shape::Rectangle {
        width: 10.0,
        height: 20.0,
    };
    let mut ob = s.__observe();
    // Use untracked_mut to access inner fields without triggering DerefMut
    if let Shape::Rectangle { width, .. } = ob.untracked_mut() {
        *width = 5.0;
    }
    let mutation = __flush!(&mut ob);
    // External tagging: variant + field combined into the path.
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["Rectangle", "width"], "before": 10.0, "after": 5.0}]),
    );
}

#[test]
fn field_variant_multiple_inner_mutations() {
    let mut s = Shape::Rectangle {
        width: 10.0,
        height: 20.0,
    };
    let mut ob = s.__observe();
    if let Shape::Rectangle { width, height } = ob.untracked_mut() {
        *width = 15.0;
        *height = 25.0;
    }
    let mutation = __flush!(&mut ob);
    // Field-level diffs under the variant.
    assert_eq!(
        mutation.into_json(),
        json!([
            {"path": ["Rectangle", "width"], "before": 10.0, "after": 15.0},
            {"path": ["Rectangle", "height"], "before": 20.0, "after": 25.0},
        ]),
    );
}

#[test]
fn field_variant_no_change() {
    let mut s = Shape::Circle { radius: 3.0 };
    let mut ob = s.__observe();
    // Read-only access through Deref (does not trigger mutation tracking)
    if let Shape::Circle { radius } = ob.untracked_ref() {
        let _ = *radius;
    }
    let mutation = __flush!(&mut ob);
    assert!(mutation.is_empty());
}

#[test]
fn field_variant_deref_mut_replace() {
    let mut s = Shape::Circle { radius: 3.0 };
    let mutation = observe!(s => {
        *s = Shape::Circle { radius: 10.0 };
    });
    assert_eq!(
        mutation.into_json(),
        json!([{"path": [], "before": null, "after": {"Circle": {"radius": 10.0}}}]),
    );
}

#[derive(Serialize, Observe)]
#[serde(rename_all = "snake_case")]
enum Action {
    DoSomething { value: i32, bar: i32 },
    DoNothing,
}

#[test]
fn enum_rename_all_variant() {
    let mut a = Action::DoNothing;
    let mutation = observe!(a => {
        *a = Action::DoSomething { value: 42, bar: 0 };
    });
    assert_eq!(
        mutation.into_json(),
        json!([{"path": [], "before": null, "after": {"do_something": {"value": 42, "bar": 0}}}]),
    );
}

#[test]
fn enum_rename_all_inner_mutation() {
    let mut a = Action::DoSomething { value: 1, bar: 0 };
    let mut ob = a.__observe();
    if let Action::DoSomething { value, .. } = ob.untracked_mut() {
        *value = 99;
    }
    let mutation = __flush!(&mut ob);
    // External tagging with rename_all: variant + field combined.
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["do_something", "value"], "before": 1, "after": 99}]),
    );
}

#[test]
fn flush_resets_state() {
    let mut s = Shape::Point;
    let mutation1 = observe!(s => {
        *s = Shape::Origin;
    });
    assert!(!mutation1.is_empty());

    let mutation2 = observe!(s => {});
    assert!(mutation2.is_empty());
}

#[test]
fn flush_resets_field_variant() {
    let mut s = Shape::Circle { radius: 3.0 };
    let mut ob = s.__observe();

    if let Shape::Circle { radius } = ob.untracked_mut() {
        *radius = 5.0;
    }
    let mutation1 = __flush!(&mut ob);
    assert!(!mutation1.is_empty());

    // No more changes.
    let mutation2 = __flush!(&mut ob);
    assert!(mutation2.is_empty());
}

#[derive(Serialize, Observe)]
#[allow(dead_code)]
enum Color {
    Red,
    Green,
    Blue,
}

#[test]
fn all_unit_enum_no_change() {
    let mut c = Color::Red;
    let mutation = observe!(c => {});
    assert!(mutation.is_empty());
}

#[test]
fn all_unit_enum_change() {
    let mut c = Color::Red;
    let mutation = observe!(c => {
        *c = Color::Blue;
    });
    assert_eq!(
        mutation.into_json(),
        json!([{"path": [], "before": null, "after": "Blue"}]),
    );
}

#[derive(Serialize, Observe)]
#[serde(tag = "type")]
enum Event {
    Click { x: i32, y: i32 },
    Scroll { delta: i32 },
}

#[test]
fn internal_tag_inner_mutation() {
    let mut e = Event::Click { x: 10, y: 20 };
    let mut ob = e.__observe();
    if let Event::Click { x, .. } = ob.untracked_mut() {
        *x = 50;
    }
    let mutation = __flush!(&mut ob);
    // Internal tagging: no variant segment in the path.
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["x"], "before": 10, "after": 50}]),
    );
}

#[test]
fn internal_tag_variant_change() {
    let mut e = Event::Click { x: 10, y: 20 };
    let mutation = observe!(e => {
        *e = Event::Scroll { delta: 5 };
    });
    assert_eq!(
        mutation.into_json(),
        json!([{"path": [], "before": null, "after": {"type": "Scroll", "delta": 5}}]),
    );
}

#[derive(Serialize, Observe)]
#[allow(dead_code)]
enum Container {
    Items { list: Vec<i32> },
    Empty,
}

#[test]
fn field_variant_vec_append() {
    let mut c = Container::Items {
        list: vec![1, 2, 3],
    };
    let mut ob = c.__observe();
    if let Container::Items { list } = ob.untracked_mut() {
        list.push(4);
    }
    let mutation = __flush!(&mut ob);
    // External tagging: variant + field combined; Vec push is an
    // index-level replace.
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["Items", "list", -1], "before": null, "after": 4}]),
    );
}

#[derive(Serialize, Observe)]
enum Wrapper {
    Single(i32),
    Pair(i32, String),
}

#[test]
fn tuple_variant_single_field() {
    let mut w = Wrapper::Single(10);
    let mut ob = w.__observe();
    if let Wrapper::Single(v) = ob.untracked_mut() {
        *v = 20;
    }
    let mutation = __flush!(&mut ob);
    // External tagging: single tuple field uses variant + index.
    assert_eq!(
        mutation.into_json(),
        json!([{"path": ["Single"], "before": 10, "after": 20}]),
    );
}

#[test]
fn tuple_variant_multi_field() {
    let mut w = Wrapper::Pair(1, "hello".into());
    let mut ob = w.__observe();
    if let Wrapper::Pair(n, s) = ob.untracked_mut() {
        *n = 42;
        s.push('!');
    }
    let mutation = __flush!(&mut ob);
    assert_eq!(
        mutation.into_json(),
        json!([
            {"path": ["Pair", 0], "before": 1, "after": 42},
            {"path": ["Pair", 1], "before": "hello", "after": "hello!"},
        ]),
    );
}
