//! Regression: `WeakObserver` must not flush a dangling element
//! observer, and must report the alive -> dead transition.
//!
//! The observer does not hold a strong reference (that is the point of
//! observing a `Weak`), so the target can be released at any time. The
//! inner observer's pointer then dangles: the flush must drop it and
//! report the transition as a whole-value `Replace` instead of reading
//! the freed memory.

use muon::observe::ObserveExt;
use std::rc::Rc;

#[test]
fn weak_target_drop_reports_alive_to_dead() {
    let strong = Rc::new("alive".to_string());
    let mut weak = Rc::downgrade(&strong);

    let mut ob = weak.__observe();

    // Release the target: the inner observer dangles.
    drop(strong);

    // The flush must report the alive -> dead transition as a
    // replace with the pre-release snapshot as `before` and no
    // `after`, and it must never touch the freed memory.
    let changes = muon_test_utils::__flush!(&mut ob);
    assert_eq!(changes.inner.len(), 1, "the transition must be reported");
    match &changes.inner[0].changed {
        muon::Changed::Replace {
            before: Some(before),
            after: None,
        } => {
            assert_eq!(before, &serde_json::json!("alive"));
        }
        other => panic!("expected Replace(before = Some(\"alive\"), after = None), got {other:?}"),
    }

    // A second flush reports nothing (state fully reset).
    let changes = muon_test_utils::__flush!(&mut ob);
    assert!(changes.is_empty(), "no residual state after the transition");
}

#[test]
fn weak_target_alive_flush_is_safe() {
    let strong = Rc::new("alive".to_string());
    let mut weak = Rc::downgrade(&strong);
    let mut ob = weak.__observe();

    // No mutation, no change.
    assert!(muon_test_utils::__flush!(&mut ob).is_empty());

    // Re-observe a fresh weak while the target is alive, then release
    // the target: the transition is reported and the state resets.
    let mut fresh = Rc::downgrade(&strong);
    ob = fresh.__observe();
    drop(strong);
    let changes = muon_test_utils::__flush!(&mut ob);
    assert_eq!(changes.inner.len(), 1);
    assert!(matches!(
        &changes.inner[0].changed,
        muon::Changed::Replace { after: None, .. }
    ));
    assert!(muon_test_utils::__flush!(&mut ob).is_empty());
}
