//! Regression: two overlapping element accesses through
//! `SliceObserver::index` must be clean under strict aliasing
//! (verified with Miri).
//!
//! The second `index` call only relocates the observer when its
//! recorded head is stale, so a live shared reference to the same
//! slot is never invalidated by a redundant write.

use muon::observe::ObserveExt;

#[test]
fn overlapping_index_access_is_safe() {
    let mut vec: Vec<i32> = vec![1, 2, 3];
    let ob = vec.__observe();

    let _ = &ob[0];
    let a = &ob[0];
    let b = &ob[0];
    let _ = (a, b);
}
