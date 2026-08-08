//! POC: appended-region elements are inaccessible through the
//! observer.
//!
//! `get_mut`/`force_index` reject `index >= back_boundary`
//! (`vec_deque.rs:285-287,313-315`), so a legal index (the deque is
//! non-empty and the index is in range) returns `None` — and
//! `IndexMut` panics on it, unlike the std container.

use muon::observe::ObserveExt;
use std::collections::VecDeque;

#[test]
fn get_mut_append_region_returns_none() {
    let mut deque: VecDeque<i32> = VecDeque::from([1, 2, 3]);
    let mut ob = deque.__observe();
    ob.push_back(4); // appended region: [3..4)

    let elem = ob.get_mut(3);
    assert!(
        elem.is_some(),
        "an in-range element must be accessible through get_mut"
    );
}

#[test]
fn back_mut_append_region_returns_none() {
    let mut deque: VecDeque<i32> = VecDeque::from([1, 2, 3]);
    let mut ob = deque.__observe();
    ob.push_back(4);

    let elem = ob.back_mut();
    assert!(
        elem.is_some(),
        "back_mut must return the last element of a non-empty deque"
    );
}

#[test]
fn index_mut_append_region_panics() {
    let mut deque: VecDeque<i32> = VecDeque::from([1, 2, 3]);
    let mut ob = deque.__observe();
    ob.push_back(4);

    let _ = &mut ob[3]; // in-range: must not panic
}
