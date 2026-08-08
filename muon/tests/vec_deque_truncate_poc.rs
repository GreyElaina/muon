//! POC: `VecDequeObserver::truncate` bookkeeping is wrong for the
//! appended region.
//!
//! `vec_deque.rs:351` computes `back_append_len -= back_boundary + len
//! - old_len`, which simplifies to `app' = 2·app - len` instead of
//! `app - (old_len - len)`. Consequences:
//! - truncating inside the appended region under-reports removed
//!   elements (the flush's appended loop takes `back_append_len`);
//! - `truncate(len)` with `len >= old_len` (a no-op) underflows the
//!   subtraction and panics in debug builds.

use muon::observe::ObserveExt;
use std::collections::VecDeque;

#[test]
fn truncate_within_append_region_reports_incorrect_count() {
    let mut deque = VecDeque::from([1, 2, 3]);
    let mut ob = deque.__observe();

    // Appended region: [3..5), length 2.
    ob.push_back(4);
    ob.push_back(5);

    // Truncate to 4: element 5 is removed and must be reported.
    ob.truncate(4);
    let changes = muon_test_utils::__flush!(&mut ob);
    assert_eq!(
        changes.inner.len(),
        1,
        "one removed appended element must be reported"
    );
}

#[test]
fn truncate_noop_does_not_panic() {
    let mut deque = VecDeque::from([1, 2, 3]);
    let mut ob = deque.__observe();

    ob.push_back(4);
    ob.push_back(5);

    // `truncate(5)` with the current length is a no-op: it must not
    // panic, and the flush reports exactly the two appended elements.
    ob.truncate(5);
    let changes = muon_test_utils::__flush!(&mut ob);
    assert_eq!(
        changes.inner.len(),
        2,
        "a no-op truncate leaves the two appended elements reported"
    );
}
