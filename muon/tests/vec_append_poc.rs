//! POC: `VecObserver` keeps appended-region element observers across
//! flushes, so a second flush re-reports the element with a fabricated
//! `before`.
//!
//! The first flush reports appended elements directly (`replace(None,
//! item)`, vec.rs:153-158) and never flushes their observers; the
//! observer created through `ob[i]` inside the appended region keeps
//! its mutated flag. After the flush moves `append_index` forward, the
//! second flush flushes that observer and reports the change again —
//! this time with the observer's creation-time snapshot as `before`.

use muon::helper::QuasiObserver;
use muon::observe::ObserveExt;

#[test]
fn appended_element_observer_is_not_reported_twice() {
    let mut vec: Vec<i32> = vec![1, 2, 3];
    let mut ob = vec.__observe();

    // Append an element and mutate it through its observer (the
    // appended region).
    ob.push(4);
    *ob[3].tracked_mut() = 40;

    // First flush: the appended element is reported once, without a
    // before value.
    let first = muon_test_utils::__flush!(&mut ob);
    assert_eq!(
        first.inner.len(),
        1,
        "the appended element is reported once"
    );
    assert!(
        matches!(
            &first.inner[0].changed,
            muon::Changed::Replace { before: None, .. }
        ),
        "the appended element has no before value"
    );

    // Second flush: nothing remains — the element was already
    // reported, so a second report would fabricate its before value.
    let second = muon_test_utils::__flush!(&mut ob);
    assert!(
        second.is_empty(),
        "no second report with a fabricated before: got {second:?}"
    );
}
