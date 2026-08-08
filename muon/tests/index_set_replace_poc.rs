//! POC: `IndexSetObserver::replace` / `replace_full` silently drop an
//! insertion when the value is not present.
//!
//! indexmap's `replace` adds the value (appending it) when it is not
//! present, but `index_set.rs:320-337` only marks a change when the
//! value already exists — the insertion never reaches the flush.

use indexmap::IndexSet;
use muon::observe::ObserveExt;

#[test]
fn replace_missing_value_is_reported() {
    let mut set = IndexSet::from(["a".to_string()]);
    let mut ob = set.__observe();

    // Value absent: indexmap semantics add it (append at the end).
    let old = ob.replace("b".to_string());
    assert_eq!(old, None, "no existing value is replaced");

    let changes = muon_test_utils::__flush!(&mut ob);
    assert_eq!(
        changes.inner.len(),
        1,
        "adding a value through replace must be reported"
    );
}

#[test]
fn replace_full_missing_value_is_reported() {
    let mut set = IndexSet::from(["a".to_string()]);
    let mut ob = set.__observe();

    let (index, old) = ob.replace_full("b".to_string());
    assert_eq!(old, None);
    assert_eq!(index, 1, "the new value is appended");

    let changes = muon_test_utils::__flush!(&mut ob);
    assert_eq!(
        changes.inner.len(),
        1,
        "adding a value through replace_full must be reported"
    );
}

#[test]
fn replace_existing_value_is_reported() {
    let mut set = IndexSet::from(["a".to_string()]);
    let mut ob = set.__observe();

    // Value present: the existing entry is replaced in place.
    let old = ob.replace("a".to_string());
    assert_eq!(old.as_deref(), Some("a"));

    let changes = muon_test_utils::__flush!(&mut ob);
    assert_eq!(
        changes.inner.len(),
        1,
        "replacing an existing value must be reported"
    );
}
