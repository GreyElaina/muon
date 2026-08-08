//! POC: an `OsString` replace event has inconsistent `before`/`after`
//! shapes.
//!
//! `before` comes from the byte snapshot (`Box<[u8]>` serialized as a
//! bare array), `after` from `OsStr`'s serde form (`{"Unix": ...}`).
//! A consumer cannot deserialize both sides of the same replace.

use muon::helper::QuasiObserver;
use muon::observe::ObserveExt;
use std::ffi::OsString;

#[test]
fn os_string_replace_shapes_are_consistent() {
    let mut s = OsString::from("hello");
    let mut ob = s.__observe();
    *ob.tracked_mut() = OsString::from("world");

    let changes = muon_test_utils::__flush!(&mut ob);
    assert_eq!(changes.inner.len(), 1);
    let muon::Changed::Replace { before, after } = &changes.inner[0].changed else {
        panic!("expected a replace");
    };
    eprintln!("before: {before:?}");
    eprintln!("after:  {after:?}");
    // Both sides must be the serde shape of `OsStr` (a tagged
    // newtype), so a consumer can deserialize either side.
    let before = before.as_ref().expect("before value");
    let after = after.as_ref().expect("after value");
    assert!(
        before.is_object() && after.is_object(),
        "both sides must be the tagged OsStr shape, got {before:?} vs {after:?}"
    );
    assert_eq!(
        before.as_object().unwrap().keys().collect::<Vec<_>>(),
        after.as_object().unwrap().keys().collect::<Vec<_>>(),
        "both sides must carry the same tag (values differ: hello vs world)"
    );
}
