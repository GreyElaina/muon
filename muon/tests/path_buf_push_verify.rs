//! Verification: relative `push` (including `..`-leading paths) is
//! reported — `PathBuf::push` appends relative paths as components
//! (std does not resolve `..`), so the length always changes and the
//! len-based detection fires.

use muon::observe::ObserveExt;
use std::path::PathBuf;

#[test]
fn push_dotdot_is_reported() {
    let mut p = PathBuf::from("/abcd");
    let mut ob = p.__observe();
    ob.push("../x");
    let changes = muon_test_utils::__flush!(&mut ob);
    assert!(
        !changes.is_empty(),
        "a relative push must be reported, got {changes:?}"
    );
}

#[test]
fn push_plain_is_reported() {
    let mut p = PathBuf::from("/a/b");
    let mut ob = p.__observe();
    ob.push("c");
    let changes = muon_test_utils::__flush!(&mut ob);
    assert!(!changes.is_empty(), "a plain push must be reported");
}
