//! POC: a NaN value produces a phantom `Replace` on every flush.
//!
//! `snapshot.rs:178` compares `self != &snapshot`; `NaN != NaN` is
//! `true`, so a value that never mutated still reports a replace
//! (with null payloads, since serde_json maps non-finite floats to
//! null) — violating the flush invariant "no intervening mutations
//! must report nothing".

use muon::observe::ObserveExt;

#[test]
fn nan_flush_is_empty_without_mutation() {
    let mut value = f64::NAN;
    let mut ob = value.__observe();
    let changes = muon_test_utils::__flush!(&mut ob);
    assert!(
        changes.is_empty(),
        "a NaN value without mutation must flush nothing, got {changes:?}"
    );
}

#[test]
fn nan_field_does_not_phantom_on_every_flush() {
    #[derive(serde::Serialize, muon::Observe)]
    struct Model {
        ratio: f64,
    }
    let mut value = Model { ratio: f64::NAN };
    let mut ob = value.__observe();
    let changes = muon_test_utils::__flush!(&mut ob);
    assert!(
        changes.is_empty(),
        "a NaN field without mutation must flush nothing, got {changes:?}"
    );
}
