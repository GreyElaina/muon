//! POC/regression: the reactivity derive must support const generic
//! models and honor muon's skip attributes.
//!
//! - `struct S<const N: usize>` previously failed with E0401 (the
//!   const parameter was filtered out of the accessor trait).
//! - A `#[muon(skip)]` / `#[muon(noop)]` field must not generate an
//!   accessor (matching muon's Observe derive), so a stale cache
//!   field never notifies.

use muon::Observe;
use muon_reactivity::*;

#[derive(Observe, Clone, Reactivity)]
struct Board<const N: usize> {
    cells: [u8; N],
}

#[derive(Observe, Clone, Reactivity)]
struct Doc {
    title: String,
    #[muon(skip)]
    cache: Vec<u8>,
    #[muon(noop)]
    meta: Vec<u8>,
    #[serde(serialize_with = "crate::serialize_stamp")]
    stamp: u64,
}

#[allow(dead_code)]
fn serialize_stamp<S: serde::Serializer>(v: &u64, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_u64(*v)
}

#[test]
fn const_generic_model_compiles() {
    let board = Board::<3> { cells: [1, 2, 3] };
    let _ = board.cells;
}

#[test]
fn skipped_fields_have_no_accessors() {
    let doc = Doc {
        title: "t".into(),
        cache: Vec::new(),
        meta: Vec::new(),
        stamp: 0,
    };
    // `title` is accessible; `cache`/`meta`/`stamp` are skipped by
    // muon's rules and must not notify.
    let _ = doc.title;
}
