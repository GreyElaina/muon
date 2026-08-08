//! Fixture modules: the `input` models and their derive-expanded `output`
//! snapshots. Both are compiled by the `fixtures` integration test, which
//! also compares the expanded tokens against the snapshots textually (see
//! the `fixtures` unit test in `muon-derive/src/lib.rs`).

pub mod input;
pub mod output;
