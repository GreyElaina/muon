//! Identity-addressed ordered sequence containers and their engine.
//!
//! [`CrdtVec`] and [`CrdtString`] wrap the arena B+ tree
//! ([`MovableVec`], [`SeqTree`]) and export the identity operations
//! (`insert_after`, `delete_by_id`, `move_after`, `update_value`).
//! This layer depends only on the shared wire types.

// The seq/seq_tree internals are referenced by other layers (the
// pipeline applies and fuzzes them), so they are crate-visible.
mod edit;
pub(crate) mod seq;
pub(crate) mod seq_tree;
mod string;
mod vec;

pub use edit::Edit;
pub use seq::{delete_by_id, insert_after, move_after, update_value, MovableVec, SeqNode};
pub use string::{CrdtString, CrdtStringObserver, Segment};
pub use vec::{CrdtVec, CrdtVecObserver};
