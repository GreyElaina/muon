//! The ordered-sequence operation vocabulary: `Edit`.
//!
//! The sync layer's in-place vocabulary for identity-addressed
//! containers (`CrdtVec`-style sequences). Operations address
//! elements by identity, never by integer position: an insert
//! anchors on the element it follows, a delete names its targets, a
//! move names the element and its destination anchor.
//!
//! A transaction carries these operations as the `Inplace` payload
//! of its [`Changed`](crate::Changed) kind; a whole-field replacement
//! is the `Replace` variant instead. The vocabulary is closed (only
//! ordered-sequence operations exist at this layer), so a replace
//! can never be smuggled into an operation stream.

use serde::{Deserialize, Serialize};

use crate::{ItemId, ItemRange};

/// One identity operation on an ordered sequence.
///
/// The element identities live in the engine's element identity
/// space ([`ItemId`]); the client namespace is stamped later by the
/// sync write path. Payloads are self-contained JSON values.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Edit {
    /// Insert a run of elements right after `anchor` (`None` = the
    /// head). The created elements' ids are the contiguous `range`;
    /// `value` is an array slice for a multi-element run, a scalar
    /// repeats per element.
    Insert {
        /// The element the run follows; `None` = the head.
        anchor: Option<ItemId>,
        /// The created elements' contiguous identity run.
        range: ItemRange,
        /// The inserted value.
        value: Box<serde_json::Value>,
    },
    /// Delete every element in every target range, per element
    /// idempotent. The value is the deleted payload, kept for undo.
    Delete {
        /// The element the deleted range followed — the
        /// re-insertion point for undo.
        anchor: Option<ItemId>,
        /// The element runs to delete.
        targets: Vec<ItemRange>,
        /// The deleted payload (for undo: the inverse re-inserts it).
        value: Box<serde_json::Value>,
    },
    /// Move an element to right after `to` (`None` = the head).
    /// `pos` is the move's own identity — the element's new
    /// placement version (idempotent by version comparison);
    /// `from_anchor` preserves the element's predecessor before the
    /// move for undo.
    Move {
        /// The element to move.
        item: ItemId,
        /// The destination anchor; `None` = the head.
        to: Option<ItemId>,
        /// The element the moved element followed before the move
        /// (for undo).
        from_anchor: Option<ItemId>,
        /// The move's placement version (its own fresh identity).
        pos: ItemId,
    },
    /// Update an element's value in place by identity (per-element
    /// last-writer-wins; the same value is a no-op). `prev` is the
    /// element's value before the update, kept for undo (the inverse
    /// swaps `prev` and `value`).
    Update {
        /// The element to update.
        id: ItemId,
        /// The element's value before the update (for undo).
        prev: Box<serde_json::Value>,
        /// The element's new value.
        value: Box<serde_json::Value>,
    },
}
