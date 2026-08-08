//! Ordered-sequence operations: the CRDT layer for `CrdtVec`-style
//! containers.
//!
//! # Model
//!
//! A sequence is a single authoritative order — the current visible
//! order — over elements with stable identities ([`ItemId`]). The
//! order lives in an arena-based measured B+ tree
//! ([`crate::crdt::seq_tree`]) with a stable identity directory; deletion
//! marks an element dead in place — a tombstone keeps its slot, so
//! anchors into deleted elements still resolve to their positions.
//!
//! Operations address elements by identity, never by integer
//! position: an insert anchors on the element it follows, a delete
//! names its targets, a move names the element and its destination
//! anchor. An anchor the tree has never seen (a delete from an
//! unknown origin) clamps to the tail — the same "interpret at apply
//! time" semantics as before; a tombstoned anchor resolves to its
//! slot.
//!
//! The wire form stays the single-node array ([`SeqNode`]), now with
//! real liveness markers.
//!
//! # Concurrency
//!
//! Every site applies operations in the same lamport order, so
//! concurrent inserts after one anchor keep the lamport order;
//! concurrent moves of one element — the later op wins (its
//! placement version survives); a move of an element a concurrent
//! delete removed is a no-op (the move's premise, the element exists,
//! is gone).

use crate::crdt::seq_tree::SeqTree;
use crate::{ItemId, ItemRange};
use serde::{Deserialize, Serialize};

/// One element of an ordered sequence: an immutable identity, a
/// liveness marker, a placement version, and the value.
///
/// The serialized form is the single-list wire contract: the server's
/// model value and the snapshot carry sequences as arrays of these
/// nodes, so the authoritative state is plain JSON. A tombstoned node
/// (`alive: false`) keeps its slot for anchoring.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SeqNode<T> {
    /// Immutable element identity.
    pub id: ItemId,
    /// `false` = tombstoned: the slot is kept for anchoring, the
    /// element is invisible.
    pub alive: bool,
    /// Placement version: the id of the operation that created or
    /// last moved the element.
    pub pos: Option<ItemId>,
    /// The element's value.
    pub value: T,
}

/// The sequence state: the visible order in a measured tree, with an
/// identity directory inside ([`SeqTree`]).
#[derive(Clone, Debug, PartialEq)]
pub struct MovableVec<T> {
    tree: SeqTree<T, 32>,
}

impl<T> MovableVec<T> {
    /// Create an empty sequence.
    pub fn new() -> Self {
        MovableVec {
            tree: SeqTree::new(),
        }
    }

    /// Rebuild the state from the wire form. Tombstoned nodes keep
    /// their slots: their positions anchor later inserts.
    pub fn from_nodes(nodes: Vec<SeqNode<T>>) -> Self {
        MovableVec {
            tree: SeqTree::from_nodes(nodes),
        }
    }

    /// The wire form: one node per element — live or tombstoned — in
    /// position order.
    pub fn to_nodes(&self) -> Vec<SeqNode<T>>
    where
        T: Clone,
    {
        self.tree.to_nodes()
    }

    /// The wire form as a borrowed view (values are `&T`); serializing
    /// it matches [`Self::to_nodes`] without copying any element.
    pub fn to_nodes_ref(&self) -> Vec<SeqNode<&T>> {
        self.tree.to_nodes_ref()
    }

    /// The live elements' values in position order.
    pub fn visible(&self) -> impl Iterator<Item = &T> {
        self.tree.iter()
    }

    /// The number of live elements.
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    /// Whether the sequence has no live elements.
    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    /// The value of a live element by identity, if any.
    pub fn value_of(&self, id: ItemId) -> Option<&T> {
        self.tree.get(id)
    }

    /// The value at live index `index`.
    pub fn at(&self, index: usize) -> Option<&T> {
        self.tree.at(index)
    }

    /// The identity at live index `index`.
    pub fn id_at(&self, index: usize) -> Option<ItemId> {
        self.tree.id_at(index)
    }

    /// The live element at `index` with its identity (mutable).
    pub fn at_mut(&mut self, index: usize) -> Option<(ItemId, &mut T)> {
        self.tree.at_mut(index)
    }

    /// The value of a live element by identity, if any (mutable).
    pub fn value_mut(&mut self, id: ItemId) -> Option<&mut T> {
        self.tree.value_mut(id)
    }
}

impl<T> Default for MovableVec<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Insert `values.len()` elements right after `anchor` (`None` = the
/// head), with identities from the contiguous `range`. The run stays
/// contiguous: each element anchors on its predecessor.
///
/// Idempotent: the range's first id already known — live or
/// tombstoned — means the whole insert was applied before → no-op
/// (`false`). A dead anchor (an id the tree has never seen) clamps to
/// the tail; a tombstoned anchor resolves to its slot.
pub fn insert_after<T>(
    vec: &mut MovableVec<T>,
    anchor: Option<ItemId>,
    range: ItemRange,
    values: Vec<T>,
) -> bool {
    debug_assert_eq!(range.len as usize, values.len());
    if vec.tree.contains(range.first) {
        return false; // already applied
    }
    let mut prev = anchor;
    for (id, value) in range.iter().zip(values) {
        vec.tree.insert_after(prev, id, value);
        prev = Some(id);
    }
    true
}

/// Tombstone every element in every target range, per element.
/// Idempotent per element: an unknown or already-dead id is a no-op
/// while the remaining live ids still delete. The slot of each
/// deleted element stays — anchors into it still resolve.
pub fn delete_by_id<T>(vec: &mut MovableVec<T>, targets: &[ItemRange]) -> bool {
    let mut changed = false;
    for range in targets {
        for id in range.iter() {
            if vec.tree.remove(id) {
                changed = true;
            }
        }
    }
    changed
}

/// Move an element to right after `to` (`None` = the head).
///
/// Idempotent: `move_id` equals the element's current placement
/// version → no-op. An unknown element (never seen, or deleted by a
/// concurrent op) is a no-op; moving after itself is a no-op.
pub fn move_after<T>(
    vec: &mut MovableVec<T>,
    item: ItemId,
    to: Option<ItemId>,
    move_id: ItemId,
) -> bool {
    vec.tree.move_after(item, to, move_id)
}

/// Update an element's value in place by identity.
///
/// The element's position and identity are unchanged. Returns the
/// element's previous value; `None` when the element is unknown
/// (never seen, or deleted by a concurrent op) or already holds the
/// same value (idempotent).
pub fn update_value<T>(vec: &mut MovableVec<T>, id: ItemId, value: T) -> Option<T>
where
    T: PartialEq + Clone,
{
    vec.tree.update_value(id, value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn item(seq: u64) -> ItemId {
        ItemId {
            client_id: 1,
            incarnation: 1,
            seq,
        }
    }

    fn range(first: u64, len: u32) -> ItemRange {
        ItemRange {
            first: item(first),
            len,
        }
    }

    fn insert(
        vec: &mut MovableVec<serde_json::Value>,
        anchor: Option<ItemId>,
        first: u64,
        values: &[&str],
    ) {
        let values: Vec<serde_json::Value> = values.iter().map(|v| json!(v)).collect();
        let r = range(first, values.len() as u32);
        assert!(insert_after(vec, anchor, r, values), "fresh insert changes");
    }

    #[test]
    fn insert_after_anchor_places_in_order() {
        let mut vec = MovableVec::new();
        insert(&mut vec, None, 1, &["a"]);
        insert(&mut vec, Some(item(1)), 2, &["b"]);
        let vals: Vec<_> = vec.visible().collect();
        assert_eq!(vals, vec![&json!("a"), &json!("b")]);
    }

    #[test]
    fn multi_element_insert_chains_positions() {
        let mut vec = MovableVec::new();
        insert(&mut vec, None, 1, &["a", "b", "c"]);
        // An insert after `a` lands between a and b.
        insert(&mut vec, Some(item(1)), 10, &["x"]);
        let vals: Vec<_> = vec.visible().collect();
        assert_eq!(
            vals,
            vec![&json!("a"), &json!("x"), &json!("b"), &json!("c")]
        );
    }

    #[test]
    fn insert_is_idempotent() {
        let mut vec = MovableVec::new();
        let values = vec![json!("a"), json!("b")];
        assert!(insert_after(&mut vec, None, range(1, 2), values.clone()));
        assert!(
            !insert_after(&mut vec, None, range(1, 2), values),
            "replay no-op"
        );
        let vals: Vec<_> = vec.visible().collect();
        assert_eq!(vals, vec![&json!("a"), &json!("b")]);
    }

    #[test]
    fn delete_is_per_element_idempotent() {
        let mut vec = MovableVec::new();
        insert(&mut vec, None, 1, &["a", "b", "c"]);
        assert!(delete_by_id(&mut vec, &[range(2, 1)]));
        assert!(delete_by_id(&mut vec, &[range(1, 3)]), "a and c still live");
        assert_eq!(vec.len(), 0);
        assert!(!delete_by_id(&mut vec, &[range(1, 3)]), "all deleted");
    }

    #[test]
    fn deleted_element_keeps_its_anchor_slot() {
        let mut vec = MovableVec::new();
        insert(&mut vec, None, 1, &["a", "b", "c"]);
        delete_by_id(&mut vec, &[range(2, 1)]);
        // An insert anchored on the deleted `b` lands at its slot,
        // between a and c — the tombstone keeps the position.
        insert(&mut vec, Some(item(2)), 10, &["x"]);
        let vals: Vec<_> = vec.visible().collect();
        assert_eq!(vals, vec![&json!("a"), &json!("x"), &json!("c")]);
        assert_eq!(vec.len(), 3);
    }

    #[test]
    fn move_updates_placement_version_and_is_idempotent() {
        let mut vec = MovableVec::new();
        insert(&mut vec, None, 1, &["a", "b", "c"]);
        assert!(move_after(&mut vec, item(3), None, item(20)));
        let vals: Vec<_> = vec.visible().collect();
        assert_eq!(vals, vec![&json!("c"), &json!("a"), &json!("b")]);
        assert!(
            !move_after(&mut vec, item(3), None, item(20)),
            "replay no-op"
        );
    }

    #[test]
    fn move_after_anchor_places_before_current_occupant() {
        let mut vec = MovableVec::new();
        insert(&mut vec, None, 1, &["a", "b", "c"]);
        // `c` moves after `a`: before `b`.
        move_after(&mut vec, item(3), Some(item(1)), item(20));
        let vals: Vec<_> = vec.visible().collect();
        assert_eq!(vals, vec![&json!("a"), &json!("c"), &json!("b")]);
    }

    #[test]
    fn move_unknown_element_is_no_op() {
        let mut vec = MovableVec::new();
        insert(&mut vec, None, 1, &["a"]);
        assert!(!move_after(&mut vec, item(99), None, item(20)));
    }

    #[test]
    fn deleted_element_move_is_no_op() {
        let mut vec = MovableVec::new();
        insert(&mut vec, None, 1, &["a", "b"]);
        delete_by_id(&mut vec, &[range(1, 1)]);
        // A concurrent move of the deleted element cannot resurrect it.
        assert!(!move_after(&mut vec, item(1), None, item(20)));
    }

    #[test]
    fn dead_anchor_insert_clamps_to_the_tail() {
        let mut vec = MovableVec::new();
        insert(&mut vec, None, 1, &["a", "b"]);
        insert(&mut vec, Some(item(99)), 10, &["x"]);
        let vals: Vec<_> = vec.visible().collect();
        assert_eq!(vals, vec![&json!("a"), &json!("b"), &json!("x")]);
    }

    #[test]
    fn wire_round_trip_is_identity() {
        let mut vec = MovableVec::new();
        insert(&mut vec, None, 1, &["a", "b", "c"]);
        move_after(&mut vec, item(3), None, item(20));
        delete_by_id(&mut vec, &[range(2, 1)]);
        let nodes = vec.to_nodes();
        assert_eq!(nodes.len(), 3, "the tombstone stays in the wire form");
        let b = nodes
            .iter()
            .find(|n| n.id == item(2))
            .expect("b in wire form");
        assert!(!b.alive, "b is marked dead");
        let rebuilt = MovableVec::from_nodes(nodes);
        let vals: Vec<_> = rebuilt.visible().collect();
        assert_eq!(vals, vec![&json!("c"), &json!("a")]);
        assert_eq!(rebuilt.len(), 2);
    }

    #[test]
    fn legacy_tombstones_keep_their_slots() {
        let nodes = vec![
            SeqNode {
                id: item(1),
                alive: true,
                pos: Some(item(1)),
                value: json!("a"),
            },
            SeqNode {
                id: item(2),
                alive: false,
                pos: Some(item(2)),
                value: json!("b"),
            },
            SeqNode {
                id: item(3),
                alive: true,
                pos: Some(item(1)),
                value: json!("c"),
            },
        ];
        let mut vec = MovableVec::from_nodes(nodes);
        let vals: Vec<_> = vec.visible().collect();
        assert_eq!(vals, vec![&json!("a"), &json!("c")], "tombstone invisible");
        assert_eq!(vec.len(), 2);
        // The tombstone's slot still anchors: an insert on `b` lands
        // between a and c.
        insert(&mut vec, Some(item(2)), 10, &["x"]);
        let vals: Vec<_> = vec.visible().collect();
        assert_eq!(vals, vec![&json!("a"), &json!("x"), &json!("c")]);
    }
}
