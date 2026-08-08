//! Arena-based measured B+ tree over the single authoritative order.
//!
//! The tree holds the current order over elements with stable
//! identities. Deletion marks an element dead in place — a tombstone
//! keeps its slot, so anchors into deleted elements still resolve to
//! their positions. The live count ([`SeqTree::len`]) counts live
//! elements only; an element's slot position in the tree is its
//! entity position, derived from the tree, never cached.
//!
//! - Elements are addressed by identity (anchors, targets), never by
//!   integer position. A stable identity directory maps each element
//!   id to the leaf that stores it; an element's position in the
//!   order is derived from the tree, never cached.
//! - Nodes live in an arena: handles are stable. A split pushes a
//!   new node instead of moving a subtree; a removed node stays in
//!   the pool as an orphan. Handles never dangle.
//! - Parent pointers support upward split propagation and position
//!   computation without re-walking from the root. The directory
//!   locates the leaf; the parent chain gives the path.

use crate::{ItemId, SeqNode};
use std::collections::HashMap;

/// A stored sequence element: identity, placement version, liveness,
/// and value.
#[derive(Clone, Debug, PartialEq)]
pub struct Stored<T> {
    /// The element's immutable identity.
    pub id: ItemId,
    /// Placement version: the id of the slot the element occupies.
    pub pos: ItemId,
    /// `false` = tombstoned: the slot is kept for anchoring, the
    /// element is invisible.
    pub alive: bool,
    /// The element's value.
    pub value: T,
}

#[derive(Clone, Debug, PartialEq)]
struct Node<T> {
    items: Vec<Stored<T>>,
    children: Vec<usize>,
    parent: Option<usize>,
    len: usize,
}

impl<T> Default for Node<T> {
    fn default() -> Self {
        Node {
            items: Vec::new(),
            children: Vec::new(),
            parent: None,
            len: 0,
        }
    }
}

/// An arena-based measured B+ tree over elements with stable
/// identities. `M` is the node order; a full node splits in half.
#[derive(Clone, Debug, PartialEq)]
pub struct SeqTree<T, const M: usize> {
    nodes: Vec<Node<T>>,
    root: usize,
    len: usize,
    /// Identity directory: element id → the leaf that stores it.
    /// A leaf split moves the right half to a new leaf, so entries
    /// for the moved elements are rewritten at split time (O(M)).
    index: HashMap<ItemId, usize>,
}

impl<T, const M: usize> SeqTree<T, M> {
    /// Create an empty tree.
    pub fn new() -> Self {
        SeqTree {
            nodes: vec![Node::default()],
            root: 0,
            len: 0,
            index: HashMap::new(),
        }
    }

    /// The number of live elements.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the tree has no elements.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The value at `index` in the current order.
    pub fn at(&self, index: usize) -> Option<&T> {
        let (leaf, off) = self.descend(index)?;
        Some(&self.nodes[leaf].items[off].value)
    }

    /// The identity at `index` in the current order.
    pub fn id_at(&self, index: usize) -> Option<ItemId> {
        let (leaf, off) = self.descend(index)?;
        Some(self.nodes[leaf].items[off].id)
    }

    /// The live element at `index` with its identity (mutable).
    pub fn at_mut(&mut self, index: usize) -> Option<(ItemId, &mut T)> {
        let (leaf, off) = self.descend(index)?;
        let stored = &mut self.nodes[leaf].items[off];
        Some((stored.id, &mut stored.value))
    }

    /// The value of a live element by identity, if any (mutable).
    /// A tombstone (deleted element) is invisible: `None`.
    pub fn value_mut(&mut self, id: ItemId) -> Option<&mut T> {
        let leaf = *self.index.get(&id)?;
        let stored = self.nodes[leaf].items.iter_mut().find(|s| s.id == id)?;
        if !stored.alive {
            return None;
        }
        Some(&mut stored.value)
    }

    /// The value of a live element by identity, if any. A tombstone
    /// (deleted element) is invisible: `None`.
    pub fn get(&self, id: ItemId) -> Option<&T> {
        let leaf = *self.index.get(&id)?;
        self.nodes[leaf]
            .items
            .iter()
            .find(|s| s.id == id)
            .filter(|s| s.alive)
            .map(|s| &s.value)
    }

    /// Whether the tree knows the element — live or tombstoned. The
    /// directory keeps deleted elements, so replay checks use this
    /// instead of [`Self::get`].
    pub fn contains(&self, id: ItemId) -> bool {
        self.index.contains_key(&id)
    }

    /// Update a live element's value in place by identity. Returns
    /// the previous value; `None` when the element is unknown,
    /// tombstoned, or already holds the same value (idempotent).
    pub fn update_value(&mut self, id: ItemId, value: T) -> Option<T>
    where
        T: PartialEq + Clone,
    {
        let leaf = *self.index.get(&id)?;
        let stored = self.nodes[leaf].items.iter_mut().find(|s| s.id == id)?;
        if !stored.alive || stored.value == value {
            return None;
        }
        Some(std::mem::replace(&mut stored.value, value))
    }

    /// Insert `value` with identity `id` right after the element
    /// `anchor`. `None` inserts at the head; an anchor the tree has
    /// never seen (a concurrent delete from an unknown origin)
    /// clamps to the tail. An anchor that is tombstoned resolves to
    /// its slot — the insert lands at the deleted element's position.
    ///
    /// Idempotent: an `id` already present means the insert was
    /// applied before → no-op (`false`).
    pub fn insert_after(&mut self, anchor: Option<ItemId>, id: ItemId, value: T) -> bool {
        if self.index.contains_key(&id) {
            return false; // already applied
        }
        let (leaf, off) = self.locate_insert_point(anchor);
        self.insert_stored(
            leaf,
            off,
            Stored {
                id,
                pos: id,
                alive: true,
                value,
            },
        );
        true
    }

    /// Tombstone the element with the given identity: the slot and
    /// its anchoring position stay, the element becomes invisible.
    /// Unknown or already-dead ids are a no-op (`false`).
    pub fn remove(&mut self, id: ItemId) -> bool {
        let Some(&leaf) = self.index.get(&id) else {
            return false;
        };
        let Some(off) = self.nodes[leaf].items.iter().position(|s| s.id == id) else {
            return false;
        };
        if !self.nodes[leaf].items[off].alive {
            return false; // already tombstoned
        }
        self.nodes[leaf].items[off].alive = false;
        // The leaf's live count and every ancestor's live count
        // shrink by one.
        self.nodes[leaf].len -= 1;
        let mut ancestor = self.nodes[leaf].parent;
        while let Some(p) = ancestor {
            self.nodes[p].len -= 1;
            ancestor = self.nodes[p].parent;
        }
        self.len -= 1;
        true
    }

    /// Physically detach a live element from its slot, returning its
    /// value. The element keeps its identity in the directory; the
    /// caller re-inserts it. Used by moves, where the element stays
    /// alive.
    fn detach(&mut self, id: ItemId) -> Option<T> {
        let leaf = *self.index.get(&id)?;
        let off = self.nodes[leaf].items.iter().position(|s| s.id == id)?;
        self.index.remove(&id);
        let mut n = std::mem::take(&mut self.nodes[leaf]);
        let removed = n.items.remove(off);
        n.len -= 1;
        let empty = n.items.is_empty();
        self.nodes[leaf] = n;
        // The removal shortens every ancestor's subtree by one.
        let mut ancestor = self.nodes[leaf].parent;
        while let Some(p) = ancestor {
            self.nodes[p].len -= 1;
            ancestor = self.nodes[p].parent;
        }
        if empty {
            self.prune_empty(leaf);
        }
        self.len -= 1;
        Some(removed.value)
    }

    /// Move the element `item` to right after the element `to`.
    /// `None` moves to the head; a dead anchor clamps to the tail.
    ///
    /// Idempotent: `move_id` equal to the element's current placement
    /// version → no-op; an unknown or tombstoned element → no-op;
    /// moving after itself → no-op.
    pub fn move_after(&mut self, item: ItemId, to: Option<ItemId>, move_id: ItemId) -> bool {
        let Some(&leaf) = self.index.get(&item) else {
            return false; // unknown or deleted element
        };
        let stored = self.nodes[leaf]
            .items
            .iter()
            .find(|s| s.id == item)
            .expect("directory entry has a stored element");
        if !stored.alive {
            return false; // a tombstone cannot be moved
        }
        if stored.pos == move_id {
            return false; // already moved here
        }
        if to == Some(item) {
            return false; // move after itself: no-op
        }
        let value = self.detach(item).expect("existence checked");
        let (leaf, off) = self.locate_insert_point(to);
        self.insert_stored(
            leaf,
            off,
            Stored {
                id: item,
                pos: move_id,
                alive: true,
                value,
            },
        );
        true
    }

    /// Iterate the live values in the current order.
    pub fn iter(&self) -> Iter<'_, T, M> {
        Iter {
            tree: self,
            stack: vec![self.root],
            items: [].iter(),
        }
    }

    /// The wire form: one node per element — live or tombstoned — in
    /// position order, with the liveness marker set.
    pub fn to_nodes(&self) -> Vec<SeqNode<T>>
    where
        T: Clone,
    {
        let mut nodes = Vec::new();
        let mut stack = vec![self.root];
        while let Some(h) = stack.pop() {
            let n = &self.nodes[h];
            if n.children.is_empty() {
                for s in &n.items {
                    nodes.push(SeqNode {
                        id: s.id,
                        alive: s.alive,
                        pos: Some(s.pos),
                        value: s.value.clone(),
                    });
                }
            } else {
                for &c in n.children.iter().rev() {
                    stack.push(c);
                }
            }
        }
        nodes
    }

    /// The wire form as a borrowed view: values are `&T` references
    /// instead of clones. Serializing the view produces the same
    /// output as [`Self::to_nodes`] without copying any element.
    pub fn to_nodes_ref(&self) -> Vec<SeqNode<&T>> {
        let mut nodes = Vec::new();
        let mut stack = vec![self.root];
        while let Some(h) = stack.pop() {
            let n = &self.nodes[h];
            if n.children.is_empty() {
                for s in &n.items {
                    nodes.push(SeqNode {
                        id: s.id,
                        alive: s.alive,
                        pos: Some(s.pos),
                        value: &s.value,
                    });
                }
            } else {
                for &c in n.children.iter().rev() {
                    stack.push(c);
                }
            }
        }
        nodes
    }

    /// Rebuild the tree from the wire form. Tombstoned nodes keep
    /// their slots: their positions anchor later inserts.
    pub fn from_nodes(nodes: Vec<SeqNode<T>>) -> Self {
        let mut tree = SeqTree::new();
        let mut prev = None;
        for node in nodes {
            let pos = node.pos.unwrap_or(node.id);
            let (leaf, off) = tree.locate_insert_point(prev);
            tree.insert_stored(
                leaf,
                off,
                Stored {
                    id: node.id,
                    pos,
                    alive: node.alive,
                    value: node.value,
                },
            );
            prev = Some(node.id);
        }
        tree
    }

    /// The leaf handle and in-leaf physical offset of the `index`-th
    /// live element. The leaf scan skips tombstones.
    fn descend(&self, index: usize) -> Option<(usize, usize)> {
        if index >= self.len {
            return None;
        }
        let mut node = self.root;
        let mut remaining = index;
        loop {
            let n = &self.nodes[node];
            if n.children.is_empty() {
                let phys = n
                    .items
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| s.alive)
                    .nth(remaining)
                    .map(|(i, _)| i)?;
                return Some((node, phys));
            }
            let mut acc = 0;
            let mut next = None;
            for &c in &n.children {
                let clen = self.nodes[c].len;
                if remaining < acc + clen {
                    next = Some(c);
                    break;
                }
                acc += clen;
            }
            node = next?;
            remaining -= acc;
        }
    }

    /// The leftmost leaf.
    fn leftmost_leaf(&self) -> usize {
        let mut node = self.root;
        while !self.nodes[node].children.is_empty() {
            node = self.nodes[node].children[0];
        }
        node
    }

    /// The rightmost leaf.
    fn rightmost_leaf(&self) -> usize {
        let mut node = self.root;
        while !self.nodes[node].children.is_empty() {
            node = *self.nodes[node].children.last().unwrap();
        }
        node
    }

    /// The insertion point for an anchor: the leaf and in-leaf
    /// offset right after the anchor element. `None` is the head; a
    /// dead anchor clamps to the tail.
    fn locate_insert_point(&self, anchor: Option<ItemId>) -> (usize, usize) {
        match anchor {
            None => (self.leftmost_leaf(), 0),
            Some(a) => match self.index.get(&a) {
                Some(&leaf) => {
                    let off = self.nodes[leaf]
                        .items
                        .iter()
                        .position(|s| s.id == a)
                        .unwrap();
                    (leaf, off + 1)
                }
                None => {
                    let leaf = self.rightmost_leaf();
                    (leaf, self.nodes[leaf].items.len())
                }
            },
        }
    }

    /// Insert into a leaf and propagate splits up the parent chain.
    /// Registers the new id in the directory; a live insert bumps the
    /// live counts.
    fn insert_stored(&mut self, leaf: usize, off: usize, stored: Stored<T>) {
        let mut child = leaf;
        // A live insert lengthens every ancestor's subtree by one,
        // even when no split propagates. Tombstones do not count.
        let live = stored.alive;
        if live {
            let mut ancestor = self.nodes[leaf].parent;
            while let Some(p) = ancestor {
                self.nodes[p].len += 1;
                ancestor = self.nodes[p].parent;
            }
        }
        let mut sibling = self.insert_into_leaf(leaf, off, stored);
        if live {
            self.len += 1;
        }
        while let Some(sib) = sibling {
            match self.nodes[child].parent {
                Some(parent) => {
                    let idx = self.nodes[parent]
                        .children
                        .iter()
                        .position(|&c| c == child)
                        .unwrap();
                    self.nodes[parent].children.insert(idx + 1, sib);
                    // The sibling's elements were split out of `child`:
                    // the parent's total did not grow by them. The new
                    // element's +1 is already applied by the ancestor
                    // walk above.
                    self.nodes[sib].parent = Some(parent);
                    if self.nodes[parent].children.len() > M {
                        sibling = self.split_internal(parent);
                    } else {
                        sibling = None;
                    }
                    child = parent;
                }
                None => {
                    // Root split: both children become the new root's.
                    let new_root = self.nodes.len();
                    let old_len = self.nodes[self.root].len;
                    self.nodes[self.root].parent = Some(new_root);
                    self.nodes[sib].parent = Some(new_root);
                    self.nodes.push(Node {
                        items: Vec::new(),
                        children: vec![self.root, sib],
                        parent: None,
                        len: old_len + self.nodes[sib].len,
                    });
                    self.root = new_root;
                    sibling = None;
                }
            }
        }
    }

    /// Insert into a leaf; split it if it overflows and return the
    /// right sibling's handle, or `None`. The directory entries of
    /// the moved elements are rewritten to the new leaf. The leaf's
    /// own `len` (live count) is bumped for live inserts only; the
    /// split halves the physical items.
    fn insert_into_leaf(&mut self, leaf: usize, off: usize, stored: Stored<T>) -> Option<usize> {
        let id = stored.id;
        let live = stored.alive;
        let mut n = std::mem::take(&mut self.nodes[leaf]);
        n.items.insert(off, stored);
        if live {
            n.len += 1;
        }
        if n.items.len() <= M {
            self.nodes[leaf] = n;
            self.index.insert(id, leaf);
            return None;
        }
        let split_at = M.div_ceil(2);
        let right_items = n.items.split_off(split_at);
        n.len = n.items.iter().filter(|s| s.alive).count();
        let right_len = right_items.iter().filter(|s| s.alive).count();
        let right = Node {
            items: right_items,
            children: Vec::new(),
            parent: n.parent,
            len: right_len,
        };
        self.nodes[leaf] = n;
        self.nodes.push(right);
        let right_handle = self.nodes.len() - 1;
        for it in &self.nodes[right_handle].items {
            self.index.insert(it.id, right_handle);
        }
        if off < split_at {
            self.index.insert(id, leaf);
        }
        Some(right_handle)
    }

    /// Split an overflowing internal node in half; return the right
    /// sibling's handle.
    fn split_internal(&mut self, node: usize) -> Option<usize> {
        let mut n = std::mem::take(&mut self.nodes[node]);
        let right_children = n.children.split_off(M.div_ceil(2));
        n.len = n.children.iter().map(|&c| self.nodes[c].len).sum();
        let right_len = right_children.iter().map(|&c| self.nodes[c].len).sum();
        let right = Node {
            items: Vec::new(),
            children: right_children,
            parent: n.parent,
            len: right_len,
        };
        self.nodes[node] = n;
        self.nodes.push(right);
        let right_handle = self.nodes.len() - 1;
        // Re-point the moved children at their new parent. The
        // handles are `Copy`, so reading one before writing the
        // other avoids both a clone and a `take`.
        for i in 0..self.nodes[right_handle].children.len() {
            let c = self.nodes[right_handle].children[i];
            self.nodes[c].parent = Some(right_handle);
        }
        Some(right_handle)
    }

    /// Remove an emptied node from its parent chain, propagating
    /// upward; collapse a single-child root.
    fn prune_empty(&mut self, leaf: usize) {
        let mut child = leaf;
        while let Some(parent) = self.nodes[child].parent {
            let idx = self.nodes[parent]
                .children
                .iter()
                .position(|&c| c == child)
                .unwrap();
            self.nodes[parent].children.remove(idx);
            self.nodes[parent].len -= self.nodes[child].len;
            if self.nodes[parent].children.is_empty() {
                child = parent;
                continue;
            }
            break;
        }
        // Root collapse: promote the only child.
        let root = self.root;
        if self.nodes[root].children.len() == 1 {
            let only = self.nodes[root].children[0];
            self.nodes[only].parent = None;
            self.root = only;
        }
    }
}

impl<T, const M: usize> Default for SeqTree<T, M> {
    fn default() -> Self {
        Self::new()
    }
}

/// An iterator over the values of a [`SeqTree`] in index order.
pub struct Iter<'a, T, const M: usize> {
    tree: &'a SeqTree<T, M>,
    stack: Vec<usize>,
    items: std::slice::Iter<'a, Stored<T>>,
}

impl<'a, T, const M: usize> Iterator for Iter<'a, T, M> {
    type Item = &'a T;

    fn next(&mut self) -> Option<&'a T> {
        loop {
            for item in self.items.by_ref() {
                if item.alive {
                    return Some(&item.value);
                }
            }
            let node = self.stack.pop()?;
            let n = &self.tree.nodes[node];
            if n.children.is_empty() {
                self.items = n.items.iter();
            } else {
                for &c in n.children.iter().rev() {
                    self.stack.push(c);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A dead id: never inserted by the driver (client 9).
    fn dead() -> ItemId {
        ItemId {
            client_id: 9,
            incarnation: 0,
            seq: 9,
        }
    }

    fn make_id(client: u64, seq: u64) -> ItemId {
        ItemId {
            client_id: client,
            incarnation: 0,
            seq,
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    struct M {
        id: ItemId,
        pos: ItemId,
        v: u8,
    }

    /// Reference `Vec` model with the same semantics.
    fn ref_insert(vec: &mut Vec<M>, anchor: Option<ItemId>, id: ItemId, v: u8) -> bool {
        if vec.iter().any(|m| m.id == id) {
            return false;
        }
        let pos = match anchor {
            None => 0,
            Some(a) => match vec.iter().position(|m| m.id == a) {
                Some(i) => i + 1,
                None => vec.len(),
            },
        };
        vec.insert(pos, M { id, pos: id, v });
        true
    }

    fn ref_remove(vec: &mut Vec<M>, id: ItemId) -> Option<u8> {
        let i = vec.iter().position(|m| m.id == id)?;
        Some(vec.remove(i).v)
    }

    fn ref_move(vec: &mut Vec<M>, item: ItemId, to: Option<ItemId>, move_id: ItemId) -> bool {
        let Some(i) = vec.iter().position(|m| m.id == item) else {
            return false;
        };
        if vec[i].pos == move_id {
            return false;
        }
        if to == Some(item) {
            return false;
        }
        let mut m = vec.remove(i);
        let pos = match to {
            None => 0,
            Some(a) => match vec.iter().position(|x| x.id == a) {
                Some(j) => j + 1,
                None => vec.len(),
            },
        };
        m.pos = move_id;
        vec.insert(pos, m);
        true
    }

    fn check_eq(tree: &SeqTree<u8, 4>, vec: &[M]) {
        assert_eq!(tree.len(), vec.len(), "len drift");
        let tv: Vec<u8> = tree.iter().copied().collect();
        let vv: Vec<u8> = vec.iter().map(|m| m.v).collect();
        assert_eq!(tv, vv, "order drift");
        for (i, m) in vec.iter().enumerate() {
            assert_eq!(tree.at(i), Some(&m.v), "at drift at {i}");
            assert_eq!(tree.id_at(i), Some(m.id), "id_at drift at {i}");
            assert_eq!(tree.get(m.id), Some(&m.v), "get drift for id {i}");
        }
    }

    /// Drive a random operation stream against the reference `Vec`.
    fn run(seed: &[u8]) {
        const M: usize = 4;
        let mut tree: SeqTree<u8, { M }> = SeqTree::new();
        let mut vec: Vec<M> = Vec::new();
        let mut seq = 0u64;
        let mut i = 0;
        while i < seed.len() {
            let kind = seed[i] % 4;
            match kind {
                // Insert after a random anchor (or the head).
                0 => {
                    let anchor = if vec.is_empty() || seed[i].is_multiple_of(4) {
                        None
                    } else {
                        Some(vec[usize::from(seed[i] / 4) % vec.len()].id)
                    };
                    seq += 1;
                    let id = make_id(1, seq);
                    let v = seed[i] / 4;
                    let want = ref_insert(&mut vec, anchor, id, v);
                    let got = tree.insert_after(anchor, id, v);
                    assert_eq!(got, want, "insert idempotence drift");
                    i += 1;
                }
                // Remove a live or dead id.
                1 => {
                    let id = if !vec.is_empty() && seed[i].is_multiple_of(4) {
                        vec[usize::from(seed[i] / 4) % vec.len()].id
                    } else {
                        dead()
                    };
                    let want = ref_remove(&mut vec, id);
                    let got = tree.remove(id);
                    assert_eq!(got, want.is_some(), "remove drift");
                    i += 1;
                }
                // Move a live element to a random anchor.
                2 => {
                    if vec.is_empty() {
                        i += 1;
                        continue;
                    }
                    let item = vec[usize::from(seed[i] / 4) % vec.len()].id;
                    let to = if seed[i].is_multiple_of(4) {
                        None
                    } else {
                        Some(vec[usize::from(seed[i] / 4) % vec.len()].id)
                    };
                    seq += 1;
                    let move_id = make_id(2, seq);
                    let want = ref_move(&mut vec, item, to, move_id);
                    let got = tree.move_after(item, to, move_id);
                    assert_eq!(got, want, "move drift");
                    i += 1;
                }
                // Insert after a dead anchor: clamps to the tail.
                _ => {
                    seq += 1;
                    let id = make_id(1, seq);
                    let v = seed[i] / 4;
                    let want = ref_insert(&mut vec, Some(dead()), id, v);
                    let got = tree.insert_after(Some(dead()), id, v);
                    assert_eq!(got, want, "dead-anchor insert drift");
                    i += 1;
                }
            }
            check_eq(&tree, &vec);
        }
    }

    proptest::proptest! {
        #[test]
        fn tree_matches_vec(seed in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..400)) {
            run(&seed);
        }
    }

    #[test]
    fn root_split_and_collapse() {
        let mut tree: SeqTree<u8, 4> = SeqTree::new();
        for i in 0..100 {
            let anchor = if i == 0 {
                None
            } else {
                Some(make_id(1, i as u64))
            };
            assert!(tree.insert_after(anchor, make_id(1, i as u64 + 1), i as u8));
        }
        assert_eq!(tree.len(), 100);
        let snapshot: Vec<u8> = tree.iter().copied().collect();
        assert_eq!(snapshot.len(), 100);
        for i in (0..100).rev() {
            assert!(tree.remove(make_id(1, i as u64 + 1)), "in-range remove");
        }
        assert!(tree.is_empty());
        // A deleted identity is a tombstone: reuse would collide with
        // the idempotence check, so a fresh insert needs a fresh id.
        assert!(tree.insert_after(None, make_id(1, 101), 7));
        assert_eq!(tree.at(0), Some(&7));
    }

    #[test]
    fn idempotence_and_dead_anchors() {
        let mut tree: SeqTree<u8, 4> = SeqTree::new();
        assert!(tree.insert_after(None, make_id(1, 1), 1));
        assert!(
            !tree.insert_after(None, make_id(1, 1), 2),
            "duplicate insert"
        );
        assert!(tree.insert_after(Some(make_id(1, 1)), make_id(1, 2), 2));
        // Dead anchor clamps to the tail.
        assert!(tree.insert_after(Some(dead()), make_id(1, 3), 3));
        assert_eq!(tree.at(2), Some(&3));
        // Move after itself is a no-op.
        assert!(!tree.move_after(make_id(1, 2), Some(make_id(1, 2)), make_id(2, 1)));
        // Replay of the same move is a no-op.
        assert!(tree.move_after(make_id(1, 2), None, make_id(2, 2)));
        assert!(!tree.move_after(make_id(1, 2), None, make_id(2, 2)));
    }

    #[test]
    fn nodes_roundtrip() {
        let mut tree: SeqTree<u8, 4> = SeqTree::new();
        for i in 0..30u8 {
            let anchor = if i == 0 {
                None
            } else {
                Some(make_id(1, i as u64))
            };
            tree.insert_after(anchor, make_id(1, i as u64 + 1), i);
        }
        assert!(tree.move_after(make_id(1, 10), Some(make_id(1, 3)), make_id(2, 1)));
        let nodes = tree.to_nodes();
        assert_eq!(nodes.len(), 30);
        let rebuilt: SeqTree<u8, 4> = SeqTree::from_nodes(nodes);
        let a: Vec<u8> = tree.iter().copied().collect();
        let b: Vec<u8> = rebuilt.iter().copied().collect();
        assert_eq!(a, b, "roundtrip order drift");
        for i in 0..30 {
            assert_eq!(
                tree.get(make_id(1, i as u64 + 1)),
                rebuilt.get(make_id(1, i as u64 + 1))
            );
        }
    }

    #[test]
    fn tombstone_is_invisible_but_keeps_its_slot() {
        let mut tree: SeqTree<u8, 4> = SeqTree::new();
        tree.insert_after(None, make_id(1, 1), 1);
        tree.insert_after(Some(make_id(1, 1)), make_id(1, 2), 2);
        tree.insert_after(Some(make_id(1, 2)), make_id(1, 3), 3);
        assert!(tree.remove(make_id(1, 2)));
        assert!(!tree.remove(make_id(1, 2)), "second delete is a no-op");
        // Invisible: live count, iteration, index and identity lookup.
        assert_eq!(tree.len(), 2);
        let vals: Vec<u8> = tree.iter().copied().collect();
        assert_eq!(vals, vec![1, 3]);
        assert_eq!(tree.at(1), Some(&3));
        assert_eq!(tree.id_at(1), Some(make_id(1, 3)));
        assert_eq!(tree.get(make_id(1, 2)), None);
        // The directory still resolves the tombstone: an insert
        // anchored on it lands at its position, not the tail.
        assert!(tree.insert_after(Some(make_id(1, 2)), make_id(1, 4), 4));
        let vals: Vec<u8> = tree.iter().copied().collect();
        assert_eq!(vals, vec![1, 4, 3]);
        assert_eq!(tree.len(), 3);
    }

    #[test]
    fn tombstoned_element_cannot_be_moved_or_updated() {
        let mut tree: SeqTree<u8, 4> = SeqTree::new();
        tree.insert_after(None, make_id(1, 1), 1);
        tree.insert_after(Some(make_id(1, 1)), make_id(1, 2), 2);
        assert!(tree.remove(make_id(1, 1)));
        assert!(
            !tree.move_after(make_id(1, 1), None, make_id(2, 1)),
            "move of a tombstone"
        );
        assert_eq!(
            tree.update_value(make_id(1, 1), 9),
            None,
            "update of a tombstone"
        );
        let vals: Vec<u8> = tree.iter().copied().collect();
        assert_eq!(vals, vec![2]);
    }

    #[test]
    fn wire_keeps_tombstones_and_anchoring() {
        let mut tree: SeqTree<u8, 4> = SeqTree::new();
        tree.insert_after(None, make_id(1, 1), 1);
        tree.insert_after(Some(make_id(1, 1)), make_id(1, 2), 2);
        tree.insert_after(Some(make_id(1, 2)), make_id(1, 3), 3);
        assert!(tree.remove(make_id(1, 2)));
        let nodes = tree.to_nodes();
        assert_eq!(nodes.len(), 3, "tombstone stays in the wire form");
        assert!(!nodes[1].alive, "the middle node is marked dead");
        // The rebuilt tree keeps the slot: an insert anchored on the
        // tombstone lands between 1 and 3, exactly as before the save.
        let mut rebuilt: SeqTree<u8, 4> = SeqTree::from_nodes(nodes);
        assert_eq!(rebuilt.len(), 2);
        assert!(rebuilt.insert_after(Some(make_id(1, 2)), make_id(1, 4), 4));
        let vals: Vec<u8> = rebuilt.iter().copied().collect();
        assert_eq!(vals, vec![1, 4, 3]);
    }
}
