//! Identity-aware ordered-sequence container: `CrdtVec<T>` plus its
//! observer.
//!
//! The container stores an ordered sequence in the engine's
//! sequence-field format: the two-layer movable state
//! ([`crate::crdt::seq::MovableVec`]), serialized as a JSON array of nodes.
//! Its inherent methods are plain data operations; the observer's
//! methods of the same name apply the operation and record the
//! corresponding identity operation ([`Edit`]) at operation
//! time.

use crate::crdt::seq::MovableVec;
use crate::crdt::seq::SeqNode;
use crate::{Edit, ItemId, ItemRange};
use muon::general::NoopObserver;
use muon::helper::{AsDeref, AsDerefMut, Invalidate, Pointer, QuasiObserver, Succ, Unsigned, Zero};
use muon::observe::{Emit, Flush, FlushWith, Observe, Observer, QuasiSink, Sink};
use serde::de::Deserialize;
use serde::ser::Serialize;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut, Index, IndexMut};

/// The per-element observer of a [`CrdtVec<T>`] element: the standard
/// observer of `T`, additionally flushable into the sync layer's
/// element-operation stream.
///
/// Gated on the concrete `T::Observer` so that containers observed with
/// a non-tracking element observer (the [`NoopObserver`] default) never
/// expose element access — element edits through such an observer would
/// be silently dropped.
pub trait ElementObserver<T>: Observer<Head = T, InnerDepth = Zero> {}

impl<'ob, T> ElementObserver<T> for <T as Observe>::Observer<'ob, T, Zero>
where
    T: Observe + 'ob,
    <T as Observe>::Observer<'ob, T, Zero>: Observer<Head = T, InnerDepth = Zero>,
{
}

/// An ordered sequence container with per-element identities.
///
/// [`PartialEq`] compares the full sequence state — identities,
/// liveness and placement versions are data, not implementation
/// detail.
pub struct CrdtVec<T> {
    vec: MovableVec<T>,
    incarnation: u64,
    next_seq: u64,
}

impl<T: PartialEq> PartialEq for CrdtVec<T> {
    fn eq(&self, other: &Self) -> bool {
        self.vec == other.vec
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for CrdtVec<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrdtVec")
            .field("nodes", &self.vec.to_nodes_ref())
            .finish()
    }
}

impl<T> CrdtVec<T> {
    /// Create an empty container.
    pub fn new() -> Self {
        Self {
            vec: MovableVec::new(),
            incarnation: rand::random(),
            next_seq: 1,
        }
    }

    /// The number of live elements.
    pub fn len(&self) -> usize {
        self.vec.len()
    }

    /// Whether the container has no live elements.
    pub fn is_empty(&self) -> bool {
        self.vec.is_empty()
    }

    /// The live element at `index` (live indexing), if any.
    pub fn get(&self, index: usize) -> Option<&T> {
        // The measured tree resolves a live index in logarithmic
        // time; a linear scan of the visible order would degrade
        // repeated indexing to O(n²).
        self.vec.at(index)
    }

    /// Iterate the live elements in position order.
    pub fn iter(&self) -> impl Iterator<Item = &T> + '_ {
        self.vec.visible()
    }

    /// Append `value` at the end (plain data operation, no recording).
    pub fn push(&mut self, value: T) {
        self.push_inner(value);
    }

    /// Insert `value` at live position `index` (plain data operation,
    /// no recording).
    pub fn insert(&mut self, index: usize, value: T) {
        let _ = self.insert_inner(index, value);
    }

    /// Remove the live element at `index`, returning a clone of it.
    /// The element leaves the live view; the slot stays in the
    /// underlying sequence as a tombstone (the wire form keeps the
    /// dead node), so anchors into it still resolve.
    pub fn remove(&mut self, index: usize) -> Option<T>
    where
        T: Clone,
    {
        self.remove_inner(index).map(|(_, _, value)| value)
    }

    /// Move the live element at `index` to live position `new_index`
    /// (plain data operation, no recording). A no-op when the indices
    /// are equal or the source is out of range; a target past the
    /// end clamps to the tail (the engine's anchor semantics).
    pub fn move_to(&mut self, index: usize, new_index: usize) {
        let _ = self.move_inner(index, new_index);
    }

    /// Replace the live element at `index` with `value` (plain data
    /// operation, no recording). An equal value is a no-op; returns
    /// the previous value.
    pub fn set(&mut self, index: usize, value: T) -> Option<T>
    where
        T: PartialEq + Clone,
    {
        let id = self.vec.id_at(index)?;
        crate::crdt::seq::update_value(&mut self.vec, id, value)
    }

    /// The live element at `index` with its identity (mutable).
    pub(crate) fn at_mut(&mut self, index: usize) -> Option<(ItemId, &mut T)> {
        self.vec.at_mut(index)
    }

    /// The value of a live element by identity, if any (mutable).
    pub(crate) fn value_mut(&mut self, id: ItemId) -> Option<&mut T> {
        self.vec.value_mut(id)
    }

    /// Replace the live element at `index`; returns the identity and
    /// the previous value (`None` for an out-of-range index or an
    /// equal-value no-op).
    pub(crate) fn set_inner(&mut self, index: usize, value: T) -> Option<(ItemId, T)>
    where
        T: PartialEq + Clone,
    {
        let id = self.vec.id_at(index)?;
        let prev = crate::crdt::seq::update_value(&mut self.vec, id, value)?;
        Some((id, prev))
    }

    // ── Internals: operations return their recorded form ─────────────

    /// Append: allocate an identity, insert the element at the end,
    /// return `(created, anchor)`.
    fn push_inner(&mut self, value: T) -> (ItemId, Option<ItemId>) {
        let id = self.alloc();
        let anchor = self.tail_anchor();
        crate::crdt::seq::insert_after(
            &mut self.vec,
            anchor,
            crate::ItemRange { first: id, len: 1 },
            vec![value],
        );
        (id, anchor)
    }

    /// Insert at live position `index`; returns `(created, anchor)`.
    /// `index` may equal the live length (append). An out-of-range
    /// index is a no-op.
    fn insert_inner(&mut self, index: usize, value: T) -> Option<(ItemId, Option<ItemId>)> {
        let n = self.vec.len();
        if index > n {
            return None; // out of range
        }
        // The anchor is the element the new one follows: the tail for
        // an append, the predecessor otherwise, `None` at the head.
        let anchor = if index == n {
            self.tail_anchor()
        } else if index == 0 {
            None
        } else {
            Some(self.vec.id_at(index - 1)?)
        };
        let id = self.alloc();
        crate::crdt::seq::insert_after(
            &mut self.vec,
            anchor,
            crate::ItemRange { first: id, len: 1 },
            vec![value],
        );
        Some((id, anchor))
    }

    /// Tombstone the live element at `index`, returning its identity,
    /// the anchor it followed and a clone of its value.
    fn remove_inner(&mut self, index: usize) -> Option<(ItemId, Option<ItemId>, T)>
    where
        T: Clone,
    {
        let id = self.vec.id_at(index)?;
        let anchor = if index == 0 {
            None
        } else {
            Some(self.vec.id_at(index - 1)?)
        };
        let value = self.vec.at(index)?.clone();
        crate::crdt::seq::delete_by_id(&mut self.vec, &[crate::ItemRange { first: id, len: 1 }]);
        Some((id, anchor, value))
    }

    /// Move the live element at `index` to live position `new_index`,
    /// returning `(item, to, from_anchor, pos)`. `None` for a no-op
    /// or an out-of-range index.
    fn move_inner(
        &mut self,
        index: usize,
        new_index: usize,
    ) -> Option<(ItemId, Option<ItemId>, Option<ItemId>, ItemId)> {
        if index == new_index {
            return None;
        }
        let item = self.vec.id_at(index)?;
        let from_anchor = if index == 0 {
            None
        } else {
            Some(self.vec.id_at(index - 1)?)
        };
        // The target position in the post-removal view: the element's
        // own slot shifts the count by one when it lay before the
        // target.
        let target = if index < new_index {
            new_index + 1
        } else {
            new_index
        };
        let to = if target == 0 {
            None
        } else if target - 1 < self.vec.len() {
            Some(self.vec.id_at(target - 1)?)
        } else {
            self.tail_anchor() // out of range: clamp to the tail
        };
        let pos = self.alloc();
        crate::crdt::seq::move_after(&mut self.vec, item, to, pos);
        Some((item, to, from_anchor, pos))
    }

    // ── Identity allocation ───────────────────────────────────────────

    /// The id of the last live element, or `None` when empty.
    fn tail_anchor(&self) -> Option<ItemId> {
        let n = self.vec.len();
        (n > 0).then(|| self.vec.id_at(n - 1).expect("non-empty"))
    }

    /// Allocate the next element identity in this container's own
    /// space: the container's random incarnation plus a monotonic
    /// element sequence. `client_id` is always 0 — the engine's client
    /// namespace applies to transactions, never to elements (the
    /// server validates a transaction's client, not its elements').
    fn alloc(&mut self) -> ItemId {
        let seq = self.next_seq;
        self.next_seq = self
            .next_seq
            .checked_add(1)
            .expect("element seq space exhausted");
        ItemId {
            client_id: 0,
            incarnation: self.incarnation,
            seq,
        }
    }
}

impl<T: Clone> Clone for CrdtVec<T> {
    fn clone(&self) -> Self {
        // A clone is an independent container: a fresh random
        // incarnation keeps its future identities disjoint from the
        // original's, even though the element sequence resumes at the
        // same counter.
        Self {
            vec: self.vec.clone(),
            incarnation: rand::random(),
            next_seq: self.next_seq,
        }
    }
}

impl<T> Default for CrdtVec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Index<usize> for CrdtVec<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).expect("index out of bounds")
    }
}

impl<T: Serialize> Serialize for CrdtVec<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.vec.to_nodes_ref().serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for CrdtVec<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let nodes = Vec::<SeqNode<T>>::deserialize(deserializer)?;
        let next_seq = nodes
            .iter()
            .map(|n| n.id.seq)
            .max()
            .map_or(1, |seq| seq.saturating_add(1));
        Ok(Self {
            vec: MovableVec::from_nodes(nodes),
            incarnation: rand::random(),
            next_seq,
        })
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Observer
// ═══════════════════════════════════════════════════════════════════════

/// Records structural operations on a [`CrdtVec`] at operation time.
///
/// The observer's inherent methods ([`push`](Self::push),
/// [`insert`](Self::insert), [`remove`](Self::remove),
/// [`move_to`](Self::move_to), [`set`](Self::set)) apply the operation
/// to the container through the pointer and record the corresponding
/// [`Edit`]; they take precedence over the deref chain, so an
/// observation body's `doc.blocks.push(x)` is intercepted here. A
/// whole-field assignment (`doc.blocks = new`) is rewritten by the
/// `observe!` macro into a `tracked_mut` call, which marks the
/// observer; the next flush then emits a whole-field
/// [`Changed::Replace`](crate::Changed::Replace) with the pre-replacement node array (captured
/// at invalidation) and the new node array read from the container.
///
/// Element access ([`Index`], [`IndexMut`], [`get`](Self::get),
/// [`get_mut`](Self::get_mut), [`iter_mut`](Self::iter_mut)) hands out
/// the element's own observer (`T::Observer`), created lazily and
/// cached in the state; a container operation discards the cached
/// observer of the touched element (its pending field edits are
/// subsumed by the structural operation). The element observer is
/// relocated by identity before every use, so arena reallocation never
/// leaves it dangling.
///
/// [`flush`](Self::flush) serializes element payloads (recorded
/// operation values were serialized at operation time; the whole-field
/// path serializes the container's nodes), flushes the cached element
/// observers with an `ItemId` path prefix, and fully resets the
/// recording state, per the valid-state invariant.
pub struct CrdtVecObserver<'ob, T, S: ?Sized, D = Zero, O = NoopObserver<'ob, T, T, Zero>> {
    ptr: Pointer<S>,
    state: CrdtVecObserverState<O>,
    phantom: PhantomData<(&'ob mut D, T)>,
}

/// Recording state: the operation stream, the cached per-element
/// observers, and a whole-replacement marker (set by invalidation — a
/// wholesale assignment, or a parent observer's cascade).
pub(crate) struct CrdtVecObserverState<O> {
    /// The recorded operations, in recording order.
    pub(crate) ops: Vec<Edit>,
    /// Cached per-element observers, keyed by element identity.
    /// Lazily created; discarded for removed or replaced elements;
    /// cleared wholesale at invalidation.
    pub(crate) elements: UnsafeCell<Vec<(ItemId, O)>>,
    /// Whether the container was replaced wholesale.
    pub(crate) replaced: bool,
    /// The pre-replacement node array, captured at invalidation.
    pub(crate) replaced_before: Option<serde_json::Value>,
}

impl<O> Default for CrdtVecObserverState<O> {
    fn default() -> Self {
        Self {
            ops: Vec::new(),
            elements: UnsafeCell::new(Vec::new()),
            replaced: false,
            replaced_before: None,
        }
    }
}

impl<T, O> Invalidate<CrdtVec<T>> for CrdtVecObserverState<O>
where
    T: Serialize,
{
    fn invalidate(&mut self, value: &CrdtVec<T>) {
        // A wholesale replacement subsumes every recorded operation
        // and every pending element edit; the pre-replacement node
        // array becomes the `Replace.before`.
        self.replaced = true;
        self.ops.clear();
        self.elements.get_mut().clear();
        self.replaced_before = Some(
            serde_json::to_value(value.vec.to_nodes_ref())
                .expect("container serialization cannot fail"),
        );
    }
}

impl<'ob, T, S: ?Sized, D, O> Deref for CrdtVecObserver<'ob, T, S, D, O> {
    type Target = Pointer<S>;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

impl<'ob, T, S: ?Sized, D, O> DerefMut for CrdtVecObserver<'ob, T, S, D, O>
where
    D: Unsigned,
    S: AsDeref<D, Target = CrdtVec<T>>,
    T: Serialize,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        std::ptr::from_mut(self).expose_provenance();
        // Invalidate the registered fallback entries, then mark
        // ourselves: a raw deref-mut through the pointer is an
        // unobserved wholesale modification.
        QuasiObserver::invalidate(&mut self.ptr);
        Invalidate::invalidate(&mut self.state, (*self.ptr).as_deref());
        &mut self.ptr
    }
}

impl<'ob, T, S: ?Sized, D, O> QuasiObserver for CrdtVecObserver<'ob, T, S, D, O>
where
    D: Unsigned,
    S: AsDeref<D, Target = CrdtVec<T>>,
    T: Serialize,
{
    type Head = S;
    type OuterDepth = Succ<Zero>;
    type InnerDepth = D;

    fn invalidate(this: &mut Self) {
        Invalidate::invalidate(&mut this.state, (*this.ptr).as_deref());
    }
}

impl<'ob, T, S: ?Sized, D, O> Observer for CrdtVecObserver<'ob, T, S, D, O>
where
    D: Unsigned,
    S: AsDeref<D, Target = CrdtVec<T>>,
    T: Serialize,
{
    unsafe fn observe(head: *mut Self::Head) -> Self {
        unsafe {
            let this = Self {
                ptr: Pointer::new_unchecked(head),
                state: CrdtVecObserverState::default(),
                phantom: PhantomData,
            };
            Pointer::register_state::<_, D>(&this.ptr, &this.state);
            this
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Self::Head) {
        unsafe { Pointer::set_unchecked(this, head) };
    }
}

impl<'ob, T, S: ?Sized, D, O, Sk: Sink + ?Sized> QuasiSink<Sk>
    for CrdtVecObserver<'ob, T, S, D, O>
{
    type Operation = Edit;
    type Identity = ItemId;
}

impl<'ob, T, S: ?Sized, D, O, Sk: Sink + ?Sized> Emit<Sk> for CrdtVecObserver<'ob, T, S, D, O>
where
    Edit: Into<Sk::Operation>,
    ItemId: Into<Sk::Identity>,
{
    fn emit(sink: &mut Sk, operation: Self::Operation) {
        sink.inplace(operation);
    }

    fn emit_identity(sink: &mut Sk, identity: Self::Identity) {
        sink.push_identity(identity);
    }
}

// The delegated form cuts the recursion cycle: the element flush
// obligation moves into the caller-supplied closure, so the impl head
// carries only the local vocabulary gates.
impl<'ob, T, S: ?Sized, D, O, Sk: Sink + ?Sized> FlushWith<Sk, O>
    for CrdtVecObserver<'ob, T, S, D, O>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = CrdtVec<T>>,
    T: Serialize,
    O: Observer<InnerDepth = Zero, Head = T>,
    Self: Emit<Sk> + QuasiSink<Sk, Operation = Edit, Identity = ItemId>,
{
    fn flush_with<F>(this: &mut Self, sink: &mut Sk, mut flush_elem: F)
    where
        F: FnMut(&mut O, &mut Sk),
    {
        if this.state.replaced {
            let before = this.state.replaced_before.take();
            let after = (*this.ptr).as_deref().vec.to_nodes_ref();
            this.state.replaced = false;
            this.state.ops.clear();
            sink.replace(
                before.as_ref().map(|v| v as &dyn erased_serde::Serialize),
                Some(&after as &dyn erased_serde::Serialize),
            );
            return;
        }
        // Structural operations first, then element changes: the two
        // touch disjoint aspects (sequence structure vs element
        // values), so the ordering does not affect convergence.
        for kind in std::mem::take(&mut this.state.ops) {
            <Self as Emit<Sk>>::emit(sink, kind);
        }
        for (id, mut ob) in std::mem::take(this.state.elements.get_mut()) {
            // Relocate by identity: arena reallocation may have moved
            // the element since the observer was cached. A dead
            // element (removed since) is dropped — the delete op
            // carries its final payload.
            let ptr: Option<*mut T> = unsafe { Pointer::as_mut(&this.ptr) }
                .as_deref_mut()
                .value_mut(id)
                .map(|value| value as *mut T);
            let Some(ptr) = ptr else {
                continue;
            };
            unsafe { O::relocate(&mut ob, ptr) };
            <Self as Emit<Sk>>::emit_identity(sink, id);
            flush_elem(&mut ob, sink);
            sink.pop_segment();
        }
    }
}

impl<'ob, T, S: ?Sized, D, O, Sk: Sink + ?Sized> Flush<Sk> for CrdtVecObserver<'ob, T, S, D, O>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = CrdtVec<T>>,
    T: Serialize,
    O: Observer<InnerDepth = Zero, Head = T> + Flush<Sk>,
    Self: Emit<Sk> + QuasiSink<Sk, Operation = Edit, Identity = ItemId> + FlushWith<Sk, O>,
{
    fn flush(this: &mut Self, sink: &mut Sk) {
        // Delegate to the closure form: the two impls differ only in
        // the element obligation, so one body serves both.
        <Self as FlushWith<Sk, O>>::flush_with(this, sink, |elem, sink| {
            <O as Flush<Sk>>::flush(elem, sink)
        });
    }
}

impl<'ob, T, S: ?Sized, D, O> CrdtVecObserver<'ob, T, S, D, O>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = CrdtVec<T>>,
{
    /// Append `value` at the end and record an [`Edit::Insert`].
    pub fn push(&mut self, value: T)
    where
        T: Serialize + Clone,
    {
        let json = serde_json::to_value(&value).expect("element serialization cannot fail");
        let (id, anchor) = {
            let vec: &mut CrdtVec<T> = self.untracked_mut();
            vec.push_inner(value)
        };
        self.state.ops.push(Edit::Insert {
            anchor,
            range: ItemRange { first: id, len: 1 },
            value: Box::new(json),
        });
    }

    /// Insert `value` at live position `index` and record an
    /// [`Edit::Insert`] at the insertion's op position.
    pub fn insert(&mut self, index: usize, value: T)
    where
        T: Serialize + Clone,
    {
        let json = serde_json::to_value(&value).expect("element serialization cannot fail");
        let Some((id, anchor)) = ({
            let vec: &mut CrdtVec<T> = self.untracked_mut();
            vec.insert_inner(index, value)
        }) else {
            return;
        };
        self.state.ops.push(Edit::Insert {
            anchor,
            range: ItemRange { first: id, len: 1 },
            value: Box::new(json),
        });
    }

    /// Remove the live element at `index` and record an
    /// [`Edit::Delete`]. The deleted payload is kept for
    /// undo; the returned clone is the caller's view of the removed
    /// value.
    pub fn remove(&mut self, index: usize) -> Option<T>
    where
        T: Serialize + Clone,
    {
        let (id, anchor, value) = {
            let vec: &mut CrdtVec<T> = self.untracked_mut();
            vec.remove_inner(index)?
        };
        // The element is gone: its cached observer (and any pending
        // field edits) is subsumed by the delete's payload.
        self.state.elements.get_mut().retain(|(eid, _)| *eid != id);
        let payload = serde_json::to_value(&value).expect("element serialization cannot fail");
        self.state.ops.push(Edit::Delete {
            anchor,
            targets: vec![ItemRange { first: id, len: 1 }],
            value: Box::new(payload),
        });
        Some(value)
    }

    /// Move the live element at `index` to live position `new_index`
    /// and record an [`Edit::Move`]. A no-op when the indices are
    /// equal or the source is out of range; a target past the end
    /// clamps to the tail (the engine's anchor semantics).
    pub fn move_to(&mut self, index: usize, new_index: usize)
    where
        T: Serialize + Clone,
    {
        let Some((item, to, from_anchor, pos)) = ({
            let vec: &mut CrdtVec<T> = self.untracked_mut();
            vec.move_inner(index, new_index)
        }) else {
            return;
        };
        self.state.ops.push(Edit::Move {
            item,
            to,
            from_anchor,
            pos,
        });
    }
}

impl<'ob, T, S: ?Sized, D, O> CrdtVecObserver<'ob, T, S, D, O>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = CrdtVec<T>>,
    T: Observe + Serialize + Clone + 'ob,
    O: ElementObserver<T>,
{
    /// The live element at `index` as its observer; created lazily.
    pub fn get(&self, index: usize) -> Option<&O> {
        let (id, value) = unsafe { Pointer::as_mut(&self.ptr) }
            .as_deref_mut()
            .at_mut(index)?;
        let elements = unsafe { &mut *self.state.elements.get() };
        match elements.iter().position(|(eid, _)| *eid == id) {
            Some(pos) => {
                let ob = &mut elements[pos].1;
                unsafe { O::relocate(ob, value as *mut T) }
                Some(ob)
            }
            None => {
                elements.push((id, unsafe { O::observe(value) }));
                let (_, ob) = elements.last_mut().unwrap();
                Some(ob)
            }
        }
    }

    /// The live element at `index` as its observer; created lazily.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut O> {
        let (id, value) = unsafe { Pointer::as_mut(&self.ptr) }
            .as_deref_mut()
            .at_mut(index)?;
        let elements = self.state.elements.get_mut();
        match elements.iter().position(|(eid, _)| *eid == id) {
            Some(pos) => {
                let ob = &mut elements[pos].1;
                unsafe { O::relocate(ob, value as *mut T) }
                Some(ob)
            }
            None => {
                elements.push((id, unsafe { O::observe(value) }));
                let (_, ob) = elements.last_mut().unwrap();
                Some(ob)
            }
        }
    }

    /// Replace the live element at `index` with `value` and record an
    /// [`Edit::Update`] (per-element last-writer-wins). An equal value
    /// is a no-op; returns the previous value.
    pub fn set(&mut self, index: usize, value: T) -> Option<T>
    where
        T: PartialEq,
    {
        let json = serde_json::to_value(&value).expect("element serialization cannot fail");
        let (id, prev) = {
            let vec: &mut CrdtVec<T> = self.untracked_mut();
            vec.set_inner(index, value)?
        };
        // The element's value was replaced wholesale: its cached
        // observer (and any pending field edits) is subsumed by the
        // update.
        self.state.elements.get_mut().retain(|(eid, _)| *eid != id);
        self.state.ops.push(Edit::Update {
            id,
            prev: Box::new(serde_json::to_value(&prev).expect("element serialization cannot fail")),
            value: Box::new(json),
        });
        Some(prev)
    }

    /// Iterate every live element as its observer, ensuring each has
    /// one. The container's structure is frozen while the iterator
    /// lives.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut O> + '_ {
        let n = {
            let vec: &mut CrdtVec<T> = self.untracked_mut();
            vec.len()
        };
        for index in 0..n {
            let _ = self.get_mut(index);
        }
        self.state.elements.get_mut().iter_mut().map(|(_, ob)| ob)
    }
}

impl<'ob, T, S: ?Sized, D, O> Index<usize> for CrdtVecObserver<'ob, T, S, D, O>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = CrdtVec<T>>,
    T: Observe + Serialize + Clone + 'ob,
    O: ElementObserver<T>,
{
    type Output = O;

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).expect("index out of bounds")
    }
}

impl<'ob, T, S: ?Sized, D, O> IndexMut<usize> for CrdtVecObserver<'ob, T, S, D, O>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = CrdtVec<T>>,
    T: Observe + Serialize + Clone + 'ob,
    O: ElementObserver<T>,
{
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.get_mut(index).expect("index out of bounds")
    }
}

impl<'ob, T, S: ?Sized, D, O> std::fmt::Debug for CrdtVecObserver<'ob, T, S, D, O>
where
    D: Unsigned,
    S: AsDeref<D, Target = CrdtVec<T>>,
    T: std::fmt::Debug + Clone + Serialize,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CrdtVecObserver")
            .field(&QuasiObserver::untracked_ref(self))
            .finish()
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Observe / MutationOf
// ═══════════════════════════════════════════════════════════════════════

impl<T> Observe for CrdtVec<T>
where
    T: Serialize + Clone + Observe,
{
    type Observer<'ob, S, D>
        = CrdtVecObserver<'ob, T, S, D, <T as Observe>::Observer<'ob, T, Zero>>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

    type Spec = muon::observe::DefaultSpec;
}

/// A [`CrdtVec`] is a tracked (writable) model field: the store's
/// tracked-write path accepts it as a whole.
impl<T> muon_store::Track for CrdtVec<T> where T: muon_store::Track + Serialize {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::ops::{apply_inplace_value, apply_txn_to_value};
    use crate::TxnId;
    use muon::helper::QuasiObserver;
    use muon::observe::ObserveExt;
    use muon::Observe;
    use muon::{Changed, PathSegment};
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    /// A composite element: LWW fields only (the element-access path
    /// produces field-level replaces).
    #[derive(Serialize, Deserialize, Observe, Clone, Debug, PartialEq)]
    struct Item {
        label: String,
        done: bool,
    }

    fn ids() -> impl FnMut() -> TxnId {
        let mut next = 1u64;
        move || {
            let id = TxnId {
                incarnation: 1,
                seq: next,
            };
            next += 1;
            id
        }
    }

    #[test]
    fn element_field_edit_records_member_prefixed_replace() {
        let mut vec = CrdtVec::new();
        vec.push(Item {
            label: "a".into(),
            done: false,
        });
        let mut ob = vec.__observe();
        *ob[0].label.tracked_mut() = "b".into();
        let changes = crate::__sync_flush!(&mut ob);
        assert_eq!(changes.inner.len(), 1);
        let change = &changes.inner[0];
        assert!(matches!(change.changed, Changed::Replace { .. }));
        assert!(matches!(&change.path[0], PathSegment::Identity(_)));
        match &change.path[1] {
            PathSegment::String(s) => assert_eq!(s, "label"),
            _ => panic!("unexpected second segment"),
        }
    }

    #[test]
    fn element_read_through_index_does_not_record() {
        let mut vec = CrdtVec::new();
        vec.push(Item {
            label: "a".into(),
            done: false,
        });
        let mut ob = vec.__observe();
        assert_eq!(*ob[0].label.untracked_ref(), "a");
        assert!(crate::__sync_flush!(&mut ob).is_empty());
    }

    #[test]
    fn set_records_update_op() {
        let mut vec = CrdtVec::new();
        vec.push(Item {
            label: "a".into(),
            done: false,
        });
        let mut ob = vec.__observe();
        ob.set(
            0,
            Item {
                label: "b".into(),
                done: true,
            },
        )
        .expect("in-range set");
        let changes = crate::__sync_flush!(&mut ob);
        assert_eq!(changes.inner.len(), 1);
        match &changes.inner[0].changed {
            Changed::Inplace(Edit::Update { prev, value, .. }) => {
                assert_eq!(prev.as_ref(), &json!({ "label": "a", "done": false }));
                assert_eq!(value.as_ref(), &json!({ "label": "b", "done": true }));
            }
            _ => panic!("set must record an update"),
        }
    }

    #[test]
    fn equal_set_is_a_noop() {
        let mut vec = CrdtVec::new();
        vec.push(Item {
            label: "a".into(),
            done: false,
        });
        let mut ob = vec.__observe();
        let value = Item {
            label: "a".into(),
            done: false,
        };
        assert_eq!(ob.set(0, value), None, "equal value: no-op");
        assert!(crate::__sync_flush!(&mut ob).is_empty());
    }

    #[test]
    fn remove_subsumes_pending_element_edits() {
        let mut vec = CrdtVec::new();
        vec.push(Item {
            label: "a".into(),
            done: false,
        });
        let mut ob = vec.__observe();
        *ob[0].label.tracked_mut() = "b".into();
        ob.remove(0).expect("in-range remove");
        let changes = crate::__sync_flush!(&mut ob);
        assert_eq!(changes.inner.len(), 1, "the field edit is subsumed");
        match &changes.inner[0].changed {
            Changed::Inplace(Edit::Delete { value, .. }) => {
                assert_eq!(&**value, &json!({ "label": "b", "done": false }));
            }
            _ => panic!("remove must record a delete"),
        }
    }

    #[test]
    fn set_discards_cached_element_observer() {
        let mut vec = CrdtVec::new();
        vec.push(Item {
            label: "a".into(),
            done: false,
        });
        let mut ob = vec.__observe();
        *ob[0].label.tracked_mut() = "b".into();
        ob.set(
            0,
            Item {
                label: "c".into(),
                done: true,
            },
        )
        .expect("in-range set");
        let changes = crate::__sync_flush!(&mut ob);
        assert_eq!(changes.inner.len(), 1, "the field edit is subsumed");
        match &changes.inner[0].changed {
            Changed::Inplace(Edit::Update { value, .. }) => {
                assert_eq!(&**value, &json!({ "label": "c", "done": true }));
            }
            _ => panic!("set must record an update"),
        }
    }

    #[test]
    fn move_to_keeps_element_observer_alive() {
        let mut vec = CrdtVec::new();
        vec.push(Item {
            label: "a".into(),
            done: false,
        });
        vec.push(Item {
            label: "b".into(),
            done: false,
        });
        let mut ob = vec.__observe();
        *ob[0].label.tracked_mut() = "x".into();
        ob.move_to(0, 1);
        let changes = crate::__sync_flush!(&mut ob);
        assert_eq!(changes.inner.len(), 2);
        assert!(matches!(
            changes.inner[0].changed,
            Changed::Inplace(Edit::Move { .. })
        ));
        assert!(matches!(changes.inner[1].changed, Changed::Replace { .. }));
        match &changes.inner[1].path[0] {
            PathSegment::Identity(_) => {}
            _ => panic!("element change carries the identity prefix"),
        }
    }

    #[test]
    fn flush_drops_observers_of_elements_removed_outside_the_observer() {
        let mut vec = CrdtVec::new();
        vec.push(Item {
            label: "a".into(),
            done: false,
        });
        let mut ob = vec.__observe();
        *ob[0].label.tracked_mut() = "b".into();
        // Mutate through the raw container: the cached observer now
        // points at a dead element; flush must drop it, not flush it.
        ob.untracked_mut().remove(0);
        assert!(crate::__sync_flush!(&mut ob).is_empty());
    }

    #[test]
    fn iter_mut_visits_every_element() {
        let mut vec = CrdtVec::new();
        vec.push(Item {
            label: "a".into(),
            done: false,
        });
        vec.push(Item {
            label: "b".into(),
            done: false,
        });
        let mut ob = vec.__observe();
        for item in ob.iter_mut() {
            *item.done.tracked_mut() = true;
        }
        let changes = crate::__sync_flush!(&mut ob);
        assert_eq!(changes.inner.len(), 2);
        for change in &changes.inner {
            assert!(matches!(change.changed, Changed::Replace { .. }));
            match &change.path[1] {
                PathSegment::String(s) => assert_eq!(s, "done"),
                _ => panic!("unexpected segment"),
            }
        }
    }

    /// The full loop: element edits lower to transactions with
    /// `Item` path segments, and the server-side application lands
    /// them on the node array's value.
    #[test]
    fn element_edit_applies_to_node_array() {
        let mut vec = CrdtVec::new();
        vec.push(Item {
            label: "a".into(),
            done: false,
        });
        let mut ob = vec.__observe();
        *ob[0].label.tracked_mut() = "b".into();
        *ob[0].done.tracked_mut() = true;
        let changes = crate::__sync_flush!(&mut ob);
        let txns = crate::sync::sink::txns_from_changes(changes, "doc", 0, &mut ids());
        assert_eq!(txns.len(), 2);

        // The node array carries the element the edits address; the
        // element's identity is the member prefix of the edits.
        let id = match &txns[0].path[0] {
            PathSegment::Identity(id) => *id,
            _ => panic!("field edit path starts with the item id"),
        };
        let mut state = json!([{
            "id": json!(id),
            "alive": true,
            "pos": null,
            "value": { "label": "a", "done": false },
        }]);
        for txn in &txns {
            let applied = match &txn.kind {
                crate::Changed::Inplace(kind) => {
                    apply_inplace_value(&mut state, &txn.path, kind).is_ok()
                }
                _ => false,
            };
            if !applied {
                apply_txn_to_value(&mut state, txn);
            }
        }
        let node = &state[0];
        assert_eq!(node["value"]["label"], json!("b"));
        assert_eq!(node["value"]["done"], json!(true));
        // The field edit addresses the element by identity.
        match &txns[0].path[0] {
            PathSegment::Identity(_) => {}
            _ => panic!("field edit path starts with the item id"),
        }
        match &txns[1].path[1] {
            PathSegment::String(f) => assert_eq!(f, "done"),
            _ => panic!("second edit targets the done field"),
        }
    }
}
