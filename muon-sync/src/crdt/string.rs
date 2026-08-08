//! Text container: characters plus style-anchor intervals on the
//! arena sequence engine.
//!
//! A `CrdtString<M>` is a [`MovableVec`] of [`Segment`] elements —
//! characters and zero-width style anchors. The style value `M` is
//! an application-defined model (any muon observable/tracked type:
//! `Serialize + Clone + PartialEq` suffices). Anchors are ordinary
//! sequence elements with identities: deletion keeps their slots
//! (tombstones), so intervals follow character edits naturally, and
//! every edit — including `annotate` — lowers to the existing
//! sequence vocabulary (`Edit::Insert`/`Edit::Delete`), so undo,
//! rebase and server application need no text-specific machinery.
//!
//! Interval semantics (Peritext-style anchors, fixed `After`):
//! - `annotate(range, style)` inserts `AnchorStart(style)` before the
//!   range's first character and `AnchorEnd(style)` before the
//!   range's excluded end — the interval covers `[start, end)`.
//! - `unmark(range, style)` inserts `ClearStart(style)`/`ClearEnd(style)`
//!   instead; synthesis removes intervals whose style equals
//!   (`PartialEq`) the cleared style.
//! - Character deletion never removes anchors: intervals shrink with
//!   the text (anchors keep their slots as tombstones).

use crate::crdt::seq::MovableVec;
use crate::crdt::seq::SeqNode;
use crate::{Edit, ItemId, ItemRange};
use muon::helper::{AsDeref, AsDerefMut, Invalidate, Pointer, QuasiObserver, Succ, Unsigned, Zero};
use muon::observe::{Emit, Flush, FlushWith, Observe, Observer, QuasiSink, Sink};
use serde::de::Deserialize;
use serde::ser::Serialize;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut, RangeBounds};

/// One segment of a text container: a character or a zero-width
/// style anchor.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Segment<M> {
    /// A character.
    Char(char),
    /// The start of a style interval carrying the style value.
    AnchorStart(M),
    /// The end of a style interval (paired with its `AnchorStart`).
    AnchorEnd(M),
    /// The start of a clearing interval: styles equal to `M` are
    /// suppressed inside it.
    ClearStart(M),
    /// The end of a clearing interval (paired with its `ClearStart`).
    ClearEnd(M),
}

impl<M> Segment<M> {
    fn is_char(&self) -> bool {
        matches!(self, Segment::Char(_))
    }
}

/// An identity-aware text container: characters and style anchors on
/// the arena sequence engine.
///
/// [`Clone`] produces an independent session: a fresh random
/// incarnation keeps future identities disjoint from the original's.
/// [`PartialEq`] compares the full sequence state — characters,
/// anchors and styles are data.
pub struct CrdtString<M> {
    vec: MovableVec<Segment<M>>,
    incarnation: u64,
    next_seq: u64,
}

impl<M: PartialEq> PartialEq for CrdtString<M> {
    fn eq(&self, other: &Self) -> bool {
        self.vec == other.vec
    }
}

impl<M: std::fmt::Debug> std::fmt::Debug for CrdtString<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrdtString")
            .field("vec", &self.vec)
            .finish()
    }
}

impl<M: Serialize + Clone + PartialEq + 'static> CrdtString<M> {
    /// Create an empty container.
    pub fn new() -> Self {
        Self {
            vec: MovableVec::new(),
            incarnation: rand::random(),
            next_seq: 1,
        }
    }

    /// The number of characters (anchors excluded).
    pub fn len(&self) -> usize {
        self.vec.visible().filter(|e| e.is_char()).count()
    }

    /// Whether the container has no characters.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The plain text: characters in order, anchors and styles
    /// stripped.
    pub fn text(&self) -> String {
        self.vec
            .visible()
            .filter_map(|e| match e {
                Segment::Char(c) => Some(*c),
                _ => None,
            })
            .collect()
    }

    /// The styled runs: `(text, styles)` per run, in position order.
    /// Adjacent characters with identical style sets merge into one
    /// run.
    ///
    /// Synthesis walks the visible elements once. `AnchorStart` opens
    /// a style, `AnchorEnd` closes its most recent matching open;
    /// `ClearStart` suppresses every matching open style inside the
    /// clearing interval and remembers whether one was suppressed, so
    /// `ClearEnd` can re-open it when the clearing interval passes.
    pub fn spans(&self) -> Vec<(String, Vec<M>)> {
        let mut runs: Vec<(String, Vec<M>)> = Vec::new();
        let mut active: Vec<M> = Vec::new();
        // Clearing stack: (style, was it active when the clear began).
        let mut clears: Vec<(M, bool)> = Vec::new();
        for item in self.vec.visible() {
            match item {
                Segment::Char(c) => match runs.last_mut() {
                    Some((text, styles)) if *styles == active => text.push(*c),
                    _ => runs.push((c.to_string(), active.clone())),
                },
                Segment::AnchorStart(style) => active.push(style.clone()),
                Segment::AnchorEnd(style) => {
                    if let Some(i) = active.iter().position(|s| s == style) {
                        active.remove(i);
                    }
                }
                Segment::ClearStart(style) => {
                    let was_active = active.iter().any(|s| s == style);
                    active.retain(|s| s != style);
                    clears.push((style.clone(), was_active));
                }
                Segment::ClearEnd(style) => {
                    if let Some((cleared, was_active)) = clears.pop() {
                        if was_active && cleared == *style {
                            active.push(cleared);
                        }
                    }
                }
            }
        }
        runs
    }

    /// The styles active at character position `char_pos` (the empty
    /// set for a position past the tail).
    pub fn styles_at(&self, char_pos: usize) -> Vec<M> {
        let mut seen = 0usize;
        let mut active: Vec<M> = Vec::new();
        let mut clears: Vec<(M, bool)> = Vec::new();
        for item in self.vec.visible() {
            match item {
                Segment::Char(_) => {
                    if seen == char_pos {
                        return active;
                    }
                    seen += 1;
                }
                Segment::AnchorStart(style) => active.push(style.clone()),
                Segment::AnchorEnd(style) => {
                    if let Some(i) = active.iter().position(|s| s == style) {
                        active.remove(i);
                    }
                }
                Segment::ClearStart(style) => {
                    let was_active = active.iter().any(|s| s == style);
                    active.retain(|s| s != style);
                    clears.push((style.clone(), was_active));
                }
                Segment::ClearEnd(style) => {
                    if let Some((cleared, was_active)) = clears.pop() {
                        if was_active && cleared == *style {
                            active.push(cleared);
                        }
                    }
                }
            }
        }
        Vec::new()
    }

    /// Insert `s` at character position `index` (plain data
    /// operation, no recording).
    pub fn insert(&mut self, index: usize, s: &str) {
        let _ = self.insert_inner(index, s);
    }

    /// Delete the characters in `range` (plain data operation, no
    /// recording). Style anchors keep their slots.
    pub fn delete(&mut self, range: impl RangeBounds<usize>) {
        let _ = self.delete_inner(range);
    }

    /// Apply `style` to the characters in `range` (plain data
    /// operation, no recording).
    pub fn annotate(&mut self, range: impl RangeBounds<usize>, style: M) {
        self.insert_anchors(
            range,
            Segment::AnchorStart(style.clone()),
            Segment::AnchorEnd(style),
        );
    }

    /// Clear `style` from the characters in `range` (plain data
    /// operation, no recording).
    pub fn unmark(&mut self, range: impl RangeBounds<usize>, style: M) {
        self.insert_anchors(
            range,
            Segment::ClearStart(style.clone()),
            Segment::ClearEnd(style),
        );
    }

    /// Insert a run of characters; returns `(created, anchor)`.
    pub(crate) fn insert_inner(
        &mut self,
        index: usize,
        s: &str,
    ) -> Option<(ItemId, Option<ItemId>)> {
        let chars: Vec<Segment<M>> = s.chars().map(Segment::Char).collect();
        if chars.is_empty() {
            return None;
        }
        let len = chars.len() as u32;
        let id = self.alloc_id();
        // A run consumes `len` consecutive sequence numbers; `alloc`
        // advanced by one.
        self.next_seq = self
            .next_seq
            .checked_add(len as u64 - 1)
            .expect("element seq space exhausted");
        let anchor = self.char_anchor(index);
        crate::crdt::seq::insert_after(&mut self.vec, anchor, ItemRange { first: id, len }, chars);
        Some((id, anchor))
    }

    /// Delete the characters in `range`; returns the recorded form
    /// `(anchor, targets, chars)` for the caller to record, if any.
    pub(crate) fn delete_inner(
        &mut self,
        range: impl RangeBounds<usize>,
    ) -> Option<(Option<ItemId>, Vec<ItemRange>, String)> {
        let len = self.len();
        let start = char_start(&range, len);
        let end = char_end(&range, len);
        if start >= end {
            return None;
        }
        let mut ids: Vec<ItemId> = Vec::new();
        let mut chars: Vec<char> = Vec::new();
        let mut first_anchor: Option<ItemId> = None;
        let mut seen = 0usize;
        for (i, item) in self.vec.visible().enumerate() {
            let is_char = item.is_char();
            if is_char && seen >= start && seen < end {
                if first_anchor.is_none() {
                    first_anchor = (i > 0).then(|| self.vec.id_at(i - 1)).flatten();
                }
                if let Segment::Char(c) = item {
                    chars.push(*c);
                }
                if let Some(id) = self.vec.id_at(i) {
                    ids.push(id);
                }
            }
            if is_char {
                seen += 1;
                if seen >= end {
                    break;
                }
            }
        }
        if ids.is_empty() {
            return None;
        }
        let mut targets: Vec<ItemRange> = Vec::new();
        for id in ids {
            match targets.last_mut() {
                Some(r)
                    if r.first.seq + r.len as u64 == id.seq
                        && r.first.client_id == id.client_id
                        && r.first.incarnation == id.incarnation =>
                {
                    r.len += 1;
                }
                _ => targets.push(ItemRange { first: id, len: 1 }),
            }
        }
        crate::crdt::seq::delete_by_id(&mut self.vec, &targets);
        Some((first_anchor, targets, chars.iter().collect()))
    }

    /// Insert a paired `start`/`end` anchor around a character range.
    /// Returns the recorded forms `(id, anchor, element)` of the end
    /// and start anchors, in insertion order.
    fn insert_anchors(
        &mut self,
        range: impl RangeBounds<usize>,
        start: Segment<M>,
        end: Segment<M>,
    ) -> Vec<(ItemId, Option<ItemId>, Segment<M>)> {
        let len = self.len();
        let start_pos = char_start(&range, len);
        let end_pos = char_end(&range, len);
        if start_pos >= end_pos {
            return Vec::new();
        }
        // The end anchor goes before the range's excluded end; the
        // start anchor before its first character (fixed `After`).
        let end_id = self.alloc_id();
        let end_anchor = self.char_anchor(end_pos);
        crate::crdt::seq::insert_after(
            &mut self.vec,
            end_anchor,
            ItemRange {
                first: end_id,
                len: 1,
            },
            vec![end.clone()],
        );
        let start_id = self.alloc_id();
        let start_anchor = self.char_anchor(start_pos);
        crate::crdt::seq::insert_after(
            &mut self.vec,
            start_anchor,
            ItemRange {
                first: start_id,
                len: 1,
            },
            vec![start.clone()],
        );
        vec![(end_id, end_anchor, end), (start_id, start_anchor, start)]
    }

    /// Allocate the next element identity in this container's own
    /// space.
    fn alloc_id(&mut self) -> ItemId {
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

    /// The anchor an element at `char_pos` follows: `None` at the
    /// head, otherwise the id of the visible element before the
    /// `char_pos`-th character (anchors included in the count).
    fn char_anchor(&self, char_pos: usize) -> Option<ItemId> {
        let mut seen = 0usize;
        for (i, item) in self.vec.visible().enumerate() {
            if item.is_char() {
                if seen >= char_pos {
                    return (i > 0).then(|| self.vec.id_at(i - 1)).flatten();
                }
                seen += 1;
            }
        }
        // Past the tail: anchor on the last visible element.
        let n = self.vec.len();
        (n > 0).then(|| self.vec.id_at(n - 1)).flatten()
    }
}

impl<M: Serialize + Clone + PartialEq + 'static> Default for CrdtString<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: Serialize + Clone + PartialEq + 'static> Clone for CrdtString<M> {
    fn clone(&self) -> Self {
        // A clone is an independent session: a fresh random
        // incarnation keeps future identities disjoint.
        Self {
            vec: self.vec.clone(),
            incarnation: rand::random(),
            next_seq: self.next_seq,
        }
    }
}

impl<M: Serialize + PartialEq + 'static> Serialize for CrdtString<M> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.vec.to_nodes_ref().serialize(serializer)
    }
}

impl<'de, M: Deserialize<'de> + Clone + 'static> Deserialize<'de> for CrdtString<M> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let nodes = Vec::<SeqNode<Segment<M>>>::deserialize(deserializer)?;
        let next_seq = nodes
            .iter()
            .filter_map(|n| n.id.seq.checked_add(1))
            .max()
            .unwrap_or(1);
        Ok(Self {
            vec: MovableVec::from_nodes(nodes),
            incarnation: rand::random(),
            next_seq,
        })
    }
}

/// Recording state: the operation stream plus a whole-replacement
/// marker (set by invalidation — a wholesale assignment, or a parent
/// observer's cascade).
#[derive(Default)]
pub(crate) struct CrdtStringObserverState {
    /// The recorded operations, in recording order.
    pub(crate) ops: Vec<Edit>,
    /// Whether the container was replaced wholesale.
    pub(crate) replaced: bool,
    /// The pre-replacement node array, captured at invalidation.
    pub(crate) replaced_before: Option<serde_json::Value>,
}

impl<M: Serialize + PartialEq + 'static> Invalidate<CrdtString<M>> for CrdtStringObserverState {
    fn invalidate(&mut self, value: &CrdtString<M>) {
        self.replaced = true;
        self.ops.clear();
        self.replaced_before = Some(
            serde_json::to_value(value.vec.to_nodes_ref())
                .expect("container serialization cannot fail"),
        );
    }
}

/// The observer of a [`CrdtString`]: the operation entry point inside
/// a `track!` closure.
pub struct CrdtStringObserver<'ob, M, S: ?Sized, D = Zero> {
    ptr: Pointer<S>,
    state: CrdtStringObserverState,
    phantom: PhantomData<(&'ob mut D, M)>,
}

impl<M: Serialize + PartialEq + 'static> Observe for CrdtString<M> {
    type Observer<'ob, S, D>
        = CrdtStringObserver<'ob, M, S, D>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

    type Spec = muon::observe::DefaultSpec;
}

/// A [`CrdtString`] is a tracked (writable) model field: the store's
/// tracked-write path accepts it as a whole. The style value `M` is
/// an atomic payload — no field-level tracking inside it.
impl<M> muon_store::Track for CrdtString<M> where M: Serialize + Clone + PartialEq + 'static {}

impl<'ob, M, S: ?Sized, D> Deref for CrdtStringObserver<'ob, M, S, D> {
    type Target = Pointer<S>;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

impl<'ob, M, S: ?Sized, D> DerefMut for CrdtStringObserver<'ob, M, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = CrdtString<M>>,
    M: Serialize + PartialEq + 'static,
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

impl<'ob, M, S: ?Sized, D> QuasiObserver for CrdtStringObserver<'ob, M, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = CrdtString<M>>,
    M: Serialize + PartialEq + 'static,
{
    type Head = S;
    type OuterDepth = Succ<Zero>;
    type InnerDepth = D;

    fn invalidate(this: &mut Self) {
        Invalidate::invalidate(&mut this.state, (*this.ptr).as_deref());
    }
}

impl<'ob, M, S: ?Sized, D> Observer for CrdtStringObserver<'ob, M, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = CrdtString<M>>,
    M: Serialize + PartialEq + 'static,
{
    unsafe fn observe(head: *mut Self::Head) -> Self {
        unsafe {
            let this = Self {
                ptr: Pointer::new_unchecked(head),
                state: CrdtStringObserverState::default(),
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

impl<'ob, M, S: ?Sized, D, Sk: Sink + ?Sized> QuasiSink<Sk> for CrdtStringObserver<'ob, M, S, D> {
    type Operation = Edit;
    type Identity = Sk::Identity;
}

impl<'ob, M, S: ?Sized, D, Sk: Sink + ?Sized> Emit<Sk> for CrdtStringObserver<'ob, M, S, D>
where
    Edit: Into<Sk::Operation>,
{
    fn emit(sink: &mut Sk, operation: Self::Operation) {
        sink.inplace(operation);
    }

    fn emit_identity(sink: &mut Sk, identity: Self::Identity) {
        sink.push_identity(identity);
    }
}

// The delegated form cuts the recursion cycle: CrdtString has no
// element observers, so the callback is ignored and the form is
// equivalent to the complete form.
impl<'ob, M, S: ?Sized, D, Sk: Sink + ?Sized, Elem: ?Sized> FlushWith<Sk, Elem>
    for CrdtStringObserver<'ob, M, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = CrdtString<M>>,
    M: Serialize + PartialEq + 'static,
    Self: Emit<Sk> + QuasiSink<Sk, Operation = Edit>,
{
    fn flush_with<F>(this: &mut Self, sink: &mut Sk, _flush_elem: F)
    where
        F: FnMut(&mut Elem, &mut Sk),
    {
        <Self as Flush<Sk>>::flush(this, sink)
    }
}

impl<'ob, M, S: ?Sized, D, Sk: Sink + ?Sized> Flush<Sk> for CrdtStringObserver<'ob, M, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = CrdtString<M>>,
    M: Serialize + PartialEq + 'static,
    Self: Emit<Sk> + QuasiSink<Sk, Operation = Edit>,
{
    fn flush(this: &mut Self, sink: &mut Sk) {
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
        for kind in std::mem::take(&mut this.state.ops) {
            <Self as Emit<Sk>>::emit(sink, kind);
        }
    }
}

impl<'ob, M, S: ?Sized, D> CrdtStringObserver<'ob, M, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = CrdtString<M>>,
    M: Serialize + Clone + PartialEq + 'static,
{
    /// Insert `s` at character position `index`, recording the
    /// insert operation. An empty string is a no-op, like the plain
    /// container API.
    pub fn insert(&mut self, index: usize, s: &str) {
        let Some((id, anchor)) = self.inner_mut().insert_inner(index, s) else {
            return;
        };
        self.state.ops.push(Edit::Insert {
            anchor,
            range: ItemRange {
                first: id,
                len: s.chars().count() as u32,
            },
            // The run's value is a per-element array of the
            // elements' serialized form: the server's sequence
            // application stores each slot verbatim.
            value: Box::new(
                serde_json::to_value(s.chars().map(Segment::<M>::Char).collect::<Vec<_>>())
                    .expect("elements serialize"),
            ),
        });
    }

    /// Delete the characters in `range` (character positions),
    /// recording the delete operation. Style anchors keep their
    /// slots: intervals shrink with the text.
    pub fn delete(&mut self, range: impl RangeBounds<usize>) {
        let Some((anchor, targets, chars)) = self.inner_mut().delete_inner(range) else {
            return;
        };
        self.state.ops.push(Edit::Delete {
            anchor,
            targets,
            // Per-element array of serialized elements, matching the
            // insert run's form (the undo inverse re-inserts it).
            value: Box::new(
                serde_json::to_value(chars.chars().map(Segment::<M>::Char).collect::<Vec<_>>())
                    .expect("elements serialize"),
            ),
        });
    }

    /// Apply `style` to the characters in `range` (character
    /// positions; the interval covers `[start, end)`). Two anchor
    /// elements carry the interval; each lowers to an
    /// `Edit::Insert`.
    pub fn annotate(&mut self, range: impl RangeBounds<usize>, style: M) {
        self.record_anchors(
            range,
            Segment::AnchorStart(style.clone()),
            Segment::AnchorEnd(style),
        );
    }

    /// Clear `style` from the characters in `range`: a clearing
    /// interval suppresses matching styles during synthesis.
    pub fn unmark(&mut self, range: impl RangeBounds<usize>, style: M) {
        self.record_anchors(
            range,
            Segment::ClearStart(style.clone()),
            Segment::ClearEnd(style),
        );
    }

    /// Insert a paired anchor set and record both insertions.
    fn record_anchors(
        &mut self,
        range: impl RangeBounds<usize>,
        start: Segment<M>,
        end: Segment<M>,
    ) {
        let recorded = self.inner_mut().insert_anchors(range, start, end);
        for (id, anchor, item) in recorded {
            self.state.ops.push(Edit::Insert {
                anchor,
                range: ItemRange { first: id, len: 1 },
                value: Box::new(serde_json::to_value(&item).expect("element serializes")),
            });
        }
    }

    fn inner_mut(&mut self) -> &mut CrdtString<M> {
        (*self.ptr).as_deref_mut()
    }
}

/// The inclusive start of a character range, clamped to the length.
fn char_start(range: &impl RangeBounds<usize>, len: usize) -> usize {
    match range.start_bound() {
        std::ops::Bound::Included(i) => (*i).min(len),
        std::ops::Bound::Excluded(i) => (*i + 1).min(len),
        std::ops::Bound::Unbounded => 0,
    }
}

/// The exclusive end of a character range, clamped to the length.
fn char_end(range: &impl RangeBounds<usize>, len: usize) -> usize {
    match range.end_bound() {
        std::ops::Bound::Included(i) => (*i + 1).min(len),
        std::ops::Bound::Excluded(i) => (*i).min(len),
        std::ops::Bound::Unbounded => len,
    }
}
