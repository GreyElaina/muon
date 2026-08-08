//! Mixed operation vocabularies in a single `Changes` stream.
//!
//! A self-contained test: the vocabularies (`TextOp`, `CounterOp`), the
//! vocabulary observers, the aggregating sink, and the derived models all
//! live in this file. Only `muon` and `muon-derive` are used — the sync
//! layer's `Edit` vocabulary is not involved.
//!
//! What this file verifies:
//!
//! - A vocabulary observer declares `QuasiSink::Operation = <its own op>`
//!   and feeds ops into any sink whose vocabulary accepts them via
//!   `Into` (the aggregator pattern: a derived composite observer
//!   declares `Operation = Sk::Operation`, and per-field predicates gate
//!   each field's vocabulary against the sink's).
//! - A single stream carries `Replace` and several `Inplace` vocabularies
//!   interleaved in event order.
//! - An operation that cannot be expressed (a wholesale `tracked_mut`
//!   write) degrades to a whole-value `Replace`.
//! - Vocabularies join the core whole-value diff domain through
//!   `From<X> for ()`, so `ObserveSink` absorbs them.
//!
//! Nested vocabulary models are supported because each vocabulary
//! observer carries its sink conversion inside `QuasiSink`. Composite
//! observers only propagate sink capability through their fields.

use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

use muon::helper::shallow::ObserverState;
use muon::helper::{AsDeref, AsDerefMut, Invalidate, Pointer, QuasiObserver, Succ, Unsigned, Zero};
use muon::observe::{
    DefaultSpec, Emit, Flush, FlushWith, ObserveExt, ObserveSink, Observer, QuasiSink, Sink,
};
use muon::{Change, Changed, Changes, Observe, Path, PathSegment};
use serde::Serialize;

// ── Vocabularies ────────────────────────────────────────────────────

/// The op vocabulary of [`Text`]: tail-only operations.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
enum TextOp {
    /// Append a string to the tail.
    Append { value: String },
    /// Truncate to a length (the removed tail is kept for undo).
    Truncate { len: usize, removed: String },
}

/// The op vocabulary of [`Counter`]: whole-value arithmetic.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
enum CounterOp {
    /// Add to the value.
    Add { by: i32 },
    /// Reset to zero.
    Reset,
}

/// The sink-side union vocabulary of this test.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
enum MixedOp {
    /// A [`Text`] operation.
    Text(TextOp),
    /// A [`Counter`] operation.
    Counter(CounterOp),
}

impl From<TextOp> for MixedOp {
    fn from(op: TextOp) -> Self {
        MixedOp::Text(op)
    }
}

impl From<CounterOp> for MixedOp {
    fn from(op: CounterOp) -> Self {
        MixedOp::Counter(op)
    }
}

// The vocabularies join the core whole-value diff domain: `()`-sinks
// absorb them, exactly as the sync layer's `From<Edit> for ()`.
impl From<TextOp> for () {
    fn from(_: TextOp) {}
}

impl From<CounterOp> for () {
    fn from(_: CounterOp) {}
}

// ── The aggregating sink ────────────────────────────────────────────

/// Collects observation events as a `Changes<MixedOp, ()>` stream.
#[derive(Default)]
struct MixedSink {
    path: Vec<PathSegment<()>>,
    changes: Vec<Change<MixedOp, ()>>,
}

impl MixedSink {
    fn new() -> Self {
        Self::default()
    }

    fn into_changes(self) -> Changes<MixedOp, ()> {
        Changes {
            inner: self.changes,
        }
    }
}

impl Sink for MixedSink {
    type Operation = MixedOp;
    type Identity = ();

    fn push_field(&mut self, name: &str) {
        self.path.push(PathSegment::String(name.to_owned()));
    }

    fn push_index(&mut self, index: usize) {
        self.path.push(PathSegment::Positive(index));
    }

    fn push_neg_index(&mut self, index: usize) {
        self.path.push(PathSegment::Negative(index));
    }

    fn push_identity<I: Into<Self::Identity>>(&mut self, _id: I) {}

    fn pop_segment(&mut self) {
        self.path.pop();
    }

    fn replace(
        &mut self,
        before: Option<&dyn muon::erased_serde::Serialize>,
        after: Option<&dyn muon::erased_serde::Serialize>,
    ) {
        let serialize = |v: &dyn muon::erased_serde::Serialize| {
            muon::serde_json::to_value(v).expect("serialization cannot fail")
        };
        self.changes.push(Change {
            path: Path::from(self.path.clone()),
            changed: Changed::Replace {
                before: before.map(serialize),
                after: after.map(serialize),
            },
        });
    }

    fn inplace<O: Into<Self::Operation>>(&mut self, op: O) {
        self.changes.push(Change {
            path: Path::from(self.path.clone()),
            changed: Changed::Inplace(op.into()),
        });
    }
}

// ── The vocabulary observer ─────────────────────────────────────────

/// Tracking state of a vocabulary observer: recorded ops plus a
/// replace fallback (a wholesale mutation invalidates the ops).
struct OpState<Op> {
    ops: Vec<Op>,
    replaced: bool,
    /// Pre-write snapshot, captured at observe time and refreshed at
    /// every flush. Serves as the `Replace.before`.
    snapshot: Option<muon::serde_json::Value>,
}

impl<T: ?Sized, Op> Invalidate<T> for OpState<Op> {
    fn invalidate(&mut self, _: &T) {
        self.replaced = true;
        self.ops.clear();
    }
}

impl<T: ?Sized + Serialize, Op> ObserverState<T> for OpState<Op> {
    fn observe(value: &T) -> Self {
        Self {
            ops: Vec::new(),
            replaced: false,
            snapshot: Some(muon::serde_json::to_value(value).expect("snapshot serializes")),
        }
    }
}

impl<Op> OpState<Op> {
    fn flush<T: ?Sized + Serialize, S: Sink + ?Sized, Ob>(&mut self, value: &T, sink: &mut S)
    where
        Ob: Emit<S> + QuasiSink<S, Operation = Op>,
    {
        if self.replaced {
            let before = self.snapshot.take();
            let after = muon::serde_json::to_value(value).expect("serialization cannot fail");
            self.snapshot = Some(after.clone());
            sink.replace(
                before
                    .as_ref()
                    .map(|v| v as &dyn muon::erased_serde::Serialize),
                Some(&after as &dyn muon::erased_serde::Serialize),
            );
            return;
        }
        for op in std::mem::take(&mut self.ops) {
            <Ob as Emit<S>>::emit(sink, op);
        }
    }
}

/// A generic vocabulary observer: tracks [`OpState`] and reports
/// recorded ops as `inplace` events (or a whole-value `Replace` when
/// the state was invalidated). `Op` is the observer's declared
/// production vocabulary; `T` is the observed type.
struct OpObserver<'ob, T: ?Sized, Op, S: ?Sized, D = Zero> {
    ptr: Pointer<S>,
    state: OpState<Op>,
    phantom: PhantomData<(&'ob mut D, *const T)>,
}

impl<'ob, T: ?Sized, Op, S: ?Sized, D> Deref for OpObserver<'ob, T, Op, S, D> {
    type Target = Pointer<S>;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

impl<'ob, T: ?Sized, Op, S: ?Sized, D> DerefMut for OpObserver<'ob, T, Op, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = T>,
    OpState<Op>: Invalidate<T>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        std::ptr::from_mut(self).expose_provenance();
        QuasiObserver::invalidate(&mut self.ptr);
        &mut self.ptr
    }
}

impl<'ob, T: ?Sized, Op, S: ?Sized, D> QuasiObserver for OpObserver<'ob, T, Op, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = T>,
    OpState<Op>: Invalidate<T>,
{
    type Head = S;
    type OuterDepth = Succ<Zero>;
    type InnerDepth = D;

    fn invalidate(this: &mut Self) {
        Invalidate::invalidate(&mut this.state, (*this.ptr).as_deref());
    }
}

impl<'ob, T: ?Sized, Op, S: ?Sized, D> Observer for OpObserver<'ob, T, Op, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = T>,
    OpState<Op>: ObserverState<T>,
{
    unsafe fn observe(head: *mut Self::Head) -> Self {
        unsafe {
            let this = Self {
                state: ObserverState::observe((&*head).as_deref()),
                ptr: Pointer::new_unchecked(head),
                phantom: PhantomData,
            };
            Pointer::register_state::<_, D>(&this.ptr, &this.state);
            this
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Self::Head) {
        unsafe { Pointer::set_unchecked(this, head) }
    }
}

impl<'ob, T: ?Sized, Op, S: ?Sized, D, Sk: Sink + ?Sized> QuasiSink<Sk>
    for OpObserver<'ob, T, Op, S, D>
{
    type Operation = Op;
    type Identity = Sk::Identity;
}

impl<'ob, T: ?Sized, Op, S: ?Sized, D, Sk: Sink + ?Sized> Emit<Sk> for OpObserver<'ob, T, Op, S, D>
where
    Op: Into<Sk::Operation>,
{
    fn emit(sink: &mut Sk, operation: Self::Operation) {
        sink.inplace(operation);
    }

    fn emit_identity(sink: &mut Sk, identity: Self::Identity) {
        sink.push_identity(identity);
    }
}

impl<'ob, T: ?Sized + Serialize, Op, S: ?Sized, D, Sk: Sink + ?Sized> Flush<Sk>
    for OpObserver<'ob, T, Op, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = T>,
    Self: Emit<Sk> + QuasiSink<Sk, Operation = Op>,
{
    fn flush(this: &mut Self, sink: &mut Sk) {
        this.state
            .flush::<T, Sk, Self>((*this.ptr).as_deref(), sink);
    }
}

impl<'ob, T: ?Sized + Serialize, Op, S: ?Sized, D, Sk: Sink + ?Sized, Elem: ?Sized>
    FlushWith<Sk, Elem> for OpObserver<'ob, T, Op, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = T>,
    Self: Emit<Sk> + QuasiSink<Sk, Operation = Op>,
{
    fn flush_with<F>(this: &mut Self, sink: &mut Sk, _flush_elem: F)
    where
        F: FnMut(&mut Elem, &mut Sk),
    {
        <Self as Flush<Sk>>::flush(this, sink)
    }
}

// ── Observed types ──────────────────────────────────────────────────

/// A text value observed at the `TextOp` vocabulary.
#[derive(Clone, Debug, PartialEq, Serialize)]
struct Text(String);

impl Text {
    fn new(value: &str) -> Self {
        Self(value.to_owned())
    }
}

/// A counter observed at the `CounterOp` vocabulary.
#[derive(Clone, Debug, PartialEq, Serialize)]
struct Counter(i32);

impl Counter {
    fn new(value: i32) -> Self {
        Self(value)
    }
}

impl Observe for Text {
    type Observer<'ob, S, D>
        = OpObserver<'ob, Text, TextOp, S, D>
    where
        Self: 'ob + Serialize,
        D: Unsigned,
        S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

    type Spec = DefaultSpec;
}

impl Observe for Counter {
    type Observer<'ob, S, D>
        = OpObserver<'ob, Counter, CounterOp, S, D>
    where
        Self: 'ob + Serialize,
        D: Unsigned,
        S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

    type Spec = DefaultSpec;
}

// ── Vocabulary-specific methods ─────────────────────────────────────

impl<'ob> OpObserver<'ob, Text, TextOp, Text, Zero> {
    /// Append a string: a precise `TextOp::Append`.
    fn append(&mut self, s: &str) {
        self.untracked_mut().0.push_str(s);
        self.state.ops.push(TextOp::Append {
            value: s.to_owned(),
        });
    }

    /// Truncate to a length: a precise `TextOp::Truncate`.
    fn truncate(&mut self, len: usize) {
        let value = self.untracked_mut();
        let removed = value.0[len..].to_owned();
        value.0.truncate(len);
        self.state.ops.push(TextOp::Truncate { len, removed });
    }
}

impl<'ob> OpObserver<'ob, Counter, CounterOp, Counter, Zero> {
    /// Add to the value: a precise `CounterOp::Add`.
    fn add(&mut self, by: i32) {
        self.untracked_mut().0 += by;
        self.state.ops.push(CounterOp::Add { by });
    }

    /// Reset the value: a precise `CounterOp::Reset`.
    fn reset(&mut self) {
        self.untracked_mut().0 = 0;
        self.state.ops.push(CounterOp::Reset);
    }
}

// ── Derived models ──────────────────────────────────────────────────

/// A model whose fields carry two different op vocabularies plus a
/// plain (vocabulary-free) field.
#[derive(Serialize, Observe)]
struct Model {
    text: Text,
    counter: Counter,
    plain: String,
}

#[derive(Serialize, Observe)]
struct Inner {
    text: Text,
}

#[derive(Serialize, Observe)]
struct Outer {
    inner: Inner,
    counter: Counter,
}

// ── Tests ───────────────────────────────────────────────────────────

/// Flush `ob` into a fresh `MixedSink` and return the stream.
fn flush_mixed<Ob>(ob: &mut Ob) -> Changes<MixedOp, ()>
where
    Ob: Flush<MixedSink>,
{
    let mut sink = MixedSink::new();
    Flush::flush(ob, &mut sink);
    sink.into_changes()
}

/// Two vocabularies and a `Replace` coexist in one stream, interleaved
/// in event order.
#[test]
fn mixed_vocabularies_in_one_stream() {
    let mut model = Model {
        text: Text::new("hi"),
        counter: Counter::new(3),
        plain: "p".to_owned(),
    };
    let mut ob = model.__observe();
    ob.text.append("!");
    ob.counter.add(2);
    ob.plain.push_str("x");

    let changes = flush_mixed(&mut ob);
    assert_eq!(
        changes,
        Changes {
            inner: vec![
                Change {
                    path: Path::from(vec![PathSegment::String("text".into())]),
                    changed: Changed::Inplace(MixedOp::Text(TextOp::Append {
                        value: "!".to_owned()
                    })),
                },
                Change {
                    path: Path::from(vec![PathSegment::String("counter".into())]),
                    changed: Changed::Inplace(MixedOp::Counter(CounterOp::Add { by: 2 })),
                },
                Change {
                    path: Path::from(vec![PathSegment::String("plain".into())]),
                    changed: Changed::Replace {
                        before: Some(muon::serde_json::json!("p")),
                        after: Some(muon::serde_json::json!("px")),
                    },
                },
            ],
        }
    );
}

#[test]
fn nested_vocabularies_in_one_stream() {
    let mut model = Outer {
        inner: Inner {
            text: Text::new("hi"),
        },
        counter: Counter::new(3),
    };
    let mut ob = model.__observe();
    ob.inner.text.append("!");
    ob.counter.add(2);

    let changes = flush_mixed(&mut ob);
    assert_eq!(
        changes,
        Changes {
            inner: vec![
                Change {
                    path: Path::from(vec![
                        PathSegment::String("inner".into()),
                        PathSegment::String("text".into()),
                    ]),
                    changed: Changed::Inplace(MixedOp::Text(TextOp::Append {
                        value: "!".to_owned(),
                    })),
                },
                Change {
                    path: Path::from(vec![PathSegment::String("counter".into())]),
                    changed: Changed::Inplace(MixedOp::Counter(CounterOp::Add { by: 2 })),
                },
            ],
        }
    );
}

/// A sequence of operations from one vocabulary stays ordered, and a
/// second vocabulary's ops interleave with it.
#[test]
fn op_sequences_interleave() {
    let mut model = Model {
        text: Text::new("hello"),
        counter: Counter::new(1),
        plain: "p".to_owned(),
    };
    let mut ob = model.__observe();
    ob.text.truncate(3); // "hel" (removed "lo")
    ob.counter.reset();
    ob.text.append("!"); // "hel!"
    ob.counter.add(5);

    let changes = flush_mixed(&mut ob);
    assert_eq!(
        changes,
        Changes {
            inner: vec![
                Change {
                    path: Path::from(vec![PathSegment::String("text".into())]),
                    changed: Changed::Inplace(MixedOp::Text(TextOp::Truncate {
                        len: 3,
                        removed: "lo".to_owned()
                    })),
                },
                Change {
                    path: Path::from(vec![PathSegment::String("text".into())]),
                    changed: Changed::Inplace(MixedOp::Text(TextOp::Append {
                        value: "!".to_owned()
                    })),
                },
                Change {
                    path: Path::from(vec![PathSegment::String("counter".into())]),
                    changed: Changed::Inplace(MixedOp::Counter(CounterOp::Reset)),
                },
                Change {
                    path: Path::from(vec![PathSegment::String("counter".into())]),
                    changed: Changed::Inplace(MixedOp::Counter(CounterOp::Add { by: 5 })),
                },
            ],
        }
    );
}

/// A wholesale mutation (via `tracked_mut`) degrades to a whole-value
/// `Replace`: granular ops are dropped and the fallback carries the
/// full before/after payload.
#[test]
fn fallback_degrades_to_replace() {
    let mut model = Model {
        text: Text::new("hi"),
        counter: Counter::new(3),
        plain: "p".to_owned(),
    };
    let mut ob = model.__observe();
    ob.text.append("!"); // recorded, then invalidated by the fallback
    *ob.text.tracked_mut() = Text::new("yo");
    ob.counter.add(2);

    let changes = flush_mixed(&mut ob);
    assert_eq!(
        changes,
        Changes {
            inner: vec![
                Change {
                    path: Path::from(vec![PathSegment::String("text".into())]),
                    changed: Changed::Replace {
                        before: Some(muon::serde_json::json!("hi")),
                        after: Some(muon::serde_json::json!("yo")),
                    },
                },
                Change {
                    path: Path::from(vec![PathSegment::String("counter".into())]),
                    changed: Changed::Inplace(MixedOp::Counter(CounterOp::Add { by: 2 })),
                },
            ],
        }
    );
}

/// Vocabularies join the core diff domain via `From<X> for ()`: an
/// `ObserveSink` absorbs vocabulary events, leaving only the plain
/// field's `Replace`.
#[test]
fn core_domain_absorbs_vocabularies() {
    let mut model = Model {
        text: Text::new("hi"),
        counter: Counter::new(3),
        plain: "p".to_owned(),
    };
    let mut ob = model.__observe();
    ob.text.append("!");
    ob.counter.add(2);
    ob.plain.push_str("x");

    let mut sink = ObserveSink::new();
    Flush::flush(&mut ob, &mut sink);
    let changes = sink.into_changes();
    assert_eq!(changes.inner.len(), 1);
    assert_eq!(
        changes.inner[0].path,
        Path::from(vec![PathSegment::String("plain".into())])
    );
    assert_eq!(
        changes.inner[0].changed,
        Changed::Replace {
            before: Some(muon::serde_json::json!("p")),
            after: Some(muon::serde_json::json!("px")),
        }
    );
}
