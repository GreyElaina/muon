//! Types and traits for observing mutations to data structures.
//!
//! See the [Observer Mechanism](https://github.com/shigma/muon#observer-mechanism) section in
//! the README for a detailed overview of the observer architecture, dereference chains, and
//! mutation tracking primitives.

use crate::general::SnapshotObserver;
use crate::general::snapshot::Snapshot;
pub use crate::general::snapshot::SnapshotSpec;
use crate::helper::{AsDeref, AsDerefMut, Pointer, QuasiObserver, Unsigned, Zero};

/// A trait for observer types that wrap and track mutations to values.
///
/// Observers provide transparent access to the underlying value while recording any mutations that
/// occur. They form a dereference chain that allows multiple levels of observation.
///
/// ## Lifecycle
///
/// - [`observe(head)`](Self::observe) fully initializes the observer: sets up the internal pointer,
///   initializes diff state, and registers any fallback invalidation entries.
/// - [`relocate(this, head)`](Self::relocate) updates the internal pointer after the observed value
///   has moved in memory (e.g., due to [`Vec`] reallocation), keeping diff state intact.
///
/// ## Invariants
///
/// ### Inline-Field Invariant
///
/// Every [`Observer`]'s [`Deref`](std::ops::Deref) target must be an inline field (or nested
/// inline field) — no [`Box`], [`Arc`](std::sync::Arc), or other heap indirection in the deref
/// chain. This ensures that every field within the observer hierarchy has a **fixed byte offset**
/// relative to the [`Pointer<Head>`](Pointer), invariant under moves.
///
/// This property is required by [`Pointer`]'s fallback invalidation mechanism: any observer in
/// the deref chain can register sibling fields with the [`Pointer`] via [`Pointer::register_state`]
/// or [`Pointer::register_observer`] during [`observe`](Observer::observe). The [`Pointer`]
/// accumulates entries from all levels. When [`DerefMut`](std::ops::DerefMut) propagates down to
/// the tail observer, the tail calls [`Pointer::invalidate`](QuasiObserver::invalidate), which
/// iterates all registered `(offset, invalidate_fn)` entries to reach those siblings via
/// offset-based addressing — invalidating siblings across the entire chain in a single pass.
///
/// Since [`&mut Pointer<S>`](Pointer) only has provenance over the [`Pointer`] itself, the
/// offset-based addressing uses the [exposed-provenance](std::ptr#exposed-provenance) API. Every
/// observer that registers siblings must also call `expose_provenance` on `&mut self` in its
/// [`DerefMut`](std::ops::DerefMut) impl, depositing the parent struct's provenance into the
/// global pool.
///
/// ### Valid-State Invariant
///
/// [`QuasiObserver::invalidate`] must fully reset all granular tracking state and clear inner
/// observer storage (dropping or resetting inner observers). This ensures that subsequent
/// [`flush`](Flush::flush) calls cannot produce incorrect mutations from stale
/// tracking state, and that later accesses cannot obtain inner observers carrying stale state.
///
/// In contrast, a stale pointer (e.g., an inner observer pointing to a previous address after
/// container reallocation) is tolerable — it will be repaired by [`relocate`](Observer::relocate)
/// before the next access. Stale state, however, cannot be repaired after the fact, which is why
/// [`QuasiObserver::invalidate`] must eagerly clear it.
///
/// See the [Observer Mechanism](https://github.com/shigma/muon#observer-mechanism) for a
/// detailed overview of the dereference chain and mutation tracking primitives.
pub trait Observer: QuasiObserver<Target = Pointer<<Self as QuasiObserver>::Head>> + Sized {
    /// Creates a new observer for the given value.
    ///
    /// This is the primary way to create an observer. The observer will track all mutations to the
    /// provided value.
    ///
    /// ## Example
    ///
    /// ```
    /// use muon::general::ShallowObserver;
    /// use muon::observe::Observer;
    ///
    /// let mut value = 42;
    /// let observer = unsafe { ShallowObserver::<i32, i32>::observe(&mut value) };
    /// ```
    ///
    /// # Safety
    ///
    /// The caller must ensure that `head` is a valid pointer to the observed value.
    unsafe fn observe(head: *mut Self::Head) -> Self;

    /// Updates the observer's internal pointer after the observed value has moved.
    ///
    /// This method updates the observer's internal pointer to point to the new location
    /// of the observed value. It is necessary when the observed value is relocated in
    /// memory (e.g., due to [`Vec`] reallocation) while the observer remains active.
    ///
    /// ## Guarantee
    ///
    /// After `relocate` returns, the observer's internal [`Pointer`] must hold provenance
    /// compatible with `head`. This ensures that subsequent accesses through the pointer
    /// (e.g., via [`DerefMut`](std::ops::DerefMut)) remain valid.
    ///
    /// ## Safety
    ///
    /// The caller must ensure that `head` refers to the same logical value with which the
    /// observer was initialized, just potentially at a new memory location.
    unsafe fn relocate(this: &mut Self, head: *mut Self::Head);
}

/// The sink protocol for observation events.
///
/// An observer reports domain-agnostic facts by pushing path segments
/// and emitting events; the sink decides how to encode them. The domain
/// of a stream (whole-value diff vs structured operation stream) is a
/// property of the sink, not of the observer.
///
/// [`Sink::Operation`] and [`Sink::Identity`] are the sink's declared production
/// vocabulary: the in-place payload the sink receives is exactly
/// `Operation` after the observer's operation converts into it. Every
/// observation event that carries a payload must convert into the
/// target sink's vocabulary via `Into` — the protocol requires it at
/// the method level.
///
/// Segment methods establish the current path; `replace` and `inplace`
/// report an event at that path. Every `push_*` must be balanced by a
/// matching [`pop_segment`](Sink::pop_segment).
pub trait Sink {
    /// The sink's production vocabulary for in-place operations.
    ///
    /// Sinks that ignore operations (the whole-value diff domain) use
    /// `()`, and each operation vocabulary joins that domain with a
    /// `From<Operation> for ()` impl.
    type Operation;
    /// The sink's production vocabulary for element identity segments.
    type Identity;

    /// Pushes a field segment (object / struct key).
    fn push_field(&mut self, name: &str);
    /// Pushes a positive index segment.
    fn push_index(&mut self, index: usize);
    /// Pushes a negative index segment (1-based from the tail).
    fn push_neg_index(&mut self, index: usize);
    /// Pushes an element identity segment.
    ///
    /// The identity payload must convert into the sink's [`Identity`](Sink::Identity)
    /// vocabulary. Sinks that do not use identities absorb it.
    fn push_identity<I: Into<Self::Identity>>(&mut self, id: I);
    /// Pops the most recently pushed segment.
    fn pop_segment(&mut self);
    /// Reports that the whole value at the current path was replaced.
    ///
    /// The sink decides whether and how to serialize the payloads; an
    /// observer only hands over references to the values.
    fn replace(
        &mut self,
        before: Option<&dyn erased_serde::Serialize>,
        after: Option<&dyn erased_serde::Serialize>,
    );
    /// Reports an in-place container operation at the current path.
    ///
    /// The operation must convert into the sink's [`Operation`](Sink::Operation)
    /// vocabulary. The sink converts inside the method and its logic
    /// sees only its declared vocabulary.
    fn inplace<O: Into<Self::Operation>>(&mut self, op: O);
}

/// The vocabulary description of an observer type for a sink.
///
/// [`QuasiSink::Operation`] and [`QuasiSink::Identity`] declare the
/// concrete payload types that an observer produces. Implementations
/// must remain unconditional when possible, so these projections can
/// reduce in generic derive predicates.
///
/// Observers without a local vocabulary use the sink types directly.
/// Observers with a local vocabulary implement [`Emit`] to
/// provide the actual emission capability for compatible sinks.
pub trait QuasiSink<S: Sink + ?Sized> {
    /// The operation vocabulary this observer produces into `S`.
    type Operation;
    /// The identity payload vocabulary this observer produces into `S`.
    type Identity;
}

/// The capability to emit an observer's vocabulary into a sink.
///
/// Vocabulary-producing observers implement this trait for compatible
/// sinks. The conversion stays inside the implementation, so generic
/// flush code only needs this capability and does not project an
/// `Into` bound through [`QuasiSink`].
pub trait Emit<S: Sink + ?Sized>: QuasiSink<S> {
    /// Emits one operation in the sink's vocabulary.
    fn emit(sink: &mut S, operation: Self::Operation);

    /// Emits one identity segment in the sink's vocabulary.
    fn emit_identity(sink: &mut S, identity: Self::Identity);
}

/// The flush capability of an observer into a specific sink.
///
/// Vocabulary-producing observers place conversion bounds on their
/// [`Emit`] implementations. Their flush implementations
/// consume that capability without restating the conversion bounds.
/// Vocabulary-free observers implement this trait for every sink.
///
/// Flushing must fully reset the observer's state: an immediately
/// subsequent flush with no intervening mutations must report nothing.
/// This invariant applies recursively to all nested observers.
pub trait Flush<S: Sink + ?Sized>: QuasiSink<S> {
    /// Reports all recorded events and fully resets internal state.
    fn flush(this: &mut Self, sink: &mut S);
}

/// The delegated flush capability of an observer into a specific sink.
///
/// `flush_with` performs the observer's own flush but hands the
/// flushing of its elements (the `Elem` type) to the caller-supplied
/// closure. Recursive model observers cut their proof obligations at
/// this callback boundary: the closure body closes over the
/// caller's in-scope axioms, so container impls carry only their local
/// vocabulary gates and no element obligations.
///
/// Observers with no elements implement this trait for any `Elem` and
/// ignore the callback; cut containers (e.g. `CrdtVec`) implement it
/// for `Elem = O` and delegate each element; complete containers keep
/// their element obligation in the impl head and ignore the callback.
pub trait FlushWith<S: Sink + ?Sized, Elem: ?Sized>: QuasiSink<S> {
    /// Reports all recorded events and fully resets internal state,
    /// flushing each element via `flush_elem`.
    fn flush_with<F>(this: &mut Self, sink: &mut S, flush_elem: F)
    where
        F: FnMut(&mut Elem, &mut S);
}

/// A sink that encodes observation events as whole-value diffs
/// ([`Changes<(), ()>`](crate::Changes)).
///
/// This is the core-domain encoding used by `observe!` and
/// `muon-store`: `inplace` events are absorbed (the core domain does
/// not use operation vocabularies), and element identities enter
/// "ignoring" mode — events inside a container are swallowed, because
/// the container itself reports a whole-value replace when it is
/// wholesale-replaced.
///
/// The sink accepts any operation vocabulary: its [`Operation`](Sink::Operation) is
/// `()`, and each operation vocabulary joins the diff domain with a
/// `From<Operation> for ()` impl.
pub struct ObserveSink {
    path: Vec<crate::PathSegment<()>>,
    ignoring: usize,
    changes: crate::Changes<(), ()>,
}

impl ObserveSink {
    /// Creates an empty sink.
    pub fn new() -> Self {
        Self {
            path: Vec::new(),
            ignoring: 0,
            changes: crate::Changes::new(),
        }
    }

    /// Consumes the sink and returns the collected whole-value diff.
    pub fn into_changes(self) -> crate::Changes<(), ()> {
        self.changes
    }
}

impl Default for ObserveSink {
    fn default() -> Self {
        Self::new()
    }
}

impl Sink for ObserveSink {
    type Operation = ();
    type Identity = ();

    fn push_field(&mut self, name: &str) {
        self.path.push(crate::PathSegment::String(name.to_owned()));
    }

    fn push_index(&mut self, index: usize) {
        self.path.push(crate::PathSegment::Positive(index));
    }

    fn push_neg_index(&mut self, index: usize) {
        self.path.push(crate::PathSegment::Negative(index));
    }

    fn push_identity<I: Into<Self::Identity>>(&mut self, _id: I) {
        self.ignoring += 1;
    }

    fn pop_segment(&mut self) {
        if self.ignoring > 0 {
            self.ignoring -= 1;
        } else {
            self.path.pop();
        }
    }

    fn replace(
        &mut self,
        before: Option<&dyn erased_serde::Serialize>,
        after: Option<&dyn erased_serde::Serialize>,
    ) {
        if self.ignoring == 0 {
            let serialize = |v: &dyn erased_serde::Serialize| {
                serde_json::to_value(v).expect("serialization cannot fail")
            };
            self.changes.push(crate::Change {
                path: crate::Path::from(self.path.clone()),
                changed: crate::Changed::Replace {
                    before: before.map(serialize),
                    after: after.map(serialize),
                },
            });
        }
    }

    fn inplace<O: Into<Self::Operation>>(&mut self, _op: O) {
        // The core domain does not encode operation vocabularies.
    }
}

/// The absorbing unit observer: it flushes nothing.
impl<S: Sink + ?Sized> QuasiSink<S> for () {
    type Operation = S::Operation;
    type Identity = S::Identity;
}

impl<S: Sink + ?Sized> Flush<S> for () {
    fn flush(_: &mut (), _: &mut S) {}
}

impl<S: Sink + ?Sized, Elem: ?Sized> FlushWith<S, Elem> for () {
    fn flush_with<F>(_: &mut (), _: &mut S, _: F)
    where
        F: FnMut(&mut Elem, &mut S),
    {
    }
}

/// Default observation specification.
///
/// [`DefaultSpec`] indicates that no special observation behavior is required for the type. For
/// most types, this means they use their standard [`Observer`] implementation. For example, if `T`
/// implements [`Observe`] with `Spec = DefaultSpec`, then [`Option<T>`] will be observed using
/// [`OptionObserver`](crate::impls::OptionObserver) which wraps `T`'s observer.
///
/// All `#[derive(Observe)]` implementations use [`DefaultSpec`] unless overridden with field
/// attributes.
pub struct DefaultSpec;

/// A trait for types that can be observed for mutations.
///
/// Types implementing [`Observe`] can be wrapped in [`Observer`]s that track mutations. The trait
/// is typically derived using the `#[derive(Observe)]` macro and used in `observe!` macros.
///
/// A single type `T` may have many possible [`Observer<'ob, Target = T>`] implementations in
/// theory, each with different change-tracking strategies. The [`Observe`] trait selects one
/// of these as the *default* observer to be used by `#[derive(Observe)]` and other generic code
/// that needs an observer for `T`.
///
/// When you `#[derive(Observe)]` on a struct, the macro requires that each field type
/// implements [`Observe`] so it can select an appropriate default observer for that field.
/// The [`Observer`] associated type of each field's [`Observe`] implementation determines which
/// observer will be instantiated in the generated code.
///
/// ## Example
///
/// ```
/// use muon::{Observe, observe};
/// use serde::Serialize;
/// use serde_json::json;
///
/// #[derive(Serialize, Observe)]
/// struct MyStruct {
///     field: String,
/// }
///
/// let mut data = MyStruct { field: "value".to_string() };
/// let changes = observe!(data => {
///     data.field.push_str(" modified");
/// });
/// assert_eq!(
///     changes.into_json(),
///     json!([{"path": ["field"], "before": "value", "after": "value modified"}]),
/// );
/// ```
pub trait Observe {
    /// The default observer implementation for this type.
    type Observer<'ob, S, D>: Observer<Head = S, InnerDepth = D>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

    /// Marker type for selecting specialized observer implementations in wrapper types.
    ///
    /// For most types, this will be [`DefaultSpec`]. Types can specify [`SnapshotSpec`] to enable
    /// snapshot-based observation strategies. For example, [`Option<T>`] uses
    /// [`OptionObserver`](crate::impls::OptionObserver) when `T::Spec = DefaultSpec`, but
    /// [`crate::general::SnapshotObserver`] when `T::Spec = SnapshotSpec`.
    type Spec;
}

/// Counterpart to [`Observe`] for shared-reference types.
///
/// A type `T` implements [`RoObserve`] if it can be observed through a shared reference (e.g.,
/// `&T`, [`Rc<T>`](std::rc::Rc), [`Arc<T>`](std::sync::Arc)).
///
/// See also: [`Observe`], [`RwObserve`].
pub trait RoObserve {
    /// The default observer implementation for `&Self`.
    type Observer<'ob, S, D>: Observer<Head = S, InnerDepth = D>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDeref<D, Target = Self> + ?Sized + 'ob;

    /// Marker type for selecting specialized observer implementations in wrapper types.
    ///
    /// For most types, this will be [`DefaultSpec`]. Types can specify [`SnapshotSpec`] to enable
    /// snapshot-based observation strategies. For example, [`Option<T>`] uses
    /// [`OptionObserver`](crate::impls::OptionObserver) when `T::Spec = DefaultSpec`, but
    /// [`crate::general::SnapshotObserver`] when `T::Spec = SnapshotSpec`.
    type Spec;
}

/// Counterpart to [`Observe`] for interior-mutable types.
///
/// A type `T` implements [`RwObserve`] if it can be observed through interior mutability (e.g.,
/// [`RefCell<T>`](std::cell::RefCell), [`Mutex<T>`](std::sync::Mutex)).
///
/// A blanket implementation is provided for all types that implement [`Snapshot`], using
/// [`SnapshotObserver`] as the observer.
///
/// See also: [`Observe`], [`RoObserve`].
pub trait RwObserve {
    /// The default observer implementation for interior-mutable wrappers.
    type Observer<'ob, S, D>: Observer<Head = S, InnerDepth = D>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDeref<D, Target = Self> + ?Sized + 'ob;

    /// Marker type for selecting specialized observer implementations in wrapper types.
    type Spec;
}

impl<T: Snapshot> RwObserve for T {
    type Observer<'ob, S, D>
        = SnapshotObserver<'ob, Self, S, D>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDeref<D, Target = Self> + ?Sized + 'ob;

    type Spec = SnapshotSpec;
}

/// Extension trait providing ergonomic methods for types implementing [`Observe`].
///
/// This trait is automatically implemented for all types that implement [`Observe`] and provides a
/// convenient way to create observers without needing to specify type parameters.
///
/// ## Example
///
/// ```
/// use muon::observe::ObserveExt;
///
/// let mut data = 42;
/// let ob = data.__observe();
/// ```
pub trait ObserveExt: Observe {
    /// Creates an observer for this value.
    ///
    /// This is a convenience method that calls [`Observer::observe`] with the appropriate type
    /// parameters automatically inferred.
    fn __observe<'ob>(&'ob mut self) -> Self::Observer<'ob, Self, Zero> {
        unsafe { Observer::observe(self) }
    }
}

impl<T: Observe + ?Sized> ObserveExt for T {}

/// Resolves the concrete [`Observer`] type for a given [`Observe`] type.
///
/// This is a convenience alias used primarily by the derive macro to refer to field observer types
/// without repeating the full associated type syntax.
///
/// ## Type Parameters
///
/// - `T`: The observed type (must implement [`Observe`]).
/// - `S`: The head type stored in the observer's [`Pointer`]. Defaults to `T` (for top-level or
///   struct-field observers where the head is the field itself).
/// - `D`: The [`InnerDepth`](QuasiObserver::InnerDepth). Defaults to [`Zero`] (no extra dereference
///   layers between `S` and `T`).
pub type DefaultObserver<'ob, T, S = T, D = Zero> = <T as Observe>::Observer<'ob, S, D>;

/// Resolves the concrete [`Observer`] type for a given [`RoObserve`] type.
///
/// This is a convenience alias used primarily by the derive macro to refer to field observer types
/// without repeating the full associated type syntax.
///
/// ## Type Parameters
///
/// - `T`: The observed type (must implement [`RoObserve`]).
/// - `S`: The head type stored in the observer's [`Pointer`]. Defaults to `T` (for top-level or
///   struct-field observers where the head is the field itself).
/// - `D`: The [`InnerDepth`](QuasiObserver::InnerDepth). Defaults to [`Zero`] (no extra dereference
///   layers between `S` and `T`).
pub type DefaultRoObserver<'ob, T, S = T, D = Zero> = <T as RoObserve>::Observer<'ob, S, D>;

/// Resolves the concrete [`Observer`] type for a given [`RwObserve`] type.
///
/// ## Type Parameters
///
/// - `T`: The observed type (must implement [`RwObserve`]).
/// - `S`: The head type stored in the observer's [`Pointer`]. Defaults to `T`.
/// - `D`: The [`InnerDepth`](QuasiObserver::InnerDepth). Defaults to [`Zero`].
pub type DefaultRwObserver<'ob, T, S = T, D = Zero> = <T as RwObserve>::Observer<'ob, S, D>;
