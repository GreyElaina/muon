//! Runtime mutation observation and typed dereference traversal.

#[cfg(feature = "alloc")]
pub mod alloc;
pub mod core;
mod depth;
mod deref;
mod dirty;
mod guard;
mod interior;
mod lifecycle;
mod newtype;
mod ops;
mod pointer;
mod quasi;
mod state;
#[cfg(feature = "std")]
pub mod std;

use ::core::marker::PhantomData;

pub use depth::{
    AsDeref, AsDerefCoinductive, AsDerefMut, AsDerefMutCoinductive, AsDerefPtrExt, DerefPtr,
};
pub use deref::DerefObserver;
pub use dirty::{Dirty, Noop, NoopObserver, ShallowObserver};
pub use guard::{ObservedGuard, ObservedGuardMut};
pub(crate) use interior::InteriorState;
pub use lifecycle::{
    Observed, ObserverCell, ObserverError, ObserverGuard, Poisoned, observed, observer_cell,
};
pub use newtype::{Newtype, NewtypeObserver};
pub use pointer::Pointer;
pub use quasi::{DerefMutUntracked, Invalidate, QuasiObserver};
pub use state::{CollectState, State, StateObserver};

mod private {
    pub trait Sealed {}
}

/// A type-level unsigned natural used as dereference depth.
pub trait Unsigned: private::Sealed + 'static {}

/// Type-level zero.
pub struct Zero;
impl private::Sealed for Zero {}
impl Unsigned for Zero {}

/// Type-level successor.
pub struct Succ<N>(PhantomData<N>);
impl<N: Unsigned> private::Sealed for Succ<N> {}
impl<N: Unsigned> Unsigned for Succ<N> {}

/// Runtime observer over a statically described dereference chain.
///
/// # Safety
///
/// An implementation may retain model pointers only as inactive links. It must access them solely
/// while the observer is bound to the live exclusive borrow supplied to [`Observer::observe`] or
/// [`Observer::relocate`], must replace every stale link before reading through it, and must not
/// access the model while being dropped in an unbound state. `rebase` must additionally tolerate
/// any live value of the same type because it establishes a new logical baseline.
pub unsafe trait Observer:
    QuasiObserver<Target = Pointer<<Self as QuasiObserver>::Head>> + Sized
{
    /// # Safety
    /// `head` must be valid and exclusively borrowed for every access performed while constructing
    /// the observer. Retained links become inactive when the surrounding binding ends.
    unsafe fn observe(head: *mut Self::Head) -> Self;

    /// # Safety
    /// `head` must be the relocated address of the same logical value and remain exclusively
    /// borrowed until the surrounding binding ends. The implementation must replace stale links
    /// before reading through them.
    unsafe fn relocate(this: &mut Self, head: *mut Self::Head);

    /// Discards recorded facts and establishes a new baseline at `head`.
    ///
    /// The default implementation rebuilds the observer. Implementations may override this to
    /// retain allocations and other reusable topology.
    ///
    /// # Safety
    /// `head` must identify a live value of the observed type and remain exclusively borrowed for
    /// every access through `this` until the surrounding binding ends. It need not be the previous
    /// logical value: rebasing discards all facts and retained topology that cannot describe it.
    unsafe fn rebase(this: &mut Self, head: *mut Self::Head) {
        *this = unsafe { Self::observe(head) };
    }
}
