#![cfg_attr(not(feature = "std"), no_std)]
#![recursion_limit = "256"]
#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

#[cfg(feature = "alloc")]
extern crate alloc;

#[doc(hidden)]
pub use kernel_macros::__tracked;
pub use kernel_macros::Observe;

pub mod collect;
pub mod observer;
pub mod path;
pub mod select;

pub use collect::{
    Change, Collect, Field, Fields, Here, Query, Replace, Scope, Through, collect, collect_async,
    emit,
};
pub(crate) use observer::ObserverSlot;
#[cfg(feature = "alloc")]
pub use observer::alloc::{CowObserver, StringObserver};
pub use observer::core::{
    ArrayObserver, BoundObserver, OptionObserver, RangeFromObserver, RangeObserver,
    RangeToInclusiveObserver, RangeToObserver, ResultObserver, TupleObserver, TupleObserver2,
    TupleObserver3, TupleObserver4, TupleObserver5, TupleObserver6, TupleObserver7, TupleObserver8,
    TupleObserver9, TupleObserver10, TupleObserver11, TupleObserver12,
};
pub use observer::core::{
    AtomicState, CellObserver, LazyCellObserver, LazyObserver, ObservedRef, ObservedRefMut,
    OnceCellObserver, OnceObserver, RefCellObserver, TryObservedBorrowError,
};
#[cfg(feature = "std")]
pub use observer::std::{
    LazyLockObserver, MutexObserver, ObservedMutexGuard, ObservedRwLockReadGuard,
    ObservedRwLockWriteGuard, OnceLockObserver, RwLockObserver,
};
pub use observer::{
    AsDeref, AsDerefCoinductive, AsDerefMut, AsDerefMutCoinductive, AsDerefPtrExt, CollectState,
    DerefMutUntracked, DerefObserver, DerefPtr, Dirty, HeadOf, Invalidate, Newtype,
    NewtypeObserver, Noop, NoopObserver, Observed, ObservedGuard, ObservedGuardMut, Observer,
    ObserverCell, ObserverError, ObserverGuard, Pointer, Poisoned, QuasiObserver, ShallowObserver,
    State, StatefulObserver, Succ, Unsigned, Zero,
};
#[cfg(feature = "alloc")]
pub use path::{OwnedPath, PathSegment};
pub use path::{Path, PathStep};
pub use select::{
    Candidates, Composite, Current, Observe, Parent, Select, SelectFrom, Selected, Slot,
};

/// Adapts a closure body to operate on an observer.
///
/// Pass the resulting synchronous or asynchronous closure to [`collect()`] or
/// [`collect_async()`]. Assignment and comparison operands are rewritten through
/// [`QuasiObserver`] so the same body works with observer fields and ordinary Rust values.
#[macro_export]
macro_rules! tracked {
    ($($input:tt)*) => {
        $crate::__tracked!($crate, $($input)*)
    };
}
