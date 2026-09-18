//! Observer support for types provided by Rust's `core` crate.

use super::{
    AsDeref, AsDerefMut, Observer, Pointer, QuasiObserver, ShallowObserver, Succ, Unsigned, Zero,
};

mod array;
mod atomic;
mod bound;
mod cell;
mod float;
pub(super) mod lazy;
pub(super) mod once_cell;
mod option;
mod range;
mod ref_cell;
mod result;
mod scalar;
mod tuple;

#[doc(hidden)]
pub use atomic::AtomicState;
pub use cell::CellObserver;
pub use float::{FloatObserver, FloatState};
pub use lazy::{LazyCellObserver, LazyObserver};
pub use once_cell::{OnceCellObserver, OnceObserver};
pub use ref_cell::{ObservedRef, ObservedRefMut, RefCellObserver, TryObservedBorrowError};
#[doc(hidden)]
pub use scalar::{ScalarObserver, ScalarState};

pub use array::ArrayObserver;
pub use bound::BoundObserver;
pub use option::OptionObserver;
pub use range::{RangeFromObserver, RangeObserver, RangeToInclusiveObserver, RangeToObserver};
pub use result::ResultObserver;
pub use tuple::{
    TupleObserver, TupleObserver2, TupleObserver3, TupleObserver4, TupleObserver5, TupleObserver6,
    TupleObserver7, TupleObserver8, TupleObserver9, TupleObserver10, TupleObserver11,
    TupleObserver12,
};
