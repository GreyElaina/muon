//! Observer support for types provided by Rust's `std` crate.

mod sync;

pub use super::core::lazy::LazyLockObserver;
pub use super::core::once_cell::OnceLockObserver;
pub use sync::{
    MutexObserver, ObservedMutexGuard, ObservedRwLockReadGuard, ObservedRwLockWriteGuard,
    RwLockObserver,
};
