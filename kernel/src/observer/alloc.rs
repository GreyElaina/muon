//! Observer support for types provided by Rust's `alloc` crate.

use super::{AsDeref, AsDerefMut, Observer, Pointer, QuasiObserver, Succ, Unsigned, Zero};

mod cow;
mod handle;
mod string;

pub use cow::CowObserver;
pub use string::StringObserver;
