//! Conservative observation of owned UTF-8 strings.

use alloc::string::String;

use crate::{AsDerefMut, Observe, ShallowObserver, Unsigned, Zero};

/// Whole-value observer for [`String`].
pub type StringObserver<Head, Depth = Zero> = ShallowObserver<String, Head, Depth>;

impl Observe for String {
    type Observer<Head, Depth>
        = StringObserver<Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}
