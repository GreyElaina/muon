//! Conservative observation of shared-ownership handles from `alloc`.

use alloc::rc::{Rc, Weak as RcWeak};
use alloc::sync::{Arc, Weak as ArcWeak};

use crate::{AsDerefMut, Observe, ShallowObserver, Unsigned};

macro_rules! handle_observe {
    ($($handle:ident),* $(,)?) => {
        $(
            impl<T: ?Sized> Observe for $handle<T> {
                type Observer<Head, Depth>
                    = ShallowObserver<Self, Head, Depth>
                where
                    Depth: Unsigned,
                    Head: AsDerefMut<Depth, Target = Self> + ?Sized;
            }
        )*
    };
}

handle_observe!(Rc, Arc, RcWeak, ArcWeak);
