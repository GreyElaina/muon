//! Transparent observation through one mutable dereference layer.

use core::ops::{Deref, DerefMut};

use crate::{Collect, Observe, Path};

use super::{AsDeref, AsDerefMut, Observer, QuasiObserver, Succ, Unsigned};

/// Observer wrapper that removes one dereference layer from an inner observer's depth.
#[repr(transparent)]
pub struct DerefObserver<O> {
    inner: O,
}

impl<O> Deref for DerefObserver<O> {
    type Target = O;

    fn deref(&self) -> &O {
        &self.inner
    }
}

impl<O> DerefMut for DerefObserver<O> {
    fn deref_mut(&mut self) -> &mut O {
        &mut self.inner
    }
}

impl<O, Depth> QuasiObserver for DerefObserver<O>
where
    Depth: Unsigned,
    O: QuasiObserver<InnerDepth = Succ<Depth>>,
    O::Head: AsDeref<Depth>,
{
    type Head = O::Head;
    type OuterDepth = Succ<O::OuterDepth>;
    type InnerDepth = Depth;

    fn invalidate(this: &mut Self) {
        O::invalidate(&mut this.inner)
    }
}

unsafe impl<O, Depth> Observer for DerefObserver<O>
where
    Depth: Unsigned,
    O: Observer<InnerDepth = Succ<Depth>>,
    O::Head: AsDeref<Depth>,
{
    unsafe fn observe(head: *mut Self::Head) -> Self {
        Self {
            inner: unsafe { O::observe(head) },
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Self::Head) {
        unsafe { O::relocate(&mut this.inner, head) }
    }

    unsafe fn rebase(this: &mut Self, head: *mut Self::Head) {
        unsafe { O::rebase(&mut this.inner, head) }
    }
}

impl<O, Context: ?Sized, Route, Error, Scopes> Collect<Context, Route, Error, Scopes>
    for DerefObserver<O>
where
    O: Collect<Context, Route, Error, Scopes>,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        Collect::<Context, Route, Error, Scopes>::collect(&mut self.inner, path, context)
    }
}

impl<'value, T: ?Sized, Route> Observe<&'value mut T, Route> for &'value mut T
where
    T: Observe<T, Route>,
{
    type Observer<Head, Depth>
        = DerefObserver<<T as Observe<T, Route>>::Observer<Head, Succ<Depth>>>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}

#[cfg(feature = "alloc")]
impl<T: ?Sized, Route> Observe<alloc::boxed::Box<T>, Route> for alloc::boxed::Box<T>
where
    T: Observe<T, Route>,
{
    type Observer<Head, Depth>
        = DerefObserver<<T as Observe<T, Route>>::Observer<Head, Succ<Depth>>>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}
