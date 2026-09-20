//! Observation through transparent single-field wrappers.

use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};

use crate::{Collect, Observe, Path};

use super::{AsDeref, AsDerefMut, Observer, Pointer, QuasiObserver, Succ, Unsigned, Zero};

/// Projection from a transparent wrapper to its single observed field.
///
/// # Safety
///
/// [`Newtype::inner_ptr`] must return the same live inline field for every pointer to one logical
/// wrapper value. The projection must preserve alignment, provenance, and exclusive access.
pub unsafe trait Newtype {
    /// Single projected field.
    type Inner: ?Sized;

    /// Projects `this` to its inline field.
    ///
    /// # Safety
    ///
    /// `this` must identify a live wrapper value with permission to access its field.
    unsafe fn inner_ptr(this: *mut Self) -> *mut Self::Inner;
}

unsafe impl<T> Newtype for core::num::Wrapping<T> {
    type Inner = T;

    unsafe fn inner_ptr(this: *mut Self) -> *mut T {
        unsafe { &raw mut (*this).0 }
    }
}

unsafe impl<T> Newtype for core::num::Saturating<T> {
    type Inner = T;

    unsafe fn inner_ptr(this: *mut Self) -> *mut T {
        unsafe { &raw mut (*this).0 }
    }
}

unsafe impl<T> Newtype for core::cmp::Reverse<T> {
    type Inner = T;

    unsafe fn inner_ptr(this: *mut Self) -> *mut T {
        unsafe { &raw mut (*this).0 }
    }
}

/// Observer that projects a transparent wrapper onto its inner observer.
pub struct NewtypeObserver<O, Head: ?Sized, Depth = Zero> {
    inner: O,
    pointer: Pointer<Head>,

    marker: PhantomData<Depth>,
}

impl<O, Head: ?Sized, Depth> Deref for NewtypeObserver<O, Head, Depth> {
    type Target = Pointer<Head>;

    fn deref(&self) -> &Self::Target {
        &self.pointer
    }
}

impl<O, Head: ?Sized, Depth> DerefMut for NewtypeObserver<O, Head, Depth>
where
    O: Observer,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target: Newtype<Inner = O::Head>>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        QuasiObserver::invalidate(self);
        &mut self.pointer
    }
}

impl<O, Head: ?Sized, Depth> QuasiObserver for NewtypeObserver<O, Head, Depth>
where
    O: Observer,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target: Newtype<Inner = O::Head>>,
{
    type OuterDepth = Succ<Zero>;
    type InnerDepth = Depth;

    fn invalidate(this: &mut Self) {
        O::invalidate(&mut this.inner)
    }
}

unsafe impl<O, Head: ?Sized, Depth> Observer for NewtypeObserver<O, Head, Depth>
where
    O: Observer<InnerDepth = Zero>,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target: Newtype<Inner = O::Head>>,
{
    type Head = Head;

    unsafe fn observe(head: *mut Head) -> Self {
        unsafe {
            let value = AsDeref::<Depth>::as_deref_ptr(head);
            let inner = O::observe(Newtype::inner_ptr(value));
            Self {
                inner,
                pointer: Pointer::new_unchecked(head),
                marker: PhantomData,
            }
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Head) {
        unsafe {
            let value = AsDeref::<Depth>::as_deref_ptr(head);
            O::relocate(&mut this.inner, Newtype::inner_ptr(value));
            Pointer::set_unchecked(&this.pointer, head);
        }
    }

    unsafe fn rebase(this: &mut Self, head: *mut Head) {
        unsafe {
            let value = AsDeref::<Depth>::as_deref_ptr(head);
            O::rebase(&mut this.inner, Newtype::inner_ptr(value));
            Pointer::set_unchecked(&this.pointer, head);
        }
    }
}

impl<O, Head: ?Sized, Depth, Context: ?Sized, Route, Error, Scopes>
    Collect<Context, Route, Error, Scopes> for NewtypeObserver<O, Head, Depth>
where
    O: Collect<Context, Route, Error, Scopes>,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        Collect::<Context, Route, Error, Scopes>::collect(&mut self.inner, path, context)
    }
}

macro_rules! newtype_observe {
    ($($wrapper:ident),* $(,)?) => {
        $(
            impl<T, Route> Observe<$wrapper<T>, Route> for $wrapper<T>
            where
                T: Observe<T, Route>,
            {
                type Observer<Head, Depth>
                    = NewtypeObserver<
                        <T as Observe<T, Route>>::Observer<T, Zero>,
                        Head,
                        Depth,
                    >
                where
                    Depth: Unsigned,
                    Head: AsDerefMut<Depth, Target = Self> + ?Sized;
            }
        )*
    };
}

use core::cmp::Reverse;
use core::num::{Saturating, Wrapping};

newtype_observe!(Wrapping, Saturating, Reverse);
