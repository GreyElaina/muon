//! Reusable observer chassis backed by caller-defined state.

use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};

use crate::{Collect, Path, Scope};

use super::{AsDeref, Invalidate, Observer, Pointer, QuasiObserver, Succ, Unsigned, Zero};

/// State initialized when observation begins and invalidated by conservative mutable access.
pub trait State<T: ?Sized>: Invalidate<T> + Sized {
    /// Initializes state from the value at the beginning of observation.
    fn observe(value: &T) -> Self;

    /// Discards pending facts and establishes a new baseline while retaining reusable storage.
    fn rebase(&mut self, value: &T) {
        *self = Self::observe(value);
    }
}

/// Delivers the facts accumulated by an observer state.
///
/// Raw-pointer recovery remains inside [`StatefulObserver`]; implementations receive an ordinary
/// shared reference to the observed value at collection time.
pub trait CollectState<T: ?Sized, Context: ?Sized, Route, Error, Semantic = ()>: State<T> {
    /// Delivers facts retained by this state for the final `value`.
    fn collect(&mut self, value: &T, path: &Path<'_>, context: &mut Context) -> Result<(), Error>;
}

/// Generic observer whose mutation semantics are supplied by `St`.
pub struct StatefulObserver<St, Head: ?Sized, Depth = Zero> {
    pointer: Pointer<Head>,
    state: St,

    marker: PhantomData<Depth>,
}

impl<St, Head: ?Sized, Depth> StatefulObserver<St, Head, Depth> {
    /// Returns the observer-specific state.
    pub const fn state(&self) -> &St {
        &self.state
    }

    /// Returns the observer-specific state mutably.
    pub fn state_mut(&mut self) -> &mut St {
        &mut self.state
    }
}

impl<St, Head: ?Sized, Depth> Deref for StatefulObserver<St, Head, Depth> {
    type Target = Pointer<Head>;

    fn deref(&self) -> &Self::Target {
        &self.pointer
    }
}

impl<St, Head: ?Sized, Depth> DerefMut for StatefulObserver<St, Head, Depth>
where
    Depth: Unsigned,
    Head: AsDeref<Depth>,
    St: Invalidate<<Head as AsDeref<Depth>>::Target>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        QuasiObserver::invalidate(self);
        &mut self.pointer
    }
}

impl<St, Head: ?Sized, Depth> QuasiObserver for StatefulObserver<St, Head, Depth>
where
    Depth: Unsigned,
    Head: AsDeref<Depth>,
    St: Invalidate<<Head as AsDeref<Depth>>::Target>,
{
    type OuterDepth = Succ<Zero>;
    type InnerDepth = Depth;

    fn invalidate(this: &mut Self) {
        let value = AsDeref::<Depth>::as_deref(&*this.pointer);
        Invalidate::invalidate(&mut this.state, value);
    }
}

unsafe impl<St, Head: ?Sized, Depth> Observer for StatefulObserver<St, Head, Depth>
where
    Depth: Unsigned,
    Head: AsDeref<Depth>,
    St: State<<Head as AsDeref<Depth>>::Target>,
{
    type Head = Head;

    unsafe fn observe(head: *mut Self::Head) -> Self {
        unsafe {
            let value = AsDeref::<Depth>::as_deref(&*head);
            Self {
                pointer: Pointer::new_unchecked(head),
                state: St::observe(value),
                marker: PhantomData,
            }
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Self::Head) {
        unsafe { Pointer::set_unchecked(&this.pointer, head) }
    }

    unsafe fn rebase(this: &mut Self, head: *mut Self::Head) {
        unsafe {
            Pointer::set_unchecked(&this.pointer, head);
            let value = AsDeref::<Depth>::as_deref(&*head);
            State::rebase(&mut this.state, value);
        }
    }
}

impl<St, Head: ?Sized, Depth, Context: ?Sized, Route, Error, Semantic, Tail>
    Collect<Context, Route, Error, Scope<Semantic, Tail>> for StatefulObserver<St, Head, Depth>
where
    Depth: Unsigned,
    Head: AsDeref<Depth>,
    St: CollectState<<Head as AsDeref<Depth>>::Target, Context, Route, Error, Semantic>,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        let value = unsafe { Pointer::as_ref(&self.pointer) };
        let value = AsDeref::<Depth>::as_deref(value);
        self.state.collect(value, path, context)
    }
}
