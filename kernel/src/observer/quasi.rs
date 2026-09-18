use core::ops::{Deref, DerefMut};

use super::{AsDeref, AsDerefMut, AsDerefMutCoinductive, Pointer, Unsigned, Zero};

/// Reaches a mutable target without running observer [`DerefMut`] hooks.
pub trait DerefMutUntracked: DerefMut {
    /// Traverses the wrapper chain while bypassing observer invalidation.
    fn deref_mut_untracked<'a, U, D>(this: &'a mut U) -> &'a mut Self::Target
    where
        Self: 'a,
        D: Unsigned,
        U: AsDerefMutCoinductive<D, Target = Self> + ?Sized,
    {
        this.as_deref_mut_coinductive().deref_mut()
    }
}

impl<T: ?Sized> DerefMutUntracked for &mut T {}

impl<S: ?Sized> DerefMutUntracked for Pointer<S> {
    fn deref_mut_untracked<'a, U, D>(this: &'a mut U) -> &'a mut S
    where
        Self: 'a,
        D: Unsigned,
        U: AsDerefMutCoinductive<D, Target = Self> + ?Sized,
    {
        unsafe { Pointer::as_mut(this.as_deref_coinductive()) }
    }
}

/// Describes an observer's outer wrapper chain and inner model projection.
///
/// A quasi-observer exposes shared access, untracked mutable access, and tracked mutable access
/// that first conservatively invalidates granular state.
pub trait QuasiObserver:
    AsDerefMutCoinductive<Self::OuterDepth, Target: Deref<Target = Self::Head>>
{
    /// Value held by the observer's internal [`Pointer`].
    type Head: AsDeref<Self::InnerDepth> + ?Sized;
    /// Dereference distance from this wrapper to its [`Pointer`].
    type OuterDepth: Unsigned;
    /// Dereference distance from [`Self::Head`] to the observed value.
    type InnerDepth: Unsigned;

    /// Conservatively invalidates granular tracking state.
    fn invalidate(this: &mut Self);

    /// Returns the observed value without changing tracking state.
    fn untracked_ref<T: ?Sized>(&self) -> &T
    where
        Self::Head: AsDeref<Self::InnerDepth, Target = T>,
    {
        self.as_deref_coinductive().deref().as_deref()
    }

    /// Returns mutable access without changing tracking state.
    fn untracked_mut<T: ?Sized>(&mut self) -> &mut T
    where
        Self::Target: DerefMutUntracked,
        Self::Head: AsDerefMut<Self::InnerDepth, Target = T>,
    {
        DerefMutUntracked::deref_mut_untracked(self).as_deref_mut()
    }

    /// Invalidates granular state and returns mutable access to the observed value.
    fn tracked_mut<T: ?Sized>(&mut self) -> &mut T
    where
        Self::Target: DerefMutUntracked,
        Self::Head: AsDerefMut<Self::InnerDepth, Target = T>,
    {
        Self::invalidate(self);
        DerefMutUntracked::deref_mut_untracked(self).as_deref_mut()
    }
}

impl<T: ?Sized> QuasiObserver for &T {
    type Head = T;
    type OuterDepth = Zero;
    type InnerDepth = Zero;

    fn invalidate(_: &mut Self) {}
}

impl<T: ?Sized> QuasiObserver for &mut T {
    type Head = T;
    type OuterDepth = Zero;
    type InnerDepth = Zero;

    fn invalidate(_: &mut Self) {}
}

/// Invalidation hook implemented by observer-specific state.
pub trait Invalidate<T: ?Sized> {
    /// Marks facts derived from `value` as conservatively invalid.
    fn invalidate(&mut self, value: &T);
}
