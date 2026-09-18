use core::ops::{Deref, DerefMut};

#[cfg(feature = "alloc")]
use alloc::{
    borrow::{Cow, ToOwned},
    boxed::Box,
    ffi::CString,
    rc::Rc,
    string::String,
    sync::Arc,
    vec::Vec,
};
#[cfg(feature = "std")]
use std::{ffi::OsString, path::PathBuf};

use super::{Succ, Unsigned, Zero};

/// Raw-pointer counterpart of [`Deref`] used by typed observer traversal.
///
/// # Safety
///
/// Implementations must project a live `Self` pointer to the same pointee as `Deref`, preserving
/// metadata, alignment, provenance, and the caller's access permissions.
pub unsafe trait DerefPtr: Deref {
    /// Projects `this` to its dereference target without creating an intermediate reference.
    ///
    /// # Safety
    ///
    /// `this` must be live and carry the access permission required for the returned pointer.
    unsafe fn deref_ptr(this: *mut Self) -> *mut Self::Target;
}

unsafe impl<T: ?Sized> DerefPtr for &T {
    unsafe fn deref_ptr(this: *mut Self) -> *mut T {
        unsafe { (*this) as *const T as *mut T }
    }
}

unsafe impl<T: ?Sized> DerefPtr for &mut T {
    unsafe fn deref_ptr(this: *mut Self) -> *mut T {
        unsafe { *this }
    }
}

#[cfg(feature = "alloc")]
unsafe impl<T: ?Sized> DerefPtr for Box<T> {
    unsafe fn deref_ptr(this: *mut Self) -> *mut T {
        unsafe { (&*this).deref() as *const T as *mut T }
    }
}

#[cfg(feature = "alloc")]
unsafe impl<T: ?Sized> DerefPtr for Rc<T> {
    unsafe fn deref_ptr(this: *mut Self) -> *mut T {
        unsafe { Rc::as_ptr(&*this) as *mut T }
    }
}

#[cfg(feature = "alloc")]
unsafe impl<T: ?Sized> DerefPtr for Arc<T> {
    unsafe fn deref_ptr(this: *mut Self) -> *mut T {
        unsafe { Arc::as_ptr(&*this) as *mut T }
    }
}

#[cfg(feature = "alloc")]
unsafe impl<'a, B: ToOwned + ?Sized> DerefPtr for Cow<'a, B> {
    unsafe fn deref_ptr(this: *mut Self) -> *mut B {
        unsafe { (*this).deref() as *const B as *mut B }
    }
}

#[cfg(feature = "alloc")]
unsafe impl<T> DerefPtr for Vec<T> {
    unsafe fn deref_ptr(this: *mut Self) -> *mut [T] {
        unsafe {
            let v = &*this;
            core::ptr::slice_from_raw_parts_mut(v.as_ptr() as *mut T, v.len())
        }
    }
}

#[cfg(feature = "alloc")]
unsafe impl DerefPtr for String {
    unsafe fn deref_ptr(this: *mut Self) -> *mut str {
        unsafe {
            let s = &*this;
            core::mem::transmute(core::ptr::slice_from_raw_parts_mut(
                s.as_ptr() as *mut u8,
                s.len(),
            ))
        }
    }
}

#[cfg(feature = "alloc")]
unsafe impl DerefPtr for CString {
    unsafe fn deref_ptr(this: *mut Self) -> *mut Self::Target {
        unsafe { (&*this).deref() as *const _ as *mut _ }
    }
}

#[cfg(feature = "std")]
unsafe impl DerefPtr for OsString {
    unsafe fn deref_ptr(this: *mut Self) -> *mut Self::Target {
        unsafe { (&*this).deref() as *const _ as *mut _ }
    }
}

#[cfg(feature = "std")]
unsafe impl DerefPtr for PathBuf {
    unsafe fn deref_ptr(this: *mut Self) -> *mut Self::Target {
        unsafe { (&*this).deref() as *const _ as *mut _ }
    }
}

/// Traverses exactly `N` statically known dereference layers.
pub trait AsDeref<N: Unsigned> {
    /// Value reached after `N` dereferences.
    type Target: ?Sized;

    /// Returns the statically selected dereference target.
    fn as_deref(&self) -> &Self::Target;

    /// Projects a raw pointer through the same dereference chain.
    ///
    /// # Safety
    ///
    /// `this` must be live for every projection in the chain.
    unsafe fn as_deref_ptr(this: *mut Self) -> *mut Self::Target;
}

/// Mutable counterpart of [`AsDeref`].
pub trait AsDerefMut<N: Unsigned>: AsDeref<N> {
    /// Returns the statically selected mutable dereference target.
    fn as_deref_mut(&mut self) -> &mut Self::Target;
}

impl<T: ?Sized> AsDeref<Zero> for T {
    type Target = T;

    fn as_deref(&self) -> &T {
        self
    }
    unsafe fn as_deref_ptr(this: *mut Self) -> *mut T {
        this
    }
}
impl<T: ?Sized> AsDerefMut<Zero> for T {
    fn as_deref_mut(&mut self) -> &mut T {
        self
    }
}
impl<T: AsDeref<N, Target: DerefPtr> + ?Sized, N: Unsigned> AsDeref<Succ<N>> for T {
    type Target = <T::Target as Deref>::Target;

    fn as_deref(&self) -> &Self::Target {
        AsDeref::<N>::as_deref(self).deref()
    }
    unsafe fn as_deref_ptr(this: *mut Self) -> *mut Self::Target {
        unsafe { DerefPtr::deref_ptr(AsDeref::<N>::as_deref_ptr(this)) }
    }
}
impl<T: AsDerefMut<N, Target: DerefMut + DerefPtr> + ?Sized, N: Unsigned> AsDerefMut<Succ<N>>
    for T
{
    fn as_deref_mut(&mut self) -> &mut Self::Target {
        AsDerefMut::<N>::as_deref_mut(self).deref_mut()
    }
}

/// Convenience projection methods for raw pointers.
pub trait AsDerefPtrExt {
    /// Pointee at the start of the traversal.
    type Pointee: ?Sized;

    /// Projects this pointer through `D` dereference layers.
    ///
    /// # Safety
    ///
    /// The pointer must satisfy [`AsDeref::as_deref_ptr`] for the entire traversal.
    unsafe fn as_deref_ptr<D>(self) -> *mut <Self::Pointee as AsDeref<D>>::Target
    where
        D: Unsigned,
        Self::Pointee: AsDeref<D>;
}

impl<T: ?Sized> AsDerefPtrExt for *mut T {
    type Pointee = T;

    unsafe fn as_deref_ptr<D>(self) -> *mut <T as AsDeref<D>>::Target
    where
        D: Unsigned,
        T: AsDeref<D>,
    {
        unsafe { AsDeref::<D>::as_deref_ptr(self) }
    }
}

/// Traverses outward through exactly `N` wrapper [`Deref`] implementations.
pub trait AsDerefCoinductive<N: Unsigned> {
    /// Wrapper target reached after `N` dereferences.
    type Target: ?Sized;

    /// Returns the selected wrapper target.
    fn as_deref_coinductive(&self) -> &Self::Target;
}

/// Mutable counterpart of [`AsDerefCoinductive`].
pub trait AsDerefMutCoinductive<N: Unsigned>: AsDerefCoinductive<N> {
    /// Returns the selected mutable wrapper target.
    fn as_deref_mut_coinductive(&mut self) -> &mut Self::Target;
}

impl<T: ?Sized> AsDerefCoinductive<Zero> for T {
    type Target = T;

    fn as_deref_coinductive(&self) -> &T {
        self
    }
}

impl<T: ?Sized> AsDerefMutCoinductive<Zero> for T {
    fn as_deref_mut_coinductive(&mut self) -> &mut T {
        self
    }
}

impl<T: Deref<Target: AsDerefCoinductive<N>> + ?Sized, N: Unsigned> AsDerefCoinductive<Succ<N>>
    for T
{
    type Target = <T::Target as AsDerefCoinductive<N>>::Target;

    fn as_deref_coinductive(&self) -> &Self::Target {
        self.deref().as_deref_coinductive()
    }
}

impl<T: DerefMut<Target: AsDerefMutCoinductive<N>> + ?Sized, N: Unsigned>
    AsDerefMutCoinductive<Succ<N>> for T
{
    fn as_deref_mut_coinductive(&mut self) -> &mut Self::Target {
        self.deref_mut().as_deref_mut_coinductive()
    }
}
