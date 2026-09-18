use core::{
    cell::Cell,
    fmt::Debug,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

fn recover<S: ?Sized>(raw: *const S) -> *const S {
    let exposed = core::ptr::with_exposed_provenance::<u8>(raw.cast::<u8>().addr());
    let mut result = raw;
    unsafe { core::ptr::write((&raw mut result).cast::<*const u8>(), exposed) }
    result
}

fn recover_mut<S: ?Sized>(raw: *mut S) -> *mut S {
    let exposed = core::ptr::with_exposed_provenance_mut::<u8>(raw.cast::<u8>().addr());
    let mut result = raw;
    unsafe { core::ptr::write((&raw mut result).cast::<*mut u8>(), exposed) }
    result
}

/// Raw observer link with relocatable provenance.
pub struct Pointer<S: ?Sized> {
    inner: Cell<NonNull<S>>,
}

impl<S: ?Sized> Pointer<S> {
    /// Returns the current observer head.
    pub const fn get(this: &Self) -> NonNull<S> {
        this.inner.get()
    }

    /// # Safety
    /// `head` must be non-null and remain valid for the observer lifetime.
    pub unsafe fn new_unchecked(head: *mut S) -> Self {
        let ptr = unsafe { NonNull::new_unchecked(head) };
        ptr.cast::<u8>().expose_provenance();
        Self {
            inner: Cell::new(ptr),
        }
    }

    /// # Safety
    /// `head` must identify the same live logical value.
    pub unsafe fn set_unchecked(this: &Self, head: *mut S) {
        let ptr = unsafe { NonNull::new_unchecked(head) };
        ptr.cast::<u8>().expose_provenance();
        this.inner.set(ptr);
    }

    /// # Safety
    /// The pointee must be live and shared access must be legal.
    pub unsafe fn as_ref<'a>(this: &Self) -> &'a S {
        unsafe { &*recover(this.inner.get().as_ptr()) }
    }

    /// # Safety
    /// The pointee must be live and exclusively accessible.
    pub unsafe fn as_mut<'a>(this: &Self) -> &'a mut S {
        unsafe { &mut *recover_mut(this.inner.get().as_ptr()) }
    }
}

impl<S: ?Sized> Deref for Pointer<S> {
    type Target = S;
    fn deref(&self) -> &S {
        unsafe { Self::as_ref(self) }
    }
}

impl<S: ?Sized> DerefMut for Pointer<S> {
    fn deref_mut(&mut self) -> &mut S {
        unsafe { Self::as_mut(self) }
    }
}

impl<S: ?Sized> Debug for Pointer<S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("Pointer").field(&self.inner.get()).finish()
    }
}

impl<S: ?Sized> PartialEq for Pointer<S> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<S: ?Sized> Eq for Pointer<S> {}
