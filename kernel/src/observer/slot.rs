//! Dormant storage and activation for child observers behind shared model access.

use core::cell::{Cell, Ref, RefCell, RefMut};
use core::ops::DerefMut;

use crate::{ObservedGuard, ObservedGuardMut, Observer, Zero};

#[derive(Clone, Copy, Eq, PartialEq)]
enum Status {
    Exact,
    Fallback,
    Stale,
}

pub(crate) struct ObserverSlot<O> {
    observer: RefCell<O>,
    status: Cell<Status>,
}

impl<O> ObserverSlot<O> {
    pub(crate) const fn new(observer: O) -> Self {
        Self {
            observer: RefCell::new(observer),
            status: Cell::new(Status::Exact),
        }
    }

    pub(crate) fn invalidate(&self) {
        self.status.set(Status::Stale);
    }

    #[cfg(feature = "std")]
    pub(crate) fn fallback(&self) {
        if self.status.get() == Status::Exact {
            self.status.set(Status::Fallback);
        }
    }

    pub(crate) fn is_exact(&self) -> bool {
        self.status.get() == Status::Exact
    }

    pub(crate) fn is_stale(&self) -> bool {
        self.status.get() == Status::Stale
    }

    /// Returns the dormant observer after its current attachment has been established.
    ///
    /// # Safety
    /// The slot must not be stale, and the caller must hold the model permission required by every
    /// access subsequently performed through the observer.
    pub(crate) unsafe fn observer_mut(&mut self) -> &mut O {
        debug_assert!(
            !self.is_stale(),
            "cannot access a stale child observer before rebuilding it"
        );
        self.observer.get_mut()
    }

    /// Activates the retained observer for an exclusively accessible current value.
    ///
    /// # Safety
    /// `value` must satisfy [`Observer::observe`] when stale and [`Observer::relocate`] otherwise.
    pub(crate) unsafe fn activate<T>(&mut self, value: *mut T)
    where
        O: Observer<Head = T, InnerDepth = Zero>,
    {
        unsafe {
            let status = self.status.replace(Status::Stale);
            if status == Status::Stale {
                *self.observer.get_mut() = O::observe(value);
                self.status.set(Status::Fallback);
            } else {
                O::relocate(self.observer.get_mut(), value);
                self.status.set(status);
            }
        }
    }

    /// Dynamically activates the retained observer for an exclusively guarded current value.
    ///
    /// # Safety
    /// `value` must satisfy [`Observer::observe`] when stale and [`Observer::relocate`] otherwise.
    pub(crate) unsafe fn activate_ref<T>(&self, value: *mut T) -> RefMut<'_, O>
    where
        O: Observer<Head = T, InnerDepth = Zero>,
    {
        unsafe {
            let status = self.status.replace(Status::Stale);
            let mut observer = self.observer.borrow_mut();
            if status == Status::Stale {
                *observer = O::observe(value);
                self.status.set(Status::Fallback);
            } else {
                O::relocate(&mut observer, value);
                self.status.set(status);
            }
            observer
        }
    }

    /// Establishes a fresh exact baseline at the current value.
    ///
    /// # Safety
    /// `value` must be the live value protected by the current outer binding.
    pub(crate) unsafe fn rebase<T>(&mut self, value: *mut T)
    where
        O: Observer<Head = T, InnerDepth = Zero>,
    {
        let status = self.status.replace(Status::Stale);
        if status == Status::Stale {
            *self.observer.get_mut() = unsafe { O::observe(value) };
        } else {
            unsafe { O::rebase(self.observer.get_mut(), value) }
        }
        self.status.set(Status::Exact);
    }

    /// Pairs a model guard with a shared borrow of an already attached observer.
    ///
    /// # Safety
    /// The slot must not be stale, and `guard` must keep the attached model value shared-accessible
    /// for the returned guard's lifetime.
    pub(crate) unsafe fn read<Guard>(&self, guard: Guard) -> ObservedGuard<'_, Guard, O> {
        debug_assert!(
            !self.is_stale(),
            "cannot share a stale child observer before rebuilding it"
        );
        ObservedGuard::new(guard, self.observer.borrow())
    }

    /// Activates and exclusively lends the child for the lifetime of `guard`.
    ///
    /// # Safety
    /// The guard must protect the current logical value retained by this slot.
    pub(crate) unsafe fn write<T, Guard>(&self, mut guard: Guard) -> ObservedGuardMut<'_, Guard, O>
    where
        O: Observer<Head = T, InnerDepth = Zero>,
        Guard: DerefMut<Target = T>,
    {
        let observer = unsafe { self.activate_ref(&mut *guard) };
        ObservedGuardMut::new(guard, observer)
    }
}

impl<O> ObserverSlot<Option<O>> {
    pub(crate) fn get(&self) -> Option<Ref<'_, O>> {
        if self.status.get() == Status::Stale {
            return None;
        }
        Ref::filter_map(self.observer.borrow(), Option::as_ref).ok()
    }

    /// Activates the optional observer for the current presence and value.
    ///
    /// # Safety
    /// A present `value` must satisfy [`Observer::observe`] when stale or presence changed and
    /// [`Observer::relocate`] otherwise.
    pub(crate) unsafe fn activate_optional<T>(&mut self, value: Option<*mut T>)
    where
        O: Observer<Head = T, InnerDepth = Zero>,
    {
        unsafe {
            let status = self.status.replace(Status::Stale);
            let observer = self.observer.get_mut();
            let same_presence = observer.is_some() == value.is_some();
            if status == Status::Stale || !same_presence {
                *observer = value.map(|value| O::observe(value));
                self.status.set(Status::Fallback);
            } else {
                if let (Some(observer), Some(value)) = (observer.as_mut(), value) {
                    O::relocate(observer, value);
                }
                self.status.set(status);
            }
        }
    }

    /// Establishes a fresh exact baseline for the current presence and value.
    ///
    /// # Safety
    /// A present `value` must be the live value protected by the current outer binding.
    pub(crate) unsafe fn rebase_optional<T>(&mut self, value: Option<*mut T>)
    where
        O: Observer<Head = T, InnerDepth = Zero>,
    {
        unsafe {
            let status = self.status.replace(Status::Stale);
            if status == Status::Stale {
                *self.observer.get_mut() = value.map(|value| O::observe(value));
            } else {
                match (self.observer.get_mut(), value) {
                    (Some(observer), Some(value)) => O::rebase(observer, value),
                    (observer @ None, Some(value)) => *observer = Some(O::observe(value)),
                    (observer @ Some(_), None) => *observer = None,
                    (None, None) => {}
                }
            }
        }
        self.status.set(Status::Exact);
    }
}
