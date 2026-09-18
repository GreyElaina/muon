//! Shared state machine for recursively observed interior mutability.

use core::cell::{Cell, RefCell, RefMut};
use core::ops::DerefMut;

use crate::{ObservedGuard, ObservedGuardMut, Observer, Zero};

pub(crate) struct InteriorState<Child> {
    child: RefCell<Child>,
    mutated: Cell<bool>,
    child_stale: Cell<bool>,
    suppress_escape: Cell<bool>,
}

impl<Child> InteriorState<Child> {
    pub(crate) const fn new(child: Child) -> Self {
        Self {
            child: RefCell::new(child),
            mutated: Cell::new(false),
            child_stale: Cell::new(false),
            suppress_escape: Cell::new(false),
        }
    }

    pub(crate) fn escape(&self) {
        self.mutated.set(true);
        self.child_stale.set(true);
    }

    pub(crate) fn is_mutated(&self) -> bool {
        self.mutated.get()
    }

    pub(crate) fn is_stale(&self) -> bool {
        self.child_stale.get()
    }

    pub(crate) fn take_stale(&self) -> bool {
        self.child_stale.replace(false)
    }

    pub(crate) fn suppress_escape(&self) {
        self.suppress_escape.set(true);
    }

    pub(crate) fn take_suppression(&self) -> bool {
        self.suppress_escape.replace(false)
    }

    pub(crate) fn child_mut(&mut self) -> &mut Child {
        self.child.get_mut()
    }

    /// Reattaches the persistent child to its current value, rebuilding it after an escape.
    ///
    /// # Safety
    /// `value` must satisfy [`Observer::observe`] or [`Observer::relocate`], respectively.
    pub(crate) unsafe fn reattach<T>(&self, value: *mut T) -> RefMut<'_, Child>
    where
        Child: Observer<Head = T, InnerDepth = Zero>,
    {
        unsafe {
            let mut child = self.child.borrow_mut();
            if self.take_stale() {
                *child = Child::observe(value);
            } else {
                Child::relocate(&mut child, value);
            }
            child
        }
    }

    /// Rebases the persistent child and clears conservative escape state.
    ///
    /// # Safety
    /// `value` must be the live value protected by the current outer binding.
    pub(crate) unsafe fn rebase<T>(&mut self, value: *mut T)
    where
        Child: Observer<Head = T, InnerDepth = Zero>,
    {
        unsafe { Child::rebase(self.child.get_mut(), value) }
        self.mutated.set(false);
        self.child_stale.set(false);
        self.suppress_escape.set(false);
    }

    /// Pairs a shared access guard with the already attached child observer.
    pub(crate) fn guard<Guard>(&self, guard: Guard) -> ObservedGuard<'_, Guard, Child> {
        ObservedGuard::new(guard, self.child.borrow())
    }

    /// Pairs an exclusive access guard with the persistent child observer.
    ///
    /// # Safety
    /// The guard must protect the current logical value represented by `child`.
    pub(crate) unsafe fn guard_mut<T, Guard>(
        &self,
        mut guard: Guard,
    ) -> ObservedGuardMut<'_, Guard, Child>
    where
        Child: Observer<Head = T, InnerDepth = Zero>,
        Guard: DerefMut<Target = T>,
    {
        let observer = unsafe { self.reattach(&mut *guard) };
        ObservedGuardMut::new(guard, observer)
    }
}
