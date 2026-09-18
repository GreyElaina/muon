//! Dynamic access guards paired with persistent child observers.

use core::cell::{Ref, RefMut};
use core::ops::{Deref, DerefMut};

/// Shared model access paired with a shared borrow of persistent child-observer state.
pub struct ObservedGuard<'a, Guard, O: ?Sized> {
    // Field order releases observer state before the model guard.
    observer: Ref<'a, O>,
    guard: Guard,
}

impl<'a, Guard, O: ?Sized> ObservedGuard<'a, Guard, O> {
    pub(super) fn new(guard: Guard, observer: Ref<'a, O>) -> Self {
        Self { guard, observer }
    }
}

impl<Guard, O: ?Sized> Deref for ObservedGuard<'_, Guard, O> {
    type Target = O;

    fn deref(&self) -> &Self::Target {
        let _ = &self.guard;
        &self.observer
    }
}

/// Exclusive model access paired with an exclusive borrow of persistent child-observer state.
pub struct ObservedGuardMut<'a, Guard, O: ?Sized> {
    // Field order releases observer state before the model guard.
    observer: RefMut<'a, O>,
    guard: Guard,
}

impl<'a, Guard, O: ?Sized> ObservedGuardMut<'a, Guard, O> {
    pub(super) fn new(guard: Guard, observer: RefMut<'a, O>) -> Self {
        Self { guard, observer }
    }
}

impl<Guard, O: ?Sized> Deref for ObservedGuardMut<'_, Guard, O> {
    type Target = O;

    fn deref(&self) -> &Self::Target {
        let _ = &self.guard;
        &self.observer
    }
}

impl<Guard, O: ?Sized> DerefMut for ObservedGuardMut<'_, Guard, O> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.observer
    }
}
