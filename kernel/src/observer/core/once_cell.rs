//! Conditional child observation for once-initialized slots.

use core::cell::{OnceCell, Ref};
use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};
#[cfg(feature = "std")]
use std::sync::OnceLock;

use crate::{
    AsDeref, AsDerefMut, Change, Collect, Composite, Observe, Path, Query, Replace, Scope, emit,
};
use crate::{Observer, ObserverSlot, Pointer, QuasiObserver, Succ, Unsigned, Zero};

trait OnceSlot {
    type Value;

    fn get(&self) -> Option<&Self::Value>;
    fn get_mut(&mut self) -> Option<&mut Self::Value>;
    fn set(&self, value: Self::Value) -> Result<(), Self::Value>;
    fn take(&mut self) -> Option<Self::Value>;
}

impl<T> OnceSlot for OnceCell<T> {
    type Value = T;

    fn get(&self) -> Option<&T> {
        self.get()
    }

    fn get_mut(&mut self) -> Option<&mut T> {
        self.get_mut()
    }

    fn set(&self, value: T) -> Result<(), T> {
        self.set(value)
    }

    fn take(&mut self) -> Option<T> {
        self.take()
    }
}

#[cfg(feature = "std")]
impl<T> OnceSlot for OnceLock<T> {
    type Value = T;

    fn get(&self) -> Option<&T> {
        self.get()
    }

    fn get_mut(&mut self) -> Option<&mut T> {
        self.get_mut()
    }

    fn set(&self, value: T) -> Result<(), T> {
        self.set(value)
    }

    fn take(&mut self) -> Option<T> {
        self.take()
    }
}

/// Observer for an optionally initialized, once-writable slot.
///
/// An already initialized value owns a structural child observer. Initialization or arbitrary
/// access that may change presence falls back to replacement of the whole slot for the current
/// observation pass.
pub struct OnceObserver<O, Slot, Head: ?Sized, Depth = Zero> {
    pointer: Pointer<Head>,
    child: ObserverSlot<Option<O>>,

    marker: PhantomData<(fn(Slot), Depth)>,
}

impl<O, Slot, Head: ?Sized, Depth> Deref for OnceObserver<O, Slot, Head, Depth> {
    type Target = Pointer<Head>;

    fn deref(&self) -> &Self::Target {
        self.child.invalidate();
        &self.pointer
    }
}

impl<O, Slot, Head: ?Sized, Depth> DerefMut for OnceObserver<O, Slot, Head, Depth> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.child.invalidate();
        &mut self.pointer
    }
}

impl<O, Slot, Head: ?Sized, Depth> QuasiObserver for OnceObserver<O, Slot, Head, Depth>
where
    O: Observer<InnerDepth = Zero>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth>,
{
    type OuterDepth = Succ<Zero>;
    type InnerDepth = Depth;

    fn invalidate(this: &mut Self) {
        this.child.invalidate();
    }

    fn untracked_ref<Value: ?Sized>(&self) -> &Value
    where
        crate::HeadOf<Self>: AsDeref<Self::InnerDepth, Target = Value>,
    {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        AsDeref::<Depth>::as_deref(head)
    }

    fn untracked_mut<Value: ?Sized>(&mut self) -> &mut Value
    where
        Head: AsDerefMut<Depth, Target = Value>,
    {
        let head = unsafe { Pointer::as_mut(&self.pointer) };
        AsDerefMut::<Depth>::as_deref_mut(head)
    }
}

unsafe impl<O, Slot, Head: ?Sized, Depth> Observer for OnceObserver<O, Slot, Head, Depth>
where
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    Slot: OnceSlot<Value = O::Head>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Slot>,
{
    type Head = Head;

    unsafe fn observe(head: *mut Head) -> Self {
        unsafe {
            let slot = AsDeref::<Depth>::as_deref_ptr(head);
            let child = OnceSlot::get_mut(&mut *slot).map(|value| O::observe(value));
            Self {
                pointer: Pointer::new_unchecked(head),
                child: ObserverSlot::new(child),

                marker: PhantomData,
            }
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Head) {
        unsafe {
            let slot = AsDeref::<Depth>::as_deref_ptr(head);
            let value = OnceSlot::get_mut(&mut *slot).map(|value| value as *mut O::Head);
            this.child.activate_optional(value);
            Pointer::set_unchecked(&this.pointer, head);
        }
    }

    unsafe fn rebase(this: &mut Self, head: *mut Head) {
        unsafe {
            let slot = AsDeref::<Depth>::as_deref_ptr(head);
            let value = OnceSlot::get_mut(&mut *slot).map(|value| value as *mut O::Head);
            this.child.rebase_optional(value);
            Pointer::set_unchecked(&this.pointer, head);
        }
    }
}

impl<O, Slot, Head: ?Sized, Depth, Context: ?Sized, ParentRoute, InnerRoute, Error, Semantic, Tail>
    Collect<Context, (ParentRoute, InnerRoute), Error, Scope<Semantic, Tail>>
    for OnceObserver<O, Slot, Head, Depth>
where
    O: Observer<InnerDepth = Zero> + Collect<Context, InnerRoute, Error, Scope<Semantic, Tail>>,
    Slot: OnceSlot<Value = O::Head>,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Slot>,
    for<'a> Context: Query<Change<'a, Slot>, ParentRoute, Semantic>,
    for<'a> <Context as Query<Change<'a, Slot>, ParentRoute, Semantic>>::Output:
        Replace<Slot, Slot>,
    for<'a> Error: From<
        <<Context as Query<Change<'a, Slot>, ParentRoute, Semantic>>::Output as Replace<
            Slot,
            Slot,
        >>::Error,
    >,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let slot = AsDeref::<Depth>::as_deref(head);
        if !self.child.is_exact() {
            return emit::<_, _, Context, ParentRoute, Semantic, Error>(
                context,
                Change::Replace {
                    path,
                    before: None,
                    after: slot,
                },
            );
        }

        if let Some(observer) = unsafe { self.child.observer_mut() }.as_mut() {
            Collect::<Context, InnerRoute, Error, Scope<Semantic, Tail>>::collect(
                observer, path, context,
            )?;
        }
        Ok(())
    }
}

#[allow(private_bounds)]
impl<O, Slot, Head: ?Sized, Depth> OnceObserver<O, Slot, Head, Depth>
where
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    Slot: OnceSlot<Value = O::Head>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Slot>,
{
    /// Returns a shared observer for an initialized value attached before this access.
    ///
    /// Shared initialization records a parent replacement; the next exclusive activation attaches
    /// the child, and a successful rebase restores exact collection.
    pub fn get(&self) -> Option<Ref<'_, O>> {
        self.child.get()
    }

    /// Returns the observer for the initialized value mutably.
    pub fn get_mut(&mut self) -> Option<&mut O> {
        let head = unsafe { Pointer::as_mut(&self.pointer) };
        let value = OnceSlot::get_mut(AsDerefMut::<Depth>::as_deref_mut(head));
        let value = value.map(|value| value as *mut O::Head);
        unsafe { self.child.activate_optional(value) };
        unsafe { self.child.observer_mut() }.as_mut()
    }

    /// Initializes the slot, falling back to a whole-slot replacement on success.
    pub fn set(&self, value: O::Head) -> Result<(), O::Head> {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        match OnceSlot::set(AsDeref::<Depth>::as_deref(head), value) {
            Ok(()) => {
                self.child.invalidate();
                Ok(())
            }
            Err(value) => Err(value),
        }
    }

    /// Initializes the slot when empty and returns its value through a conservative parent escape.
    pub fn get_or_init(&self, initialize: impl FnOnce() -> O::Head) -> &O::Head {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let slot = AsDeref::<Depth>::as_deref(head);
        if OnceSlot::get(slot).is_none() {
            let _ = OnceSlot::set(slot, initialize());
        }
        self.child.invalidate();
        OnceSlot::get(slot).expect("once slot remained uninitialized")
    }

    /// Fallibly initializes the slot and returns its value through a conservative parent escape.
    pub fn get_or_try_init<Error>(
        &self,
        initialize: impl FnOnce() -> Result<O::Head, Error>,
    ) -> Result<&O::Head, Error> {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let slot = AsDeref::<Depth>::as_deref(head);
        if OnceSlot::get(slot).is_none() {
            let _ = OnceSlot::set(slot, initialize()?);
        }
        self.child.invalidate();
        Ok(OnceSlot::get(slot).expect("once slot remained uninitialized"))
    }

    /// Initializes the slot when empty and returns its child observer using exclusive access.
    pub fn get_or_init_mut(&mut self, initialize: impl FnOnce() -> O::Head) -> &mut O {
        if self.get_mut().is_none() {
            let _ = self.set(initialize());
        }
        self.get_mut().expect("once slot remained uninitialized")
    }

    /// Fallibly initializes the slot and returns its child observer using exclusive access.
    pub fn get_or_try_init_mut<Error>(
        &mut self,
        initialize: impl FnOnce() -> Result<O::Head, Error>,
    ) -> Result<&mut O, Error> {
        if self.get_mut().is_none() {
            let _ = self.set(initialize()?);
        }
        Ok(self.get_mut().expect("once slot remained uninitialized"))
    }

    /// Takes the initialized value, if any.
    pub fn take(&mut self) -> Option<O::Head> {
        let head = unsafe { Pointer::as_mut(&self.pointer) };
        let value = OnceSlot::take(AsDerefMut::<Depth>::as_deref_mut(head));
        if value.is_some() {
            self.child.invalidate();
        }
        value
    }
}

/// Observer for [`OnceCell<T>`].
pub type OnceCellObserver<O, Head, Depth = Zero> =
    OnceObserver<O, OnceCell<<O as Observer>::Head>, Head, Depth>;

#[cfg(feature = "std")]
/// Observer for [`OnceLock<T>`].
pub type OnceLockObserver<O, Head, Depth = Zero> =
    OnceObserver<O, OnceLock<<O as Observer>::Head>, Head, Depth>;

impl<T, Selection> Observe<OnceCell<T>, Composite<(Selection,)>> for OnceCell<T>
where
    T: Observe<T, Selection>,
{
    type Observer<Head, Depth>
        = OnceCellObserver<<T as Observe<T, Selection>>::Observer<T, Zero>, Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}

#[cfg(feature = "std")]
impl<T, Selection> Observe<OnceLock<T>, Composite<(Selection,)>> for OnceLock<T>
where
    T: Observe<T, Selection>,
{
    type Observer<Head, Depth>
        = OnceLockObserver<<T as Observe<T, Selection>>::Observer<T, Zero>, Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}
