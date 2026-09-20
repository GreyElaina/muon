//! Conditional child observation for lazily initialized slots.

use core::cell::{LazyCell, Ref};
use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};
#[cfg(feature = "std")]
use std::sync::LazyLock;

use crate::{
    AsDeref, AsDerefMut, Change, Collect, Composite, Observe, Path, Query, Replace, Scope, emit,
};
use crate::{Observer, ObserverSlot, Pointer, QuasiObserver, Succ, Unsigned, Zero};

trait LazySlot {
    type Value;

    fn get(&self) -> Option<&Self::Value>;
    fn get_mut(&mut self) -> Option<&mut Self::Value>;
    fn force(&self) -> &Self::Value;
    fn force_mut(&mut self) -> &mut Self::Value;
}

impl<T, F: FnOnce() -> T> LazySlot for LazyCell<T, F> {
    type Value = T;

    fn get(&self) -> Option<&T> {
        LazyCell::get(self)
    }

    fn get_mut(&mut self) -> Option<&mut T> {
        LazyCell::get_mut(self)
    }

    fn force(&self) -> &T {
        LazyCell::force(self)
    }

    fn force_mut(&mut self) -> &mut T {
        LazyCell::force_mut(self)
    }
}

#[cfg(feature = "std")]
impl<T, F: FnOnce() -> T> LazySlot for LazyLock<T, F> {
    type Value = T;

    fn get(&self) -> Option<&T> {
        LazyLock::get(self)
    }

    fn get_mut(&mut self) -> Option<&mut T> {
        LazyLock::get_mut(self)
    }

    fn force(&self) -> &T {
        LazyLock::force(self)
    }

    fn force_mut(&mut self) -> &mut T {
        LazyLock::force_mut(self)
    }
}

/// Observer for a lazily initialized slot.
///
/// An initialized slot owns a structural child observer. Forcing an uninitialized slot records a
/// whole-slot replacement for that observation pass; later passes recover structural precision.
pub struct LazyObserver<O, Slot, Head: ?Sized, Depth = Zero> {
    pointer: Pointer<Head>,
    child: ObserverSlot<Option<O>>,

    marker: PhantomData<(fn(Slot), Depth)>,
}

impl<O, Slot, Head: ?Sized, Depth> Deref for LazyObserver<O, Slot, Head, Depth> {
    type Target = Pointer<Head>;

    fn deref(&self) -> &Self::Target {
        self.child.invalidate();
        &self.pointer
    }
}

impl<O, Slot, Head: ?Sized, Depth> DerefMut for LazyObserver<O, Slot, Head, Depth> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.child.invalidate();
        &mut self.pointer
    }
}

impl<O, Slot, Head: ?Sized, Depth> QuasiObserver for LazyObserver<O, Slot, Head, Depth>
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

unsafe impl<O, Slot, Head: ?Sized, Depth> Observer for LazyObserver<O, Slot, Head, Depth>
where
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    Slot: LazySlot<Value = O::Head>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Slot>,
{
    type Head = Head;

    unsafe fn observe(head: *mut Head) -> Self {
        unsafe {
            let slot = AsDeref::<Depth>::as_deref_ptr(head);
            let child = LazySlot::get_mut(&mut *slot).map(|value| O::observe(value));
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
            let value = LazySlot::get_mut(&mut *slot).map(|value| value as *mut O::Head);
            this.child.activate_optional(value);
            Pointer::set_unchecked(&this.pointer, head);
        }
    }

    unsafe fn rebase(this: &mut Self, head: *mut Head) {
        unsafe {
            let slot = AsDeref::<Depth>::as_deref_ptr(head);
            let value = LazySlot::get_mut(&mut *slot).map(|value| value as *mut O::Head);
            this.child.rebase_optional(value);
            Pointer::set_unchecked(&this.pointer, head);
        }
    }
}

impl<O, Slot, Head: ?Sized, Depth, Context: ?Sized, ParentRoute, InnerRoute, Error, Semantic, Tail>
    Collect<Context, (ParentRoute, InnerRoute), Error, Scope<Semantic, Tail>>
    for LazyObserver<O, Slot, Head, Depth>
where
    O: Observer<InnerDepth = Zero> + Collect<Context, InnerRoute, Error, Scope<Semantic, Tail>>,
    Slot: LazySlot<Value = O::Head>,
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
impl<O, Slot, Head: ?Sized, Depth> LazyObserver<O, Slot, Head, Depth>
where
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    Slot: LazySlot<Value = O::Head>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Slot>,
{
    /// Returns a shared observer for an initialized value attached before this access.
    ///
    /// Shared forcing records a parent replacement; the next exclusive activation attaches the
    /// child, and a successful rebase restores exact collection.
    pub fn get(&self) -> Option<Ref<'_, O>> {
        self.child.get()
    }

    /// Returns the observer for the initialized value mutably without forcing initialization.
    pub fn get_mut(&mut self) -> Option<&mut O> {
        let head = unsafe { Pointer::as_mut(&self.pointer) };
        let value = LazySlot::get_mut(AsDerefMut::<Depth>::as_deref_mut(head));
        let value = value.map(|value| value as *mut O::Head);
        unsafe { self.child.activate_optional(value) };
        unsafe { self.child.observer_mut() }.as_mut()
    }

    /// Forces initialization and returns the value through a conservative parent escape.
    pub fn force(&self) -> &O::Head {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let slot = AsDeref::<Depth>::as_deref(head);
        self.child.invalidate();
        LazySlot::force(slot)
    }

    /// Forces initialization and returns the child observer using exclusive access.
    pub fn force_mut(&mut self) -> &mut O {
        let head = unsafe { Pointer::as_mut(&self.pointer) };
        let slot = AsDerefMut::<Depth>::as_deref_mut(head);
        if LazySlot::get(slot).is_none() {
            self.child.invalidate();
        }
        let value = LazySlot::force_mut(slot);
        unsafe { self.child.activate_optional(Some(value)) };
        unsafe { self.child.observer_mut() }
            .as_mut()
            .expect("forced lazy slot remained uninitialized")
    }
}

/// Observer for [`LazyCell<T, F>`].
pub type LazyCellObserver<F, O, Head, Depth = Zero> =
    LazyObserver<O, LazyCell<<O as Observer>::Head, F>, Head, Depth>;

#[cfg(feature = "std")]
/// Observer for [`LazyLock<T, F>`].
pub type LazyLockObserver<F, O, Head, Depth = Zero> =
    LazyObserver<O, LazyLock<<O as Observer>::Head, F>, Head, Depth>;

impl<T, F, Selection> Observe<LazyCell<T, F>, Composite<(Selection,)>> for LazyCell<T, F>
where
    F: FnOnce() -> T,
    T: Observe<T, Selection>,
{
    type Observer<Head, Depth>
        = LazyCellObserver<F, <T as Observe<T, Selection>>::Observer<T, Zero>, Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}

#[cfg(feature = "std")]
impl<T, F, Selection> Observe<LazyLock<T, F>, Composite<(Selection,)>> for LazyLock<T, F>
where
    F: FnOnce() -> T,
    T: Observe<T, Selection>,
{
    type Observer<Head, Depth>
        = LazyLockObserver<F, <T as Observe<T, Selection>>::Observer<T, Zero>, Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}
