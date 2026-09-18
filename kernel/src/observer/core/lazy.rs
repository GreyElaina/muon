//! Conditional child observation for lazily initialized slots.

use core::cell::{Cell, LazyCell, Ref, RefCell};
use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};
#[cfg(feature = "std")]
use std::sync::LazyLock;

use crate::{
    AsDeref, AsDerefMut, Change, Collect, Composite, Observe, Path, Query, Replace, Scope, emit,
};
use crate::{Observer, Pointer, QuasiObserver, Succ, Unsigned, Zero};

trait LazySlot<T> {
    fn get(&self) -> Option<&T>;
    fn get_mut(&mut self) -> Option<&mut T>;
    fn force(&self) -> &T;
    fn force_mut(&mut self) -> &mut T;
}

impl<T, F: FnOnce() -> T> LazySlot<T> for LazyCell<T, F> {
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
impl<T, F: FnOnce() -> T> LazySlot<T> for LazyLock<T, F> {
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
pub struct LazyObserver<T, O, Slot, Head: ?Sized, Depth = Zero> {
    pointer: Pointer<Head>,
    child: RefCell<Option<O>>,
    mutated: Cell<bool>,
    suppress_escape: Cell<bool>,

    marker: PhantomData<(fn(T, Slot), Depth)>,
}

impl<T, O, Slot, Head: ?Sized, Depth> LazyObserver<T, O, Slot, Head, Depth> {
    fn escape(&self) {
        self.mutated.set(true);
    }

    fn invalidate(&mut self) {
        self.mutated.set(true);
        *self.child.get_mut() = None;
    }
}

impl<T, O, Slot, Head: ?Sized, Depth> Deref for LazyObserver<T, O, Slot, Head, Depth> {
    type Target = Pointer<Head>;

    fn deref(&self) -> &Self::Target {
        if !self.suppress_escape.replace(false) {
            self.escape();
        }
        &self.pointer
    }
}

impl<T, O, Slot, Head: ?Sized, Depth> DerefMut for LazyObserver<T, O, Slot, Head, Depth> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        if !self.suppress_escape.replace(false) {
            self.invalidate();
        }
        &mut self.pointer
    }
}

impl<T, O, Slot, Head: ?Sized, Depth> QuasiObserver for LazyObserver<T, O, Slot, Head, Depth>
where
    O: QuasiObserver<Head = T, InnerDepth = Zero>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth>,
{
    type Head = Head;
    type OuterDepth = Succ<Zero>;
    type InnerDepth = Depth;

    fn invalidate(this: &mut Self) {
        this.invalidate();
    }

    fn untracked_ref<Value: ?Sized>(&self) -> &Value
    where
        Self::Head: AsDeref<Self::InnerDepth, Target = Value>,
    {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        AsDeref::<Depth>::as_deref(head)
    }

    fn untracked_mut<Value: ?Sized>(&mut self) -> &mut Value
    where
        Head: AsDerefMut<Depth, Target = Value>,
    {
        self.suppress_escape.set(true);
        let head = <Pointer<Head> as crate::DerefMutUntracked>::deref_mut_untracked::<
            Self,
            Succ<Zero>,
        >(self);
        AsDerefMut::<Depth>::as_deref_mut(head)
    }
}

unsafe impl<T, O, Slot, Head: ?Sized, Depth> Observer for LazyObserver<T, O, Slot, Head, Depth>
where
    O: Observer<Head = T, InnerDepth = Zero>,
    Slot: LazySlot<T>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Slot>,
{
    unsafe fn observe(head: *mut Head) -> Self {
        unsafe {
            let slot = AsDeref::<Depth>::as_deref_ptr(head);
            let child = LazySlot::get_mut(&mut *slot).map(|value| O::observe(value));
            Self {
                pointer: Pointer::new_unchecked(head),
                child: RefCell::new(child),
                mutated: Cell::new(false),
                suppress_escape: Cell::new(false),

                marker: PhantomData,
            }
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Head) {
        unsafe {
            let slot = AsDeref::<Depth>::as_deref_ptr(head);
            match (this.child.get_mut(), LazySlot::get_mut(&mut *slot)) {
                (Some(observer), Some(value)) => O::relocate(observer, value),
                (None, _) => {}
                (Some(_), None) => panic!("inconsistent lazy-slot observer state"),
            }
            Pointer::set_unchecked(&this.pointer, head);
        }
    }

    unsafe fn rebase(this: &mut Self, head: *mut Head) {
        unsafe {
            let slot = AsDeref::<Depth>::as_deref_ptr(head);
            match (this.child.get_mut(), LazySlot::get_mut(&mut *slot)) {
                (Some(observer), Some(value)) => O::rebase(observer, value),
                (child @ None, Some(value)) => *child = Some(O::observe(value)),
                (child @ Some(_), None) => *child = None,
                (None, None) => {}
            }
            this.mutated.set(false);
            this.suppress_escape.set(false);
            Pointer::set_unchecked(&this.pointer, head);
        }
    }
}

impl<
    T,
    O,
    Slot,
    Head: ?Sized,
    Depth,
    Context: ?Sized,
    ParentRoute,
    InnerRoute,
    Error,
    Semantic,
    Tail,
> Collect<Context, (ParentRoute, InnerRoute), Error, Scope<Semantic, Tail>>
    for LazyObserver<T, O, Slot, Head, Depth>
where
    O: Observer<Head = T, InnerDepth = Zero>
        + Collect<Context, InnerRoute, Error, Scope<Semantic, Tail>>,
    Slot: LazySlot<T>,
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
        if self.mutated.get() {
            return emit::<_, _, Context, ParentRoute, Semantic, Error>(
                context,
                Change::Replace {
                    path,
                    before: None,
                    after: slot,
                },
            );
        }

        if let Some(observer) = self.child.get_mut() {
            Collect::<Context, InnerRoute, Error, Scope<Semantic, Tail>>::collect(
                observer, path, context,
            )?;
        }
        Ok(())
    }
}

#[allow(private_bounds)]
impl<T, O, Slot, Head: ?Sized, Depth> LazyObserver<T, O, Slot, Head, Depth>
where
    O: Observer<Head = T, InnerDepth = Zero>,
    Slot: LazySlot<T>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Slot>,
{
    /// Returns a shared observer for an initialized value attached before this access.
    ///
    /// Shared forcing records a parent replacement and makes the child available after the next
    /// rebase; exclusive [`Self::get_mut`] can attach it immediately.
    pub fn get(&self) -> Option<Ref<'_, O>> {
        Ref::filter_map(self.child.borrow(), Option::as_ref).ok()
    }

    /// Returns the observer for the initialized value mutably without forcing initialization.
    pub fn get_mut(&mut self) -> Option<&mut O> {
        let head = unsafe { Pointer::as_mut(&self.pointer) };
        let value = LazySlot::get_mut(AsDerefMut::<Depth>::as_deref_mut(head))?;
        let observer = match self.child.get_mut() {
            Some(observer) => observer,
            slot @ None => slot.insert(unsafe { O::observe(value) }),
        };
        unsafe { O::relocate(observer, value) }
        Some(observer)
    }

    /// Forces initialization and returns the value through a conservative parent escape.
    pub fn force(&self) -> &T {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let slot = AsDeref::<Depth>::as_deref(head);
        self.escape();
        LazySlot::force(slot)
    }

    /// Forces initialization and returns the child observer using exclusive access.
    pub fn force_mut(&mut self) -> &mut O {
        let head = unsafe { Pointer::as_mut(&self.pointer) };
        let slot = AsDerefMut::<Depth>::as_deref_mut(head);
        if LazySlot::get(slot).is_none() {
            self.escape();
        }
        let value = LazySlot::force_mut(slot);
        let observer = match self.child.get_mut() {
            Some(observer) => observer,
            child @ None => child.insert(unsafe { O::observe(value) }),
        };
        unsafe { O::relocate(observer, value) }
        observer
    }
}

/// Observer for [`LazyCell<T, F>`].
pub type LazyCellObserver<T, F, O, Head, Depth = Zero> =
    LazyObserver<T, O, LazyCell<T, F>, Head, Depth>;

#[cfg(feature = "std")]
/// Observer for [`LazyLock<T, F>`].
pub type LazyLockObserver<T, F, O, Head, Depth = Zero> =
    LazyObserver<T, O, LazyLock<T, F>, Head, Depth>;

impl<T, F, Selection> Observe<LazyCell<T, F>, Composite<(Selection,)>> for LazyCell<T, F>
where
    F: FnOnce() -> T,
    T: Observe<T, Selection>,
{
    type Observer<Head, Depth>
        = LazyCellObserver<T, F, <T as Observe<T, Selection>>::Observer<T, Zero>, Head, Depth>
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
        = LazyLockObserver<T, F, <T as Observe<T, Selection>>::Observer<T, Zero>, Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}
