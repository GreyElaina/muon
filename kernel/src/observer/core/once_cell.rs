//! Conditional child observation for once-initialized slots.

use core::cell::{Cell, OnceCell, Ref, RefCell};
use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};
#[cfg(feature = "std")]
use std::sync::OnceLock;

use crate::{
    AsDeref, AsDerefMut, Change, Collect, Composite, Observe, Path, Query, Replace, Scope, emit,
};
use crate::{Observer, Pointer, QuasiObserver, Succ, Unsigned, Zero};

trait OnceSlot<T> {
    fn get(&self) -> Option<&T>;
    fn get_mut(&mut self) -> Option<&mut T>;
    fn set(&self, value: T) -> Result<(), T>;
    fn take(&mut self) -> Option<T>;
}

impl<T> OnceSlot<T> for OnceCell<T> {
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
impl<T> OnceSlot<T> for OnceLock<T> {
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
pub struct OnceObserver<T, O, Slot, Head: ?Sized, Depth = Zero> {
    pointer: Pointer<Head>,
    child: RefCell<Option<O>>,
    mutated: Cell<bool>,
    suppress_escape: Cell<bool>,

    marker: PhantomData<(fn(T, Slot), Depth)>,
}

impl<T, O, Slot, Head: ?Sized, Depth> OnceObserver<T, O, Slot, Head, Depth> {
    fn escape(&self) {
        self.mutated.set(true);
    }

    fn invalidate(&mut self) {
        self.mutated.set(true);
        *self.child.get_mut() = None;
    }
}

impl<T, O, Slot, Head: ?Sized, Depth> Deref for OnceObserver<T, O, Slot, Head, Depth> {
    type Target = Pointer<Head>;

    fn deref(&self) -> &Self::Target {
        if !self.suppress_escape.replace(false) {
            self.escape();
        }
        &self.pointer
    }
}

impl<T, O, Slot, Head: ?Sized, Depth> DerefMut for OnceObserver<T, O, Slot, Head, Depth> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        if !self.suppress_escape.replace(false) {
            self.invalidate();
        }
        &mut self.pointer
    }
}

impl<T, O, Slot, Head: ?Sized, Depth> QuasiObserver for OnceObserver<T, O, Slot, Head, Depth>
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

unsafe impl<T, O, Slot, Head: ?Sized, Depth> Observer for OnceObserver<T, O, Slot, Head, Depth>
where
    O: Observer<Head = T, InnerDepth = Zero>,
    Slot: OnceSlot<T>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Slot>,
{
    unsafe fn observe(head: *mut Head) -> Self {
        unsafe {
            let slot = AsDeref::<Depth>::as_deref_ptr(head);
            let child = OnceSlot::get_mut(&mut *slot).map(|value| O::observe(value));
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
            match (this.child.get_mut(), OnceSlot::get_mut(&mut *slot)) {
                (Some(observer), Some(value)) => O::relocate(observer, value),
                (None, _) => {}
                (Some(_), None) => panic!("inconsistent once-slot observer state"),
            }
            Pointer::set_unchecked(&this.pointer, head);
        }
    }

    unsafe fn rebase(this: &mut Self, head: *mut Head) {
        unsafe {
            let slot = AsDeref::<Depth>::as_deref_ptr(head);
            match (this.child.get_mut(), OnceSlot::get_mut(&mut *slot)) {
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
    for OnceObserver<T, O, Slot, Head, Depth>
where
    O: Observer<Head = T, InnerDepth = Zero>
        + Collect<Context, InnerRoute, Error, Scope<Semantic, Tail>>,
    Slot: OnceSlot<T>,
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
impl<T, O, Slot, Head: ?Sized, Depth> OnceObserver<T, O, Slot, Head, Depth>
where
    O: Observer<Head = T, InnerDepth = Zero>,
    Slot: OnceSlot<T>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Slot>,
{
    /// Returns a shared observer for an initialized value attached before this access.
    ///
    /// Shared initialization records a parent replacement and makes the child available after the
    /// next rebase; exclusive [`Self::get_mut`] can attach it immediately.
    pub fn get(&self) -> Option<Ref<'_, O>> {
        Ref::filter_map(self.child.borrow(), Option::as_ref).ok()
    }

    /// Returns the observer for the initialized value mutably.
    pub fn get_mut(&mut self) -> Option<&mut O> {
        let head = unsafe { Pointer::as_mut(&self.pointer) };
        let value = OnceSlot::get_mut(AsDerefMut::<Depth>::as_deref_mut(head))?;
        let observer = match self.child.get_mut() {
            Some(observer) => observer,
            slot @ None => slot.insert(unsafe { O::observe(value) }),
        };
        unsafe { O::relocate(observer, value) }
        Some(observer)
    }

    /// Initializes the slot, falling back to a whole-slot replacement on success.
    pub fn set(&self, value: T) -> Result<(), T> {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        match OnceSlot::set(AsDeref::<Depth>::as_deref(head), value) {
            Ok(()) => {
                self.escape();
                Ok(())
            }
            Err(value) => Err(value),
        }
    }

    /// Initializes the slot when empty and returns its value through a conservative parent escape.
    pub fn get_or_init(&self, initialize: impl FnOnce() -> T) -> &T {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let slot = AsDeref::<Depth>::as_deref(head);
        if OnceSlot::get(slot).is_none() {
            let _ = OnceSlot::set(slot, initialize());
        }
        self.escape();
        OnceSlot::get(slot).expect("once slot remained uninitialized")
    }

    /// Fallibly initializes the slot and returns its value through a conservative parent escape.
    pub fn get_or_try_init<Error>(
        &self,
        initialize: impl FnOnce() -> Result<T, Error>,
    ) -> Result<&T, Error> {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let slot = AsDeref::<Depth>::as_deref(head);
        if OnceSlot::get(slot).is_none() {
            let _ = OnceSlot::set(slot, initialize()?);
        }
        self.escape();
        Ok(OnceSlot::get(slot).expect("once slot remained uninitialized"))
    }

    /// Initializes the slot when empty and returns its child observer using exclusive access.
    pub fn get_or_init_mut(&mut self, initialize: impl FnOnce() -> T) -> &mut O {
        if self.get_mut().is_none() {
            let _ = self.set(initialize());
        }
        self.get_mut().expect("once slot remained uninitialized")
    }

    /// Fallibly initializes the slot and returns its child observer using exclusive access.
    pub fn get_or_try_init_mut<Error>(
        &mut self,
        initialize: impl FnOnce() -> Result<T, Error>,
    ) -> Result<&mut O, Error> {
        if self.get_mut().is_none() {
            let _ = self.set(initialize()?);
        }
        Ok(self.get_mut().expect("once slot remained uninitialized"))
    }

    /// Takes the initialized value, if any.
    pub fn take(&mut self) -> Option<T> {
        let head = unsafe { Pointer::as_mut(&self.pointer) };
        let value = OnceSlot::take(AsDerefMut::<Depth>::as_deref_mut(head));
        if value.is_some() {
            self.invalidate();
        }
        value
    }
}

/// Observer for [`OnceCell<T>`].
pub type OnceCellObserver<T, O, Head, Depth = Zero> = OnceObserver<T, O, OnceCell<T>, Head, Depth>;

#[cfg(feature = "std")]
/// Observer for [`OnceLock<T>`].
pub type OnceLockObserver<T, O, Head, Depth = Zero> = OnceObserver<T, O, OnceLock<T>, Head, Depth>;

impl<T, Selection> Observe<OnceCell<T>, Composite<(Selection,)>> for OnceCell<T>
where
    T: Observe<T, Selection>,
{
    type Observer<Head, Depth>
        = OnceCellObserver<T, <T as Observe<T, Selection>>::Observer<T, Zero>, Head, Depth>
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
        = OnceLockObserver<T, <T as Observe<T, Selection>>::Observer<T, Zero>, Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}
