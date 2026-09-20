//! Structural observation through [`RefCell`]'s dynamic borrow guards.

use core::cell::{BorrowError, BorrowMutError, Ref, RefCell, RefMut};
use core::fmt;
use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};

use crate::{
    AsDeref, AsDerefMut, Change, Collect, Composite, Observe, Path, Query, Replace, Scope, emit,
};

use crate::{
    ObservedGuard, ObservedGuardMut, Observer, ObserverSlot, Pointer, QuasiObserver, Succ,
    Unsigned, Zero,
};

/// Shared dynamic borrow paired with the observed child.
pub type ObservedRef<'a, T, O> = ObservedGuard<'a, Ref<'a, T>, O>;

/// Exclusive dynamic borrow paired with the observed child.
pub type ObservedRefMut<'a, T, O> = ObservedGuardMut<'a, RefMut<'a, T>, O>;

/// Failure to acquire a shared observed borrow.
#[derive(Debug)]
pub enum TryObservedBorrowError {
    /// The underlying `RefCell` rejected a shared borrow.
    Borrow(BorrowError),
    /// A prior escape made the child stale and an exclusive borrow was unavailable for rebuilding
    /// it.
    Rebuild(BorrowMutError),
}

impl fmt::Display for TryObservedBorrowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Borrow(error) => error.fmt(formatter),
            Self::Rebuild(error) => write!(formatter, "cannot rebuild child observer: {error}"),
        }
    }
}

impl core::error::Error for TryObservedBorrowError {}

/// Observer for the value stored inside a [`RefCell`].
///
/// The child observer persists in the observation tree, while every access that uses it is
/// protected by a fresh [`Ref`] or [`RefMut`] guard. Escaping to the underlying `RefCell`, or a
/// leaked dynamic borrow detected during collection, conservatively replaces the whole cell.
pub struct RefCellObserver<T, O, Head: ?Sized, Depth = Zero> {
    pointer: Pointer<Head>,
    child: ObserverSlot<O>,

    marker: PhantomData<(T, Depth)>,
}

impl<T, O, Head: ?Sized, Depth> Deref for RefCellObserver<T, O, Head, Depth> {
    type Target = Pointer<Head>;

    fn deref(&self) -> &Self::Target {
        self.child.invalidate();
        &self.pointer
    }
}

impl<T, O, Head: ?Sized, Depth> DerefMut for RefCellObserver<T, O, Head, Depth> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.child.invalidate();
        &mut self.pointer
    }
}

impl<T, O, Head: ?Sized, Depth> QuasiObserver for RefCellObserver<T, O, Head, Depth>
where
    O: QuasiObserver<Head = T, InnerDepth = Zero>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth>,
{
    type Head = Head;
    type OuterDepth = Succ<Zero>;
    type InnerDepth = Depth;

    fn invalidate(this: &mut Self) {
        this.child.invalidate();
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
        let head = unsafe { Pointer::as_mut(&self.pointer) };
        AsDerefMut::<Depth>::as_deref_mut(head)
    }
}

unsafe impl<T, O, Head: ?Sized, Depth> Observer for RefCellObserver<T, O, Head, Depth>
where
    O: Observer<Head = T, InnerDepth = Zero>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = RefCell<T>>,
{
    unsafe fn observe(head: *mut Head) -> Self {
        unsafe {
            let cell = AsDeref::<Depth>::as_deref_ptr(head);
            Self {
                pointer: Pointer::new_unchecked(head),
                child: ObserverSlot::new(O::observe((*cell).get_mut())),

                marker: PhantomData,
            }
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Head) {
        unsafe {
            let cell = AsDeref::<Depth>::as_deref_ptr(head);
            this.child.activate((*cell).get_mut());
            Pointer::set_unchecked(&this.pointer, head);
        }
    }

    unsafe fn rebase(this: &mut Self, head: *mut Head) {
        unsafe {
            let cell = AsDeref::<Depth>::as_deref_ptr(head);
            this.child.rebase((*cell).get_mut());
            Pointer::set_unchecked(&this.pointer, head);
        }
    }
}

impl<T, O, Head: ?Sized, Depth, Context: ?Sized, ParentRoute, InnerRoute, Error, Semantic, Tail>
    Collect<Context, (ParentRoute, InnerRoute), Error, Scope<Semantic, Tail>>
    for RefCellObserver<T, O, Head, Depth>
where
    O: Observer<Head = T, InnerDepth = Zero>
        + Collect<Context, InnerRoute, Error, Scope<Semantic, Tail>>,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = RefCell<T>>,
    for<'a> Context: Query<Change<'a, RefCell<T>>, ParentRoute, Semantic>,
    for<'a> <Context as Query<Change<'a, RefCell<T>>, ParentRoute, Semantic>>::Output:
        Replace<RefCell<T>, RefCell<T>>,
    for<'a> Error: From<
        <<Context as Query<Change<'a, RefCell<T>>, ParentRoute, Semantic>>::Output as Replace<
            RefCell<T>,
            RefCell<T>,
        >>::Error,
    >,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let cell = AsDeref::<Depth>::as_deref(head);
        if self.child.is_exact() {
            if let Ok(mut guard) = cell.try_borrow_mut() {
                unsafe { self.child.activate(&mut *guard) }
                return Collect::<Context, InnerRoute, Error, Scope<Semantic, Tail>>::collect(
                    unsafe { self.child.observer_mut() },
                    path,
                    context,
                );
            }
            self.child.invalidate();
        }

        emit::<_, _, Context, ParentRoute, Semantic, Error>(
            context,
            Change::Replace {
                path,
                before: None,
                after: cell,
            },
        )
    }
}

impl<T, O, Head: ?Sized, Depth> RefCellObserver<T, O, Head, Depth>
where
    O: Observer<Head = T, InnerDepth = Zero>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = RefCell<T>>,
{
    /// Returns the child observer using this observer's exclusive access to the cell.
    pub fn get_mut(&mut self) -> &mut O {
        let head = unsafe { Pointer::as_mut(&self.pointer) };
        let value = AsDerefMut::<Depth>::as_deref_mut(head).get_mut();
        unsafe { self.child.activate(value) };
        unsafe { self.child.observer_mut() }
    }

    /// Immutably borrows the stored value and pairs the borrow with its child observer.
    pub fn borrow(&self) -> ObservedRef<'_, T, O> {
        self.try_borrow().unwrap_or_else(|error| panic!("{error}"))
    }

    /// Attempts to immutably borrow the value and pair it with its child observer.
    pub fn try_borrow(&self) -> Result<ObservedRef<'_, T, O>, TryObservedBorrowError> {
        if self.child.is_stale() {
            let head = unsafe { Pointer::as_ref(&self.pointer) };
            let cell = AsDeref::<Depth>::as_deref(head);
            let mut guard = cell
                .try_borrow_mut()
                .map_err(TryObservedBorrowError::Rebuild)?;
            drop(unsafe { self.child.activate_ref(&mut *guard) });
        }

        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let cell = AsDeref::<Depth>::as_deref(head);
        let guard = cell.try_borrow().map_err(TryObservedBorrowError::Borrow)?;
        Ok(unsafe { self.child.read(guard) })
    }

    /// Mutably borrows the stored value and pairs the borrow with its child observer.
    pub fn borrow_mut(&self) -> ObservedRefMut<'_, T, O> {
        self.try_borrow_mut()
            .unwrap_or_else(|error| panic!("{error}"))
    }

    /// Attempts to mutably borrow the value and pair it with its child observer.
    pub fn try_borrow_mut(&self) -> Result<ObservedRefMut<'_, T, O>, BorrowMutError> {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let cell = AsDeref::<Depth>::as_deref(head);
        let guard = cell.try_borrow_mut()?;
        Ok(unsafe { self.child.write(guard) })
    }

    /// Replaces the stored value and conservatively records a whole-cell replacement.
    pub fn replace(&mut self, value: T) -> T {
        self.child.invalidate();
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        AsDeref::<Depth>::as_deref(head).replace(value)
    }

    /// Replaces the stored value with the result of an observed update closure.
    pub fn replace_with(&mut self, update: impl FnOnce(&mut O) -> T) -> T {
        let value = update(self.get_mut());
        self.replace(value)
    }

    /// Swaps this cell's value with another cell and conservatively records this cell as replaced.
    pub fn swap(&mut self, other: &RefCell<T>) {
        self.child.invalidate();
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        AsDeref::<Depth>::as_deref(head).swap(other);
    }

    /// Takes the stored value, leaving its default behind.
    pub fn take(&mut self) -> T
    where
        T: Default,
    {
        self.replace(T::default())
    }
}

impl<T, Selection> Observe<RefCell<T>, Composite<(Selection,)>> for RefCell<T>
where
    T: Observe<T, Selection>,
{
    type Observer<Head, Depth>
        = RefCellObserver<T, <T as Observe<T, Selection>>::Observer<T, Zero>, Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}
