//! Structural observation through [`Cell`]'s tracked access surface.

use core::cell::Cell;
use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};

use crate::{
    AsDeref, AsDerefMut, Change, Collect, Composite, Observe, Path, Query, Replace, Scope, emit,
};

use crate::{Observer, ObserverSlot, Pointer, QuasiObserver, Succ, Unsigned, Zero};

/// Observer for the value stored inside a [`Cell`].
///
/// Observed access stays structural: [`CellObserver::get`] reads through the child observer and
/// [`CellObserver::get_mut`] exposes that observer directly. Escaping to the underlying `Cell`
/// through [`Deref`] conservatively replaces the whole cell, because arbitrary shared code may
/// mutate it.
pub struct CellObserver<O, Head: ?Sized, Depth = Zero> {
    pointer: Pointer<Head>,
    child: ObserverSlot<O>,

    marker: PhantomData<Depth>,
}

impl<O, Head: ?Sized, Depth> Deref for CellObserver<O, Head, Depth> {
    type Target = Pointer<Head>;

    fn deref(&self) -> &Self::Target {
        self.child.invalidate();
        &self.pointer
    }
}

impl<O, Head: ?Sized, Depth> DerefMut for CellObserver<O, Head, Depth> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.child.invalidate();
        &mut self.pointer
    }
}

impl<O, Head: ?Sized, Depth> QuasiObserver for CellObserver<O, Head, Depth>
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

unsafe impl<O, Head: ?Sized, Depth> Observer for CellObserver<O, Head, Depth>
where
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Cell<O::Head>>,
{
    type Head = Head;

    unsafe fn observe(head: *mut Head) -> Self {
        unsafe {
            let cell = AsDeref::<Depth>::as_deref_ptr(head);
            Self {
                pointer: Pointer::new_unchecked(head),
                child: ObserverSlot::new(O::observe((*cell).as_ptr())),

                marker: PhantomData,
            }
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Head) {
        unsafe {
            let cell = AsDeref::<Depth>::as_deref_ptr(head);
            this.child.activate((*cell).as_ptr());
            Pointer::set_unchecked(&this.pointer, head);
        }
    }

    unsafe fn rebase(this: &mut Self, head: *mut Head) {
        unsafe {
            let cell = AsDeref::<Depth>::as_deref_ptr(head);
            this.child.rebase((*cell).as_ptr());
            Pointer::set_unchecked(&this.pointer, head);
        }
    }
}

impl<O, Head: ?Sized, Depth, Context: ?Sized, ParentRoute, InnerRoute, Error, Semantic, Tail>
    Collect<Context, (ParentRoute, InnerRoute), Error, Scope<Semantic, Tail>>
    for CellObserver<O, Head, Depth>
where
    O: Observer<InnerDepth = Zero> + Collect<Context, InnerRoute, Error, Scope<Semantic, Tail>>,
    O::Head: Sized,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Cell<O::Head>>,
    for<'a> Context: Query<Change<'a, Cell<O::Head>>, ParentRoute, Semantic>,
    for<'a> <Context as Query<Change<'a, Cell<O::Head>>, ParentRoute, Semantic>>::Output:
        Replace<Cell<O::Head>, Cell<O::Head>>,
    for<'a> Error: From<
        <<Context as Query<Change<'a, Cell<O::Head>>, ParentRoute, Semantic>>::Output as Replace<
            Cell<O::Head>,
            Cell<O::Head>,
        >>::Error,
    >,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        if !self.child.is_exact() {
            let head = unsafe { Pointer::as_ref(&self.pointer) };
            let cell = AsDeref::<Depth>::as_deref(head);
            return emit::<_, _, Context, ParentRoute, Semantic, Error>(
                context,
                Change::Replace {
                    path,
                    before: None,
                    after: cell,
                },
            );
        }

        Collect::<Context, InnerRoute, Error, Scope<Semantic, Tail>>::collect(
            unsafe { self.child.observer_mut() },
            path,
            context,
        )
    }
}

impl<O, Head: ?Sized, Depth> CellObserver<O, Head, Depth>
where
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized + AsDeref<Zero, Target = O::Head>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Cell<O::Head>>,
{
    fn refresh_child(&mut self) {
        if !self.child.is_stale() {
            return;
        }
        let head = unsafe { Pointer::as_mut(&self.pointer) };
        let cell = AsDerefMut::<Depth>::as_deref_mut(head);
        unsafe { self.child.activate(cell.as_ptr()) };
    }

    /// Returns an untracked shared reference through the child observer.
    ///
    /// The mutable receiver prevents another access through this observer while the returned
    /// reference remains live.
    pub fn get(&mut self) -> &O::Head {
        self.refresh_child();
        let observer = unsafe { self.child.observer_mut() };
        let pointer =
            <O as crate::AsDerefCoinductive<O::OuterDepth>>::as_deref_coinductive(observer);
        unsafe { Pointer::as_ref(pointer) }
    }

    /// Returns the observer for the value currently stored in the cell.
    pub fn get_mut(&mut self) -> &mut O {
        self.refresh_child();
        unsafe { self.child.observer_mut() }
    }

    /// Replaces the stored value and conservatively records a whole-cell replacement.
    pub fn set(&self, value: O::Head) {
        self.child.invalidate();
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        AsDeref::<Depth>::as_deref(head).set(value);
    }

    /// Replaces the stored value, returning its previous value.
    pub fn replace(&self, value: O::Head) -> O::Head {
        self.child.invalidate();
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        AsDeref::<Depth>::as_deref(head).replace(value)
    }

    /// Swaps this cell's value with another cell and conservatively records this cell as replaced.
    pub fn swap(&self, other: &Cell<O::Head>) {
        self.child.invalidate();
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        AsDeref::<Depth>::as_deref(head).swap(other);
    }

    /// Updates the stored value in place.
    pub fn update(&self, update: impl FnOnce(O::Head) -> O::Head)
    where
        O::Head: Copy,
    {
        self.child.invalidate();
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        AsDeref::<Depth>::as_deref(head).update(update)
    }

    /// Takes the stored value, leaving its default behind.
    pub fn take(&self) -> O::Head
    where
        O::Head: Default,
    {
        self.replace(O::Head::default())
    }
}

impl<T, Selection> Observe<Cell<T>, Composite<(Selection,)>> for Cell<T>
where
    T: Observe<T, Selection>,
{
    type Observer<Head, Depth>
        = CellObserver<<T as Observe<T, Selection>>::Observer<T, Zero>, Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}
