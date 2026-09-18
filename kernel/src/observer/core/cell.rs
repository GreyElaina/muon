//! Structural observation through [`Cell`]'s tracked access surface.

use core::cell::Cell;
use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};

use crate::{
    AsDeref, AsDerefMut, Change, Collect, Composite, Observe, Path, Query, Replace, Scope, emit,
};

use crate::{InteriorState, Observer, Pointer, QuasiObserver, Succ, Unsigned, Zero};

/// Observer for the value stored inside a [`Cell`].
///
/// Observed access stays structural: [`CellObserver::get`] reads through the child observer and
/// [`CellObserver::get_mut`] exposes that observer directly. Escaping to the underlying `Cell`
/// through [`Deref`] conservatively replaces the whole cell, because arbitrary shared code may
/// mutate it.
pub struct CellObserver<T, O, Head: ?Sized, Depth = Zero> {
    pointer: Pointer<Head>,
    state: InteriorState<O>,

    marker: PhantomData<(T, Depth)>,
}

impl<T, O, Head: ?Sized, Depth> CellObserver<T, O, Head, Depth> {
    fn escape(&self) {
        self.state.escape();
    }
}

impl<T, O, Head: ?Sized, Depth> Deref for CellObserver<T, O, Head, Depth> {
    type Target = Pointer<Head>;

    fn deref(&self) -> &Self::Target {
        if !self.state.take_suppression() {
            self.escape();
        }
        &self.pointer
    }
}

impl<T, O, Head: ?Sized, Depth> DerefMut for CellObserver<T, O, Head, Depth> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        if !self.state.take_suppression() {
            self.escape();
        }
        &mut self.pointer
    }
}

impl<T, O, Head: ?Sized, Depth> QuasiObserver for CellObserver<T, O, Head, Depth>
where
    O: QuasiObserver<Head = T, InnerDepth = Zero>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth>,
{
    type Head = Head;
    type OuterDepth = Succ<Zero>;
    type InnerDepth = Depth;

    fn invalidate(this: &mut Self) {
        this.escape();
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
        self.state.suppress_escape();
        let head = <Pointer<Head> as crate::DerefMutUntracked>::deref_mut_untracked::<
            Self,
            Succ<Zero>,
        >(self);
        AsDerefMut::<Depth>::as_deref_mut(head)
    }
}

unsafe impl<T, O, Head: ?Sized, Depth> Observer for CellObserver<T, O, Head, Depth>
where
    O: Observer<Head = T, InnerDepth = Zero>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Cell<T>>,
{
    unsafe fn observe(head: *mut Head) -> Self {
        unsafe {
            let cell = AsDeref::<Depth>::as_deref_ptr(head);
            Self {
                pointer: Pointer::new_unchecked(head),
                state: InteriorState::new(O::observe((*cell).as_ptr())),

                marker: PhantomData,
            }
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Head) {
        unsafe {
            let cell = AsDeref::<Depth>::as_deref_ptr(head);
            O::relocate(this.state.child_mut(), (*cell).as_ptr());
            Pointer::set_unchecked(&this.pointer, head);
        }
    }

    unsafe fn rebase(this: &mut Self, head: *mut Head) {
        unsafe {
            let cell = AsDeref::<Depth>::as_deref_ptr(head);
            this.state.rebase((*cell).as_ptr());
            Pointer::set_unchecked(&this.pointer, head);
        }
    }
}

impl<T, O, Head: ?Sized, Depth, Context: ?Sized, ParentRoute, InnerRoute, Error, Semantic, Tail>
    Collect<Context, (ParentRoute, InnerRoute), Error, Scope<Semantic, Tail>>
    for CellObserver<T, O, Head, Depth>
where
    O: Observer<Head = T, InnerDepth = Zero>
        + Collect<Context, InnerRoute, Error, Scope<Semantic, Tail>>,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Cell<T>>,
    for<'a> Context: Query<Change<'a, Cell<T>>, ParentRoute, Semantic>,
    for<'a> <Context as Query<Change<'a, Cell<T>>, ParentRoute, Semantic>>::Output:
        Replace<Cell<T>, Cell<T>>,
    for<'a> Error: From<
        <<Context as Query<Change<'a, Cell<T>>, ParentRoute, Semantic>>::Output as Replace<
            Cell<T>,
            Cell<T>,
        >>::Error,
    >,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        if self.state.is_mutated() {
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
            self.state.child_mut(),
            path,
            context,
        )
    }
}

impl<T, O, Head: ?Sized, Depth> CellObserver<T, O, Head, Depth>
where
    O: Observer<Head = T, InnerDepth = Zero>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Cell<T>>,
{
    fn refresh_child(&mut self) {
        if !self.state.take_stale() {
            return;
        }
        let head = unsafe { Pointer::as_mut(&self.pointer) };
        let cell = AsDerefMut::<Depth>::as_deref_mut(head);
        *self.state.child_mut() = unsafe { O::observe(cell.as_ptr()) };
    }

    /// Returns an untracked shared reference through the child observer.
    ///
    /// The mutable receiver prevents another access through this observer while the returned
    /// reference remains live.
    pub fn get(&mut self) -> &T {
        self.refresh_child();
        QuasiObserver::untracked_ref(self.state.child_mut())
    }

    /// Returns the observer for the value currently stored in the cell.
    pub fn get_mut(&mut self) -> &mut O {
        self.refresh_child();
        self.state.child_mut()
    }

    /// Replaces the stored value and conservatively records a whole-cell replacement.
    pub fn set(&self, value: T) {
        self.escape();
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        AsDeref::<Depth>::as_deref(head).set(value);
    }

    /// Replaces the stored value, returning its previous value.
    pub fn replace(&self, value: T) -> T {
        self.escape();
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        AsDeref::<Depth>::as_deref(head).replace(value)
    }

    /// Swaps this cell's value with another cell and conservatively records this cell as replaced.
    pub fn swap(&self, other: &Cell<T>) {
        self.escape();
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        AsDeref::<Depth>::as_deref(head).swap(other);
    }

    /// Updates the stored value in place.
    pub fn update(&self, update: impl FnOnce(T) -> T)
    where
        T: Copy,
    {
        self.escape();
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        AsDeref::<Depth>::as_deref(head).update(update)
    }

    /// Takes the stored value, leaving its default behind.
    pub fn take(&self) -> T
    where
        T: Default,
    {
        self.replace(T::default())
    }
}

impl<T, Selection> Observe<Cell<T>, Composite<(Selection,)>> for Cell<T>
where
    T: Observe<T, Selection>,
{
    type Observer<Head, Depth>
        = CellObserver<T, <T as Observe<T, Selection>>::Observer<T, Zero>, Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}
