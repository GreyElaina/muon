//! Observation of range-bound sum values from `core`.

use core::marker::PhantomData;
use core::ops::{Bound, Deref, DerefMut};

use crate::{Change, Collect, Composite, Field, Observe, Path, Query, Replace, Scope, emit};

use super::{AsDeref, AsDerefMut, Observer, Pointer, QuasiObserver, Succ, Unsigned, Zero};

enum BoundChild<O> {
    Included(Field<O>),
    Excluded(Field<O>),
}

struct BoundState<O> {
    mutated: bool,
    child: Option<BoundChild<O>>,
}

impl<O> BoundState<O> {
    fn invalidate(&mut self) {
        self.mutated = true;
        self.child = None;
    }
}

/// Observer for [`Bound<T>`].
pub struct BoundObserver<O, Head: ?Sized, Depth = Zero> {
    pointer: Pointer<Head>,
    state: BoundState<O>,

    marker: PhantomData<Depth>,
}

impl<O, Head: ?Sized, Depth> Deref for BoundObserver<O, Head, Depth> {
    type Target = Pointer<Head>;

    fn deref(&self) -> &Self::Target {
        &self.pointer
    }
}

impl<O, Head: ?Sized, Depth> DerefMut for BoundObserver<O, Head, Depth>
where
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Bound<O::Head>>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        QuasiObserver::invalidate(self);
        &mut self.pointer
    }
}

impl<O, Head: ?Sized, Depth> QuasiObserver for BoundObserver<O, Head, Depth>
where
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Bound<O::Head>>,
{
    type OuterDepth = Succ<Zero>;
    type InnerDepth = Depth;

    fn invalidate(this: &mut Self) {
        this.state.invalidate()
    }
}

unsafe impl<O, Head: ?Sized, Depth> Observer for BoundObserver<O, Head, Depth>
where
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Bound<O::Head>>,
{
    type Head = Head;

    unsafe fn observe(head: *mut Head) -> Self {
        unsafe {
            let bound = AsDeref::<Depth>::as_deref_ptr(head);
            let child = match &mut *bound {
                Bound::Included(value) => Some(BoundChild::Included(Field::named(
                    O::observe(value),
                    "Included",
                ))),
                Bound::Excluded(value) => Some(BoundChild::Excluded(Field::named(
                    O::observe(value),
                    "Excluded",
                ))),
                Bound::Unbounded => None,
            };
            Self {
                pointer: Pointer::new_unchecked(head),
                state: BoundState {
                    mutated: false,
                    child,
                },

                marker: PhantomData,
            }
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Head) {
        unsafe {
            let bound = AsDeref::<Depth>::as_deref_ptr(head);
            match (&mut this.state.child, &mut *bound) {
                (Some(BoundChild::Included(child)), Bound::Included(value))
                | (Some(BoundChild::Excluded(child)), Bound::Excluded(value)) => {
                    O::relocate(child.observer_mut(), value)
                }
                (None, _) => {}
                _ => panic!("inconsistent Bound observer state"),
            }
            Pointer::set_unchecked(&this.pointer, head);
        }
    }

    unsafe fn rebase(this: &mut Self, head: *mut Head) {
        unsafe {
            let bound = AsDeref::<Depth>::as_deref_ptr(head);
            match (&mut this.state.child, &mut *bound) {
                (Some(BoundChild::Included(child)), Bound::Included(value))
                | (Some(BoundChild::Excluded(child)), Bound::Excluded(value)) => {
                    O::rebase(child.observer_mut(), value)
                }
                (slot, Bound::Included(value)) => {
                    *slot = Some(BoundChild::Included(Field::named(
                        O::observe(value),
                        "Included",
                    )))
                }
                (slot, Bound::Excluded(value)) => {
                    *slot = Some(BoundChild::Excluded(Field::named(
                        O::observe(value),
                        "Excluded",
                    )))
                }
                (slot, Bound::Unbounded) => *slot = None,
            }
            this.state.mutated = false;
            Pointer::set_unchecked(&this.pointer, head);
        }
    }
}

impl<
    O,
    Head: ?Sized,
    Depth,
    Context: ?Sized,
    ParentRoute,
    InnerRoute,
    Error,
    Semantic,
    Tail,
> Collect<Context, (ParentRoute, InnerRoute), Error, Scope<Semantic, Tail>>
    for BoundObserver<O, Head, Depth>
where
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    Field<O>: Collect<Context, InnerRoute, Error, Scope<Semantic, Tail>>,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Bound<O::Head>>,
    for<'a> Context: Query<Change<'a, Bound<O::Head>>, ParentRoute, Semantic>,
    for<'a> <Context as Query<Change<'a, Bound<O::Head>>, ParentRoute, Semantic>>::Output:
        Replace<Bound<O::Head>, Bound<O::Head>>,
    for<'a> Error: From<
        <<Context as Query<Change<'a, Bound<O::Head>>, ParentRoute, Semantic>>::Output as Replace<
            Bound<O::Head>,
            Bound<O::Head>,
        >>::Error,
    >,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let bound = AsDeref::<Depth>::as_deref(head);
        if !self.state.mutated {
            match (&mut self.state.child, bound) {
                (Some(BoundChild::Included(child)), Bound::Included(_))
                | (Some(BoundChild::Excluded(child)), Bound::Excluded(_)) => {
                    return Collect::<
                        Context,
                        InnerRoute,
                        Error,
                        Scope<Semantic, Tail>,
                    >::collect(child, path, context);
                }
                (None, Bound::Unbounded) => return Ok(()),
                _ => {}
            }
        }

        emit::<_, _, Context, ParentRoute, Semantic, Error>(
            context,
            Change::Replace {
                path,
                before: None,
                after: bound,
            },
        )
    }
}

impl<O, Head: ?Sized, Depth> BoundObserver<O, Head, Depth>
where
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Bound<O::Head>>,
{
    /// Returns the active variant containing its child observer.
    pub fn as_mut(&mut self) -> Bound<&mut O> {
        let head = unsafe { Pointer::as_mut(&self.pointer) };
        match AsDerefMut::<Depth>::as_deref_mut(head) {
            Bound::Included(value) => {
                if !matches!(self.state.child, Some(BoundChild::Included(_))) {
                    self.state.child = Some(BoundChild::Included(Field::named(
                        unsafe { O::observe(value) },
                        "Included",
                    )));
                }
                let Some(BoundChild::Included(child)) = &mut self.state.child else {
                    unreachable!()
                };
                unsafe { O::relocate(child.observer_mut(), value) }
                Bound::Included(child.observer_mut())
            }
            Bound::Excluded(value) => {
                if !matches!(self.state.child, Some(BoundChild::Excluded(_))) {
                    self.state.child = Some(BoundChild::Excluded(Field::named(
                        unsafe { O::observe(value) },
                        "Excluded",
                    )));
                }
                let Some(BoundChild::Excluded(child)) = &mut self.state.child else {
                    unreachable!()
                };
                unsafe { O::relocate(child.observer_mut(), value) }
                Bound::Excluded(child.observer_mut())
            }
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

impl<T, Selection> Observe<Bound<T>, Composite<(Selection,)>> for Bound<T>
where
    T: Observe<T, Selection>,
{
    type Observer<Head, Depth>
        = BoundObserver<<T as Observe<T, Selection>>::Observer<T, Zero>, Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}
