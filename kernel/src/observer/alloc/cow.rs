//! Conditional child observation for copy-on-write values from `alloc`.

use alloc::borrow::{Cow, ToOwned};
use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};

use crate::{Change, Collect, Composite, Observe, Path, Query, Replace, Scope, emit};

use super::{AsDeref, AsDerefMut, Observer, Pointer, QuasiObserver, Succ, Unsigned, Zero};

/// Observer for [`Cow<'_, B>`].
///
/// Borrowed values have no mutable child. Calling [`CowObserver::to_mut`] creates the owned value
/// and installs its observer; arbitrary mutable access conservatively falls back to a replacement
/// of the whole `Cow`.
pub struct CowObserver<B: ToOwned + ?Sized, O, Head: ?Sized, Depth = Zero> {
    pointer: Pointer<Head>,
    owned: Option<O>,
    mutated: bool,

    marker: PhantomData<(fn(&B), Depth)>,
}

impl<B: ToOwned + ?Sized, O, Head: ?Sized, Depth> Deref for CowObserver<B, O, Head, Depth> {
    type Target = Pointer<Head>;

    fn deref(&self) -> &Self::Target {
        &self.pointer
    }
}

impl<'value, B: ToOwned + ?Sized, O, Head: ?Sized, Depth> DerefMut
    for CowObserver<B, O, Head, Depth>
where
    B: 'value,
    O: QuasiObserver<Head = B::Owned, InnerDepth = Zero>,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Cow<'value, B>>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        QuasiObserver::invalidate(self);
        &mut self.pointer
    }
}

impl<'value, B, O, Head: ?Sized, Depth> QuasiObserver for CowObserver<B, O, Head, Depth>
where
    B: ToOwned + ?Sized + 'value,
    O: QuasiObserver<Head = B::Owned, InnerDepth = Zero>,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Cow<'value, B>>,
{
    type Head = Head;
    type OuterDepth = Succ<Zero>;
    type InnerDepth = Depth;

    fn invalidate(this: &mut Self) {
        this.mutated = true;
        this.owned = None;
    }
}

unsafe impl<'value, B, O, Head: ?Sized, Depth> Observer for CowObserver<B, O, Head, Depth>
where
    B: ToOwned + ?Sized + 'value,
    O: Observer<Head = B::Owned, InnerDepth = Zero>,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Cow<'value, B>>,
{
    unsafe fn observe(head: *mut Head) -> Self {
        unsafe {
            let cow = AsDeref::<Depth>::as_deref_ptr(head);
            let owned = match &mut *cow {
                Cow::Borrowed(_) => None,
                Cow::Owned(value) => Some(O::observe(value)),
            };
            Self {
                pointer: Pointer::new_unchecked(head),
                owned,
                mutated: false,

                marker: PhantomData,
            }
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Head) {
        unsafe {
            if let Some(observer) = &mut this.owned {
                let cow = AsDeref::<Depth>::as_deref_ptr(head);
                let Cow::Owned(value) = &mut *cow else {
                    panic!("inconsistent Cow observer state")
                };
                O::relocate(observer, value);
            }
            Pointer::set_unchecked(&this.pointer, head);
        }
    }

    unsafe fn rebase(this: &mut Self, head: *mut Head) {
        unsafe {
            let cow = AsDeref::<Depth>::as_deref_ptr(head);
            match (&mut this.owned, &mut *cow) {
                (Some(observer), Cow::Owned(value)) => O::rebase(observer, value),
                (slot @ None, Cow::Owned(value)) => *slot = Some(O::observe(value)),
                (slot @ Some(_), Cow::Borrowed(_)) => *slot = None,
                (None, Cow::Borrowed(_)) => {}
            }
            this.mutated = false;
            Pointer::set_unchecked(&this.pointer, head);
        }
    }
}

impl<
    'value,
    B,
    O,
    Head: ?Sized,
    Depth,
    Context: ?Sized,
    ParentRoute,
    OwnedRoute,
    Error,
    Semantic,
    Tail,
> Collect<Context, (ParentRoute, OwnedRoute), Error, Scope<Semantic, Tail>>
    for CowObserver<B, O, Head, Depth>
where
    B: ToOwned + ?Sized + 'value,
    O: Observer<Head = B::Owned, InnerDepth = Zero>
        + Collect<Context, OwnedRoute, Error, Scope<Semantic, Tail>>,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Cow<'value, B>>,
    for<'a> Context: Query<Change<'a, Cow<'value, B>>, ParentRoute, Semantic>,
    for<'a> <Context as Query<Change<'a, Cow<'value, B>>, ParentRoute, Semantic>>::Output:
        Replace<Cow<'value, B>, Cow<'value, B>>,
    for<'a> Error: From<
        <<Context as Query<Change<'a, Cow<'value, B>>, ParentRoute, Semantic>>::Output as Replace<
            Cow<'value, B>,
            Cow<'value, B>,
        >>::Error,
    >,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        if self.mutated {
            let head = unsafe { Pointer::as_ref(&self.pointer) };
            let cow = AsDeref::<Depth>::as_deref(head);
            return emit::<_, _, Context, ParentRoute, Semantic, Error>(
                context,
                Change::Replace {
                    path,
                    before: None,
                    after: cow,
                },
            );
        }

        if let Some(observer) = &mut self.owned {
            Collect::<Context, OwnedRoute, Error, Scope<Semantic, Tail>>::collect(
                observer, path, context,
            )?;
        }
        Ok(())
    }
}

impl<'value, B, O, Head: ?Sized, Depth> CowObserver<B, O, Head, Depth>
where
    B: ToOwned + ?Sized + 'value,
    O: Observer<Head = B::Owned, InnerDepth = Zero>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Cow<'value, B>>,
{
    /// Clones a borrowed value if necessary and returns its owned child observer.
    pub fn to_mut(&mut self) -> &mut O {
        let head = unsafe { Pointer::as_mut(&self.pointer) };
        let owned = AsDerefMut::<Depth>::as_deref_mut(head).to_mut();
        let observer = self
            .owned
            .get_or_insert_with(|| unsafe { O::observe(owned) });
        unsafe { O::relocate(observer, owned) }
        observer
    }
}

impl<'value, B, Selection> Observe<Cow<'value, B>, Composite<(Selection,)>> for Cow<'value, B>
where
    B: ToOwned + ?Sized + 'value,
    B::Owned: Observe<B::Owned, Selection>,
{
    type Observer<Head, Depth>
        = CowObserver<
        B,
        <B::Owned as Observe<B::Owned, Selection>>::Observer<B::Owned, Zero>,
        Head,
        Depth,
    >
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}
