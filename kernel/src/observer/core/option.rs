//! Observation of an optional child value from `core`.

use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};

use crate::{Change, Collect, Composite, Observe, Path, Query, Replace, Scope, emit};

use super::{AsDeref, AsDerefMut, Observer, Pointer, QuasiObserver, Succ, Unsigned, Zero};

struct OptionState<O> {
    mutated: bool,
    inner: Option<O>,
}

impl<O> OptionState<O> {
    fn invalidate(&mut self) {
        self.mutated = true;
        self.inner = None;
    }
}

/// Observer for [`Option<T>`].
pub struct OptionObserver<O, Head: ?Sized, Depth = Zero> {
    pointer: Pointer<Head>,
    state: OptionState<O>,

    marker: PhantomData<Depth>,
}

impl<O, Head: ?Sized, Depth> Deref for OptionObserver<O, Head, Depth> {
    type Target = Pointer<Head>;

    fn deref(&self) -> &Self::Target {
        &self.pointer
    }
}

impl<O, Head: ?Sized, Depth> DerefMut for OptionObserver<O, Head, Depth>
where
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Option<O::Head>>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        QuasiObserver::invalidate(self);
        &mut self.pointer
    }
}

impl<O, Head: ?Sized, Depth> QuasiObserver for OptionObserver<O, Head, Depth>
where
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Option<O::Head>>,
{
    type OuterDepth = Succ<Zero>;
    type InnerDepth = Depth;

    fn invalidate(this: &mut Self) {
        this.state.invalidate()
    }
}

unsafe impl<O, Head: ?Sized, Depth> Observer for OptionObserver<O, Head, Depth>
where
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Option<O::Head>>,
{
    type Head = Head;

    unsafe fn observe(head: *mut Head) -> Self {
        unsafe {
            let option = AsDeref::<Depth>::as_deref_ptr(head);
            let inner = match &mut *option {
                Some(value) => Some(O::observe(value)),
                None => None,
            };
            Self {
                pointer: Pointer::new_unchecked(head),
                state: OptionState {
                    mutated: false,
                    inner,
                },

                marker: PhantomData,
            }
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Head) {
        unsafe {
            let option = AsDeref::<Depth>::as_deref_ptr(head);
            match (&mut this.state.inner, &mut *option) {
                (Some(inner), Some(value)) => O::relocate(inner, value),
                (None, _) => {}
                (Some(_), None) => panic!("inconsistent Option observer state"),
            }
            Pointer::set_unchecked(&this.pointer, head);
        }
    }

    unsafe fn rebase(this: &mut Self, head: *mut Head) {
        unsafe {
            let option = AsDeref::<Depth>::as_deref_ptr(head);
            match (&mut this.state.inner, &mut *option) {
                (Some(inner), Some(value)) => O::rebase(inner, value),
                (slot @ None, Some(value)) => *slot = Some(O::observe(value)),
                (slot @ Some(_), None) => *slot = None,
                (None, None) => {}
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
    for OptionObserver<O, Head, Depth>
where
    O: Observer<InnerDepth = Zero>
        + Collect<Context, InnerRoute, Error, Scope<Semantic, Tail>>,
    O::Head: Sized,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Option<O::Head>>,
    for<'a> Context: Query<Change<'a, Option<O::Head>>, ParentRoute, Semantic>,
    for<'a> <Context as Query<Change<'a, Option<O::Head>>, ParentRoute, Semantic>>::Output:
        Replace<Option<O::Head>, Option<O::Head>>,
    for<'a> Error: From<
        <<Context as Query<Change<'a, Option<O::Head>>, ParentRoute, Semantic>>::Output as Replace<
            Option<O::Head>,
            Option<O::Head>,
        >>::Error,
    >,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let option = AsDeref::<Depth>::as_deref(head);
        if self.state.mutated {
            emit::<_, _, Context, ParentRoute, Semantic, Error>(
                context,
                Change::Replace {
                    path,
                    before: None,
                    after: option,
                },
            )?;
            return Ok(());
        }

        match (&mut self.state.inner, option) {
            (Some(inner), Some(_)) => {
                Collect::<Context, InnerRoute, Error, Scope<Semantic, Tail>>::collect(
                    inner, path, context,
                )
            }
            _ => Ok(()),
        }
    }
}

impl<O, Head: ?Sized, Depth> OptionObserver<O, Head, Depth>
where
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Option<O::Head>>,
{
    /// Returns the child observer when the option is currently `Some`.
    pub fn as_mut(&mut self) -> Option<&mut O> {
        let head = unsafe { Pointer::as_mut(&self.pointer) };
        let value = AsDerefMut::<Depth>::as_deref_mut(head).as_mut()?;
        let inner = match &mut self.state.inner {
            Some(inner) => inner,
            slot @ None => slot.insert(unsafe { O::observe(value) }),
        };
        unsafe { O::relocate(inner, value) }
        Some(inner)
    }

    /// Inserts a value and returns its observer.
    pub fn insert(&mut self, value: O::Head) -> &mut O {
        *QuasiObserver::tracked_mut::<Option<O::Head>>(self) = Some(value);
        self.as_mut().unwrap()
    }

    /// Inserts `value` only when currently `None`.
    pub fn get_or_insert(&mut self, value: O::Head) -> &mut O {
        self.get_or_insert_with(|| value)
    }

    /// Inserts the default value only when currently `None`.
    pub fn get_or_insert_default(&mut self) -> &mut O
    where
        O::Head: Default,
    {
        self.get_or_insert_with(Default::default)
    }

    /// Lazily inserts a value only when currently `None`.
    pub fn get_or_insert_with(&mut self, f: impl FnOnce() -> O::Head) -> &mut O {
        if QuasiObserver::untracked_ref::<Option<O::Head>>(self).is_none() {
            *QuasiObserver::tracked_mut::<Option<O::Head>>(self) = Some(f());
        }
        self.as_mut().unwrap()
    }

    /// Takes the value out of the option, if any.
    pub fn take(&mut self) -> Option<O::Head> {
        if QuasiObserver::untracked_ref::<Option<O::Head>>(self).is_none() {
            return None;
        }
        QuasiObserver::tracked_mut::<Option<O::Head>>(self).take()
    }

    /// Takes the value out when `predicate` accepts its child observer.
    pub fn take_if(&mut self, predicate: impl FnOnce(&mut O) -> bool) -> Option<O::Head> {
        if !predicate(self.as_mut()?) {
            return None;
        }
        QuasiObserver::tracked_mut::<Option<O::Head>>(self).take()
    }

    /// Replaces the contained value and returns the previous one, if any.
    pub fn replace(&mut self, value: O::Head) -> Option<O::Head> {
        QuasiObserver::tracked_mut::<Option<O::Head>>(self).replace(value)
    }
}

impl<T, Selection> Observe<Option<T>, Composite<(Selection,)>> for Option<T>
where
    T: Observe<T, Selection>,
{
    type Observer<Head, Depth>
        = OptionObserver<<T as Observe<T, Selection>>::Observer<T, Zero>, Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}
