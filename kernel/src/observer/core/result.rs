//! Observation of a `core::result::Result` child value.

use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};

use crate::{Change, Collect, Composite, Field, Observe, Path, Query, Replace, Scope, emit};

use super::{AsDeref, AsDerefMut, Observer, Pointer, QuasiObserver, Succ, Unsigned, Zero};

enum ResultChild<OkObserver, ErrObserver> {
    Ok(Field<OkObserver>),
    Err(Field<ErrObserver>),
}

struct ResultState<OkObserver, ErrObserver> {
    mutated: bool,
    child: Option<ResultChild<OkObserver, ErrObserver>>,
}

impl<OkObserver, ErrObserver> ResultState<OkObserver, ErrObserver> {
    fn invalidate(&mut self) {
        self.mutated = true;
        self.child = None;
    }
}

/// Observer for [`Result<Ok, Err>`].
pub struct ResultObserver<OkObserver, ErrObserver, Head: ?Sized, Depth = Zero> {
    pointer: Pointer<Head>,
    state: ResultState<OkObserver, ErrObserver>,

    marker: PhantomData<Depth>,
}

impl<OkObserver, ErrObserver, Head: ?Sized, Depth> Deref
    for ResultObserver<OkObserver, ErrObserver, Head, Depth>
{
    type Target = Pointer<Head>;

    fn deref(&self) -> &Self::Target {
        &self.pointer
    }
}

impl<OkObserver, ErrObserver, Head: ?Sized, Depth> DerefMut
    for ResultObserver<OkObserver, ErrObserver, Head, Depth>
where
    OkObserver: QuasiObserver<InnerDepth = Zero, Head: Sized>,
    ErrObserver: QuasiObserver<InnerDepth = Zero, Head: Sized>,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Result<OkObserver::Head, ErrObserver::Head>>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        QuasiObserver::invalidate(self);
        &mut self.pointer
    }
}

impl<OkObserver, ErrObserver, Head: ?Sized, Depth> QuasiObserver
    for ResultObserver<OkObserver, ErrObserver, Head, Depth>
where
    OkObserver: QuasiObserver<InnerDepth = Zero, Head: Sized>,
    ErrObserver: QuasiObserver<InnerDepth = Zero, Head: Sized>,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Result<OkObserver::Head, ErrObserver::Head>>,
{
    type Head = Head;
    type OuterDepth = Succ<Zero>;
    type InnerDepth = Depth;

    fn invalidate(this: &mut Self) {
        this.state.invalidate()
    }
}

unsafe impl<OkObserver, ErrObserver, Head: ?Sized, Depth> Observer
    for ResultObserver<OkObserver, ErrObserver, Head, Depth>
where
    OkObserver: Observer<InnerDepth = Zero, Head: Sized>,
    ErrObserver: Observer<InnerDepth = Zero, Head: Sized>,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Result<OkObserver::Head, ErrObserver::Head>>,
{
    unsafe fn observe(head: *mut Head) -> Self {
        unsafe {
            let result = AsDeref::<Depth>::as_deref_ptr(head);
            let child = Some(match &mut *result {
                Ok(value) => ResultChild::Ok(Field::named(OkObserver::observe(value), "Ok")),
                Err(value) => ResultChild::Err(Field::named(ErrObserver::observe(value), "Err")),
            });
            Self {
                pointer: Pointer::new_unchecked(head),
                state: ResultState {
                    mutated: false,
                    child,
                },

                marker: PhantomData,
            }
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Head) {
        unsafe {
            let result = AsDeref::<Depth>::as_deref_ptr(head);
            match (&mut this.state.child, &mut *result) {
                (Some(ResultChild::Ok(child)), Ok(value)) => {
                    OkObserver::relocate(child.observer_mut(), value)
                }
                (Some(ResultChild::Err(child)), Err(value)) => {
                    ErrObserver::relocate(child.observer_mut(), value)
                }
                (None, _) => {}
                _ => panic!("inconsistent Result observer state"),
            }
            Pointer::set_unchecked(&this.pointer, head);
        }
    }

    unsafe fn rebase(this: &mut Self, head: *mut Head) {
        unsafe {
            let result = AsDeref::<Depth>::as_deref_ptr(head);
            match (&mut this.state.child, &mut *result) {
                (Some(ResultChild::Ok(child)), Ok(value)) => {
                    OkObserver::rebase(child.observer_mut(), value)
                }
                (Some(ResultChild::Err(child)), Err(value)) => {
                    ErrObserver::rebase(child.observer_mut(), value)
                }
                (slot, Ok(value)) => {
                    *slot = Some(ResultChild::Ok(Field::named(
                        OkObserver::observe(value),
                        "Ok",
                    )))
                }
                (slot, Err(value)) => {
                    *slot = Some(ResultChild::Err(Field::named(
                        ErrObserver::observe(value),
                        "Err",
                    )))
                }
            }
            this.state.mutated = false;
            Pointer::set_unchecked(&this.pointer, head);
        }
    }
}

impl<
    OkObserver,
    ErrObserver,
    Head: ?Sized,
    Depth,
    Context: ?Sized,
    ParentRoute,
    OkRoute,
    ErrRoute,
    Error,
    Semantic,
    Tail,
> Collect<Context, (ParentRoute, OkRoute, ErrRoute), Error, Scope<Semantic, Tail>>
    for ResultObserver<OkObserver, ErrObserver, Head, Depth>
where
    OkObserver: Observer<InnerDepth = Zero, Head: Sized>,
    ErrObserver: Observer<InnerDepth = Zero, Head: Sized>,
    Field<OkObserver>: Collect<Context, OkRoute, Error, Scope<Semantic, Tail>>,
    Field<ErrObserver>: Collect<Context, ErrRoute, Error, Scope<Semantic, Tail>>,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Result<OkObserver::Head, ErrObserver::Head>>,
    for<'a> Context:
        Query<Change<'a, Result<OkObserver::Head, ErrObserver::Head>>, ParentRoute, Semantic>,
    for<'a> <Context as Query<
        Change<'a, Result<OkObserver::Head, ErrObserver::Head>>,
        ParentRoute,
        Semantic,
    >>::Output: Replace<
            Result<OkObserver::Head, ErrObserver::Head>,
            Result<OkObserver::Head, ErrObserver::Head>,
        >,
    for<'a> Error: From<
        <<Context as Query<
            Change<'a, Result<OkObserver::Head, ErrObserver::Head>>,
            ParentRoute,
            Semantic,
        >>::Output as Replace<
            Result<OkObserver::Head, ErrObserver::Head>,
            Result<OkObserver::Head, ErrObserver::Head>,
        >>::Error,
    >,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let result = AsDeref::<Depth>::as_deref(head);
        if !self.state.mutated {
            match (&mut self.state.child, result) {
                (Some(ResultChild::Ok(child)), Ok(_)) => {
                    return Collect::<Context, OkRoute, Error, Scope<Semantic, Tail>>::collect(
                        child, path, context,
                    );
                }
                (Some(ResultChild::Err(child)), Err(_)) => {
                    return Collect::<Context, ErrRoute, Error, Scope<Semantic, Tail>>::collect(
                        child, path, context,
                    );
                }
                _ => {}
            }
        }

        emit::<_, _, Context, ParentRoute, Semantic, Error>(
            context,
            Change::Replace {
                path,
                before: None,
                after: result,
            },
        )
    }
}

impl<OkObserver, ErrObserver, Head: ?Sized, Depth>
    ResultObserver<OkObserver, ErrObserver, Head, Depth>
where
    OkObserver: Observer<InnerDepth = Zero, Head: Sized>,
    ErrObserver: Observer<InnerDepth = Zero, Head: Sized>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Result<OkObserver::Head, ErrObserver::Head>>,
{
    /// Returns the observer for the currently active child.
    pub fn as_mut(&mut self) -> Result<&mut OkObserver, &mut ErrObserver> {
        let head = unsafe { Pointer::as_mut(&self.pointer) };
        match AsDerefMut::<Depth>::as_deref_mut(head) {
            Ok(value) => {
                if !matches!(self.state.child, Some(ResultChild::Ok(_))) {
                    self.state.child = Some(ResultChild::Ok(Field::named(
                        unsafe { OkObserver::observe(value) },
                        "Ok",
                    )));
                }
                let Some(ResultChild::Ok(child)) = &mut self.state.child else {
                    unreachable!()
                };
                unsafe { OkObserver::relocate(child.observer_mut(), value) }
                Ok(child.observer_mut())
            }
            Err(value) => {
                if !matches!(self.state.child, Some(ResultChild::Err(_))) {
                    self.state.child = Some(ResultChild::Err(Field::named(
                        unsafe { ErrObserver::observe(value) },
                        "Err",
                    )));
                }
                let Some(ResultChild::Err(child)) = &mut self.state.child else {
                    unreachable!()
                };
                unsafe { ErrObserver::relocate(child.observer_mut(), value) }
                Err(child.observer_mut())
            }
        }
    }
}

impl<Ok, Err, OkSelection, ErrSelection>
    Observe<Result<Ok, Err>, Composite<(OkSelection, ErrSelection)>> for Result<Ok, Err>
where
    Ok: Observe<Ok, OkSelection>,
    Err: Observe<Err, ErrSelection>,
{
    type Observer<Head, Depth>
        = ResultObserver<
        <Ok as Observe<Ok, OkSelection>>::Observer<Ok, Zero>,
        <Err as Observe<Err, ErrSelection>>::Observer<Err, Zero>,
        Head,
        Depth,
    >
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}
