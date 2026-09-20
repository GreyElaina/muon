//! Semantic scope transitions attached to selected observers.

use core::marker::PhantomData;

use crate::{Collect, Observer, Path, QuasiObserver, Query, Scope, Succ};

use super::Select;

/// Keeps a selected observer's records in its current semantic scope.
pub enum Current {}

/// Delivers a selected observer's records to the enclosing semantic scope.
pub enum Parent {}

/// Observer plus the semantic selection under which its records are collected.
#[repr(transparent)]
pub struct Selected<O, Semantic, Mode = Current> {
    observer: O,

    marker: PhantomData<fn() -> (Semantic, Mode)>,
}

impl<O, Semantic, Mode> core::ops::Deref for Selected<O, Semantic, Mode> {
    type Target = O;

    fn deref(&self) -> &O {
        &self.observer
    }
}

impl<O, Semantic, Mode> core::ops::DerefMut for Selected<O, Semantic, Mode> {
    fn deref_mut(&mut self) -> &mut O {
        &mut self.observer
    }
}

impl<O: QuasiObserver, Semantic, Mode> QuasiObserver for Selected<O, Semantic, Mode> {
    type OuterDepth = Succ<O::OuterDepth>;
    type InnerDepth = O::InnerDepth;

    fn invalidate(this: &mut Self) {
        O::invalidate(&mut this.observer)
    }
}

unsafe impl<O: Observer, Semantic, Mode> Observer for Selected<O, Semantic, Mode> {
    type Head = O::Head;

    unsafe fn observe(head: *mut Self::Head) -> Self {
        Self {
            observer: unsafe { O::observe(head) },
            marker: PhantomData,
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Self::Head) {
        unsafe { O::relocate(&mut this.observer, head) }
    }

    unsafe fn rebase(this: &mut Self, head: *mut Self::Head) {
        unsafe { O::rebase(&mut this.observer, head) }
    }
}

impl<Q: ?Sized, Context: ?Sized, Set, Active> Query<Q, crate::Here, Select<Set, Active>> for Context
where
    Context: Query<Q, crate::Here, Active>,
{
    type Output = <Context as Query<Q, crate::Here, Active>>::Output;

    fn query(&mut self) -> &mut Self::Output {
        <Context as Query<Q, crate::Here, Active>>::query(self)
    }
}

impl<O, Set, Active, Context: ?Sized, Routes, Error, Scopes> Collect<Context, Routes, Error, Scopes>
    for Selected<O, Select<Set, Active>, Current>
where
    O: Collect<Context, Routes, Error, Scope<Select<Set, Active>, Scopes>>,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        Collect::<Context, Routes, Error, Scope<Select<Set, Active>, Scopes>>::collect(
            &mut self.observer,
            path,
            context,
        )
    }
}

impl<O, Set, Active, Context: ?Sized, Routes, Error, Scopes> Collect<Context, Routes, Error, Scopes>
    for Selected<O, Select<Set, Active>, Parent>
where
    O: Collect<Context, Routes, Error, Scopes>,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        Collect::<Context, Routes, Error, Scopes>::collect(&mut self.observer, path, context)
    }
}

impl<O, Context: ?Sized, Routes, Error, Scopes> Collect<Context, Routes, Error, Scopes>
    for Selected<O, (), Current>
where
    O: Collect<Context, Routes, Error, Scopes>,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        Collect::<Context, Routes, Error, Scopes>::collect(&mut self.observer, path, context)
    }
}

impl<O, Context: ?Sized, Routes, Error, Head, Tail>
    Collect<Context, Routes, Error, Scope<Head, Tail>> for Selected<O, (), Parent>
where
    O: Collect<Context, Routes, Error, Tail>,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        Collect::<Context, Routes, Error, Tail>::collect(&mut self.observer, path, context)
    }
}
