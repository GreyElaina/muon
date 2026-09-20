//! Minimal whole-value and no-op observer states.

use crate::{Change, Path, Query, Replace, emit};

use super::{CollectState, Invalidate, State, StatefulObserver, Zero};

/// Dirty-bit state: conservative mutable access becomes one whole-value replacement.
pub struct Dirty {
    dirty: bool,
}

impl<T: ?Sized> Invalidate<T> for Dirty {
    fn invalidate(&mut self, _: &T) {
        self.dirty = true;
    }
}

impl Dirty {
    /// Marks the value as conservatively changed.
    pub fn mark(&mut self) {
        self.dirty = true;
    }
}

impl<T: ?Sized> State<T> for Dirty {
    fn observe(_: &T) -> Self {
        Self { dirty: false }
    }
}

impl<T: ?Sized, Context: ?Sized, Route, Error, Semantic>
    CollectState<T, Context, Route, Error, Semantic> for Dirty
where
    for<'a> Context: Query<Change<'a, T>, Route, Semantic>,
    for<'a> <Context as Query<Change<'a, T>, Route, Semantic>>::Output: Replace<T, T>,
    for<'a> Error:
        From<<<Context as Query<Change<'a, T>, Route, Semantic>>::Output as Replace<T, T>>::Error>,
{
    fn collect(&mut self, value: &T, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        if self.dirty {
            emit::<T, T, Context, Route, Semantic, Error>(
                context,
                Change::Replace {
                    path,
                    before: None,
                    after: value,
                },
            )?;
        }
        Ok(())
    }
}

/// State that deliberately records and delivers nothing.
pub struct Noop;

impl<T: ?Sized> Invalidate<T> for Noop {
    fn invalidate(&mut self, _: &T) {}
}

impl<T: ?Sized> State<T> for Noop {
    fn observe(_: &T) -> Self {
        Self
    }
}

impl<T: ?Sized, Context: ?Sized, Error, Semantic> CollectState<T, Context, (), Error, Semantic>
    for Noop
{
    fn collect(&mut self, _: &T, _: &Path<'_>, _: &mut Context) -> Result<(), Error> {
        Ok(())
    }
}

/// Whole-value observer backed by [`Dirty`].
pub type ShallowObserver<Head, Depth = Zero> = StatefulObserver<Dirty, Head, Depth>;

/// Observer that deliberately ignores every mutation.
pub type NoopObserver<Head, Depth = Zero> = StatefulObserver<Noop, Head, Depth>;
