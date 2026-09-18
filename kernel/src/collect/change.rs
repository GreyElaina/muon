//! Borrowed change vocabulary and handler capabilities.

use crate::Path;

use super::Query;

/// A borrowed fact emitted by an observer during collection.
pub enum Change<'a, T: ?Sized, Before: ?Sized = T> {
    /// The value at `path` has been replaced by its final observed value.
    Replace {
        /// Location of the replaced value.
        path: &'a Path<'a>,
        /// Initial value when the observer retained one.
        before: Option<&'a Before>,
        /// Final value at collection time.
        after: &'a T,
    },
}

/// Capability to interpret a whole-value replacement.
pub trait Replace<T: ?Sized, Before: ?Sized> {
    /// Error returned while applying a replacement.
    type Error;

    /// Applies a whole-value replacement at `path`.
    fn replace(
        &mut self,
        path: &Path<'_>,
        before: Option<&Before>,
        after: &T,
    ) -> Result<(), Self::Error>;
}

/// Delivers one change through context selection and converts the handler-local error.
pub fn emit<'a, T: ?Sized, Before: ?Sized, Context: ?Sized, Route, Semantic, Error>(
    context: &mut Context,
    change: Change<'a, T, Before>,
) -> Result<(), Error>
where
    Context: Query<Change<'a, T, Before>, Route, Semantic>,
    <Context as Query<Change<'a, T, Before>, Route, Semantic>>::Output: Replace<T, Before>,
    Error: From<
        <<Context as Query<Change<'a, T, Before>, Route, Semantic>>::Output as Replace<
            T,
            Before,
        >>::Error,
    >,
{
    match change {
        Change::Replace {
            path,
            before,
            after,
        } => Context::query(context)
            .replace(path, before, after)
            .map_err(Error::from),
    }
}
