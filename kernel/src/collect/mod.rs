//! Recursive delivery of observer-produced changes.

mod change;
mod field;
mod query;

use core::marker::PhantomData;

use crate::{Observe, Observer, Path, Zero};

pub use change::{Change, Replace, emit};
pub use field::{Field, Fields};
pub use query::{Here, Query, Through};

/// A type-level stack of semantic boundaries surrounding an observer.
pub struct Scope<Head, Tail>(PhantomData<fn() -> (Head, Tail)>);

/// Recursive delivery protocol. Routes and scopes remain explicit type-level evidence.
pub trait Collect<Context: ?Sized, Routes, Error, Scopes = Scope<(), ()>> {
    /// Delivers the facts recorded at `path` and below into `context`.
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error>;
}

/// Observes a synchronous body, delivers its changes, and returns its output.
pub fn collect<Model, Selection, Body, Output, Context, Routes, Error>(
    model: &mut Model,
    body: Body,
    context: &mut Context,
) -> Result<Output, Error>
where
    Model: Observe<Model, Selection> + ?Sized,
    <Model as Observe<Model, Selection>>::Observer<Model, Zero>:
        Collect<Context, Routes, Error, Scope<(), ()>>,
    Body: FnOnce(&mut <Model as Observe<Model, Selection>>::Observer<Model, Zero>) -> Output,
{
    let mut observer = unsafe {
        <<Model as Observe<Model, Selection>>::Observer<Model, Zero> as Observer>::observe(model)
    };
    let output = body(&mut observer);
    Collect::<Context, Routes, Error, Scope<(), ()>>::collect(
        &mut observer,
        &Path::root(),
        context,
    )?;
    Ok(output)
}

/// Observes an asynchronous body, delivers its changes, and returns its output.
pub async fn collect_async<Model, Selection, Body, Output, Context, Routes, Error>(
    model: &mut Model,
    body: Body,
    context: &mut Context,
) -> Result<Output, Error>
where
    Model: Observe<Model, Selection> + ?Sized,
    <Model as Observe<Model, Selection>>::Observer<Model, Zero>:
        Collect<Context, Routes, Error, Scope<(), ()>>,
    Body: AsyncFnOnce(&mut <Model as Observe<Model, Selection>>::Observer<Model, Zero>) -> Output,
{
    let mut observer = unsafe {
        <<Model as Observe<Model, Selection>>::Observer<Model, Zero> as Observer>::observe(model)
    };
    let output = body(&mut observer).await;
    Collect::<Context, Routes, Error, Scope<(), ()>>::collect(
        &mut observer,
        &Path::root(),
        context,
    )?;
    Ok(output)
}

impl<Context: ?Sized, Error, Scopes> Collect<Context, (), Error, Scopes> for () {
    fn collect(&mut self, _: &Path<'_>, _: &mut Context) -> Result<(), Error> {
        Ok(())
    }
}

impl<O, Context: ?Sized, Routes, Error, Scopes> Collect<Context, Routes, Error, Scopes> for &mut O
where
    O: Collect<Context, Routes, Error, Scopes>,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        Collect::<Context, Routes, Error, Scopes>::collect(*self, path, context)
    }
}
