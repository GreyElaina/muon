//! Persistent observer state and temporary model bindings.

use core::fmt::{self, Display, Formatter};
use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};

use crate::{Collect, Observe, Path, Scope, Zero};

use super::{Observer, QuasiObserver};

/// Indicates that an observer cannot be rebound after an incomplete delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Poisoned;

impl Display for Poisoned {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("observer delivery did not complete")
    }
}

/// Failure to use or deliver a persistent observer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObserverError<Error> {
    /// A previous delivery did not complete; call [`ObserverCell::reset`] before reuse.
    Poisoned,
    /// The destination rejected or failed while applying an observed change.
    Collect(Error),
}

impl<Error: Display> Display for ObserverError<Error> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Poisoned => Poisoned.fmt(formatter),
            Self::Collect(error) => error.fmt(formatter),
        }
    }
}

#[cfg(feature = "std")]
impl<Error> std::error::Error for ObserverError<Error> where Error: std::error::Error + 'static {}

#[cfg(feature = "std")]
impl std::error::Error for Poisoned {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Ready,
    Poisoned,
}

/// Long-lived observation state which can be rebound to a model for short access sessions.
pub struct ObserverCell<O> {
    observer: O,
    phase: Phase,
}

/// A model owned together with reusable observation state.
pub struct Observed<Model, O> {
    model: Model,
    observer: ObserverCell<O>,
}

impl<Model, O> Observed<Model, O>
where
    O: Observer<Head = Model>,
{
    /// Conservatively invalidates observation state and returns arbitrary model access.
    ///
    /// Use an observer session for precise reads and mutations. This escape hatch exists for code
    /// that cannot operate on the observer API; collection will conservatively cover any mutation
    /// performed through the returned reference, including interior mutation through `&self`.
    pub fn escape(&mut self) -> &mut Model {
        unsafe { self.observer.escape(&mut self.model) }
    }

    /// Runs a tracked editing session.
    pub fn edit<Output>(
        &mut self,
        body: impl FnOnce(&mut O) -> Output,
    ) -> Result<Output, Poisoned> {
        unsafe { self.observer.with(&mut self.model, body) }
    }

    /// Delivers pending facts and establishes a fresh baseline.
    pub fn collect<Context: ?Sized, Routes, Error>(
        &mut self,
        context: &mut Context,
    ) -> Result<(), ObserverError<Error>>
    where
        O: Collect<Context, Routes, Error, Scope<(), ()>>,
    {
        unsafe { self.observer.collect(&mut self.model, context) }
    }

    /// Runs a tracked editing session and immediately delivers its facts.
    pub fn collect_with<Output, Context: ?Sized, Routes, Error>(
        &mut self,
        body: impl FnOnce(&mut O) -> Output,
        context: &mut Context,
    ) -> Result<Output, ObserverError<Error>>
    where
        O: Collect<Context, Routes, Error, Scope<(), ()>>,
    {
        unsafe { self.observer.collect_with(&mut self.model, body, context) }
    }

    /// Discards pending facts and starts a new baseline from the current model.
    pub fn reset(&mut self) {
        self.observer.reset(&mut self.model);
    }

    /// Consumes the facade and returns the model.
    pub fn into_inner(self) -> Model {
        self.model
    }
}

impl<O: Observer> ObserverCell<O> {
    /// Conservatively invalidates observation state and returns arbitrary model access.
    ///
    /// # Safety
    ///
    /// `model` must be the same logical value previously observed by this cell.
    pub unsafe fn escape<'model>(&mut self, model: &'model mut O::Head) -> &'model mut O::Head {
        if self.phase == Phase::Poisoned {
            return model;
        }
        self.phase = Phase::Poisoned;
        unsafe { O::relocate(&mut self.observer, model) };
        QuasiObserver::invalidate(&mut self.observer);
        self.phase = Phase::Ready;
        model
    }

    /// Temporarily binds this observer to the current address of the same logical value.
    ///
    /// # Safety
    ///
    /// `model` must be the same logical value previously observed by this cell. It may have moved,
    /// but pending facts and retained child topology must still describe it.
    pub unsafe fn bind<'cell, 'model>(
        &'cell mut self,
        model: &'model mut O::Head,
    ) -> Result<ObserverGuard<'cell, 'model, O>, Poisoned> {
        if self.phase == Phase::Poisoned {
            return Err(Poisoned);
        }
        self.phase = Phase::Poisoned;
        unsafe { O::relocate(&mut self.observer, model) };
        self.phase = Phase::Ready;
        Ok(ObserverGuard {
            observer: &mut self.observer,
            marker: PhantomData,
        })
    }

    /// Runs `body` with a temporary observer binding.
    ///
    /// # Safety
    ///
    /// `model` must be the same logical value previously observed by this cell.
    pub unsafe fn with<Output>(
        &mut self,
        model: &mut O::Head,
        body: impl FnOnce(&mut O) -> Output,
    ) -> Result<Output, Poisoned> {
        let mut guard = unsafe { self.bind(model)? };
        Ok(body(&mut guard))
    }

    /// Delivers all pending facts and establishes a fresh baseline after successful delivery.
    ///
    /// # Safety
    ///
    /// `model` must be the same logical value previously observed by this cell.
    pub unsafe fn collect<Context: ?Sized, Routes, Error>(
        &mut self,
        model: &mut O::Head,
        context: &mut Context,
    ) -> Result<(), ObserverError<Error>>
    where
        O: Collect<Context, Routes, Error, Scope<(), ()>>,
    {
        if self.phase == Phase::Poisoned {
            return Err(ObserverError::Poisoned);
        }

        self.phase = Phase::Poisoned;
        unsafe { O::relocate(&mut self.observer, model) };
        Collect::<Context, Routes, Error, Scope<(), ()>>::collect(
            &mut self.observer,
            &Path::root(),
            context,
        )
        .map_err(ObserverError::Collect)?;
        unsafe { O::rebase(&mut self.observer, model) };
        self.phase = Phase::Ready;
        Ok(())
    }

    fn deliver_bound<Context: ?Sized, Routes, Error>(
        &mut self,
        context: &mut Context,
    ) -> Result<(), ObserverError<Error>>
    where
        O: Collect<Context, Routes, Error, Scope<(), ()>>,
    {
        self.phase = Phase::Poisoned;
        Collect::<Context, Routes, Error, Scope<(), ()>>::collect(
            &mut self.observer,
            &Path::root(),
            context,
        )
        .map_err(ObserverError::Collect)?;
        Ok(())
    }

    /// Runs `body`, delivers the resulting facts, and returns its output.
    ///
    /// # Safety
    ///
    /// `model` must be the same logical value previously observed by this cell.
    pub unsafe fn collect_with<Output, Context: ?Sized, Routes, Error>(
        &mut self,
        model: &mut O::Head,
        body: impl FnOnce(&mut O) -> Output,
        context: &mut Context,
    ) -> Result<Output, ObserverError<Error>>
    where
        O: Collect<Context, Routes, Error, Scope<(), ()>>,
    {
        if self.phase == Phase::Poisoned {
            return Err(ObserverError::Poisoned);
        }

        self.phase = Phase::Poisoned;
        unsafe { O::relocate(&mut self.observer, model) };
        self.phase = Phase::Ready;
        let output = body(&mut self.observer);
        self.deliver_bound(context)?;
        unsafe { O::rebase(&mut self.observer, model) };
        self.phase = Phase::Ready;
        Ok(output)
    }

    /// Discards pending facts and starts a new observation baseline at `model`.
    pub fn reset(&mut self, model: &mut O::Head) {
        self.phase = Phase::Poisoned;
        unsafe { O::rebase(&mut self.observer, model) };
        self.phase = Phase::Ready;
    }
}

/// A temporary capability to access an observer rebound to a live model borrow.
pub struct ObserverGuard<'cell, 'model, O: Observer> {
    observer: &'cell mut O,

    marker: PhantomData<&'model mut O::Head>,
}

impl<O: Observer> Deref for ObserverGuard<'_, '_, O> {
    type Target = O;

    fn deref(&self) -> &O {
        self.observer
    }
}

impl<O: Observer> DerefMut for ObserverGuard<'_, '_, O> {
    fn deref_mut(&mut self) -> &mut O {
        self.observer
    }
}

/// Creates reusable observation state selected through an inferred route.
pub fn observer_cell<T, Route>(
    value: &mut T,
) -> ObserverCell<<T as Observe<T, Route>>::Observer<T, Zero>>
where
    T: Observe<T, Route> + ?Sized,
{
    ObserverCell {
        observer: unsafe { Observer::observe(value) },
        phase: Phase::Ready,
    }
}

/// Wraps a model with reusable observation state selected through an inferred route.
pub fn observed<T, Route>(mut value: T) -> Observed<T, <T as Observe<T, Route>>::Observer<T, Zero>>
where
    T: Observe<T, Route>,
{
    let observer = observer_cell(&mut value);
    Observed {
        model: value,
        observer,
    }
}
