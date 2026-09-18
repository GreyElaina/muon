//! Structural collection of statically known model fields.

use core::ops::{Deref, DerefMut};

use crate::{Collect, Path, PathStep, QuasiObserver, Succ};

/// An observer paired with its statically known path step.
///
/// This wrapper owns only collection metadata. Constructing and relocating the observer still
/// belongs to the model-specific projection generated above the kernel.
pub struct Field<O> {
    observer: O,
    step: PathStep<'static>,
}

impl<O> Field<O> {
    /// Attaches a named-struct field to an observer.
    pub const fn named(observer: O, name: &'static str) -> Self {
        Self {
            observer,
            step: PathStep::Field(name),
        }
    }

    /// Attaches a tuple-field position to an observer.
    pub const fn indexed(observer: O, index: usize) -> Self {
        Self {
            observer,
            step: PathStep::Positive(index),
        }
    }

    /// Returns the wrapped observer.
    pub const fn observer(&self) -> &O {
        &self.observer
    }

    /// Returns the wrapped observer mutably.
    pub fn observer_mut(&mut self) -> &mut O {
        &mut self.observer
    }

    /// Removes the collection metadata and returns the observer.
    pub fn into_observer(self) -> O {
        self.observer
    }
}

impl<O> Deref for Field<O> {
    type Target = O;

    fn deref(&self) -> &O {
        &self.observer
    }
}

impl<O> DerefMut for Field<O> {
    fn deref_mut(&mut self) -> &mut O {
        &mut self.observer
    }
}

impl<O: QuasiObserver> QuasiObserver for Field<O> {
    type Head = O::Head;
    type OuterDepth = Succ<O::OuterDepth>;
    type InnerDepth = O::InnerDepth;

    fn invalidate(this: &mut Self) {
        O::invalidate(&mut this.observer)
    }
}

impl<O, Context: ?Sized, Route, Error, Scopes> Collect<Context, Route, Error, Scopes> for Field<O>
where
    O: Collect<Context, Route, Error, Scopes>,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        let child = path.child(self.step.clone());
        Collect::<Context, Route, Error, Scopes>::collect(&mut self.observer, &child, context)
    }
}

/// A heterogeneous product of structural fields.
pub struct Fields<T>(pub T);

impl<T> Fields<T> {
    /// Creates a structural field product.
    pub const fn new(fields: T) -> Self {
        Self(fields)
    }

    /// Returns the underlying product.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<Context: ?Sized, Error, Scopes> Collect<Context, (), Error, Scopes> for Fields<()> {
    fn collect(&mut self, _: &Path<'_>, _: &mut Context) -> Result<(), Error> {
        Ok(())
    }
}

impl<O, Context: ?Sized, Route, Error, Scopes, const N: usize>
    Collect<Context, Route, Error, Scopes> for Fields<[O; N]>
where
    O: Collect<Context, Route, Error, Scopes>,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        for observer in &mut self.0 {
            Collect::<Context, Route, Error, Scopes>::collect(observer, path, context)?;
        }
        Ok(())
    }
}

macro_rules! tuple_fields {
    ($(($index:tt, $observer:ident, $route:ident)),+ $(,)?) => {
        tuple_fields!(@tuples [] ; $(($index, $observer, $route)),+);
    };
    (@tuples [$($prefix:tt)*] ; ($index:tt, $observer:ident, $route:ident) $(, $rest:tt)*) => {
        tuple_fields!(@tuple [$($prefix)* ($index, $observer, $route)]);
        tuple_fields!(@tuples [$($prefix)* ($index, $observer, $route)] ; $($rest),*);
    };
    (@tuples [$($prefix:tt)*] ;) => {};
    (@tuple [$(($index:tt, $observer:ident, $route:ident))+]) => {
        impl<Context: ?Sized, Error, Scopes, $($observer, $route),+>
            Collect<Context, ($($route,)+), Error, Scopes> for Fields<($($observer,)+)>
        where
            $($observer: Collect<Context, $route, Error, Scopes>,)+
        {
            fn collect(
                &mut self,
                path: &Path<'_>,
                context: &mut Context,
            ) -> Result<(), Error> {
                $(Collect::<Context, $route, Error, Scopes>::collect(
                    &mut self.0.$index,
                    path,
                    context,
                )?;)+
                Ok(())
            }
        }
    };
}

tuple_fields!(
    (0, O0, R0),
    (1, O1, R1),
    (2, O2, R2),
    (3, O3, R3),
    (4, O4, R4),
    (5, O5, R5),
    (6, O6, R6),
    (7, O7, R7),
    (8, O8, R8),
    (9, O9, R9),
    (10, O10, R10),
    (11, O11, R11),
);
