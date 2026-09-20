//! Compile-time observer selection and semantic ownership.

mod scope;

use core::marker::PhantomData;

use crate::{AsDerefMut, Observer, Unsigned};

pub use scope::{Current, Parent, Selected};

/// Position in an observer-provider set.
pub struct Slot<const N: usize>;

/// Built-in whole-value terminal in a generated observer route.
#[doc(hidden)]
pub enum Shallow {}

/// Built-in no-op terminal in a generated observer route.
#[doc(hidden)]
pub enum Noop {}

/// Marks the recursively derived observer shape of a composite model.
pub struct Composite<Route>(PhantomData<fn() -> Route>);

/// Compile-time set of observer providers.
pub struct Candidates<Providers>(PhantomData<fn() -> Providers>);

/// The provider set and the provider selected for one model boundary.
pub struct Select<Set, Active>(PhantomData<fn() -> (Set, Active)>);

/// Selects an observer implementation for a target through a route witness.
pub trait Observe<T: ?Sized = Self, Route = ()> {
    /// Observer selected for `T`, parameterized by its owner and dereference depth.
    type Observer<Head, Depth>: Observer<Head = Head, InnerDepth = Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = T> + ?Sized;
}

/// Selects observer semantics from a provider set.
pub trait SelectFrom<T: ?Sized, Route> {
    /// Semantic scope introduced by the selected provider.
    type Semantic;
    /// Observer supplied by the selected provider.
    type Observer<Head, Depth>: Observer<Head = Head, InnerDepth = Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = T> + ?Sized;
}

impl<T: ?Sized, Providers, Inner> SelectFrom<T, (Slot<0>, Inner)> for Candidates<Providers>
where
    T: Observe<T, Inner>,
{
    type Semantic = ();
    type Observer<Head, Depth>
        = <T as Observe<T, Inner>>::Observer<Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = T> + ?Sized;
}

impl<T: ?Sized, Providers> SelectFrom<T, (Shallow, ())> for Candidates<Providers> {
    type Semantic = ();
    type Observer<Head, Depth>
        = crate::ShallowObserver<Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = T> + ?Sized;
}

impl<T: ?Sized, Providers> SelectFrom<T, (Noop, ())> for Candidates<Providers> {
    type Semantic = ();
    type Observer<Head, Depth>
        = crate::NoopObserver<Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = T> + ?Sized;
}

impl<T: ?Sized, Providers, Route> Observe<T, (Current, Route)> for Candidates<Providers>
where
    Candidates<Providers>: SelectFrom<T, Route>,
{
    type Observer<Head, Depth>
        = Selected<
        <Self as SelectFrom<T, Route>>::Observer<Head, Depth>,
        <Self as SelectFrom<T, Route>>::Semantic,
        Current,
    >
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = T> + ?Sized;
}

impl<T: ?Sized, Providers, Route> Observe<T, (Parent, Route)> for Candidates<Providers>
where
    Candidates<Providers>: SelectFrom<T, Route>,
{
    type Observer<Head, Depth>
        = Selected<
        <Self as SelectFrom<T, Route>>::Observer<Head, Depth>,
        <Self as SelectFrom<T, Route>>::Semantic,
        Parent,
    >
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = T> + ?Sized;
}

macro_rules! tuple_observe {
    ($(($index:tt, $ty:ident)),+ $(,)?) => { tuple_observe!(@tuples [] ; $(($index, $ty)),+); };
    (@tuples [$($prefix:tt)*] ; ($index:tt, $ty:ident) $(, $rest:tt)*) => {
        tuple_observe!(@tuple [$($prefix)* ($index, $ty)]);
        tuple_observe!(@tuples [$($prefix)* ($index, $ty)] ; $($rest),*);
    };
    (@tuples [$($prefix:tt)*] ;) => {};
    (@tuple [$(($index:tt, $ty:ident))+]) => { tuple_observe!(@slots [$(($index, $ty))+] ; $(($index, $ty))+); };
    (@slots [$(($index:tt, $ty:ident))+] ; ($slot:tt, $target:ident) $($rest:tt)*) => {
        impl<T: ?Sized, Inner, $($ty),+> SelectFrom<T, (Slot<{ $slot + 1 }>, Inner)>
            for Candidates<($($ty,)+)>
        where
            $target: Observe<T, (Select<Candidates<($($ty,)+)>, $target>, Inner)>,
        {
            type Semantic = Select<Candidates<($($ty,)+)>, $target>;
            type Observer<Head, Depth> = <$target as Observe<
                T, (Select<Candidates<($($ty,)+)>, $target>, Inner)
            >>::Observer<Head, Depth>
            where
                Depth: Unsigned,
                Head: AsDerefMut<Depth, Target = T> + ?Sized;
        }
        tuple_observe!(@slots [$(($index, $ty))+] ; $($rest)*);
    };
    (@slots [$($all:tt)*] ;) => {};
}

tuple_observe!(
    (0, T0),
    (1, T1),
    (2, T2),
    (3, T3),
    (4, T4),
    (5, T5),
    (6, T6),
    (7, T7),
    (8, T8),
    (9, T9),
    (10, T10),
    (11, T11)
);
