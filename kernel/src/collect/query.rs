//! Compile-time selection of a handler from a context.

/// Selects a handler from a context. `Route` is compile-time selection evidence.
pub trait Query<Q: ?Sized, Route, Scope = ()> {
    /// Handler selected for `Q`.
    type Output: ?Sized;
    /// Returns the selected handler.
    fn query(&mut self) -> &mut Self::Output;
}

/// Directly selects the context itself.
pub enum Here {}

/// Selects tuple member `N`, then follows `Route` inside it.
pub struct Through<const N: usize, Route>(core::marker::PhantomData<fn() -> Route>);

impl<T: ?Sized> Query<T, Here> for &mut T {
    type Output = T;

    fn query(&mut self) -> &mut T {
        self
    }
}

macro_rules! tuple_query {
    ($(($index:tt, $ty:ident)),+ $(,)?) => { tuple_query!(@tuples [] ; $(($index, $ty)),+); };
    (@tuples [$($prefix:tt)*] ; ($index:tt, $ty:ident) $(, $rest:tt)*) => {
        tuple_query!(@tuple [$($prefix)* ($index, $ty)]);
        tuple_query!(@tuples [$($prefix)* ($index, $ty)] ; $($rest),*);
    };
    (@tuples [$($prefix:tt)*] ;) => {};
    (@tuple [$(($index:tt, $ty:ident))+]) => { tuple_query!(@slots [$(($index, $ty))+] ; $(($index, $ty))+); };
    (@slots [$(($index:tt, $ty:ident))+] ; ($slot:tt, $target:ident) $($rest:tt)*) => {
        impl<Q: ?Sized, Scope, Inner, $($ty),+> Query<Q, Through<$slot, Inner>, Scope> for ($($ty,)+)
        where
            $target: Query<Q, Inner, Scope>,
        {
            type Output = <$target as Query<Q, Inner, Scope>>::Output;

            fn query(&mut self) -> &mut Self::Output {
                self.$slot.query()
            }
        }
        tuple_query!(@slots [$(($index, $ty))+] ; $($rest)*);
    };
    (@slots [$($all:tt)*] ;) => {};
}

tuple_query!(
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
