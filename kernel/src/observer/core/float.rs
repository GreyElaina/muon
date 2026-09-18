//! NaN-aware endpoint observers for built-in floating-point values.

use crate::{
    AsDerefMut, Change, CollectState, Invalidate, Observe, Path, Query, Replace, State,
    StateObserver, Unsigned, Zero, emit,
};

/// Initial-value state for a floating-point observer.
#[doc(hidden)]
pub struct FloatState<T> {
    before: T,
}

/// Observer for built-in floating-point values with NaN-aware equality.
#[doc(hidden)]
pub type FloatObserver<T, Head, Depth = Zero> = StateObserver<T, FloatState<T>, Head, Depth>;

macro_rules! float_observe {
    ($($ty:ty),* $(,)?) => {
        $(
            impl Invalidate<$ty> for FloatState<$ty> {
                fn invalidate(&mut self, _: &$ty) {}
            }

            impl State<$ty> for FloatState<$ty> {
                fn observe(value: &$ty) -> Self {
                    Self { before: *value }
                }
            }

            impl<Context: ?Sized, Route, Error, Semantic>
                CollectState<$ty, Context, Route, Error, Semantic> for FloatState<$ty>
            where
                for<'a> Context: Query<Change<'a, $ty>, Route, Semantic>,
                for<'a> <Context as Query<Change<'a, $ty>, Route, Semantic>>::Output:
                    Replace<$ty, $ty>,
                for<'a> Error: From<
                    <<Context as Query<Change<'a, $ty>, Route, Semantic>>::Output as Replace<
                        $ty,
                        $ty,
                    >>::Error,
                >,
            {
                fn collect(
                    &mut self,
                    value: &$ty,
                    path: &Path<'_>,
                    context: &mut Context,
                ) -> Result<(), Error> {
                    if self.before != *value && !(self.before.is_nan() && value.is_nan()) {
                        emit::<$ty, $ty, Context, Route, Semantic, Error>(
                            context,
                            Change::Replace {
                                path,
                                before: Some(&self.before),
                                after: value,
                            },
                        )?;
                    }
                    Ok(())
                }
            }

            impl Observe for $ty {
                type Observer<Head, Depth>
                    = FloatObserver<Self, Head, Depth>
                where
                    Depth: Unsigned,
                    Head: AsDerefMut<Depth, Target = Self> + ?Sized;
            }
        )*
    };
}

float_observe!(f32, f64);
