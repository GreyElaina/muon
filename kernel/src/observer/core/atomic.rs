//! Value observation for atomic scalars from `core`.

use core::sync::atomic::Ordering;

use crate::{
    AsDerefMut, Change, CollectState, Invalidate, Observe, Path, Query, Replace, State,
    StateObserver, Unsigned, emit,
};

/// Initial scalar value retained by an atomic observer.
#[doc(hidden)]
pub struct AtomicState<Value> {
    before: Value,
}

macro_rules! atomic_observe {
    ($($atomic:ident => $value:ty),* $(,)?) => {
        $(
            impl Invalidate<core::sync::atomic::$atomic> for AtomicState<$value> {
                fn invalidate(&mut self, _: &core::sync::atomic::$atomic) {}
            }

            impl State<core::sync::atomic::$atomic> for AtomicState<$value> {
                fn observe(value: &core::sync::atomic::$atomic) -> Self {
                    Self {
                        before: value.load(Ordering::Relaxed),
                    }
                }
            }

            impl<Context: ?Sized, Route, Error, Semantic>
                CollectState<core::sync::atomic::$atomic, Context, Route, Error, Semantic>
                for AtomicState<$value>
            where
                for<'a> Context:
                    Query<Change<'a, core::sync::atomic::$atomic, $value>, Route, Semantic>,
                for<'a> <Context as Query<
                    Change<'a, core::sync::atomic::$atomic, $value>,
                    Route,
                    Semantic,
                >>::Output: Replace<core::sync::atomic::$atomic, $value>,
                for<'a> Error: From<
                    <<Context as Query<
                        Change<'a, core::sync::atomic::$atomic, $value>,
                        Route,
                        Semantic,
                    >>::Output as Replace<core::sync::atomic::$atomic, $value>>::Error,
                >,
            {
                fn collect(
                    &mut self,
                    value: &core::sync::atomic::$atomic,
                    path: &Path<'_>,
                    context: &mut Context,
                ) -> Result<(), Error> {
                    if self.before != value.load(Ordering::Relaxed) {
                        emit::<
                            core::sync::atomic::$atomic,
                            $value,
                            Context,
                            Route,
                            Semantic,
                            Error,
                        >(
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

            impl Observe for core::sync::atomic::$atomic {
                type Observer<Head, Depth>
                    = StateObserver<Self, AtomicState<$value>, Head, Depth>
                where
                    Depth: Unsigned,
                    Head: AsDerefMut<Depth, Target = Self> + ?Sized;
            }
        )*
    };
}

#[cfg(target_has_atomic = "8")]
atomic_observe! {
    AtomicBool => bool,
    AtomicU8 => u8,
    AtomicI8 => i8,
}

#[cfg(target_has_atomic = "16")]
atomic_observe! {
    AtomicU16 => u16,
    AtomicI16 => i16,
}

#[cfg(target_has_atomic = "32")]
atomic_observe! {
    AtomicU32 => u32,
    AtomicI32 => i32,
}

#[cfg(target_has_atomic = "64")]
atomic_observe! {
    AtomicU64 => u64,
    AtomicI64 => i64,
}

#[cfg(target_has_atomic = "ptr")]
atomic_observe! {
    AtomicUsize => usize,
    AtomicIsize => isize,
}
