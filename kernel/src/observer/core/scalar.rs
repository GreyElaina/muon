//! Endpoint observers for the closed set of scalar values supported by the kernel.

use crate::{
    AsDerefMut, Change, CollectState, Invalidate, Observe, Path, Query, Replace, State,
    StatefulObserver, Unsigned, Zero, emit,
};

/// Initial-value state for a scalar observer.
#[doc(hidden)]
pub struct ScalarState<T> {
    before: T,
}

/// Observer used by the kernel's closed set of scalar values.
#[doc(hidden)]
pub type ScalarObserver<T, Head, Depth = Zero> = StatefulObserver<ScalarState<T>, Head, Depth>;

impl<T: Copy> Invalidate<T> for ScalarState<T> {
    fn invalidate(&mut self, _: &T) {}
}

impl<T: Copy> State<T> for ScalarState<T> {
    fn observe(value: &T) -> Self {
        Self { before: *value }
    }
}

impl<T, Context: ?Sized, Route, Error, Semantic> CollectState<T, Context, Route, Error, Semantic>
    for ScalarState<T>
where
    T: Copy + PartialEq,
    for<'a> Context: Query<Change<'a, T>, Route, Semantic>,
    for<'a> <Context as Query<Change<'a, T>, Route, Semantic>>::Output: Replace<T, T>,
    for<'a> Error:
        From<<<Context as Query<Change<'a, T>, Route, Semantic>>::Output as Replace<T, T>>::Error>,
{
    fn collect(&mut self, value: &T, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        if self.before != *value {
            emit::<T, T, Context, Route, Semantic, Error>(
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

macro_rules! scalar_observe {
    ($($ty:ty),* $(,)?) => {
        $(
            impl Observe for $ty {
                type Observer<Head, Depth>
                    = ScalarObserver<Self, Head, Depth>
                where
                    Depth: Unsigned,
                    Head: AsDerefMut<Depth, Target = Self> + ?Sized;
            }
        )*
    };
}

scalar_observe!(
    (),
    bool,
    char,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    core::num::NonZeroI8,
    core::num::NonZeroI16,
    core::num::NonZeroI32,
    core::num::NonZeroI64,
    core::num::NonZeroI128,
    core::num::NonZeroIsize,
    core::num::NonZeroU8,
    core::num::NonZeroU16,
    core::num::NonZeroU32,
    core::num::NonZeroU64,
    core::num::NonZeroU128,
    core::num::NonZeroUsize,
    core::cmp::Ordering,
    core::net::IpAddr,
    core::net::Ipv4Addr,
    core::net::Ipv6Addr,
    core::net::SocketAddr,
    core::net::SocketAddrV4,
    core::net::SocketAddrV6,
    core::ops::RangeFull,
    core::time::Duration,
);

#[cfg(feature = "std")]
scalar_observe!(std::time::SystemTime);
