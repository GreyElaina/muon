use std::num::NonZero;

use crate::Observe;
use crate::helper::shallow::{ObserverState, SerializeObserverState, shallow_observer};
use crate::helper::{AsDeref, AsDerefMut, Invalidate, Unsigned};
use crate::observe::{RoObserve, Sink};

/// A trait for creating snapshots of observable values.
///
/// [`Snapshot`] is used by [`SnapshotObserver`](crate::general::SnapshotObserver) to capture the
/// initial state of a value. The companion trait [`SerializeSnapshot`] handles comparison and
/// mutation production during flush.
///
/// ## Deep Copy Semantics
///
/// For most simple types, [`Snapshot`](Snapshot::Snapshot) is the type itself (i.e., `type Snapshot
/// = Self`). However, for pointer types like [`Rc<T>`](std::rc::Rc), [`&T`](reference), and
/// [`&mut T`](reference), the associated [`Snapshot`](Snapshot::Snapshot) type is `T::Snapshot`
/// rather than `Self`. This means [`Snapshot`] performs a "deep copy" through indirections,
/// capturing the underlying value rather than the pointer itself.
pub trait Snapshot {
    /// The snapshot type used for comparison.
    ///
    /// For value types, this is typically `Self`. For pointer and reference types, this is the
    /// snapshot type of the pointed-to value.
    type Snapshot;

    /// Creates a snapshot of the current value.
    ///
    /// For pointer types, this performs a deep copy of the underlying value.
    fn to_snapshot(&self) -> Self::Snapshot;
}

/// Extends [`Snapshot`] with the ability to flush recorded changes by comparing against
/// a stored snapshot.
///
/// The `flush` method compares the current value against the old snapshot and reports
/// events into the sink. The snapshot has already been updated (re-observed) before
/// `flush` is called; `flush` only needs to compare and report changes.
///
/// The supertrait constrains the snapshot type to be serializable and
/// `'static`, so a `T: SerializeSnapshot` bound always carries
/// `T::Snapshot: Serialize + 'static` (supertrait associated-type bounds
/// are implied bounds; a trait-level `where` clause would not be).
pub trait SerializeSnapshot
where
    Self: Snapshot<Snapshot: serde::Serialize + 'static> + serde::Serialize,
{
    /// Compares the current value against the old snapshot and reports changes.
    fn flush<S: Sink + ?Sized>(&self, snapshot: Self::Snapshot, sink: &mut S);
}

struct SnapshotObserverState<T: Snapshot + ?Sized> {
    snapshot: T::Snapshot,
}

impl<T: Snapshot + ?Sized> Invalidate<T> for SnapshotObserverState<T> {
    fn invalidate(&mut self, _: &T) {}
}

impl<T: Snapshot + ?Sized> ObserverState<T> for SnapshotObserverState<T> {
    fn observe(value: &T) -> Self {
        Self {
            snapshot: value.to_snapshot(),
        }
    }
}

impl<T: SerializeSnapshot + ?Sized, S: Sink + ?Sized> SerializeObserverState<T, S>
    for SnapshotObserverState<T>
{
    fn flush(&mut self, value: &T, sink: &mut S) {
        SerializeSnapshot::flush(
            value,
            std::mem::replace(&mut self.snapshot, value.to_snapshot()),
            sink,
        )
    }
}

shallow_observer! {
    /// A general observer that uses snapshot comparison to detect actual value changes.
    ///
    /// [`SnapshotObserver`] creates a clone of the initial value and compares it with the
    /// final value using [`PartialEq`]. This provides accurate change detection by comparing
    /// actual values rather than tracking access patterns.
    ///
    /// ## Requirements
    ///
    /// The observed type must implement:
    /// - [`Clone`] - for creating the snapshot
    /// - [`PartialEq`] - for comparing values
    ///
    /// ## Derive Usage
    ///
    /// Can be used via the `#[muon(snapshot)]` attribute in derive macros:
    ///
    /// ```
    /// # use muon::Observe;
    /// # use serde::Serialize;
    /// # #[derive(Serialize, Observe)]
    /// # struct Uuid;
    /// # #[derive(Serialize, Observe)]
    /// # struct BitFlags;
    /// #[derive(Serialize, Observe)]
    /// struct MyStruct {
    ///     #[muon(snapshot)]
    ///     id: Uuid,           // Cheap to clone and compare
    ///     #[muon(snapshot)]
    ///     flags: BitFlags,    // Small Copy type
    /// }
    /// ```
    ///
    /// ## When to Use
    ///
    /// [`SnapshotObserver`] is ideal when:
    /// 1. The type implements [`Clone`] and [`PartialEq`] with low cost
    /// 2. Values may be modified and then restored to original (so that
    ///    [`ShallowObserver`](super::ShallowObserver) would yield false positives)
    ///
    /// ## Built-in Usage
    ///
    /// All `Copy` + [`PartialEq`] standard types use [`SnapshotObserver`] as their default
    /// implementation. This includes numeric primitives ([`i32`], [`f64`], [`bool`], [`char`],
    /// etc.), the unit type `()`, [`NonZero`](std::num::NonZero) variants, network types
    /// ([`IpAddr`](core::net::IpAddr), [`SocketAddr`](core::net::SocketAddr)), and time types
    /// ([`Duration`](core::time::Duration), [`SystemTime`](std::time::SystemTime)).
    struct SnapshotObserver<T: Snapshot>(T, SnapshotObserverState<T>);
}

macro_rules! impl_ops_assign {
    ($($trait:ident => $method:ident),* $(,)?) => {
        $(
            impl<'ob, T, S: ?Sized, D, U> std::ops::$trait<U> for SnapshotObserver<'ob, T, S, D>
            where
                T: Snapshot + std::ops::$trait<U>,
                D: $crate::helper::Unsigned,
                S: $crate::helper::AsDerefMut<D, Target = T>,
            {
                fn $method(&mut self, rhs: U) {
                    $crate::helper::QuasiObserver::tracked_mut(self).$method(rhs);
                }
            }
        )*
    };
}

impl_ops_assign! {
    AddAssign => add_assign,
    SubAssign => sub_assign,
    MulAssign => mul_assign,
    DivAssign => div_assign,
    RemAssign => rem_assign,
    BitAndAssign => bitand_assign,
    BitOrAssign => bitor_assign,
    BitXorAssign => bitxor_assign,
    ShlAssign => shl_assign,
    ShrAssign => shr_assign,
}

/// Snapshot-based observation specification.
///
/// [`SnapshotSpec`] marks a type as supporting efficient snapshot comparison (requires [`Clone`] +
/// [`PartialEq`]). When used as the [`Spec`](crate::Observe::Spec) for a type `T`, it affects
/// certain wrapper type observations, such as [`Option<T>`].
pub struct SnapshotSpec;

macro_rules! impl_snapshot_observe {
    ($($ty:ty),* $(,)?) => {
        $(
            impl Snapshot for $ty {
                type Snapshot = Self;
                fn to_snapshot(&self) -> Self {
                    *self
                }
            }

            impl SerializeSnapshot for $ty {
                fn flush<S: Sink + ?Sized>(&self, snapshot: Self, sink: &mut S) {
                    if self != &snapshot {
                        sink.replace(
                            Some(&snapshot),
                            Some(self),
                        );
                    }
                }
            }

            impl Observe for $ty {
                type Observer<'ob, S, D>
                    = SnapshotObserver<'ob, Self, S, D>
                where
                    Self: 'ob,
                    D: Unsigned,
                    S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

                type Spec = SnapshotSpec;
            }



            impl RoObserve for $ty {
                type Observer<'ob, S, D>
                    = SnapshotObserver<'ob, Self, S, D>
                where
                    Self: 'ob,
                    D: Unsigned,
                    S: AsDeref<D, Target = Self> + ?Sized + 'ob;

                type Spec = SnapshotSpec;
            }
        )*
    };
}

impl_snapshot_observe! {
    (), usize, u8, u16, u32, u64, u128, isize, i8, i16, i32, i64, i128, bool, char,
    NonZero<usize>, NonZero<u8>, NonZero<u16>, NonZero<u32>, NonZero<u64>, NonZero<u128>,
    NonZero<isize>, NonZero<i8>, NonZero<i16>, NonZero<i32>, NonZero<i64>, NonZero<i128>,
    core::net::IpAddr, core::net::Ipv4Addr, core::net::Ipv6Addr,
    core::net::SocketAddr, core::net::SocketAddrV4, core::net::SocketAddrV6,
    core::time::Duration, std::time::SystemTime,
}

/// Floats get a NaN-aware comparison: IEEE 754 says `NaN != NaN`,
/// but an unchanged value must not report a phantom replace.
macro_rules! impl_float_snapshot_observe {
    ($($ty:ty),* $(,)?) => {
        $(
            impl Snapshot for $ty {
                type Snapshot = Self;
                fn to_snapshot(&self) -> Self {
                    *self
                }
            }

            impl SerializeSnapshot for $ty {
                fn flush<S: Sink + ?Sized>(&self, snapshot: Self, sink: &mut S) {
                    if self != &snapshot && !(self.is_nan() && snapshot.is_nan()) {
                        sink.replace(
                            Some(&snapshot),
                            Some(self),
                        );
                    }
                }
            }

            impl Observe for $ty {
                type Observer<'ob, S, D>
                    = SnapshotObserver<'ob, Self, S, D>
                where
                    Self: 'ob,
                    D: Unsigned,
                    S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

                type Spec = SnapshotSpec;
            }

            impl RoObserve for $ty {
                type Observer<'ob, S, D>
                    = SnapshotObserver<'ob, Self, S, D>
                where
                    Self: 'ob,
                    D: Unsigned,
                    S: AsDeref<D, Target = Self> + ?Sized + 'ob;

                type Spec = SnapshotSpec;
            }
        )*
    };
}

impl_float_snapshot_observe! { f32, f64 }

#[cfg(feature = "chrono")]
impl_snapshot_observe! {
    chrono::Month, chrono::NaiveDate, chrono::NaiveDateTime,
    chrono::NaiveTime, chrono::TimeDelta, chrono::Weekday,
}

#[cfg(feature = "uuid")]
impl_snapshot_observe! {
    uuid::Uuid, uuid::NonNilUuid,
}

macro_rules! generic_impl_snapshot_observe {
    ($(impl $([$($gen:tt)*])? _ for $ty:ty);* $(;)?) => {
        $(
            impl<$($($gen)*)?> Snapshot for $ty {
                type Snapshot = Self;
                fn to_snapshot(&self) -> Self {
                    self.clone()
                }
            }

            impl<$($($gen)*)?> SerializeSnapshot for $ty where Self: serde::Serialize + 'static {
                fn flush<S: Sink + ?Sized>(&self, snapshot: Self, sink: &mut S) {
                    if self != &snapshot {
                        sink.replace(
                            Some(&snapshot),
                            Some(self),
                        );
                    }
                }
            }

            impl<$($($gen)*)?> Observe for $ty {
                type Observer<'ob, S, D>
                    = SnapshotObserver<'ob, Self, S, D>
                where
                    Self: 'ob,
                    D: Unsigned,
                    S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

                type Spec = SnapshotSpec;
            }

            impl<$($($gen)*)?> RoObserve for $ty {
                type Observer<'ob, S, D>
                    = SnapshotObserver<'ob, Self, S, D>
                where
                    Self: 'ob,
                    D: Unsigned,
                    S: AsDeref<D, Target = Self> + ?Sized + 'ob;

                type Spec = SnapshotSpec;
            }
        )*
    };
}

generic_impl_snapshot_observe! {
    impl [T] _ for std::marker::PhantomData<T>;
}

#[cfg(feature = "chrono")]
generic_impl_snapshot_observe! {
    impl [Tz: chrono::TimeZone] _ for chrono::DateTime<Tz>;
}
