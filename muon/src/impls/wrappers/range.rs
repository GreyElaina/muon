use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut, Range, RangeFrom, RangeInclusive, RangeTo};

use serde::Serialize;

use crate::general::{SerializeSnapshot, Snapshot};
use crate::helper::macros::{spec_impl_observe, spec_impl_observe_from_ro, spec_impl_ro_observe};
use crate::helper::{
    AsDeref, AsDerefMut, AsDerefPtrExt, Pointer, QuasiObserver, Succ, Unsigned, Zero,
};
use crate::observe::{Flush, FlushWith, Observer, QuasiSink, Sink};

macro_rules! impl_range {
    ($($ty:ident ($($field:ident),* $(,)?) => $ob:ident, $helper_ref:ident, $helper_mut:ident;)*) => {
        $(
            /// Observer implementation for [`Range<Idx>`].
            #[doc = concat!("Observer implementation for [`", stringify!($ty), "<Idx>`].")]
            pub struct $ob<O, S: ?Sized, D = Zero> {
                $(
                    #[doc = concat!("See [`", stringify!($ty), "::", stringify!($field), "`].")]
                    pub $field: O,
                )*
                ptr: Pointer<S>,
                phantom: PhantomData<D>,
            }

            impl<O, S: ?Sized, D> Deref for $ob<O, S, D> {
                type Target = Pointer<S>;

                fn deref(&self) -> &Self::Target {
                    &self.ptr
                }
            }

            impl<O, S: ?Sized, D> DerefMut for $ob<O, S, D> {
                fn deref_mut(&mut self) -> &mut Self::Target {
                    std::ptr::from_mut(self).expose_provenance();
                    Pointer::invalidate(&mut self.ptr);
                    &mut self.ptr
                }
            }

            impl<O, S: ?Sized, D> QuasiObserver for $ob<O, S, D>
            where
                O: QuasiObserver,
                D: Unsigned,
                S: AsDeref<D>,
            {
                type Head = S;
                type OuterDepth = Succ<Zero>;
                type InnerDepth = D;

                fn invalidate(this: &mut Self) {
                    $(O::invalidate(&mut this.$field);)*
                }
            }

            impl<O, S: ?Sized, D> Observer for $ob<O, S, D>
            where
                D: Unsigned,
                S: AsDeref<D, Target = $ty<O::Head>>,
                O: Observer<InnerDepth = Zero>,
                O::Head: Sized,
            {
                unsafe fn observe(head: *mut Self::Head) -> Self {
                    unsafe {
                        let value = head.as_deref_ptr::<D>();
                        let this = Self {
                            $($field: O::observe(&raw mut (*value).$field),)*
                            ptr: Pointer::new_unchecked(head),
                            phantom: PhantomData,
                        };
                        $(Pointer::register_observer(&this.ptr, &this.$field);)*
                        this
                    }
                }

                unsafe fn relocate(this: &mut Self, head: *mut Self::Head) {
                    unsafe {
                        let value = head.as_deref_ptr::<D>();
                        $(O::relocate(&mut this.$field, &raw mut (*value).$field);)*
                        Pointer::set_unchecked(this, head);
                    }
                }
            }

            impl<O, S: ?Sized, D, Sk: Sink + ?Sized> QuasiSink<Sk> for $ob<O, S, D> {
                type Operation = Sk::Operation;
                type Identity = Sk::Identity;
            }

            impl<O, S: ?Sized, D, Sk: Sink + ?Sized, Elem: ?Sized> FlushWith<Sk, Elem> for $ob<O, S, D>
            where
                D: Unsigned,
                S: AsDeref<D, Target = $ty<O::Head>>,
                O: Observer<InnerDepth = Zero> + Flush<Sk>,
                O::Head: Serialize + Sized + 'static,
            {
                fn flush_with<F>(this: &mut Self, sink: &mut Sk, _flush_elem: F)
                where
                    F: FnMut(&mut Elem, &mut Sk),
                {
                    <Self as Flush<Sk>>::flush(this, sink)
                }
            }

            impl<O, S: ?Sized, D, Sk: Sink + ?Sized> Flush<Sk> for $ob<O, S, D>
            where
                D: Unsigned,
                S: AsDeref<D, Target = $ty<O::Head>>,
                O: Observer<InnerDepth = Zero> + Flush<Sk>,
                O::Head: Serialize + Sized + 'static,
            {
                fn flush(this: &mut Self, sink: &mut Sk) {
                    $(
                        sink.push_field(stringify!($field));
                        <O as Flush<Sk>>::flush(&mut this.$field, sink);
                        sink.pop_segment();
                    )*
                }
            }


            impl<O, S: ?Sized, D> Debug for $ob<O, S, D>
            where
                O: QuasiObserver,
                D: Unsigned,
                S: AsDeref<D>,
                S::Target: Debug,
            {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.debug_tuple(stringify!($ob)).field(&self.untracked_ref()).finish()
                }
            }

            impl<O, S: ?Sized, D, U> PartialEq<$ty<U>> for $ob<O, S, D>
            where
                O: QuasiObserver,
                D: Unsigned,
                S: AsDeref<D>,
                S::Target: PartialEq<$ty<U>>,
            {
                fn eq(&self, other: &$ty<U>) -> bool {
                    self.untracked_ref().eq(other)
                }
            }

            impl<O1, O2, S1: ?Sized, S2: ?Sized, D1, D2> PartialEq<$ob<O2, S2, D2>> for $ob<O1, S1, D1>
            where
                O1: QuasiObserver<Target: Deref<Target: AsDeref<O1::InnerDepth>>>,
                O2: QuasiObserver<Target: Deref<Target: AsDeref<O2::InnerDepth>>>,
                D1: Unsigned,
                D2: Unsigned,
                S1: AsDeref<D1>,
                S2: AsDeref<D2>,
                S1::Target: PartialEq<S2::Target>,
            {
                fn eq(&self, other: &$ob<O2, S2, D2>) -> bool {
                    self.untracked_ref().eq(other.untracked_ref())
                }
            }

            impl<O, S: ?Sized, D> Eq for $ob<O, S, D>
            where
                O: QuasiObserver,
                D: Unsigned,
                S: AsDeref<D>,
                S::Target: Eq,
            {
            }

            spec_impl_observe!($helper_ref, $ty<Self>, $ty<T>, $ob);
            spec_impl_ro_observe!($helper_mut, $ty<Self>, $ty<T>, $ob);

            impl<T: Snapshot> Snapshot for $ty<T> {
                type Snapshot = $ty<T::Snapshot>;

                fn to_snapshot(&self) -> Self::Snapshot {
                    $ty {
                        $($field: self.$field.to_snapshot(),)*
                    }
                }
            }

            impl<T: SerializeSnapshot> SerializeSnapshot for $ty<T>
            where
                Self::Snapshot: serde::Serialize + 'static,
            {
                fn flush<S: Sink + ?Sized>(&self, snapshot: Self::Snapshot, sink: &mut S) {
                    $(
                        sink.push_field(stringify!($field));
                        SerializeSnapshot::flush(&self.$field, snapshot.$field, sink);
                        sink.pop_segment();
                    )*
                }
            }
        )*
    };
}

impl_range! {
    Range (start, end) => RangeObserver, RangeObserveImpl, RangeRoObserveImpl;
    RangeFrom (start) => RangeFromObserver, RangeFromObserveImpl, RangeFromRoObserveImpl;
    RangeTo (end) => RangeToObserver, RangeToObserveImpl, RangeToRoObserveImpl;
}

/// Observer implementation for [`RangeInclusive<Idx>`].
pub struct RangeInclusiveObserver<O, S: ?Sized, D = Zero> {
    start: O,
    end: O,
    ptr: Pointer<S>,
    phantom: PhantomData<D>,
}

impl<O, S: ?Sized, D> Deref for RangeInclusiveObserver<O, S, D> {
    type Target = Pointer<S>;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

impl<O, S: ?Sized, D> DerefMut for RangeInclusiveObserver<O, S, D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        std::ptr::from_mut(self).expose_provenance();
        Pointer::invalidate(&mut self.ptr);
        &mut self.ptr
    }
}

impl<O, S: ?Sized, D> QuasiObserver for RangeInclusiveObserver<O, S, D>
where
    O: QuasiObserver,
    D: Unsigned,
    S: AsDeref<D>,
{
    type Head = S;
    type OuterDepth = Succ<Zero>;
    type InnerDepth = D;

    fn invalidate(this: &mut Self) {
        O::invalidate(&mut this.start);
        O::invalidate(&mut this.end);
    }
}

impl<O, S: ?Sized, D> Observer for RangeInclusiveObserver<O, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = RangeInclusive<O::Head>>,
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
{
    unsafe fn observe(head: *mut Self::Head) -> Self {
        unsafe {
            let value = &*head.as_deref_ptr::<D>();
            let this = Self {
                start: O::observe(std::ptr::from_ref(value.start()).cast_mut()),
                end: O::observe(std::ptr::from_ref(value.end()).cast_mut()),
                ptr: Pointer::new_unchecked(head),
                phantom: PhantomData,
            };
            Pointer::register_observer(&this.ptr, &this.start);
            Pointer::register_observer(&this.ptr, &this.end);
            this
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Self::Head) {
        unsafe {
            let value = &*head.as_deref_ptr::<D>();
            O::relocate(
                &mut this.start,
                std::ptr::from_ref(value.start()).cast_mut(),
            );
            O::relocate(&mut this.end, std::ptr::from_ref(value.end()).cast_mut());
            Pointer::set_unchecked(this, head);
        }
    }
}

impl<O, S: ?Sized, D, Sk: Sink + ?Sized> QuasiSink<Sk> for RangeInclusiveObserver<O, S, D> {
    type Operation = Sk::Operation;
    type Identity = Sk::Identity;
}

impl<O, S: ?Sized, D, Sk: Sink + ?Sized, Elem: ?Sized> FlushWith<Sk, Elem>
    for RangeInclusiveObserver<O, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = RangeInclusive<O::Head>>,
    O: Observer<InnerDepth = Zero> + Flush<Sk>,
    O::Head: Serialize + Sized + 'static,
{
    fn flush_with<F>(this: &mut Self, sink: &mut Sk, _flush_elem: F)
    where
        F: FnMut(&mut Elem, &mut Sk),
    {
        <Self as Flush<Sk>>::flush(this, sink)
    }
}

impl<O, S: ?Sized, D, Sk: Sink + ?Sized> Flush<Sk> for RangeInclusiveObserver<O, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = RangeInclusive<O::Head>>,
    O: Observer<InnerDepth = Zero> + Flush<Sk>,
    O::Head: Serialize + Sized + 'static,
{
    fn flush(this: &mut Self, sink: &mut Sk) {
        sink.push_field("start");
        <O as Flush<Sk>>::flush(&mut this.start, sink);
        sink.pop_segment();
        sink.push_field("end");
        <O as Flush<Sk>>::flush(&mut this.end, sink);
        sink.pop_segment();
    }
}

impl<O, S: ?Sized, D> Debug for RangeInclusiveObserver<O, S, D>
where
    O: QuasiObserver,
    D: Unsigned,
    S: AsDeref<D>,
    S::Target: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RangeInclusiveObserver")
            .field(&self.untracked_ref())
            .finish()
    }
}

impl<O, S: ?Sized, D, U> PartialEq<RangeInclusive<U>> for RangeInclusiveObserver<O, S, D>
where
    O: QuasiObserver,
    D: Unsigned,
    S: AsDeref<D>,
    S::Target: PartialEq<RangeInclusive<U>>,
{
    fn eq(&self, other: &RangeInclusive<U>) -> bool {
        self.untracked_ref().eq(other)
    }
}

impl<O1, O2, S1: ?Sized, S2: ?Sized, D1, D2> PartialEq<RangeInclusiveObserver<O2, S2, D2>>
    for RangeInclusiveObserver<O1, S1, D1>
where
    O1: QuasiObserver<Target: Deref<Target: AsDeref<O1::InnerDepth>>>,
    O2: QuasiObserver<Target: Deref<Target: AsDeref<O2::InnerDepth>>>,
    D1: Unsigned,
    D2: Unsigned,
    S1: AsDeref<D1>,
    S2: AsDeref<D2>,
    S1::Target: PartialEq<S2::Target>,
{
    fn eq(&self, other: &RangeInclusiveObserver<O2, S2, D2>) -> bool {
        self.untracked_ref().eq(other.untracked_ref())
    }
}

impl<O, S: ?Sized, D> Eq for RangeInclusiveObserver<O, S, D>
where
    O: QuasiObserver,
    D: Unsigned,
    S: AsDeref<D>,
    S::Target: Eq,
{
}

spec_impl_observe_from_ro!(
    RangeInclusiveObserveImpl,
    RangeInclusive<Self>,
    RangeInclusive<T>,
    RangeInclusiveObserver
);

spec_impl_ro_observe!(
    RangeInclusiveRoObserveImpl,
    RangeInclusive<Self>,
    RangeInclusive<T>,
    RangeInclusiveObserver
);

impl<T: Snapshot> Snapshot for RangeInclusive<T> {
    type Snapshot = (T::Snapshot, T::Snapshot);

    fn to_snapshot(&self) -> Self::Snapshot {
        (self.start().to_snapshot(), self.end().to_snapshot())
    }
}

impl<T: SerializeSnapshot> SerializeSnapshot for RangeInclusive<T>
where
    Self::Snapshot: serde::Serialize + 'static,
{
    fn flush<S: Sink + ?Sized>(&self, snapshot: Self::Snapshot, sink: &mut S) {
        sink.push_field("start");
        SerializeSnapshot::flush(&self.start(), snapshot.0, sink);
        sink.pop_segment();
        sink.push_field("end");
        SerializeSnapshot::flush(&self.end(), snapshot.1, sink);
        sink.pop_segment();
    }
}

#[cfg(test)]
mod tests {
    use muon_test_utils::*;
    use serde_json::json;

    use super::*;
    use crate::general::SnapshotObserver;
    use crate::helper::QuasiObserver;
    use crate::observe::ObserveExt;

    #[test]
    fn range_no_change_returns_none() {
        let mut range = 0..10i32;
        let mut ob = range.__observe();
        assert!(__flush!(&mut ob).is_empty());
    }

    #[test]
    fn range_deref_triggers_replace() {
        let mut range = 0..10i32;
        let mut ob = range.__observe();
        *ob.tracked_mut() = 5..15;
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["start"], "before": 0, "after": 5},
                {"path": ["end"], "before": 10, "after": 15},
            ]),
        );
    }

    #[test]
    fn range_granular_start_change() {
        let mut range = String::from("a")..String::from("z");
        let mut ob = range.__observe();
        ob.start.push_str("bc");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["start"], "before": "a", "after": "abc"}]),
        );
    }

    #[test]
    fn range_granular_end_change() {
        let mut range = String::from("a")..String::from("z");
        let mut ob = range.__observe();
        ob.end.push_str("yx");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["end"], "before": "z", "after": "zyx"}]),
        );
    }

    #[test]
    fn range_both_fields_replace_collapse() {
        let mut range = String::from("a")..String::from("z");
        let mut ob = range.__observe();
        *ob.tracked_mut() = String::from("b")..String::from("y");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["start"], "before": "a", "after": "b"},
                {"path": ["end"], "before": "z", "after": "y"},
            ]),
        );
    }

    #[test]
    fn range_specialization() {
        let mut range = 0..10i32;
        let ob: SnapshotObserver<_, _, _> = range.__observe();
        assert_eq!(format!("{ob:?}"), "SnapshotObserver(0..10)");

        let mut range = String::from("a")..String::from("z");
        let ob: RangeObserver<_, _, _> = range.__observe();
        assert_eq!(format!("{ob:?}"), r#"RangeObserver("a".."z")"#);
    }
}
