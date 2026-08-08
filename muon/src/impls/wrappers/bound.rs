use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Bound, Deref, DerefMut};

use crate::general::{SerializeSnapshot, Snapshot};
use crate::helper::macros::{spec_impl_observe, spec_impl_ro_observe};
use crate::helper::{
    AsDeref, AsDerefMut, AsDerefPtrExt, Invalidate, Pointer, QuasiObserver, Succ, Unsigned, Zero,
};
use crate::observe::{Flush, FlushWith, Observer, QuasiSink, Sink};

struct BoundObserverState<O> {
    initial: bool,
    mutated: bool,
    inner: Bound<O>,
    /// Pre-write snapshot of the whole bound, captured at observe time
    /// and refreshed at every flush. Serves as the `Replace.before` of
    /// a whole-bound replace.
    snapshot: Option<serde_json::Value>,
}

impl<O> Invalidate<Bound<O::Head>> for BoundObserverState<O>
where
    O: QuasiObserver<Head: Sized>,
{
    fn invalidate(&mut self, _value: &Bound<O::Head>) {
        self.mutated = true;
        self.inner = Bound::Unbounded;
    }
}

/// Observer implementation for [`Bound<T>`].
pub struct BoundObserver<O, S: ?Sized, D = Zero> {
    ptr: Pointer<S>,
    state: BoundObserverState<O>,
    phantom: PhantomData<D>,
}

impl<O, S: ?Sized, D> Deref for BoundObserver<O, S, D> {
    type Target = Pointer<S>;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

impl<O, S: ?Sized, D> DerefMut for BoundObserver<O, S, D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        std::ptr::from_mut(self).expose_provenance();
        Pointer::invalidate(&mut self.ptr);
        &mut self.ptr
    }
}

impl<O, S: ?Sized, D> QuasiObserver for BoundObserver<O, S, D>
where
    O: QuasiObserver<InnerDepth = Zero, Head: Sized>,
    D: Unsigned,
    S: AsDeref<D, Target = Bound<O::Head>>,
{
    type Head = S;
    type OuterDepth = Succ<Zero>;
    type InnerDepth = D;

    fn invalidate(this: &mut Self) {
        Invalidate::invalidate(&mut this.state, (*this.ptr).as_deref());
    }
}

impl<O, S: ?Sized, D> Observer for BoundObserver<O, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = Bound<O::Head>>,
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized + SerializeSnapshot,
{
    unsafe fn observe(head: *mut Self::Head) -> Self {
        unsafe {
            let target = head.as_deref_ptr::<D>();
            let value = &*target;
            let initial = !matches!(value, Bound::Unbounded);
            let inner = match value {
                Bound::Included(v) => {
                    Bound::Included(O::observe(target.with_addr(v as *const _ as usize).cast()))
                }
                Bound::Excluded(v) => {
                    Bound::Excluded(O::observe(target.with_addr(v as *const _ as usize).cast()))
                }
                Bound::Unbounded => Bound::Unbounded,
            };
            let this = Self {
                state: BoundObserverState {
                    initial,
                    mutated: false,
                    snapshot: Some(
                        serde_json::to_value(value.to_snapshot()).expect("snapshot serializes"),
                    ),
                    inner,
                },
                ptr: Pointer::new_unchecked(head),
                phantom: PhantomData,
            };
            Pointer::register_state::<_, D>(&this.ptr, &this.state);
            this
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Self::Head) {
        unsafe {
            let target = head.as_deref_ptr::<D>();
            match (&mut this.state.inner, &*target) {
                (Bound::Included(o), Bound::Included(v)) => {
                    O::relocate(o, target.with_addr(v as *const _ as usize).cast());
                }
                (Bound::Excluded(o), Bound::Excluded(v)) => {
                    O::relocate(o, target.with_addr(v as *const _ as usize).cast());
                }
                (Bound::Unbounded, _) => {}
                _ => panic!("inconsistent state for BoundObserver"),
            }
            Pointer::set_unchecked(this, head);
        }
    }
}
impl<O, S: ?Sized, D, Sk: Sink + ?Sized> QuasiSink<Sk> for BoundObserver<O, S, D> {
    type Operation = Sk::Operation;
    type Identity = Sk::Identity;
}

impl<O, S: ?Sized, D, Sk: Sink + ?Sized, Elem: ?Sized> FlushWith<Sk, Elem>
    for BoundObserver<O, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = Bound<O::Head>>,
    O: Observer<InnerDepth = Zero> + Flush<Sk>,
    O::Head: SerializeSnapshot + Sized + 'static,
{
    fn flush_with<F>(this: &mut Self, sink: &mut Sk, _flush_elem: F)
    where
        F: FnMut(&mut Elem, &mut Sk),
    {
        <Self as Flush<Sk>>::flush(this, sink)
    }
}

impl<O, S: ?Sized, D, Sk: Sink + ?Sized> Flush<Sk> for BoundObserver<O, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = Bound<O::Head>>,
    O: Observer<InnerDepth = Zero> + Flush<Sk>,
    O::Head: SerializeSnapshot + Sized + 'static,
{
    fn flush(this: &mut Self, sink: &mut Sk) {
        let value = (*this.ptr).as_deref();
        let initial =
            std::mem::replace(&mut this.state.initial, !matches!(value, Bound::Unbounded));
        let mutated = std::mem::take(&mut this.state.mutated);
        if !mutated {
            match &mut this.state.inner {
                Bound::Included(o) => {
                    sink.push_field("Included");
                    <O as Flush<Sk>>::flush(o, sink);
                    sink.pop_segment();
                }
                Bound::Excluded(o) => {
                    sink.push_field("Excluded");
                    <O as Flush<Sk>>::flush(o, sink);
                    sink.pop_segment();
                }
                Bound::Unbounded => {}
            }
            return;
        }
        this.state.inner = Bound::Unbounded;
        if initial || !matches!(value, Bound::Unbounded) {
            let before = this.state.snapshot.take();
            let after = Some(&value as &dyn erased_serde::Serialize);
            this.state.snapshot =
                Some(serde_json::to_value(value.to_snapshot()).expect("snapshot serializes"));
            sink.replace(
                before.as_ref().map(|v| v as &dyn erased_serde::Serialize),
                after.as_ref().map(|v| v as &dyn erased_serde::Serialize),
            )
        }
    }
}

impl<O, S: ?Sized, D> Debug for BoundObserver<O, S, D>
where
    O: QuasiObserver<InnerDepth = Zero, Head: Sized>,
    D: Unsigned,
    S: AsDeref<D, Target = Bound<O::Head>>,
    Bound<O::Head>: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BoundObserver")
            .field(&self.untracked_ref())
            .finish()
    }
}

impl<O, S: ?Sized, D, U> PartialEq<Bound<U>> for BoundObserver<O, S, D>
where
    O: QuasiObserver<InnerDepth = Zero, Head: Sized>,
    D: Unsigned,
    S: AsDeref<D, Target = Bound<O::Head>>,
    Bound<O::Head>: PartialEq<Bound<U>>,
{
    fn eq(&self, other: &Bound<U>) -> bool {
        self.untracked_ref().eq(other)
    }
}

impl<O1, O2, S1: ?Sized, S2: ?Sized, D1, D2> PartialEq<BoundObserver<O2, S2, D2>>
    for BoundObserver<O1, S1, D1>
where
    O1: QuasiObserver<InnerDepth = Zero, Head: Sized>,
    O2: QuasiObserver<InnerDepth = Zero, Head: Sized>,
    D1: Unsigned,
    D2: Unsigned,
    S1: AsDeref<D1, Target = Bound<O1::Head>>,
    S2: AsDeref<D2, Target = Bound<O2::Head>>,
    Bound<O1::Head>: PartialEq<Bound<O2::Head>>,
{
    fn eq(&self, other: &BoundObserver<O2, S2, D2>) -> bool {
        self.untracked_ref().eq(other.untracked_ref())
    }
}

impl<O, S: ?Sized, D> Eq for BoundObserver<O, S, D>
where
    O: QuasiObserver<InnerDepth = Zero, Head: Sized + Eq>,
    D: Unsigned,
    S: AsDeref<D, Target = Bound<O::Head>>,
{
}

spec_impl_observe!(BoundObserveImpl, Bound<Self>, Bound<T>, BoundObserver);
spec_impl_ro_observe!(BoundRoObserveImpl, Bound<Self>, Bound<T>, BoundObserver);

impl<T: Snapshot> Snapshot for Bound<T> {
    type Snapshot = Bound<T::Snapshot>;

    fn to_snapshot(&self) -> Self::Snapshot {
        match self {
            Bound::Included(v) => Bound::Included(v.to_snapshot()),
            Bound::Excluded(v) => Bound::Excluded(v.to_snapshot()),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

impl<T: SerializeSnapshot> SerializeSnapshot for Bound<T>
where
    Self::Snapshot: serde::Serialize + 'static,
{
    fn flush<S: Sink + ?Sized>(&self, snapshot: Self::Snapshot, sink: &mut S) {
        match (self, snapshot) {
            (Bound::Included(v), Bound::Included(s)) => {
                sink.push_field("Included");
                SerializeSnapshot::flush(&v, s, sink);
                sink.pop_segment();
            }
            (Bound::Excluded(v), Bound::Excluded(s)) => {
                sink.push_field("Excluded");
                SerializeSnapshot::flush(&v, s, sink);
                sink.pop_segment();
            }
            (Bound::Unbounded, Bound::Unbounded) => {}
            (_, snapshot) => sink.replace(
                Some(&snapshot as &dyn erased_serde::Serialize),
                Some(&self as &dyn erased_serde::Serialize),
            ),
        }
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
    fn no_change_returns_none() {
        let mut bound: Bound<i32> = Bound::Unbounded;
        let mut ob = bound.__observe();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());

        let mut bound: Bound<i32> = Bound::Included(1);
        let mut ob = bound.__observe();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());

        let mut bound: Bound<i32> = Bound::Excluded(1);
        let mut ob = bound.__observe();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn deref_triggers_replace() {
        let mut bound: Bound<i32> = Bound::Included(42);
        let mut ob = bound.__observe();
        *ob.tracked_mut() = Bound::Unbounded;
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"Included": 42}, "after": "Unbounded"}]),
        );

        let mut bound: Bound<i32> = Bound::Unbounded;
        let mut ob = bound.__observe();
        *ob.tracked_mut() = Bound::Included(42);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "Unbounded", "after": {"Included": 42}}]),
        );

        let mut bound: Bound<i32> = Bound::Included(1);
        let mut ob = bound.__observe();
        *ob.tracked_mut() = Bound::Excluded(2);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"Included": 1}, "after": {"Excluded": 2}}]),
        );

        let mut bound: Bound<i32> = Bound::Unbounded;
        let mut ob = bound.__observe();
        *ob.tracked_mut() = Bound::Included(1);
        *ob.tracked_mut() = Bound::Unbounded;
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn inner_change_granular() {
        let mut bound: Bound<String> = Bound::Included(String::from("foo"));
        let mut ob = bound.__observe();
        *ob.tracked_mut() = Bound::Included(String::from("bar"));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"Included": "foo"}, "after": {"Included": "bar"}}]),
        );
    }

    #[test]
    fn specialization() {
        let mut bound: Bound<i32> = Bound::Included(0);
        let ob: SnapshotObserver<_, _, _> = bound.__observe();
        assert_eq!(format!("{ob:?}"), "SnapshotObserver(Included(0))");

        let mut bound: Bound<&str> = Bound::Included("");
        let ob: BoundObserver<_, _, _> = bound.__observe();
        assert_eq!(format!("{ob:?}"), r#"BoundObserver(Included(""))"#);
    }

    #[test]
    fn relocate_provenance_mut() {
        let mut vec = vec![Bound::Included(String::from("hello"))];
        let mut ob = vec.__observe();
        *ob[0].tracked_mut() = Bound::Excluded(String::from("world"));
        ob.reserve(10); // force reallocation, triggers relocate
        *ob[0].tracked_mut() = Bound::Included(String::from("after"));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-1], "before": {"Included": "hello"}, "after": {"Included": "after"}}]),
        );
    }

    #[test]
    fn relocate_provenance_ref() {
        let mut vec = vec![Bound::Included(String::from("hello"))];
        let mut ob = vec.__observe();
        // Access element to create inner BoundObserver with inner StringObserver
        assert_eq!(
            *ob[0].untracked_ref(),
            Bound::Included(String::from("hello"))
        );
        // Flush relocates the BoundObserver with a shared-provenance pointer.
        // This would fail under Miri if relocate used .as_deref_mut() internally.
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }
}
