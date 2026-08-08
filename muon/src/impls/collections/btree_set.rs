//! Observer implementation for [`BTreeSet`](std::collections::BTreeSet).

use std::borrow::Borrow;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::iter::FusedIterator;
use std::mem::MaybeUninit;
use std::ops::RangeBounds;

use serde::Serialize;

use crate::Observe;
use crate::general::{SerializeSnapshot, Snapshot};
use crate::helper::macros::{default_impl_ro_observe, delegate_methods};
use crate::helper::shallow::{ObserverState, SerializeObserverState, shallow_observer};
use crate::helper::{AsDeref, AsDerefMut, Invalidate, Pointer, QuasiObserver, Unsigned};
use crate::observe::{DefaultSpec, Sink};

shallow_observer! {
    /// Observer implementation for [`BTreeSet<T>`].
    ///
    /// Tracks granular mutations by maintaining a prefix boundary. Elements up to and including
    /// the boundary are unchanged from the last flush; elements beyond the boundary form the
    /// "tail" region that is re-serialized on flush.
    ///
    /// ## Limitations
    ///
    /// Most methods require `T: Clone` because the observer stores the boundary element.
    struct BTreeSetObserver<T>(BTreeSet<T>, BTreeSetObserverState<T>);
}

default_impl_ro_observe! {
    impl [T] RoObserve for BTreeSet<T>;
}

struct BTreeSetObserverState<T> {
    mutated: bool,
    snapshot: Option<serde_json::Value>,
    _marker: std::marker::PhantomData<fn(&T)>,
}

impl<T: Ord> Invalidate<BTreeSet<T>> for BTreeSetObserverState<T> {
    fn invalidate(&mut self, _: &BTreeSet<T>) {
        self.mutated = true;
    }
}

impl<T: Serialize + Clone + Ord + 'static> ObserverState<BTreeSet<T>> for BTreeSetObserverState<T> {
    fn observe(set: &BTreeSet<T>) -> Self {
        Self {
            mutated: false,
            snapshot: Some(serde_json::to_value(set.to_snapshot()).expect("snapshot serializes")),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: Serialize + Clone + Ord + 'static, S: Sink + ?Sized> SerializeObserverState<BTreeSet<T>, S>
    for BTreeSetObserverState<T>
{
    fn flush(&mut self, set: &BTreeSet<T>, sink: &mut S) {
        if !std::mem::take(&mut self.mutated) {
            return;
        }
        let before = self.snapshot.take();
        let after = Some(serde_json::to_value(set).expect("serialization cannot fail"));
        self.snapshot = Some(serde_json::to_value(set.to_snapshot()).expect("snapshot serializes"));
        sink.replace(
            before.as_ref().map(|v| v as &dyn erased_serde::Serialize),
            after.as_ref().map(|v| v as &dyn erased_serde::Serialize),
        )
    }
}

impl<'ob, T, S: ?Sized, D> BTreeSetObserver<'ob, T, S, D>
where
    T: Clone + Ord,
    D: Unsigned,
    S: AsDerefMut<D, Target = BTreeSet<T>>,
{
    fn nonempty_mut(&mut self) -> &mut BTreeSet<T> {
        if (*self).untracked_ref().is_empty() {
            self.untracked_mut()
        } else {
            self.tracked_mut()
        }
    }

    delegate_methods! { nonempty_mut() as BTreeSet =>
        pub fn clear(&mut self);
        pub fn pop_first(&mut self) -> Option<T>;
    }

    /// See [`BTreeSet::pop_last`].
    pub fn pop_last(&mut self) -> Option<T> {
        self.nonempty_mut().pop_last()
    }

    /// See [`BTreeSet::insert`].
    pub fn insert(&mut self, value: T) -> bool {
        self.tracked_mut().insert(value)
    }

    /// See [`BTreeSet::replace`].
    pub fn replace(&mut self, value: T) -> Option<T> {
        self.tracked_mut().replace(value)
    }

    /// See [`BTreeSet::remove`].
    pub fn remove<Q>(&mut self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.tracked_mut().remove(value)
    }

    /// See [`BTreeSet::take`].
    pub fn take<Q>(&mut self, value: &Q) -> Option<T>
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.tracked_mut().take(value)
    }

    /// See [`BTreeSet::retain`].
    #[rustversion::since(1.91)]
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&T) -> bool,
    {
        self.extract_if(.., |v| !f(v)).for_each(drop);
    }

    /// See [`BTreeSet::append`].
    pub fn append(&mut self, other: &mut BTreeSet<T>) {
        self.tracked_mut().append(other);
    }

    /// See [`BTreeSet::split_off`].
    pub fn split_off(&mut self, value: &T) -> BTreeSet<T> {
        self.tracked_mut().split_off(value)
    }

    /// See [`BTreeSet::extract_if`].
    #[rustversion::since(1.91)]
    pub fn extract_if<F, R>(&mut self, range: R, pred: F) -> ExtractIf<'_, 'ob, T, S, D, R, F>
    where
        R: RangeBounds<T>,
        F: FnMut(&T) -> bool,
    {
        let set = unsafe { Pointer::as_mut(&self.ptr).as_deref_mut() };
        let inner = MaybeUninit::new(set.extract_if(range, pred));
        ExtractIf {
            inner,
            ob: self,
            first_extracted: None,
        }
    }
}

impl<'ob, T, S: ?Sized, D, U> Extend<U> for BTreeSetObserver<'ob, T, S, D>
where
    T: Clone + Ord,
    D: Unsigned,
    S: AsDerefMut<D, Target = BTreeSet<T>>,
    BTreeSet<T>: Extend<U>,
{
    fn extend<I: IntoIterator<Item = U>>(&mut self, iter: I) {
        self.tracked_mut().extend(iter);
    }
}

/// Iterator produced by [`BTreeSetObserver::extract_if`].
#[rustversion::since(1.91)]
pub struct ExtractIf<'a, 'ob, T, S: ?Sized, D, R, F>
where
    T: Clone + Ord,
    D: Unsigned,
    S: AsDeref<D, Target = BTreeSet<T>>,
{
    /// Wrapped in [`MaybeUninit`] (a union) to prevent SB from deep-retagging the internal mutable
    /// references inside stdlib's [`ExtractIf`](std::collections::btree_set::ExtractIf) when
    /// [`Drop::drop`] is entered. SB does not recurse into unions during retagging, so the strongly
    /// protected Unique tag from the [`drop_in_place`](std::ptr::drop_in_place) shim won't cover
    /// those inner references, allowing subsequent [`Pointer`]-based reads of the [`BTreeSet`]
    /// after the inner iterator is dropped.
    inner: MaybeUninit<std::collections::btree_set::ExtractIf<'a, T, R, F>>,
    ob: &'a mut BTreeSetObserver<'ob, T, S, D>,
    first_extracted: Option<T>,
}

#[rustversion::since(1.91)]
impl<T, S: ?Sized, D, R, F> Drop for ExtractIf<'_, '_, T, S, D, R, F>
where
    T: Clone + Ord,
    D: Unsigned,
    S: AsDeref<D, Target = BTreeSet<T>>,
{
    fn drop(&mut self) {
        unsafe { self.inner.assume_init_drop() }
        if self.first_extracted.is_some() {
            self.ob.state.mutated = true;
        }
    }
}

#[rustversion::since(1.91)]
impl<T, S: ?Sized, D, R, F> Iterator for ExtractIf<'_, '_, T, S, D, R, F>
where
    T: Clone + Ord,
    D: Unsigned,
    S: AsDeref<D, Target = BTreeSet<T>>,
    R: RangeBounds<T>,
    F: FnMut(&T) -> bool,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let value = unsafe { self.inner.assume_init_mut() }.next()?;
        if self.first_extracted.is_none() {
            self.first_extracted = Some(value.clone());
        }
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        unsafe { self.inner.assume_init_ref() }.size_hint()
    }
}

#[rustversion::since(1.91)]
impl<T, S: ?Sized, D, R, F> FusedIterator for ExtractIf<'_, '_, T, S, D, R, F>
where
    T: Clone + Ord,
    D: Unsigned,
    S: AsDeref<D, Target = BTreeSet<T>>,
    R: RangeBounds<T>,
    F: FnMut(&T) -> bool,
{
}

#[rustversion::since(1.91)]
impl<T, S: ?Sized, D, R, F> Debug for ExtractIf<'_, '_, T, S, D, R, F>
where
    T: Clone + Ord + Debug,
    D: Unsigned,
    S: AsDeref<D, Target = BTreeSet<T>>,
    R: RangeBounds<T>,
    F: FnMut(&T) -> bool,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe { self.inner.assume_init_ref() }.fmt(f)
    }
}

impl<T: Serialize + Clone + Ord + 'static> Observe for BTreeSet<T> {
    type Observer<'ob, S, D>
        = BTreeSetObserver<'ob, T, S, D>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

    type Spec = DefaultSpec;
}

impl<T: Serialize + Clone + Ord> Snapshot for BTreeSet<T> {
    type Snapshot = Box<[T]>;

    fn to_snapshot(&self) -> Self::Snapshot {
        self.iter().cloned().collect()
    }
}

impl<T: Serialize + Clone + Ord + 'static> SerializeSnapshot for BTreeSet<T> {
    fn flush<S: Sink + ?Sized>(&self, snapshot: Self::Snapshot, sink: &mut S) {
        if !self.iter().eq(snapshot.iter()) {
            sink.replace(Some(&snapshot), Some(self))
        }
    }
}

#[cfg(test)]
mod tests {
    use muon_test_utils::*;
    use std::collections::BTreeSet;

    use serde_json::json;

    use crate::helper::QuasiObserver;
    use crate::observe::ObserveExt;

    #[test]
    fn no_change() {
        let mut set = BTreeSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        assert!(__flush!(&mut ob).is_empty());
    }

    #[test]
    fn insert_append() {
        let mut set = BTreeSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        ob.insert(4);
        ob.insert(5);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [1, 2, 3, 4, 5]}]),
        );
    }

    #[test]
    fn remove_last_as_truncate() {
        let mut set = BTreeSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        ob.remove(&3);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [1, 2]}]),
        );
    }

    #[test]
    fn remove_middle() {
        let mut set = BTreeSet::from([1, 2, 3, 4, 5]);
        let mut ob = set.__observe();
        ob.remove(&3);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3, 4, 5], "after": [1, 2, 4, 5]}]),
        );
    }

    #[test]
    fn insert_middle_then_append() {
        let mut set = BTreeSet::from([1, 3, 5]);
        let mut ob = set.__observe();
        ob.insert(2);
        ob.insert(6);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 3, 5], "after": [1, 2, 3, 5, 6]}]),
        );
    }

    #[test]
    fn clear_non_empty() {
        let mut set = BTreeSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        ob.clear();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": []}]),
        );
    }

    #[test]
    fn clear_empty() {
        let mut set: BTreeSet<i32> = BTreeSet::new();
        let mut ob = set.__observe();
        ob.clear();
        assert!(__flush!(&mut ob).is_empty());
    }

    #[test]
    fn deref_mut_triggers_replace() {
        let mut set = BTreeSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        *ob.tracked_mut() = BTreeSet::from([4, 5]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [4, 5]}]),
        );
    }

    #[test]
    fn double_flush() {
        let mut set = BTreeSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        ob.insert(4);
        let changes = __flush!(&mut ob);
        assert!(!changes.is_empty());
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn pop_first() {
        let mut set = BTreeSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        assert_eq!(ob.pop_first(), Some(1));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [2, 3]}]),
        );
    }

    #[test]
    fn pop_last_as_truncate() {
        let mut set = BTreeSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        assert_eq!(ob.pop_last(), Some(3));
        assert_eq!(ob.pop_last(), Some(2));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [1]}]),
        );
    }

    #[test]
    fn retain_noop() {
        let mut set = BTreeSet::from([1, 2, 3, 4, 5]);
        let mut ob = set.__observe();
        ob.retain(|v| *v < 10);
        assert!(__flush!(&mut ob).is_empty());
    }

    #[test]
    fn retain_truncate() {
        let mut set = BTreeSet::from([1, 2, 3, 4, 5]);
        let mut ob = set.__observe();
        ob.retain(|v| *v % 2 == 1);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3, 4, 5], "after": [1, 3, 5]}]),
        );
    }

    #[test]
    fn extract_if_drop() {
        let mut set = BTreeSet::from([1, 2, 3, 4, 5]);
        let mut ob = set.__observe();
        let mut iter = ob.extract_if(.., |v| *v % 2 == 0);
        assert_eq!(iter.next(), Some(2));
        drop(iter);
        // The unconsumed match (4) is restored to the set on drop.
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3, 4, 5], "after": [1, 3, 4, 5]}]),
        );
    }
}

#[cfg(test)]
mod snapshot_tests {
    use std::collections::BTreeSet;

    use serde_json::json;

    use crate::general::Snapshot;
    #[test]
    fn no_change() {
        let set = BTreeSet::from([1, 2, 3]);
        let snapshot = set.to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&set, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert!(changes.is_empty());
    }

    #[test]
    fn append_elements() {
        let set = BTreeSet::from([1, 2, 3, 4, 5]);
        let snapshot = BTreeSet::from([1, 2, 3]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&set, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [1, 2, 3, 4, 5]}]),
        );
    }

    #[test]
    fn truncate_elements() {
        let set = BTreeSet::from([1, 2]);
        let snapshot = BTreeSet::from([1, 2, 3, 4]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&set, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3, 4], "after": [1, 2]}]),
        );
    }

    #[test]
    fn diverge_in_middle() {
        let set = BTreeSet::from([1, 2, 10, 11]);
        let snapshot = BTreeSet::from([1, 2, 3, 4, 5]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&set, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3, 4, 5], "after": [1, 2, 10, 11]}]),
        );
    }

    #[test]
    fn all_different() {
        let set = BTreeSet::from([10, 20, 30]);
        let snapshot = BTreeSet::from([1, 2, 3]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&set, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [10, 20, 30]}]),
        );
    }

    #[test]
    fn empty_to_nonempty() {
        let set = BTreeSet::from([1, 2, 3]);
        let snapshot = BTreeSet::<i32>::new().to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&set, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [], "after": [1, 2, 3]}]),
        );
    }

    #[test]
    fn nonempty_to_empty() {
        let set = BTreeSet::<i32>::new();
        let snapshot = BTreeSet::from([1, 2, 3]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&set, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": []}]),
        );
    }
}
