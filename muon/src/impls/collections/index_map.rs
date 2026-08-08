use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::Hash;
use std::iter::FusedIterator;
use std::ops::{Bound, Index, IndexMut, RangeBounds};

use cfg_version::cfg_version;
use indexmap::map::Entry;
use indexmap::{Equivalent, IndexMap, TryReserveError};
use serde::Serialize;

use crate::Observe;
use crate::general::{SerializeSnapshot, Snapshot};
use crate::helper::macros::{default_impl_ro_observe, delegate_methods};
use crate::helper::shallow::{ObserverState, SerializeObserverState, shallow_observer};
use crate::helper::{AsDerefMut, Invalidate, Pointer, QuasiObserver, Unsigned, Zero};
use crate::observe::{DefaultSpec, Flush, Observer, Sink};

enum ValueState {
    /// Key existed in the original map and was overwritten via
    /// [`insert`](IndexMapObserver::insert). Carries the serialized old
    /// value as the `Replace.before`.
    Replaced(serde_json::Value),
    /// Key is new (did not exist in the original map), added via
    /// [`insert`](IndexMapObserver::insert).
    Inserted,
    /// Key existed in the original map and was removed. Carries the
    /// serialized old value when captured at the call site.
    Deleted(Option<serde_json::Value>),
}

struct IndexMapObserverState<K, O> {
    mutated: bool,
    diff: IndexMap<K, ValueState>,
    /// Pre-write snapshot of the whole map, captured at observe time
    /// and refreshed at every flush. Serves as the `Replace.before` of
    /// a wholesale (whole-map) replace.
    snapshot: Option<serde_json::Value>,
    /// Boxed to ensure pointer stability: [`IndexMap`] rehashing moves all entries to a new
    /// allocation, which would invalidate references to inline values. [`Box`] adds a layer
    /// of indirection so that only the pointer is moved, not the observer itself.
    inner: UnsafeCell<IndexMap<K, Box<O>>>,
}

impl<K, O> Invalidate<IndexMap<K, O::Head>> for IndexMapObserverState<K, O>
where
    K: Clone + Eq + Hash,
    O: QuasiObserver<InnerDepth = Zero, Head: Sized>,
{
    fn invalidate(&mut self, map: &IndexMap<K, O::Head>) {
        if !self.mutated {
            self.mutated = true;
            for key in map.keys() {
                self.mark_deleted(key.clone());
            }
        }
        self.inner.get_mut().clear();
    }
}

impl<K, O> IndexMapObserverState<K, O>
where
    K: Eq + Hash,
    O: QuasiObserver<InnerDepth = Zero, Head: Sized>,
{
    fn mark_deleted(&mut self, key: K) {
        self.inner.get_mut().swap_remove(&key);
        match self.diff.entry(key) {
            Entry::Occupied(mut e) => {
                if matches!(e.get(), ValueState::Inserted) {
                    e.swap_remove();
                } else {
                    e.insert(ValueState::Deleted(None));
                }
            }
            Entry::Vacant(e) => {
                e.insert(ValueState::Deleted(None));
            }
        }
    }

    /// Marks a key as removed, capturing its pre-write value for the
    /// `Replace.before`.
    fn mark_deleted_value(&mut self, key: K, value: &O::Head)
    where
        O::Head: SerializeSnapshot,
    {
        self.inner.get_mut().swap_remove(&key);
        let before: Option<serde_json::Value> =
            Some(serde_json::to_value(value.to_snapshot()).expect("snapshot serializes"));
        match self.diff.entry(key) {
            Entry::Occupied(mut e) => {
                if matches!(e.get(), ValueState::Inserted) {
                    e.swap_remove();
                } else {
                    e.insert(ValueState::Deleted(before));
                }
            }
            Entry::Vacant(e) => {
                e.insert(ValueState::Deleted(before));
            }
        }
    }
}

impl<K, O> ObserverState<IndexMap<K, O::Head>> for IndexMapObserverState<K, O>
where
    K: Serialize + Clone + Eq + Hash + 'static,
    O: Observer<InnerDepth = Zero, Head: Sized>,
    O::Head: SerializeSnapshot,
{
    fn observe(map: &IndexMap<K, O::Head>) -> Self {
        Self {
            mutated: false,
            diff: Default::default(),
            snapshot: Some(serde_json::to_value(map.to_snapshot()).expect("snapshot serializes")),
            inner: Default::default(),
        }
    }
}

impl<K, O, S: Sink + ?Sized> SerializeObserverState<IndexMap<K, O::Head>, S>
    for IndexMapObserverState<K, O>
where
    K: Serialize + Clone + Eq + Hash + 'static,
    O: Observer<InnerDepth = Zero> + Flush<S>,
    O::Head: SerializeSnapshot + Sized + 'static,
{
    fn flush(&mut self, map: &IndexMap<K, O::Head>, sink: &mut S) {
        if !self.mutated {
            return self.partial_flush(map, sink);
        }
        self.mutated = false;
        self.diff.clear();
        self.inner.get_mut().clear();
        let before = self.snapshot.take();
        let after = Some(map as &dyn erased_serde::Serialize);
        self.snapshot = Some(serde_json::to_value(map.to_snapshot()).expect("snapshot serializes"));
        sink.replace(
            before.as_ref().map(|v| v as &dyn erased_serde::Serialize),
            after.as_ref().map(|v| v as &dyn erased_serde::Serialize),
        );
    }
}

impl<K, O> IndexMapObserverState<K, O>
where
    K: Serialize + Clone + Eq + Hash + 'static,
    O: Observer<InnerDepth = Zero, Head: Sized>,
    O::Head: SerializeSnapshot + Sized + 'static,
{
    fn partial_flush<S: Sink + ?Sized>(&mut self, map: &IndexMap<K, O::Head>, sink: &mut S)
    where
        O: Flush<S>,
    {
        let diff = std::mem::take(&mut self.diff);
        // Without delete support a removal cannot be expressed as a
        // per-key event: fall back to a whole-map replace before any
        // incremental event is emitted (the old snapshot is the
        // `before`). Emitting increments first and then a replace
        // would leave the stream with mismatched `before`s.
        #[cfg(not(feature = "delete"))]
        if diff.values().any(|v| matches!(v, ValueState::Deleted(_))) {
            let before = self.snapshot.take();
            self.snapshot =
                Some(serde_json::to_value(map.to_snapshot()).expect("snapshot serializes"));
            sink.replace(
                before.as_ref().map(|v| v as &dyn erased_serde::Serialize),
                Some(map as &dyn erased_serde::Serialize),
            );
            return;
        }
        let mut inner = std::mem::take(self.inner.get_mut());
        for (key, value_state) in diff {
            let key_str = match serde_json::to_value(&key).expect("key serializes") {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            };
            match value_state {
                ValueState::Deleted(before) => {
                    sink.push_field(&key_str);
                    sink.replace(
                        before.as_ref().map(|v| v as &dyn erased_serde::Serialize),
                        None,
                    );
                    sink.pop_segment();
                }
                ValueState::Replaced(before) => {
                    inner.swap_remove(&key);
                    let value = map
                        .get(&key)
                        .expect("replaced key not found in observed map");
                    sink.push_field(&key_str);
                    sink.replace(Some(&before), Some(value));
                    sink.pop_segment();
                }
                ValueState::Inserted => {
                    inner.swap_remove(&key);
                    let value = map
                        .get(&key)
                        .expect("inserted key not found in observed map");
                    sink.push_field(&key_str);
                    sink.replace(None, Some(value));
                    sink.pop_segment();
                }
            }
        }
        for (key, mut ob) in inner {
            let value = map
                .get(&key)
                .expect("observer key not found in observed map");
            unsafe { O::relocate(&mut ob, value as *const O::Head as *mut O::Head) }
            let key_str = match serde_json::to_value(&key).expect("key serializes") {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            };
            sink.push_field(&key_str);
            <O as Flush<S>>::flush(&mut ob, sink);
            sink.pop_segment();
        }
    }
}

/// Iterator produced by [`IndexMapObserver::extract_if`].
#[cfg_version(indexmap = "2.10")]
pub struct ExtractIf<'a, K, V, O, F>
where
    F: FnMut(&K, &mut V) -> bool,
{
    inner: indexmap::map::ExtractIf<'a, K, V, F>,
    state: Option<&'a mut IndexMapObserverState<K, O>>,
}

#[cfg_version(indexmap = "2.10")]
impl<K, V, O, F> Iterator for ExtractIf<'_, K, V, O, F>
where
    K: Clone + Eq + Hash,
    F: FnMut(&K, &mut V) -> bool,
    O: QuasiObserver<InnerDepth = Zero, Head = V>,
    V: SerializeSnapshot + Sized,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.inner.next()?;
        if let Some(state) = &mut self.state {
            state.mark_deleted_value(key.clone(), &value);
        }
        Some((key, value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[cfg_version(indexmap = "2.10")]
impl<K, V, O, F> FusedIterator for ExtractIf<'_, K, V, O, F>
where
    K: Clone + Eq + Hash,
    F: FnMut(&K, &mut V) -> bool,
    O: QuasiObserver<InnerDepth = Zero, Head = V>,
    V: SerializeSnapshot + Sized,
{
}

#[cfg_version(indexmap = "2.10")]
impl<K, V, O, F> Debug for ExtractIf<'_, K, V, O, F>
where
    F: FnMut(&K, &mut V) -> bool,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExtractIf").finish_non_exhaustive()
    }
}

shallow_observer! {
    /// Observer implementation for [`IndexMap<K, V>`](IndexMap).
    ///
    /// ## Limitations
    ///
    /// Most methods (e.g. [`insert`](Self::insert), [`swap_remove`](Self::swap_remove),
    /// [`get_mut`](Self::get_mut)) require `K: Clone` because the observer maintains its own
    /// [`IndexMap`] of cloned keys to track per-key observers independently of the observed map's
    /// internal storage.
    struct IndexMapObserver<K, O>(for<V> IndexMap<K, V>, IndexMapObserverState<K, O>);
}

impl<'ob, K, O, S: ?Sized, D, V> IndexMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = IndexMap<K, V>>,
    O: Observer<InnerDepth = Zero, Head = V>,
    K: Clone + Eq + Hash,
{
    delegate_methods! { untracked_mut() as IndexMap =>
        pub fn reserve(&mut self, additional: usize);
        pub fn reserve_exact(&mut self, additional: usize);
        pub fn try_reserve(&mut self, additional: usize) -> Result<(), TryReserveError>;
        pub fn try_reserve_exact(&mut self, additional: usize) -> Result<(), TryReserveError>;
        pub fn shrink_to_fit(&mut self);
        pub fn shrink_to(&mut self, min_capacity: usize);
    }

    delegate_methods! { tracked_mut() as IndexMap =>
        pub fn sort_keys(&mut self) where K: Ord;
        pub fn sort_by<F>(&mut self, cmp: F) where F: FnMut(&K, &V, &K, &V) -> Ordering;
        #[cfg_version(indexmap = "2.11")]
        pub fn sort_by_key<T, F>(&mut self, sort_key: F) where T: Ord, F: FnMut(&K, &V) -> T;
        pub fn sort_unstable_keys(&mut self) where K: Ord;
        pub fn sort_unstable_by<F>(&mut self, cmp: F) where F: FnMut(&K, &V, &K, &V) -> Ordering;
        #[cfg_version(indexmap = "2.11")]
        pub fn sort_unstable_by_key<T, F>(&mut self, sort_key: F) where T: Ord, F: FnMut(&K, &V) -> T;
        pub fn sort_by_cached_key<T, F>(&mut self, sort_key: F) where T: Ord, F: FnMut(&K, &V) -> T;
        pub fn reverse(&mut self);
        pub fn move_index(&mut self, from: usize, to: usize);
        pub fn swap_indices(&mut self, a: usize, b: usize);
    }
}

impl<'ob, K, O, S: ?Sized, D, V> IndexMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = IndexMap<K, V>>,
    O: Observer<InnerDepth = Zero, Head = V>,
    K: Clone + Eq + Hash,
{
    delegate_methods! { tracked_mut() as IndexMap =>
        #[cfg_version(indexmap = "2.2.4")]
        pub fn insert_sorted(&mut self, key: K, value: O::Head) -> (usize, Option<O::Head>) where K: Ord;
        #[cfg_version(indexmap = "2.11")]
        pub fn insert_sorted_by<F>(&mut self, key: K, value: O::Head, cmp: F) -> (usize, Option<O::Head>) where F: FnMut(&K, &O::Head, &K, &O::Head) -> Ordering;
        #[cfg_version(indexmap = "2.11")]
        pub fn insert_sorted_by_key<B, F>(&mut self, key: K, value: O::Head, sort_key: F) -> (usize, Option<O::Head>) where B: Ord, F: FnMut(&K, &O::Head) -> B;
        #[cfg_version(indexmap = "2.5")]
        pub fn insert_before(&mut self, index: usize, key: K, value: O::Head) -> (usize, Option<O::Head>);
        #[cfg_version(indexmap = "2.2.3")]
        pub fn shift_insert(&mut self, index: usize, key: K, value: O::Head) -> Option<O::Head>;
        #[cfg_version(indexmap = "2.11")]
        pub fn replace_index(&mut self, index: usize, key: K) -> Result<K, (usize, K)>;
    }
}

impl<'ob, K, O, S: ?Sized, D> IndexMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = IndexMap<K, O::Head>>,
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    K: Clone + Eq + Hash,
{
    /// See [`IndexMap::get`].
    pub fn get<Q>(&self, key: &Q) -> Option<&O>
    where
        Q: ?Sized + Hash + Equivalent<K>,
    {
        let key_cloned = (*self.ptr).as_deref().get_key_value(key)?.0.clone();
        let value = unsafe { Pointer::as_mut(&self.ptr) }
            .as_deref_mut()
            .get_mut(key)?;
        match unsafe { (*self.state.inner.get()).entry(key_cloned) } {
            Entry::Occupied(occupied) => {
                let ob = occupied.into_mut().as_mut();
                unsafe { O::relocate(ob, value) }
                Some(ob)
            }
            Entry::Vacant(vacant) => Some(vacant.insert(Box::new(unsafe { O::observe(value) }))),
        }
    }

    /// See [`IndexMap::get_index`].
    pub fn get_index(&self, index: usize) -> Option<(&K, &O)> {
        let key_cloned = (*self.ptr).as_deref().get_index(index)?.0.clone();
        let (key, value) = unsafe { Pointer::as_mut(&self.ptr) }
            .as_deref_mut()
            .get_index_mut(index)?;
        match unsafe { (*self.state.inner.get()).entry(key_cloned) } {
            Entry::Occupied(occupied) => {
                let ob = occupied.into_mut().as_mut();
                unsafe { O::relocate(ob, value) }
                Some((key, ob))
            }
            Entry::Vacant(vacant) => {
                Some((key, vacant.insert(Box::new(unsafe { O::observe(value) }))))
            }
        }
    }
}

impl<'ob, K, O, S: ?Sized, D> IndexMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = IndexMap<K, O::Head>>,
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    K: Clone + Eq + Hash,
{
    fn replacing_mut(&mut self) -> &mut IndexMap<K, O::Head> {
        self.state.inner.get_mut().clear();
        if (*self).untracked_ref().is_empty() {
            self.untracked_mut()
        } else {
            self.tracked_mut()
        }
    }

    delegate_methods! { replacing_mut() as IndexMap =>
        pub fn clear(&mut self);
    }
}

impl<'ob, K, O, S: ?Sized, D> IndexMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = IndexMap<K, O::Head>>,
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    K: Clone + Eq + Hash,
{
    fn __force_all(&mut self) -> &mut IndexMap<K, Box<O>> {
        let map = (*self.ptr).as_deref_mut();
        let inner = self.state.inner.get_mut();
        for (key, value) in map.iter_mut() {
            match inner.entry(key.clone()) {
                Entry::Occupied(occupied) => {
                    let observer = occupied.into_mut().as_mut();
                    unsafe { O::relocate(observer, value) }
                }
                Entry::Vacant(vacant) => {
                    vacant.insert(Box::new(unsafe { O::observe(value) }));
                }
            }
        }
        inner
    }

    /// See [`IndexMap::iter_mut`].
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut O)> + '_ {
        self.__force_all().iter_mut().map(|(k, v)| (k, v.as_mut()))
    }

    /// See [`IndexMap::values_mut`].
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut O> + '_ {
        self.__force_all().values_mut().map(|v| v.as_mut())
    }

    /// See [`IndexMap::truncate`].
    pub fn truncate(&mut self, len: usize) {
        if self.state.mutated {
            (*self.ptr).as_deref_mut().truncate(len);
            return;
        }
        let map = (*self.ptr).as_deref_mut();
        for key in map.keys().skip(len).cloned() {
            self.state.mark_deleted(key);
        }
        map.truncate(len);
    }

    // TODO
    /// See [`IndexMap::drain`].
    pub fn drain<R>(&mut self, range: R) -> indexmap::map::Drain<'_, K, O::Head>
    where
        R: RangeBounds<usize>,
    {
        if self.state.mutated {
            return (*self.ptr).as_deref_mut().drain(range);
        }
        let map = (*self.ptr).as_deref_mut();
        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&n) => n + 1,
            Bound::Excluded(&n) => n,
            Bound::Unbounded => map.len(),
        };
        let keys: Vec<K> = map
            .keys()
            .skip(start)
            .take(end.saturating_sub(start))
            .cloned()
            .collect();
        let drain = map.drain(range);
        for key in keys {
            self.state.mark_deleted(key);
        }
        drain
    }

    /// See [`IndexMap::extract_if`].
    #[cfg_version(indexmap = "2.10")]
    pub fn extract_if<F, R>(&mut self, range: R, pred: F) -> ExtractIf<'_, K, O::Head, O, F>
    where
        R: RangeBounds<usize>,
        F: FnMut(&K, &mut O::Head) -> bool,
    {
        let inner = (*self.ptr).as_deref_mut().extract_if(range, pred);
        let state = if self.state.mutated {
            None
        } else {
            Some(&mut self.state)
        };
        ExtractIf { inner, state }
    }

    /// See [`IndexMap::split_off`].
    pub fn split_off(&mut self, at: usize) -> IndexMap<K, O::Head> {
        if self.state.mutated {
            return self.tracked_mut().split_off(at);
        }
        let split = (*self.ptr).as_deref_mut().split_off(at);
        for key in split.keys().cloned() {
            self.state.mark_deleted(key);
        }
        split
    }

    /// See [`IndexMap::insert`].
    pub fn insert(&mut self, key: K, value: O::Head) -> Option<O::Head>
    where
        O::Head: SerializeSnapshot,
    {
        self.insert_full(key, value).1
    }

    /// See [`IndexMap::insert_full`].
    pub fn insert_full(&mut self, key: K, value: O::Head) -> (usize, Option<O::Head>)
    where
        O::Head: SerializeSnapshot,
    {
        if self.state.mutated {
            return self.tracked_mut().insert_full(key, value);
        }
        let key_cloned = key.clone();
        let (index, old_value) = (*self.ptr).as_deref_mut().insert_full(key_cloned, value);
        self.state.inner.get_mut().swap_remove(&key);
        let state = match &old_value {
            Some(old) => ValueState::Replaced(
                serde_json::to_value(old.to_snapshot()).expect("snapshot serializes"),
            ),
            None => ValueState::Inserted,
        };
        match self.state.diff.entry(key) {
            Entry::Occupied(mut e) => {
                if matches!(e.get(), ValueState::Deleted(_)) {
                    e.insert(state);
                }
            }
            Entry::Vacant(e) => {
                e.insert(state);
            }
        }
        (index, old_value)
    }

    // TODO
    /// See [`IndexMap::splice`].
    #[cfg_version(indexmap = "2.2")]
    pub fn splice<R, I>(&mut self, range: R, replace_with: I) -> std::vec::IntoIter<(K, O::Head)>
    where
        R: RangeBounds<usize>,
        I: IntoIterator<Item = (K, O::Head)>,
    {
        if self.state.mutated {
            return self
                .tracked_mut()
                .splice(range, replace_with)
                .collect::<Vec<_>>()
                .into_iter();
        }
        let map = (*self.ptr).as_deref_mut();
        let replace_with: Vec<_> = replace_with.into_iter().collect();

        // The splice may reorder keys; fall back to a whole-map replace.
        let removed: Vec<_> = map.splice(range, replace_with).collect();
        self.state.mutated = true;
        self.state.diff.clear();
        self.state.inner.get_mut().clear();

        // Mark removed keys that are no longer in the map
        for (key, _) in &removed {
            if !map.contains_key(key) {
                self.state.mark_deleted(key.clone());
            }
        }

        removed.into_iter()
    }

    /// See [`IndexMap::append`].
    #[cfg_version(indexmap = "2.4")]
    pub fn append(&mut self, other: &mut IndexMap<K, O::Head>)
    where
        O::Head: SerializeSnapshot,
    {
        self.extend(other.drain(..))
    }

    /// See [`IndexMap::get_mut`].
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut O>
    where
        Q: Equivalent<K> + Hash + ?Sized,
    {
        self.get_full_mut(key).map(|(_, _, v)| v)
    }

    /// See [`IndexMap::get_key_value_mut`].
    pub fn get_key_value_mut<Q>(&mut self, key: &Q) -> Option<(&K, &mut O)>
    where
        Q: Equivalent<K> + Hash + ?Sized,
    {
        self.get_full_mut(key).map(|(_, k, v)| (k, v))
    }

    /// See [`IndexMap::get_full_mut`].
    pub fn get_full_mut<Q>(&mut self, key: &Q) -> Option<(usize, &K, &mut O)>
    where
        Q: Equivalent<K> + Hash + ?Sized,
    {
        let key_cloned = (*self.ptr).as_deref().get_full(key)?.1.clone();
        let (index, key, value) = (*self.ptr).as_deref_mut().get_full_mut(key)?;
        match self.state.inner.get_mut().entry(key_cloned) {
            Entry::Occupied(occupied) => {
                let ob = occupied.into_mut().as_mut();
                unsafe { O::relocate(ob, value) }
                Some((index, key, ob))
            }
            Entry::Vacant(vacant) => Some((
                index,
                key,
                vacant.insert(Box::new(unsafe { O::observe(value) })),
            )),
        }
    }

    // TODO: get_disjoint_mut

    /// See [`IndexMap::swap_remove`].
    pub fn swap_remove<Q>(&mut self, key: &Q) -> Option<O::Head>
    where
        Q: ?Sized + Hash + Equivalent<K>,
        O::Head: SerializeSnapshot,
    {
        self.swap_remove_full(key).map(|(_, _, v)| v)
    }

    /// See [`IndexMap::swap_remove_entry`].
    pub fn swap_remove_entry<Q>(&mut self, key: &Q) -> Option<(K, O::Head)>
    where
        Q: ?Sized + Hash + Equivalent<K>,
        O::Head: SerializeSnapshot,
    {
        self.swap_remove_full(key).map(|(_, k, v)| (k, v))
    }

    /// See [`IndexMap::swap_remove_full`].
    pub fn swap_remove_full<Q>(&mut self, key: &Q) -> Option<(usize, K, O::Head)>
    where
        Q: ?Sized + Hash + Equivalent<K>,
        O::Head: SerializeSnapshot,
    {
        if self.state.mutated {
            return self.tracked_mut().swap_remove_full(key);
        }
        let (index, key, old_value) = (*self.ptr).as_deref_mut().swap_remove_full(key)?;
        self.state.mark_deleted_value(key.clone(), &old_value);
        Some((index, key, old_value))
    }

    /// See [`IndexMap::shift_remove`].
    pub fn shift_remove<Q>(&mut self, key: &Q) -> Option<O::Head>
    where
        Q: ?Sized + Hash + Equivalent<K>,
        O::Head: SerializeSnapshot,
    {
        self.shift_remove_full(key).map(|(_, _, v)| v)
    }

    /// See [`IndexMap::shift_remove_entry`].
    pub fn shift_remove_entry<Q>(&mut self, key: &Q) -> Option<(K, O::Head)>
    where
        Q: ?Sized + Hash + Equivalent<K>,
        O::Head: SerializeSnapshot,
    {
        self.shift_remove_full(key).map(|(_, k, v)| (k, v))
    }

    /// See [`IndexMap::shift_remove_full`].
    pub fn shift_remove_full<Q>(&mut self, key: &Q) -> Option<(usize, K, O::Head)>
    where
        Q: ?Sized + Hash + Equivalent<K>,
        O::Head: SerializeSnapshot,
    {
        if self.state.mutated {
            return self.tracked_mut().shift_remove_full(key);
        }
        let (index, key, old_value) = (*self.ptr).as_deref_mut().shift_remove_full(key)?;
        self.state.mark_deleted_value(key.clone(), &old_value);
        Some((index, key, old_value))
    }

    /// See [`IndexMap::pop`].
    pub fn pop(&mut self) -> Option<(K, O::Head)>
    where
        O::Head: SerializeSnapshot,
    {
        if self.state.mutated {
            return self.tracked_mut().pop();
        }
        let (key, old_value) = (*self.ptr).as_deref_mut().pop()?;
        self.state.mark_deleted_value(key.clone(), &old_value);
        Some((key, old_value))
    }

    /// See [`IndexMap::retain`].
    #[cfg_version(indexmap = "2.10")]
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&K, &mut O::Head) -> bool,
        O::Head: SerializeSnapshot,
    {
        self.extract_if(.., |k, v| !f(k, v)).for_each(drop);
    }

    // TODO: as_mut_slice

    /// See [`IndexMap::get_index_mut`].
    pub fn get_index_mut(&mut self, index: usize) -> Option<(&K, &mut O)> {
        let key_cloned = (*self.ptr).as_deref().get_index(index)?.0.clone();
        let (key, value) = (*self.ptr).as_deref_mut().get_index_mut(index)?;
        match self.state.inner.get_mut().entry(key_cloned) {
            Entry::Occupied(occupied) => {
                let ob = occupied.into_mut().as_mut();
                unsafe { O::relocate(ob, value) }
                Some((key, ob))
            }
            Entry::Vacant(vacant) => {
                Some((key, vacant.insert(Box::new(unsafe { O::observe(value) }))))
            }
        }
    }

    // TODO: get_index_entry
    // TODO: get_disjoint_indices_mut
    // TODO: get_range_mut

    /// See [`IndexMap::first_mut`].
    pub fn first_mut(&mut self) -> Option<(&K, &mut O)> {
        self.get_index_mut(0)
    }

    // TODO: first_entry

    /// See [`IndexMap::last_mut`].
    pub fn last_mut(&mut self) -> Option<(&K, &mut O)> {
        let last = (*self.ptr).as_deref().len().checked_sub(1)?;
        self.get_index_mut(last)
    }

    // TODO: last_entry

    /// See [`IndexMap::swap_remove_index`].
    pub fn swap_remove_index(&mut self, index: usize) -> Option<(K, O::Head)>
    where
        O::Head: SerializeSnapshot,
    {
        if self.state.mutated {
            return self.tracked_mut().swap_remove_index(index);
        }
        let (key, old_value) = (*self.ptr).as_deref_mut().swap_remove_index(index)?;
        self.state.mark_deleted_value(key.clone(), &old_value);
        Some((key, old_value))
    }

    /// See [`IndexMap::shift_remove_index`].
    pub fn shift_remove_index(&mut self, index: usize) -> Option<(K, O::Head)>
    where
        O::Head: SerializeSnapshot,
    {
        if self.state.mutated {
            return self.tracked_mut().shift_remove_index(index);
        }
        let (key, old_value) = (*self.ptr).as_deref_mut().shift_remove_index(index)?;
        self.state.mark_deleted_value(key.clone(), &old_value);
        Some((key, old_value))
    }
}

impl<'ob, 'q, K, O, S: ?Sized, D, V, Q: ?Sized> Index<&'q Q> for IndexMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = IndexMap<K, V>>,
    O: Observer<InnerDepth = Zero, Head = V>,
    K: Clone + Eq + Hash,
    Q: Hash + Equivalent<K>,
{
    type Output = O;

    fn index(&self, index: &'q Q) -> &Self::Output {
        self.get(index).expect("no entry found for key")
    }
}

impl<'ob, 'q, K, O, S: ?Sized, D, V, Q: ?Sized> IndexMut<&'q Q>
    for IndexMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = IndexMap<K, V>>,
    O: Observer<InnerDepth = Zero, Head = V>,
    K: Clone + Eq + Hash,
    Q: Hash + Equivalent<K>,
{
    fn index_mut(&mut self, index: &'q Q) -> &mut Self::Output {
        self.get_mut(index).expect("no entry found for key")
    }
}

impl<'ob, K, O, S: ?Sized, D> Index<usize> for IndexMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = IndexMap<K, O::Head>>,
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    K: Clone + Eq + Hash,
{
    type Output = O;

    fn index(&self, index: usize) -> &Self::Output {
        self.get_index(index).expect("index out of bounds").1
    }
}

impl<'ob, K, O, S: ?Sized, D> IndexMut<usize> for IndexMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = IndexMap<K, O::Head>>,
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    K: Clone + Eq + Hash,
{
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.get_index_mut(index).expect("index out of bounds").1
    }
}

// TODO: this inserts elements one by one, which is much slower than `IndexMap::extend`.
// Consider a bulk-insert approach that updates `diff` in one pass.
impl<'ob, K, O, S: ?Sized, D> Extend<(K, O::Head)> for IndexMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = IndexMap<K, O::Head>>,
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized + SerializeSnapshot,
    K: Clone + Eq + Hash,
{
    fn extend<I: IntoIterator<Item = (K, O::Head)>>(&mut self, iter: I) {
        let iter = iter.into_iter();
        let additional = if (*self).untracked_ref().is_empty() {
            iter.size_hint().0
        } else {
            iter.size_hint().0.div_ceil(2)
        };
        self.reserve(additional);
        for (key, value) in iter {
            self.insert(key, value);
        }
    }
}

impl<'ob, 'a, K, O, S: ?Sized, D> Extend<(&'a K, &'a O::Head)> for IndexMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = IndexMap<K, O::Head>>,
    O: Observer<InnerDepth = Zero>,
    O::Head: Copy,
    K: Copy + Eq + Hash,
    O::Head: SerializeSnapshot,
{
    fn extend<I: IntoIterator<Item = (&'a K, &'a O::Head)>>(&mut self, iter: I) {
        self.extend(iter.into_iter().map(|(&key, &value)| (key, value)));
    }
}

impl<K: Serialize + Clone + Eq + Hash + 'static, V: Observe + SerializeSnapshot + 'static> Observe
    for IndexMap<K, V>
{
    type Observer<'ob, S, D>
        = IndexMapObserver<'ob, K, V::Observer<'ob, V, Zero>, S, D>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

    type Spec = DefaultSpec;
}

default_impl_ro_observe! {
    impl [K, V] RoObserve for IndexMap<K, V>;
}

impl<K, V> Snapshot for IndexMap<K, V>
where
    K: Clone + Eq + Hash,
    V: Snapshot,
{
    type Snapshot = Box<[(K, V::Snapshot)]>;

    fn to_snapshot(&self) -> Self::Snapshot {
        self.iter()
            .map(|(k, v)| (k.clone(), v.to_snapshot()))
            .collect()
    }
}

impl<K, V> SerializeSnapshot for IndexMap<K, V>
where
    K: Serialize + Clone + Eq + Hash + 'static,
    V: SerializeSnapshot,
    Self: Serialize,
    Self::Snapshot: serde::Serialize + 'static,
{
    fn flush<S: Sink + ?Sized>(&self, snapshot: Self::Snapshot, sink: &mut S) {
        let mut snapshot: Vec<(K, V::Snapshot)> = snapshot.into_vec();
        // Without delete support a removal cannot be expressed as a
        // per-key event: fall back to a whole-map replace before any
        // incremental event is emitted, so the `before` (the old
        // snapshot) matches the whole-map `after` and the segment
        // stack stays balanced.
        #[cfg(not(feature = "delete"))]
        if snapshot.iter().any(|(k, _)| !self.contains_key(k)) {
            sink.replace(
                Some(&serde_json::to_value(&snapshot).expect("snapshot serializes")),
                Some(self),
            );
            return;
        }
        for (k, v) in self.iter() {
            let key_str = match serde_json::to_value(k).expect("key serializes") {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            };
            sink.push_field(&key_str);
            if let Some(pos) = snapshot.iter().position(|(sk, _)| sk == k) {
                let (_, s) = snapshot.swap_remove(pos);
                SerializeSnapshot::flush(&v, s, sink);
            } else {
                sink.replace(None, Some(v));
            }
            sink.pop_segment();
        }
        for (k, s) in snapshot {
            let key_str = match serde_json::to_value(&k).expect("key serializes") {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            };
            sink.push_field(&key_str);
            sink.replace(Some(&s), None);
            sink.pop_segment();
        }
    }
}

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;
    use muon_test_utils::*;
    use serde_json::json;

    use crate::helper::QuasiObserver;
    use crate::observe::ObserveExt;

    #[test]
    fn remove_nonexistent_key() {
        let mut map = IndexMap::from([("a", "x".to_string())]);
        let mut ob = map.__observe();
        assert_eq!(ob.shift_remove("nonexistent"), None);
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn insert_then_remove() {
        let mut map = IndexMap::from([("a", "x".to_string())]);
        let mut ob = map.__observe();
        assert_eq!(ob.insert("b", "y".to_string()), None);
        assert_eq!(ob.shift_remove("b"), Some("y".to_string()));
        assert_eq!(ob.untracked_ref().len(), 1);
        assert_eq!(ob.untracked_ref().get("a"), Some(&"x".to_string()));
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn remove_then_insert() {
        let mut map = IndexMap::from([("a", "x".to_string())]);
        let mut ob = map.__observe();
        assert_eq!(ob.shift_remove("a"), Some("x".to_string()));
        assert_eq!(ob.insert("a", "y".to_string()), None);
        assert_eq!(ob.untracked_ref().get("a"), Some(&"y".to_string()));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["a"], "before": null, "after": "y"}]),
        );
    }

    #[test]
    fn swap_remove() {
        let mut map = IndexMap::from([
            ("a", "x".to_string()),
            ("b", "y".to_string()),
            ("c", "z".to_string()),
        ]);
        let mut ob = map.__observe();
        // swap_remove "a" swaps it with the last element "c"
        assert_eq!(ob.swap_remove("a"), Some("x".to_string()));
        assert_eq!(ob.untracked_ref().len(), 2);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["a"], "before": "x", "after": null}]),
        );
    }

    #[test]
    fn shift_remove_entry() {
        let mut map = IndexMap::from([("a", "x".to_string()), ("b", "y".to_string())]);
        let mut ob = map.__observe();
        assert_eq!(ob.shift_remove_entry("a"), Some(("a", "x".to_string())));
        assert_eq!(ob.untracked_ref().len(), 1);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["a"], "before": "x", "after": null}]),
        );
    }

    #[test]
    fn retain() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2), ("c", 3)]);
        let mut ob = map.__observe();
        ob.retain(|_, v| *v % 2 != 0);
        assert_eq!(ob.untracked_ref(), &IndexMap::from([("a", 1), ("c", 3)]));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["b"], "before": 2, "after": null}]),
        );
    }

    #[test]
    fn extend() {
        let mut map = IndexMap::from([("a", "x".to_string())]);
        let mut ob = map.__observe();
        ob.extend([("b", "y".to_string()), ("c", "z".to_string())]);
        assert_eq!(ob.untracked_ref().len(), 3);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["b"], "before": null, "after": "y"},
                {"path": ["c"], "before": null, "after": "z"},
            ]),
        );
    }

    #[test]
    fn get_mut_then_insert() {
        let mut map = IndexMap::from([("a", "x".to_string())]);
        let mut ob = map.__observe();
        ob.get_mut("a").unwrap().push_str(" world");
        ob.insert("a", "bye".to_string());
        assert_eq!(ob.untracked_ref().get("a"), Some(&"bye".to_string()));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["a"], "before": "x world", "after": "bye"}]),
        );
    }

    #[test]
    fn insert_then_get_mut() {
        let mut map = IndexMap::from([("a", "x".to_string())]);
        let mut ob = map.__observe();
        ob.insert("b", "hello".to_string());
        ob.get_mut("b").unwrap().push_str(" world");
        assert_eq!(
            ob.untracked_ref().get("b"),
            Some(&"hello world".to_string())
        );
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["b"], "before": null, "after": "hello world"}]),
        );
    }

    #[test]
    fn iter_mut() {
        let mut map = IndexMap::from([("a", "x".to_string()), ("b", "y".to_string())]);
        let mut ob = map.__observe();
        for (_, v) in ob.iter_mut() {
            v.push_str("!");
        }
        assert_eq!(ob.untracked_ref().get("a"), Some(&"x!".to_string()));
        assert_eq!(ob.untracked_ref().get("b"), Some(&"y!".to_string()));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["a"], "before": "x", "after": "x!"},
                {"path": ["b"], "before": "y", "after": "y!"},
            ]),
        );
    }

    #[test]
    fn pop() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2), ("c", 3)]);
        let mut ob = map.__observe();
        assert_eq!(ob.pop(), Some(("c", 3)));
        assert_eq!(ob.untracked_ref(), &IndexMap::from([("a", 1), ("b", 2)]));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["c"], "before": 3, "after": null}]),
        );
    }

    #[test]
    fn insert_then_pop() {
        let mut map: IndexMap<&str, i32> = IndexMap::new();
        let mut ob = map.__observe();
        ob.insert("a", 1);
        ob.insert("b", 2);
        assert_eq!(ob.pop(), Some(("b", 2)));
        // "b" was inserted then popped: net no-op
        // "a" was inserted: Inserted
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["a"], "before": null, "after": 1}]),
        );
    }

    #[test]
    fn extract_if() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2), ("c", 3), ("d", 4)]);
        let mut ob = map.__observe();
        let extracted: IndexMap<_, _> = ob.extract_if(.., |_, v| *v % 2 == 0).collect();
        assert_eq!(extracted, IndexMap::from([("b", 2), ("d", 4)]));
        assert_eq!(ob.untracked_ref(), &IndexMap::from([("a", 1), ("c", 3)]));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["b"], "before": 2, "after": null},
                {"path": ["d"], "before": 4, "after": null},
            ]),
        );
    }

    #[test]
    fn extract_if_insert_then_extract() {
        let mut map = IndexMap::from([("a", 1i32)]);
        let mut ob = map.__observe();
        ob.insert("b", 2);
        // extract "b" which was just inserted: net no-op
        let extracted: IndexMap<_, _> = ob.extract_if(.., |k, _| *k == "b").collect();
        assert_eq!(extracted, IndexMap::from([("b", 2)]));
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn extract_if_with_range() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2), ("c", 3), ("d", 4)]);
        let mut ob = map.__observe();
        // Only extract from indices 1..3 ("b" and "c")
        let extracted: IndexMap<_, _> = ob.extract_if(1..3, |_, _| true).collect();
        assert_eq!(extracted, IndexMap::from([("b", 2), ("c", 3)]));
        assert_eq!(ob.untracked_ref(), &IndexMap::from([("a", 1), ("d", 4)]));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["b"], "before": 2, "after": null},
                {"path": ["c"], "before": 3, "after": null},
            ]),
        );
    }

    #[test]
    fn index_by_usize() {
        let mut map = IndexMap::from([("a", "x".to_string()), ("b", "y".to_string())]);
        let ob = map.__observe();
        assert_eq!(ob[0].untracked_ref(), "x");
        assert_eq!(ob[1].untracked_ref(), "y");
    }

    #[test]
    fn index_mut_by_usize() {
        let mut map = IndexMap::from([("a", "x".to_string()), ("b", "y".to_string())]);
        let mut ob = map.__observe();
        ob[0].push_str("!");
        ob[1].push_str("?");
        assert_eq!(ob.untracked_ref().get("a"), Some(&"x!".to_string()));
        assert_eq!(ob.untracked_ref().get("b"), Some(&"y?".to_string()));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["a"], "before": "x", "after": "x!"},
                {"path": ["b"], "before": "y", "after": "y?"},
            ]),
        );
    }

    #[test]
    fn values_mut() {
        let mut map = IndexMap::from([("a", "hello".to_string()), ("b", "world".to_string())]);
        let mut ob = map.__observe();
        for v in ob.values_mut() {
            v.push('~');
        }
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["a"], "before": "hello", "after": "hello~"},
                {"path": ["b"], "before": "world", "after": "world~"},
            ]),
        );
    }

    #[test]
    fn truncate() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2), ("c", 3), ("d", 4)]);
        let mut ob = map.__observe();
        ob.truncate(2);
        assert_eq!(ob.untracked_ref(), &IndexMap::from([("a", 1), ("b", 2)]));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["c"], "before": null, "after": null},
                {"path": ["d"], "before": null, "after": null},
            ]),
        );
    }

    #[test]
    fn truncate_noop() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        ob.truncate(5); // len is 2, truncating to 5 is a no-op
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn drain() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2), ("c", 3), ("d", 4)]);
        let mut ob = map.__observe();
        let drained: IndexMap<_, _> = ob.drain(1..3).collect();
        assert_eq!(drained, IndexMap::from([("b", 2), ("c", 3)]));
        assert_eq!(ob.untracked_ref(), &IndexMap::from([("a", 1), ("d", 4)]));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["b"], "before": null, "after": null},
                {"path": ["c"], "before": null, "after": null},
            ]),
        );
    }

    #[test]
    fn drain_all() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        let drained: Vec<_> = ob.drain(..).collect();
        assert_eq!(drained, vec![("a", 1), ("b", 2)]);
        assert!(ob.untracked_ref().is_empty());
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["a"], "before": null, "after": null},
                {"path": ["b"], "before": null, "after": null},
            ]),
        );
    }

    #[test]
    fn append_from_other() {
        let mut map = IndexMap::from([("a", "x".to_string())]);
        let mut ob = map.__observe();
        let mut other = IndexMap::from([("b", "y".to_string()), ("c", "z".to_string())]);
        ob.append(&mut other);
        assert!(other.is_empty());
        assert_eq!(ob.untracked_ref().len(), 3);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["b"], "before": null, "after": "y"},
                {"path": ["c"], "before": null, "after": "z"},
            ]),
        );
    }

    #[test]
    fn split_off() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2), ("c", 3)]);
        let mut ob = map.__observe();
        let split = ob.split_off(1);
        assert_eq!(split, IndexMap::from([("b", 2), ("c", 3)]));
        assert_eq!(ob.untracked_ref(), &IndexMap::from([("a", 1)]));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["b"], "before": null, "after": null},
                {"path": ["c"], "before": null, "after": null},
            ]),
        );
    }

    #[test]
    fn swap_remove_full() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2), ("c", 3)]);
        let mut ob = map.__observe();
        assert_eq!(ob.swap_remove_full("b"), Some((1, "b", 2)));
        assert_eq!(ob.untracked_ref().len(), 2);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["b"], "before": 2, "after": null}]),
        );
    }

    #[test]
    fn shift_remove_full() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2), ("c", 3)]);
        let mut ob = map.__observe();
        assert_eq!(ob.shift_remove_full("a"), Some((0, "a", 1)));
        assert_eq!(ob.untracked_ref().len(), 2);
        // Order preserved: b, c
        assert_eq!(ob.untracked_ref().get_index(0), Some((&"b", &2)));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["a"], "before": 1, "after": null}]),
        );
    }

    #[test]
    fn get_full_mut() {
        let mut map = IndexMap::from([("a", "x".to_string()), ("b", "y".to_string())]);
        let mut ob = map.__observe();
        let (index, key, value) = ob.get_full_mut("b").unwrap();
        assert_eq!(index, 1);
        assert_eq!(*key, "b");
        value.push_str("!");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["b"], "before": "y", "after": "y!"}]),
        );
    }

    #[test]
    fn first_mut() {
        let mut map = IndexMap::from([("a", "x".to_string()), ("b", "y".to_string())]);
        let mut ob = map.__observe();
        let (key, value) = ob.first_mut().unwrap();
        assert_eq!(*key, "a");
        value.push_str("!");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["a"], "before": "x", "after": "x!"}]),
        );
    }

    #[test]
    fn last_mut() {
        let mut map = IndexMap::from([("a", "x".to_string()), ("b", "y".to_string())]);
        let mut ob = map.__observe();
        let (key, value) = ob.last_mut().unwrap();
        assert_eq!(*key, "b");
        value.push_str("!");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["b"], "before": "y", "after": "y!"}]),
        );
    }

    #[test]
    fn last_mut_empty() {
        let mut map: IndexMap<&str, String> = IndexMap::new();
        let mut ob = map.__observe();
        assert!(ob.last_mut().is_none());
    }

    #[test]
    fn splice_replace_range() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2), ("c", 3), ("d", 4)]);
        let mut ob = map.__observe();
        let removed: Vec<_> = ob.splice(1..3, [("x", 10), ("y", 20)]).collect();
        assert_eq!(removed, vec![("b", 2), ("c", 3)]);
        // Final order: a, x, y, d
        assert_eq!(ob.untracked_ref().get("a"), Some(&1));
        assert_eq!(ob.untracked_ref().get("x"), Some(&10));
        assert_eq!(ob.untracked_ref().get("y"), Some(&20));
        assert_eq!(ob.untracked_ref().get("d"), Some(&4));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{
                "path": [],
                "before": [["a", 1], ["b", 2], ["c", 3], ["d", 4]],
                "after": {"a": 1, "x": 10, "y": 20, "d": 4},
            }]),
        );
    }

    #[test]
    fn splice_reinsert_key() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2), ("c", 3)]);
        let mut ob = map.__observe();
        // Remove "b" and re-insert "b" with a new value
        let removed: Vec<_> = ob.splice(1..2, [("b", 20)]).collect();
        assert_eq!(removed, vec![("b", 2)]);
        assert_eq!(ob.untracked_ref().get("b"), Some(&20));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{
                "path": [],
                "before": [["a", 1], ["b", 2], ["c", 3]],
                "after": {"a": 1, "b": 20, "c": 3},
            }]),
        );
    }

    #[test]
    fn extend_ref() {
        let mut map = IndexMap::from([("a", 1i32)]);
        let mut ob = map.__observe();
        ob.extend([(&"b", &2), (&"c", &3)]);
        assert_eq!(ob.untracked_ref().len(), 3);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["b"], "before": null, "after": 2},
                {"path": ["c"], "before": null, "after": 3},
            ]),
        );
    }

    #[test]
    fn flush_no_change() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn flush_deref_mut_only() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        *ob.tracked_mut() = IndexMap::from([("a", 10), ("b", 20)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [["a", 1], ["b", 2]], "after": {"a": 10, "b": 20}}]),
        );
    }

    // Inserted key, then deref_mut to a value without that key → whole-map replace
    #[test]
    fn flush_inserted_then_absent() {
        let mut map = IndexMap::from([("a", 1i32)]);
        let mut ob = map.__observe();
        ob.insert("b", 2);
        *ob.tracked_mut() = IndexMap::from([("a", 10)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [["a", 1]], "after": {"a": 10}}]),
        );
    }

    // Inserted key, then deref_mut to a value with that key → whole-map replace
    #[test]
    fn flush_inserted_then_present() {
        let mut map = IndexMap::from([("a", 1i32)]);
        let mut ob = map.__observe();
        ob.insert("b", 2);
        *ob.tracked_mut() = IndexMap::from([("a", 10), ("b", 20)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [["a", 1]], "after": {"a": 10, "b": 20}}]),
        );
    }

    // Deleted key, then deref_mut to a value without that key → whole-map replace
    #[test]
    fn flush_deleted_then_absent() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        ob.shift_remove("b");
        *ob.tracked_mut() = IndexMap::from([("a", 10)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [["a", 1], ["b", 2]], "after": {"a": 10}}]),
        );
    }

    // Deleted key, then deref_mut to a value with that key → whole-map replace
    #[test]
    fn flush_deleted_then_present() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        ob.shift_remove("b");
        *ob.tracked_mut() = IndexMap::from([("a", 10), ("b", 20)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [["a", 1], ["b", 2]], "after": {"a": 10, "b": 20}}]),
        );
    }

    // Replaced key, then deref_mut to a value without that key → whole-map replace
    #[test]
    fn flush_replaced_then_absent() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        ob.insert("b", 99);
        *ob.tracked_mut() = IndexMap::from([("a", 10)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [["a", 1], ["b", 2]], "after": {"a": 10}}]),
        );
    }

    // Replaced key, then deref_mut to a value with that key → whole-map replace
    #[test]
    fn flush_replaced_then_present() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        ob.insert("b", 99);
        *ob.tracked_mut() = IndexMap::from([("a", 10), ("b", 20)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [["a", 1], ["b", 2]], "after": {"a": 10, "b": 20}}]),
        );
    }

    // Without deref_mut, flush returns per-key changes
    #[test]
    fn flush_granular() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        ob.insert("a", 10);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["a"], "before": 1, "after": 10}]),
        );
    }

    // deref_mut replaces with entirely new keys
    #[test]
    fn flush_deref_mut_new_keys() {
        let mut map = IndexMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        *ob.tracked_mut() = IndexMap::from([("c", 30)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [["a", 1], ["b", 2]], "after": {"c": 30}}]),
        );
    }
}

#[cfg(test)]
mod snapshot_tests {
    use indexmap::IndexMap;
    use serde_json::json;

    use crate::general::Snapshot;

    #[test]
    fn no_change() {
        let map = IndexMap::from([("a", 1), ("b", 2)]);
        let snapshot = map.to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&map, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert!(changes.is_empty());
    }

    #[test]
    fn value_changed() {
        let map = IndexMap::from([("a", 1), ("b", 99), ("c", 3)]);
        let snapshot = IndexMap::from([("a", 1), ("b", 2), ("c", 3)]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&map, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["b"], "before": 2, "after": 99}]),
        );
    }

    #[test]
    fn all_values_changed() {
        let map = IndexMap::from([("a", 10), ("b", 20)]);
        let snapshot = IndexMap::from([("a", 1), ("b", 2)]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&map, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["a"], "before": 1, "after": 10},
                {"path": ["b"], "before": 2, "after": 20},
            ]),
        );
    }

    #[test]
    fn append_entries() {
        let map = IndexMap::from([("a", 1), ("b", 2), ("c", 3)]);
        let snapshot = IndexMap::from([("a", 1)]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&map, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["b"], "before": null, "after": 2},
                {"path": ["c"], "before": null, "after": 3},
            ]),
        );
    }

    #[test]
    fn truncate_entries() {
        let map = IndexMap::from([("a", 1)]);
        let snapshot = IndexMap::from([("a", 1), ("b", 2), ("c", 3)]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&map, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        // snapshot.swap_remove moves the tail entry ("c") into the removed
        // slot, so the leftover iteration order is c, b.
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["c"], "before": 3, "after": null},
                {"path": ["b"], "before": 2, "after": null},
            ]),
        );
    }

    #[test]
    fn keys_diverge() {
        let map = IndexMap::from([("a", 1), ("x", 10), ("y", 20)]);
        let snapshot = IndexMap::from([("a", 1), ("b", 2), ("c", 3), ("d", 4)]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&map, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        // Removing "a" via swap_remove swaps "d" into its slot, so the
        // leftover iteration order is d, b, c.
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["x"], "before": null, "after": 10},
                {"path": ["y"], "before": null, "after": 20},
                {"path": ["d"], "before": 4, "after": null},
                {"path": ["b"], "before": 2, "after": null},
                {"path": ["c"], "before": 3, "after": null},
            ]),
        );
    }

    #[test]
    fn append_with_value_change() {
        let map = IndexMap::from([("a", 99), ("b", 2), ("c", 3)]);
        let snapshot = IndexMap::from([("a", 1), ("b", 2)]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&map, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["a"], "before": 1, "after": 99},
                {"path": ["c"], "before": null, "after": 3},
            ]),
        );
    }
}
