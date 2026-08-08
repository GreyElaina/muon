use std::borrow::Borrow;
use std::cell::UnsafeCell;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt::Debug;
use std::iter::FusedIterator;
use std::ops::{Index, IndexMut, RangeBounds};

use crate::Observe;
use crate::general::{SerializeSnapshot, Snapshot};
use crate::helper::macros::default_impl_ro_observe;
use crate::helper::shallow::{ObserverState, SerializeObserverState, shallow_observer};
use crate::helper::{AsDerefMut, Invalidate, Pointer, QuasiObserver, Unsigned, Zero};
use crate::observe::{DefaultSpec, Flush, Observer, Sink};
use serde::Serialize;

enum ValueState {
    /// Key existed in the original map and was overwritten via
    /// [`insert`](BTreeMapObserver::insert). Carries the serialized old
    /// value as the `Replace.before`.
    Replaced(serde_json::Value),
    /// Key is new (did not exist in the original map), added via
    /// [`insert`](BTreeMapObserver::insert).
    Inserted,
    /// Key existed in the original map and was removed. Carries the
    /// serialized old value when captured at the call site (`None` when
    /// only the wholesale invalidation path marked it).
    Deleted(Option<serde_json::Value>),
}

struct BTreeMapObserverState<K, O> {
    mutated: bool,
    diff: BTreeMap<K, ValueState>,
    /// Pre-write snapshot of the whole map, captured at observe time and
    /// refreshed at every flush. Serves as the `Replace.before` of a
    /// wholesale (whole-map) replace.
    snapshot: Option<serde_json::Value>,
    /// Boxed to ensure pointer stability: [`BTreeMap`] node splits move entries between nodes
    /// via `memcpy`, which would invalidate references to inline values. [`Box`] adds a layer
    /// of indirection so that only the pointer is moved, not the observer itself.
    inner: UnsafeCell<BTreeMap<K, Box<O>>>,
}

impl<K, O> BTreeMapObserverState<K, O>
where
    K: Ord,
    O: QuasiObserver<InnerDepth = Zero, Head: Sized>,
{
    fn mark_deleted(&mut self, key: K) {
        self.inner.get_mut().remove(&key);
        match self.diff.entry(key) {
            Entry::Occupied(mut e) => {
                if matches!(e.get(), ValueState::Inserted) {
                    e.remove();
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
        self.inner.get_mut().remove(&key);
        let before: Option<serde_json::Value> =
            Some(serde_json::to_value(value.to_snapshot()).expect("snapshot serializes"));
        match self.diff.entry(key) {
            Entry::Occupied(mut e) => {
                if matches!(e.get(), ValueState::Inserted) {
                    e.remove();
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

impl<K, O> Invalidate<BTreeMap<K, O::Head>> for BTreeMapObserverState<K, O>
where
    K: Clone + Ord,
    O: QuasiObserver<InnerDepth = Zero, Head: Sized>,
{
    fn invalidate(&mut self, map: &BTreeMap<K, O::Head>) {
        if !self.mutated {
            self.mutated = true;
            for key in map.keys() {
                self.mark_deleted(key.clone());
            }
        }
        self.inner.get_mut().clear();
    }
}

impl<K, O> ObserverState<BTreeMap<K, O::Head>> for BTreeMapObserverState<K, O>
where
    K: Serialize + Clone + Ord + 'static,
    O: Observer<InnerDepth = Zero, Head: Sized>,
    O::Head: SerializeSnapshot,
{
    fn observe(map: &BTreeMap<K, O::Head>) -> Self {
        Self {
            mutated: false,
            diff: Default::default(),
            snapshot: Some(serde_json::to_value(map.to_snapshot()).expect("snapshot serializes")),
            inner: Default::default(),
        }
    }
}

impl<K, O, S: Sink + ?Sized> SerializeObserverState<BTreeMap<K, O::Head>, S>
    for BTreeMapObserverState<K, O>
where
    K: Serialize + Clone + Ord + 'static,
    O: Observer<InnerDepth = Zero> + Flush<S>,
    O::Head: SerializeSnapshot + Sized + 'static,
{
    fn flush(&mut self, map: &BTreeMap<K, O::Head>, sink: &mut S) {
        if !self.mutated {
            return self.partial_flush(map, sink);
        }
        self.mutated = false;
        self.diff.clear();
        self.inner.get_mut().clear();
        let before = self.snapshot.take();
        self.snapshot = Some(serde_json::to_value(map.to_snapshot()).expect("snapshot serializes"));
        sink.replace(
            before.as_ref().map(|v| v as &dyn erased_serde::Serialize),
            Some(map),
        );
    }
}

impl<K, O> BTreeMapObserverState<K, O>
where
    K: Serialize + Clone + Ord + 'static,
    O: Observer<InnerDepth = Zero, Head: Sized>,
    O::Head: SerializeSnapshot + Sized + 'static,
{
    fn partial_flush<S: Sink + ?Sized>(&mut self, map: &BTreeMap<K, O::Head>, sink: &mut S)
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
                    inner.remove(&key);
                    let value = map
                        .get(&key)
                        .expect("replaced key not found in observed map");
                    sink.push_field(&key_str);
                    sink.replace(Some(&before), Some(value));
                    sink.pop_segment();
                }
                ValueState::Inserted => {
                    inner.remove(&key);
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

/// Iterator produced by [`BTreeMapObserver::extract_if`].
#[rustversion::since(1.91)]
pub struct ExtractIf<'a, K, V, O, R, F>
where
    R: RangeBounds<K>,
    F: FnMut(&K, &mut V) -> bool,
{
    inner: std::collections::btree_map::ExtractIf<'a, K, V, R, F>,
    state: Option<&'a mut BTreeMapObserverState<K, O>>,
}

#[rustversion::since(1.91)]
impl<K, V, O, R, F> Iterator for ExtractIf<'_, K, V, O, R, F>
where
    K: Clone + Ord,
    R: RangeBounds<K>,
    F: FnMut(&K, &mut V) -> bool,
    O: Observer<InnerDepth = Zero, Head = V>,
    V: SerializeSnapshot,
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

#[rustversion::since(1.91)]
impl<K, V, O, R, F> FusedIterator for ExtractIf<'_, K, V, O, R, F>
where
    K: Clone + Ord,
    R: RangeBounds<K>,
    F: FnMut(&K, &mut V) -> bool,
    O: Observer<InnerDepth = Zero, Head = V>,
    V: SerializeSnapshot,
{
}

#[rustversion::since(1.91)]
impl<K, V, O, R, F> Debug for ExtractIf<'_, K, V, O, R, F>
where
    K: Debug,
    V: Debug,
    R: RangeBounds<K>,
    F: FnMut(&K, &mut V) -> bool,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

shallow_observer! {
    /// Observer implementation for [`BTreeMap<K, V>`].
    ///
    /// ## Limitations
    ///
    /// Most methods (e.g. [`insert`](Self::insert), [`remove`](Self::remove),
    /// [`get_mut`](Self::get_mut)) require `K: Clone` because the observer maintains its own
    /// [`BTreeMap`] of cloned keys to track per-key observers independently of the observed map's
    /// internal storage.
    struct BTreeMapObserver<K, O>(for<V> BTreeMap<K, V>, BTreeMapObserverState<K, O>);
}

impl<'ob, K, O, S: ?Sized, D> BTreeMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = BTreeMap<K, O::Head>>,
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    K: Clone + Ord,
{
    /// See [`BTreeMap::get`].
    pub fn get<Q>(&self, key: &Q) -> Option<&O>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
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

    fn __force_all(&mut self) -> &mut BTreeMap<K, Box<O>> {
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

    /// See [`BTreeMap::get_mut`].
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut O>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let key_cloned = (*self.ptr).as_deref().get_key_value(key)?.0.clone();
        let value = (*self.ptr).as_deref_mut().get_mut(key)?;
        match self.state.inner.get_mut().entry(key_cloned) {
            Entry::Occupied(occupied) => {
                let ob = occupied.into_mut().as_mut();
                unsafe { O::relocate(ob, value) }
                Some(ob)
            }
            Entry::Vacant(vacant) => Some(vacant.insert(Box::new(unsafe { O::observe(value) }))),
        }
    }

    /// See [`BTreeMap::clear`].
    pub fn clear(&mut self) {
        self.state.inner.get_mut().clear();
        if (*self).untracked_ref().is_empty() {
            self.untracked_mut().clear()
        } else {
            self.tracked_mut().clear()
        }
    }

    /// See [`BTreeMap::insert`].
    pub fn insert(&mut self, key: K, value: O::Head) -> Option<O::Head>
    where
        O::Head: SerializeSnapshot,
    {
        if self.state.mutated {
            return self.tracked_mut().insert(key, value);
        }
        let key_cloned = key.clone();
        let old_value = (*self.ptr).as_deref_mut().insert(key_cloned, value);
        self.state.inner.get_mut().remove(&key);
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
        old_value
    }

    /// See [`BTreeMap::remove`].
    pub fn remove<Q>(&mut self, key: &Q) -> Option<O::Head>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
        O::Head: SerializeSnapshot,
    {
        if self.state.mutated {
            return self.tracked_mut().remove(key);
        }
        let (key, old_value) = (*self.ptr).as_deref_mut().remove_entry(key)?;
        self.state.mark_deleted_value(key, &old_value);
        Some(old_value)
    }

    /// See [`BTreeMap::remove_entry`].
    pub fn remove_entry<Q>(&mut self, key: &Q) -> Option<(K, O::Head)>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
        O::Head: SerializeSnapshot,
    {
        if self.state.mutated {
            return self.tracked_mut().remove_entry(key);
        }
        let (key, old_value) = (*self.ptr).as_deref_mut().remove_entry(key)?;
        self.state.mark_deleted_value(key.clone(), &old_value);
        Some((key, old_value))
    }

    /// See [`BTreeMap::pop_first`].
    pub fn pop_first(&mut self) -> Option<(K, O::Head)>
    where
        O::Head: SerializeSnapshot,
    {
        if self.state.mutated {
            return self.tracked_mut().pop_first();
        }
        let (key, old_value) = (*self.ptr).as_deref_mut().pop_first()?;
        self.state.mark_deleted_value(key.clone(), &old_value);
        Some((key, old_value))
    }

    /// See [`BTreeMap::pop_last`].
    pub fn pop_last(&mut self) -> Option<(K, O::Head)>
    where
        O::Head: SerializeSnapshot,
    {
        if self.state.mutated {
            return self.tracked_mut().pop_last();
        }
        let (key, old_value) = (*self.ptr).as_deref_mut().pop_last()?;
        self.state.mark_deleted_value(key.clone(), &old_value);
        Some((key, old_value))
    }

    /// See [`BTreeMap::retain`].
    #[rustversion::since(1.91)]
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&K, &mut O::Head) -> bool,
        O::Head: SerializeSnapshot,
    {
        self.extract_if(.., |k, v| !f(k, v)).for_each(drop);
    }

    /// See [`BTreeMap::append`].
    // TODO: this drains `other` into individual inserts, which is much slower than
    // `BTreeMap::append`. Consider a bulk-insert approach that updates `diff` in one pass.
    pub fn append(&mut self, other: &mut BTreeMap<K, O::Head>)
    where
        O::Head: SerializeSnapshot,
    {
        if self.state.mutated {
            return self.tracked_mut().append(other);
        }
        for (key, value) in std::mem::take(other) {
            self.insert(key, value);
        }
    }

    /// See [`BTreeMap::split_off`].
    pub fn split_off<Q>(&mut self, key: &Q) -> BTreeMap<K, O::Head>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        if self.state.mutated {
            return self.tracked_mut().split_off(key);
        }
        let split = (*self.ptr).as_deref_mut().split_off(key);
        for key in split.keys().cloned() {
            self.state.mark_deleted(key);
        }
        split
    }

    /// See [`BTreeMap::extract_if`].
    #[rustversion::since(1.91)]
    pub fn extract_if<F, R>(&mut self, range: R, pred: F) -> ExtractIf<'_, K, O::Head, O, R, F>
    where
        R: RangeBounds<K>,
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

    /// See [`BTreeMap::iter_mut`].
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut O)> + '_ {
        self.__force_all().iter_mut().map(|(k, v)| (k, v.as_mut()))
    }

    /// See [`BTreeMap::values_mut`].
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut O> + '_ {
        self.__force_all().values_mut().map(|v| v.as_mut())
    }
}

impl<'ob, 'q, K, O, S: ?Sized, D, V, Q: ?Sized> Index<&'q Q> for BTreeMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = BTreeMap<K, V>>,
    O: Observer<InnerDepth = Zero, Head = V>,
    K: Borrow<Q> + Clone + Ord,
    Q: Ord,
{
    type Output = O;

    fn index(&self, index: &'q Q) -> &Self::Output {
        self.get(index).expect("no entry found for key")
    }
}

impl<'ob, 'q, K, O, S: ?Sized, D, V, Q: ?Sized> IndexMut<&'q Q>
    for BTreeMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = BTreeMap<K, V>>,
    O: Observer<InnerDepth = Zero, Head = V>,
    K: Borrow<Q> + Clone + Ord,
    Q: Ord,
{
    fn index_mut(&mut self, index: &'q Q) -> &mut Self::Output {
        self.get_mut(index).expect("no entry found for key")
    }
}

// TODO: this inserts elements one by one, which is much slower than `BTreeMap::extend`.
// Consider a bulk-insert approach that updates `diff` in one pass.
impl<'ob, K, O, S: ?Sized, D> Extend<(K, O::Head)> for BTreeMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = BTreeMap<K, O::Head>>,
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized + SerializeSnapshot,
    K: Clone + Ord,
{
    fn extend<I: IntoIterator<Item = (K, O::Head)>>(&mut self, iter: I) {
        for (key, value) in iter {
            self.insert(key, value);
        }
    }
}

impl<K: Serialize + Clone + Ord + 'static, V: Observe + SerializeSnapshot + 'static> Observe
    for BTreeMap<K, V>
{
    type Observer<'ob, S, D>
        = BTreeMapObserver<'ob, K, V::Observer<'ob, V, Zero>, S, D>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

    type Spec = DefaultSpec;
}

default_impl_ro_observe! {
    impl [K, V] RoObserve for BTreeMap<K, V>;
}

impl<K, V> Snapshot for BTreeMap<K, V>
where
    K: Clone + Ord,
    V: Snapshot,
{
    type Snapshot = BTreeMap<K, V::Snapshot>;

    fn to_snapshot(&self) -> Self::Snapshot {
        self.iter()
            .map(|(k, v)| (k.clone(), v.to_snapshot()))
            .collect()
    }
}

impl<K, V> SerializeSnapshot for BTreeMap<K, V>
where
    K: Serialize + Clone + Ord + 'static,
    V: SerializeSnapshot,
    Self: Serialize,
    Self::Snapshot: serde::Serialize + 'static,
{
    fn flush<S: Sink + ?Sized>(&self, mut snapshot: Self::Snapshot, sink: &mut S) {
        // Without delete support a removal cannot be expressed as a
        // per-key event: fall back to a whole-map replace before any
        // incremental event is emitted, so the `before` (the old
        // snapshot) matches the whole-map `after` and the segment
        // stack stays balanced.
        #[cfg(not(feature = "delete"))]
        if snapshot.keys().any(|k| !self.contains_key(k)) {
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
            if let Some((_, s)) = snapshot.remove_entry(k) {
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
    use muon_test_utils::*;
    use std::collections::BTreeMap;

    use serde_json::json;

    use crate::helper::QuasiObserver;
    use crate::observe::ObserveExt;

    #[test]
    fn pointer_stability_across_inner_splits() {
        let mut map = BTreeMap::new();
        for i in 0..100 {
            map.insert(i, format!("value {i}"));
        }
        let ob = map.__observe();
        // Create observer for key 0
        assert_eq!(ob.get(&0).unwrap().untracked_ref(), "value 0");
        // Create many more observers, triggering node splits
        // Box<O> ensures previously created observers remain valid.
        for i in 1..100 {
            assert_eq!(ob.get(&i).unwrap().untracked_ref(), &format!("value {i}"));
        }
        // Key 0's observer is still valid thanks to Box pointer stability
        assert_eq!(ob.get(&0).unwrap().untracked_ref(), "value 0");
    }

    #[test]
    fn remove_nonexistent_key() {
        let mut map = BTreeMap::from([("a", "x".to_string())]);
        let mut ob = map.__observe();
        assert_eq!(ob.remove("nonexistent"), None);
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn insert_then_remove() {
        let mut map = BTreeMap::from([("a", "x".to_string())]);
        let mut ob = map.__observe();
        assert_eq!(ob.insert("b", "y".to_string()), None);
        assert_eq!(ob.remove("b"), Some("y".to_string()));
        assert_eq!(ob.untracked_ref().len(), 1);
        assert_eq!(ob.untracked_ref().get("a"), Some(&"x".to_string()));
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn remove_then_insert() {
        let mut map = BTreeMap::from([("a", "x".to_string())]);
        let mut ob = map.__observe();
        assert_eq!(ob.remove("a"), Some("x".to_string()));
        assert_eq!(ob.insert("a", "y".to_string()), None);
        assert_eq!(ob.untracked_ref().get("a"), Some(&"y".to_string()));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["a"], "before": null, "after": "y"}]),
        );
    }

    #[test]
    fn get_mut_refresh_across_splits() {
        let mut map = BTreeMap::new();
        map.insert("a".to_string(), "hello".to_string());
        let mut ob = map.__observe();
        // First get_mut: modify the value through the child observer
        ob.get_mut("a").unwrap().push_str(" world");
        assert_eq!(ob.untracked_ref().get("a").unwrap(), "hello world");
        // Insert many keys via untracked_mut to trigger node splits in the
        // observed BTreeMap without adding to diff.replaced
        for i in 1..100 {
            ob.untracked_mut()
                .insert(i.to_string(), format!("value {i}"));
        }
        // Second get_mut: relocate updates the child observer's stale pointer
        ob.get_mut("a").unwrap().push_str("!");
        assert_eq!(ob.untracked_ref().get("a").unwrap(), "hello world!");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["a"], "before": "hello", "after": "hello world!"}]),
        );
    }

    #[test]
    fn insert_then_get_mut() {
        let mut map = BTreeMap::from([("a", "x".to_string())]);
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
    fn get_mut_then_insert() {
        let mut map = BTreeMap::from([("a", "x".to_string())]);
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
    fn remove_entry() {
        let mut map = BTreeMap::from([("a", "x".to_string()), ("b", "y".to_string())]);
        let mut ob = map.__observe();
        assert_eq!(ob.remove_entry("a"), Some(("a", "x".to_string())));
        assert_eq!(ob.untracked_ref().len(), 1);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["a"], "before": "x", "after": null}]),
        );
    }

    #[test]
    fn pop_first_and_last() {
        let mut map = BTreeMap::from([("a", 1i32), ("b", 2), ("c", 3)]);
        let mut ob = map.__observe();
        assert_eq!(ob.pop_first(), Some(("a", 1)));
        assert_eq!(ob.pop_last(), Some(("c", 3)));
        assert_eq!(ob.untracked_ref(), &BTreeMap::from([("b", 2)]));
        let changes = __flush!(&mut ob);
        // Two deletions: "a" and "c"
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["a"], "before": 1, "after": null},
                {"path": ["c"], "before": 3, "after": null},
            ]),
        );
    }

    #[test]
    fn retain() {
        let mut map = BTreeMap::from([("a", 1i32), ("b", 2), ("c", 3)]);
        let mut ob = map.__observe();
        ob.retain(|_, v| *v % 2 != 0);
        assert_eq!(ob.untracked_ref(), &BTreeMap::from([("a", 1), ("c", 3)]));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["b"], "before": 2, "after": null}]),
        );
    }

    #[test]
    fn append_from_other() {
        let mut map = BTreeMap::from([("a", "x".to_string())]);
        let mut ob = map.__observe();
        let mut other = BTreeMap::from([("b", "y".to_string()), ("c", "z".to_string())]);
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
        let mut map = BTreeMap::from([("a", 1i32), ("b", 2), ("c", 3)]);
        let mut ob = map.__observe();
        let split = ob.split_off("b");
        assert_eq!(split, BTreeMap::from([("b", 2), ("c", 3)]));
        assert_eq!(ob.untracked_ref(), &BTreeMap::from([("a", 1)]));
        let changes = __flush!(&mut ob);
        // mark_deleted does not capture pre-write values
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["b"], "before": null, "after": null},
                {"path": ["c"], "before": null, "after": null},
            ]),
        );
    }

    #[test]
    fn extend() {
        let mut map = BTreeMap::from([("a", "x".to_string())]);
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
    fn extract_if() {
        let mut map = BTreeMap::from([("a", 1i32), ("b", 2), ("c", 3), ("d", 4)]);
        let mut ob = map.__observe();
        let extracted: BTreeMap<_, _> = ob.extract_if(.., |_, v| *v % 2 == 0).collect();
        assert_eq!(extracted, BTreeMap::from([("b", 2), ("d", 4)]));
        assert_eq!(ob.untracked_ref(), &BTreeMap::from([("a", 1), ("c", 3)]));
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
    fn extract_if_partial_drain() {
        let mut map = BTreeMap::from([("a", 1i32), ("b", 2), ("c", 3), ("d", 4)]);
        let mut ob = map.__observe();
        // Only take the first matching element, then drop the iterator.
        let first = ob.extract_if(.., |_, v| *v % 2 == 0).next();
        assert_eq!(first, Some(("b", 2)));
        // "d" matched the predicate but was never yielded, so it must be retained.
        assert_eq!(
            ob.untracked_ref(),
            &BTreeMap::from([("a", 1), ("c", 3), ("d", 4)])
        );
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["b"], "before": 2, "after": null}]),
        );
    }

    #[test]
    fn extract_if_insert_then_extract() {
        let mut map = BTreeMap::from([("a", 1i32)]);
        let mut ob = map.__observe();
        ob.insert("b", 2);
        // extract "b" which was just inserted: net no-op
        let extracted: BTreeMap<_, _> = ob.extract_if(.., |k, _| *k == "b").collect();
        assert_eq!(extracted, BTreeMap::from([("b", 2)]));
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn iter_mut() {
        let mut map = BTreeMap::from([("a", "x".to_string()), ("b", "y".to_string())]);
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
    fn values_mut() {
        let mut map = BTreeMap::from([("a", "hello".to_string()), ("b", "world".to_string())]);
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
    fn insert_then_pop() {
        let mut map: BTreeMap<&str, i32> = BTreeMap::new();
        let mut ob = map.__observe();
        ob.insert("a", 1);
        ob.insert("b", 2);
        assert_eq!(ob.pop_first(), Some(("a", 1)));
        // "a" was inserted then popped: net no-op
        // "b" was inserted: Inserted
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["b"], "before": null, "after": 2}]),
        );
    }

    #[test]
    fn flat_flush_no_change() {
        let mut map = BTreeMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn flat_flush_deref_mut_only() {
        let mut map = BTreeMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        *ob.tracked_mut() = BTreeMap::from([("a", 10), ("b", 20)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"a": 1, "b": 2}, "after": {"a": 10, "b": 20}}]),
        );
    }

    // Inserted key, then deref_mut to a value without that key -> no Delete for the inserted key
    #[test]
    fn flat_flush_inserted_then_absent() {
        let mut map = BTreeMap::from([("a", 1i32)]);
        let mut ob = map.__observe();
        ob.insert("b", 2);
        *ob.tracked_mut() = BTreeMap::from([("a", 10)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"a": 1}, "after": {"a": 10}}]),
        );
    }

    // Inserted key, then deref_mut to a value with that key -> Replace for the key
    #[test]
    fn flat_flush_inserted_then_present() {
        let mut map = BTreeMap::from([("a", 1i32)]);
        let mut ob = map.__observe();
        ob.insert("b", 2);
        *ob.tracked_mut() = BTreeMap::from([("a", 10), ("b", 20)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"a": 1}, "after": {"a": 10, "b": 20}}]),
        );
    }

    // Deleted key, then deref_mut to a value without that key -> Delete for the key
    #[test]
    fn flat_flush_deleted_then_absent() {
        let mut map = BTreeMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        ob.remove("b");
        *ob.tracked_mut() = BTreeMap::from([("a", 10)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"a": 1, "b": 2}, "after": {"a": 10}}]),
        );
    }

    // Deleted key, then deref_mut to a value with that key -> Replace (not Delete)
    #[test]
    fn flat_flush_deleted_then_present() {
        let mut map = BTreeMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        ob.remove("b");
        *ob.tracked_mut() = BTreeMap::from([("a", 10), ("b", 20)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"a": 1, "b": 2}, "after": {"a": 10, "b": 20}}]),
        );
    }

    // Replaced key, then deref_mut to a value without that key -> Delete for the key
    #[test]
    fn flat_flush_replaced_then_absent() {
        let mut map = BTreeMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        ob.insert("b", 99);
        *ob.tracked_mut() = BTreeMap::from([("a", 10)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"a": 1, "b": 2}, "after": {"a": 10}}]),
        );
    }

    // Replaced key, then deref_mut to a value with that key -> Replace
    #[test]
    fn flat_flush_replaced_then_present() {
        let mut map = BTreeMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        ob.insert("b", 99);
        *ob.tracked_mut() = BTreeMap::from([("a", 10), ("b", 20)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"a": 1, "b": 2}, "after": {"a": 10, "b": 20}}]),
        );
    }

    // Without deref_mut, flush returns granular per-key changes
    #[test]
    fn flat_flush_granular() {
        let mut map = BTreeMap::from([("a", 1i32), ("b", 2)]);
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
    fn flat_flush_deref_mut_new_keys() {
        let mut map = BTreeMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        *ob.tracked_mut() = BTreeMap::from([("c", 30)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"a": 1, "b": 2}, "after": {"c": 30}}]),
        );
    }
}

#[cfg(test)]
mod snapshot_tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use crate::general::Snapshot;

    #[test]
    fn no_change() {
        let map = BTreeMap::from([("a", 1), ("b", 2), ("c", 3)]);
        let snapshot = map.to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&map, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert!(changes.is_empty());
    }

    #[test]
    fn value_changed() {
        let map = BTreeMap::from([("a", 1), ("b", 99), ("c", 3)]);
        let snapshot = BTreeMap::from([("a", 1), ("b", 2), ("c", 3)]).to_snapshot();
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
        let map = BTreeMap::from([("a", 10), ("b", 20), ("c", 30)]);
        let snapshot = BTreeMap::from([("a", 1), ("b", 2), ("c", 3)]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&map, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["a"], "before": 1, "after": 10},
                {"path": ["b"], "before": 2, "after": 20},
                {"path": ["c"], "before": 3, "after": 30},
            ]),
        );
    }

    #[test]
    fn key_inserted() {
        let map = BTreeMap::from([("a", 1), ("b", 2), ("c", 3)]);
        let snapshot = BTreeMap::from([("a", 1), ("c", 3)]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&map, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["b"], "before": null, "after": 2}]),
        );
    }

    #[test]
    fn key_deleted() {
        let map = BTreeMap::from([("a", 1), ("c", 3)]);
        let snapshot = BTreeMap::from([("a", 1), ("b", 2), ("c", 3)]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&map, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["b"], "before": 2, "after": null}]),
        );
    }

    #[test]
    fn insert_and_delete() {
        let map = BTreeMap::from([("a", 1), ("d", 4)]);
        let snapshot = BTreeMap::from([("a", 1), ("b", 2), ("c", 3)]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&map, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["d"], "before": null, "after": 4},
                {"path": ["b"], "before": 2, "after": null},
                {"path": ["c"], "before": 3, "after": null},
            ]),
        );
    }

    #[test]
    fn value_change_with_insert() {
        let map = BTreeMap::from([("a", 10), ("b", 2), ("c", 3)]);
        let snapshot = BTreeMap::from([("a", 1), ("b", 2)]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&map, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["a"], "before": 1, "after": 10},
                {"path": ["c"], "before": null, "after": 3},
            ]),
        );
    }

    #[test]
    fn value_change_with_delete() {
        let map = BTreeMap::from([("a", 10)]);
        let snapshot = BTreeMap::from([("a", 1), ("b", 2)]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&map, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["a"], "before": 1, "after": 10},
                {"path": ["b"], "before": 2, "after": null},
            ]),
        );
    }

    #[test]
    fn all_replaced_with_insert_collapses() {
        let map = BTreeMap::from([("a", 10), ("b", 20), ("c", 30)]);
        let snapshot = BTreeMap::from([("a", 1), ("b", 2)]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&map, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        // a and b are replaced, c is new: three granular changes, no collapse
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["a"], "before": 1, "after": 10},
                {"path": ["b"], "before": 2, "after": 20},
                {"path": ["c"], "before": null, "after": 30},
            ]),
        );
    }

    #[test]
    fn all_same_keys_replaced_collapses() {
        let map = BTreeMap::from([("a", 10), ("b", 20)]);
        let snapshot = BTreeMap::from([("a", 1), ("b", 2)]).to_snapshot();
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
    fn empty_to_nonempty() {
        let map = BTreeMap::from([("a", 1), ("b", 2)]);
        let snapshot = BTreeMap::<&str, i32>::new().to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&map, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        // All new keys: one Insert per key
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["a"], "before": null, "after": 1},
                {"path": ["b"], "before": null, "after": 2},
            ]),
        );
    }

    #[test]
    fn nonempty_to_empty() {
        let map = BTreeMap::<&str, i32>::new();
        let snapshot = BTreeMap::from([("a", 1), ("b", 2)]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&map, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        // All deleted: one Delete per key
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": ["a"], "before": 1, "after": null},
                {"path": ["b"], "before": 2, "after": null},
            ]),
        );
    }

    #[test]
    fn granular_inner_mutation() {
        let map = BTreeMap::from([("a", "hello!".to_string()), ("b", "world".to_string())]);
        let snapshot =
            BTreeMap::from([("a", "hello".to_string()), ("b", "world".to_string())]).to_snapshot();
        let mut __sink = crate::observe::ObserveSink::new();
        crate::general::SerializeSnapshot::flush(&map, snapshot, &mut __sink);
        let changes = __sink.into_changes();
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["a"], "before": "hello", "after": "hello!"}]),
        );
    }
}
