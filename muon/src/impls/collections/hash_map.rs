use std::borrow::Borrow;
use std::cell::UnsafeCell;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, TryReserveError};
use std::fmt::Debug;
use std::hash::Hash;
use std::iter::FusedIterator;
use std::ops::{Index, IndexMut};

use serde::Serialize;

use crate::Observe;
use crate::general::{SerializeSnapshot, Snapshot};
use crate::helper::macros::{default_impl_ro_observe, delegate_methods};
use crate::helper::shallow::{ObserverState, SerializeObserverState, shallow_observer};
use crate::helper::{AsDerefMut, Invalidate, Pointer, QuasiObserver, Unsigned, Zero};
use crate::observe::{DefaultSpec, Flush, Observer, Sink};

enum ValueState {
    /// Key existed in the original map and was overwritten via [`insert`](HashMapObserver::insert).
    /// Carries the serialized old value as the `Replace.before`.
    Replaced(serde_json::Value),
    /// Key is new (did not exist in the original map), added via
    /// [`insert`](HashMapObserver::insert).
    Inserted,
    /// Key existed in the original map and was removed. Carries the
    /// serialized old value when captured at the call site (`None` when
    /// only the wholesale invalidation path marked it).
    Deleted(Option<serde_json::Value>),
}

struct HashMapObserverState<K, O> {
    mutated: bool,
    diff: HashMap<K, ValueState>,
    /// Pre-write snapshot of the whole map, captured at observe time and
    /// refreshed at every flush. Serves as the `Replace.before` of a
    /// wholesale (whole-map) replace.
    snapshot: Option<serde_json::Value>,
    /// Boxed to ensure pointer stability: [`HashMap`] rehashing moves all entries to a new
    /// allocation, which would invalidate references to inline values. [`Box`] adds a layer
    /// of indirection so that only the pointer is moved, not the observer itself.
    inner: UnsafeCell<HashMap<K, Box<O>>>,
}

impl<K, O> Invalidate<HashMap<K, O::Head>> for HashMapObserverState<K, O>
where
    K: Clone + Eq + Hash,
    O: QuasiObserver<InnerDepth = Zero, Head: Sized>,
{
    fn invalidate(&mut self, map: &HashMap<K, O::Head>) {
        if !self.mutated {
            self.mutated = true;
            for key in map.keys() {
                self.mark_deleted(key.clone());
            }
        }
        self.inner.get_mut().clear();
    }
}

impl<K, O> ObserverState<HashMap<K, O::Head>> for HashMapObserverState<K, O>
where
    K: Serialize + Clone + Eq + Hash + 'static,
    O: Observer<InnerDepth = Zero, Head: Sized>,
    O::Head: SerializeSnapshot,
{
    fn observe(map: &HashMap<K, O::Head>) -> Self {
        Self {
            mutated: false,
            diff: Default::default(),
            snapshot: Some(serde_json::to_value(map.to_snapshot()).expect("snapshot serializes")),
            inner: Default::default(),
        }
    }
}

impl<K, O, S: Sink + ?Sized> SerializeObserverState<HashMap<K, O::Head>, S>
    for HashMapObserverState<K, O>
where
    K: Serialize + Clone + Eq + Hash + 'static,
    O: Observer<InnerDepth = Zero> + Flush<S>,
    O::Head: SerializeSnapshot + Sized + 'static,
{
    fn flush(&mut self, map: &HashMap<K, O::Head>, sink: &mut S) {
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

impl<K, O> HashMapObserverState<K, O>
where
    K: Eq + Hash,
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

impl<K, O> HashMapObserverState<K, O>
where
    K: Serialize + Clone + Eq + Hash + 'static,
    O: Observer<InnerDepth = Zero, Head: Sized>,
    O::Head: SerializeSnapshot + Sized + 'static,
{
    fn partial_flush<S: Sink + ?Sized>(&mut self, map: &HashMap<K, O::Head>, sink: &mut S)
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

/// Iterator produced by [`HashMapObserver::extract_if`].
pub struct ExtractIf<'a, K, V, O, F>
where
    F: FnMut(&K, &mut V) -> bool,
{
    inner: std::collections::hash_map::ExtractIf<'a, K, V, F>,
    state: Option<&'a mut HashMapObserverState<K, O>>,
}

impl<K, V, O, F> Iterator for ExtractIf<'_, K, V, O, F>
where
    K: Clone + Eq + Hash,
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

impl<K, V, O, F> FusedIterator for ExtractIf<'_, K, V, O, F>
where
    K: Clone + Eq + Hash,
    F: FnMut(&K, &mut V) -> bool,
    O: Observer<InnerDepth = Zero, Head = V>,
    V: SerializeSnapshot,
{
}

impl<K, V, O, F> Debug for ExtractIf<'_, K, V, O, F>
where
    K: Debug,
    V: Debug,
    F: FnMut(&K, &mut V) -> bool,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

shallow_observer! {
    /// Observer implementation for [`HashMap<K, V>`].
    ///
    /// ## Limitations
    ///
    /// Most methods (e.g. [`insert`](Self::insert), [`remove`](Self::remove),
    /// [`get_mut`](Self::get_mut)) require `K: Clone` because the observer maintains its own
    /// [`HashMap`] of cloned keys to track per-key observers independently of the observed map's
    /// internal storage.
    struct HashMapObserver<K, O>(for<V> HashMap<K, V>, HashMapObserverState<K, O>);
}

impl<'ob, K, O, S: ?Sized, D> HashMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = HashMap<K, O::Head>>,
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    K: Clone + Eq + Hash,
{
    /// See [`HashMap::get`].
    pub fn get<Q>(&self, key: &Q) -> Option<&O>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
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
}

impl<'ob, K, O, S: ?Sized, D, V> HashMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = HashMap<K, V>>,
    O: Observer<InnerDepth = Zero, Head = V>,
    K: Clone + Eq + Hash,
{
    delegate_methods! { untracked_mut() as HashMap =>
        pub fn reserve(&mut self, additional: usize);
        pub fn try_reserve(&mut self, additional: usize) -> Result<(), TryReserveError>;
        pub fn shrink_to_fit(&mut self);
        pub fn shrink_to(&mut self, min_capacity: usize);
    }
}

impl<'ob, K, O, S: ?Sized, D> HashMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = HashMap<K, O::Head>>,
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized,
    K: Clone + Eq + Hash,
{
    fn __force_all(&mut self) -> &mut HashMap<K, Box<O>> {
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

    /// See [`HashMap::get_mut`].
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut O>
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Eq + Hash + ?Sized,
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

    /// See [`HashMap::clear`].
    pub fn clear(&mut self) {
        self.state.inner.get_mut().clear();
        if (*self).untracked_ref().is_empty() {
            self.untracked_mut().clear()
        } else {
            self.tracked_mut().clear()
        }
    }

    /// See [`HashMap::insert`].
    pub fn insert(&mut self, key: K, value: O::Head) -> Option<O::Head>
    where
        K: Eq + Hash,
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

    /// See [`HashMap::remove`].
    pub fn remove<Q>(&mut self, key: &Q) -> Option<O::Head>
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Eq + Hash + ?Sized,
        O::Head: SerializeSnapshot,
    {
        if self.state.mutated {
            return self.tracked_mut().remove(key);
        }
        let (key, old_value) = (*self.ptr).as_deref_mut().remove_entry(key)?;
        self.state.mark_deleted_value(key, &old_value);
        Some(old_value)
    }

    /// See [`HashMap::remove_entry`].
    pub fn remove_entry<Q>(&mut self, key: &Q) -> Option<(K, O::Head)>
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Eq + Hash + ?Sized,
        O::Head: SerializeSnapshot,
    {
        if self.state.mutated {
            return self.tracked_mut().remove_entry(key);
        }
        let (key, old_value) = (*self.ptr).as_deref_mut().remove_entry(key)?;
        self.state.mark_deleted_value(key.clone(), &old_value);
        Some((key, old_value))
    }

    /// See [`HashMap::retain`].
    pub fn retain<F>(&mut self, mut f: F)
    where
        K: Eq + Hash,
        F: FnMut(&K, &mut O::Head) -> bool,
        O::Head: SerializeSnapshot,
    {
        self.extract_if(|k, v| !f(k, v)).for_each(drop);
    }

    /// See [`HashMap::extract_if`].
    pub fn extract_if<F>(&mut self, pred: F) -> ExtractIf<'_, K, O::Head, O, F>
    where
        K: Eq + Hash,
        F: FnMut(&K, &mut O::Head) -> bool,
    {
        let inner = (*self.ptr).as_deref_mut().extract_if(pred);
        let state = if self.state.mutated {
            None
        } else {
            Some(&mut self.state)
        };
        ExtractIf { inner, state }
    }

    /// See [`HashMap::iter_mut`].
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut O)> + '_
    where
        K: Eq + Hash,
    {
        self.__force_all().iter_mut().map(|(k, v)| (k, v.as_mut()))
    }

    /// See [`HashMap::values_mut`].
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut O> + '_
    where
        K: Eq + Hash,
    {
        self.__force_all().values_mut().map(|v| v.as_mut())
    }
}

impl<'ob, 'q, K, O, S: ?Sized, D, V, Q: ?Sized> Index<&'q Q> for HashMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = HashMap<K, V>>,
    O: Observer<InnerDepth = Zero, Head = V>,
    K: Borrow<Q> + Clone + Eq + Hash,
    Q: Eq + Hash,
{
    type Output = O;

    fn index(&self, index: &'q Q) -> &Self::Output {
        self.get(index).expect("no entry found for key")
    }
}

impl<'ob, 'q, K, O, S: ?Sized, D, V, Q: ?Sized> IndexMut<&'q Q> for HashMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = HashMap<K, V>>,
    O: Observer<InnerDepth = Zero, Head = V>,
    K: Borrow<Q> + Clone + Eq + Hash,
    Q: Eq + Hash,
{
    fn index_mut(&mut self, index: &'q Q) -> &mut Self::Output {
        self.get_mut(index).expect("no entry found for key")
    }
}

// TODO: this inserts elements one by one, which is much slower than `HashMap::extend`. Consider a
// bulk-insert approach that updates `state` in one pass.
impl<'ob, K, O, S: ?Sized, D> Extend<(K, O::Head)> for HashMapObserver<'ob, K, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = HashMap<K, O::Head>>,
    O: Observer<InnerDepth = Zero>,
    O::Head: Sized + SerializeSnapshot,
    K: Clone + Eq + Hash,
{
    fn extend<I: IntoIterator<Item = (K, O::Head)>>(&mut self, iter: I) {
        for (key, value) in iter {
            self.insert(key, value);
        }
    }
}

impl<K: Serialize + Clone + Eq + Hash + 'static, V: Observe + SerializeSnapshot + 'static> Observe
    for HashMap<K, V>
{
    type Observer<'ob, S, D>
        = HashMapObserver<'ob, K, V::Observer<'ob, V, Zero>, S, D>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

    type Spec = DefaultSpec;
}

default_impl_ro_observe! {
    impl [K, V] RoObserve for HashMap<K, V>;
}

impl<K, V> Snapshot for HashMap<K, V>
where
    K: Clone + Eq + Hash,
    V: Snapshot,
{
    type Snapshot = HashMap<K, V::Snapshot>;

    fn to_snapshot(&self) -> Self::Snapshot {
        self.iter()
            .map(|(k, v)| (k.clone(), v.to_snapshot()))
            .collect()
    }
}

impl<K, V> SerializeSnapshot for HashMap<K, V>
where
    K: Serialize + Clone + Eq + Hash + 'static,
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
    use std::collections::HashMap;

    use muon_test_utils::*;
    use serde_json::json;

    use crate::helper::QuasiObserver;
    use crate::observe::ObserveExt;

    #[test]
    fn remove_nonexistent_key() {
        let mut map = HashMap::from([("a", "x".to_string())]);
        let mut ob = map.__observe();
        assert_eq!(ob.remove("nonexistent"), None);
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn insert_then_remove() {
        let mut map = HashMap::from([("a", "x".to_string())]);
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
        let mut map = HashMap::from([("a", "x".to_string())]);
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
    fn remove_entry() {
        let mut map = HashMap::from([("a", "x".to_string()), ("b", "y".to_string())]);
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
    fn retain() {
        let mut map = HashMap::from([("a", 1i32), ("b", 2), ("c", 3)]);
        let mut ob = map.__observe();
        ob.retain(|_, v| *v % 2 != 0);
        assert_eq!(ob.untracked_ref(), &HashMap::from([("a", 1), ("c", 3)]));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": ["b"], "before": 2, "after": null}]),
        );
    }

    #[test]
    fn extend() {
        let mut map = HashMap::from([("a", "x".to_string())]);
        let mut ob = map.__observe();
        ob.extend([("b", "y".to_string()), ("c", "z".to_string())]);
        assert_eq!(ob.untracked_ref().len(), 3);
        let changes = __flush!(&mut ob);
        assert_eq!(
            sorted_changes(changes.into_json()),
            json!([
                {"path": ["b"], "before": null, "after": "y"},
                {"path": ["c"], "before": null, "after": "z"},
            ]),
        );
    }

    #[test]
    fn extract_if() {
        let mut map = HashMap::from([("a", 1i32), ("b", 2), ("c", 3), ("d", 4)]);
        let mut ob = map.__observe();
        let extracted: HashMap<_, _> = ob.extract_if(|_, v| *v % 2 == 0).collect();
        assert_eq!(extracted, HashMap::from([("b", 2), ("d", 4)]));
        assert_eq!(ob.untracked_ref(), &HashMap::from([("a", 1), ("c", 3)]));
        let changes = __flush!(&mut ob);
        assert_eq!(
            sorted_changes(changes.into_json()),
            json!([
                {"path": ["b"], "before": 2, "after": null},
                {"path": ["d"], "before": 4, "after": null},
            ]),
        );
    }

    #[test]
    fn extract_if_insert_then_extract() {
        let mut map = HashMap::from([("a", 1i32)]);
        let mut ob = map.__observe();
        ob.insert("b", 2);
        // extract "b" which was just inserted: net no-op
        let extracted: HashMap<_, _> = ob.extract_if(|k, _| *k == "b").collect();
        assert_eq!(extracted, HashMap::from([("b", 2)]));
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn get_mut_then_insert() {
        let mut map = HashMap::from([("a", "x".to_string())]);
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
        let mut map = HashMap::from([("a", "x".to_string())]);
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
        let mut map = HashMap::from([("a", "x".to_string()), ("b", "y".to_string())]);
        let mut ob = map.__observe();
        for (_, v) in ob.iter_mut() {
            v.push_str("!");
        }
        assert_eq!(ob.untracked_ref().get("a"), Some(&"x!".to_string()));
        assert_eq!(ob.untracked_ref().get("b"), Some(&"y!".to_string()));
        let changes = __flush!(&mut ob);
        assert_eq!(
            sorted_changes(changes.into_json()),
            json!([
                {"path": ["a"], "before": "x", "after": "x!"},
                {"path": ["b"], "before": "y", "after": "y!"},
            ]),
        );
    }

    #[test]
    fn values_mut() {
        let mut map = HashMap::from([("a", "hello".to_string()), ("b", "world".to_string())]);
        let mut ob = map.__observe();
        for v in ob.values_mut() {
            v.push('~');
        }
        let changes = __flush!(&mut ob);
        assert_eq!(
            sorted_changes(changes.into_json()),
            json!([
                {"path": ["a"], "before": "hello", "after": "hello~"},
                {"path": ["b"], "before": "world", "after": "world~"},
            ]),
        );
    }

    fn sorted_changes(mut value: serde_json::Value) -> serde_json::Value {
        let array = value.as_array_mut().expect("expected a JSON array");
        array.sort_by(|a, b| a["path"].to_string().cmp(&b["path"].to_string()));
        value
    }

    #[test]
    fn flush_no_change() {
        let mut map = HashMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn flush_deref_mut_only() {
        let mut map = HashMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        *ob.tracked_mut() = HashMap::from([("a", 10), ("b", 20)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"a": 1, "b": 2}, "after": {"a": 10, "b": 20}}]),
        );
    }

    // Inserted key, then deref_mut to a value without that key → whole-map replace
    #[test]
    fn flush_inserted_then_absent() {
        let mut map = HashMap::from([("a", 1i32)]);
        let mut ob = map.__observe();
        ob.insert("b", 2);
        *ob.tracked_mut() = HashMap::from([("a", 10)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"a": 1}, "after": {"a": 10}}]),
        );
    }

    // Inserted key, then deref_mut to a value with that key → whole-map replace
    #[test]
    fn flush_inserted_then_present() {
        let mut map = HashMap::from([("a", 1i32)]);
        let mut ob = map.__observe();
        ob.insert("b", 2);
        *ob.tracked_mut() = HashMap::from([("a", 10), ("b", 20)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"a": 1}, "after": {"a": 10, "b": 20}}]),
        );
    }

    // Deleted key, then deref_mut to a value without that key → whole-map replace
    #[test]
    fn flush_deleted_then_absent() {
        let mut map = HashMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        ob.remove("b");
        *ob.tracked_mut() = HashMap::from([("a", 10)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"a": 1, "b": 2}, "after": {"a": 10}}]),
        );
    }

    // Deleted key, then deref_mut to a value with that key → whole-map replace
    #[test]
    fn flush_deleted_then_present() {
        let mut map = HashMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        ob.remove("b");
        *ob.tracked_mut() = HashMap::from([("a", 10), ("b", 20)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"a": 1, "b": 2}, "after": {"a": 10, "b": 20}}]),
        );
    }

    // Replaced key, then deref_mut to a value without that key → whole-map replace
    #[test]
    fn flush_replaced_then_absent() {
        let mut map = HashMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        ob.insert("b", 99);
        *ob.tracked_mut() = HashMap::from([("a", 10)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"a": 1, "b": 2}, "after": {"a": 10}}]),
        );
    }

    // Replaced key, then deref_mut to a value with that key → whole-map replace
    #[test]
    fn flush_replaced_then_present() {
        let mut map = HashMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        ob.insert("b", 99);
        *ob.tracked_mut() = HashMap::from([("a", 10), ("b", 20)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"a": 1, "b": 2}, "after": {"a": 10, "b": 20}}]),
        );
    }

    // Without deref_mut, flush returns per-key changes
    #[test]
    fn flush_granular() {
        let mut map = HashMap::from([("a", 1i32), ("b", 2)]);
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
        let mut map = HashMap::from([("a", 1i32), ("b", 2)]);
        let mut ob = map.__observe();
        *ob.tracked_mut() = HashMap::from([("c", 30)]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"a": 1, "b": 2}, "after": {"c": 30}}]),
        );
    }
}
