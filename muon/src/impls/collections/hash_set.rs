//! Observer implementation for [`HashSet`](std::collections::HashSet).

use std::borrow::Borrow;
use std::collections::hash_set::Drain;
use std::collections::{HashSet, TryReserveError};
use std::hash::Hash;
use std::ops::{Deref, DerefMut};

use serde::Serialize;

use crate::Observe;
use crate::general::{SerializeSnapshot, Snapshot};
use crate::helper::macros::{default_impl_ro_observe, delegate_methods};
use crate::helper::shallow::ShallowState;
use crate::helper::shallow::shallow_observer;
use crate::helper::{AsDerefMut, QuasiObserver, Unsigned};
use crate::observe::{DefaultSpec, Sink};

shallow_observer! {
    /// Observer implementation for [`HashSet<T>`].
    struct HashSetObserver(for<T> HashSet<T>);
}

struct LenGuard<'a, T> {
    old_len: usize,
    state: &'a mut ShallowState<HashSet<T>>,
    inner: &'a mut HashSet<T>,
}

impl<T> Drop for LenGuard<'_, T> {
    fn drop(&mut self) {
        if self.old_len != self.inner.len() {
            self.state.mark();
        }
    }
}

impl<T> Deref for LenGuard<'_, T> {
    type Target = HashSet<T>;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<T> DerefMut for LenGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
    }
}

impl<'ob, S: ?Sized, D, T> HashSetObserver<'ob, T, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = HashSet<T>>,
{
    fn nonempty_mut(&mut self) -> &mut HashSet<T> {
        if (*self).untracked_ref().is_empty() {
            self.untracked_mut()
        } else {
            self.tracked_mut()
        }
    }

    fn guarded_mut(&mut self) -> LenGuard<'_, T> {
        let inner = (*self.ptr).as_deref_mut();
        LenGuard {
            old_len: inner.len(),
            state: &mut self.state,
            inner,
        }
    }

    delegate_methods! { nonempty_mut() as HashSet =>
        pub fn drain(&mut self) -> Drain<'_, T>;
        pub fn clear(&mut self);
    }

    delegate_methods! { guarded_mut() as HashSet =>
        pub fn retain<F>(&mut self, f: F) where F: FnMut(&T) -> bool;
    }

    /// See [`HashSet::extract_if`].
    pub fn extract_if<F>(&mut self, pred: F) -> ExtractIf<'_, T, F>
    where
        F: FnMut(&T) -> bool,
    {
        ExtractIf {
            inner: (*self.ptr).as_deref_mut().extract_if(pred),
            state: &mut self.state,
        }
    }
}

impl<'ob, S: ?Sized, D, T> HashSetObserver<'ob, T, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = HashSet<T>>,
    T: Eq + Hash,
{
    delegate_methods! { untracked_mut() as HashSet =>
        pub fn reserve(&mut self, additional: usize);
        pub fn try_reserve(&mut self, additional: usize) -> Result<(), TryReserveError>;
        pub fn shrink_to_fit(&mut self);
        pub fn shrink_to(&mut self, min_capacity: usize);
    }

    delegate_methods! { tracked_mut() as HashSet =>
        pub fn replace(&mut self, value: T) -> Option<T>;
    }

    delegate_methods! { guarded_mut() as HashSet =>
        pub fn insert(&mut self, value: T) -> bool;
        pub fn remove<Q>(&mut self, value: &Q) -> bool where T: Borrow<Q>, Q: Hash + Eq + ?Sized;
        pub fn take<Q>(&mut self, value: &Q) -> Option<T> where T: Borrow<Q>, Q: Hash + Eq + ?Sized;
    }
}

impl<'ob, S: ?Sized, D, T, U> Extend<U> for HashSetObserver<'ob, T, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = HashSet<T>>,
    HashSet<T>: Extend<U>,
{
    fn extend<I: IntoIterator<Item = U>>(&mut self, iter: I) {
        self.guarded_mut().extend(iter)
    }
}

/// Iterator produced by [`HashSetObserver::extract_if`].
pub struct ExtractIf<'a, T, F> {
    inner: std::collections::hash_set::ExtractIf<'a, T, F>,
    state: &'a mut ShallowState<HashSet<T>>,
}

impl<T, F: FnMut(&T) -> bool> Iterator for ExtractIf<'_, T, F> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        let result = self.inner.next();
        if result.is_some() {
            self.state.mark();
        }
        result
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<T: Serialize + Clone + Eq + Hash + 'static> Observe for HashSet<T> {
    type Observer<'ob, S, D>
        = HashSetObserver<'ob, T, S, D>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

    type Spec = DefaultSpec;
}

default_impl_ro_observe! {
    impl [T] RoObserve for HashSet<T>;
}

impl<T: Serialize + Clone + Eq + Hash> Snapshot for HashSet<T> {
    type Snapshot = Box<[T]>;

    fn to_snapshot(&self) -> Self::Snapshot {
        self.iter().cloned().collect()
    }
}

impl<T: Serialize + Clone + Eq + Hash + 'static> SerializeSnapshot for HashSet<T> {
    fn flush<S: Sink + ?Sized>(&self, snapshot: Self::Snapshot, sink: &mut S) {
        if !self.iter().eq(snapshot.iter()) {
            sink.replace(Some(&snapshot), Some(self))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use muon_test_utils::*;

    use crate::helper::QuasiObserver;
    use crate::observe::ObserveExt;
    use crate::{Changed, Changes};

    /// Asserts the stream is a single whole-container Replace at the root path.
    ///
    /// HashSet element order is nondeterministic across processes
    /// (RandomState), so exact before/after arrays are not asserted here;
    /// the BTreeSet tests cover the exact value shape.
    fn is_replace(changes: &Changes<()>) -> bool {
        let mut iter = changes.inner.iter();
        match iter.next() {
            Some(change) => {
                change.path.is_empty()
                    && matches!(&change.changed, Changed::Replace { .. })
                    && iter.next().is_none()
            }
            None => false,
        }
    }

    #[test]
    fn no_change() {
        let mut set = HashSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        assert!(__flush!(&mut ob).is_empty());
    }

    #[test]
    fn insert_triggers_replace() {
        let mut set = HashSet::from([1, 2]);
        let mut ob = set.__observe();
        ob.insert(3);
        let changes = __flush!(&mut ob);
        assert!(is_replace(&changes));
    }

    #[test]
    fn insert_duplicate_no_mutation() {
        let mut set = HashSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        assert!(!ob.insert(2));
        assert!(__flush!(&mut ob).is_empty());
    }

    #[test]
    fn remove_existing_triggers_replace() {
        let mut set = HashSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        assert!(ob.remove(&2));
        let changes = __flush!(&mut ob);
        assert!(is_replace(&changes));
    }

    #[test]
    fn remove_nonexistent_no_mutation() {
        let mut set = HashSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        assert!(!ob.remove(&99));
        assert!(__flush!(&mut ob).is_empty());
    }

    #[test]
    fn take_triggers_replace() {
        let mut set = HashSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        assert_eq!(ob.take(&2), Some(2));
        let changes = __flush!(&mut ob);
        assert!(is_replace(&changes));
    }

    #[test]
    fn take_nonexistent_no_mutation() {
        let mut set = HashSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        assert_eq!(ob.take(&99), None);
        assert!(__flush!(&mut ob).is_empty());
    }

    #[test]
    fn clear_empty_no_mutation() {
        let mut set: HashSet<i32> = HashSet::new();
        let mut ob = set.__observe();
        ob.clear();
        assert!(__flush!(&mut ob).is_empty());
    }

    #[test]
    fn clear_non_empty_triggers_replace() {
        let mut set = HashSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        ob.clear();
        let changes = __flush!(&mut ob);
        assert!(is_replace(&changes));
    }

    #[test]
    fn double_flush() {
        let mut set = HashSet::from([1, 2]);
        let mut ob = set.__observe();
        ob.insert(3);
        let changes = __flush!(&mut ob);
        assert!(is_replace(&changes));
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn reserve_no_mutation() {
        let mut set = HashSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        ob.reserve(100);
        assert!(__flush!(&mut ob).is_empty());
    }

    #[test]
    fn extend_triggers_replace() {
        let mut set = HashSet::from([1]);
        let mut ob = set.__observe();
        ob.extend([2, 3, 4]);
        let changes = __flush!(&mut ob);
        assert!(is_replace(&changes));
    }

    #[test]
    fn extend_duplicates_no_mutation() {
        let mut set = HashSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        ob.extend([1, 2, 3]);
        assert!(__flush!(&mut ob).is_empty());
    }

    #[test]
    fn deref_mut_triggers_replace() {
        let mut set = HashSet::from([1, 2]);
        let mut ob = set.__observe();
        *ob.tracked_mut() = HashSet::from([10, 20, 30]);
        let changes = __flush!(&mut ob);
        assert!(is_replace(&changes));
    }

    #[test]
    fn retain_triggers_replace() {
        let mut set = HashSet::from([1, 2, 3, 4]);
        let mut ob = set.__observe();
        ob.retain(|&x| x % 2 == 0);
        assert_eq!(*ob.untracked_ref(), HashSet::from([2, 4]));
        let changes = __flush!(&mut ob);
        assert!(is_replace(&changes));
    }

    #[test]
    fn retain_all_no_mutation() {
        let mut set = HashSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        ob.retain(|_| true);
        assert!(__flush!(&mut ob).is_empty());
    }

    #[test]
    fn extract_if_triggers_replace() {
        let mut set = HashSet::from([1, 2, 3, 4]);
        let mut ob = set.__observe();
        let extracted: HashSet<_> = ob.extract_if(|&x| x % 2 == 0).collect();
        assert_eq!(extracted, HashSet::from([2, 4]));
        let changes = __flush!(&mut ob);
        assert!(is_replace(&changes));
    }

    #[test]
    fn extract_if_none_no_mutation() {
        let mut set = HashSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        let extracted: Vec<_> = ob.extract_if(|_| false).collect();
        assert!(extracted.is_empty());
        assert!(__flush!(&mut ob).is_empty());
    }

    #[test]
    fn extract_if_no_consume_no_mutation() {
        let mut set = HashSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        let _ = ob.extract_if(|_| true);
        assert!(__flush!(&mut ob).is_empty());
    }

    #[test]
    fn drain_empty_no_mutation() {
        let mut set: HashSet<i32> = HashSet::new();
        let mut ob = set.__observe();
        let drained: Vec<_> = ob.drain().collect();
        assert!(drained.is_empty());
        assert!(__flush!(&mut ob).is_empty());
    }

    #[test]
    fn replace_triggers_replace() {
        let mut set = HashSet::from([1, 2, 3]);
        let mut ob = set.__observe();
        ob.replace(2);
        let changes = __flush!(&mut ob);
        assert!(is_replace(&changes));
    }
}
