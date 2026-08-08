use std::cell::UnsafeCell;
use std::collections::LinkedList;
use std::marker::PhantomData;

use crate::Observe;
use crate::general::{SerializeSnapshot, Snapshot};
use crate::helper::macros::default_impl_ro_observe;
use crate::helper::shallow::{ObserverState, SerializeObserverState, shallow_observer};
use crate::helper::{AsDerefMut, Invalidate, QuasiObserver, Unsigned, Zero};
use crate::observe::{DefaultSpec, Flush, Observer, Sink};

struct LinkedListObserverSideState<O> {
    append_len: usize,
    truncate_len: usize,
    inner: UnsafeCell<LinkedList<O>>,
}

impl<O> LinkedListObserverSideState<O> {
    fn new() -> Self {
        Self {
            append_len: 0,
            truncate_len: 0,
            inner: UnsafeCell::new(LinkedList::new()),
        }
    }
}

struct LinkedListObserverState<O> {
    front: LinkedListObserverSideState<O>,
    back: LinkedListObserverSideState<O>,
    /// Pre-write snapshot of the whole list, captured at observe time
    /// and refreshed at every flush. Serves as the `Replace.before` of
    /// a whole-list replace.
    snapshot: Option<serde_json::Value>,
}

impl<O> LinkedListObserverState<O> {
    fn mark_replace(&mut self, len: usize) {
        self.front.inner.get_mut().clear();
        self.back.inner.get_mut().clear();
        self.front.append_len = len;
        self.front.truncate_len = len;
        self.back.append_len = 0;
        self.back.truncate_len = 0;
    }
}

impl<O> Invalidate<LinkedList<O::Head>> for LinkedListObserverState<O>
where
    O: Observer<InnerDepth = Zero, Head: Sized>,
{
    fn invalidate(&mut self, list: &LinkedList<O::Head>) {
        self.mark_replace(list.len());
    }
}

impl<O> ObserverState<LinkedList<O::Head>> for LinkedListObserverState<O>
where
    O: Observer<InnerDepth = Zero, Head: Sized>,
    O::Head: SerializeSnapshot,
{
    fn observe(list: &LinkedList<O::Head>) -> Self {
        Self {
            front: LinkedListObserverSideState::new(),
            back: LinkedListObserverSideState::new(),
            snapshot: Some(serde_json::to_value(list.to_snapshot()).expect("snapshot serializes")),
        }
    }
}

impl<O, S: Sink + ?Sized> SerializeObserverState<LinkedList<O::Head>, S>
    for LinkedListObserverState<O>
where
    O: Observer<InnerDepth = Zero, Head: Sized> + Flush<S>,
    O::Head: SerializeSnapshot + 'static,
{
    fn flush(&mut self, list: &LinkedList<O::Head>, sink: &mut S) {
        let len = list.len();
        let front_append = core::mem::replace(&mut self.front.append_len, 0);
        let front_truncate = core::mem::replace(&mut self.front.truncate_len, 0);
        let back_append = core::mem::replace(&mut self.back.append_len, 0);
        let back_truncate = core::mem::replace(&mut self.back.truncate_len, 0);

        // Any removal, or an unbalanced front operation: whole-list
        // replace (the pre-write snapshot is the `before`).
        if front_append != front_truncate || front_truncate > 0 || back_truncate > 0 {
            self.front.inner.get_mut().clear();
            self.back.inner.get_mut().clear();
            let before = self.snapshot.take();
            let after = Some(serde_json::to_value(list).expect("serialization cannot fail"));
            self.snapshot =
                Some(serde_json::to_value(list.to_snapshot()).expect("snapshot serializes"));
            sink.replace(
                before.as_ref().map(|v| v as &dyn erased_serde::Serialize),
                after.as_ref().map(|v| v as &dyn erased_serde::Serialize),
            );
            return;
        }

        let bb = len - back_append;

        // Front-appended elements: no before value.
        for (i, item) in list.iter().take(front_append).enumerate() {
            sink.push_neg_index(len - i);
            sink.replace(None, Some(item));
            sink.pop_segment();
        }

        // Back-appended elements: no before value.
        for (i, item) in list.iter().rev().take(back_append).enumerate() {
            sink.push_neg_index(1 + i);
            sink.replace(None, Some(item));
            sink.pop_segment();
        }

        let front_inner = self.front.inner.get_mut();
        let back_inner = self.back.inner.get_mut();

        // Strip appended observers from each end (outermost = front of inner)
        let front_appended_obs = front_append.min(front_inner.len());
        for _ in 0..front_appended_obs {
            front_inner.pop_front();
        }
        let back_appended_obs = back_append.min(back_inner.len());
        for _ in 0..back_appended_obs {
            back_inner.pop_front();
        }

        // Remaining observers are for existing-region elements.
        // Observers may extend into the other end's appended region — truncate to existing bounds.
        let existing_count = bb - front_append;
        while front_inner.len() + back_inner.len() > existing_count {
            // Prefer trimming from the end that extended further
            if front_inner.len() >= back_inner.len() {
                front_inner.pop_back();
            } else {
                back_inner.pop_back();
            }
        }

        // Process back_inner: back_inner[j] is at absolute position len - back_append - 1 - j
        //   → neg_idx = back_append + 1 + j
        for (j, ob) in back_inner.iter_mut().enumerate() {
            sink.push_neg_index(back_append + 1 + j);
            <O as Flush<S>>::flush(ob, sink);
            sink.pop_segment();
        }

        // Process front_inner: front_inner[k] is at absolute position front_append + k
        //   → neg_idx = len - front_append - k
        for (k, ob) in front_inner.iter_mut().enumerate().rev() {
            sink.push_neg_index(len - front_append - k);
            <O as Flush<S>>::flush(ob, sink);
            sink.pop_segment();
        }

        // Refresh the whole-list snapshot so a later wholesale replace
        // reports the state after this partial flush as its `before`.
        self.snapshot =
            Some(serde_json::to_value(list.to_snapshot()).expect("snapshot serializes"));
    }
}

shallow_observer! {
    /// Observer implementation for [`LinkedList<T>`].
    struct LinkedListObserver<O>(for<T> LinkedList<T>, LinkedListObserverState<O>);
}

/// Iterator returned by [`LinkedListObserver::iter_mut`].
pub struct IterMut<'a, O: Observer<InnerDepth = Zero, Head: Sized>> {
    front_source: LinkedList<O>,
    back_source: LinkedList<O>,
    gap: std::collections::linked_list::IterMut<'a, O::Head>,
    front_dest: *mut LinkedList<O>,
    back_dest: *mut LinkedList<O>,
    front_skip: usize,
    back_skip: usize,
    _marker: PhantomData<&'a mut O>,
}

impl<'a, O: Observer<InnerDepth = Zero, Head: Sized>> Iterator for IterMut<'a, O> {
    type Item = &'a mut O;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ob) = self.front_source.pop_front() {
            let dest = unsafe { &mut *self.front_dest };
            dest.push_back(ob);
            return dest.back_mut();
        }
        if self.front_skip > 0 {
            let front_dest = unsafe { &mut *self.front_dest };
            for ob in front_dest.iter_mut() {
                let value = self.gap.next().unwrap();
                unsafe { Observer::relocate(ob, value) };
            }
            for ob in self.front_source.iter_mut() {
                let value = self.gap.next().unwrap();
                unsafe { Observer::relocate(ob, value) };
            }
            self.front_skip = 0;
        }
        if self.gap.len() > self.back_skip {
            let value = self.gap.next().unwrap();
            let ob = unsafe { O::observe(value) };
            let dest = unsafe { &mut *self.front_dest };
            dest.push_back(ob);
            return dest.back_mut();
        }
        if let Some(ob) = self.back_source.pop_back() {
            let dest = unsafe { &mut *self.front_dest };
            dest.push_back(ob);
            return dest.back_mut();
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.front_source.len() + self.gap.len() - self.front_skip - self.back_skip
            + self.back_source.len();
        (len, Some(len))
    }
}

impl<'a, O: Observer<InnerDepth = Zero, Head: Sized>> DoubleEndedIterator for IterMut<'a, O> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(ob) = self.back_source.pop_front() {
            let dest = unsafe { &mut *self.back_dest };
            dest.push_back(ob);
            return dest.back_mut();
        }
        if self.back_skip > 0 {
            let back_dest = unsafe { &mut *self.back_dest };
            for ob in back_dest.iter_mut() {
                let value = self.gap.next_back().unwrap();
                unsafe { Observer::relocate(ob, value) };
            }
            for ob in self.back_source.iter_mut() {
                let value = self.gap.next_back().unwrap();
                unsafe { Observer::relocate(ob, value) };
            }
            self.back_skip = 0;
        }
        if self.gap.len() > self.front_skip {
            let value = self.gap.next_back().unwrap();
            let ob = unsafe { O::observe(value) };
            let dest = unsafe { &mut *self.back_dest };
            dest.push_back(ob);
            return dest.back_mut();
        }
        if let Some(ob) = self.front_source.pop_back() {
            let dest = unsafe { &mut *self.back_dest };
            dest.push_back(ob);
            return dest.back_mut();
        }
        None
    }
}

impl<'a, O: Observer<InnerDepth = Zero, Head: Sized>> ExactSizeIterator for IterMut<'a, O> {}

impl<'a, O: Observer<InnerDepth = Zero, Head: Sized>> Drop for IterMut<'a, O> {
    fn drop(&mut self) {
        let front_dest = unsafe { &mut *self.front_dest };
        front_dest.append(&mut self.front_source);
        let back_dest = unsafe { &mut *self.back_dest };
        back_dest.append(&mut self.back_source);
    }
}

impl<'ob, O, S: ?Sized, D> LinkedListObserver<'ob, O, S, D>
where
    D: Unsigned,
    O: Observer<InnerDepth = Zero, Head: Sized>,
    S: AsDerefMut<D, Target = LinkedList<O::Head>>,
{
    fn push_side(this: &mut LinkedListObserverSideState<O>, value: &mut O::Head) {
        this.append_len += 1;
        let this_inner = this.inner.get_mut();
        if !this_inner.is_empty() {
            this_inner.push_front(unsafe { O::observe(value) });
        }
    }

    #[rustversion::since(1.95)]
    fn push_side_mut<'a>(
        this: &'a mut LinkedListObserverSideState<O>,
        value: &mut O::Head,
    ) -> &'a mut O {
        this.append_len += 1;
        this.inner
            .get_mut()
            .push_front_mut(unsafe { O::observe(value) })
    }

    fn pop_side(
        this: &mut LinkedListObserverSideState<O>,
        other: &mut LinkedListObserverSideState<O>,
        len: usize,
    ) {
        if this.append_len > 0 {
            this.append_len -= 1;
        } else {
            this.truncate_len += 1;
        }
        let this_inner = this.inner.get_mut();
        if !this_inner.is_empty() {
            this_inner.pop_front();
        } else {
            let other_inner = other.inner.get_mut();
            if other_inner.len() > len {
                other_inner.pop_back();
            }
        }
    }

    /// See [`LinkedList::append`].
    pub fn append(&mut self, other: &mut LinkedList<O::Head>) {
        self.state.back.append_len += other.len();
        self.untracked_mut().append(other);
    }

    /// See [`LinkedList::iter_mut`].
    pub fn iter_mut(&mut self) -> IterMut<'_, O> {
        let list = (*self.ptr).as_deref_mut();
        let front_source = std::mem::take(self.state.front.inner.get_mut());
        let back_source = std::mem::take(self.state.back.inner.get_mut());
        let front_skip = front_source.len();
        let back_skip = back_source.len();
        let gap = list.iter_mut();
        IterMut {
            front_source,
            back_source,
            gap,
            front_dest: self.state.front.inner.get(),
            back_dest: self.state.back.inner.get(),
            front_skip,
            back_skip,
            _marker: PhantomData,
        }
    }

    /// See [`LinkedList::clear`].
    pub fn clear(&mut self) {
        let len = (*self).untracked_ref().len();
        if len == 0 {
            return;
        }
        self.untracked_mut().clear();
        let existing = len - self.state.front.append_len - self.state.back.append_len;
        self.state.front.inner.get_mut().clear();
        self.state.back.inner.get_mut().clear();
        self.state.front.truncate_len += existing;
        self.state.front.append_len = 0;
        self.state.back.truncate_len = 0;
        self.state.back.append_len = 0;
    }

    /// See [`LinkedList::front_mut`].
    pub fn front_mut(&mut self) -> Option<&mut O> {
        let list = (*self.ptr).as_deref_mut();
        let len = list.len();
        if len == 0 {
            return None;
        }
        let this = &mut self.state.front;
        let other = &mut self.state.back;
        let this_inner = this.inner.get_mut();
        if !this_inner.is_empty() {
            return this_inner.front_mut();
        }
        let other_inner = other.inner.get_mut();
        if other_inner.len() >= len {
            return other_inner.back_mut();
        }
        let value = list.front_mut().unwrap();
        this_inner.push_front(unsafe { O::observe(value) });
        this_inner.front_mut()
    }

    /// See [`LinkedList::back_mut`].
    pub fn back_mut(&mut self) -> Option<&mut O> {
        let list = (*self.ptr).as_deref_mut();
        let len = list.len();
        if len == 0 {
            return None;
        }
        let this = &mut self.state.back;
        let other = &mut self.state.front;
        let this_inner = this.inner.get_mut();
        if !this_inner.is_empty() {
            return this_inner.front_mut();
        }
        let other_inner = other.inner.get_mut();
        if other_inner.len() >= len {
            return other_inner.back_mut();
        }
        let value = list.back_mut().unwrap();
        this_inner.push_front(unsafe { O::observe(value) });
        this_inner.front_mut()
    }

    /// See [`LinkedList::push_front`].
    pub fn push_front(&mut self, value: O::Head) {
        self.untracked_mut().push_front(value);
        let value = (*self.ptr).as_deref_mut().front_mut().unwrap();
        Self::push_side(&mut self.state.front, value);
    }

    /// See [`LinkedList::push_front_mut`].
    #[rustversion::since(1.95)]
    pub fn push_front_mut(&mut self, value: O::Head) -> &mut O {
        let value = (*self.ptr).as_deref_mut().push_front_mut(value);
        Self::push_side_mut(&mut self.state.front, value)
    }

    /// See [`LinkedList::pop_front`].
    pub fn pop_front(&mut self) -> Option<O::Head> {
        let value = self.untracked_mut().pop_front()?;
        let len = (*self).untracked_ref().len();
        Self::pop_side(&mut self.state.front, &mut self.state.back, len);
        Some(value)
    }

    /// See [`LinkedList::push_back`].
    pub fn push_back(&mut self, value: O::Head) {
        self.untracked_mut().push_back(value);
        let value = (*self.ptr).as_deref_mut().back_mut().unwrap();
        Self::push_side(&mut self.state.back, value);
    }

    /// See [`LinkedList::push_back_mut`].
    #[rustversion::since(1.95)]
    pub fn push_back_mut(&mut self, value: O::Head) -> &mut O {
        let value = (*self.ptr).as_deref_mut().push_back_mut(value);
        Self::push_side_mut(&mut self.state.back, value)
    }

    /// See [`LinkedList::pop_back`].
    pub fn pop_back(&mut self) -> Option<O::Head> {
        let value = self.untracked_mut().pop_back()?;
        let len = (*self).untracked_ref().len();
        Self::pop_side(&mut self.state.back, &mut self.state.front, len);
        Some(value)
    }

    /// See [`LinkedList::split_off`].
    pub fn split_off(&mut self, at: usize) -> LinkedList<O::Head> {
        let len = (*self).untracked_ref().len();
        let back_boundary = len - self.state.back.append_len;
        let split = self.untracked_mut().split_off(at);
        if at >= back_boundary {
            // Splitting within the appended region
            let removed = len - at;
            self.state.back.append_len -= removed;
            let back_inner = self.state.back.inner.get_mut();
            for _ in 0..removed.min(back_inner.len()) {
                back_inner.pop_front();
            }
        } else if at > self.state.front.append_len {
            // Splitting within the existing region
            self.state.back.truncate_len += back_boundary - at;
            self.state.back.append_len = 0;
            self.state.back.inner.get_mut().clear();
            let front_inner = self.state.front.inner.get_mut();
            while front_inner.len() > at {
                front_inner.pop_back();
            }
        } else {
            self.state.mark_replace(at);
        }
        split
    }

    /// See [`LinkedList::extract_if`].
    pub fn extract_if<F>(
        &mut self,
        filter: F,
    ) -> std::collections::linked_list::ExtractIf<'_, O::Head, F>
    where
        F: FnMut(&mut O::Head) -> bool,
    {
        let new_len = (*self).untracked_ref().len();
        self.state.mark_replace(new_len);
        self.untracked_mut().extract_if(filter)
    }
}

impl<'ob, O, S: ?Sized, D, U> Extend<U> for LinkedListObserver<'ob, O, S, D>
where
    D: Unsigned,
    O: Observer<InnerDepth = Zero, Head: Sized>,
    S: AsDerefMut<D, Target = LinkedList<O::Head>>,
    LinkedList<O::Head>: Extend<U>,
{
    fn extend<I: IntoIterator<Item = U>>(&mut self, other: I) {
        let old_len = (*self).untracked_ref().len();
        self.untracked_mut().extend(other);
        let new_len = (*self).untracked_ref().len();
        self.state.back.append_len += new_len - old_len;
    }
}

impl<T: Observe + SerializeSnapshot> Observe for LinkedList<T> {
    type Observer<'ob, S, D>
        = LinkedListObserver<'ob, T::Observer<'ob, T, Zero>, S, D>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

    type Spec = DefaultSpec;
}

default_impl_ro_observe! {
    impl [T: Observe] RoObserve for LinkedList<T>;
}

impl<T: Snapshot> Snapshot for LinkedList<T> {
    type Snapshot = Box<[T::Snapshot]>;

    fn to_snapshot(&self) -> Self::Snapshot {
        self.iter().map(|item| item.to_snapshot()).collect()
    }
}

impl<T: SerializeSnapshot> SerializeSnapshot for LinkedList<T>
where
    Self::Snapshot: serde::Serialize + 'static,
{
    fn flush<S: Sink + ?Sized>(&self, snapshot: Self::Snapshot, sink: &mut S) {
        let mut snapshot = snapshot.into_vec().into_iter();
        for (i, v) in self.iter().enumerate() {
            sink.push_neg_index(self.len() - i);
            if let Some(s) = snapshot.next() {
                SerializeSnapshot::flush(&v, s, sink);
            } else {
                sink.replace(None, Some(v));
            }
            sink.pop_segment();
        }
        for (i, s) in snapshot.enumerate() {
            sink.push_neg_index(i + 1);
            sink.replace(Some(&s), None);
            sink.pop_segment();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::LinkedList;

    use muon_test_utils::*;
    use serde_json::json;

    use crate::helper::QuasiObserver;
    use crate::observe::ObserveExt;

    #[test]
    fn no_change() {
        let mut list = LinkedList::from([1, 2, 3]);
        let mut ob = list.__observe();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn push_back_append() {
        let mut list = LinkedList::from([1, 2]);
        let mut ob = list.__observe();
        ob.push_back(3);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-1], "before": null, "after": 3}]),
        );
    }

    #[test]
    fn push_front_pop_front() {
        let mut list = LinkedList::from([1, 2, 3]);
        let mut ob = list.__observe();
        ob.push_front(0);
        ob.pop_front();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn push_front_unbalanced() {
        let mut list = LinkedList::from([1, 2]);
        let mut ob = list.__observe();
        ob.push_front(0);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2], "after": [0, 1, 2]}]),
        );
    }

    #[test]
    fn pop_front_triggers_replace() {
        let mut list = LinkedList::from([1, 2, 3]);
        let mut ob = list.__observe();
        ob.pop_front();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [2, 3]}]),
        );
    }

    #[test]
    fn pop_back_truncate() {
        let mut list = LinkedList::from([1, 2, 3]);
        let mut ob = list.__observe();
        ob.pop_back();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [1, 2]}]),
        );
    }

    #[test]
    fn clear_non_empty() {
        let mut list = LinkedList::from([1, 2, 3]);
        let mut ob = list.__observe();
        ob.clear();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": []}]),
        );
    }

    #[test]
    fn clear_empty_no_mutation() {
        let mut list: LinkedList<i32> = LinkedList::new();
        let mut ob = list.__observe();
        ob.clear();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn inner_observer_front() {
        let mut list = LinkedList::from(["hello".to_string(), "world".to_string()]);
        let mut ob = list.__observe();
        ob.front_mut().unwrap().push_str("!");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-2], "before": "hello", "after": "hello!"}]),
        );
    }

    #[test]
    fn inner_observer_back() {
        let mut list = LinkedList::from(["hello".to_string(), "world".to_string()]);
        let mut ob = list.__observe();
        ob.back_mut().unwrap().push_str("!");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-1], "before": "world", "after": "world!"}]),
        );
    }

    #[test]
    fn extend_appends() {
        let mut list = LinkedList::from([1]);
        let mut ob = list.__observe();
        ob.extend([2, 3]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": [-1], "before": null, "after": 3},
                {"path": [-2], "before": null, "after": 2},
            ]),
        );
    }

    #[test]
    fn split_off_in_appended_region() {
        let mut list = LinkedList::from([1, 2]);
        let mut ob = list.__observe();
        ob.push_back(3);
        ob.push_back(4);
        let split = ob.split_off(3);
        assert_eq!(split, LinkedList::from([4]));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-1], "before": null, "after": 3}]),
        );
    }

    #[test]
    fn split_off_in_existing_region() {
        let mut list = LinkedList::from([1, 2, 3, 4]);
        let mut ob = list.__observe();
        let split = ob.split_off(2);
        assert_eq!(split, LinkedList::from([3, 4]));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3, 4], "after": [1, 2]}]),
        );
    }

    #[test]
    fn append_other_list() {
        let mut list = LinkedList::from([1, 2]);
        let mut ob = list.__observe();
        let mut other = LinkedList::from([3, 4]);
        ob.append(&mut other);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": [-1], "before": null, "after": 4},
                {"path": [-2], "before": null, "after": 3},
            ]),
        );
    }

    #[test]
    fn double_flush() {
        let mut list = LinkedList::from([1, 2]);
        let mut ob = list.__observe();
        ob.push_back(3);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-1], "before": null, "after": 3}]),
        );
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn iter_mut_all() {
        let mut list = LinkedList::from(["a".to_string(), "b".to_string()]);
        let mut ob = list.__observe();
        for inner in ob.iter_mut() {
            inner.push_str("!");
        }
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": [-1], "before": "b", "after": "b!"},
                {"path": [-2], "before": "a", "after": "a!"},
            ]),
        );
    }

    #[test]
    fn deref_mut_triggers_replace() {
        let mut list = LinkedList::from([1, 2, 3]);
        let mut ob = list.__observe();
        *ob.tracked_mut() = LinkedList::from([10, 20]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [10, 20]}]),
        );
    }

    #[test]
    fn pop_back_then_push_back() {
        let mut list = LinkedList::from([1, 2, 3]);
        let mut ob = list.__observe();
        ob.pop_back();
        ob.push_back(4);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [1, 2, 4]}]),
        );
    }

    #[test]
    fn extract_if_triggers_replace() {
        let mut list = LinkedList::from([1, 2, 3, 4]);
        let mut ob = list.__observe();
        let _: Vec<_> = ob.extract_if(|x| *x % 2 == 0).collect();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3, 4], "after": [1, 3]}]),
        );
    }

    #[test]
    fn iter_mut_partial_from_both_ends() {
        let mut list = LinkedList::from([
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ]);
        let mut ob = list.__observe();
        let mut iter = ob.iter_mut();
        iter.next().unwrap().push_str("1");
        iter.next_back().unwrap().push_str("4");
        drop(iter);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": [-1], "before": "d", "after": "d4"},
                {"path": [-4], "before": "a", "after": "a1"},
            ]),
        );
    }

    #[test]
    fn front_and_back_mut_independent() {
        let mut list = LinkedList::from(["x".to_string(), "y".to_string(), "z".to_string()]);
        let mut ob = list.__observe();
        ob.front_mut().unwrap().push_str("1");
        ob.back_mut().unwrap().push_str("3");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": [-1], "before": "z", "after": "z3"},
                {"path": [-3], "before": "x", "after": "x1"},
            ]),
        );
    }

    #[rustversion::since(1.95)]
    #[test]
    fn push_back_mut_then_flush() {
        let mut list = LinkedList::from(["a".to_string(), "b".to_string()]);
        let mut ob = list.__observe();
        ob.push_back_mut("c".to_string()).push_str("!");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-1], "before": null, "after": "c!"}]),
        );
    }

    #[rustversion::since(1.95)]
    #[test]
    fn push_back_mut_with_existing_back_observer() {
        let mut list = LinkedList::from(["a".to_string(), "b".to_string()]);
        let mut ob = list.__observe();
        ob.back_mut().unwrap().push_str("!");
        ob.push_back_mut("c".to_string()).push_str("?");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": [-1], "before": null, "after": "c?"},
                {"path": [-2], "before": "b", "after": "b!"},
            ]),
        );
    }

    #[test]
    fn iter_mut_back_then_pop_front() {
        let mut list = LinkedList::from(["a".to_string(), "b".to_string(), "c".to_string()]);
        let mut ob = list.__observe();
        let mut iter = ob.iter_mut();
        iter.next_back().unwrap().push_str("!");
        drop(iter);
        ob.pop_front();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": ["a", "b", "c"], "after": ["b", "c!"]}]),
        );
    }

    #[test]
    fn iter_mut_back_then_front_mut() {
        let mut list = LinkedList::from(["a".to_string(), "b".to_string()]);
        let mut ob = list.__observe();
        let mut iter = ob.iter_mut();
        iter.next_back();
        iter.next_back();
        drop(iter);
        ob.front_mut().unwrap().push_str("!");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-2], "before": "a", "after": "a!"}]),
        );
    }

    #[test]
    fn push_back_then_back_mut() {
        let mut list = LinkedList::from(["a".to_string(), "b".to_string()]);
        let mut ob = list.__observe();
        ob.push_back("c".to_string());
        ob.back_mut().unwrap().push_str("!");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-1], "before": null, "after": "c!"}]),
        );
    }

    #[test]
    fn push_front_then_front_mut() {
        let mut list = LinkedList::from(["a".to_string(), "b".to_string()]);
        let mut ob = list.__observe();
        ob.push_front("z".to_string());
        ob.front_mut().unwrap().push_str("!");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": ["a", "b"], "after": ["z!", "a", "b"]}]),
        );
    }

    #[test]
    fn push_back_then_iter_mut_covers_all() {
        let mut list = LinkedList::from(["a".to_string(), "b".to_string()]);
        let mut ob = list.__observe();
        ob.push_back("c".to_string());
        let count = ob.iter_mut().count();
        assert_eq!(count, 3);
    }

    #[test]
    fn push_front_then_pop_front_cancels() {
        let mut list = LinkedList::from(["a".to_string(), "b".to_string()]);
        let mut ob = list.__observe();
        ob.push_front("z".to_string());
        ob.pop_front();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn push_back_then_pop_back_cancels() {
        let mut list = LinkedList::from(["a".to_string(), "b".to_string()]);
        let mut ob = list.__observe();
        ob.push_back("c".to_string());
        ob.pop_back();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn multiple_push_back_then_pop_back() {
        let mut list = LinkedList::from([1, 2]);
        let mut ob = list.__observe();
        ob.push_back(3);
        ob.push_back(4);
        ob.pop_back();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-1], "before": null, "after": 3}]),
        );
    }

    #[test]
    fn multiple_push_front_then_pop_front() {
        let mut list = LinkedList::from([1, 2]);
        let mut ob = list.__observe();
        ob.push_front(0);
        ob.push_front(-1);
        ob.pop_front();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2], "after": [0, 1, 2]}]),
        );
    }

    #[test]
    fn push_back_then_front_mut_all_appended() {
        let mut list: LinkedList<String> = LinkedList::new();
        let mut ob = list.__observe();
        ob.push_back("a".to_string());
        ob.push_back("b".to_string());
        ob.front_mut().unwrap().push_str("!");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": [-1], "before": null, "after": "b"},
                {"path": [-2], "before": null, "after": "a!"},
            ]),
        );
    }

    #[test]
    fn push_back_mixed_then_back_mut() {
        let mut list = LinkedList::from(["x".to_string()]);
        let mut ob = list.__observe();
        ob.push_back("a".to_string());
        ob.push_back("b".to_string());
        ob.back_mut().unwrap().push_str("!");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": [-1], "before": null, "after": "b!"},
                {"path": [-2], "before": null, "after": "a"},
            ]),
        );
    }

    #[rustversion::since(1.95)]
    #[test]
    fn push_back_maintains_back_inner_symmetry() {
        let mut list = LinkedList::from(["x".to_string()]);
        let mut ob = list.__observe();
        ob.push_back_mut("a".to_string()).push_str("!");
        ob.push_back("b".to_string());
        ob.back_mut().unwrap().push_str("?");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": [-1], "before": null, "after": "b?"},
                {"path": [-2], "before": null, "after": "a!"},
            ]),
        );
    }

    #[test]
    fn iter_mut_with_existing_front_observer() {
        let mut list = LinkedList::from(["a".to_string(), "b".to_string(), "c".to_string()]);
        let mut ob = list.__observe();
        ob.front_mut().unwrap();
        let count = ob.iter_mut().count();
        assert_eq!(count, 3);
    }

    #[test]
    fn iter_mut_with_existing_back_observer() {
        let mut list = LinkedList::from(["a".to_string(), "b".to_string(), "c".to_string()]);
        let mut ob = list.__observe();
        ob.back_mut().unwrap();
        let count = ob.iter_mut().rev().count();
        assert_eq!(count, 3);
    }

    #[test]
    fn iter_mut_front_skip_then_gap() {
        let mut list = LinkedList::from(["a".to_string(), "b".to_string(), "c".to_string()]);
        let mut ob = list.__observe();
        ob.front_mut().unwrap();
        for inner in ob.iter_mut() {
            inner.push_str("!");
        }
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": [-1], "before": "c", "after": "c!"},
                {"path": [-2], "before": "b", "after": "b!"},
                {"path": [-3], "before": "a", "after": "a!"},
            ]),
        );
    }

    #[test]
    fn iter_mut_forward_with_back_observer() {
        let mut list = LinkedList::from(["a".to_string(), "b".to_string(), "c".to_string()]);
        let mut ob = list.__observe();
        ob.back_mut().unwrap().push_str("!");
        for inner in ob.iter_mut() {
            inner.push_str("?");
        }
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": [-1], "before": "c", "after": "c!?"},
                {"path": [-2], "before": "b", "after": "b?"},
                {"path": [-3], "before": "a", "after": "a?"},
            ]),
        );
    }

    #[test]
    fn iter_mut_backward_with_front_observer() {
        let mut list = LinkedList::from(["a".to_string(), "b".to_string(), "c".to_string()]);
        let mut ob = list.__observe();
        ob.front_mut().unwrap().push_str("!");
        for inner in ob.iter_mut().rev() {
            inner.push_str("?");
        }
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": [-1], "before": "c", "after": "c?"},
                {"path": [-2], "before": "b", "after": "b?"},
                {"path": [-3], "before": "a", "after": "a!?"},
            ]),
        );
    }

    #[test]
    fn iter_mut_both_sides_have_observers() {
        let mut list = LinkedList::from([
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ]);
        let mut ob = list.__observe();
        ob.front_mut().unwrap().push_str("1");
        ob.back_mut().unwrap().push_str("4");
        // Forward: front_source yields "a1", gap yields "b" and "c", back observer "d4" via Drop
        for inner in ob.iter_mut() {
            inner.push_str("!");
        }
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": [-1], "before": "d", "after": "d4!"},
                {"path": [-2], "before": "c", "after": "c!"},
                {"path": [-3], "before": "b", "after": "b!"},
                {"path": [-4], "before": "a", "after": "a1!"},
            ]),
        );
    }

    #[test]
    fn iter_mut_drop_restores_observers() {
        let mut list = LinkedList::from(["a".to_string(), "b".to_string(), "c".to_string()]);
        let mut ob = list.__observe();
        ob.front_mut().unwrap().push_str("!");
        ob.back_mut().unwrap().push_str("?");
        let mut iter = ob.iter_mut();
        iter.next();
        drop(iter);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": [-1], "before": "c", "after": "c?"},
                {"path": [-3], "before": "a", "after": "a!"},
            ]),
        );
    }

    #[test]
    fn iter_mut_mixed_directions() {
        let mut list = LinkedList::from([
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
        ]);
        let mut ob = list.__observe();
        ob.front_mut().unwrap();
        ob.back_mut().unwrap();
        // front_source=[obs_a], back_source=[obs_e], gap covers [a,b,c,d,e]
        let mut iter = ob.iter_mut();
        iter.next().unwrap().push_str("1"); // obs_a from front_source
        iter.next_back().unwrap().push_str("5"); // obs_e from back_source
        iter.next().unwrap().push_str("2"); // gap.next() = b
        iter.next_back().unwrap().push_str("4"); // gap.next_back() = d
        iter.next().unwrap().push_str("3"); // gap.next() = c
        assert!(iter.next().is_none());
        drop(iter);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": [-1], "before": "e", "after": "e5"},
                {"path": [-2], "before": "d", "after": "d4"},
                {"path": [-3], "before": "c", "after": "c3"},
                {"path": [-4], "before": "b", "after": "b2"},
                {"path": [-5], "before": "a", "after": "a1"},
            ]),
        );
    }
}
