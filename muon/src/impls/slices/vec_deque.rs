use std::cell::UnsafeCell;
use std::collections::vec_deque::Drain;
use std::collections::{TryReserveError, VecDeque};
use std::io::{Read, Write};
use std::mem::MaybeUninit;
use std::ops::{Bound, Index, IndexMut, Range, RangeBounds};

use crate::Observe;
use crate::general::{SerializeSnapshot, Snapshot};
use crate::helper::macros::{default_impl_ro_observe, delegate_methods};
use crate::helper::shallow::{ObserverState, SerializeObserverState, shallow_observer};
use crate::helper::{AsDerefMut, Invalidate, Pointer, QuasiObserver, Unsigned, Zero};
use crate::impls::slices::range_set::RangeSet;
use crate::observe::{DefaultSpec, Flush, Observer, Sink};

/// Lazily-initialized element observer storage for [`VecDeque`]-backed observers.
///
/// Uses a [`VecDeque<MaybeUninit<O>>`] for O(1) front/back operations, and a
/// [`RangeSet<isize>`] with an offset for O(1) index translation on front shifts.
struct LazyVecDeque<O> {
    data: VecDeque<MaybeUninit<O>>,
    initialized: RangeSet<isize>,
    offset: isize,
}

impl<O> LazyVecDeque<O> {
    fn new() -> Self {
        Self {
            data: VecDeque::new(),
            initialized: RangeSet::new(),
            offset: 0,
        }
    }

    fn key(&self, index: usize) -> isize {
        index as isize + self.offset
    }

    fn key_range(&self, range: Range<usize>) -> Range<isize> {
        self.key(range.start)..self.key(range.end)
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn truncate(&mut self, new_len: usize) {
        if new_len >= self.data.len() {
            return;
        }
        let remove_range = self.key_range(new_len..self.data.len());
        for range in self.initialized.overlapping(&remove_range) {
            let start = (range.start - self.offset) as usize;
            let end = (range.end - self.offset) as usize;
            for i in start.max(new_len)..end {
                unsafe { self.data[i].assume_init_drop() };
            }
        }
        self.initialized.remove(remove_range);
        self.data.truncate(new_len);
    }

    fn pop_front(&mut self) {
        if self.data.is_empty() {
            return;
        }
        let key = self.key(0);
        if self
            .initialized
            .overlapping(&(key..key + 1))
            .next()
            .is_some()
        {
            unsafe { self.data[0].assume_init_drop() };
            self.initialized.remove(key..key + 1);
        }
        self.data.pop_front();
        self.offset += 1;
    }

    fn push_front_uninit(&mut self) {
        self.offset -= 1;
        self.data.push_front(MaybeUninit::uninit());
    }
}

impl<O> Drop for LazyVecDeque<O> {
    fn drop(&mut self) {
        for range in self.initialized.iter() {
            let start = (range.start - self.offset) as usize;
            let end = (range.end - self.offset) as usize;
            for i in start..end {
                unsafe { self.data[i].assume_init_drop() };
            }
        }
    }
}

impl<O> LazyVecDeque<O>
where
    O: Observer<InnerDepth = Zero, Head: Sized>,
{
    #[expect(clippy::needless_range_loop)]
    fn relocate(&mut self, range: Range<usize>, slice: &mut [O::Head]) {
        while self.data.len() < slice.len() {
            self.data.push_back(MaybeUninit::uninit());
        }
        if self.data.len() > slice.len() {
            self.truncate(slice.len());
        }
        if range.is_empty() {
            return;
        }
        let key_range = self.key_range(range.clone());
        for gap in self.initialized.gaps(&key_range) {
            let start = (gap.start - self.offset) as usize;
            let end = (gap.end - self.offset) as usize;
            for i in start..end {
                self.data[i] = MaybeUninit::new(unsafe { O::observe(&mut slice[i]) });
            }
        }
        for existing in self.initialized.overlapping(&key_range) {
            let start = (existing.start - self.offset) as usize;
            let end = (existing.end - self.offset) as usize;
            for i in start..end {
                unsafe { Observer::relocate(self.data[i].assume_init_mut(), &mut slice[i]) }
            }
        }
        self.initialized.insert(key_range);
    }

    fn get_mut(&mut self, index: usize) -> Option<&mut MaybeUninit<O>> {
        self.data.get_mut(index)
    }

    fn make_contiguous(&mut self) -> &mut [MaybeUninit<O>] {
        self.data.make_contiguous()
    }
}

struct VecDequeObserverState<O> {
    front_prepend_len: usize,
    front_truncate_len: usize,
    back_append_len: usize,
    back_truncate_len: usize,
    /// Pre-write snapshot of the whole deque, captured at observe time
    /// and refreshed at every flush. Serves as the `Replace.before` of
    /// a whole-deque replace.
    snapshot: Option<serde_json::Value>,
    inner: UnsafeCell<LazyVecDeque<O>>,
}

impl<O> VecDequeObserverState<O> {
    fn back_boundary(&self, len: usize) -> usize {
        len - self.back_append_len
    }

    fn mark_replace(&mut self, len: usize) {
        self.inner.get_mut().truncate(0);
        self.front_prepend_len = len;
        self.front_truncate_len = len;
        self.back_append_len = 0;
        self.back_truncate_len = 0;
    }
}

impl<O> Invalidate<VecDeque<O::Head>> for VecDequeObserverState<O>
where
    O: Observer<InnerDepth = Zero, Head: Sized>,
{
    fn invalidate(&mut self, deque: &VecDeque<O::Head>) {
        self.mark_replace(deque.len());
    }
}

impl<O> ObserverState<VecDeque<O::Head>> for VecDequeObserverState<O>
where
    O: Observer<InnerDepth = Zero, Head: Sized>,
    O::Head: SerializeSnapshot,
{
    fn observe(deque: &VecDeque<O::Head>) -> Self {
        Self {
            front_prepend_len: 0,
            front_truncate_len: 0,
            back_append_len: 0,
            back_truncate_len: 0,
            snapshot: Some(serde_json::to_value(deque.to_snapshot()).expect("snapshot serializes")),
            inner: UnsafeCell::new(LazyVecDeque::new()),
        }
    }
}

impl<O, S: Sink + ?Sized> SerializeObserverState<VecDeque<O::Head>, S> for VecDequeObserverState<O>
where
    O: Observer<InnerDepth = Zero, Head: Sized> + Flush<S>,
    O::Head: SerializeSnapshot + 'static,
{
    fn flush(&mut self, deque: &VecDeque<O::Head>, sink: &mut S) {
        let len = deque.len();
        let front_prepend_len = core::mem::replace(&mut self.front_prepend_len, 0);
        let front_truncate_len = core::mem::replace(&mut self.front_truncate_len, 0);
        let back_append_len = core::mem::replace(&mut self.back_append_len, 0);
        let back_truncate_len = core::mem::replace(&mut self.back_truncate_len, 0);

        let back_boundary = len - back_append_len;

        // Any removal, or an unbalanced front operation: whole-deque
        // replace (the pre-write snapshot is the `before`).
        if front_prepend_len != front_truncate_len
            || front_truncate_len > 0
            || back_truncate_len > 0
        {
            self.inner.get_mut().truncate(0);
            let before = self.snapshot.take();
            let after = Some(serde_json::to_value(deque).expect("serialization cannot fail"));
            self.snapshot =
                Some(serde_json::to_value(deque.to_snapshot()).expect("snapshot serializes"));
            sink.replace(
                before.as_ref().map(|v| v as &dyn erased_serde::Serialize),
                after.as_ref().map(|v| v as &dyn erased_serde::Serialize),
            );
            return;
        }

        let prepend_len = front_prepend_len.min(back_boundary);
        let existing_range = prepend_len..back_boundary;

        // Phase 1: Relocate initialized observers in [prepend_len..back_boundary].
        let inner = self.inner.get_mut();
        let key_range = inner.key_range(existing_range.clone());
        for range in inner.initialized.overlapping(&key_range) {
            let start = (range.start - inner.offset) as usize;
            let end = (range.end - inner.offset) as usize;
            #[expect(clippy::needless_range_loop)]
            for i in start..end {
                let head = &deque[i] as *const O::Head as *mut O::Head;
                unsafe { Observer::relocate(inner.data[i].assume_init_mut(), head) };
            }
        }

        // Prepended elements: no before value.
        for (i, item) in deque.iter().take(prepend_len).enumerate() {
            sink.push_neg_index(len - i);
            sink.replace(None, Some(item));
            sink.pop_segment();
        }

        // Back-appended elements: no before value.
        for (i, item) in deque.iter().rev().take(back_append_len).enumerate() {
            sink.push_neg_index(1 + i);
            sink.replace(None, Some(item));
            sink.pop_segment();
        }
        // The appended region's observers are subsumed by the direct
        // value report: drop them now, or a later flush would flush
        // them and re-report the element with a fabricated before
        // (their creation-time snapshot).
        inner.truncate(back_boundary);

        // Phase 3: Flush initialized observers in [prepend_len..back_boundary].
        let offset = inner.offset;
        let contiguous: *mut [MaybeUninit<O>] = inner.make_contiguous();
        for range in inner.initialized.overlapping(&key_range).rev() {
            let start = ((range.start - offset) as usize).max(prepend_len);
            let end = ((range.end - offset) as usize).min(back_boundary);
            for i in (start..end).rev() {
                let ob = unsafe { (*contiguous)[i].assume_init_mut() };
                sink.push_neg_index(len - i);
                <O as Flush<S>>::flush(ob, sink);
                sink.pop_segment();
            }
        }

        // Reset inner state for next cycle.
        inner.truncate(0);
        // Refresh the whole-deque snapshot so a later wholesale replace
        // reports the state after this partial flush as its `before`.
        self.snapshot =
            Some(serde_json::to_value(deque.to_snapshot()).expect("snapshot serializes"));
    }
}

shallow_observer! {
    /// Observer implementation for [`VecDeque<T>`].
    struct VecDequeObserver<O>(for<T> VecDeque<T>, VecDequeObserverState<O>);
}

impl<'ob, O, S: ?Sized, D> VecDequeObserver<'ob, O, S, D>
where
    D: Unsigned,
    O: Observer<InnerDepth = Zero, Head: Sized>,
    S: AsDerefMut<D, Target = VecDeque<O::Head>>,
{
    #[expect(clippy::mut_from_ref)]
    fn force_index(&self, index: usize) -> Option<&mut O> {
        let deque = unsafe { Pointer::as_mut(&self.ptr).as_deref_mut() };
        let len = deque.len();
        if index >= len {
            return None;
        }
        let slice = deque.make_contiguous();
        let inner = unsafe { &mut *self.state.inner.get() };
        inner.relocate(index..index + 1, &mut slice[..]);
        Some(unsafe { inner.get_mut(index).unwrap().assume_init_mut() })
    }

    fn force_all(&mut self) -> &mut [O] {
        let deque = (*self.ptr).as_deref_mut();
        let len = deque.len();
        let back_boundary = self.state.back_boundary(len);
        let slice = deque.make_contiguous();
        let inner = self.state.inner.get_mut();
        inner.relocate(0..back_boundary, &mut slice[..back_boundary]);
        let contiguous = inner.make_contiguous();
        unsafe { std::mem::transmute(&mut contiguous[..back_boundary]) }
    }

    /// See [`VecDeque::get_mut`].
    pub fn get_mut(&mut self, index: usize) -> Option<&mut O> {
        let deque = (*self.ptr).as_deref_mut();
        let len = deque.len();
        if index >= len {
            return None;
        }
        let slice = deque.make_contiguous();
        let inner = self.state.inner.get_mut();
        inner.relocate(index..index + 1, &mut slice[..]);
        Some(unsafe { inner.get_mut(index).unwrap().assume_init_mut() })
    }

    /// See [`VecDeque::swap`].
    pub fn swap(&mut self, i: usize, j: usize) {
        if i != j {
            if let Some(ob) = self.get_mut(i) {
                QuasiObserver::invalidate(ob);
            }
            if let Some(ob) = self.get_mut(j) {
                QuasiObserver::invalidate(ob);
            }
            self.untracked_mut().swap(i, j);
        }
    }

    delegate_methods! { untracked_mut() as VecDeque =>
        pub fn reserve_exact(&mut self, additional: usize);
        pub fn reserve(&mut self, additional: usize);
        pub fn try_reserve_exact(&mut self, additional: usize) -> Result<(), TryReserveError>;
        pub fn try_reserve(&mut self, additional: usize) -> Result<(), TryReserveError>;
        pub fn shrink_to_fit(&mut self);
        pub fn shrink_to(&mut self, min_capacity: usize);
    }

    /// See [`VecDeque::truncate`].
    pub fn truncate(&mut self, len: usize) {
        let old_len = (*self).untracked_ref().len();
        let back_boundary = self.state.back_boundary(old_len);
        if len >= back_boundary {
            // Only truncating appended: the appended region shrinks
            // by the number of removed elements (zero for a no-op
            // with `len >= old_len`).
            self.state.back_append_len -= old_len.saturating_sub(len);
            self.untracked_mut().truncate(len);
        } else if len > self.state.front_prepend_len {
            // Truncating into existing from back
            self.state.back_truncate_len += back_boundary - len;
            self.state.back_append_len = 0;
            self.state.inner.get_mut().truncate(len);
            self.untracked_mut().truncate(len);
        } else {
            // All existing gone
            self.untracked_mut().truncate(len);
            self.state.mark_replace(len);
        }
    }

    /// See [`VecDeque::iter_mut`].
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut O> {
        let observers = self.force_all();
        observers.iter_mut()
    }

    /// See [`VecDeque::as_mut_slices`].
    pub fn as_mut_slices(&mut self) -> (&mut [O], &mut [O]) {
        let observers = self.force_all();
        (observers, &mut [])
    }

    /// See [`VecDeque::range_mut`].
    pub fn range_mut<R>(&mut self, range: R) -> impl Iterator<Item = &mut O>
    where
        R: RangeBounds<usize> + Clone,
    {
        let deque = (*self.ptr).as_deref_mut();
        let len = deque.len();
        let back_boundary = self.state.back_boundary(len);
        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&n) => (n + 1).min(back_boundary),
            Bound::Excluded(&n) => n.min(back_boundary),
            Bound::Unbounded => back_boundary,
        };
        let slice = deque.make_contiguous();
        let inner = self.state.inner.get_mut();
        inner.relocate(start..end, &mut slice[..back_boundary]);
        let contiguous = inner.make_contiguous();
        contiguous[start..end]
            .iter_mut()
            .map(|slot| unsafe { slot.assume_init_mut() })
    }

    /// See [`VecDeque::drain`].
    pub fn drain<R>(&mut self, range: R) -> Drain<'_, O::Head>
    where
        R: RangeBounds<usize>,
    {
        let old_len = (*self).untracked_ref().len();
        let back_boundary = self.state.back_boundary(old_len);
        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&n) => n + 1,
            Bound::Excluded(&n) => n,
            Bound::Unbounded => old_len,
        };
        if start >= back_boundary {
            self.state.back_append_len -= end - start;
            return self.untracked_mut().drain(range);
        }
        let new_len = old_len - (end - start);
        self.state.mark_replace(new_len);
        self.untracked_mut().drain(range)
    }

    /// See [`VecDeque::clear`].
    pub fn clear(&mut self) {
        let len = (*self).untracked_ref().len();
        if len == 0 {
            return;
        }
        self.untracked_mut().clear();
        let existing = len - self.state.front_prepend_len - self.state.back_append_len;
        self.state.inner.get_mut().truncate(0);
        self.state.front_truncate_len += existing;
        self.state.front_prepend_len = 0;
        self.state.back_truncate_len = 0;
        self.state.back_append_len = 0;
    }

    /// See [`VecDeque::front_mut`].
    pub fn front_mut(&mut self) -> Option<&mut O> {
        if (*self).untracked_ref().is_empty() {
            return None;
        }
        self.get_mut(0)
    }

    /// See [`VecDeque::back_mut`].
    pub fn back_mut(&mut self) -> Option<&mut O> {
        let len = (*self).untracked_ref().len();
        if len == 0 {
            return None;
        }
        self.get_mut(len - 1)
    }

    /// See [`VecDeque::pop_front`].
    pub fn pop_front(&mut self) -> Option<O::Head> {
        let value = self.untracked_mut().pop_front()?;
        if self.state.front_prepend_len > 0 {
            self.state.front_prepend_len -= 1;
        } else {
            self.state.front_truncate_len += 1;
        }
        self.state.inner.get_mut().pop_front();
        Some(value)
    }

    /// See [`VecDeque::pop_back`].
    pub fn pop_back(&mut self) -> Option<O::Head> {
        let value = self.untracked_mut().pop_back()?;
        if self.state.back_append_len > 0 {
            self.state.back_append_len -= 1;
        } else {
            self.state.back_truncate_len += 1;
            let len = (*self).untracked_ref().len();
            let back_boundary = self.state.back_boundary(len);
            self.state.inner.get_mut().truncate(back_boundary);
        }
        Some(value)
    }

    /// See [`VecDeque::pop_front_if`].
    #[rustversion::since(1.93)]
    pub fn pop_front_if(
        &mut self,
        predicate: impl FnOnce(&mut O::Head) -> bool,
    ) -> Option<O::Head> {
        let front = self.untracked_mut().front_mut()?;
        if predicate(front) {
            self.pop_front()
        } else {
            None
        }
    }

    /// See [`VecDeque::pop_back_if`].
    #[rustversion::since(1.93)]
    pub fn pop_back_if(&mut self, predicate: impl FnOnce(&mut O::Head) -> bool) -> Option<O::Head> {
        let back = self.untracked_mut().back_mut()?;
        if predicate(back) {
            self.pop_back()
        } else {
            None
        }
    }

    /// See [`VecDeque::push_front`].
    pub fn push_front(&mut self, value: O::Head) {
        self.state.front_prepend_len += 1;
        self.untracked_mut().push_front(value);
        self.state.inner.get_mut().push_front_uninit();
    }

    /// See [`VecDeque::push_back`].
    pub fn push_back(&mut self, value: O::Head) {
        self.state.back_append_len += 1;
        self.untracked_mut().push_back(value);
    }

    /// See [`VecDeque::push_front_mut`].
    #[rustversion::since(1.95)]
    pub fn push_front_mut(&mut self, value: O::Head) -> &mut O {
        self.push_front(value);
        self.force_all().first_mut().unwrap()
    }

    /// See [`VecDeque::push_back_mut`].
    #[rustversion::since(1.95)]
    pub fn push_back_mut(&mut self, value: O::Head) -> &mut O {
        self.untracked_mut().push_back(value);
        let deque = (*self.ptr).as_deref_mut();
        let len = deque.len();
        let index = len - 1;
        let slice = deque.make_contiguous();
        let inner = self.state.inner.get_mut();
        inner.relocate(index..len, &mut slice[..len]);
        self.state.back_append_len += 1;
        unsafe { inner.get_mut(index).unwrap().assume_init_mut() }
    }

    /// See [`VecDeque::swap_remove_front`].
    pub fn swap_remove_front(&mut self, index: usize) -> Option<O::Head> {
        let len = (*self).untracked_ref().len();
        let fc = self.state.front_prepend_len;
        let value = self.untracked_mut().swap_remove_front(index)?;
        if index < fc {
            self.state.front_prepend_len -= 1;
        } else if index == fc {
            self.state.front_truncate_len += 1;
        } else {
            self.state.mark_replace(len - 1);
            return Some(value);
        }
        self.state.inner.get_mut().pop_front();
        Some(value)
    }

    /// See [`VecDeque::swap_remove_back`].
    pub fn swap_remove_back(&mut self, index: usize) -> Option<O::Head> {
        let len = (*self).untracked_ref().len();
        let back_boundary = self.state.back_boundary(len);
        let value = self.untracked_mut().swap_remove_back(index)?;
        if index >= back_boundary {
            self.state.back_append_len -= 1;
        } else if index + 1 == back_boundary && self.state.back_append_len == 0 {
            self.state.back_truncate_len += 1;
            self.state.inner.get_mut().truncate(index);
        } else {
            self.state.mark_replace(len - 1);
        }
        Some(value)
    }

    /// See [`VecDeque::insert`].
    pub fn insert(&mut self, index: usize, value: O::Head) {
        let len = (*self).untracked_ref().len();
        let back_boundary = self.state.back_boundary(len);
        if index >= back_boundary {
            self.state.back_append_len += 1;
            self.untracked_mut().insert(index, value);
        } else if index <= self.state.front_prepend_len {
            self.state.front_prepend_len += 1;
            self.untracked_mut().insert(index, value);
            self.state.inner.get_mut().push_front_uninit();
        } else {
            self.untracked_mut().insert(index, value);
            self.state.mark_replace(len + 1);
        }
    }

    /// See [`VecDeque::insert_mut`].
    #[rustversion::since(1.95)]
    pub fn insert_mut(&mut self, index: usize, value: O::Head) -> &mut O {
        self.insert(index, value);
        let deque = (*self.ptr).as_deref_mut();
        let len = deque.len();
        let slice = deque.make_contiguous();
        let inner = self.state.inner.get_mut();
        inner.relocate(index..index + 1, &mut slice[..len]);
        unsafe { inner.get_mut(index).unwrap().assume_init_mut() }
    }

    /// See [`VecDeque::remove`].
    pub fn remove(&mut self, index: usize) -> Option<O::Head> {
        let len = (*self).untracked_ref().len();
        let back_boundary = self.state.back_boundary(len);
        let value = self.untracked_mut().remove(index)?;
        if index >= back_boundary {
            self.state.back_append_len -= 1;
        } else if index + 1 == back_boundary {
            self.state.back_truncate_len += 1;
            self.state.back_append_len = 0;
            self.state.inner.get_mut().truncate(index);
        } else {
            self.state.mark_replace(len - 1);
        }
        Some(value)
    }

    /// See [`VecDeque::split_off`].
    pub fn split_off(&mut self, at: usize) -> VecDeque<O::Head> {
        let len = (*self).untracked_ref().len();
        let back_boundary = self.state.back_boundary(len);
        let split = self.untracked_mut().split_off(at);
        if at >= back_boundary {
            self.state.back_append_len -= len - at;
        } else if at > self.state.front_prepend_len {
            self.state.back_truncate_len += back_boundary - at;
            self.state.back_append_len = 0;
            self.state.inner.get_mut().truncate(at);
        } else {
            self.state.mark_replace(at);
        }
        split
    }

    /// See [`VecDeque::append`].
    pub fn append(&mut self, other: &mut VecDeque<O::Head>) {
        self.state.back_append_len += other.len();
        self.untracked_mut().append(other);
    }

    /// See [`VecDeque::retain`].
    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&O::Head) -> bool,
    {
        self.untracked_mut().retain(f);
        let new_len = (*self).untracked_ref().len();
        self.state.mark_replace(new_len);
    }

    /// See [`VecDeque::retain_mut`].
    pub fn retain_mut<F>(&mut self, f: F)
    where
        F: FnMut(&mut O::Head) -> bool,
    {
        self.untracked_mut().retain_mut(f);
        let new_len = (*self).untracked_ref().len();
        self.state.mark_replace(new_len);
    }

    /// See [`VecDeque::resize_with`].
    pub fn resize_with(&mut self, new_len: usize, generator: impl FnMut() -> O::Head) {
        let old_len = (*self).untracked_ref().len();
        let back_boundary = self.state.back_boundary(old_len);
        self.untracked_mut().resize_with(new_len, generator);
        if new_len >= back_boundary {
            self.state.back_append_len += new_len - old_len;
        } else if new_len > self.state.front_prepend_len {
            self.state.back_truncate_len += back_boundary - new_len;
            self.state.back_append_len = 0;
            self.state.inner.get_mut().truncate(new_len);
        } else {
            self.state.mark_replace(new_len);
        }
    }

    /// See [`VecDeque::make_contiguous`].
    pub fn make_contiguous(&mut self) -> &mut [O] {
        self.force_all()
    }

    /// See [`VecDeque::rotate_left`].
    pub fn rotate_left(&mut self, n: usize) {
        let len = (*self).untracked_ref().len();
        if n != 0 && len > 1 {
            self.untracked_mut().rotate_left(n);
            self.state.mark_replace(len);
        }
    }

    /// See [`VecDeque::rotate_right`].
    pub fn rotate_right(&mut self, n: usize) {
        let len = (*self).untracked_ref().len();
        if n != 0 && len > 1 {
            self.untracked_mut().rotate_right(n);
            self.state.mark_replace(len);
        }
    }
}

impl<'ob, O, S: ?Sized, D> VecDequeObserver<'ob, O, S, D>
where
    D: Unsigned,
    O: Observer<InnerDepth = Zero, Head: Sized>,
    O::Head: Clone,
    S: AsDerefMut<D, Target = VecDeque<O::Head>>,
{
    /// See [`VecDeque::resize`].
    pub fn resize(&mut self, new_len: usize, value: O::Head) {
        let old_len = (*self).untracked_ref().len();
        let back_boundary = self.state.back_boundary(old_len);
        self.untracked_mut().resize(new_len, value);
        if new_len >= back_boundary {
            self.state.back_append_len += new_len - old_len;
        } else if new_len > self.state.front_prepend_len {
            self.state.back_truncate_len += back_boundary - new_len;
            self.state.back_append_len = 0;
            self.state.inner.get_mut().truncate(new_len);
        } else {
            self.state.mark_replace(new_len);
        }
    }
}

impl<'ob, O, S: ?Sized, D, U> Extend<U> for VecDequeObserver<'ob, O, S, D>
where
    D: Unsigned,
    O: Observer<InnerDepth = Zero, Head: Sized>,
    S: AsDerefMut<D, Target = VecDeque<O::Head>>,
    VecDeque<O::Head>: Extend<U>,
{
    fn extend<I: IntoIterator<Item = U>>(&mut self, other: I) {
        let old_len = (*self).untracked_ref().len();
        self.untracked_mut().extend(other);
        let new_len = (*self).untracked_ref().len();
        self.state.back_append_len += new_len - old_len;
    }
}

impl<'ob, O, S: ?Sized, D> Read for VecDequeObserver<'ob, O, S, D>
where
    D: Unsigned,
    O: Observer<InnerDepth = Zero, Head = u8>,
    S: AsDerefMut<D, Target = VecDeque<u8>>,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.untracked_mut().read(buf)?;
        if n > 0 {
            if self.state.front_prepend_len >= n {
                self.state.front_prepend_len -= n;
            } else {
                let from_existing = n - self.state.front_prepend_len;
                self.state.front_truncate_len += from_existing;
                self.state.front_prepend_len = 0;
            }
            let inner = self.state.inner.get_mut();
            for _ in 0..n.min(inner.len()) {
                inner.pop_front();
            }
        }
        Ok(n)
    }
}

impl<'ob, O, S: ?Sized, D> Write for VecDequeObserver<'ob, O, S, D>
where
    D: Unsigned,
    O: Observer<InnerDepth = Zero, Head = u8>,
    S: AsDerefMut<D, Target = VecDeque<u8>>,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.untracked_mut().write(buf)?;
        self.state.back_append_len += n;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'ob, O, S: ?Sized, D> Index<usize> for VecDequeObserver<'ob, O, S, D>
where
    D: Unsigned,
    O: Observer<InnerDepth = Zero, Head: Sized>,
    S: AsDerefMut<D, Target = VecDeque<O::Head>>,
{
    type Output = O;

    fn index(&self, index: usize) -> &Self::Output {
        self.force_index(index).expect("index out of bounds")
    }
}

impl<'ob, O, S: ?Sized, D> IndexMut<usize> for VecDequeObserver<'ob, O, S, D>
where
    D: Unsigned,
    O: Observer<InnerDepth = Zero, Head: Sized>,
    S: AsDerefMut<D, Target = VecDeque<O::Head>>,
{
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.get_mut(index).expect("index out of bounds")
    }
}

impl<T: Observe + SerializeSnapshot> Observe for VecDeque<T> {
    type Observer<'ob, S, D>
        = VecDequeObserver<'ob, T::Observer<'ob, T, Zero>, S, D>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

    type Spec = DefaultSpec;
}

default_impl_ro_observe! {
    impl [T: Observe] RoObserve for VecDeque<T>;
}

impl<T: Snapshot> Snapshot for VecDeque<T> {
    type Snapshot = Box<[T::Snapshot]>;

    fn to_snapshot(&self) -> Self::Snapshot {
        self.iter().map(|item| item.to_snapshot()).collect()
    }
}

impl<T: SerializeSnapshot> SerializeSnapshot for VecDeque<T>
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
    use std::collections::VecDeque;

    use muon_test_utils::*;
    use serde_json::json;

    use crate::helper::QuasiObserver;
    use crate::observe::ObserveExt;

    #[test]
    fn no_change_returns_none() {
        let mut deque = VecDeque::from([1, 2, 3]);
        let mut ob = deque.__observe();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn reserve_returns_none() {
        let mut deque = VecDeque::from([1, 2, 3]);
        let mut ob = deque.__observe();
        ob.reserve(100);
        ob.shrink_to_fit();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn make_contiguous_returns_none() {
        let mut deque = VecDeque::new();
        deque.push_back(1);
        deque.push_back(2);
        deque.push_front(0);
        let mut ob = deque.__observe();
        ob.make_contiguous();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn push_back_triggers_append() {
        let mut deque = VecDeque::from([1]);
        let mut ob = deque.__observe();
        ob.push_back(2);
        ob.push_back(3);
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
    fn extend_triggers_append() {
        let mut deque = VecDeque::from([1]);
        let mut ob = deque.__observe();
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
    fn append_other_deque() {
        let mut deque = VecDeque::from([1]);
        let mut ob = deque.__observe();
        let mut other = VecDeque::from([4, 5]);
        ob.append(&mut other);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": [-1], "before": null, "after": 5},
                {"path": [-2], "before": null, "after": 4},
            ]),
        );
    }

    #[test]
    fn pop_back_triggers_truncate() {
        let mut deque = VecDeque::from([1, 2, 3]);
        let mut ob = deque.__observe();
        ob.pop_back();
        ob.pop_back();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [1]}]),
        );
    }

    #[test]
    fn truncate_method() {
        let mut deque = VecDeque::from([1, 2, 3, 4, 5]);
        let mut ob = deque.__observe();
        ob.truncate(2);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3, 4, 5], "after": [1, 2]}]),
        );
    }

    #[test]
    fn pop_back_then_push_back() {
        let mut deque = VecDeque::from([1, 2, 3]);
        let mut ob = deque.__observe();
        ob.pop_back();
        ob.push_back(4);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [1, 2, 4]}]),
        );
    }

    #[test]
    fn pop_back_from_appended_region() {
        let mut deque = VecDeque::from([1]);
        let mut ob = deque.__observe();
        ob.push_back(2);
        ob.push_back(3);
        ob.pop_back();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-1], "before": null, "after": 2}]),
        );
    }

    #[test]
    fn push_front_triggers_replace() {
        let mut deque = VecDeque::from([1, 2]);
        let mut ob = deque.__observe();
        ob.push_front(0);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2], "after": [0, 1, 2]}]),
        );
    }

    #[test]
    fn pop_front_triggers_replace() {
        let mut deque = VecDeque::from([1, 2, 3]);
        let mut ob = deque.__observe();
        ob.pop_front();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [2, 3]}]),
        );
    }

    #[rustversion::since(1.93)]
    #[test]
    fn pop_front_if_true_triggers_replace() {
        let mut deque = VecDeque::from([1, 2, 3]);
        let mut ob = deque.__observe();
        let result = ob.pop_front_if(|x| *x == 1);
        assert_eq!(result, Some(1));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [2, 3]}]),
        );
    }

    #[rustversion::since(1.93)]
    #[test]
    fn pop_front_if_false_returns_none() {
        let mut deque = VecDeque::from([1, 2, 3]);
        let mut ob = deque.__observe();
        let result = ob.pop_front_if(|x| *x == 99);
        assert_eq!(result, None);
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn push_front_overrides_back_append() {
        let mut deque = VecDeque::from([1]);
        let mut ob = deque.__observe();
        ob.push_back(2);
        ob.push_front(0);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1], "after": [0, 1, 2]}]),
        );
    }

    #[test]
    fn deref_mut_triggers_replace() {
        let mut deque = VecDeque::from([1, 2]);
        let mut ob = deque.__observe();
        ob.retain(|x| *x > 1);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2], "after": [2]}]),
        );
    }

    #[test]
    fn empty_deque_no_mutation() {
        let mut deque: VecDeque<i32> = VecDeque::new();
        let mut ob = deque.__observe();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn empty_deque_push_back() {
        let mut deque: VecDeque<i32> = VecDeque::new();
        let mut ob = deque.__observe();
        ob.push_back(1);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-1], "before": null, "after": 1}]),
        );
    }

    #[test]
    fn empty_deque_push_front() {
        let mut deque: VecDeque<i32> = VecDeque::new();
        let mut ob = deque.__observe();
        ob.push_front(1);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [], "after": [1]}]),
        );
    }

    #[test]
    fn clear_empty_deque() {
        let mut deque: VecDeque<i32> = VecDeque::new();
        let mut ob = deque.__observe();
        ob.clear();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn clear_nonempty_deque() {
        let mut deque = VecDeque::from([1, 2, 3]);
        let mut ob = deque.__observe();
        ob.clear();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": []}]),
        );
    }

    #[test]
    fn split_off_from_existing() {
        let mut deque = VecDeque::from([1, 2, 3, 4]);
        let mut ob = deque.__observe();
        let split = ob.split_off(2);
        assert_eq!(split, VecDeque::from([3, 4]));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3, 4], "after": [1, 2]}]),
        );
    }

    #[test]
    fn split_off_from_appended() {
        let mut deque = VecDeque::from([1]);
        let mut ob = deque.__observe();
        ob.push_back(2);
        ob.push_back(3);
        let split = ob.split_off(2);
        assert_eq!(split, VecDeque::from([3]));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-1], "before": null, "after": 2}]),
        );
    }

    #[test]
    fn remove_at_back_append_boundary() {
        let mut deque = VecDeque::from([1, 2, 3]);
        let mut ob = deque.__observe();
        let val = ob.remove(2);
        assert_eq!(val, Some(3));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [1, 2]}]),
        );
    }

    #[test]
    fn remove_from_middle_triggers_replace() {
        let mut deque = VecDeque::from([1, 2, 3, 4]);
        let mut ob = deque.__observe();
        let val = ob.remove(1);
        assert_eq!(val, Some(2));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3, 4], "after": [1, 3, 4]}]),
        );
    }

    #[test]
    fn insert_in_appended_region() {
        let mut deque = VecDeque::from([1]);
        let mut ob = deque.__observe();
        ob.push_back(2);
        ob.insert(2, 3);
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
    fn insert_in_existing_region() {
        let mut deque = VecDeque::from([1, 2, 3]);
        let mut ob = deque.__observe();
        ob.insert(1, 99);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [1, 99, 2, 3]}]),
        );
    }

    #[test]
    fn swap_remove_front_triggers_replace() {
        let mut deque = VecDeque::from([1, 2, 3]);
        let mut ob = deque.__observe();
        let val = ob.swap_remove_front(1);
        assert_eq!(val, Some(2));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [1, 3]}]),
        );
    }

    #[test]
    fn multiple_flushes() {
        let mut deque = VecDeque::from([1, 2]);
        let mut ob = deque.__observe();
        ob.push_back(3);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-1], "before": null, "after": 3}]),
        );
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
        ob.pop_back();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [1, 2]}]),
        );
    }

    #[test]
    fn resize_shrink() {
        let mut deque = VecDeque::from([1, 2, 3, 4, 5]);
        let mut ob = deque.__observe();
        ob.resize(2, 0);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3, 4, 5], "after": [1, 2]}]),
        );
    }

    #[test]
    fn resize_grow() {
        let mut deque = VecDeque::from([1]);
        let mut ob = deque.__observe();
        ob.resize(3, 0);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": [-1], "before": null, "after": 0},
                {"path": [-2], "before": null, "after": 0},
            ]),
        );
    }

    #[rustversion::since(1.93)]
    #[test]
    fn pop_back_if_true() {
        let mut deque = VecDeque::from([1, 2, 3]);
        let mut ob = deque.__observe();
        let result = ob.pop_back_if(|x| *x == 3);
        assert_eq!(result, Some(3));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [1, 2]}]),
        );
    }

    #[rustversion::since(1.93)]
    #[test]
    fn pop_back_if_false() {
        let mut deque = VecDeque::from([1, 2, 3]);
        let mut ob = deque.__observe();
        let result = ob.pop_back_if(|x| *x == 99);
        assert_eq!(result, None);
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn drain_from_appended() {
        let mut deque = VecDeque::from([1]);
        let mut ob = deque.__observe();
        ob.push_back(2);
        ob.push_back(3);
        let drained: Vec<_> = ob.drain(1..).collect();
        assert_eq!(drained, vec![2, 3]);
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn drain_straddles_boundary() {
        let mut deque = VecDeque::from([1, 2, 3]);
        let mut ob = deque.__observe();
        ob.push_back(4);
        let drained: Vec<_> = ob.drain(1..).collect();
        assert_eq!(drained, vec![2, 3, 4]);
        let changes = __flush!(&mut ob);
        // The push and the drain collapse into a single wholesale replace;
        // the before is the pre-flush snapshot (the push was never flushed).
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [1]}]),
        );
    }

    #[test]
    fn index_mut_triggers_replace() {
        let mut deque = VecDeque::from([1i32, 2, 3]);
        let mut ob = deque.__observe();
        *ob[1].tracked_mut() = 99;
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-2], "before": 2, "after": 99}]),
        );
    }

    #[test]
    fn index_returns_inner_observer() {
        let mut deque = VecDeque::from(["hello".to_string(), "world".to_string()]);
        let mut ob = deque.__observe();
        assert_eq!(*ob[0].untracked_ref(), "hello");
        assert_eq!(*ob[1].untracked_ref(), "world");
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn modify_element_via_inner_observer() {
        let mut deque = VecDeque::from(["hello".to_string(), "world".to_string()]);
        let mut ob = deque.__observe();
        ob[0].push_str("!");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-2], "before": "hello", "after": "hello!"}]),
        );
    }

    #[test]
    fn modify_multiple_elements_via_inner_observer() {
        let mut deque = VecDeque::from(["a".to_string(), "b".to_string(), "c".to_string()]);
        let mut ob = deque.__observe();
        ob[0].push_str("1");
        ob[2].push_str("3");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": [-1], "before": "c", "after": "c3"},
                {"path": [-3], "before": "a", "after": "a1"},
            ]),
        );
    }

    #[test]
    fn get_mut_returns_observer() {
        let mut deque = VecDeque::from(["foo".to_string(), "bar".to_string()]);
        let mut ob = deque.__observe();
        let elem = ob.get_mut(1).unwrap();
        elem.push_str("baz");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-1], "before": "bar", "after": "barbaz"}]),
        );
    }

    #[test]
    fn front_mut_returns_observer() {
        let mut deque = VecDeque::from(["first".to_string(), "second".to_string()]);
        let mut ob = deque.__observe();
        let front = ob.front_mut().unwrap();
        front.push_str("!");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-2], "before": "first", "after": "first!"}]),
        );
    }

    #[test]
    fn back_mut_returns_observer() {
        let mut deque = VecDeque::from(["first".to_string(), "second".to_string()]);
        let mut ob = deque.__observe();
        let back = ob.back_mut().unwrap();
        back.push_str("!");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-1], "before": "second", "after": "second!"}]),
        );
    }

    #[test]
    fn iter_mut_returns_observers() {
        let mut deque = VecDeque::from(["a".to_string(), "b".to_string(), "c".to_string()]);
        let mut ob = deque.__observe();
        for elem in ob.iter_mut() {
            elem.push_str("!");
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
    fn make_contiguous_returns_observer_slice() {
        let mut deque = VecDeque::from(["x".to_string(), "y".to_string()]);
        let mut ob = deque.__observe();
        let slice = ob.make_contiguous();
        slice[0].push_str("1");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-2], "before": "x", "after": "x1"}]),
        );
    }

    #[test]
    fn modify_then_append() {
        let mut deque = VecDeque::from(["a".to_string()]);
        let mut ob = deque.__observe();
        ob[0].push_str("!");
        ob.push_back("b".to_string());
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([
                {"path": [-1], "before": null, "after": "b"},
                {"path": [-2], "before": "a", "after": "a!"},
            ]),
        );
    }

    #[test]
    fn no_modify_then_append() {
        let mut deque = VecDeque::from(["a".to_string()]);
        let mut ob = deque.__observe();
        let _ = &ob[0];
        ob.push_back("b".to_string());
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-1], "before": null, "after": "b"}]),
        );
    }

    #[test]
    fn modify_element_then_pop_back() {
        let mut deque = VecDeque::from(["a".to_string(), "b".to_string(), "c".to_string()]);
        let mut ob = deque.__observe();
        ob[0].push_str("!");
        ob.pop_back();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": ["a", "b", "c"], "after": ["a!", "b"]}]),
        );
    }

    #[test]
    fn index_read_only_no_mutation() {
        let mut deque = VecDeque::from(["hello".to_string(), "world".to_string()]);
        let mut ob = deque.__observe();
        let _val = &ob[0];
        let _val2 = &ob[1];
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn pop_push_clears_stale_observer_state() {
        let mut deque = VecDeque::from(["a".to_string(), "b".to_string(), "ab".to_string()]);
        let mut ob = deque.__observe();
        ob[2].truncate(1);
        ob.pop_back();
        ob.push_back("cd".to_string());
        let changes = __flush!(&mut ob);
        assert!(!changes.is_empty());
        assert_eq!(*ob[2].untracked_ref(), "cd");
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[rustversion::since(1.95)]
    #[test]
    fn push_back_mut_returns_observer() {
        let mut deque = VecDeque::from(["a".to_string()]);
        let mut ob = deque.__observe();
        let pushed = ob.push_back_mut("b".into());
        pushed.push_str("c");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-1], "before": null, "after": "bc"}]),
        );
    }

    #[rustversion::since(1.95)]
    #[test]
    fn push_front_mut_returns_observer() {
        let mut deque = VecDeque::from(["a".to_string(), "b".into()]);
        let mut ob = deque.__observe();
        let pushed = ob.push_front_mut("x".into());
        pushed.push_str("y");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": ["a", "b"], "after": ["xy", "a", "b"]}]),
        );
    }

    #[rustversion::since(1.95)]
    #[test]
    fn insert_mut_at_back_returns_observer() {
        let mut deque = VecDeque::from(["a".to_string(), "b".into()]);
        let mut ob = deque.__observe();
        let inserted = ob.insert_mut(2, "c".into());
        inserted.push_str("!");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-1], "before": null, "after": "c!"}]),
        );
    }

    #[rustversion::since(1.95)]
    #[test]
    fn insert_mut_in_middle_returns_observer() {
        let mut deque = VecDeque::from(["a".to_string(), "b".into(), "c".into()]);
        let mut ob = deque.__observe();
        let inserted = ob.insert_mut(1, "X".into());
        inserted.push_str("Y");
        assert_eq!(
            *ob.untracked_ref(),
            VecDeque::from(["a".to_string(), "XY".into(), "b".into(), "c".into()])
        );
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": ["a", "b", "c"], "after": ["a", "XY", "b", "c"]}]),
        );
    }

    #[test]
    fn push_front_pop_front_cancel() {
        let mut deque = VecDeque::from([1, 2, 3]);
        let mut ob = deque.__observe();
        ob.push_front(0);
        ob.pop_front();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn pop_front_push_front_element_replace() {
        let mut deque = VecDeque::from([1, 2, 3]);
        let mut ob = deque.__observe();
        ob.pop_front();
        ob.push_front(99);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": [1, 2, 3], "after": [99, 2, 3]}]),
        );
    }

    #[test]
    fn push_front_pop_front_with_back_append() {
        let mut deque = VecDeque::from([1, 2]);
        let mut ob = deque.__observe();
        ob.push_front(0);
        ob.pop_front();
        ob.push_back(3);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [-1], "before": null, "after": 3}]),
        );
    }

    #[test]
    fn push_back_pop_back_cancel() {
        let mut deque = VecDeque::from([1, 2, 3]);
        let mut ob = deque.__observe();
        ob.push_back(4);
        ob.pop_back();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn pop_front_push_front_with_existing_modify() {
        let mut deque = VecDeque::from(["a".to_string(), "b".to_string(), "c".to_string()]);
        let mut ob = deque.__observe();
        ob[1].push_str("!");
        ob.pop_front();
        ob.push_front("x".to_string());
        // fc=1, ftl=1: any front removal forces a whole-deque replace.
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": ["a", "b", "c"], "after": ["x", "b!", "c"]}]),
        );
        // Second flush: no stale state.
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }
    #[test]
    fn modify_then_pop_back_no_stale_leak() {
        let mut deque = VecDeque::from(["a".to_string(), "b".to_string()]);
        let mut ob = deque.__observe();
        ob[0].push_str("!");
        ob.pop_back();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": ["a", "b"], "after": ["a!"]}]),
        );
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }
}
