use std::collections::TryReserveError;
use std::fmt::{Debug, Display, Write};
use std::ops::{AddAssign, Bound, Deref, DerefMut, Index, IndexMut, RangeBounds};
use std::slice::SliceIndex;
use std::string::Drain;

use crate::Observe;
use crate::general::{SerializeSnapshot, Snapshot};
use crate::helper::macros::{default_impl_ro_observe, delegate_methods};
use crate::helper::shallow::{ObserverState, SerializeObserverState, ShallowMut};
use crate::helper::{AsDeref, AsDerefMut, Invalidate, QuasiObserver, Succ, Unsigned, Zero};
use crate::impls::strings::str::StrObserver;
use crate::observe::{DefaultSpec, Flush, FlushWith, Observer, QuasiSink, Sink};

pub struct StringObserverState {
    /// Known length of the string at the last flush. Appends through
    /// untracked methods (e.g. `push_str`) are detected by comparison.
    pub last_len: usize,
    /// Set by explicit mutating methods (`truncate`, `pop`, ...).
    mutated: bool,
    /// Pre-write snapshot, captured at observe time and refreshed at
    /// every flush. Serves as the `Replace.before`.
    snapshot: Option<serde_json::Value>,
}

impl StringObserverState {
    pub fn mark_truncate(&mut self, _: &str, _: usize) {
        self.mutated = true;
    }
}

impl<T: ?Sized> Invalidate<T> for StringObserverState {
    fn invalidate(&mut self, _: &T) {
        self.mutated = true;
    }
}

impl<T: AsRef<str> + ?Sized> ObserverState<T> for StringObserverState {
    fn observe(value: &T) -> Self {
        let value = value.as_ref();
        Self {
            last_len: value.len(),
            mutated: false,
            snapshot: Some(serde_json::to_value(value.to_snapshot()).expect("snapshot serializes")),
        }
    }
}

impl<T: AsRef<str> + ?Sized, S: Sink + ?Sized> SerializeObserverState<T, S>
    for StringObserverState
{
    fn flush(&mut self, value: &T, sink: &mut S) {
        let value = value.as_ref();
        let changed = std::mem::take(&mut self.mutated) || value.len() != self.last_len;
        self.last_len = value.len();
        if !changed {
            return;
        }
        let before = self.snapshot.take();
        let after = Some(&value as &dyn erased_serde::Serialize);
        self.snapshot =
            Some(serde_json::to_value(value.to_snapshot()).expect("snapshot serializes"));
        sink.replace(
            before.as_ref().map(|v| v as &dyn erased_serde::Serialize),
            after.as_ref().map(|v| v as &dyn erased_serde::Serialize),
        )
    }
}

/// Observer implementation for [`String`].
pub struct StringObserver<'ob, S: ?Sized, D = Zero> {
    inner: StrObserver<'ob, StringObserverState, S, Succ<D>>,
}

impl<'ob, S: ?Sized, D> Deref for StringObserver<'ob, S, D> {
    type Target = StrObserver<'ob, StringObserverState, S, Succ<D>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'ob, S: ?Sized, D> DerefMut for StringObserver<'ob, S, D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<'ob, S: ?Sized, D> QuasiObserver for StringObserver<'ob, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = String>,
{
    type Head = S;
    type OuterDepth = Succ<Succ<Zero>>;
    type InnerDepth = D;

    fn invalidate(this: &mut Self) {
        Invalidate::invalidate(&mut this.inner.state, (*this.inner.ptr).as_deref().as_str());
    }
}

impl<'ob, S: ?Sized, D> Observer for StringObserver<'ob, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = String>,
{
    unsafe fn observe(head: *mut Self::Head) -> Self {
        Self {
            inner: unsafe { Observer::observe(head) },
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Self::Head) {
        unsafe { Observer::relocate(&mut this.inner, head) }
    }
}

impl<'ob, S: ?Sized, D, Sk: Sink + ?Sized> QuasiSink<Sk> for StringObserver<'ob, S, D> {
    type Operation = Sk::Operation;
    type Identity = Sk::Identity;
}

impl<'ob, S: ?Sized, D, Sk: Sink + ?Sized, Elem: ?Sized> FlushWith<Sk, Elem>
    for StringObserver<'ob, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = String>,
{
    fn flush_with<F>(this: &mut Self, sink: &mut Sk, _flush_elem: F)
    where
        F: FnMut(&mut Elem, &mut Sk),
    {
        <Self as Flush<Sk>>::flush(this, sink)
    }
}

impl<'ob, S: ?Sized, D, Sk: Sink + ?Sized> Flush<Sk> for StringObserver<'ob, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = String>,
{
    fn flush(this: &mut Self, sink: &mut Sk) {
        Flush::flush(&mut this.inner, sink)
    }
}

impl<'ob, S: ?Sized, D> StringObserver<'ob, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = String>,
{
    /// See [`String::as_mut_str`].
    pub fn as_mut_str(&mut self) -> &mut StrObserver<'ob, StringObserverState, S, Succ<D>> {
        &mut self.inner
    }

    delegate_methods! { untracked_mut() as String =>
        pub fn push_str(&mut self, string: &str);
        pub fn extend_from_within<R>(&mut self, src: R) where R: RangeBounds<usize>;
        pub fn reserve(&mut self, additional: usize);
        pub fn reserve_exact(&mut self, additional: usize);
        pub fn try_reserve(&mut self, additional: usize) -> Result<(), TryReserveError>;
        pub fn try_reserve_exact(&mut self, additional: usize) -> Result<(), TryReserveError>;
        pub fn shrink_to_fit(&mut self);
        pub fn shrink_to(&mut self, min_capacity: usize);
        pub fn push(&mut self, ch: char);
    }

    /// See [`String::truncate`].
    pub fn truncate(&mut self, len: usize) {
        let state = &mut self.inner.state;
        let value = (*self.inner.ptr).as_deref_mut();
        state.mark_truncate(value.as_str(), len);
        value.truncate(len);
    }

    /// See [`String::pop`].
    pub fn pop(&mut self) -> Option<char> {
        let state = &mut self.inner.state;
        let value = (*self.inner.ptr).as_deref_mut();
        let ch = value.pop()?;
        state.mark_truncate(value, value.len());
        Some(ch)
    }

    /// See [`String::remove`].
    pub fn remove(&mut self, idx: usize) -> char {
        let state = &mut self.inner.state;
        let value = (*self.inner.ptr).as_deref_mut();
        state.mark_truncate(value.as_str(), idx);
        value.remove(idx)
    }

    /// See [`String::retain`].
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(char) -> bool,
    {
        let state = &mut self.inner.state;
        let value = (*self.inner.ptr).as_deref_mut();
        let mut removed = false;
        value.retain(|ch| {
            let kept = f(ch);
            removed |= !kept;
            kept
        });
        if removed {
            state.mark_truncate(value, value.len());
        }
    }

    /// See [`String::insert`].
    pub fn insert(&mut self, idx: usize, ch: char) {
        let state = &mut self.inner.state;
        let value = (*self.inner.ptr).as_deref_mut();
        state.mark_truncate(value.as_str(), idx);
        value.insert(idx, ch);
    }

    /// See [`String::insert_str`].
    pub fn insert_str(&mut self, idx: usize, string: &str) {
        let state = &mut self.inner.state;
        let value = (*self.inner.ptr).as_deref_mut();
        state.mark_truncate(value.as_str(), idx);
        value.insert_str(idx, string);
    }

    /// See [`String::as_mut_vec`].
    ///
    /// ## Safety
    ///
    /// See [`String::as_mut_vec`] for safety requirements.
    pub unsafe fn as_mut_vec(&mut self) -> ShallowMut<'_, Vec<u8>, StringObserverState> {
        let inner = unsafe { (*self.inner.ptr).as_deref_mut().as_mut_vec() };
        ShallowMut::new(inner, &raw mut self.inner.state)
    }

    /// See [`String::split_off`].
    pub fn split_off(&mut self, at: usize) -> String {
        let state = &mut self.inner.state;
        let value = (*self.inner.ptr).as_deref_mut();
        state.mark_truncate(value.as_str(), at);
        value.split_off(at)
    }

    /// See [`String::clear`].
    pub fn clear(&mut self) {
        let state = &mut self.inner.state;
        let value = (*self.inner.ptr).as_deref_mut();
        state.mark_truncate(value.as_str(), 0);
        value.clear();
    }

    /// See [`String::drain`].
    pub fn drain<R>(&mut self, range: R) -> Drain<'_>
    where
        R: RangeBounds<usize>,
    {
        let start_index = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };
        let state = &mut self.inner.state;
        let value = (*self.inner.ptr).as_deref_mut();
        state.mark_truncate(value.as_str(), start_index);
        value.drain(range)
    }

    /// See [`String::replace_range`].
    pub fn replace_range<R>(&mut self, range: R, replace_with: &str)
    where
        R: RangeBounds<usize>,
    {
        let start_index = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };
        let state = &mut self.inner.state;
        let value = (*self.inner.ptr).as_deref_mut();
        state.mark_truncate(value.as_str(), start_index);
        value.replace_range(range, replace_with);
    }
}

impl<'ob, S: ?Sized, D> AddAssign<&str> for StringObserver<'ob, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = String>,
{
    fn add_assign(&mut self, rhs: &str) {
        self.untracked_mut().add_assign(rhs);
    }
}

impl<'ob, S: ?Sized, D, U> Extend<U> for StringObserver<'ob, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = String>,
    String: Extend<U>,
{
    fn extend<I: IntoIterator<Item = U>>(&mut self, other: I) {
        self.untracked_mut().extend(other);
    }
}

impl<'ob, S: ?Sized, D, I> Index<I> for StringObserver<'ob, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = String>,
    I: SliceIndex<str>,
{
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.untracked_ref().index(index)
    }
}

impl<'ob, S: ?Sized, D, I> IndexMut<I> for StringObserver<'ob, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = String>,
    I: SliceIndex<str>,
{
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.tracked_mut().index_mut(index)
    }
}

impl<'ob, S: ?Sized, D> Write for StringObserver<'ob, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = String>,
{
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.untracked_mut().write_str(s)
    }

    fn write_char(&mut self, c: char) -> std::fmt::Result {
        self.untracked_mut().write_char(c)
    }
}

impl<'ob, S: ?Sized, D> Debug for StringObserver<'ob, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = String>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("StringObserver")
            .field(&self.untracked_ref())
            .finish()
    }
}

impl<'ob, S: ?Sized, D> Display for StringObserver<'ob, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = String>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.untracked_ref(), f)
    }
}

impl<'ob, S1, S2, D1, D2> PartialEq<StringObserver<'ob, S2, D2>> for StringObserver<'ob, S1, D1>
where
    D1: Unsigned,
    D2: Unsigned,
    S1: AsDeref<D1, Target = String>,
    S2: AsDeref<D2, Target = String>,
{
    fn eq(&self, other: &StringObserver<'ob, S2, D2>) -> bool {
        self.untracked_ref().eq(other.untracked_ref())
    }
}

impl<'ob, S, D> Eq for StringObserver<'ob, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = String>,
{
}

impl<'ob, S1, S2, D1, D2> PartialOrd<StringObserver<'ob, S2, D2>> for StringObserver<'ob, S1, D1>
where
    D1: Unsigned,
    D2: Unsigned,
    S1: AsDeref<D1, Target = String>,
    S2: AsDeref<D2, Target = String>,
{
    fn partial_cmp(&self, other: &StringObserver<'ob, S2, D2>) -> Option<std::cmp::Ordering> {
        self.untracked_ref().partial_cmp(other.untracked_ref())
    }
}

impl<'ob, S, D> Ord for StringObserver<'ob, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = String>,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.untracked_ref().cmp(other.untracked_ref())
    }
}

impl Observe for String {
    type Observer<'ob, S, D>
        = StringObserver<'ob, S, D>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

    type Spec = DefaultSpec;
}

default_impl_ro_observe! {
    impl RoObserve for String;
}

impl Snapshot for String {
    type Snapshot = Box<str>;

    fn to_snapshot(&self) -> Box<str> {
        self.as_str().to_snapshot()
    }
}

impl SerializeSnapshot for String {
    fn flush<S: Sink + ?Sized>(&self, snapshot: Box<str>, sink: &mut S) {
        SerializeSnapshot::flush(&self.as_str(), snapshot, sink)
    }
}

#[cfg(test)]
mod tests {
    use muon_test_utils::*;
    use serde_json::json;

    use crate::helper::QuasiObserver;
    use crate::observe::ObserveExt;

    #[test]
    fn no_mutation_returns_none() {
        let mut s = String::from("hello");
        let mut ob = s.__observe();
        assert!(__flush!(&mut ob).is_empty());
    }

    #[test]
    fn replace_on_deref_mut() {
        let mut s = String::from("hello");
        let mut ob = s.__observe();
        ob.clear();
        ob.push_str("world"); // append after replace should have no effect
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "hello", "after": "world"}]),
        );
    }

    #[test]
    fn append_with_push() {
        let mut s = String::from("a");
        let mut ob = s.__observe();
        ob.push('b');
        ob.push('c');
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "a", "after": "abc"}]),
        );
    }

    #[test]
    fn append_with_push_str() {
        let mut s = String::from("foo");
        let mut ob = s.__observe();
        ob.push_str("bar");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "foo", "after": "foobar"}]),
        );
    }

    #[test]
    fn append_with_add_assign() {
        let mut s = String::from("foo");
        let mut ob = s.__observe();
        ob += "bar";
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "foo", "after": "foobar"}]),
        );
    }

    #[test]
    fn append_empty_string() {
        let mut s = String::from("foo");
        let mut ob = s.__observe();
        ob.push_str("");
        ob += "";
        assert!(__flush!(&mut ob).is_empty());
    }

    #[test]
    fn replace_after_append() {
        let mut s = String::from("abc");
        let mut ob = s.__observe();
        ob.push_str("def");
        *ob.tracked_mut() = String::from("xyz");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "abc", "after": "xyz"}]),
        );
    }

    #[test]
    fn truncate() {
        let mut s = String::from("你好，世界！");
        let mut ob = s.__observe();
        ob.truncate("你好".len());
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "你好，世界！", "after": "你好"}]),
        );
    }

    #[test]
    fn pop_as_truncate() {
        let mut s = String::from("你好，世界！");
        let mut ob = s.__observe();
        ob.pop();
        ob.pop();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "你好，世界！", "after": "你好，世"}]),
        );
    }

    #[test]
    fn pop_after_append() {
        let mut s = String::from("你好！");
        let mut ob = s.__observe();
        ob.push_str("世界！");
        ob.pop();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "你好！", "after": "你好！世界"}]),
        );
    }

    #[test]
    fn append_after_pop() {
        let mut s = String::from("你好，世界！");
        let mut ob = s.__observe();
        ob.pop();
        ob.push('~');
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "你好，世界！", "after": "你好，世界~"}]),
        );
    }

    #[test]
    fn remove_before_append_index() {
        let mut s = String::from("你好，世界！");
        let mut ob = s.__observe();
        assert_eq!(ob.remove("你好".len()), '，');
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "你好，世界！", "after": "你好世界！"}]),
        );
    }

    #[test]
    fn remove_at_append_index() {
        let mut s = String::from("你好，世界！");
        let mut ob = s.__observe();
        assert_eq!(ob.remove("你好，世界".len()), '！');
        assert_eq!(ob.remove("你好，世".len()), '界');
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "你好，世界！", "after": "你好，世"}]),
        );
    }

    #[test]
    fn retain_no_removal() {
        let mut s = String::from("hello");
        let mut ob = s.__observe();
        ob.retain(|_| true);
        assert!(__flush!(&mut ob).is_empty());
    }

    #[test]
    fn retain_remove_from_tracked() {
        let mut s = String::from("你好，世界！");
        let mut ob = s.__observe();
        ob.retain(|c| c != '，' && c != '！');
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "你好，世界！", "after": "你好世界"}]),
        );
    }

    #[test]
    fn retain_remove_only_after_append() {
        let mut s = String::from("ab");
        let mut ob = s.__observe();
        ob.push_str("cd");
        ob.retain(|c| c != 'c');
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "ab", "after": "abd"}]),
        );
    }

    #[test]
    fn retain_remove_all() {
        let mut s = String::from("hello");
        let mut ob = s.__observe();
        ob.retain(|_| false);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "hello", "after": ""}]),
        );
    }

    #[test]
    fn retain_straddles_append_index() {
        let mut s = String::from("ab");
        let mut ob = s.__observe();
        ob.push_str("cd");
        ob.retain(|c| c != 'b' && c != 'd');
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "ab", "after": "ac"}]),
        );
    }

    #[test]
    fn write_str_appends() {
        use std::fmt::Write;
        let mut s = String::from("foo");
        let mut ob = s.__observe();
        write!(ob, "bar{}", 42).unwrap();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "foo", "after": "foobar42"}]),
        );
    }
}
