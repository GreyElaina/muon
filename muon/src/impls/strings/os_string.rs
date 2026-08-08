use std::collections::TryReserveError;
use std::ffi::{OsStr, OsString};
use std::fmt::{Debug, Display, Write};
use std::ops::{Deref, DerefMut, Index, IndexMut, RangeFull};

use super::os_str::{OsStrObserver, os_str_len};
use crate::Observe;
use crate::general::{SerializeSnapshot, Snapshot};
use crate::helper::macros::{default_impl_ro_observe, delegate_methods};
use crate::helper::shallow::{ObserverState, SerializeObserverState};
use crate::helper::{AsDeref, AsDerefMut, Invalidate, QuasiObserver, Succ, Unsigned, Zero};
use crate::observe::{DefaultSpec, Flush, FlushWith, Observer, QuasiSink, Sink};

pub struct OsStringObserverState {
    /// Known length of the string at the last flush. Appends through
    /// untracked methods are detected by comparison.
    pub last_len: usize,
    /// Set by explicit mutating methods.
    mutated: bool,
    /// Pre-write snapshot for the `Replace.before`.
    snapshot: Option<serde_json::Value>,
}

impl OsStringObserverState {
    fn mark_truncate(&mut self, _: usize) {
        self.mutated = true;
    }
}

impl Invalidate<OsStr> for OsStringObserverState {
    fn invalidate(&mut self, _value: &OsStr) {
        self.mutated = true;
    }
}

impl Invalidate<()> for OsStringObserverState {
    fn invalidate(&mut self, _: &()) {
        self.mutated = true;
    }
}

impl ObserverState<OsStr> for OsStringObserverState {
    fn observe(value: &OsStr) -> Self {
        Self {
            last_len: os_str_len(value),
            mutated: false,
            // The snapshot is the serde shape of `OsStr`, so the
            // replace's `before` matches its `after` (a bare byte
            // array would be a different, non-deserializable shape).
            snapshot: Some(serde_json::to_value(value).expect("snapshot serializes")),
        }
    }
}

impl<S: Sink + ?Sized> SerializeObserverState<OsStr, S> for OsStringObserverState {
    fn flush(&mut self, value: &OsStr, sink: &mut S) {
        let new_len = os_str_len(value);
        let changed = std::mem::take(&mut self.mutated) || new_len != self.last_len;
        self.last_len = new_len;
        if !changed {
            return;
        }
        let before = self.snapshot.take();
        let after = Some(&value as &dyn erased_serde::Serialize);
        self.snapshot = Some(serde_json::to_value(value).expect("snapshot serializes"));
        sink.replace(
            before.as_ref().map(|v| v as &dyn erased_serde::Serialize),
            after.as_ref().map(|v| v as &dyn erased_serde::Serialize),
        )
    }
}

/// Observer implementation for [`OsString`].
pub struct OsStringObserver<'ob, V, S: ?Sized, D = Zero> {
    pub(super) inner: OsStrObserver<'ob, V, S, Succ<D>>,
}

impl<'ob, V, S: ?Sized, D> Deref for OsStringObserver<'ob, V, S, D> {
    type Target = OsStrObserver<'ob, V, S, Succ<D>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'ob, V, S: ?Sized, D> DerefMut for OsStringObserver<'ob, V, S, D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<'ob, V, S: ?Sized, D> QuasiObserver for OsStringObserver<'ob, V, S, D>
where
    V: Invalidate<OsStr>,
    D: Unsigned,
    S: AsDeref<D, Target = OsString>,
{
    type Head = S;
    type OuterDepth = Succ<Succ<Zero>>;
    type InnerDepth = D;

    fn invalidate(this: &mut Self) {
        Invalidate::invalidate(
            &mut this.inner.state,
            (*this.inner.ptr).as_deref().as_os_str(),
        );
    }
}

impl<'ob, S: ?Sized, D> Observer for OsStringObserver<'ob, OsStringObserverState, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = OsString>,
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

impl<'ob, S: ?Sized, D, Sk: Sink + ?Sized> QuasiSink<Sk>
    for OsStringObserver<'ob, OsStringObserverState, S, D>
{
    type Operation = Sk::Operation;
    type Identity = Sk::Identity;
}

impl<'ob, S: ?Sized, D, Sk: Sink + ?Sized, Elem: ?Sized> FlushWith<Sk, Elem>
    for OsStringObserver<'ob, OsStringObserverState, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = OsString>,
{
    fn flush_with<F>(this: &mut Self, sink: &mut Sk, _flush_elem: F)
    where
        F: FnMut(&mut Elem, &mut Sk),
    {
        <Self as Flush<Sk>>::flush(this, sink)
    }
}

impl<'ob, S: ?Sized, D, Sk: Sink + ?Sized> Flush<Sk>
    for OsStringObserver<'ob, OsStringObserverState, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = OsString>,
{
    fn flush(this: &mut Self, sink: &mut Sk) {
        Flush::flush(&mut this.inner, sink)
    }
}

// Methods requiring OsStringObserverState (append/truncate tracking)
impl<'ob, S: ?Sized, D> OsStringObserver<'ob, OsStringObserverState, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = OsString>,
{
    /// See [`OsString::push`].
    pub fn push<T: AsRef<OsStr>>(&mut self, s: T) {
        self.untracked_mut().push(s);
    }

    /// See [`OsString::clear`].
    pub fn clear(&mut self) {
        let state = &mut self.inner.state;
        state.mark_truncate(0);
        (*self.inner.ptr).as_deref_mut().clear();
    }
}

// Capacity-only methods (generic over V)
impl<'ob, V, S: ?Sized, D> OsStringObserver<'ob, V, S, D>
where
    V: Invalidate<OsStr>,
    D: Unsigned,
    S: AsDerefMut<D, Target = OsString>,
{
    delegate_methods! { untracked_mut() as OsString =>
        pub fn reserve(&mut self, additional: usize);
        pub fn try_reserve(&mut self, additional: usize) -> Result<(), TryReserveError>;
        pub fn reserve_exact(&mut self, additional: usize);
        pub fn try_reserve_exact(&mut self, additional: usize) -> Result<(), TryReserveError>;
        pub fn shrink_to_fit(&mut self);
        pub fn shrink_to(&mut self, min_capacity: usize);
    }
}

impl<'ob, S: ?Sized, D, U> Extend<U> for OsStringObserver<'ob, OsStringObserverState, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = OsString>,
    OsString: Extend<U>,
{
    fn extend<I: IntoIterator<Item = U>>(&mut self, iter: I) {
        self.untracked_mut().extend(iter);
    }
}

impl<'ob, V, S: ?Sized, D> IndexMut<RangeFull> for OsStringObserver<'ob, V, S, D>
where
    V: Invalidate<OsStr>,
    D: Unsigned,
    S: AsDerefMut<D, Target = OsString>,
{
    fn index_mut(&mut self, index: RangeFull) -> &mut Self::Output {
        self.tracked_mut().index_mut(index)
    }
}

impl<'ob, V, S: ?Sized, D> Index<RangeFull> for OsStringObserver<'ob, V, S, D>
where
    V: Invalidate<OsStr>,
    D: Unsigned,
    S: AsDerefMut<D, Target = OsString>,
{
    type Output = OsStr;

    fn index(&self, index: RangeFull) -> &Self::Output {
        self.untracked_ref().index(index)
    }
}

impl<'ob, S: ?Sized, D> Write for OsStringObserver<'ob, OsStringObserverState, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = OsString>,
{
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.untracked_mut().write_str(s)
    }

    fn write_char(&mut self, c: char) -> std::fmt::Result {
        self.untracked_mut().write_char(c)
    }
}

impl<'ob, V, S: ?Sized, D> Debug for OsStringObserver<'ob, V, S, D>
where
    V: Invalidate<OsStr>,
    D: Unsigned,
    S: AsDerefMut<D, Target = OsString>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("OsStringObserver")
            .field(&self.untracked_ref())
            .finish()
    }
}

impl<'ob, V, S: ?Sized, D> Display for OsStringObserver<'ob, V, S, D>
where
    V: Invalidate<OsStr>,
    D: Unsigned,
    S: AsDerefMut<D, Target = OsString>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.untracked_ref().to_string_lossy(), f)
    }
}

impl Observe for OsString {
    type Observer<'ob, S, D>
        = OsStringObserver<'ob, OsStringObserverState, S, D>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

    type Spec = DefaultSpec;
}

default_impl_ro_observe! {
    impl RoObserve for OsString;
}

impl Snapshot for OsString {
    #[cfg(unix)]
    type Snapshot = Box<[u8]>;
    #[cfg(windows)]
    type Snapshot = Box<[u16]>;

    fn to_snapshot(&self) -> Self::Snapshot {
        self.as_os_str().to_snapshot()
    }
}

impl SerializeSnapshot for OsString {
    fn flush<S: Sink + ?Sized>(&self, snapshot: Self::Snapshot, sink: &mut S) {
        SerializeSnapshot::flush(&self.as_os_str(), snapshot, sink)
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use muon_test_utils::*;
    use serde_json::json;

    use crate::helper::QuasiObserver;
    use crate::observe::ObserveExt;

    #[test]
    fn no_mutation_returns_none() {
        let mut s = OsString::from("hello");
        let mut ob = s.__observe();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn replace_on_deref_mut() {
        let mut s = OsString::from("hello");
        let mut ob = s.__observe();
        *ob.tracked_mut() = OsString::from("world");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{
                "path": [],
                "before": {"Unix": [104, 101, 108, 108, 111]},
                "after": {"Unix": [119, 111, 114, 108, 100]},
            }]),
        );
    }

    #[test]
    fn append_with_push() {
        let mut s = OsString::from("foo");
        let mut ob = s.__observe();
        ob.push("bar");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{
                "path": [],
                "before": {"Unix": [102, 111, 111]},
                "after": {"Unix": [102, 111, 111, 98, 97, 114]},
            }]),
        );
    }

    #[test]
    fn append_empty_string() {
        let mut s = OsString::from("foo");
        let mut ob = s.__observe();
        ob.push("");
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn replace_after_append() {
        let mut s = OsString::from("abc");
        let mut ob = s.__observe();
        ob.push("def");
        *ob.tracked_mut() = OsString::from("xyz");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{
                "path": [],
                "before": {"Unix": [97, 98, 99]},
                "after": {"Unix": [120, 121, 122]},
            }]),
        );
    }

    #[test]
    fn write_str_appends() {
        use std::fmt::Write;
        let mut s = OsString::from("foo");
        let mut ob = s.__observe();
        write!(ob, "bar{}", 42).unwrap();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{
                "path": [],
                "before": {"Unix": [102, 111, 111]},
                "after": {"Unix": [102, 111, 111, 98, 97, 114, 52, 50]},
            }]),
        );
    }

    #[test]
    fn extend_appends() {
        let mut s = OsString::from("foo");
        let mut ob = s.__observe();
        ob.extend([OsString::from("bar"), OsString::from("baz")]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{
                "path": [],
                "before": {"Unix": [102, 111, 111]},
                "after": {"Unix": [102, 111, 111, 98, 97, 114, 98, 97, 122]},
            }]),
        );
    }

    #[test]
    fn capacity_only_no_mutation() {
        let mut s = OsString::from("hello");
        let mut ob = s.__observe();
        ob.reserve(100);
        ob.shrink_to_fit();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn clear_empty_no_mutation() {
        let mut s = OsString::new();
        let mut ob = s.__observe();
        ob.clear();
        // clear() always marks the state, even on an empty string
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": {"Unix": []}, "after": {"Unix": []}}]),
        );
    }

    #[test]
    fn clear_as_replace() {
        let mut s = OsString::from("hello");
        let mut ob = s.__observe();
        ob.clear();
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{
                "path": [],
                "before": {"Unix": [104, 101, 108, 108, 111]},
                "after": {"Unix": []},
            }]),
        );
    }

    #[test]
    fn clear_then_push_as_replace() {
        let mut s = OsString::from("hello");
        let mut ob = s.__observe();
        ob.clear();
        ob.push("world");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{
                "path": [],
                "before": {"Unix": [104, 101, 108, 108, 111]},
                "after": {"Unix": [119, 111, 114, 108, 100]},
            }]),
        );
    }

    #[test]
    fn append_after_clear() {
        let mut s = OsString::from("hi");
        let mut ob = s.__observe();
        ob.clear();
        ob.push("hello world");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{
                "path": [],
                "before": {"Unix": [104, 105]},
                "after": {"Unix": [104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100]},
            }]),
        );
    }
}
