use std::borrow::Cow;
use std::fmt::Debug;
use std::ops::{AddAssign, Deref, DerefMut};

use crate::Observe;
use crate::general::{SerializeSnapshot, Snapshot};
use crate::helper::{
    AsDeref, AsDerefMut, AsDerefPtrExt, Pointer, QuasiObserver, Succ, Unsigned, Zero,
};
use crate::impls::{DerefObserver, StringObserver};
use crate::observe::{DefaultSpec, Flush, FlushWith, Observer, QuasiSink, RoObserve, Sink};

/// Observer implementation for [`Cow<'a, T>`].
pub struct CowObserver<B, O> {
    inner: B,
    owned: Option<O>,
    mutated: bool,
}

impl<B, O> Deref for CowObserver<B, O> {
    type Target = B;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<B, O> DerefMut for CowObserver<B, O> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.owned = None;
        self.mutated = true;
        &mut self.inner
    }
}

impl<B, O, D> QuasiObserver for CowObserver<B, O>
where
    D: Unsigned,
    B: QuasiObserver<InnerDepth = Succ<D>>,
    B::Head: AsDeref<D>,
{
    type Head = B::Head;
    type OuterDepth = Succ<B::OuterDepth>;
    type InnerDepth = D;

    fn invalidate(this: &mut Self) {
        this.owned = None;
        this.mutated = true;
        B::invalidate(&mut this.inner);
    }
}

impl<'a, B, O, D, T> Observer for CowObserver<B, O>
where
    B: Observer<InnerDepth = Succ<D>>,
    B::Head: AsDeref<D, Target = Cow<'a, T>>,
    O: Observer<InnerDepth = Zero, Head = T::Owned>,
    D: Unsigned,
    T: ToOwned + ?Sized + 'a,
{
    unsafe fn observe(head: *mut Self::Head) -> Self {
        unsafe {
            let inner = B::observe(head);
            let target = head.as_deref_ptr::<D>();
            let owned = match &*target {
                Cow::Borrowed(_) => None,
                Cow::Owned(value) => Some(O::observe(
                    target.with_addr(value as *const _ as usize).cast(),
                )),
            };
            Pointer::set_unchecked(inner.as_deref_coinductive(), head);
            Self {
                inner,
                owned,
                mutated: false,
            }
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Self::Head) {
        unsafe {
            B::relocate(&mut this.inner, head);
            if let Some(owned) = &mut this.owned {
                let target = head.as_deref_ptr::<D>();
                match &*target {
                    Cow::Borrowed(_) => panic!("inconsistent state for CowObserver"),
                    Cow::Owned(value) => {
                        O::relocate(owned, target.with_addr(value as *const _ as usize).cast());
                    }
                }
            }
            Pointer::set_unchecked(this.inner.as_deref_coinductive(), head);
        }
    }
}
impl<B, O, Sk: Sink + ?Sized> QuasiSink<Sk> for CowObserver<B, O> {
    type Operation = Sk::Operation;
    type Identity = Sk::Identity;
}

impl<'a, B, O, D, T, Sk: Sink + ?Sized, Elem: ?Sized> FlushWith<Sk, Elem> for CowObserver<B, O>
where
    D: Unsigned,
    B: Observer<InnerDepth = Succ<D>> + Flush<Sk>,
    B::Head: AsDeref<D, Target = Cow<'a, T>>,
    O: Observer<InnerDepth = Zero, Head = T::Owned> + Flush<Sk>,
    T: ToOwned + ?Sized + 'a,
{
    fn flush_with<F>(this: &mut Self, sink: &mut Sk, _flush_elem: F)
    where
        F: FnMut(&mut Elem, &mut Sk),
    {
        <Self as Flush<Sk>>::flush(this, sink)
    }
}

impl<'a, B, O, D, T, Sk: Sink + ?Sized> Flush<Sk> for CowObserver<B, O>
where
    D: Unsigned,
    B: Observer<InnerDepth = Succ<D>> + Flush<Sk>,
    B::Head: AsDeref<D, Target = Cow<'a, T>>,
    O: Observer<InnerDepth = Zero, Head = T::Owned> + Flush<Sk>,
    T: ToOwned + ?Sized + 'a,
{
    fn flush(this: &mut Self, sink: &mut Sk) {
        if let Some(owned) = this.owned.as_mut()
            && !this.mutated
        {
            <B as Flush<Sk>>::flush(&mut this.inner, sink);
            <O as Flush<Sk>>::flush(owned, sink)
        } else {
            this.owned = None;
            this.mutated = false;
            <B as Flush<Sk>>::flush(&mut this.inner, sink)
        }
    }
}

impl<'a, B, O, T, D> CowObserver<B, O>
where
    D: Unsigned,
    B: Observer<InnerDepth = Succ<D>>,
    B::Head: AsDerefMut<D, Target = Cow<'a, T>>,
    O: Observer<InnerDepth = Zero, Head = T::Owned>,
    T: ToOwned + ?Sized + 'a,
{
    /// See [`Cow::to_mut`].
    pub fn to_mut(&mut self) -> &mut O {
        let head = unsafe { Pointer::as_mut(self.inner.as_deref_coinductive()) };
        let cow = AsDerefMut::<D>::as_deref_mut(head);
        let owned = cow.to_mut();
        self.owned
            .get_or_insert_with(|| unsafe { O::observe(owned) })
    }
}

impl<B, O> Debug for CowObserver<B, O>
where
    B: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CowObserver").field(&self.inner).finish()
    }
}

impl<B1, B2, O1, O2> PartialEq<CowObserver<B2, O2>> for CowObserver<B1, O1>
where
    B1: PartialEq<B2>,
{
    fn eq(&self, other: &CowObserver<B2, O2>) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl<B, O> Eq for CowObserver<B, O> where B: Eq {}

impl<B1, B2, O1, O2> PartialOrd<CowObserver<B2, O2>> for CowObserver<B1, O1>
where
    B1: PartialOrd<B2>,
{
    fn partial_cmp(&self, other: &CowObserver<B2, O2>) -> Option<std::cmp::Ordering> {
        self.inner.partial_cmp(&other.inner)
    }
}

impl<B, O> Ord for CowObserver<B, O>
where
    B: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.inner.cmp(&other.inner)
    }
}

impl<'a, 'ob, B, D, R> AddAssign<R> for CowObserver<B, StringObserver<'ob, String, Zero>>
where
    D: Unsigned,
    B: Observer<InnerDepth = Succ<D>>,
    B::Head: AsDerefMut<D, Target = Cow<'a, str>>,
    Cow<'a, str>: AddAssign<R>,
    R: Deref<Target = str> + Into<Cow<'a, str>>,
{
    fn add_assign(&mut self, rhs: R) {
        let head = unsafe { Pointer::as_mut(self.inner.as_deref_coinductive()) };
        let cow = AsDerefMut::<D>::as_deref_mut(head);
        if cow.is_empty() {
            self.mutated = true;
            B::invalidate(&mut self.inner);
            *cow = rhs.into();
        } else if !rhs.is_empty() {
            if let Cow::Borrowed(lhs) = cow {
                let mut s = String::with_capacity(lhs.len() + rhs.len());
                s.push_str(lhs);
                *cow = Cow::Owned(s);
            }
            self.to_mut().push_str(&rhs);
        }
    }
}

impl<'a, T> Observe for Cow<'a, T>
where
    T: ToOwned + RoObserve + ?Sized + 'a,
    T::Owned: Observe,
{
    type Observer<'ob, S, D>
        = CowObserver<
        T::Observer<'ob, S, Succ<D>>,
        <T::Owned as Observe>::Observer<'ob, T::Owned, Zero>,
    >
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

    type Spec = DefaultSpec;
}

impl<'a, T> RoObserve for Cow<'a, T>
where
    T: RoObserve + ToOwned + ?Sized + 'a,
{
    type Observer<'ob, S, D>
        = DerefObserver<T::Observer<'ob, S, Succ<D>>>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDeref<D, Target = Self> + ?Sized + 'ob;

    type Spec = T::Spec;
}

impl<'a, T> Snapshot for Cow<'a, T>
where
    T: Snapshot + ToOwned + ?Sized,
{
    type Snapshot = T::Snapshot;

    fn to_snapshot(&self) -> Self::Snapshot {
        (**self).to_snapshot()
    }
}

impl<'a, T> SerializeSnapshot for Cow<'a, T>
where
    T: SerializeSnapshot + ToOwned + ?Sized,
    Self::Snapshot: serde::Serialize + 'static,
{
    fn flush<S: Sink + ?Sized>(&self, snapshot: Self::Snapshot, sink: &mut S) {
        SerializeSnapshot::flush(&**self, snapshot, sink)
    }
}

#[cfg(test)]
mod tests {
    use muon_test_utils::*;
    use serde_json::json;

    use super::*;
    use crate::helper::QuasiObserver;
    use crate::observe::ObserveExt;

    #[test]
    fn no_change_returns_none() {
        let mut cow = Cow::Borrowed("hello");
        let mut ob = cow.__observe();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn replace_via_deref_mut() {
        let mut cow = Cow::Borrowed("hello");
        let mut ob = cow.__observe();
        *ob.tracked_mut() = Cow::Owned(String::from("world"));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "hello", "after": "world"}]),
        );
    }

    #[test]
    fn unsize_append() {
        const S: &str = "hello world";
        let mut cow = Cow::Borrowed(&S[..5]);
        let mut ob = cow.__observe();
        *ob.tracked_mut() = Cow::Borrowed(S);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "hello", "after": "hello world"}]),
        );
    }

    #[test]
    fn unsize_truncate() {
        const S: &str = "hello world";
        let mut cow = Cow::Borrowed(S);
        let mut ob = cow.__observe();
        *ob.tracked_mut() = Cow::Borrowed(&S[..5]);
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "hello world", "after": "hello"}]),
        );
    }

    #[test]
    fn to_mut_no_change() {
        let mut cow = Cow::Borrowed("hello");
        let mut ob = cow.__observe();
        ob.to_mut();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn to_mut_granular_tracking() {
        let mut cow = Cow::Borrowed("hello");
        let mut ob = cow.__observe();
        ob.to_mut().push_str(" world");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "hello", "after": "hello world"}]),
        );
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn replace_after_to_mut() {
        let mut cow = Cow::Borrowed("hello");
        let mut ob = cow.__observe();
        ob.to_mut().push_str(" world");
        *ob.tracked_mut() = Cow::Borrowed("replaced");
        let changes = __flush!(&mut ob);
        // The outer observer's snapshot was refreshed to "hello world" at
        // the first flush (the to_mut change is reported through the owned
        // observer), so it serves as the Replace.before.
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "hello world", "after": "replaced"}]),
        );
    }

    #[test]
    fn to_mut_after_replace() {
        let mut cow = Cow::Borrowed("hello");
        let mut ob = cow.__observe();
        *ob.tracked_mut() = Cow::Borrowed("replaced");
        ob.to_mut().push_str(" world");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "hello", "after": "replaced world"}]),
        );
    }

    #[test]
    fn owned_cow_no_change() {
        let mut cow: Cow<'_, str> = Cow::Owned(String::from("hello"));
        let mut ob = cow.__observe();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn owned_cow_replace() {
        let mut cow: Cow<'_, str> = Cow::Owned(String::from("hello"));
        let mut ob = cow.__observe();
        *ob.tracked_mut() = Cow::Owned(String::from("world"));
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "hello", "after": "world"}]),
        );
    }

    #[test]
    fn comparison_with_cow() {
        let mut cow = Cow::Borrowed("hello");
        let ob = cow.__observe();
        assert_eq!(*ob.untracked_ref(), Cow::Borrowed("hello"));
        assert_eq!(
            *ob.untracked_ref(),
            Cow::<str>::Owned(String::from("hello"))
        );
    }

    #[test]
    fn to_mut_truncate_then_append() {
        let mut cow: Cow<'_, str> = Cow::Owned(String::from("hello world"));
        let mut ob = cow.__observe();
        let s = ob.to_mut();
        s.truncate(5);
        s.push_str("!");
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "hello world", "after": "hello!"}]),
        );
    }

    #[test]
    fn add_assign_borrowed() {
        let mut cow = Cow::Borrowed("hello");
        let mut ob = cow.__observe();
        ob += " world";
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "hello", "after": "hello world"}]),
        );
    }

    #[test]
    fn add_assign_empty_lhs() {
        let mut cow = Cow::Borrowed("");
        let mut ob = cow.__observe();
        ob += "hello";
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "", "after": "hello"}]),
        );
    }

    #[test]
    fn add_assign_empty_rhs() {
        let mut cow = Cow::Borrowed("hello");
        let mut ob = cow.__observe();
        ob += "";
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }

    #[test]
    fn add_assign_owned() {
        let mut cow: Cow<'_, str> = Cow::Owned(String::from("hello"));
        let mut ob = cow.__observe();
        ob += " world";
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "hello", "after": "hello world"}]),
        );
    }

    #[test]
    fn add_assign_multiple() {
        let mut cow = Cow::Borrowed("a");
        let mut ob = cow.__observe();
        ob += "b";
        ob += "c";
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "a", "after": "abc"}]),
        );
    }

    #[test]
    fn relocate_provenance_mut() {
        let mut cow: Cow<'_, str> = Cow::Owned(String::from("hello"));
        let mut ob = cow.__observe();
        let mut new_cow: Cow<'_, str> = Cow::Owned(String::from("world"));
        unsafe { Observer::relocate(&mut ob, &mut new_cow) };
        *ob.tracked_mut() = Cow::Borrowed("replaced");
        assert_eq!(*ob.untracked_ref(), "replaced");
    }

    #[test]
    fn relocate_provenance_ref() {
        use std::collections::BTreeMap;
        let mut map =
            BTreeMap::from([(String::from("a"), Cow::<str>::Owned(String::from("hello")))]);
        let mut ob = map.__observe();
        ob.get_mut("a").unwrap().to_mut();
        let changes = __flush!(&mut ob);
        assert!(changes.is_empty());
    }
}
