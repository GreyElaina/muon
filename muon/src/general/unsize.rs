use std::marker::PhantomData;

use crate::general::{DebugHandler, GeneralHandler, GeneralObserver, SerializeHandler};
use crate::helper::{AsDeref, Invalidate, Zero};
use crate::observe::Sink;

use crate::general::Snapshot;

pub type UnsizeObserver<'ob, S, D = Zero> =
    GeneralObserver<'ob, UnsizeHandler<<S as AsDeref<D>>::Target>, S, D>;

pub trait Unsize {
    type Slice: ?Sized;

    fn len(&self) -> usize;

    fn range_from(&self, from: usize) -> &Self::Slice;

    /// Compute the truncate unit count for the removed tail `[new_len..old_len]`.
    ///
    /// # Safety
    ///
    /// `ptr` must point to valid memory of at least `old_len` bytes, and
    /// `ptr[new_len..old_len]` must be a valid instance of the removed content.
    unsafe fn removed_len(ptr: *const u8, new_len: usize, old_len: usize) -> usize;
}

pub struct UnsizeHandler<T: ?Sized> {
    /// The pre-write snapshot, captured at the first mutable access.
    raw_parts: Option<serde_json::Value>,
    phantom: PhantomData<*const T>,
}

impl<T: ?Sized> Invalidate<T> for UnsizeHandler<T>
where
    T: Unsize + Snapshot,
    T::Snapshot: serde::Serialize + 'static,
{
    fn invalidate(&mut self, value: &T) {
        self.raw_parts.get_or_insert_with(|| {
            serde_json::to_value(value.to_snapshot()).expect("snapshot serializes")
        });
    }
}

impl<T: ?Sized> GeneralHandler for UnsizeHandler<T>
where
    T: Unsize + Snapshot,
    T::Snapshot: serde::Serialize + 'static,
{
    type Target = T;

    fn observe(_: &T) -> Self {
        Self {
            raw_parts: None,
            phantom: PhantomData,
        }
    }
}

impl<T: ?Sized> SerializeHandler for UnsizeHandler<T>
where
    T: Unsize<Slice: serde::Serialize> + serde::Serialize + Snapshot + 'static,
    T::Snapshot: serde::Serialize + 'static,
{
    fn flush<S: Sink + ?Sized>(&mut self, value: &T, sink: &mut S) {
        let Some(snapshot) = self.raw_parts.take() else {
            return;
        };
        sink.replace(
            Some(&snapshot),
            Some(&value as &dyn erased_serde::Serialize),
        );
    }
}

impl<T: Unsize + Snapshot + ?Sized> DebugHandler for UnsizeHandler<T>
where
    T::Snapshot: serde::Serialize + 'static,
{
    const NAME: &'static str = "UnsizeObserver";
}

#[cfg(test)]
mod test {
    use muon_test_utils::*;
    use serde_json::json;

    use crate::helper::QuasiObserver;
    use crate::observe::ObserveExt;

    #[test]
    fn test_str_ref_replace() {
        const A: &str = "hello world 1";
        const B: &str = "hello world 2";
        let mut a = &A[0..12];
        let mut ob = a.__observe();
        *ob.tracked_mut() = &B[0..12];
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "hello world ", "after": "hello world "}]),
        );
    }

    #[test]
    fn test_str_ref_eq() {
        const A: &str = "hello world";
        let mut a = A;
        let mut ob = a.__observe();
        *ob.tracked_mut() = &A[0..];
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "hello world", "after": "hello world"}]),
        );
    }

    #[test]
    fn test_str_ref_append() {
        const A: &str = "hello world";
        let mut a = &A[0..5];
        let mut ob = a.__observe();
        *ob.tracked_mut() = A;
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "hello", "after": "hello world"}]),
        );
    }

    #[test]
    fn test_str_ref_truncate() {
        const A: &str = "hello world";
        let mut a = A;
        let mut ob = a.__observe();
        *ob.tracked_mut() = &A[0..5];
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "hello world", "after": "hello"}]),
        );
    }

    #[test]
    fn test_str_ref_truncate_multi_byte() {
        const A: &str = "你好世界！";
        let mut a = A;
        let mut ob = a.__observe();
        // Truncate to "你好" (6 bytes), removing "世界！" (3 chars)
        *ob.tracked_mut() = &A[0..6];
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "你好世界！", "after": "你好"}]),
        );
    }

    #[test]
    fn test_str_ref_append_multi_byte() {
        const A: &str = "你好世界";
        let mut a = &A[0..6]; // "你好"
        let mut ob = a.__observe();
        *ob.tracked_mut() = A; // "你好世界"
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "你好", "after": "你好世界"}]),
        );
    }

    #[test]
    fn test_str_ref_replace_different_addr() {
        const A: &str = "你好";
        const B: &str = "世界";
        let mut a = A;
        let mut ob = a.__observe();
        *ob.tracked_mut() = B;
        let changes = __flush!(&mut ob);
        assert_eq!(
            changes.into_json(),
            json!([{"path": [], "before": "你好", "after": "世界"}]),
        );
    }
}
