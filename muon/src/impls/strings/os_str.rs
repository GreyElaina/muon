use std::ffi::OsStr;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;

use crate::Observe;
use crate::general::{SerializeSnapshot, Snapshot};
use crate::helper::macros::delegate_methods;
use crate::helper::shallow::ShallowState;
use crate::helper::shallow::{ObserverState, SerializeObserverState, shallow_observer};
use crate::helper::{AsDeref, AsDerefMut, Invalidate, QuasiObserver, Unsigned};
use crate::observe::{DefaultSpec, RoObserve, Sink};

#[cfg(unix)]
pub(super) fn os_str_len(value: &OsStr) -> usize {
    value.as_bytes().len()
}

#[cfg(windows)]
pub(super) fn os_str_len(value: &OsStr) -> usize {
    value.encode_wide().count()
}

shallow_observer! {
    /// Observer implementation for [`OsStr`].
    struct OsStrObserver<V>(pub(crate) OsStr, pub(crate) V);
}

impl<'ob, V, S: ?Sized, D> OsStrObserver<'ob, V, S, D>
where
    V: Invalidate<OsStr>,
    D: Unsigned,
    S: AsDerefMut<D, Target = OsStr>,
{
    fn nonempty_mut(&mut self) -> &mut OsStr {
        if (*self).untracked_ref().is_empty() {
            self.untracked_mut()
        } else {
            self.tracked_mut()
        }
    }

    delegate_methods! { nonempty_mut() as OsStr =>
        pub fn make_ascii_uppercase(&mut self);
        pub fn make_ascii_lowercase(&mut self);
    }
}

pub struct OsStrRoObserverState {
    snapshot: Option<serde_json::Value>,
}

impl Invalidate<OsStr> for OsStrRoObserverState {
    fn invalidate(&mut self, value: &OsStr) {
        self.snapshot.get_or_insert_with(|| {
            serde_json::to_value(value.to_snapshot()).expect("snapshot serializes")
        });
    }
}

impl ObserverState<OsStr> for OsStrRoObserverState {
    fn observe(_: &OsStr) -> Self {
        Self { snapshot: None }
    }
}

impl<S: Sink + ?Sized> SerializeObserverState<OsStr, S> for OsStrRoObserverState {
    fn flush(&mut self, value: &OsStr, sink: &mut S) {
        let Some(snapshot) = self.snapshot.take() else {
            return;
        };
        sink.replace(
            Some(&snapshot),
            Some(&value as &dyn erased_serde::Serialize),
        )
    }
}

impl Observe for OsStr {
    type Observer<'ob, S, D>
        = OsStrObserver<'ob, ShallowState<OsStr>, S, D>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

    type Spec = DefaultSpec;
}

impl RoObserve for OsStr {
    type Observer<'ob, S, D>
        = OsStrObserver<'ob, OsStrRoObserverState, S, D>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDeref<D, Target = Self> + ?Sized + 'ob;

    type Spec = DefaultSpec;
}

impl Snapshot for OsStr {
    #[cfg(unix)]
    type Snapshot = Box<[u8]>;
    #[cfg(windows)]
    type Snapshot = Box<[u16]>;

    fn to_snapshot(&self) -> Self::Snapshot {
        #[cfg(unix)]
        return self.as_bytes().into();
        #[cfg(windows)]
        return self.encode_wide().collect();
    }
}

impl SerializeSnapshot for OsStr {
    fn flush<S: Sink + ?Sized>(&self, snapshot: Self::Snapshot, sink: &mut S) {
        #[cfg(unix)]
        {
            sink.push_field("Unix");
            SerializeSnapshot::flush(&self.to_snapshot(), snapshot, sink);
            sink.pop_segment();
        }
        #[cfg(windows)]
        {
            sink.push_field("Windows");
            SerializeSnapshot::flush(&self.to_snapshot(), snapshot, sink);
            sink.pop_segment();
        }
    }
}
