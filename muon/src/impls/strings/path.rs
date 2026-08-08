use std::ffi::OsStr;
use std::marker::PhantomData;
use std::path::Path;

use crate::Observe;
use crate::general::{SerializeSnapshot, Snapshot};
use crate::helper::shallow::ShallowState;
use crate::helper::shallow::{
    ObserverState, SerializeObserverState, ShallowDelegate, shallow_observer,
};
use crate::helper::{AsDeref, AsDerefMut, Invalidate, Pointer, Unsigned, Zero};
use crate::impls::strings::os_str::OsStrObserver;
use crate::observe::{DefaultSpec, RoObserve, Sink};

shallow_observer! {
    /// Observer implementation for [`Path`].
    struct PathObserver<V>(pub(crate) Path, pub(crate) V);
}

impl<'ob, V, S: ?Sized, D> PathObserver<'ob, V, S, D>
where
    V: Invalidate<()> + Invalidate<Path> + Invalidate<OsStr>,
    D: Unsigned,
    S: AsDerefMut<D, Target = Path>,
{
    /// See [`Path::as_mut_os_str`].
    pub fn as_mut_os_str(&mut self) -> OsStrObserver<'_, ShallowDelegate<V>, OsStr> {
        let state = ShallowDelegate::new(&raw mut self.state);
        let os_str = (*self.ptr).as_deref_mut().as_mut_os_str();
        let ob = OsStrObserver {
            state,
            ptr: Pointer::new(os_str),
            phantom: PhantomData,
        };
        Pointer::register_state::<_, Zero>(&ob.ptr, &ob.state);
        ob
    }
}

pub struct PathRoObserverState {
    snapshot: Option<serde_json::Value>,
}

impl Invalidate<Path> for PathRoObserverState {
    fn invalidate(&mut self, value: &Path) {
        self.snapshot.get_or_insert_with(|| {
            serde_json::to_value(value.to_snapshot()).expect("snapshot serializes")
        });
    }
}

impl ObserverState<Path> for PathRoObserverState {
    fn observe(_: &Path) -> Self {
        Self { snapshot: None }
    }
}

impl<S: Sink + ?Sized> SerializeObserverState<Path, S> for PathRoObserverState {
    fn flush(&mut self, value: &Path, sink: &mut S) {
        let Some(snapshot) = self.snapshot.take() else {
            return;
        };
        sink.replace(
            Some(&snapshot),
            Some(&value as &dyn erased_serde::Serialize),
        )
    }
}

impl Observe for Path {
    type Observer<'ob, S, D>
        = PathObserver<'ob, ShallowState<Path>, S, D>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDerefMut<D, Target = Self> + ?Sized + 'ob;

    type Spec = DefaultSpec;
}

impl RoObserve for Path {
    type Observer<'ob, S, D>
        = PathObserver<'ob, PathRoObserverState, S, D>
    where
        Self: 'ob,
        D: Unsigned,
        S: AsDeref<D, Target = Self> + ?Sized + 'ob;

    type Spec = DefaultSpec;
}

impl Snapshot for Path {
    type Snapshot = Option<Box<str>>;

    fn to_snapshot(&self) -> Option<Box<str>> {
        self.to_str().map(|s| s.into())
    }
}

impl SerializeSnapshot for Path {
    fn flush<S: Sink + ?Sized>(&self, snapshot: Option<Box<str>>, sink: &mut S) {
        match (self.to_str(), snapshot) {
            (Some(s), Some(snapshot)) => SerializeSnapshot::flush(&s, snapshot, sink),
            (None, None) => {}
            (_, snapshot) => sink.replace(
                Some(&snapshot as &dyn erased_serde::Serialize),
                Some(&self as &dyn erased_serde::Serialize),
            ),
        }
    }
}
