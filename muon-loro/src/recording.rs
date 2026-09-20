use core::ops::{Deref, DerefMut};

use kernel::{
    AsDeref, Collect, Invalidate, Observer, Path, Pointer, QuasiObserver, State, StatefulObserver,
    Succ, Unsigned, Zero,
};

pub(crate) struct Recording<D>(Option<Vec<D>>);

/// Observer carrying Loro-specific operation recording state.
#[repr(transparent)]
pub struct RecordingObserver<Delta, Head: ?Sized, Depth = Zero> {
    inner: StatefulObserver<Recording<Delta>, Head, Depth>,
}

impl<Delta, Head: ?Sized, Depth> Deref for RecordingObserver<Delta, Head, Depth> {
    type Target = Pointer<Head>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<Delta, Head: ?Sized, Depth> DerefMut for RecordingObserver<Delta, Head, Depth>
where
    Depth: Unsigned,
    Head: AsDeref<Depth>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<Delta, Head: ?Sized, Depth> RecordingObserver<Delta, Head, Depth> {
    pub(crate) fn state_mut(&mut self) -> &mut Recording<Delta> {
        self.inner.state_mut()
    }
}

impl<Delta, Head: ?Sized, Depth> QuasiObserver for RecordingObserver<Delta, Head, Depth>
where
    Depth: Unsigned,
    Head: AsDeref<Depth>,
{
    type OuterDepth = Succ<Zero>;
    type InnerDepth = Depth;

    fn invalidate(this: &mut Self) {
        QuasiObserver::invalidate(&mut this.inner)
    }
}

unsafe impl<Delta, Head: ?Sized, Depth> Observer for RecordingObserver<Delta, Head, Depth>
where
    Depth: Unsigned,
    Head: AsDeref<Depth>,
    StatefulObserver<Recording<Delta>, Head, Depth>: Observer<Head = Head>,
{
    type Head = Head;

    unsafe fn observe(head: *mut Self::Head) -> Self {
        Self {
            inner: unsafe { Observer::observe(head) },
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Self::Head) {
        unsafe { Observer::relocate(&mut this.inner, head) }
    }

    unsafe fn rebase(this: &mut Self, head: *mut Self::Head) {
        unsafe { Observer::rebase(&mut this.inner, head) }
    }
}

impl<Delta, Head: ?Sized, Depth, Context: ?Sized, Route, Error, Scopes>
    Collect<Context, Route, Error, Scopes> for RecordingObserver<Delta, Head, Depth>
where
    StatefulObserver<Recording<Delta>, Head, Depth>: Collect<Context, Route, Error, Scopes>,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        Collect::<Context, Route, Error, Scopes>::collect(&mut self.inner, path, context)
    }
}

impl<D> Recording<D> {
    pub(crate) fn push(&mut self, delta: D) {
        if let Some(deltas) = &mut self.0 {
            deltas.push(delta);
        }
    }

    pub(crate) fn drain(&mut self) -> Option<std::vec::Drain<'_, D>> {
        self.0.as_mut().map(|deltas| deltas.drain(..))
    }
}

impl<D> Default for Recording<D> {
    fn default() -> Self {
        Self(Some(Vec::new()))
    }
}

impl<T: ?Sized, D> Invalidate<T> for Recording<D> {
    fn invalidate(&mut self, _: &T) {
        self.0 = None;
    }
}

impl<T: ?Sized, D> State<T> for Recording<D> {
    fn observe(_: &T) -> Self {
        Self::default()
    }

    fn rebase(&mut self, _: &T) {
        self.0.get_or_insert_default().clear();
    }
}
