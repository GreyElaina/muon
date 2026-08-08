// Add leading colons to std imports to avoid rustfmt inserting newlines
use ::std::ops::{Deref, DerefMut};
#[allow(unused_imports)]
use muon::Observe;
use serde::Serialize;

#[rustfmt::skip]
#[derive(Serialize, Observe)]
// FIXME: #[muon(derive(PartialEq))]
pub struct Foo<T> {
    #[muon(deref)]
    a: Vec<T>,
    b: i32,
}

impl<T> Deref for Foo<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.a
    }
}

impl<T> DerefMut for Foo<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.a
    }
}

#[rustfmt::skip]
#[derive(Serialize, Observe)]
pub struct Bar(#[muon(deref, shallow)] Qux, i32);

impl Deref for Bar {
    type Target = Qux;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Bar {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[rustfmt::skip]
#[derive(Serialize, Observe)]
pub struct Qux(#[muon(deref)] pub i32);

impl Deref for Qux {
    type Target = i32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Qux {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// `#[muon(shallow)]` observes the whole value: provide the snapshot
// machinery manually (the shallow observer requires it).
impl ::muon::general::Snapshot for Qux {
    type Snapshot = i32;

    fn to_snapshot(&self) -> i32 {
        self.0
    }
}

impl ::muon::general::SerializeSnapshot for Qux {
    fn flush<S: ::muon::observe::Sink + ?Sized>(&self, snapshot: i32, sink: &mut S) {
        if self.0 != snapshot {
            sink.replace(
                Some(&snapshot as &dyn ::muon::erased_serde::Serialize),
                Some(&self.0 as &dyn ::muon::erased_serde::Serialize),
            );
        }
    }
}

// Generic `deref` + `shallow`: the derived `Observe` impl must carry
// `T: SerializeSnapshot + Serialize` so the projection stays usable.
#[rustfmt::skip]
#[derive(Serialize, Observe)]
pub struct Baz<T> {
    #[muon(deref, shallow)]
    a: Vec<T>,
    b: i32,
}

impl<T> Deref for Baz<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.a
    }
}

impl<T> DerefMut for Baz<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.a
    }
}

// Single-field generic `deref`: the observer impls still need `'ob`
// for `Vec<T>: 'ob` even though the observer struct does not.
#[rustfmt::skip]
#[derive(Serialize, Observe)]
pub struct SingleDeref<T>(#[muon(deref)] Vec<T>);

impl<T> Deref for SingleDeref<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for SingleDeref<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// Single-field generic `deref` + `shallow`: same `'ob` need, plus the
// general-observer bounds on the `Observe` impl.
#[rustfmt::skip]
#[derive(Serialize, Observe)]
pub struct SingleShallowDeref<T>(#[muon(deref, shallow)] Vec<T>);

impl<T> Deref for SingleShallowDeref<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for SingleShallowDeref<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
