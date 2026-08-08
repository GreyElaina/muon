#[allow(unused_imports)]
use muon::Observe;
use serde::Serialize;
#[rustfmt::skip]
#[derive(Serialize)]
pub struct Foo<T> {
    a: T,
}
#[rustfmt::skip]
#[automatically_derived]
impl<T> ::muon::Observe for Foo<T>
where
    Self: ::muon::general::SerializeSnapshot + ::serde::Serialize,
{
    type Observer<'ob, S, N> = ::muon::general::ShallowObserver<'ob, Self, S, N>
    where
        Self: 'ob,
        N: ::muon::helper::Unsigned,
        S: ::muon::helper::AsDerefMut<N, Target = Self> + ?Sized + 'ob;
    type Spec = ::muon::observe::DefaultSpec;
}
#[rustfmt::skip]
#[derive(Serialize)]
pub struct Bar<T: ::serde::Serialize + 'static> {
    a: Vec<T>,
}
#[rustfmt::skip]
const _: () = {
    #[derive(Serialize)]
    pub struct BarSnapshot<T: ::serde::Serialize + 'static>
    where
        Vec<T>: ::muon::general::SerializeSnapshot,
    {
        a: <Vec<T> as ::muon::general::Snapshot>::Snapshot,
    }
    #[automatically_derived]
    impl<T: ::serde::Serialize + 'static> ::muon::general::Snapshot for Bar<T>
    where
        Vec<T>: ::muon::general::SerializeSnapshot,
    {
        type Snapshot = BarSnapshot<T>;
        fn to_snapshot(&self) -> Self::Snapshot {
            BarSnapshot {
                a: ::muon::general::Snapshot::to_snapshot(&self.a),
            }
        }
    }
    #[automatically_derived]
    impl<T: ::serde::Serialize + 'static> ::muon::general::SerializeSnapshot for Bar<T>
    where
        Vec<T>: ::muon::general::SerializeSnapshot,
        Self: ::serde::Serialize,
    {
        fn flush<S: ::muon::observe::Sink + ?Sized>(
            &self,
            snapshot: Self::Snapshot,
            sink: &mut S,
        ) {
            sink.push_field("a");
            ::muon::general::SerializeSnapshot::flush(&self.a, snapshot.a, sink);
            sink.pop_segment();
        }
    }
};
#[rustfmt::skip]
#[automatically_derived]
impl<T: ::serde::Serialize + 'static> ::muon::Observe for Bar<T>
where
    Self: ::muon::general::SerializeSnapshot + ::serde::Serialize,
{
    type Observer<'ob, S, N> = ::muon::general::SnapshotObserver<'ob, Self, S, N>
    where
        Self: 'ob,
        N: ::muon::helper::Unsigned,
        S: ::muon::helper::AsDerefMut<N, Target = Self> + ?Sized + 'ob;
    type Spec = ::muon::observe::SnapshotSpec;
}
#[rustfmt::skip]
#[derive(Serialize)]
pub struct NoopStruct {}
#[rustfmt::skip]
#[automatically_derived]
impl ::muon::general::Snapshot for NoopStruct {
    type Snapshot = ();
    fn to_snapshot(&self) {}
}
#[rustfmt::skip]
#[automatically_derived]
impl ::muon::general::SerializeSnapshot for NoopStruct {
    fn flush<S: ::muon::observe::Sink + ?Sized>(&self, _snapshot: (), _sink: &mut S) {
        {}
    }
}
#[rustfmt::skip]
#[automatically_derived]
impl ::muon::Observe for NoopStruct {
    type Observer<'ob, S, N> = ::muon::general::NoopObserver<'ob, Self, S, N>
    where
        Self: 'ob,
        N: ::muon::helper::Unsigned,
        S: ::muon::helper::AsDerefMut<N, Target = Self> + ?Sized + 'ob;
    type Spec = ::muon::observe::SnapshotSpec;
}
