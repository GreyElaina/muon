#[allow(unused_imports)]
use muon::Observe;
use serde::Serialize;

#[rustfmt::skip]
#[derive(Serialize, Observe)]
#[muon(shallow)]
pub struct Foo<T> {
    a: T,
}

#[rustfmt::skip]
#[derive(Serialize, Observe)]
#[muon(snapshot)]
pub struct Bar<T: ::serde::Serialize + 'static> {
    a: Vec<T>,
}

#[rustfmt::skip]
#[derive(Serialize, Observe)]
pub struct NoopStruct {}
