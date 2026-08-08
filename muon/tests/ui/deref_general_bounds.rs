// The derived `Observe` impl for a generic `deref` + `shallow` field
// must carry the general-observer bounds (`T: SerializeSnapshot +
// Serialize`). A type that serializes but lacks the snapshot machinery
// must be rejected, with the error naming `SerializeSnapshot` — not a
// deep failure inside the observer's internals.

use muon::Observe;
use serde::Serialize;

#[derive(Serialize)]
struct SerialOnly(i32);

#[derive(Serialize, Observe)]
struct Wrapper<T> {
    #[muon(deref, shallow)]
    inner: T,
    other: i32,
}

impl<T> std::ops::Deref for Wrapper<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> std::ops::DerefMut for Wrapper<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

fn assert_observe<T: Observe>() {}

fn main() {
    assert_observe::<Wrapper<SerialOnly>>();
}
