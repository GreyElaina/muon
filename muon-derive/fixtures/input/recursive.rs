#[allow(unused_imports)]
use muon::Observe;
use serde::Serialize;

#[rustfmt::skip]
#[derive(Serialize, Observe)]
pub struct Rec {
    pub label: String,
    pub children: Vec<Rec>,
}

// `Vec<T>: Observe` requires `T: SerializeSnapshot` (the vec observer
// delegates element-level flushing to the element's snapshot
// machinery). Hand-written here, mirroring the `Qux` fixture.
impl ::muon::general::Snapshot for Rec {
    type Snapshot = serde_json::Value;

    fn to_snapshot(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("Rec serializes")
    }
}

impl ::muon::general::SerializeSnapshot for Rec {
    fn flush<S: ::muon::observe::Sink + ?Sized>(&self, snapshot: serde_json::Value, sink: &mut S) {
        let current = serde_json::to_value(self).expect("Rec serializes");
        if current != snapshot {
            sink.replace(
                Some(&snapshot as &dyn ::muon::erased_serde::Serialize),
                Some(&current as &dyn ::muon::erased_serde::Serialize),
            );
        }
    }
}
