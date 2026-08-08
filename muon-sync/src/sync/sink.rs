//! The sync layer's observation sink.
//!
//! [`SyncSink`] implements the core [`Sink`] protocol, encoding the
//! domain-agnostic event stream as a [`SyncChanges`] stream
//! ([`Changes<Edit, ItemId>`](crate::SyncChanges)): whole-value replaces
//! keep their before/after payloads, container operations become
//! [`Inplace`](muon::Changed::Inplace) edits, and element identities are
//! carried as [`PathSegment::Identity`](muon::PathSegment::Identity)
//! segments so nested paths survive serialization unchanged.
//!
//! The sink declares its production vocabulary as the associated types
//! [`Operation`](Sink::Operation) and [`Identity`](Sink::Identity); the protocol requires every
//! incoming payload to convert into that vocabulary via `Into`, which
//! the layer's `From` impls ([`From<Edit> for ()`](Edit),
//! [`From<ItemId> for ()`](ItemId)) also extend to the whole-value diff
//! domain.

use muon::observe::Sink;
use muon::{Change, Changed, Path, PathSegment};

use crate::types::{SyncChanges, Transaction, TxnId};
use crate::{Edit, ItemId};

/// Collects observation events into a sync-layer change stream.
///
/// The path stack mirrors the observer tree during flush; every event is
/// recorded at the current stack position. The resulting stream is the
/// sync domain's canonical mutation form: it serializes directly (the
/// stream is the wire form) and feeds `apply`, rebase and undo.
pub struct SyncSink {
    path: Vec<PathSegment<ItemId>>,
    changes: Vec<Change<Edit, ItemId>>,
}

impl SyncSink {
    /// Creates an empty sink.
    pub fn new() -> Self {
        Self {
            path: Vec::new(),
            changes: Vec::new(),
        }
    }

    /// Consumes the sink and returns the collected change stream.
    pub fn into_changes(self) -> SyncChanges {
        SyncChanges {
            inner: self.changes,
        }
    }
}

impl Default for SyncSink {
    fn default() -> Self {
        Self::new()
    }
}

impl Sink for SyncSink {
    type Operation = Edit;
    type Identity = ItemId;

    fn push_field(&mut self, name: &str) {
        self.path.push(PathSegment::String(name.to_owned()));
    }

    fn push_index(&mut self, index: usize) {
        self.path.push(PathSegment::Positive(index));
    }

    fn push_neg_index(&mut self, index: usize) {
        self.path.push(PathSegment::Negative(index));
    }

    fn push_identity<I: Into<Self::Identity>>(&mut self, id: I) {
        self.path.push(PathSegment::Identity(id.into()));
    }

    fn pop_segment(&mut self) {
        self.path.pop();
    }

    fn replace(
        &mut self,
        before: Option<&dyn erased_serde::Serialize>,
        after: Option<&dyn erased_serde::Serialize>,
    ) {
        let serialize = |v: &dyn erased_serde::Serialize| {
            serde_json::to_value(v).expect("serialization cannot fail")
        };
        self.changes.push(Change {
            path: Path::from(self.path.clone()),
            changed: Changed::Replace {
                before: before.map(serialize),
                after: after.map(serialize),
            },
        });
    }

    fn inplace<O: Into<Self::Operation>>(&mut self, op: O) {
        self.changes.push(Change {
            path: Path::from(self.path.clone()),
            changed: Changed::Inplace(op.into()),
        });
    }
}

// The sync vocabulary joins the whole-value diff domain: `()`-sinks
// (ObserveSink) absorb Edit ops and ItemId identities through these
// conversions, so models containing sync containers keep flushing into
// the core diff encoding (the store's local path).
impl From<Edit> for () {
    fn from(_: Edit) {}
}

impl From<ItemId> for () {
    fn from(_: ItemId) {}
}

/// Convert a sync-layer change stream into leaf transactions: each
/// change becomes one transaction with its path and kind carried over
/// directly. The stream's paths already carry item identities
/// ([`PathSegment::Identity`](muon::PathSegment::Identity));
/// `client_id` is stamped by the queue on enqueue.
pub(crate) fn txns_from_changes(
    changes: SyncChanges,
    model_id: &str,
    now: u64,
    ids: &mut impl FnMut() -> TxnId,
) -> Vec<Transaction> {
    changes
        .inner
        .into_iter()
        .map(|change| Transaction {
            id: ids(),
            client_id: 0,
            timestamp: now,
            kind: change.changed,
            model_id: model_id.to_owned(),
            path: change.path.into_vec(),
        })
        .collect()
}
