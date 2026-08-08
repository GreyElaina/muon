//! Sync engine over muon's observation stream.
#![allow(rustdoc::private_intra_doc_links)]
//!
//! The crate is organized in four layers:
//!
//! - **Shared wire types** — transaction, element and
//!   batch identities, plus the [`Transaction`] payload. These cross
//!   every layer and are re-exported at the crate root.
//! - **Sequence containers** — [`CrdtVec`] and
//!   [`CrdtString`]: identity-addressed ordered sequences over the
//!   arena B+ tree ([`MovableVec`], plus the identity operations it
//!   exports).
//! - **Sync pipeline** — [`SyncSink`] implements the core
//!   [`Sink`](muon::observe::Sink) protocol. A flush turns observation events into a
//!   [`SyncChanges`] stream: whole-value replaces keep their
//!   before/after payloads, container operations become
//!   identity-addressed [`Edit`]s. The stream is the sync domain's
//!   canonical mutation form — it serializes directly (observation
//!   output is the wire form) and feeds apply, rebase and undo. The
//!   pipeline continues with the queue ([`TransactionQueue`]), client
//!   ([`SyncDriver`] / [`SyncChannel`]), server ([`SyncServer`]),
//!   transport ([`SyncTransport`]), crash recovery ([`RedbCache`]) and
//!   rebase ([`reconcile`]).
//! - **Application layer** — application undo
//!   ([`UndoStack`]) over the inversion rules of the
//!   undo module.

// Shared wire types
mod types;

// Sequence containers and their engine
mod crdt;

// Sync pipeline
mod sync;

// Application layer
mod undo;

#[cfg(test)]
mod test;

// Shared wire types
pub use types::{
    BatchId, BatchKey, ClientId, ItemId, ItemRange, SyncChanges, SyncId, Transaction, TxnId,
};

// Sequence containers and their engine
pub use crdt::{
    delete_by_id, insert_after, move_after, update_value, CrdtString, CrdtStringObserver, CrdtVec,
    CrdtVecObserver, Edit, MovableVec, Segment, SeqNode,
};

// Sync pipeline
pub use sync::{
    reconcile, sync_loop, sync_step, AwaitingCommit, CancelOutcome, Commit, CommitBatch,
    DeltaAction, DeltaApplyError, DeltaPacket, NoopTransport, PollOutcome, ReconcileOutcome,
    RedbCache, RemoteView, SendError, SendResponse, SyncChannel, SyncClient, SyncCommand,
    SyncDriver, SyncLoopError, SyncServer, SyncSink, SyncTransport, SyncWriteError,
    TransactionCache, TransactionQueue, WriteOutcome, DEFAULT_IN_FLIGHT_MAX,
};

// Application layer
pub use undo::UndoStack;

// Core re-exports
pub use muon::{Change, Changed, Changes, Path, PathSegment};

/// Flush an observer into a sync-layer change stream (test helper).
#[doc(hidden)]
#[macro_export]
macro_rules! __sync_flush {
    ($ob:expr) => {{
        let mut __sink = $crate::SyncSink::new();
        ::muon::observe::Flush::flush($ob, &mut __sink);
        __sink.into_changes()
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use muon::Path;
    use serde_json::json;

    fn ids() -> impl FnMut() -> TxnId {
        let mut next = 1u64;
        move || {
            let id = TxnId {
                incarnation: 1,
                seq: next,
            };
            next += 1;
            id
        }
    }

    /// A change stream lowers to one transaction per change, with the
    /// path and kind carried over directly.
    #[test]
    fn change_stream_lowers_to_transactions() {
        let changes: SyncChanges = Changes {
            inner: vec![Change {
                path: Path::from(vec![PathSegment::String("name".into())]),
                changed: Changed::Replace {
                    before: Some(json!("Alice")),
                    after: Some(json!("Bob")),
                },
            }],
        };

        let txns = crate::sync::sink::txns_from_changes(changes, "person", 1000, &mut ids());

        assert_eq!(txns.len(), 1);
        assert_eq!(
            txns[0].kind,
            Changed::Replace {
                before: Some(json!("Alice")),
                after: Some(json!("Bob")),
            }
        );
        assert_eq!(txns[0].model_id, "person");
        assert_eq!(txns[0].id.seq, 1);
        assert_eq!(txns[0].path, vec![PathSegment::String("name".to_owned())]);
    }

    /// An empty stream produces no transactions.
    #[test]
    fn empty_stream_produces_no_transactions() {
        let txns =
            crate::sync::sink::txns_from_changes(SyncChanges::new(), "person", 1000, &mut ids());
        assert!(txns.is_empty());
    }

    /// An in-place operation passes through without materialization.
    #[test]
    fn inplace_operation_passes_through() {
        let anchor = ItemId {
            client_id: 1,
            incarnation: 1,
            seq: 5,
        };
        let changes: SyncChanges = Changes {
            inner: vec![Change {
                path: Path::new(),
                changed: Changed::Inplace(Edit::Insert {
                    anchor: Some(anchor),
                    range: ItemRange {
                        first: ItemId {
                            client_id: 1,
                            incarnation: 1,
                            seq: 1,
                        },
                        len: 1,
                    },
                    value: Box::new(json!("x")),
                }),
            }],
        };

        let txns = crate::sync::sink::txns_from_changes(changes, "person", 1000, &mut ids());
        assert_eq!(txns.len(), 1);
        match &txns[0].kind {
            Changed::Inplace(Edit::Insert {
                anchor: a, range, ..
            }) => {
                assert_eq!(*a, Some(anchor));
                assert_eq!(range.len, 1);
            }
            other => panic!("unexpected kind: {other:?}"),
        }
    }
}
