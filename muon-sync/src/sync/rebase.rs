//! Reconcile engine: replay unsynced changes over an authoritative
//! server value.
//!
//! [`reconcile`] is the single reconciliation primitive shared by every
//! path that must rebuild the optimistic overlay: applying an inbound
//! delta, rejecting a change, or cancelling one. It takes the model's
//! authoritative value (owned by the adapter's remote) and the model's
//! unconfirmed changes, replays them in order, and returns the final
//! value plus the re-captured `before` for each replayed replacement.
//!
//! Rebase is sequential, mirroring LSE's `UpdateTransaction.rebase()`:
//! each replacement's new `before` is read from the value produced by
//! the previous replay, not from the delta's raw value — so a later
//! change is based on the earlier one's replayed result, and undoing it
//! restores exactly that. In-place operations are identity-addressed;
//! they merge exactly and need no re-capture.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::sync::ops::{apply_inplace_value, apply_txn_to_value, value_at_path};
use crate::{BatchId, BatchKey, Changed, Commit, Transaction, TxnId};

/// A server-originated delta packet containing one or more actions.
///
/// Corresponds to LSE's delta packet with sync actions I / V / U / A / C.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeltaPacket {
    /// Monotonically increasing sync identifier assigned by the server.
    pub sync_id: u64,
    /// Actions contained in this packet.
    pub actions: Vec<DeltaAction>,
    /// The batch whose effects are fully included at this `sync_id`.
    ///
    /// The server applies batches in receive order and reports each one
    /// here (one batch per packet); the client moves that batch from
    /// in-flight to awaiting with `sync_id` as its completion threshold.
    /// The full [`BatchKey`](crate::BatchKey) is echoed so the client
    /// can match reports for its own `client_id` and session — batch
    /// ids alone collide across clients.
    /// A resend that is deduplicated is reported through
    /// [`SendResponse::deduped_at`](crate::SendResponse) instead and
    /// never appears here again.
    pub applied_batch: Option<BatchKey>,
    /// Transactions rejected during application (permission, constraint).
    ///
    /// Rejected transactions do not change state, but the server still
    /// advances its sync id for the event (the sync id is an event
    /// cursor, not a state version): a rejection-only packet must be
    /// visible to the client that sent the batch, even when another
    /// client's poll drained it first.
    pub rejected: Vec<TxnId>,
}

/// A single action within a delta packet.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DeltaAction {
    /// Insert a new model (Create acknowledgment + broadcast).
    Insert { model_id: String },
    /// Full value replacement for a model.
    Value { model_id: String, value: Value },
    /// Partial update of a model.
    Update { model_id: String, value: Value },
    /// Archive / soft-delete a model.
    Archive { model_id: String },
    /// Clear / hard-delete a model.
    Clear { model_id: String },
    /// A structural operation on an ordered-sequence field: identity
    /// semantics (insert after an anchor, delete by element ids, move
    /// by placement version). The client applies it to its sequence
    /// state; it is the precise incremental form for sequence fields
    /// (their single-list arrays are never merge-patched).
    SeqOp {
        model_id: String,
        /// The sequence field's path (possibly through nested
        /// elements).
        path: Vec<crate::PathSegment<crate::ItemId>>,
        /// The operation to apply.
        kind: crate::Changed<crate::Edit>,
    },
}

/// Outcome of replaying a model's unsynced changes.
#[derive(Debug, Default)]
pub struct ReconcileOutcome {
    /// The final value: authoritative value plus every replayed
    /// unsynced change.
    pub value: Value,
    /// Re-captured `before` for each replayed replacement, addressed
    /// by (batch id, ordinal, transaction id) for the queue to write
    /// back.
    pub rebased: Vec<(BatchId, u32, Transaction)>,
}

/// Replay a model's unsynced changes over an authoritative value.
///
/// Sequential rebase (LSE `UpdateTransaction.rebase()`): each
/// replacement's `before` is re-captured from the value produced by
/// the previous replay, then the transaction's `kind` (user intent)
/// is applied. In-place operations are identity-addressed and merge
/// exactly; they need no re-capture. The model's changes arrive in
/// lifecycle order, so later changes see earlier ones' replayed
/// results.
pub fn reconcile(authoritative: &Value, unsynced: &[(BatchId, Commit)]) -> ReconcileOutcome {
    let mut value = authoritative.clone();
    let mut rebased = Vec::new();
    for (batch_id, commit) in unsynced {
        for txn in &commit.txns {
            if let Changed::Replace { .. } = &txn.kind {
                if let Some(orig) = value_at_path(&value, &txn.path) {
                    let mut t = txn.clone();
                    // Move the transaction's own `after` out instead
                    // of cloning it a second time (the initial `clone`
                    // already copied it).
                    t.kind = match t.kind {
                        Changed::Replace { after, .. } => Changed::Replace {
                            before: Some(orig),
                            after,
                        },
                        _ => unreachable!("guarded by the caller"),
                    };
                    rebased.push((*batch_id, commit.ordinal, t));
                }
            }
            match &txn.kind {
                Changed::Replace { .. } => apply_txn_to_value(&mut value, txn),
                // In-place (sequence) operations apply to the field's
                // sequence state directly (identity-addressed); a
                // failure means the field no longer exists in the
                // authoritative value and the op is dropped.
                Changed::Inplace(kind) => {
                    let _ = apply_inplace_value(&mut value, &txn.path, kind);
                }
            }
        }
    }
    ReconcileOutcome { value, rebased }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use crate::{Changed, Edit, ItemId, TxnId};

    fn txn(seq: u64, path: &[muon::PathSegment<ItemId>], kind: Changed<Edit>) -> Transaction {
        Transaction {
            id: TxnId {
                incarnation: 7,
                seq,
            },
            client_id: 1,
            timestamp: 1,
            kind,
            model_id: "test".into(),
            path: path.to_vec(),
        }
    }

    fn replace(value: serde_json::Value) -> Changed<Edit> {
        Changed::Replace {
            before: Some(serde_json::Value::Null),
            after: Some(value),
        }
    }

    fn field(name: &str) -> muon::PathSegment<ItemId> {
        muon::PathSegment::String(name.to_owned())
    }

    #[test]
    fn sequential_rebase_recaptures_before_in_order() {
        // Server base: x = 0. Local A: x = 1 (before 0). Local B:
        // x = 2 (before 1). Inbound delta: x = 5.
        let authoritative = json!({ "x": 5 });
        let a = Commit {
            ordinal: 0,
            txns: vec![txn(1, &[field("x")], replace(json!(1)))],
        };
        let b = Commit {
            ordinal: 1,
            txns: vec![txn(2, &[field("x")], replace(json!(2)))],
        };

        let outcome = reconcile(&authoritative, &[(1, a), (2, b)]);

        assert_eq!(outcome.value, json!({ "x": 2 }), "B's intent wins");
        // A.before = 5 (delta's value); B.before = 1 (A's replayed
        // result) — undoing B restores 1, not 5.
        assert_eq!(outcome.rebased.len(), 2);
        let Changed::Replace {
            before: a_before, ..
        } = &outcome.rebased[0].2.kind
        else {
            panic!("kind preserved");
        };
        assert_eq!(a_before, &Some(json!(5)));
        let Changed::Replace {
            before: b_before, ..
        } = &outcome.rebased[1].2.kind
        else {
            panic!("kind preserved");
        };
        assert_eq!(b_before, &Some(json!(1)));
    }

    #[test]
    fn append_replaces_after_delta() {
        // Authoritative array ["a"]; delta appended "c" → ["a", "c"].
        // A local unconfirmed push is a whole-array Replace (the
        // snapshot model has no append increment), so replay
        // overwrites the field with the user's intent — LWW.
        let authoritative = json!({ "items": ["a", "c"] });
        let push = Commit {
            ordinal: 0,
            txns: vec![txn(1, &[field("items")], replace(json!(["a", "b"])))],
        };
        let outcome = reconcile(&authoritative, &[(1, push)]);
        assert_eq!(
            outcome.value,
            json!({ "items": ["a", "b"] }),
            "the local replace intent wins (LWW)",
        );
    }

    #[test]
    fn root_replace_replays() {
        let authoritative = json!({ "title": "Server" });
        let local = Commit {
            ordinal: 0,
            txns: vec![txn(1, &[field("title")], replace(json!("Local")))],
        };
        let outcome = reconcile(&authoritative, &[(1, local)]);
        assert_eq!(outcome.value, json!({ "title": "Local" }));
        let Changed::Replace { before, .. } = &outcome.rebased[0].2.kind else {
            panic!("kind preserved");
        };
        assert_eq!(before, &Some(json!("Server")));
    }
}
