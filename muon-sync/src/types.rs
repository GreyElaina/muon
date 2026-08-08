//! Shared wire types: transaction, element, and batch identities, plus
//! the transaction payload.
//!
//! These types cross every layer (containers, pipeline, application),
//! so they live outside the layer modules. `lib.rs` re-exports them at
//! the crate root.

use muon::{Changed, PathSegment};
use serde::{Deserialize, Serialize};

/// Session-unique transaction identifier.
///
/// A random incarnation (one per process start) plus a session-local
/// sequence number. Uniqueness comes from the incarnation's randomness:
/// a new process never reuses ids — even after a full cache wipe — so
/// the server's transaction-id deduplication can never mistake a new
/// transaction for a historical one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TxnId {
    /// Random session identity; generated once per process start.
    pub incarnation: u64,
    /// Session-local monotonic sequence number.
    pub seq: u64,
}

/// Identity of one element in an ordered sequence
/// ([`crate::crdt::MovableVec`]).
///
/// A separate type from [`TxnId`]: element ids are assigned to values
/// (characters, list items) at creation and never change, while
/// transaction ids identify operations. The client namespace is
/// stamped when a transaction's identities are refreshed (undo/redo);
/// the server validates the batch's `client_id` and never trusts
/// element payloads for ownership.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ItemId {
    /// Client that created the element.
    pub client_id: ClientId,
    /// Session that created it (the producing transaction's
    /// incarnation).
    pub incarnation: u64,
    /// Session-local monotonic sequence at creation.
    pub seq: u64,
}

impl From<&Transaction> for ItemId {
    fn from(txn: &Transaction) -> Self {
        ItemId {
            client_id: txn.client_id,
            incarnation: txn.id.incarnation,
            seq: txn.id.seq,
        }
    }
}

/// A contiguous run of element ids: `first` plus `len - 1` consecutive
/// sequences in the same client session. One multi-element insertion or
/// deletion carries a range instead of one id per element.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemRange {
    /// The first element id of the run.
    pub first: ItemId,
    /// Number of elements in the run.
    pub len: u32,
}

impl ItemRange {
    /// Iterate the run's element ids in order.
    pub fn iter(&self) -> impl Iterator<Item = ItemId> + '_ {
        (0..self.len).map(move |k| ItemId {
            client_id: self.first.client_id,
            incarnation: self.first.incarnation,
            seq: self.first.seq + u64::from(k),
        })
    }

    /// Whether the range contains the id.
    pub fn contains(&self, id: ItemId) -> bool {
        id.client_id == self.first.client_id
            && id.incarnation == self.first.incarnation
            && id.seq >= self.first.seq
            && id.seq < self.first.seq + u64::from(self.len)
    }
}

/// Client identifier used as a tiebreaker in last-writer-wins conflict resolution.
pub type ClientId = u64;

/// A batch identifier; monotonic within a session.
pub type BatchId = u64;

/// Identity of a batch on the wire.
///
/// `session` is the random process incarnation (see [`TxnId`]); it
/// separates two process generations that share a `client_id` (for
/// example a reinstall). `batch_id` is monotonic within the session,
/// and the recovered allocator never reuses a stored id, so a client's
/// batch order equals its creation order across crashes. The server
/// applies each client's batches in `batch_id` order and echoes the
/// full key back through [`crate::sync::DeltaPacket::applied_batch`];
/// a client only matches reports for its own `client_id` and `session`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BatchKey {
    /// Client that produced the batch.
    pub client_id: ClientId,
    /// Process incarnation that produced it.
    pub session: u64,
    /// Monotonic batch number within the session.
    pub batch_id: BatchId,
}

/// The current wall-clock time in milliseconds since the Unix epoch.
pub(crate) fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Server-assigned monotonically increasing sync identifier.
pub type SyncId = u64;

/// The sync layer's observation stream: `Edit` operations with
/// item-identity path segments (`[Identity(id), ...]`). The stream is
/// the sync domain's canonical mutation form — it serializes directly
/// (observation output is the wire form) and feeds apply, rebase and
/// undo.
pub type SyncChanges = muon::Changes<Edit, ItemId>;

/// A serializable path segment for [`Transaction`].
///
/// A reversible mutation unit ready for serialization and network transport.
///
/// The kind is the observation stream's [`Changed`] description with
/// `serde_json::Value` payloads: a whole-field replacement carries its
/// own `before` (the pre-write value, re-captured by rebase), so
/// LSE-style rebase updates the `before` inside [`Self::kind`] while
/// preserving the user's intended write.
///
/// Lifecycle is not stored here: the queue's containers (pending, in-flight,
/// completed) express it. A transaction is always a leaf mutation — batches
/// of leaves from one store publication are grouped by [`crate::sync::CommitBatch`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Transaction {
    /// Locally-unique identifier, allocated by the queue.
    pub id: TxnId,
    /// Client that produced this transaction.
    pub client_id: ClientId,
    /// Monotonic timestamp (local clock at creation time).
    pub timestamp: u64,
    /// The change this transaction carries: a whole-field replacement
    /// (`Replace` with `before`/`after`) or an ordered-sequence
    /// operation (`Inplace` with an [`Edit`](crate::crdt::Edit)).
    pub kind: Changed<Edit>,
    /// Logical model identifier (sync channel name).
    pub model_id: String,
    /// Path to the target field, root-to-leaf order.
    /// Empty vector means root-level mutation. Element identities use
    /// [`PathSegment::Identity`].
    pub path: Vec<PathSegment<ItemId>>,
}

impl Transaction {
    /// Refresh this transaction's identity: a new id and timestamp.
    /// The mutation itself is unchanged — like an access token renewed
    /// through its refresh token. A re-run must never reuse a
    /// historical id: the server deduplicates by transaction id.
    ///
    /// Creating identities are renewed with the operation: an
    /// `Insert`'s element range and a `Move`'s placement version
    /// start at the new transaction id (undo never resurrects a
    /// deleted id — a fresh insert creates fresh elements).
    /// Referencing identities (`Delete` targets, `Move`'s
    /// element/anchor) are kept: they point at existing elements.
    pub fn refresh(&self, now: u64, next_id: &mut impl FnMut() -> TxnId) -> Transaction {
        let id = next_id();
        let client_id = self.client_id;
        let mut txn = Transaction {
            id,
            timestamp: now,
            ..self.clone()
        };
        match &mut txn.kind {
            Changed::Inplace(Edit::Insert { range, .. }) => {
                *range = ItemRange {
                    first: ItemId {
                        client_id,
                        incarnation: id.incarnation,
                        seq: id.seq,
                    },
                    len: range.len,
                };
            }
            Changed::Inplace(Edit::Move { pos, .. }) => {
                *pos = ItemId {
                    client_id,
                    incarnation: id.incarnation,
                    seq: id.seq,
                };
            }
            _ => {}
        }
        txn
    }
}

use crate::crdt::Edit;
