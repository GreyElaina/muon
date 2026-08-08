//! Sans-I/O server core: the authoritative state machine behind one
//! synchronized model set.
//!
//! One [`SyncServer`] instance is one entity in the LSE sense: it owns
//! the authoritative model values, the sync watermark, the
//! transaction-dedup table, and the delta history of a single
//! synchronized model set. Entities are independent — no operation
//! crosses from one server instance into another. Scale comes from the
//! number of entities (one instance per document), not from the size of
//! any one instance.
//!
//! The core is Sans-I/O: it is a pure state machine with synchronous
//! methods (`send` / `poll`). A transport (HTTP, WebSocket, a test
//! stub) wraps it and calls these methods; the core never touches the
//! network.
//!
//! # Server contract
//!
//! - `send` delivers a batch and returns immediately. Application
//!   happens on the next `poll`, in batch-id order per client session
//!   (a contiguous prefix per session; a gap buffers that session
//!   without stalling the others). A resend of an already-applied
//!   batch is answered with [`SendResponse::deduped_at`] carrying the
//!   original completion threshold. Deduplication is keyed by
//!   `(client, transaction)` — batch and transaction ids alone
//!   collide across clients.
//! - `poll(Some(since))` applies the buffered prefixes, broadcasts
//!   one delta per applied batch (RFC 7396 merge patch from the last
//!   broadcast value), and returns every retained delta past `since`
//!   in ascending sync id, or [`PollOutcome::ResetRequired`] when
//!   `since` fell out of the retention window.
//! - `poll(None)` is the bootstrap [`PollOutcome::Snapshot`]: the
//!   complete value of every model at the current sync id, plus
//!   report-only packets for the batches applied during this poll
//!   (their `rejected` and `applied_batch` reports must not be lost;
//!   they carry no state actions — the snapshot already includes
//!   their effects).
//! - Deltas are retained for `retention` packets (0 = forever). The
//!   client must advance its anchor often enough to stay inside the
//!   window; a client whose anchor fell out of the window receives
//!   [`PollOutcome::ResetRequired`] and must bootstrap with `None`.
//! - Rejected transactions are reported through
//!   [`DeltaPacket::rejected`]; they are not applied, but they still
//!   advance the sync id — the sync id is an event cursor, not a
//!   state version, so a rejection-only packet stays visible to the
//!   client that sent the batch even when another client's poll
//!   drained it first. Rejected ids are not added to the dedup table,
//!   so a resend is rejected again. Rejection policy is application
//!   logic (permissions, quotas); inject it with
//!   [`SyncServer::with_reject`].
//!
//! # Deployment notes
//!
//! The `pending` queue is an in-memory reference implementation of
//! "delivered but not yet applied". A stateless deployment (e.g.
//! serverless) can move it to a message queue: drain it outside the
//! core and feed each batch through the same apply path that `poll`
//! uses. The core keeps all its state as plain data — a
//! `Serialize`/`Deserialize` derive on this struct is the only step
//! needed to snapshot and restore an entity (for migration or cold
//! start). The rejection policy is assembly-time code, not state: it
//! is re-injected on restore.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use serde_json::{Map, Value};

use crate::sync::ops::{apply_inplace_value, apply_txn_to_value};
use crate::{
    BatchId, BatchKey, Changed, ClientId, DeltaAction, DeltaPacket, Edit, PollOutcome,
    SendResponse, SyncId, Transaction, TxnId,
};

/// The authoritative server state machine for one synchronized model set.
pub struct SyncServer {
    models: HashMap<String, Value>,
    /// The next sync id to assign; `last_sync_id()` is this minus one.
    next_sync_id: SyncId,
    /// Retained delta history, ascending sync id, clipped to `retention`.
    deltas: VecDeque<DeltaPacket>,
    /// (client, transaction) pairs already applied; the dedup key for
    /// resends. The client namespace is structural: batch ids and
    /// transaction ids alone collide across clients.
    seen: HashSet<(ClientId, TxnId)>,
    /// The sync id at which each (client, transaction) was first
    /// applied: the original completion threshold returned on a
    /// deduplicated resend.
    first_sync: HashMap<(ClientId, TxnId), SyncId>,
    /// Delivered-but-unapplied batches, indexed per client session and
    /// ordered by batch id. Poll applies each session's contiguous
    /// prefix — a gap buffers without stalling other sessions.
    pending: HashMap<(ClientId, u64), BTreeMap<BatchId, Vec<Transaction>>>,
    /// The next batch id each session must apply to continue its
    /// contiguous prefix.
    next_expected: HashMap<(ClientId, u64), BatchId>,
    /// The full model value at the last broadcast: the diff baseline
    /// for the next merge patch.
    last_broadcast: HashMap<String, Value>,
    /// How many delta packets to retain; 0 = unlimited.
    retention: usize,
    /// Whether `clip_history` ever dropped a packet: only then can an
    /// anchor fall out of the window. Without clipping, every anchor
    /// is valid (including 0, after an empty-server snapshot).
    clipped: bool,
    /// Application-time rejection policy (assembly-time code, not state).
    reject: Box<dyn Fn(&Transaction) -> bool + Send + Sync>,
    /// How many transactions were actually applied (dedup excluded).
    applied: u64,
}

impl SyncServer {
    /// Create an empty server: no models, unlimited delta retention,
    /// no rejections.
    pub fn new() -> Self {
        Self {
            models: HashMap::new(),
            next_sync_id: 1,
            deltas: VecDeque::new(),
            seen: HashSet::new(),
            first_sync: HashMap::new(),
            pending: HashMap::new(),
            next_expected: HashMap::new(),
            last_broadcast: HashMap::new(),
            retention: 0,
            clipped: false,
            reject: Box::new(|_| false),
            applied: 0,
        }
    }

    /// Bound the retained delta history to the newest `n` packets.
    /// A client whose anchor fell out of the window must bootstrap
    /// from 0 again.
    pub fn with_retention(mut self, n: usize) -> Self {
        self.retention = n;
        self
    }

    /// Inject the rejection policy. A transaction for which the policy
    /// returns `true` is not applied and is reported through
    /// [`DeltaPacket::rejected`]. The policy is application logic
    /// (permissions, quotas); the core only provides the mechanism.
    pub fn with_reject<F>(mut self, reject: F) -> Self
    where
        F: Fn(&Transaction) -> bool + Send + Sync + 'static,
    {
        self.reject = Box::new(reject);
        self
    }

    /// Seed an initial model value (e.g. a document loaded from
    /// storage). The first broadcast of the model diffs from an empty
    /// baseline, so the whole value is delivered.
    pub fn seed(&mut self, model_id: &str, value: Value) {
        self.models.insert(model_id.to_owned(), value);
    }

    /// Accept a delivered batch. Returns immediately — application
    /// happens on the next `poll`. A resend whose transactions are all
    /// already applied is answered with the original completion
    /// threshold instead.
    pub fn send(&mut self, batch_key: BatchKey, txns: &[Transaction]) -> SendResponse {
        if txns
            .iter()
            .all(|t| self.seen.contains(&(batch_key.client_id, t.id)))
        {
            let sid = txns
                .iter()
                .filter_map(|t| self.first_sync.get(&(batch_key.client_id, t.id)).copied())
                .max()
                .unwrap_or(0);
            return SendResponse {
                deduped_at: Some(sid),
            };
        }
        let session = (batch_key.client_id, batch_key.session);
        self.next_expected.entry(session).or_insert(1);
        self.pending
            .entry(session)
            .or_default()
            .insert(batch_key.batch_id, txns.to_vec());
        SendResponse { deduped_at: None }
    }

    /// Apply every session's contiguous batch prefix in batch-id order,
    /// then answer the poll. A gap in a session's prefix buffers that
    /// session without stalling the others (receive-window shape: the
    /// network stays concurrent, application stays ordered).
    ///
    /// Cross-session order is the lamport total order
    /// `(client_id, session, batch_id)` — the same order every site
    /// would apply the operations in — so concurrent batches converge
    /// regardless of arrival order. Batches never interleave across
    /// sessions: the client id is the primary key.
    pub fn poll(&mut self, since: Option<SyncId>) -> PollOutcome {
        let mut reports = Vec::new();
        let mut sessions: Vec<(ClientId, u64)> = self.next_expected.keys().copied().collect();
        sessions.sort_unstable();
        for session in sessions {
            loop {
                let expected = self.next_expected[&session];
                let Some(txns) = self
                    .pending
                    .get_mut(&session)
                    .and_then(|m| m.remove(&expected))
                else {
                    break; // gap: the contiguous prefix is interrupted
                };
                let batch_id = expected;
                if let Some(packet) = self.apply_batch(
                    BatchKey {
                        client_id: session.0,
                        session: session.1,
                        batch_id,
                    },
                    txns,
                ) {
                    reports.push(packet);
                }
                self.next_expected.insert(session, expected + 1);
            }
        }
        self.pending.retain(|_, batches| !batches.is_empty());

        match since {
            Some(since) => {
                // Only a clipped window can lose an anchor: without
                // clipping every anchor is valid, including 0 right
                // after an empty-server snapshot (0 < 1, but nothing
                // was dropped).
                if self.clipped {
                    if let Some(first) = self.deltas.front() {
                        if since < first.sync_id {
                            return PollOutcome::ResetRequired;
                        }
                    }
                }
                PollOutcome::Deltas(
                    self.deltas
                        .iter()
                        .filter(|d| d.sync_id > since)
                        .cloned()
                        .collect(),
                )
            }
            None => {
                let sync_id = self.last_sync_id();
                let models = self
                    .models
                    .iter()
                    .map(|(mid, value)| (mid.clone(), value.clone()))
                    .collect();
                // The snapshot already contains every applied batch's
                // effect, so the accompanying reports are report-only:
                // bookkeeping (`rejected`, `applied_batch`) without
                // state actions, which the client must not apply twice.
                let reports = reports
                    .into_iter()
                    .map(|mut p| {
                        p.actions.clear();
                        p
                    })
                    .collect();
                PollOutcome::Snapshot {
                    sync_id,
                    models,
                    reports,
                }
            }
        }
    }

    /// Apply one batch at the event cursor: applied leaves and
    /// rejected leaves both advance the sync id (it is an event
    /// cursor, not a state version), so a rejection-only packet stays
    /// visible to the client that sent the batch even when another
    /// client's poll drained it first. Returns `None` when the batch
    /// was fully deduplicated (no event occurred).
    fn apply_batch(&mut self, batch_key: BatchKey, txns: Vec<Transaction>) -> Option<DeltaPacket> {
        let mut applied_ids = Vec::new();
        let mut rejected_ids = Vec::new();
        for txn in &txns {
            if txn.client_id != batch_key.client_id {
                rejected_ids.push(txn.id); // namespace mismatch: never apply
                continue;
            }
            if self.seen.contains(&(batch_key.client_id, txn.id)) {
                continue; // already applied; a partial resend
            }
            if (self.reject)(txn) {
                rejected_ids.push(txn.id);
                continue;
            }
            let state = match self.models.get_mut(&txn.model_id) {
                Some(state) => state,
                None => self
                    .models
                    .entry(txn.model_id.clone())
                    .or_insert_with(|| Value::Object(Map::new())),
            };
            // In-place operations apply to the field's container
            // state: sequence ops to the single-list array, text
            // ops to the text container's export (parse-or-err:
            // the identity semantics begin with the first
            // structural operation; a missing field is
            // established as an empty container). A malformed
            // operation is rejected, never confirmed: confirming
            // it would silently drop the change.
            let applied = match &txn.kind {
                Changed::Inplace(kind) => apply_inplace_value(state, &txn.path, kind).is_ok(),
                Changed::Replace { .. } => {
                    apply_txn_to_value(state, txn);
                    true
                }
            };
            if !applied {
                rejected_ids.push(txn.id);
                continue;
            }
            self.seen.insert((batch_key.client_id, txn.id));
            self.applied += 1;
            applied_ids.push(txn.id);
        }
        if applied_ids.is_empty() && rejected_ids.is_empty() {
            return None; // fully deduplicated; `send` answered already
        }
        let sync_id = self.next_sync_id;
        self.next_sync_id += 1;
        for id in &applied_ids {
            self.first_sync.insert((batch_key.client_id, *id), sync_id);
        }
        // Broadcast: one merge patch per model touched by plain
        // transactions, plus one SeqOp action per applied structural
        // operation (sequence fields are never merge-patched — the
        // operation itself is the precise incremental form). The
        // applied set is keyed by id so a duplicate id in the batch
        // (the second copy was rejected above) is never broadcast.
        let applied: std::collections::HashSet<TxnId> = applied_ids.iter().copied().collect();
        let mut actions = Vec::new();
        let mut touched: Vec<String> = Vec::new();
        // Broadcast each distinct txn id once: a duplicate id in the
        // batch is skipped by the `seen` set (not rejected), so the
        // per-txn `applied` match would otherwise double the action.
        let mut broadcast: std::collections::HashSet<TxnId> = std::collections::HashSet::new();
        for txn in &txns {
            if applied.contains(&txn.id) && broadcast.insert(txn.id) {
                if is_structural(&txn.kind) {
                    actions.push(DeltaAction::SeqOp {
                        model_id: txn.model_id.clone(),
                        path: txn.path.clone(),
                        kind: txn.kind.clone(),
                    });
                } else if !touched.contains(&txn.model_id) {
                    touched.push(txn.model_id.clone());
                }
            }
        }
        for mid in touched {
            let current = self.models.get(&mid).unwrap().clone();
            let previous = self
                .last_broadcast
                .entry(mid.clone())
                .or_insert(Value::Null);
            let patch = merge_patch_diff(previous, &current);
            *previous = current;
            actions.push(DeltaAction::Update {
                model_id: mid,
                value: patch,
            });
        }
        let packet = DeltaPacket {
            sync_id,
            actions,
            applied_batch: Some(batch_key),
            rejected: rejected_ids,
        };
        self.deltas.push_back(packet.clone());
        self.clip_history();
        Some(packet)
    }

    /// Simulate another actor clearing a model: remove it and broadcast
    /// a `Clear` delta. The diff baseline is reset, so a later recreate
    /// diffs from an empty model.
    pub fn clear_model(&mut self, model_id: &str) {
        self.models.remove(model_id);
        self.last_broadcast.remove(model_id);
        let sync_id = self.next_sync_id;
        self.next_sync_id += 1;
        self.deltas.push_back(DeltaPacket {
            sync_id,
            actions: vec![DeltaAction::Clear {
                model_id: model_id.to_owned(),
            }],
            applied_batch: None,
            rejected: vec![],
        });
        self.clip_history();
    }

    /// The number of transactions actually applied (dedup excluded).
    pub fn applied_count(&self) -> u64 {
        self.applied
    }

    /// The current authoritative value of a model.
    pub fn model(&self, model_id: &str) -> Option<&Value> {
        self.models.get(model_id)
    }

    /// The server's current `lastSyncId`.
    pub fn last_sync_id(&self) -> SyncId {
        self.next_sync_id - 1
    }

    fn clip_history(&mut self) {
        if self.retention > 0 {
            while self.deltas.len() > self.retention {
                self.deltas.pop_front();
                self.clipped = true;
            }
        }
    }
}

/// Whether a transaction kind is a sequence (structural) operation:
/// an in-place operation applies to the sequence state; a whole-field
/// replacement applies to the plain value at the path.
fn is_structural(kind: &Changed<Edit>) -> bool {
    matches!(kind, Changed::Inplace(_))
}

impl Default for SyncServer {
    fn default() -> Self {
        Self::new()
    }
}

/// Build the RFC 7396 merge patch from `prev` to `next`.
///
/// Objects are diffed key by key: unchanged members are omitted, added
/// or changed members carry the new value, removed members become
/// `null`. Non-object values compare equal or replace.
fn merge_patch_diff(prev: &Value, next: &Value) -> Value {
    match (prev, next) {
        (Value::Object(prev_obj), Value::Object(next_obj)) => {
            let mut patch = Map::new();
            for (k, v) in next_obj {
                match prev_obj.get(k) {
                    Some(pv) if pv != v => {
                        let sub = merge_patch_diff(pv, v);
                        // An empty object from an object-object
                        // recursion means "no change"; an empty
                        // object replacing a scalar is a real value
                        // (RFC 7396) and must be kept.
                        if !(sub.is_object()
                            && sub.as_object().unwrap().is_empty()
                            && pv.is_object())
                        {
                            patch.insert(k.clone(), sub);
                        }
                    }
                    Some(_) => {}
                    None => {
                        patch.insert(k.clone(), v.clone());
                    }
                }
            }
            for k in prev_obj.keys() {
                if !next_obj.contains_key(k) {
                    patch.insert(k.clone(), Value::Null);
                }
            }
            Value::Object(patch)
        }
        (a, b) if a == b => Value::Null,
        (_, b) => b.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ItemId;
    use serde_json::json;

    use crate::{Changed, Edit, PollOutcome};
    use muon::PathSegment;

    fn txn(id: u64, model: &str, path: &[&str], kind: Changed<Edit>) -> Transaction {
        Transaction {
            id: TxnId {
                incarnation: 1,
                seq: id,
            },
            client_id: 1,
            timestamp: 0,
            kind,
            model_id: model.to_owned(),
            path: path
                .iter()
                .map(|f| PathSegment::String((*f).to_owned()))
                .collect(),
        }
    }

    fn replace(id: u64, model: &str, field: &str, value: Value) -> Transaction {
        txn(
            id,
            model,
            &[field],
            Changed::Replace {
                before: None,
                after: Some(value),
            },
        )
    }

    /// Batch key for client 1, session 1.
    fn bk(id: u64) -> BatchKey {
        BatchKey {
            client_id: 1,
            session: 1,
            batch_id: id,
        }
    }

    /// The incremental deltas from a poll; panics on other outcomes.
    fn deltas(server: &mut SyncServer, since: Option<SyncId>) -> Vec<DeltaPacket> {
        match server.poll(since) {
            PollOutcome::Deltas(packets) => packets,
            PollOutcome::ResetRequired => panic!("unexpected reset"),
            PollOutcome::Snapshot { .. } => panic!("unexpected snapshot"),
        }
    }

    /// The report packets from a bootstrap poll.
    fn reports(server: &mut SyncServer) -> Vec<DeltaPacket> {
        match server.poll(None) {
            PollOutcome::Snapshot { reports, .. } => reports,
            _ => panic!("expected a snapshot"),
        }
    }

    #[test]
    fn applies_in_receive_order() {
        let mut server = SyncServer::new();
        server.send(bk(1), &[replace(1, "doc", "a", json!("first"))]);
        server.send(bk(2), &[replace(2, "doc", "a", json!("second"))]);
        // Both batches are delivered; the later one overwrites.
        let packets = reports(&mut server);
        assert_eq!(packets.len(), 2, "two applied packets");
        assert_eq!(packets[0].sync_id, 1);
        assert_eq!(packets[1].sync_id, 2);
        assert_eq!(
            server.model("doc").unwrap()["a"],
            json!("second"),
            "receive order = apply order"
        );
        assert_eq!(server.applied_count(), 2);
        assert_eq!(server.last_sync_id(), 2);
    }

    #[test]
    fn poll_returns_applied_and_retained_deltas_ascending() {
        let mut server = SyncServer::new();
        server.send(bk(1), &[replace(1, "doc", "a", json!("one"))]);
        server.poll(None);
        server.send(bk(2), &[replace(2, "doc", "b", json!("two"))]);
        let packets = deltas(&mut server, Some(1));
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].sync_id, 2);
        assert_eq!(packets[0].applied_batch, Some(bk(2)));
        let DeltaAction::Update { model_id, value } = &packets[0].actions[0] else {
            panic!("expected an Update action");
        };
        assert_eq!(model_id, "doc");
        assert_eq!(value["b"], json!("two"), "unchanged member `a` is omitted");
    }

    #[test]
    fn resend_returns_original_threshold() {
        let mut server = SyncServer::new();
        server.send(bk(1), &[replace(1, "doc", "a", json!("x"))]);
        server.poll(None);
        // The same batch, resent after a client crash.
        let response = server.send(bk(1), &[replace(1, "doc", "a", json!("x"))]);
        assert_eq!(response.deduped_at, Some(1));
        assert_eq!(server.applied_count(), 1, "nothing reapplied");
    }

    #[test]
    fn multi_model_packets_broadcast_per_model() {
        let mut server = SyncServer::new();
        server.send(
            bk(1),
            &[
                replace(1, "issues", "i1", json!({"title": "a"})),
                replace(2, "board", "name", json!("B")),
            ],
        );
        let packets = reports(&mut server);
        assert_eq!(packets.len(), 1, "one applied batch");
        assert_eq!(packets[0].applied_batch, Some(bk(1)));
        // A later batch's packet exposes the per-model broadcast split
        // (the first batch's actions were consumed report-only by the
        // bootstrap snapshot).
        server.send(bk(2), &[replace(3, "issues", "i2", json!({"title": "c"}))]);
        let packets = deltas(&mut server, Some(1));
        assert_eq!(packets.len(), 1);
        let DeltaAction::Update { model_id, .. } = &packets[0].actions[0] else {
            panic!("expected an Update action");
        };
        assert_eq!(model_id, "issues");
        assert_eq!(packets[0].actions.len(), 1);
    }

    #[test]
    fn mixed_batch_with_stale_id_buffers_under_the_prefix_guard() {
        let mut server = SyncServer::new();
        let first = replace(1, "doc", "a", json!("x"));
        server.send(bk(1), std::slice::from_ref(&first));
        server.poll(None);
        // A stale batch id with a fresh leaf cannot be a legitimate
        // resend (batches are atomic; a resend carries the same leaves
        // and is answered by the dedup threshold in `send`). The
        // contiguous-prefix guard buffers it instead of applying it —
        // the mixed shape would break the per-client batch order.
        let second = replace(2, "doc", "b", json!("y"));
        server.send(bk(1), &[first, second]);
        let packets = reports(&mut server);
        assert!(packets.is_empty(), "stale batch id buffers under the gap");
        assert_eq!(server.applied_count(), 1, "the fresh leaf is not applied");
    }

    #[test]
    fn reject_reports_and_advances_the_event_cursor() {
        let mut server = SyncServer::new().with_reject(|t| {
            t.path
                .iter()
                .any(|s| matches!(s, PathSegment::String(f) if f == "title"))
        });
        server.send(bk(1), &[replace(1, "doc", "title", json!("x"))]);
        let packets = reports(&mut server);
        // Rejections changed no state, but the sync id still advances:
        // the sync id is an event cursor, so the rejection report stays
        // visible to the sending client even when another client's poll
        // drains it first.
        assert_eq!(packets.len(), 1, "one rejection report");
        assert_eq!(packets[0].sync_id, 1, "rejections advance the cursor");
        assert_eq!(packets[0].rejected.len(), 1);
        assert!(server.model("doc").is_none(), "rejected leaf not applied");
        assert_eq!(server.applied_count(), 0);
        // The rejected id is not deduplicated: a resend is rejected again.
        let response = server.send(bk(1), &[replace(1, "doc", "title", json!("x"))]);
        assert_eq!(response.deduped_at, None, "rejected ids are not seen");
    }

    #[test]
    fn bootstrap_returns_full_snapshot() {
        let mut server = SyncServer::new();
        server.seed("doc", json!({ "a": 1 }));
        server.send(bk(1), &[replace(1, "doc", "b", json!(2))]);
        server.poll(Some(1));
        match server.poll(None) {
            PollOutcome::Snapshot {
                sync_id,
                models,
                reports,
            } => {
                assert_eq!(sync_id, 1);
                assert_eq!(reports.len(), 0, "no pending batches");
                assert_eq!(models.len(), 1);
                assert_eq!(models[0].0, "doc");
                assert_eq!(models[0].1, json!({ "a": 1, "b": 2 }));
            }
            _ => panic!("expected a snapshot"),
        }
    }

    #[test]
    fn snapshot_includes_pending_batch_reports_only() {
        let mut server = SyncServer::new();
        server.send(bk(1), &[replace(1, "doc", "a", json!("x"))]);
        match server.poll(None) {
            PollOutcome::Snapshot {
                sync_id,
                models,
                reports,
            } => {
                // The snapshot already contains the batch's effect;
                // the report carries only bookkeeping, no state actions.
                assert_eq!(sync_id, 1);
                assert_eq!(models[0].1, json!({ "a": "x" }));
                assert_eq!(reports.len(), 1);
                assert!(reports[0].actions.is_empty(), "report-only");
                assert_eq!(reports[0].applied_batch, Some(bk(1)));
            }
            _ => panic!("expected a snapshot"),
        }
    }

    #[test]
    fn retention_answers_reset_required_outside_the_window() {
        let mut server = SyncServer::new().with_retention(2);
        for i in 1..=3u64 {
            server.send(bk(i), &[replace(i, "doc", "a", json!(i))]);
            server.poll(Some(i - 1));
        }
        // Batch 1 was clipped: an anchor at 1 is outside the window.
        assert!(matches!(server.poll(Some(1)), PollOutcome::ResetRequired));
        // An anchor inside the window still gets the retained tail.
        let packets = deltas(&mut server, Some(2));
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].sync_id, 3);
    }

    #[test]
    fn clear_model_resets_baseline() {
        let mut server = SyncServer::new();
        server.seed("doc", json!({ "a": 1, "b": 2 }));
        server.send(bk(1), &[replace(1, "doc", "a", json!(10))]);
        server.poll(None);
        server.clear_model("doc");
        assert!(server.model("doc").is_none());
        // A recreate after clear diffs from an empty baseline: the
        // whole value arrives, including members that equal the old one.
        server.send(bk(2), &[replace(2, "doc", "b", json!(2))]);
        let packets = deltas(&mut server, Some(1));
        assert_eq!(packets.len(), 2, "clear delta + recreate delta");
        let DeltaAction::Update { value, .. } = &packets[1].actions[0] else {
            panic!("expected an Update action");
        };
        assert_eq!(value["b"], json!(2), "full value after a clear");
    }

    #[test]
    fn batch_ids_collide_across_clients_without_confusion() {
        let mut server = SyncServer::new();
        // Two clients both use batch id 1 with different transaction ids.
        let a = replace(10, "doc", "a", json!("A"));
        let mut b_txn = replace(20, "doc", "a", json!("B"));
        b_txn.client_id = 2;
        let b = b_txn;
        server.send(
            BatchKey {
                client_id: 1,
                session: 1,
                batch_id: 1,
            },
            std::slice::from_ref(&a),
        );
        server.send(
            BatchKey {
                client_id: 2,
                session: 1,
                batch_id: 1,
            },
            std::slice::from_ref(&b),
        );
        let packets = reports(&mut server);
        assert_eq!(packets.len(), 2, "both batches applied");
        // Sessions drain in the lamport order (client id first): each
        // client's report matches its full key.
        let keys: Vec<BatchKey> = packets
            .iter()
            .map(|p| p.applied_batch.expect("applied report"))
            .collect();
        assert_eq!(
            keys[0],
            BatchKey {
                client_id: 1,
                session: 1,
                batch_id: 1
            }
        );
        assert_eq!(
            keys[1],
            BatchKey {
                client_id: 2,
                session: 1,
                batch_id: 1
            }
        );
        assert_eq!(server.applied_count(), 2);
    }

    #[test]
    fn concurrent_batches_apply_in_lamport_order() {
        let mut server = SyncServer::new();
        // Client 2 arrives first; client 1's batch is still in flight.
        let mut b = replace(20, "doc", "a", json!("B"));
        b.client_id = 2;
        server.send(
            BatchKey {
                client_id: 2,
                session: 1,
                batch_id: 1,
            },
            std::slice::from_ref(&b),
        );
        server.send(
            BatchKey {
                client_id: 1,
                session: 1,
                batch_id: 1,
            },
            std::slice::from_ref(&replace(10, "doc", "a", json!("A"))),
        );
        // The lamport order `(client_id, session, batch_id)` applies
        // client 1 first regardless of arrival order.
        let packets = deltas(&mut server, Some(0));
        assert_eq!(packets.len(), 2, "both batches broadcast");
        let DeltaAction::Update { value, .. } = &packets[0].actions[0] else {
            panic!("expected an Update action");
        };
        assert_eq!(value["a"], json!("A"), "client 1 applied first");
        let DeltaAction::Update { value, .. } = &packets[1].actions[0] else {
            panic!("expected an Update action");
        };
        assert_eq!(value["a"], json!("B"), "client 2 applied second");
    }

    #[test]
    fn out_of_order_batches_buffer_until_the_gap_closes() {
        let mut server = SyncServer::new();
        // Batch 2 arrives first; batch 1 is still in flight.
        server.send(bk(2), &[replace(2, "doc", "a", json!("second"))]);
        match server.poll(None) {
            PollOutcome::Snapshot {
                sync_id,
                models,
                reports,
            } => {
                assert_eq!(sync_id, 0, "no events yet");
                assert!(models.is_empty());
                assert!(reports.is_empty(), "gap: batch 2 buffers");
            }
            _ => panic!("expected a snapshot"),
        }
        assert!(server.model("doc").is_none(), "nothing applied yet");
        // The gap closes: both batches apply, in batch-id order.
        server.send(bk(1), &[replace(1, "doc", "a", json!("first"))]);
        let packets = reports(&mut server);
        assert_eq!(packets.len(), 2);
        assert_eq!(packets[0].applied_batch, Some(bk(1)), "prefix first");
        assert_eq!(packets[1].applied_batch, Some(bk(2)));
        assert_eq!(
            server.model("doc").unwrap()["a"],
            json!("second"),
            "batch order = apply order"
        );
    }

    #[test]
    fn namespace_mismatch_is_rejected_not_applied() {
        let mut server = SyncServer::new();
        // The batch key says client 1, but the transaction claims
        // client 2: it must never be applied.
        let mut t = replace(1, "doc", "a", json!("x"));
        t.client_id = 2;
        server.send(bk(1), std::slice::from_ref(&t));
        let packets = reports(&mut server);
        assert_eq!(packets[0].rejected.len(), 1, "namespace mismatch rejected");
        assert!(server.model("doc").is_none());
    }

    // ── Sequence (structural) operations ────────────────────────────

    fn seq_txn(id: u64, kind: Changed<Edit>) -> Transaction {
        let mut t = txn(
            id,
            "doc",
            &[],
            Changed::Replace {
                before: None,
                after: None,
            },
        );
        t.kind = kind;
        t.path = vec![PathSegment::String("blocks".into())];
        t
    }

    fn insert_op(
        _id: u64,
        anchor: Option<ItemId>,
        first: u64,
        len: u32,
        value: Value,
    ) -> Changed<Edit> {
        Changed::Inplace(Edit::Insert {
            anchor,
            range: crate::ItemRange {
                first: crate::ItemId {
                    client_id: 1,
                    incarnation: 1,
                    seq: first,
                },
                len,
            },
            value: Box::new(value),
        })
    }

    fn item(seq: u64) -> ItemId {
        ItemId {
            client_id: 1,
            incarnation: 1,
            seq,
        }
    }

    #[test]
    fn structural_ops_apply_to_single_list_and_broadcast_seqop() {
        let mut server = SyncServer::new();
        // Two inserts into `blocks`: [a, b], then x after a.
        server.send(
            bk(1),
            &[seq_txn(1, insert_op(1, None, 1, 2, json!(["a", "b"])))],
        );
        // Incremental poll: the applied batch's packet carries the
        // SeqOp action (bootstrap reports are report-only).
        let packets = deltas(&mut server, Some(0));
        assert_eq!(packets.len(), 1);
        let DeltaAction::SeqOp {
            model_id,
            path,
            kind,
        } = &packets[0].actions[0]
        else {
            panic!("structural ops broadcast as SeqOp, not merge patches");
        };
        assert_eq!(model_id, "doc");
        assert_eq!(path.len(), 1);
        assert!(matches!(kind, Changed::Inplace(Edit::Insert { .. })));

        // The model's value is the single-list array (identity form).
        let value = server.model("doc").unwrap();
        let nodes = value["blocks"].as_array().unwrap();
        assert_eq!(nodes.len(), 2, "two elements, no merge-patch duplication");
        assert_eq!(nodes[0]["value"], json!("a"));
        assert_eq!(nodes[1]["value"], json!("b"));
        assert!(nodes[0]["alive"].as_bool().unwrap());

        // A second insert after a lands between a and b.
        server.send(
            bk(2),
            &[seq_txn(2, insert_op(2, Some(item(1)), 10, 1, json!("x")))],
        );
        let packets = deltas(&mut server, Some(1));
        let DeltaAction::SeqOp { kind, .. } = &packets[0].actions[0] else {
            panic!("expected a SeqOp");
        };
        assert!(matches!(kind, Changed::Inplace(Edit::Insert { .. })));
        let nodes = server.model("doc").unwrap()["blocks"].as_array().unwrap();
        let values: Vec<&Value> = nodes
            .iter()
            .filter(|n| n["alive"].as_bool().unwrap())
            .map(|n| &n["value"])
            .collect();
        assert_eq!(values, vec![&json!("a"), &json!("x"), &json!("b")]);
    }

    #[test]
    fn structural_ops_are_idempotent_on_replay() {
        let mut server = SyncServer::new();
        let op = seq_txn(1, insert_op(1, None, 1, 2, json!(["a", "b"])));
        server.send(bk(1), std::slice::from_ref(&op));
        server.poll(None);
        // Replay the same insert: the server deduplicates by id before
        // applying (no state change, no new event).
        let response = server.send(bk(1), std::slice::from_ref(&op));
        assert_eq!(response.deduped_at, Some(1), "dedup by transaction id");
        assert_eq!(server.applied_count(), 1);
        assert_eq!(server.last_sync_id(), 1, "no new event");
    }

    #[test]
    fn delete_removes_and_snapshot_keeps_the_single_list() {
        let mut server = SyncServer::new();
        server.send(
            bk(1),
            &[seq_txn(1, insert_op(1, None, 1, 2, json!(["a", "b"])))],
        );
        server.poll(None);
        server.send(
            bk(2),
            &[seq_txn(
                2,
                Changed::Inplace(Edit::Delete {
                    anchor: Some(item(1)),
                    targets: vec![crate::ItemRange {
                        first: item(2),
                        len: 1,
                    }],
                    value: Box::new(json!("b")),
                }),
            )],
        );
        let packets = deltas(&mut server, Some(1));
        assert_eq!(packets.len(), 1);
        assert!(matches!(
            packets[0].actions[0],
            DeltaAction::SeqOp {
                kind: Changed::Inplace(Edit::Delete { .. }),
                ..
            }
        ));
        let nodes = server.model("doc").unwrap()["blocks"].as_array().unwrap();
        assert_eq!(
            nodes.len(),
            2,
            "the tombstone keeps its slot in the wire form"
        );
        assert_eq!(nodes[0]["value"], json!("a"));
        assert!(!nodes[1]["alive"].as_bool().unwrap(), "b is marked dead");

        // The snapshot carries the single list unchanged (identity
        // form, the deleted element tombstoned).
        match server.poll(None) {
            PollOutcome::Snapshot { models, .. } => {
                let blocks = models[0].1["blocks"].as_array().unwrap();
                assert_eq!(blocks.len(), 2);
                assert!(!blocks[1]["alive"].as_bool().unwrap());
            }
            _ => panic!("expected a snapshot"),
        }
    }

    #[test]
    fn move_updates_placement_and_broadcasts() {
        let mut server = SyncServer::new();
        server.send(
            bk(1),
            &[seq_txn(1, insert_op(1, None, 1, 3, json!(["a", "b", "c"])))],
        );
        server.poll(None);
        // Move c to the head (pos = the move's own identity 20).
        server.send(
            bk(2),
            &[seq_txn(
                2,
                Changed::Inplace(Edit::Move {
                    item: item(3),
                    to: None,
                    from_anchor: Some(item(2)),
                    pos: item(20),
                }),
            )],
        );
        let packets = deltas(&mut server, Some(1));
        assert!(matches!(
            packets[0].actions[0],
            DeltaAction::SeqOp {
                kind: Changed::Inplace(Edit::Move { .. }),
                ..
            }
        ));
        let nodes = server.model("doc").unwrap()["blocks"].as_array().unwrap();
        let values: Vec<&Value> = nodes
            .iter()
            .filter(|n| n["alive"].as_bool().unwrap())
            .map(|n| &n["value"])
            .collect();
        assert_eq!(values, vec![&json!("c"), &json!("a"), &json!("b")]);
        // The moved element carries its new placement version.
        assert_eq!(nodes[0]["pos"], json!(item(20)));
    }
}
