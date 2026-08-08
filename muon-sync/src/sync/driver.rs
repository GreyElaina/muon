//! Sync orchestration: a pure synchronous driver (sans-I/O core).
//!
//! The driver translates I/O results into queue transitions and queue
//! state into I/O commands. It performs no I/O itself and depends on no
//! async runtime: an outer adapter executes the commands and feeds the
//! results back. The queue and the driver stay fully synchronous and
//! unit-testable.
//!
//! Single-flight is structural: [`SyncClient`] is the only construction
//! entry point, [`SyncDriver`] is not `Clone`, and every transition
//! method takes `&mut self` — a driver can never drive two concurrent
//! sync steps.
//!
//! Protocol (mirroring LSE):
//! - **Outbound**: the collecting batch is closed, the front queued
//!   batch is persisted before sending, and a window of batches is in
//!   flight (bounded by [`crate::DEFAULT_IN_FLIGHT_MAX`]). A successful
//!   send is delivery-only: the batch stays in flight until the server
//!   reports it applied; a redelivery that the server deduplicated
//!   completes immediately through its original threshold. A rejection
//!   reconciles the model from an authoritative base.
//! - **Inbound**: deltas are polled after the last anchor; each packet
//!   advances the anchor, then the adapter applies it to its remote and
//!   reconciles the model's unsynced changes against the new
//!   authoritative value.
//! - **Recovery**: persisted batches are re-queued with their original
//!   ids (the server deduplicates by transaction id), and the inbound
//!   anchor is restored so polling resumes from the last applied delta
//!   instead of from 0.

use std::sync::{Arc, Mutex};

use or_poisoned::OrPoisoned;
use serde_json::Value;

use crate::{
    BatchId, BatchKey, CancelOutcome, Commit, CommitBatch, DeltaPacket, SendResponse, SyncId,
    Transaction, TransactionQueue, TxnId,
};

/// An I/O operation for the adapter to execute.
#[derive(Debug)]
pub enum SyncCommand {
    /// Persist a batch to the offline cache (crash recovery). On success
    /// call [`SyncDriver::on_persisted`].
    Persist {
        /// The batch to store (or rewrite after a cancellation).
        batch: CommitBatch,
    },
    /// Send a batch's transactions to the server (delivery only; the
    /// server applies asynchronously). On response call
    /// [`SyncDriver::on_sent`].
    Send {
        /// The batch's wire identity (client, session, batch id).
        batch_key: BatchKey,
        /// The flattened leaf transactions of the batch.
        txns: Vec<Transaction>,
    },
    /// Poll the server for delta packets after this sync id. Feed the
    /// packets to the adapter's inbound handling. `None` is the
    /// bootstrap case: the client has no anchor yet.
    PollDeltas {
        /// The inbound anchor to poll after; `None` on first sync.
        since: Option<SyncId>,
    },
    /// Persist the client's inbound anchor after it advanced. The
    /// anchor is the highest delta sync id applied; recovery resumes
    /// polling from it, so the server only needs to retain deltas
    /// after it.
    SaveAnchor {
        /// The new anchor value.
        sync_id: SyncId,
    },
    /// Remove a batch from the offline cache (fully resolved: every
    /// change was accepted or rejected).
    RemoveBatch {
        /// The batch id to remove.
        batch_id: BatchId,
    },
    /// A change was rejected by the server (application-level, reported
    /// through [`DeltaPacket::rejected`], or a whole-batch transport
    /// rejection). The adapter must reconcile the model from an
    /// authoritative base (call [`SyncDriver::reconcile_model`]); the
    /// rejected change is not replayed.
    Rejected {
        /// The model the rejected changes belong to.
        model_id: String,
    },
    /// Apply a reconciled final value to the store. The adapter
    /// deserializes it into the store's type.
    ApplyValue {
        /// The model this value belongs to.
        model_id: String,
        /// The final value: authoritative value plus replayed
        /// unsynced changes.
        value: Value,
    },
    /// A change was completed: the server confirmed it (its threshold
    /// reached the inbound anchor). The adapter decides what to do with
    /// the change — e.g. keep it in an application-level undo history.
    /// Ignoring the command is a valid choice (no undo support).
    Completed {
        /// The completed change.
        commit: Commit,
    },
}

/// The single entry point for a sync client.
///
/// Owns the only [`SyncDriver`] for its queue and hands out cloneable
/// [`SyncChannel`](crate::SyncChannel) write handles. A driver can never
/// be constructed from a bare queue handle, so two drivers cannot drive
/// one queue concurrently.
pub struct SyncClient {
    driver: SyncDriver,
}

impl SyncClient {
    /// Create a sync client with a fresh queue.
    pub fn new(client_id: u64) -> Self {
        Self::with_in_flight_max(client_id, crate::DEFAULT_IN_FLIGHT_MAX)
    }

    /// Create a sync client with a custom in-flight window.
    ///
    /// The window bounds how many batches may be delivered to the
    /// server before their application is confirmed. It is a memory
    /// watermark: the server applies batches in receive order, so a
    /// larger window never risks reordering, only memory.
    pub fn with_in_flight_max(client_id: u64, in_flight_max: usize) -> Self {
        let queue = Arc::new(Mutex::new(TransactionQueue::with_in_flight_max(
            client_id,
            in_flight_max,
        )));
        Self {
            driver: SyncDriver { queue },
        }
    }

    /// The unique driver for this client. All transition methods take
    /// `&mut self`; the caller must not run two sync steps concurrently.
    pub fn driver(&mut self) -> &mut SyncDriver {
        &mut self.driver
    }

    /// A write channel for one logical model, appending to this client's
    /// queue. Cloneable and shareable across threads.
    pub fn channel(&self, model_id: impl Into<String>) -> crate::SyncChannel {
        crate::SyncChannel::new(self.driver.queue.clone(), model_id)
    }

    /// Restore persisted batches, the inbound anchor, and the last
    /// authoritative model set after a crash. The batches are rebuilt
    /// into the queue in order and are already durable; the anchor
    /// resumes polling from the last applied delta; the model set
    /// distinguishes server-side removals from offline creations when
    /// the next snapshot aligns.
    pub fn recover(
        &mut self,
        batches: Vec<CommitBatch>,
        anchor: Option<SyncId>,
        known_models: Vec<String>,
    ) {
        self.driver
            .queue
            .lock()
            .or_poisoned()
            .recover(batches, anchor, known_models);
    }

    /// The shared queue (for tests and introspection).
    pub fn queue(&self) -> &Arc<Mutex<TransactionQueue>> {
        &self.driver.queue
    }
}

/// Pure synchronous sync orchestrator (sans-I/O core).
///
/// Owns no I/O. Methods take queue transitions and return the commands
/// an adapter must execute; the adapter feeds results back through the
/// [`on_persisted`](Self::on_persisted) / [`on_sent`](Self::on_sent) /
/// [`on_delta`](Self::on_delta) entry points.
pub struct SyncDriver {
    queue: Arc<Mutex<TransactionQueue>>,
}

impl SyncDriver {
    /// The queue this driver drives.
    pub fn queue(&self) -> &Arc<Mutex<TransactionQueue>> {
        &self.queue
    }

    /// One outbound step: close the collecting batch, emit a persist for
    /// the front queued batch if it is not yet durable, and send the
    /// front durable batch (up to the in-flight window).
    ///
    /// The adapter executes the returned commands in order, calling
    /// [`on_persisted`](Self::on_persisted) after each persist and
    /// [`on_sent`](Self::on_sent) after each send. Call again to
    /// advance a batch that was persisted this round.
    pub fn outbound_step(&mut self) -> Vec<SyncCommand> {
        let mut cmds = Vec::new();
        let mut guard = self.queue.lock().or_poisoned();
        guard.collect();
        if let Some(batch) = guard.front_unpersisted() {
            cmds.push(SyncCommand::Persist { batch });
        }
        if let Some(batch) = guard.dequeue() {
            let txns: Vec<Transaction> = batch
                .commits
                .iter()
                .flat_map(|c| c.txns.iter().cloned())
                .collect();
            // The session is the producing process's incarnation. A
            // recovered batch's transactions keep their original
            // incarnation, so the resend joins the same server-side
            // prefix as the batch it was created in — a fresh session
            // would restart the prefix at 1 and buffer the resend as a
            // gap forever.
            let session = txns
                .first()
                .map(|t| t.id.incarnation)
                .unwrap_or_else(|| guard.session());
            cmds.push(SyncCommand::Send {
                batch_key: BatchKey {
                    client_id: guard.client_id(),
                    session,
                    batch_id: batch.id,
                },
                txns,
            });
        }
        cmds
    }

    /// Confirm that a batch was durably persisted (front queued batch
    /// becomes sendable).
    pub fn on_persisted(&mut self, id: BatchId) {
        self.queue.lock().or_poisoned().confirm_persisted(id);
    }

    /// Handle the delivery receipt for a sent batch.
    ///
    /// A fresh batch carries no application information: it stays in
    /// flight until the server reports it through
    /// [`DeltaPacket::applied_batch`]. A redelivered batch that the
    /// server deduplicated carries its original application sync id:
    /// the batch moves straight to awaiting, completes as soon as the
    /// anchor satisfies that threshold, and its cache copy is removed.
    pub fn on_sent(&mut self, batch_key: BatchKey, response: SendResponse) -> Vec<SyncCommand> {
        let mut cmds = Vec::new();
        if let Some(threshold) = response.deduped_at {
            let mut guard = self.queue.lock().or_poisoned();
            if guard.mark_applied(batch_key.batch_id, threshold) {
                cmds.push(SyncCommand::RemoveBatch {
                    batch_id: batch_key.batch_id,
                });
            }
            // The restored anchor may already satisfy the original
            // threshold: complete now instead of waiting for a delta
            // that may never arrive.
            for a in guard.complete_ready() {
                cmds.push(SyncCommand::Completed { commit: a.commit });
            }
        }
        cmds
    }

    /// Handle a whole-batch transport rejection ([`SendError::Rejected`](crate::SendError::Rejected)).
    ///
    /// Every transaction of the batch is rejected: the batch is dropped
    /// from the queue and its models are flagged for reconciliation.
    pub fn on_batch_rejected(&mut self, batch_key: BatchKey) -> Vec<SyncCommand> {
        let batch_id = batch_key.batch_id;
        let txns: Vec<TxnId> = {
            let guard = self.queue.lock().or_poisoned();
            let mut txns = Vec::new();
            for batch in guard.in_flight_batches() {
                if batch.id == batch_id {
                    txns.extend(
                        batch
                            .commits
                            .iter()
                            .flat_map(|c| c.txns.iter().map(|t| t.id)),
                    );
                }
            }
            txns
        };
        self.rejected_commands(&txns)
    }

    /// Handle one inbound delta packet: rejected transactions,
    /// applied-batch report, and ready changes.
    ///
    /// Pure in-memory transitions — the anchor is *not* advanced here.
    /// The adapter persists the emitted [`SyncCommand::SaveAnchor`]
    /// (the checkpoint) and only then calls
    /// [`confirm_anchor`](Self::confirm_anchor), so a crash or store
    /// error replays the whole packet idempotently instead of skipping
    /// it.
    ///
    /// Order within the packet: rejected changes are removed first (a
    /// batch may lose changes to rejection before its remaining changes
    /// are reported applied), then the applied batch moves to awaiting
    /// with the packet's sync id as threshold, then every awaiting
    /// change whose threshold is now satisfied completes.
    pub fn on_delta(&mut self, packet: &DeltaPacket) -> Vec<SyncCommand> {
        let mut cmds = Vec::new();
        let mut guard = self.queue.lock().or_poisoned();

        if !packet.rejected.is_empty() {
            let outcome = guard.remove_rejected(&packet.rejected);
            for model_id in outcome.model_ids {
                cmds.push(SyncCommand::Rejected { model_id });
            }
            for batch_id in outcome.cleared_batches {
                cmds.push(SyncCommand::RemoveBatch { batch_id });
            }
        }
        if let Some(batch_key) = packet.applied_batch {
            // Match only our own batches: batch ids alone collide
            // across clients, and a foreign report must never confirm
            // (or drop from the cache) one of ours. The session is
            // deliberately not compared here — a recovered batch is
            // resent under its producing session while this process's
            // session is fresh; only the batch id (unique within the
            // client) identifies it, and `mark_applied` ignores ids
            // that are not in flight.
            if batch_key.client_id == guard.client_id()
                && guard.mark_applied(batch_key.batch_id, packet.sync_id)
            {
                cmds.push(SyncCommand::RemoveBatch {
                    batch_id: batch_key.batch_id,
                });
            }
        }
        // Complete changes the packet confirms, before reconciling it
        // (they must not be replayed as unsynced).
        for a in guard.complete_ready_for(packet.sync_id) {
            cmds.push(SyncCommand::Completed { commit: a.commit });
        }
        match guard.last_sync_id() {
            Some(last) if last >= packet.sync_id => {}
            _ => cmds.push(SyncCommand::SaveAnchor {
                sync_id: packet.sync_id,
            }),
        }
        cmds
    }

    /// Shared rejection handling: remove rejected transactions and emit
    /// the reconciliation / cleanup commands for what they hit.
    fn rejected_commands(&mut self, txns: &[TxnId]) -> Vec<SyncCommand> {
        let outcome = self.queue.lock().or_poisoned().remove_rejected(txns);
        let mut cmds = Vec::new();
        for model_id in outcome.model_ids {
            cmds.push(SyncCommand::Rejected { model_id });
        }
        for batch_id in outcome.cleared_batches {
            cmds.push(SyncCommand::RemoveBatch { batch_id });
        }
        cmds
    }

    /// Re-queue a specific in-flight batch after a network failure. The
    /// request may or may not have reached the server; resending is
    /// idempotent by transaction id.
    pub fn requeue_network(&mut self, batch_key: BatchKey) {
        self.queue.lock().or_poisoned().requeue(batch_key.batch_id);
    }

    /// Emit a poll command for deltas after the current inbound anchor.
    /// A client with no anchor polls with `None` — the first sync acts
    /// as a bootstrap (the server returns a full snapshot).
    pub fn poll_command(&mut self) -> SyncCommand {
        let guard = self.queue.lock().or_poisoned();
        SyncCommand::PollDeltas {
            since: guard.last_sync_id(),
        }
    }

    /// Abandon the inbound anchor: the next poll bootstraps with
    /// `None`. The server answers [`PollOutcome::ResetRequired`](crate::PollOutcome::ResetRequired)
    /// (crate::PollOutcome) when the anchor fell out of its retention
    /// window.
    pub fn reset_anchor(&mut self) {
        self.queue.lock().or_poisoned().reset_anchor();
    }

    /// Advance the in-memory anchor after the adapter confirmed the
    /// durable write. Idempotent — a retried packet converges to the
    /// same anchor.
    pub fn confirm_anchor(&mut self, sync_id: SyncId) {
        self.queue.lock().or_poisoned().advance_sync(sync_id);
    }

    /// Release the recovered batches after the catch-up phase: the
    /// adapter has applied every pending delta, so the frozen batches
    /// may join the normal FIFO.
    pub fn finish_catch_up(&mut self) {
        self.queue.lock().or_poisoned().finish_catch_up();
    }

    /// True while recovered batches are still frozen, i.e. the adapter
    /// must run the catch-up phase before any recovered batch may be
    /// sent.
    pub fn has_recovered(&self) -> bool {
        self.queue.lock().or_poisoned().has_recovered()
    }

    /// Reconcile a model's unsynced changes against a new
    /// authoritative value (the adapter's remote after applying a delta
    /// action).
    ///
    /// Replays every unconfirmed change of the model on top of the
    /// authoritative value, re-capturing each transaction's `before`
    /// in sequence, and emits the final value for the store.
    ///
    /// Emits a [`SyncCommand::Persist`] rewrite for every affected
    /// batch that has a persisted cache copy (queued and durable, or in
    /// flight): a crash must recover the rebased transactions, not the
    /// stale pre-rebase `before` values. The final
    /// [`SyncCommand::ApplyValue`] is always the last command.
    pub fn reconcile_model(&mut self, model_id: &str, authoritative: &Value) -> Vec<SyncCommand> {
        let mut guard = self.queue.lock().or_poisoned();
        let unsynced = guard.unsynced_changes(model_id);
        let outcome = crate::reconcile(authoritative, &unsynced);
        let dirty = guard.apply_rebased(outcome.rebased);
        let mut cmds = Vec::new();
        for batch_id in dirty {
            if let Some(batch) = guard.persisted_batch(batch_id) {
                cmds.push(SyncCommand::Persist { batch });
            }
        }
        drop(guard);
        cmds.push(SyncCommand::ApplyValue {
            model_id: model_id.to_owned(),
            value: outcome.value,
        });
        cmds
    }

    /// The set of model ids with unsynced (not yet completed) changes.
    /// Used by the bootstrap alignment: a model that was in the last
    /// authoritative set and is absent from a snapshot was removed on
    /// the server while the client was away.
    pub(crate) fn unsynced_models(&self) -> Vec<String> {
        self.queue.lock().or_poisoned().unsynced_models()
    }

    /// The last authoritative model set (restored by [`recover`] or
    /// updated by [`remember_models`](Self::remember_models)).
    pub(crate) fn known_models(&self) -> Vec<String> {
        self.queue
            .lock()
            .or_poisoned()
            .known_models()
            .iter()
            .cloned()
            .collect()
    }

    /// Remember the authoritative model set from a bootstrap snapshot.
    pub(crate) fn remember_models(&mut self, models: &[String]) {
        self.queue.lock().or_poisoned().remember_models(models);
    }

    /// Discard every unsent change of a model (Clear/Archive action).
    ///
    /// Only unsent changes are dropped: an in-flight change is already
    /// on its way to the server and an accepted change keeps its
    /// completion tracking — neither can pretend it never existed.
    ///
    /// Emits cache commands so the persisted copies match the
    /// discarded state: a rewrite when a batch still has changes, a
    /// removal when it was emptied. Without them, a crash would
    /// recover and re-send a discarded change.
    pub fn discard_model(&mut self, model_id: &str) -> Vec<SyncCommand> {
        let mut guard = self.queue.lock().or_poisoned();
        let dirty = guard.discard_model(model_id);
        let mut cmds = Vec::new();
        for batch_id in dirty {
            match guard.persisted_batch(batch_id) {
                Some(b) if !b.commits.is_empty() => {
                    cmds.push(SyncCommand::Persist { batch: b });
                }
                Some(_) => cmds.push(SyncCommand::RemoveBatch { batch_id }),
                None => {}
            }
        }
        cmds
    }

    /// Remove an unsent change (queue-only dedup, LSE
    /// `cancelTransaction`). Never touches the store; the caller decides
    /// whether to reconcile.
    ///
    /// Emits cache commands: a persisted batch is rewritten (or removed
    /// when the cancellation emptied it).
    pub fn cancel_commit(
        &mut self,
        batch_id: BatchId,
        ordinal: u32,
    ) -> (CancelOutcome, Vec<SyncCommand>) {
        let mut guard = self.queue.lock().or_poisoned();
        let outcome = guard.remove_commit(batch_id, ordinal);
        let cmds = if outcome == CancelOutcome::Cancelled {
            match guard.persisted_batch(batch_id) {
                Some(b) if !b.commits.is_empty() => {
                    vec![SyncCommand::Persist { batch: b }]
                }
                Some(_) => vec![SyncCommand::RemoveBatch { batch_id }],
                None => Vec::new(),
            }
        } else {
            Vec::new()
        };
        (outcome, cmds)
    }

    /// Undo a completed change: build its inverse and enqueue it with
    /// refreshed identities. Returns the fresh inverse transactions so
    /// the caller can apply them to the store optimistically. Returns
    /// `None` when the change is not invertible.
    pub fn undo(&mut self, commit: &Commit) -> Vec<Transaction> {
        let inverse = commit.invert();
        self.queue.lock().or_poisoned().push_commit(&inverse)
    }

    /// Redo a change: enqueue its original transactions with refreshed
    /// identities. Returns the fresh transactions for optimistic
    /// application.
    pub fn redo(&mut self, commit: &Commit) -> Vec<Transaction> {
        self.queue.lock().or_poisoned().push_commit(commit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use crate::{Changed, Edit, SendResponse};

    fn make_txn(seq: u64, kind: Changed<Edit>) -> Transaction {
        Transaction {
            id: TxnId {
                incarnation: 7,
                seq,
            },
            client_id: 1,
            timestamp: 1000,
            kind,
            model_id: "test".into(),
            path: vec![],
        }
    }

    /// A whole-field replacement kind; the tests do not inspect `before`.
    fn replace(value: serde_json::Value) -> Changed<Edit> {
        Changed::Replace {
            before: Some(serde_json::Value::Null),
            after: Some(value),
        }
    }

    fn push_one(client: &mut SyncClient) {
        let mut guard = client.queue().lock().unwrap();
        let id = guard.next_txn_id();
        let mut txn = make_txn(0, replace(json!("x")));
        txn.id = id;
        guard.push_txn(vec![txn]);
    }

    #[test]
    fn outbound_persists_then_sends() {
        let mut client = SyncClient::new(1);
        push_one(&mut client);
        let driver = client.driver();

        // First step: persist only — an unpersisted batch is not
        // sendable (the persist gate precedes the send gate).
        let cmds = driver.outbound_step();
        assert_eq!(cmds.len(), 1, "persist first");
        let batch_id = match &cmds[0] {
            SyncCommand::Persist { batch } => batch.id,
            _ => panic!("expected persist first"),
        };

        // Confirm the persist, then the second step sends.
        driver.on_persisted(batch_id);
        let cmds = driver.outbound_step();
        assert_eq!(cmds.len(), 1, "send after persist confirmation");
        assert!(matches!(&cmds[0], SyncCommand::Send { .. }));
    }

    #[test]
    fn send_waits_for_persist_confirmation() {
        let mut client = SyncClient::new(1);
        push_one(&mut client);
        let driver = client.driver();

        // First step emits persist + send (front is already durable? No —
        // dequeue requires persisted, so only persist this round if the
        // front was confirmed earlier). Verify: a fresh batch is not
        // sendable until on_persisted.
        let cmds = driver.outbound_step();
        let batch_id = match &cmds[0] {
            SyncCommand::Persist { batch } => batch.id,
            _ => panic!("expected persist first"),
        };
        assert!(
            cmds.len() == 1,
            "unpersisted front batch is not sendable yet"
        );
        driver.on_persisted(batch_id);

        let cmds = driver.outbound_step();
        assert_eq!(cmds.len(), 1, "persisted batch is now sendable");
        assert!(matches!(&cmds[0], SyncCommand::Send { .. }));
    }

    #[test]
    fn on_sent_fresh_batch_stays_in_flight() {
        let mut client = SyncClient::new(1);
        push_one(&mut client);
        let driver = client.driver();
        let cmds = driver.outbound_step();
        let batch_id = match &cmds[0] {
            SyncCommand::Persist { batch } => batch.id,
            _ => panic!("expected persist first"),
        };
        driver.on_persisted(batch_id);
        let cmds = driver.outbound_step();
        let (batch_key, _txns) = match &cmds[0] {
            SyncCommand::Send { batch_key, txns } => (*batch_key, txns.clone()),
            _ => panic!("expected send"),
        };

        // A fresh batch carries no application information: no commands,
        // the batch stays in flight until the server reports it applied.
        let cmds = driver.on_sent(batch_key, SendResponse { deduped_at: None });
        assert!(cmds.is_empty(), "fresh batch waits for the delta report");
        assert!(driver.queue().lock().unwrap().in_flight_front().is_some());
        assert!(driver.queue().lock().unwrap().awaiting_commits().is_empty());
    }

    #[test]
    fn on_sent_deduped_resolves_and_removes_cache() {
        let mut client = SyncClient::new(1);
        push_one(&mut client);
        let driver = client.driver();
        let cmds = driver.outbound_step();
        let batch_id = match &cmds[0] {
            SyncCommand::Persist { batch } => batch.id,
            _ => panic!("expected persist first"),
        };
        driver.on_persisted(batch_id);
        let cmds = driver.outbound_step();
        let (batch_key, _txns) = match &cmds[0] {
            SyncCommand::Send { batch_key, txns } => (*batch_key, txns.clone()),
            _ => panic!("expected send"),
        };

        // A redelivered batch is deduplicated: the response carries the
        // original application sync id, the batch resolves immediately.
        let cmds = driver.on_sent(
            batch_key,
            SendResponse {
                deduped_at: Some(42),
            },
        );
        assert!(
            matches!(
                &cmds[..],
                [SyncCommand::RemoveBatch { batch_id: b }] if *b == batch_key.batch_id
            ),
            "deduplicated batch removed from cache"
        );
        assert_eq!(driver.queue().lock().unwrap().awaiting_commits().len(), 1);
        assert_eq!(driver.queue().lock().unwrap().last_sync_id(), None);
    }

    #[test]
    fn on_batch_rejected_emits_rejected_per_model() {
        let mut client = SyncClient::new(1);
        push_one(&mut client);
        let driver = client.driver();
        let cmds = driver.outbound_step();
        let batch_id = match &cmds[0] {
            SyncCommand::Persist { batch } => batch.id,
            _ => panic!("expected persist first"),
        };
        driver.on_persisted(batch_id);
        let cmds = driver.outbound_step();
        let (batch_key, _txns) = match &cmds[0] {
            SyncCommand::Send { batch_key, txns } => (*batch_key, txns.clone()),
            _ => panic!("expected send"),
        };

        let cmds = driver.on_batch_rejected(batch_key);
        assert!(cmds
            .iter()
            .any(|c| matches!(c, SyncCommand::Rejected { model_id } if model_id == "test")));
        assert!(cmds
            .iter()
            .any(|c| matches!(c, SyncCommand::RemoveBatch { .. })));
        assert!(driver.queue().lock().unwrap().in_flight_front().is_none());
    }

    #[test]
    fn poll_command_uses_anchor() {
        let mut client = SyncClient::new(1);
        assert!(matches!(
            client.driver().poll_command(),
            SyncCommand::PollDeltas { since: None }
        ));
        client.driver().confirm_anchor(100);
        assert!(matches!(
            client.driver().poll_command(),
            SyncCommand::PollDeltas { since: Some(100) }
        ));
    }

    #[test]
    fn on_delta_advances_anchor_only_after_confirm() {
        let mut client = SyncClient::new(1);
        let driver = client.driver();

        // on_delta emits the SaveAnchor command; the memory anchor is
        // still untouched — a failed persist must not skip the packet.
        let packet = DeltaPacket {
            sync_id: 100,
            actions: vec![],
            applied_batch: None,
            rejected: vec![],
        };
        let cmds = driver.on_delta(&packet);
        assert!(matches!(
            &cmds[..],
            [SyncCommand::SaveAnchor { sync_id: 100 }]
        ));
        assert_eq!(
            driver.queue().lock().unwrap().last_sync_id(),
            None,
            "memory anchor does not advance before the durable write",
        );

        // Only after the adapter persisted does the anchor move.
        driver.confirm_anchor(100);
        assert_eq!(driver.queue().lock().unwrap().last_sync_id(), Some(100));

        // Idempotent retry: an older packet emits no command and no
        // regression.
        let older = DeltaPacket {
            sync_id: 50,
            actions: vec![],
            applied_batch: None,
            rejected: vec![],
        };
        assert!(
            driver.on_delta(&older).is_empty(),
            "older packet: nothing to save"
        );
        driver.confirm_anchor(50);
        assert_eq!(driver.queue().lock().unwrap().last_sync_id(), Some(100));
    }

    #[test]
    fn zero_in_flight_window_is_rejected() {
        let mut client = SyncClient::with_in_flight_max(1, 0);
        let driver = client.driver();
        // A zero window would deadlock the pipeline; the constructor
        // clamps it to a working minimum.
        assert_eq!(
            driver.queue().lock().unwrap().in_flight_max(),
            crate::DEFAULT_IN_FLIGHT_MAX,
        );
    }

    #[test]
    fn on_delta_emits_completed_for_confirmed_changes() {
        let mut client = SyncClient::new(1);
        push_one(&mut client);
        let driver = client.driver();

        // Deliver a change, then confirm it with an applied report: the
        // packet both resolves the batch and satisfies its threshold,
        // so the completion flows out as a `Completed` command.
        let cmds = driver.outbound_step();
        let batch_id = match &cmds[0] {
            SyncCommand::Persist { batch } => batch.id,
            _ => panic!("expected persist first"),
        };
        driver.on_persisted(batch_id);
        let cmds = driver.outbound_step();
        let (batch_key, _txns) = match &cmds[0] {
            SyncCommand::Send { batch_key, .. } => (*batch_key, ()),
            _ => panic!("expected send"),
        };

        let cmds = driver.on_delta(&DeltaPacket {
            sync_id: 1,
            actions: vec![],
            applied_batch: Some(batch_key),
            rejected: vec![],
        });
        assert!(
            cmds.iter()
                .any(|c| matches!(c, SyncCommand::Completed { .. })),
            "completion flows out as a command",
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c, SyncCommand::SaveAnchor { .. })),
            "the confirming packet also persists the anchor",
        );
    }

    #[test]
    fn on_delta_does_not_complete_below_threshold() {
        let mut client = SyncClient::new(1);
        push_one(&mut client);
        let driver = client.driver();

        let cmds = driver.outbound_step();
        let batch_id = match &cmds[0] {
            SyncCommand::Persist { batch } => batch.id,
            _ => panic!("expected persist first"),
        };
        driver.on_persisted(batch_id);
        let cmds = driver.outbound_step();
        let (batch_key, _txns) = match &cmds[0] {
            SyncCommand::Send { batch_key, .. } => (*batch_key, ()),
            _ => panic!("expected send"),
        };

        // The applied report resolves the batch with a threshold; the
        // completion still awaits the confirming packet's sync id.
        let cmds = driver.on_delta(&DeltaPacket {
            sync_id: 5,
            actions: vec![],
            applied_batch: Some(batch_key),
            rejected: vec![],
        });
        assert!(
            cmds.iter()
                .any(|c| matches!(c, SyncCommand::Completed { .. })),
            "threshold satisfied by the same packet: completed",
        );

        // A later packet below the anchor completes nothing new.
        driver.confirm_anchor(5);
        let cmds = driver.on_delta(&DeltaPacket {
            sync_id: 4,
            actions: vec![],
            applied_batch: None,
            rejected: vec![],
        });
        assert!(
            !cmds
                .iter()
                .any(|c| matches!(c, SyncCommand::Completed { .. })),
            "no completion below the anchor",
        );
    }

    #[test]
    fn recovered_batch_frozen_until_finish_catch_up() {
        let mut client = SyncClient::new(1);
        let batch = crate::CommitBatch {
            id: 5,
            commits: vec![crate::Commit {
                ordinal: 0,
                txns: vec![make_txn(0, replace(json!("x")))],
            }],
        };
        client.recover(vec![batch], Some(42), Vec::new());
        let driver = client.driver();

        // Frozen: outbound never emits a send for the recovered batch.
        assert!(
            driver.outbound_step().is_empty(),
            "recovered batch is frozen"
        );
        assert_eq!(driver.queue().lock().unwrap().last_sync_id(), Some(42));

        // Catch-up completes: the batch becomes sendable.
        driver.finish_catch_up();
        let cmds = driver.outbound_step();
        assert!(
            matches!(&cmds[..], [SyncCommand::Send { batch_key, .. }] if batch_key.batch_id == 5),
            "released batch is sendable after catch-up",
        );
    }

    #[test]
    fn discard_model_rewrites_or_removes_cache_copy() {
        let mut client = SyncClient::new(1);
        // Two changes of different models share one batch, which is
        // persisted (it has a cache copy).
        {
            let mut guard = client.queue().lock().unwrap();
            let mut a = make_txn(0, replace(json!("a")));
            a.id = guard.next_txn_id();
            a.model_id = "test".into();
            let mut b = make_txn(0, replace(json!("b")));
            b.id = guard.next_txn_id();
            b.model_id = "other".into();
            guard.push_txn(vec![a]);
            guard.push_txn(vec![b]);
        }
        let driver = client.driver();
        let cmds = driver.outbound_step();
        let batch_id = match &cmds[0] {
            SyncCommand::Persist { batch } => batch.id,
            _ => panic!("expected persist first"),
        };
        driver.on_persisted(batch_id);

        // Discard one model: the batch still has changes, so its cache
        // copy is rewritten without the discarded change.
        let cmds = driver.discard_model("test");
        assert!(
            matches!(&cmds[..], [SyncCommand::Persist { batch }] if batch.commits.len() == 1),
            "remaining change rewritten to the cache",
        );

        // Discard the other model: the batch is emptied, so its cache
        // copy is removed.
        let cmds = driver.discard_model("other");
        assert!(
            matches!(&cmds[..], [SyncCommand::RemoveBatch { batch_id: id }] if *id == batch_id),
            "emptied batch removed from the cache",
        );
    }

    #[test]
    fn reconcile_emits_final_value() {
        let mut client = SyncClient::new(1);
        {
            let mut guard = client.queue().lock().unwrap();
            let id = guard.next_txn_id();
            let mut t = make_txn(0, replace(json!("Local")));
            t.id = id;
            t.model_id = "test".into();
            t.path = vec![crate::PathSegment::String("title".to_owned())];
            t.kind = Changed::Replace {
                before: Some(json!("Server")),
                after: Some(json!("Local")),
            };
            guard.push_txn(vec![t]);
        }

        let cmds = client
            .driver()
            .reconcile_model("test", &json!({"title": "Server"}));
        let apply = cmds
            .iter()
            .find_map(|c| match c {
                SyncCommand::ApplyValue { value, .. } => Some(value.clone()),
                _ => None,
            })
            .expect("reconcile emits ApplyValue");
        assert_eq!(
            apply,
            json!({"title": "Local"}),
            "unsynced intent replayed on top of the authoritative value",
        );
    }

    #[test]
    fn reconcile_rewrites_persisted_batch() {
        let mut client = SyncClient::new(1);
        {
            let mut guard = client.queue().lock().unwrap();
            let id = guard.next_txn_id();
            let mut t = make_txn(0, replace(json!("Local")));
            t.id = id;
            t.model_id = "test".into();
            t.path = vec![crate::PathSegment::String("title".to_owned())];
            t.kind = Changed::Replace {
                before: Some(json!("Old Base")),
                after: Some(json!("Local")),
            };
            guard.push_txn(vec![t]);
        }
        let driver = client.driver();

        // Persist the batch so it has a cache copy, then reconcile.
        let cmds = driver.outbound_step();
        let batch_id = match &cmds[0] {
            SyncCommand::Persist { batch } => batch.id,
            _ => panic!("expected persist first"),
        };
        driver.on_persisted(batch_id);

        let cmds = driver.reconcile_model("test", &json!({"title": "New Base"}));
        assert!(
            cmds.iter().any(|c| matches!(
                c,
                SyncCommand::Persist { batch } if batch.id == batch_id
            )),
            "rebased persisted batch is rewritten to the cache",
        );
        let rewrite = cmds
            .iter()
            .find_map(|c| match c {
                SyncCommand::Persist { batch } if batch.id == batch_id => Some(batch),
                _ => None,
            })
            .expect("rewrite command present");
        let kind = &rewrite.commits[0].txns[0].kind;
        assert_eq!(
            kind,
            &Changed::Replace {
                before: Some(json!("New Base")),
                after: Some(json!("Local")),
            },
            "rewritten copy carries the rebased before",
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c, SyncCommand::ApplyValue { .. })),
            "ApplyValue is still emitted",
        );
    }

    #[test]
    fn cancel_rewrites_persisted_batch() {
        let mut client = SyncClient::new(1);
        // Two changes in one batch.
        {
            let mut guard = client.queue().lock().unwrap();
            let mut a = make_txn(0, replace(json!("a")));
            a.id = guard.next_txn_id();
            let mut b = make_txn(0, replace(json!("b")));
            b.id = guard.next_txn_id();
            guard.push_txn(vec![a]);
            guard.push_txn(vec![b]);
        }
        let driver = client.driver();
        let cmds = driver.outbound_step();
        let batch_id = match &cmds[0] {
            SyncCommand::Persist { batch } => batch.id,
            _ => panic!("expected persist first"),
        };
        driver.on_persisted(batch_id);

        let (outcome, cmds) = driver.cancel_commit(batch_id, 0);
        assert_eq!(outcome, CancelOutcome::Cancelled);
        assert!(
            matches!(&cmds[..], [SyncCommand::Persist { batch }] if batch.commits.len() == 1),
            "persisted batch rewritten after cancellation",
        );

        let (outcome, cmds) = driver.cancel_commit(batch_id, 1);
        assert_eq!(outcome, CancelOutcome::Cancelled);
        assert!(
            matches!(&cmds[..], [SyncCommand::RemoveBatch { .. }]),
            "emptied batch removed from the cache",
        );
    }
}
