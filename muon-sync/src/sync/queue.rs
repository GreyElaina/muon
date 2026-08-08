//! Transaction queue: manages the lifecycle of changes from creation to
//! server confirmation.
//!
//! Lifecycle is expressed by container membership — a batch moves through
//! `collecting → queued → in_flight → awaiting`:
//!
//! - `collecting`: a batch still accepting changes ([`push`]); the flush
//!   boundary that closes it ([`collect`]) mirrors LSE's
//!   `commitCreatedTransactions` microtask. All writes within one flush
//!   cycle share one batch — one network request.
//! - `queued`: sealed, awaiting persist confirmation and sending. FIFO:
//!   only the front batch may be confirmed or dequeued, so a persist
//!   failure blocks every later batch.
//! - `in_flight`: delivered to the server, awaiting its application
//!   report. A windowed queue (bounded by `in_flight_max`): the server
//!   applies batches asynchronously in receive order and reports each
//!   one through `DeltaPacket::applied_batch`.
//! - `awaiting`: accepted by the server, waiting for the confirming
//!   delta packet ([`advance_sync`]).
//!
//! One atomic change ([`Commit`]) is the acknowledgment, rollback, and
//! undo unit; one batch ([`CommitBatch`]) is the flush, persistence, and
//! request unit.

use std::collections::{HashSet, VecDeque};

use serde::{Deserialize, Serialize};

use crate::types::BatchId;
use crate::{Changed, ClientId, SyncId, Transaction, TxnId};

/// Default in-flight window: batches delivered but not yet applied.
///
/// A memory watermark, not a correctness bound. Sixteen batches in
/// transit covers typical high-RTT links; the server applies in receive
/// order regardless.
pub const DEFAULT_IN_FLIGHT_MAX: usize = 16;

/// One atomic change: the leaf transactions of one store publication.
///
/// Never split by the engine: a change is the acknowledgment, rollback,
/// and undo unit, and its leaves share a commit across sends and
/// rebases. A rejected leaf leaves the change (the server never applied
/// it); a change whose leaves are all rejected disappears.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Commit {
    /// Stable ordinal within its batch. Assigned monotonically; never
    /// reused after a cancellation, so persisted keys stay unambiguous.
    pub ordinal: u32,
    /// The leaf transactions of this change.
    pub txns: Vec<Transaction>,
}

/// A batch of same-cycle changes: one flush boundary, one network
/// request, one atomic persistence unit (one cache file).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommitBatch {
    /// Session-unique batch identifier.
    pub id: BatchId,
    /// The changes of this batch, in publication order.
    pub commits: Vec<Commit>,
}

impl Commit {
    /// Refresh every transaction's identity in order: each leaf gets a
    /// new id and timestamp through `next_id`. The change itself is
    /// unchanged; only its identities are renewed.
    pub fn refresh(&self, now: u64, mut next_id: impl FnMut() -> TxnId) -> Commit {
        Commit {
            ordinal: self.ordinal,
            txns: self
                .txns
                .iter()
                .map(|t| t.refresh(now, &mut next_id))
                .collect(),
        }
    }
}

/// A change the server accepted, awaiting its confirming delta packet.
#[derive(Clone, Debug)]
pub struct AwaitingCommit {
    /// The batch this change belonged to.
    pub batch_id: BatchId,
    /// The accepted change.
    pub commit: Commit,
    /// The server's `lastSyncId` at or below which this change is
    /// complete (LSE's `syncIdNeededForCompletion`).
    pub threshold: SyncId,
}

/// Outcome of cancelling a change that has not been sent yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The change was removed from the queue.
    Cancelled,
    /// No such batch/ordinal exists.
    NotFound,
    /// The change was already sent or is completing; it cannot be cancelled.
    TooLate,
}

/// Outcome of removing rejected transactions from the queue.
#[derive(Debug, Default)]
pub(crate) struct RejectedOutcome {
    /// Model ids of the removed changes; the caller reconciles each
    /// from an authoritative base.
    pub model_ids: Vec<String>,
    /// Batches whose changes were all rejected; they never resolve as
    /// applied, so the caller drops their persisted copies.
    pub cleared_batches: Vec<BatchId>,
}

struct QueuedBatch {
    batch: CommitBatch,
    persisted: bool,
}

/// Manages the lifecycle of changes from creation to server confirmation.
pub struct TransactionQueue {
    client_id: ClientId,
    /// Random session identity for [`TxnId`] allocation.
    incarnation: u64,
    collecting: Option<CommitBatch>,
    queued: VecDeque<QueuedBatch>,
    /// Batches restored after a crash, frozen until the catch-up phase
    /// (applying the deltas missed while offline) completes. Container
    /// membership, not a flag: `dequeue` never sees them, so a
    /// recovered batch cannot be sent before its model was reconciled
    /// against the server. Normal operation keeps this empty.
    recovered: VecDeque<QueuedBatch>,
    /// Batches delivered to the server, awaiting application
    /// information. A windowed queue (not a single slot): the server
    /// applies batches asynchronously and reports each one through
    /// `DeltaPacket::applied_batch`, so several batches can be in
    /// transit at once. The window size is a memory watermark, not a
    /// correctness bound — the server applies in receive order.
    in_flight: VecDeque<QueuedBatch>,
    /// Upper bound on [`Self::in_flight`]; the dequeue gate.
    in_flight_max: usize,
    awaiting: Vec<AwaitingCommit>,
    next_seq: u64,
    next_batch_id: BatchId,
    last_sync_id: Option<SyncId>,
    /// The model set the client last saw as authoritative (from a
    /// bootstrap snapshot). Snapshot alignment discards a model's
    /// unsynced changes only when the model was in this set and is
    /// absent from the new snapshot — a removal on the server — never
    /// for a model created offline (never in the set).
    known_models: HashSet<String>,
}

impl TransactionQueue {
    /// Create a new empty queue.
    pub fn new(client_id: ClientId) -> Self {
        Self::with_in_flight_max(client_id, DEFAULT_IN_FLIGHT_MAX)
    }

    /// Create a new empty queue with a custom in-flight window.
    ///
    /// A zero window would deadlock the pipeline (nothing could ever
    /// be sent), so `0` falls back to [`DEFAULT_IN_FLIGHT_MAX`].
    pub fn with_in_flight_max(client_id: ClientId, in_flight_max: usize) -> Self {
        let in_flight_max = if in_flight_max == 0 {
            DEFAULT_IN_FLIGHT_MAX
        } else {
            in_flight_max
        };
        Self {
            client_id,
            incarnation: rand::random(),
            collecting: None,
            queued: VecDeque::new(),
            recovered: VecDeque::new(),
            in_flight: VecDeque::new(),
            in_flight_max,
            awaiting: Vec::new(),
            next_seq: 1,
            next_batch_id: 1,
            last_sync_id: None,
            known_models: HashSet::new(),
        }
    }

    /// The client this queue belongs to.
    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    /// The random process incarnation (session identity).
    pub fn session(&self) -> u64 {
        self.incarnation
    }

    /// The in-flight window bound (delivered-but-unapplied batches).
    pub fn in_flight_max(&self) -> usize {
        self.in_flight_max
    }

    /// Allocate the next transaction id. Caller must hold the lock.
    ///
    /// Ids are unique within the session's incarnation; a fresh process
    /// start draws a new random incarnation, so ids are never reused
    /// even after a full cache wipe — the server's transaction-id
    /// deduplication can never mistake a new transaction for a
    /// historical one.
    pub(crate) fn next_txn_id(&mut self) -> TxnId {
        let seq = self.next_seq;
        self.next_seq = self
            .next_seq
            .checked_add(1)
            .expect("transaction seq space exhausted");
        TxnId {
            incarnation: self.incarnation,
            seq,
        }
    }

    fn next_batch_id(&mut self) -> BatchId {
        let id = self.next_batch_id;
        self.next_batch_id = self
            .next_batch_id
            .checked_add(1)
            .expect("batch id space exhausted");
        id
    }

    // ── Write side ─────────────────────────────────────────────────

    /// Append one change's leaf transactions to the collecting batch.
    ///
    /// Called from the sync write path while the store write lock is
    /// held, so batch order equals store publication order. The ordinal
    /// is assigned monotonically within the batch and never reused.
    /// Returns the change as enqueued, for the caller to record (e.g.
    /// as undo history).
    pub fn push_txn(&mut self, txns: Vec<Transaction>) -> Commit {
        if self.collecting.is_none() {
            let id = self.next_batch_id();
            self.collecting = Some(CommitBatch {
                id,
                commits: Vec::new(),
            });
        }
        let batch = self.collecting.as_mut().expect("collecting batch exists");
        let ordinal = batch
            .commits
            .iter()
            .map(|c| c.ordinal)
            .max()
            .map_or(0, |m| m + 1);
        for txn in &txns {
            debug_assert_eq!(txn.client_id, self.client_id, "push_txn stamps client id");
        }
        let commit = Commit { ordinal, txns };
        batch.commits.push(commit.clone());
        commit
    }

    /// Push a change with refreshed identities: re-stamp every
    /// transaction (new id and timestamp), then append it to the
    /// collecting batch. Returns the fresh transactions for optimistic
    /// application to the store.
    ///
    /// A re-run must never reuse a historical id: the server
    /// deduplicates by transaction id, so an old id would be mistaken
    /// for the already-applied original.
    pub(crate) fn push_commit(&mut self, commit: &Commit) -> Vec<Transaction> {
        let fresh = commit.refresh(crate::types::now_millis(), || self.next_txn_id());
        let txns = fresh.txns;
        self.push_txn(txns.clone());
        txns
    }

    /// Close the collecting batch and move it to the queue (LSE
    /// `commitCreatedTransactions`). Returns whether a batch was closed.
    ///
    /// The batch is not sendable until persisted
    /// ([`confirm_persisted`](Self::confirm_persisted)).
    pub fn collect(&mut self) -> bool {
        match self.collecting.take() {
            Some(batch) => {
                self.queued.push_back(QueuedBatch {
                    batch,
                    persisted: false,
                });
                true
            }
            None => false,
        }
    }

    /// Mark the front queued batch as durably persisted. Only the front
    /// may transition: FIFO requires an earlier batch's persistence to
    /// be confirmed before any later batch becomes sendable.
    pub fn confirm_persisted(&mut self, id: BatchId) {
        if let Some(front) = self.queued.front_mut() {
            if front.batch.id == id {
                front.persisted = true;
            }
        }
    }

    /// The front queued batch, if any.
    pub fn queued_front(&self) -> Option<&CommitBatch> {
        self.queued.front().map(|qb| &qb.batch)
    }

    /// The front queued batch when it is not yet durably persisted
    /// (clone for the persist command).
    pub(crate) fn front_unpersisted(&self) -> Option<CommitBatch> {
        let front = self.queued.front()?;
        if front.persisted {
            None
        } else {
            Some(front.batch.clone())
        }
    }

    /// The persisted batch with the given id, in its current state.
    /// Used to rewrite the cache copy after a cancellation or a rebase
    /// (a persisted queued batch, a recovered batch, and the in-flight
    /// batch all have a cache copy).
    pub(crate) fn persisted_batch(&self, id: BatchId) -> Option<CommitBatch> {
        if let Some(qb) = self
            .queued
            .iter()
            .find(|qb| qb.persisted && qb.batch.id == id)
        {
            return Some(qb.batch.clone());
        }
        if let Some(qb) = self
            .recovered
            .iter()
            .find(|qb| qb.persisted && qb.batch.id == id)
        {
            return Some(qb.batch.clone());
        }
        self.in_flight
            .iter()
            .find(|qb| qb.batch.id == id)
            .map(|qb| qb.batch.clone())
    }

    /// Apply reconcile results: update each unsynced transaction's
    /// kind in place (the re-captured `before`), matched by batch,
    /// ordinal, and id. Awaiting changes keep their `before` fresh so
    /// a later undo restores the value the server actually based on.
    ///
    /// Returns the ids of every affected batch whose persisted cache
    /// copy must be rewritten: a queued batch already marked persisted,
    /// a recovered batch, and any in-flight batch (all have a cache
    /// copy that would otherwise recover the stale pre-rebase
    /// `before`). Unpersisted queued and collecting batches have no
    /// cache copy yet — their first persist already uses the rebased
    /// state.
    pub(crate) fn apply_rebased(
        &mut self,
        rebased: Vec<(BatchId, u32, Transaction)>,
    ) -> Vec<BatchId> {
        let mut dirty: HashSet<BatchId> = HashSet::new();
        for (batch_id, ordinal, mut txn) in rebased {
            // Try each container in lifecycle order; a batch lives in
            // exactly one, and the transaction's kind is moved out
            // only when its entry is found.
            let (updated, rewrite) = self.rebase_one(batch_id, ordinal, &mut txn);
            if !updated {
                for a in &mut self.awaiting {
                    if a.batch_id == batch_id && a.commit.ordinal == ordinal {
                        if let Some(t) = a.commit.txns.iter_mut().find(|t| t.id == txn.id) {
                            t.kind = txn.kind;
                        }
                        break;
                    }
                }
            }
            if rewrite {
                dirty.insert(batch_id);
            }
        }
        dirty.into_iter().collect()
    }

    /// Apply one rebased transaction to its batch, in lifecycle
    /// order: collecting, queued, recovered, then in-flight. Returns
    /// whether the transaction was found and whether the batch's
    /// persisted cache copy must be rewritten.
    fn rebase_one(
        &mut self,
        batch_id: BatchId,
        ordinal: u32,
        txn: &mut Transaction,
    ) -> (bool, bool) {
        if let Some(batch) = self.collecting.as_mut().filter(|b| b.id == batch_id) {
            return (apply_one(batch, ordinal, txn), false);
        }
        for qb in &mut self.queued {
            if qb.batch.id == batch_id {
                let applied = apply_one(&mut qb.batch, ordinal, txn);
                return (applied, applied && qb.persisted);
            }
        }
        for qb in &mut self.recovered {
            if qb.batch.id == batch_id {
                let applied = apply_one(&mut qb.batch, ordinal, txn);
                return (applied, applied && qb.persisted);
            }
        }
        if let Some(qb) = self.in_flight.iter_mut().find(|qb| qb.batch.id == batch_id) {
            let applied = apply_one(&mut qb.batch, ordinal, txn);
            return (applied, applied);
        }
        (false, false)
    }

    /// Discard every unsent change of a model (Clear/Archive action).
    ///
    /// Only unsent changes are dropped: an in-flight change is already
    /// on its way to the server and an accepted change keeps its
    /// completion tracking — neither can pretend it never existed.
    ///
    /// Returns the ids of every persisted batch whose cache copy must
    /// be rewritten or removed to match the discarded state: a crash
    /// must not recover and re-send a discarded change. Recovered
    /// batches are included — the catch-up phase must discard them
    /// before they become sendable.
    pub(crate) fn discard_model(&mut self, model_id: &str) -> Vec<BatchId> {
        let mut dirty: HashSet<BatchId> = HashSet::new();
        let keep = |commits: &mut Vec<Commit>| {
            let len = commits.len();
            commits.retain(|c| !c.txns.iter().any(|t| t.model_id == model_id));
            commits.len() != len
        };
        if let Some(b) = &mut self.collecting {
            keep(&mut b.commits);
        }
        for qb in &mut self.queued {
            if keep(&mut qb.batch.commits) && qb.persisted {
                dirty.insert(qb.batch.id);
            }
        }
        for qb in &mut self.recovered {
            if keep(&mut qb.batch.commits) && qb.persisted {
                dirty.insert(qb.batch.id);
            }
        }
        dirty.into_iter().collect()
    }

    /// Move the front persisted batch to in-flight and return it for
    /// sending.
    ///
    /// Returns `None` when the in-flight window is full, the queue is
    /// empty, or the front batch is not yet persisted. A batch is never
    /// partially dequeued.
    pub fn dequeue(&mut self) -> Option<CommitBatch> {
        if self.in_flight.len() >= self.in_flight_max {
            return None;
        }
        let front = self.queued.front()?;
        if !front.persisted {
            return None;
        }
        let qb = self.queued.pop_front()?;
        let batch = qb.batch.clone();
        self.in_flight.push_back(qb);
        Some(batch)
    }

    /// Move a specific in-flight batch back to the queue head for
    /// re-sending.
    ///
    /// LSE: a transaction that was sent but never confirmed is resent
    /// (idempotent by transaction id). The batch keeps its identity and
    /// its persistence; only batches still awaiting application are
    /// re-queued. Unknown ids are ignored.
    pub(crate) fn requeue(&mut self, batch_id: BatchId) {
        if let Some(pos) = self.in_flight.iter().position(|b| b.batch.id == batch_id) {
            let qb = self.in_flight.remove(pos).unwrap();
            self.queued.push_front(qb);
        }
    }

    /// Report a batch as applied by the server.
    ///
    /// Moves the batch from in-flight to [`Self::awaiting`] with
    /// `threshold` as the completion threshold for each change (LSE's
    /// `syncIdNeededForCompletion`). The caller must have removed any
    /// rejected changes first ([`Self::remove_rejected`]).
    ///
    /// Returns `true` when the batch is fully resolved (every change is
    /// accounted for), signalling the caller to drop the persisted
    /// copy. Unknown batch ids are ignored (idempotent).
    pub fn mark_applied(&mut self, batch_id: BatchId, threshold: SyncId) -> bool {
        let pos = match self.in_flight.iter().position(|b| b.batch.id == batch_id) {
            Some(pos) => pos,
            None => return false,
        };
        let qb = self.in_flight.remove(pos).unwrap();
        for commit in qb.batch.commits {
            self.awaiting.push(AwaitingCommit {
                batch_id,
                commit,
                threshold,
            });
        }
        true
    }

    /// Remove rejected leaves from in-flight and awaiting changes.
    ///
    /// Rejection granularity is per leaf transaction (the server may
    /// reject individual leaves, e.g. a per-field permission check). A
    /// rejected leaf is dropped from its change and never replayed; the
    /// change keeps its accepted leaves. A change whose leaves are all
    /// rejected disappears, and a batch whose changes are all gone is
    /// reported as cleared — the server will never report it applied.
    ///
    /// A normal server rejects whole changes (one publication applies
    /// atomically): all leaves of the change appear in `rejected`
    /// together, and the change is dropped as a unit.
    ///
    /// Returns the affected model ids (to reconcile) and the ids of
    /// batches that became empty (to drop from the cache).
    pub(crate) fn remove_rejected(&mut self, rejected: &[TxnId]) -> RejectedOutcome {
        let mut outcome = RejectedOutcome::default();
        if rejected.is_empty() {
            return outcome;
        }
        let rejected_set: HashSet<TxnId> = rejected.iter().copied().collect();
        let mut models = HashSet::new();

        // Remove rejected leaves from awaiting changes and shrink the
        // rest. Applied changes cannot really be rejected; this guards
        // against malformed servers.
        self.awaiting.retain_mut(|a| {
            for txn in a
                .commit
                .txns
                .iter()
                .filter(|t| rejected_set.contains(&t.id))
            {
                models.insert(txn.model_id.clone());
            }
            a.commit.txns.retain(|t| !rejected_set.contains(&t.id));
            !a.commit.txns.is_empty()
        });

        // Remove rejected leaves from in-flight changes and shrink the
        // rest; drop changes that become empty, then batches that
        // become empty.
        let mut kept = VecDeque::new();
        while let Some(mut qb) = self.in_flight.pop_front() {
            for commit in &mut qb.batch.commits {
                for txn in commit.txns.iter().filter(|t| rejected_set.contains(&t.id)) {
                    models.insert(txn.model_id.clone());
                }
                commit.txns.retain(|t| !rejected_set.contains(&t.id));
            }
            qb.batch.commits.retain(|c| !c.txns.is_empty());
            if qb.batch.commits.is_empty() {
                outcome.cleared_batches.push(qb.batch.id);
            } else {
                kept.push_back(qb);
            }
        }
        self.in_flight = kept;
        outcome.model_ids = models.into_iter().collect();
        outcome
    }

    // ── Inbound ────────────────────────────────────────────────────

    /// Advance the inbound anchor and complete awaiting changes whose
    /// threshold is now reached.
    ///
    /// LSE step 6-7 of delta application: update the client's
    /// `last_sync_id`, then complete every change whose completion
    /// threshold is satisfied. Completed changes leave the queue.
    ///
    /// Returns the completed changes.
    pub(crate) fn advance_sync(&mut self, delta_sync_id: SyncId) -> Vec<AwaitingCommit> {
        let new_last = self
            .last_sync_id
            .map_or(delta_sync_id, |l| l.max(delta_sync_id));
        self.last_sync_id = Some(new_last);
        self.complete_ready()
    }

    /// Complete every awaiting change whose threshold is already
    /// satisfied by the current anchor.
    ///
    /// Invoked after the anchor advances and after a batch is resolved.
    /// The latter covers the recovery path: a batch re-sent after a
    /// crash may be deduplicated by the server, which returns its
    /// original threshold; when that threshold is at or below the
    /// restored anchor, the change must complete immediately instead of
    /// waiting for a delta that may never arrive (the server may have
    /// nothing new to broadcast).
    pub(crate) fn complete_ready(&mut self) -> Vec<AwaitingCommit> {
        match self.last_sync_id {
            Some(last) => self.complete_ready_for(last),
            None => Vec::new(),
        }
    }

    /// Complete every awaiting change whose threshold is at or below
    /// the given sync id, without advancing the anchor.
    ///
    /// The adapter uses this against the incoming packet's sync id
    /// before reconciling, so a change the packet confirms is not
    /// replayed as unsynced. The anchor itself advances only after
    /// the packet is durably persisted ([`Self::advance_sync`]).
    pub(crate) fn complete_ready_for(&mut self, sync_id: SyncId) -> Vec<AwaitingCommit> {
        let mut completed = Vec::new();
        self.awaiting.retain_mut(|a| {
            if a.threshold <= sync_id {
                completed.push(a.clone());
                false
            } else {
                true
            }
        });
        completed
    }

    // ── Recovery ───────────────────────────────────────────────────

    /// Restore persisted batches and the inbound anchor after a crash.
    ///
    /// Rebuilt batches are already durable. Validates the recovered
    /// data: every transaction must belong to this client and carry no
    /// completion threshold (a pre-send batch is never synced). The
    /// restored anchor resumes inbound polling from the last applied
    /// delta, so the server only needs to retain deltas after it.
    ///
    /// Recovered batches land in the frozen [`Self::recovered`]
    /// container: they are not sendable until
    /// [`finish_catch_up`](Self::finish_catch_up) releases them after
    /// the catch-up phase has reconciled them against the server.
    pub fn recover(
        &mut self,
        batches: Vec<CommitBatch>,
        anchor: Option<SyncId>,
        known_models: Vec<String>,
    ) {
        // Recovered batches already occupy ids in the store. New batches
        // must never reuse them (an overwrite would silently destroy the
        // persisted copy of a not-yet-confirmed batch), so advance the
        // allocator past the highest recovered id.
        if let Some(max_id) = batches.iter().map(|b| b.id).max() {
            self.next_batch_id = self.next_batch_id.max(max_id + 1);
        }
        if let Some(anchor) = anchor {
            self.last_sync_id = Some(anchor);
        }
        self.known_models = known_models.into_iter().collect();
        for batch in batches {
            for commit in &batch.commits {
                for txn in &commit.txns {
                    assert_eq!(
                        txn.client_id, self.client_id,
                        "recovered batch belongs to another client"
                    );
                    // A recovered transaction cannot carry completion
                    // state: the field that used to hold it is gone.
                }
            }
            self.recovered.push_back(QueuedBatch {
                batch,
                persisted: true,
            });
        }
    }

    /// Release recovered batches after the catch-up phase: the queue
    /// has applied every pending server delta (reconciling and
    /// discarding the recovered batches as needed), so they may now
    /// enter the normal FIFO.
    ///
    /// Recovered batches join the queue **ahead** of any batch written
    /// after recovery (they existed first), preserving the global FIFO.
    /// Normal operation never calls this (the container is empty).
    pub fn finish_catch_up(&mut self) {
        let fresh = std::mem::take(&mut self.queued);
        self.queued.extend(self.recovered.drain(..));
        self.queued.extend(fresh);
    }

    // ── Cancellation ───────────────────────────────────────────────

    /// Remove an unsent change (queue-only dedup, LSE
    /// `cancelTransaction`). Never touches the store; the caller decides
    /// whether to reconcile.
    pub fn remove_commit(&mut self, batch_id: BatchId, ordinal: u32) -> CancelOutcome {
        if let Some(batch) = &mut self.collecting {
            if batch.id == batch_id {
                return match batch.commits.iter().position(|c| c.ordinal == ordinal) {
                    Some(pos) => {
                        batch.commits.remove(pos);
                        CancelOutcome::Cancelled
                    }
                    None => CancelOutcome::NotFound,
                };
            }
        }
        for qb in &mut self.queued {
            if qb.batch.id == batch_id {
                return match qb.batch.commits.iter().position(|c| c.ordinal == ordinal) {
                    Some(pos) => {
                        qb.batch.commits.remove(pos);
                        CancelOutcome::Cancelled
                    }
                    None => CancelOutcome::NotFound,
                };
            }
        }
        for qb in &mut self.recovered {
            if qb.batch.id == batch_id {
                return match qb.batch.commits.iter().position(|c| c.ordinal == ordinal) {
                    Some(pos) => {
                        qb.batch.commits.remove(pos);
                        CancelOutcome::Cancelled
                    }
                    None => CancelOutcome::NotFound,
                };
            }
        }
        // Both the in-flight and awaiting checks are batch-scoped:
        // every batch's first commit has ordinal 0, so an ordinal-only
        // match across batches would report TooLate for a nonexistent
        // batch.
        if self.in_flight.iter().any(|b| b.batch.id == batch_id)
            || self
                .awaiting
                .iter()
                .any(|a| a.batch_id == batch_id && a.commit.ordinal == ordinal)
        {
            return CancelOutcome::TooLate;
        }
        CancelOutcome::NotFound
    }

    // ── Queries ────────────────────────────────────────────────────

    /// All unconfirmed changes for a model, oldest first (awaiting →
    /// recovered → in-flight → queued → collecting), each paired with
    /// its batch id. Feed to [`crate::reconcile`] as the unsynced set:
    /// the rebase replays oldest intent first so the newest intent
    /// wins — the same order the server applies the batches in. The
    /// recovered batches precede the fresh queued ones because they
    /// were committed before the crash.
    pub fn unsynced_changes(&self, model_id: &str) -> Vec<(BatchId, Commit)> {
        let mut out = Vec::new();
        let mut push_model = |batch_id: BatchId, commits: &[Commit]| {
            for c in commits {
                if c.txns.iter().any(|t| t.model_id == model_id) {
                    out.push((batch_id, c.clone()));
                }
            }
        };
        for a in &self.awaiting {
            push_model(a.batch_id, std::slice::from_ref(&a.commit));
        }
        for qb in &self.recovered {
            push_model(qb.batch.id, &qb.batch.commits);
        }
        for qb in &self.in_flight {
            push_model(qb.batch.id, &qb.batch.commits);
        }
        for qb in &self.queued {
            push_model(qb.batch.id, &qb.batch.commits);
        }
        if let Some(b) = &self.collecting {
            push_model(b.id, &b.commits);
        }
        out
    }

    /// True while recovered batches are still frozen (the catch-up
    /// phase has not released them yet).
    pub fn has_recovered(&self) -> bool {
        !self.recovered.is_empty()
    }

    /// The set of model ids with unsynced (not yet completed)
    /// changes, in first-seen order across the containers.
    pub(crate) fn unsynced_models(&self) -> Vec<String> {
        let mut models: Vec<String> = Vec::new();
        let push_commit = |commits: &[Commit], models: &mut Vec<String>| {
            for c in commits {
                for t in &c.txns {
                    if !models.contains(&t.model_id) {
                        models.push(t.model_id.clone());
                    }
                }
            }
        };
        if let Some(b) = &self.collecting {
            push_commit(&b.commits, &mut models);
        }
        for qb in &self.recovered {
            push_commit(&qb.batch.commits, &mut models);
        }
        for qb in &self.queued {
            push_commit(&qb.batch.commits, &mut models);
        }
        for qb in &self.in_flight {
            push_commit(&qb.batch.commits, &mut models);
        }
        for a in &self.awaiting {
            push_commit(std::slice::from_ref(&a.commit), &mut models);
        }
        models
    }

    /// The queue's inbound anchor: the highest delta sync id applied.
    pub fn last_sync_id(&self) -> Option<SyncId> {
        self.last_sync_id
    }

    /// Abandon the inbound anchor (the server answered
    /// `ResetRequired`): the next poll bootstraps with `None`.
    pub(crate) fn reset_anchor(&mut self) {
        self.last_sync_id = None;
    }

    /// Remember the authoritative model set from a bootstrap snapshot.
    pub(crate) fn remember_models(&mut self, models: &[String]) {
        self.known_models = models.iter().cloned().collect();
    }

    /// The last authoritative model set (see the `known_models` field).
    pub(crate) fn known_models(&self) -> &HashSet<String> {
        &self.known_models
    }

    /// Accepted changes still waiting for their confirming delta.
    pub fn awaiting_commits(&self) -> &[AwaitingCommit] {
        &self.awaiting
    }

    /// The batches currently in flight, in send order.
    pub fn in_flight_batches(&self) -> impl Iterator<Item = &CommitBatch> {
        self.in_flight.iter().map(|qb| &qb.batch)
    }

    /// The front-most in-flight batch, if any.
    pub fn in_flight_front(&self) -> Option<&CommitBatch> {
        self.in_flight.front().map(|qb| &qb.batch)
    }

    /// True when no change is collecting, queued, recovered, in flight,
    /// or awaiting its confirming delta. An empty collecting batch
    /// (every change cancelled) counts as idle; a frozen recovered
    /// batch does not.
    pub fn is_idle(&self) -> bool {
        self.collecting
            .as_ref()
            .is_none_or(|b| b.commits.is_empty())
            && self.queued.is_empty()
            && self.recovered.is_empty()
            && self.in_flight.is_empty()
            && self.awaiting.is_empty()
    }
}

/// Update one transaction's kind (the rebased `before`) within a
/// batch's matching change. The rebased transaction's kind is moved
/// out only when its entry is found; a failed match leaves it
/// untouched for the next container.
fn apply_one(batch: &mut CommitBatch, ordinal: u32, txn: &mut Transaction) -> bool {
    if let Some(commit) = batch.commits.iter_mut().find(|c| c.ordinal == ordinal) {
        if let Some(t) = commit.txns.iter_mut().find(|t| t.id == txn.id) {
            t.kind = std::mem::replace(
                &mut txn.kind,
                Changed::Replace {
                    before: None,
                    after: None,
                },
            );
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use crate::{Changed, Edit};

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

    fn make_batch(id: u64, txns: Vec<Transaction>) -> CommitBatch {
        CommitBatch {
            id,
            commits: vec![Commit { ordinal: 0, txns }],
        }
    }

    fn txns(batch: &CommitBatch) -> Vec<&Transaction> {
        batch.commits.iter().flat_map(|c| c.txns.iter()).collect()
    }

    fn flush_and_persist(q: &mut TransactionQueue) {
        q.collect();
        if let Some(front) = q.queued_front() {
            let id = front.id;
            q.confirm_persisted(id);
        }
    }

    #[test]
    fn lifecycle_collect_dequeue_mark_applied_advance() {
        let mut q = TransactionQueue::new(1);

        q.push_txn(vec![make_txn(1, replace(json!("x")))]);
        q.push_txn(vec![make_txn(2, replace(json!("y")))]);
        assert!(q.collect(), "collect closes the batch");
        assert!(!q.collect(), "second collect is a no-op");

        assert!(q.dequeue().is_none(), "unpersisted batch is not sendable");
        flush_and_persist(&mut q);

        let batch = q.dequeue().expect("persisted batch is sendable");
        assert_eq!(batch.id, 1);
        assert_eq!(batch.commits.len(), 2, "same-cycle writes share one batch");

        // The in-flight window allows more batches (default 16), so a
        // second dequeue after persisting the next batch also succeeds.
        assert!(q.dequeue().is_none(), "queue empty after the only batch");

        assert!(q.mark_applied(batch.id, 42), "batch fully resolved");
        assert_eq!(
            q.awaiting_commits().len(),
            2,
            "both same-cycle changes await their confirming delta",
        );

        // advance_sync does not complete until the confirming delta.
        assert!(q.advance_sync(41).is_empty(), "threshold not reached");
        assert!(!q.is_idle());
        let completed = q.advance_sync(42);
        assert_eq!(completed.len(), 2);
        assert!(q.is_idle());
        assert_eq!(q.last_sync_id(), Some(42));
    }

    #[test]
    fn in_flight_window_limits_concurrent_sends() {
        let mut q = TransactionQueue::with_in_flight_max(1, 2);
        for i in 1..=3u64 {
            q.push_txn(vec![make_txn(i, replace(json!(i)))]);
            q.collect();
        }
        // The persist gate is front-only: confirm each batch only once
        // it becomes the front (after the previous one is dequeued).
        q.confirm_persisted(1);
        let first = q.dequeue().expect("first batch sendable");
        q.confirm_persisted(2);
        let second = q.dequeue().expect("second batch sendable");
        assert!(q.dequeue().is_none(), "window full");
        assert_eq!(first.id, 1);
        assert_eq!(second.id, 2);

        // The third batch is persisted (now the front) but the window
        // is full; resolving one frees a slot.
        q.confirm_persisted(3);
        assert!(q.dequeue().is_none(), "window still full");
        q.mark_applied(first.id, 1);
        let third = q.dequeue().expect("slot freed after application");
        assert_eq!(third.id, 3, "FIFO order preserved");
    }

    #[test]
    fn remove_rejected_drops_change_and_reports_model() {
        let mut q = TransactionQueue::new(1);
        q.push_txn(vec![make_txn(1, replace(json!("x")))]);
        flush_and_persist(&mut q);
        let batch = q.dequeue().unwrap();
        let id = txns(&batch)[0].id;

        let outcome = q.remove_rejected(&[id]);
        assert_eq!(outcome.model_ids, vec!["test"]);
        assert_eq!(outcome.cleared_batches, vec![batch.id], "batch emptied");
        assert!(q.in_flight_front().is_none());
        assert!(q.awaiting_commits().is_empty());
    }

    #[test]
    fn remove_rejected_keeps_remaining_changes() {
        let mut q = TransactionQueue::new(1);
        // One batch, two changes: one rejected, one kept.
        let mut rejected = make_txn(1, replace(json!("x")));
        rejected.model_id = "dropme".into();
        let mut kept = make_txn(2, replace(json!("y")));
        kept.model_id = "keepme".into();
        let kept_id = kept.id;
        // One push = one commit: two changes in one batch.
        q.push_txn(vec![rejected]);
        q.push_txn(vec![kept]);
        flush_and_persist(&mut q);
        let batch = q.dequeue().unwrap();
        let rejected_id = batch.commits[0].txns[0].id;

        let outcome = q.remove_rejected(&[rejected_id]);
        assert_eq!(outcome.model_ids, vec!["dropme"]);
        assert!(outcome.cleared_batches.is_empty(), "batch keeps one change");
        assert_eq!(
            q.in_flight_front().unwrap().commits.len(),
            1,
            "kept change stays"
        );

        // The kept change resolves normally once reported applied.
        assert!(q.mark_applied(batch.id, 7));
        assert_eq!(q.awaiting_commits().len(), 1);
        assert_eq!(q.awaiting_commits()[0].commit.txns[0].id, kept_id);
    }

    #[test]
    fn requeue_returns_batch_to_front() {
        let mut q = TransactionQueue::new(1);
        q.push_txn(vec![make_txn(1, replace(json!("x")))]);
        flush_and_persist(&mut q);
        let batch = q.dequeue().unwrap();

        q.requeue(batch.id);
        assert!(q.in_flight_front().is_none());
        let again = q.dequeue().expect("re-queued batch is sendable");
        assert_eq!(again.id, batch.id, "batch identity preserved");
    }

    #[test]
    fn requeue_ignores_unknown_id() {
        let mut q = TransactionQueue::new(1);
        q.push_txn(vec![make_txn(1, replace(json!("x")))]);
        flush_and_persist(&mut q);
        let batch = q.dequeue().unwrap();

        q.requeue(99);
        assert_eq!(
            q.in_flight_front().unwrap().id,
            batch.id,
            "unknown id is a no-op"
        );
    }

    #[test]
    fn fifo_persist_gate_blocks_later_batches() {
        let mut q = TransactionQueue::new(1);
        q.push_txn(vec![make_txn(1, replace(json!("a")))]);
        q.collect(); // batch 1: not persisted
        q.push_txn(vec![make_txn(2, replace(json!("b")))]);
        q.collect(); // batch 2: not persisted

        // Even if batch 2 were somehow confirmed, the front (batch 1)
        // blocks it. Confirm only batch 2 and verify it is still blocked.
        q.confirm_persisted(2);
        assert!(
            q.dequeue().is_none(),
            "earlier unpersisted batch blocks later ones"
        );
        q.confirm_persisted(1);
        let first = q.dequeue().expect("front batch sendable after persist");
        assert_eq!(first.id, 1, "FIFO order");

        // Batch 2 is now the front; confirming its persist lets the
        // window carry both batches in flight.
        q.confirm_persisted(2);
        let second = q.dequeue().expect("second batch sendable");
        assert_eq!(second.id, 2, "FIFO order");
    }

    #[test]
    fn push_commit_refreshes_identities() {
        let mut q = TransactionQueue::new(1);
        let txn = make_txn(1, replace(json!("a")));
        let original = Commit {
            ordinal: 0,
            txns: vec![txn],
        };

        // push_commit re-stamps every transaction with a new id: the
        // returned transactions are the new identities, the queue holds
        // them, and the historical id is never reused.
        let fresh = q.push_commit(&original);
        assert_eq!(fresh.len(), 1);
        assert_ne!(
            fresh[0].id, original.txns[0].id,
            "new id, never the historical one"
        );

        // The refreshed change is queued as a normal change: it syncs
        // and completes like any other.
        flush_and_persist(&mut q);
        let batch = q.dequeue().unwrap();
        assert_eq!(
            txns(&batch)[0].id,
            fresh[0].id,
            "queue holds the fresh identities"
        );
        assert!(q.mark_applied(batch.id, 10));
        let completed = q.advance_sync(10);
        assert_eq!(completed.len(), 1, "refreshed change completes normally");
        assert!(q.is_idle());
    }

    #[test]
    fn cancel_removes_unsent_commit() {
        let mut q = TransactionQueue::new(1);
        q.push_txn(vec![make_txn(1, replace(json!("x")))]);
        // The collecting batch's id is 1 (first allocation); the commit
        // carries ordinal 0.
        assert_eq!(q.remove_commit(1, 0), CancelOutcome::Cancelled);
        assert!(q.is_idle(), "the only change was cancelled");

        assert_eq!(q.remove_commit(99, 0), CancelOutcome::NotFound);

        q.push_txn(vec![make_txn(2, replace(json!("y")))]);
        flush_and_persist(&mut q);
        let batch = q.dequeue().unwrap();
        assert_eq!(q.remove_commit(batch.id, 0), CancelOutcome::TooLate);
    }

    #[test]
    fn recover_restores_durable_batches() {
        let mut q = TransactionQueue::new(1);
        let batch = make_batch(3, vec![make_txn(1, replace(json!("x")))]);
        q.recover(vec![batch], None, Vec::new());
        // Frozen: the recovered batch is not sendable until catch-up.
        assert!(q.queued_front().is_none(), "recovered batch is frozen");
        assert!(q.dequeue().is_none(), "frozen batch is not sendable");
        assert!(!q.is_idle(), "frozen batch counts as pending work");
        // Catch-up completes: the batch joins the FIFO and is sendable.
        q.finish_catch_up();
        let front = q.queued_front().expect("recovered batch is queued");
        assert_eq!(front.id, 3);
        let dequeued = q.dequeue().expect("recovered batch is durable");
        assert_eq!(dequeued.id, 3, "recovery preserves batch identity");
    }

    #[test]
    fn finish_catch_up_preserves_fifo_order() {
        let mut q = TransactionQueue::new(1);
        q.recover(
            vec![
                make_batch(3, vec![make_txn(1, replace(json!("x")))]),
                make_batch(7, vec![make_txn(2, replace(json!("y")))]),
            ],
            None,
            Vec::new(),
        );
        // A post-recovery write lands in the normal queue.
        q.push_txn(vec![make_txn(3, replace(json!("z")))]);
        q.collect();
        // Catch-up releases the recovered batches ahead of the fresh one.
        q.finish_catch_up();
        let ids: Vec<u64> = q.queued.iter().map(|qb| qb.batch.id).collect();
        assert_eq!(
            ids,
            vec![3, 7, 8],
            "recovered batches precede fresh ones, FIFO kept"
        );
    }

    #[test]
    fn recover_advances_batch_id_allocator() {
        let mut q = TransactionQueue::new(1);
        // Recovered batches occupy ids 3 and 7 in the store.
        q.recover(
            vec![
                make_batch(3, vec![make_txn(1, replace(json!("x")))]),
                make_batch(7, vec![make_txn(2, replace(json!("y")))]),
            ],
            None,
            Vec::new(),
        );
        // A fresh write after recovery must not reuse a recovered id:
        // an overwrite would destroy the persisted copy of a
        // not-yet-confirmed batch.
        q.push_txn(vec![make_txn(3, replace(json!("z")))]);
        q.collect();
        // The new batch is queued (the recovered ones are still frozen
        // in the recovered container).
        let new_id = q
            .queued
            .iter()
            .last()
            .expect("new batch is queued")
            .batch
            .id;
        assert!(
            new_id > 7,
            "new batch id {new_id} does not reuse recovered ids",
        );
    }

    #[test]
    fn recover_restores_anchor_and_completes_ready_batches() {
        let mut q = TransactionQueue::new(1);
        // A batch was persisted, sent, and acknowledged (threshold 5)
        // before the crash; the anchor had advanced to 5.
        let batch = make_batch(1, vec![make_txn(1, replace(json!("x")))]);
        q.recover(vec![batch], Some(5), Vec::new());
        assert_eq!(q.last_sync_id(), Some(5), "anchor restored");

        // Resend: the server deduplicates and returns the original
        // threshold, which is already at or below the restored anchor.
        q.finish_catch_up();
        let b = q.dequeue().expect("recovered batch is sendable");
        // Resend: the server deduplicates and reports the original
        // application sync id, which is at or below the restored anchor.
        assert!(q.mark_applied(b.id, 5), "batch fully resolved");
        // The driver completes deduplicated batches against the
        // restored anchor immediately (on_sent → complete_ready).
        q.complete_ready();
        assert!(
            q.awaiting_commits().is_empty(),
            "resolved change completed immediately (threshold met)",
        );
        assert!(q.is_idle());
    }

    #[test]
    fn unsynced_changes_filters_by_model_in_order() {
        let mut q = TransactionQueue::new(1);
        let mut other = make_txn(1, replace(json!("x")));
        other.model_id = "other".into();
        let mut mine = make_txn(2, replace(json!("y")));
        mine.model_id = "test".into();
        q.push_txn(vec![other]);
        q.push_txn(vec![mine]);

        let out = q.unsynced_changes("test");
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].1.txns[0].id.seq, 2,
            "only the matching model's change"
        );
    }

    #[test]
    fn unsynced_changes_orders_oldest_first_across_containers() {
        let mut q = TransactionQueue::new(1);
        // Change 1 (oldest): pushed, collected, persisted, dequeued
        // into in-flight.
        q.push_txn(vec![make_txn(1, replace(json!("a")))]);
        assert!(q.collect());
        let batch = q.queued_front().unwrap().clone();
        q.confirm_persisted(batch.id);
        q.dequeue().expect("dequeues into in-flight");
        // Change 2 (newest): still collecting.
        q.push_txn(vec![make_txn(2, replace(json!("b")))]);

        // The rebase replays oldest first so the newest intent wins —
        // the same order the server applies the batches in.
        let out = q.unsynced_changes("test");
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].1.txns[0].id.seq, 1, "oldest (in-flight) first");
        assert_eq!(out[1].1.txns[0].id.seq, 2, "newest (collecting) last");
    }

    #[test]
    fn ids_have_incarnation() {
        let mut q = TransactionQueue::new(1);
        let a = q.next_txn_id();
        let b = q.next_txn_id();
        assert_eq!(a.incarnation, b.incarnation, "same session");
        assert_ne!(a.seq, b.seq);
        assert!(a.seq < b.seq);
    }

    #[test]
    fn remove_commit_awaiting_ordinal_is_batch_scoped() {
        let mut q = TransactionQueue::new(1);
        // Awaiting: batch 1's first commit (ordinal 0).
        let batch = make_batch(
            1,
            vec![make_txn(
                1,
                Changed::Replace {
                    before: None,
                    after: Some(json!(1)),
                },
            )],
        );
        q.awaiting.push(AwaitingCommit {
            batch_id: 1,
            commit: batch.commits[0].clone(),
            threshold: 1,
        });
        // Cancel batch 2's ordinal 0 — batch 2 does not exist
        // anywhere, so this must be NotFound, not TooLate.
        let outcome = q.remove_commit(2, 0);
        assert_eq!(
            outcome,
            CancelOutcome::NotFound,
            "a nonexistent batch must not be TooLate"
        );
    }
}
