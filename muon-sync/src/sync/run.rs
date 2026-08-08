//! Async adapter: drives the pure synchronous [`SyncDriver`] against a
//! [`SyncTransport`] and a [`TransactionCache`].
//!
//! This layer is deliberately **runtime-free**: it is a plain `async fn`
//! with no tokio/async-std dependency. The caller owns the executor —
//! spawn [`sync_loop`] on any runtime, or call [`sync_step`] from a
//! timer/callback of their choosing.
//!
//! Storage is an adapter too: [`sync_step`] takes any
//! [`TransactionCache`] (a redb file via [`RedbCache`], memory, a database)
//! and never hard-codes a concrete implementation.
//!
//! The authoritative view of the remote lives in the caller's
//! [`RemoteView`](crate::RemoteView): the step updates it from inbound deltas and
//! publishes every reconciled final value through the `publish` callback,
//! which the caller routes into its typed stores.
//!
//! Error model:
//! - A **network failure** re-queues the affected in-flight batches and
//!   returns [`SyncLoopError::Network`]; the next call retries
//!   (idempotent by transaction id).
//! - A **whole-batch transport rejection** ([`SendError::Rejected`])
//!   rolls the batch back, reconciles its models, and returns
//!   [`SyncLoopError::Rejected`] — final, not retried.
//! - An **application-level rejection** (reported through
//!   [`DeltaPacket::rejected`]) drops the rejected changes, reconciles
//!   their models, and is *not* an error.
//! - A **persistent failure** (storage I/O) returns
//!   [`SyncLoopError::Store`]; these are storage errors and are not
//!   retried automatically.
//!
//! Server contract (crash recovery depends on all of these):
//! - **Delivery is not application**: [`SyncTransport::send`] returns as
//!   soon as the server accepted the batch. The server applies batches
//!   in receive order and reports each applied batch through
//!   [`DeltaPacket::applied_batch`]; the client completes the batch
//!   when its anchor reaches the packet's sync id.
//! - **Dedup resends**: a re-sent transaction id must not be applied
//!   again, and the response must carry the original application sync
//!   id through [`SendResponse::deduped_at`] — the client completes a
//!   recovered resend as soon as the threshold is at or below its
//!   restored anchor.
//! - **Bootstrap snapshots**: a poll with `since: None` (first start
//!   or lost anchor) returns the full current state — one `Value`
//!   action per known model — instead of replaying the whole delta
//!   history. The snapshot defines the authoritative model set: local
//!   changes for a model absent from it are discarded (the model was
//!   removed while the client was away).
//! - **Merge-patch updates**: an `Update` action follows RFC 7396 JSON
//!   Merge Patch — objects merge recursively, `null` removes a member,
//!   other values replace. This is how keyed deletions converge.
//!   Incremental updates require a baseline: the client must already
//!   hold the model's full value (a bootstrap snapshot or earlier
//!   packets) before merging an increment onto it. After a crash, the
//!   adapter rebuilds the remote from its locally persisted value;
//!   without one (a new device, lost local data) it must abandon the
//!   restored anchor and bootstrap with `None`.
//!   The client's remote becomes the snapshot, and polling resumes
//!   incrementally from the snapshot's `sync_id`.
//! - **Delta retention**: the server must retain deltas at least as far
//!   back as every client's persisted anchor (the client resumes
//!   polling from its anchor after a crash, never from `None`).

use futures::future::join_all;
use serde_json::Value;
use std::collections::HashSet;

use crate::{
    BatchKey, Commit, DeltaPacket, PollOutcome, RemoteView, SendError, SyncCommand, SyncDriver,
    SyncTransport, Transaction, TransactionCache,
};

/// Errors from one synchronization step.
#[derive(Debug, thiserror::Error)]
pub enum SyncLoopError {
    /// The transport failed (network down, timeout, server unreachable).
    /// Safe to retry; nothing was lost.
    #[error("transport failure: {0}")]
    Network(String),
    /// The server rejected a whole batch. The batch was rolled back and
    /// the model reconciled; this is final — do not retry.
    #[error("server rejected the batch: {0}")]
    Rejected(String),
    /// Applying a transaction to a store failed.
    #[error("delta application failed: {0}")]
    Delta(#[from] crate::DeltaApplyError),
    /// The transaction store failed.
    #[error("store failure: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// Run one synchronization step: send queued changes, then poll and
/// apply inbound deltas.
///
/// `remote` is the caller's authoritative view of the remote; `publish` receives
/// every reconciled final value (model id + JSON) and is responsible for
/// deserializing it into the store and notifying subscribers.
/// `on_completed` receives every change the server confirmed; the caller
/// may keep it in an application-level undo history (an empty closure
/// disables undo support entirely).
///
/// `cache` is optional: `None` runs without crash recovery — a crash
/// loses unsent local changes, and the next start bootstraps the full
/// state from the server (a poll with `None`). `Some(cache)` persists
/// batches and the inbound anchor, so a crash resumes instead.
///
/// On a network failure the in-flight batch is re-queued and the error
/// returned — the caller should retry (typically after a backoff).
pub async fn sync_step<Tr, S>(
    remote: &mut RemoteView,
    driver: &mut SyncDriver,
    transport: &Tr,
    cache: Option<&S>,
    publish: impl FnMut(&str, &Value),
    on_completed: impl FnMut(&Commit),
) -> Result<(), SyncLoopError>
where
    Tr: SyncTransport,
    S: TransactionCache + ?Sized,
{
    let mut publish = publish;
    let mut on_completed = on_completed;

    // ── Catch-up phase ──────────────────────────────────────────────
    // After a crash recovery, the recovered batches are frozen until
    // the client has applied every delta it missed while offline: the
    // catch-up reconciles (and possibly discards) them against the
    // current server state before any recovered batch may be sent.
    // Inbound-only, until a poll returns empty (caught up to now);
    // a network failure here keeps the batches frozen and is retried.
    while driver.has_recovered() {
        let since = match driver.poll_command() {
            SyncCommand::PollDeltas { since } => since,
            _ => unreachable!("poll_command always emits PollDeltas"),
        };
        let outcome = match transport.poll_deltas(since).await {
            Ok(outcome) => outcome,
            Err(msg) => return Err(SyncLoopError::Network(msg)),
        };
        let caught_up = handle_poll_outcome(
            remote,
            driver,
            cache,
            &mut publish,
            &mut on_completed,
            outcome,
        )
        .await?;
        if caught_up {
            driver.finish_catch_up();
            break;
        }
    }

    // ── Outbound ────────────────────────────────────────────────────
    // Keep stepping until no new commands: a batch persisted this round
    // becomes sendable on the next step. Sends are delivery-only (the
    // server applies asynchronously), so they are collected and
    // dispatched concurrently — one round-trip for the whole in-flight
    // window. Application results arrive with the inbound deltas.
    let mut send_cmds: Vec<(BatchKey, Vec<Transaction>)> = Vec::new();
    loop {
        let commands = driver.outbound_step();
        if commands.is_empty() {
            break;
        }
        for command in commands {
            match command {
                SyncCommand::Persist { batch } => {
                    if let Some(cache) = cache {
                        cache.persist_batch(&batch)?;
                    }
                    // Without a cache the gate passes immediately: the
                    // batch is never durable, but there is nothing to
                    // recover either.
                    driver.on_persisted(batch.id);
                }
                SyncCommand::Send { batch_key, txns } => {
                    send_cmds.push((batch_key, txns));
                }
                _ => unreachable!("outbound_step emits only Persist/Send"),
            }
        }
    }
    let futures = send_cmds.iter().map(|(batch_key, txns)| async move {
        (*batch_key, transport.send(*batch_key, txns).await)
    });
    let results = join_all(futures).await;
    // Handle every result before returning: a failure must not strand
    // the later batches (they would sit in-flight forever, neither
    // re-queued nor reported by the server). Network failures are
    // re-queued in their original order (re-queue pushes to the front,
    // so iterate in reverse); the first error is returned last.
    let mut first_error = None;
    let mut network_failures: Vec<BatchKey> = Vec::new();
    for (batch_key, result) in results {
        match result {
            Ok(response) => {
                for cmd in driver.on_sent(batch_key, response) {
                    if let Err(e) =
                        run_inbound_command(cache, driver, &mut publish, &mut on_completed, cmd)
                    {
                        if first_error.is_none() {
                            first_error = Some(e);
                        }
                    }
                }
            }
            Err(SendError::Network(msg)) => {
                network_failures.push(batch_key);
                if first_error.is_none() {
                    first_error = Some(SyncLoopError::Network(msg));
                }
            }
            Err(SendError::Rejected(msg)) => {
                // Whole-batch rejection: every transaction of the batch
                // is rejected. The batch is fully resolved (rolled back
                // and reconciled); the error is final and must not be
                // retried.
                for cmd in driver.on_batch_rejected(batch_key) {
                    if let Err(e) = run_resolved_command(
                        remote,
                        driver,
                        cache,
                        &mut publish,
                        &mut on_completed,
                        cmd,
                    ) {
                        if first_error.is_none() {
                            first_error = Some(e);
                        }
                    }
                }
                if first_error.is_none() {
                    first_error = Some(SyncLoopError::Rejected(msg));
                }
            }
        }
    }
    for batch_key in network_failures.iter().rev() {
        driver.requeue_network(*batch_key);
    }
    if let Some(err) = first_error {
        return Err(err);
    }

    // ── Inbound ─────────────────────────────────────────────────────
    let since = match driver.poll_command() {
        SyncCommand::PollDeltas { since } => since,
        _ => unreachable!("poll_command always emits PollDeltas"),
    };
    match transport.poll_deltas(since).await {
        Ok(outcome) => {
            handle_poll_outcome(
                remote,
                driver,
                cache,
                &mut publish,
                &mut on_completed,
                outcome,
            )
            .await?;
        }
        Err(msg) => return Err(SyncLoopError::Network(msg)),
    }

    Ok(())
}

/// Handle one [`PollOutcome`]: feed deltas or a snapshot through the
/// inbound pipeline. Returns whether the client is now caught up with
/// the server (an empty delta batch, or a full snapshot).
async fn handle_poll_outcome<S>(
    remote: &mut RemoteView,
    driver: &mut SyncDriver,
    cache: Option<&S>,
    publish: &mut impl FnMut(&str, &Value),
    on_completed: &mut impl FnMut(&Commit),
    outcome: PollOutcome,
) -> Result<bool, SyncLoopError>
where
    S: TransactionCache + ?Sized,
{
    match outcome {
        PollOutcome::Deltas(packets) => {
            let empty = packets.is_empty();
            for packet in packets {
                apply_packet(remote, driver, cache, publish, on_completed, packet).await?;
            }
            Ok(empty)
        }
        PollOutcome::Snapshot {
            sync_id,
            models,
            reports,
        } => {
            // The reports first (queue bookkeeping: rejected leaves,
            // applied batches, completions — the snapshot itself is
            // pure state, so the anchor advances after both).
            for packet in reports {
                apply_packet(remote, driver, cache, publish, on_completed, packet).await?;
            }
            apply_snapshot(remote, driver, cache, publish, on_completed, &models).await?;
            // The snapshot is the new authoritative base: remember the
            // model set and persist the checkpoint before advancing
            // the in-memory anchor, so a crash resumes from the
            // snapshot instead of replaying it.
            let model_ids: Vec<String> = models.iter().map(|(mid, _)| mid.clone()).collect();
            driver.remember_models(&model_ids);
            if let Some(cache) = cache {
                cache.save_anchor(sync_id)?;
                cache.save_known_models(&model_ids)?;
            }
            driver.confirm_anchor(sync_id);
            Ok(true)
        }
        PollOutcome::ResetRequired => {
            // Abandon the anchor; the next poll bootstraps with `None`.
            driver.reset_anchor();
            Ok(false)
        }
    }
}

/// Apply one delta packet.
///
/// 1. Handle the packet in memory ([`SyncDriver::on_delta`]): drop
///    rejected transactions, move the reported applied batch to
///    awaiting, complete every change the packet confirms — all before
///    reconciling, so confirmed changes are not replayed as
///    unsynced. The anchor is not advanced here.
/// 2. Apply the packet: update the remote, then reconcile or discard
///    each changed model (rewriting or removing the affected cache
///    copies), and publish the reconciled values.
/// 3. Execute the packet's commands: reconcile rejected models, drop
///    resolved batches from the cache, and persist the anchor (the
///    checkpoint).
/// 4. Only after the durable write succeeded, advance the in-memory
///    anchor ([`SyncDriver::confirm_anchor`]). A crash or store error
///    before this point replays the whole packet on the next sync —
///    idempotent — instead of skipping it with the cache and remote
///    still in the pre-packet state.
async fn apply_packet<S>(
    remote: &mut RemoteView,
    driver: &mut SyncDriver,
    cache: Option<&S>,
    publish: &mut impl FnMut(&str, &Value),
    on_completed: &mut impl FnMut(&Commit),
    packet: DeltaPacket,
) -> Result<(), SyncLoopError>
where
    S: TransactionCache + ?Sized,
{
    // 1. In-memory packet handling (anchor not advanced).
    let cmds = driver.on_delta(&packet);

    // 2. Apply the packet: remote update + reconcile/discard + publish.
    let changed = remote.apply_packet(&packet);
    for model_id in changed {
        if let Some(authoritative) = remote.value(&model_id).cloned() {
            for cmd in driver.reconcile_model(&model_id, &authoritative) {
                run_inbound_command(cache, driver, publish, on_completed, cmd)?;
            }
        } else {
            // The model was cleared or archived: drop its unsynced
            // changes (rewriting or removing the cache copies) and
            // publish null so the adapter can clear the store.
            for cmd in driver.discard_model(&model_id) {
                run_inbound_command(cache, driver, publish, on_completed, cmd)?;
            }
            publish(&model_id, &Value::Null);
        }
    }

    // 3. Execute the packet's commands: reconcile rejected models
    //    (idempotent with step 2 when the model was also changed),
    //    drop resolved batches, persist the anchor checkpoint.
    for cmd in cmds {
        match cmd {
            SyncCommand::Rejected { model_id } => {
                if let Some(authoritative) = remote.value(&model_id).cloned() {
                    for cmd in driver.reconcile_model(&model_id, &authoritative) {
                        run_inbound_command(cache, driver, publish, on_completed, cmd)?;
                    }
                }
            }
            SyncCommand::RemoveBatch { batch_id } => {
                if let Some(cache) = cache {
                    cache.remove_batch(batch_id)?;
                }
            }
            SyncCommand::SaveAnchor { sync_id } => {
                if let Some(cache) = cache {
                    cache.save_anchor(sync_id)?;
                }
            }
            SyncCommand::Completed { commit } => {
                on_completed(&commit);
            }
            _ => unreachable!("on_delta emits only Rejected/RemoveBatch/SaveAnchor/Completed"),
        }
    }

    // 4. Advance the in-memory anchor only after the durable write.
    driver.confirm_anchor(packet.sync_id);
    Ok(())
}

/// Apply a bootstrap snapshot: the authoritative model set and every
/// model's complete value.
///
/// Snapshot alignment: a model that was in the last authoritative set
/// and is absent from this snapshot was removed on the server — its
/// unsynced local changes are discarded, not replayed (replaying them
/// would resurrect a deleted model on the next send). A model never in
/// the set (an offline creation) is kept and sent normally. (LSE's
/// explicit `delete` broadcast; here the snapshot itself is the
/// signal.) The anchor is advanced by the caller.
async fn apply_snapshot<S>(
    remote: &mut RemoteView,
    driver: &mut SyncDriver,
    cache: Option<&S>,
    publish: &mut impl FnMut(&str, &Value),
    on_completed: &mut impl FnMut(&Commit),
    models: &[(String, Value)],
) -> Result<(), SyncLoopError>
where
    S: TransactionCache + ?Sized,
{
    let snapshot_models: HashSet<&str> = models.iter().map(|(mid, _)| mid.as_str()).collect();
    let known = driver.known_models();
    for model_id in driver.unsynced_models() {
        if known.contains(&model_id) && !snapshot_models.contains(model_id.as_str()) {
            for cmd in driver.discard_model(&model_id) {
                run_inbound_command(cache, driver, publish, on_completed, cmd)?;
            }
            publish(&model_id, &Value::Null);
        }
    }
    // Rebuild the remote and reconcile every model against its
    // authoritative value.
    let changed = remote.apply_snapshot(models);
    for model_id in changed {
        let authoritative = remote
            .value(&model_id)
            .cloned()
            .expect("snapshot inserted the model");
        for cmd in driver.reconcile_model(&model_id, &authoritative) {
            run_inbound_command(cache, driver, publish, on_completed, cmd)?;
        }
    }
    Ok(())
}

/// Execute one command produced during inbound handling: a cache
/// rewrite or removal (`Persist` / `RemoveBatch`), an anchor persist
/// (`SaveAnchor`), or a value publish (`ApplyValue`).
///
/// A `Persist` here rewrites the cache copy of an already-persisted
/// batch after a rebase or cancellation. Confirming it is idempotent:
/// `on_persisted` only transitions the front queued batch, and a
/// rewritten batch is already marked persisted (or in flight, which is
/// never the front).
fn run_inbound_command<S>(
    cache: Option<&S>,
    driver: &mut SyncDriver,
    publish: &mut impl FnMut(&str, &Value),
    on_completed: &mut impl FnMut(&Commit),
    cmd: SyncCommand,
) -> Result<(), SyncLoopError>
where
    S: TransactionCache + ?Sized,
{
    match cmd {
        SyncCommand::Persist { batch } => {
            if let Some(cache) = cache {
                cache.persist_batch(&batch)?;
            }
            driver.on_persisted(batch.id);
        }
        SyncCommand::RemoveBatch { batch_id } => {
            if let Some(cache) = cache {
                cache.remove_batch(batch_id)?;
            }
        }
        SyncCommand::SaveAnchor { sync_id } => {
            if let Some(cache) = cache {
                cache.save_anchor(sync_id)?;
            }
        }
        SyncCommand::ApplyValue { model_id, value } => {
            publish(&model_id, &value);
        }
        SyncCommand::Completed { commit } => {
            on_completed(&commit);
        }
        _ => {
            unreachable!("inbound commands are Persist/RemoveBatch/SaveAnchor/ApplyValue/Completed")
        }
    }
    Ok(())
}

/// Handle one command produced after a send ([`SyncDriver::on_sent`]
/// or [`SyncDriver::on_batch_rejected`]).
fn run_resolved_command<S>(
    remote: &mut RemoteView,
    driver: &mut SyncDriver,
    cache: Option<&S>,
    publish: &mut impl FnMut(&str, &Value),
    on_completed: &mut impl FnMut(&Commit),
    cmd: SyncCommand,
) -> Result<(), SyncLoopError>
where
    S: TransactionCache + ?Sized,
{
    match cmd {
        SyncCommand::RemoveBatch { batch_id } => {
            if let Some(cache) = cache {
                cache.remove_batch(batch_id)?;
            }
        }
        SyncCommand::Completed { commit } => {
            on_completed(&commit);
        }
        SyncCommand::Rejected { model_id } => {
            // Reconcile the model from the remote's authoritative value;
            // the rejected change is not replayed.
            if let Some(authoritative) = remote.value(&model_id).cloned() {
                for cmd in driver.reconcile_model(&model_id, &authoritative) {
                    run_inbound_command(cache, driver, publish, on_completed, cmd)?;
                }
            }
        }
        _ => unreachable!("send commands are RemoveBatch/Rejected/Completed"),
    }
    Ok(())
}

/// Run the synchronization loop forever.
///
/// Runtime-free: this is a busy loop with no timer. For production use,
/// prefer calling [`sync_step`] from your own scheduler (e.g. with a
/// backoff delay between attempts).
pub async fn sync_loop<Tr, S>(
    remote: &mut RemoteView,
    driver: &mut SyncDriver,
    transport: &Tr,
    cache: Option<&S>,
    mut publish: impl FnMut(&str, &Value),
    mut on_completed: impl FnMut(&Commit),
) -> Result<(), SyncLoopError>
where
    Tr: SyncTransport,
    S: TransactionCache + ?Sized,
{
    loop {
        sync_step(
            remote,
            driver,
            transport,
            cache,
            &mut publish,
            &mut on_completed,
        )
        .await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SyncId;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use serde_json::json;

    use crate::{
        BatchKey, Changed, DeltaAction, DeltaPacket, PollOutcome, RedbCache, SendResponse,
        SyncClient, Transaction,
    };

    /// A fake transport for tests: records sent batches and reports
    /// them applied on the next poll (asynchronous application).
    type SentBatch = (BatchKey, Vec<Transaction>);
    struct FakeTransport {
        fail: AtomicBool,
        reject: AtomicBool,
        sent: Arc<Mutex<Vec<SentBatch>>>,
        applied: Arc<Mutex<Vec<BatchKey>>>,
        next_sync_id: AtomicU64,
    }

    impl FakeTransport {
        fn new() -> Self {
            Self {
                fail: AtomicBool::new(false),
                reject: AtomicBool::new(false),
                sent: Arc::new(Mutex::new(Vec::new())),
                applied: Arc::new(Mutex::new(Vec::new())),
                next_sync_id: AtomicU64::new(1),
            }
        }
    }

    impl SyncTransport for FakeTransport {
        async fn send(
            &self,
            batch_key: BatchKey,
            transactions: &[Transaction],
        ) -> Result<SendResponse, SendError> {
            if self.fail.load(Ordering::Relaxed) {
                return Err(SendError::Network("network down".into()));
            }
            if self.reject.load(Ordering::Relaxed) {
                return Err(SendError::Rejected("invalid model".into()));
            }
            self.sent
                .lock()
                .unwrap()
                .push((batch_key, transactions.to_vec()));
            Ok(SendResponse { deduped_at: None })
        }

        async fn poll_deltas(&self, since: Option<SyncId>) -> Result<PollOutcome, String> {
            let mut packets = Vec::new();
            let sent = self.sent.lock().unwrap().clone();
            let mut applied = self.applied.lock().unwrap();
            for (batch_key, _) in sent {
                if !applied.contains(&batch_key) {
                    let sync_id = self.next_sync_id.fetch_add(1, Ordering::Relaxed);
                    if sync_id > since.unwrap_or(0) {
                        packets.push(DeltaPacket {
                            sync_id,
                            actions: vec![],
                            applied_batch: Some(batch_key),
                            rejected: vec![],
                        });
                        applied.push(batch_key);
                    }
                }
            }
            Ok(PollOutcome::Deltas(packets))
        }
    }

    /// A transport that returns a canned delta packet from poll.
    struct DeltaTransport(Arc<Mutex<Vec<DeltaPacket>>>);

    impl SyncTransport for DeltaTransport {
        async fn send(
            &self,
            _batch_key: BatchKey,
            _txns: &[Transaction],
        ) -> Result<SendResponse, SendError> {
            Ok(SendResponse { deduped_at: None })
        }
        async fn poll_deltas(&self, _since: Option<SyncId>) -> Result<PollOutcome, String> {
            Ok(PollOutcome::Deltas(self.0.lock().unwrap().clone()))
        }
    }

    /// A transport that proves concurrent delivery: every `send` future
    /// waits on a barrier until all N sends of the window have entered.
    ///
    /// If the outbound adapter serialized the deliveries (awaiting each
    /// send before polling the next), the first send would wait on the
    /// barrier forever and the step would deadlock — the test fails by
    /// timeout. With `join_all`, all N futures are polled in one pass,
    /// the barrier opens, and every send completes. No OS timers are
    /// involved, so the test is deterministic.
    struct GateTransport {
        barrier: Arc<tokio::sync::Barrier>,
        sent: Arc<Mutex<Vec<SentBatch>>>,
        applied: Arc<Mutex<Vec<BatchKey>>>,
        next_sync_id: AtomicU64,
    }

    impl SyncTransport for GateTransport {
        async fn send(
            &self,
            batch_key: BatchKey,
            transactions: &[Transaction],
        ) -> Result<SendResponse, SendError> {
            self.barrier.wait().await;
            self.sent
                .lock()
                .unwrap()
                .push((batch_key, transactions.to_vec()));
            Ok(SendResponse { deduped_at: None })
        }

        async fn poll_deltas(&self, since: Option<SyncId>) -> Result<PollOutcome, String> {
            let mut packets = Vec::new();
            let sent = self.sent.lock().unwrap().clone();
            let mut applied = self.applied.lock().unwrap();
            for (batch_key, _) in sent {
                if !applied.contains(&batch_key) {
                    let sync_id = self.next_sync_id.fetch_add(1, Ordering::Relaxed);
                    if sync_id > since.unwrap_or(0) {
                        packets.push(DeltaPacket {
                            sync_id,
                            actions: vec![],
                            applied_batch: Some(batch_key),
                            rejected: vec![],
                        });
                        applied.push(batch_key);
                    }
                }
            }
            Ok(PollOutcome::Deltas(packets))
        }
    }

    #[test]
    fn outbound_delivers_window_concurrently() {
        for n in [2usize, 4, 16] {
            let mut client = SyncClient::new(42);
            for _ in 0..n {
                push_one(&client);
                client.queue().lock().unwrap().collect();
            }
            let transport = GateTransport {
                barrier: Arc::new(tokio::sync::Barrier::new(n)),
                sent: Arc::new(Mutex::new(Vec::new())),
                applied: Arc::new(Mutex::new(Vec::new())),
                next_sync_id: AtomicU64::new(1),
            };
            let cache = temp_cache(&format!("muon-sync-test-gate-{n}"));
            let mut remote = RemoteView::new();

            // Drive the step on a helper thread; the main thread fails
            // by timeout if the sends were serialized (the gate never
            // opens). All assertions run inside the closure (the
            // transport and client are moved into it).
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let check = rt.block_on(sync_step(
                    &mut remote,
                    client.driver(),
                    &transport,
                    Some(&cache),
                    |_, _| {},
                    |_| {},
                ));
                let sent = transport.sent.lock().unwrap().len();
                let outcome = check.map(|()| {
                    let guard = client.queue().lock().unwrap();
                    assert!(guard.is_idle(), "all {n} batches completed");
                });
                tx.send((outcome, sent)).unwrap();
            });
            match rx.recv_timeout(std::time::Duration::from_secs(5)) {
                Ok((Ok(()), sent)) => {
                    assert_eq!(sent, n, "all {n} batches delivered");
                }
                Ok((Err(e), _)) => panic!("sync_step failed for n={n}: {e}"),
                Err(_) => {
                    panic!("outbound serialized the deliveries for n={n}: the gate never opened",)
                }
            }
        }
    }

    fn temp_cache(dir: &str) -> RedbCache {
        let path = std::env::temp_dir().join(dir);
        // Start clean: a failed run (or a legacy file-per-batch cache
        // dir) may have left a file or a directory behind.
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&path);
        RedbCache::open(path).unwrap()
    }

    fn push_one(client: &SyncClient) {
        let mut guard = client.queue().lock().unwrap();
        let id = guard.next_txn_id();
        guard.push_txn(vec![Transaction {
            id,
            client_id: 42,
            timestamp: 1,
            kind: Changed::Replace {
                before: Some(json!("Hello")),
                after: Some(json!("New")),
            },
            model_id: "test".into(),
            path: vec![crate::PathSegment::String("title".to_owned())],
        }]);
    }

    #[test]
    fn step_sends_and_completes_on_applied_report() {
        let mut client = SyncClient::new(42);
        push_one(&client);
        let transport = FakeTransport::new();
        let cache = temp_cache("muon-sync-test-ack");
        let mut remote = RemoteView::new();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let completed = Arc::new(Mutex::new(Vec::new()));
        let completed_clone = completed.clone();
        rt.block_on(sync_step(
            &mut remote,
            client.driver(),
            &transport,
            Some(&cache),
            |_, _| {},
            move |commit| completed_clone.lock().unwrap().push(commit.clone()),
        ))
        .unwrap();

        // One round: deliver, then the poll reports the batch applied;
        // the batch completes within the same step.
        assert_eq!(transport.sent.lock().unwrap().len(), 1);
        let guard = client.queue().lock().unwrap();
        assert_eq!(
            guard.awaiting_commits().len(),
            0,
            "applied report completed the change"
        );
        assert!(guard.is_idle());
        assert_eq!(guard.last_sync_id(), Some(1));
        drop(guard);
        assert_eq!(
            completed.lock().unwrap().len(),
            1,
            "completed change reported"
        );
        assert!(
            cache.load_batches().unwrap().is_empty(),
            "cache cleared on application"
        );
    }

    #[test]
    fn step_runs_without_cache() {
        let mut client = SyncClient::new(42);
        push_one(&client);
        let transport = FakeTransport::new();
        let mut remote = RemoteView::new();
        let completed = Arc::new(Mutex::new(Vec::new()));
        let completed_clone = completed.clone();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(sync_step(
            &mut remote,
            client.driver(),
            &transport,
            None::<&RedbCache>, // no crash recovery: nothing persists, nothing recovers
            |_, _| {},
            move |commit| completed_clone.lock().unwrap().push(commit.clone()),
        ))
        .unwrap();

        // The pipeline runs identically without a cache: delivery,
        // applied report, completion — only persistence is skipped.
        assert_eq!(transport.sent.lock().unwrap().len(), 1, "batch delivered");
        let guard = client.queue().lock().unwrap();
        assert!(guard.is_idle(), "change completed");
        assert_eq!(
            guard.last_sync_id(),
            Some(1),
            "anchor still advances in memory"
        );
        drop(guard);
        assert_eq!(completed.lock().unwrap().len(), 1, "completion reported");
    }

    #[test]
    fn step_requeues_on_network_failure() {
        let mut client = SyncClient::new(42);
        push_one(&client);
        let transport = FakeTransport::new();
        let cache = temp_cache("muon-sync-test-netfail");
        let mut remote = RemoteView::new();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        transport.fail.store(true, Ordering::Relaxed);
        let err = rt
            .block_on(sync_step(
                &mut remote,
                client.driver(),
                &transport,
                Some(&cache),
                |_, _| {},
                |_| {},
            ))
            .unwrap_err();
        assert!(matches!(err, SyncLoopError::Network(_)));

        assert!(transport.sent.lock().unwrap().is_empty());
        assert!(client.queue().lock().unwrap().in_flight_front().is_none());
        assert_eq!(cache.load_batches().unwrap().len(), 1, "nothing is lost");

        transport.fail.store(false, Ordering::Relaxed);
        let completed = Arc::new(Mutex::new(Vec::new()));
        let completed_clone = completed.clone();
        rt.block_on(sync_step(
            &mut remote,
            client.driver(),
            &transport,
            Some(&cache),
            |_, _| {},
            move |commit| completed_clone.lock().unwrap().push(commit.clone()),
        ))
        .unwrap();
        assert_eq!(transport.sent.lock().unwrap().len(), 1);
        assert_eq!(completed.lock().unwrap().len(), 1, "resent batch completed");
    }

    #[test]
    fn step_requeues_every_network_failure() {
        let mut client = SyncClient::new(42);
        push_one(&client);
        client.queue().lock().unwrap().collect();
        push_one(&client);
        client.queue().lock().unwrap().collect();

        let transport = FakeTransport::new();
        let cache = temp_cache("muon-sync-test-netfail-multi");
        let mut remote = RemoteView::new();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        // Both batches of the window fail. The first error must not
        // strand the second batch: every failure is re-queued in its
        // original order before the error is returned.
        transport.fail.store(true, Ordering::Relaxed);
        let err = rt
            .block_on(sync_step(
                &mut remote,
                client.driver(),
                &transport,
                Some(&cache),
                |_, _| {},
                |_| {},
            ))
            .unwrap_err();
        assert!(matches!(err, SyncLoopError::Network(_)));

        {
            let guard = client.queue().lock().unwrap();
            assert!(
                guard.in_flight_front().is_none(),
                "nothing stranded in-flight"
            );
            let queued = guard.queued_front().map(|b| b.id);
            assert_eq!(queued, Some(1), "batch 1 re-queued first, in order");
            assert_eq!(guard.unsynced_changes("test").len(), 2, "both re-queued");
        }
        assert_eq!(cache.load_batches().unwrap().len(), 2, "nothing lost");

        // A retry resends both batches.
        transport.fail.store(false, Ordering::Relaxed);
        rt.block_on(sync_step(
            &mut remote,
            client.driver(),
            &transport,
            Some(&cache),
            |_, _| {},
            |_| {},
        ))
        .unwrap();
        assert_eq!(transport.sent.lock().unwrap().len(), 2, "both resent");
    }

    #[test]
    fn step_sends_multiple_batches_in_one_round() {
        let mut client = SyncClient::new(42);
        push_one(&client);
        client.queue().lock().unwrap().collect();
        push_one(&client);
        client.queue().lock().unwrap().collect();

        let transport = FakeTransport::new();
        let cache = temp_cache("muon-sync-test-multibatch");
        let mut remote = RemoteView::new();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let completed = Arc::new(Mutex::new(Vec::new()));
        let completed_clone = completed.clone();
        rt.block_on(sync_step(
            &mut remote,
            client.driver(),
            &transport,
            Some(&cache),
            |_, _| {},
            move |commit| completed_clone.lock().unwrap().push(commit.clone()),
        ))
        .unwrap();

        // Both batches were delivered concurrently in one round, then
        // both completed from the applied reports of the same poll.
        assert_eq!(
            transport.sent.lock().unwrap().len(),
            2,
            "two batches delivered"
        );
        let guard = client.queue().lock().unwrap();
        assert!(guard.is_idle(), "both batches completed in one round");
        assert_eq!(
            guard.last_sync_id(),
            Some(2),
            "anchor covers both applied reports"
        );
        drop(guard);
        assert_eq!(
            completed.lock().unwrap().len(),
            2,
            "both completed changes reported"
        );
    }

    #[test]
    fn step_delta_rejection_removes_change() {
        let mut client = SyncClient::new(42);
        let txn_id = {
            let mut guard = client.queue().lock().unwrap();
            let id = guard.next_txn_id();
            guard.push_txn(vec![Transaction {
                id,
                client_id: 42,
                timestamp: 1,
                kind: Changed::Replace {
                    before: Some(json!("Hello")),
                    after: Some(json!("x")),
                },
                model_id: "test".into(),
                path: vec![crate::PathSegment::String("title".to_owned())],
            }]);
            id
        };
        // The server applies nothing and reports the transaction as
        // rejected in the delta stream.
        let packets = Arc::new(Mutex::new(vec![DeltaPacket {
            sync_id: 1,
            actions: vec![],
            applied_batch: None,
            rejected: vec![txn_id],
        }]));
        let transport = DeltaTransport(packets);
        let cache = temp_cache("muon-sync-test-delta-reject");
        let mut remote = RemoteView::new();
        let published = Arc::new(Mutex::new(None::<serde_json::Value>));
        let published_clone = published.clone();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(sync_step(
            &mut remote,
            client.driver(),
            &transport,
            Some(&cache),
            move |_model_id, value| {
                *published_clone.lock().unwrap() = Some(value.clone());
            },
            |_| {},
        ))
        .unwrap();

        // The rejected change is dropped (never completes); the empty
        // batch's cache copy is removed; nothing was published.
        let guard = client.queue().lock().unwrap();
        assert!(guard.is_idle(), "rejected change removed from the queue");
        assert_eq!(guard.last_sync_id(), Some(1), "anchor still advanced");
        drop(guard);
        assert!(
            cache.load_batches().unwrap().is_empty(),
            "cache copy removed"
        );
        assert!(
            published.lock().unwrap().is_none(),
            "no value published for a rejected change",
        );
    }

    #[test]
    fn step_applies_inbound_delta_and_completes_sent_change() {
        let mut client = SyncClient::new(42);
        {
            let mut guard = client.queue().lock().unwrap();
            let id = guard.next_txn_id();
            guard.push_txn(vec![Transaction {
                id,
                client_id: 42,
                timestamp: 1,
                kind: Changed::Replace {
                    before: Some(json!("Hello")),
                    after: Some(json!("Local")),
                },
                model_id: "test".into(),
                path: vec![crate::PathSegment::String("title".to_owned())],
            }]);
        }

        let packets = Arc::new(Mutex::new(vec![DeltaPacket {
            sync_id: 101,
            actions: vec![DeltaAction::Value {
                model_id: "test".into(),
                value: json!({"title": "Server Title"}),
            }],
            applied_batch: Some(BatchKey {
                client_id: 42,
                session: client.queue().lock().unwrap().session(),
                batch_id: 1,
            }),
            rejected: vec![],
        }]));
        let transport = DeltaTransport(packets);
        let cache = temp_cache("muon-sync-test-inbound");
        let mut remote = RemoteView::new();
        let published = Arc::new(Mutex::new(None::<serde_json::Value>));
        let published_clone = published.clone();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(sync_step(
            &mut remote,
            client.driver(),
            &transport,
            Some(&cache),
            move |_model_id, value| {
                *published_clone.lock().unwrap() = Some(value.clone());
            },
            |_| {},
        ))
        .unwrap();

        // The outbound step sent the local change first (accepted,
        // threshold 1); the inbound delta (sync id 101) completes it, so
        // nothing is replayed — the published value is authoritative.
        assert_eq!(
            *published.lock().unwrap(),
            Some(json!({"title": "Server Title"})),
            "delta applied; the sent change completed without replay",
        );
        let guard = client.queue().lock().unwrap();
        assert_eq!(
            guard.awaiting_commits().len(),
            0,
            "delta completed the sent change"
        );
        assert_eq!(guard.last_sync_id(), Some(101));
    }
}
