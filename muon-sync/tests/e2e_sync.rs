//! Full-pipeline end-to-end tests: local write → send → server delta →
//! reconcile → completion → undo, multi-client broadcast, and crash
//! recovery (persisted batches survive a restart and resend idempotently).
//!
//! Unlike `integration.rs` (which tests pipeline stages in isolation),
//! these tests drive the complete loop through the runtime-free
//! [`sync_step`] adapter against an in-memory fake server, with the
//! authoritative view in a [`RemoteView`] and a `publish` callback that
//! writes the store.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use muon::Observe;
use muon_store::{track, Store, Track};
use muon_sync::*;
mod common;
use common::TestServer;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

// ── Test model ──────────────────────────────────────────────────────────

#[derive(Clone, Serialize, Deserialize, Observe, Track)]
struct Issue {
    title: String,
    description: String,
    priority: i32,
}

fn make_store() -> Store<Issue> {
    Store::new(Issue {
        title: "Hello".into(),
        description: "World".into(),
        priority: 0,
    })
}

// ── In-memory server ────────────────────────────────────────────────────

/// A server seeded with the `issue` model, the shared fixture for
/// these tests.
fn server() -> TestServer {
    let s = TestServer::new();
    s.seed(
        "issue",
        json!({
            "title": "Hello",
            "description": "World",
            "priority": 0,
        }),
    );
    s
}

/// A server with a bounded delta window (retention 2), used by the
/// bootstrap tests.
fn snapshot_server() -> TestServer {
    let s = TestServer::with_retention(2);
    s.seed(
        "issue",
        json!({
            "title": "Hello",
            "description": "World",
            "priority": 0,
        }),
    );
    s
}

/// Build a client (store + client + channel + cache) sharing the server.
fn client(client_id: u64, cache_path: &str) -> (Store<Issue>, SyncClient, SyncChannel, RedbCache) {
    let store = make_store();
    let client = SyncClient::new(client_id);
    let channel = client.channel("issue");
    let cache = RedbCache::open(std::env::temp_dir().join(cache_path)).unwrap();
    (store, client, channel, cache)
}

/// The adapter's `publish` callback: deserialize the reconciled value
/// into the store's type and replace the value.
fn publisher(store: &Store<Issue>) -> impl FnMut(&str, &Value) + '_ {
    move |_model_id, value| {
        let t: Issue = serde_json::from_value(value.clone())
            .unwrap_or_else(|e| panic!("reconciled value {value} does not fit Issue: {e}"));
        store.write(|arc| *arc = Arc::new(t));
    }
}

/// A `TransactionCache` wrapper that fails once on a designated call,
/// driving the real `sync_step` through a real store failure.
struct FaultyStore {
    inner: RedbCache,
    /// When set, the next `save_anchor` call fails once and clears.
    fail_save_anchor: AtomicBool,
}

impl FaultyStore {
    fn new(inner: RedbCache) -> Self {
        Self {
            inner,
            fail_save_anchor: AtomicBool::new(false),
        }
    }
}

impl TransactionCache for FaultyStore {
    fn persist_batch(
        &self,
        batch: &CommitBatch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner.persist_batch(batch)
    }

    fn load_batches(&self) -> Result<Vec<CommitBatch>, Box<dyn std::error::Error + Send + Sync>> {
        self.inner.load_batches()
    }

    fn remove_batch(&self, id: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner.remove_batch(id)
    }

    fn save_anchor(&self, sync_id: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.fail_save_anchor.swap(false, Ordering::Relaxed) {
            return Err("injected save_anchor failure".into());
        }
        self.inner.save_anchor(sync_id)
    }

    fn load_anchor(&self) -> Result<Option<u64>, Box<dyn std::error::Error + Send + Sync>> {
        self.inner.load_anchor()
    }

    fn save_known_models(
        &self,
        models: &[String],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner.save_known_models(models)
    }

    fn load_known_models(
        &self,
    ) -> Result<Option<Vec<String>>, Box<dyn std::error::Error + Send + Sync>> {
        self.inner.load_known_models()
    }
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

// ── Tests ───────────────────────────────────────────────────────────────

/// Full cycle on one client: a local write is sent, the server's delta
/// completes the change, the value converges, and the change is
/// undoable through the application-level [`UndoStack`] (recorded at
/// enqueue time, undone through the engine's `undo(commit)`).
#[test]
fn e2e_full_cycle_send_delta_complete_undo() {
    let server = server();
    // Start clean: a legacy file-per-batch cache dir may occupy the path.
    let path = std::env::temp_dir().join("muon-e2e-single");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir_all(&path);
    let (store, mut client, channel, cache) = client(42, "muon-e2e-single");
    let mut remote = RemoteView::new();
    let mut manager = UndoStack::new(0);

    // 1. Local write (optimistic): the store updates immediately; the
    //    change is recorded for undo right away (enqueue time).
    let outcome = channel
        .sync_write(track!(&store, |s| s.title = "Client Title".into()))
        .unwrap();
    manager.record(&outcome.commit);
    assert_eq!(store.snapshot().title, "Client Title");

    // 2. One sync step: send (ack → awaiting), then poll with `None`
    //    (bootstrap) and apply the server's confirming delta, which
    //    completes the change and advances the anchor.
    block_on(sync_step(
        &mut remote,
        client.driver(),
        &server,
        Some(&cache),
        publisher(&store),
        |_| {},
    ))
    .unwrap();

    let guard = client.queue().lock().unwrap();
    assert_eq!(
        guard.awaiting_commits().len(),
        0,
        "confirming delta completed the change",
    );
    assert_eq!(
        guard.last_sync_id(),
        Some(1),
        "anchor advanced by the delta"
    );
    drop(guard);

    // 3. The store converged to the authoritative value (server echo).
    assert_eq!(store.snapshot().title, "Client Title");

    // 4. Undo through the manager: the inverse is enqueued with
    //    refreshed identities and applied optimistically; the queue
    //    syncs it on the next step.
    let inverses = manager.undo(client.driver()).expect("undoable change");
    assert_eq!(manager.redo_len(), 1, "undone change is redoable");
    for inverse in &inverses {
        inverse.apply_into(&store).unwrap();
    }
    assert_eq!(
        store.snapshot().title,
        "Hello",
        "undo restored the pre-write value",
    );
    assert_eq!(
        client
            .queue()
            .lock()
            .unwrap()
            .unsynced_changes("issue")
            .len(),
        1,
        "undo re-queued the inverse for syncing",
    );
}

/// Local-first undo: an edit is undone *before* the server confirms it.
/// The inverse is enqueued right after the change, the pair goes out in
/// one batch, and the server applies them in receive order — the change
/// and its inverse cancel on both sides.
#[test]
fn e2e_undo_before_confirm_fifo_cancels() {
    let server = server();
    let path = std::env::temp_dir().join("muon-e2e-undo-fifo");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir_all(&path);
    let (store, mut client, channel, cache) = client(42, "muon-e2e-undo-fifo");
    let mut remote = RemoteView::new();
    let mut manager = UndoStack::new(0);

    // 1. Write, then undo immediately — no sync step in between. The
    //    store goes back to its pre-write value right away (optimistic
    //    application of the fresh inverse).
    let outcome = channel
        .sync_write(track!(&store, |s| s.title = "New Title".into()))
        .unwrap();
    assert_eq!(store.snapshot().title, "New Title", "optimistic write");
    manager.record(&outcome.commit);
    let inverses = manager.undo(client.driver()).expect("undoable change");
    for inverse in &inverses {
        inverse.apply_into(&store).unwrap();
    }
    assert_eq!(
        store.snapshot().title,
        "Hello",
        "undo applied optimistically"
    );
    assert_eq!(
        client
            .queue()
            .lock()
            .unwrap()
            .unsynced_changes("issue")
            .len(),
        2,
        "change and inverse both queued, without any server round trip",
    );

    // 2. One sync step: the change and its inverse go out in one batch;
    //    the server applies them in receive order (they cancel); the
    //    confirming delta converges both sides to the pre-write value.
    block_on(sync_step(
        &mut remote,
        client.driver(),
        &server,
        Some(&cache),
        publisher(&store),
        |_| {},
    ))
    .unwrap();

    let guard = client.queue().lock().unwrap();
    assert!(guard.is_idle(), "change and inverse both completed");
    assert_eq!(guard.last_sync_id(), Some(1), "anchor advanced once");
    drop(guard);
    let issue = server.model("issue").unwrap();
    assert_eq!(
        issue["title"],
        json!("Hello"),
        "server applied change + inverse in order"
    );
    drop(issue);
    assert_eq!(store.snapshot().title, "Hello", "local converged");
}

/// Multi-client: a write by client A reaches client B through the server's
/// delta broadcast (B polls from 0 and applies A's delta).
#[test]
fn e2e_broadcast_reaches_other_client() {
    let server = server();
    // Start clean: a legacy file-per-batch cache dir may occupy the paths.
    for name in ["muon-e2e-bc-a", "muon-e2e-bc-b"] {
        let path = std::env::temp_dir().join(name);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&path);
    }
    let (store_a, mut client_a, channel_a, cache_a) = client(42, "muon-e2e-bc-a");
    let (store_b, mut client_b, _channel_b, cache_b) = client(43, "muon-e2e-bc-b");
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    // A writes and syncs (full cycle on A).
    channel_a
        .sync_write(track!(&store_a, |s| s.title = "From A".into()))
        .unwrap();
    block_on(sync_step(
        &mut remote_a,
        client_a.driver(),
        &server,
        Some(&cache_a),
        publisher(&store_a),
        |_| {},
    ))
    .unwrap();
    assert_eq!(store_a.snapshot().title, "From A");

    // B's first sync polls from 0 (bootstrap) and receives A's delta.
    block_on(sync_step(
        &mut remote_b,
        client_b.driver(),
        &server,
        Some(&cache_b),
        publisher(&store_b),
        |_| {},
    ))
    .unwrap();
    assert_eq!(
        store_b.snapshot().title,
        "From A",
        "B received A's write via the server delta",
    );
    assert_eq!(
        client_b.queue().lock().unwrap().last_sync_id(),
        Some(1),
        "B's anchor advanced after applying the delta",
    );
}

// ── Crash recovery ─────────────────────────────────────────────────────

/// Crash recovery, end-to-end (crash before send): a client persists two
/// flush-cycle batches and crashes before either is sent. A fresh client
/// with the same id recovers both batches from the offline cache,
/// resends them in FIFO order, and converges.
#[test]
fn e2e_crash_recovery_resends_persisted_batches() {
    let server = server();
    let dir = std::env::temp_dir().join("muon-e2e-crash-recover");
    // Start clean: a legacy file-per-batch cache dir may occupy the path.
    let _ = std::fs::remove_file(&dir);
    let _ = std::fs::remove_dir_all(&dir);

    // ── First session: two flush cycles, both batches durable, then
    //    a crash before anything is sent.
    {
        let (store, mut client, channel, cache) = client(42, "muon-e2e-crash-recover");
        channel
            .sync_write(track!(&store, |s| s.title = "A".into()))
            .unwrap();
        client.queue().lock().unwrap().collect(); // flush boundary → batch 1
        channel
            .sync_write(track!(&store, |s| s.priority = 3))
            .unwrap();
        client.queue().lock().unwrap().collect(); // flush boundary → batch 2

        // First outbound step: persist the front batch only (the persist
        // gate precedes the send gate).
        let cmds = client.driver().outbound_step();
        let [SyncCommand::Persist { batch }] = cmds.as_slice() else {
            panic!("first outbound step persists the front batch");
        };
        assert_eq!(batch.id, 1, "first flush cycle is batch 1");
        cache.persist_batch(batch).unwrap();
        client.driver().on_persisted(batch.id);

        // Second step: batch 1 is sendable (it was persisted). The
        // adapter would send it, but the crash happens first — the send
        // is never executed.
        let cmds = client.driver().outbound_step();
        let [SyncCommand::Send { batch_key, .. }] = cmds.as_slice() else {
            panic!("second outbound step sends batch 1");
        };
        assert_eq!(batch_key.batch_id, 1, "batch 1 is in flight");

        // Third step: batch 2 becomes the front batch and is persisted.
        // The crash happens here — batch 2 is durable, and neither batch
        // was ever sent.
        let cmds = client.driver().outbound_step();
        let [SyncCommand::Persist { batch }] = cmds.as_slice() else {
            panic!("third outbound step persists the second batch");
        };
        assert_eq!(batch.id, 2, "second flush cycle is batch 2");
        cache.persist_batch(batch).unwrap();
        client.driver().on_persisted(batch.id);
        // Crash: the block ends without executing either send or
        // resolving any response; the client, store, and channel are
        // dropped.
        assert_eq!(
            cache.load_batches().unwrap().len(),
            2,
            "both batches durable",
        );
        assert_eq!(server.applied_count(), 0, "server never saw the changes");
    }

    // ── Second session: recover from the cache and sync. The recovered
    //    batches are frozen (catch-up pending): not sendable yet, but
    //    the server has no deltas to catch up on, so the first sync
    //    releases and resends them.
    let (store2, mut client2, channel2, cache2) = client(42, "muon-e2e-crash-recover");
    client2.recover(
        cache2.load_batches().unwrap(),
        cache2.load_anchor().unwrap(),
        cache2.load_known_models().unwrap().unwrap_or_default(),
    );
    {
        let guard = client2.queue().lock().unwrap();
        assert!(
            guard.has_recovered(),
            "recovered batches are frozen until catch-up",
        );
        assert!(
            guard.queued_front().is_none(),
            "frozen batches are not in the sendable queue",
        );
    }
    let mut remote = RemoteView::new();
    block_on(sync_step(
        &mut remote,
        client2.driver(),
        &server,
        Some(&cache2),
        publisher(&store2),
        |_| {},
    ))
    .unwrap();

    // Converged: both recovered changes reached the server and echoed
    // back through the confirming deltas.
    assert_eq!(
        store2.snapshot().title,
        "A",
        "first recovered change applied"
    );
    assert_eq!(
        store2.snapshot().priority,
        3,
        "second recovered change applied",
    );
    assert_eq!(
        server.applied_count(),
        2,
        "both changes applied exactly once"
    );
    assert!(
        cache2.load_batches().unwrap().is_empty(),
        "cache cleared after confirm",
    );
    let guard = client2.queue().lock().unwrap();
    assert!(guard.is_idle(), "queue fully drained");
    drop(guard);

    // A fresh write after recovery must not reuse a recovered batch id.
    // Reusing one would overwrite the persisted copy of a
    // not-yet-confirmed batch and silently lose it on the next crash.
    channel2
        .sync_write(track!(&store2, |s| s.title = "New".into()))
        .unwrap();
    let cmds = client2.driver().outbound_step();
    let [SyncCommand::Persist { batch }] = cmds.as_slice() else {
        panic!("post-recovery write persists a fresh batch");
    };
    assert!(
        batch.id > 2,
        "new batch id {} does not reuse recovered ids",
        batch.id,
    );
}

/// Crash recovery, end-to-end (crash after send): the server applied the
/// change but the client crashed before resolving the response. A fresh
/// client resends the same transaction ids; the server deduplicates, so
/// the change applies exactly once and the cycle still completes.
#[test]
fn e2e_crash_recovery_resend_deduplicates_on_server() {
    let server = server();
    let dir = std::env::temp_dir().join("muon-e2e-crash-resend");
    // Start clean: a legacy file-per-batch cache dir may occupy the path.
    let _ = std::fs::remove_file(&dir);
    let _ = std::fs::remove_dir_all(&dir);

    // ── First session: write, persist, send (the server applies the
    //    change), then crash before the response is resolved.
    {
        let (store, mut client, channel, cache) = client(42, "muon-e2e-crash-resend");
        channel
            .sync_write(track!(&store, |s| s.title = "Client Title".into()))
            .unwrap();

        let cmds = client.driver().outbound_step();
        let [SyncCommand::Persist { batch }] = cmds.as_slice() else {
            panic!("first outbound step persists the front batch");
        };
        cache.persist_batch(batch).unwrap();
        client.driver().on_persisted(batch.id);

        let cmds = client.driver().outbound_step();
        let [SyncCommand::Send { batch_key, txns }] = cmds.as_slice() else {
            panic!("second outbound step sends the batch");
        };
        // The server accepts the batch; the response is dropped unread
        // (the crash happens before the applied report).
        let _response = block_on(server.send(*batch_key, txns)).unwrap();
        // The server applies asynchronously (here: on the next poll).
        block_on(server.poll_deltas(None)).unwrap();
        assert_eq!(server.applied_count(), 1, "server applied the change");
        assert_eq!(server.model("issue").unwrap()["title"], "Client Title");
        // Crash: the block ends; the applied report was never seen.
    }

    // ── Second session: recover, resend (deduplicated), resolve, and
    //    converge.
    let (store2, mut client2, _channel2, cache2) = client(42, "muon-e2e-crash-resend");
    client2.recover(
        cache2.load_batches().unwrap(),
        cache2.load_anchor().unwrap(),
        cache2.load_known_models().unwrap().unwrap_or_default(),
    );
    let mut remote = RemoteView::new();
    block_on(sync_step(
        &mut remote,
        client2.driver(),
        &server,
        Some(&cache2),
        publisher(&store2),
        |_| {},
    ))
    .unwrap();

    assert_eq!(
        server.applied_count(),
        1,
        "resend deduplicated: the change applied exactly once",
    );
    assert_eq!(
        store2.snapshot().title,
        "Client Title",
        "converged to the server value",
    );
    assert!(
        cache2.load_batches().unwrap().is_empty(),
        "cache cleared after confirm",
    );
    let guard = client2.queue().lock().unwrap();
    assert!(guard.is_idle(), "queue fully drained");
}

/// Window 1 (rebase-cache gap), end-to-end: a persisted, unsent batch
/// is rebased by an inbound delta (its `original` re-captured from the
/// new authoritative value), then the client crashes. Recovery must
/// load the rebased batch — the rewrite the adapter executed on
/// reconcile — not the stale pre-rebase original.
#[test]
fn e2e_rebase_rewrites_cache_before_crash() {
    let server = server();
    let dir = std::env::temp_dir().join("muon-e2e-rebase");
    let _ = std::fs::remove_file(&dir);
    let _ = std::fs::remove_dir_all(&dir);

    // ── First session: two writers. A sends batch 1 and persists batch
    //    2 (unsent); B advances the server; A's inbound poll rebases
    //    batch 2 against the new authoritative value.
    //
    // The remote outlives the session: it stands in for the client's
    // locally persisted value, which a real adapter rebuilds the
    // remote from after a crash (incremental patches need a baseline).
    let mut recovered_remote = RemoteView::new();
    {
        let (store_a, mut client_a, channel_a, cache_a) = client(42, "muon-e2e-rebase");

        // A batch 1: title = "A", persisted, sent, applied, cache
        // cleared (full outbound cycle).
        channel_a
            .sync_write(track!(&store_a, |s| s.title = "A".into()))
            .unwrap();
        client_a.queue().lock().unwrap().collect();
        let cmds = client_a.driver().outbound_step();
        let [SyncCommand::Persist { batch }] = cmds.as_slice() else {
            panic!("first outbound step persists batch 1");
        };
        cache_a.persist_batch(batch).unwrap();
        client_a.driver().on_persisted(batch.id);
        let cmds = client_a.driver().outbound_step();
        let [SyncCommand::Send { batch_key, txns }] = cmds.as_slice() else {
            panic!("second outbound step sends batch 1");
        };
        let _response = block_on(server.send(*batch_key, txns)).unwrap();
        // Inbound: the server applies batch 1 and reports it (report
        // packets), then serves the bootstrap snapshot (the model
        // state). The client resolves the batch and rebuilds its
        // remote from the snapshot.
        let PollOutcome::Snapshot {
            sync_id,
            models,
            reports,
        } = block_on(server.poll_deltas(None)).unwrap()
        else {
            panic!("expected a bootstrap snapshot");
        };
        for packet in reports {
            for cmd in client_a.driver().on_delta(&packet) {
                match cmd {
                    SyncCommand::SaveAnchor { sync_id } => cache_a.save_anchor(sync_id).unwrap(),
                    SyncCommand::RemoveBatch { batch_id } => {
                        cache_a.remove_batch(batch_id).unwrap()
                    }
                    SyncCommand::Completed { .. } => {}
                    _ => unreachable!("on_delta emits SaveAnchor/RemoveBatch/Rejected/Completed"),
                }
            }
            client_a.driver().confirm_anchor(packet.sync_id);
        }
        let changed = recovered_remote.apply_snapshot(&models);
        for model_id in changed {
            let authoritative = recovered_remote.value(&model_id).cloned().unwrap();
            for cmd in client_a.driver().reconcile_model(&model_id, &authoritative) {
                if let SyncCommand::Persist { batch } = cmd {
                    cache_a.persist_batch(&batch).unwrap();
                    client_a.driver().on_persisted(batch.id);
                }
            }
        }
        client_a.driver().confirm_anchor(sync_id);

        // B advances the server: title = "Server". B uses its own cache
        // path (two clients cannot open the same redb file).
        let (store_b, mut client_b, channel_b, cache_b) = client(43, "muon-e2e-rebase-b");
        channel_b
            .sync_write(track!(&store_b, |s| s.title = "Server".into()))
            .unwrap();
        let cmds = client_b.driver().outbound_step();
        let [SyncCommand::Persist { batch }] = cmds.as_slice() else {
            panic!("B persists its batch");
        };
        cache_b.persist_batch(batch).unwrap();
        client_b.driver().on_persisted(batch.id);
        let cmds = client_b.driver().outbound_step();
        let [SyncCommand::Send { batch_key, txns }] = cmds.as_slice() else {
            panic!("B sends its batch");
        };
        let _response = block_on(server.send(*batch_key, txns)).unwrap();
        // B completes its own cycle too (so the delta history is clean).
        let mut remote_b = RemoteView::new();
        let PollOutcome::Snapshot {
            sync_id,
            models,
            reports,
        } = block_on(server.poll_deltas(None)).unwrap()
        else {
            panic!("expected a bootstrap snapshot");
        };
        for packet in reports {
            for cmd in client_b.driver().on_delta(&packet) {
                match cmd {
                    SyncCommand::SaveAnchor { sync_id } => cache_b.save_anchor(sync_id).unwrap(),
                    SyncCommand::RemoveBatch { batch_id } => {
                        cache_b.remove_batch(batch_id).unwrap()
                    }
                    SyncCommand::Completed { .. } => {}
                    _ => unreachable!("on_delta emits SaveAnchor/RemoveBatch/Rejected/Completed"),
                }
            }
            client_b.driver().confirm_anchor(packet.sync_id);
        }
        let changed = remote_b.apply_snapshot(&models);
        for model_id in changed {
            let authoritative = remote_b.value(&model_id).cloned().unwrap();
            let _ = client_b.driver().reconcile_model(&model_id, &authoritative);
        }
        client_b.driver().confirm_anchor(sync_id);

        // A batch 2: title = "B", persisted, never sent. Its original
        // is A's optimistic value ("A").
        channel_a
            .sync_write(track!(&store_a, |s| s.title = "B".into()))
            .unwrap();
        client_a.queue().lock().unwrap().collect();
        let cmds = client_a.driver().outbound_step();
        let [SyncCommand::Persist { batch }] = cmds.as_slice() else {
            panic!("A persists batch 2");
        };
        assert_eq!(batch.id, 2, "A's second batch");
        cache_a.persist_batch(batch).unwrap();
        client_a.driver().on_persisted(batch.id);

        // A's inbound poll: the bootstrap snapshot arrives; reconciling
        // against the final authoritative value ("Server") rebases
        // batch 2, and the adapter executes the emitted rewrite. (A's
        // batch 2 was never sent, so there are no reports.)
        let PollOutcome::Snapshot {
            sync_id,
            models,
            reports,
        } = block_on(server.poll_deltas(None)).unwrap()
        else {
            panic!("expected a bootstrap snapshot");
        };
        assert!(reports.is_empty(), "A sent nothing since its last poll");
        assert_eq!(
            models[0].1["title"],
            json!("Server"),
            "B's write in the snapshot"
        );
        for packet in reports {
            for cmd in client_a.driver().on_delta(&packet) {
                match cmd {
                    SyncCommand::SaveAnchor { sync_id } => cache_a.save_anchor(sync_id).unwrap(),
                    SyncCommand::RemoveBatch { batch_id } => {
                        cache_a.remove_batch(batch_id).unwrap()
                    }
                    SyncCommand::Completed { .. } => {}
                    _ => unreachable!("on_delta emits SaveAnchor/RemoveBatch/Rejected/Completed"),
                }
            }
            client_a.driver().confirm_anchor(packet.sync_id);
        }
        let changed = recovered_remote.apply_snapshot(&models);
        for model_id in changed {
            let authoritative = recovered_remote.value(&model_id).cloned().unwrap();
            for cmd in client_a.driver().reconcile_model(&model_id, &authoritative) {
                if let SyncCommand::Persist { batch } = cmd {
                    cache_a.persist_batch(&batch).unwrap();
                    client_a.driver().on_persisted(batch.id);
                }
            }
        }
        client_a.driver().confirm_anchor(sync_id);

        // The cache copy of batch 2 now carries the rebased before.
        let batches = cache_a.load_batches().unwrap();
        let b2 = batches.iter().find(|b| b.id == 2).expect("batch 2 cached");
        let Changed::Replace { before, .. } = &b2.commits[0].txns[0].kind else {
            panic!("expected Replace");
        };
        assert_eq!(
            before,
            &Some(json!("Server")),
            "rebased before written back to the cache",
        );
        // Crash: the block ends without sending batch 2.
    }

    // ── Second session: recovery loads the rebased batch, not the
    //    stale original.
    let (store2, mut client2, _channel2, cache2) = client(42, "muon-e2e-rebase");
    client2.recover(
        cache2.load_batches().unwrap(),
        cache2.load_anchor().unwrap(),
        cache2.load_known_models().unwrap().unwrap_or_default(),
    );
    let batches = cache2.load_batches().unwrap();
    let b2 = batches
        .iter()
        .find(|b| b.id == 2)
        .expect("batch 2 recovered");
    let Changed::Replace { before, .. } = &b2.commits[0].txns[0].kind else {
        panic!("expected Replace");
    };
    assert_eq!(
        before,
        &Some(json!("Server")),
        "recovered batch carries the rebased before",
    );

    // Full sync converges: batch 2 is sent, applied, and echoed back.
    let mut remote = recovered_remote;
    block_on(sync_step(
        &mut remote,
        client2.driver(),
        &server,
        Some(&cache2),
        publisher(&store2),
        |_| {},
    ))
    .unwrap();
    assert_eq!(
        server.applied_count(),
        3,
        "both of A's batches and B's write applied"
    );
    assert_eq!(store2.snapshot().title, "B", "A's unsent intent wins");
}

/// Window 2 (anchor persistence), end-to-end: after a completed sync the
/// client's inbound anchor is durable. A restart recovers the anchor and
/// resumes polling from it — it does not depend on the server retaining
/// the full delta history from the beginning.
#[test]
fn e2e_anchor_persisted_and_resumed() {
    let server = server();
    let dir = std::env::temp_dir().join("muon-e2e-anchor");
    let _ = std::fs::remove_file(&dir);
    let _ = std::fs::remove_dir_all(&dir);

    // ── First session: a full sync cycle completes; the anchor is
    //    persisted and the cache drained.
    {
        let (store, mut client, channel, cache) = client(42, "muon-e2e-anchor");
        channel
            .sync_write(track!(&store, |s| s.title = "Anchor".into()))
            .unwrap();
        let mut remote = RemoteView::new();
        block_on(sync_step(
            &mut remote,
            client.driver(),
            &server,
            Some(&cache),
            publisher(&store),
            |_| {},
        ))
        .unwrap();
        assert_eq!(
            cache.load_anchor().unwrap(),
            Some(1),
            "anchor persisted after the sync",
        );
        assert!(
            cache.load_batches().unwrap().is_empty(),
            "cache drained after confirm",
        );
        // Crash: the queue state is gone, only the cache file remains.
    }

    // ── Second session: recovery restores the anchor; polling resumes
    //    from it.
    let (store2, mut client2, _channel2, cache2) = client(42, "muon-e2e-anchor");
    client2.recover(
        cache2.load_batches().unwrap(),
        cache2.load_anchor().unwrap(),
        cache2.load_known_models().unwrap().unwrap_or_default(),
    );
    assert_eq!(
        client2.queue().lock().unwrap().last_sync_id(),
        Some(1),
        "anchor restored into the queue",
    );
    assert!(
        matches!(
            client2.driver().poll_command(),
            SyncCommand::PollDeltas { since: Some(1) }
        ),
        "poll resumes from the restored anchor, not from 0",
    );

    let mut remote = RemoteView::new();
    block_on(sync_step(
        &mut remote,
        client2.driver(),
        &server,
        Some(&cache2),
        publisher(&store2),
        |_| {},
    ))
    .unwrap();
    // The resumed poll starts after the anchor, so the already-seen
    // delta (sync id 1) is never replayed: with no new delta the store
    // keeps its initial value instead of being re-published. (The
    // store's own durable state is the adapter's concern, not the
    // sync layer's.)
    assert_eq!(
        store2.snapshot().title,
        "Hello",
        "old delta not replayed from the resumed anchor",
    );
    assert_eq!(server.applied_count(), 1, "nothing reapplied");
    assert_eq!(
        client2.queue().lock().unwrap().last_sync_id(),
        Some(1),
        "anchor unchanged after a no-op sync",
    );
}

/// Immediate completion, end-to-end: a batch acknowledged before a crash
/// (its cache removal never ran) is recovered, re-sent, deduplicated by
/// the server (original threshold returned), and — because the restored
/// anchor already satisfies that threshold — completes immediately,
/// without waiting for a new delta.
#[test]
fn e2e_recovered_resend_completes_without_new_delta() {
    let server = server();
    let dir = std::env::temp_dir().join("muon-e2e-immediate");
    let _ = std::fs::remove_file(&dir);
    let _ = std::fs::remove_dir_all(&dir);

    // ── Session 1: a full sync cycle; the anchor is 1.
    {
        let (store, mut client, channel, cache) = client(42, "muon-e2e-immediate");
        channel
            .sync_write(track!(&store, |s| s.title = "One".into()))
            .unwrap();
        let mut remote = RemoteView::new();
        block_on(sync_step(
            &mut remote,
            client.driver(),
            &server,
            Some(&cache),
            publisher(&store),
            |_| {},
        ))
        .unwrap();
        assert_eq!(cache.load_anchor().unwrap(), Some(1));
        // Crash.
    }

    // ── Session 2: batch 2 is persisted, sent, and resolved — but the
    //    RemoveBatch is never executed (the crash happens between
    //    resolve and cache cleanup). An inbound poll completes the
    //    change and advances the anchor to 2.
    {
        let (store, mut client, channel, cache) = client(42, "muon-e2e-immediate");
        channel
            .sync_write(track!(&store, |s| s.title = "Two".into()))
            .unwrap();
        let cmds = client.driver().outbound_step();
        let [SyncCommand::Persist { batch }] = cmds.as_slice() else {
            panic!("persist batch 2");
        };
        cache.persist_batch(batch).unwrap();
        client.driver().on_persisted(batch.id);
        let cmds = client.driver().outbound_step();
        let [SyncCommand::Send { batch_key, txns }] = cmds.as_slice() else {
            panic!("send batch 2");
        };
        let response = block_on(server.send(*batch_key, txns)).unwrap();
        // A fresh batch: no commands; it stays in flight until the
        // applied report arrives (which is never executed here).
        let _cmds = client.driver().on_sent(*batch_key, response);

        // Inbound: poll from 1 — the server applies batch 2 and reports
        // it; the RemoveBatch command is collected but never executed
        // (the crash happens between application and cache cleanup).
        let mut remote = RemoteView::new();
        let PollOutcome::Deltas(packets) = block_on(server.poll_deltas(Some(1))).unwrap() else {
            panic!("expected incremental deltas");
        };
        for packet in packets {
            let cmds = client.driver().on_delta(&packet);
            for cmd in cmds {
                if let SyncCommand::SaveAnchor { sync_id } = cmd {
                    cache.save_anchor(sync_id).unwrap();
                }
                // RemoveBatch deliberately ignored: the crash window.
            }
            let changed = remote.apply_packet(&packet);
            for model_id in changed {
                if let Some(authoritative) = remote.value(&model_id).cloned() {
                    let _ = client.driver().reconcile_model(&model_id, &authoritative);
                }
            }
            client.driver().confirm_anchor(packet.sync_id);
        }
        assert_eq!(
            cache.load_anchor().unwrap(),
            Some(2),
            "anchor advanced to 2"
        );
        assert_eq!(
            cache.load_batches().unwrap().len(),
            1,
            "batch 2 still cached (RemoveBatch never ran)",
        );
        // Crash.
    }

    // ── Session 3: recover batch 2 with anchor 2. The resend is
    //    deduplicated (original threshold 2 returned), which the
    //    restored anchor already satisfies: the change completes
    //    immediately, no new delta needed.
    let (store3, mut client3, _channel3, cache3) = client(42, "muon-e2e-immediate");
    client3.recover(
        cache3.load_batches().unwrap(),
        cache3.load_anchor().unwrap(),
        cache3.load_known_models().unwrap().unwrap_or_default(),
    );
    let mut remote = RemoteView::new();
    block_on(sync_step(
        &mut remote,
        client3.driver(),
        &server,
        Some(&cache3),
        publisher(&store3),
        |_| {},
    ))
    .unwrap();
    assert_eq!(
        server.applied_count(),
        2,
        "resend deduplicated: nothing reapplied"
    );
    assert!(cache3.load_batches().unwrap().is_empty(), "cache cleared");
    let guard = client3.queue().lock().unwrap();
    assert!(guard.is_idle(), "recovered change completed immediately");
}

/// Discard consistency, end-to-end: a Clear delta discards a persisted,
/// unsent change and removes its cache copy. A crash must not recover
/// the discarded change — it must not resurrect on the server.
#[test]
fn e2e_clear_discards_persisted_batch_before_crash() {
    let server = server();
    let dir = std::env::temp_dir().join("muon-e2e-clear");
    let _ = std::fs::remove_file(&dir);
    let _ = std::fs::remove_dir_all(&dir);

    // ── First session: a change is persisted (unsent), then a Clear
    //    delta arrives. The adapter discards the change and removes
    //    its cache copy; the client crashes before anything is sent.
    {
        let (store, mut client, channel, cache) = client(42, "muon-e2e-clear");
        channel
            .sync_write(track!(&store, |s| s.title = "A".into()))
            .unwrap();
        client.queue().lock().unwrap().collect();
        let cmds = client.driver().outbound_step();
        let [SyncCommand::Persist { batch }] = cmds.as_slice() else {
            panic!("first outbound step persists the batch");
        };
        cache.persist_batch(batch).unwrap();
        client.driver().on_persisted(batch.id);
        assert_eq!(cache.load_batches().unwrap().len(), 1, "batch durable");

        // Clear delta: the remote forgets the model, and the adapter
        // discards the unsynced change (removing its cache copy).
        let mut remote = RemoteView::new();
        let packet = DeltaPacket {
            sync_id: 1,
            actions: vec![DeltaAction::Clear {
                model_id: "issue".into(),
            }],
            applied_batch: None,
            rejected: vec![],
        };
        let cmds = client.driver().on_delta(&packet);
        for cmd in cmds {
            if let SyncCommand::SaveAnchor { sync_id } = cmd {
                cache.save_anchor(sync_id).unwrap();
            }
        }
        let changed = remote.apply_packet(&packet);
        assert_eq!(changed, vec!["issue".to_owned()], "clear changed the model");
        for model_id in changed {
            // The adapter's discard branch (mirrors sync_step).
            for cmd in client.driver().discard_model(&model_id) {
                if let SyncCommand::RemoveBatch { batch_id } = cmd {
                    cache.remove_batch(batch_id).unwrap();
                }
            }
        }
        client.driver().confirm_anchor(packet.sync_id);
        assert!(
            cache.load_batches().unwrap().is_empty(),
            "discarded change's cache copy removed",
        );
        // Crash: the block ends; nothing was ever sent.
    }

    // ── Second session: nothing to recover, nothing resurrects.
    let (store2, mut client2, _channel2, cache2) = client(42, "muon-e2e-clear");
    client2.recover(
        cache2.load_batches().unwrap(),
        cache2.load_anchor().unwrap(),
        cache2.load_known_models().unwrap().unwrap_or_default(),
    );
    assert!(
        client2.queue().lock().unwrap().is_idle(),
        "no queued changes after recovery",
    );
    let mut remote = RemoteView::new();
    block_on(sync_step(
        &mut remote,
        client2.driver(),
        &server,
        Some(&cache2),
        publisher(&store2),
        |_| {},
    ))
    .unwrap();
    assert_eq!(
        server.applied_count(),
        0,
        "discarded change never reached the server",
    );
    assert_eq!(store2.snapshot().title, "Hello", "store unchanged");
}

/// Bootstrap, end-to-end: a fresh client (no local state) polls with
/// `since: None` and receives the server's full snapshot — even though
/// the server has discarded part of its delta history. The store is
/// bootstrapped, the anchor is the snapshot's sync id, and nothing is
/// re-applied.
#[test]
fn e2e_bootstrap_snapshot_on_first_sync() {
    let server = snapshot_server();
    let dir = std::env::temp_dir().join("muon-e2e-bootstrap");
    let _ = std::fs::remove_file(&dir);
    let _ = std::fs::remove_dir_all(&dir);

    // Seed three historical changes. The server keeps only the most
    // recent DELTA_WINDOW packets, so the full history is not replayable.
    {
        let (store, mut client, channel, cache) = client(42, "muon-e2e-bootstrap");
        // One continuously running client: the remote outlives every
        // step, so incremental merge patches always merge onto a
        // baseline (a fresh remote + non-zero anchor would have none).
        let mut remote = RemoteView::new();
        for title in ["A", "B", "C"] {
            channel
                .sync_write(track!(&store, |s| s.title = title.into()))
                .unwrap();
            block_on(sync_step(
                &mut remote,
                client.driver(),
                &server,
                Some(&cache),
                publisher(&store),
                |_| {},
            ))
            .unwrap();
        }
        assert_eq!(server.applied_count(), 3, "three changes applied");
    }

    // A fresh client with no local state: `poll_deltas(None)` returns the
    // snapshot, which bootstraps the store and the anchor.
    let (store, mut client, _channel, cache) = client(44, "muon-e2e-bootstrap");
    let mut remote = RemoteView::new();
    block_on(sync_step(
        &mut remote,
        client.driver(),
        &server,
        Some(&cache),
        publisher(&store),
        |_| {},
    ))
    .unwrap();
    assert_eq!(
        store.snapshot().title,
        "C",
        "snapshot bootstrapped the store to the current server state",
    );
    assert_eq!(
        cache.load_anchor().unwrap(),
        Some(3),
        "anchor is the snapshot's sync id",
    );
    assert_eq!(server.applied_count(), 3, "the snapshot applied nothing");
}

/// Bootstrap, end-to-end: local data loss (cleared cache / new device)
/// leaves a client without an anchor. Recovery finds nothing, the next
/// sync polls from 0, and the snapshot rebuilds the store — including
/// changes made by other clients while this one was away.
#[test]
fn e2e_bootstrap_rebuilds_after_local_data_loss() {
    let server = snapshot_server();
    let dir = std::env::temp_dir().join("muon-e2e-bootstrap-loss");
    let _ = std::fs::remove_file(&dir);
    let _ = std::fs::remove_dir_all(&dir);

    // Session 1: client 42 writes and syncs (anchor 1), then its local
    // cache is wiped.
    {
        let (store, mut client, channel, cache) = client(42, "muon-e2e-bootstrap-loss");
        channel
            .sync_write(track!(&store, |s| s.title = "A".into()))
            .unwrap();
        let mut remote = RemoteView::new();
        block_on(sync_step(
            &mut remote,
            client.driver(),
            &server,
            Some(&cache),
            publisher(&store),
            |_| {},
        ))
        .unwrap();
        assert_eq!(cache.load_anchor().unwrap(), Some(1));
        // Local data loss: the cache file is gone.
    }
    let _ = std::fs::remove_file(&dir);
    let _ = std::fs::remove_dir_all(&dir);

    // Another client advances the server while 42 is away. Its cache is
    // independent (the lost file's path must stay gone).
    {
        let (store, mut client, channel, cache) = client(43, "muon-e2e-bootstrap-loss-b");
        channel
            .sync_write(track!(&store, |s| s.title = "B".into()))
            .unwrap();
        let mut remote = RemoteView::new();
        block_on(sync_step(
            &mut remote,
            client.driver(),
            &server,
            Some(&cache),
            publisher(&store),
            |_| {},
        ))
        .unwrap();
    }

    // Session 2: the same client id, no local state. The snapshot
    // rebuilds the store and re-establishes the anchor.
    let (store2, mut client2, _channel2, cache2) = client(42, "muon-e2e-bootstrap-loss");
    client2.recover(
        cache2.load_batches().unwrap(),
        cache2.load_anchor().unwrap(),
        cache2.load_known_models().unwrap().unwrap_or_default(),
    );
    assert_eq!(
        client2.queue().lock().unwrap().last_sync_id(),
        None,
        "anchor lost with the local data",
    );
    let mut remote = RemoteView::new();
    block_on(sync_step(
        &mut remote,
        client2.driver(),
        &server,
        Some(&cache2),
        publisher(&store2),
        |_| {},
    ))
    .unwrap();
    assert_eq!(
        store2.snapshot().title,
        "B",
        "snapshot rebuilt the store including the other client's write",
    );
    assert_eq!(
        cache2.load_anchor().unwrap(),
        Some(2),
        "anchor re-established from the snapshot",
    );
    assert_eq!(server.applied_count(), 2, "nothing reapplied");
}

/// Anchor-as-checkpoint, end-to-end: the anchor is persisted only after
/// a packet is fully applied (remote update and cache rewrites
/// included). A crash in between — anchor not yet persisted — must
/// replay the packet on recovery instead of skipping it, and the replay
/// is idempotent.
#[test]
fn e2e_crash_before_anchor_persist_replays_packet() {
    let server = server();
    let dir = std::env::temp_dir().join("muon-e2e-anchor-checkpoint");
    let _ = std::fs::remove_file(&dir);
    let _ = std::fs::remove_dir_all(&dir);

    // ── Session 1: a change is persisted, sent (server applied, delta
    //    1 exists), and resolved — but the client crashes while
    //    applying the delta, before the anchor persist (the checkpoint)
    //    runs. Neither the anchor nor the cache removal happened.
    {
        let (store, mut client, channel, cache) = client(42, "muon-e2e-anchor-checkpoint");
        channel
            .sync_write(track!(&store, |s| s.title = "A".into()))
            .unwrap();
        let cmds = client.driver().outbound_step();
        let [SyncCommand::Persist { batch }] = cmds.as_slice() else {
            panic!("persist");
        };
        cache.persist_batch(batch).unwrap();
        client.driver().on_persisted(batch.id);
        let cmds = client.driver().outbound_step();
        let [SyncCommand::Send { batch_key, txns }] = cmds.as_slice() else {
            panic!("send");
        };
        let response = block_on(server.send(*batch_key, txns)).unwrap();
        // Fresh batch: no commands; it stays in flight.
        let _cmds = client.driver().on_sent(*batch_key, response);

        // The server serves the bootstrap snapshot: the batch report
        // (whose SaveAnchor never runs — the crash window) plus the
        // model state (also unread — the crash precedes the rebuild).
        let PollOutcome::Snapshot {
            sync_id,
            models,
            reports,
        } = block_on(server.poll_deltas(None)).unwrap()
        else {
            panic!("expected a bootstrap snapshot");
        };
        let remote = RemoteView::new();
        for packet in reports {
            let anchor_cmds = client.driver().on_delta(&packet);
            // Crash: anchor_cmds (SaveAnchor) is never executed, and
            // the in-memory anchor never advances (no confirm_anchor).
            let _ = anchor_cmds;
        }
        let _ = (sync_id, models, remote);
        assert!(
            cache.load_anchor().unwrap().is_none(),
            "anchor not persisted (the checkpoint never ran)",
        );
        assert_eq!(cache.load_batches().unwrap().len(), 1, "batch still cached");
        // Crash: the block ends.
    }

    // ── Session 2: no anchor → polling resumes from 0, the packet is
    //    replayed (idempotent: the resend deduplicates, the delta
    //    re-applies the same value), and everything converges.
    let (store2, mut client2, _channel2, cache2) = client(42, "muon-e2e-anchor-checkpoint");
    client2.recover(
        cache2.load_batches().unwrap(),
        cache2.load_anchor().unwrap(),
        cache2.load_known_models().unwrap().unwrap_or_default(),
    );
    assert_eq!(
        client2.queue().lock().unwrap().last_sync_id(),
        None,
        "anchor lost with the crash",
    );
    let mut remote = RemoteView::new();
    block_on(sync_step(
        &mut remote,
        client2.driver(),
        &server,
        Some(&cache2),
        publisher(&store2),
        |_| {},
    ))
    .unwrap();
    assert_eq!(server.applied_count(), 1, "resend deduplicated");
    assert_eq!(store2.snapshot().title, "A", "converged via replay");
    assert_eq!(
        cache2.load_anchor().unwrap(),
        Some(1),
        "anchor established after the replay",
    );
    assert!(cache2.load_batches().unwrap().is_empty(), "cache drained");
    let guard = client2.queue().lock().unwrap();
    assert!(guard.is_idle(), "queue fully drained");
}

/// Dedup contract, end-to-end: a re-sent transaction receives its
/// *original* completion threshold — not the server's current sync id —
/// so a client whose restored anchor already covers the threshold
/// completes immediately, without waiting for a delta that may never be
/// broadcast again.
#[test]
fn e2e_resend_returns_original_threshold() {
    let server = server();
    let dir = std::env::temp_dir().join("muon-e2e-dedup-threshold");
    let _ = std::fs::remove_file(&dir);
    let _ = std::fs::remove_dir_all(&dir);

    // ── Session 1: client 42's batch is applied by the server
    //    (threshold 1), the delta is applied and the anchor persisted
    //    (1), but the cache removal never runs — the batch stays
    //    cached.
    //
    // The remote outlives the session: it stands in for the client's
    // locally persisted value, which a real adapter rebuilds the
    // remote from after a crash. Incremental merge patches merge onto
    // this baseline; without it a restored anchor has nothing to
    // merge onto (the server contract requires a baseline).
    let mut recovered_remote = RemoteView::new();
    {
        let (store, mut client, channel, cache) = client(42, "muon-e2e-dedup-threshold");
        channel
            .sync_write(track!(&store, |s| s.title = "A".into()))
            .unwrap();
        let cmds = client.driver().outbound_step();
        let [SyncCommand::Persist { batch }] = cmds.as_slice() else {
            panic!("persist");
        };
        cache.persist_batch(batch).unwrap();
        client.driver().on_persisted(batch.id);
        let cmds = client.driver().outbound_step();
        let [SyncCommand::Send { batch_key, txns }] = cmds.as_slice() else {
            panic!("send");
        };
        let response = block_on(server.send(*batch_key, txns)).unwrap();
        // Fresh batch: stays in flight; RemoveBatch never runs.
        let _cmds = client.driver().on_sent(*batch_key, response);

        // Inbound: apply the batch report, persist the anchor (the
        // checkpoint), then rebuild the remote from the snapshot.
        let PollOutcome::Snapshot {
            sync_id,
            models,
            reports,
        } = block_on(server.poll_deltas(None)).unwrap()
        else {
            panic!("expected a bootstrap snapshot");
        };
        for packet in reports {
            let anchor_cmds = client.driver().on_delta(&packet);
            for cmd in anchor_cmds {
                if let SyncCommand::SaveAnchor { sync_id } = cmd {
                    cache.save_anchor(sync_id).unwrap();
                }
            }
            client.driver().confirm_anchor(packet.sync_id);
        }
        let changed = recovered_remote.apply_snapshot(&models);
        for model_id in changed {
            let authoritative = recovered_remote.value(&model_id).cloned().unwrap();
            let _ = client.driver().reconcile_model(&model_id, &authoritative);
        }
        client.driver().confirm_anchor(sync_id);
        assert_eq!(cache.load_anchor().unwrap(), Some(1), "anchor persisted");
        assert_eq!(cache.load_batches().unwrap().len(), 1, "batch still cached");
        // Crash.
    }

    // ── Another client advances the server to sync id 2. ──
    {
        let (store, mut client, channel, cache) = client(43, "muon-e2e-dedup-threshold-b");
        channel
            .sync_write(track!(&store, |s| s.title = "B".into()))
            .unwrap();
        let mut remote = RemoteView::new();
        block_on(sync_step(
            &mut remote,
            client.driver(),
            &server,
            Some(&cache),
            publisher(&store),
            |_| {},
        ))
        .unwrap();
        assert_eq!(server.applied_count(), 2);
    }

    // ── Session 2: client 42 recovers with anchor 1. Its cached batch
    //    is re-sent; the server deduplicates and returns the original
    //    threshold (1) — not the current sync id (2) — which the
    //    restored anchor already satisfies, so the change completes
    //    immediately.
    let (store2, mut client2, _channel2, cache2) = client(42, "muon-e2e-dedup-threshold");
    client2.recover(
        cache2.load_batches().unwrap(),
        cache2.load_anchor().unwrap(),
        cache2.load_known_models().unwrap().unwrap_or_default(),
    );

    // Manually drive the resend to observe the completion timing. The
    // catch-up phase has nothing to apply here (the server has no delta
    // for this client after its anchor), so release the frozen batch
    // the way the catch-up phase would.
    client2.driver().finish_catch_up();
    let cmds = client2.driver().outbound_step();
    let [SyncCommand::Send { batch_key, txns }] = cmds.as_slice() else {
        panic!("recovered batch is sendable after catch-up");
    };
    let response = block_on(server.send(*batch_key, txns)).unwrap();
    assert_eq!(
        response.deduped_at,
        Some(1),
        "dedup returns the original threshold (1), not the current sync id (2)",
    );
    let cmds = client2.driver().on_sent(*batch_key, response);
    for cmd in cmds {
        if let SyncCommand::RemoveBatch { batch_id } = cmd {
            cache2.remove_batch(batch_id).unwrap();
        }
    }
    let guard = client2.queue().lock().unwrap();
    assert!(
        guard.awaiting_commits().is_empty(),
        "completed immediately: original threshold already covered by the anchor",
    );
    assert!(guard.is_idle(), "queue fully drained");
    drop(guard);

    // A full sync finishes the cleanup (inbound poll from the anchor).
    // The remote carries the last-synced baseline (above), so the
    // incremental patch merges onto it.
    let mut remote = recovered_remote;
    block_on(sync_step(
        &mut remote,
        client2.driver(),
        &server,
        Some(&cache2),
        publisher(&store2),
        |_| {},
    ))
    .unwrap();
    assert_eq!(server.applied_count(), 2, "nothing reapplied");
    assert!(cache2.load_batches().unwrap().is_empty(), "cache drained");
    assert_eq!(
        store2.snapshot().title,
        "B",
        "later writes from other clients still apply",
    );
}

/// Dedup contract, end-to-end: a re-sent batch whose transactions were
/// all applied before is answered in `send` with the **original**
/// application sync id (`deduped_at`) — never the server's current sync
/// id — and never re-applied.
#[test]
fn e2e_whole_batch_dedup_returns_original_threshold() {
    let server = server();
    let txn = |seq: u64| Transaction {
        id: TxnId {
            incarnation: 7,
            seq,
        },
        client_id: 42,
        timestamp: 1,
        kind: Changed::Replace {
            before: Some(json!("Hello")),
            after: Some(json!("x")),
        },
        model_id: "issue".into(),
        path: vec![muon_sync::PathSegment::String("title".to_owned())],
    };

    // T1 and T2 are applied first (thresholds 1 and 2). The poll
    // applies the pending batches (asynchronous application).
    let bk = |id: u64| BatchKey {
        client_id: 42,
        session: 7,
        batch_id: id,
    };
    let r = block_on(server.send(bk(1), &[txn(1)])).unwrap();
    assert_eq!(r.deduped_at, None, "fresh batch: no dedup info");
    let r = block_on(server.send(bk(2), &[txn(2)])).unwrap();
    assert_eq!(r.deduped_at, None, "fresh batch: no dedup info");
    block_on(server.poll_deltas(None)).unwrap();
    assert_eq!(server.applied_count(), 2, "both applied");

    // Full resend: every transaction is seen. The response carries the
    // original application sync id (2), not the current one.
    let r = block_on(server.send(bk(1), &[txn(1), txn(2)])).unwrap();
    assert_eq!(
        r.deduped_at,
        Some(2),
        "dedup returns the original threshold (2)",
    );
    assert_eq!(server.applied_count(), 2, "nothing reapplied");
}

/// Window: anchor-persist failure. A `save_anchor` failure mid-inbound
/// returns [`SyncLoopError::Store`] while the in-memory anchor stays at
/// its old value; a retry on the same driver replays the packet
/// (idempotent) and converges — the packet is never skipped.
#[test]
fn e2e_anchor_persist_failure_retries_without_skipping() {
    let server = server();
    let dir = std::env::temp_dir().join("muon-e2e-anchor-fail");
    let _ = std::fs::remove_file(&dir);
    let _ = std::fs::remove_dir_all(&dir);

    // ── Session 1: a change is persisted, sent (server applied, delta
    //    1 exists), and resolved — but the cache removal never runs.
    {
        let (store, mut client, channel, cache) = client(42, "muon-e2e-anchor-fail");
        channel
            .sync_write(track!(&store, |s| s.title = "A".into()))
            .unwrap();
        let cmds = client.driver().outbound_step();
        let [SyncCommand::Persist { batch }] = cmds.as_slice() else {
            panic!("persist");
        };
        cache.persist_batch(batch).unwrap();
        client.driver().on_persisted(batch.id);
        let cmds = client.driver().outbound_step();
        let [SyncCommand::Send { batch_key, txns }] = cmds.as_slice() else {
            panic!("send");
        };
        let response = block_on(server.send(*batch_key, txns)).unwrap();
        let _cmds = client.driver().on_sent(*batch_key, response);
        // Crash: the block ends without removing the batch from the
        // cache and without ever polling.
    }

    // ── Session 2: recover, then let the first inbound checkpoint fail.
    let (store2, mut client2, _channel2, cache2) = client(42, "muon-e2e-anchor-fail");
    client2.recover(
        cache2.load_batches().unwrap(),
        cache2.load_anchor().unwrap(),
        cache2.load_known_models().unwrap().unwrap_or_default(),
    );
    let faulty = FaultyStore::new(cache2);
    faulty.fail_save_anchor.store(true, Ordering::Relaxed);
    let mut remote = RemoteView::new();

    // First sync: catch-up applies delta 1 and rebases the recovered
    // batch, then the anchor persist fails. The in-memory anchor must
    // not have advanced past the failed checkpoint.
    let err = block_on(sync_step(
        &mut remote,
        client2.driver(),
        &server,
        Some(&faulty),
        publisher(&store2),
        |_| {},
    ))
    .unwrap_err();
    assert!(
        matches!(err, SyncLoopError::Store(_)),
        "store failure surfaces as SyncLoopError::Store",
    );
    assert_eq!(
        client2.queue().lock().unwrap().last_sync_id(),
        None,
        "memory anchor did not advance past the failed checkpoint",
    );

    // Second sync on the same driver: the packet is replayed
    // (idempotent), the anchor persists, and everything converges.
    block_on(sync_step(
        &mut remote,
        client2.driver(),
        &server,
        Some(&faulty),
        publisher(&store2),
        |_| {},
    ))
    .unwrap();
    assert_eq!(
        faulty.load_anchor().unwrap(),
        Some(1),
        "anchor persisted on retry"
    );
    assert_eq!(server.applied_count(), 1, "nothing reapplied");
    assert!(faulty.load_batches().unwrap().is_empty(), "cache drained");
    assert_eq!(store2.snapshot().title, "A", "converged");
    let guard = client2.queue().lock().unwrap();
    assert!(guard.is_idle(), "queue fully drained");
}

/// Window: clear-recovery. A change is persisted while the client
/// knows the model set; the model is cleared on the server while the
/// client is down; recovery discards the batch during catch-up —
/// before any send — so the server never applies it.
#[test]
fn e2e_catch_up_discards_recovered_batch() {
    let server = server();
    let dir = std::env::temp_dir().join("muon-e2e-catchup-clear");
    let _ = std::fs::remove_file(&dir);
    let _ = std::fs::remove_dir_all(&dir);

    // ── Session 1: create the model and complete a sync (the known
    //    model set is established), then persist a change that is
    //    never sent; the client crashes.
    {
        let (store, mut client, channel, cache) = client(42, "muon-e2e-catchup-clear");
        channel
            .sync_write(track!(&store, |s| s.title = "Seed".into()))
            .unwrap();
        let mut remote = RemoteView::new();
        block_on(sync_step(
            &mut remote,
            client.driver(),
            &server,
            Some(&cache),
            publisher(&store),
            |_| {},
        ))
        .unwrap();
        assert_eq!(server.applied_count(), 1, "seed applied");
        assert_eq!(
            cache.load_known_models().unwrap(),
            Some(vec!["issue".to_string()]),
            "the known model set was persisted",
        );
        channel
            .sync_write(track!(&store, |s| s.title = "A".into()))
            .unwrap();
        client.queue().lock().unwrap().collect();
        let cmds = client.driver().outbound_step();
        let [SyncCommand::Persist { batch }] = cmds.as_slice() else {
            panic!("persist");
        };
        cache.persist_batch(batch).unwrap();
        client.driver().on_persisted(batch.id);
        assert_eq!(cache.load_batches().unwrap().len(), 1, "batch durable");
        // Crash: never sent, never resolved.
    }

    // ── The model is cleared on the server while the client is down. ──
    server.clear_model("issue");

    // ── Session 2: recovery. The catch-up phase applies the Clear
    //    delta and discards the recovered batch before it can be sent.
    let (_store2, mut client2, _channel2, cache2) = client(42, "muon-e2e-catchup-clear");
    client2.recover(
        cache2.load_batches().unwrap(),
        cache2.load_anchor().unwrap(),
        cache2.load_known_models().unwrap().unwrap_or_default(),
    );
    let mut remote = RemoteView::new();
    let published = Arc::new(Mutex::new(None::<Value>));
    let published_clone = Arc::clone(&published);
    block_on(sync_step(
        &mut remote,
        client2.driver(),
        &server,
        Some(&cache2),
        move |_model_id, value| {
            *published_clone.lock().unwrap() = Some(value.clone());
        },
        |_| {},
    ))
    .unwrap();
    assert_eq!(
        server.applied_count(),
        1,
        "only the seed reached the server; the discarded change never did",
    );
    assert!(
        cache2.load_batches().unwrap().is_empty(),
        "the discarded batch's cache copy was removed",
    );
    assert_eq!(
        *published.lock().unwrap(),
        Some(Value::Null),
        "the clear published null",
    );
    let guard = client2.queue().lock().unwrap();
    assert!(guard.is_idle(), "queue fully drained");
}

/// Window: rebase-recovery. The catch-up phase rebases a recovered
/// batch and rewrites its cache copy before it becomes sendable — the
/// change that is finally sent (and later undone) carries the
/// server-based `original`, not the stale pre-rebase one.
#[test]
fn e2e_catch_up_rebases_recovered_batch() {
    let server = server();
    let dir = std::env::temp_dir().join("muon-e2e-catchup-rebase");
    let _ = std::fs::remove_file(&dir);
    let _ = std::fs::remove_dir_all(&dir);

    // ── Session 1: a change is persisted but never sent; the client
    //    crashes. Its `original` is the stale local value ("Hello").
    {
        let (store, mut client, channel, cache) = client(42, "muon-e2e-catchup-rebase");
        channel
            .sync_write(track!(&store, |s| s.title = "A".into()))
            .unwrap();
        client.queue().lock().unwrap().collect();
        let cmds = client.driver().outbound_step();
        let [SyncCommand::Persist { batch }] = cmds.as_slice() else {
            panic!("persist");
        };
        cache.persist_batch(batch).unwrap();
        client.driver().on_persisted(batch.id);
        // Crash.
    }

    // ── Another client advances the server while 42 is down. ──
    {
        let (store, mut client, channel, cache) = client(43, "muon-e2e-catchup-rebase-b");
        channel
            .sync_write(track!(&store, |s| s.title = "Server".into()))
            .unwrap();
        let mut remote = RemoteView::new();
        block_on(sync_step(
            &mut remote,
            client.driver(),
            &server,
            Some(&cache),
            publisher(&store),
            |_| {},
        ))
        .unwrap();
        assert_eq!(server.applied_count(), 1);
    }

    // ── Session 2: recovery. The first sync's catch-up applies the
    //    delta (rebasing the recovered batch's `original` to the
    //    server value and rewriting its cache copy), then the anchor
    //    persist fails — freezing the checkpoint before the batch is
    //    sent. The rewrite is already durable.
    let (store2, mut client2, _channel2, cache2) = client(42, "muon-e2e-catchup-rebase");
    client2.recover(
        cache2.load_batches().unwrap(),
        cache2.load_anchor().unwrap(),
        cache2.load_known_models().unwrap().unwrap_or_default(),
    );
    let faulty = FaultyStore::new(cache2);
    faulty.fail_save_anchor.store(true, Ordering::Relaxed);
    let mut remote = RemoteView::new();
    let err = block_on(sync_step(
        &mut remote,
        client2.driver(),
        &server,
        Some(&faulty),
        publisher(&store2),
        |_| {},
    ))
    .unwrap_err();
    assert!(matches!(err, SyncLoopError::Store(_)));
    assert_eq!(
        client2.queue().lock().unwrap().last_sync_id(),
        None,
        "checkpoint not advanced",
    );

    // The cache copy of the recovered batch already carries the
    // rebased before (rewritten before the anchor persist).
    let batches = faulty.load_batches().unwrap();
    let b = batches.iter().find(|b| b.id == 1).expect("recovered batch");
    let Changed::Replace { before, .. } = &b.commits[0].txns[0].kind else {
        panic!("expected Replace");
    };
    assert_eq!(
        before,
        &Some(json!("Server")),
        "catch-up rebase rewrote the cache copy",
    );

    // Second sync: replay converges; the batch is sent with the
    // rebased original and completes.
    block_on(sync_step(
        &mut remote,
        client2.driver(),
        &server,
        Some(&faulty),
        publisher(&store2),
        |_| {},
    ))
    .unwrap();
    assert_eq!(server.applied_count(), 2, "the recovered change applied");
    assert_eq!(store2.snapshot().title, "A", "converged");
    assert_eq!(
        faulty.load_anchor().unwrap(),
        Some(2),
        "anchor advanced through the sent batch's delta",
    );
    let guard = client2.queue().lock().unwrap();
    assert!(guard.is_idle(), "queue fully drained");
}

/// Rejection granularity, end-to-end: a user writes two fields in one
/// change; the server rejects one leaf (a per-field permission check).
/// The rejected leaf is removed from the change and never replayed;
/// the accepted leaf applies and completes normally; the model
/// converges to the authoritative value; undo restores the accepted
/// leaf and its inverse over the denied one is a no-op.
#[test]
fn e2e_partial_rejection_removes_rejected_leaf_only() {
    let server = server();
    // The description field is read-only for this client: writes to it
    // are rejected at application time.
    server.deny_field("description");
    let path = std::env::temp_dir().join("muon-e2e-partial-reject");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir_all(&path);
    let (store, mut client, channel, cache) = client(42, "muon-e2e-partial-reject");
    let mut remote = RemoteView::new();
    // Enqueue-time strategy: the adapter records the change when it is
    // written. A completion-time record would be useless — by then the
    // rebase has re-captured every `original` to the applied value, so
    // an inverse built from it would be a no-op. The stack holds an
    // independent copy that the rebase never rewrites.
    let manager = Arc::new(Mutex::new(UndoStack::new(0)));
    let manager_clone = manager.clone();

    // One change with two leaves: title (allowed) + description (denied).
    let outcome = channel
        .sync_write(track!(&store, |s| {
            s.title = "Allowed".into();
            s.description = "Denied".into();
        }))
        .unwrap();
    manager_clone.lock().unwrap().record(&outcome.commit);
    assert_eq!(store.snapshot().title, "Allowed", "optimistic write");

    block_on(sync_step(
        &mut remote,
        client.driver(),
        &server,
        Some(&cache),
        publisher(&store),
        |_| {},
    ))
    .unwrap();

    // The accepted leaf applied; the denied leaf did not — the store
    // converged to the server's authoritative value.
    assert_eq!(store.snapshot().title, "Allowed", "accepted leaf converged");
    assert_eq!(
        store.snapshot().description,
        "World",
        "denied leaf never applied"
    );
    let issue = server.model("issue").unwrap();
    assert_eq!(
        issue["title"],
        json!("Allowed"),
        "server applied the allowed leaf"
    );
    assert_eq!(
        issue["description"],
        json!("World"),
        "server state unchanged for the denied field"
    );
    drop(issue);

    // The shrunk change completed and is undoable. Undo applies the
    // full inverse: the accepted leaf is restored, and the denied
    // leaf's inverse is a no-op (it never applied, so its original
    // equals the current value).
    let guard = client.queue().lock().unwrap();
    assert!(guard.is_idle(), "shrunk change completed");
    drop(guard);
    assert!(cache.load_batches().unwrap().is_empty(), "cache drained");
    let mut manager = manager.lock().unwrap();
    assert_eq!(manager.undo_len(), 1, "change recorded at enqueue time");
    let inverses = manager.undo(client.driver()).expect("undoable change");
    assert_eq!(inverses.len(), 2, "the full change, both leaves");
    for inverse in &inverses {
        inverse.apply_into(&store).unwrap();
    }
    assert_eq!(
        store.snapshot().title,
        "Hello",
        "undo restored the accepted leaf"
    );
    assert_eq!(
        store.snapshot().description,
        "World",
        "denied leaf's inverse is a no-op"
    );
}

/// A batch with two changes: the server rejects one change's leaf (a
/// per-field permission check) and accepts the other. The rejected
/// change is dropped as a unit (its only leaf was denied); the accepted
/// change completes normally.
#[test]
fn e2e_batch_rejection_keeps_accepted_changes() {
    let server = server();
    server.deny_field("description");
    let path = std::env::temp_dir().join("muon-e2e-batch-reject");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir_all(&path);
    let (store, mut client, channel, cache) = client(42, "muon-e2e-batch-reject");
    let mut remote = RemoteView::new();

    // Two changes in one flush cycle (one batch): title (allowed) and
    // description (denied).
    channel
        .sync_write(track!(&store, |s| s.title = "Allowed".into()))
        .unwrap();
    channel
        .sync_write(track!(&store, |s| s.description = "Denied".into()))
        .unwrap();
    client.queue().lock().unwrap().collect();

    block_on(sync_step(
        &mut remote,
        client.driver(),
        &server,
        Some(&cache),
        publisher(&store),
        |_| {},
    ))
    .unwrap();

    // The accepted change applied and completed; the rejected change
    // is gone (dropped as a unit — its only leaf was denied).
    assert_eq!(
        store.snapshot().title,
        "Allowed",
        "accepted change converged"
    );
    assert_eq!(
        store.snapshot().description,
        "World",
        "rejected change never applied"
    );
    let issue = server.model("issue").unwrap();
    assert_eq!(issue["title"], json!("Allowed"));
    assert_eq!(issue["description"], json!("World"));
    drop(issue);
    let guard = client.queue().lock().unwrap();
    assert!(guard.is_idle(), "queue fully drained");
    drop(guard);
    assert!(cache.load_batches().unwrap().is_empty(), "cache drained");
    assert_eq!(server.applied_count(), 1, "exactly one leaf applied");
}
