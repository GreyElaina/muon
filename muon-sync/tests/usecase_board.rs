//! End-to-end usecase: a collaborative kanban with two models and an
//! application-layer local database.
//!
//! This test drives the integrated design end to end:
//!
//! - **Phase 1 (multi-model wiring)**: two models (`issue` collection,
//!   `board` single value) share one `TransactionQueue` through two
//!   `SyncChannel`s. Inbound deltas are routed per model.
//! - **Collection form**: `Store<IndexMap<String, Issue>>` — keyed
//!   mutations outbound (`[Field(id)]` path segments) and keyed deltas
//!   inbound (JSON object keys merged by RFC 7396 merge patch).
//! - **Phase 2/3 integration**: every reconciled value (authoritative
//!   delta plus replayed local intent) drives the local database (a
//!   subset of the server). A fresh client hydrates from
//!   it with no network round trip.
//!
//! The local database is written only from reconciled server state (LSE
//! invariant: client optimistic writes never touch the local database).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use indexmap::IndexMap;
use muon::Observe;
use muon_store::{track, Store, Track};
use muon_sync::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};

// ── Models ─────────────────────────────────────────────────────────────

#[derive(Clone, PartialEq, Serialize, Deserialize, Observe, Track)]
struct Issue {
    title: String,
    status: String,
}

// A composed model value (`Issue` inside an observed map) needs a
// snapshot implementation: the map's observer captures each value's
// pre-write state by snapshotting it, and the map's tracked-write
// capability (muon_store::Track for `IndexMap`) requires the value to
// be snapshotable.
impl muon::general::Snapshot for Issue {
    type Snapshot = Issue;

    fn to_snapshot(&self) -> Self::Snapshot {
        self.clone()
    }
}

impl muon::general::SerializeSnapshot for Issue {
    fn flush<S: muon::observe::Sink + ?Sized>(&self, snapshot: Self::Snapshot, sink: &mut S) {
        if self != &snapshot {
            sink.replace(
                Some(&snapshot as &dyn muon::erased_serde::Serialize),
                Some(self as &dyn muon::erased_serde::Serialize),
            );
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Observe, Track)]
struct Board {
    name: String,
}

// ── Application-layer local database ───────────────────────────────────

/// The local database as a subset of the server (LSE invariant).
///
/// Only reconciled server state writes to it; the client's optimistic
/// writes never do. `hydrate` restores the in-memory value at startup.
#[derive(Default)]
struct LocalDb {
    models: Mutex<HashMap<String, Value>>,
}

impl LocalDb {
    /// Project a reconciled final value into the local database.
    fn apply_value(&self, model_id: &str, value: &Value) {
        self.models
            .lock()
            .unwrap()
            .insert(model_id.to_owned(), value.clone());
    }

    /// Remove a model from the local database (Clear/Archive).
    fn remove_model(&self, model_id: &str) {
        self.models.lock().unwrap().remove(model_id);
    }

    /// Restore the in-memory value for `model_id` at startup.
    fn hydrate<T: DeserializeOwned>(&self, model_id: &str) -> Option<T> {
        let guard = self.models.lock().unwrap();
        guard
            .get(model_id)
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }
}

// ── Client ─────────────────────────────────────────────────────────────

/// One client: two models share one queue and one transport (phase 1).
/// The authoritative view of the remote lives in the client's [`RemoteView`]; the
/// local database in its [`LocalDb`].
struct Client {
    issues: Store<IndexMap<String, Issue>>,
    board: Store<Board>,
    client: SyncClient,
    issue_channel: SyncChannel,
    board_channel: SyncChannel,
    remote: RemoteView,
    local_db: LocalDb,
    server: Arc<Mutex<SyncServer>>,
}

impl Client {
    fn new(client_id: u64, server: Arc<Mutex<SyncServer>>) -> Self {
        // Startup hydrate: an empty local database yields the defaults.
        let local_db = LocalDb::default();
        let issues = Store::new(
            local_db
                .hydrate::<IndexMap<String, Issue>>("issue")
                .unwrap_or_default(),
        );
        let board = Store::new(local_db.hydrate::<Board>("board").unwrap_or_else(|| Board {
            name: "Shared Board".into(),
        }));
        let client = SyncClient::new(client_id);
        let issue_channel = client.channel("issue");
        let board_channel = client.channel("board");
        Self {
            issues,
            board,
            client,
            issue_channel,
            board_channel,
            remote: RemoteView::new(),
            local_db,
            server,
        }
    }
}

/// Publish a reconciled value into the typed store.
fn publish_to<T: DeserializeOwned>(store: &Store<T>, value: &Value) {
    let t = serde_json::from_value(value.clone()).expect("reconciled value fits the model");
    store.write(|arc| *arc = Arc::new(t));
}

/// One sync round for one model: drive the outbound pipeline (close,
/// persist, send, resolve), then pull deltas past our anchor, routing
/// each packet per model. Every reconciled value publishes to the store
/// and drives the local database.
impl Client {
    /// Sync the `issue` collection model.
    fn sync_issues(&mut self) {
        sync_model(
            &mut self.client,
            &mut self.remote,
            &self.local_db,
            &self.server,
            &self.issues,
            "issue",
        );
    }

    /// Sync the `board` single-value model.
    fn sync_board(&mut self) {
        sync_model(
            &mut self.client,
            &mut self.remote,
            &self.local_db,
            &self.server,
            &self.board,
            "board",
        );
    }
}

fn sync_model<T>(
    client: &mut SyncClient,
    remote: &mut RemoteView,
    local_db: &LocalDb,
    server: &Mutex<SyncServer>,
    store: &Store<T>,
    model_id: &str,
) where
    T: Serialize + DeserializeOwned + 'static,
{
    // Outbound: keep stepping until nothing new to send.
    loop {
        let cmds = client.driver().outbound_step();
        if cmds.is_empty() {
            break;
        }
        for cmd in cmds {
            match cmd {
                SyncCommand::Persist { batch } => {
                    // This usecase has no crash recovery; treat
                    // persistence as immediately durable.
                    client.driver().on_persisted(batch.id);
                }
                SyncCommand::Send { batch_key, txns } => {
                    let response = server.lock().unwrap().send(batch_key, &txns);
                    for cmd in client.driver().on_sent(batch_key, response) {
                        handle_resolved(client, remote, local_db, store, cmd);
                    }
                }
                _ => unreachable!("outbound_step emits only Persist/Send"),
            }
        }
    }

    // Inbound: pull deltas past our anchor, keep only this model's
    // actions, reconcile, and project into the local database. The
    // applied-batch report is batch-level and survives the per-model
    // action filter. A bootstrap snapshot (no anchor yet) rebuilds the
    // remote and reconciles every model.
    let since = match client.driver().poll_command() {
        SyncCommand::PollDeltas { since } => since,
        _ => unreachable!("poll_command always emits PollDeltas"),
    };
    match server.lock().unwrap().poll(since) {
        PollOutcome::ResetRequired => panic!("anchor fell out of the window"),
        PollOutcome::Snapshot {
            sync_id,
            models,
            reports,
        } => {
            for packet in reports {
                apply_packet(client, remote, local_db, store, model_id, packet);
            }
            let changed = remote.apply_snapshot(&models);
            for mid in changed {
                reconcile_and_publish(client, remote, local_db, store, &mid);
            }
            client.driver().confirm_anchor(sync_id);
        }
        PollOutcome::Deltas(deltas) => {
            for delta in deltas {
                apply_packet(client, remote, local_db, store, model_id, delta);
            }
        }
    }
}

/// Apply one delta packet: filter to the model's actions, run the
/// queue bookkeeping, reconcile the changed models, and advance the
/// anchor.
fn apply_packet<T>(
    client: &mut SyncClient,
    remote: &mut RemoteView,
    local_db: &LocalDb,
    store: &Store<T>,
    model_id: &str,
    delta: DeltaPacket,
) where
    T: Serialize + DeserializeOwned + 'static,
{
    let actions: Vec<DeltaAction> = delta
        .actions
        .into_iter()
        .filter(|a| action_model_id(a) == Some(model_id))
        .collect();
    let filtered = DeltaPacket {
        sync_id: delta.sync_id,
        actions,
        applied_batch: delta.applied_batch,
        rejected: delta.rejected,
    };
    let cmds = client.driver().on_delta(&filtered);
    for cmd in cmds {
        if let SyncCommand::Rejected { model_id } = cmd {
            reconcile_and_publish(client, remote, local_db, store, &model_id);
        }
    }
    let changed = remote.apply_packet(&filtered);
    for mid in changed {
        reconcile_and_publish(client, remote, local_db, store, &mid);
    }
    client.driver().confirm_anchor(filtered.sync_id);
}

/// Handle a command produced by resolving a send (rejections reconcile
/// the model from the local database; cache removals are a no-op here).
fn handle_resolved<T>(
    client: &mut SyncClient,
    remote: &mut RemoteView,
    local_db: &LocalDb,
    store: &Store<T>,
    cmd: SyncCommand,
) where
    T: Serialize + DeserializeOwned + 'static,
{
    match cmd {
        SyncCommand::RemoveBatch { .. } => {}
        SyncCommand::Rejected { model_id } => {
            reconcile_and_publish(client, remote, local_db, store, &model_id);
        }
        _ => unreachable!("on_sent emits only RemoveBatch/Rejected"),
    }
}

/// Reconcile one model from the remote's authoritative value and publish
/// the result to the store and the local database.
fn reconcile_and_publish<T>(
    client: &mut SyncClient,
    remote: &mut RemoteView,
    local_db: &LocalDb,
    store: &Store<T>,
    model_id: &str,
) where
    T: Serialize + DeserializeOwned + 'static,
{
    if let Some(authoritative) = remote.value(model_id).cloned() {
        for cmd in client.driver().reconcile_model(model_id, &authoritative) {
            if let SyncCommand::ApplyValue { model_id, value } = cmd {
                publish_to(store, &value);
                local_db.apply_value(&model_id, &value);
            }
            // Rewrite/anchor commands are adapter bookkeeping.
        }
    } else {
        // The model was cleared or archived.
        client.driver().discard_model(model_id);
        local_db.remove_model(model_id);
    }
}

fn action_model_id(action: &DeltaAction) -> Option<&str> {
    match action {
        DeltaAction::Insert { model_id } => Some(model_id),
        DeltaAction::Value { model_id, .. } => Some(model_id),
        DeltaAction::Update { model_id, .. } => Some(model_id),
        DeltaAction::Archive { model_id } => Some(model_id),
        DeltaAction::Clear { model_id } => Some(model_id),
        DeltaAction::SeqOp { model_id, .. } => Some(model_id),
    }
}

// ── The usecase ────────────────────────────────────────────────────────

#[test]
fn kanban_multi_model_local_hydrate() {
    let server = Arc::new(Mutex::new(SyncServer::new()));

    // Two clients start from empty local databases (first launch).
    let mut a = Client::new(42, server.clone());
    let mut b = Client::new(43, server.clone());

    // 1. A creates issue-1: keyed mutation outbound.
    a.issue_channel
        .sync_write(track!(&a.issues, |s| s.insert(
            "issue-1".into(),
            Issue {
                title: "Write the design".into(),
                status: "todo".into(),
            },
        )))
        .unwrap();
    assert_eq!(
        a.issues.snapshot().get("issue-1").unwrap().title,
        "Write the design",
    );
    {
        let guard = a.client.queue().lock().unwrap();
        let out = guard.unsynced_changes("issue");
        let txn = &out[0].1.txns[0];
        assert_eq!(
            txn.path,
            vec![muon_sync::PathSegment::String("issue-1".into())]
        );
        assert!(matches!(txn.kind, Changed::Replace { .. }));
    }

    // 2. A syncs: send, server applies, confirming delta returns.
    a.sync_issues();

    // 3. B boots and pulls A's delta: keyed insert inbound.
    b.sync_issues();
    assert_eq!(
        b.issues.snapshot().get("issue-1").unwrap().title,
        "Write the design",
    );
    // B's remote holds the issue model (delta → remote).
    let b_issues = b
        .local_db
        .hydrate::<IndexMap<String, Issue>>("issue")
        .unwrap();
    assert!(b_issues.contains_key("issue-1"));

    // 4. B edits issue-1.title: keyed field-level mutation outbound.
    b.issue_channel
        .sync_write(track!(&b.issues, |s| s.get_mut("issue-1").unwrap().title =
            "Write the design (rev 2)".into()))
        .unwrap();
    {
        let guard = b.client.queue().lock().unwrap();
        let out = guard.unsynced_changes("issue");
        let txn = &out[0].1.txns[0];
        assert_eq!(
            txn.path,
            vec![
                muon_sync::PathSegment::String("issue-1".into()),
                muon_sync::PathSegment::String("title".into()),
            ],
        );
    }

    // 5. B and A sync; the field-level edit reaches A.
    b.sync_issues();
    a.sync_issues();
    assert_eq!(
        a.issues.snapshot().get("issue-1").unwrap().title,
        "Write the design (rev 2)",
    );

    // 6. B edits board.name: the single-value model shares the pipeline.
    //    `Board` is a single-field struct, so muon promotes the field
    //    assignment to a root-level replace (all fields replaced). The
    //    sync semantics are unchanged — the server treats it as an
    //    upsert of the whole model.
    b.board_channel
        .sync_write(track!(&b.board, |s| s.name = "Shared Board v2".into()))
        .unwrap();
    b.sync_board();
    a.sync_board();
    assert_eq!(a.board.snapshot().name, "Shared Board v2");

    // 7. Hydrate: a fresh client restores both models from A's remote
    //    with no network round trip.
    let c_issues = Store::new(
        a.local_db
            .hydrate::<IndexMap<String, Issue>>("issue")
            .unwrap(),
    );
    let c_board = Store::new(a.local_db.hydrate::<Board>("board").unwrap());
    assert_eq!(
        c_issues.snapshot().get("issue-1").unwrap().title,
        "Write the design (rev 2)",
    );
    assert_eq!(c_board.snapshot().name, "Shared Board v2");

    // 8. Convergence: server state matches every client's memory.
    assert_eq!(
        server.lock().unwrap().model("issue").unwrap()["issue-1"]["title"],
        json!("Write the design (rev 2)"),
    );
    assert_eq!(
        server.lock().unwrap().model("board").unwrap()["name"],
        json!("Shared Board v2"),
    );
}

#[test]
fn key_removal_converges_via_merge_patch() {
    let server = Arc::new(Mutex::new(SyncServer::new()));
    let mut a = Client::new(46, server.clone());
    let mut b = Client::new(47, server.clone());

    // A creates two issues; both clients see both.
    a.issue_channel
        .sync_write(track!(&a.issues, |s| s.insert(
            "issue-1".into(),
            Issue {
                title: "One".into(),
                status: "todo".into(),
            },
        )))
        .unwrap();
    a.issue_channel
        .sync_write(track!(&a.issues, |s| s.insert(
            "issue-2".into(),
            Issue {
                title: "Two".into(),
                status: "todo".into(),
            },
        )))
        .unwrap();
    a.sync_issues();
    b.sync_issues();
    assert!(b.issues.snapshot().contains_key("issue-2"));

    // A removes issue-2: a keyed Delete outbound, applied server-side.
    a.issue_channel
        .sync_write(track!(&a.issues, |s| s.shift_remove("issue-2")))
        .unwrap();
    a.sync_issues();

    // The server state dropped the key; A converged. The broadcast
    // merge patch carried `null` for the removed member, the remote
    // dropped it, and the reconciled value removed it from the store.
    {
        assert!(!server
            .lock()
            .unwrap()
            .model("issue")
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("issue-2"));
    }
    assert!(!a.issues.snapshot().contains_key("issue-2"));
    assert!(a.remote.value("issue").unwrap().get("issue-2").is_none());

    // B pulls the same patch and converges too.
    b.sync_issues();
    assert!(!b.issues.snapshot().contains_key("issue-2"));
    assert!(b.remote.value("issue").unwrap().get("issue-2").is_none());
}
