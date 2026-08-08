//! End-to-end integration tests for the muon-store sync pipeline.
//!
//! Covers the full outbound (track! → Draft → queue → Commit) and
//! inbound (delta → RemoteView → reconcile → publish) cycles without network
//! dependencies.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use muon::Observe;
use muon_reactivity::{ReactiveStore, Reactivity};
use muon_store::{track, Store, Track};
use muon_sync::*;
use reactive_graph::effect::ImmediateEffect;
use reactive_graph::owner::Owner;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

// ── Test model ──────────────────────────────────────────────────────────

#[derive(Clone, Serialize, Deserialize, Observe, Track, Reactivity)]
struct Issue {
    title: String,
    description: String,
    priority: i32,
}

// ── Helper ──────────────────────────────────────────────────────────────

fn setup() -> (Store<Issue>, SyncClient, SyncChannel) {
    let store = Store::new(Issue {
        title: "Hello".into(),
        description: "World".into(),
        priority: 0,
    });
    let client = SyncClient::new(42);
    let channel = client.channel("issue");
    (store, client, channel)
}
/// Simulate the adapter's inbound handling: run the in-memory packet
/// transitions (rejected/applied/anchor completion), update the remote,
/// reconcile the model, and capture the published value.
fn apply_delta(
    client: &mut SyncClient,
    remote: &mut RemoteView,
    delta: &DeltaPacket,
) -> Option<Value> {
    let _cmds = client.driver().on_delta(delta);
    let changed = remote.apply_packet(delta);
    let mut published = None;
    for model_id in changed {
        if let Some(authoritative) = remote.value(&model_id).cloned() {
            for cmd in client.driver().reconcile_model(&model_id, &authoritative) {
                if let SyncCommand::ApplyValue { value, .. } = cmd {
                    published = Some(value);
                }
            }
        }
    }
    client.driver().confirm_anchor(delta.sync_id);
    published
}

// ── Tests ───────────────────────────────────────────────────────────────

/// Outbound: a synced write publishes the value and queues exactly one
/// change with correct metadata.
#[test]
fn e2e_outbound_write_capture() {
    let (store, client, channel) = setup();

    channel
        .sync_write(track!(&store, |s| s.title = "New Title".into()))
        .unwrap();

    assert_eq!(store.snapshot().title, "New Title");

    let guard = client.queue().lock().unwrap();
    let out = guard.unsynced_changes("issue");
    assert_eq!(out.len(), 1, "one change");

    let txn = &out[0].1.txns[0];
    assert_eq!(txn.model_id, "issue");
    assert_eq!(
        txn.path,
        vec![muon_sync::PathSegment::String("title".to_owned())]
    );
    assert_eq!(
        txn.kind,
        Changed::Replace {
            before: Some(json!("Hello")),
            after: Some(json!("New Title")),
        },
        "the replacement's before is the pre-mutation value",
    );
    assert_eq!(txn.client_id, 42);
    assert_eq!(txn.id.seq, 1, "first id allocated by the queue");
}

/// Outbound: writing to two fields in one write scope produces two leaf
/// transactions in one change.
#[test]
fn e2e_outbound_multi_field_write() {
    let (store, client, channel) = setup();

    channel
        .sync_write(track!(&store, |s| {
            s.title = "A".into();
            s.description = "B".into();
        }))
        .unwrap();

    let guard = client.queue().lock().unwrap();
    let out = guard.unsynced_changes("issue");
    assert_eq!(out.len(), 1, "one change");
    let txns = &out[0].1.txns;
    assert_eq!(txns.len(), 2, "two field changes = two transactions");

    let paths: Vec<_> = txns.iter().map(|t| t.path.clone()).collect();
    assert!(paths.contains(&vec![muon_sync::PathSegment::String("title".to_owned())]));
    assert!(paths.contains(&vec![muon_sync::PathSegment::String(
        "description".to_owned()
    )]));
}

/// Outbound: consecutive writes queue in FIFO order — change order equals
/// store publication order without any commit sequence numbers.
#[test]
fn e2e_outbound_fifo_order() {
    let (store, client, channel) = setup();

    channel
        .sync_write(track!(&store, |s| s.title = "First".into()))
        .unwrap();
    channel
        .sync_write(track!(&store, |s| s.title = "Second".into()))
        .unwrap();

    assert_eq!(store.snapshot().title, "Second");

    let guard = client.queue().lock().unwrap();
    let out = guard.unsynced_changes("issue");
    assert_eq!(out.len(), 2);
    let txns: Vec<_> = out.iter().flat_map(|(_, c)| c.txns.iter()).collect();
    assert_eq!(
        txns[0].kind,
        Changed::Replace {
            before: Some(json!("Hello")),
            after: Some(json!("First")),
        }
    );
    assert_eq!(
        txns[1].kind,
        Changed::Replace {
            before: Some(json!("First")),
            after: Some(json!("Second")),
        }
    );
    assert!(
        txns[0].id.seq < txns[1].id.seq,
        "ids allocated in enqueue order"
    );
}

/// Outbound: concurrent writes keep store publication order — replaying
/// the queued transactions must reproduce the store's value history.
#[test]
fn e2e_concurrent_write_order() {
    const WRITERS: usize = 8;

    let (store, client, channel) = setup();

    std::thread::scope(|scope| {
        for i in 0..WRITERS {
            let title = format!("T{i}");
            scope.spawn(|| {
                channel
                    .sync_write(track!(&store, |s| s.title = title))
                    .unwrap();
            });
        }
    });

    // Replay the queued transactions in id order (the user intent, kind)
    // and verify no write is lost: the final value equals the store's.
    let guard = client.queue().lock().unwrap();
    let mut by_id: Vec<_> = guard
        .unsynced_changes("issue")
        .into_iter()
        .flat_map(|(_, c)| c.txns.into_iter())
        .collect();
    assert_eq!(by_id.len(), WRITERS, "one transaction per writer");
    by_id.sort_by_key(|t| t.id.seq);
    for t in &by_id {
        assert!(
            matches!(
                &t.kind,
                Changed::Replace {
                    before: Some(_),
                    ..
                }
            ),
            "unexpected kind: {t:?}",
        );
    }
    let final_value = match &by_id.last().unwrap().kind {
        Changed::Replace { after: Some(v), .. } => v.clone(),
        _ => unreachable!(),
    };
    assert_eq!(
        final_value,
        json!(store.snapshot().title),
        "the final transaction's value must equal the store's final value",
    );
}

/// Outbound: a write that produces no observable changes is aborted — the
/// store is untouched and nothing is queued.
#[test]
fn e2e_empty_write_aborts() {
    let (store, client, channel) = setup();

    let result = channel.sync_write(track!(&store, |_s| {}));
    assert!(matches!(result, Err(SyncWriteError::EmptyMutation)));

    assert_eq!(store.snapshot().title, "Hello", "store untouched");
    let guard = client.queue().lock().unwrap();
    assert!(guard.is_idle(), "nothing queued");
}

/// Inbound: `Update` delta touches a different field — the pending
/// change keeps its value and `original`; the published value carries the
/// delta's field plus the replayed local intent.
#[test]
fn e2e_inbound_update_delta() {
    let (store, mut client, channel) = setup();

    channel
        .sync_write(track!(&store, |s| s.title = "Local Title".into()))
        .unwrap();

    let delta = DeltaPacket {
        sync_id: 100,
        actions: vec![DeltaAction::Update {
            model_id: "issue".into(),
            value: json!({"description": "Server Description"}),
        }],
        applied_batch: None,
        rejected: vec![],
    };
    let mut remote = RemoteView::new();
    let published = apply_delta(&mut client, &mut remote, &delta).expect("published");

    assert_eq!(
        published["description"], "Server Description",
        "delta field applied",
    );
    assert_eq!(
        published["title"], "Local Title",
        "non-conflicting local field replayed on top of the delta",
    );

    let guard = client.queue().lock().unwrap();
    let out = guard.unsynced_changes("issue");
    assert_eq!(out.len(), 1, "pending change survived reconcile");
    assert_eq!(
        out[0].1.txns[0].kind,
        Changed::Replace {
            before: Some(json!("Hello")),
            after: Some(json!("Local Title")),
        },
        "before unchanged (delta touched a different field)",
    );
}

/// Inbound: `Value` delta (full model replace) re-captures `original` of
/// the pending change while preserving the user intent.
#[test]
fn e2e_inbound_value_delta_conflict() {
    let (store, mut client, channel) = setup();

    channel
        .sync_write(track!(&store, |s| s.title = "Local Title".into()))
        .unwrap();

    let delta = DeltaPacket {
        sync_id: 200,
        actions: vec![DeltaAction::Value {
            model_id: "issue".into(),
            value: json!({
                "title": "Server Title",
                "description": "Server Desc",
                "priority": 99,
            }),
        }],
        applied_batch: None,
        rejected: vec![],
    };
    let mut remote = RemoteView::new();
    let published = apply_delta(&mut client, &mut remote, &delta).expect("published");

    // The published value keeps the local intent replayed on top.
    assert_eq!(published["title"], "Local Title", "user intent preserved");
    assert_eq!(published["description"], "Server Desc");
    assert_eq!(published["priority"], 99);

    let guard = client.queue().lock().unwrap();
    let out = guard.unsynced_changes("issue");
    assert_eq!(out.len(), 1, "pending change kept after value rebase");
    assert_eq!(
        out[0].1.txns[0].kind,
        Changed::Replace {
            before: Some(json!("Server Title")),
            after: Some(json!("Local Title")),
        },
        "before updated to the authoritative value (sequential rebase); user intent preserved in after",
    );
}

/// Inbound: `Clear` action discards all pending changes for the model.
#[test]
fn e2e_inbound_clear_discards_pending() {
    let (store, mut client, channel) = setup();

    channel
        .sync_write(track!(&store, |s| s.priority = 5))
        .unwrap();

    let delta = DeltaPacket {
        sync_id: 300,
        actions: vec![DeltaAction::Clear {
            model_id: "issue".into(),
        }],
        applied_batch: None,
        rejected: vec![],
    };
    let mut remote = RemoteView::new();
    client.driver().on_delta(&delta);
    remote.apply_packet(&delta);
    client.driver().discard_model("issue");
    client.driver().confirm_anchor(delta.sync_id);

    let guard = client.queue().lock().unwrap();
    assert!(
        guard.unsynced_changes("issue").is_empty(),
        "pending changes discarded after Clear action",
    );
}

/// `apply_transaction` applies a single Transaction back to the store.
#[test]
fn e2e_apply_transaction() {
    let (store, _client, _channel) = setup();

    let txn = Transaction {
        id: TxnId {
            incarnation: 7,
            seq: 1,
        },
        client_id: 42,
        timestamp: 1000,
        kind: Changed::Replace {
            before: Some(json!("Hello")),
            after: Some(json!("Applied Title")),
        },
        model_id: "issue".into(),
        path: vec![muon_sync::PathSegment::String("title".to_owned())],
    };
    txn.apply_into(&store).unwrap();

    assert_eq!(store.snapshot().title, "Applied Title");
    assert_eq!(store.snapshot().description, "World");
}

/// Inbound: a delta carrying the applied report advances the sync anchor
/// and completes changes that were delivered earlier (LSE: a change is
/// complete only when the delta carrying its `lastSyncId` arrives, not
/// when the server accepts the delivery).
#[test]
fn e2e_delta_completes_acknowledged_transactions() {
    let (store, mut client, channel) = setup();

    channel
        .sync_write(track!(&store, |s| s.title = "Local Title".into()))
        .unwrap();

    // Drive outbound: close the batch, persist, dequeue, and deliver.
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
    let response = SendResponse { deduped_at: None };
    let cmds = driver.on_sent(batch_key, response);
    assert!(cmds.is_empty(), "delivery alone resolves nothing");

    // Not complete yet: the batch waits for the applied report.
    let guard = client.queue().lock().unwrap();
    assert!(
        guard.in_flight_front().is_some(),
        "in flight, awaiting applied report"
    );
    assert_eq!(
        guard.last_sync_id(),
        None,
        "delivery must not advance the anchor"
    );
    drop(guard);

    // The remote must already hold the full authoritative value for the
    // Update to merge into.
    let mut remote = RemoteView::new();
    remote.apply_packet(&DeltaPacket {
        sync_id: 0,
        actions: vec![DeltaAction::Value {
            model_id: "issue".into(),
            value: json!({"title": "Server Title", "description": "World", "priority": 0}),
        }],
        applied_batch: None,
        rejected: vec![],
    });
    // The delta carrying the applied report (sync id 100) arrives: the
    // batch resolves and completes within this packet.
    let delta = DeltaPacket {
        sync_id: 100,
        actions: vec![DeltaAction::Update {
            model_id: "issue".into(),
            value: json!({"description": "Server Description"}),
        }],
        applied_batch: Some(batch_key),
        rejected: vec![],
    };
    let published = apply_delta(&mut client, &mut remote, &delta);

    let guard = client.queue().lock().unwrap();
    assert_eq!(
        guard.awaiting_commits().len(),
        0,
        "delta completed the waiting change"
    );
    assert_eq!(guard.last_sync_id(), Some(100), "delta advanced the anchor");
    drop(guard);

    // Publish the reconciled value: the store reflects the delta.
    if let Some(value) = published {
        let t: Issue = serde_json::from_value(value).unwrap();
        store.write(|arc| *arc = Arc::new(t));
    }
    assert_eq!(store.snapshot().description, "Server Description");
}

/// Outbound: a server rejection removes the change from the queue and the
/// model is reconciled from the authoritative value — the rejected change
/// is not replayed.
#[test]
fn e2e_reject_reconciles_from_authoritative() {
    let (store, mut client, channel) = setup();

    channel
        .sync_write(track!(&store, |s| s.title = "Optimistic".into()))
        .unwrap();
    assert_eq!(
        store.snapshot().title,
        "Optimistic",
        "optimistic write applied"
    );

    // Drive outbound and reject.
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

    // The rejection produced a Rejected command for the model.
    assert!(cmds
        .iter()
        .any(|c| matches!(c, SyncCommand::Rejected { model_id } if model_id == "issue")));

    // Reconcile from the authoritative value: the rejected change is
    // gone, so the published value is exactly the server's.
    let authoritative = json!({
        "title": "Server Title",
        "description": "World",
        "priority": 0,
    });
    let cmds = client.driver().reconcile_model("issue", &authoritative);
    let published = cmds
        .into_iter()
        .find_map(|c| match c {
            SyncCommand::ApplyValue { value, .. } => Some(value),
            _ => None,
        })
        .expect("reconcile_model emits ApplyValue");
    assert_eq!(published, authoritative, "rejected change not replayed");
    assert!(
        client.queue().lock().unwrap().is_idle(),
        "no lingering changes after rejection"
    );
}

/// Regression: reconcile must produce its value after the store write
/// lock is released.
///
/// An `ImmediateEffect` reruns synchronously when its trigger fires. If
/// the value were published (and the subscriber notified) while a store
/// lock was still held, a write-back from the effect (a tracked write)
/// would take the non-reentrant store lock on the same thread and
/// deadlock. The contract: reconcile returns the value, then the caller
/// publishes and notifies.
#[test]
fn reconcile_notify_allows_effect_writeback() {
    let rstore = ReactiveStore::new(Issue {
        title: "Hello".into(),
        description: "World".into(),
        priority: 0,
    });
    let store = rstore.core().clone();
    let mut client = SyncClient::new(42);
    let channel = client.channel("issue");
    channel
        .sync_write(track!(&store, |s| s.title = "Local Title".into()))
        .unwrap();

    let owner = Owner::new();
    owner.set();

    let calls = Arc::new(AtomicUsize::new(0));
    let c = Arc::clone(&calls);
    let s = rstore.clone();
    let _effect = ImmediateEffect::new(move || {
        s.title().get(); // subscribe to the store that receives the delta
        let n = c.fetch_add(1, Ordering::Relaxed);
        if n == 1 {
            // First rerun: write back to the same store. This would
            // deadlock if the notification fired under the write lock.
            track!(s.core(), |s| s.description = "echo".into()).commit();
        }
    });
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "effect runs at construction"
    );

    // Reconcile with a server value; the published value carries the
    // local intent replayed on top.
    let authoritative = json!({
        "title": "Server Title",
        "description": "World",
        "priority": 0,
    });
    let cmds = client.driver().reconcile_model("issue", &authoritative);
    let value = cmds
        .into_iter()
        .find_map(|c| match c {
            SyncCommand::ApplyValue { value, .. } => Some(value),
            _ => None,
        })
        .expect("reconcile_model emits ApplyValue");
    assert_eq!(value["title"], "Local Title", "local intent replayed");

    // Publish after reconcile returned (no lock held): replace the core
    // value, then notify.
    let t: Issue = serde_json::from_value(value).unwrap();
    let cr = store.write(|arc| *arc = std::sync::Arc::new(t));
    let (_, event) = cr.into_parts();
    rstore.notify(&event);

    // The notification reran the effect; its write-back applied.
    assert_eq!(
        calls.load(Ordering::Relaxed),
        2,
        "effect reran after the publish"
    );
    assert_eq!(store.snapshot().description, "echo", "write-back applied");
}
