//! Multi-client collaboration end-to-end: two clients drive `CrdtVec`
//! fields through the real server (send → apply → delta → reconcile),
//! covering the merge semantics the property tests cannot — concurrent
//! inserts, moves, deletes, and whole-field replacement across
//! clients.
//!
//! The server is the single applier in receive order; each client is
//! an independent identity allocator (its container draws its own
//! incarnation), so merged elements keep distinct identities.
//!
//! The last test fuzzes the collaboration itself: two independent
//! random operation streams, applied through the full pipeline and
//! cross-synced until idle, must converge on both clients and on the
//! server.

use std::sync::{Arc, Mutex};

use muon::Observe;
use muon_store::{track, Store, Track};
use muon_sync::*;
mod common;
use common::TestServer;
use proptest::prelude::*;
use proptest::test_runner::Config;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

// ── Test model ──────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Observe, Clone, Debug, PartialEq, Track)]
struct Block {
    id: u32,
    text: String,
}

#[derive(Serialize, Deserialize, Observe, Clone, Debug, PartialEq, Track)]
struct Doc {
    blocks: CrdtVec<Block>,
}

fn make_store() -> Store<Doc> {
    Store::new(Doc {
        blocks: CrdtVec::new(),
    })
}

fn server() -> TestServer {
    let s = TestServer::new();
    s.seed("doc", serde_json::json!({ "blocks": [] }));
    s
}

/// An in-memory `TransactionCache`: the persist gate without disk.
struct MemCache {
    batches: Mutex<Vec<CommitBatch>>,
    anchor: Mutex<Option<u64>>,
}

impl MemCache {
    fn new() -> Self {
        Self {
            batches: Mutex::new(Vec::new()),
            anchor: Mutex::new(None),
        }
    }
}

impl TransactionCache for MemCache {
    fn persist_batch(
        &self,
        batch: &CommitBatch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.batches.lock().unwrap().push(batch.clone());
        Ok(())
    }

    fn load_batches(&self) -> Result<Vec<CommitBatch>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.batches.lock().unwrap().clone())
    }

    fn remove_batch(&self, id: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.batches.lock().unwrap().retain(|b| b.id != id);
        Ok(())
    }

    fn save_anchor(&self, sync_id: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        *self.anchor.lock().unwrap() = Some(sync_id);
        Ok(())
    }

    fn load_anchor(&self) -> Result<Option<u64>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(*self.anchor.lock().unwrap())
    }

    fn save_known_models(
        &self,
        _models: &[String],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    fn load_known_models(
        &self,
    ) -> Result<Option<Vec<String>>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(None)
    }
}

fn client(client_id: u64) -> (Store<Doc>, SyncClient, SyncChannel, MemCache) {
    let store = make_store();
    let client = SyncClient::new(client_id);
    let channel = client.channel("doc");
    (store, client, channel, MemCache::new())
}

/// The adapter's `publish` callback: deserialize the reconciled value
/// into the store's type and replace the value.
fn publisher<D>(store: &Store<D>) -> impl FnMut(&str, &Value) + '_
where
    D: for<'de> Deserialize<'de> + 'static,
{
    move |_model_id, value| {
        let t: D = serde_json::from_value(value.clone())
            .unwrap_or_else(|e| panic!("reconciled value {value} does not fit: {e}"));
        store.write(|arc| *arc = Arc::new(t));
    }
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

/// Drive one full sync step: send what is queued, poll deltas from the
/// last anchor, apply them to the remote, and publish the reconciled
/// value into the store.
fn sync<D>(
    remote: &mut RemoteView,
    client: &mut SyncClient,
    server: &TestServer,
    cache: &MemCache,
    store: &Store<D>,
) where
    D: for<'de> Deserialize<'de> + 'static,
{
    block_on(sync_step(
        remote,
        client.driver(),
        server,
        Some(cache),
        publisher(store),
        |_| {},
    ))
    .expect("sync step must succeed");
}

fn texts(doc: &Doc) -> Vec<String> {
    doc.blocks.iter().map(|b| b.text.clone()).collect()
}

// ── Tests ───────────────────────────────────────────────────────────────

/// Concurrent pushes from two clients merge on the server in receive
/// order, and both clients converge to the merged list with distinct
/// element identities.
#[test]
fn concurrent_pushes_converge() {
    let server = server();
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    channel_a
        .sync_write(track!(&store_a, |s| s.blocks.push(Block {
            id: 1,
            text: "A-1".into()
        })))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);

    // B pushes from the empty baseline and syncs independently.
    channel_b
        .sync_write(track!(&store_b, |s| s.blocks.push(Block {
            id: 2,
            text: "B-1".into()
        })))
        .unwrap();
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // A's next sync receives B's delta. B pushed from its empty
    // baseline (it never saw A-1), so B's insert anchors at the head —
    // the server merges in receive order to [B-1, A-1], and both
    // clients converge to that list.
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);

    let expected = vec!["B-1", "A-1"];
    assert_eq!(texts(&store_a.snapshot()), expected);
    assert_eq!(texts(&store_b.snapshot()), expected);

    // The server holds both elements with distinct identities.
    let server_blocks = server.model("doc").expect("seeded model")["blocks"]
        .as_array()
        .expect("single-list array")
        .clone();
    assert_eq!(server_blocks.len(), 2);
    let id_a = &server_blocks[0]["id"];
    let id_b = &server_blocks[1]["id"];
    assert_ne!(
        id_a, id_b,
        "independent allocators keep distinct identities"
    );
}

/// A delete and a move racing from the same baseline converge: the
/// server applies in receive order, and both clients end on the same
/// live view.
#[test]
fn concurrent_move_and_delete_converge() {
    let server = server();
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    // Shared baseline [x, y, z].
    for (id, text) in [(1u32, "x"), (2, "y"), (3, "z")] {
        channel_a
            .sync_write(track!(&store_a, |s| s.blocks.push(Block {
                id,
                text: text.into()
            })))
            .unwrap();
        sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    }
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    assert_eq!(texts(&store_b.snapshot()), vec!["x", "y", "z"]);

    // A deletes y; B moves z to the head — both from the same baseline.
    channel_a
        .sync_write(track!(&store_a, |s| {
            s.blocks.remove(1);
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);

    channel_b
        .sync_write(track!(&store_b, |s| {
            s.blocks.move_to(2, 0);
        }))
        .unwrap();
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // Cross-sync: both clients receive the other's change.
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // Server applied delete first ([x, z]), then the move (z → head):
    // both converge to [z, x]. The deleted y stays as a tombstone in
    // the wire form.
    let expected = vec!["z", "x"];
    assert_eq!(texts(&store_a.snapshot()), expected);
    assert_eq!(texts(&store_b.snapshot()), expected);
    let server_blocks = server.model("doc").expect("seeded model")["blocks"]
        .as_array()
        .expect("single-list array")
        .clone();
    assert_eq!(server_blocks.len(), 3, "the tombstone keeps its slot");
    let y = server_blocks
        .iter()
        .find(|n| n["value"]["text"] == json!("y"))
        .expect("y in the wire form");
    assert!(!y["alive"].as_bool().unwrap(), "y is marked dead");
}

/// Two clients concurrently move the *same* element to different
/// places. The lamport order (client 1 first) decides: A's move lands
/// at the head; B's move then re-applies with its recorded `to`
/// interpreted against the new state — the later move's slot wins, the
/// earlier one is removed, and both clients converge on the same
/// result.
#[test]
fn concurrent_moves_of_one_element_converge() {
    let server = server();
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    // Shared baseline [x, y, z].
    for (id, text) in [(1u32, "x"), (2, "y"), (3, "z")] {
        channel_a
            .sync_write(track!(&store_a, |s| s.blocks.push(Block {
                id,
                text: text.into()
            })))
            .unwrap();
        sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    }
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    assert_eq!(texts(&store_b.snapshot()), vec!["x", "y", "z"]);

    // A moves z to the head; B moves z after y — from the same baseline.
    channel_a
        .sync_write(track!(&store_a, |s| {
            s.blocks.move_to(2, 0);
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);

    channel_b
        .sync_write(track!(&store_b, |s| {
            s.blocks.move_to(2, 1);
        }))
        .unwrap();
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // Cross-sync: both clients receive the other's change.
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // A's move applied first leaves z at the head. B's move recorded
    // its target as the anchor of y's slot in the baseline — the
    // element before y, which is x. In the post-A state x still
    // follows z, so B's move places z right after x: the element
    // occupies exactly the slot B intended (between x and y), and
    // both sides converge on the same deterministic result.
    let expected = vec!["x", "z", "y"];
    assert_eq!(texts(&store_a.snapshot()), expected);
    assert_eq!(texts(&store_b.snapshot()), expected);
    let server_blocks = server.model("doc").expect("seeded model")["blocks"]
        .as_array()
        .expect("single-list array")
        .clone();
    assert_eq!(server_blocks.len(), 3);
}

/// A whole-field replacement on one client is a field-level Replace
/// transaction: the other client converges to the fresh list without
/// any doc-level overwrite.
#[test]
fn whole_replace_reaches_other_client() {
    let server = server();
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, _channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    channel_a
        .sync_write(track!(&store_a, |s| s.blocks.push(Block {
            id: 1,
            text: "old".into()
        })))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    assert_eq!(texts(&store_b.snapshot()), vec!["old"]);

    // A replaces the whole field with a fresh container.
    let mut fresh = CrdtVec::new();
    fresh.push(Block {
        id: 9,
        text: "new".into(),
    });
    channel_a
        .sync_write(track!(&store_a, |s| {
            s.blocks = fresh;
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);

    // B receives the field-level Replace and converges.
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    assert_eq!(texts(&store_b.snapshot()), vec!["new"]);
    assert_eq!(texts(&store_a.snapshot()), vec!["new"]);

    // The server holds the fresh container's own identities (the
    // replacement payload, not a doc-level rewrite).
    let server_blocks = server.model("doc").expect("seeded model")["blocks"]
        .as_array()
        .expect("single-list array")
        .clone();
    assert_eq!(server_blocks.len(), 1);
    assert_eq!(server_blocks[0]["value"]["id"], 9);
}

// ═══════════════════════════════════════════════════════════════════
// Collaboration fuzz
// ═══════════════════════════════════════════════════════════════════

/// One structural operation on a `CrdtVec` field, generated at random.
/// Indices resolve against the live view at application time (modulo
/// the current length).
#[derive(Debug, Clone, Copy)]
enum Op {
    Push { value: u32 },
    InsertAt { index: usize, value: u32 },
    Remove { index: usize },
    Move { index: usize, new_index: usize },
}

impl Arbitrary for Op {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            any::<u32>().prop_map(|v| Op::Push { value: v }),
            (any::<usize>(), any::<u32>()).prop_map(|(i, v)| Op::InsertAt { index: i, value: v }),
            any::<usize>().prop_map(|i| Op::Remove { index: i }),
            (any::<usize>(), any::<usize>()).prop_map(|(i, n)| Op::Move {
                index: i,
                new_index: n
            }),
        ]
        .boxed()
    }
}

/// Apply one op through a full sync write on the client's store. The
/// indices resolve against the store's current live view inside the
/// observed body. A no-op op (an empty list, equal move indices)
/// records nothing and is reported as `EmptyMutation` — the fuzz
/// generator may produce one, so it is tolerated.
fn apply_op(channel: &SyncChannel, store: &Store<Doc>, op: Op) {
    let result = match op {
        Op::Push { value } => channel.sync_write(track!(store, |s| s.blocks.push(Block {
            id: value,
            text: format!("v{value}"),
        }))),
        Op::InsertAt { index, value } => channel.sync_write(track!(store, |s| {
            let n = s.blocks.len();
            if n == 0 {
                s.blocks.push(Block {
                    id: value,
                    text: format!("v{value}"),
                });
            } else {
                s.blocks.insert(
                    index % n,
                    Block {
                        id: value,
                        text: format!("v{value}"),
                    },
                );
            }
        })),
        Op::Remove { index } => channel.sync_write(track!(store, |s| {
            let n = s.blocks.len();
            if n > 0 {
                s.blocks.remove(index % n);
            }
        })),
        Op::Move { index, new_index } => channel.sync_write(track!(store, |s| {
            let n = s.blocks.len();
            if n > 0 {
                let from = index % n;
                let to = new_index % n;
                if from != to {
                    s.blocks.move_to(from, to);
                }
            }
        })),
    };
    match result {
        Ok(_) => {}
        // A no-op op records nothing; the empty write is aborted.
        Err(SyncWriteError::EmptyMutation) => {}
    }
}

proptest! {
    // E2E cases are heavier than the in-lib properties (every op is a
    // full sync write, every case drives several sync steps), so the
    // case count is smaller.
    #![proptest_config(Config::with_cases(128))]

    #[test]
    fn two_clients_random_streams_converge(
        ops_a in prop::collection::vec(any::<Op>(), 0..=12),
        ops_b in prop::collection::vec(any::<Op>(), 0..=12),
    ) {
        let server = server();
        let (store_a, mut client_a, channel_a, cache_a) = client(1);
        let (store_b, mut client_b, channel_b, cache_b) = client(2);
        let mut remote_a = RemoteView::new();
        let mut remote_b = RemoteView::new();

        // A applies its whole stream, then syncs once.
        for op in &ops_a {
            apply_op(&channel_a, &store_a, *op);
        }
        sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);

        // B applies its whole stream from the baseline it has (A's
        // first sync may already be visible if B bootstrapped), then
        // syncs.
        for op in &ops_b {
            apply_op(&channel_b, &store_b, *op);
        }
        sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

        // Cross-sync until both queues are idle: each client receives
        // the other's deltas and reconciles.
        sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
        sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
        sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
        sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

        // Three-way convergence: both clients and the server agree on
        // the live view (position order of element texts).
        let a = texts(&store_a.snapshot());
        let b = texts(&store_b.snapshot());
        let server_texts: Vec<String> = server
            .model("doc")
            .expect("seeded model")["blocks"]
            .as_array()
            .expect("single-list array")
            .iter()
            .filter(|n| n["alive"].as_bool().unwrap())
            .map(|n| n["value"]["text"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(a, b, "clients converge");
        assert_eq!(a, server_texts, "clients match the server");
    }
}

/// One interleaving step across `n` clients. `Op(k)` applies client
/// k's next op from its stream; `Sync(k)` drives client k through one
/// full sync step (send queued + poll deltas + reconcile + publish).
/// The generated index is taken modulo the client count at runtime.
#[derive(Debug, Clone, Copy)]
enum Step {
    Op(usize),
    Sync(usize),
}

impl Arbitrary for Step {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            any::<usize>().prop_map(Step::Op),
            any::<usize>().prop_map(Step::Sync),
        ]
        .boxed()
    }
}

/// One sync step that reports whether the client reconciled an
/// inbound delta (its publish callback fired). A full round with no
/// reconcile anywhere means every queue is idle.
fn sync_flag<D>(
    remote: &mut RemoteView,
    client: &mut SyncClient,
    server: &TestServer,
    cache: &MemCache,
    store: &Store<D>,
    changed: &mut bool,
) where
    D: for<'de> Deserialize<'de> + 'static,
{
    *changed = false;
    block_on(sync_step(
        remote,
        client.driver(),
        server,
        Some(cache),
        |_model_id, value| {
            let t: D = serde_json::from_value(value.clone())
                .unwrap_or_else(|e| panic!("reconciled value {value} does not fit: {e}"));
            store.write(|arc| *arc = Arc::new(t));
            *changed = true;
        },
        |_| {},
    ))
    .expect("sync step must succeed");
}

proptest! {
    // The heaviest case class: every step is a full sync write or a
    // transport round trip, so the case count is small.
    #![proptest_config(Config::with_cases(64))]

    #[test]
    fn n_clients_interleaved_streams_converge(
        op_streams in prop::collection::vec(
            prop::collection::vec(any::<Op>(), 0..=10),
            3usize..=5,
        ),
        schedule in prop::collection::vec(any::<Step>(), 0..=48),
    ) {
        let n = op_streams.len();
        let server = server();

        // One independent client per stream: store, sync client,
        // channel, cache, and remote view. Client ids start at 1;
        // the lamport order is the client-id order.
        let mut stores = Vec::new();
        let mut clients = Vec::new();
        let mut channels = Vec::new();
        let mut caches = Vec::new();
        let mut remotes = Vec::new();
        for i in 0..n {
            let (store, client, channel, cache) = client(i as u64 + 1);
            stores.push(store);
            clients.push(client);
            channels.push(channel);
            caches.push(cache);
            remotes.push(RemoteView::new());
        }

        // A per-client cursor into its op stream. Ops and syncs
        // interleave globally: a client may issue ops against a
        // stale view (its own deltas not yet pulled), or sync while
        // its peers race — the full concurrent-mutation space.
        let mut cursor = vec![0usize; n];
        for step in &schedule {
            match *step {
                Step::Op(k) => {
                    let k = k % n;
                    let stream = &op_streams[k];
                    if cursor[k] < stream.len() {
                        apply_op(&channels[k], &stores[k], stream[cursor[k]]);
                        cursor[k] += 1;
                    }
                }
                Step::Sync(k) => {
                    let k = k % n;
                    sync(&mut remotes[k], &mut clients[k], &server, &caches[k], &stores[k]);
                }
            }
        }

        // Drain: keep syncing every client until a full round
        // reconciles nothing anywhere. The bound is a dead-loop
        // guard; the convergence asserts below catch a premature
        // exit.
        for _ in 0..=(n * 4) {
            let mut any = false;
            for k in 0..n {
                let mut changed = false;
                sync_flag(
                    &mut remotes[k],
                    &mut clients[k],
                    &server,
                    &caches[k],
                    &stores[k],
                    &mut changed,
                );
                any |= changed;
            }
            if !any {
                break;
            }
        }

        // Full convergence: every client agrees with every other
        // client and with the server on the live view.
        let first = texts(&stores[0].snapshot());
        for (k, store) in stores.iter().enumerate().skip(1) {
            assert_eq!(
                texts(&store.snapshot()),
                first,
                "client {k} diverges from client 0"
            );
        }
        let server_texts: Vec<String> = server
            .model("doc")
            .expect("seeded model")["blocks"]
            .as_array()
            .expect("single-list array")
            .iter()
            .filter(|node| node["alive"].as_bool().unwrap())
            .map(|node| node["value"]["text"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(first, server_texts, "clients diverge from the server");

        // Structural invariants on the server's wire state: every
        // node carries a liveness marker (deleted elements stay as
        // tombstones), and element identities stay unique across all
        // clients.
        let server_blocks = server
            .model("doc")
            .expect("seeded model")["blocks"]
            .as_array()
            .expect("single-list array")
            .clone();
        assert!(
            server_blocks.iter().all(|node| node["alive"].is_boolean()),
            "every node carries a liveness marker"
        );
        let mut ids: Vec<_> = server_blocks.iter().map(|node| node["id"].clone()).collect();
        ids.sort_by_key(|a| a.to_string());
        for pair in ids.windows(2) {
            assert_ne!(pair[0], pair[1], "duplicate element id on the server");
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// Failure-injection fuzz
// ═══════════════════════════════════════════════════════════════════

/// A [`TestServer`] wrapper that injects transport faults on demand:
/// a poll failure (the client re-sends its in-flight batch — the
/// server must deduplicate), a stale-since poll (the server replays
/// deltas the client already applied — replay must be idempotent),
/// and field denial (transactions are rejected and reconciled away).
struct FaultyServer {
    inner: TestServer,
    fail_poll: std::sync::atomic::AtomicBool,
    stale_poll: std::sync::atomic::AtomicBool,
    deny_field: &'static str,
}

impl FaultyServer {
    /// A fault-injecting wrapper over a fresh server seeded with
    /// `seed_json`; `deny_field` names the field that `deny_all`
    /// rejects.
    fn new(seed_json: Value, deny_field: &'static str) -> Self {
        let s = TestServer::new();
        s.seed("doc", seed_json);
        Self {
            inner: s,
            fail_poll: std::sync::atomic::AtomicBool::new(false),
            stale_poll: std::sync::atomic::AtomicBool::new(false),
            deny_field,
        }
    }

    /// The next poll fails like a network error; the caller retries
    /// and re-sends its in-flight batch.
    fn fail_next_poll(&self) {
        self.fail_poll
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// The next poll asks from one sync id earlier than the client's
    /// anchor, so the server replays the last delta packet.
    fn stale_next_poll(&self) {
        self.stale_poll
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    fn deny_all(&self) {
        self.inner.deny_field(self.deny_field);
    }
}

impl SyncTransport for FaultyServer {
    async fn send(
        &self,
        batch_key: BatchKey,
        txns: &[Transaction],
    ) -> Result<SendResponse, SendError> {
        self.inner.send(batch_key, txns).await
    }

    async fn poll_deltas(&self, since: Option<u64>) -> Result<PollOutcome, String> {
        if self
            .fail_poll
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            return Err("injected poll failure".to_owned());
        }
        let since = if self
            .stale_poll
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            since.map(|s| s.saturating_sub(1))
        } else {
            since
        };
        self.inner.poll_deltas(since).await
    }
}

/// One interleaving step with injected faults. `Op(k)` and `Sync(k)`
/// mirror the collaboration fuzz; the fault steps wrap a sync with a
/// transport fault.
#[derive(Debug, Clone, Copy)]
enum FaultStep {
    Op(usize),
    Sync(usize),
    FailPoll(usize),
    StalePoll(usize),
    DenyAll,
}

impl Arbitrary for FaultStep {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            any::<usize>().prop_map(FaultStep::Op),
            any::<usize>().prop_map(FaultStep::Sync),
            any::<usize>().prop_map(FaultStep::FailPoll),
            any::<usize>().prop_map(FaultStep::StalePoll),
            Just(FaultStep::DenyAll),
        ]
        .boxed()
    }
}

/// One sync step that retries a network failure once (the re-sent
/// batch must be deduplicated by the server). Records whether the
/// client reconciled an inbound delta.
fn sync_fault<D>(
    remote: &mut RemoteView,
    client: &mut SyncClient,
    server: &FaultyServer,
    cache: &MemCache,
    store: &Store<D>,
    changed: &mut bool,
) where
    D: for<'de> Deserialize<'de> + 'static,
{
    *changed = false;
    let mut attempt = || {
        block_on(sync_step(
            remote,
            client.driver(),
            server,
            Some(cache),
            |_model_id, value| {
                let t: D = serde_json::from_value(value.clone())
                    .unwrap_or_else(|e| panic!("reconciled value {value} does not fit: {e}"));
                store.write(|arc| *arc = Arc::new(t));
                *changed = true;
            },
            |_| {},
        ))
    };
    match attempt() {
        Ok(_) => {}
        Err(SyncLoopError::Network(_)) => {
            attempt().expect("retry after an injected poll failure succeeds");
        }
        Err(e) => panic!("sync step failed: {e}"),
    }
}

proptest! {
    // Each case is a full pipeline with several sync steps; the case
    // count stays small.
    #![proptest_config(Config::with_cases(48))]

    #[test]
    fn fault_injection_converges(
        op_streams in prop::collection::vec(
            prop::collection::vec(any::<Op>(), 0..=10),
            2usize..=4,
        ),
        schedule in prop::collection::vec(any::<FaultStep>(), 0..=40),
    ) {
        let n = op_streams.len();
        let server = FaultyServer::new(json!({ "blocks": [] }), "blocks");

        let mut stores = Vec::new();
        let mut clients = Vec::new();
        let mut channels = Vec::new();
        let mut caches = Vec::new();
        let mut remotes = Vec::new();
        for i in 0..n {
            let (store, client, channel, cache) = client(i as u64 + 1);
            stores.push(store);
            clients.push(client);
            channels.push(channel);
            caches.push(cache);
            remotes.push(RemoteView::new());
        }

        let mut cursor = vec![0usize; n];
        for step in &schedule {
            match *step {
                FaultStep::Op(k) => {
                    let k = k % n;
                    let stream = &op_streams[k];
                    if cursor[k] < stream.len() {
                        apply_op(&channels[k], &stores[k], stream[cursor[k]]);
                        cursor[k] += 1;
                    }
                }
                FaultStep::Sync(k) => {
                    let k = k % n;
                    let mut changed = false;
                    sync_fault(
                        &mut remotes[k],
                        &mut clients[k],
                        &server,
                        &caches[k],
                        &stores[k],
                        &mut changed,
                    );
                }
                FaultStep::FailPoll(k) => {
                    server.fail_next_poll();
                    let k = k % n;
                    let mut changed = false;
                    sync_fault(
                        &mut remotes[k],
                        &mut clients[k],
                        &server,
                        &caches[k],
                        &stores[k],
                        &mut changed,
                    );
                }
                FaultStep::StalePoll(k) => {
                    server.stale_next_poll();
                    let k = k % n;
                    let mut changed = false;
                    sync_fault(
                        &mut remotes[k],
                        &mut clients[k],
                        &server,
                        &caches[k],
                        &stores[k],
                        &mut changed,
                    );
                }
                FaultStep::DenyAll => {
                    server.deny_all();
                }
            }
        }

        // Drain: keep syncing until a full round reconciles nothing.
        for _ in 0..=(n * 4) {
            let mut any = false;
            for k in 0..n {
                let mut changed = false;
                sync_fault(
                    &mut remotes[k],
                    &mut clients[k],
                    &server,
                    &caches[k],
                    &stores[k],
                    &mut changed,
                );
                any |= changed;
            }
            if !any {
                break;
            }
        }

        // Full convergence: every client agrees with every other and
        // with the server.
        let first = texts(&stores[0].snapshot());
        for (k, store) in stores.iter().enumerate().skip(1) {
            assert_eq!(
                texts(&store.snapshot()),
                first,
                "client {k} diverges from client 0"
            );
        }
        let server_texts: Vec<String> = server
            .inner
            .model("doc")
            .expect("seeded model")["blocks"]
            .as_array()
            .expect("single-list array")
            .iter()
            .filter(|node| node["alive"].as_bool().unwrap())
            .map(|node| node["value"]["text"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(first, server_texts, "clients diverge from the server");

        // Structural invariants after faults: every node carries a
        // liveness marker (deleted elements stay as tombstones with
        // their slots), and element identities stay unique (a
        // re-sent batch must never double-apply).
        let server_blocks = server
            .inner
            .model("doc")
            .expect("seeded model")["blocks"]
            .as_array()
            .expect("single-list array")
            .clone();
        assert!(
            server_blocks.iter().all(|node| node["alive"].is_boolean()),
            "every node carries a liveness marker"
        );
        let mut ids: Vec<_> = server_blocks.iter().map(|node| node["id"].clone()).collect();
        ids.sort_by_key(|a| a.to_string());
        for pair in ids.windows(2) {
            assert_ne!(pair[0], pair[1], "duplicate element id on the server");
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// Rich text: `CrdtString<Style>` end to end

/// A run-level style attribute: an application-defined model (any
/// muon observable/tracked type works as the style value).
#[derive(Serialize, Deserialize, Observe, Clone, Debug, PartialEq, Track)]
struct Style {
    bold: bool,
}

#[derive(Serialize, Deserialize, Observe, Clone, Debug, PartialEq, Track)]
struct RichDoc {
    text: CrdtString<Style>,
}

fn rich_make_store() -> Store<RichDoc> {
    Store::new(RichDoc {
        text: CrdtString::new(),
    })
}

fn rich_server() -> TestServer {
    let s = TestServer::new();
    s.seed("doc", serde_json::json!({ "text": [] }));
    s
}

fn rich_client(client_id: u64) -> (Store<RichDoc>, SyncClient, SyncChannel, MemCache) {
    let store = rich_make_store();
    let client = SyncClient::new(client_id);
    let channel = client.channel("doc");
    (store, client, channel, MemCache::new())
}

/// One user-level rich-text operation in character offsets.
#[derive(Debug, Clone)]
enum RichOp {
    Insert { pos: usize, s: String, bold: bool },
    Delete { pos: usize, len: usize },
    ToggleBold { pos: usize, len: usize },
}

impl Arbitrary for RichOp {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            (any::<usize>(), any::<bool>()).prop_flat_map(|(p, bold)| {
                prop::sample::select(&["a", "abc", "中", "中文", "😀", "e\u{301}", "héllo"])
                    .prop_map(move |s| RichOp::Insert {
                        pos: p,
                        s: s.to_string(),
                        bold,
                    })
                    .boxed()
            }),
            (any::<usize>(), any::<usize>()).prop_map(|(p, l)| RichOp::Delete { pos: p, len: l }),
            (any::<usize>(), any::<usize>())
                .prop_map(|(p, l)| RichOp::ToggleBold { pos: p, len: l }),
        ]
        .boxed()
    }
}

/// Apply one rich-text op through a full sync write on the client's
/// store. Indices normalize against the current live view; a no-op op
/// records nothing.
fn apply_rich_op(channel: &SyncChannel, store: &Store<RichDoc>, op: RichOp) {
    match op {
        RichOp::Insert { pos, s, bold } => {
            let _ = channel.sync_write(track!(store, |doc| {
                let n = doc.text.len();
                let p = pos % (n + 1);
                doc.text.insert(p, &s);
                if bold && !s.is_empty() {
                    doc.text
                        .annotate(p..p + s.chars().count(), Style { bold: true });
                }
            }));
        }
        RichOp::Delete { pos, len } => {
            let _ = channel.sync_write(track!(store, |doc| {
                let n = doc.text.len();
                if n > 0 {
                    let p = pos % n;
                    let l = len % (n - p + 1);
                    if l > 0 {
                        doc.text.delete(p..p + l);
                    }
                }
            }));
        }
        RichOp::ToggleBold { pos, len } => {
            let _ = channel.sync_write(track!(store, |doc| {
                let n = doc.text.len();
                if n > 0 {
                    let p = pos % n;
                    let l = len % (n - p + 1);
                    if l > 0 {
                        let bold = doc.text.styles_at(p).iter().any(|s| s.bold);
                        if bold {
                            doc.text.unmark(p..p + l, Style { bold: true });
                        } else {
                            doc.text.annotate(p..p + l, Style { bold: true });
                        }
                    }
                }
            }));
        }
    }
}

/// The visible rich text: `(character, bold)` in position order.
fn rich_texts(doc: &RichDoc) -> Vec<(char, bool)> {
    doc.text
        .spans()
        .iter()
        .flat_map(|(s, styles)| {
            let bold = styles.iter().any(|st| st.bold);
            s.chars().map(move |c| (c, bold)).collect::<Vec<_>>()
        })
        .collect()
}

/// The server's wire text as `(character, bold)` pairs: the field is
/// the container's node array, rebuilt through the container for
/// comparison.
fn server_rich_texts(server: &TestServer) -> Vec<(char, bool)> {
    let nodes = server.model("doc").expect("seeded model")["text"]
        .as_array()
        .expect("text field is a node array")
        .clone();
    let text: CrdtString<Style> =
        serde_json::from_value(Value::Array(nodes)).expect("node array decodes");
    text.spans()
        .iter()
        .flat_map(|(s, styles)| {
            let bold = styles.iter().any(|st| st.bold);
            s.chars().map(move |c| (c, bold)).collect::<Vec<_>>()
        })
        .collect()
}

proptest! {
    // Each case is a full pipeline with several sync steps; the case
    // count stays small.
    #![proptest_config(Config::with_cases(24))]

    #[test]
    fn rich_text_fault_injection_converges(
        op_streams in prop::collection::vec(
            prop::collection::vec(any::<RichOp>(), 0..=10),
            2usize..=4,
        ),
        schedule in prop::collection::vec(any::<FaultStep>(), 0..=40),
    ) {
        let n = op_streams.len();
        let server = FaultyServer::new(json!({ "text": [] }), "text");

        let mut stores = Vec::new();
        let mut clients = Vec::new();
        let mut channels = Vec::new();
        let mut caches = Vec::new();
        let mut remotes = Vec::new();
        for i in 0..n {
            let (store, client, channel, cache) = rich_client(i as u64 + 1);
            stores.push(store);
            clients.push(client);
            channels.push(channel);
            caches.push(cache);
            remotes.push(RemoteView::new());
        }

        let mut cursor = vec![0usize; n];
        for step in &schedule {
            match *step {
                FaultStep::Op(k) => {
                    let k = k % n;
                    let stream = &op_streams[k];
                    if cursor[k] < stream.len() {
                        apply_rich_op(&channels[k], &stores[k], stream[cursor[k]].clone());
                        cursor[k] += 1;
                    }
                }
                FaultStep::Sync(k) => {
                    let k = k % n;
                    sync(&mut remotes[k], &mut clients[k], &server.inner, &caches[k], &stores[k]);
                }
                FaultStep::FailPoll(k) => {
                    let k = k % n;
                    server.fail_next_poll();
                    let mut changed = false;
                    sync_fault(&mut remotes[k], &mut clients[k], &server, &caches[k], &stores[k], &mut changed);
                }
                FaultStep::StalePoll(k) => {
                    let k = k % n;
                    server.stale_next_poll();
                    let mut changed = false;
                    sync_fault(&mut remotes[k], &mut clients[k], &server, &caches[k], &stores[k], &mut changed);
                }
                FaultStep::DenyAll => {
                    server.deny_all();
                }
            }
        }

        // Drain: keep syncing every client until a full round
        // reconciles nothing anywhere.
        for _ in 0..=(n * 4) {
            let mut any = false;
            for k in 0..n {
                let mut changed = false;
                sync_flag(
                    &mut remotes[k],
                    &mut clients[k],
                    &server.inner,
                    &caches[k],
                    &stores[k],
                    &mut changed,
                );
                any |= changed;
            }
            if !any {
                break;
            }
        }

        // Full convergence after faults.
        let first = rich_texts(&stores[0].snapshot());
        for (k, store) in stores.iter().enumerate().skip(1) {
            assert_eq!(
                rich_texts(&store.snapshot()),
                first,
                "client {k} diverges from client 0"
            );
        }
        assert_eq!(
            first,
            server_rich_texts(&server.inner),
            "clients diverge from the server"
        );
    }
}

/// A text edit undo/redo round trip through the full pipeline: the
/// undo inverse restores the transaction's start snapshot (a
/// whole-field replace), and the redo replays the original
/// transaction (the container re-imports its delta).
#[test]
fn text_undo_restores_snapshot_and_redo_replays() {
    let server = rich_server();
    let (store, mut client, channel, cache) = rich_client(1);
    let mut remote = RemoteView::new();
    let mut undo = UndoStack::new(0);

    // Edit: insert "hello".
    let out1 = channel
        .sync_write(track!(&store, |doc| {
            doc.text.insert(0, "hello");
        }))
        .unwrap();
    undo.record(&out1.commit);
    sync(&mut remote, &mut client, &server, &cache, &store);
    assert_eq!(store.snapshot().text.text(), "hello");

    // Edit: insert "!".
    let out2 = channel
        .sync_write(track!(&store, |doc| {
            doc.text.insert(5, "!");
        }))
        .unwrap();
    undo.record(&out2.commit);
    sync(&mut remote, &mut client, &server, &cache, &store);
    assert_eq!(store.snapshot().text.text(), "hello!");

    // Undo the second edit: the snapshot restore propagates through
    // the server and back.
    let inverses = undo.undo(client.driver());
    assert_eq!(inverses.as_ref().map(Vec::len), Some(1));
    sync(&mut remote, &mut client, &server, &cache, &store);
    assert_eq!(store.snapshot().text.text(), "hello");

    // Redo: the original transaction replays.
    let redone = undo.redo(client.driver());
    assert_eq!(redone.as_ref().map(Vec::len), Some(1));
    sync(&mut remote, &mut client, &server, &cache, &store);
    assert_eq!(store.snapshot().text.text(), "hello!");
}
