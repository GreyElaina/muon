//! A Notion-like document model exercised through the full sync
//! pipeline on multiple ends.
//!
//! The model combines every container mechanism in one place:
//! a plain field (`title` — `Replace`), a rich-text field
//! (`body` — `Edit` over `Segment` anchors) and a block sequence
//! (`blocks` — atomic blocks with identity-addressed edits). The
//! multi-end scenarios cover concurrent edits, reordering, style
//! synthesis, undo propagation, rejection recovery and snapshot
//! joins.

mod common;

use common::TestServer;
use muon::Observe;
use muon_store::{track, Store, Track};
use muon_sync::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{Arc, Mutex};

/// A block's role.
#[derive(Serialize, Deserialize, Observe, Clone, Debug, PartialEq, Track)]
enum BlockKind {
    Title,
    Text,
    Todo,
    List,
}

/// A block: LWW fields (`kind`/`label`/`done`) plus CRDT fields
/// (`body` rich text, `children` self-recursive nested sequence).
/// Element access addresses both kinds: field edits are field-level
/// LWW replaces, container operations are identity-addressed edits.
///
/// `children` recurses directly over `Block`; the derive closes the
/// observer type over itself by keeping concrete fields out of the
/// generated `SerializeObserver` bounds.
#[derive(Serialize, Deserialize, Observe, Clone, Debug, PartialEq, Track)]
struct Block {
    kind: BlockKind,
    label: String,
    done: bool,
    body: CrdtString<Style>,
    children: CrdtVec<Block>,
}

/// A run-level style attribute.
#[derive(Serialize, Deserialize, Observe, Clone, Debug, PartialEq, Track)]
struct Style {
    bold: bool,
}

/// A Notion-like page.
#[derive(Serialize, Deserialize, Observe, Clone, Debug, PartialEq, Track)]
struct Page {
    title: String,
    body: CrdtString<Style>,
    blocks: CrdtVec<Block>,
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

fn client(client_id: u64) -> (Store<Page>, SyncClient, SyncChannel, MemCache) {
    client_with(
        client_id,
        Page {
            title: "Untitled".into(),
            body: CrdtString::new(),
            blocks: CrdtVec::new(),
        },
    )
}

fn client_with<D: Serialize + for<'de> Deserialize<'de> + 'static>(
    client_id: u64,
    initial: D,
) -> (Store<D>, SyncClient, SyncChannel, MemCache) {
    let store = Store::new(initial);
    let client = SyncClient::new(client_id);
    let channel = client.channel("doc");
    (store, client, channel, MemCache::new())
}

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

fn block(kind: BlockKind, label: &str) -> Block {
    Block {
        kind,
        label: label.into(),
        done: false,
        body: CrdtString::new(),
        children: CrdtVec::new(),
    }
}

/// The page's visible block labels.
fn labels(doc: &Page) -> Vec<&str> {
    doc.blocks.iter().map(|b| b.label.as_str()).collect()
}

/// The labels of one block's children.
fn child_labels(doc: &Page, index: usize) -> Vec<&str> {
    doc.blocks
        .get(index)
        .map(|b| b.children.iter().map(|c| c.label.as_str()).collect())
        .unwrap_or_default()
}

/// A block's rich text.
fn block_text(doc: &Page, index: usize) -> String {
    doc.blocks
        .get(index)
        .map(|b| b.body.text())
        .unwrap_or_default()
}

/// The page's visible rich text.
fn text(doc: &Page) -> String {
    doc.body.text()
}

/// The bold runs of the page's rich text: `(char, bold)` per char.
fn bold_runs(doc: &Page) -> Vec<(char, bool)> {
    doc.body
        .spans()
        .iter()
        .flat_map(|(s, styles)| {
            let bold = styles.iter().any(|st| st.bold);
            s.chars().map(move |c| (c, bold)).collect::<Vec<_>>()
        })
        .collect()
}

// ── Tests ────────────────────────────────────────────────────────────

/// Two ends push blocks at different positions concurrently: the
/// server merges in receive order, both ends converge with distinct
/// identities.
#[test]
fn concurrent_block_inserts_converge() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    // Shared baseline: one heading block.
    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks.push(block(BlockKind::Title, "Plan"));
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // Concurrent: A appends a todo, B inserts a text block at the head.
    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks.push(block(BlockKind::Todo, "Ship it"));
        }))
        .unwrap();
    channel_b
        .sync_write(track!(&store_b, |d| {
            d.blocks.insert(0, block(BlockKind::Text, "Intro"));
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    let expected = vec!["Intro", "Plan", "Ship it"];
    assert_eq!(labels(&store_a.snapshot()), expected);
    assert_eq!(labels(&store_b.snapshot()), expected);
    assert_eq!(store_a.snapshot(), store_b.snapshot());
}

/// A reorder on one end races a rich-text edit on the other: the
/// sequence move and the text edit are independent mechanisms and
/// both converge.
#[test]
fn concurrent_reorder_and_rich_text_edit_converge() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    for (kind, label) in [
        (BlockKind::Title, "A"),
        (BlockKind::Text, "B"),
        (BlockKind::Text, "C"),
    ] {
        channel_a
            .sync_write(track!(&store_a, |d| {
                d.blocks.push(block(kind, label));
            }))
            .unwrap();
        sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    }
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // Concurrent: A moves the last block to the head; B edits the
    // page's rich text.
    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks.move_to(2, 0);
        }))
        .unwrap();
    channel_b
        .sync_write(track!(&store_b, |d| {
            d.body.insert(0, "Welcome");
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    let expected = vec!["C", "A", "B"];
    assert_eq!(labels(&store_a.snapshot()), expected);
    assert_eq!(labels(&store_b.snapshot()), expected);
    assert_eq!(text(&store_a.snapshot()), "Welcome");
    assert_eq!(text(&store_b.snapshot()), "Welcome");
    assert_eq!(store_a.snapshot(), store_b.snapshot());
}

/// Two ends edit the rich text concurrently from the same baseline:
/// character inserts and deletes merge deterministically.
#[test]
fn concurrent_rich_text_edits_converge() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    channel_a
        .sync_write(track!(&store_a, |d| {
            d.body.insert(0, "ac");
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // Concurrent from the same baseline: A inserts between, B
    // appends.
    channel_a
        .sync_write(track!(&store_a, |d| {
            d.body.insert(1, "b");
        }))
        .unwrap();
    channel_b
        .sync_write(track!(&store_b, |d| {
            d.body.insert(2, "d");
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    assert_eq!(text(&store_a.snapshot()), "abcd");
    assert_eq!(text(&store_b.snapshot()), "abcd");
    assert_eq!(store_a.snapshot(), store_b.snapshot());
}

/// Two ends style overlapping ranges concurrently: intervals union in
/// the synthesis, both ends converge on the same runs.
#[test]
fn concurrent_styles_union() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    channel_a
        .sync_write(track!(&store_a, |d| {
            d.body.insert(0, "abcdef");
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // Concurrent: A bolds [1..5), B unmarks its middle [2..4).
    channel_a
        .sync_write(track!(&store_a, |d| {
            d.body.annotate(1..5, Style { bold: true });
        }))
        .unwrap();
    channel_b
        .sync_write(track!(&store_b, |d| {
            d.body.unmark(2..4, Style { bold: true });
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // The clear interval suppresses the style inside it; the anchor
    // intervals are independent of receive order.
    let runs_a = bold_runs(&store_a.snapshot());
    let runs_b = bold_runs(&store_b.snapshot());
    assert_eq!(runs_a, runs_b, "style synthesis converges");
    assert_eq!(
        runs_a,
        vec![
            ('a', false),
            ('b', true),
            ('c', false),
            ('d', false),
            ('e', true),
            ('f', false),
        ],
    );
}

/// The same style scenario with the opposite receive order: B's
/// unmark reaches the server first. The clear interval still
/// suppresses the style inside it, and both ends converge on the
/// same runs — pinning the receive-order independence claimed by
/// [`concurrent_styles_union`].
#[test]
fn concurrent_styles_union_reversed_sync_order() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    channel_a
        .sync_write(track!(&store_a, |d| {
            d.body.insert(0, "abcdef");
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // Concurrent: A bolds [1..5), B unmarks its middle [2..4).
    channel_a
        .sync_write(track!(&store_a, |d| {
            d.body.annotate(1..5, Style { bold: true });
        }))
        .unwrap();
    channel_b
        .sync_write(track!(&store_b, |d| {
            d.body.unmark(2..4, Style { bold: true });
        }))
        .unwrap();
    // Reversed receive order: B's unmark reaches the server first.
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);

    let runs_a = bold_runs(&store_a.snapshot());
    let runs_b = bold_runs(&store_b.snapshot());
    assert_eq!(runs_a, runs_b, "style synthesis converges");
    assert_eq!(
        runs_a,
        vec![
            ('a', false),
            ('b', true),
            ('c', false),
            ('d', false),
            ('e', true),
            ('f', false),
        ],
    );
}

/// Concurrent whole-field assignments to the title: the server's
/// receive order decides (last-writer-wins), both ends converge.
#[test]
fn title_replace_lww() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    channel_a
        .sync_write(track!(&store_a, |d| {
            d.title = "From A".into();
        }))
        .unwrap();
    channel_b
        .sync_write(track!(&store_b, |d| {
            d.title = "From B".into();
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // Receive order: A first, then B — B's write wins on both ends.
    assert_eq!(store_a.snapshot().title, "From B");
    assert_eq!(store_b.snapshot().title, "From B");
}

/// Undoing a block insert on one end propagates: the inverse edit
/// (delete) travels through the server and both ends converge back
/// to the pre-insert state.
#[test]
fn undo_block_insert_propagates() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, _channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();
    let mut undo = UndoStack::new(0);

    let out = channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks.push(block(BlockKind::Todo, "Draft"));
        }))
        .unwrap();
    undo.record(&out.commit);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    assert_eq!(labels(&store_a.snapshot()), vec!["Draft"]);

    // Undo: the insert's inverse (a delete over the created run)
    // propagates to both ends.
    undo.undo(client_a.driver());
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    assert_eq!(labels(&store_a.snapshot()), Vec::<&str>::new());
    assert_eq!(labels(&store_b.snapshot()), Vec::<&str>::new());
}

/// A rich-text edit undo/redo round trip propagates through the
/// pipeline: undo restores the pre-edit text, redo replays it.
#[test]
fn undo_text_edit_propagates() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, _channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();
    let mut undo = UndoStack::new(0);

    let out = channel_a
        .sync_write(track!(&store_a, |d| {
            d.body.insert(0, "hello");
        }))
        .unwrap();
    undo.record(&out.commit);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    assert_eq!(text(&store_a.snapshot()), "hello");

    undo.undo(client_a.driver());
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    assert_eq!(text(&store_a.snapshot()), "");
    assert_eq!(text(&store_b.snapshot()), "");

    undo.redo(client_a.driver());
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    assert_eq!(text(&store_a.snapshot()), "hello");
    assert_eq!(text(&store_b.snapshot()), "hello");
}

/// The server rejects title writes: the write disappears from the
/// authoritative state, and a full sync reconciles both ends back to
/// the accepted value.
#[test]
fn field_rejection_recovers() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    server.deny_field("title");
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, _channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    // The denied write never lands; the local value must reconcile
    // back to the accepted one.
    channel_a
        .sync_write(track!(&store_a, |d| {
            d.title = "Rejected".into();
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    assert_eq!(store_a.snapshot().title, "Page");

    // The other end agrees after a sync.
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    assert_eq!(store_b.snapshot().title, "Page");
    assert_eq!(store_a.snapshot(), store_b.snapshot());
}

/// A new end joins from the server's snapshot: after one sync it
/// holds the same state as the existing ends.
#[test]
fn new_end_joins_from_snapshot() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let mut remote_a = RemoteView::new();

    channel_a
        .sync_write(track!(&store_a, |d| {
            d.title = "Board".into();
            d.body.insert(0, "Notes");
            d.body.annotate(0..5, Style { bold: true });
            d.blocks.push(block(BlockKind::Todo, "Task 1"));
            d.blocks.push(block(BlockKind::Todo, "Task 2"));
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);

    // A fresh end with an empty store joins and catches up.
    let (store_c, mut client_c, _, cache_c) = client(3);
    let mut remote_c = RemoteView::new();
    sync(&mut remote_c, &mut client_c, &server, &cache_c, &store_c);

    assert_eq!(store_c.snapshot(), store_a.snapshot());
    assert_eq!(labels(&store_c.snapshot()), vec!["Task 1", "Task 2"]);
    assert_eq!(text(&store_c.snapshot()), "Notes");
    assert_eq!(bold_runs(&store_c.snapshot())[0], ('N', true));
}

// ── Element access ────────────────────────────────────────────────────

/// Two ends edit different fields of the same block concurrently:
/// field-level LWW keeps both edits (the element is not replaced
/// wholesale).
#[test]
fn concurrent_block_field_edits_converge() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks.push(block(BlockKind::Todo, "Task"));
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // Concurrent: A rewrites the label, B ticks the checkbox.
    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks[0].label = "Renamed".into();
        }))
        .unwrap();
    channel_b
        .sync_write(track!(&store_b, |d| {
            d.blocks[0].done = true;
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    let snap = store_a.snapshot();
    assert_eq!(snap.blocks[0].label, "Renamed");
    assert!(
        snap.blocks[0].done,
        "the field edit survives the label edit"
    );
    assert_eq!(store_a.snapshot(), store_b.snapshot());
}

/// Two ends edit the same field concurrently: last-writer-wins at
/// field granularity, both ends converge.
#[test]
fn concurrent_same_field_edit_lww() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks.push(block(BlockKind::Todo, "Task"));
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks[0].label = "From A".into();
        }))
        .unwrap();
    channel_b
        .sync_write(track!(&store_b, |d| {
            d.blocks[0].label = "From B".into();
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    assert_eq!(store_a.snapshot().blocks[0].label, "From B");
    assert_eq!(store_a.snapshot(), store_b.snapshot());
}

/// Two ends edit the rich text inside the same block concurrently:
/// the element's CRDT field merges like a top-level text field.
#[test]
fn concurrent_block_rich_text_edits_converge() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks.push(block(BlockKind::Text, "Note"));
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks[0].body.insert(0, "ac");
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // Concurrent inserts into the block's body, from the same baseline.
    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks[0].body.insert(1, "b");
        }))
        .unwrap();
    channel_b
        .sync_write(track!(&store_b, |d| {
            d.blocks[0].body.insert(2, "d");
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    assert_eq!(block_text(&store_a.snapshot(), 0), "abcd");
    assert_eq!(store_a.snapshot(), store_b.snapshot());
}

/// Nested children: two ends push into the same block's children
/// concurrently; the nested sequence merges by identity.
#[test]
fn nested_children_ops_converge() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks.push(block(BlockKind::List, "Parent"));
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // Concurrent pushes into the same parent's children.
    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks[0].children.push(block(BlockKind::Todo, "Child A"));
        }))
        .unwrap();
    channel_b
        .sync_write(track!(&store_b, |d| {
            d.blocks[0].children.push(block(BlockKind::Todo, "Child B"));
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    let snap = store_a.snapshot();
    let mut labels = child_labels(&snap, 0);
    labels.sort();
    assert_eq!(labels, vec!["Child A", "Child B"]);
    assert_eq!(store_a.snapshot(), store_b.snapshot());
}

/// A field edit on a nested child propagates through two levels of
/// element addressing.
#[test]
fn nested_field_edit_converges() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, _channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks.push(block(BlockKind::List, "Parent"));
            d.blocks[0].children.push(block(BlockKind::Todo, "Child"));
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks[0].children[0].done = true;
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    let snap = store_a.snapshot();
    assert!(snap.blocks[0].children[0].done);
    assert_eq!(store_a.snapshot(), store_b.snapshot());
}

/// `set` replaces an element wholesale (per-element LWW): a concurrent
/// field edit is either kept or overwritten by receive order, never
/// split.
#[test]
fn set_block_replaces_value_lww() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks.push(block(BlockKind::Todo, "Task"));
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // Concurrent: A replaces the element wholesale, B rewrites its
    // label. A's replacement lands first in receive order; B's field
    // edit is applied on top of the new element value, so the label
    // edit survives whole and the replacement's untouched fields win.
    let mut replacement = block(BlockKind::Todo, "Replaced");
    replacement.done = true;
    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks.set(0, replacement.clone());
        }))
        .unwrap();
    channel_b
        .sync_write(track!(&store_b, |d| {
            d.blocks[0].label = "Edited".into();
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    let snap = store_a.snapshot();
    assert_eq!(
        snap.blocks[0].label, "Edited",
        "the later field edit is kept"
    );
    assert!(
        snap.blocks[0].done,
        "the replacement's untouched fields win"
    );
    assert_eq!(store_a.snapshot(), store_b.snapshot());
}

/// Undoing a field edit propagates: the inverse replace travels
/// through the server, both ends restore the pre-edit value.
#[test]
fn undo_field_edit_propagates() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, _channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();
    let mut undo = UndoStack::new(0);

    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks.push(block(BlockKind::Todo, "Task"));
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    let out = channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks[0].label = "Edited".into();
        }))
        .unwrap();
    undo.record(&out.commit);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    assert_eq!(store_a.snapshot().blocks[0].label, "Edited");

    undo.undo(client_a.driver());
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    assert_eq!(store_a.snapshot().blocks[0].label, "Task");
    assert_eq!(store_b.snapshot().blocks[0].label, "Task");
}

/// A's undo of a block insert races B's concurrent insert into the
/// same sequence: the undo's delete is identity-addressed, so it
/// tombstones only its own run and leaves the concurrent element
/// in place.
#[test]
fn undo_insert_races_concurrent_insert() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();
    let mut undo = UndoStack::new(0);

    let out = channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks.push(block(BlockKind::Todo, "Draft"));
        }))
        .unwrap();
    undo.record(&out.commit);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // Concurrent: A undoes the insert (a delete over its run), B
    // appends after the same baseline element.
    undo.undo(client_a.driver());
    channel_b
        .sync_write(track!(&store_b, |d| {
            d.blocks.push(block(BlockKind::Todo, "Mine"));
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    assert_eq!(labels(&store_a.snapshot()), vec!["Mine"]);
    assert_eq!(labels(&store_b.snapshot()), vec!["Mine"]);
    assert_eq!(store_a.snapshot(), store_b.snapshot());
}

/// A's undo of a field edit races B's concurrent edit of the same
/// field: the undo is itself a replace, so last-writer-wins decides
/// by receive order.
#[test]
fn undo_field_edit_races_concurrent_same_field_edit() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();
    let mut undo = UndoStack::new(0);

    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks.push(block(BlockKind::Todo, "Task"));
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    let out = channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks[0].label = "From A".into();
        }))
        .unwrap();
    undo.record(&out.commit);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    assert_eq!(store_a.snapshot().blocks[0].label, "From A");

    // Concurrent: A undoes its label edit (writes "Task" back), B
    // rewrites the same field. A's undo reaches the server first;
    // B's write lands after it and wins by LWW.
    undo.undo(client_a.driver());
    channel_b
        .sync_write(track!(&store_b, |d| {
            d.blocks[0].label = "From B".into();
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    assert_eq!(store_a.snapshot().blocks[0].label, "From B");
    assert_eq!(store_b.snapshot().blocks[0].label, "From B");
    assert_eq!(store_a.snapshot(), store_b.snapshot());
}

/// A's undo of a field edit races B's concurrent edit of a different
/// field on the same element: the undo's replace touches only its own
/// field, so B's edit survives.
#[test]
fn undo_field_edit_keeps_concurrent_other_field_edit() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();
    let mut undo = UndoStack::new(0);

    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks.push(block(BlockKind::Todo, "Task"));
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    let out = channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks[0].label = "From A".into();
        }))
        .unwrap();
    undo.record(&out.commit);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // Concurrent: A undoes its label edit, B ticks the checkbox.
    undo.undo(client_a.driver());
    channel_b
        .sync_write(track!(&store_b, |d| {
            d.blocks[0].done = true;
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    let snap = store_a.snapshot();
    assert_eq!(
        snap.blocks[0].label, "Task",
        "the undo restores its own field"
    );
    assert!(
        snap.blocks[0].done,
        "the concurrent other-field edit survives"
    );
    assert_eq!(store_a.snapshot(), store_b.snapshot());
}

/// A's redo of a block insert races B's concurrent insert into the
/// now-empty sequence: the redo re-inserts with fresh identities (a
/// tombstoned id is never resurrected), so both elements live and
/// converge in the same order on both ends.
#[test]
fn redo_insert_races_concurrent_insert() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();
    let mut undo = UndoStack::new(0);

    let out = channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks.push(block(BlockKind::Todo, "Draft"));
        }))
        .unwrap();
    undo.record(&out.commit);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    undo.undo(client_a.driver());
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    assert!(store_a.snapshot().blocks.is_empty());

    // Concurrent: A redoes the insert (fresh identities), B pushes
    // into the empty sequence from the same baseline.
    undo.redo(client_a.driver());
    channel_b
        .sync_write(track!(&store_b, |d| {
            d.blocks.push(block(BlockKind::Todo, "Mine"));
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    let snap = store_a.snapshot();
    let mut both = labels(&snap);
    both.sort();
    assert_eq!(both, vec!["Draft", "Mine"]);
    assert_eq!(store_a.snapshot(), store_b.snapshot());
}

/// One observation body mixes element edits, nested container ops and
/// structural ops; the stream carries all of them and converges.
#[test]
fn mixed_element_and_container_ops_converge() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, _channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks.push(block(BlockKind::List, "Parent"));
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // One body: field edit + child push + structural insert.
    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks[0].label = "Parent!".into();
            d.blocks[0].children.push(block(BlockKind::Todo, "Child"));
            d.blocks.insert(1, block(BlockKind::Text, "Sibling"));
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    let snap = store_a.snapshot();
    assert_eq!(snap.blocks[0].label, "Parent!");
    assert_eq!(snap.blocks[0].children[0].label, "Child");
    assert_eq!(labels(&snap), vec!["Parent!", "Sibling"]);
    assert_eq!(store_a.snapshot(), store_b.snapshot());
}

/// Self-recursive nesting: a block's children are blocks again. A field
/// edit three levels deep propagates through identity addressing, and
/// both ends converge on the same nested structure.
#[test]
fn recursive_children_three_levels_converge() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    // Level 1: root block with a level-2 child that has a level-3 child.
    channel_a
        .sync_write(track!(&store_a, |d| {
            let mut l3 = block(BlockKind::Todo, "Leaf");
            l3.children.push(block(BlockKind::Todo, "Deepest"));
            let mut l2 = block(BlockKind::List, "Mid");
            l2.children.push(l3);
            let mut root = block(BlockKind::List, "Root");
            root.children.push(l2);
            d.blocks.push(root);
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // Both ends edit different levels of the nested tree concurrently.
    channel_a
        .sync_write(track!(&store_a, |d| {
            d.blocks[0].children[0].children[0].label = "Leaf edited".into();
        }))
        .unwrap();
    channel_b
        .sync_write(track!(&store_b, |d| {
            d.blocks[0].children[0].done = true;
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    let snap = store_a.snapshot();
    assert_eq!(snap.blocks[0].children[0].children[0].label, "Leaf edited");
    assert!(snap.blocks[0].children[0].done);
    assert_eq!(store_a.snapshot(), store_b.snapshot());
}

/// Recursive undo: undoing a nested insert removes the whole subtree,
/// and redo restores it — including the nested element identities.
#[test]
fn recursive_undo_restores_subtree() {
    let server = TestServer::new();
    server.seed(
        "doc",
        serde_json::json!({ "title": "Page", "body": [], "blocks": [] }),
    );
    let (store_a, mut client_a, channel_a, cache_a) = client(1);
    let (store_b, mut client_b, _channel_b, cache_b) = client(2);
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();
    let mut undo = UndoStack::new(0);

    let out = channel_a
        .sync_write(track!(&store_a, |d| {
            let mut child = block(BlockKind::Todo, "Child");
            child.children.push(block(BlockKind::Todo, "Grandchild"));
            let mut root = block(BlockKind::List, "Root");
            root.children.push(child);
            d.blocks.push(root);
        }))
        .unwrap();
    undo.record(&out.commit);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    assert_eq!(
        store_a.snapshot().blocks[0].children[0].children[0].label,
        "Grandchild"
    );

    // Undo the root insert: the whole subtree is removed, and redo
    // restores it — including the nested element identities.
    undo.undo(client_a.driver());
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    assert!(store_a.snapshot().blocks.is_empty());

    undo.redo(client_a.driver());
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    let snap = store_a.snapshot();
    assert_eq!(snap.blocks[0].label, "Root");
    assert_eq!(snap.blocks[0].children[0].label, "Child");
    assert_eq!(snap.blocks[0].children[0].children[0].label, "Grandchild");
    assert_eq!(store_a.snapshot(), store_b.snapshot());
}

// ─────────────────────────────────────────────────────────────────────
// Recursive model validation
// ─────────────────────────────────────────────────────────────────────

/// A self-recursive enum element: branches carry nested children.
#[derive(Serialize, Deserialize, Observe, Clone, Debug, PartialEq, Track)]
enum Node {
    Leaf { label: String },
    Branch { children: CrdtVec<Node> },
}

/// Top-level document for the recursive enum model.
#[derive(Serialize, Deserialize, Observe, Clone, Debug, PartialEq, Track)]
struct RecDoc {
    nodes: CrdtVec<Node>,
}

/// Top-level document for the mutually recursive model.
#[derive(Serialize, Deserialize, Observe, Clone, Debug, PartialEq, Track)]
struct MutDoc {
    alphas: CrdtVec<Alpha>,
}

/// A mutually recursive pair: alphas contain betas which contain alphas.
#[derive(Serialize, Deserialize, Observe, Clone, Debug, PartialEq, Track)]
struct Alpha {
    name: String,
    betas: CrdtVec<Beta>,
}

#[derive(Serialize, Deserialize, Observe, Clone, Debug, PartialEq, Track)]
struct Beta {
    name: String,
    alphas: CrdtVec<Alpha>,
}

/// Recursive enum: nested branch insertion converges across ends, and
/// undo removes the whole nested subtree.
#[test]
fn recursive_enum_branch_converges() {
    let server = TestServer::new();
    server.seed("doc", serde_json::json!({ "nodes": [] }));
    let (store_a, mut client_a, channel_a, cache_a) = client_with(
        1,
        RecDoc {
            nodes: CrdtVec::new(),
        },
    );
    let (store_b, mut client_b, _channel_b, cache_b) = client_with(
        2,
        RecDoc {
            nodes: CrdtVec::new(),
        },
    );
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();
    let mut undo = UndoStack::new(0);

    // A branch whose children contain a nested leaf branch.
    let make_tree = || Node::Branch {
        children: {
            let mut c = CrdtVec::new();
            c.push(Node::Branch {
                children: {
                    let mut c2 = CrdtVec::new();
                    c2.push(Node::Leaf {
                        label: "deep".into(),
                    });
                    c2
                },
            });
            c
        },
    };
    let out = channel_a
        .sync_write(track!(&store_a, |d| {
            d.nodes.push(make_tree());
        }))
        .unwrap();
    undo.record(&out.commit);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // The full nested tree: branch -> branch -> leaf. The assertion
    // checks every level, so a partial restore cannot pass.
    let assert_tree = |doc: &RecDoc| {
        assert!(matches!(
            &doc.nodes[0],
            Node::Branch { children } if matches!(
                &children[0],
                Node::Branch { children } if matches!(
                    &children[0],
                    Node::Leaf { label } if label == "deep"
                )
            )
        ));
    };
    assert_tree(&store_a.snapshot());
    assert_eq!(store_a.snapshot(), store_b.snapshot());

    // Undo removes the entire nested subtree; redo restores it.
    undo.undo(client_a.driver());
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    assert!(store_a.snapshot().nodes.is_empty());
    assert_eq!(store_a.snapshot(), store_b.snapshot());

    undo.redo(client_a.driver());
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    assert_tree(&store_a.snapshot());
    assert_eq!(store_a.snapshot(), store_b.snapshot());
}

/// Mutually recursive structs: a field-level op three levels deep
/// (alpha -> beta -> alpha) propagates through identity addressing, and
/// concurrent ops on both ends converge.
#[test]
fn mutually_recursive_field_ops_converge() {
    let server = TestServer::new();
    server.seed("doc", serde_json::json!({ "alphas": [] }));
    let (store_a, mut client_a, channel_a, cache_a) = client_with(
        1,
        MutDoc {
            alphas: CrdtVec::new(),
        },
    );
    let (store_b, mut client_b, channel_b, cache_b) = client_with(
        2,
        MutDoc {
            alphas: CrdtVec::new(),
        },
    );
    let mut remote_a = RemoteView::new();
    let mut remote_b = RemoteView::new();

    channel_a
        .sync_write(track!(&store_a, |d| {
            let mut b = Beta {
                name: "beta".into(),
                alphas: CrdtVec::new(),
            };
            b.alphas.push(Alpha {
                name: "inner".into(),
                betas: CrdtVec::new(),
            });
            let mut a = Alpha {
                name: "alpha".into(),
                betas: CrdtVec::new(),
            };
            a.betas.push(b);
            d.alphas.push(a);
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    // Field-level ops at the deepest level from both ends.
    channel_a
        .sync_write(track!(&store_a, |d| {
            d.alphas[0].betas[0].alphas[0].name = "edited-a".into();
        }))
        .unwrap();
    channel_b
        .sync_write(track!(&store_b, |d| {
            d.alphas[0].betas[0].alphas.push(Alpha {
                name: "from-b".into(),
                betas: CrdtVec::new(),
            });
        }))
        .unwrap();
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);
    sync(&mut remote_a, &mut client_a, &server, &cache_a, &store_a);
    sync(&mut remote_b, &mut client_b, &server, &cache_b, &store_b);

    let snap = store_a.snapshot();
    assert_eq!(snap.alphas[0].betas[0].alphas[0].name, "edited-a");
    assert_eq!(snap.alphas[0].betas[0].alphas.len(), 2);
    assert_eq!(snap.alphas[0].betas[0].alphas[1].name, "from-b");
    assert_eq!(store_a.snapshot(), store_b.snapshot());
}
