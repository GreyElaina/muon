//! POC: undoing a multi-run delete restores the wrong text.
//!
//! A delete spanning a style anchor produces multiple target runs
//! (`crdt_string.rs:280-291`). The undo inverse (`undo.rs:62-77`)
//! emits one `Insert` per run, but every insert carries the *full*
//! value array and the same anchor; the server takes the first
//! `range.len` elements of the array (`ops.rs:169-173`), so later
//! runs re-insert the wrong elements.

mod common;

use common::TestServer;
use muon::Observe;
use muon_store::{track, Store, Track};
use muon_sync::*;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Serialize, Deserialize, Observe, Clone, Debug, PartialEq, Track)]
struct Style {
    bold: bool,
}

#[derive(Serialize, Deserialize, Observe, Clone, Debug, PartialEq, Track)]
struct Doc {
    body: CrdtString<Style>,
}

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

fn client() -> (Store<Doc>, SyncClient, SyncChannel, MemCache) {
    let store = Store::new(Doc {
        body: CrdtString::new(),
    });
    let client = SyncClient::new(1);
    let channel = client.channel("doc");
    (store, client, channel, MemCache::new())
}

fn publisher(store: &Store<Doc>) -> impl FnMut(&str, &serde_json::Value) + '_ {
    move |_model_id, value| {
        let t: Doc = serde_json::from_value(value.clone()).unwrap();
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

fn sync(
    remote: &mut RemoteView,
    client: &mut SyncClient,
    server: &TestServer,
    cache: &MemCache,
    store: &Store<Doc>,
) {
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

#[test]
fn undo_multirun_delete_restores_text() {
    let server = TestServer::new();
    server.seed("doc", serde_json::json!({ "body": [] }));
    let (store, mut client, channel, cache) = client();
    let mut undo = UndoStack::new(0);
    let mut remote = RemoteView::new();

    let out = channel
        .sync_write(track!(&store, |d| {
            d.body.insert(0, "abcdef");
        }))
        .unwrap();
    undo.record(&out.commit);
    sync(&mut remote, &mut client, &server, &cache, &store);

    // Delete the middle first: c,d become tombstones that occupy seq
    // slots between b and e.
    let out = channel
        .sync_write(track!(&store, |d| {
            d.body.delete(2..4);
        }))
        .unwrap();
    undo.record(&out.commit);
    sync(&mut remote, &mut client, &server, &cache, &store);
    assert_eq!(store.snapshot().body.text(), "abef");

    // Delete [1..3) ("b", "e") — spans the tombstoned middle, so the
    // targets form two runs (b and e are not seq-consecutive).
    let out = channel
        .sync_write(track!(&store, |d| {
            d.body.delete(1..3);
        }))
        .unwrap();
    undo.record(&out.commit);
    sync(&mut remote, &mut client, &server, &cache, &store);
    assert_eq!(store.snapshot().body.text(), "af");

    // Undo the delete: the text must come back as "abef".
    undo.undo(client.driver()).expect("history is not empty");
    sync(&mut remote, &mut client, &server, &cache, &store);
    assert_eq!(
        store.snapshot().body.text(),
        "abef",
        "undo must restore the deleted text, got {:?}",
        store.snapshot().body.text()
    );
}
