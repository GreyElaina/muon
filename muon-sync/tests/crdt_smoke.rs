//! Smoke tests: `CrdtVec` through the full sync write path.
//!
//! These exercise the observer interception (inherent methods), the
//! lower boundary (identity operations into transactions) and the
//! container's serialized form (the engine's single-list array
//! format), without a server. Convergence with the server is covered
//! by the property tests in `src/fuzz.rs` (single client) and the
//! multi-client tests in `crdt_e2e.rs`.

use muon::Observe;
use muon_store::{track, Store, Track};
use muon_sync::*;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Serialize, Deserialize, Observe, Clone, Debug, PartialEq, Track)]
struct Block {
    id: u32,
    text: String,
}

#[derive(Serialize, Observe, Clone, Debug, PartialEq, Track)]
struct Doc {
    blocks: CrdtVec<Block>,
}

fn setup() -> (SyncChannel, Store<Doc>) {
    let queue = Arc::new(Mutex::new(TransactionQueue::new(1)));
    let channel = SyncChannel::new(queue, "doc");
    let store = Store::new(Doc {
        blocks: CrdtVec::new(),
    });
    (channel, store)
}

#[test]
fn push_lowers_to_insert_after() {
    let (channel, store) = setup();
    let out = channel
        .sync_write(track!(&store, |doc| {
            doc.blocks.push(Block {
                id: 1,
                text: "hi".into(),
            });
        }))
        .unwrap();

    let txns = &out.commit.txns;
    assert_eq!(txns.len(), 1);
    let txn = &txns[0];
    assert_eq!(
        txn.path,
        vec![muon_sync::PathSegment::String("blocks".into())]
    );
    let Changed::Inplace(Edit::Insert {
        anchor: None,
        range,
        value,
    }) = &txn.kind
    else {
        panic!("expected Insert, got {:?}", txn.kind);
    };
    assert_eq!(range.len, 1);
    assert_eq!(range.first.client_id, 0, "elements carry no client");
    assert_eq!(range.first.seq, 1, "first container identity");
    assert_eq!(&**value, &serde_json::json!({ "id": 1, "text": "hi" }));

    // The store's container holds the element; the transaction id is
    // independent of the element identity (both counters start at 1,
    // so compare the incarnation — queue-allocated vs container-
    // allocated).
    let doc = store.snapshot();
    assert_eq!(doc.blocks.len(), 1);
    assert_eq!(
        doc.blocks.get(0).unwrap(),
        &Block {
            id: 1,
            text: "hi".into()
        }
    );
    assert_ne!(
        txn.id.incarnation, range.first.incarnation,
        "txn id is queue-allocated"
    );
}

#[test]
fn insert_anchors_before_the_insertion_point() {
    let (channel, store) = setup();
    let out = channel
        .sync_write(track!(&store, |doc| {
            doc.blocks.push(Block {
                id: 1,
                text: "a".into(),
            });
            doc.blocks.insert(
                0,
                Block {
                    id: 2,
                    text: "b".into(),
                },
            );
        }))
        .unwrap();

    let txns = &out.commit.txns;
    assert_eq!(txns.len(), 2);
    // push first: empty container, head anchor, identity seq 1.
    let Changed::Inplace(Edit::Insert {
        anchor: None,
        range: first_range,
        ..
    }) = &txns[0].kind
    else {
        panic!("expected Insert, got {:?}", txns[0].kind);
    };
    assert_eq!(first_range.first.seq, 1);
    // insert at the head second: head anchor again (before the pushed
    // element), identity seq 2.
    let Changed::Inplace(Edit::Insert {
        anchor: None,
        range: second_range,
        ..
    }) = &txns[1].kind
    else {
        panic!("expected Insert, got {:?}", txns[1].kind);
    };
    assert_eq!(second_range.first.seq, 2);

    // The head insert lands before the pushed element.
    let doc = store.snapshot();
    assert_eq!(doc.blocks.get(0).unwrap().id, 2);
    assert_eq!(doc.blocks.get(1).unwrap().id, 1);
}

#[test]
fn remove_tombstones_and_keeps_payload() {
    let (channel, store) = setup();
    channel
        .sync_write(track!(&store, |doc| {
            doc.blocks.push(Block {
                id: 1,
                text: "a".into(),
            });
            doc.blocks.push(Block {
                id: 2,
                text: "b".into(),
            });
        }))
        .unwrap();
    let out = channel
        .sync_write(track!(&store, |doc| {
            doc.blocks.remove(0);
        }))
        .unwrap();

    let txns = &out.commit.txns;
    assert_eq!(txns.len(), 1);
    let Changed::Inplace(Edit::Delete {
        anchor: None,
        targets,
        value,
    }) = &txns[0].kind
    else {
        panic!("expected Delete, got {:?}", txns[0].kind);
    };
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].first.seq, 1, "removes the first element");
    assert_eq!(targets[0].len, 1);
    assert_eq!(
        &**value,
        &serde_json::json!({ "id": 1, "text": "a" }),
        "payload kept"
    );

    // The element is removed physically: the live view drops it and
    // the slot is gone.
    let doc = store.snapshot();
    assert_eq!(doc.blocks.len(), 1);
    assert_eq!(doc.blocks.get(0).unwrap().id, 2);
}

#[test]
fn move_to_emits_placement_version() {
    let (channel, store) = setup();
    channel
        .sync_write(track!(&store, |doc| {
            doc.blocks.push(Block {
                id: 1,
                text: "a".into(),
            });
            doc.blocks.push(Block {
                id: 2,
                text: "b".into(),
            });
            doc.blocks.push(Block {
                id: 3,
                text: "c".into(),
            });
        }))
        .unwrap();
    let out = channel
        .sync_write(track!(&store, |doc| {
            doc.blocks.move_to(2, 0);
        }))
        .unwrap();

    let txns = &out.commit.txns;
    assert_eq!(txns.len(), 1);
    let Changed::Inplace(Edit::Move {
        item,
        to: None,
        from_anchor,
        pos,
    }) = &txns[0].kind
    else {
        panic!("expected Move, got {:?}", txns[0].kind);
    };
    assert_eq!(item.seq, 3, "moves the third element");
    assert_eq!(
        from_anchor.map(|e| e.seq),
        Some(2),
        "predecessor before the move"
    );
    assert_eq!(pos.seq, 4, "fresh placement version from the container");

    let doc = store.snapshot();
    assert_eq!(doc.blocks.get(0).unwrap().id, 3);
    assert_eq!(doc.blocks.get(1).unwrap().id, 1);
    assert_eq!(doc.blocks.get(2).unwrap().id, 2);
}

#[test]
fn whole_field_assignment_lowers_to_replace() {
    let (channel, store) = setup();
    channel
        .sync_write(track!(&store, |doc| {
            doc.blocks.push(Block {
                id: 1,
                text: "a".into(),
            });
        }))
        .unwrap();
    let mut fresh = CrdtVec::new();
    fresh.push(Block {
        id: 9,
        text: "fresh".into(),
    });
    let out = channel
        .sync_write(track!(&store, |doc| {
            doc.blocks = fresh;
        }))
        .unwrap();

    let txns = &out.commit.txns;
    assert_eq!(txns.len(), 1);
    let Changed::Replace {
        before: Some(before),
        after: Some(after),
    } = &txns[0].kind
    else {
        panic!("expected Replace, got {:?}", txns[0].kind);
    };
    // The payloads are the complete single-list arrays (the engine's
    // sequence-field format): `before` the pre-replacement container,
    // `after` the new one.
    let before_nodes = before.as_array().expect("single-list array");
    assert_eq!(before_nodes.len(), 1);
    assert_eq!(before_nodes[0]["value"]["id"], 1);
    let after_nodes = after.as_array().expect("single-list array");
    assert_eq!(after_nodes.len(), 1);
    assert_eq!(after_nodes[0]["value"]["id"], 9);

    let doc = store.snapshot();
    assert_eq!(doc.blocks.len(), 1);
    assert_eq!(doc.blocks.get(0).unwrap().id, 9);
}

#[test]
fn container_serializes_to_single_list_array() {
    let mut blocks = CrdtVec::new();
    blocks.push(Block {
        id: 1,
        text: "a".into(),
    });
    blocks.push(Block {
        id: 2,
        text: "b".into(),
    });
    blocks.remove(0);

    let json = serde_json::to_value(&blocks).unwrap();
    let nodes = json.as_array().expect("single-list array");
    assert_eq!(nodes.len(), 2, "the tombstone keeps its slot");
    assert_eq!(nodes[0]["alive"], false, "deleted element marked dead");
    assert_eq!(nodes[0]["id"]["seq"], 1);
    assert_eq!(nodes[1]["alive"], true);
    assert_eq!(nodes[1]["id"]["seq"], 2);

    // A round trip resumes identity allocation after the largest id.
    let restored: CrdtVec<Block> = serde_json::from_value(json).unwrap();
    assert_eq!(restored.len(), 1);
    assert_eq!(restored.get(0).unwrap().id, 2);
    let mut restored = restored;
    restored.push(Block {
        id: 3,
        text: "c".into(),
    });
    let json = serde_json::to_value(&restored).unwrap();
    assert_eq!(
        json.as_array().unwrap().len(),
        3,
        "the tombstone slot stays"
    );
    assert_eq!(json[2]["id"]["seq"], 3, "identity sequence resumed");
}
