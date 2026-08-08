//! POC: field edits through element identities can crash or corrupt
//! the server's sequence state (applied on `poll`).
//!
//! - A node without a `value` member makes the field-replace path
//!   panic (`ops.rs:269`).
//! - A missing node (concurrent delete race) appends a bare `Null`
//!   to the single list (`ops.rs:271`), which later sequence ops
//!   cannot parse and which is broadcast to clients.

use muon_sync::*;

fn id(seq: u64) -> ItemId {
    ItemId {
        client_id: 0,
        incarnation: 1,
        seq,
    }
}

fn field_edit(target: ItemId) -> Transaction {
    Transaction {
        id: TxnId {
            incarnation: 2,
            seq: 1,
        },
        client_id: 1,
        timestamp: 1,
        kind: Changed::Replace {
            before: None,
            after: Some(serde_json::json!("x")),
        },
        model_id: "doc".to_owned(),
        path: vec![
            PathSegment::String("blocks".into()),
            PathSegment::Identity(target),
            PathSegment::String("label".into()),
        ],
    }
}

fn send_and_poll(server: &mut SyncServer, txn: &Transaction) {
    server.send(
        BatchKey {
            client_id: 1,
            session: 1,
            batch_id: 1,
        },
        std::slice::from_ref(txn),
    );
    server.poll(None);
}

#[test]
fn missing_value_member_panics_server() {
    let mut server = SyncServer::new();
    // A sequence node without a `value` member.
    server.seed(
        "doc",
        serde_json::json!({
            "blocks": [ { "id": { "client_id": 0, "incarnation": 1, "seq": 1 } } ]
        }),
    );
    // Must not panic.
    send_and_poll(&mut server, &field_edit(id(1)));
}

#[test]
fn missing_node_appends_null_to_single_list() {
    let mut server = SyncServer::new();
    // Element seq 1 exists; the edit references seq 2 (as if it was
    // deleted concurrently before the edit landed).
    server.seed(
        "doc",
        serde_json::json!({
            "blocks": [ {
                "id": { "client_id": 0, "incarnation": 1, "seq": 1 },
                "value": { "label": "a" }
            } ]
        }),
    );
    send_and_poll(&mut server, &field_edit(id(2)));
    let model = server.model("doc").unwrap();
    assert_eq!(
        model["blocks"].as_array().unwrap().len(),
        1,
        "no bogus slot may be appended to the single list"
    );
}
