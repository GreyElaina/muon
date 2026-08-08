//! POC: `reconcile` silently drops unsynced in-place (sequence) ops.
//!
//! `rebase.rs:132-139` applies every txn through
//! `apply_txn_to_value`, which is a no-op for `Inplace` kinds
//! (`ops.rs:38-41` — the claimed dispatch by operation class does not
//! exist). A locally queued sequence op (e.g. a `push`) therefore
//! disappears from the store whenever an inbound delta triggers a
//! reconcile, until the server's echo comes back.

use muon_sync::*;

fn id(seq: u64) -> ItemId {
    ItemId {
        client_id: 0,
        incarnation: 1,
        seq,
    }
}

#[test]
fn inplace_unsynced_op_is_replayed() {
    // Authoritative value: blocks = [A] (post-delta server state).
    let authoritative = serde_json::json!({
        "blocks": [ {
            "id": { "client_id": 0, "incarnation": 1, "seq": 1 },
            "alive": true,
            "pos": null,
            "value": "A"
        } ]
    });
    // Locally queued but unsynced: push B after A.
    let push = Commit {
        ordinal: 0,
        txns: vec![Transaction {
            id: TxnId {
                incarnation: 2,
                seq: 1,
            },
            client_id: 1,
            timestamp: 1,
            kind: Changed::Inplace(Edit::Insert {
                anchor: Some(id(1)),
                range: ItemRange {
                    first: id(2),
                    len: 1,
                },
                value: Box::new(serde_json::json!(["B"])),
            }),
            model_id: "doc".to_owned(),
            path: vec![PathSegment::String("blocks".into())],
        }],
    };
    let outcome = reconcile(&authoritative, &[(1, push)]);
    let blocks = outcome.value["blocks"].as_array().unwrap();
    assert_eq!(
        blocks.len(),
        2,
        "the unsynced insert must be replayed over the authoritative value, got {blocks:?}"
    );
}
