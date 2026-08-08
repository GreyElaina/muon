//! POC: `merge_patch_diff` drops a member whose new value is an
//! empty object replacing a scalar.
//!
//! `server.rs:447-449` skips a `sub` that is an empty object — the
//! intended "no change" marker for object-object recursion — but the
//! leaf branch `(_, b) => b.clone()` also yields `{}` when a scalar
//! becomes `{}`. The member is omitted from the patch, so remotes
//! keep the old scalar and diverge from the server.

use muon_sync::*;

#[test]
fn scalar_to_empty_object_is_broadcast() {
    let mut server = SyncServer::new();
    server.seed("doc", serde_json::json!({ "a": 5, "b": 1 }));
    server.poll(None);

    // Establish the broadcast baseline: after this poll the server's
    // `last_broadcast` is `{ a: 5, b: 1 }`, so the next patch is a
    // real diff.
    server.send(
        BatchKey {
            client_id: 1,
            session: 1,
            batch_id: 1,
        },
        &[Transaction {
            id: TxnId {
                incarnation: 2,
                seq: 1,
            },
            client_id: 1,
            timestamp: 1,
            kind: Changed::Replace {
                before: None,
                after: Some(serde_json::json!(2)),
            },
            model_id: "doc".to_owned(),
            path: vec![PathSegment::String("b".into())],
        }],
    );
    server.poll(None);
    let base = server.last_sync_id();

    // Now replace the scalar `a` with an empty object.
    server.send(
        BatchKey {
            client_id: 1,
            session: 1,
            batch_id: 2,
        },
        &[Transaction {
            id: TxnId {
                incarnation: 2,
                seq: 2,
            },
            client_id: 1,
            timestamp: 2,
            kind: Changed::Replace {
                before: None,
                after: Some(serde_json::json!({})),
            },
            model_id: "doc".to_owned(),
            path: vec![PathSegment::String("a".into())],
        }],
    );
    let packets = match server.poll(Some(base)) {
        PollOutcome::Deltas(packets) => packets,
        _ => panic!("expected delta packets"),
    };
    let update = packets
        .iter()
        .flat_map(|p| p.actions.iter())
        .find_map(|a| match a {
            DeltaAction::Update { value, .. } => Some(value),
            _ => None,
        })
        .expect("an update action");
    assert!(
        update.get("a").is_some(),
        "the member must appear in the patch (as an empty object), got {update:?}"
    );
}
