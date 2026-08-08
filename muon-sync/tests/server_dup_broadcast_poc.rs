//! POC: a duplicate txn id in one batch broadcasts its SeqOp twice.
//!
//! `apply_batch` skips the second copy via the `seen` set (not
//! `rejected`), but the broadcast loop matches `applied.contains(id)`
//! per txn — both copies match and the SeqOp is pushed twice. The
//! comment claims "a duplicate id ... is never broadcast"; it is.

use muon_sync::*;

fn insert_txn(seq: u64) -> Transaction {
    Transaction {
        id: TxnId {
            incarnation: 2,
            seq,
        },
        client_id: 1,
        timestamp: 1,
        kind: Changed::Inplace(Edit::Insert {
            anchor: None,
            range: ItemRange {
                first: ItemId {
                    client_id: 0,
                    incarnation: 1,
                    seq: 1,
                },
                len: 1,
            },
            value: Box::new(serde_json::json!(["A"])),
        }),
        model_id: "doc".to_owned(),
        path: vec![PathSegment::String("blocks".into())],
    }
}

#[test]
fn duplicate_id_seqop_is_broadcast_once() {
    let mut server = SyncServer::new();
    server.seed("doc", serde_json::json!({ "blocks": [] }));
    // Bootstrap: consume the snapshot so the next poll returns
    // incremental deltas.
    server.poll(None);
    let base = server.last_sync_id();

    let txn = insert_txn(1);
    // Same txn id twice in one batch.
    server.send(
        BatchKey {
            client_id: 1,
            session: 1,
            batch_id: 1,
        },
        &[txn.clone(), txn],
    );
    let packets = match server.poll(Some(base)) {
        PollOutcome::Deltas(packets) => packets,
        _ => panic!("expected delta packets"),
    };
    assert_eq!(
        server.applied_count(),
        1,
        "the duplicate copy is not applied"
    );
    let seqops = packets
        .iter()
        .flat_map(|p| p.actions.iter())
        .filter(|a| matches!(a, DeltaAction::SeqOp { .. }))
        .count();
    assert_eq!(
        seqops, 1,
        "a duplicate txn id must be broadcast once, got {seqops}"
    );
}
