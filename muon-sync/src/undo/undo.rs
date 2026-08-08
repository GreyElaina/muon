//! Undo/redo inverse construction: build the inverse of a change or
//! transaction.
//!
//! The inversion rules mirror LSE's `undoTransaction` mechanism. Every
//! transaction carries everything the inverse needs: a whole-field
//! replacement carries its own `before`/`after` pair, and an in-place
//! operation carries its undo payload (`prev`, `from_anchor`,
//! deleted values). No inverse can fail to construct.

use serde_json::Value;

use crate::{Changed, Commit, Edit, Transaction};

impl Commit {
    /// Build the inverse of this change: each leaf transaction's
    /// inverses, in reverse order (LSE undoes operations in reverse).
    pub fn invert(&self) -> Commit {
        let mut inv_txns = Vec::new();
        for txn in self.txns.iter().rev() {
            inv_txns.extend(txn.invert());
        }
        Commit {
            ordinal: self.ordinal,
            txns: inv_txns,
        }
    }
}

impl Transaction {
    /// Compute the inverse of this transaction for undo/redo.
    ///
    /// The inversion rules mirror LSE's `undoTransaction` mechanism:
    /// - `Replace { before, after }` → `Replace { before: after, after: before }`
    ///   — the symmetric swap restores the pre-write value (and the
    ///   redo restores the post-write value from the same pair).
    /// - `Insert` → `Delete` over the inserted run (the elements are
    ///   tombstoned, never resurrected)
    /// - `Delete` → one `Insert` per target run, back after the
    ///   recorded anchor (the runs' relative order is preserved).
    ///   The re-inserted elements carry **fresh identities** on re-run
    ///   (see [`Transaction::refresh`]): a tombstoned id is never
    ///   resurrected.
    /// - `Move` → `Move` back to the previous anchor.
    /// - `Update` → `Update` with `prev` and `value` swapped.
    pub fn invert(&self) -> Vec<Transaction> {
        let inverse_kinds: Vec<Changed<Edit>> = match &self.kind {
            // Replace(new) → Replace(old): the symmetric swap.
            Changed::Replace { before, after } => vec![Changed::Replace {
                before: after.clone(),
                after: before.clone(),
            }],
            Changed::Inplace(kind) => match kind {
                // Insert → delete the inserted run (identity-based).
                Edit::Insert {
                    anchor,
                    range,
                    value,
                } => vec![Changed::Inplace(Edit::Delete {
                    anchor: *anchor,
                    targets: vec![*range],
                    value: value.clone(),
                })],
                // Delete → re-insert every run after the recorded anchor.
                // A per-element array value (rich text) is split so
                // each run's insert carries its own slice: the
                // server's insert takes the first `range.len`
                // elements, so sharing the full array across runs
                // would duplicate the early elements and drop the
                // later ones. A single value (plain containers) is
                // repeated by the server and needs no split. The runs
                // are emitted in reverse order: consecutive inserts
                // behind the same anchor place the later one first,
                // so the last run must be inserted before the first.
                Edit::Delete {
                    anchor,
                    targets,
                    value,
                } => {
                    let values: Vec<Value> = match &**value {
                        Value::Array(items) => items.clone(),
                        _ => Vec::new(),
                    };
                    let mut offset = 0usize;
                    let runs: Vec<(_, Value)> = targets
                        .iter()
                        .map(|range| {
                            let len = range.len as usize;
                            let payload = if values.is_empty() {
                                (**value).clone()
                            } else {
                                let end = (offset + len).min(values.len());
                                Value::Array(values[offset..end].to_vec())
                            };
                            offset += len;
                            (*range, payload)
                        })
                        .collect();
                    runs.into_iter()
                        .rev()
                        .map(|(range, payload)| {
                            Changed::Inplace(Edit::Insert {
                                anchor: *anchor,
                                range,
                                value: Box::new(payload),
                            })
                        })
                        .collect()
                }
                // Move → move back after the recorded `from_anchor`. The
                // inverse's placement version is renewed on refresh
                // (creating identity), so a re-run is a fresh move.
                Edit::Move {
                    item,
                    to,
                    from_anchor,
                    pos: _,
                } => vec![Changed::Inplace(Edit::Move {
                    item: *item,
                    to: *from_anchor,
                    from_anchor: *to,
                    pos: *item,
                })],
                // Update → swap prev and value: the inverse writes
                // the recorded previous value back onto the same element.
                Edit::Update { id, prev, value } => {
                    vec![Changed::Inplace(Edit::Update {
                        id: *id,
                        prev: value.clone(),
                        value: prev.clone(),
                    })]
                }
            },
        };

        inverse_kinds
            .into_iter()
            .map(|kind| Transaction {
                id: self.id,
                client_id: self.client_id,
                timestamp: self.timestamp,
                kind,
                model_id: self.model_id.clone(),
                path: self.path.clone(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use crate::{ItemId, ItemRange, TxnId};
    use muon::PathSegment;

    fn item(seq: u64) -> ItemId {
        ItemId {
            client_id: 1,
            incarnation: 7,
            seq,
        }
    }

    fn make_txn(seq: u64, kind: Changed<Edit>) -> Transaction {
        Transaction {
            id: TxnId {
                incarnation: 7,
                seq,
            },
            client_id: 1,
            timestamp: 1000,
            kind,
            model_id: "test".into(),
            path: vec![],
        }
    }

    #[test]
    fn replace_inverse_swaps_before_and_after() {
        // The inverse of a truncation is the symmetric swap: the
        // pre-write container is `before`, restored wholesale.
        let mut txn = make_txn(
            1,
            Changed::Replace {
                before: Some(json!(["a", "b", "c", "d", "e"])),
                after: Some(json!(["a", "b"])),
            },
        );
        txn.path = vec![PathSegment::String("items".into())];
        let inv = txn.invert();
        assert_eq!(inv.len(), 1);
        assert_eq!(
            inv[0].kind,
            Changed::Replace {
                before: Some(json!(["a", "b"])),
                after: Some(json!(["a", "b", "c", "d", "e"])),
            },
            "inverse restores the full pre-write container",
        );
        assert_eq!(inv[0].path, txn.path, "inverse applies at the same path");
    }

    #[test]
    fn deletion_inverse_restores_removed_entry() {
        // A deletion is a Replace with `after: None`: the inverse
        // re-inserts the removed entry.
        let mut txn = make_txn(
            1,
            Changed::Replace {
                before: Some(json!(true)),
                after: None,
            },
        );
        txn.path = vec![
            PathSegment::String("labels".into()),
            PathSegment::String("urgent".into()),
        ];
        let inv = txn.invert();
        assert_eq!(inv.len(), 1);
        assert_eq!(
            inv[0].kind,
            Changed::Replace {
                before: None,
                after: Some(json!(true)),
            },
        );
    }

    #[test]
    fn insert_inverse_deletes_the_inserted_run() {
        let anchor = item(2);
        let txn = make_txn(
            1,
            Changed::Inplace(Edit::Insert {
                anchor: Some(anchor),
                range: ItemRange {
                    first: item(1),
                    len: 2,
                },
                value: Box::new(json!(["a", "b"])),
            }),
        );
        let inv = txn.invert();
        assert_eq!(inv.len(), 1);
        match &inv[0].kind {
            Changed::Inplace(Edit::Delete {
                anchor: inv_anchor,
                targets,
                value,
            }) => {
                assert_eq!(inv_anchor, &Some(anchor), "re-insertion anchor kept");
                assert_eq!(targets.len(), 1);
                assert_eq!(targets[0].len, 2, "deletes the whole inserted run");
                assert_eq!(&**value, &json!(["a", "b"]), "payload kept for redo");
            }
            _ => panic!("insert inverts to a delete"),
        }
    }

    #[test]
    fn delete_ranges_inverse_reinserts_at_the_anchor() {
        let anchor = item(5);
        let txn = make_txn(
            1,
            Changed::Inplace(Edit::Delete {
                anchor: Some(anchor),
                targets: vec![ItemRange {
                    first: item(1),
                    len: 1,
                }],
                value: Box::new(json!("b")),
            }),
        );
        let inv = txn.invert();
        assert_eq!(inv.len(), 1, "one insert per target run");
        match &inv[0].kind {
            Changed::Inplace(Edit::Insert {
                anchor: inv_anchor,
                range,
                value,
            }) => {
                assert_eq!(
                    inv_anchor,
                    &Some(anchor),
                    "re-inserts after the recorded anchor"
                );
                assert_eq!(range.len, 1);
                assert_eq!(&**value, &json!("b"));
            }
            _ => panic!("delete inverts to an insert"),
        }
    }

    #[test]
    fn move_inverse_moves_back() {
        let elem_id = item(3);
        let to = item(9);
        let from_anchor = item(2);
        let txn = make_txn(
            1,
            Changed::Inplace(Edit::Move {
                item: elem_id,
                to: Some(to),
                from_anchor: Some(from_anchor),
                pos: item(1),
            }),
        );
        let inv = txn.invert();
        assert_eq!(inv.len(), 1);
        match &inv[0].kind {
            Changed::Inplace(Edit::Move {
                item,
                to: inv_to,
                from_anchor: inv_from,
                pos: _,
            }) => {
                assert_eq!(item, &elem_id);
                assert_eq!(
                    *inv_to,
                    Some(from_anchor),
                    "moves back after the previous anchor"
                );
                assert_eq!(*inv_from, Some(to));
            }
            _ => panic!("move inverts to a move"),
        }
    }

    #[test]
    fn update_element_inverse_swaps_prev_and_value() {
        let txn = make_txn(
            1,
            Changed::Inplace(Edit::Update {
                id: item(3),
                prev: Box::new(json!(["a", false])),
                value: Box::new(json!(["a", true])),
            }),
        );
        let inv = txn.invert();
        assert_eq!(inv.len(), 1);
        match &inv[0].kind {
            Changed::Inplace(Edit::Update { id, prev, value }) => {
                assert_eq!(id, &item(3), "updates the same element");
                assert_eq!(
                    &**prev,
                    &json!(["a", true]),
                    "inverse prev is the forward value"
                );
                assert_eq!(
                    &**value,
                    &json!(["a", false]),
                    "inverse value is the forward prev"
                );
            }
            _ => panic!("update inverts to an update"),
        }
    }

    #[test]
    fn refresh_renews_insert_range_but_keeps_referencing_ids() {
        // A delete's inverse is an insert: refreshing it must assign a
        // fresh element range (never resurrecting tombstoned ids).
        let txn = make_txn(
            1,
            Changed::Inplace(Edit::Insert {
                anchor: Some(item(1)),
                range: ItemRange {
                    first: item(1),
                    len: 3,
                },
                value: Box::new(json!("abc")),
            }),
        );
        let mut next = 100u64;
        let refreshed = txn.refresh(2000, &mut || {
            let id = next;
            next += 1;
            TxnId {
                incarnation: 7,
                seq: id,
            }
        });
        match &refreshed.kind {
            Changed::Inplace(Edit::Insert { range, .. }) => {
                assert_eq!(range.first.seq, 100, "range renewed with the op id");
                assert_eq!(range.len, 3, "length preserved");
            }
            _ => panic!("kind preserved"),
        }
        assert_eq!(
            refreshed.id.seq, 100,
            "outer id and first element share the seq"
        );

        // An insert's inverse is a delete: refreshing it must keep the
        // targets pointing at the original elements.
        let inv = txn.invert().pop().unwrap();
        let refreshed = inv.refresh(2000, &mut || {
            let id = next;
            next += 1;
            TxnId {
                incarnation: 7,
                seq: id,
            }
        });
        match &refreshed.kind {
            Changed::Inplace(Edit::Delete { targets, .. }) => {
                assert_eq!(targets[0].first.seq, 1, "referencing ids are kept");
            }
            _ => panic!("kind preserved"),
        }
    }

    #[test]
    fn replace_inverse_is_always_constructible() {
        // A replacement without a known previous value still inverts:
        // the symmetric swap is defined on the pair as it stands.
        let txn = make_txn(
            1,
            Changed::Replace {
                before: None,
                after: Some(json!("x")),
            },
        );
        let inv = txn.invert();
        assert_eq!(
            inv[0].kind,
            Changed::Replace {
                before: Some(json!("x")),
                after: None,
            },
        );
    }
}
