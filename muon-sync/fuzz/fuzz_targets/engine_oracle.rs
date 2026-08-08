//! Coverage-guided differential fuzzing: the sequence engine against
//! Loro's `MovableList` as an independent oracle.
//!
//! The same random operation stream applies to both implementations,
//! and the visible sequences are asserted equal after every step —
//! the same differential check as the lib's proptest oracle, driven
//! by libFuzzer's coverage guidance instead of uniform randomness.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use muon_sync::{delete_by_id, insert_after, move_after, ItemId, ItemRange, MovableVec};
use serde_json::{json, Value};

/// One structural operation on the sequence, resolved against the
/// current state at application time.
#[derive(Arbitrary, Debug, Clone, Copy)]
enum RawOp {
    InsertAt { index: usize, value: i32 },
    Delete { index: usize },
    Move { index: usize, new_index: usize },
}

fn elem(seq: u64) -> ItemId {
    ItemId {
        client_id: 0,
        incarnation: 1,
        seq,
    }
}

fn loro_value_to_json(v: &loro::LoroValue) -> Value {
    match v {
        loro::LoroValue::Null => Value::Null,
        loro::LoroValue::Bool(b) => json!(b),
        loro::LoroValue::I64(i) => json!(i),
        loro::LoroValue::Double(f) => json!(f),
        loro::LoroValue::String(s) => json!(s.to_string()),
        loro::LoroValue::List(l) => Value::Array(l.iter().map(loro_value_to_json).collect()),
        other => panic!("unexpected loro value: {other:?}"),
    }
}

/// The engine's live values must equal Loro's deep value — the same
/// visible sequence.
fn assert_visible_eq(nodes: &MovableVec<Value>, loro: &loro::LoroMovableList) {
    let engine_values: Vec<Value> = nodes.visible().cloned().collect();
    let loro_values: Vec<Value> = match loro.get_deep_value() {
        loro::LoroValue::List(list) => list.iter().map(loro_value_to_json).collect(),
        other => panic!("loro deep value is not a list: {other:?}"),
    };
    assert_eq!(
        engine_values, loro_values,
        "engine diverged from Loro ({} elements)",
        engine_values.len()
    );
}

fuzz_target!(|ops: Vec<RawOp>| {
    let mut nodes: MovableVec<Value> = MovableVec::new();
    let doc = loro::LoroDoc::new();
    let loro = doc.get_movable_list("blocks");
    let mut seq = 1u64;
    for op in ops {
        let n = nodes.len();
        match op {
            RawOp::InsertAt { index, value } => {
                let pos = index % (n + 1);
                let anchor = if pos == 0 {
                    None
                } else {
                    Some(nodes.id_at(pos - 1).expect("in-range anchor"))
                };
                let id = elem(seq);
                seq += 1;
                insert_after(
                    &mut nodes,
                    anchor,
                    ItemRange { first: id, len: 1 },
                    vec![json!(value)],
                );
                loro.insert(pos, value).expect("loro insert");
            }
            RawOp::Delete { index } => {
                if n == 0 {
                    continue;
                }
                let index = index % n;
                let Some(id) = nodes.id_at(index) else {
                    continue;
                };
                delete_by_id(&mut nodes, &[ItemRange { first: id, len: 1 }]);
                loro.delete(index, 1).expect("loro delete");
            }
            RawOp::Move { index, new_index } => {
                if n == 0 {
                    continue;
                }
                let from = index % n;
                let to = new_index % n;
                if from == to {
                    continue;
                }
                let Some(id) = nodes.id_at(from) else {
                    continue;
                };
                // The engine's `move_after` takes an identity anchor
                // in the pre-removal coordinate; Loro's `mov` takes
                // the post-operation position (one less when the
                // target lies after the moved element).
                let to_op = if to > from { to + 1 } else { to };
                let to_anchor = if to_op == 0 {
                    None
                } else {
                    Some(nodes.id_at(to_op - 1).expect("in-range target"))
                };
                let move_id = elem(seq);
                seq += 1;
                move_after(&mut nodes, id, to_anchor, move_id);
                loro.mov(from, to).expect("loro mov");
            }
        }
        assert_visible_eq(&nodes, &loro);
    }
});
