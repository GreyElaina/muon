//! Differential testing: the sequence engine against Loro's
//! `MovableList` as an independent oracle.
//!
//! The container-to-engine property shares its conventions between
//! both sides (the observer records positions exactly as the engine
//! interprets them), so a convention bug can pass on both sides at
//! once. Loro shares no code with the engine: the same random
//! operation stream applies to both, and the visible sequences are
//! asserted equal after every step. Position semantics agree in this
//! setting — both interpret a position against the current state,
//! and the engine's op index equals its visible index because
//! unpointed slots never persist.

#![cfg(test)]

use crate::crdt::seq::{self, MovableVec};
use crate::sync::ops::{apply_inplace_value, apply_txn_to_value};
use crate::{CrdtString, CrdtStringObserver, CrdtVec, CrdtVecObserver, ItemId, ItemRange, TxnId};
use loro::{LoroDoc, LoroMovableList, LoroText, LoroValue};
use muon::helper::{QuasiObserver, Zero};
use muon::observe::Observer;
use proptest::prelude::*;
use proptest::test_runner::Config;
use serde_json::{json, Value};

/// Differential cases are slower than the in-lib properties (Loro
/// runs its full state machine per operation), so the count is lower.
const CASES: u32 = 4096;

/// One structural operation, generated at random. Indices resolve
/// against the live view at application time (modulo the current
/// length), so arbitrary values stay in range.
#[derive(Debug, Clone, Copy)]
enum RawOp {
    InsertAt { index: usize, value: i32 },
    Delete { index: usize },
    Move { index: usize, new_index: usize },
}

impl Arbitrary for RawOp {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            (any::<usize>(), any::<i32>())
                .prop_map(|(i, v)| RawOp::InsertAt { index: i, value: v }),
            any::<usize>().prop_map(|i| RawOp::Delete { index: i }),
            (any::<usize>(), any::<usize>()).prop_map(|(i, n)| RawOp::Move {
                index: i,
                new_index: n
            }),
        ]
        .boxed()
    }
}

/// A resolved operation with concrete identities — the form both
/// implementations apply. Move positions use the user-visible
/// semantics (the element ends up at `to`), matching Loro's `mov`.
/// The insert keeps both coordinates: `pos` feeds Loro's numeric
/// insert, `anchor` is the engine's identity anchor.
#[derive(Debug, Clone)]
enum Concrete {
    Insert {
        pos: usize,
        anchor: Option<ItemId>,
        id: ItemId,
        value: i32,
    },
    Delete {
        index: usize,
        id: ItemId,
    },
    Move {
        from: usize,
        to: usize,
        id: ItemId,
        move_id: ItemId,
    },
}

fn item(seq: u64) -> ItemId {
    ItemId {
        client_id: 0,
        incarnation: 1,
        seq,
    }
}

fn live_id(nodes: &MovableVec<Value>, index: usize) -> Option<ItemId> {
    nodes.id_at(index)
}

/// Resolve a raw op against the current engine state, allocating
/// fresh identities from `seq`. A no-op (an empty list, an equal
/// move) resolves to `None`.
///
/// The move target is restricted to `[0, len)` — Loro's public `mov`
/// rejects a target at the tail (`to >= len`), while the engine
/// accepts it; the tail-move case stays covered by the in-lib
/// properties.
fn resolve(nodes: &MovableVec<Value>, op: RawOp, seq: &mut u64) -> Option<Concrete> {
    let n = nodes.len();
    match op {
        RawOp::InsertAt { index, value } => {
            let pos = index % (n + 1);
            // The anchor is the element the new one follows: the
            // predecessor at `pos`, `None` at the head.
            let anchor = if pos == 0 {
                None
            } else {
                Some(nodes.id_at(pos - 1)?)
            };
            let id = item(*seq);
            *seq += 1;
            Some(Concrete::Insert {
                pos,
                anchor,
                id,
                value,
            })
        }
        RawOp::Delete { index } => {
            if n == 0 {
                return None;
            }
            let index = index % n;
            let id = live_id(nodes, index)?;
            Some(Concrete::Delete { index, id })
        }
        RawOp::Move { index, new_index } => {
            if n == 0 {
                return None;
            }
            let from = index % n;
            let to = new_index % n;
            if from == to {
                return None;
            }
            let id = live_id(nodes, from)?;
            let move_id = item(*seq);
            *seq += 1;
            Some(Concrete::Move {
                from,
                to,
                id,
                move_id,
            })
        }
    }
}

/// Apply one concrete op to both implementations and assert that the
/// visible sequences agree.
fn apply_and_check(nodes: &mut MovableVec<Value>, loro: &LoroMovableList, c: &Concrete) {
    match c {
        Concrete::Insert {
            pos,
            anchor,
            id,
            value,
        } => {
            seq::insert_after(
                nodes,
                *anchor,
                ItemRange { first: *id, len: 1 },
                vec![json!(value)],
            );
            loro.insert(*pos, *value).expect("loro insert");
        }
        Concrete::Delete { index, id } => {
            seq::delete_by_id(nodes, &[ItemRange { first: *id, len: 1 }]);
            loro.delete(*index, 1).expect("loro delete");
        }
        Concrete::Move {
            from,
            to,
            id,
            move_id,
        } => {
            // The engine's `move_after` takes an identity anchor in
            // the pre-removal coordinate space (the observer converts
            // at record time); Loro's `mov` takes the post-operation
            // position. The pre-removal target is one past the
            // post-operation position when it lies after the moved
            // element.
            let to_op = if *to > *from { *to + 1 } else { *to };
            let to_anchor = if to_op == 0 {
                None
            } else {
                Some(nodes.id_at(to_op - 1).expect("in-range target"))
            };
            seq::move_after(nodes, *id, to_anchor, *move_id);
            loro.mov(*from, *to).expect("loro mov");
        }
    }
    assert_visible_eq(nodes, loro);
}

/// The engine's live values (alive nodes in position order) must
/// equal Loro's deep value — the same visible sequence.
fn assert_visible_eq(nodes: &MovableVec<Value>, loro: &LoroMovableList) {
    let engine_values: Vec<Value> = nodes.visible().cloned().collect();
    let loro_values: Vec<Value> = match loro.get_deep_value() {
        LoroValue::List(list) => list.iter().map(loro_value_to_json).collect(),
        other => panic!("loro deep value is not a list: {other:?}"),
    };
    assert_eq!(
        engine_values,
        loro_values,
        "engine diverged from Loro ({} elements)",
        engine_values.len()
    );
}

fn loro_value_to_json(v: &LoroValue) -> Value {
    match v {
        LoroValue::Null => Value::Null,
        LoroValue::Bool(b) => json!(b),
        LoroValue::I64(i) => json!(i),
        LoroValue::Double(f) => json!(f),
        LoroValue::String(s) => json!(s.to_string()),
        LoroValue::List(l) => Value::Array(l.iter().map(loro_value_to_json).collect()),
        other => panic!("unexpected loro value: {other:?}"),
    }
}

proptest! {
    #![proptest_config(Config::with_cases(CASES))]

    #[test]
    fn engine_matches_loro_movable_list(
        ops in prop::collection::vec(any::<RawOp>(), 0..=40),
    ) {
        let mut nodes: MovableVec<Value> = MovableVec::new();
        let doc = LoroDoc::new();
        let loro = doc.get_movable_list("blocks");
        let mut seq = 1u64;
        for op in &ops {
            let Some(concrete) = resolve(&nodes, *op, &mut seq) else {
                continue;
            };
            apply_and_check(&mut nodes, &loro, &concrete);
        }
    }

    /// The same differential check on degenerate streams: delete-heavy
    /// (shrinks toward empty), move-heavy (length stable, positions
    /// churn), alternating insert/delete (hovers near empty), a move
    /// chain (each element shifts by one), and insert-only (monotonic
    /// growth).
    #[test]
    fn engine_matches_loro_on_degenerate_streams(
        ops in degenerate_stream(),
    ) {
        let mut nodes: MovableVec<Value> = MovableVec::new();
        let doc = LoroDoc::new();
        let loro = doc.get_movable_list("blocks");
        let mut seq = 1u64;
        for op in &ops {
            let Some(concrete) = resolve(&nodes, *op, &mut seq) else {
                continue;
            };
            apply_and_check(&mut nodes, &loro, &concrete);
        }
    }
}

/// One degenerate `RawOp` stream drawn from a weighted distribution.
fn degenerate_stream() -> impl Strategy<Value = Vec<RawOp>> {
    let insert =
        (any::<usize>(), any::<i32>()).prop_map(|(i, v)| RawOp::InsertAt { index: i, value: v });
    prop_oneof![
        prop::collection::vec(
            prop_oneof![9 => any::<usize>().prop_map(|i| RawOp::Delete { index: i }), 1 => insert.clone()],
            0..=60usize,
        ),
        prop::collection::vec(
            prop_oneof![
                8 => (any::<usize>(), any::<usize>()).prop_map(|(i, n)| RawOp::Move { index: i, new_index: n }),
                1 => insert.clone(),
                1 => any::<usize>().prop_map(|i| RawOp::Delete { index: i }),
            ],
            0..=60usize,
        ),
        prop::collection::vec(
            prop_oneof![
                1 => insert.clone(),
                1 => any::<usize>().prop_map(|i| RawOp::Delete { index: i }),
            ],
            0..=60usize,
        ),
        prop::collection::vec(
            prop_oneof![
                6 => (any::<usize>(), any::<i32>()).prop_map(|(i, _)| RawOp::Move { index: i, new_index: i + 1 }),
                1 => insert.clone(),
                1 => any::<usize>().prop_map(|i| RawOp::Delete { index: i }),
            ],
            0..=60usize,
        ),
        prop::collection::vec(insert, 0..=60usize),
    ]
    .boxed()
}

// ═══════════════════════════════════════════════════════════════════
// Full-chain oracle: user-level API through the observer and lowering
// ═══════════════════════════════════════════════════════════════════

/// A scalar element value. Loro's public `insert` rejects container
/// values (lists, maps, nested containers), so the shared value
/// domain is scalars only.
#[derive(Debug, Clone)]
enum Scalar {
    I64(i64),
    Str(String),
    Bool(bool),
    Null,
}

impl Arbitrary for Scalar {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            any::<i64>().prop_map(Scalar::I64),
            prop::sample::select(&["alpha", "beta", "gamma"])
                .prop_map(|s| Scalar::Str(s.to_string())),
            any::<bool>().prop_map(Scalar::Bool),
            Just(Scalar::Null),
        ]
        .boxed()
    }
}

fn scalar_to_json(s: &Scalar) -> Value {
    match s {
        Scalar::I64(i) => json!(i),
        Scalar::Str(s) => json!(s),
        Scalar::Bool(b) => json!(b),
        Scalar::Null => Value::Null,
    }
}

fn scalar_to_loro(s: &Scalar) -> LoroValue {
    match s {
        Scalar::I64(i) => LoroValue::I64(*i),
        Scalar::Str(s) => LoroValue::String(s.clone().into()),
        Scalar::Bool(b) => LoroValue::Bool(*b),
        Scalar::Null => LoroValue::Null,
    }
}

/// One user-level operation on the sequence field — the same API a
/// client drives. Both sides interpret indices against their current
/// live view; the observer records the operation, lowers it to a
/// transaction, and the engine applies it — the full recording chain
/// is under test, not just the engine.
#[derive(Debug, Clone)]
enum ApiOp {
    Push { value: Scalar },
    InsertAt { index: usize, value: Scalar },
    Remove { index: usize },
    MoveTo { index: usize, new_index: usize },
}

impl Arbitrary for ApiOp {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            any::<Scalar>().prop_map(|v| ApiOp::Push { value: v }),
            (any::<usize>(), any::<Scalar>())
                .prop_map(|(i, v)| ApiOp::InsertAt { index: i, value: v }),
            any::<usize>().prop_map(|i| ApiOp::Remove { index: i }),
            (any::<usize>(), any::<usize>()).prop_map(|(i, n)| ApiOp::MoveTo {
                index: i,
                new_index: n
            }),
        ]
        .boxed()
    }
}

fn apply_api_to_vec(ob: &mut CrdtVecObserver<Value, CrdtVec<Value>, Zero>, op: &ApiOp) {
    match op {
        ApiOp::Push { value } => ob.push(scalar_to_json(value)),
        ApiOp::InsertAt { index, value } => {
            let n = QuasiObserver::untracked_ref(ob).len();
            ob.insert(index % (n + 1), scalar_to_json(value));
        }
        ApiOp::Remove { index } => {
            let n = QuasiObserver::untracked_ref(ob).len();
            if n > 0 {
                ob.remove(index % n);
            }
        }
        ApiOp::MoveTo { index, new_index } => {
            let n = QuasiObserver::untracked_ref(ob).len();
            if n > 0 {
                let from = index % n;
                let to = new_index % n;
                if from != to {
                    ob.move_to(from, to);
                }
            }
        }
    }
}

fn apply_api_to_loro(loro: &LoroMovableList, op: &ApiOp) {
    match op {
        ApiOp::Push { value } => loro.push(scalar_to_loro(value)).expect("loro push"),
        ApiOp::InsertAt { index, value } => {
            let n = loro.len();
            loro.insert(index % (n + 1), scalar_to_loro(value))
                .expect("loro insert");
        }
        ApiOp::Remove { index } => {
            let n = loro.len();
            if n > 0 {
                loro.delete(index % n, 1).expect("loro delete");
            }
        }
        ApiOp::MoveTo { index, new_index } => {
            let n = loro.len();
            if n > 0 {
                let from = index % n;
                let to = new_index % n;
                if from != to {
                    loro.mov(from, to).expect("loro mov");
                }
            }
        }
    }
}

/// The engine state's live values must equal Loro's deep value.
fn assert_visible_eq_state(state: &Value, loro: &LoroMovableList) {
    let engine_values: Vec<Value> = state["blocks"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter(|n| n["alive"].as_bool().unwrap_or(false))
                .map(|n| n["value"].clone())
                .collect()
        })
        .unwrap_or_default();
    let loro_values: Vec<Value> = match loro.get_deep_value() {
        LoroValue::List(list) => list.iter().map(loro_value_to_json).collect(),
        other => panic!("loro deep value is not a list: {other:?}"),
    };
    assert_eq!(
        engine_values,
        loro_values,
        "observer chain diverged from Loro ({} elements)",
        engine_values.len()
    );
}

proptest! {
    #![proptest_config(Config::with_cases(CASES))]

    #[test]
    fn observer_chain_matches_loro_movable_list(
        ops in prop::collection::vec(any::<ApiOp>(), 0..=80),
    ) {
        let mut vec: CrdtVec<Value> = CrdtVec::new();
        let doc = LoroDoc::new();
        let loro = doc.get_movable_list("blocks");
        let mut state = json!({ "blocks": [] });
        let mut next = 1u64;
        for op in &ops {
            // Loro applies the user-level op directly.
            apply_api_to_loro(&loro, op);
            // Our side: observe, apply, flush, lower, apply.
            {
                let mut ob = unsafe {
                    CrdtVecObserver::<Value, CrdtVec<Value>, Zero>::observe(&mut vec)
                };
                apply_api_to_vec(&mut ob, op);
                let seq_ops = crate::__sync_flush!(&mut ob);
                drop(ob);
                // Mirror the composite observer's field prefix (the
                // store path prefixes "blocks"); a bare root
                // observation has no field of its own.
                let seq_ops = seq_ops.with_prefix("blocks");
                let mut ids = || TxnId {
                    incarnation: 1,
                    seq: {
                        let s = next;
                        next += 1;
                        s
                    },
                };
                if std::env::var("ORACLE_DEBUG").is_ok() {
                    eprintln!("seq_ops: {} change(s)", seq_ops.inner.len());
                }
                let txns = crate::sync::sink::txns_from_changes(seq_ops, "doc", 0, &mut ids);
                if std::env::var("ORACLE_DEBUG").is_ok() {
                    eprintln!(
                        "txns: {}",
                        serde_json::to_string(&txns).expect("transaction serialization")
                    );
                }
                for txn in &txns {
                    let applied = match &txn.kind {
                        crate::Changed::Inplace(kind) => {
                            apply_inplace_value(&mut state, &txn.path, kind).is_ok()
                        }
                        _ => false,
                    };
                    if !applied {
                        apply_txn_to_value(&mut state, txn);
                    }
                }
            }
            assert_visible_eq_state(&state, &loro);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// Text oracle: `CrdtString` against Loro's `LoroText`
// ═══════════════════════════════════════════════════════════════════

/// One user-level text operation, in character offsets — the same
/// unit as `char` and as Loro's Rust text API (Unicode scalar
/// positions), so the two sides need no coordinate conversion.
#[derive(Debug, Clone)]
enum TextOp {
    Insert { pos: usize, s: String },
    Delete { pos: usize, len: usize },
}

/// A text fragment: ASCII, multi-byte (Chinese), an emoji, a
/// combining sequence, or a mix. Byte length differs from char count,
/// which exercises the Unicode-scalar unit on both sides.
fn text_fragment() -> impl Strategy<Value = String> {
    prop::sample::select(&[
        "a",
        "abc",
        "中",
        "中文文",
        "😀",
        "e\u{301}",
        "héllo",
        "a中😀e",
    ])
    .prop_map(|s: &str| s.to_string())
}

impl Arbitrary for TextOp {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            (any::<usize>(), text_fragment()).prop_map(|(p, s)| TextOp::Insert { pos: p, s }),
            (any::<usize>(), any::<usize>()).prop_map(|(p, l)| TextOp::Delete { pos: p, len: l }),
        ]
        .boxed()
    }
}

/// Apply one op to Loro, normalizing positions against the current
/// length: an insert at `pos % (n + 1)`, a delete of
/// `[pos % n, +len % (n - pos + 1))` (a zero length is a no-op).
fn apply_text_to_loro(loro: &LoroText, op: &TextOp) {
    match op {
        TextOp::Insert { pos, s } => {
            let n = loro.len_unicode();
            loro.insert(pos % (n + 1), s).expect("loro insert");
        }
        TextOp::Delete { pos, len } => {
            let n = loro.len_unicode();
            if n > 0 {
                let p = pos % n;
                let l = len % (n - p + 1);
                if l > 0 {
                    loro.delete(p, l).expect("loro delete");
                }
            }
        }
    }
}

/// Apply one op through the observer chain, with the same
/// normalization as [`apply_text_to_loro`].
fn apply_text_to_string(ob: &mut CrdtStringObserver<(), CrdtString<()>, Zero>, op: &TextOp) {
    match op {
        TextOp::Insert { pos, s } => {
            let n = QuasiObserver::untracked_ref(ob).len();
            ob.insert(pos % (n + 1), s);
        }
        TextOp::Delete { pos, len } => {
            let n = QuasiObserver::untracked_ref(ob).len();
            if n > 0 {
                let p = pos % n;
                let l = len % (n - p + 1);
                if l > 0 {
                    ob.delete(p..p + l);
                }
            }
        }
    }
}

/// The engine's text (the field's node array — characters only) must
/// equal Loro's text.
fn assert_text_eq(state: &Value, loro: &LoroText) {
    let engine_text: String = state["text"]
        .as_array()
        .expect("text field is a node array")
        .iter()
        .filter(|n| n["alive"].as_bool().expect("alive flag"))
        .filter_map(|n| match &n["value"] {
            // A character element serializes as `{"Char": "c"}`.
            Value::Object(map) => map
                .get("Char")
                .and_then(Value::as_str)
                .and_then(|s| s.chars().next()),
            _ => None,
        })
        .collect();
    let loro_text = loro.slice(0, loro.len_unicode()).expect("loro slice");
    assert_eq!(
        engine_text,
        loro_text,
        "text diverged from Loro ({} chars)",
        engine_text.chars().count()
    );
}

proptest! {
    #![proptest_config(Config::with_cases(CASES))]

    #[test]
    fn text_matches_loro_text(
        ops in prop::collection::vec(any::<TextOp>(), 0..=60),
    ) {
        let mut text: CrdtString<()> = CrdtString::new();
        let doc = LoroDoc::new();
        let loro = doc.get_text("text");
        let mut state = json!({ "text": [] });
        let mut next = 1u64;
        for op in &ops {
            // Loro applies the user-level op directly.
            apply_text_to_loro(&loro, op);
            // Our side: observe, apply, flush, lower, apply.
            {
                let mut ob = unsafe {
                    CrdtStringObserver::<(), CrdtString<()>, Zero>::observe(&mut text)
                };
                apply_text_to_string(&mut ob, op);
                let seq_ops = crate::__sync_flush!(&mut ob);
                drop(ob);
                let seq_ops = seq_ops.with_prefix("text");
                let mut ids = || TxnId {
                    incarnation: 1,
                    seq: {
                        let s = next;
                        next += 1;
                        s
                    },
                };
                let txns = crate::sync::sink::txns_from_changes(seq_ops, "doc", 0, &mut ids);
                for txn in &txns {
                    let applied = match &txn.kind {
                        crate::Changed::Inplace(kind) => {
                            apply_inplace_value(&mut state, &txn.path, kind).is_ok()
                        }
                        _ => false,
                    };
                    if !applied {
                        apply_txn_to_value(&mut state, txn);
                    }
                }
            }
            assert_text_eq(&state, &loro);
        }
    }


}
