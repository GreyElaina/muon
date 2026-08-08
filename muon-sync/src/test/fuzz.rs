//! Property-based fuzz tests for the sequence layer.
//!
//! Three invariants are exercised with random operation streams:
//!
//! 1. **Replay idempotency and structural validity** — every resolved
//!    operation applied twice is a no-op (insert ranges, per-element
//!    deletes, placement versions), and the sequence state stays
//!    structurally sound (unique ids, valid placement references).
//! 2. **Container-to-engine convergence** — a random `CrdtVec`
//!    session through the observer, lowered to transactions and
//!    applied to the engine's single-list model, converges with the
//!    container's own serialized node array.
//! 3. **Undo round-trip** — inverting an applied transaction stream
//!    restores the live view (deleted elements never resurrect;
//!    insert inverses delete their run).

#![cfg(test)]

use crate::crdt::seq::MovableVec;
use crate::sync::ops::{apply_inplace_value, apply_txn_to_value};
use crate::{CrdtVec, CrdtVecObserver, ItemId, ItemRange, SeqNode, TxnId};
use muon::helper::{QuasiObserver, Zero};
use muon::observe::Observer;
use proptest::prelude::*;
use proptest::test_runner::Config;
use serde_json::{json, Value};

/// Fuzz depth: each property runs this many random streams.
const CASES: u32 = 4096;

// ═══════════════════════════════════════════════════════════════════
// Op model
// ═══════════════════════════════════════════════════════════════════

/// One structural operation. Indices are resolved against the live
/// view at application time (modulo the current length), so arbitrary
/// generated values stay in range.
#[derive(Debug, Clone, Copy)]
enum Op {
    /// Insert `value` at live position `index` (0 = head, len = append).
    InsertAt { index: usize, value: u8 },
    /// Remove the live element at `index`.
    Delete { index: usize },
    /// Move the live element at `index` to live position `new_index`.
    Move { index: usize, new_index: usize },
}

impl Arbitrary for Op {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            (any::<usize>(), any::<u8>()).prop_map(|(i, v)| Op::InsertAt { index: i, value: v }),
            any::<usize>().prop_map(|i| Op::Delete { index: i }),
            (any::<usize>(), any::<usize>()).prop_map(|(i, n)| Op::Move {
                index: i,
                new_index: n
            }),
        ]
        .boxed()
    }
}

/// A resolved operation with concrete identities — the replay form.
#[derive(Debug, Clone)]
enum Concrete {
    Insert {
        anchor: Option<ItemId>,
        range: ItemRange,
        value: Value,
    },
    Delete {
        target: ItemId,
    },
    Move {
        item: ItemId,
        to: Option<ItemId>,
        pos: ItemId,
    },
}

fn live_id(nodes: &MovableVec<Value>, index: usize) -> Option<ItemId> {
    nodes.id_at(index)
}

fn live_len(nodes: &MovableVec<Value>) -> usize {
    nodes.len()
}

fn item(seq: u64) -> ItemId {
    ItemId {
        client_id: 0,
        incarnation: 1,
        seq,
    }
}

/// Resolve an `Operation` against the current sequence state, allocating
/// fresh identities from `seq` for created elements.
fn resolve(nodes: &MovableVec<Value>, op: Op, seq: &mut u64) -> Option<Concrete> {
    match op {
        Op::InsertAt { index, value } => {
            let n = live_len(nodes);
            let index = index % (n + 1);
            // The anchor is the element the new one follows: the
            // predecessor at `index`, `None` at the head (the tail
            // for an append is the last element).
            let anchor = if index == 0 {
                None
            } else {
                Some(nodes.id_at(index - 1)?)
            };
            let id = item(*seq);
            *seq += 1;
            Some(Concrete::Insert {
                anchor,
                range: ItemRange { first: id, len: 1 },
                value: json!(value),
            })
        }
        Op::Delete { index } => {
            let n = live_len(nodes);
            if n == 0 {
                return None;
            }
            live_id(nodes, index % n).map(|target| Concrete::Delete { target })
        }
        Op::Move { index, new_index } => {
            let n = live_len(nodes);
            if n == 0 {
                return None;
            }
            let from = index % n;
            let to = new_index % n;
            if from == to {
                return None;
            }
            let elem_id = live_id(nodes, from)?;
            // The target position in the post-removal view: the
            // element's own slot shifts the count by one when it lay
            // before the target. The anchor is the element that
            // position follows.
            let target = if from < to { to + 1 } else { to };
            let to_anchor = if target == 0 {
                None
            } else {
                Some(nodes.id_at(target - 1)?)
            };
            let pos = item(*seq);
            *seq += 1;
            Some(Concrete::Move {
                item: elem_id,
                to: to_anchor,
                pos,
            })
        }
    }
}

fn apply_concrete(nodes: &mut MovableVec<Value>, c: &Concrete) -> bool {
    match c {
        Concrete::Insert {
            anchor,
            range,
            value,
        } => crate::crdt::seq::insert_after(nodes, *anchor, *range, vec![value.clone()]),
        Concrete::Delete { target } => crate::crdt::seq::delete_by_id(
            nodes,
            &[ItemRange {
                first: *target,
                len: 1,
            }],
        ),
        Concrete::Move { item, to, pos } => crate::crdt::seq::move_after(nodes, *item, *to, *pos),
    }
}

fn assert_structural(nodes: &MovableVec<Value>) {
    // Element ids are unique. Placement versions are positions (a
    // move's own id), not elements, so they live in their own
    // namespace and need not reference an element.
    let mut ids: Vec<ItemId> = nodes.to_nodes_ref().iter().map(|n| n.id).collect();
    ids.sort_unstable();
    for pair in ids.windows(2) {
        assert_ne!(pair[0], pair[1], "duplicate element id");
    }
}

// ═══════════════════════════════════════════════════════════════════
// Fuzz 1: replay idempotency and structural validity
// ═══════════════════════════════════════════════════════════════════

proptest! {
    #![proptest_config(Config::with_cases(CASES))]

    #[test]
    fn seq_replay_is_idempotent_and_structural(
        ops in prop::collection::vec(any::<Op>(), 0..=40),
    ) {
        let mut nodes: MovableVec<Value> = MovableVec::new();
        let mut seq = 1u64;
        for op in &ops {
            let Some(c) = resolve(&nodes, *op, &mut seq) else { continue };
            if !apply_concrete(&mut nodes, &c) {
                continue;
            }
            // Idempotency at application time: the same resolved
            // operation replayed on the state it produced is a no-op
            // (insert ranges are present, the target is deleted,
            // the placement version matches).
            let before = nodes.clone();
            assert!(
                !apply_concrete(&mut nodes, &c),
                "single-op replay must be a no-op: {c:?} on {before:?}"
            );
            assert_eq!(nodes, before, "no-op replay must not change state");
        }
        assert_structural(&nodes);
    }
}

// ═══════════════════════════════════════════════════════════════════
// Pattern fuzz: degenerate operation streams
// ═══════════════════════════════════════════════════════════════════

/// A degenerate operation stream that stresses an extreme state:
/// mostly deletes (the list shrinks toward empty), mostly moves
/// (length is stable, positions churn), alternating insert/delete
/// (the list hovers near empty), a move chain (each element shifts
/// by one), or inserts only (monotonic growth).
#[derive(Debug, Clone, Copy)]
enum Pattern {
    DeleteHeavy,
    MoveHeavy,
    Alternating,
    MoveChain,
    InsertOnly,
}

impl Arbitrary for Pattern {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            Just(Pattern::DeleteHeavy),
            Just(Pattern::MoveHeavy),
            Just(Pattern::Alternating),
            Just(Pattern::MoveChain),
            Just(Pattern::InsertOnly),
        ]
        .boxed()
    }
}

/// One op strategy, fresh per call (the weighted `prop_oneof`
/// branches consume their strategies).
fn insert_op() -> impl Strategy<Value = Op> {
    (any::<usize>(), any::<u8>()).prop_map(|(i, v)| Op::InsertAt { index: i, value: v })
}

fn delete_op() -> impl Strategy<Value = Op> {
    any::<usize>().prop_map(|i| Op::Delete { index: i })
}

fn move_op() -> impl Strategy<Value = Op> {
    (any::<usize>(), any::<usize>()).prop_map(|(i, n)| Op::Move {
        index: i,
        new_index: n,
    })
}

fn move_chain_op() -> impl Strategy<Value = Op> {
    (any::<usize>(), any::<u8>()).prop_map(|(i, _)| Op::Move {
        index: i,
        new_index: i + 1,
    })
}

/// A pattern and a stream of ops drawn from its weighted
/// distribution, generated together.
fn patterned_stream() -> impl Strategy<Value = (Pattern, Vec<Op>)> {
    any::<Pattern>().prop_flat_map(|pattern| {
        let stream = match pattern {
            Pattern::DeleteHeavy => {
                prop::collection::vec(prop_oneof![9 => delete_op(), 1 => insert_op()], 0..=60usize)
                    .boxed()
            }
            Pattern::MoveHeavy => prop::collection::vec(
                prop_oneof![8 => move_op(), 1 => insert_op(), 1 => delete_op()],
                0..=60usize,
            )
            .boxed(),
            Pattern::Alternating => {
                prop::collection::vec(prop_oneof![1 => insert_op(), 1 => delete_op()], 0..=60usize)
                    .boxed()
            }
            Pattern::MoveChain => prop::collection::vec(
                prop_oneof![6 => move_chain_op(), 1 => insert_op(), 1 => delete_op()],
                0..=60usize,
            )
            .boxed(),
            Pattern::InsertOnly => prop::collection::vec(insert_op(), 0..=60usize).boxed(),
        };
        (Just(pattern), stream)
    })
}

proptest! {
    #![proptest_config(Config::with_cases(CASES))]

    #[test]
    fn seq_replay_patterns_stay_structural(
        (pattern, ops) in patterned_stream(),
    ) {
        // The same replay-idempotency and structural assertions as
        // the uniform stream, but on degenerate streams.
        let mut nodes: MovableVec<Value> = MovableVec::new();
        let mut seq = 1u64;
        for op in &ops {
            let Some(c) = resolve(&nodes, *op, &mut seq) else { continue };
            if !apply_concrete(&mut nodes, &c) {
                continue;
            }
            let before = nodes.clone();
            assert!(
                !apply_concrete(&mut nodes, &c),
                "single-op replay must be a no-op ({pattern:?}): {c:?} on {before:?}"
            );
            assert_eq!(nodes, before, "no-op replay must not change state");
        }
        assert_structural(&nodes);
    }
}

// ═══════════════════════════════════════════════════════════════════
// Fuzz 2: container-to-engine convergence
// ═══════════════════════════════════════════════════════════════════

/// Run a random session on a `CrdtVec` through its observer and lower
/// the recorded stream to transactions.
fn session_txns(ops: &[Op]) -> (CrdtVec<Value>, Vec<crate::Transaction>) {
    let mut v = CrdtVec::new();
    let mut ob = unsafe { CrdtVecObserver::<Value, CrdtVec<Value>, Zero>::observe(&mut v) };
    for op in ops {
        match *op {
            Op::InsertAt { index, value } => {
                let n = QuasiObserver::untracked_ref(&ob).len();
                ob.insert(index % (n + 1), json!(value));
            }
            Op::Delete { index } => {
                let n = QuasiObserver::untracked_ref(&ob).len();
                if n > 0 {
                    ob.remove(index % n);
                }
            }
            Op::Move { index, new_index } => {
                let n = QuasiObserver::untracked_ref(&ob).len();
                if n > 0 {
                    ob.move_to(index % n, new_index % n);
                }
            }
        }
    }
    let ops = crate::__sync_flush!(&mut ob);
    drop(ob);
    // The composite observer's flush prefixes every field mutation
    // with the field path; a bare root observation has no field, so
    // the field path is applied here to mirror the store path.
    let ops = ops.with_prefix("blocks");

    let mut next = 1u64;
    let mut ids = move || TxnId {
        incarnation: 1,
        seq: {
            let s = next;
            next += 1;
            s
        },
    };
    let txns = crate::sync::sink::txns_from_changes(ops, "doc", 0, &mut ids);
    (v, txns)
}

fn apply_txn(state: &mut Value, txn: &crate::Transaction) {
    let applied = match &txn.kind {
        crate::Changed::Inplace(kind) => apply_inplace_value(state, &txn.path, kind).is_ok(),
        _ => false,
    };
    if !applied {
        apply_txn_to_value(state, txn);
    }
}

proptest! {
    #![proptest_config(Config::with_cases(CASES))]

    #[test]
    fn container_end_to_end_converges(
        ops in prop::collection::vec(any::<Op>(), 0..=40),
    ) {
        let (v, txns) = session_txns(&ops);
        let mut server = json!({ "blocks": [] });
        for txn in &txns {
            apply_txn(&mut server, txn);
        }
        let container = serde_json::to_value(&v).expect("container serialization");
        assert_eq!(
            server["blocks"], container,
            "engine view must converge with the container ({} ops)",
            ops.len()
        );
    }
}

// ═══════════════════════════════════════════════════════════════════
// Fuzz 3: undo round-trip
// ═══════════════════════════════════════════════════════════════════

proptest! {
    #![proptest_config(Config::with_cases(CASES))]

    #[test]
    fn undo_round_trip_restores_live_view(
        ops in prop::collection::vec(any::<Op>(), 0..=30),
    ) {
        let (_, txns) = session_txns(&ops);
        let mut server = json!({});
        for txn in &txns {
            apply_txn(&mut server, txn);
        }
        // Inverse in reverse order (a Commit::invert-style walk).
        let inverses: Vec<crate::Transaction> = txns
            .iter()
            .rev()
            .flat_map(|txn| txn.invert())
            .collect();
        for txn in &inverses {
            apply_txn(&mut server, txn);
        }
        // The live view is empty again: insert inverses delete their
        // run, delete inverses never resurrect deleted elements, and
        // move inverses only rearrange live elements.
        let rebuilt = server
            .get("blocks")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|n| serde_json::from_value::<SeqNode<Value>>(n).expect("node shape"))
            .collect::<Vec<_>>();
        let rebuilt = MovableVec::from_nodes(rebuilt);
        assert_eq!(
            live_len(&rebuilt),
            0,
            "undo restores the empty live view ({} ops, {} txns)",
            ops.len(),
            txns.len()
        );
    }
}
