//! Transaction operations on a `serde_json::Value` tree: apply a
//! transaction's mutation at its path, and read the value at a path.
//!
//! This is the execution half of the transaction lifecycle — the
//! forward operations that [`crate::undo`] inverts. Shared by the
//! reconcile engine (replay), the store write-back (direct
//! application), and the write path (snapshot capture). The path is
//! root-to-leaf; an empty path means a root-level mutation.

use serde::de::value::SeqDeserializer;
use serde::Deserialize;
use serde_json::Value;

use crate::{Changed, Edit, ItemId, Transaction};
use muon::PathSegment;

/// Apply a transaction's change at its path within a JSON value.
///
/// The path is root-to-leaf; an empty path means a root-level mutation.
pub(crate) fn apply_txn_to_value(value: &mut Value, txn: &Transaction) {
    if txn.path.is_empty() {
        match &txn.kind {
            Changed::Replace { after: Some(v), .. } => *value = v.clone(),
            Changed::Replace { after: None, .. } => *value = Value::Null,
            Changed::Inplace(_) => {}
        }
        return;
    }
    let (parent_path, last_seg) = txn.path.split_at(txn.path.len() - 1);
    let mut current = value;
    for seg in parent_path {
        // An unresolvable segment (a deleted or malformed element)
        // makes the whole transaction a no-op.
        let Some(next) = navigate_mut(current, seg) else {
            return;
        };
        current = next;
    }
    match &txn.kind {
        Changed::Replace { after: Some(v), .. } => set_at_path(current, &last_seg[0], v.clone()),
        Changed::Replace { after: None, .. } => delete_at_path(current, &last_seg[0]),
        // Sequence operations apply to the ordered-sequence state
        // (`crate::crdt::seq::MovableVec`), not to a plain value; the
        // reconcile engine dispatches them by operation class before
        // ever reaching this function.
        Changed::Inplace(_) => {}
    }
}

/// Navigate into a `serde_json::Value` tree following a transaction
/// path. Returns `None` if any segment cannot be resolved.
pub(crate) fn value_at_path(value: &Value, path: &[PathSegment<ItemId>]) -> Option<Value> {
    let mut current = value;
    for seg in path {
        match (current, seg) {
            (Value::Object(obj), PathSegment::String(f)) => {
                current = obj.get(f.as_str())?;
            }
            (Value::Array(arr), PathSegment::Positive(i)) => {
                current = arr.get(*i)?;
            }
            (Value::Array(arr), PathSegment::Negative(i)) => {
                let len = arr.len();
                let idx = len.checked_sub(*i)?;
                current = arr.get(idx)?;
            }
            // Element identities exist only in sequence state, never
            // in a plain value tree; a value-path lookup through one
            // cannot resolve.
            (Value::Array(arr), PathSegment::Identity(id)) => {
                let node = arr.iter().find(|n| node_has_id(n, *id))?;
                current = node.get("value")?;
            }
            _ => return None,
        }
    }
    Some(current.clone())
}

// ── Navigation helpers ─────────────────────────────────────────────────

/// Navigate a mutable JSON value tree following a transaction path,
/// including element identities: an `Elem` segment locates a node in a
/// sequence's single-list array (by id) and descends into its `value`
/// member. Used by the server's sequence-field application.
pub(crate) fn seq_navigate_mut<'a>(
    value: &'a mut Value,
    path: &[PathSegment<ItemId>],
) -> Option<&'a mut Value> {
    let mut current = value;
    for seg in path {
        current = match (current, seg) {
            (Value::Object(map), PathSegment::String(f)) => map.get_mut(f.as_str())?,
            (Value::Array(arr), PathSegment::Positive(i)) => arr.get_mut(*i)?,
            (Value::Array(arr), PathSegment::Negative(i)) => {
                let idx = arr.len().checked_sub(*i)?;
                arr.get_mut(idx)?
            }
            // An element identity locates a single-list node by id and
            // descends into its value.
            (Value::Array(arr), PathSegment::Identity(id)) => {
                let node = arr.iter_mut().find(|n| node_has_id(n, *id))?;
                node.get_mut("value")?
            }
            _ => return None,
        };
    }
    Some(current)
}

/// Whether a single-list node object carries the given element id.
fn node_has_id(node: &Value, id: crate::ItemId) -> bool {
    node.get("id")
        .and_then(Value::as_object)
        .is_some_and(|id_obj| {
            id_obj.get("client_id").and_then(Value::as_u64) == Some(id.client_id)
                && id_obj.get("incarnation").and_then(Value::as_u64) == Some(id.incarnation)
                && id_obj.get("seq").and_then(Value::as_u64) == Some(id.seq)
        })
}

// ── Sequence-field application (shared by server and remote view) ───

/// Apply a structural (sequence) operation to a model's value: navigate
/// to the field's single-list array, decode it into the two-layer
/// sequence state, apply the operation, and encode it back. A missing
/// field is established as an empty single list (parse-or-err: the
/// identity semantics begin with the first structural operation).
///
/// `Ok(changed)` means the operation was acceptable: `true` when it
/// changed the sequence, `false` for an idempotent no-op (an
/// operation whose premise is already satisfied). `Err(())` means the
/// operation is malformed — a zero-length run, a field that is not a
/// sequence, a corrupt single list, or a duplicate element identity —
/// and the state is left untouched.
pub(crate) fn apply_structural_value(
    state: &mut Value,
    path: &[PathSegment<ItemId>],
    kind: &Edit,
) -> Result<bool, ()> {
    // A zero-length run is malformed: reject it before any state is
    // touched, so a rejected operation never establishes a field.
    if matches!(kind, Edit::Insert { range, .. } if range.len == 0) {
        return Err(());
    }
    if seq_navigate_mut(state, path).is_none() {
        establish_sequence(state, path);
    }
    let Some(Value::Array(arr)) = seq_navigate_mut(state, path) else {
        return Err(()); // shape mismatch: not a sequence field
    };
    // Parse the single list from the borrowed array: zero copies, and
    // a corrupt list leaves the state untouched.
    let nodes = match Vec::<crate::SeqNode<Value>>::deserialize(SeqDeserializer::new(arr.iter())) {
        Ok(nodes) => nodes,
        Err(_) => return Err(()), // corrupt single list
    };
    // The identity directory requires unique ids: a duplicate id in
    // the wire form would leave a visible element that no operation
    // can address.
    let mut ids = std::collections::HashSet::with_capacity(nodes.len());
    if nodes.iter().any(|n| !ids.insert(n.id)) {
        return Err(());
    }
    let mut vec = crate::crdt::seq::MovableVec::from_nodes(nodes);
    let changed = match kind {
        Edit::Insert {
            anchor,
            range,
            value,
        } => {
            // A scalar payload repeats once per element; an array
            // payload carries one value per element (a shorter array
            // pads with nulls, never truncates the run).
            let values = match &**value {
                Value::Array(items) => {
                    let mut values = items.clone();
                    values.resize(range.len as usize, Value::Null);
                    values
                }
                other => vec![other.clone(); range.len as usize],
            };
            crate::crdt::seq::insert_after(&mut vec, *anchor, *range, values)
        }
        Edit::Delete { targets, .. } => crate::crdt::seq::delete_by_id(&mut vec, targets),
        Edit::Move { item, to, pos, .. } => {
            crate::crdt::seq::move_after(&mut vec, *item, *to, *pos)
        }
        Edit::Update { id, value, .. } => {
            crate::crdt::seq::update_value(&mut vec, *id, (**value).clone()).is_some()
        }
    };
    if changed {
        if let Value::Array(encoded) =
            serde_json::to_value(vec.to_nodes_ref()).unwrap_or(Value::Null)
        {
            *arr = encoded;
        }
    }
    Ok(changed)
}

/// Create an empty single-list array at the sequence field's path,
/// creating any missing intermediate objects.
fn establish_sequence(state: &mut Value, path: &[PathSegment<ItemId>]) {
    let mut current = state;
    for seg in path {
        let key = match seg {
            PathSegment::String(f) => f.clone(),
            _ => return, // nested paths need an existing parent
        };
        let entry = current
            .as_object_mut()
            .map(|map| map.entry(key).or_insert(Value::Array(Vec::new())));
        match entry {
            Some(next) => current = next,
            None => return,
        }
    }
}

/// Apply an in-place operation to a model's value through the
/// sequence path. See [`apply_structural_value`].
pub(crate) fn apply_inplace_value(
    state: &mut Value,
    path: &[PathSegment<ItemId>],
    kind: &Edit,
) -> Result<bool, ()> {
    apply_structural_value(state, path, kind)
}

fn navigate_mut<'a>(value: &'a mut Value, seg: &PathSegment<ItemId>) -> Option<&'a mut Value> {
    match seg {
        PathSegment::String(f) => {
            if let Value::Object(map) = value {
                // Hit the existing key before allocating a clone for
                // the insert path (the common case).
                if map.contains_key(f.as_str()) {
                    return Some(&mut map[f.as_str()]);
                }
                return Some(map.entry(f.clone()).or_insert(Value::Null));
            }
            *value = Value::Object(Default::default());
            match value {
                Value::Object(ref mut map) => Some(map.entry(f.clone()).or_insert(Value::Null)),
                _ => unreachable!(),
            }
        }
        PathSegment::Positive(i) => {
            if let Value::Array(arr) = value {
                if *i >= arr.len() {
                    arr.resize(*i + 1, Value::Null);
                }
                return Some(&mut arr[*i]);
            }
            Some(placeholder_mut(value))
        }
        PathSegment::Negative(i) => {
            if let Value::Array(arr) = value {
                let idx = arr.len().saturating_sub(*i);
                if idx >= arr.len() {
                    arr.resize(idx + 1, Value::Null);
                }
                return Some(&mut arr[idx]);
            }
            Some(placeholder_mut(value))
        }
        // An element identity locates a single-list node by id and
        // descends into its value. A missing node (concurrent delete)
        // or a node without a `value` member (malformed input) makes
        // the transaction a no-op: the leaf edit has no target, and
        // appending a slot would corrupt the single list.
        PathSegment::Identity(id) => {
            if let Value::Array(arr) = value {
                let pos = arr.iter().position(|n| node_has_id(n, *id))?;
                return arr[pos].get_mut("value");
            }
            Some(placeholder_mut(value))
        }
    }
}

/// The lenient fallback for a segment applied to a non-container
/// value: turn the value into an object and return the slot under a
/// placeholder key (the leaf operation's own checks decide the
/// outcome).
fn placeholder_mut(value: &mut Value) -> &mut Value {
    if let Value::Object(ref mut map) = value {
        return map.entry("0".to_owned()).or_insert(Value::Null);
    }
    *value = Value::Object(Default::default());
    match value {
        Value::Object(ref mut map) => map.entry("0".to_owned()).or_insert(Value::Null),
        _ => unreachable!(),
    }
}

fn set_at_path(root: &mut Value, seg: &PathSegment<ItemId>, val: Value) {
    match (root, seg) {
        (Value::Object(map), PathSegment::String(f)) => {
            map.insert(f.clone(), val);
        }
        (Value::Array(arr), PathSegment::Positive(i)) if *i < arr.len() => {
            arr[*i] = val;
        }
        // Negative indices are 1-based; `0` is malformed and no-ops.
        (Value::Array(arr), PathSegment::Negative(i)) if *i > 0 => {
            if let Some(idx) = arr.len().checked_sub(*i) {
                arr[idx] = val;
            }
        }
        (Value::Array(arr), PathSegment::Identity(id)) => {
            if let Some(node) = arr.iter_mut().find(|n| node_has_id(n, *id)) {
                node["value"] = val;
            }
        }
        _ => {}
    }
}

fn delete_at_path(root: &mut Value, seg: &PathSegment<ItemId>) {
    match (root, seg) {
        (Value::Object(map), PathSegment::String(f)) => {
            map.remove(f.as_str());
        }
        (Value::Array(arr), PathSegment::Positive(i)) if *i < arr.len() => {
            arr.remove(*i);
        }
        // Negative indices are 1-based; `0` is malformed and no-ops.
        (Value::Array(arr), PathSegment::Negative(i)) if *i > 0 => {
            if let Some(idx) = arr.len().checked_sub(*i) {
                arr.remove(idx);
            }
        }
        // A whole-element deletion has no producer today; null the
        // node's value defensively (the sequence operation vocabulary
        // covers element removal).
        (Value::Array(arr), PathSegment::Identity(id)) => {
            if let Some(node) = arr.iter_mut().find(|n| node_has_id(n, *id)) {
                node["value"] = Value::Null;
            }
        }
        _ => {}
    }
}
