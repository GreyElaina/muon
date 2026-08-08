//! Delta writeback: apply server delta packets to the adapter's remote,
//! and apply transactions to a typed store.
//!
//! The authoritative view of the remote lives in the **adapter's** [`RemoteView`] —
//! never in the sync core. When a delta packet arrives, the adapter
//! updates its remote and calls `SyncDriver::reconcile_model`, which
//! replays the model's unsynced changes over the authoritative value
//! and emits the final value to publish.

use std::collections::HashMap;

use serde::de::DeserializeOwned;
use serde_json::Value;

use muon_store::{ChangeEvent, Store};

use crate::{DeltaAction, DeltaPacket, Transaction};

/// The authoritative view of the remote, owned by the adapter.
///
/// One JSON value per model, updated **only** from delta packets — never
/// from local writes. The sync core never touches it; the adapter feeds
/// it to [`SyncDriver::reconcile_model`](crate::SyncDriver::reconcile_model)
/// to rebuild the optimistic overlay after every inbound change.
#[derive(Debug, Default, Clone)]
pub struct RemoteView {
    values: HashMap<String, Value>,
}

impl RemoteView {
    /// Create an empty remote (no models known yet).
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a delta packet's actions. Returns the models whose value
    /// changed, in deterministic order (for reconciliation).
    pub fn apply_packet(&mut self, packet: &DeltaPacket) -> Vec<String> {
        let mut changed = Vec::new();
        for action in &packet.actions {
            match action {
                DeltaAction::Value { model_id, value } => {
                    self.values.insert(model_id.clone(), value.clone());
                    changed.push(model_id.clone());
                }
                DeltaAction::Update { model_id, value } => {
                    // RFC 7396 JSON Merge Patch: objects merge
                    // recursively, `null` removes a member, other
                    // values replace.
                    let entry = self.values.entry(model_id.clone()).or_insert(Value::Null);
                    json_patch::merge(entry, value);
                    changed.push(model_id.clone());
                }
                DeltaAction::Archive { model_id } | DeltaAction::Clear { model_id } => {
                    self.values.remove(model_id);
                    changed.push(model_id.clone());
                }
                DeltaAction::Insert { model_id } => {
                    self.values.entry(model_id.clone()).or_insert(Value::Null);
                }
                DeltaAction::SeqOp {
                    model_id,
                    path,
                    kind,
                } => {
                    // A structural operation updates the model's
                    // single-list array (identity semantics), exactly
                    // as the server applies it.
                    let entry = self.values.entry(model_id.clone()).or_insert(Value::Null);
                    if let crate::Changed::Inplace(element_kind) = kind {
                        // The server only broadcasts operations it
                        // applied itself, so a malformed op here is
                        // unreachable; keep the previous value if it
                        // ever happens.
                        let _ = crate::sync::ops::apply_inplace_value(entry, path, element_kind);
                    }
                    changed.push(model_id.clone());
                }
            }
        }
        changed.sort();
        changed.dedup();
        changed
    }

    /// The authoritative value of a model, if known.
    pub fn value(&self, model_id: &str) -> Option<&Value> {
        self.values.get(model_id)
    }

    /// The model ids currently known to the remote.
    pub fn models(&self) -> impl Iterator<Item = &String> {
        self.values.keys()
    }

    /// Replace the whole view with a bootstrap snapshot: every model's
    /// complete authoritative value. Returns the model ids, in
    /// deterministic order (for reconciliation).
    pub fn apply_snapshot(&mut self, models: &[(String, Value)]) -> Vec<String> {
        self.values.clear();
        let mut changed = Vec::with_capacity(models.len());
        for (mid, value) in models {
            self.values.insert(mid.clone(), value.clone());
            changed.push(mid.clone());
        }
        changed.sort();
        changed
    }
}

/// Errors that can occur while applying a transaction to a store.
#[derive(Debug, thiserror::Error)]
pub enum DeltaApplyError {
    /// Failed to serialize the current store value to JSON.
    #[error("failed to serialize store value: {error}")]
    Serialize {
        /// The underlying serde error.
        #[source]
        error: serde_json::Error,
    },
    /// Failed to deserialize the resulting JSON back into the store's type.
    #[error("failed to deserialize model `{model_id}`: {error}")]
    Deserialize {
        /// The model id of the affected store.
        model_id: String,
        /// The underlying serde error.
        #[source]
        error: serde_json::Error,
    },
}

impl Transaction {
    /// Apply this transaction into a [`Store<T>`]: serialize the
    /// current value, apply the transaction's [`kind`](crate::Changed)
    /// at its path, and replace the value.
    ///
    /// Used for direct application (e.g. replaying a snapshot), not by
    /// the sync pipeline — the pipeline publishes reconciled values via
    /// `SyncDriver::reconcile_model`.
    ///
    /// The read-compute-replace runs inside one store write-lock
    /// critical section, so a concurrent local write cannot be
    /// overwritten by a result computed from a stale base.
    pub fn apply_into<T>(&self, store: &Store<T>) -> Result<ChangeEvent, DeltaApplyError>
    where
        T: serde::Serialize + DeserializeOwned + 'static,
    {
        let cr = store.write(|arc| {
            let mut json: Value = serde_json::to_value(&**arc)
                .map_err(|e| DeltaApplyError::Serialize { error: e })?;

            crate::sync::ops::apply_txn_to_value(&mut json, self);

            let new_t: T =
                serde_json::from_value(json).map_err(|e| DeltaApplyError::Deserialize {
                    model_id: self.model_id.clone(),
                    error: e,
                })?;

            *arc = std::sync::Arc::new(new_t);
            Ok::<_, DeltaApplyError>(())
        });
        let (result, event) = cr.into_parts();
        result?;
        Ok(event)
    }
}
