//! Shared test helpers.
//!
//! [`TestServer`] is the reference transport around the Sans-I/O
//! server core ([`SyncServer`]): it wraps the core in a mutex and
//! implements [`SyncTransport`], mirroring the assertion surface
//! (`applied_count`, `model`, `last_sync_id`) through the lock. The
//! per-field rejection set is wired into the core's injected policy,
//! so `deny_field` works on a live server.
//!
//! Each test binary compiles this module independently and uses a
//! different subset of the helper surface, so unused items are
//! expected per binary.

#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use muon_sync::{
    BatchKey, PollOutcome, SendError, SendResponse, SyncServer, SyncTransport, Transaction,
};
use serde_json::Value;

/// A mutex-wrapped [`SyncServer`] behind the [`SyncTransport`] trait.
pub struct TestServer {
    inner: Mutex<SyncServer>,
    deny: Arc<Mutex<HashSet<String>>>,
}

impl TestServer {
    /// Create a server with unlimited delta retention and no denied
    /// fields. Seed models explicitly.
    pub fn new() -> Self {
        Self::build(0)
    }

    /// Create a server that retains only the newest `n` delta packets.
    pub fn with_retention(n: usize) -> Self {
        Self::build(n)
    }

    fn build(retention: usize) -> Self {
        let deny = Arc::new(Mutex::new(HashSet::new()));
        let inner = SyncServer::new().with_retention(retention).with_reject({
            let deny = deny.clone();
            move |txn: &Transaction| {
                txn.path.iter().any(|seg| {
                    matches!(seg, muon_sync::PathSegment::String(f) if deny.lock().unwrap().contains(f))
                })
            }
        });
        Self {
            inner: Mutex::new(inner),
            deny,
        }
    }

    /// Seed an initial model value.
    pub fn seed(&self, model_id: &str, value: Value) {
        self.inner.lock().unwrap().seed(model_id, value);
    }

    /// Deny writes to a field: transactions touching it are rejected
    /// on application and reported through `DeltaPacket::rejected`.
    pub fn deny_field(&self, field: &str) {
        self.deny.lock().unwrap().insert(field.to_owned());
    }

    /// Simulate another actor clearing a model.
    pub fn clear_model(&self, model_id: &str) {
        self.inner.lock().unwrap().clear_model(model_id);
    }

    /// The number of transactions actually applied (dedup excluded).
    pub fn applied_count(&self) -> u64 {
        self.inner.lock().unwrap().applied_count()
    }

    /// The current authoritative value of a model (a clone, so the
    /// lock is not held past the call).
    pub fn model(&self, model_id: &str) -> Option<Value> {
        self.inner.lock().unwrap().model(model_id).cloned()
    }
}

impl SyncTransport for TestServer {
    async fn send(
        &self,
        batch_key: BatchKey,
        txns: &[Transaction],
    ) -> Result<SendResponse, SendError> {
        Ok(self.inner.lock().unwrap().send(batch_key, txns))
    }

    async fn poll_deltas(&self, since: Option<u64>) -> Result<PollOutcome, String> {
        Ok(self.inner.lock().unwrap().poll(since))
    }
}

impl Default for TestServer {
    fn default() -> Self {
        Self::new()
    }
}
