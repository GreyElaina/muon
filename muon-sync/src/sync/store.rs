//! Sync behavior: a [`SyncChannel`] that turns observed writes into
//! queued changes.
//!
//! The reactive half (mutation capture, materialization, publication) lives
//! in `muon_store`; the channel only adds the sync behavior — converting
//! the observation stream into transactions and pushing them onto the
//! queue. The queue guard is held across publication, so queue insertion
//! and store publication stay in one critical section: batch order in the
//! queue always equals store publication order.
//!
//! If anything fails (materialization, empty mutation), the write is
//! aborted before publication: the store is untouched.

use or_poisoned::OrPoisoned;
use std::sync::{Arc, Mutex};

use muon::observe::Flush;
use muon::Observe;
use muon_store::{Track, Write};
use serde::Serialize;

use crate::{Commit, TransactionQueue};

/// Errors that can occur while queuing a synced write.
#[derive(Debug, thiserror::Error)]
pub enum SyncWriteError {
    /// The mutation body produced no observable changes.
    #[error("mutation body produced no changes")]
    EmptyMutation,
}

/// Outcome of a synchronous write: the published result and the change
/// that entered the pipeline.
pub struct WriteOutcome<R> {
    /// Result of publishing the draft to the store.
    pub result: R,
    /// The change (leaf transactions) enqueued by this write. Record it
    /// for application-level undo history.
    pub commit: Commit,
}

/// A per-model sync channel: the destination queue plus the logical model
/// identity stamped onto every transaction.
///
/// The channel does not own a store — the store belongs to the caller and
/// arrives as the [`Write`](muon_store::Write) produced by `track!`.
/// Store identity stays at the call site instead of hiding a second
/// store binding inside the channel.
pub struct SyncChannel {
    queue: Arc<Mutex<TransactionQueue>>,
    model_id: String,
}

impl SyncChannel {
    /// Create a channel for `model_id` appending to `queue`.
    pub fn new(queue: Arc<Mutex<TransactionQueue>>, model_id: impl Into<String>) -> Self {
        Self {
            queue,
            model_id: model_id.into(),
        }
    }

    /// The queue this channel appends to.
    pub fn queue(&self) -> &Arc<Mutex<TransactionQueue>> {
        &self.queue
    }

    /// The logical model stamped onto queued transactions.
    pub fn model_id(&self) -> &str {
        &self.model_id
    }

    /// Run one synced write: observe the tracked write, convert the
    /// observation stream into transactions, push them onto the queue,
    /// and publish — all under one store write-lock section.
    ///
    /// `write` is a [`track!`](muon_store::track) intent. The body is
    /// still unexecuted when this method runs; the pre-mutation values
    /// are captured by the observers' snapshots under the write lock.
    ///
    /// On any failure the write is aborted before publication: the
    /// store is untouched.
    ///
    /// Returns the published result plus the change as enqueued, so the
    /// caller can record it (e.g. as undo history) without re-deriving
    /// it from the mutation.
    pub fn sync_write<'a, T, R, B>(
        &self,
        write: Write<'a, T, B>,
    ) -> Result<WriteOutcome<R>, SyncWriteError>
    where
        T: Track + Serialize,
        B: for<'ob> FnOnce(&mut <T as Observe>::Observer<'ob, T, muon::helper::Zero>) -> R,
        for<'ob> <T as Observe>::Observer<'ob, T, muon::helper::Zero>: Flush<crate::SyncSink>,
    {
        let now = crate::types::now_millis();
        write
            .observe()
            .sync_flush(crate::SyncSink::new(), |sink, result| {
                // Lock order: store write lock (held here) → queue lock. The
                // queue guard spans publication, so the change enters the
                // queue and the store becomes visible atomically.
                let changes = sink.into_changes();
                let mut guard = self.queue.lock().or_poisoned();
                let mut ids = || guard.next_txn_id();
                let mut txns =
                    crate::sync::sink::txns_from_changes(changes, &self.model_id, now, &mut ids);
                if txns.is_empty() {
                    return Err(SyncWriteError::EmptyMutation);
                }
                for txn in &mut txns {
                    txn.client_id = guard.client_id();
                }
                let commit = guard.push_txn(txns);
                drop(guard);
                Ok(WriteOutcome { result, commit })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muon_store::track;
    use serde_json::json;

    /// A document with one sequence field and one scalar field.
    #[derive(Clone, Debug, Serialize, Observe, Track)]
    struct Doc {
        blocks: crate::CrdtVec<String>,
        title: String,
    }

    impl Default for Doc {
        fn default() -> Self {
            Self {
                blocks: crate::CrdtVec::new(),
                title: String::from("old"),
            }
        }
    }

    fn setup() -> (
        muon_store::Store<Doc>,
        crate::SyncClient,
        crate::SyncChannel,
    ) {
        let store = muon_store::Store::new(Doc::default());
        let client = crate::SyncClient::new(1);
        let channel = client.channel("doc");
        (store, client, channel)
    }

    /// A sequence write carries in-place operations only: no
    /// `Replace.before` capture happens (the full serialization of the
    /// base value is never paid).
    #[test]
    fn sequence_write_carries_inplace_operations() {
        let (store, _client, channel) = setup();
        let out = channel
            .sync_write(track!(&store, |d| d.blocks.push("1".to_string())))
            .expect("seq write must succeed");
        assert!(
            out.commit
                .txns
                .iter()
                .all(|t| matches!(t.kind, crate::Changed::Inplace(_))),
            "sequence writes must carry in-place operations: {:?}",
            out.commit.txns
        );
    }

    /// A replace write captures the pre-mutation value as the
    /// replacement's `before` — the undo inverse's input.
    #[test]
    fn replace_write_captures_before() {
        let (store, _client, channel) = setup();
        let out = channel
            .sync_write(track!(&store, |d| d.title = String::from("new")))
            .expect("replace write must succeed");
        assert_eq!(out.commit.txns.len(), 1);
        assert_eq!(
            out.commit.txns[0].kind,
            crate::Changed::Replace {
                before: Some(json!("old")),
                after: Some(json!("new")),
            },
            "the replace's before is the pre-mutation field value"
        );
    }

    /// A combined write (sequence + replace in one body) carries the
    /// in-place operations and the replacement side by side.
    #[test]
    fn combined_write_carries_both_kinds() {
        let (store, _client, channel) = setup();
        let out = channel
            .sync_write(track!(&store, |d| {
                d.blocks.push("1".to_string());
                d.title = String::from("new");
            }))
            .expect("combined write must succeed");
        assert!(out.commit.txns.len() >= 2);
        let replace = out
            .commit
            .txns
            .iter()
            .find(|t| matches!(t.kind, crate::Changed::Replace { .. }))
            .expect("combined write carries a replace");
        assert_eq!(
            replace.kind,
            crate::Changed::Replace {
                before: Some(json!("old")),
                after: Some(json!("new")),
            }
        );
        assert!(
            out.commit
                .txns
                .iter()
                .any(|t| matches!(t.kind, crate::Changed::Inplace(_))),
            "combined write carries the in-place operation too"
        );
    }
}
