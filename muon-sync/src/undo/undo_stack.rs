//! Optional application-side undo history, in the LSE `UndoQueue` shape.
//!
//! The engine keeps no history of its own. It exposes the primitives —
//! [`Commit::invert`], [`SyncDriver::undo`], [`SyncDriver::redo`] — and
//! leaves stack policy to the application. This module is the reference
//! policy: a bounded LIFO history recorded at *enqueue* time, so an
//! edit is undoable immediately, without waiting for server
//! confirmation (local-first).
//!
//! Why enqueue-time recording is correct: the client sends batches in
//! FIFO order and the server applies them in receive order, so an undo
//! enqueued right after its change is applied after it — the pair
//! cancels. A rebase in between re-captures `before` against the
//! latest authoritative value, so the inverse stays correct.

use crate::{Commit, SyncDriver, Transaction};

/// A bounded undo/redo history of changes the application wrote.
///
/// Record at enqueue time — right after [`SyncChannel::sync_write`](crate::SyncChannel::sync_write)
/// returns — so undo works before the server confirms the change.
pub struct UndoStack {
    undo: Vec<Commit>,
    redo: Vec<Commit>,
    limit: usize,
}

impl UndoStack {
    /// Create an empty history. `limit` caps the undo stack, dropping
    /// the oldest entries; `0` means unbounded.
    pub fn new(limit: usize) -> Self {
        Self {
            undo: Vec::new(),
            redo: Vec::new(),
            limit,
        }
    }

    /// Record a change just enqueued. Clears the redo stack: a new
    /// branch invalidates redo.
    pub fn record(&mut self, commit: &Commit) {
        self.undo.push(commit.clone());
        self.redo.clear();
        if self.limit > 0 && self.undo.len() > self.limit {
            self.undo.remove(0);
        }
    }

    /// Undo the most recent change: build its inverse and enqueue it
    /// with refreshed identities. Returns the fresh inverse
    /// transactions for optimistic application, or `None` when the
    /// history is empty.
    pub fn undo(&mut self, driver: &mut SyncDriver) -> Option<Vec<Transaction>> {
        let commit = self.undo.pop()?;
        let fresh = driver.undo(&commit);
        self.redo.push(commit);
        Some(fresh)
    }

    /// Redo the most recently undone change: enqueue its original
    /// transactions with refreshed identities.
    pub fn redo(&mut self, driver: &mut SyncDriver) -> Option<Vec<Transaction>> {
        let commit = self.redo.pop()?;
        let fresh = driver.redo(&commit);
        self.undo.push(commit);
        Some(fresh)
    }

    /// Number of recorded changes (undoable).
    pub fn undo_len(&self) -> usize {
        self.undo.len()
    }

    /// Number of undone changes (redoable).
    pub fn redo_len(&self) -> usize {
        self.redo.len()
    }

    /// Iterate the recorded changes, newest first (the order `undo`
    /// pops them).
    pub fn iter(&self) -> impl Iterator<Item = &Commit> {
        self.undo.iter().rev()
    }

    /// Iterate the undone changes, newest first (the order `redo`
    /// pops them).
    pub fn iter_redo(&self) -> impl Iterator<Item = &Commit> {
        self.redo.iter().rev()
    }
}

impl<'a> IntoIterator for &'a UndoStack {
    type Item = &'a Commit;
    type IntoIter = std::iter::Rev<std::slice::Iter<'a, Commit>>;

    fn into_iter(self) -> Self::IntoIter {
        self.undo.iter().rev()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Changed, SyncClient, TxnId};
    use serde_json::json;

    fn commit(seq: u64, value: &str) -> Commit {
        Commit {
            ordinal: 0,
            txns: vec![crate::Transaction {
                id: TxnId {
                    incarnation: 1,
                    seq,
                },
                client_id: 1,
                model_id: "issue".into(),
                path: vec![crate::PathSegment::String("title".into())],
                kind: Changed::Replace {
                    before: Some(json!("Hello")),
                    after: Some(json!(value)),
                },
                timestamp: seq,
            }],
        }
    }

    #[test]
    fn record_caps_at_limit() {
        let mut m = UndoStack::new(2);
        m.record(&commit(1, "a"));
        m.record(&commit(2, "b"));
        m.record(&commit(3, "c"));
        assert_eq!(m.undo_len(), 2, "oldest entry dropped");
    }

    #[test]
    fn record_clears_redo() {
        let mut m = UndoStack::new(0);
        m.record(&commit(1, "a"));
        let mut client = SyncClient::new(1);
        assert!(m.undo(client.driver()).is_some());
        assert_eq!(m.redo_len(), 1);
        m.record(&commit(2, "b"));
        assert_eq!(m.redo_len(), 0, "new branch invalidates redo");
    }

    #[test]
    fn undo_redo_roundtrip() {
        let mut m = UndoStack::new(0);
        m.record(&commit(1, "a"));
        let mut client = SyncClient::new(1);
        let inverses = m.undo(client.driver()).expect("undoable");
        assert_eq!(inverses.len(), 1);
        assert_eq!(m.undo_len(), 0);
        assert_eq!(m.redo_len(), 1);
        let redone = m.redo(client.driver()).expect("redoable");
        assert_eq!(redone.len(), 1);
        assert_eq!(m.undo_len(), 1);
        assert_eq!(m.redo_len(), 0);
    }

    #[test]
    fn undo_on_empty_history_is_none() {
        let mut m = UndoStack::new(0);
        let mut client = SyncClient::new(1);
        assert!(m.undo(client.driver()).is_none());
        assert!(m.redo(client.driver()).is_none());
    }

    #[test]
    fn iter_newest_first() {
        let mut m = UndoStack::new(0);
        m.record(&commit(1, "a"));
        m.record(&commit(2, "b"));
        let stamps: Vec<u64> = m.iter().map(|c| c.txns[0].timestamp).collect();
        assert_eq!(stamps, vec![2, 1], "newest first, matching pop order");
    }

    #[test]
    fn iter_redo_after_undo() {
        let mut m = UndoStack::new(0);
        m.record(&commit(1, "a"));
        m.record(&commit(2, "b"));
        let mut client = SyncClient::new(1);
        m.undo(client.driver()).expect("undoable");
        let stamps: Vec<u64> = m.iter_redo().map(|c| c.txns[0].timestamp).collect();
        assert_eq!(stamps, vec![2], "the undone change is redoable");
    }

    #[test]
    fn into_iter_matches_iter() {
        let mut m = UndoStack::new(0);
        m.record(&commit(1, "a"));
        let stamps: Vec<u64> = (&m).into_iter().map(|c| c.txns[0].timestamp).collect();
        assert_eq!(stamps, vec![1]);
    }
}
