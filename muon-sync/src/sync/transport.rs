//! Transport abstraction for sending transactions and receiving delta packets.

use crate::{BatchKey, DeltaPacket, SyncId, Transaction};

/// Outcome of a [`SyncTransport::poll_deltas`] call.
#[derive(Debug, Clone, PartialEq)]
pub enum PollOutcome {
    /// Bootstrap (first start or lost anchor): the complete
    /// authoritative state of every known model at `sync_id`, plus the
    /// report-only outcomes of any pending batches the server applied
    /// while serving this poll. The snapshot is the base; the reports
    /// carry `rejected` and `applied_batch` but no state actions.
    Snapshot {
        /// The server's `lastSyncId` at snapshot time.
        sync_id: SyncId,
        /// Complete authoritative value of each known model.
        models: Vec<(String, serde_json::Value)>,
        /// Report-only packets for batches applied during this poll.
        reports: Vec<DeltaPacket>,
    },
    /// Incremental deltas past the client's anchor, ascending sync id.
    Deltas(Vec<DeltaPacket>),
    /// The client's anchor fell out of the server's retention window.
    /// The client must abandon its anchor and poll again with `None`.
    ResetRequired,
}

/// Why a [`SyncTransport::send`] failed.
///
/// The distinction matters for recovery: a network failure is retried
/// (the request may or may not have reached the server), while a
/// rejection is final — the batch is rolled back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendError {
    /// The request never reached the server, or no response arrived.
    /// Safe to retry; the server deduplicates by transaction id.
    Network(String),
    /// The server processed the batch and rejected it as a whole (e.g.
    /// authentication or protocol failure). Every transaction is rolled
    /// back.
    Rejected(String),
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendError::Network(msg) => write!(f, "network: {msg}"),
            SendError::Rejected(msg) => write!(f, "rejected: {msg}"),
        }
    }
}

impl std::error::Error for SendError {}

/// Delivery receipt for a [`SyncTransport::send`].
///
/// `send` only confirms that the server accepted the batch for
/// processing. Application is asynchronous: the server applies batches
/// in receive order and reports each applied batch through
/// [`DeltaPacket::applied_batch`]; application-level rejections arrive
/// through [`DeltaPacket::rejected`]. The client never waits for a
/// per-batch acknowledgment, so throughput is bounded by the server's
/// apply rate, not by the round-trip time.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SendResponse {
    /// The batch was already applied by the server before this resend
    /// (crash-recovery redelivery). Contains the original sync id at
    /// which it was applied; the client completes the batch as soon as
    /// its anchor reaches that threshold.
    pub deduped_at: Option<SyncId>,
}

/// Pluggable transport layer for the sync engine.
///
/// Implementations can wrap WebSocket connections, HTTP polling,
/// or any other mechanism that can send transactions and receive delta packets.
pub trait SyncTransport: Send + Sync {
    /// Deliver a batch of transactions to the server.
    ///
    /// `batch_id` is the client's opaque batch identifier; the server
    /// echoes it back through [`DeltaPacket::applied_batch`] when the
    /// batch has been applied, so the client can match the report to
    /// the batch. Returns immediately once the server has accepted the
    /// batch for processing — it does not wait for application. The
    /// server must apply batches in receive order and must deduplicate
    /// redelivered transactions by id (returning the original
    /// application sync id through [`SendResponse::deduped_at`]). A
    /// [`SendError::Network`] is retried (idempotent by transaction
    /// id); a [`SendError::Rejected`] rolls the whole batch back.
    fn send(
        &self,
        batch_key: BatchKey,
        transactions: &[Transaction],
    ) -> impl std::future::Future<Output = Result<SendResponse, SendError>> + Send;

    /// Poll for new delta packets since the given sync id.
    ///
    /// `since` is the client's inbound anchor. `None` is the
    /// **bootstrap case** (first start or a lost anchor): the server
    /// must return the full snapshot — the complete value of every
    /// known model at its current `lastSyncId` — instead of replaying
    /// the entire delta history. The client treats the snapshot as its
    /// base and continues with incremental polling from that `sync_id`.
    ///
    /// For `Some(since)` the server returns the deltas after `since`;
    /// it must retain deltas at least as far back as every client's
    /// anchor, and answer [`PollOutcome::ResetRequired`] when the
    /// anchor fell out of the window.
    fn poll_deltas(
        &self,
        since: Option<SyncId>,
    ) -> impl std::future::Future<Output = Result<PollOutcome, String>> + Send;
}

/// A no-op transport for testing or offline-first scenarios.
pub struct NoopTransport;

impl SyncTransport for NoopTransport {
    async fn send(
        &self,
        _batch_key: BatchKey,
        _transactions: &[Transaction],
    ) -> Result<SendResponse, SendError> {
        Ok(SendResponse::default())
    }
    async fn poll_deltas(&self, _since: Option<SyncId>) -> Result<PollOutcome, String> {
        Ok(PollOutcome::Deltas(Vec::new()))
    }
}
