//! The sync pipeline: observation lowering, queue, client, server,
//! transport, crash recovery, rebase and writeback.
//!
//! [`SyncSink`] implements the core [`Sink`](muon::observe::Sink)
//! protocol: a flush turns observation events into a [`SyncChanges`]
//! stream. The remaining modules consume or transport that stream.
//! This layer depends on the shared wire types and the crdt layer.

// The ops/sink internals are referenced by the crdt layer and the
// test layer (apply helpers, transaction lowering), so they are
// crate-visible.
mod cache;
mod driver;
pub(crate) mod ops;
mod queue;
mod rebase;
mod run;
mod server;
pub(crate) mod sink;
mod store;
mod transport;
mod writeback;

pub use cache::{RedbCache, TransactionCache};
pub use driver::{SyncClient, SyncCommand, SyncDriver};
pub use queue::{
    AwaitingCommit, CancelOutcome, Commit, CommitBatch, TransactionQueue, DEFAULT_IN_FLIGHT_MAX,
};
pub use rebase::{reconcile, DeltaAction, DeltaPacket, ReconcileOutcome};
pub use run::{sync_loop, sync_step, SyncLoopError};
pub use server::SyncServer;
pub use sink::SyncSink;
pub use store::{SyncChannel, SyncWriteError, WriteOutcome};
pub use transport::{NoopTransport, PollOutcome, SendError, SendResponse, SyncTransport};
pub use writeback::{DeltaApplyError, RemoteView};
