//! Storage for unconfirmed transactions (crash recovery).
//!
//! The sync core never touches storage: it emits `Persist` / `Remove`
//! commands that an adapter executes against a [`TransactionCache`].
//! [`RedbCache`] is the reference implementation (a single redb
//! database file); an adapter may substitute any other store (memory,
//! SQLite, ...) without touching the core or the driver.
//!
//! The atomic persistence unit is one [`CommitBatch`](crate::CommitBatch):
//! a batch is persisted, loaded, and removed as a whole, so a crash can
//! never leave half of a network request on disk, and recovery never
//! re-queues a truncated batch.

use crate::CommitBatch;
use redb::{
    Database, DatabaseError, Durability, ReadableDatabase, ReadableTable, TableDefinition,
    TableError,
};
use std::path::Path;

/// Storage for transactions that must survive a crash.
///
/// Implementations decide where batches live — files, memory, a
/// database, anything that can round-trip a [`CommitBatch`]. Persisting
/// a batch must be atomic (a reader never observes a partial batch), and
/// loading must return batches in a deterministic order (recovery
/// rebuilds the queue FIFO from this order).
pub trait TransactionCache: Send + Sync {
    /// Persist a batch durably (or overwrite it after a cancellation).
    fn persist_batch(
        &self,
        batch: &CommitBatch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Load all persisted batches (startup recovery), in batch order.
    fn load_batches(&self) -> Result<Vec<CommitBatch>, Box<dyn std::error::Error + Send + Sync>>;

    /// Remove a batch that no longer needs persistence (confirmed,
    /// rejected, or cancelled).
    fn remove_batch(&self, id: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Persist the client's inbound anchor (the highest delta sync id
    /// applied). Recovery resumes polling from the anchor, so the
    /// server only needs to retain deltas after it.
    fn save_anchor(&self, sync_id: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Load the persisted anchor, if any. `None` means the client has
    /// never applied a delta (bootstrap polling from 0).
    fn load_anchor(&self) -> Result<Option<u64>, Box<dyn std::error::Error + Send + Sync>>;

    /// Persist the model set the client last saw as authoritative (the
    /// bootstrap snapshot's models). Recovery uses it to distinguish
    /// "a model was removed on the server" (in the set, absent from the
    /// next snapshot → discard its unsynced changes) from "a model was
    /// created offline" (never in the set → keep and send). Stored with
    /// the anchor so the two advance together.
    fn save_known_models(
        &self,
        models: &[String],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Load the last authoritative model set, if any.
    fn load_known_models(
        &self,
    ) -> Result<Option<Vec<String>>, Box<dyn std::error::Error + Send + Sync>>;
}

/// A redb-backed cache for pending batches.
///
/// Two tables in a single redb database file: pending batches
/// (`batch id` → serialized JSON, one row per batch) and the client's
/// inbound anchor (last applied delta sync id, one meta row). redb
/// commits are ACID and its recovery is crash-safe, so a crash never
/// leaves a partial batch behind. The batch B-tree iterates rows in
/// batch-id order, which is exactly the FIFO order recovery needs.
///
/// The crash-safety contract matches the file cache this store
/// replaces: a process crash never loses or corrupts a batch. Persist
/// commits are non-durable ([`Durability::None`]); in redb 4.1.x this
/// still issues the writes to the OS page cache (only the fsync is
/// skipped), so a process crash with the OS alive keeps the data — but
/// this is **current behavior, not a contract**: redb's documentation
/// states that non-durable commits "will not be persisted to disk",
/// and its own TODO notes the page-cache write may be dropped for
/// speed. The `redb` version is pinned to `=4.1.0` so this behavior is
/// reproducible; a redb upgrade must re-verify it. A power loss rolls
/// the cache back to the last durable commit — for a None-only
/// session, to an empty cache. The lost batches are unconfirmed ones,
/// and a restart simply re-syncs what remains; the server deduplicates
/// by transaction id. Deployments that must survive power loss should
/// use a different [`TransactionCache`] implementation.
///
/// Removal is always best-effort ([`Durability::None`]): a batch is
/// removed only after it is confirmed, rejected, or cancelled, so a
/// removal lost to a crash just leaves the batch in the cache. A
/// restart re-sends it and the server deduplicates by transaction id.
pub struct RedbCache {
    db: Database,
}

/// The table holding one row per pending batch.
const BATCHES: TableDefinition<u64, Vec<u8>> = TableDefinition::new("batches");

/// The table holding the client's inbound anchor (last applied delta).
const META: TableDefinition<&'static str, u64> = TableDefinition::new("meta");

/// The table holding blob meta values (the known-model set).
const META_BLOB: TableDefinition<&'static str, Vec<u8>> = TableDefinition::new("meta_blob");

/// The meta key for the inbound anchor.
const ANCHOR_KEY: &str = "last_sync_id";

/// The meta-blob key for the last authoritative model set.
const KNOWN_MODELS_KEY: &str = "known_models";

impl RedbCache {
    /// Open (or create) a redb cache file.
    ///
    /// Commits are process-crash safe but not power-loss safe, and the
    /// page-cache behavior of [`Durability::None`] is pinned to the
    /// redb `=4.1.0` version; see [`RedbCache`] for the full contract.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        let db = Database::create(path)?;
        Ok(Self { db })
    }
}

impl TransactionCache for RedbCache {
    fn persist_batch(
        &self,
        batch: &CommitBatch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut write = self.db.begin_write()?;
        {
            let mut table = write.open_table(BATCHES)?;
            table.insert(batch.id, serde_json::to_vec(batch)?)?;
        }
        write.set_durability(Durability::None)?;
        write.commit()?;
        Ok(())
    }

    fn load_batches(&self) -> Result<Vec<CommitBatch>, Box<dyn std::error::Error + Send + Sync>> {
        let read = self.db.begin_read()?;
        match read.open_table(BATCHES) {
            Ok(table) => {
                let mut batches = Vec::new();
                for entry in table.iter()? {
                    let (_, value) = entry?;
                    batches.push(serde_json::from_slice(&value.value())?);
                }
                Ok(batches)
            }
            // A cache that never persisted a batch has no batches table.
            Err(TableError::TableDoesNotExist(_)) => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }

    fn remove_batch(&self, id: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut write = self.db.begin_write()?;
        {
            let mut table = write.open_table(BATCHES)?;
            table.remove(id)?;
        }
        // Best-effort cleanup: a removal lost to a crash only leaves a
        // fully-resolved batch in the cache, which a restart re-sends
        // and the server deduplicates by transaction id. Skipping the
        // fsync halves the durable commit cost.
        write.set_durability(Durability::None)?;
        write.commit()?;
        Ok(())
    }

    fn save_anchor(&self, sync_id: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut write = self.db.begin_write()?;
        {
            let mut table = write.open_table(META)?;
            table.insert(ANCHOR_KEY, sync_id)?;
        }
        // Same best-effort class as batch removal: a lost anchor write
        // only repeats polling from an older anchor, and applying an
        // already-seen delta is idempotent (the anchor only advances).
        write.set_durability(Durability::None)?;
        write.commit()?;
        Ok(())
    }

    fn load_anchor(&self) -> Result<Option<u64>, Box<dyn std::error::Error + Send + Sync>> {
        let read = self.db.begin_read()?;
        match read.open_table(META) {
            Ok(table) => Ok(table.get(ANCHOR_KEY)?.map(|v| v.value())),
            // A cache that never persisted an anchor has no meta table.
            Err(TableError::TableDoesNotExist(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn save_known_models(
        &self,
        models: &[String],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut write = self.db.begin_write()?;
        {
            let mut table = write.open_table(META_BLOB)?;
            table.insert(KNOWN_MODELS_KEY, serde_json::to_vec(models)?)?;
        }
        write.set_durability(Durability::None)?;
        write.commit()?;
        Ok(())
    }

    fn load_known_models(
        &self,
    ) -> Result<Option<Vec<String>>, Box<dyn std::error::Error + Send + Sync>> {
        let read = self.db.begin_read()?;
        match read.open_table(META_BLOB) {
            Ok(table) => match table.get(KNOWN_MODELS_KEY)? {
                Some(v) => Ok(Some(serde_json::from_slice(&v.value())?)),
                None => Ok(None),
            },
            Err(TableError::TableDoesNotExist(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}
