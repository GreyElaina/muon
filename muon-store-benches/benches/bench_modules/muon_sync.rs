//! Windowed sync pipeline benchmarks.
//!
//! Every iteration drives a fixed number of transactions (TXNS_PER_ITER)
//! through the full pipeline — tracked write, enqueue, collect, persist,
//! send, resolve, poll, complete — so per-iteration times are directly
//! comparable across benches. The dimensions:
//!
//! - `flush`: transactions per batch (writes between `sync_step` calls).
//!   The in-flight window carries several batches per network round
//!   trip, so a larger flush amortizes the per-batch overhead (persist +
//!   send + poll) over more transactions.
//! - `store`: the persist gate is mandatory before any send; this
//!   quantifies its cost across backings. `FileStore` (one JSON file
//!   per batch, no fsync) is the file-cache baseline, `SingleFileStore`
//!   (whole-cache rewrite per mutation) is the narrowest design, and
//!   `RedbCache` is the shipped cache (incremental writes, same
//!   crash-safety class as the baselines).
//! - `rtt`: simulated network round trip in the fake transport. With a
//!   windowed in-flight pipeline, sends are delivery-only and overlap:
//!   throughput is bounded by the persist/apply rate, not by `rtt` per
//!   batch.

use criterion::{black_box, Criterion};
use muon_store::{track, Store};
use muon_sync::{
    insert_after, sync_step, BatchKey, CommitBatch, CrdtVec, DeltaAction, DeltaPacket, ItemId,
    ItemRange, MovableVec, PollOutcome, RedbCache, RemoteView, SendError, SendResponse,
    SyncChannel, SyncClient, SyncTransport, Transaction, TransactionCache, DEFAULT_IN_FLIGHT_MAX,
};
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::types::Small;

/// Transactions per measured iteration; identical across all benches.
const TXNS_PER_ITER: usize = 512;

/// Write through the sync pipeline (tracked write → queue → publish).
fn synced_write_small(channel: &SyncChannel, store: &Store<Small>, v: i32) {
    channel
        .sync_write(track!(store, |s| s.value = v))
        .expect("sync write must succeed");
}

/// A transport that accepts every batch immediately (delivery-only) and
/// reports it applied on the next poll, optionally after a simulated
/// network round trip per delivery.
struct EchoTransport {
    rtt: Duration,
    next_sync_id: AtomicU64,
    pending: Mutex<VecDeque<(BatchKey, Vec<Transaction>)>>,
}

impl EchoTransport {
    fn new(rtt: Duration) -> Self {
        Self {
            rtt,
            next_sync_id: AtomicU64::new(0),
            pending: Mutex::new(VecDeque::new()),
        }
    }
}

impl SyncTransport for EchoTransport {
    async fn send(
        &self,
        batch_key: BatchKey,
        txns: &[Transaction],
    ) -> Result<SendResponse, SendError> {
        // Asynchronous round trip: the future suspends (Pending) during
        // the delay, so a window of batches delivered through
        // `join_all` overlaps in real time — exactly how an async
        // network transport behaves. A blocking `thread::sleep` here
        // would serialize the deliveries (the whole point of the
        // window is lost in the measurement).
        if !self.rtt.is_zero() {
            tokio::time::sleep(self.rtt).await;
        }
        self.pending
            .lock()
            .unwrap()
            .push_back((batch_key, txns.to_vec()));
        Ok(SendResponse { deduped_at: None })
    }

    async fn poll_deltas(&self, since: Option<u64>) -> Result<PollOutcome, String> {
        // Apply the pending batches (asynchronous application): one
        // confirming delta per batch, reporting `applied_batch`.
        let mut packets = Vec::new();
        let mut pending = self.pending.lock().unwrap();
        let mut next = self.next_sync_id.load(Ordering::Relaxed) + 1;
        while let Some((batch_key, _)) = pending.pop_front() {
            let sync_id = next;
            next += 1;
            if sync_id > since.unwrap_or(0) {
                packets.push(DeltaPacket {
                    sync_id,
                    actions: vec![DeltaAction::Value {
                        model_id: "small".into(),
                        value: serde_json::json!({"value": 0}),
                    }],
                    applied_batch: Some(batch_key),
                    rejected: vec![],
                });
            }
        }
        self.next_sync_id.store(next - 1, Ordering::Relaxed);
        Ok(PollOutcome::Deltas(packets))
    }
}

/// A `TransactionCache` that keeps batches in memory: the persist gate
/// without any disk I/O.
struct MemStore {
    batches: Mutex<Vec<CommitBatch>>,
    anchor: Mutex<Option<u64>>,
}

impl MemStore {
    fn new() -> Self {
        Self {
            batches: Mutex::new(Vec::new()),
            anchor: Mutex::new(None),
        }
    }
}

impl TransactionCache for MemStore {
    fn persist_batch(
        &self,
        batch: &CommitBatch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.batches.lock().unwrap().push(batch.clone());
        Ok(())
    }

    fn load_batches(&self) -> Result<Vec<CommitBatch>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.batches.lock().unwrap().clone())
    }

    fn remove_batch(&self, id: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.batches.lock().unwrap().retain(|b| b.id != id);
        Ok(())
    }

    fn save_anchor(&self, sync_id: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        *self.anchor.lock().unwrap() = Some(sync_id);
        Ok(())
    }

    fn load_anchor(&self) -> Result<Option<u64>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(*self.anchor.lock().unwrap())
    }

    fn save_known_models(
        &self,
        _models: &[String],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    fn load_known_models(
        &self,
    ) -> Result<Option<Vec<String>>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(None)
    }
}

/// A `TransactionCache` that rewrites the whole cache as one JSON file
/// on every mutation (temp file + rename). The dataset is small — the
/// number of unconfirmed batches in the window — so the rewrite
/// cost is bounded by cache size, not by operation count.
struct SingleFileStore {
    path: std::path::PathBuf,
}

impl SingleFileStore {
    fn new(path: std::path::PathBuf) -> Self {
        Self { path }
    }

    fn read_all(&self) -> Result<Vec<CommitBatch>, Box<dyn std::error::Error + Send + Sync>> {
        match std::fs::read(&self.path) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }

    fn write_all(
        &self,
        batches: &[CommitBatch],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let tmp = self.path.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_vec(batches)?)?;
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

impl TransactionCache for SingleFileStore {
    fn persist_batch(
        &self,
        batch: &CommitBatch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut all = self.read_all()?;
        match all.iter_mut().find(|b| b.id == batch.id) {
            Some(existing) => *existing = batch.clone(),
            None => all.push(batch.clone()),
        }
        all.sort_by_key(|b| b.id);
        self.write_all(&all)
    }

    fn load_batches(&self) -> Result<Vec<CommitBatch>, Box<dyn std::error::Error + Send + Sync>> {
        let mut all = self.read_all()?;
        all.sort_by_key(|b| b.id);
        Ok(all)
    }

    fn remove_batch(&self, id: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut all = self.read_all()?;
        all.retain(|b| b.id != id);
        self.write_all(&all)
    }

    fn save_anchor(&self, sync_id: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        std::fs::write(self.path.with_extension("anchor"), sync_id.to_string())?;
        Ok(())
    }

    fn load_anchor(&self) -> Result<Option<u64>, Box<dyn std::error::Error + Send + Sync>> {
        match std::fs::read_to_string(self.path.with_extension("anchor")) {
            Ok(s) => Ok(s.trim().parse().ok()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn save_known_models(
        &self,
        _models: &[String],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    fn load_known_models(
        &self,
    ) -> Result<Option<Vec<String>>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(None)
    }
}

/// A `TransactionCache` that writes one JSON file per batch (temp +
/// rename, no fsync): the file-backed baseline that `RedbCache`
/// replaces. Kept in the bench so the new backing can be measured
/// against what came before.
struct FileStore {
    dir: std::path::PathBuf,
}

impl FileStore {
    fn open(dir: std::path::PathBuf) -> Self {
        std::fs::create_dir_all(&dir).expect("create bench cache dir");
        Self { dir }
    }
}

impl TransactionCache for FileStore {
    fn persist_batch(
        &self,
        batch: &CommitBatch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let path = self.dir.join(format!("{}.json", batch.id));
        let tmp = self.dir.join(format!("{}.tmp", batch.id));
        let json = serde_json::to_string_pretty(batch)?;
        std::fs::write(&tmp, json)?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    fn load_batches(&self) -> Result<Vec<CommitBatch>, Box<dyn std::error::Error + Send + Sync>> {
        let mut batches: Vec<CommitBatch> = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !name.ends_with(".json") || name.ends_with(".tmp") {
                continue;
            }
            let json = std::fs::read_to_string(&path)?;
            batches.push(serde_json::from_str(&json)?);
        }
        batches.sort_by_key(|b| b.id);
        Ok(batches)
    }

    fn remove_batch(&self, id: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let path = self.dir.join(format!("{id}.json"));
        let _ = std::fs::remove_file(&path);
        Ok(())
    }

    fn save_anchor(&self, sync_id: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        std::fs::write(self.dir.join("anchor"), sync_id.to_string())?;
        Ok(())
    }

    fn load_anchor(&self) -> Result<Option<u64>, Box<dyn std::error::Error + Send + Sync>> {
        match std::fs::read_to_string(self.dir.join("anchor")) {
            Ok(s) => Ok(s.trim().parse().ok()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn save_known_models(
        &self,
        _models: &[String],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    fn load_known_models(
        &self,
    ) -> Result<Option<Vec<String>>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(None)
    }
}

/// Shares one open `RedbCache` across benchmark iterations. Opening and
/// closing the redb file is a process-lifecycle cost (the close alone
/// takes tens of milliseconds), so it must not be attributed to every
/// iteration; a real deployment opens once at startup.
struct SharedCache(Arc<RedbCache>);

impl TransactionCache for SharedCache {
    fn persist_batch(
        &self,
        batch: &CommitBatch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.0.persist_batch(batch)
    }

    fn load_batches(&self) -> Result<Vec<CommitBatch>, Box<dyn std::error::Error + Send + Sync>> {
        self.0.load_batches()
    }

    fn remove_batch(&self, id: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.0.remove_batch(id)
    }

    fn save_anchor(&self, sync_id: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.0.save_anchor(sync_id)
    }

    fn load_anchor(&self) -> Result<Option<u64>, Box<dyn std::error::Error + Send + Sync>> {
        self.0.load_anchor()
    }

    fn save_known_models(
        &self,
        models: &[String],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.0.save_known_models(models)
    }

    fn load_known_models(
        &self,
    ) -> Result<Option<Vec<String>>, Box<dyn std::error::Error + Send + Sync>> {
        self.0.load_known_models()
    }
}

/// One full sync client wired for benchmarking: queue, channel, store,
/// remote, transport, and transaction store.
struct Pipeline<S: TransactionCache> {
    client: SyncClient,
    channel: SyncChannel,
    store: Store<Small>,
    remote: RemoteView,
    transport: EchoTransport,
    store_txn: S,
    /// The in-flight window this pipeline was built with.
    window: usize,
}

impl<S: TransactionCache> Pipeline<S> {
    fn new(store_txn: S, rtt: Duration, window: usize) -> Self {
        let client = SyncClient::with_in_flight_max(1, window);
        let channel = client.channel("small");
        let store = Store::new(Small { value: 42 });
        Self {
            client,
            channel,
            store,
            remote: RemoteView::new(),
            transport: EchoTransport::new(rtt),
            store_txn,
            window,
        }
    }

    /// Drive `flush` tracked writes, then one `sync_step` per batch until
    /// all TXNS_PER_ITER transactions are confirmed.
    fn round(&mut self, rt: &tokio::runtime::Runtime, flush: usize, next: &mut i32) {
        debug_assert_eq!(TXNS_PER_ITER % flush, 0);
        let batches = TXNS_PER_ITER / flush;
        // Produce every batch first (each flush boundary closes one),
        // then drain through sync steps: a step delivers up to the
        // in-flight window concurrently and completes them from the
        // polled applied reports. With a windowed pipeline the total
        // delivery time is `ceil(batches / window) × rtt` — the round
        // trip no longer multiplies per batch.
        for _ in 0..batches {
            for _ in 0..flush {
                *next = next.wrapping_add(1);
                synced_write_small(&self.channel, &self.store, *next);
            }
            self.client.queue().lock().unwrap().collect();
        }
        let mut steps = 0;
        loop {
            rt.block_on(sync_step(
                &mut self.remote,
                self.client.driver(),
                &self.transport,
                Some(&self.store_txn),
                |_, v| {
                    black_box(v);
                },
                |_| {},
            ))
            .expect("pipeline step must succeed");
            steps += 1;
            if self.client.queue().lock().unwrap().is_idle() {
                break;
            }
        }
        debug_assert_eq!(
            steps,
            batches.div_ceil(self.window),
            "window drains in ceil(batches/window) steps"
        );
    }
}

pub fn bench_sync(c: &mut Criterion) {
    let mut g = c.benchmark_group("muon-sync/pipeline");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();

    // ── Flush coalescing: transactions per batch ────────────────────
    // In-memory store, zero latency: isolates the local pipeline cost.
    for flush in [1usize, 8, 64, 512] {
        g.bench_function(format!("flush_{flush}"), |b| {
            b.iter_batched(
                || Pipeline::new(MemStore::new(), Duration::ZERO, DEFAULT_IN_FLIGHT_MAX),
                |mut pipe| {
                    let mut next = 0i32;
                    pipe.round(&rt, flush, &mut next);
                    black_box(next);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    // ── Persist gate: file vs redb vs in-memory store ─────────────
    // The file baseline (one JSON file per batch, no fsync) is the
    // store `RedbCache` replaces. `SingleFileStore` (whole-cache
    // rewrite per mutation) is the narrowest possible design; redb
    // wins because its writes scale with the change, not the cache.
    let flush = 64usize;

    let file_dir =
        std::env::temp_dir().join(format!("muon-sync-bench-file-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&file_dir);
    g.bench_function("persist_file_flush_64", |b| {
        b.iter_batched(
            || {
                Pipeline::new(
                    FileStore::open(file_dir.clone()),
                    Duration::ZERO,
                    DEFAULT_IN_FLIGHT_MAX,
                )
            },
            |mut pipe| {
                let mut next = 0i32;
                pipe.round(&rt, flush, &mut next);
                black_box(next);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // Whole-cache rewrite on every mutation: the narrowest possible
    // design. Under the windowed pipeline the cache holds a few
    // batches, so each rewrite is a few kilobytes.
    let single_path = std::env::temp_dir().join(format!(
        "muon-sync-bench-singlefile-{}.json",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&single_path);
    g.bench_function("persist_singlefile_flush_64", |b| {
        // `PerIteration`: the store is not safe under concurrent
        // instances (no lock, whole-file rewrite).
        b.iter_batched(
            || {
                Pipeline::new(
                    SingleFileStore::new(single_path.clone()),
                    Duration::ZERO,
                    DEFAULT_IN_FLIGHT_MAX,
                )
            },
            |mut pipe| {
                let mut next = 0i32;
                pipe.round(&rt, flush, &mut next);
                black_box(next);
            },
            criterion::BatchSize::PerIteration,
        );
    });

    let redb_path =
        std::env::temp_dir().join(format!("muon-sync-bench-redb-{}.redb", std::process::id()));
    let _ = std::fs::remove_file(&redb_path);
    // Open once: the redb file is held for the whole benchmark, like a
    // real deployment holds it for the process lifetime. Per-iteration
    // setups share it (each gets a fresh client/store).
    let redb_cache = Arc::new(RedbCache::open(&redb_path).expect("open bench cache"));
    g.bench_function("persist_redb_flush_64", |b| {
        b.iter_batched(
            || {
                Pipeline::new(
                    SharedCache(redb_cache.clone()),
                    Duration::ZERO,
                    DEFAULT_IN_FLIGHT_MAX,
                )
            },
            |mut pipe| {
                let mut next = 0i32;
                pipe.round(&rt, flush, &mut next);
                black_box(next);
            },
            criterion::BatchSize::PerIteration,
        );
    });

    g.bench_function("persist_mem_flush_64", |b| {
        b.iter_batched(
            || Pipeline::new(MemStore::new(), Duration::ZERO, DEFAULT_IN_FLIGHT_MAX),
            |mut pipe| {
                let mut next = 0i32;
                pipe.round(&rt, flush, &mut next);
                black_box(next);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // ── Round-trip sensitivity: windowed delivery ───────────────────
    // Fixed flush of 256 and the default window; the send sleeps one
    // full RTT per batch, but the window delivers the whole in-flight
    // set concurrently.
    let flush = 256usize;
    for rtt_ms in [0u64, 1, 5, 20] {
        let rtt = Duration::from_millis(rtt_ms);
        g.bench_function(format!("rtt_{rtt_ms}ms_flush_256"), |b| {
            b.iter_batched(
                || Pipeline::new(MemStore::new(), rtt, DEFAULT_IN_FLIGHT_MAX),
                |mut pipe| {
                    let mut next = 0i32;
                    pipe.round(&rt, flush, &mut next);
                    black_box(next);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    // ── Window dimension: in-flight delivery concurrency ────────────
    // Fixed flush of 32 (16 batches of 512 transactions) and a 5 ms
    // RTT; the window bounds how many batches are delivered before
    // application confirms. Total delivery time is
    // `ceil(batches / window) × rtt` plus one pipeline step per
    // drain, so the window traces a diminishing-returns curve: 16
    // round trips at window 1, 4 at window 4, and a single round trip
    // once the window covers the whole stream. The window is a memory
    // watermark, never a correctness bound (the server applies in
    // receive order regardless).
    let flush = 32usize;
    let rtt = Duration::from_millis(5);
    for window in [1usize, 4, DEFAULT_IN_FLIGHT_MAX, 64] {
        g.bench_function(format!("window_{window}_rtt_5ms_flush_32"), |b| {
            b.iter_batched(
                || Pipeline::new(MemStore::new(), rtt, window),
                |mut pipe| {
                    let mut next = 0i32;
                    pipe.round(&rt, flush, &mut next);
                    black_box(next);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    g.finish();
}

// ═══════════════════════════════════════════════════════════════════
// Sequence benchmarks
// ═══════════════════════════════════════════════════════════════════

/// Element inserts measured per bench iteration, after pre-filling the
/// list in the setup phase.
const SEQ_INSERTS: usize = 200;

fn seq_elem(seq: u64) -> ItemId {
    ItemId {
        client_id: 0,
        incarnation: 1,
        seq,
    }
}

/// The engine's insertion curve: head/middle/tail inserts on
/// pre-filled lists. The indexed B-tree locates by position, so
/// throughput stays flat across positions and list sizes (a Vec would
/// degrade linearly on head/middle inserts). This quantifies the
/// B-tree investment at the sizes a real document reaches.
pub fn bench_seq_engine(c: &mut Criterion) {
    let mut g = c.benchmark_group("muon-sync/seq-engine");
    // `PerIteration`: rebuild the tree once per iteration. The
    // default `SmallInput` batches ~iters/10 setup results into
    // memory before timing, so several hundred MB of trees stay
    // resident at once and inflate the measured insert time.
    g.sample_size(50).measurement_time(Duration::from_secs(1));
    for size in [1_000usize, 10_000, 100_000] {
        for pos in ["head", "middle", "tail"] {
            g.bench_function(format!("insert_{pos}_{size}"), |b| {
                b.iter_batched_ref(
                    || {
                        let mut v: MovableVec<Value> = MovableVec::new();
                        let mut seq = 1u64;
                        for i in 0..size {
                            let anchor = if v.is_empty() {
                                None
                            } else {
                                v.id_at(v.len() - 1)
                            };
                            insert_after(
                                &mut v,
                                anchor,
                                ItemRange {
                                    first: seq_elem(seq),
                                    len: 1,
                                },
                                vec![json!(i)],
                            );
                            seq += 1;
                        }
                        (v, seq)
                    },
                    |(v, seq): &mut (MovableVec<Value>, u64)| {
                        for _ in 0..SEQ_INSERTS {
                            let n = v.len();
                            let at = match pos {
                                "head" => 0,
                                "middle" => n / 2,
                                _ => n,
                            };
                            let anchor = if at == 0 { None } else { v.id_at(at - 1) };
                            insert_after(
                                v,
                                anchor,
                                ItemRange {
                                    first: seq_elem(*seq),
                                    len: 1,
                                },
                                vec![json!(0)],
                            );
                            *seq += 1;
                        }
                        black_box(v.len());
                    },
                    criterion::BatchSize::PerIteration,
                );
            });

            // The plain-`Vec` baseline: the engine's indexed B-tree
            // trades small-scale and tail costs for flat head/middle
            // cost at scale. The baseline makes the crossover
            // measurable.
            g.bench_function(format!("vec_insert_{pos}_{size}"), |b| {
                b.iter_batched_ref(
                    || {
                        let mut v: Vec<Value> = Vec::new();
                        for i in 0..size {
                            v.push(json!(i));
                        }
                        v
                    },
                    |v: &mut Vec<Value>| {
                        for _ in 0..SEQ_INSERTS {
                            let n = v.len();
                            let at = match pos {
                                "head" => 0,
                                "middle" => n / 2,
                                _ => n,
                            };
                            v.insert(at, json!(0));
                        }
                        black_box(v.len());
                    },
                    criterion::BatchSize::PerIteration,
                );
            });
        }
    }
    g.finish();
}

/// A document whose only field is a `CrdtVec` — the sync write bench
/// payload.
#[derive(Clone, Debug, serde::Serialize, muon::Observe, muon_store::Track)]
struct SeqDoc {
    blocks: CrdtVec<i32>,
}

impl Default for SeqDoc {
    fn default() -> Self {
        Self {
            blocks: CrdtVec::new(),
        }
    }
}

/// One full sync client over a `CrdtVec` field: the `Pipeline` shape
/// with a sequence payload. Every write is a tracked `push`, recorded
/// by the observer, lowered to a transaction, and delivered through
/// the transport.
struct SeqPipeline {
    client: SyncClient,
    channel: SyncChannel,
    store: Store<SeqDoc>,
    remote: RemoteView,
    transport: EchoTransport,
    store_txn: MemStore,
}

impl SeqPipeline {
    fn new(rtt: Duration) -> Self {
        let client = SyncClient::new(1);
        let channel = client.channel("seq");
        let store = Store::new(SeqDoc::default());
        Self {
            client,
            channel,
            store,
            remote: RemoteView::new(),
            transport: EchoTransport::new(rtt),
            store_txn: MemStore::new(),
        }
    }

    /// Drive `flush` tracked pushes, then one `sync_step` per batch
    /// until the queue is idle.
    fn round(&mut self, rt: &tokio::runtime::Runtime, flush: usize, next: &mut i32) {
        debug_assert_eq!(TXNS_PER_ITER % flush, 0);
        let batches = TXNS_PER_ITER / flush;
        for _ in 0..batches {
            for _ in 0..flush {
                *next = next.wrapping_add(1);
                self.channel
                    .sync_write(track!(&self.store, |d| d.blocks.push(*next)))
                    .expect("seq sync write must succeed");
            }
            self.client.queue().lock().unwrap().collect();
        }
        loop {
            rt.block_on(sync_step(
                &mut self.remote,
                self.client.driver(),
                &self.transport,
                Some(&self.store_txn),
                |_, v| {
                    black_box(v);
                },
                |_| {},
            ))
            .expect("seq pipeline step must succeed");
            if self.client.queue().lock().unwrap().is_idle() {
                break;
            }
        }
    }
}

/// Sync write throughput of a `CrdtVec` field: tracked push through
/// the full pipeline (record → lower → enqueue → persist → send →
/// poll → apply) across flush sizes. The scalar bench measures the
/// same pipeline on a replace; the sequence path adds the observer's
/// record + lower step per operation.
pub fn bench_seq_sync(c: &mut Criterion) {
    let mut g = c.benchmark_group("muon-sync/seq-sync");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    for flush in [1usize, 8, 64] {
        g.bench_function(format!("push_flush_{flush}"), |b| {
            b.iter_batched(
                || SeqPipeline::new(Duration::ZERO),
                |mut pipe| {
                    let mut next = 0i32;
                    pipe.round(&rt, flush, &mut next);
                    black_box(next);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    g.finish();
}
