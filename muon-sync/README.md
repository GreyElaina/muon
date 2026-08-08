# muon-sync

同步引擎：CRDT 序列容器（`CrdtVec` / `CrdtString`）与 LSE 风格同步管线（队列 / rebase / 服务端核心 / 应用层撤销）。观察流（[`muon`](https://crates.io/crates/muon)）经 `SyncSink` 下沉为可序列化的事务流——观察输出即 wire form。

## 快速开始

```rust
use muon::Observe;
use muon_store::{track, Store, Track};
use muon_sync::{SyncClient, UndoStack};

#[derive(Clone, serde::Serialize, serde::Deserialize, Observe, Track)]
struct Doc {
    title: String,
}

let store = Store::new(Doc { title: "hello".into() });
let client = SyncClient::new(1);        // driver + 写通道的唯一来源
let channel = client.channel("doc");        // 每 model 一个通道，共享同一队列
let mut undo = UndoStack::new(50);          // 可选：应用层撤销历史（限 50 步）

// 同步写：track! 意图 → 队列（collecting 批次）+ 乐观发布，一步完成
let outcome = channel.sync_write(track!(&store, |d| d.title = "world".into()))?;
undo.record(&outcome.commit);               // 入队时记录：立即可撤销，无需等 server 确认
```

## 结构

四层：

- **共享 wire 类型** — `Transaction` / `BatchId` / `ItemId` 等，跨层复用
- **CRDT 序列容器** — `CrdtVec` / `CrdtString`：身份寻址的 arena B+ 树，`Edit` 操作流
- **同步管线** — `SyncSink` 下沉 / 队列（`TransactionQueue`）/ `SyncClient` / `SyncServer` / `SyncTransport` / rebase（`reconcile`）/ 崩溃恢复（`RedbCache`）
- **应用层撤销** — `UndoStack`（基于操作的逆规则）

## 服务端核心

`SyncServer` 是库级 server 端核心：Sans-I/O 状态机，一个实例 = 一个实体的同步状态——权威模型值、水位、事务去重表、delta 历史。实体间零耦合，规模按实体分片；传输层（HTTP / WebSocket / 测试替身）包裹它并调用同步方法：

```rust
let mut server = SyncServer::new().with_retention(16);  // delta 保留窗口
server.seed("doc", json!({ "title": "Hello" }));
let resp = server.send(batch_id, &txns);               // 投递即返回（异步应用）
let packets = server.poll(Some(anchor));               // 应用 + 返回增量
```

## 依赖

- [`muon`](https://crates.io/crates/muon) — 观察协议与 `Sink`
- [`muon-store`](https://crates.io/crates/muon-store) — 存储与写路径
