# muon-store

`RwLock<Arc<T>>` 原子快照存储与 `track!` 写路径：跟踪写、快照、变更事件。观察由 [`muon`](https://crates.io/crates/muon) 提供，本 crate 负责"写"——执行 body、记录变更、发布新值。

## 快速开始

```rust
use muon::Observe;
use muon_store::{track, Store, Track};

#[derive(Clone, serde::Serialize, Observe, Track)]
struct AppState {
    name: String,
    count: i32,
}

let store = Store::new(AppState { name: "hello".into(), count: 0 });

// 执行 + 发布，返回 body 的返回值
let result = track!(store, |s| s.name = "world".into()).commit().result();

// 或取出 ChangeEvent（paths，供通知 / 同步消费）
let ev = track!(store, |s| s.count += 1).commit().event();
```

## 写路径

`track!` 产生惰性的 `Write`，不执行、不持锁。消费方式：

- `commit()` — 执行 + 发布，一步完成
- `observe().flush()` — 分步：持写锁 → 执行 body → 释放锁
- `sync_flush()` — 同步路径：观察流直接下沉为事务（[`muon-sync`](https://crates.io/crates/muon-sync) 的 `SyncSink`），无中间物化

写入是写锁内 `Arc::make_mut`：无读者时零拷贝原地改；有读者持旧 `Arc` 时深拷贝（COW 按需）。`ChangeEvent` 记录 `paths`（哪些路径变了）；store 本身不通知——事件交给上层（`muon-reactivity` 的 `notify` 或同步引擎）。

## 快照

`snapshot() -> Arc<T>` 是独立前置动作（O(1) 引用计数克隆），不参与写路径；它同时是同步事务的 `original` 来源——过期版本由 rebase 处理。

## 依赖

- [`muon`](https://crates.io/crates/muon) — 观察协议
