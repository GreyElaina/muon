# muon-reactivity

[`muon-store`](https://crates.io/crates/muon-store) 之上的响应式订阅层：`ReactiveStore`（store + 触发器表）、`Field` 字段访问器、一步「写 + 通知」的 `CommitNotify`。`#[derive(Reactivity)]` 生成字段访问器，经 `Deref` 暴露，零 import。

## 快速开始

```rust
use muon::Observe;
use muon_reactivity::{CommitNotify, Reactivity, ReactiveStore};
use muon_store::{track, Track};
use reactive_graph::effect::ImmediateEffect;
use reactive_graph::owner::Owner;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, serde::Serialize, Observe, Track, Reactivity)]
struct AppState {
    name: String,
    count: i32,
}

let owner = Owner::new();
owner.set();
let store = Arc::new(ReactiveStore::new(AppState { name: "hello".into(), count: 0 }));

let runs = Arc::new(AtomicUsize::new(0));
let r = Arc::clone(&runs);
let s = Arc::clone(&store);
let _effect = ImmediateEffect::new(move || {
    s.name().get();
    r.fetch_add(1, Ordering::Relaxed);
});
assert_eq!(runs.load(Ordering::Relaxed), 1);

track!(store.core(), |s| s.name = "world".into()).commit().notify(&store);
assert_eq!(runs.load(Ordering::Relaxed), 2);
```

## 通知语义

通知是失效（invalidation）而非提交事件：只表示「你读过的值可能过期了，请重读」，不携带变更内容。`notify` 在写锁释放后调用（`commit().notify(&store)` 一步完成）；订阅者经 `Field::get()` 重读，永远不会错过最新值。store 不持有消费者——通知是调用方职责。

## 依赖

- [`muon-store`](https://crates.io/crates/muon-store) — 存储与写路径
- `reactive_graph` — effect / owner 运行时
