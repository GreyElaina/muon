# muon

基于 [`muon`](muon/README.md) 的开发。值变化观察、状态存储、响应式订阅与 LWW+CRDT 混合同步。

## 快速开始

```rust
use serde::Serialize;
use muon::{Observe, observe};

#[derive(Serialize, Observe)]
struct Point {
    x: f64,
    y: f64,
}

let mut point = Point { x: 1.0, y: 2.0 };

let changes = observe!(point => {
    point.x += 1.0;
    point.y *= 2.0;
});
assert_eq!(
    changes.into_json(),
    serde_json::json!([
        {"path": ["x"], "before": 1.0, "after": 2.0},
        {"path": ["y"], "before": 2.0, "after": 4.0},
    ]),
);
```

`observe!` 记录闭包内的所有变更，产出结构化的 `Changes` 流：每条变更是一条路径 + `Replace { before, after }`（或容器操作 `Inplace`）。消费是 push 模型——flush 把事件推给 `Sink`，编码决策属于 sink：内置 `ObserveSink` 产出 whole-value diff，`muon-sync` 的 `SyncSink` 把同一事件流编码为可序列化的事务流。

## 包

- `muon` — 观察协议与各类型观察者实现
- `muon-store` — `RwLock<Arc<T>>` 原子快照存储与 `track!` 写路径
- `muon-reactivity` — 响应式订阅：`Field` 访问器与提交通知
- `muon-sync` — CRDT 序列容器（`CrdtVec` / `CrdtString`）与 LSE 风格同步引擎（`SyncClient` / 队列 / rebase / 服务端核心）

## License

MIT
