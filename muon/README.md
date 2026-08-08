# muon

观察并序列化 mutation 的核心库（fork 自 [shigma/muon](https://github.com/shigma/muon)）。

`#[derive(Observe)]` 为任意数据结构生成观察者：`observe!` 闭包内的可变操作被逐字段记录，flush 后产出结构化的变化流（`Changes`），供 store、同步引擎或自定义 sink 消费。

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

## 观察模型

- `#[derive(Observe)]` 生成观察者类型（`T::Observer`）。字段级控制通过 `#[muon(...)]` 属性：
  - `#[muon(noop)]` — 不观察该字段（[`NoopObserver`](crate::general::NoopObserver)）
  - `#[muon(shallow)]` — 整值替换粒度（[`ShallowObserver`](crate::general::ShallowObserver)）
  - `#[muon(snapshot)]` — 快照比较粒度（[`SnapshotObserver`](crate::general::SnapshotObserver)）
- `observe!` 宏把闭包内的赋值、比较与容器操作改写为观察者方法（`tracked_mut`/`untracked_ref` 的 autoref 特化），使观察者与普通引用共用同一套代码路径。
- 核心域的编码是 whole-value diff：[`Changes<(), ()>`](crate::Changes) 流，每个变更是一条路径 + `Replace { before, after }`。

## Sink 协议

观察事件的消费是 push 模型：flush 把事件推给 [`Sink`](crate::observe::Sink)，编码决策属于 sink。协议由四个 trait 组成：

- [`Sink`](crate::observe::Sink)：接受端。声明接受的词汇（`Operation`/`Identity`），方法签名级强制载荷的 `Into` 转换（`push_identity<I: Into<Self::Identity>>`、`inplace<O: Into<Self::Operation>>`）：
  - `push_field` / `push_index` / `push_neg_index` / `push_identity`：路径段
  - `replace(before, after)`：整值替换（载荷为 `&dyn erased_serde::Serialize`，序列化决策属于 sink）
  - `inplace(op)`：容器操作（`muon-sync` 用它传递身份寻址的 `Edit`）
  - `pop_segment`：回溯路径栈
- [`QuasiSink<S>`](crate::observe::QuasiSink)：兼容端。观察者类型声明它在 sink `S` 语境下生产的词汇（`Operation`/`Identity`）；derive 用它生成编译期门控子句 `<FieldOb as QuasiSink<S>>::Operation: Into<S::Operation>`——观察者的生产必须能转换进目标 sink 的接受词汇，失配在编译期报错。
- [`Flush<S>`](crate::observe::Flush)：完整 flush 能力（`QuasiSink` 的 super trait）。
- [`FlushWith<S, Elem>`](crate::observe::FlushWith)：委托 flush 能力。元素 flush 由调用方闭包提供，容器 impl 头只携带本地词汇门控——递归模型（自递归、递归 enum、互递归）的证明义务在闭包边界切断，无需任何标记或递归检测。

内置的 [`ObserveSink`](crate::observe::ObserveSink) 实现 whole-value diff 编码；`muon-sync` 的 `SyncSink` 把同一事件流编码为可序列化的事务流。

## 分层

```text
muon            观察协议 + 各类型实现（本 crate）
├─ muon-store    RwLock<Arc<T>> 存储 + 写路径（track! → CommitResult）
├─ muon-sync     CRDT 序列容器（CrdtVec/CrdtString）+ 同步引擎
│                （SyncSink → 事务 → 队列 / rebase / 服务端核心）
└─ muon-reactivity  响应式订阅（Field 访问器 + TriggerMap）
```

## Observer Mechanism

本节描述观察者系统的内部机制，面向贡献者与进阶用户。

### 观察者如何工作

观察者是对被观察类型的 `Deref`/`DerefMut` 包装：通过 Rust 的自动解引用拦截所有 `&mut self` 方法调用。例如 `StringObserver` 解引用到 `String`，`.push_str("hello")` 透明地到达底层 `String`，同时观察者记录这次变更。

对 `Vec::push`、`String::push_str` 这类方法，观察者提供专门实现以记录精确变更；对没有专门实现的 `&mut self` 方法，调用落入 `DerefMut`，触发保守的整值替换（`Replace`）。观察者永远正确——不会漏掉变更——但未实现的方法产生更粗粒度的输出。

### 解引用链

对 `String`、`i32` 这类简单类型，观察者可以直接解引用到目标。但对已经实现 `Deref` 的类型（如 `Vec<T>` → `[T]`），直接解引用会破坏精度。解法是引入 `Pointer<A>` 打断链条：

```text
A' → B' → Pointer<A> → A → B
```

链条分为两段：

```text
Self ──[OuterDepth]──> Pointer<Head> ───> Head ──[InnerDepth]──> Target
        coinductive                               inductive
```

- **OuterDepth**：从观察者到内部 `Pointer` 的共归纳解引用次数。多数观察者（`StringObserver`、`HashMapObserver`）为 1；复合观察者（`VecObserver` 包装 `SliceObserver`）为 2；`Pointer<T>` 自身为 0。
- **InnerDepth**：从 `Head`（`Pointer` 中存储的类型）到最终观察目标的归纳解引用次数。`VecObserver` 的 `Head = Vec<T>`、`Target = [T]`，所以 `InnerDepth = 1`。

深度用类型级 `Zero`/`Succ<N>` 追踪，由编译器验证链条良构。

#### 尾部与非尾部观察者

- **尾部观察者**直接解引用到 `Pointer<S>`（如 `StringObserver`、`SliceObserver`、`HashMapObserver`），位于链条最内层。
- **非尾部观察者**解引用到另一个观察者（如 `VecObserver` → `SliceObserver`），构成外层。

### mutation 追踪的原语

观察者上的可变方法调用产生三种行为之一：

#### 完全追踪的操作（`untracked_mut`）

`Vec::push`、`String::push_str` 等有显式观察者实现，精确知道发生了什么。它们用 `untracked_mut()` 访问底层值（不触发失效），再手工更新自己的记录状态：

```rs
fn push(&mut self, value: T) {
    self.untracked_mut().push(value);
    // 记录状态自行处理；flush 产出对应变更
}
```

#### 粗粒度操作（`tracked_mut`）

`Vec::retain`、`String::insert` 等无法表达为精确变更的方法，使用 `tracked_mut()`：

1. 对当前观察者调用 `invalidate`，重置记录状态。
2. 向观察者与 `Pointer` 之间的所有兄弟观察者传播失效。
3. 经 `DerefMutUntracked` 返回底层值的可变引用，绕过所有 `DerefMut` 钩子。

失效后，下一次 `flush` 对该值产出 `Replace`。

#### 未实现的方法（回退失效）

任何没有显式观察者实现的 `&mut self` 方法都会落入 `DerefMut`。**尾部观察者**的 `DerefMut` 触发回退失效：调用 `Pointer::invalidate`，遍历所有注册的观察者状态并失效。**非尾部观察者**的 `DerefMut` 只是透传给内层观察者，由内层处理失效。

回退失效是最大保守的：失效整条链，下一次 `flush` 产出完整 `Replace`。保证正确性（不漏变更），代价是未实现方法的粒度。

### QuasiObserver 特质

`QuasiObserver` 形式化了解引用链，并提供上述三个原语：

```rs
trait QuasiObserver {
    type Head: ?Sized;
    type OuterDepth: Unsigned;
    type InnerDepth: Unsigned;

    fn invalidate(this: &mut Self);
    fn untracked_ref(&self) -> &Target { .. }
    fn untracked_mut(&mut self) -> &mut Target { .. }
    fn tracked_mut(&mut self) -> &mut Target { .. }
}
```

- **`untracked_ref()`** 只读遍历：共归纳解引用到 `Pointer`，再经 `Deref` 到 `Target`。读不改值，无需失效。
- **`tracked_mut()`** 先对自身调用 `invalidate`，再经 `DerefMutUntracked` 到达 `Target`——该特质借助 `Pointer` 的内部可变性，通过不可变共归纳遍历取得 `&mut` 访问，绕过所有 `DerefMut` 钩子。只失效被调用者（及其与 `Pointer` 之间的观察者），外层观察者不受影响。
- **`untracked_mut()`** 与 `tracked_mut()` 走同一 `DerefMutUntracked` 路径，但跳过 `invalidate`。调用者负责更新记录状态。

#### 基于 autoref 的特化

`observe!` 宏需要把赋值与比较表达式改写为观察者与普通值都适用的形式：

- **赋值**：`observer.field = value` 会替换观察者本身而非被观察字段。宏改写为 `*(&mut observer.field).tracked_mut() = value`。
- **比较**：宏把 `lhs == rhs` 改写为 `*(&lhs).untracked_ref() == *(&rhs).untracked_ref()`。

`QuasiObserver` 对 `&T`/`&mut T` 也有实现（各方法退化为恒等），Rust 的方法解析自动选择观察者实现或引用实现。

## MSRV

最低支持 Rust 版本：**1.89.0**。

## Features

- `derive`（默认）：启用 `derive(Observe)` 与 `observe!` 宏
- `json`：启用 `serde_json` 序列化支持（`Changes::into_json` 等）
- `delete`（默认）：map 删除的细粒度编码（只报告被删的 key 而非整个 map）
- `utf8` / `utf16`：互斥；控制截断长度的字符计数（UTF-8 码点 / UTF-16 码元），默认按字节
- `indexmap` / `url` / `uuid` / `chrono`：第三方类型集成
