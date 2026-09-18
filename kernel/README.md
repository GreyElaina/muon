# kernel

`kernel` is the policy-free mutation-observation core extracted from muon. It combines:

- Shigma's observer substrate: typed dereference depth, `Pointer`, `QuasiObserver`, relocation,
  and fallback invalidation;
- Akashina's delivery substrate: `Change`, `Query`, `Select`, semantic `Scope`, `Replace`,
  and `Collect`;
- a hygienic `tracked!` closure transform that routes plain, compound, and comparison expressions
  through the observer primitives at any wrapper depth;
- allocation-free borrowed paths, plus an `alloc`-gated owned path that preserves static field
  names without reallocating them;
- structural collection through `Field` path prefixes and heterogeneous `Fields` products, while
  model-specific projection remains outside the kernel;
- transparent mutable indirection through `DerefObserver`, currently used by `&mut T` and `Box<T>`;
- transparent single-field projection through `NewtypeObserver` for standard newtype wrappers;
- conditional owned-child observation for `Cow`, while shared-ownership pointers are observed only
  as conservatively replaced handles;
- dedicated endpoint comparison for atomic scalars, capturing `&self` mutations without a generic
  snapshot protocol;
- structural observation of arbitrary `Cell<T>` values through tracked `get`/`get_mut` access,
  with whole-cell replacement as the fallback when the underlying `Cell` escapes;
- structural observation of arbitrary `RefCell<T>` values while real dynamic borrow guards prove
  every child access, including recursive descent through shared outer guards, with leaked or
  escaped borrows falling back to whole-cell replacement;
- conditional child observation for `OnceCell<T>`, with shared initialization and removal
  represented as whole-cell replacement and an already attached value observable through shared
  outer guards;
- the same once-slot observer reused by `OnceLock<T>`, plus non-forcing observation of
  `LazyCell<T>` and `LazyLock<T>`; shared forcing conservatively replaces the parent for that pass,
  then successful collection recovers structural child precision;
- `std` synchronization observers for `Mutex<T>` and `RwLock<T>`, pairing real lock guards with
  independently borrowed child-observer state, so internal mutability composes through shared
  outer guards while poison changes or unavailable collection locks fall back to whole-lock
  replacement;
- structural tuple products up to arity 12, with independent observer selection and collection
  routes for every element;
- fixed-size arrays with one child observer and index-prefixed path per statically stable position;
- optional sum values that preserve child observation while `Some` remains stable and fall back to
  a parent replacement when the discriminant may change;
- result sums with independently selected `Ok` and `Err` child observers and the same conservative
  discriminant fallback;
- range-bound sums with variant-prefixed child paths and the same conservative discriminant
  fallback;
- standard range products with field-level observation, while `RangeInclusive` conservatively
  observes the whole value because its semantic exhaustion state is private.

The observer namespace makes the implementation boundary explicit: its root contains the kernel's
own pointer, depth, invalidation, state, and guard machinery, while `observer::core`,
`observer::alloc`, and `observer::std` adapt that machinery to the corresponding Rust platform
crates. The crate-root re-exports keep the common API flat.

The crate deliberately contains no snapshot policy, serialization format, or CRDT integration.
It provides observers for Rust's `core`, `alloc`, and `std` types where their mutation semantics
can be expressed without an application-specific data model, plus derive-generated structural
observers. Domain-specific containers and change interpretation belong above the kernel.

`collect` is the one-shot observation scope: it constructs the selected observer, runs a
`tracked!`-transformed closure, delivers the retained facts, and only then returns the closure's
output:

```rust
# use core::convert::Infallible;
# use kernel::{Change, Here, Path, Query, Replace};
# struct Sink;
# impl Replace<i32, i32> for Sink {
#     type Error = Infallible;
#     fn replace(&mut self, _: &Path<'_>, _: Option<&i32>, _: &i32) -> Result<(), Self::Error> {
#         Ok(())
#     }
# }
# impl<'a> Query<Change<'a, i32>, Here> for Sink {
#     type Output = Sink;
#     fn query(&mut self) -> &mut Sink { self }
# }
let mut value = 1_i32;
let mut sink = Sink;
let result: Result<bool, Infallible> = kernel::collect(
    &mut value,
    kernel::tracked!(|value| {
        value += 1;
        value == 2
    }),
    &mut sink,
);
assert_eq!(result, Ok(true));
```

Long-lived models can retain the selected observer tree in an `ObserverCell`. The cell owns
baselines, pending facts, child observers, and reusable allocations without borrowing the model.
Each `bind` or `with` call relocates the tree to a current exclusive model borrow and returns, or
internally creates, an `ObserverGuard` whose lifetime prevents either side from being rebound or
moved while observer access is active. Several sessions may accumulate facts before `collect`.
Successful delivery rebases the complete tree in one step; failed delivery poisons the cell until
`reset` explicitly abandons the pending round. `Observed` is the owning facade for applications
that want the model and its cell to move together while exposing only tracked mutation.

`Observed::escape` is the explicit boundary for code that must access the model rather than its
observer. It first relocates and conservatively invalidates the observer tree, then returns
`&mut Model`; this covers interior mutation that could otherwise pass through an apparently shared
model reference. Precise inspection remains an observer session rather than a raw model borrow.

The owning `Observed` facade proves logical identity itself. Detached `ObserverCell` binding and
collection are `unsafe`: Rust can prove the temporary exclusive borrow, but cannot prove that an
arbitrary `&mut Head` still denotes the logical value whose pending facts and retained topology the
cell carries. `reset` is the safe way to discard that identity and establish a baseline on another
value.

This separates two lifetimes which a reusable observer must not conflate: observation state may
live across frames, messages, or storage relocation, while permission to access the model lasts
only for one guard. `Observer::relocate` preserves pending state across an address change;
`Observer::rebase` establishes a fresh baseline after a completed delivery. The selection,
semantic scope, and collection routes remain encoded entirely in the retained observer type.

`tracked!` rewrites places rooted at its observer parameter when Rust syntax already determines
whether the operation is a read or a write. Assignments use tracked mutable access; comparisons,
arithmetic, bitwise and logical operators, unary `-`/`!`, and casts use untracked shared access.
Method receivers and call arguments remain observers so domain-specific APIs such as a CRDT list's
`push` are not erased. In otherwise ambiguous value positions (`let value = model.field`,
`function(model.field)`, `match model.field { .. }`), borrow explicitly and call
`QuasiObserver::untracked_ref` when the model value rather than its observer is intended.

`collect_async` applies the same lifecycle to an `async |observer| { ... }` closure. The observer
remains inside the returned future across suspension and is collected only after the body
completes; dropping that future discards the observations but does not roll back mutations already
made to a borrowed model.

`kernel` is `no_std` without default features. `alloc` adds `DerefPtr` implementations for owned
pointers and collections; `std` additionally enables synchronization observers and standard
shared-ownership handles. Syntax transforms and derive macros live outside this runtime kernel.
