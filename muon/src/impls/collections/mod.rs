//! Observer implementations for collection types in [`std::collections`].

/// Observer implementation for [`BinaryHeap`](std::collections::BinaryHeap).
pub mod binary_heap;
/// Observer implementation for [`BTreeMap`](std::collections::BTreeMap).
pub mod btree_map;
/// Observer implementation for [`BTreeSet`](std::collections::BTreeSet).
pub mod btree_set;
/// Observer implementation for [`HashMap`](std::collections::HashMap).
pub mod hash_map;
/// Observer implementation for [`HashSet`](std::collections::HashSet).
pub mod hash_set;
/// Observer implementation for [`IndexMap`](indexmap::IndexMap).
#[cfg(feature = "indexmap")]
pub mod index_map;
/// Observer implementation for [`IndexSet`](indexmap::IndexSet).
#[cfg(feature = "indexmap")]
pub mod index_set;

pub use binary_heap::BinaryHeapObserver;
pub use btree_map::BTreeMapObserver;
pub use btree_set::BTreeSetObserver;
pub use hash_map::HashMapObserver;
pub use hash_set::HashSetObserver;
#[cfg(feature = "indexmap")]
pub use index_map::IndexMapObserver;
#[cfg(feature = "indexmap")]
pub use index_set::IndexSetObserver;
