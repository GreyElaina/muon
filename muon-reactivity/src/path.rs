use muon_store::{StorePath, StorePathSegment};
use reactive_graph::signal::ArcTrigger;
use std::collections::HashMap;

// ── Types ───────────────────────────────────────────────────────────────

/// A reactive trigger pair for a single store field.
#[derive(Debug, Clone, Default)]
pub struct StoreFieldTrigger {
    pub this: ArcTrigger,
    pub children: ArcTrigger,
}

impl StoreFieldTrigger {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Which trigger slot should be notified.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum TriggerSlot {
    This,
    Children,
}

// ── TriggerMap ──────────────────────────────────────────────────────────

/// A map from store paths to their reactive triggers.
///
/// Built at `ReactiveStore::new()` time via `Reactivity::register_triggers()`
/// and never mutated afterward. Read-only during the store's lifetime.
#[derive(Debug, Default)]
pub struct TriggerMap {
    triggers: HashMap<StorePath, StoreFieldTrigger>,
}

impl TriggerMap {
    /// Create an empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Pre-insert a path and its ancestor prefixes.
    ///
    /// Called during `ReactiveStore::new()` by the generated
    /// `register_triggers`. Inserts not just the leaf path but all ancestor
    /// paths, so that a change at `["a", "b"]` can notify `["a"].children`.
    pub fn preinsert(&mut self, path: &[StorePathSegment]) {
        // Insert the full path first.
        let full: StorePath = path.iter().cloned().collect();
        self.triggers.entry(full.clone()).or_default();

        // Insert all ancestors.
        for end in 0..path.len() {
            let ancestor: StorePath = path[..end].iter().cloned().collect();
            self.triggers.entry(ancestor).or_default();
        }
    }

    /// Look up a path — returns `None` only if the path was not registered
    /// (i.e. a dynamic path like a map key at runtime).
    pub fn get(&self, key: &StorePath) -> Option<&StoreFieldTrigger> {
        self.triggers.get(key)
    }

    /// Returns true if no triggers have been registered.
    pub fn is_empty(&self) -> bool {
        self.triggers.is_empty()
    }
}
