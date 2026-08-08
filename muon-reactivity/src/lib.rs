//! Reactive layer on top of muon-store.
//!
//! Adds subscription and notification to a plain [`Store`](muon_store::Store):
//! [`ReactiveStore`] (a store plus triggers), [`Field`] accessors, and the
//! [`CommitNotify`] extension for write + notify in one step.
//!
//! `#[derive(Reactivity)]` generates the field accessors and trigger
//! registration; it is re-exported here.

#[cfg(test)]
extern crate self as muon_reactivity;

mod field;
mod path;
mod reactive;

pub use field::Field;
pub use muon_reactivity_derive::Reactivity;
pub use path::{StoreFieldTrigger, TriggerMap};
pub use reactive::{CommitNotify, ReactiveStore, Reactivity, StoreFieldAccess};

#[cfg(test)]
mod tests;
