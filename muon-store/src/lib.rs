//! Reactive store backed by muon mutation tracking.
//!
//! The plain data layer: tracked writes, snapshots, change events.
//! Three orthogonal capabilities, selected by derive:
//!
//! - `#[derive(Observe, Track)]` — plain data layer (this crate).
//! - `#[derive(Observe, Track, Reactivity)]` — plus field accessors and
//!   reactive subscription (in `muon-reactivity`).

#[cfg(test)]
extern crate self as muon_store;

mod path;
mod store;

pub use muon_store_derive::{track, Track};
pub use path::{StorePath, StorePathSegment};
pub use store::{ChangeEvent, CommitResult, ObservedWrite, Store, Track, Write};

#[cfg(test)]
mod tests;
