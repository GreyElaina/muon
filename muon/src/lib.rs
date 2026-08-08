#![cfg_attr(docsrs, allow(internal_features))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, feature(rustdoc_internals))]
#![cfg_attr(docsrs, feature(intra_doc_pointers))]
#![allow(rustdoc::private_intra_doc_links)]
#![warn(missing_docs)]
#![recursion_limit = "256"]
#![doc = include_str!("../README.md")]

#[cfg(all(feature = "utf8", feature = "utf16"))]
compile_error!("Features `utf8` and `utf16` are mutually exclusive");

#[cfg(test)]
extern crate self as muon;

pub mod change;
pub mod general;
pub mod helper;
pub mod impls;
pub mod observe;
mod path;

pub use change::{Change, Changed, Changes};
pub use erased_serde;
#[cfg(feature = "derive")]
pub use muon_derive::{Observe, observe};
pub use observe::Observe;
pub use path::{Path, PathSegment};
/// Re-export for generated code and integration crates.
pub use serde_json;
