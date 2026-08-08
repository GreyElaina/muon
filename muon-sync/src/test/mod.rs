//! Property tests against a reference model and the Loro oracle.
//!
//! These live inside the crate (`cfg(test)`) because they exercise
//! private internals.

mod fuzz;
mod oracle_fuzz;
