//! Application-layer undo/redo.
//!
//! [`undo`] builds inverses of changes and transactions (the inversion
//! rules live next to the types they invert); [`UndoStack`] manages
//! the undo/redo stacks and replays refreshed transactions.

mod undo;
mod undo_stack;

pub use undo_stack::UndoStack;
