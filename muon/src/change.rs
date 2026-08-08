//! Unified change stream: the sole output shape of observation.
//!
//! A [`Changes`] stream is a flat list of [`Change`]s, each carrying a
//! path and a [`Changed`] description. The `Op` parameter is the
//! operation vocabulary for in-place changes; the core leaves it open
//! (the sync layer fills in its element operations). The `Id` parameter
//! is the path-segment payload type (`()` in the core; the sync layer
//! instantiates it with element identities).
//!
//! Values in `Replace` are owned [`serde_json::Value`]s, so a
//! stream is self-contained: it can be serialized, transmitted,
//! navigated and compared without any external references.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::path::Path;

/// A single change at a path.
///
/// `Replace` is the result-semantics variant: the whole value at the
/// path was replaced. `before`/`after` are the serialized old and new
/// values; `before` is `None` when the previous value is unknown
/// (for example a freshly inserted key). Undo of a replace is the
/// symmetric swap of the two values.
///
/// `Inplace` carries an operation-level description (`Op`): the editing
/// vocabulary of the observing container (element operations in the
/// sync layer). It is identity-addressed and merges exactly.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(serialize = "Op: Serialize", deserialize = "Op: Deserialize<'de>"))]
pub enum Changed<Op> {
    /// The whole value at the path was replaced (LWW semantics).
    Replace {
        /// Serialized value before the change, if known.
        before: Option<Value>,
        /// Serialized value after the change.
        after: Option<Value>,
    },
    /// An in-place operation described by the vocabulary `Op`.
    Inplace(Op),
}

/// A path plus the change that happened there.
///
/// The second parameter `Id` is the path-segment payload type (`()` in the
/// core; the sync layer instantiates it with element identities), so a
/// two-argument form `Change<Op, Id>` keeps the common shape.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "Op: Serialize, Id: Serialize",
    deserialize = "Op: Deserialize<'de>, Id: Deserialize<'de>"
))]
pub struct Change<Op, Id = ()> {
    /// Root-to-leaf path of the changed value.
    pub path: Path<Id>,
    /// The change at that path.
    pub changed: Changed<Op>,
}

/// The unified output stream of observation: a flat list of changes.
///
/// Combination is pure concatenation: composite observers flush their
/// fields and extend one stream.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "Op: Serialize, Id: Serialize",
    deserialize = "Op: Deserialize<'de>, Id: Deserialize<'de>"
))]
pub struct Changes<Op, Id = ()> {
    /// The changes, in collection order.
    pub inner: Vec<Change<Op, Id>>,
}

impl<Op, Id> Default for Changes<Op, Id> {
    fn default() -> Self {
        Self { inner: Vec::new() }
    }
}

impl<Op, Id> Changes<Op, Id> {
    /// Creates an empty stream.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a stream with the given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Vec::with_capacity(capacity),
        }
    }

    /// Appends a change.
    pub fn push(&mut self, change: Change<Op, Id>) {
        self.inner.push(change);
    }

    /// Appends all changes from another stream.
    pub fn extend(&mut self, other: Changes<Op, Id>) {
        self.inner.extend(other.inner);
    }

    /// Returns the number of changes.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns whether the stream is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Creates a stream with a single whole-value replace at the root path.
    ///
    /// `before` is the serialized value before the change (`None` when
    /// unknown, e.g. a freshly inserted key); `after` is the current
    /// value (`None` for a deletion).
    pub fn replace(before: Option<Value>, after: Option<Value>) -> Self {
        Self {
            inner: vec![Change {
                path: Path::new(),
                changed: Changed::Replace { before, after },
            }],
        }
    }

    /// Prefixes every change's path with a segment.
    ///
    /// Reserved for legacy internal use; path composition is now
    /// performed by the sink during flush.
    pub fn with_prefix(mut self, segment: impl Into<crate::PathSegment<Id>>) -> Self
    where
        Id: Clone,
    {
        let segment = segment.into();
        for change in &mut self.inner {
            change.path.insert(0, segment.clone());
        }
        self
    }
}

impl<Op, Id> IntoIterator for Changes<Op, Id> {
    type Item = Change<Op, Id>;
    type IntoIter = std::vec::IntoIter<Change<Op, Id>>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

#[cfg(feature = "json")]
impl<Op, Id: Serialize> Changes<Op, Id> {
    /// Serializes the stream into a flat JSON array view.
    ///
    /// Each element has the shape `{"path": [...], "before": ..., "after": ...}`,
    /// where `path` segments are object keys (strings), positive indices
    /// (numbers), negative indices (negative numbers, 1-based from the
    /// tail) or element identities (their serialized form). `before`/`after`
    /// are `null` when the corresponding value is unknown (fresh insert)
    /// or absent (deletion).
    ///
    /// Intended for assertions and display; the stream itself remains the
    /// canonical representation.
    pub fn into_json(self) -> serde_json::Value {
        let mut out = Vec::with_capacity(self.inner.len());
        for change in self.inner {
            let (before, after) = match change.changed {
                Changed::Replace { before, after } => (before, after),
                Changed::Inplace(_) => continue,
            };
            let path: Vec<serde_json::Value> = change
                .path
                .iter()
                .map(|segment| match segment {
                    crate::PathSegment::String(key) => serde_json::Value::String(key.clone()),
                    crate::PathSegment::Positive(index) => serde_json::Value::from(*index),
                    crate::PathSegment::Negative(index) => {
                        serde_json::Value::from(-(*index as i64))
                    }
                    crate::PathSegment::Identity(value) => {
                        serde_json::to_value(value).expect("path segment serializes")
                    }
                })
                .collect();
            out.push(serde_json::json!({
                "path": path,
                "before": before,
                "after": after,
            }));
        }
        serde_json::Value::Array(out)
    }
}
