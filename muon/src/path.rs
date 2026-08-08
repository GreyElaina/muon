use std::fmt::{Debug, Display};
use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

/// A segment of a mutation path.
///
/// [`PathSegment`] represents a single step in navigating to a nested value:
/// - [`String`](PathSegment::String): Access an object / struct field by name
/// - [`Positive`](PathSegment::Positive): Access an array / vec element by index from the start
/// - [`Negative`](PathSegment::Negative): Access an array / vec element by index from the end
/// - [`Identity`](PathSegment::Identity): Access an element of a container by its identity
///   (the sync layer instantiates `T` with its element identity type)
///
/// `T` defaults to `()`; the sync layer instantiates it with `muon_sync::ItemId`
/// so element-prefixed paths can carry the
/// element identity itself instead of a positional index.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(bound(serialize = "T: Serialize", deserialize = "T: Deserialize<'de>"))]
pub enum PathSegment<T = ()> {
    /// A string key for accessing object/struct fields.
    String(String),
    /// A positive index for accessing elements from the start (0-based).
    Positive(usize),
    /// A negative index for accessing elements from the end (1-based, where 1 is the last element).
    Negative(usize),
    /// An element identity in a container's element space.
    Identity(T),
}

impl<T> From<usize> for PathSegment<T> {
    fn from(n: usize) -> Self {
        Self::Positive(n)
    }
}

impl<T> From<&str> for PathSegment<T> {
    fn from(s: &str) -> Self {
        Self::String(s.to_owned())
    }
}

impl<T> From<String> for PathSegment<T> {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

impl<T: Debug> Display for PathSegment<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PathSegment::String(s) => write!(f, ".{s}"),
            PathSegment::Positive(n) => write!(f, "[{n}]"),
            PathSegment::Negative(n) => write!(f, "[-{n}]"),
            PathSegment::Identity(t) => write!(f, "[identity {t:?}]"),
        }
    }
}

/// A path to a nested value within a data structure.
///
/// [`Path`] is a sequence of [`PathSegment`]s that describes how to
/// navigate from a root value to a nested value, stored in natural
/// order (root to leaf).
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(bound(serialize = "T: Serialize", deserialize = "T: Deserialize<'de>"))]
pub struct Path<T = ()>(Vec<PathSegment<T>>);

impl<T> Default for Path<T> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<T> Path<T> {
    /// Creates a new empty path.
    pub fn new() -> Self {
        Self::default()
    }

    /// Consumes the path and returns its segments, in root-to-leaf
    /// order.
    pub fn into_vec(self) -> Vec<PathSegment<T>> {
        self.0
    }
}

impl<T> From<Vec<PathSegment<T>>> for Path<T> {
    fn from(segments: Vec<PathSegment<T>>) -> Self {
        Self(segments)
    }
}

impl<T: Debug> Display for Path<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for segment in self.0.iter() {
            write!(f, "{segment}")?;
        }
        Ok(())
    }
}

impl<T: Debug> Debug for Path<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Path").field(&self.to_string()).finish()
    }
}

impl<T> Deref for Path<T> {
    type Target = Vec<PathSegment<T>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Path<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> FromIterator<PathSegment<T>> for Path<T> {
    fn from_iter<I: IntoIterator<Item = PathSegment<T>>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}
