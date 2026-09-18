//! Borrowed collection paths and their optional owned representation.

use core::fmt::Debug;

#[cfg(feature = "alloc")]
use core::fmt::Display;

#[cfg(feature = "alloc")]
use alloc::{
    borrow::{Cow, ToOwned},
    string::{String, ToString},
    vec::Vec,
};

/// One owned path segment retained beyond a collection walk.
#[cfg(feature = "alloc")]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PathSegment<T = ()> {
    /// A statically borrowed field name or an owned runtime key.
    String(Cow<'static, str>),
    /// An index counted from the front.
    Positive(usize),
    /// An index counted from the back.
    Negative(usize),
    /// A domain-specific stable identity.
    Identity(T),
}

#[cfg(feature = "alloc")]
impl<T> From<usize> for PathSegment<T> {
    fn from(value: usize) -> Self {
        Self::Positive(value)
    }
}

#[cfg(feature = "alloc")]
impl<T> From<&str> for PathSegment<T> {
    fn from(value: &str) -> Self {
        Self::String(Cow::Owned(value.to_owned()))
    }
}

#[cfg(feature = "alloc")]
impl<T> From<String> for PathSegment<T> {
    fn from(value: String) -> Self {
        Self::String(Cow::Owned(value))
    }
}

#[cfg(feature = "alloc")]
impl<T: Debug> Display for PathSegment<T> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::String(value) => write!(formatter, ".{value}"),
            Self::Positive(index) => write!(formatter, "[{index}]"),
            Self::Negative(index) => write!(formatter, "[-{index}]"),
            Self::Identity(identity) => write!(formatter, "[identity {identity:?}]"),
        }
    }
}

/// One statically or dynamically known step in an observation path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PathStep<'a, T = ()> {
    /// A field name known at compile time.
    Field(&'static str),
    /// A runtime string key borrowed for the collection walk.
    Key(&'a str),
    /// An index counted from the front.
    Positive(usize),
    /// An index counted from the back.
    Negative(usize),
    /// A domain-specific stable identity borrowed for the collection walk.
    Identity(&'a T),
}

impl<T> From<usize> for PathStep<'_, T> {
    fn from(value: usize) -> Self {
        Self::Positive(value)
    }
}

impl<T> From<&'static str> for PathStep<'_, T> {
    fn from(value: &'static str) -> Self {
        Self::Field(value)
    }
}

/// A borrowed, persistent root-to-leaf path.
#[derive(Clone, Debug, Default)]
pub struct Path<'a, T = ()> {
    link: Option<(&'a Path<'a, T>, PathStep<'a, T>)>,
}

impl<T> Path<'static, T> {
    /// Creates the empty root path.
    pub const fn root() -> Self {
        Self { link: None }
    }
}

impl<'a, T> Path<'a, T> {
    /// Appends one borrowed step without mutating the parent path.
    pub fn child<'b>(&'b self, step: PathStep<'b, T>) -> Path<'b, T>
    where
        'a: 'b,
    {
        Path {
            link: Some((self, step)),
        }
    }

    /// Returns the preceding path node, if this is not the root.
    pub const fn parent(&self) -> Option<&'a Path<'a, T>> {
        match &self.link {
            Some((parent, _)) => Some(*parent),
            None => None,
        }
    }

    /// Returns this node's step, if this is not the root.
    pub const fn step(&self) -> Option<&PathStep<'a, T>> {
        match &self.link {
            Some((_, step)) => Some(step),
            None => None,
        }
    }

    /// Copies this ephemeral path into storage that may outlive collection.
    #[cfg(feature = "alloc")]
    pub fn to_owned(&self) -> OwnedPath<T>
    where
        T: Clone,
    {
        let mut segments = Vec::new();
        let mut path = self;
        while let Some((parent, step)) = &path.link {
            segments.push(match step {
                PathStep::Field(field) => PathSegment::String(Cow::Borrowed(field)),
                PathStep::Key(key) => PathSegment::String(Cow::Owned((*key).to_owned())),
                PathStep::Positive(index) => PathSegment::Positive(*index),
                PathStep::Negative(index) => PathSegment::Negative(*index),
                PathStep::Identity(identity) => PathSegment::Identity((*identity).clone()),
            });
            path = parent;
        }
        segments.reverse();
        OwnedPath(segments)
    }

    /// Returns whether this borrowed path begins with `prefix`.
    #[cfg(feature = "alloc")]
    pub fn starts_with(&self, prefix: &OwnedPath<T>) -> bool
    where
        T: PartialEq,
    {
        let mut path = self;
        let mut depth = self.len();
        if prefix.len() > depth {
            return false;
        }
        while depth > prefix.len() {
            path = path.parent().expect("path depth");
            depth -= 1;
        }
        prefix.iter().rev().all(|segment| {
            let matches = path.step().is_some_and(|step| step == segment);
            if let Some(parent) = path.parent() {
                path = parent;
            }
            matches
        })
    }

    #[cfg(feature = "alloc")]
    fn len(&self) -> usize {
        let mut len = 0;
        let mut path = self;
        while let Some((parent, _)) = &path.link {
            len += 1;
            path = parent;
        }
        len
    }
}

/// A root-to-leaf path retained independently of an observer walk.
#[cfg(feature = "alloc")]
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct OwnedPath<T = ()>(Vec<PathSegment<T>>);

#[cfg(feature = "alloc")]
impl<T> OwnedPath<T> {
    /// Consumes the path and returns its segments.
    pub fn into_vec(self) -> Vec<PathSegment<T>> {
        self.0
    }

    /// Returns whether this owned path begins with `prefix`.
    pub fn starts_with(&self, prefix: &Path<'_, T>) -> bool
    where
        T: PartialEq,
    {
        let depth = prefix.len();
        if depth > self.len() {
            return false;
        }
        let mut path = prefix;
        self[..depth].iter().rev().all(|segment| {
            let matches = path.step().is_some_and(|step| step == segment);
            if let Some(parent) = path.parent() {
                path = parent;
            }
            matches
        })
    }
}

#[cfg(feature = "alloc")]
impl<T: PartialEq> PartialEq<PathSegment<T>> for PathStep<'_, T> {
    fn eq(&self, other: &PathSegment<T>) -> bool {
        match (self, other) {
            (Self::Field(left), PathSegment::String(right)) => *left == right.as_ref(),
            (Self::Key(left), PathSegment::String(right)) => *left == right.as_ref(),
            (Self::Positive(left), PathSegment::Positive(right)) => left == right,
            (Self::Negative(left), PathSegment::Negative(right)) => left == right,
            (Self::Identity(left), PathSegment::Identity(right)) => *left == right,
            _ => false,
        }
    }
}

#[cfg(feature = "alloc")]
impl<T> From<Vec<PathSegment<T>>> for OwnedPath<T> {
    fn from(segments: Vec<PathSegment<T>>) -> Self {
        Self(segments)
    }
}

#[cfg(feature = "alloc")]
impl<T> core::ops::Deref for OwnedPath<T> {
    type Target = [PathSegment<T>];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(feature = "alloc")]
impl<T: Debug> Display for OwnedPath<T> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        for segment in &self.0 {
            write!(formatter, "{segment}")?;
        }
        Ok(())
    }
}

#[cfg(feature = "alloc")]
impl<T: Debug> Debug for OwnedPath<T> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_tuple("Path")
            .field(&self.to_string())
            .finish()
    }
}

#[cfg(feature = "alloc")]
impl<T> FromIterator<PathSegment<T>> for OwnedPath<T> {
    fn from_iter<I: IntoIterator<Item = PathSegment<T>>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}
