use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct MovableList<T>(pub(crate) Vec<T>);

impl<T: Serialize> Serialize for MovableList<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_newtype_struct(crate::materialize::MOVABLE_LIST, &self.0)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for MovableList<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::deserialize(deserializer).map(Self)
    }
}

impl<T> MovableList<T> {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn into_vec(self) -> Vec<T> {
        self.0
    }
}

impl<T> Deref for MovableList<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for MovableList<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> From<Vec<T>> for MovableList<T> {
    fn from(value: Vec<T>) -> Self {
        Self(value)
    }
}

impl<T> From<MovableList<T>> for Vec<T> {
    fn from(value: MovableList<T>) -> Self {
        value.0
    }
}
