use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[repr(transparent)]
pub struct Map<T>(pub(crate) HashMap<String, T>);

impl<T: Serialize> Serialize for Map<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_newtype_struct(crate::materialize::MAP, &self.0)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Map<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        HashMap::deserialize(deserializer).map(Self)
    }
}

impl<T> Map<T> {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn into_hash_map(self) -> HashMap<String, T> {
        self.0
    }
}

impl<T> Deref for Map<T> {
    type Target = HashMap<String, T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Map<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> From<HashMap<String, T>> for Map<T> {
    fn from(value: HashMap<String, T>) -> Self {
        Self(value)
    }
}

impl<T> From<Map<T>> for HashMap<String, T> {
    fn from(value: Map<T>) -> Self {
        value.0
    }
}
