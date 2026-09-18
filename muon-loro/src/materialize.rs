use kernel::{OwnedPath, Path, PathSegment};
use loro::{
    Container, LoroCounter, LoroList, LoroMap, LoroMovableList, LoroText, LoroValue,
    ValueOrContainer,
};
use serde::Serialize;
use serde::ser::{
    self, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
    SerializeTupleStruct, SerializeTupleVariant,
};

use crate::{Context, Error};

pub(crate) const TEXT: &str = "muon-loro::Text";
pub(crate) const COUNTER: &str = "muon-loro::Counter";
pub(crate) const LIST: &str = "muon-loro::List";
pub(crate) const MAP: &str = "muon-loro::Map";
pub(crate) const MOVABLE_LIST: &str = "muon-loro::MovableList";

pub(crate) enum Value {
    Scalar(LoroValue),
    Map(Vec<(String, Value)>),
    List(Vec<Value>),
    Text(String),
    Counter(f64),
    MovableList(Vec<Value>),
}

pub(crate) fn replace<T: Serialize + ?Sized>(
    context: &Context,
    path: &Path<'_>,
    after: &T,
) -> Result<(), Error> {
    let value = encode(after)?;
    if path.to_owned().is_empty() {
        materialize_container(context.root().clone(), value)
    } else {
        replace_at(context, path, value)
    }
}

pub(crate) fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Value, Error> {
    value.serialize(Serializer)
}

fn replace_at(context: &Context, path: &Path<'_>, value: Value) -> Result<(), Error> {
    let owned = path.to_owned();
    let (parent, segment) = context.resolve_parent(path)?;
    match parent {
        Container::Map(map) => match segment {
            PathSegment::String(key) => set_map(&map, &key, value),
            PathSegment::Identity(_) => Err(Error::UnsupportedIdentity(owned)),
            _ => Err(Error::InvalidSegment(owned)),
        },
        Container::List(list) => {
            let index = Context::index(&segment, list.len(), &owned)?;
            set_list(&list, index, value, &owned)
        }
        Container::MovableList(list) => {
            let index = Context::index(&segment, list.len(), &owned)?;
            set_movable_list(&list, index, value, &owned)
        }
        _ => Err(Error::ExpectedContainer(owned)),
    }
}

fn materialize_container(container: Container, value: Value) -> Result<(), Error> {
    match (container, value) {
        (Container::Map(map), Value::Map(value)) => fill_map(&map, value),
        (Container::List(list), Value::List(value)) => fill_list(&list, value),
        (Container::MovableList(list), Value::MovableList(value)) => {
            fill_movable_list(&list, value)
        }
        (Container::Text(text), Value::Text(value)) => {
            text.update(&value, Default::default())?;
            Ok(())
        }
        (Container::Counter(counter), Value::Counter(value)) => {
            counter.increment(value - counter.get())?;
            Ok(())
        }
        _ => Err(Error::EmptyPath),
    }
}

fn fill_map(map: &LoroMap, entries: Vec<(String, Value)>) -> Result<(), Error> {
    let old_keys: Vec<String> = map.keys().map(|key| key.to_string()).collect();
    for key in old_keys {
        if !entries.iter().any(|(candidate, _)| candidate == &key) {
            map.delete(&key)?;
        }
    }
    for (key, value) in entries {
        set_map(map, &key, value)?;
    }
    Ok(())
}

fn set_list(list: &LoroList, index: usize, value: Value, path: &OwnedPath) -> Result<(), Error> {
    if index >= list.len() {
        return Err(Error::IndexOutOfBounds(path.clone()));
    }
    match value {
        Value::Map(value) => match list.get(index) {
            Some(ValueOrContainer::Container(Container::Map(child))) => fill_map(&child, value),
            _ => {
                list.delete(index, 1)?;
                fill_map(&list.insert_container(index, LoroMap::new())?, value)
            }
        },
        Value::List(value) => match list.get(index) {
            Some(ValueOrContainer::Container(Container::List(child))) => fill_list(&child, value),
            _ => {
                list.delete(index, 1)?;
                fill_list(&list.insert_container(index, LoroList::new())?, value)
            }
        },
        Value::MovableList(value) => match list.get(index) {
            Some(ValueOrContainer::Container(Container::MovableList(child))) => {
                fill_movable_list(&child, value)
            }
            _ => {
                list.delete(index, 1)?;
                fill_movable_list(
                    &list.insert_container(index, LoroMovableList::new())?,
                    value,
                )
            }
        },
        Value::Text(value) => match list.get(index) {
            Some(ValueOrContainer::Container(Container::Text(child))) => child
                .update(&value, Default::default())
                .map_err(Error::from),
            _ => {
                list.delete(index, 1)?;
                list.insert_container(index, LoroText::new())?
                    .update(&value, Default::default())
                    .map_err(Error::from)
            }
        },
        Value::Counter(value) => {
            let counter = match list.get(index) {
                Some(ValueOrContainer::Container(Container::Counter(child))) => child,
                _ => {
                    list.delete(index, 1)?;
                    list.insert_container(index, LoroCounter::new())?
                }
            };
            counter.increment(value - counter.get())?;
            Ok(())
        }
        Value::Scalar(value) => {
            list.delete(index, 1)?;
            list.insert(index, value)?;
            Ok(())
        }
    }
}

fn set_movable_list(
    list: &LoroMovableList,
    index: usize,
    value: Value,
    path: &OwnedPath,
) -> Result<(), Error> {
    if index >= list.len() {
        return Err(Error::IndexOutOfBounds(path.clone()));
    }
    match value {
        Value::Map(value) => match list.get(index) {
            Some(ValueOrContainer::Container(Container::Map(child))) => fill_map(&child, value),
            _ => fill_map(&list.set_container(index, LoroMap::new())?, value),
        },
        Value::List(value) => match list.get(index) {
            Some(ValueOrContainer::Container(Container::List(child))) => fill_list(&child, value),
            _ => fill_list(&list.set_container(index, LoroList::new())?, value),
        },
        Value::MovableList(value) => match list.get(index) {
            Some(ValueOrContainer::Container(Container::MovableList(child))) => {
                fill_movable_list(&child, value)
            }
            _ => fill_movable_list(&list.set_container(index, LoroMovableList::new())?, value),
        },
        Value::Text(value) => match list.get(index) {
            Some(ValueOrContainer::Container(Container::Text(child))) => child
                .update(&value, Default::default())
                .map_err(Error::from),
            _ => list
                .set_container(index, LoroText::new())?
                .update(&value, Default::default())
                .map_err(Error::from),
        },
        Value::Counter(value) => {
            let counter = match list.get(index) {
                Some(ValueOrContainer::Container(Container::Counter(child))) => child,
                _ => list.set_container(index, LoroCounter::new())?,
            };
            counter.increment(value - counter.get())?;
            Ok(())
        }
        Value::Scalar(value) => {
            list.set(index, value)?;
            Ok(())
        }
    }
}

pub(crate) fn set_map(map: &LoroMap, key: &str, value: Value) -> Result<(), Error> {
    match value {
        Value::Scalar(value) => map.insert(key, value)?,
        Value::Map(value) => match map.get(key) {
            Some(ValueOrContainer::Container(Container::Map(child))) => fill_map(&child, value)?,
            _ => fill_map(&map.insert_container(key, LoroMap::new())?, value)?,
        },
        Value::List(value) => match map.get(key) {
            Some(ValueOrContainer::Container(Container::List(child))) => fill_list(&child, value)?,
            _ => fill_list(&map.insert_container(key, LoroList::new())?, value)?,
        },
        Value::MovableList(value) => match map.get(key) {
            Some(ValueOrContainer::Container(Container::MovableList(child))) => {
                fill_movable_list(&child, value)?
            }
            _ => fill_movable_list(&map.insert_container(key, LoroMovableList::new())?, value)?,
        },
        Value::Text(value) => match map.get(key) {
            Some(ValueOrContainer::Container(Container::Text(child))) => {
                child.update(&value, Default::default())?
            }
            _ => map
                .insert_container(key, LoroText::new())?
                .update(&value, Default::default())?,
        },
        Value::Counter(value) => {
            let counter = match map.get(key) {
                Some(ValueOrContainer::Container(Container::Counter(child))) => child,
                _ => map.insert_container(key, LoroCounter::new())?,
            };
            counter.increment(value - counter.get())?;
        }
    }
    Ok(())
}

pub(crate) fn insert_list(list: &LoroList, index: usize, value: Value) -> Result<(), Error> {
    match value {
        Value::Scalar(value) => list.insert(index, value)?,
        Value::Map(value) => fill_map(&list.insert_container(index, LoroMap::new())?, value)?,
        Value::List(value) => fill_list(&list.insert_container(index, LoroList::new())?, value)?,
        Value::MovableList(value) => fill_movable_list(
            &list.insert_container(index, LoroMovableList::new())?,
            value,
        )?,
        Value::Text(value) => list
            .insert_container(index, LoroText::new())?
            .update(&value, Default::default())?,
        Value::Counter(value) => list
            .insert_container(index, LoroCounter::new())?
            .increment(value)?,
    }
    Ok(())
}

pub(crate) fn insert_movable_list(
    list: &LoroMovableList,
    index: usize,
    value: Value,
) -> Result<(), Error> {
    match value {
        Value::Scalar(value) => list.insert(index, value)?,
        Value::Map(value) => fill_map(&list.insert_container(index, LoroMap::new())?, value)?,
        Value::List(value) => fill_list(&list.insert_container(index, LoroList::new())?, value)?,
        Value::MovableList(value) => fill_movable_list(
            &list.insert_container(index, LoroMovableList::new())?,
            value,
        )?,
        Value::Text(value) => list
            .insert_container(index, LoroText::new())?
            .update(&value, Default::default())?,
        Value::Counter(value) => list
            .insert_container(index, LoroCounter::new())?
            .increment(value)?,
    }
    Ok(())
}

fn fill_list(list: &LoroList, values: Vec<Value>) -> Result<(), Error> {
    list.clear()?;
    for value in values {
        push_list(list, value)?;
    }
    Ok(())
}

fn push_list(list: &LoroList, value: Value) -> Result<(), Error> {
    match value {
        Value::Scalar(value) => list.push(value)?,
        Value::Map(value) => fill_map(&list.push_container(LoroMap::new())?, value)?,
        Value::List(value) => fill_list(&list.push_container(LoroList::new())?, value)?,
        Value::MovableList(value) => {
            fill_movable_list(&list.push_container(LoroMovableList::new())?, value)?
        }
        Value::Text(value) => list
            .push_container(LoroText::new())?
            .update(&value, Default::default())?,
        Value::Counter(value) => list.push_container(LoroCounter::new())?.increment(value)?,
    }
    Ok(())
}

fn fill_movable_list(list: &LoroMovableList, values: Vec<Value>) -> Result<(), Error> {
    list.clear()?;
    for value in values {
        push_movable_list(list, value)?;
    }
    Ok(())
}

fn push_movable_list(list: &LoroMovableList, value: Value) -> Result<(), Error> {
    match value {
        Value::Scalar(value) => list.push(value)?,
        Value::Map(value) => fill_map(&list.push_container(LoroMap::new())?, value)?,
        Value::List(value) => fill_list(&list.push_container(LoroList::new())?, value)?,
        Value::MovableList(value) => {
            fill_movable_list(&list.push_container(LoroMovableList::new())?, value)?
        }
        Value::Text(value) => list
            .push_container(LoroText::new())?
            .update(&value, Default::default())?,
        Value::Counter(value) => list.push_container(LoroCounter::new())?.increment(value)?,
    }
    Ok(())
}

struct Serializer;

impl ser::Serializer for Serializer {
    type Ok = Value;
    type Error = Error;
    type SerializeSeq = Sequence;
    type SerializeTuple = Sequence;
    type SerializeTupleStruct = Sequence;
    type SerializeTupleVariant = TupleVariant;
    type SerializeMap = MapSerializer;
    type SerializeStruct = MapSerializer;
    type SerializeStructVariant = StructVariant;

    fn serialize_bool(self, value: bool) -> Result<Value, Error> {
        Ok(Value::Scalar(value.into()))
    }
    fn serialize_i8(self, value: i8) -> Result<Value, Error> {
        self.serialize_i64(value.into())
    }
    fn serialize_i16(self, value: i16) -> Result<Value, Error> {
        self.serialize_i64(value.into())
    }
    fn serialize_i32(self, value: i32) -> Result<Value, Error> {
        self.serialize_i64(value.into())
    }
    fn serialize_i64(self, value: i64) -> Result<Value, Error> {
        Ok(Value::Scalar(value.into()))
    }
    fn serialize_i128(self, value: i128) -> Result<Value, Error> {
        self.serialize_i64(value.try_into().map_err(|_| Error::IntegerOutOfRange)?)
    }
    fn serialize_u8(self, value: u8) -> Result<Value, Error> {
        self.serialize_u64(value.into())
    }
    fn serialize_u16(self, value: u16) -> Result<Value, Error> {
        self.serialize_u64(value.into())
    }
    fn serialize_u32(self, value: u32) -> Result<Value, Error> {
        self.serialize_u64(value.into())
    }
    fn serialize_u64(self, value: u64) -> Result<Value, Error> {
        self.serialize_i64(value.try_into().map_err(|_| Error::IntegerOutOfRange)?)
    }
    fn serialize_u128(self, value: u128) -> Result<Value, Error> {
        self.serialize_i64(value.try_into().map_err(|_| Error::IntegerOutOfRange)?)
    }
    fn serialize_f32(self, value: f32) -> Result<Value, Error> {
        self.serialize_f64(value.into())
    }
    fn serialize_f64(self, value: f64) -> Result<Value, Error> {
        Ok(Value::Scalar(value.into()))
    }
    fn serialize_char(self, value: char) -> Result<Value, Error> {
        self.serialize_str(&value.to_string())
    }
    fn serialize_str(self, value: &str) -> Result<Value, Error> {
        Ok(Value::Scalar(value.to_owned().into()))
    }
    fn serialize_bytes(self, value: &[u8]) -> Result<Value, Error> {
        Ok(Value::Scalar(LoroValue::Binary(value.to_vec().into())))
    }
    fn serialize_none(self) -> Result<Value, Error> {
        self.serialize_unit()
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Value, Error> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<Value, Error> {
        Ok(Value::Scalar(LoroValue::Null))
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<Value, Error> {
        self.serialize_unit()
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<Value, Error> {
        self.serialize_str(variant)
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Value, Error> {
        let value = value.serialize(Serializer)?;
        match (name, value) {
            (TEXT, Value::Scalar(LoroValue::String(value))) => Ok(Value::Text((*value).clone())),
            (COUNTER, Value::Scalar(LoroValue::Double(value))) => Ok(Value::Counter(value)),
            (LIST, Value::List(value)) => Ok(Value::List(value)),
            (MAP, Value::Map(value)) => Ok(Value::Map(value)),
            (MOVABLE_LIST, Value::List(value)) => Ok(Value::MovableList(value)),
            (_, value) => Ok(value),
        }
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Value, Error> {
        Ok(Value::Map(vec![(
            variant.to_owned(),
            value.serialize(Serializer)?,
        )]))
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Sequence, Error> {
        Ok(Sequence(Vec::with_capacity(len.unwrap_or(0))))
    }
    fn serialize_tuple(self, len: usize) -> Result<Sequence, Error> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_struct(self, _: &'static str, len: usize) -> Result<Sequence, Error> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<TupleVariant, Error> {
        Ok(TupleVariant {
            variant,
            values: Vec::with_capacity(len),
        })
    }
    fn serialize_map(self, len: Option<usize>) -> Result<MapSerializer, Error> {
        Ok(MapSerializer {
            entries: Vec::with_capacity(len.unwrap_or(0)),
            key: None,
        })
    }
    fn serialize_struct(self, _: &'static str, len: usize) -> Result<MapSerializer, Error> {
        self.serialize_map(Some(len))
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<StructVariant, Error> {
        Ok(StructVariant {
            variant,
            entries: Vec::with_capacity(len),
        })
    }
}

struct Sequence(Vec<Value>);

impl SerializeSeq for Sequence {
    type Ok = Value;
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.0.push(value.serialize(Serializer)?);
        Ok(())
    }
    fn end(self) -> Result<Value, Error> {
        Ok(Value::List(self.0))
    }
}

impl SerializeTuple for Sequence {
    type Ok = Value;
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        SerializeSeq::serialize_element(self, value)
    }
    fn end(self) -> Result<Value, Error> {
        SerializeSeq::end(self)
    }
}

impl SerializeTupleStruct for Sequence {
    type Ok = Value;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        SerializeSeq::serialize_element(self, value)
    }
    fn end(self) -> Result<Value, Error> {
        SerializeSeq::end(self)
    }
}

struct TupleVariant {
    variant: &'static str,
    values: Vec<Value>,
}

impl SerializeTupleVariant for TupleVariant {
    type Ok = Value;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.values.push(value.serialize(Serializer)?);
        Ok(())
    }
    fn end(self) -> Result<Value, Error> {
        Ok(Value::Map(vec![(
            self.variant.to_owned(),
            Value::List(self.values),
        )]))
    }
}

struct MapSerializer {
    entries: Vec<(String, Value)>,
    key: Option<String>,
}

impl SerializeMap for MapSerializer {
    type Ok = Value;
    type Error = Error;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Error> {
        self.key = Some(key.serialize(KeySerializer)?);
        Ok(())
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        let key = self
            .key
            .take()
            .ok_or_else(|| Error::Serialization("map value without key".into()))?;
        self.entries.push((key, value.serialize(Serializer)?));
        Ok(())
    }
    fn end(self) -> Result<Value, Error> {
        if self.key.is_some() {
            return Err(Error::Serialization("map key without value".into()));
        }
        Ok(Value::Map(self.entries))
    }
}

impl SerializeStruct for MapSerializer {
    type Ok = Value;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Error> {
        self.entries
            .push((key.to_owned(), value.serialize(Serializer)?));
        Ok(())
    }
    fn end(self) -> Result<Value, Error> {
        Ok(Value::Map(self.entries))
    }
}

struct StructVariant {
    variant: &'static str,
    entries: Vec<(String, Value)>,
}

impl SerializeStructVariant for StructVariant {
    type Ok = Value;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Error> {
        self.entries
            .push((key.to_owned(), value.serialize(Serializer)?));
        Ok(())
    }
    fn end(self) -> Result<Value, Error> {
        Ok(Value::Map(vec![(
            self.variant.to_owned(),
            Value::Map(self.entries),
        )]))
    }
}

struct KeySerializer;

macro_rules! key_number {
    ($($method:ident($ty:ty)),* $(,)?) => {$(
        fn $method(self, value: $ty) -> Result<String, Error> {
            Ok(value.to_string())
        }
    )*};
}

impl ser::Serializer for KeySerializer {
    type Ok = String;
    type Error = Error;
    type SerializeSeq = ser::Impossible<String, Error>;
    type SerializeTuple = ser::Impossible<String, Error>;
    type SerializeTupleStruct = ser::Impossible<String, Error>;
    type SerializeTupleVariant = ser::Impossible<String, Error>;
    type SerializeMap = ser::Impossible<String, Error>;
    type SerializeStruct = ser::Impossible<String, Error>;
    type SerializeStructVariant = ser::Impossible<String, Error>;

    fn serialize_str(self, value: &str) -> Result<String, Error> {
        Ok(value.to_owned())
    }
    fn serialize_char(self, value: char) -> Result<String, Error> {
        Ok(value.to_string())
    }
    fn serialize_bool(self, value: bool) -> Result<String, Error> {
        Ok(value.to_string())
    }
    key_number! {
        serialize_i8(i8), serialize_i16(i16), serialize_i32(i32), serialize_i64(i64),
        serialize_i128(i128), serialize_u8(u8), serialize_u16(u16), serialize_u32(u32),
        serialize_u64(u64), serialize_u128(u128), serialize_f32(f32), serialize_f64(f64),
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<String, Error> {
        Ok(variant.to_owned())
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<String, Error> {
        value.serialize(self)
    }
    fn serialize_bytes(self, _: &[u8]) -> Result<String, Error> {
        Err(key_error())
    }
    fn serialize_none(self) -> Result<String, Error> {
        Err(key_error())
    }
    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<String, Error> {
        Err(key_error())
    }
    fn serialize_unit(self) -> Result<String, Error> {
        Err(key_error())
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<String, Error> {
        Err(key_error())
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<String, Error> {
        Err(key_error())
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Error> {
        Err(key_error())
    }
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Error> {
        Err(key_error())
    }
    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct, Error> {
        Err(key_error())
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        Err(key_error())
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Error> {
        Err(key_error())
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct, Error> {
        Err(key_error())
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Error> {
        Err(key_error())
    }
}

fn key_error() -> Error {
    Error::Serialization("Loro map keys must serialize as strings or primitive scalars".into())
}
