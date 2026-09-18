use std::ops::{Deref, DerefMut};

use kernel::{
    AsDerefMut, CollectState, Observe as ObserveWith, Path, QuasiObserver, Query, Unsigned, Zero,
};
use serde::{Deserialize, Deserializer, Serialize};

use crate::recording::{Recording, RecordingObserver};
use crate::{Context, Error};

#[derive(Clone, Copy, Debug, Default, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Counter(f64);

impl Serialize for Counter {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_newtype_struct(crate::materialize::COUNTER, &self.0)
    }
}

impl<'de> Deserialize<'de> for Counter {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        f64::deserialize(deserializer).map(Self)
    }
}

impl Counter {
    pub fn new(value: f64) -> Self {
        Self(value)
    }

    pub fn value(self) -> f64 {
        self.0
    }
}

impl Deref for Counter {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Counter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<f64> for Counter {
    fn from(value: f64) -> Self {
        Self(value)
    }
}

impl From<Counter> for f64 {
    fn from(value: Counter) -> Self {
        value.0
    }
}

impl Context {
    fn replace_counter(&mut self, path: &Path<'_>, after: Counter) -> Result<(), Error> {
        let counter = self.resolve_counter(path)?;
        counter.increment(after.0 - counter.get())?;
        Ok(())
    }
}

impl<ContextType: ?Sized, Route, CollectError, Semantic>
    CollectState<Counter, ContextType, Route, CollectError, Semantic> for Recording<f64>
where
    ContextType: Query<Context, Route, Semantic, Output = Context>,
    CollectError: From<Error>,
{
    fn collect(
        &mut self,
        value: &Counter,
        path: &Path<'_>,
        context: &mut ContextType,
    ) -> Result<(), CollectError> {
        let context = <ContextType as Query<Context, Route, Semantic>>::query(context);
        match self.drain() {
            Some(deltas) => {
                let counter = context.resolve_counter(path).map_err(CollectError::from)?;
                for delta in deltas {
                    counter
                        .increment(delta)
                        .map_err(Error::from)
                        .map_err(CollectError::from)?;
                }
                Ok(())
            }
            None => context
                .replace_counter(path, *value)
                .map_err(CollectError::from),
        }
    }
}

pub type CounterObserver<Head, Depth = Zero> = RecordingObserver<Counter, f64, Head, Depth>;

impl ObserveWith for Counter {
    type Observer<Head, Depth>
        = CounterObserver<Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Counter> + ?Sized;
}

impl<Head: ?Sized, Depth> CounterObserver<Head, Depth>
where
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Counter>,
{
    pub fn increment(&mut self, by: f64) {
        QuasiObserver::untracked_mut::<Counter>(self).0 += by;
        self.state_mut().push(by);
    }

    pub fn decrement(&mut self, by: f64) {
        self.increment(-by);
    }
}
