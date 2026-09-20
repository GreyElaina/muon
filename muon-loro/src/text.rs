use std::ops::{Bound, Deref, DerefMut, RangeBounds};

use kernel::{
    AsDerefMut, CollectState, Observe as ObserveWith, Path, QuasiObserver, Query, Unsigned, Zero,
};
use loro::TextDelta;
use serde::{Deserialize, Deserializer, Serialize};

use crate::recording::{Recording, RecordingObserver};
use crate::{Context, Error};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Text(String);

impl Serialize for Text {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_newtype_struct(crate::materialize::TEXT, &self.0)
    }
}

impl<'de> Deserialize<'de> for Text {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(Self)
    }
}

impl Text {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl Deref for Text {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Text {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<String> for Text {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for Text {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<Text> for String {
    fn from(value: Text) -> Self {
        value.0
    }
}

impl AsRef<str> for Text {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Context {
    fn replace_text(&mut self, path: &Path<'_>, after: &str) -> Result<(), Error> {
        self.resolve_text(path)?.update(after, Default::default())?;
        Ok(())
    }
}

impl<ContextType: ?Sized, Route, CollectError, Semantic>
    CollectState<Text, ContextType, Route, CollectError, Semantic> for Recording<Vec<TextDelta>>
where
    ContextType: Query<Context, Route, Semantic, Output = Context>,
    CollectError: From<Error>,
{
    fn collect(
        &mut self,
        value: &Text,
        path: &Path<'_>,
        context: &mut ContextType,
    ) -> Result<(), CollectError> {
        let context = <ContextType as Query<Context, Route, Semantic>>::query(context);
        match self.drain() {
            Some(deltas) => {
                let text = context.resolve_text(path).map_err(CollectError::from)?;
                for delta in deltas {
                    text.apply_delta(&delta)
                        .map_err(Error::from)
                        .map_err(CollectError::from)?;
                }
                Ok(())
            }
            None => context
                .replace_text(path, value.as_ref())
                .map_err(CollectError::from),
        }
    }
}

pub type TextObserver<Head, Depth = Zero> = RecordingObserver<Vec<TextDelta>, Head, Depth>;

impl ObserveWith for Text {
    type Observer<Head, Depth>
        = TextObserver<Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Text> + ?Sized;
}

impl<Head: ?Sized, Depth> TextObserver<Head, Depth>
where
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Text>,
{
    fn value(&self) -> &Text {
        QuasiObserver::untracked_ref(self)
    }

    fn value_mut(&mut self) -> &mut Text {
        QuasiObserver::untracked_mut(self)
    }

    fn record(&mut self, index: usize, delete: usize, insert: String) {
        let mut delta = Vec::with_capacity(3);
        if index != 0 {
            delta.push(TextDelta::Retain {
                retain: index,
                attributes: None,
            });
        }
        if !insert.is_empty() {
            delta.push(TextDelta::Insert {
                insert,
                attributes: None,
            });
        }
        if delete != 0 {
            delta.push(TextDelta::Delete { delete });
        }
        if !delta.is_empty() {
            self.state_mut().push(delta);
        }
    }

    pub fn push_str(&mut self, string: &str) {
        let index = self.value().chars().count();
        self.value_mut().0.push_str(string);
        self.record(index, 0, string.into());
    }

    pub fn push(&mut self, ch: char) {
        let index = self.value().chars().count();
        self.value_mut().0.push(ch);
        self.record(index, 0, ch.to_string());
    }

    pub fn insert_str(&mut self, index: usize, string: &str) {
        let event_index = self.value()[..index].chars().count();
        self.value_mut().0.insert_str(index, string);
        self.record(event_index, 0, string.into());
    }

    pub fn insert(&mut self, index: usize, ch: char) {
        let event_index = self.value()[..index].chars().count();
        self.value_mut().0.insert(index, ch);
        self.record(event_index, 0, ch.to_string());
    }

    pub fn pop(&mut self) -> Option<char> {
        let ch = self.value().chars().next_back()?;
        let index = self.value().chars().count() - 1;
        self.value_mut().0.pop();
        self.record(index, 1, String::new());
        Some(ch)
    }

    pub fn remove(&mut self, index: usize) -> char {
        let event_index = self.value()[..index].chars().count();
        let ch = self.value_mut().0.remove(index);
        self.record(event_index, 1, String::new());
        ch
    }

    pub fn truncate(&mut self, len: usize) {
        let old_len = self.value().len();
        if len < old_len {
            let index = self.value()[..len].chars().count();
            let delete = self.value()[len..].chars().count();
            self.value_mut().0.truncate(len);
            self.record(index, delete, String::new());
        }
    }

    pub fn clear(&mut self) {
        let len = self.value().chars().count();
        self.value_mut().0.clear();
        if len != 0 {
            self.record(0, len, String::new());
        }
    }

    pub fn replace_range<R>(&mut self, range: R, replace_with: &str)
    where
        R: RangeBounds<usize>,
    {
        let start = match range.start_bound() {
            Bound::Included(index) => *index,
            Bound::Excluded(index) => index.checked_add(1).expect("range start overflow"),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(index) => index.checked_add(1).expect("range end overflow"),
            Bound::Excluded(index) => *index,
            Bound::Unbounded => self.value().len(),
        };
        let event_start = self.value()[..start].chars().count();
        let delete = self.value()[start..end].chars().count();
        self.value_mut().0.replace_range(start..end, replace_with);
        self.record(event_start, delete, replace_with.into());
    }
}
