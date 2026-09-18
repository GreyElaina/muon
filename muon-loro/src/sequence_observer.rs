use std::collections::BTreeMap;

use kernel::{Invalidate, Observer, Zero};

use crate::Error;

pub(crate) type Edit<B> = Box<dyn FnOnce(&B) -> Result<(), Error>>;

pub(crate) struct State<B, O> {
    pub(crate) edits: Option<Vec<Edit<B>>>,
    pub(crate) observers: BTreeMap<usize, O>,
}

impl<B, O> State<B, O> {
    pub(crate) fn new() -> Self {
        Self {
            edits: Some(Vec::new()),
            observers: BTreeMap::new(),
        }
    }

    pub(crate) fn record(&mut self, edit: impl FnOnce(&B) -> Result<(), Error> + 'static) {
        if let Some(edits) = &mut self.edits {
            edits.push(Box::new(edit));
        }
    }

    pub(crate) unsafe fn initialize<T>(&mut self, index: usize, value: *mut T) -> &mut O
    where
        O: Observer<Head = T, InnerDepth = Zero>,
    {
        match self.observers.entry(index) {
            std::collections::btree_map::Entry::Occupied(entry) => {
                let observer = entry.into_mut();
                unsafe { O::relocate(observer, value) }
                observer
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(unsafe { O::observe(value) })
            }
        }
    }

    fn remap(&mut self, mut map: impl FnMut(usize) -> Option<usize>) {
        self.observers = core::mem::take(&mut self.observers)
            .into_iter()
            .filter_map(|(index, observer)| map(index).map(|index| (index, observer)))
            .collect();
    }

    pub(crate) fn insert(&mut self, at: usize) {
        self.remap(|index| Some(if index >= at { index + 1 } else { index }));
    }

    pub(crate) fn remove(&mut self, at: usize) {
        self.remap(|index| match index.cmp(&at) {
            core::cmp::Ordering::Less => Some(index),
            core::cmp::Ordering::Equal => None,
            core::cmp::Ordering::Greater => Some(index - 1),
        });
    }

    pub(crate) fn mov(&mut self, from: usize, to: usize) {
        self.remap(|index| {
            Some(if index == from {
                to
            } else if from < to && (from..=to).contains(&index) {
                index - 1
            } else if to < from && (to..from).contains(&index) {
                index + 1
            } else {
                index
            })
        });
    }

    pub(crate) fn truncate(&mut self, len: usize) {
        drop(self.observers.split_off(&len));
    }

    /// Clears delivered edits and rebases initialized children without releasing their storage.
    ///
    /// # Safety
    /// Every value must remain exclusively borrowed for its corresponding child rebase.
    pub(crate) unsafe fn rebase<T>(&mut self, values: &mut [T])
    where
        O: Observer<Head = T, InnerDepth = Zero>,
    {
        self.edits.get_or_insert_default().clear();
        self.observers.retain(|index, _| *index < values.len());
        for (index, observer) in &mut self.observers {
            unsafe { O::rebase(observer, &mut values[*index]) }
        }
    }
}

impl<B, O, T: ?Sized> Invalidate<T> for State<B, O> {
    fn invalidate(&mut self, _: &T) {
        self.edits = None;
        self.observers.clear();
    }
}
