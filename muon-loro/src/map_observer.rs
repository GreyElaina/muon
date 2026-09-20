use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};
use std::collections::HashMap;

use kernel::{
    AsDeref, AsDerefMut, AsDerefPtrExt, Collect, Composite, Invalidate, Observe as ObserveWith,
    Observer, Path, PathStep, Pointer, QuasiObserver, Query, Scope, Succ, Unsigned, Zero,
};
use loro::LoroMap;
use serde::Serialize;

use crate::materialize;
use crate::{Context, Error, Map};

type Edit = Box<dyn FnOnce(&LoroMap) -> Result<(), Error>>;

struct State<O> {
    edits: Option<Vec<Edit>>,
    observers: HashMap<String, O>,
}

impl<O> State<O> {
    fn new() -> Self {
        Self {
            edits: Some(Vec::new()),
            observers: HashMap::new(),
        }
    }

    fn record(&mut self, edit: impl FnOnce(&LoroMap) -> Result<(), Error> + 'static) {
        if let Some(edits) = &mut self.edits {
            edits.push(Box::new(edit));
        }
    }
}

impl<T, O> Invalidate<Map<T>> for State<O> {
    fn invalidate(&mut self, _: &Map<T>) {
        self.edits = None;
        self.observers.clear();
    }
}

pub struct MapObserver<T, O, S: ?Sized, D = Zero> {
    ptr: Pointer<S>,
    state: State<O>,

    marker: PhantomData<(fn(&mut T), D)>,
}

impl<T, O, S: ?Sized, D> Deref for MapObserver<T, O, S, D> {
    type Target = Pointer<S>;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

impl<T, O, S: ?Sized, D> DerefMut for MapObserver<T, O, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = Map<T>>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        QuasiObserver::invalidate(self);
        &mut self.ptr
    }
}

impl<T, O, S: ?Sized, D> QuasiObserver for MapObserver<T, O, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = Map<T>>,
{
    type OuterDepth = Succ<Zero>;
    type InnerDepth = D;

    fn invalidate(this: &mut Self) {
        Invalidate::invalidate(&mut this.state, (*this.ptr).as_deref());
    }
}

unsafe impl<T, O, S: ?Sized, D> Observer for MapObserver<T, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = Map<T>>,
    O: Observer<Head = T, InnerDepth = Zero>,
{
    type Head = S;

    unsafe fn observe(head: *mut S) -> Self {
        unsafe {
            Self {
                ptr: Pointer::new_unchecked(head),
                state: State::new(),
                marker: PhantomData,
            }
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut S) {
        unsafe { Pointer::set_unchecked(&this.ptr, head) }
    }

    unsafe fn rebase(this: &mut Self, head: *mut S) {
        unsafe {
            let values = &mut *head.as_deref_ptr::<D>();
            Pointer::set_unchecked(&this.ptr, head);
            this.state.edits.get_or_insert_default().clear();
            this.state
                .observers
                .retain(|key, _| values.0.contains_key(key));
            for (key, observer) in &mut this.state.observers {
                if let Some(value) = values.0.get_mut(key) {
                    O::rebase(observer, value);
                }
            }
        }
    }
}

impl<T, O, S: ?Sized, D, ContextType: ?Sized, ItemRoute, ChildRoute, CollectError, Semantic, Tail>
    Collect<ContextType, (ItemRoute, ChildRoute), CollectError, Scope<Semantic, Tail>>
    for MapObserver<T, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = Map<T>>,
    O: Observer<Head = T, InnerDepth = Zero>
        + Collect<ContextType, ChildRoute, CollectError, Scope<Semantic, Tail>>,
    ContextType: Query<Context, ItemRoute, Semantic, Output = Context>,
    CollectError: From<Error>,
    T: Serialize,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut ContextType) -> Result<(), CollectError> {
        let loro = <ContextType as Query<Context, ItemRoute, Semantic>>::query(context);
        let values = unsafe { &mut *Pointer::get(&self.ptr).as_ptr().as_deref_ptr::<D>() };

        if let Some(edits) = &mut self.state.edits {
            let map = loro.resolve_map(path).map_err(CollectError::from)?;
            for edit in edits.drain(..) {
                edit(&map).map_err(CollectError::from)?;
            }
        } else {
            return materialize::replace(loro, path, values).map_err(CollectError::from);
        }

        for (key, observer) in &mut self.state.observers {
            let Some(value) = values.0.get_mut(key) else {
                continue;
            };
            unsafe { O::relocate(observer, value) };
            let child = path.child(PathStep::Key(key));
            Collect::<ContextType, ChildRoute, CollectError, Scope<Semantic, Tail>>::collect(
                observer, &child, context,
            )?;
        }
        Ok(())
    }
}

impl<T, ItemRoute> ObserveWith<Map<T>, Composite<(ItemRoute,)>> for Map<T>
where
    T: Serialize,
    T: ObserveWith<T, ItemRoute>,
{
    type Observer<S, D>
        = MapObserver<T, <T as ObserveWith<T, ItemRoute>>::Observer<T, Zero>, S, D>
    where
        D: Unsigned,
        S: AsDerefMut<D, Target = Map<T>> + ?Sized;
}

impl<T, O, S: ?Sized, D> MapObserver<T, O, S, D>
where
    T: Serialize,
    D: Unsigned,
    S: AsDerefMut<D, Target = Map<T>>,
    O: Observer<Head = T, InnerDepth = Zero>,
{
    fn values_mut(&mut self) -> &mut Map<T> {
        QuasiObserver::untracked_mut(self)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut O> {
        let value = self.values_mut().0.get_mut(key)? as *mut T;
        Some(match self.state.observers.entry(key.to_owned()) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                let observer = entry.into_mut();
                unsafe { O::relocate(observer, value) };
                observer
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(unsafe { O::observe(value) })
            }
        })
    }

    pub fn insert(&mut self, key: String, value: T) -> Option<T> {
        let encoded = materialize::encode(&value);
        let old = self.values_mut().0.insert(key.clone(), value);
        self.state.observers.remove(&key);
        self.state
            .record(move |map| materialize::set_map(map, &key, encoded?));
        old
    }

    pub fn remove(&mut self, key: &str) -> Option<T> {
        let value = self.values_mut().0.remove(key)?;
        self.state.observers.remove(key);
        let key = key.to_owned();
        self.state
            .record(move |map| map.delete(&key).map_err(Error::from));
        Some(value)
    }

    pub fn clear(&mut self) {
        let values = self.values_mut();
        if values.is_empty() {
            return;
        }
        values.0.clear();
        self.state.observers.clear();
        self.state.record(|map| {
            let keys: Vec<_> = map.keys().map(|key| key.to_string()).collect();
            for key in keys {
                map.delete(&key)?;
            }
            Ok(())
        });
    }
}
