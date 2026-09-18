use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};

use kernel::{
    AsDeref, AsDerefMut, AsDerefPtrExt, Collect, Composite, Invalidate, Observe as ObserveWith,
    Observer, Path, PathStep, Pointer, QuasiObserver, Query, Scope, Succ, Unsigned, Zero,
};
use loro::LoroList;
use serde::Serialize;

use crate::materialize;
use crate::sequence_observer::State;
use crate::{Context, Error, List};

pub struct ListObserver<T, O, S: ?Sized, D = Zero> {
    ptr: Pointer<S>,
    state: State<LoroList, O>,

    marker: PhantomData<(fn(&mut T), D)>,
}

impl<T, O, S: ?Sized, D> Deref for ListObserver<T, O, S, D> {
    type Target = Pointer<S>;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

impl<T, O, S: ?Sized, D> DerefMut for ListObserver<T, O, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = List<T>>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        QuasiObserver::invalidate(self);
        &mut self.ptr
    }
}

impl<T, O, S: ?Sized, D> QuasiObserver for ListObserver<T, O, S, D>
where
    D: Unsigned,
    S: AsDeref<D, Target = List<T>>,
{
    type Head = S;
    type OuterDepth = Succ<Zero>;
    type InnerDepth = D;

    fn invalidate(this: &mut Self) {
        Invalidate::invalidate(&mut this.state, (*this.ptr).as_deref());
    }
}

unsafe impl<T, O, S: ?Sized, D> Observer for ListObserver<T, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = List<T>>,
    O: Observer<Head = T, InnerDepth = Zero>,
{
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
            this.state.rebase(&mut values.0);
        }
    }
}

impl<T, O, S: ?Sized, D, ContextType: ?Sized, ItemRoute, ChildRoute, CollectError, Semantic, Tail>
    Collect<ContextType, (ItemRoute, ChildRoute), CollectError, Scope<Semantic, Tail>>
    for ListObserver<T, O, S, D>
where
    D: Unsigned,
    S: AsDerefMut<D, Target = List<T>>,
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
            let list = loro.resolve_list(path).map_err(CollectError::from)?;
            for edit in edits.drain(..) {
                edit(&list).map_err(CollectError::from)?;
            }
        } else {
            return materialize::replace(loro, path, values).map_err(CollectError::from);
        }

        for (index, observer) in &mut self.state.observers {
            let index = *index;
            unsafe { O::relocate(observer, &raw mut values.0[index]) };
            let child = path.child(PathStep::Positive(index));
            Collect::<ContextType, ChildRoute, CollectError, Scope<Semantic, Tail>>::collect(
                observer, &child, context,
            )?;
        }
        Ok(())
    }
}

impl<T, ItemRoute> ObserveWith<List<T>, Composite<(ItemRoute,)>> for List<T>
where
    T: Serialize,
    T: ObserveWith<T, ItemRoute>,
{
    type Observer<S, D>
        = ListObserver<T, <T as ObserveWith<T, ItemRoute>>::Observer<T, Zero>, S, D>
    where
        D: Unsigned,
        S: AsDerefMut<D, Target = List<T>> + ?Sized;
}

impl<T, O, S: ?Sized, D> ListObserver<T, O, S, D>
where
    T: Serialize,
    D: Unsigned,
    S: AsDerefMut<D, Target = List<T>>,
    O: Observer<Head = T, InnerDepth = Zero>,
{
    fn values_mut(&mut self) -> &mut List<T> {
        QuasiObserver::untracked_mut(self)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut O> {
        let value = self.values_mut().0.get_mut(index)? as *mut T;
        Some(unsafe { self.state.initialize(index, value) })
    }

    pub fn push(&mut self, value: T) {
        let encoded = materialize::encode(&value);
        let values = self.values_mut();
        let index = values.len();
        values.0.push(value);
        self.state
            .record(move |list| materialize::insert_list(list, index, encoded?));
    }

    pub fn pop(&mut self) -> Option<T> {
        let values = self.values_mut();
        let index = values.len().checked_sub(1)?;
        let value = values.0.pop();
        self.state.remove(index);
        self.state.record(move |list| {
            list.delete(index, 1)?;
            Ok(())
        });
        value
    }

    pub fn insert(&mut self, index: usize, value: T) {
        let encoded = materialize::encode(&value);
        self.values_mut().0.insert(index, value);
        self.state.insert(index);
        self.state
            .record(move |list| materialize::insert_list(list, index, encoded?));
    }

    pub fn remove(&mut self, index: usize) -> T {
        self.state.remove(index);
        let value = self.values_mut().0.remove(index);
        self.state.record(move |list| {
            list.delete(index, 1)?;
            Ok(())
        });
        value
    }

    pub fn set(&mut self, index: usize, value: T) -> T {
        let encoded = materialize::encode(&value);
        let old = core::mem::replace(&mut self.values_mut().0[index], value);
        self.state.observers.remove(&index);
        self.state.record(move |list| {
            list.delete(index, 1)?;
            materialize::insert_list(list, index, encoded?)
        });
        old
    }

    pub fn truncate(&mut self, len: usize) {
        let values = self.values_mut();
        let old_len = values.len();
        values.0.truncate(len);
        self.state.truncate(len);
        if len < old_len {
            self.state.record(move |list| {
                list.delete(len, old_len - len)?;
                Ok(())
            });
        }
    }

    pub fn clear(&mut self) {
        let values = self.values_mut();
        if values.is_empty() {
            return;
        }
        values.0.clear();
        self.state.observers.clear();
        self.state.record(|list| {
            list.clear()?;
            Ok(())
        });
    }
}
