//! Structural observation through standard synchronization guards.

use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};
use std::sync::{
    LockResult, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard,
    TryLockError, TryLockResult,
};

use crate::{
    AsDeref, AsDerefMut, Change, Collect, Composite, Observe, ObservedGuard, ObservedGuardMut,
    Path, Query, Replace, Scope, emit,
};
use crate::{Observer, ObserverSlot, Pointer, QuasiObserver, Succ, Unsigned, Zero};

#[doc(hidden)]
pub trait ExclusiveLock {
    type Value;
    type Guard<'a>: DerefMut<Target = Self::Value>
    where
        Self: 'a;

    fn is_poisoned(&self) -> bool;
    fn clear_poison(&self);
    fn get_mut(&mut self) -> LockResult<&mut Self::Value>;
    fn acquire(&self) -> LockResult<Self::Guard<'_>>;
    fn try_acquire(&self) -> TryLockResult<Self::Guard<'_>>;
}

impl<T> ExclusiveLock for Mutex<T> {
    type Value = T;
    type Guard<'a>
        = MutexGuard<'a, T>
    where
        T: 'a;

    fn is_poisoned(&self) -> bool {
        self.is_poisoned()
    }

    fn clear_poison(&self) {
        self.clear_poison()
    }

    fn get_mut(&mut self) -> LockResult<&mut T> {
        self.get_mut()
    }

    fn acquire(&self) -> LockResult<Self::Guard<'_>> {
        self.lock()
    }

    fn try_acquire(&self) -> TryLockResult<Self::Guard<'_>> {
        self.try_lock()
    }
}

impl<T> ExclusiveLock for RwLock<T> {
    type Value = T;
    type Guard<'a>
        = RwLockWriteGuard<'a, T>
    where
        T: 'a;

    fn is_poisoned(&self) -> bool {
        self.is_poisoned()
    }

    fn clear_poison(&self) {
        self.clear_poison()
    }

    fn get_mut(&mut self) -> LockResult<&mut T> {
        self.get_mut()
    }

    fn acquire(&self) -> LockResult<Self::Guard<'_>> {
        self.write()
    }

    fn try_acquire(&self) -> TryLockResult<Self::Guard<'_>> {
        self.try_write()
    }
}

/// Implementation shared by [`MutexObserver`] and [`RwLockObserver`].
#[doc(hidden)]
pub struct LockObserver<O, Lock, Head: ?Sized, Depth = Zero> {
    pointer: Pointer<Head>,
    child: ObserverSlot<O>,

    marker: PhantomData<(fn(Lock), Depth)>,
}

impl<O, Lock, Head: ?Sized, Depth> Deref for LockObserver<O, Lock, Head, Depth> {
    type Target = Pointer<Head>;

    fn deref(&self) -> &Self::Target {
        self.child.invalidate();
        &self.pointer
    }
}

impl<O, Lock, Head: ?Sized, Depth> DerefMut for LockObserver<O, Lock, Head, Depth> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.child.invalidate();
        &mut self.pointer
    }
}

impl<T, O, Lock, Head: ?Sized, Depth> QuasiObserver for LockObserver<O, Lock, Head, Depth>
where
    O: QuasiObserver<Head = T, InnerDepth = Zero>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth>,
{
    type Head = Head;
    type OuterDepth = Succ<Zero>;
    type InnerDepth = Depth;

    fn invalidate(this: &mut Self) {
        this.child.invalidate();
    }

    fn untracked_ref<Value: ?Sized>(&self) -> &Value
    where
        Self::Head: AsDeref<Self::InnerDepth, Target = Value>,
    {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        AsDeref::<Depth>::as_deref(head)
    }

    fn untracked_mut<Value: ?Sized>(&mut self) -> &mut Value
    where
        Head: AsDerefMut<Depth, Target = Value>,
    {
        let head = unsafe { Pointer::as_mut(&self.pointer) };
        AsDerefMut::<Depth>::as_deref_mut(head)
    }
}

unsafe impl<T, O, Lock, Head: ?Sized, Depth> Observer for LockObserver<O, Lock, Head, Depth>
where
    O: Observer<Head = T, InnerDepth = Zero>,
    Lock: ExclusiveLock<Value = T>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Lock>,
{
    unsafe fn observe(head: *mut Head) -> Self {
        unsafe {
            let lock = AsDeref::<Depth>::as_deref_ptr(head);
            let value = ExclusiveLock::get_mut(&mut *lock).unwrap_or_else(PoisonError::into_inner);
            Self {
                pointer: Pointer::new_unchecked(head),
                child: ObserverSlot::new(O::observe(value)),

                marker: PhantomData,
            }
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Head) {
        unsafe {
            let lock = AsDeref::<Depth>::as_deref_ptr(head);
            let value = ExclusiveLock::get_mut(&mut *lock).unwrap_or_else(PoisonError::into_inner);
            this.child.activate(value);
            Pointer::set_unchecked(&this.pointer, head);
        }
    }

    unsafe fn rebase(this: &mut Self, head: *mut Head) {
        unsafe {
            let lock = AsDeref::<Depth>::as_deref_ptr(head);
            let value = ExclusiveLock::get_mut(&mut *lock).unwrap_or_else(PoisonError::into_inner);
            this.child.rebase(value);
            Pointer::set_unchecked(&this.pointer, head);
        }
    }
}

impl<
    T,
    O,
    Lock,
    Head: ?Sized,
    Depth,
    Context: ?Sized,
    ParentRoute,
    InnerRoute,
    Error,
    Semantic,
    Tail,
> Collect<Context, (ParentRoute, InnerRoute), Error, Scope<Semantic, Tail>>
    for LockObserver<O, Lock, Head, Depth>
where
    O: Observer<Head = T, InnerDepth = Zero>
        + Collect<Context, InnerRoute, Error, Scope<Semantic, Tail>>,
    Lock: ExclusiveLock<Value = T>,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = Lock>,
    for<'a> Context: Query<Change<'a, Lock>, ParentRoute, Semantic>,
    for<'a> <Context as Query<Change<'a, Lock>, ParentRoute, Semantic>>::Output:
        Replace<Lock, Lock>,
    for<'a> Error: From<
        <<Context as Query<Change<'a, Lock>, ParentRoute, Semantic>>::Output as Replace<
            Lock,
            Lock,
        >>::Error,
    >,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let lock = AsDeref::<Depth>::as_deref(head);
        if self.child.is_exact() {
            let mut guard = match lock.try_acquire() {
                Ok(guard) => guard,
                Err(error) => {
                    drop(error);
                    self.child.invalidate();
                    return emit::<_, _, Context, ParentRoute, Semantic, Error>(
                        context,
                        Change::Replace {
                            path,
                            before: None,
                            after: lock,
                        },
                    );
                }
            };
            unsafe { self.child.activate(&mut *guard) };
            return Collect::<Context, InnerRoute, Error, Scope<Semantic, Tail>>::collect(
                unsafe { self.child.observer_mut() },
                path,
                context,
            );
        }

        emit::<_, _, Context, ParentRoute, Semantic, Error>(
            context,
            Change::Replace {
                path,
                before: None,
                after: lock,
            },
        )
    }
}

impl<T, O, Lock, Head: ?Sized, Depth> LockObserver<O, Lock, Head, Depth>
where
    O: Observer<Head = T, InnerDepth = Zero>,
    Lock: ExclusiveLock<Value = T>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Lock>,
{
    /// Returns whether the underlying lock is poisoned without escaping observation.
    pub fn is_poisoned(&self) -> bool {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        ExclusiveLock::is_poisoned(AsDeref::<Depth>::as_deref(head))
    }

    /// Clears poison and records a whole-lock replacement when the poison state changed.
    pub fn clear_poison(&self) {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let lock = AsDeref::<Depth>::as_deref(head);
        if ExclusiveLock::is_poisoned(lock) {
            self.child.fallback();
            ExclusiveLock::clear_poison(lock);
        }
    }

    /// Returns the child observer using this observer's exclusive access to the lock.
    pub fn get_mut(&mut self) -> LockResult<&mut O> {
        let head = unsafe { Pointer::as_mut(&self.pointer) };
        let lock = AsDerefMut::<Depth>::as_deref_mut(head);
        match ExclusiveLock::get_mut(lock) {
            Ok(value) => {
                unsafe { self.child.activate(value) };
                Ok(unsafe { self.child.observer_mut() })
            }
            Err(error) => {
                let value = error.into_inner();
                self.child.fallback();
                unsafe { self.child.activate(value) };
                Err(PoisonError::new(unsafe { self.child.observer_mut() }))
            }
        }
    }

    fn acquire(&self) -> LockResult<ObservedGuardMut<'_, <Lock as ExclusiveLock>::Guard<'_>, O>> {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let lock = AsDeref::<Depth>::as_deref(head);
        match lock.acquire() {
            Ok(guard) => Ok(unsafe { self.child.write(guard) }),
            Err(error) => {
                let guard = error.into_inner();
                self.child.fallback();
                Err(PoisonError::new(unsafe { self.child.write(guard) }))
            }
        }
    }

    fn try_acquire(
        &self,
    ) -> TryLockResult<ObservedGuardMut<'_, <Lock as ExclusiveLock>::Guard<'_>, O>> {
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let lock = AsDeref::<Depth>::as_deref(head);
        match lock.try_acquire() {
            Ok(guard) => Ok(unsafe { self.child.write(guard) }),
            Err(TryLockError::Poisoned(error)) => {
                let guard = error.into_inner();
                self.child.fallback();
                Err(TryLockError::Poisoned(PoisonError::new(unsafe {
                    self.child.write(guard)
                })))
            }
            Err(TryLockError::WouldBlock) => Err(TryLockError::WouldBlock),
        }
    }
}

/// Observer for a value protected by [`Mutex`].
pub type MutexObserver<T, O, Head, Depth = Zero> = LockObserver<O, Mutex<T>, Head, Depth>;

/// A [`MutexGuard`] paired with the mutex's persistent child observer.
pub type ObservedMutexGuard<'a, T, O> = ObservedGuardMut<'a, MutexGuard<'a, T>, O>;

impl<T, O, Head: ?Sized, Depth> LockObserver<O, Mutex<T>, Head, Depth>
where
    O: Observer<Head = T, InnerDepth = Zero>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = Mutex<T>>,
{
    /// Locks the value and pairs the guard with its child observer.
    pub fn lock(&self) -> LockResult<ObservedMutexGuard<'_, T, O>> {
        self.acquire()
    }

    /// Attempts to lock the value and pair the guard with its child observer.
    pub fn try_lock(&self) -> TryLockResult<ObservedMutexGuard<'_, T, O>> {
        self.try_acquire()
    }
}

impl<T, Selection> Observe<Mutex<T>, Composite<(Selection,)>> for Mutex<T>
where
    T: Observe<T, Selection>,
{
    type Observer<Head, Depth>
        = MutexObserver<T, <T as Observe<T, Selection>>::Observer<T, Zero>, Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}

/// Observer for a value protected by [`RwLock`].
pub type RwLockObserver<T, O, Head, Depth = Zero> = LockObserver<O, RwLock<T>, Head, Depth>;

/// A [`RwLockReadGuard`] paired with the lock's persistent child observer.
pub type ObservedRwLockReadGuard<'a, T, O> = ObservedGuard<'a, RwLockReadGuard<'a, T>, O>;

/// A [`RwLockWriteGuard`] paired with the lock's persistent child observer.
pub type ObservedRwLockWriteGuard<'a, T, O> = ObservedGuardMut<'a, RwLockWriteGuard<'a, T>, O>;

impl<T, O, Head: ?Sized, Depth> LockObserver<O, RwLock<T>, Head, Depth>
where
    O: Observer<Head = T, InnerDepth = Zero>,
    Depth: Unsigned,
    Head: AsDerefMut<Depth, Target = RwLock<T>>,
{
    fn try_refresh_for_read(&self) -> bool {
        if !self.child.is_stale() {
            return true;
        }
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let lock = AsDeref::<Depth>::as_deref(head);
        let mut guard = match lock.try_write() {
            Ok(guard) => guard,
            Err(TryLockError::Poisoned(error)) => {
                self.child.fallback();
                error.into_inner()
            }
            Err(TryLockError::WouldBlock) => return false,
        };
        drop(unsafe { self.child.activate_ref(&mut *guard) });
        true
    }

    fn refresh_for_read(&self) {
        assert!(
            self.try_refresh_for_read(),
            "cannot rebuild an RwLock observer while a borrow remains active"
        );
    }

    /// Read-locks the value and pairs the guard with its shared child observer.
    pub fn read(&self) -> LockResult<ObservedRwLockReadGuard<'_, T, O>> {
        self.refresh_for_read();
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let lock = AsDeref::<Depth>::as_deref(head);
        match lock.read() {
            Ok(guard) => Ok(unsafe { self.child.read(guard) }),
            Err(error) => {
                let guard = error.into_inner();
                self.child.fallback();
                Err(PoisonError::new(unsafe { self.child.read(guard) }))
            }
        }
    }

    /// Attempts to read-lock the value and pair it with its shared child observer.
    pub fn try_read(&self) -> TryLockResult<ObservedRwLockReadGuard<'_, T, O>> {
        if !self.try_refresh_for_read() {
            return Err(TryLockError::WouldBlock);
        }
        let head = unsafe { Pointer::as_ref(&self.pointer) };
        let lock = AsDeref::<Depth>::as_deref(head);
        match lock.try_read() {
            Ok(guard) => Ok(unsafe { self.child.read(guard) }),
            Err(TryLockError::Poisoned(error)) => {
                let guard = error.into_inner();
                self.child.fallback();
                Err(TryLockError::Poisoned(PoisonError::new(unsafe {
                    self.child.read(guard)
                })))
            }
            Err(TryLockError::WouldBlock) => Err(TryLockError::WouldBlock),
        }
    }

    /// Write-locks the value and pairs the guard with its mutable child observer.
    pub fn write(&self) -> LockResult<ObservedRwLockWriteGuard<'_, T, O>> {
        self.acquire()
    }

    /// Attempts to write-lock the value and pair the guard with its mutable child observer.
    pub fn try_write(&self) -> TryLockResult<ObservedRwLockWriteGuard<'_, T, O>> {
        self.try_acquire()
    }
}

impl<T, Selection> Observe<RwLock<T>, Composite<(Selection,)>> for RwLock<T>
where
    T: Observe<T, Selection>,
{
    type Observer<Head, Depth>
        = RwLockObserver<T, <T as Observe<T, Selection>>::Observer<T, Zero>, Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}
