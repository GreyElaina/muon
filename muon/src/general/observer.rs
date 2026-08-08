use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

use crate::helper::{
    AsDeref, AsDerefMut, AsDerefPtrExt, Invalidate, Pointer, QuasiObserver, Succ, Unsigned, Zero,
};
use crate::observe::{Flush, FlushWith, Observer, QuasiSink, Sink};

/// A handler trait for implementing change detection strategies in [`GeneralObserver`].
///
/// [`GeneralHandler`] defines the interface for pluggable change detection strategies used
/// exclusively with [`GeneralObserver`]. Each handler implementation encapsulates a specific
/// approach to detecting whether a value has changed.
///
/// ## Example
///
/// A [`ShallowObserver`](super::ShallowObserver) implementation that treats any mutation through
/// [`DerefMut`] as a complete replacement:
///
/// ```
/// # use std::marker::PhantomData;
/// # use muon::general::{GeneralHandler, GeneralObserver};
/// # use muon::helper::Invalidate;
/// # use muon::observe::DefaultSpec;
/// struct ShallowHandler<T> {
///     mutated: bool,
///     phantom: PhantomData<T>,
/// }
///
/// impl<T> Invalidate<T> for ShallowHandler<T> {
///     fn invalidate(&mut self, _value: &T) {
///         self.mutated = true;
///     }
/// }
///
/// impl<T> GeneralHandler for ShallowHandler<T> {
///     type Target = T;
///     fn observe(_value: &T) -> Self {
///         Self { mutated: false, phantom: PhantomData }
///     }
/// }
///
/// type ShallowObserver<'ob, T> = GeneralObserver<'ob, T, ShallowHandler<T>>;
/// ```
pub trait GeneralHandler: Invalidate<Self::Target> {
    /// The observed value type that this handler tracks changes for.
    type Target: ?Sized;

    /// Implementation for [`Observer::observe`].
    fn observe(value: &Self::Target) -> Self;
}

/// A handler that can serialize changes for [`GeneralObserver`].
///
/// This trait extends [`GeneralHandler`] with serialization capabilities. A [`GeneralHandler`]
/// must implement [`SerializeHandler`] for its corresponding [`GeneralObserver`] to implement
/// [`Flush`].
///
/// The handler is responsible for producing the complete [`Changes`](crate::Changes)
/// stream, including the `before` snapshot of the pre-write value: the
/// handler is created by [`GeneralHandler::observe`] with the initial
/// value in hand, so it can capture the snapshot there.
pub trait SerializeHandler: GeneralHandler {
    /// Flushes all recorded changes into the sink.
    ///
    /// Must fully reset internal state so an immediately subsequent call reports nothing.
    fn flush<S: Sink + ?Sized>(&mut self, value: &Self::Target, sink: &mut S);
}

/// A handler that can only express replace-style changes.
///
/// This trait provides a simplified interface for handlers that only need to track whether the
/// observed value has changed.
pub trait ReplaceHandler: GeneralHandler {
    /// Returns whether the next flush would produce a [`Replace`](crate::Changed::Replace)
    /// change.
    fn is_replace(&self, value: &Self::Target) -> bool;
}

/// The blanket bridge from [`ReplaceHandler`] to [`SerializeHandler`].
///
/// A handler that only answers `is_replace` gains full serialization
/// support automatically: the boolean result is translated into a
/// whole-value `Replace` (without a `before` — the handler does not
/// capture snapshots), and the handler re-observes the value so the
/// next flush starts from the new baseline. Handlers that need
/// non-replace events (e.g. `UnsizeHandler`) implement
/// [`SerializeHandler`] directly.
impl<H> SerializeHandler for H
where
    H: ReplaceHandler,
    H::Target: serde::Serialize,
{
    fn flush<S: Sink + ?Sized>(&mut self, value: &Self::Target, sink: &mut S) {
        let is_replace = ReplaceHandler::is_replace(self, value);
        *self = H::observe(value);
        if is_replace {
            sink.replace(None, Some(&value as &dyn erased_serde::Serialize));
        }
    }
}

/// A helper trait for providing a custom name when formatting [`GeneralObserver`] with [`Debug`].
///
/// [`DebugHandler`] extends [`GeneralHandler`] by adding a [`NAME`](DebugHandler::NAME) constant
/// used as the type label in [`Debug`] output for [`GeneralObserver`].
///
/// ## Example
///
/// ```
/// # use std::marker::PhantomData;
/// use muon::general::{DebugHandler, GeneralHandler, GeneralObserver};
/// use muon::helper::Invalidate;
/// use muon::observe::Observer;
///
/// pub struct MyHandler<T>(PhantomData<T>);
///
/// impl<T> Invalidate<T> for MyHandler<T> {
///     fn invalidate(&mut self, _: &T) {}
/// }
///
/// impl<T> GeneralHandler for MyHandler<T> {
///     type Target = T;
///     fn observe(_value: &T) -> Self { Self(PhantomData) }
/// }
///
/// impl<T> DebugHandler for MyHandler<T> {
///     const NAME: &'static str = "MyObserver";
/// }
///
/// let mut value = 123;
/// let ob = unsafe { GeneralObserver::<MyHandler<i32>, i32>::observe(&mut value) };
/// println!("{:?}", ob); // prints: MyObserver(123)
/// ```
pub trait DebugHandler: GeneralHandler {
    /// The name displayed when formatting the observer with [`Debug`].
    const NAME: &'static str;
}

/// A general-purpose [`Observer`] implementation with extensible change detection strategies.
///
/// [`GeneralObserver`] provides a flexible framework for implementing different change detection
/// strategies through the [`GeneralHandler`] trait. It serves as the foundation for several
/// built-in observer types.
///
/// ## Capabilities and Limitations
///
/// [`GeneralObserver`] can:
/// - Detect whether a value has changed via [`DerefMut`]
/// - Produce [`Replace`](crate::Changed::Replace) changes when mutations are detected
///
/// [`GeneralObserver`] cannot:
/// - Track field-level changes or interior mutations within complex types
/// - Add specialized implementations for common traits (e.g. [`AddAssign`](std::ops::AddAssign))
///
/// For types that benefit from more sophisticated change tracking, muon provides specialized
/// observer implementations. These include built-in support for [`String`] and [`Vec`] (which can
/// track append operations), as well as custom observers generated by `#[derive(Observe)]` (which
/// can track field-level changes).
///
/// ## Built-in Implementations
///
/// The following observer types are built on [`GeneralObserver`]:
///
/// - [`ShallowObserver`](super::ShallowObserver) - Tracks any [`DerefMut`] access as a change
/// - [`NoopObserver`](super::NoopObserver) - Ignores all changes
/// - [`SnapshotObserver`](super::SnapshotObserver) - Compares cloned snapshots to detect changes
pub struct GeneralObserver<'ob, H, S: ?Sized, D = Zero> {
    ptr: Pointer<S>,
    handler: H,
    phantom: PhantomData<&'ob mut D>,
}

impl<'ob, H, S: ?Sized, D> Deref for GeneralObserver<'ob, H, S, D> {
    type Target = Pointer<S>;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

impl<'ob, H, S: ?Sized, D> DerefMut for GeneralObserver<'ob, H, S, D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        std::ptr::from_mut(self).expose_provenance();
        Pointer::invalidate(&mut self.ptr);
        &mut self.ptr
    }
}

impl<'ob, H, S: ?Sized, D, T: ?Sized> QuasiObserver for GeneralObserver<'ob, H, S, D>
where
    S: AsDeref<D, Target = T>,
    H: GeneralHandler<Target = T>,
    D: Unsigned,
{
    type Head = S;
    type OuterDepth = Succ<Zero>;
    type InnerDepth = D;

    fn invalidate(this: &mut Self) {
        H::invalidate(&mut this.handler, (*this.ptr).as_deref());
    }
}

impl<'ob, H, S: ?Sized, D, T: ?Sized> Observer for GeneralObserver<'ob, H, S, D>
where
    S: AsDeref<D, Target = T>,
    H: GeneralHandler<Target = T>,
    D: Unsigned,
{
    unsafe fn observe(head: *mut Self::Head) -> Self {
        unsafe {
            let this = Self {
                handler: H::observe(&*head.as_deref_ptr::<D>()),
                ptr: Pointer::new_unchecked(head),
                phantom: PhantomData,
            };
            Pointer::register_state::<_, D>(&this.ptr, &this.handler);
            this
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Self::Head) {
        unsafe { Pointer::set_unchecked(this, head) };
    }
}

impl<'ob, H, S: ?Sized, D, Sk: Sink + ?Sized> QuasiSink<Sk> for GeneralObserver<'ob, H, S, D> {
    type Operation = Sk::Operation;
    type Identity = Sk::Identity;
}

impl<'ob, H, S: ?Sized, D, T: ?Sized, Sk: Sink + ?Sized, Elem: ?Sized> FlushWith<Sk, Elem>
    for GeneralObserver<'ob, H, S, D>
where
    S: AsDeref<D, Target = T>,
    H: SerializeHandler<Target = T>,
    D: Unsigned,
{
    fn flush_with<F>(this: &mut Self, sink: &mut Sk, _flush_elem: F)
    where
        F: FnMut(&mut Elem, &mut Sk),
    {
        <Self as Flush<Sk>>::flush(this, sink)
    }
}

impl<'ob, H, S: ?Sized, D, T: ?Sized, Sk: Sink + ?Sized> Flush<Sk> for GeneralObserver<'ob, H, S, D>
where
    S: AsDeref<D, Target = T>,
    H: SerializeHandler<Target = T>,
    D: Unsigned,
{
    fn flush(this: &mut Self, sink: &mut Sk) {
        SerializeHandler::flush(&mut this.handler, (*this.ptr).as_deref(), sink)
    }
}

macro_rules! impl_fmt {
    ($($trait:ident),* $(,)?) => {
        $(
            impl<'ob, H, S: ?Sized, D> std::fmt::$trait for GeneralObserver<'ob, H, S, D>
            where
                H: GeneralHandler<Target = S::Target>,
                S: AsDeref<D>,
                D: Unsigned,
                S::Target: std::fmt::$trait,
            {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    std::fmt::$trait::fmt(self.untracked_ref(), f)
                }
            }
        )*
    };
}

impl_fmt! {
    Binary,
    Display,
    LowerExp,
    LowerHex,
    Octal,
    Pointer,
    UpperExp,
    UpperHex,
}

impl<'ob, H, S: ?Sized, D, T: ?Sized> Debug for GeneralObserver<'ob, H, S, D>
where
    S: AsDeref<D, Target = T>,
    H: DebugHandler<Target = T>,
    D: Unsigned,
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple(H::NAME).field(&self.untracked_ref()).finish()
    }
}

impl<'ob, H, S: ?Sized, D, I> std::ops::Index<I> for GeneralObserver<'ob, H, S, D>
where
    H: GeneralHandler<Target = S::Target>,
    S: AsDeref<D>,
    D: Unsigned,
    S::Target: std::ops::Index<I>,
{
    type Output = <S::Target as std::ops::Index<I>>::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.untracked_ref().index(index)
    }
}

impl<'ob, H, S: ?Sized, D, I> std::ops::IndexMut<I> for GeneralObserver<'ob, H, S, D>
where
    S: AsDerefMut<D>,
    H: GeneralHandler<Target = S::Target>,
    D: Unsigned,
    S::Target: std::ops::IndexMut<I>,
{
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.tracked_mut().index_mut(index)
    }
}

impl<'ob, H1, H2, S1: ?Sized, S2: ?Sized, D1, D2> PartialEq<GeneralObserver<'ob, H2, S2, D2>>
    for GeneralObserver<'ob, H1, S1, D1>
where
    H1: GeneralHandler<Target = S1::Target>,
    H2: GeneralHandler<Target = S2::Target>,
    S1: AsDeref<D1>,
    S2: AsDeref<D2>,
    D1: Unsigned,
    D2: Unsigned,
    S1::Target: PartialEq<S2::Target>,
{
    fn eq(&self, other: &GeneralObserver<'ob, H2, S2, D2>) -> bool {
        self.untracked_ref().eq(other.untracked_ref())
    }
}

impl<'ob, H, S: ?Sized, D> Eq for GeneralObserver<'ob, H, S, D>
where
    H: GeneralHandler<Target = S::Target>,
    S: AsDeref<D>,
    D: Unsigned,
    S::Target: Eq,
{
}

impl<'ob, H1, H2, S1: ?Sized, S2: ?Sized, D1, D2> PartialOrd<GeneralObserver<'ob, H2, S2, D2>>
    for GeneralObserver<'ob, H1, S1, D1>
where
    H1: GeneralHandler<Target = S1::Target>,
    H2: GeneralHandler<Target = S2::Target>,
    S1: AsDeref<D1>,
    S2: AsDeref<D2>,
    D1: Unsigned,
    D2: Unsigned,
    S1::Target: PartialOrd<S2::Target>,
{
    fn partial_cmp(&self, other: &GeneralObserver<'ob, H2, S2, D2>) -> Option<std::cmp::Ordering> {
        self.untracked_ref().partial_cmp(other.untracked_ref())
    }
}

impl<'ob, H, S: ?Sized, D> Ord for GeneralObserver<'ob, H, S, D>
where
    H: GeneralHandler<Target = S::Target>,
    S: AsDeref<D>,
    D: Unsigned,
    S::Target: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.untracked_ref().cmp(other.untracked_ref())
    }
}

macro_rules! impl_ops_assign {
    ($($trait:ident => $method:ident),* $(,)?) => {
        $(
            impl<'ob, H, S: ?Sized, D, T: ?Sized, U> std::ops::$trait<U> for GeneralObserver<'ob, H, S, D>
            where
                S: AsDerefMut<D, Target = T>,
                H: GeneralHandler<Target = T>,
                D: Unsigned,
                T: std::ops::$trait<U>,
            {
                fn $method(&mut self, rhs: U) {
                    self.tracked_mut().$method(rhs);
                }
            }
        )*
    };
}

impl_ops_assign! {
    AddAssign => add_assign,
    SubAssign => sub_assign,
    MulAssign => mul_assign,
    DivAssign => div_assign,
    RemAssign => rem_assign,
    BitAndAssign => bitand_assign,
    BitOrAssign => bitor_assign,
    BitXorAssign => bitxor_assign,
    ShlAssign => shl_assign,
    ShrAssign => shr_assign,
}

macro_rules! impl_ops_copy {
    ($($trait:ident => $method:ident),* $(,)?) => {
        $(
            impl<'ob, H, S: ?Sized, D, T: ?Sized, U> std::ops::$trait<U> for GeneralObserver<'ob, H, S, D>
            where
                H: GeneralHandler<Target = T>,
                S: AsDeref<D, Target = T>,
                D: Unsigned,
                T: std::ops::$trait<U> + Copy,
            {
                type Output = <T as std::ops::$trait<U>>::Output;

                fn $method(self, rhs: U) -> Self::Output {
                    self.untracked_ref().$method(rhs)
                }
            }
        )*
    };
}

impl_ops_copy! {
    Add => add,
    Sub => sub,
    Mul => mul,
    Div => div,
    Rem => rem,
    BitAnd => bitand,
    BitOr => bitor,
    BitXor => bitxor,
    Shl => shl,
    Shr => shr,
}

macro_rules! impl_ops_copy_unary {
    ($($trait:ident => $method:ident),* $(,)?) => {
        $(
            impl<'ob, H, S: ?Sized, D, T: ?Sized> std::ops::$trait for GeneralObserver<'ob, H, S, D>
            where
                H: GeneralHandler<Target = T>,
                S: AsDeref<D, Target = T>,
                D: Unsigned,
                T: std::ops::$trait + Copy,
            {
                type Output = <T as std::ops::$trait>::Output;

                fn $method(self) -> Self::Output {
                    (*self.untracked_ref()).$method()
                }
            }
        )*
    }
}

impl_ops_copy_unary! {
    Neg => neg,
    Not => not,
}
