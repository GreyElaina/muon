//! Structural observation of fixed-size arrays from `core`.

use core::marker::PhantomData;
use core::ops::{Deref, DerefMut, Index, IndexMut};

use crate::{Collect, Composite, Field, Fields, Observe, Path};

use super::{AsDeref, AsDerefMut, Observer, Pointer, QuasiObserver, Succ, Unsigned, Zero};

/// Observer for `[T; N]` with one child observer per fixed position.
pub struct ArrayObserver<T, O, Head: ?Sized, Depth, const N: usize> {
    fields: Fields<[Field<O>; N]>,
    pointer: Pointer<Head>,

    marker: PhantomData<(T, Depth)>,
}

impl<T, O, Head: ?Sized, Depth, const N: usize> Deref for ArrayObserver<T, O, Head, Depth, N> {
    type Target = Pointer<Head>;

    fn deref(&self) -> &Self::Target {
        &self.pointer
    }
}

impl<T, O, Head: ?Sized, Depth, const N: usize> DerefMut for ArrayObserver<T, O, Head, Depth, N>
where
    O: QuasiObserver<Head = T, InnerDepth = Zero>,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = [T; N]>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        QuasiObserver::invalidate(self);
        &mut self.pointer
    }
}

impl<T, O, Head: ?Sized, Depth, const N: usize> Index<usize>
    for ArrayObserver<T, O, Head, Depth, N>
{
    type Output = Field<O>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.fields.0[index]
    }
}

impl<T, O, Head: ?Sized, Depth, const N: usize> IndexMut<usize>
    for ArrayObserver<T, O, Head, Depth, N>
{
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.fields.0[index]
    }
}

impl<T, O, Head: ?Sized, Depth, const N: usize> QuasiObserver
    for ArrayObserver<T, O, Head, Depth, N>
where
    T: Sized,
    O: QuasiObserver<Head = T, InnerDepth = Zero>,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = [T; N]>,
{
    type Head = Head;
    type OuterDepth = Succ<Zero>;
    type InnerDepth = Depth;

    fn invalidate(this: &mut Self) {
        for field in &mut this.fields.0 {
            QuasiObserver::invalidate(field);
        }
    }
}

unsafe impl<T, O, Head: ?Sized, Depth, const N: usize> Observer
    for ArrayObserver<T, O, Head, Depth, N>
where
    T: Sized,
    O: Observer<Head = T, InnerDepth = Zero>,
    Depth: Unsigned,
    Head: AsDeref<Depth, Target = [T; N]>,
{
    unsafe fn observe(head: *mut Head) -> Self {
        unsafe {
            let array = AsDeref::<Depth>::as_deref_ptr(head);
            let fields = core::array::from_fn(|index| {
                Field::indexed(O::observe(&raw mut (*array)[index]), index)
            });
            Self {
                fields: Fields::new(fields),
                pointer: Pointer::new_unchecked(head),

                marker: PhantomData,
            }
        }
    }

    unsafe fn relocate(this: &mut Self, head: *mut Head) {
        unsafe {
            let array = AsDeref::<Depth>::as_deref_ptr(head);
            for (index, field) in this.fields.0.iter_mut().enumerate() {
                O::relocate(field.observer_mut(), &raw mut (*array)[index]);
            }
            Pointer::set_unchecked(&this.pointer, head);
        }
    }

    unsafe fn rebase(this: &mut Self, head: *mut Head) {
        unsafe {
            let array = AsDeref::<Depth>::as_deref_ptr(head);
            for (index, field) in this.fields.0.iter_mut().enumerate() {
                O::rebase(field.observer_mut(), &raw mut (*array)[index]);
            }
            Pointer::set_unchecked(&this.pointer, head);
        }
    }
}

impl<T, O, Head: ?Sized, Depth, Context: ?Sized, Route, Error, Scopes, const N: usize>
    Collect<Context, Route, Error, Scopes> for ArrayObserver<T, O, Head, Depth, N>
where
    Fields<[Field<O>; N]>: Collect<Context, Route, Error, Scopes>,
{
    fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
        Collect::<Context, Route, Error, Scopes>::collect(&mut self.fields, path, context)
    }
}

impl<T, Selection, const N: usize> Observe<[T; N], Composite<(Selection,)>> for [T; N]
where
    T: Observe<T, Selection>,
{
    type Observer<Head, Depth>
        = ArrayObserver<T, <T as Observe<T, Selection>>::Observer<T, Zero>, Head, Depth, N>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}
