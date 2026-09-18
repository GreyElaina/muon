//! Structural observation of statically sized tuple products from `core`.

use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};

use crate::{Collect, Composite, Field, Fields, Observe, Path};

use super::{AsDeref, AsDerefMut, Observer, Pointer, QuasiObserver, Succ, Unsigned, Zero};

macro_rules! tuple_observer {
    (
        $name:ident, $pointer:tt;
        $(($index:tt, $observer:ident, $target:ident, $selection:ident, $route:ident)),+ $(,)?
    ) => {
        #[doc = concat!("Observer for a ", stringify!($pointer), "-element tuple.")]
        pub struct $name<$($observer,)* Head: ?Sized, Depth = Zero>(
            $(pub Field<$observer>,)*
            Pointer<Head>,

            PhantomData<Depth>,
        );

        impl<$($observer,)* Head: ?Sized, Depth> Deref
            for $name<$($observer,)* Head, Depth>
        {
            type Target = Pointer<Head>;

            fn deref(&self) -> &Self::Target {
                &self.$pointer
            }
        }

        impl<$($observer,)* Head: ?Sized, Depth> DerefMut
            for $name<$($observer,)* Head, Depth>
        where
            $($observer: QuasiObserver<Head: Sized>,)*
            Depth: Unsigned,
            Head: AsDeref<Depth, Target = ($($observer::Head,)*)>,
        {
            fn deref_mut(&mut self) -> &mut Self::Target {
                QuasiObserver::invalidate(self);
                &mut self.$pointer
            }
        }

        impl<$($observer,)* Head: ?Sized, Depth> QuasiObserver
            for $name<$($observer,)* Head, Depth>
        where
            $($observer: QuasiObserver<Head: Sized>,)*
            Depth: Unsigned,
            Head: AsDeref<Depth, Target = ($($observer::Head,)*)>,
        {
            type Head = Head;
            type OuterDepth = Succ<Zero>;
            type InnerDepth = Depth;

            fn invalidate(this: &mut Self) {
                $(QuasiObserver::invalidate(&mut this.$index);)*
            }

        }

        unsafe impl<$($observer,)* Head: ?Sized, Depth> Observer
            for $name<$($observer,)* Head, Depth>
        where
            $($observer: Observer<InnerDepth = Zero, Head: Sized>,)*
            Depth: Unsigned,
            Head: AsDeref<Depth, Target = ($($observer::Head,)*)>,
        {
            unsafe fn observe(head: *mut Head) -> Self {
                unsafe {
                    let tuple = AsDeref::<Depth>::as_deref_ptr(head);
                    Self(
                        $(Field::indexed($observer::observe(&raw mut (*tuple).$index), $index),)*
                        Pointer::new_unchecked(head),

                        PhantomData,
                    )
                }
            }

            unsafe fn relocate(this: &mut Self, head: *mut Head) {
                unsafe {
                    let tuple = AsDeref::<Depth>::as_deref_ptr(head);
                    $(
                        $observer::relocate(
                            this.$index.observer_mut(),
                            &raw mut (*tuple).$index,
                        );
                    )*
                    Pointer::set_unchecked(&this.$pointer, head);
                }
            }

            unsafe fn rebase(this: &mut Self, head: *mut Head) {
                unsafe {
                    let tuple = AsDeref::<Depth>::as_deref_ptr(head);
                    $(
                        $observer::rebase(
                            this.$index.observer_mut(),
                            &raw mut (*tuple).$index,
                        );
                    )*
                    Pointer::set_unchecked(&this.$pointer, head);
                }
            }
        }

        impl<$($observer,)* Head: ?Sized, Depth, Context: ?Sized, $($route,)* Error, Scopes>
            Collect<Context, ($($route,)*), Error, Scopes>
            for $name<$($observer,)* Head, Depth>
        where
            $(Field<$observer>: Collect<Context, $route, Error, Scopes>,)*
        {
            fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
                let mut fields = Fields::new(($(&mut self.$index,)*));
                Collect::<Context, ($($route,)*), Error, Scopes>::collect(
                    &mut fields,
                    path,
                    context,
                )
            }
        }

        impl<$($target, $selection,)*> Observe<($($target,)*), Composite<($($selection,)*)>>
            for ($($target,)*)
        where
            $($target: Observe<$target, $selection>,)*
        {
            type Observer<Head, Depth>
                = $name<
                    $(<$target as Observe<$target, $selection>>::Observer<$target, Zero>,)*
                    Head,
                    Depth,
                >
            where
                Depth: Unsigned,
                Head: AsDerefMut<Depth, Target = Self> + ?Sized;
        }
    };
}

macro_rules! tuple_observers {
    ($(($name:ident, $pointer:tt, $index:tt, $observer:ident, $target:ident, $selection:ident, $route:ident)),+ $(,)?) => {
        tuple_observers!(@prefix [] ; $(($name, $pointer, $index, $observer, $target, $selection, $route)),+);
    };
    (@prefix [$($prefix:tt)*] ;
        ($name:ident, $pointer:tt, $index:tt, $observer:ident, $target:ident, $selection:ident, $route:ident)
        $(, $rest:tt)*
    ) => {
        tuple_observer!(
            $name,
            $pointer;
            $($prefix)*
            ($index, $observer, $target, $selection, $route),
        );
        tuple_observers!(
            @prefix [$($prefix)* ($index, $observer, $target, $selection, $route),] ;
            $($rest),*
        );
    };
    (@prefix [$($prefix:tt)*] ;) => {};
}

tuple_observers!(
    (TupleObserver, 1, 0, O0, T0, S0, R0),
    (TupleObserver2, 2, 1, O1, T1, S1, R1),
    (TupleObserver3, 3, 2, O2, T2, S2, R2),
    (TupleObserver4, 4, 3, O3, T3, S3, R3),
    (TupleObserver5, 5, 4, O4, T4, S4, R4),
    (TupleObserver6, 6, 5, O5, T5, S5, R5),
    (TupleObserver7, 7, 6, O6, T6, S6, R6),
    (TupleObserver8, 8, 7, O7, T7, S7, R7),
    (TupleObserver9, 9, 8, O8, T8, S8, R8),
    (TupleObserver10, 10, 9, O9, T9, S9, R9),
    (TupleObserver11, 11, 10, O10, T10, S10, R10),
    (TupleObserver12, 12, 11, O11, T11, S11, R11),
);
