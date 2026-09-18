//! Observation of standard range products from `core`.

use core::marker::PhantomData;
use core::ops::{Deref, DerefMut, Range, RangeFrom, RangeInclusive, RangeTo, RangeToInclusive};

use crate::{Collect, Composite, Field, Fields, Observe, Path};

use super::{
    AsDeref, AsDerefMut, Observer, Pointer, QuasiObserver, ShallowObserver, Succ, Unsigned, Zero,
};

macro_rules! range_observer {
    (
        $range:ident => $name:ident {
            $(($field:ident, $observer:ident, $selection:ident, $route:ident)),+ $(,)?
        }
    ) => {
        #[doc = concat!("Observer for [`", stringify!($range), "`].")]
        pub struct $name<T, $($observer,)* Head: ?Sized, Depth = Zero> {
            $(
                #[doc = concat!("Observer for the `", stringify!($field), "` field.")]
                pub $field: Field<$observer>,
            )*
            pointer: Pointer<Head>,

            marker: PhantomData<(T, Depth)>,
        }

        impl<T, $($observer,)* Head: ?Sized, Depth> Deref
            for $name<T, $($observer,)* Head, Depth>
        {
            type Target = Pointer<Head>;

            fn deref(&self) -> &Self::Target {
                &self.pointer
            }
        }

        impl<T, $($observer,)* Head: ?Sized, Depth> DerefMut
            for $name<T, $($observer,)* Head, Depth>
        where
            $($observer: QuasiObserver<Head = T, InnerDepth = Zero>,)*
            Depth: Unsigned,
            Head: AsDeref<Depth, Target = $range<T>>,
        {
            fn deref_mut(&mut self) -> &mut Self::Target {
                QuasiObserver::invalidate(self);
                &mut self.pointer
            }
        }

        impl<T, $($observer,)* Head: ?Sized, Depth> QuasiObserver
            for $name<T, $($observer,)* Head, Depth>
        where
            T: Sized,
            $($observer: QuasiObserver<Head = T, InnerDepth = Zero>,)*
            Depth: Unsigned,
            Head: AsDeref<Depth, Target = $range<T>>,
        {
            type Head = Head;
            type OuterDepth = Succ<Zero>;
            type InnerDepth = Depth;

            fn invalidate(this: &mut Self) {
                $(QuasiObserver::invalidate(&mut this.$field);)*
            }

        }

        unsafe impl<T, $($observer,)* Head: ?Sized, Depth> Observer
            for $name<T, $($observer,)* Head, Depth>
        where
            T: Sized,
            $($observer: Observer<Head = T, InnerDepth = Zero>,)*
            Depth: Unsigned,
            Head: AsDeref<Depth, Target = $range<T>>,
        {
            unsafe fn observe(head: *mut Head) -> Self {
                unsafe {
                    let range = AsDeref::<Depth>::as_deref_ptr(head);
                    Self {
                        $($field: Field::named($observer::observe(&raw mut (*range).$field), stringify!($field)),)*
                        pointer: Pointer::new_unchecked(head),

                        marker: PhantomData,
                    }
                }
            }

            unsafe fn relocate(this: &mut Self, head: *mut Head) {
                unsafe {
                    let range = AsDeref::<Depth>::as_deref_ptr(head);
                    $(
                        $observer::relocate(
                            this.$field.observer_mut(),
                            &raw mut (*range).$field,
                        );
                    )*
                    Pointer::set_unchecked(&this.pointer, head);
                }
            }

            unsafe fn rebase(this: &mut Self, head: *mut Head) {
                unsafe {
                    let range = AsDeref::<Depth>::as_deref_ptr(head);
                    $(
                        $observer::rebase(
                            this.$field.observer_mut(),
                            &raw mut (*range).$field,
                        );
                    )*
                    Pointer::set_unchecked(&this.pointer, head);
                }
            }
        }

        impl<
            T,
            $($observer,)*
            Head: ?Sized,
            Depth,
            Context: ?Sized,
            $($route,)*
            Error,
            Scopes,
        > Collect<Context, ($($route,)*), Error, Scopes>
            for $name<T, $($observer,)* Head, Depth>
        where
            $(Field<$observer>: Collect<Context, $route, Error, Scopes>,)*
        {
            fn collect(&mut self, path: &Path<'_>, context: &mut Context) -> Result<(), Error> {
                let mut fields = Fields::new(($(&mut self.$field,)*));
                Collect::<Context, ($($route,)*), Error, Scopes>::collect(
                    &mut fields,
                    path,
                    context,
                )
            }
        }

        impl<T, $($selection,)*> Observe<$range<T>, Composite<($($selection,)*)>> for $range<T>
        where
            $(T: Observe<T, $selection>,)*
        {
            type Observer<Head, Depth>
                = $name<
                    T,
                    $(<T as Observe<T, $selection>>::Observer<T, Zero>,)*
                    Head,
                    Depth,
                >
            where
                Depth: Unsigned,
                Head: AsDerefMut<Depth, Target = Self> + ?Sized;
        }
    };
}

range_observer!(Range => RangeObserver {
    (start, StartObserver, StartSelection, StartRoute),
    (end, EndObserver, EndSelection, EndRoute),
});
range_observer!(RangeFrom => RangeFromObserver {
    (start, StartObserver, StartSelection, StartRoute),
});
range_observer!(RangeTo => RangeToObserver {
    (end, EndObserver, EndSelection, EndRoute),
});
range_observer!(RangeToInclusive => RangeToInclusiveObserver {
    (end, EndObserver, EndSelection, EndRoute),
});

impl<T> Observe for RangeInclusive<T> {
    type Observer<Head, Depth>
        = ShallowObserver<Self, Head, Depth>
    where
        Depth: Unsigned,
        Head: AsDerefMut<Depth, Target = Self> + ?Sized;
}
