use ::std::ops::{Deref, DerefMut};
#[allow(unused_imports)]
use muon::Observe;
use serde::Serialize;
#[rustfmt::skip]
#[derive(Serialize)]
pub struct Foo<T> {
    a: Vec<T>,
    b: i32,
}
#[rustfmt::skip]
#[allow(clippy::extra_unused_lifetimes)]
const _: () = {
    pub struct FooObserver<'ob, O> {
        a: O,
        b: ::muon::observe::DefaultObserver<'ob, i32>,
    }
    #[automatically_derived]
    impl<'ob, O> ::std::ops::Deref for FooObserver<'ob, O> {
        type Target = O;
        fn deref(&self) -> &Self::Target {
            &self.a
        }
    }
    #[automatically_derived]
    impl<'ob, O> ::std::ops::DerefMut for FooObserver<'ob, O> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            ::std::ptr::from_mut(self).expose_provenance();
            &mut self.a
        }
    }
    #[automatically_derived]
    impl<'ob, O, N> ::muon::helper::QuasiObserver for FooObserver<'ob, O>
    where
        O: ::muon::helper::QuasiObserver<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDeref<N>,
        N: ::muon::helper::Unsigned,
    {
        type Head = O::Head;
        type OuterDepth = ::muon::helper::Succ<O::OuterDepth>;
        type InnerDepth = N;
        fn invalidate(this: &mut Self) {
            ::muon::helper::QuasiObserver::invalidate(&mut this.b);
            ::muon::helper::QuasiObserver::invalidate(&mut this.a);
        }
    }
    #[automatically_derived]
    impl<'ob, T, O, N> ::muon::observe::Observer for FooObserver<'ob, O>
    where
        Vec<T>: 'ob,
        O: ::muon::observe::Observer<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDerefMut<N, Target = Foo<T>>,
        N: ::muon::helper::Unsigned,
    {
        #[inline(always)]
        unsafe fn observe(head: *mut O::Head) -> Self {
            unsafe {
                let __value = ::muon::helper::AsDeref::<N>::as_deref_ptr(head);
                let b = ::muon::observe::Observer::observe(&raw mut (*__value).b);
                let a = ::muon::observe::Observer::observe(head);
                let this = Self { a, b };
                let ptr = O::as_deref_coinductive(&this.a);
                ::muon::helper::Pointer::register_observer(ptr, &this.b);
                this
            }
        }
        unsafe fn relocate(this: &mut Self, head: *mut O::Head) {
            unsafe {
                let __value = ::muon::helper::AsDeref::<N>::as_deref_ptr(head);
                ::muon::observe::Observer::relocate(&mut this.b, &raw mut (*__value).b);
                ::muon::observe::Observer::relocate(&mut this.a, head);
            }
        }
    }
    #[automatically_derived]
    impl<'ob, O, Sk: ::muon::observe::Sink + ?Sized> ::muon::observe::QuasiSink<Sk>
    for FooObserver<'ob, O> {
        type Operation = Sk::Operation;
        type Identity = Sk::Identity;
    }
    #[automatically_derived]
    impl<
        'ob,
        T,
        O,
        N,
        Sk: ::muon::observe::Sink + ?Sized,
        Elem: ?Sized,
    > ::muon::observe::FlushWith<Sk, Elem> for FooObserver<'ob, O>
    where
        Foo<T>: ::muon::helper::serde::Serialize + 'static,
        Vec<T>: 'ob,
        O: ::muon::observe::Observer<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDerefMut<N, Target = Foo<T>>,
        O: ::muon::observe::Flush<Sk>,
        N: ::muon::helper::Unsigned,
        ::muon::observe::DefaultObserver<'ob, i32>: ::muon::observe::Flush<Sk>,
        O: ::muon::observe::Flush<Sk>,
    {
        fn flush_with<F>(this: &mut Self, sink: &mut Sk, _flush_elem: F)
        where
            F: FnMut(&mut Elem, &mut Sk),
        {
            <Self as ::muon::observe::Flush<Sk>>::flush(this, sink)
        }
    }
    #[automatically_derived]
    impl<'ob, T, O, N, Sk: ::muon::observe::Sink + ?Sized> ::muon::observe::Flush<Sk>
    for FooObserver<'ob, O>
    where
        Foo<T>: ::muon::helper::serde::Serialize + 'static,
        Vec<T>: 'ob,
        O: ::muon::observe::Observer<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDerefMut<N, Target = Foo<T>>,
        O: ::muon::observe::Flush<Sk>,
        N: ::muon::helper::Unsigned,
        ::muon::observe::DefaultObserver<'ob, i32>: ::muon::observe::Flush<Sk>,
        O: ::muon::observe::Flush<Sk>,
    {
        #[inline(always)]
        fn flush(this: &mut Self, sink: &mut Sk) {
            sink.push_field("a");
            ::muon::observe::Flush::flush(&mut this.a, sink);
            sink.pop_segment();
            sink.push_field("b");
            ::muon::observe::Flush::flush(&mut this.b, sink);
            sink.pop_segment();
        }
    }
    #[automatically_derived]
    impl<T> ::muon::Observe for Foo<T>
    where
        Self: ::muon::helper::serde::Serialize,
        Vec<T>: ::muon::Observe,
    {
        type Observer<'ob, S, N> = FooObserver<
            'ob,
            ::muon::observe::DefaultObserver<'ob, Vec<T>, S, ::muon::helper::Succ<N>>,
        >
        where
            Self: 'ob,
            N: ::muon::helper::Unsigned,
            S: ::muon::helper::AsDerefMut<N, Target = Self> + ?Sized + 'ob;
        type Spec = ::muon::observe::DefaultSpec;
    }
    #[automatically_derived]
    unsafe impl<T> ::muon::helper::DerefPtr for Foo<T> {
        unsafe fn deref_ptr(this: *mut Self) -> *mut Self::Target {
            unsafe { &raw mut (*this).a }
        }
    }
};
impl<T> Deref for Foo<T> {
    type Target = Vec<T>;
    fn deref(&self) -> &Self::Target {
        &self.a
    }
}
impl<T> DerefMut for Foo<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.a
    }
}
#[rustfmt::skip]
#[derive(Serialize)]
pub struct Bar(Qux, i32);
#[rustfmt::skip]
#[allow(clippy::extra_unused_lifetimes)]
const _: () = {
    pub struct BarObserver<'ob, O>(O, ::muon::observe::DefaultObserver<'ob, i32>);
    #[automatically_derived]
    impl<'ob, O> ::std::ops::Deref for BarObserver<'ob, O> {
        type Target = O;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }
    #[automatically_derived]
    impl<'ob, O> ::std::ops::DerefMut for BarObserver<'ob, O> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            ::std::ptr::from_mut(self).expose_provenance();
            &mut self.0
        }
    }
    #[automatically_derived]
    impl<'ob, O, N> ::muon::helper::QuasiObserver for BarObserver<'ob, O>
    where
        O: ::muon::helper::QuasiObserver<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDeref<N>,
        N: ::muon::helper::Unsigned,
    {
        type Head = O::Head;
        type OuterDepth = ::muon::helper::Succ<O::OuterDepth>;
        type InnerDepth = N;
        fn invalidate(this: &mut Self) {
            ::muon::helper::QuasiObserver::invalidate(&mut this.1);
            ::muon::helper::QuasiObserver::invalidate(&mut this.0);
        }
    }
    #[automatically_derived]
    impl<'ob, O, N> ::muon::observe::Observer for BarObserver<'ob, O>
    where
        O: ::muon::observe::Observer<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDerefMut<N, Target = Bar>,
        N: ::muon::helper::Unsigned,
    {
        #[inline(always)]
        unsafe fn observe(head: *mut O::Head) -> Self {
            unsafe {
                let __value = ::muon::helper::AsDeref::<N>::as_deref_ptr(head);
                let observer_1 = ::muon::observe::Observer::observe(
                    &raw mut (*__value).1,
                );
                let observer_0 = ::muon::observe::Observer::observe(head);
                let this = Self(observer_0, observer_1);
                let ptr = O::as_deref_coinductive(&this.0);
                ::muon::helper::Pointer::register_observer(ptr, &this.1);
                this
            }
        }
        unsafe fn relocate(this: &mut Self, head: *mut O::Head) {
            unsafe {
                let __value = ::muon::helper::AsDeref::<N>::as_deref_ptr(head);
                ::muon::observe::Observer::relocate(&mut this.1, &raw mut (*__value).1);
                ::muon::observe::Observer::relocate(&mut this.0, head);
            }
        }
    }
    #[automatically_derived]
    impl<'ob, O, Sk: ::muon::observe::Sink + ?Sized> ::muon::observe::QuasiSink<Sk>
    for BarObserver<'ob, O> {
        type Operation = Sk::Operation;
        type Identity = Sk::Identity;
    }
    #[automatically_derived]
    impl<
        'ob,
        O,
        N,
        Sk: ::muon::observe::Sink + ?Sized,
        Elem: ?Sized,
    > ::muon::observe::FlushWith<Sk, Elem> for BarObserver<'ob, O>
    where
        O: ::muon::observe::Observer<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDerefMut<N, Target = Bar>,
        O: ::muon::observe::Flush<Sk>,
        N: ::muon::helper::Unsigned,
        ::muon::observe::DefaultObserver<'ob, i32>: ::muon::observe::Flush<Sk>,
        O: ::muon::observe::Flush<Sk>,
    {
        fn flush_with<F>(this: &mut Self, sink: &mut Sk, _flush_elem: F)
        where
            F: FnMut(&mut Elem, &mut Sk),
        {
            <Self as ::muon::observe::Flush<Sk>>::flush(this, sink)
        }
    }
    #[automatically_derived]
    impl<'ob, O, N, Sk: ::muon::observe::Sink + ?Sized> ::muon::observe::Flush<Sk>
    for BarObserver<'ob, O>
    where
        O: ::muon::observe::Observer<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDerefMut<N, Target = Bar>,
        O: ::muon::observe::Flush<Sk>,
        N: ::muon::helper::Unsigned,
        ::muon::observe::DefaultObserver<'ob, i32>: ::muon::observe::Flush<Sk>,
        O: ::muon::observe::Flush<Sk>,
    {
        #[inline(always)]
        fn flush(this: &mut Self, sink: &mut Sk) {
            sink.push_index(0usize);
            ::muon::observe::Flush::flush(&mut this.0, sink);
            sink.pop_segment();
            sink.push_index(1usize);
            ::muon::observe::Flush::flush(&mut this.1, sink);
            sink.pop_segment();
        }
    }
    #[automatically_derived]
    impl ::muon::Observe for Bar {
        type Observer<'ob, S, N> = BarObserver<
            'ob,
            ::muon::general::ShallowObserver<'ob, Qux, S, ::muon::helper::Succ<N>>,
        >
        where
            Self: 'ob,
            N: ::muon::helper::Unsigned,
            S: ::muon::helper::AsDerefMut<N, Target = Self> + ?Sized + 'ob;
        type Spec = ::muon::observe::DefaultSpec;
    }
    #[automatically_derived]
    unsafe impl ::muon::helper::DerefPtr for Bar {
        unsafe fn deref_ptr(this: *mut Self) -> *mut Self::Target {
            unsafe { &raw mut (*this).0 }
        }
    }
};
impl Deref for Bar {
    type Target = Qux;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for Bar {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
#[rustfmt::skip]
#[derive(Serialize)]
pub struct Qux(pub i32);
#[rustfmt::skip]
#[allow(clippy::extra_unused_lifetimes)]
const _: () = {
    pub struct QuxObserver<O>(pub O);
    #[automatically_derived]
    impl<O> ::std::ops::Deref for QuxObserver<O> {
        type Target = O;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }
    #[automatically_derived]
    impl<O> ::std::ops::DerefMut for QuxObserver<O> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            ::std::ptr::from_mut(self).expose_provenance();
            &mut self.0
        }
    }
    #[automatically_derived]
    impl<O, N> ::muon::helper::QuasiObserver for QuxObserver<O>
    where
        O: ::muon::helper::QuasiObserver<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDeref<N>,
        N: ::muon::helper::Unsigned,
    {
        type Head = O::Head;
        type OuterDepth = ::muon::helper::Succ<O::OuterDepth>;
        type InnerDepth = N;
        fn invalidate(this: &mut Self) {
            ::muon::helper::QuasiObserver::invalidate(&mut this.0);
        }
    }
    #[automatically_derived]
    impl<O, N> ::muon::observe::Observer for QuxObserver<O>
    where
        O: ::muon::observe::Observer<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDerefMut<N, Target = Qux>,
        N: ::muon::helper::Unsigned,
    {
        #[inline(always)]
        unsafe fn observe(head: *mut O::Head) -> Self {
            unsafe {
                let observer_0 = ::muon::observe::Observer::observe(head);
                Self(observer_0)
            }
        }
        unsafe fn relocate(this: &mut Self, head: *mut O::Head) {
            unsafe {
                ::muon::observe::Observer::relocate(&mut this.0, head);
            }
        }
    }
    #[automatically_derived]
    impl<O, Sk: ::muon::observe::Sink + ?Sized> ::muon::observe::QuasiSink<Sk>
    for QuxObserver<O> {
        type Operation = Sk::Operation;
        type Identity = Sk::Identity;
    }
    #[automatically_derived]
    impl<
        O,
        N,
        Sk: ::muon::observe::Sink + ?Sized,
        Elem: ?Sized,
    > ::muon::observe::FlushWith<Sk, Elem> for QuxObserver<O>
    where
        O: ::muon::observe::Observer<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDerefMut<N, Target = Qux>,
        O: ::muon::observe::Flush<Sk>,
        N: ::muon::helper::Unsigned,
        O: ::muon::observe::Flush<Sk>,
    {
        fn flush_with<F>(this: &mut Self, sink: &mut Sk, _flush_elem: F)
        where
            F: FnMut(&mut Elem, &mut Sk),
        {
            <Self as ::muon::observe::Flush<Sk>>::flush(this, sink)
        }
    }
    #[automatically_derived]
    impl<O, N, Sk: ::muon::observe::Sink + ?Sized> ::muon::observe::Flush<Sk>
    for QuxObserver<O>
    where
        O: ::muon::observe::Observer<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDerefMut<N, Target = Qux>,
        O: ::muon::observe::Flush<Sk>,
        N: ::muon::helper::Unsigned,
        O: ::muon::observe::Flush<Sk>,
    {
        #[inline(always)]
        fn flush(this: &mut Self, sink: &mut Sk) {
            sink.push_index(0usize);
            ::muon::observe::Flush::flush(&mut this.0, sink);
            sink.pop_segment();
        }
    }
    #[automatically_derived]
    impl ::muon::Observe for Qux
    where
        i32: ::muon::Observe,
    {
        type Observer<'ob, S, N> = QuxObserver<
            ::muon::observe::DefaultObserver<'ob, i32, S, ::muon::helper::Succ<N>>,
        >
        where
            Self: 'ob,
            N: ::muon::helper::Unsigned,
            S: ::muon::helper::AsDerefMut<N, Target = Self> + ?Sized + 'ob;
        type Spec = ::muon::observe::DefaultSpec;
    }
    #[automatically_derived]
    unsafe impl ::muon::helper::DerefPtr for Qux {
        unsafe fn deref_ptr(this: *mut Self) -> *mut Self::Target {
            unsafe { &raw mut (*this).0 }
        }
    }
};
impl Deref for Qux {
    type Target = i32;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for Qux {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl ::muon::general::Snapshot for Qux {
    type Snapshot = i32;
    fn to_snapshot(&self) -> i32 {
        self.0
    }
}
impl ::muon::general::SerializeSnapshot for Qux {
    fn flush<S: ::muon::observe::Sink + ?Sized>(&self, snapshot: i32, sink: &mut S) {
        if self.0 != snapshot {
            sink.replace(
                Some(&snapshot as &dyn ::muon::erased_serde::Serialize),
                Some(&self.0 as &dyn ::muon::erased_serde::Serialize),
            );
        }
    }
}
#[rustfmt::skip]
#[derive(Serialize)]
pub struct Baz<T> {
    a: Vec<T>,
    b: i32,
}
#[rustfmt::skip]
#[allow(clippy::extra_unused_lifetimes)]
const _: () = {
    pub struct BazObserver<'ob, O> {
        a: O,
        b: ::muon::observe::DefaultObserver<'ob, i32>,
    }
    #[automatically_derived]
    impl<'ob, O> ::std::ops::Deref for BazObserver<'ob, O> {
        type Target = O;
        fn deref(&self) -> &Self::Target {
            &self.a
        }
    }
    #[automatically_derived]
    impl<'ob, O> ::std::ops::DerefMut for BazObserver<'ob, O> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            ::std::ptr::from_mut(self).expose_provenance();
            &mut self.a
        }
    }
    #[automatically_derived]
    impl<'ob, O, N> ::muon::helper::QuasiObserver for BazObserver<'ob, O>
    where
        O: ::muon::helper::QuasiObserver<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDeref<N>,
        N: ::muon::helper::Unsigned,
    {
        type Head = O::Head;
        type OuterDepth = ::muon::helper::Succ<O::OuterDepth>;
        type InnerDepth = N;
        fn invalidate(this: &mut Self) {
            ::muon::helper::QuasiObserver::invalidate(&mut this.b);
            ::muon::helper::QuasiObserver::invalidate(&mut this.a);
        }
    }
    #[automatically_derived]
    impl<'ob, T, O, N> ::muon::observe::Observer for BazObserver<'ob, O>
    where
        Vec<T>: 'ob,
        Vec<T>: ::muon::general::SerializeSnapshot + ::serde::Serialize,
        O: ::muon::observe::Observer<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDerefMut<N, Target = Baz<T>>,
        N: ::muon::helper::Unsigned,
    {
        #[inline(always)]
        unsafe fn observe(head: *mut O::Head) -> Self {
            unsafe {
                let __value = ::muon::helper::AsDeref::<N>::as_deref_ptr(head);
                let b = ::muon::observe::Observer::observe(&raw mut (*__value).b);
                let a = ::muon::observe::Observer::observe(head);
                let this = Self { a, b };
                let ptr = O::as_deref_coinductive(&this.a);
                ::muon::helper::Pointer::register_observer(ptr, &this.b);
                this
            }
        }
        unsafe fn relocate(this: &mut Self, head: *mut O::Head) {
            unsafe {
                let __value = ::muon::helper::AsDeref::<N>::as_deref_ptr(head);
                ::muon::observe::Observer::relocate(&mut this.b, &raw mut (*__value).b);
                ::muon::observe::Observer::relocate(&mut this.a, head);
            }
        }
    }
    #[automatically_derived]
    impl<'ob, O, Sk: ::muon::observe::Sink + ?Sized> ::muon::observe::QuasiSink<Sk>
    for BazObserver<'ob, O> {
        type Operation = Sk::Operation;
        type Identity = Sk::Identity;
    }
    #[automatically_derived]
    impl<
        'ob,
        T,
        O,
        N,
        Sk: ::muon::observe::Sink + ?Sized,
        Elem: ?Sized,
    > ::muon::observe::FlushWith<Sk, Elem> for BazObserver<'ob, O>
    where
        Baz<T>: ::muon::helper::serde::Serialize + 'static,
        Vec<T>: 'ob,
        Vec<T>: ::muon::general::SerializeSnapshot + ::serde::Serialize,
        O: ::muon::observe::Observer<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDerefMut<N, Target = Baz<T>>,
        O: ::muon::observe::Flush<Sk>,
        N: ::muon::helper::Unsigned,
        ::muon::observe::DefaultObserver<'ob, i32>: ::muon::observe::Flush<Sk>,
        O: ::muon::observe::Flush<Sk>,
    {
        fn flush_with<F>(this: &mut Self, sink: &mut Sk, _flush_elem: F)
        where
            F: FnMut(&mut Elem, &mut Sk),
        {
            <Self as ::muon::observe::Flush<Sk>>::flush(this, sink)
        }
    }
    #[automatically_derived]
    impl<'ob, T, O, N, Sk: ::muon::observe::Sink + ?Sized> ::muon::observe::Flush<Sk>
    for BazObserver<'ob, O>
    where
        Baz<T>: ::muon::helper::serde::Serialize + 'static,
        Vec<T>: 'ob,
        Vec<T>: ::muon::general::SerializeSnapshot + ::serde::Serialize,
        O: ::muon::observe::Observer<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDerefMut<N, Target = Baz<T>>,
        O: ::muon::observe::Flush<Sk>,
        N: ::muon::helper::Unsigned,
        ::muon::observe::DefaultObserver<'ob, i32>: ::muon::observe::Flush<Sk>,
        O: ::muon::observe::Flush<Sk>,
    {
        #[inline(always)]
        fn flush(this: &mut Self, sink: &mut Sk) {
            sink.push_field("a");
            ::muon::observe::Flush::flush(&mut this.a, sink);
            sink.pop_segment();
            sink.push_field("b");
            ::muon::observe::Flush::flush(&mut this.b, sink);
            sink.pop_segment();
        }
    }
    #[automatically_derived]
    impl<T> ::muon::Observe for Baz<T>
    where
        Self: ::muon::helper::serde::Serialize,
        Vec<T>: ::muon::general::SerializeSnapshot + ::serde::Serialize,
    {
        type Observer<'ob, S, N> = BazObserver<
            'ob,
            ::muon::general::ShallowObserver<'ob, Vec<T>, S, ::muon::helper::Succ<N>>,
        >
        where
            Self: 'ob,
            N: ::muon::helper::Unsigned,
            S: ::muon::helper::AsDerefMut<N, Target = Self> + ?Sized + 'ob;
        type Spec = ::muon::observe::DefaultSpec;
    }
    #[automatically_derived]
    unsafe impl<T> ::muon::helper::DerefPtr for Baz<T> {
        unsafe fn deref_ptr(this: *mut Self) -> *mut Self::Target {
            unsafe { &raw mut (*this).a }
        }
    }
};
impl<T> Deref for Baz<T> {
    type Target = Vec<T>;
    fn deref(&self) -> &Self::Target {
        &self.a
    }
}
impl<T> DerefMut for Baz<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.a
    }
}
#[rustfmt::skip]
#[derive(Serialize)]
pub struct SingleDeref<T>(Vec<T>);
#[rustfmt::skip]
#[allow(clippy::extra_unused_lifetimes)]
const _: () = {
    pub struct SingleDerefObserver<O>(O);
    #[automatically_derived]
    impl<O> ::std::ops::Deref for SingleDerefObserver<O> {
        type Target = O;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }
    #[automatically_derived]
    impl<O> ::std::ops::DerefMut for SingleDerefObserver<O> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            ::std::ptr::from_mut(self).expose_provenance();
            &mut self.0
        }
    }
    #[automatically_derived]
    impl<O, N> ::muon::helper::QuasiObserver for SingleDerefObserver<O>
    where
        O: ::muon::helper::QuasiObserver<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDeref<N>,
        N: ::muon::helper::Unsigned,
    {
        type Head = O::Head;
        type OuterDepth = ::muon::helper::Succ<O::OuterDepth>;
        type InnerDepth = N;
        fn invalidate(this: &mut Self) {
            ::muon::helper::QuasiObserver::invalidate(&mut this.0);
        }
    }
    #[automatically_derived]
    impl<'ob, T, O, N> ::muon::observe::Observer for SingleDerefObserver<O>
    where
        Vec<T>: 'ob,
        O: ::muon::observe::Observer<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDerefMut<N, Target = SingleDeref<T>>,
        N: ::muon::helper::Unsigned,
    {
        #[inline(always)]
        unsafe fn observe(head: *mut O::Head) -> Self {
            unsafe {
                let observer_0 = ::muon::observe::Observer::observe(head);
                Self(observer_0)
            }
        }
        unsafe fn relocate(this: &mut Self, head: *mut O::Head) {
            unsafe {
                ::muon::observe::Observer::relocate(&mut this.0, head);
            }
        }
    }
    #[automatically_derived]
    impl<O, Sk: ::muon::observe::Sink + ?Sized> ::muon::observe::QuasiSink<Sk>
    for SingleDerefObserver<O> {
        type Operation = Sk::Operation;
        type Identity = Sk::Identity;
    }
    #[automatically_derived]
    impl<
        'ob,
        T,
        O,
        N,
        Sk: ::muon::observe::Sink + ?Sized,
        Elem: ?Sized,
    > ::muon::observe::FlushWith<Sk, Elem> for SingleDerefObserver<O>
    where
        SingleDeref<T>: ::muon::helper::serde::Serialize + 'static,
        Vec<T>: 'ob,
        O: ::muon::observe::Observer<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDerefMut<N, Target = SingleDeref<T>>,
        O: ::muon::observe::Flush<Sk>,
        N: ::muon::helper::Unsigned,
        O: ::muon::observe::Flush<Sk>,
    {
        fn flush_with<F>(this: &mut Self, sink: &mut Sk, _flush_elem: F)
        where
            F: FnMut(&mut Elem, &mut Sk),
        {
            <Self as ::muon::observe::Flush<Sk>>::flush(this, sink)
        }
    }
    #[automatically_derived]
    impl<'ob, T, O, N, Sk: ::muon::observe::Sink + ?Sized> ::muon::observe::Flush<Sk>
    for SingleDerefObserver<O>
    where
        SingleDeref<T>: ::muon::helper::serde::Serialize + 'static,
        Vec<T>: 'ob,
        O: ::muon::observe::Observer<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDerefMut<N, Target = SingleDeref<T>>,
        O: ::muon::observe::Flush<Sk>,
        N: ::muon::helper::Unsigned,
        O: ::muon::observe::Flush<Sk>,
    {
        #[inline(always)]
        fn flush(this: &mut Self, sink: &mut Sk) {
            sink.push_index(0usize);
            ::muon::observe::Flush::flush(&mut this.0, sink);
            sink.pop_segment();
        }
    }
    #[automatically_derived]
    impl<T> ::muon::Observe for SingleDeref<T>
    where
        Self: ::muon::helper::serde::Serialize,
        Vec<T>: ::muon::Observe,
    {
        type Observer<'ob, S, N> = SingleDerefObserver<
            ::muon::observe::DefaultObserver<'ob, Vec<T>, S, ::muon::helper::Succ<N>>,
        >
        where
            Self: 'ob,
            N: ::muon::helper::Unsigned,
            S: ::muon::helper::AsDerefMut<N, Target = Self> + ?Sized + 'ob;
        type Spec = ::muon::observe::DefaultSpec;
    }
    #[automatically_derived]
    unsafe impl<T> ::muon::helper::DerefPtr for SingleDeref<T> {
        unsafe fn deref_ptr(this: *mut Self) -> *mut Self::Target {
            unsafe { &raw mut (*this).0 }
        }
    }
};
impl<T> Deref for SingleDeref<T> {
    type Target = Vec<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T> DerefMut for SingleDeref<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
#[rustfmt::skip]
#[derive(Serialize)]
pub struct SingleShallowDeref<T>(Vec<T>);
#[rustfmt::skip]
#[allow(clippy::extra_unused_lifetimes)]
const _: () = {
    pub struct SingleShallowDerefObserver<O>(O);
    #[automatically_derived]
    impl<O> ::std::ops::Deref for SingleShallowDerefObserver<O> {
        type Target = O;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }
    #[automatically_derived]
    impl<O> ::std::ops::DerefMut for SingleShallowDerefObserver<O> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            ::std::ptr::from_mut(self).expose_provenance();
            &mut self.0
        }
    }
    #[automatically_derived]
    impl<O, N> ::muon::helper::QuasiObserver for SingleShallowDerefObserver<O>
    where
        O: ::muon::helper::QuasiObserver<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDeref<N>,
        N: ::muon::helper::Unsigned,
    {
        type Head = O::Head;
        type OuterDepth = ::muon::helper::Succ<O::OuterDepth>;
        type InnerDepth = N;
        fn invalidate(this: &mut Self) {
            ::muon::helper::QuasiObserver::invalidate(&mut this.0);
        }
    }
    #[automatically_derived]
    impl<'ob, T, O, N> ::muon::observe::Observer for SingleShallowDerefObserver<O>
    where
        Vec<T>: 'ob,
        Vec<T>: ::muon::general::SerializeSnapshot + ::serde::Serialize,
        O: ::muon::observe::Observer<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDerefMut<N, Target = SingleShallowDeref<T>>,
        N: ::muon::helper::Unsigned,
    {
        #[inline(always)]
        unsafe fn observe(head: *mut O::Head) -> Self {
            unsafe {
                let observer_0 = ::muon::observe::Observer::observe(head);
                Self(observer_0)
            }
        }
        unsafe fn relocate(this: &mut Self, head: *mut O::Head) {
            unsafe {
                ::muon::observe::Observer::relocate(&mut this.0, head);
            }
        }
    }
    #[automatically_derived]
    impl<O, Sk: ::muon::observe::Sink + ?Sized> ::muon::observe::QuasiSink<Sk>
    for SingleShallowDerefObserver<O> {
        type Operation = Sk::Operation;
        type Identity = Sk::Identity;
    }
    #[automatically_derived]
    impl<
        'ob,
        T,
        O,
        N,
        Sk: ::muon::observe::Sink + ?Sized,
        Elem: ?Sized,
    > ::muon::observe::FlushWith<Sk, Elem> for SingleShallowDerefObserver<O>
    where
        SingleShallowDeref<T>: ::muon::helper::serde::Serialize + 'static,
        Vec<T>: 'ob,
        Vec<T>: ::muon::general::SerializeSnapshot + ::serde::Serialize,
        O: ::muon::observe::Observer<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDerefMut<N, Target = SingleShallowDeref<T>>,
        O: ::muon::observe::Flush<Sk>,
        N: ::muon::helper::Unsigned,
        O: ::muon::observe::Flush<Sk>,
    {
        fn flush_with<F>(this: &mut Self, sink: &mut Sk, _flush_elem: F)
        where
            F: FnMut(&mut Elem, &mut Sk),
        {
            <Self as ::muon::observe::Flush<Sk>>::flush(this, sink)
        }
    }
    #[automatically_derived]
    impl<'ob, T, O, N, Sk: ::muon::observe::Sink + ?Sized> ::muon::observe::Flush<Sk>
    for SingleShallowDerefObserver<O>
    where
        SingleShallowDeref<T>: ::muon::helper::serde::Serialize + 'static,
        Vec<T>: 'ob,
        Vec<T>: ::muon::general::SerializeSnapshot + ::serde::Serialize,
        O: ::muon::observe::Observer<InnerDepth = ::muon::helper::Succ<N>>,
        O::Head: ::muon::helper::AsDerefMut<N, Target = SingleShallowDeref<T>>,
        O: ::muon::observe::Flush<Sk>,
        N: ::muon::helper::Unsigned,
        O: ::muon::observe::Flush<Sk>,
    {
        #[inline(always)]
        fn flush(this: &mut Self, sink: &mut Sk) {
            sink.push_index(0usize);
            ::muon::observe::Flush::flush(&mut this.0, sink);
            sink.pop_segment();
        }
    }
    #[automatically_derived]
    impl<T> ::muon::Observe for SingleShallowDeref<T>
    where
        Self: ::muon::helper::serde::Serialize,
        Vec<T>: ::muon::general::SerializeSnapshot + ::serde::Serialize,
    {
        type Observer<'ob, S, N> = SingleShallowDerefObserver<
            ::muon::general::ShallowObserver<'ob, Vec<T>, S, ::muon::helper::Succ<N>>,
        >
        where
            Self: 'ob,
            N: ::muon::helper::Unsigned,
            S: ::muon::helper::AsDerefMut<N, Target = Self> + ?Sized + 'ob;
        type Spec = ::muon::observe::DefaultSpec;
    }
    #[automatically_derived]
    unsafe impl<T> ::muon::helper::DerefPtr for SingleShallowDeref<T> {
        unsafe fn deref_ptr(this: *mut Self) -> *mut Self::Target {
            unsafe { &raw mut (*this).0 }
        }
    }
};
impl<T> Deref for SingleShallowDeref<T> {
    type Target = Vec<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T> DerefMut for SingleShallowDeref<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
