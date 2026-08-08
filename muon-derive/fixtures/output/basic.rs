use ::std::collections::HashMap;
use ::std::fmt::Display;
#[allow(unused_imports)]
use muon::Observe;
use serde::Serialize;
#[rustfmt::skip]
#[derive(Debug, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub struct Foo {
    r#a: i32,
    #[serde(rename = "bar")]
    b: String,
    #[serde(flatten)]
    c: HashMap<String, i32>,
}
#[rustfmt::skip]
#[allow(clippy::extra_unused_lifetimes)]
const _: () = {
    pub struct FooObserver<'ob, S: ?Sized, N = ::muon::helper::Zero> {
        r#a: ::muon::observe::DefaultObserver<'ob, i32>,
        b: ::muon::observe::DefaultObserver<'ob, String>,
        c: ::muon::observe::DefaultObserver<'ob, HashMap<String, i32>>,
        __ptr: ::muon::helper::Pointer<S>,
        __phantom: ::std::marker::PhantomData<&'ob mut N>,
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::std::ops::Deref for FooObserver<'ob, S, N> {
        type Target = ::muon::helper::Pointer<S>;
        fn deref(&self) -> &Self::Target {
            &self.__ptr
        }
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::std::ops::DerefMut for FooObserver<'ob, S, N> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            ::std::ptr::from_mut(self).expose_provenance();
            ::muon::helper::QuasiObserver::invalidate(&mut self.__ptr);
            ::muon::helper::QuasiObserver::invalidate(&mut self.r#a);
            ::muon::helper::QuasiObserver::invalidate(&mut self.b);
            ::muon::helper::QuasiObserver::invalidate(&mut self.c);
            &mut self.__ptr
        }
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::muon::helper::QuasiObserver for FooObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDeref<N>,
        N: ::muon::helper::Unsigned,
    {
        type Head = S;
        type OuterDepth = ::muon::helper::Succ<::muon::helper::Zero>;
        type InnerDepth = N;
        fn invalidate(this: &mut Self) {
            ::muon::helper::QuasiObserver::invalidate(&mut this.r#a);
            ::muon::helper::QuasiObserver::invalidate(&mut this.b);
            ::muon::helper::QuasiObserver::invalidate(&mut this.c);
        }
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::muon::observe::Observer for FooObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDerefMut<N, Target = Foo>,
        N: ::muon::helper::Unsigned,
    {
        #[inline(always)]
        unsafe fn observe(head: *mut S) -> Self {
            unsafe {
                let __value = ::muon::helper::AsDeref::<N>::as_deref_ptr(head);
                let r#a = ::muon::observe::Observer::observe(&raw mut (*__value).r#a);
                let b = ::muon::observe::Observer::observe(&raw mut (*__value).b);
                let c = ::muon::observe::Observer::observe(&raw mut (*__value).c);
                Self {
                    r#a,
                    b,
                    c,
                    __ptr: ::muon::helper::Pointer::new_unchecked(head),
                    __phantom: ::std::marker::PhantomData,
                }
            }
        }
        unsafe fn relocate(this: &mut Self, head: *mut S) {
            unsafe {
                let __value = ::muon::helper::AsDeref::<N>::as_deref_ptr(head);
                ::muon::observe::Observer::relocate(
                    &mut this.r#a,
                    &raw mut (*__value).r#a,
                );
                ::muon::observe::Observer::relocate(&mut this.b, &raw mut (*__value).b);
                ::muon::observe::Observer::relocate(&mut this.c, &raw mut (*__value).c);
                ::muon::helper::Pointer::set_unchecked(this, head);
            }
        }
    }
    #[automatically_derived]
    impl<
        'ob,
        S: ?Sized,
        N,
        Sk: ::muon::observe::Sink + ?Sized,
    > ::muon::observe::QuasiSink<Sk> for FooObserver<'ob, S, N> {
        type Operation = Sk::Operation;
        type Identity = Sk::Identity;
    }
    #[automatically_derived]
    impl<
        'ob,
        S: ?Sized,
        N,
        Sk: ::muon::observe::Sink + ?Sized,
        Elem: ?Sized,
    > ::muon::observe::FlushWith<Sk, Elem> for FooObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDerefMut<N, Target = Foo>,
        N: ::muon::helper::Unsigned,
        ::muon::observe::DefaultObserver<'ob, i32>: ::muon::observe::Flush<Sk>,
        ::muon::observe::DefaultObserver<'ob, String>: ::muon::observe::Flush<Sk>,
        <::muon::observe::DefaultObserver<
            'ob,
            HashMap<String, i32>,
        > as ::muon::observe::QuasiSink<
            Sk,
        >>::Operation: ::std::convert::Into<Sk::Operation>,
        <::muon::observe::DefaultObserver<
            'ob,
            HashMap<String, i32>,
        > as ::muon::observe::QuasiSink<
            Sk,
        >>::Identity: ::std::convert::Into<Sk::Identity>,
    {
        fn flush_with<F>(this: &mut Self, sink: &mut Sk, _flush_elem: F)
        where
            F: FnMut(&mut Elem, &mut Sk),
        {
            <Self as ::muon::observe::Flush<Sk>>::flush(this, sink)
        }
    }
    #[automatically_derived]
    impl<
        'ob,
        S: ?Sized,
        N,
        Sk: ::muon::observe::Sink + ?Sized,
    > ::muon::observe::Flush<Sk> for FooObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDerefMut<N, Target = Foo>,
        N: ::muon::helper::Unsigned,
        ::muon::observe::DefaultObserver<'ob, i32>: ::muon::observe::Flush<Sk>,
        ::muon::observe::DefaultObserver<'ob, String>: ::muon::observe::Flush<Sk>,
        <::muon::observe::DefaultObserver<
            'ob,
            HashMap<String, i32>,
        > as ::muon::observe::QuasiSink<
            Sk,
        >>::Operation: ::std::convert::Into<Sk::Operation>,
        <::muon::observe::DefaultObserver<
            'ob,
            HashMap<String, i32>,
        > as ::muon::observe::QuasiSink<
            Sk,
        >>::Identity: ::std::convert::Into<Sk::Identity>,
    {
        #[inline(always)]
        fn flush(this: &mut Self, sink: &mut Sk) {
            sink.push_field("A");
            ::muon::observe::Flush::flush(&mut this.r#a, sink);
            sink.pop_segment();
            sink.push_field("bar");
            ::muon::observe::Flush::flush(&mut this.b, sink);
            sink.pop_segment();
            sink.push_field("C");
            <::muon::observe::DefaultObserver<
                'ob,
                HashMap<String, i32>,
            > as ::muon::observe::FlushWith<
                Sk,
                ::muon::observe::DefaultObserver<'ob, i32>,
            >>::flush_with(
                &mut this.c,
                sink,
                |e, s| <::muon::observe::DefaultObserver<
                    'ob,
                    i32,
                > as ::muon::observe::Flush<Sk>>::flush(e, s),
            );
            sink.pop_segment();
        }
    }
    #[automatically_derived]
    impl ::muon::Observe for Foo {
        type Observer<'ob, S, N> = FooObserver<'ob, S, N>
        where
            Self: 'ob,
            N: ::muon::helper::Unsigned,
            S: ::muon::helper::AsDerefMut<N, Target = Self> + ?Sized + 'ob;
        type Spec = ::muon::observe::DefaultSpec;
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::std::fmt::Debug for FooObserver<'ob, S, N>
    where
        ::muon::observe::DefaultObserver<'ob, i32>: ::std::fmt::Debug,
        ::muon::observe::DefaultObserver<'ob, String>: ::std::fmt::Debug,
        ::muon::observe::DefaultObserver<'ob, HashMap<String, i32>>: ::std::fmt::Debug,
    {
        fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
            f.debug_struct("FooObserver")
                .field("a", &self.r#a)
                .field("b", &self.b)
                .field("c", &self.c)
                .finish()
        }
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::std::fmt::Display for FooObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDerefMut<N, Target = Foo>,
        N: ::muon::helper::Unsigned,
    {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let inner = ::muon::helper::QuasiObserver::untracked_ref(self);
            ::std::fmt::Display::fmt(inner, f)
        }
    }
};
impl Display for Foo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Foo {{ a: {}, b: {} }}", self.a, self.b)
    }
}
#[rustfmt::skip]
#[derive(Serialize)]
pub struct Bar(i32);
#[rustfmt::skip]
pub struct BarObserver<'ob, S: ?Sized, N = ::muon::helper::Zero>(
    ::muon::observe::DefaultObserver<'ob, i32>,
    ::muon::helper::Pointer<S>,
    ::std::marker::PhantomData<&'ob mut N>,
);
#[rustfmt::skip]
#[automatically_derived]
impl<'ob, S: ?Sized, N> ::std::ops::Deref for BarObserver<'ob, S, N> {
    type Target = ::muon::helper::Pointer<S>;
    fn deref(&self) -> &Self::Target {
        &self.1
    }
}
#[rustfmt::skip]
#[automatically_derived]
impl<'ob, S: ?Sized, N> ::std::ops::DerefMut for BarObserver<'ob, S, N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        ::std::ptr::from_mut(self).expose_provenance();
        ::muon::helper::QuasiObserver::invalidate(&mut self.1);
        ::muon::helper::QuasiObserver::invalidate(&mut self.0);
        &mut self.1
    }
}
#[rustfmt::skip]
#[automatically_derived]
impl<'ob, S: ?Sized, N> ::muon::helper::QuasiObserver for BarObserver<'ob, S, N>
where
    S: ::muon::helper::AsDeref<N>,
    N: ::muon::helper::Unsigned,
{
    type Head = S;
    type OuterDepth = ::muon::helper::Succ<::muon::helper::Zero>;
    type InnerDepth = N;
    fn invalidate(this: &mut Self) {
        ::muon::helper::QuasiObserver::invalidate(&mut this.0);
    }
}
#[rustfmt::skip]
#[automatically_derived]
impl<'ob, S: ?Sized, N> ::muon::observe::Observer for BarObserver<'ob, S, N>
where
    S: ::muon::helper::AsDerefMut<N, Target = Bar>,
    N: ::muon::helper::Unsigned,
{
    #[inline(always)]
    unsafe fn observe(head: *mut S) -> Self {
        unsafe {
            let __value = ::muon::helper::AsDeref::<N>::as_deref_ptr(head);
            let observer_0 = ::muon::observe::Observer::observe(&raw mut (*__value).0);
            Self(
                observer_0,
                ::muon::helper::Pointer::new_unchecked(head),
                ::std::marker::PhantomData,
            )
        }
    }
    unsafe fn relocate(this: &mut Self, head: *mut S) {
        unsafe {
            let __value = ::muon::helper::AsDeref::<N>::as_deref_ptr(head);
            ::muon::observe::Observer::relocate(&mut this.0, &raw mut (*__value).0);
            ::muon::helper::Pointer::set_unchecked(this, head);
        }
    }
}
#[rustfmt::skip]
#[automatically_derived]
impl<
    'ob,
    S: ?Sized,
    N,
    Sk: ::muon::observe::Sink + ?Sized,
> ::muon::observe::QuasiSink<Sk> for BarObserver<'ob, S, N> {
    type Operation = Sk::Operation;
    type Identity = Sk::Identity;
}
#[rustfmt::skip]
#[automatically_derived]
impl<
    'ob,
    S: ?Sized,
    N,
    Sk: ::muon::observe::Sink + ?Sized,
    Elem: ?Sized,
> ::muon::observe::FlushWith<Sk, Elem> for BarObserver<'ob, S, N>
where
    S: ::muon::helper::AsDerefMut<N, Target = Bar>,
    N: ::muon::helper::Unsigned,
    ::muon::observe::DefaultObserver<'ob, i32>: ::muon::observe::Flush<Sk>,
{
    fn flush_with<F>(this: &mut Self, sink: &mut Sk, _flush_elem: F)
    where
        F: FnMut(&mut Elem, &mut Sk),
    {
        <Self as ::muon::observe::Flush<Sk>>::flush(this, sink)
    }
}
#[rustfmt::skip]
#[automatically_derived]
impl<'ob, S: ?Sized, N, Sk: ::muon::observe::Sink + ?Sized> ::muon::observe::Flush<Sk>
for BarObserver<'ob, S, N>
where
    S: ::muon::helper::AsDerefMut<N, Target = Bar>,
    N: ::muon::helper::Unsigned,
    ::muon::observe::DefaultObserver<'ob, i32>: ::muon::observe::Flush<Sk>,
{
    #[inline(always)]
    fn flush(this: &mut Self, sink: &mut Sk) {
        sink.push_index(0usize);
        ::muon::observe::Flush::flush(&mut this.0, sink);
        sink.pop_segment();
    }
}
#[rustfmt::skip]
#[automatically_derived]
impl ::muon::Observe for Bar {
    type Observer<'ob, S, N> = BarObserver<'ob, S, N>
    where
        Self: 'ob,
        N: ::muon::helper::Unsigned,
        S: ::muon::helper::AsDerefMut<N, Target = Self> + ?Sized + 'ob;
    type Spec = ::muon::observe::DefaultSpec;
}
#[rustfmt::skip]
#[derive(PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Baz(i32, String);
#[rustfmt::skip]
#[allow(clippy::extra_unused_lifetimes)]
const _: () = {
    pub struct BazObserver<'ob, S: ?Sized, N = ::muon::helper::Zero>(
        ::muon::observe::DefaultObserver<'ob, i32>,
        ::muon::observe::DefaultObserver<'ob, String>,
        ::muon::helper::Pointer<S>,
        ::std::marker::PhantomData<&'ob mut N>,
    );
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::std::ops::Deref for BazObserver<'ob, S, N> {
        type Target = ::muon::helper::Pointer<S>;
        fn deref(&self) -> &Self::Target {
            &self.2
        }
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::std::ops::DerefMut for BazObserver<'ob, S, N> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            ::std::ptr::from_mut(self).expose_provenance();
            ::muon::helper::QuasiObserver::invalidate(&mut self.2);
            ::muon::helper::QuasiObserver::invalidate(&mut self.0);
            ::muon::helper::QuasiObserver::invalidate(&mut self.1);
            &mut self.2
        }
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::muon::helper::QuasiObserver for BazObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDeref<N>,
        N: ::muon::helper::Unsigned,
    {
        type Head = S;
        type OuterDepth = ::muon::helper::Succ<::muon::helper::Zero>;
        type InnerDepth = N;
        fn invalidate(this: &mut Self) {
            ::muon::helper::QuasiObserver::invalidate(&mut this.0);
            ::muon::helper::QuasiObserver::invalidate(&mut this.1);
        }
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::muon::observe::Observer for BazObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDerefMut<N, Target = Baz>,
        N: ::muon::helper::Unsigned,
    {
        #[inline(always)]
        unsafe fn observe(head: *mut S) -> Self {
            unsafe {
                let __value = ::muon::helper::AsDeref::<N>::as_deref_ptr(head);
                let observer_0 = ::muon::observe::Observer::observe(
                    &raw mut (*__value).0,
                );
                let observer_1 = ::muon::observe::Observer::observe(
                    &raw mut (*__value).1,
                );
                Self(
                    observer_0,
                    observer_1,
                    ::muon::helper::Pointer::new_unchecked(head),
                    ::std::marker::PhantomData,
                )
            }
        }
        unsafe fn relocate(this: &mut Self, head: *mut S) {
            unsafe {
                let __value = ::muon::helper::AsDeref::<N>::as_deref_ptr(head);
                ::muon::observe::Observer::relocate(&mut this.0, &raw mut (*__value).0);
                ::muon::observe::Observer::relocate(&mut this.1, &raw mut (*__value).1);
                ::muon::helper::Pointer::set_unchecked(this, head);
            }
        }
    }
    #[automatically_derived]
    impl<
        'ob,
        S: ?Sized,
        N,
        Sk: ::muon::observe::Sink + ?Sized,
    > ::muon::observe::QuasiSink<Sk> for BazObserver<'ob, S, N> {
        type Operation = Sk::Operation;
        type Identity = Sk::Identity;
    }
    #[automatically_derived]
    impl<
        'ob,
        S: ?Sized,
        N,
        Sk: ::muon::observe::Sink + ?Sized,
        Elem: ?Sized,
    > ::muon::observe::FlushWith<Sk, Elem> for BazObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDerefMut<N, Target = Baz>,
        N: ::muon::helper::Unsigned,
        ::muon::observe::DefaultObserver<'ob, i32>: ::muon::observe::Flush<Sk>,
        ::muon::observe::DefaultObserver<'ob, String>: ::muon::observe::Flush<Sk>,
    {
        fn flush_with<F>(this: &mut Self, sink: &mut Sk, _flush_elem: F)
        where
            F: FnMut(&mut Elem, &mut Sk),
        {
            <Self as ::muon::observe::Flush<Sk>>::flush(this, sink)
        }
    }
    #[automatically_derived]
    impl<
        'ob,
        S: ?Sized,
        N,
        Sk: ::muon::observe::Sink + ?Sized,
    > ::muon::observe::Flush<Sk> for BazObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDerefMut<N, Target = Baz>,
        N: ::muon::helper::Unsigned,
        ::muon::observe::DefaultObserver<'ob, i32>: ::muon::observe::Flush<Sk>,
        ::muon::observe::DefaultObserver<'ob, String>: ::muon::observe::Flush<Sk>,
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
    impl ::muon::Observe for Baz {
        type Observer<'ob, S, N> = BazObserver<'ob, S, N>
        where
            Self: 'ob,
            N: ::muon::helper::Unsigned,
            S: ::muon::helper::AsDerefMut<N, Target = Self> + ?Sized + 'ob;
        type Spec = ::muon::observe::DefaultSpec;
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::std::fmt::Debug for BazObserver<'ob, S, N>
    where
        ::muon::observe::DefaultObserver<'ob, i32>: ::std::fmt::Debug,
        ::muon::observe::DefaultObserver<'ob, String>: ::std::fmt::Debug,
    {
        fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
            f.debug_tuple("BazObserver").field(&self.0).field(&self.1).finish()
        }
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::std::cmp::PartialEq for BazObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDerefMut<N, Target = Baz>,
        N: ::muon::helper::Unsigned,
    {
        fn eq(&self, other: &Self) -> bool {
            let lhs = ::muon::helper::QuasiObserver::untracked_ref(self);
            let rhs = ::muon::helper::QuasiObserver::untracked_ref(other);
            lhs.eq(rhs)
        }
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::std::cmp::Eq for BazObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDerefMut<N, Target = Baz>,
        N: ::muon::helper::Unsigned,
    {}
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::std::cmp::PartialOrd for BazObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDerefMut<N, Target = Baz>,
        N: ::muon::helper::Unsigned,
    {
        fn partial_cmp(
            &self,
            other: &Self,
        ) -> ::std::option::Option<::std::cmp::Ordering> {
            let lhs = ::muon::helper::QuasiObserver::untracked_ref(self);
            let rhs = ::muon::helper::QuasiObserver::untracked_ref(other);
            lhs.partial_cmp(rhs)
        }
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::std::cmp::Ord for BazObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDerefMut<N, Target = Baz>,
        N: ::muon::helper::Unsigned,
    {
        fn cmp(&self, other: &Self) -> ::std::cmp::Ordering {
            let lhs = ::muon::helper::QuasiObserver::untracked_ref(self);
            let rhs = ::muon::helper::QuasiObserver::untracked_ref(other);
            lhs.cmp(rhs)
        }
    }
};
