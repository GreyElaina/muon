#[allow(unused_imports)]
use muon::Observe;
use serde::Serialize;
#[rustfmt::skip]
#[derive(Serialize)]
#[serde(tag = "type", content = "data")]
pub enum Foo<'i> {
    A(u32),
    B(u32, u32),
    C { bar: &'i mut String },
}
#[rustfmt::skip]
const _: () = {
    pub struct FooObserver<'ob, 'i, S: ?Sized, N = ::muon::helper::Zero>
    where
        &'i mut String: ::muon::Observe + 'ob,
    {
        ptr: ::muon::helper::Pointer<S>,
        mutated: bool,
        variant: FooObserverVariant<'ob, 'i>,
        phantom: ::std::marker::PhantomData<&'ob mut N>,
    }
    pub enum FooObserverVariant<'ob, 'i>
    where
        &'i mut String: ::muon::Observe + 'ob,
    {
        A(::muon::observe::DefaultObserver<'ob, u32>),
        B(
            ::muon::observe::DefaultObserver<'ob, u32>,
            ::muon::observe::DefaultObserver<'ob, u32>,
        ),
        C { bar: ::muon::observe::DefaultObserver<'ob, &'i mut String> },
        __Unknown,
    }
    impl<'ob, 'i> FooObserverVariant<'ob, 'i>
    where
        &'i mut String: ::muon::Observe,
    {
        unsafe fn observe(__ptr: *mut Foo<'i>) -> Self {
            unsafe {
                match &*__ptr {
                    Foo::A(v0) => {
                        Self::A(
                            ::muon::observe::Observer::observe(
                                __ptr.with_addr(v0 as *const _ as usize).cast(),
                            ),
                        )
                    }
                    Foo::B(v0, v1) => {
                        Self::B(
                            ::muon::observe::Observer::observe(
                                __ptr.with_addr(v0 as *const _ as usize).cast(),
                            ),
                            ::muon::observe::Observer::observe(
                                __ptr.with_addr(v1 as *const _ as usize).cast(),
                            ),
                        )
                    }
                    Foo::C { bar } => {
                        Self::C {
                            bar: ::muon::observe::Observer::observe(
                                __ptr.with_addr(bar as *const _ as usize).cast(),
                            ),
                        }
                    }
                }
            }
        }
        unsafe fn relocate(&mut self, __ptr: *mut Foo<'i>) {
            unsafe {
                match (self, &*__ptr) {
                    (Self::A(u0), Foo::A(v0)) => {
                        ::muon::observe::Observer::relocate(
                            u0,
                            __ptr.with_addr(v0 as *const _ as usize).cast(),
                        );
                    }
                    (Self::B(u0, u1), Foo::B(v0, v1)) => {
                        ::muon::observe::Observer::relocate(
                            u0,
                            __ptr.with_addr(v0 as *const _ as usize).cast(),
                        );
                        ::muon::observe::Observer::relocate(
                            u1,
                            __ptr.with_addr(v1 as *const _ as usize).cast(),
                        );
                    }
                    (Self::C { bar: u0 }, Foo::C { bar: v0 }) => {
                        ::muon::observe::Observer::relocate(
                            u0,
                            __ptr.with_addr(v0 as *const _ as usize).cast(),
                        );
                    }
                    (Self::__Unknown, _) => {}
                    _ => panic!("inconsistent state for FooObserver"),
                }
            }
        }
        fn flush<Sk: ::muon::observe::Sink + ?Sized>(
            &mut self,
            __ptr: *const Foo<'i>,
            sink: &mut Sk,
        )
        where
            Foo<'i>: ::muon::helper::serde::Serialize + 'static,
            ::muon::observe::DefaultObserver<'ob, u32>: ::muon::observe::Flush<Sk>,
            ::muon::observe::DefaultObserver<'ob, u32>: ::muon::observe::Flush<Sk>,
            ::muon::observe::DefaultObserver<'ob, u32>: ::muon::observe::Flush<Sk>,
            ::muon::observe::DefaultObserver<
                'ob,
                &'i mut String,
            >: ::muon::observe::Flush<Sk>,
        {
            match self {
                Self::A(u0) => {
                    sink.push_field("data");
                    ::muon::observe::Flush::flush(u0, sink);
                    sink.pop_segment();
                }
                Self::B(u0, u1) => {
                    sink.push_field("data");
                    sink.push_index(0usize);
                    ::muon::observe::Flush::flush(u0, sink);
                    sink.pop_segment();
                    sink.push_index(1usize);
                    ::muon::observe::Flush::flush(u1, sink);
                    sink.pop_segment();
                    sink.pop_segment();
                }
                Self::C { bar } => {
                    sink.push_field("data");
                    sink.push_field("bar");
                    ::muon::observe::Flush::flush(bar, sink);
                    sink.pop_segment();
                    sink.pop_segment();
                }
                Self::__Unknown => {}
            }
        }
    }
    #[automatically_derived]
    impl<'ob, 'i, S: ?Sized, N> ::std::ops::Deref for FooObserver<'ob, 'i, S, N>
    where
        &'i mut String: ::muon::Observe,
    {
        type Target = ::muon::helper::Pointer<S>;
        fn deref(&self) -> &Self::Target {
            &self.ptr
        }
    }
    #[automatically_derived]
    impl<'ob, 'i, S: ?Sized, N> ::std::ops::DerefMut for FooObserver<'ob, 'i, S, N>
    where
        &'i mut String: ::muon::Observe,
    {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.mutated = true;
            self.variant = FooObserverVariant::__Unknown;
            &mut self.ptr
        }
    }
    #[automatically_derived]
    impl<'ob, 'i, S: ?Sized, N> ::muon::helper::QuasiObserver
    for FooObserver<'ob, 'i, S, N>
    where
        &'i mut String: ::muon::Observe,
        S: ::muon::helper::AsDeref<N>,
        N: ::muon::helper::Unsigned,
    {
        type Head = S;
        type OuterDepth = ::muon::helper::Succ<::muon::helper::Zero>;
        type InnerDepth = N;
        fn invalidate(this: &mut Self) {
            this.mutated = true;
            this.variant = FooObserverVariant::__Unknown;
        }
    }
    #[automatically_derived]
    impl<'ob, 'i, S: ?Sized, N> ::muon::observe::Observer for FooObserver<'ob, 'i, S, N>
    where
        &'i mut String: ::muon::Observe,
        S: ::muon::helper::AsDeref<N, Target = Foo<'i>>,
        N: ::muon::helper::Unsigned,
    {
        unsafe fn observe(head: *mut S) -> Self {
            unsafe {
                let __ptr = ::muon::helper::AsDerefPtrExt::as_deref_ptr::<N>(head);
                Self {
                    mutated: false,
                    variant: FooObserverVariant::observe(__ptr),
                    ptr: ::muon::helper::Pointer::new_unchecked(head),
                    phantom: ::std::marker::PhantomData,
                }
            }
        }
        unsafe fn relocate(this: &mut Self, head: *mut S) {
            let __ptr = unsafe {
                ::muon::helper::AsDerefPtrExt::as_deref_ptr::<N>(head)
            };
            unsafe { this.variant.relocate(__ptr) }
            unsafe { ::muon::helper::Pointer::set_unchecked(this, head) };
        }
    }
    #[automatically_derived]
    impl<
        'ob,
        'i,
        S: ?Sized,
        N,
        Sk: ::muon::observe::Sink + ?Sized,
    > ::muon::observe::QuasiSink<Sk> for FooObserver<'ob, 'i, S, N>
    where
        &'i mut String: ::muon::Observe + 'ob,
    {
        type Operation = Sk::Operation;
        type Identity = Sk::Identity;
    }
    #[automatically_derived]
    impl<
        'ob,
        'i,
        S: ?Sized,
        N,
        Sk: ::muon::observe::Sink + ?Sized,
        Elem: ?Sized,
    > ::muon::observe::FlushWith<Sk, Elem> for FooObserver<'ob, 'i, S, N>
    where
        Foo<'i>: ::muon::helper::serde::Serialize + 'static,
        &'i mut String: ::muon::Observe + 'ob,
        S: ::muon::helper::AsDeref<N, Target = Foo<'i>>,
        N: ::muon::helper::Unsigned,
        ::muon::observe::DefaultObserver<'ob, u32>: ::muon::observe::Flush<Sk>,
        ::muon::observe::DefaultObserver<'ob, u32>: ::muon::observe::Flush<Sk>,
        ::muon::observe::DefaultObserver<'ob, u32>: ::muon::observe::Flush<Sk>,
        ::muon::observe::DefaultObserver<
            'ob,
            &'i mut String,
        >: ::muon::observe::Flush<Sk>,
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
        'i,
        S: ?Sized,
        N,
        Sk: ::muon::observe::Sink + ?Sized,
    > ::muon::observe::Flush<Sk> for FooObserver<'ob, 'i, S, N>
    where
        Foo<'i>: ::muon::helper::serde::Serialize + 'static,
        &'i mut String: ::muon::Observe + 'ob,
        S: ::muon::helper::AsDeref<N, Target = Foo<'i>>,
        N: ::muon::helper::Unsigned,
        ::muon::observe::DefaultObserver<'ob, u32>: ::muon::observe::Flush<Sk>,
        ::muon::observe::DefaultObserver<'ob, u32>: ::muon::observe::Flush<Sk>,
        ::muon::observe::DefaultObserver<'ob, u32>: ::muon::observe::Flush<Sk>,
        ::muon::observe::DefaultObserver<
            'ob,
            &'i mut String,
        >: ::muon::observe::Flush<Sk>,
    {
        fn flush(this: &mut Self, sink: &mut Sk) {
            let value = this.ptr.as_deref();
            if !this.mutated {
                this.variant.flush(value, sink);
                return;
            }
            this.mutated = false;
            this.variant = FooObserverVariant::__Unknown;
            sink.replace(
                None,
                Some(this.as_deref() as &dyn ::muon::erased_serde::Serialize),
            )
        }
    }
    #[automatically_derived]
    impl<'i> ::muon::Observe for Foo<'i>
    where
        Self: ::muon::helper::serde::Serialize,
        &'i mut String: ::muon::Observe,
    {
        type Observer<'ob, S, N> = FooObserver<'ob, 'i, S, N>
        where
            Self: 'ob,
            &'i mut String: 'ob,
            N: ::muon::helper::Unsigned,
            S: ::muon::helper::AsDerefMut<N, Target = Self> + ?Sized + 'ob;
        type Spec = ::muon::observe::DefaultSpec;
    }
};
