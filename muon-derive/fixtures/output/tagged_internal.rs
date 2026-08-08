#[allow(unused_imports)]
use muon::Observe;
use serde::Serialize;
#[rustfmt::skip]
#[derive(Serialize)]
#[serde(tag = "type")]
pub enum Foo<const N: usize> {
    #[serde(serialize_with = "<[_]>::serialize")]
    A([u32; N]),
    C {
        #[serde(skip_serializing_if = "String::is_empty")]
        bar: String,
        #[serde(flatten)]
        qux: Qux,
    },
}
#[rustfmt::skip]
const _: () = {
    pub struct FooObserver<'ob, const N: usize, S: ?Sized, _N = ::muon::helper::Zero> {
        ptr: ::muon::helper::Pointer<S>,
        mutated: bool,
        variant: FooObserverVariant<'ob, N>,
        phantom: ::std::marker::PhantomData<&'ob mut _N>,
    }
    pub enum FooObserverVariant<'ob, const N: usize> {
        A(::muon::observe::DefaultObserver<'ob, [u32; N]>),
        C {
            bar: ::muon::observe::DefaultObserver<'ob, String>,
            qux: ::muon::observe::DefaultObserver<'ob, Qux>,
        },
        __Unknown,
    }
    impl<'ob, const N: usize> FooObserverVariant<'ob, N> {
        unsafe fn observe(__ptr: *mut Foo<N>) -> Self {
            unsafe {
                match &*__ptr {
                    Foo::A(v0) => {
                        Self::A(
                            ::muon::observe::Observer::observe(
                                __ptr.with_addr(v0 as *const _ as usize).cast(),
                            ),
                        )
                    }
                    Foo::C { bar, qux } => {
                        Self::C {
                            bar: ::muon::observe::Observer::observe(
                                __ptr.with_addr(bar as *const _ as usize).cast(),
                            ),
                            qux: ::muon::observe::Observer::observe(
                                __ptr.with_addr(qux as *const _ as usize).cast(),
                            ),
                        }
                    }
                }
            }
        }
        unsafe fn relocate(&mut self, __ptr: *mut Foo<N>) {
            unsafe {
                match (self, &*__ptr) {
                    (Self::A(u0), Foo::A(v0)) => {
                        ::muon::observe::Observer::relocate(
                            u0,
                            __ptr.with_addr(v0 as *const _ as usize).cast(),
                        );
                    }
                    (Self::C { bar: u0, qux: u1 }, Foo::C { bar: v0, qux: v1 }) => {
                        ::muon::observe::Observer::relocate(
                            u0,
                            __ptr.with_addr(v0 as *const _ as usize).cast(),
                        );
                        ::muon::observe::Observer::relocate(
                            u1,
                            __ptr.with_addr(v1 as *const _ as usize).cast(),
                        );
                    }
                    (Self::__Unknown, _) => {}
                    _ => panic!("inconsistent state for FooObserver"),
                }
            }
        }
        fn flush<Sk: ::muon::observe::Sink + ?Sized>(
            &mut self,
            __ptr: *const Foo<N>,
            sink: &mut Sk,
        )
        where
            Foo<N>: ::muon::helper::serde::Serialize + 'static,
            ::muon::observe::DefaultObserver<'ob, [u32; N]>: ::muon::observe::Flush<Sk>,
            ::muon::observe::DefaultObserver<'ob, String>: ::muon::observe::Flush<Sk>,
            ::muon::observe::DefaultObserver<'ob, Qux>: ::muon::observe::Flush<Sk>,
        {
            match self {
                Self::A(u0) => {
                    ::muon::observe::Flush::flush(u0, sink);
                }
                Self::C { bar, qux } => {
                    sink.push_field("bar");
                    ::muon::observe::Flush::flush(bar, sink);
                    sink.pop_segment();
                    ::muon::observe::Flush::flush(qux, sink);
                }
                Self::__Unknown => {}
            }
        }
    }
    #[automatically_derived]
    impl<'ob, const N: usize, S: ?Sized, _N> ::std::ops::Deref
    for FooObserver<'ob, N, S, _N> {
        type Target = ::muon::helper::Pointer<S>;
        fn deref(&self) -> &Self::Target {
            &self.ptr
        }
    }
    #[automatically_derived]
    impl<'ob, const N: usize, S: ?Sized, _N> ::std::ops::DerefMut
    for FooObserver<'ob, N, S, _N> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.mutated = true;
            self.variant = FooObserverVariant::__Unknown;
            &mut self.ptr
        }
    }
    #[automatically_derived]
    impl<'ob, const N: usize, S: ?Sized, _N> ::muon::helper::QuasiObserver
    for FooObserver<'ob, N, S, _N>
    where
        S: ::muon::helper::AsDeref<_N>,
        _N: ::muon::helper::Unsigned,
    {
        type Head = S;
        type OuterDepth = ::muon::helper::Succ<::muon::helper::Zero>;
        type InnerDepth = _N;
        fn invalidate(this: &mut Self) {
            this.mutated = true;
            this.variant = FooObserverVariant::__Unknown;
        }
    }
    #[automatically_derived]
    impl<'ob, const N: usize, S: ?Sized, _N> ::muon::observe::Observer
    for FooObserver<'ob, N, S, _N>
    where
        S: ::muon::helper::AsDeref<_N, Target = Foo<N>>,
        _N: ::muon::helper::Unsigned,
    {
        unsafe fn observe(head: *mut S) -> Self {
            unsafe {
                let __ptr = ::muon::helper::AsDerefPtrExt::as_deref_ptr::<_N>(head);
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
                ::muon::helper::AsDerefPtrExt::as_deref_ptr::<_N>(head)
            };
            unsafe { this.variant.relocate(__ptr) }
            unsafe { ::muon::helper::Pointer::set_unchecked(this, head) };
        }
    }
    #[automatically_derived]
    impl<
        'ob,
        const N: usize,
        S: ?Sized,
        _N,
        Sk: ::muon::observe::Sink + ?Sized,
    > ::muon::observe::QuasiSink<Sk> for FooObserver<'ob, N, S, _N> {
        type Operation = Sk::Operation;
        type Identity = Sk::Identity;
    }
    #[automatically_derived]
    impl<
        'ob,
        const N: usize,
        S: ?Sized,
        _N,
        Sk: ::muon::observe::Sink + ?Sized,
        Elem: ?Sized,
    > ::muon::observe::FlushWith<Sk, Elem> for FooObserver<'ob, N, S, _N>
    where
        Foo<N>: ::muon::helper::serde::Serialize + 'static,
        S: ::muon::helper::AsDeref<_N, Target = Foo<N>>,
        _N: ::muon::helper::Unsigned,
        ::muon::observe::DefaultObserver<'ob, [u32; N]>: ::muon::observe::Flush<Sk>,
        ::muon::observe::DefaultObserver<'ob, String>: ::muon::observe::Flush<Sk>,
        ::muon::observe::DefaultObserver<'ob, Qux>: ::muon::observe::Flush<Sk>,
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
        const N: usize,
        S: ?Sized,
        _N,
        Sk: ::muon::observe::Sink + ?Sized,
    > ::muon::observe::Flush<Sk> for FooObserver<'ob, N, S, _N>
    where
        Foo<N>: ::muon::helper::serde::Serialize + 'static,
        S: ::muon::helper::AsDeref<_N, Target = Foo<N>>,
        _N: ::muon::helper::Unsigned,
        ::muon::observe::DefaultObserver<'ob, [u32; N]>: ::muon::observe::Flush<Sk>,
        ::muon::observe::DefaultObserver<'ob, String>: ::muon::observe::Flush<Sk>,
        ::muon::observe::DefaultObserver<'ob, Qux>: ::muon::observe::Flush<Sk>,
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
    impl<const N: usize> ::muon::Observe for Foo<N>
    where
        Self: ::muon::helper::serde::Serialize,
    {
        type Observer<'ob, S, _N> = FooObserver<'ob, N, S, _N>
        where
            Self: 'ob,
            _N: ::muon::helper::Unsigned,
            S: ::muon::helper::AsDerefMut<_N, Target = Self> + ?Sized + 'ob;
        type Spec = ::muon::observe::DefaultSpec;
    }
};
#[rustfmt::skip]
#[derive(Serialize)]
pub struct Qux {}
#[rustfmt::skip]
#[automatically_derived]
impl ::muon::general::Snapshot for Qux {
    type Snapshot = ();
    fn to_snapshot(&self) {}
}
#[rustfmt::skip]
#[automatically_derived]
impl ::muon::general::SerializeSnapshot for Qux {
    fn flush<S: ::muon::observe::Sink + ?Sized>(&self, _snapshot: (), _sink: &mut S) {
        {}
    }
}
#[rustfmt::skip]
#[automatically_derived]
impl ::muon::Observe for Qux {
    type Observer<'ob, S, N> = ::muon::general::NoopObserver<'ob, Self, S, N>
    where
        Self: 'ob,
        N: ::muon::helper::Unsigned,
        S: ::muon::helper::AsDerefMut<N, Target = Self> + ?Sized + 'ob;
    type Spec = ::muon::observe::SnapshotSpec;
}
