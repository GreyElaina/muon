use ::std::fmt::Display;
#[allow(unused_imports)]
use muon::Observe;
use serde::Serialize;
#[rustfmt::skip]
#[derive(Serialize)]
#[serde(untagged, rename_all_fields = "UPPERCASE")]
pub enum Foo {
    A(u32),
    B(u32, u32),
    C { bar: String },
    D,
    E(),
    F {},
}
#[rustfmt::skip]
const _: () = {
    #[::std::prelude::v1::derive()]
    pub struct FooObserver<'ob, S: ?Sized, N = ::muon::helper::Zero> {
        ptr: ::muon::helper::Pointer<S>,
        mutated: bool,
        initial: FooObserverInitial,
        variant: FooObserverVariant<'ob>,
        phantom: ::std::marker::PhantomData<&'ob mut N>,
    }
    #[derive(Clone, Copy)]
    #[allow(clippy::enum_variant_names)]
    pub enum FooObserverInitial {
        D,
        E,
        F,
        __Unknown,
    }
    impl FooObserverInitial {
        fn new(value: &Foo) -> Self {
            match value {
                Foo::D => FooObserverInitial::D,
                Foo::E() => FooObserverInitial::E,
                Foo::F {} => FooObserverInitial::F,
                _ => FooObserverInitial::__Unknown,
            }
        }
    }
    pub enum FooObserverVariant<'ob> {
        A(::muon::observe::DefaultObserver<'ob, u32>),
        B(
            ::muon::observe::DefaultObserver<'ob, u32>,
            ::muon::observe::DefaultObserver<'ob, u32>,
        ),
        C { bar: ::muon::observe::DefaultObserver<'ob, String> },
        __Unknown,
    }
    impl<'ob> FooObserverVariant<'ob> {
        unsafe fn observe(__ptr: *mut Foo) -> Self {
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
                    _ => Self::__Unknown,
                }
            }
        }
        unsafe fn relocate(&mut self, __ptr: *mut Foo) {
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
            __ptr: *const Foo,
            sink: &mut Sk,
        )
        where
            ::muon::observe::DefaultObserver<'ob, u32>: ::muon::observe::Flush<Sk>,
            ::muon::observe::DefaultObserver<'ob, u32>: ::muon::observe::Flush<Sk>,
            ::muon::observe::DefaultObserver<'ob, u32>: ::muon::observe::Flush<Sk>,
            ::muon::observe::DefaultObserver<'ob, String>: ::muon::observe::Flush<Sk>,
        {
            match self {
                Self::A(u0) => {
                    ::muon::observe::Flush::flush(u0, sink);
                }
                Self::B(u0, u1) => {
                    sink.push_index(0usize);
                    ::muon::observe::Flush::flush(u0, sink);
                    sink.pop_segment();
                    sink.push_index(1usize);
                    ::muon::observe::Flush::flush(u1, sink);
                    sink.pop_segment();
                }
                Self::C { bar } => {
                    sink.push_field("BAR");
                    ::muon::observe::Flush::flush(bar, sink);
                    sink.pop_segment();
                }
                Self::__Unknown => {}
            }
        }
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::std::ops::Deref for FooObserver<'ob, S, N> {
        type Target = ::muon::helper::Pointer<S>;
        fn deref(&self) -> &Self::Target {
            &self.ptr
        }
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::std::ops::DerefMut for FooObserver<'ob, S, N> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.mutated = true;
            self.variant = FooObserverVariant::__Unknown;
            &mut self.ptr
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
            this.mutated = true;
            this.variant = FooObserverVariant::__Unknown;
        }
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::muon::observe::Observer for FooObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDeref<N, Target = Foo>,
        N: ::muon::helper::Unsigned,
    {
        unsafe fn observe(head: *mut S) -> Self {
            unsafe {
                let __ptr = ::muon::helper::AsDerefPtrExt::as_deref_ptr::<N>(head);
                Self {
                    mutated: false,
                    initial: FooObserverInitial::new(&*__ptr),
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
        S: ::muon::helper::AsDeref<N, Target = Foo>,
        N: ::muon::helper::Unsigned,
        ::muon::observe::DefaultObserver<'ob, u32>: ::muon::observe::Flush<Sk>,
        ::muon::observe::DefaultObserver<'ob, u32>: ::muon::observe::Flush<Sk>,
        ::muon::observe::DefaultObserver<'ob, u32>: ::muon::observe::Flush<Sk>,
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
    > ::muon::observe::Flush<Sk> for FooObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDeref<N, Target = Foo>,
        N: ::muon::helper::Unsigned,
        ::muon::observe::DefaultObserver<'ob, u32>: ::muon::observe::Flush<Sk>,
        ::muon::observe::DefaultObserver<'ob, u32>: ::muon::observe::Flush<Sk>,
        ::muon::observe::DefaultObserver<'ob, u32>: ::muon::observe::Flush<Sk>,
        ::muon::observe::DefaultObserver<'ob, String>: ::muon::observe::Flush<Sk>,
    {
        fn flush(this: &mut Self, sink: &mut Sk) {
            let value = this.ptr.as_deref();
            let initial = this.initial;
            this.initial = FooObserverInitial::new(value);
            if !this.mutated {
                this.variant.flush(value, sink);
                return;
            }
            this.mutated = false;
            this.variant = FooObserverVariant::__Unknown;
            match (initial, value) {
                (FooObserverInitial::D, Foo::D)
                | (FooObserverInitial::E, Foo::E())
                | (FooObserverInitial::F, Foo::F {}) => {}
                _ => {
                    sink.replace(
                        None,
                        Some(value as &dyn ::muon::erased_serde::Serialize),
                    )
                }
            }
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
    impl<'ob, S: ?Sized, N> ::std::fmt::Display for FooObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDeref<N, Target = Foo>,
        N: ::muon::helper::Unsigned,
    {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            ::std::fmt::Display::fmt(self.as_deref(), f)
        }
    }
};
impl Display for Foo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Foo::A(a) => write!(f, "Foo::A({})", a),
            Foo::B(a, b) => write!(f, "Foo::B({}, {})", a, b),
            Foo::C { bar } => write!(f, "Foo::C {{ bar: {} }}", bar),
            Foo::D => write!(f, "Foo::D"),
            Foo::E() => write!(f, "Foo::E()"),
            Foo::F {} => write!(f, "Foo::F {{}}"),
        }
    }
}
