#[allow(unused_imports)]
use muon::Observe;
use serde::Serialize;
#[rustfmt::skip]
#[derive(Serialize)]
pub struct Rec {
    pub label: String,
    pub children: Vec<Rec>,
}
#[rustfmt::skip]
#[allow(clippy::extra_unused_lifetimes)]
const _: () = {
    pub struct RecObserver<'ob, S: ?Sized, N = ::muon::helper::Zero> {
        pub label: ::muon::observe::DefaultObserver<'ob, String>,
        pub children: ::muon::observe::DefaultObserver<'ob, Vec<Rec>>,
        __ptr: ::muon::helper::Pointer<S>,
        __phantom: ::std::marker::PhantomData<&'ob mut N>,
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::std::ops::Deref for RecObserver<'ob, S, N> {
        type Target = ::muon::helper::Pointer<S>;
        fn deref(&self) -> &Self::Target {
            &self.__ptr
        }
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::std::ops::DerefMut for RecObserver<'ob, S, N> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            ::std::ptr::from_mut(self).expose_provenance();
            ::muon::helper::QuasiObserver::invalidate(&mut self.__ptr);
            ::muon::helper::QuasiObserver::invalidate(&mut self.label);
            ::muon::helper::QuasiObserver::invalidate(&mut self.children);
            &mut self.__ptr
        }
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::muon::helper::QuasiObserver for RecObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDeref<N>,
        N: ::muon::helper::Unsigned,
    {
        type Head = S;
        type OuterDepth = ::muon::helper::Succ<::muon::helper::Zero>;
        type InnerDepth = N;
        fn invalidate(this: &mut Self) {
            ::muon::helper::QuasiObserver::invalidate(&mut this.label);
            ::muon::helper::QuasiObserver::invalidate(&mut this.children);
        }
    }
    #[automatically_derived]
    impl<'ob, S: ?Sized, N> ::muon::observe::Observer for RecObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDerefMut<N, Target = Rec>,
        N: ::muon::helper::Unsigned,
    {
        #[inline(always)]
        unsafe fn observe(head: *mut S) -> Self {
            unsafe {
                let __value = ::muon::helper::AsDeref::<N>::as_deref_ptr(head);
                let label = ::muon::observe::Observer::observe(
                    &raw mut (*__value).label,
                );
                let children = ::muon::observe::Observer::observe(
                    &raw mut (*__value).children,
                );
                Self {
                    label,
                    children,
                    __ptr: ::muon::helper::Pointer::new_unchecked(head),
                    __phantom: ::std::marker::PhantomData,
                }
            }
        }
        unsafe fn relocate(this: &mut Self, head: *mut S) {
            unsafe {
                let __value = ::muon::helper::AsDeref::<N>::as_deref_ptr(head);
                ::muon::observe::Observer::relocate(
                    &mut this.label,
                    &raw mut (*__value).label,
                );
                ::muon::observe::Observer::relocate(
                    &mut this.children,
                    &raw mut (*__value).children,
                );
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
    > ::muon::observe::QuasiSink<Sk> for RecObserver<'ob, S, N> {
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
    > ::muon::observe::FlushWith<Sk, Elem> for RecObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDerefMut<N, Target = Rec>,
        N: ::muon::helper::Unsigned,
        ::muon::observe::DefaultObserver<'ob, String>: ::muon::observe::Flush<Sk>,
        <::muon::observe::DefaultObserver<
            'ob,
            Vec<Rec>,
        > as ::muon::observe::QuasiSink<
            Sk,
        >>::Operation: ::std::convert::Into<Sk::Operation>,
        <::muon::observe::DefaultObserver<
            'ob,
            Vec<Rec>,
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
    > ::muon::observe::Flush<Sk> for RecObserver<'ob, S, N>
    where
        S: ::muon::helper::AsDerefMut<N, Target = Rec>,
        N: ::muon::helper::Unsigned,
        ::muon::observe::DefaultObserver<'ob, String>: ::muon::observe::Flush<Sk>,
        <::muon::observe::DefaultObserver<
            'ob,
            Vec<Rec>,
        > as ::muon::observe::QuasiSink<
            Sk,
        >>::Operation: ::std::convert::Into<Sk::Operation>,
        <::muon::observe::DefaultObserver<
            'ob,
            Vec<Rec>,
        > as ::muon::observe::QuasiSink<
            Sk,
        >>::Identity: ::std::convert::Into<Sk::Identity>,
    {
        #[inline(always)]
        fn flush(this: &mut Self, sink: &mut Sk) {
            sink.push_field("label");
            ::muon::observe::Flush::flush(&mut this.label, sink);
            sink.pop_segment();
            sink.push_field("children");
            <::muon::observe::DefaultObserver<
                'ob,
                Vec<Rec>,
            > as ::muon::observe::FlushWith<
                Sk,
                ::muon::observe::DefaultObserver<'ob, Rec>,
            >>::flush_with(
                &mut this.children,
                sink,
                |e, s| <::muon::observe::DefaultObserver<
                    'ob,
                    Rec,
                > as ::muon::observe::Flush<Sk>>::flush(e, s),
            );
            sink.pop_segment();
        }
    }
    #[automatically_derived]
    impl ::muon::Observe for Rec {
        type Observer<'ob, S, N> = RecObserver<'ob, S, N>
        where
            Self: 'ob,
            N: ::muon::helper::Unsigned,
            S: ::muon::helper::AsDerefMut<N, Target = Self> + ?Sized + 'ob;
        type Spec = ::muon::observe::DefaultSpec;
    }
};
impl ::muon::general::Snapshot for Rec {
    type Snapshot = serde_json::Value;
    fn to_snapshot(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("Rec serializes")
    }
}
impl ::muon::general::SerializeSnapshot for Rec {
    fn flush<S: ::muon::observe::Sink + ?Sized>(&self, snapshot: serde_json::Value, sink: &mut S) {
        let current = serde_json::to_value(self).expect("Rec serializes");
        if current != snapshot {
            sink.replace(
                Some(&snapshot as &dyn ::muon::erased_serde::Serialize),
                Some(&current as &dyn ::muon::erased_serde::Serialize),
            );
        }
    }
}
