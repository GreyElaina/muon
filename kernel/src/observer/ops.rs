//! Mutation operators forwarded through the observer invalidation boundary.

use super::{AsDerefMut, Invalidate, QuasiObserver, StatefulObserver, Unsigned};

macro_rules! impl_assign {
    ($($trait:ident::$method:ident),* $(,)?) => {
        $(
            impl<St, Head: ?Sized, Depth, Rhs> core::ops::$trait<Rhs>
                for StatefulObserver<St, Head, Depth>
            where
                Depth: Unsigned,
                Head: AsDerefMut<Depth>,
                <Head as super::AsDeref<Depth>>::Target: core::ops::$trait<Rhs>,
                St: Invalidate<<Head as super::AsDeref<Depth>>::Target>,
            {
                fn $method(&mut self, rhs: Rhs) {
                    QuasiObserver::tracked_mut(self).$method(rhs);
                }
            }
        )*
    };
}

impl_assign! {
    AddAssign::add_assign,
    SubAssign::sub_assign,
    MulAssign::mul_assign,
    DivAssign::div_assign,
    RemAssign::rem_assign,
    BitAndAssign::bitand_assign,
    BitOrAssign::bitor_assign,
    BitXorAssign::bitxor_assign,
    ShlAssign::shl_assign,
    ShrAssign::shr_assign,
}

impl<T: ?Sized, St, Head: ?Sized, Depth> AsMut<T> for StatefulObserver<St, Head, Depth>
where
    St: Invalidate<T>,
    Head: AsDerefMut<Depth, Target = T>,
    Depth: Unsigned,
{
    fn as_mut(&mut self) -> &mut T {
        QuasiObserver::tracked_mut(self)
    }
}
