//! Syntax transforms used by `kernel`.

mod derive;
mod tracked;

use proc_macro::TokenStream;

/// Derives structural mutation observation for a model.
#[proc_macro_derive(Observe, attributes(observe, select, scope, shallow, noop))]
pub fn derive_observe(input: TokenStream) -> TokenStream {
    derive::expand(input)
}

/// Implementation detail of `kernel::tracked!`.
#[proc_macro]
pub fn __tracked(input: TokenStream) -> TokenStream {
    tracked::expand(input)
}
