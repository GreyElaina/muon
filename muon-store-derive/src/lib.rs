use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::parse_macro_input;
use syn::spanned::Spanned;
use syn::visit_mut::VisitMut;
use syn::{DeriveInput, Expr, ExprClosure, Token};

struct StoreInput {
    store: Expr,
    closure: ExprClosure,
}
impl Parse for StoreInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let store: Expr = input.parse()?;
        let _: Token![,] = input.parse()?;
        let closure: ExprClosure = input.parse()?;
        Ok(Self { store, closure })
    }
}

/// Rewrite assignments/comparisons to use tracked_mut/untracked_ref.
///
/// Visits children before rewriting the current node, so nested
/// expressions are fully rewritten (matching muon's own observer macro).
struct TransformQuasiObserver;

impl VisitMut for TransformQuasiObserver {
    fn visit_expr_assign_mut(&mut self, expr_assign: &mut syn::ExprAssign) {
        syn::visit_mut::visit_expr_assign_mut(self, expr_assign);
        let left = &expr_assign.left;
        let span = left.span();
        expr_assign.left = syn::parse_quote_spanned! { span =>
            *(&mut #left).tracked_mut()
        };
    }

    fn visit_expr_binary_mut(&mut self, expr_binary: &mut syn::ExprBinary) {
        syn::visit_mut::visit_expr_binary_mut(self, expr_binary);
        match &expr_binary.op {
            syn::BinOp::Eq(_)
            | syn::BinOp::Ne(_)
            | syn::BinOp::Le(_)
            | syn::BinOp::Lt(_)
            | syn::BinOp::Ge(_)
            | syn::BinOp::Gt(_) => {
                let left = &expr_binary.left;
                let span = left.span();
                expr_binary.left = syn::parse_quote_spanned! { span =>
                    *(&#left).untracked_ref()
                };
                let right = &expr_binary.right;
                let span = right.span();
                expr_binary.right = syn::parse_quote_spanned! { span =>
                    *(&#right).untracked_ref()
                };
            }
            _ => {}
        }
    }
}

fn rewrite_body(body: &mut syn::Expr) {
    TransformQuasiObserver.visit_expr_mut(body);
}

/// The mid-level write macro: rewrite the mutation body to use muon's
/// observer API and construct a `muon_store::Write` intent.
///
/// The write is lazy: no lock is taken until `Write::observe` or
/// `Write::commit` is called.
///
/// ```ignore
/// track!(store, |s| s.title = "A".into()).commit();
/// track!(store, |s| s.x = 1).observe();
/// ```
#[proc_macro]
pub fn track(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as StoreInput);
    if input.closure.inputs.len() != 1 {
        return syn::Error::new_spanned(&input.closure, "track! expects one parameter")
            .to_compile_error()
            .into();
    }
    // Keep the full closure (move, return type, attributes, parameter
    // pattern) and only replace its body: lazy writes make capture
    // semantics observable.
    let mut closure = input.closure;
    let mut body = closure.body.clone();
    rewrite_body(&mut body);
    closure.body = syn::parse_quote! {
        {
            #[allow(unused_imports)] use ::muon::helper::QuasiObserver;
            #body
        }
    };
    let store_expr = &input.store;
    let expanded = quote! {{
        let store = &#store_expr;
        store.write_tracked(#closure)
    }};
    expanded.into()
}

/// Marks a model type as writable through `track!`.
///
/// Emits a marker impl of `muon_store::Track` (supertraits: `muon::Observe
/// + Clone`), merging into any user-supplied `where` clause so generic
/// models work. Field accessors and reactive triggers come from
/// `muon-reactivity`'s `#[derive(Reactivity)]`.
#[proc_macro_derive(Track)]
pub fn derive_track(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (ig, ty, _) = input.generics.split_for_impl();
    let st = quote! { #name #ty };
    let mut where_clause = input
        .generics
        .where_clause
        .clone()
        .unwrap_or_else(|| syn::parse_quote!(where));
    where_clause
        .predicates
        .push(syn::parse_quote!(#st: ::muon::Observe + Clone + 'static));
    let expanded = quote! {
        impl #ig ::muon_store::Track for #st #where_clause {}
    };
    expanded.into()
}
