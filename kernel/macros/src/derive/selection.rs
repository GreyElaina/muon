use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::{Path, parse_quote};

use super::meta::Field;
use super::same_path;

pub(super) struct FieldSelection {
    pub(super) observer: TokenStream,
    pub(super) route: TokenStream,
    pub(super) param: Option<Ident>,
    pub(super) predicate: Option<syn::WherePredicate>,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn select_field(
    field: &Field,
    providers: &[Path],
    provider_tuple: &TokenStream,
    generated_param: Ident,
    head: TokenStream,
    depth: TokenStream,
) -> FieldSelection {
    let ty = &field.ty;
    if field.wrappers().is_empty() && (field.shallow() || field.noop()) {
        let observer = if field.noop() {
            quote! { __kernel_runtime::NoopObserver<#ty, #head, #depth> }
        } else {
            quote! { __kernel_runtime::ShallowObserver<#ty, #head, #depth> }
        };
        let observer = if field.scope_parent() {
            quote! { __kernel_runtime::Selected<#observer, (), __kernel_runtime::Parent> }
        } else {
            observer
        };
        return FieldSelection {
            observer,
            route: quote! { () },
            param: None,
            predicate: None,
        };
    }

    let (mut inner, param) = if field.noop() {
        (quote! { (__kernel_runtime::select::Noop, ()) }, None)
    } else if field.shallow() {
        (quote! { (__kernel_runtime::select::Shallow, ()) }, None)
    } else if let Some(provider) = field.selected_provider() {
        let slot = providers
            .iter()
            .position(|candidate| same_path(candidate, provider))
            .expect("validated provider")
            + 1;
        (
            quote! { (__kernel_runtime::Slot<#slot>, #generated_param) },
            Some(generated_param.clone()),
        )
    } else {
        (
            quote! { (__kernel_runtime::Slot<0>, #generated_param) },
            Some(generated_param.clone()),
        )
    };

    for wrapper in field.wrappers().iter().rev() {
        let slot = providers
            .iter()
            .position(|candidate| same_path(candidate, wrapper))
            .expect("validated provider")
            + 1;
        inner = quote! { (__kernel_runtime::Slot<#slot>, #inner) };
    }

    let set = quote! { __kernel_runtime::Select<#provider_tuple> };
    let scope = if field.scope_parent() {
        quote! { __kernel_runtime::Parent }
    } else {
        quote! { __kernel_runtime::Current }
    };
    let route = quote! { (#scope, #inner) };
    let observer = quote! {
        <#set as __kernel_runtime::Observe<#ty, #route>>::Observer<#head, #depth>
    };
    let predicate = parse_quote! { #set: __kernel_runtime::Observe<#ty, #route> };
    FieldSelection {
        observer,
        route,
        param,
        predicate: Some(predicate),
    }
}
