mod r#enum;
mod meta;
mod selection;
mod r#struct;

use darling::FromDeriveInput;
use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::Ident;
use quote::{ToTokens, quote};
use std::collections::HashSet;
use syn::{GenericParam, Generics, Path};

pub(crate) fn expand(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    let expanded = match meta::Input::from_derive_input(&input) {
        Ok(input) => input.expand(),
        Err(error) => return error.write_errors().into(),
    };
    let runtime = match crate_name("kernel") {
        Ok(FoundCrate::Itself) => quote! { crate },
        Ok(FoundCrate::Name(name)) => {
            let name = Ident::new(&name, proc_macro2::Span::call_site());
            quote! { ::#name }
        }
        Err(error) => {
            return syn::Error::new(proc_macro2::Span::call_site(), error)
                .to_compile_error()
                .into();
        }
    };

    quote! {
        const _: () = {
            use #runtime as __kernel_runtime;
            #expanded
        };
    }
    .into()
}

fn type_ident(generics: &syn::Generics, preferred: &str) -> Ident {
    let used = generics
        .type_params()
        .map(|parameter| parameter.ident.to_string())
        .chain(
            generics
                .const_params()
                .map(|parameter| parameter.ident.to_string()),
        )
        .collect::<HashSet<_>>();
    fresh_ident(&used, preferred)
}

fn lifetime(generics: &syn::Generics, preferred: &str) -> syn::Lifetime {
    let used = generics
        .lifetimes()
        .map(|parameter| parameter.lifetime.ident.to_string())
        .collect::<HashSet<_>>();
    let ident = fresh_ident(&used, preferred);
    syn::Lifetime::new(&format!("'{ident}"), ident.span())
}

fn fresh_ident(used: &HashSet<String>, preferred: &str) -> Ident {
    for suffix in 0.. {
        let candidate = if suffix == 0 {
            preferred.to_owned()
        } else {
            format!("{preferred}{suffix}")
        };
        if !used.contains(&candidate) {
            return Ident::new(&candidate, proc_macro2::Span::call_site());
        }
    }
    unreachable!()
}

fn generic_arguments(generics: &Generics) -> Vec<proc_macro2::TokenStream> {
    generics
        .params
        .iter()
        .map(|parameter| match parameter {
            GenericParam::Lifetime(parameter) => parameter.lifetime.to_token_stream(),
            GenericParam::Type(parameter) => parameter.ident.to_token_stream(),
            GenericParam::Const(parameter) => parameter.ident.to_token_stream(),
        })
        .collect()
}

fn without_defaults(mut generics: Generics) -> Generics {
    for parameter in &mut generics.params {
        match parameter {
            GenericParam::Type(parameter) => parameter.default = None,
            GenericParam::Const(parameter) => parameter.default = None,
            GenericParam::Lifetime(_) => {}
        }
    }
    generics
}

fn same_path(left: &Path, right: &Path) -> bool {
    quote!(#left).to_string() == quote!(#right).to_string()
}
