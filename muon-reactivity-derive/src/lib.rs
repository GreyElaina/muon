use heck::{
    ToKebabCase, ToLowerCamelCase, ToShoutyKebabCase, ToShoutySnakeCase, ToSnakeCase,
    ToUpperCamelCase,
};
use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::parse_macro_input;
use syn::{Data, DeriveInput, Fields};

/// The serde `rename_all` rule applied to a container.
///
/// Mirrors `muon`'s own `RenameRule` (muon-derive/src/derive/meta.rs), so
/// that the trigger paths registered here match the mutation paths that
/// muon's `Observe` derive produces.
#[derive(Default, Clone, Copy)]
enum RenameRule {
    #[default]
    None,
    LowerCase,
    UpperCase,
    PascalCase,
    CamelCase,
    SnakeCase,
    ScreamingSnakeCase,
    KebabCase,
    ScreamingKebabCase,
}

impl RenameRule {
    fn from_str(input: &str) -> Option<Self> {
        Some(match input {
            "lowercase" => Self::LowerCase,
            "UPPERCASE" => Self::UpperCase,
            "PascalCase" => Self::PascalCase,
            "camelCase" => Self::CamelCase,
            "snake_case" => Self::SnakeCase,
            "SCREAMING_SNAKE_CASE" => Self::ScreamingSnakeCase,
            "kebab-case" => Self::KebabCase,
            "SCREAMING-KEBAB-CASE" => Self::ScreamingKebabCase,
            _ => return None,
        })
    }

    fn apply(self, name: &str) -> String {
        match self {
            Self::None => name.to_string(),
            Self::LowerCase => name.to_ascii_lowercase(),
            Self::UpperCase => name.to_ascii_uppercase(),
            Self::PascalCase => name.to_upper_camel_case(),
            Self::CamelCase => name.to_lower_camel_case(),
            Self::SnakeCase => name.to_snake_case(),
            Self::ScreamingSnakeCase => name.to_shouty_snake_case(),
            Self::KebabCase => name.to_kebab_case(),
            Self::ScreamingKebabCase => name.to_shouty_kebab_case(),
        }
    }
}

/// Key/value pairs from a `#[serde(...)]` attribute. A bare path (e.g.
/// `flatten`, `skip`) yields `(key, None)`.
fn serde_attrs(attrs: &[syn::Attribute]) -> Vec<(String, Option<String>)> {
    let mut out = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let syn::Meta::List(list) = &attr.meta else {
            continue;
        };
        let parsed = list
            .parse_args_with(
                syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated,
            )
            .unwrap_or_default();
        for meta in parsed {
            match meta {
                syn::Meta::NameValue(nv) => {
                    let key = nv.path.get_ident().map(|i| i.to_string());
                    let value = match &nv.value {
                        syn::Expr::Lit(syn::ExprLit {
                            lit: syn::Lit::Str(s),
                            ..
                        }) => Some(s.value()),
                        _ => None,
                    };
                    if let Some(key) = key {
                        out.push((key, value));
                    }
                }
                syn::Meta::Path(p) => {
                    if let Some(key) = p.get_ident().map(|i| i.to_string()) {
                        out.push((key, None));
                    }
                }
                _ => {}
            }
        }
    }
    out
}

fn serde_value<'a>(attrs: &'a [(String, Option<String>)], key: &str) -> Option<&'a str> {
    attrs
        .iter()
        .find(|(k, _)| k == key)
        .and_then(|(_, v)| v.as_deref())
}

fn has_serde_flag(attrs: &[(String, Option<String>)], key: &str) -> bool {
    attrs.iter().any(|(k, _)| k == key)
}

/// The effective path segment for a field: a field-level `rename` wins,
/// otherwise the container `rename_all` rule is applied — matching muon's
/// `Observe` derive path generation exactly.
fn path_segment(
    field_name: &str,
    field_serde: &[(String, Option<String>)],
    rename_all: RenameRule,
) -> String {
    if let Some(rename) = serde_value(field_serde, "rename") {
        rename.to_string()
    } else {
        rename_all.apply(field_name)
    }
}

/// Generates field accessors on `ReactiveStore` plus trigger registration.
///
/// For each field `f: T`, emits:
///
/// - a per-type accessor trait (`MuonTrack<Struct>`, parameterized by the
///   model's generics) implemented on `ReactiveStore<Struct>`, exposed
///   through the `StoreFieldAccess` deref bridge — `store.f()` returns a
///   `Field<Struct, T>` (offset read, reactive subscription) with no trait
///   import.
/// - `Reactivity::register_triggers` pre-inserting the field path.
///
/// The trigger path uses the **same serde naming as muon's `Observe`
/// derive** (field `rename` wins, else container `rename_all`), so a
/// renamed field's subscribers actually receive its mutation
/// notifications.
///
/// `#[track(skip)]`, `#[serde(skip)]` and `#[serde(skip_serializing)]`
/// exclude a field. `#[serde(flatten)]` is rejected: muon promotes
/// flattened child mutations to the parent level, which a single
/// `StorePath` accessor cannot represent — a silently-wrong subscription
/// is worse than a compile error.
#[proc_macro_derive(Reactivity, attributes(muon, serde, track))]
pub fn derive_reactivity(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let container_serde = serde_attrs(&input.attrs);
    let rename_all = serde_value(&container_serde, "rename_all")
        .and_then(RenameRule::from_str)
        .unwrap_or_default();

    if input.attrs.iter().any(|a| {
        a.path().is_ident("repr") && a.meta.to_token_stream().to_string().contains("packed")
    }) {
        return syn::Error::new_spanned(
            &input.ident,
            "Reactivity does not support #[repr(packed)] structs: field access forms references \
             that require the field's alignment",
        )
        .to_compile_error()
        .into();
    }
    let fields = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(n) => &n.named,
            _ => {
                return syn::Error::new_spanned(name, "Reactivity only supports named structs")
                    .to_compile_error()
                    .into()
            }
        },
        _ => {
            return syn::Error::new_spanned(name, "Reactivity only supports structs")
                .to_compile_error()
                .into()
        }
    };

    // The accessor trait is parameterized by the model's generics so
    // generic models work (`MuonTrackGeneric<T>`).
    let generic_params: Vec<&syn::GenericParam> = input
        .generics
        .params
        .iter()
        .filter(|p| {
            matches!(
                p,
                syn::GenericParam::Type(_)
                    | syn::GenericParam::Lifetime(_)
                    | syn::GenericParam::Const(_)
            )
        })
        .collect();
    let trait_params = generic_params.iter().map(|p| quote! { #p });
    let trait_param_idents: Vec<_> = generic_params
        .iter()
        .map(|p| match p {
            syn::GenericParam::Type(t) => {
                let i = &t.ident;
                quote! { #i }
            }
            syn::GenericParam::Lifetime(l) => {
                let lt = &l.lifetime;
                quote! { #lt }
            }
            syn::GenericParam::Const(c) => {
                let i = &c.ident;
                quote! { #i }
            }
        })
        .collect();

    let (ig, ty, _) = input.generics.split_for_impl();
    let st = quote! { #name #ty };
    let tn = syn::Ident::new(&format!("MuonTrack{}", name), name.span());
    let b = quote! { ::muon::Observe + Clone + 'static };
    // Merge the model's own `where` clause with the required bounds, so
    // generic models get a single valid clause on every generated impl.
    let mut where_clause = input
        .generics
        .where_clause
        .clone()
        .unwrap_or_else(|| syn::parse_quote!(where));
    where_clause.predicates.push(syn::parse_quote!(#st: #b));

    let mut ms = Vec::new();
    let mut mi = Vec::new();
    let mut rs = Vec::new();

    for f in fields {
        let fn_ = f.ident.as_ref().expect("named");
        let mut raw_name = fn_.to_string();
        if let Some(stripped) = raw_name.strip_prefix("r#") {
            raw_name = stripped.to_string();
        }
        let field_serde = serde_attrs(&f.attrs);
        if f.attrs.iter().any(|a| a.path().is_ident("track")
            && a.meta.to_token_stream().to_string().contains("skip"))
            || f.attrs.iter().any(|a| a.path().is_ident("muon")
                && (a.meta.to_token_stream().to_string().contains("skip")
                    || a.meta.to_token_stream().to_string().contains("noop")))
            || has_serde_flag(&field_serde, "skip")
            || has_serde_flag(&field_serde, "skip_serializing")
            // A custom `serialize_with` replaces the field's
            // serialization entirely — muon's Observe derive skips
            // such fields, so the accessor must not be generated.
            || has_serde_flag(&field_serde, "serialize_with")
        {
            continue;
        }
        if has_serde_flag(&field_serde, "flatten") {
            return syn::Error::new_spanned(
                fn_,
                "Reactivity does not support #[serde(flatten)]: muon promotes flattened child \
                 mutations to the parent level, which a single StorePath accessor cannot \
                 represent",
            )
            .to_compile_error()
            .into();
        }
        let ns = path_segment(&raw_name, &field_serde, rename_all);
        let ft = &f.ty;
        let o = quote! { ::core::mem::offset_of!(#st, #fn_) };
        ms.push(quote! { fn #fn_(&self) -> ::muon_reactivity::Field<#st, #ft>; });
        mi.push(quote! {
            fn #fn_(&self) -> ::muon_reactivity::Field<#st, #ft> {
                // SAFETY: `offset_of!` yields the exact byte offset of the
                // field inside `#st`, and the derive rejects
                // `#[repr(packed)]` layouts that would break alignment.
                unsafe {
                    ::muon_reactivity::Field::new(
                        ::std::iter::once(::muon::PathSegment::String(
                            (#ns).to_owned())).collect(),
                        #o,
                        self.core().clone(),
                        self.triggers().clone(),
                    )
                }
            }
        });
        rs.push(quote! { map.preinsert(&[::muon::PathSegment::String((#ns).to_owned())]); });
    }

    let expanded = quote! {
        #[doc(hidden)]
        pub trait #tn<#(#trait_params),*>: 'static { #(#ms)* }
        impl #ig #tn<#(#trait_param_idents),*> for ::muon_reactivity::ReactiveStore<#st> #where_clause { #(#mi)* }
        impl #ig ::muon_reactivity::StoreFieldAccess for #st #where_clause {
            type Target = dyn #tn<#(#trait_param_idents),*>;
            fn field_access(store: &::muon_reactivity::ReactiveStore<Self>) -> &Self::Target { store }
        }
        impl #ig ::muon_reactivity::Reactivity for #st #where_clause {
            fn register_triggers(map: &mut ::muon_reactivity::TriggerMap) { #(#rs)* }
        }
    };
    expanded.into()
}
