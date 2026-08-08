use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::parse_quote;
use syn::spanned::Spanned;

use crate::derive::GenericsDetector;
use crate::derive::meta::{AttributeKind, DeriveKind, ObserveMeta};

/// The field's serde rename as a string, when it is a string literal
/// (a non-literal rename falls back to the container rule).
fn rename_segment(field_meta: &ObserveMeta) -> Option<String> {
    match &field_meta.serde.rename {
        Some(syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(s),
            ..
        })) => Some(s.value()),
        _ => None,
    }
}

pub fn derive_snapshot(input: &syn::DeriveInput) -> TokenStream {
    let mut snapshot = input.clone();
    let mut input_name = input.ident.to_string();
    if input_name.starts_with("r#") {
        input_name = input_name[2..].to_string();
    }
    let snap_ident = syn::Ident::new(&format!("{input_name}Snapshot"), input.ident.span());
    // The snapshot helper must serialize like the observed value;
    // keep a `Serialize` derive (drop the muon/observe attributes).
    // Collect each named field's event segment (serde rename wins,
    // the container's rename rule fills in) before the helper's
    // attributes are stripped below.
    let mut errors = TokenStream::new();
    let derive_kind = match &input.data {
        syn::Data::Struct(_) => DeriveKind::Struct,
        syn::Data::Enum(_) => DeriveKind::Enum,
        _ => {
            return syn::Error::new(
                input.span(),
                "Snapshot can only be derived for structs and enums",
            )
            .to_compile_error();
        }
    };
    let input_meta =
        ObserveMeta::parse_attrs(&input.attrs, &mut errors, AttributeKind::Item, derive_kind);
    let mut segments: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    match &input.data {
        syn::Data::Struct(data_struct) => {
            if let syn::Fields::Named(fields) = &data_struct.fields {
                for field in &fields.named {
                    let field_meta = ObserveMeta::parse_attrs(
                        &field.attrs,
                        &mut errors,
                        AttributeKind::Field,
                        DeriveKind::Struct,
                    );
                    let name = field.ident.as_ref().unwrap().to_string();
                    let segment = rename_segment(&field_meta)
                        .unwrap_or_else(|| input_meta.serde.rename_all.apply(&name));
                    segments.insert(name, segment);
                }
            }
        }
        syn::Data::Enum(data_enum) => {
            for variant in &data_enum.variants {
                if let syn::Fields::Named(fields) = &variant.fields {
                    for field in &fields.named {
                        let field_meta = ObserveMeta::parse_attrs(
                            &field.attrs,
                            &mut errors,
                            AttributeKind::Field,
                            DeriveKind::Enum,
                        );
                        let name = field.ident.as_ref().unwrap().to_string();
                        let segment = rename_segment(&field_meta)
                            .unwrap_or_else(|| input_meta.serde.rename_all_fields.apply(&name));
                        segments.insert(name, segment);
                    }
                }
            }
        }
        _ => {}
    }
    if !errors.is_empty() {
        return errors;
    }
    // A bare `Serialize` ident (not a path): the fixture expander
    // re-expands generated derives and parses the attribute as
    // identifiers, and the user's `serde::Serialize` import is in
    // scope at the expansion site.
    snapshot.attrs = vec![parse_quote! { #[derive(Serialize)] }];
    snapshot.ident = snap_ident.clone();

    let where_predicates = &mut snapshot.generics.make_where_clause().predicates;
    match &mut snapshot.data {
        syn::Data::Struct(data_struct) => match &mut data_struct.fields {
            syn::Fields::Named(fields) => {
                for field in &mut fields.named {
                    field.attrs = vec![];
                    if GenericsDetector::detect(&field.ty, &input.generics) {
                        let field_ty = &field.ty;
                        where_predicates.push(parse_quote! {
                            #field_ty: ::muon::general::SerializeSnapshot
                        });
                        field.ty = parse_quote! {
                            <#field_ty as ::muon::general::Snapshot>::Snapshot
                        };
                    }
                }
            }
            syn::Fields::Unnamed(fields) => {
                for field in &mut fields.unnamed {
                    field.attrs = vec![];
                    if GenericsDetector::detect(&field.ty, &input.generics) {
                        let field_ty = &field.ty;
                        where_predicates.push(parse_quote! {
                            #field_ty: ::muon::general::SerializeSnapshot
                        });
                        field.ty = parse_quote! {
                            <#field_ty as ::muon::general::Snapshot>::Snapshot
                        };
                    }
                }
            }
            syn::Fields::Unit => {}
        },
        syn::Data::Enum(data_enum) => {
            for variant in &mut data_enum.variants {
                variant.attrs = vec![];
                match &mut variant.fields {
                    syn::Fields::Named(fields) => {
                        for field in &mut fields.named {
                            field.attrs = vec![];
                            if GenericsDetector::detect(&field.ty, &input.generics) {
                                let field_ty = &field.ty;
                                where_predicates.push(parse_quote! {
                                    #field_ty: ::muon::general::SerializeSnapshot
                                });
                                field.ty = parse_quote! {
                                    <#field_ty as ::muon::general::Snapshot>::Snapshot
                                };
                            }
                        }
                    }
                    syn::Fields::Unnamed(fields) => {
                        for field in &mut fields.unnamed {
                            field.attrs = vec![];
                            if GenericsDetector::detect(&field.ty, &input.generics) {
                                let field_ty = &field.ty;
                                where_predicates.push(parse_quote! {
                                    #field_ty: ::muon::general::SerializeSnapshot
                                });
                                field.ty = parse_quote! {
                                    <#field_ty as ::muon::general::Snapshot>::Snapshot
                                };
                            }
                        }
                    }
                    syn::Fields::Unit => {}
                }
            }
        }
        syn::Data::Union(_data_union) => {
            return syn::Error::new(input.span(), "PartialEq cannot be derived for unions")
                .to_compile_error();
        }
    }

    let (to_snapshot, flush_body) = match &input.data {
        syn::Data::Struct(data_struct) => match &data_struct.fields {
            syn::Fields::Named(fields) if fields.named.is_empty() => {
                (quote! { #snap_ident {} }, quote! { {} })
            }
            syn::Fields::Named(fields) => {
                let field_values = fields.named.iter().map(|field| {
                    let ident = field.ident.as_ref().unwrap();
                    let span = field.span();
                    quote_spanned! { span => #ident: ::muon::general::Snapshot::to_snapshot(&self.#ident) }
                });
                let flush_lets = fields.named.iter().map(|field| {
                    let ident = field.ident.as_ref().unwrap();
                    let span = field.span();
                    let name = segments
                        .get(&ident.to_string())
                        .cloned()
                        .unwrap_or_else(|| ident.to_string());
                    quote_spanned! { span =>
                        sink.push_field(#name);
                        ::muon::general::SerializeSnapshot::flush(&self.#ident, snapshot.#ident, sink);
                        sink.pop_segment();
                    }
                });
                (
                    quote! { #snap_ident { #(#field_values),* } },
                    quote! {
                        #(#flush_lets)*
                    },
                )
            }
            syn::Fields::Unnamed(fields) if fields.unnamed.is_empty() => {
                (quote! { #snap_ident () }, quote! { {} })
            }
            syn::Fields::Unnamed(fields) => {
                let field_values = fields.unnamed.iter().enumerate().map(|(i, field)| {
                    let index = syn::Index::from(i);
                    let span = field.span();
                    quote_spanned! { span => ::muon::general::Snapshot::to_snapshot(&self.#index) }
                });
                let flush_lets = fields.unnamed.iter().enumerate().map(|(i, field)| {
                    let index = syn::Index::from(i);
                    let span = field.span();
                    quote_spanned! { span =>
                        sink.push_index(#i);
                        ::muon::general::SerializeSnapshot::flush(&self.#index, snapshot.#index, sink);
                        sink.pop_segment();
                    }
                });
                (
                    quote! { #snap_ident ( #(#field_values),* ) },
                    quote! {
                        #(#flush_lets)*
                    },
                )
            }
            syn::Fields::Unit => (quote! { #snap_ident }, quote! { {} }),
        },
        syn::Data::Enum(data_enum) => {
            let (to_snapshot_arms, flush_arms): (Vec<_>, Vec<_>) = data_enum.variants.iter().map(|variant| {
                let variant_ident = &variant.ident;
                match &variant.fields {
                    syn::Fields::Named(fields) if fields.named.is_empty() => (
                        quote! {
                            Self::#variant_ident {} => #snap_ident::#variant_ident {}
                        },
                        quote! {
                            (Self::#variant_ident {}, #snap_ident::#variant_ident {}) => {}
                        },
                    ),
                    syn::Fields::Named(fields) => {
                        let field_idents: Vec<_> = fields
                            .named
                            .iter()
                            .map(|f| f.ident.as_ref().unwrap())
                            .collect();
                        let field_values = fields.named.iter().map(|field| {
                            let ident = field.ident.as_ref().unwrap();
                            let span = field.span();
                            quote_spanned! { span => #ident: ::muon::general::Snapshot::to_snapshot(#ident) }
                        });
                        let self_idents: Vec<_> = fields
                            .named
                            .iter()
                            .enumerate()
                            .map(|(i, f)| syn::Ident::new(&format!("__self_{}", i), f.span()))
                            .collect();
                        let snap_idents: Vec<_> = fields
                            .named
                            .iter()
                            .enumerate()
                            .map(|(i, f)| syn::Ident::new(&format!("__snap_{}", i), f.span()))
                            .collect();
                        let flush_lets = fields.named.iter().enumerate().map(|(i, field)| {
                            let ident = field.ident.as_ref().unwrap();
                            let span = field.span();
                            let self_ident = &self_idents[i];
                            let snap_ident = &snap_idents[i];
                            let name = segments
                                .get(&ident.to_string())
                                .cloned()
                                .unwrap_or_else(|| ident.to_string());
                            quote_spanned! { span =>
                                sink.push_field(#name);
                                ::muon::general::SerializeSnapshot::flush(#self_ident, #snap_ident, sink);
                                sink.pop_segment();
                            }
                        });
                        (
                            quote! {
                                Self::#variant_ident { #(#field_idents),* } => #snap_ident::#variant_ident { #(#field_values),* }
                            },
                            quote! {
                                (
                                    Self::#variant_ident { #(#field_idents: #self_idents),* },
                                    #snap_ident::#variant_ident { #(#field_idents: #snap_idents),* },
                                ) => {
                                    #(#flush_lets)*
                                }
                            },
                        )
                    }
                    syn::Fields::Unnamed(fields) if fields.unnamed.is_empty() => (
                        quote! {
                            Self::#variant_ident() => #snap_ident::#variant_ident()
                        },
                        quote! {
                            (Self::#variant_ident(), #snap_ident::#variant_ident()) => {}
                        },
                    ),
                    syn::Fields::Unnamed(fields) => {
                        let self_idents: Vec<_> = fields
                            .unnamed
                            .iter()
                            .enumerate()
                            .map(|(i, field)| syn::Ident::new(&format!("__self_{}", i), field.span()))
                            .collect();
                        let field_values = self_idents.iter().map(|ident| {
                            let span = ident.span();
                            quote_spanned! { span => ::muon::general::Snapshot::to_snapshot(#ident) }
                        });
                        let snap_idents: Vec<_> = fields
                            .unnamed
                            .iter()
                            .enumerate()
                            .map(|(i, field)| syn::Ident::new(&format!("__snap_{}", i), field.span()))
                            .collect();
                        let flush_lets = fields.unnamed.iter().enumerate().map(|(i, field)| {
                            let span = field.span();
                            let self_ident = &self_idents[i];
                            let snap_ident = &snap_idents[i];
                            quote_spanned! { span =>
                                sink.push_index(#i);
                                ::muon::general::SerializeSnapshot::flush(#self_ident, #snap_ident, sink);
                                sink.pop_segment();
                            }
                        });
                        (
                            quote! {
                                Self::#variant_ident( #(#self_idents),* ) => #snap_ident::#variant_ident( #(#field_values),* )
                            },
                            quote! {
                                (
                                    Self::#variant_ident( #(#self_idents),* ),
                                    #snap_ident::#variant_ident( #(#snap_idents),* ),
                                ) => {
                                    #(#flush_lets)*
                                }
                            },
                        )
                    }
                    syn::Fields::Unit => (
                        quote! { Self::#variant_ident => #snap_ident::#variant_ident },
                        quote! { (Self::#variant_ident, #snap_ident::#variant_ident) => {} },
                    ),
                }
            }).unzip();
            (
                quote! {
                    match self {
                        #(#to_snapshot_arms,)*
                    }
                },
                quote! {
                    match (self, snapshot) {
                        #(#flush_arms,)*
                        // A fallback variant's fields were partially
                        // moved by the arms above: the pre-change
                        // snapshot is not recoverable, so `before` is
                        // unknown (matching the whole-field replace
                        // semantics of the enum observer).
                        _ => sink.replace(None, Some(self as &dyn ::muon::erased_serde::Serialize)),
                    }
                },
            )
        }
        syn::Data::Union(_data_union) => unreachable!(),
    };

    let input_ident = &input.ident;
    let mut serialize_generics = snapshot.generics.clone();
    {
        // `snapshot.generics` already carries the field predicates
        // (generic fields: `FieldTy: SerializeSnapshot`), which
        // `SerializeSnapshot: Snapshot` discharges for the solver's
        // chain. Only the whole-value compare needs `Self: Serialize`
        // on top.
        let predicates = &mut serialize_generics.make_where_clause().predicates;
        predicates.push(parse_quote! { Self: ::serde::Serialize });
    }
    let (impl_generics, ty_generics, where_clause) = snapshot.generics.split_for_impl();
    let (_, _, serialize_where_clause) = serialize_generics.split_for_impl();
    quote! {
        const _: () = {
            #snapshot

            #[automatically_derived]
            impl #impl_generics ::muon::general::Snapshot for #input_ident #ty_generics #where_clause {
                type Snapshot = #snap_ident #ty_generics;
                fn to_snapshot(&self) -> Self::Snapshot {
                    #to_snapshot
                }
            }

            #[automatically_derived]
            impl #impl_generics ::muon::general::SerializeSnapshot for #input_ident #ty_generics #serialize_where_clause {
                fn flush<S: ::muon::observe::Sink + ?Sized>(&self, snapshot: Self::Snapshot, sink: &mut S) {
                    #flush_body
                }
            }
        };
    }
}

pub fn derive_noop_snapshot(input: &syn::DeriveInput) -> TokenStream {
    let input_ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    quote! {
        #[automatically_derived]
        impl #impl_generics ::muon::general::Snapshot for #input_ident #ty_generics #where_clause {
            type Snapshot = ();
            fn to_snapshot(&self) {}
        }

        #[automatically_derived]
        impl #impl_generics ::muon::general::SerializeSnapshot for #input_ident #ty_generics #where_clause {
            fn flush<S: ::muon::observe::Sink + ?Sized>(&self, _snapshot: (), _sink: &mut S) {
                {}
            }
        }
    }
}

pub fn derive_default(_input: &syn::DeriveInput) -> TokenStream {
    quote! {}
}
